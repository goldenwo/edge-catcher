"""Capture transport implementations.

``CaptureTransport`` is a structural protocol; concrete implementations:

  * ``LocalTransport`` — copies to/from a local directory. Used in tests and
    for dev-mode capture where the bundles don't need to travel.
  * ``R2Transport``   — Cloudflare R2 (S3-compatible) for production. The
    Pi uploads each rotated bundle; the dev workstation downloads for replay.

Adding a new backend (e.g. Backblaze B2, GCS) is one class that satisfies
``CaptureTransport`` — everything else is provider-neutral.

Environment variables (provider-neutral so a future migration doesn't
require renaming anything):

  * ``CAPTURE_TRANSPORT_BUCKET``
  * ``CAPTURE_TRANSPORT_ENDPOINT_URL``
  * ``CAPTURE_TRANSPORT_ACCESS_KEY_ID``
  * ``CAPTURE_TRANSPORT_SECRET_ACCESS_KEY``
"""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any, Iterator, Optional, Protocol

log = logging.getLogger(__name__)


class CaptureTransport(Protocol):
	"""Structural protocol for bundle upload/download/listing.

	All implementations must be idempotent: re-uploading the same key should
	overwrite without raising; downloading a missing key should raise
	FileNotFoundError (not silently produce an empty directory).
	"""

	def upload_bundle(self, local_path: Path, remote_key: str) -> None: ...
	def download_bundle(self, remote_key: str, local_path: Path) -> None: ...
	def list_bundles(self, prefix: str) -> Iterator[str]: ...


# ---------------------------------------------------------------------------
# LocalTransport
# ---------------------------------------------------------------------------


class LocalTransport:
	"""Local-disk transport. Used in tests and dev-mode capture."""

	def __init__(self, root: Path) -> None:
		self.root = Path(root)
		self.root.mkdir(parents=True, exist_ok=True)

	def upload_bundle(self, local_path: Path, remote_key: str) -> None:
		dst = self.root / remote_key
		if dst.exists():
			shutil.rmtree(dst)
		shutil.copytree(local_path, dst)

	def download_bundle(self, remote_key: str, local_path: Path) -> None:
		src = self.root / remote_key
		if not src.exists():
			raise FileNotFoundError(f"no bundle at {src}")
		if local_path.exists():
			shutil.rmtree(local_path)
		shutil.copytree(src, local_path)

	def list_bundles(self, prefix: str) -> Iterator[str]:
		base = self.root / prefix
		if not base.exists():
			return
		for child in sorted(base.iterdir()):
			if child.is_dir():
				yield f"{prefix}/{child.name}"


# ---------------------------------------------------------------------------
# R2Transport (S3-compatible via boto3)
# ---------------------------------------------------------------------------


def _s3_client_factory(**kwargs: Any) -> Any:
	"""Indirection seam for tests.

	Tests monkeypatch this symbol to return a MagicMock. Production code
	calls ``boto3.client("s3", **kwargs)``. Keeping it here (instead of
	importing boto3 at module level) also defers the boto3 import until
	someone actually constructs an R2Transport — tests that only use
	LocalTransport don't need boto3 installed.
	"""
	import boto3  # deferred import
	return boto3.client("s3", **kwargs)


class R2Transport:
	"""Cloudflare R2 (S3-compatible) transport.

	Reads credentials from either explicit constructor args OR the
	provider-neutral ``CAPTURE_TRANSPORT_*`` environment variables.
	Fails fast at construction time if neither is set.
	"""

	def __init__(
		self,
		bucket: Optional[str] = None,
		endpoint_url: Optional[str] = None,
		access_key: Optional[str] = None,
		secret_key: Optional[str] = None,
	) -> None:
		self.bucket = bucket or os.environ["CAPTURE_TRANSPORT_BUCKET"]
		endpoint = endpoint_url or os.environ["CAPTURE_TRANSPORT_ENDPOINT_URL"]
		key_id = access_key or os.environ["CAPTURE_TRANSPORT_ACCESS_KEY_ID"]
		secret = secret_key or os.environ["CAPTURE_TRANSPORT_SECRET_ACCESS_KEY"]
		self.client = _s3_client_factory(
			endpoint_url=endpoint,
			aws_access_key_id=key_id,
			aws_secret_access_key=secret,
		)

	def upload_bundle(self, local_path: Path, remote_key: str) -> None:
		"""Upload every regular file under ``local_path`` to ``<remote_key>/<relpath>``."""
		local_path = Path(local_path)
		for f in sorted(local_path.rglob("*")):
			if not f.is_file():
				continue
			rel = f.relative_to(local_path)
			key = f"{remote_key}/{rel.as_posix()}"
			self.client.upload_file(str(f), self.bucket, key)

	def download_bundle(self, remote_key: str, local_path: Path) -> None:
		"""Fetch every object under ``<remote_key>/`` into ``local_path`` mirroring the structure."""
		local_path = Path(local_path)
		local_path.mkdir(parents=True, exist_ok=True)
		paginator = self.client.get_paginator("list_objects_v2")
		prefix_with_slash = f"{remote_key}/"
		any_seen = False
		for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix_with_slash):
			for obj in page.get("Contents", []):
				any_seen = True
				key = obj["Key"]
				rel = key[len(prefix_with_slash):]
				dst = local_path / rel
				dst.parent.mkdir(parents=True, exist_ok=True)
				self.client.download_file(self.bucket, key, str(dst))
		if not any_seen:
			raise FileNotFoundError(f"no objects under {prefix_with_slash} in bucket {self.bucket}")

	def list_bundles(self, prefix: str) -> Iterator[str]:
		"""Yield immediate child "directories" under ``prefix/`` via S3 delimiter."""
		paginator = self.client.get_paginator("list_objects_v2")
		for page in paginator.paginate(
			Bucket=self.bucket,
			Prefix=f"{prefix}/",
			Delimiter="/",
		):
			for cp in page.get("CommonPrefixes", []) or []:
				yield cp["Prefix"].rstrip("/")
