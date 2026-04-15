"""Tests for CaptureTransport implementations.

LocalTransport covers the happy path (and is what tests use for the replay
end-to-end path). R2Transport is verified against a mocked boto3 client so
we don't require live R2 credentials to run CI.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from edge_catcher.monitors.capture.transport import CaptureTransport, LocalTransport


# ---------------------------------------------------------------------------
# LocalTransport — round-trip happy path
# ---------------------------------------------------------------------------


def test_local_transport_upload_download_round_trip(tmp_path: Path) -> None:
	"""A bundle uploaded via LocalTransport can be downloaded back byte-for-byte."""
	src = tmp_path / "bundle-2026-04-14"
	src.mkdir()
	(src / "manifest.json").write_text('{"schema_version": 1}', encoding="utf-8")
	(src / "kalshi_engine_2026-04-14.jsonl.zst").write_bytes(b"\x28\xb5\x2f\xfd" + b"stub")
	(src / "paper-trader.yaml").write_text("strategies: {}\n", encoding="utf-8")

	remote_root = tmp_path / "remote"
	remote_root.mkdir()
	transport = LocalTransport(root=remote_root)

	transport.upload_bundle(src, "kalshi/2026-04-14")
	remote_dir = remote_root / "kalshi" / "2026-04-14"
	assert remote_dir.is_dir()
	assert (remote_dir / "manifest.json").read_text(encoding="utf-8") == '{"schema_version": 1}'
	assert (remote_dir / "kalshi_engine_2026-04-14.jsonl.zst").read_bytes() == b"\x28\xb5\x2f\xfd" + b"stub"

	# Download to a fresh location
	dst = tmp_path / "downloaded"
	transport.download_bundle("kalshi/2026-04-14", dst)
	assert (dst / "manifest.json").read_text(encoding="utf-8") == '{"schema_version": 1}'
	assert (dst / "kalshi_engine_2026-04-14.jsonl.zst").read_bytes() == b"\x28\xb5\x2f\xfd" + b"stub"


def test_local_transport_list_bundles(tmp_path: Path) -> None:
	"""list_bundles yields each child directory under the given prefix."""
	remote_root = tmp_path / "remote"
	(remote_root / "kalshi" / "2026-04-13").mkdir(parents=True)
	(remote_root / "kalshi" / "2026-04-14").mkdir(parents=True)
	(remote_root / "kalshi" / "2026-04-15").mkdir(parents=True)
	(remote_root / "kalshi" / "README.txt").write_text("not a bundle", encoding="utf-8")

	transport = LocalTransport(root=remote_root)
	bundles = list(transport.list_bundles("kalshi"))
	assert "kalshi/2026-04-13" in bundles
	assert "kalshi/2026-04-14" in bundles
	assert "kalshi/2026-04-15" in bundles
	# Files (not directories) should not be listed as bundles
	assert not any(b.endswith("README.txt") for b in bundles)


def test_local_transport_list_bundles_missing_prefix_yields_nothing(tmp_path: Path) -> None:
	"""An unknown prefix returns an empty iterator, not an error."""
	transport = LocalTransport(root=tmp_path / "empty_root")
	assert list(transport.list_bundles("kalshi")) == []


def test_local_transport_download_missing_bundle_raises(tmp_path: Path) -> None:
	"""Downloading a nonexistent remote_key should raise FileNotFoundError
	rather than silently producing an empty directory — the caller should
	know the prior bundle wasn't found."""
	transport = LocalTransport(root=tmp_path / "remote")
	with pytest.raises(FileNotFoundError):
		transport.download_bundle("kalshi/2099-12-31", tmp_path / "dst")


def test_local_transport_upload_overwrites_existing(tmp_path: Path) -> None:
	"""Re-uploading the same remote_key should overwrite, not fail."""
	src = tmp_path / "bundle"
	src.mkdir()
	(src / "manifest.json").write_text('{"v": 1}', encoding="utf-8")
	remote_root = tmp_path / "remote"
	remote_root.mkdir()
	transport = LocalTransport(root=remote_root)

	transport.upload_bundle(src, "kalshi/2026-04-14")
	# Second upload with different content
	(src / "manifest.json").write_text('{"v": 2}', encoding="utf-8")
	transport.upload_bundle(src, "kalshi/2026-04-14")

	assert (remote_root / "kalshi" / "2026-04-14" / "manifest.json").read_text(encoding="utf-8") == '{"v": 2}'


# ---------------------------------------------------------------------------
# Protocol satisfaction
# ---------------------------------------------------------------------------


def test_local_transport_satisfies_protocol() -> None:
	"""LocalTransport should be a structural match for CaptureTransport."""
	# Protocol check via hasattr — runtime_checkable isn't used on Protocol to
	# keep the interface lightweight. Verify the three methods exist.
	assert hasattr(LocalTransport, "upload_bundle")
	assert hasattr(LocalTransport, "download_bundle")
	assert hasattr(LocalTransport, "list_bundles")


# ---------------------------------------------------------------------------
# R2Transport — mocked boto3 client
# ---------------------------------------------------------------------------


def _mock_s3_client() -> MagicMock:
	"""Return a MagicMock that looks like a boto3 s3 client for our purposes."""
	client = MagicMock()
	client.upload_file = MagicMock()
	client.download_file = MagicMock()
	# paginator.paginate returns an iterable of pages, each with "Contents" and "CommonPrefixes"
	paginator = MagicMock()
	client.get_paginator = MagicMock(return_value=paginator)
	return client


def test_r2_transport_upload_bundle_sends_each_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
	"""R2Transport.upload_bundle should call s3.upload_file once per regular
	file under the bundle directory, keying on posix-style relative paths."""
	from edge_catcher.monitors.capture.transport import R2Transport

	# Build a bundle with a nested structure
	bundle = tmp_path / "bundle"
	bundle.mkdir()
	(bundle / "manifest.json").write_text('{"v": 1}', encoding="utf-8")
	(bundle / "data.bin").write_bytes(b"\x00\x01\x02")
	(bundle / "subdir").mkdir()
	(bundle / "subdir" / "nested.txt").write_text("hello", encoding="utf-8")

	# Stub boto3.client -> MagicMock
	mock_client = _mock_s3_client()
	import edge_catcher.monitors.capture.transport as transport_mod
	monkeypatch.setattr(transport_mod, "_s3_client_factory", lambda **kw: mock_client)

	transport = R2Transport(
		bucket="test-bucket",
		endpoint_url="https://example.r2.cloudflarestorage.com",
		access_key="k",
		secret_key="s",
	)
	transport.upload_bundle(bundle, "kalshi/2026-04-14")

	# 3 files uploaded
	assert mock_client.upload_file.call_count == 3
	# Collect the (local_path, bucket, key) triples
	calls = {
		args[2]  # Key (third positional arg in upload_file(Filename, Bucket, Key))
		for args, _kwargs in mock_client.upload_file.call_args_list
	}
	assert "kalshi/2026-04-14/manifest.json" in calls
	assert "kalshi/2026-04-14/data.bin" in calls
	assert "kalshi/2026-04-14/subdir/nested.txt" in calls


def test_r2_transport_download_bundle_fetches_each_object(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
	"""R2Transport.download_bundle should list objects under the prefix and
	download each one to the matching relative path under local_path."""
	from edge_catcher.monitors.capture.transport import R2Transport

	mock_client = _mock_s3_client()
	# paginator.paginate(...) yields pages with "Contents": [{Key: ...}, ...]
	mock_client.get_paginator.return_value.paginate = MagicMock(return_value=iter([
		{"Contents": [
			{"Key": "kalshi/2026-04-14/manifest.json"},
			{"Key": "kalshi/2026-04-14/subdir/nested.txt"},
		]}
	]))

	# Make download_file actually create the file so the test can verify path resolution
	def fake_download(bucket: str, key: str, local: str) -> None:
		p = Path(local)
		p.parent.mkdir(parents=True, exist_ok=True)
		p.write_text(f"stub-for-{key}", encoding="utf-8")

	mock_client.download_file = MagicMock(side_effect=fake_download)

	import edge_catcher.monitors.capture.transport as transport_mod
	monkeypatch.setattr(transport_mod, "_s3_client_factory", lambda **kw: mock_client)

	transport = R2Transport(
		bucket="test-bucket",
		endpoint_url="https://example.r2.cloudflarestorage.com",
		access_key="k",
		secret_key="s",
	)
	dst = tmp_path / "downloaded"
	transport.download_bundle("kalshi/2026-04-14", dst)

	assert (dst / "manifest.json").exists()
	assert (dst / "subdir" / "nested.txt").exists()
	assert "stub-for-kalshi/2026-04-14/manifest.json" in (dst / "manifest.json").read_text(encoding="utf-8")


def test_r2_transport_reads_credentials_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
	"""When constructor args are None, R2Transport falls back to the
	provider-neutral CAPTURE_TRANSPORT_* env vars."""
	from edge_catcher.monitors.capture.transport import R2Transport

	monkeypatch.setenv("CAPTURE_TRANSPORT_BUCKET", "env-bucket")
	monkeypatch.setenv("CAPTURE_TRANSPORT_ENDPOINT_URL", "https://env.example.com")
	monkeypatch.setenv("CAPTURE_TRANSPORT_ACCESS_KEY_ID", "env-key")
	monkeypatch.setenv("CAPTURE_TRANSPORT_SECRET_ACCESS_KEY", "env-secret")

	captured: dict = {}

	def capturing_factory(**kwargs):
		captured.update(kwargs)
		return _mock_s3_client()

	import edge_catcher.monitors.capture.transport as transport_mod
	monkeypatch.setattr(transport_mod, "_s3_client_factory", capturing_factory)

	R2Transport()  # no explicit args
	assert captured["endpoint_url"] == "https://env.example.com"
	assert captured["aws_access_key_id"] == "env-key"
	assert captured["aws_secret_access_key"] == "env-secret"
