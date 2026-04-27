"""YAML config loader for notification channels."""
from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from edge_catcher.notifications.adapters.file import FileChannel
from edge_catcher.notifications.adapters.smtp import SMTPChannel
from edge_catcher.notifications.adapters.stdout import StdoutChannel
from edge_catcher.notifications.adapters.webhook import WebhookChannel
from edge_catcher.notifications.base import Channel
from edge_catcher.notifications.exceptions import NotificationConfigError


_TYPE_TO_CLASS: dict[str, type] = {
	"stdout": StdoutChannel,
	"file": FileChannel,
	"webhook": WebhookChannel,
	"smtp": SMTPChannel,
}

# YAML-key → constructor-kwarg renaming (handles Python keywords like `from`).
_KEY_RENAMES: dict[str, str] = {
	"from": "from_addr",
}

# Allowed top-level keys.
_ALLOWED_TOP_LEVEL = {"version", "channels"}

# Allowed fields per channel type (excluding 'type'). Required fields
# are listed first in each tuple; remainder are optional.
_REQUIRED_FIELDS: dict[str, set[str]] = {
	"stdout": set(),
	"file": {"path"},
	"webhook": {"url"},
	"smtp": {"host", "port", "user", "password", "from", "to"},
}
_OPTIONAL_FIELDS: dict[str, set[str]] = {
	"stdout": set(),
	"file": set(),
	"webhook": {"style", "timeout_seconds"},
	"smtp": {"use_tls"},
}

_SUPPORTED_VERSION = 1
_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _pre_interpolate_text(text: str, source: str) -> str:
	"""Substitute ${ENV_VAR} occurrences in raw config text before YAML parsing.

	This runs before YAML parsing so that env var references in flow sequences
	(e.g. ``to: [${ALERT_EMAIL}, ops@x]``) don't trigger YAML parser errors
	(YAML treats bare ``{`` inside a flow sequence as a mapping opener).
	"""
	def replace(m: re.Match) -> str:
		var = m.group(1)
		try:
			return os.environ[var]
		except KeyError:
			raise NotificationConfigError(
				f"env var {var!r} referenced in {source} is not set"
			) from None
	return _ENV_VAR_RE.sub(replace, text)


def load_channels(config_path: Path | str) -> dict[str, Channel]:
	"""Read YAML config, instantiate adapters, return name → channel map."""
	path = Path(config_path)
	if not path.exists():
		raise NotificationConfigError(f"config not found: {path}")
	try:
		text = path.read_text(encoding="utf-8")
		# Pre-interpolate env vars at the text level so that ${VAR} in YAML
		# flow sequences (e.g. to: [${EMAIL}, other@x]) doesn't cause a parse
		# error — YAML treats bare { as a mapping opener in flow context.
		text = _pre_interpolate_text(text, str(path))
		raw = yaml.safe_load(text)
	except yaml.YAMLError as exc:
		raise NotificationConfigError(f"malformed YAML: {exc}") from exc

	if not isinstance(raw, dict):
		raise NotificationConfigError(f"top-level YAML must be a mapping; got {type(raw).__name__}")

	# Top-level unknown-keys check.
	unknown_top = set(raw.keys()) - _ALLOWED_TOP_LEVEL
	if unknown_top:
		raise NotificationConfigError(
			f"unknown top-level field(s): {sorted(unknown_top)}; allowed: {sorted(_ALLOWED_TOP_LEVEL)}"
		)

	# Version handshake. Must be int (or absent). String "1" is rejected
	# to keep the user signal honest — quoted YAML scalars usually mean
	# the user typed the wrong syntax.
	if "version" in raw:
		version = raw["version"]
		if not isinstance(version, int) or version != _SUPPORTED_VERSION:
			raise NotificationConfigError(
				f"config version {version!r} is not supported by this edge-catcher "
				f"(expected integer version: {_SUPPORTED_VERSION}). "
				f"Upgrade edge-catcher or pin to a v1 config."
			)

	channels_section = raw.get("channels")
	if not isinstance(channels_section, dict):
		raise NotificationConfigError("missing or invalid `channels:` mapping")

	out: dict[str, Channel] = {}
	for name, spec in channels_section.items():
		out[name] = _build_channel(name, spec)
	return out


def _build_channel(name: str, spec: dict) -> Channel:
	if not isinstance(spec, dict):
		raise NotificationConfigError(f"channel {name!r}: spec must be a mapping")
	channel_type = spec.get("type")
	if channel_type is None:
		raise NotificationConfigError(f"channel {name!r}: missing `type` field")
	if channel_type not in _TYPE_TO_CLASS:
		raise NotificationConfigError(
			f"channel {name!r}: unknown channel type {channel_type!r}; "
			f"allowed: {sorted(_TYPE_TO_CLASS)}"
		)

	required = _REQUIRED_FIELDS[channel_type]
	optional = _OPTIONAL_FIELDS[channel_type]
	allowed = required | optional | {"type"}

	# Validate field set.
	provided = set(spec.keys())
	missing = required - provided
	if missing:
		raise NotificationConfigError(
			f"channel {name!r}: missing required field(s): {sorted(missing)}"
		)
	unknown = provided - allowed
	if unknown:
		raise NotificationConfigError(
			f"channel {name!r}: unknown field(s): {sorted(unknown)}; "
			f"allowed: {sorted(allowed)}"
		)

	# Type-specific structural validation that goes beyond presence-of-field.
	if channel_type == "smtp":
		to_value = spec.get("to")
		if not isinstance(to_value, list) or not to_value:
			raise NotificationConfigError(
				f"channel {name!r}: `to` must be a non-empty list of recipient strings; "
				f"got {to_value!r}"
			)

	# Build constructor kwargs with key renames.
	# Note: env var interpolation was already performed at the text level
	# before YAML parsing (see _pre_interpolate_text), so values here are
	# already fully substituted. _interpolate is retained as a no-op safety
	# pass for any non-string values (int, bool, None) that need no action.
	kwargs: dict = {"name": name}
	for k, v in spec.items():
		if k == "type":
			continue
		dest_key = _KEY_RENAMES.get(k, k)
		kwargs[dest_key] = v

	cls = _TYPE_TO_CLASS[channel_type]
	try:
		return cls(**kwargs)
	except TypeError as exc:
		# Should not happen if our schema validation above is correct,
		# but surface it as a config error rather than a generic TypeError.
		raise NotificationConfigError(
			f"channel {name!r}: failed to construct {cls.__name__}: {exc}"
		) from exc


def _interpolate(value, channel_name: str, field: str):
	"""Recursively interpolate ${ENV_VAR} in string values.

	Strings: ${NAME} substring substitution (whole or embedded).
	Lists: each element interpolated.
	Other (int, bool, None, dict): passed through unchanged.
	"""
	if isinstance(value, str):
		def replace(m: re.Match) -> str:
			var = m.group(1)
			try:
				return os.environ[var]
			except KeyError:
				raise NotificationConfigError(
					f"env var {var!r} referenced in channels.{channel_name}.{field} is not set"
				) from None
		return _ENV_VAR_RE.sub(replace, value)
	if isinstance(value, list):
		return [_interpolate(v, channel_name, field) for v in value]
	return value
