"""Unit tests for edge_catcher.live.venue — the venue-neutral live-execution
contract (value objects + the client_order_id charset helpers).

Focus: the charset contract. ``_CLIENT_ORDER_ID_PATTERN`` (what the venue ACCEPTS)
and ``_CLIENT_ORDER_ID_DISALLOWED`` (what ``sanitize_client_order_id_component``
STRIPS) are two hand-maintained regexes that MUST stay exact complements. These
tests pin them together so a future edit that widens one charset without the other
fails loudly here instead of silently desyncing sanitize from validate (the
charset is independently encoded in a few places — venue's pattern, venue's
disallowed-inverse, and execution.py's ``_CLIENT_ORDER_ID_CHARSET`` — so a guard
that ties the two venue copies is the cheapest drift tripwire).
"""
from __future__ import annotations

from edge_catcher.live.venue import (
	_CLIENT_ORDER_ID_PATTERN,
	sanitize_client_order_id_component,
)


def test_sanitizer_output_satisfies_the_pattern() -> None:
	"""Any non-empty component, however hostile, sanitizes to a value that
	satisfies the venue's client_order_id charset contract."""
	dirty = "A.B/C:D E@F#G+H=I~J%K\\L"
	clean = sanitize_client_order_id_component(dirty)
	assert _CLIENT_ORDER_ID_PATTERN.match(clean), clean


def test_sanitizer_is_noop_on_already_clean_input() -> None:
	"""A component already in-charset is returned byte-identical — the property
	the reconciler's idempotency exact-string match depends on (a clean ticker
	must not drift across runs)."""
	clean = "KXSOL15M-26MAY16H12_yes-no"
	assert sanitize_client_order_id_component(clean) == clean


def test_disallowed_set_is_exact_complement_of_pattern_charset() -> None:
	"""``sanitize_client_order_id_component`` strips a character IFF the pattern
	would reject it as a one-character id. Walking all printable ASCII pins the
	DISALLOWED regex as the exact complement of the PATTERN charset, so widening
	one (e.g. someday allowing ``.``) without the other trips this guard rather
	than silently breaking sanitize-vs-validate."""
	for codepoint in range(32, 127):
		ch = chr(codepoint)
		survives = sanitize_client_order_id_component(ch) == ch
		allowed = _CLIENT_ORDER_ID_PATTERN.match(ch) is not None
		assert survives == allowed, (ch, survives, allowed)
