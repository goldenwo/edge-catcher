"""Graduation handoff: turn statistical survivors into a takeability work-list.

This is a REPORTING/HANDOFF layer, not an executor. It ranks the crypto
survivors for a bounded, operator-authorized live test (the only honest
takeability oracle — edge_catcher/fill_realism_gate.py) and parks non-crypto
survivors as `unproven (needs-live-capability)` so they are never silently
promoted. Nothing here places live orders.
"""

from __future__ import annotations

# DB files that carry crypto series (have a Coinbase OHLC fair-value reference
# and live engine wiring), so candidates on them can be live-tested today.
_CRYPTO_DB_MARKERS = ("btc", "altcrypto", "crypto")

# Statistical verdicts that count as "survived the gauntlet".
_SURVIVOR_VERDICTS = frozenset({"promote", "review"})

_NEEDS_LIVE = "unproven (needs-live-capability)"


def is_crypto_db(db_path: str) -> bool:
	"""True if db_path is a crypto market DB (live-testable today)."""
	name = (db_path or "").replace("\\", "/").rsplit("/", 1)[-1].lower()
	return any(marker in name for marker in _CRYPTO_DB_MARKERS)


def shortlist_for_live(results: list[dict], top_n: int = 3) -> dict:
	"""Split statistical survivors into a crypto live-test shortlist + parked rest.

	``results`` are tracker rows (dicts from Tracker.list_results). Returns
	{"crypto_shortlist": [top_n crypto survivors, ranked by sharpe desc],
	 "parked_non_crypto": [non-crypto survivors, tagged needs-live-capability]}.
	"""
	survivors = [r for r in results if r.get("verdict") in _SURVIVOR_VERDICTS]

	crypto = sorted(
		(r for r in survivors if is_crypto_db(r.get("db_path", ""))),
		key=lambda r: r.get("sharpe", 0.0),
		reverse=True,
	)
	non_crypto = [r for r in survivors if not is_crypto_db(r.get("db_path", ""))]
	for r in non_crypto:
		r["takeability_status"] = _NEEDS_LIVE

	return {
		"crypto_shortlist": crypto[:top_n],
		"parked_non_crypto": non_crypto,
	}
