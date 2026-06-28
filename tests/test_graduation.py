"""Tests for the graduation handoff: crypto shortlist, non-crypto parking."""

from edge_catcher.research.graduation import is_crypto_db, shortlist_for_live


def _row(strategy, db, sharpe, verdict="promote"):
	# Mirrors the dict shape returned by Tracker.list_results().
	return {
		"strategy": strategy, "db_path": db, "series": "S", "sharpe": sharpe,
		"verdict": verdict, "hypothesis_id": f"{strategy}-{db}",
		"takeability_status": "unproven",
	}


def test_is_crypto_db():
	assert is_crypto_db("data/kalshi-btc.db") is True
	assert is_crypto_db("data/kalshi-altcrypto.db") is True
	assert is_crypto_db("data/kalshi-crypto.db") is True
	assert is_crypto_db("data/kalshi-sports.db") is False


def test_shortlist_ranks_crypto_and_parks_non_crypto():
	rows = [
		_row("a", "data/kalshi-altcrypto.db", 3.0),
		_row("b", "data/kalshi-altcrypto.db", 5.0),
		_row("c", "data/kalshi-altcrypto.db", 1.0),
		_row("d", "data/kalshi-sports.db", 9.0),
	]
	out = shortlist_for_live(rows, top_n=2)
	# Top-2 crypto by sharpe, ranked desc:
	assert [r["strategy"] for r in out["crypto_shortlist"]] == ["b", "a"]
	# Non-crypto survivor is parked, not shortlisted:
	assert [r["strategy"] for r in out["parked_non_crypto"]] == ["d"]
	assert out["parked_non_crypto"][0]["takeability_status"] == "unproven (needs-live-capability)"


def test_shortlist_ignores_non_survivors():
	rows = [_row("k", "data/kalshi-altcrypto.db", 4.0, verdict="kill")]
	out = shortlist_for_live(rows, top_n=3)
	assert out["crypto_shortlist"] == []
	assert out["parked_non_crypto"] == []


def test_shortlist_does_not_mutate_input_rows():
	row = _row("d", "data/kalshi-sports.db", 9.0)  # non-crypto survivor
	shortlist_for_live([row], top_n=3)
	assert row["takeability_status"] == "unproven"  # caller's row untouched
