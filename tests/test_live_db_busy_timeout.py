import sqlite3

from edge_catcher.live.state import connect_live_trades_db


def test_busy_timeout_is_set(tmp_path):
	conn = connect_live_trades_db(tmp_path / "live_trades.db")
	try:
		assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
		assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
	finally:
		conn.close()


def test_busy_timeout_5000_is_set_by_our_helper_not_an_inherent_sqlite_default(tmp_path):
	# Python's sqlite3.connect() defaults timeout=5.0s, which itself yields
	# busy_timeout=5000 on every platform — so the contract test above cannot
	# witness a TDD-RED and would false-green if the pragma were silently
	# removed while the connect-call keeps the default timeout. This test pins
	# the real safety property: a raw connection with timeout=0 has NO busy
	# wait (0), proving 5000 is not an inherent sqlite default; the live helper
	# deliberately establishes the 5000ms contention window (spec §5).
	raw = sqlite3.connect(str(tmp_path / "raw.db"), timeout=0)
	try:
		assert raw.execute("PRAGMA busy_timeout").fetchone()[0] == 0
	finally:
		raw.close()
	conn = connect_live_trades_db(tmp_path / "live2.db")
	try:
		assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
	finally:
		conn.close()
