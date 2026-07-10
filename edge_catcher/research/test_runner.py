"""Generic statistical test runner for hypothesis validation."""

from __future__ import annotations

import logging
import math
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, ClassVar, NamedTuple, Optional

from edge_catcher.adapters.kalshi.fees import INDEX_FEE, STANDARD_FEE
from edge_catcher.fees import ZERO_FEE, FeeModel
from edge_catcher.research.stats_utils import (
	clustered_z_from_stats,
	exact_binom_pvalue,
	fee_adjusted_edge_curve,
	mc_null_pvalue,
	proportions_ztest,
	t_pvalue,
	wilson_ci,
	z_over_excesses,
)

logger = logging.getLogger(__name__)

# Verdict constants
EDGE_EXISTS = "EDGE_EXISTS"
NO_EDGE = "NO_EDGE"
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"

# Artifact class (c) floor: a bucket's market-level expected wins AND losses
# (n_markets × mean in-band price / its complement) must both reach this for the
# clustered normal/t machinery to be trusted; below it the z can be pure price
# dispersion (wins == 0 or == n in every cluster) and only an exact binomial
# cross-check on the market-level outcome count counts as evidence.
MIN_EXPECTED_MARKET_OUTCOMES = 5.0

# Artifact class (d): simulations for the Monte-Carlo null gate on each
# EDGE_EXISTS candidate bucket. The achievable p floor is 1/(n_sims+1), so a
# FIXED count silently makes the gate unpassable once the Bonferroni alpha
# drops below it (a z=3.5 threshold with K>=5 evaluated buckets already does).
# MC_NULL_SIMS is the base; the verdict scales sims up so the floor sits an
# order of magnitude below the corrected alpha, capped at MC_NULL_SIMS_MAX to
# bound runtime (at the cap the floor is 5e-7 — passable for every realistic
# threshold config; z would need to exceed ~5.0 at K=1 to out-run it).
MC_NULL_SIMS = 10_000
MC_NULL_SIMS_MAX = 2_000_000

# One (day, n_trades, sum_price) row per in-band market — the MC null's input.
McMarketRows = list[tuple[Optional[str], int, float]]

# Momentum regime classification (MomentumAlignmentTest): the |lookback return|
# must exceed this for a directional regime; below it the regime is "flat" and
# its trades sit outside the hypothesis. Fixed, not a param — a sweepable
# threshold here is a garden of forking paths, and 0.1% per lookback window is
# well above spot microstructure noise at the 1-minute candles we capture.
FLAT_MOMENTUM_THRESHOLD = 0.001

# A candle's regime stops being current after this many MEDIAN inter-candle
# gaps without a fresher candle. Trades past that horizon are UNCLASSIFIED and
# excluded from grading: the pre-port implementation silently classified months
# of post-capture trades by the final candle's frozen regime label.
REGIME_STALENESS_GAPS = 10

# Fee model mapping: name → FeeModel. The gate applies each model's real
# per-contract fee curve via fee_adjusted_edge_curve() (see stats_utils.py), so
# fee accounting matches live execution rather than a flat approximation.
FEE_MODELS: dict[str, FeeModel] = {
	"zero": ZERO_FEE,
	"standard": STANDARD_FEE,
	"index": INDEX_FEE,
}
# "kalshi" is the name research configs pass for the live Kalshi taker fee; it
# resolves to the standard fee model. Aliased (not a second entry) so the two
# can't silently drift apart.
FEE_MODELS["kalshi"] = FEE_MODELS["standard"]


def _resolve_fee_model(fee_model: str) -> FeeModel:
	"""Map a fee_model name to its FeeModel, failing loud on unknown names.

	An unknown name used to fall back to 0.0 silently, which ran the fee-adjusted
	edge gate with ZERO fees — a false-positive risk where a small raw edge could
	pass a gate that real fees would have killed. Unknown names now raise.
	"""
	if fee_model not in FEE_MODELS:
		raise ValueError(
			f"Unknown fee_model {fee_model!r}. Valid options: {sorted(FEE_MODELS)}"
		)
	return FEE_MODELS[fee_model]


def _normalize_buckets(buckets: list[list[float]]) -> list[tuple[float, float]]:
	"""Normalize bucket bounds to the 0–1 implied-probability scale, PER BUCKET.

	Fail-safe for #1: the LLM ideator historically emitted cents-scale buckets
	(e.g. [[1,30]]) against 0–1 implied data, so `1 <= implied` was unsatisfiable
	→ every bucket n=0 → a silent INSUFFICIENT_DATA total-drop.

	Normalization is PER BUCKET (not whole-list): a single cents-scale bucket must
	not corrupt valid 0–1 siblings. For each `[lo, hi]`, if EITHER bound > 1.0 the
	bucket is treated as cents-scale and both its bounds are divided by 100; ≤1
	bounds are left as-is (a `[0, 1]` bucket is the full 0–1 probability range —
	the correct default, not cents to disambiguate). We warn once per call listing
	which buckets were rescaled — turning a silent drop into a corrected run plus a
	visible warning (mirrors the fail-loud stance on fee models).

	After normalization we VALIDATE and raise ValueError (fail loud, consistent with
	the fee_model handling) if any bucket has `lo >= hi`, `lo < 0`, or `hi > 1.0`.
	"""
	normalized: list[tuple[float, float]] = []
	rescaled: list[list[float]] = []
	for lo, hi in buckets:
		if lo > 1.0 or hi > 1.0:
			rescaled.append([lo, hi])
			normalized.append((lo / 100.0, hi / 100.0))
		else:
			normalized.append((lo, hi))

	if rescaled:
		logger.warning(
			"Bucket(s) %r appear to be cents-scale (a bound > 1.0); "
			"auto-normalizing those to the 0–1 implied-probability scale by dividing by 100. "
			"Configs should pass buckets on a 0–1 scale (e.g. [[0.01,0.30]]).",
			rescaled,
		)

	for lo, hi in normalized:
		if lo >= hi or lo < 0.0 or hi > 1.0:
			raise ValueError(
				f"Invalid bucket [{lo}, {hi}] after normalization: require "
				f"0.0 <= lo < hi <= 1.0."
			)
		# Kalshi prices are INTEGER cents; the SQL band filter binds integer-cent
		# bounds. A sub-cent bound (e.g. 0.115) cannot be represented — float
		# rounding silently empties one band and double-widens its neighbor
		# (round(0.115*100) == 12 == round(0.125*100)), so fail loud instead.
		for bound in (lo, hi):
			if abs(bound * 100 - round(bound * 100)) > 1e-6:
				raise ValueError(
					f"Bucket bound {bound} is not integer-cent representable; "
					f"bounds must be whole cents on the 0-1 scale (e.g. 0.12, not 0.115)."
				)

	return normalized


class _BandDayStats(NamedTuple):
	"""Per-day-cluster aggregate of in-band trades (one SQL GROUP BY row per day).

	The per-taker-side splits (artifact class (b)) carry the same n/wins/Σprice
	shape restricted to prints where the aggressor was the YES / NO buyer. Rows
	whose taker_side is neither value (defensive — the schema says NOT NULL
	'yes'/'no') count in the totals but in neither side split.
	"""

	day: Optional[str]
	n_trades: int
	n_markets: int
	wins: int
	sum_price: float  # Σ per-trade yes_price on the 0–1 scale
	market_wins: int = 0  # distinct in-band markets that settled YES (class (c))
	yes_n: int = 0
	yes_wins: int = 0
	yes_sum_price: float = 0.0
	no_n: int = 0
	no_wins: int = 0
	no_sum_price: float = 0.0


def _bands_to_cents(bucket_tuples: list[tuple[float, float]]) -> list[tuple[int, int]]:
	"""Convert validated 0-1 bounds to the INTEGER cent bounds the SQL binds.

	round() (not int()) because `0.07*100 == 7.0000000000000009 > 7` would
	silently misfile boundary cents against the INTEGER yes_price column;
	_normalize_buckets already rejected sub-cent bounds, so round() is exact.
	"""
	return [(round(lo * 100), round(hi * 100)) for lo, hi in bucket_tuples]


def _bands_are_disjoint(bands_c: list[tuple[int, int]]) -> bool:
	"""True when no two [lo, hi) cent bands overlap (shared boundaries are fine)."""
	ordered = sorted(bands_c)
	return all(prev[1] <= curr[0] for prev, curr in zip(ordered, ordered[1:]))


# Per-print CAUSAL cumulative volume: contracts traded in the market STRICTLY
# BEFORE each print (running Σ of prior t.count, ordered by created_time with
# trade_id as the deterministic tiebreak; a market's first print sits at 0).
# This is the only volume a trader standing at that print could have observed —
# final settled m.volume is outcome-endogenous on in-play venues (artifact
# class (a)). Placeholder rows (count <= 0/NULL) and unorderable rows (NULL
# created_time) are excluded from both the running sum and the observations.
# Binds one param: series_ticker.
_CAUSAL_CUM_VOLUME_SUBQUERY = (
	"SELECT t.ticker, t.trade_id, t.yes_price, t.count, t.created_time, t.taker_side, "
	"       COALESCE(SUM(t.count) OVER ("
	"PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id "
	"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS cum_before "
	"FROM trades t JOIN markets m ON t.ticker = m.ticker "
	"WHERE m.series_ticker = ? AND m.result IN ('yes', 'no') "
	"AND t.count > 0 AND t.created_time IS NOT NULL"
)


# Per-print momentum regime at TRADE time: each print picks up the regime of
# the temp._momentum_regime interval containing its epoch second (populated by
# MomentumAlignmentTest from the OHLC candles BEFORE the scan; intervals are
# non-overlapping by construction, the ORDER BY/LIMIT is defensive). The trade
# time is CAST through strftime('%s') to INTEGER — binding a TEXT ISO time
# against an INTEGER epoch column compares by SQLite TYPE ORDER (every INTEGER
# < any TEXT), which is exactly the affinity bug that made the pre-port test
# classify every market by the last candle in the DB. Prints outside every
# interval (before coverage, past the staleness horizon, or in a candle gap)
# get regime NULL and match no regime filter. Binds one param: series_ticker.
_MOMENTUM_REGIME_SUBQUERY = (
	"SELECT t.ticker, t.trade_id, t.yes_price, t.count, t.created_time, t.taker_side, "
	"       (SELECT CASE WHEN r.ts_end > CAST(strftime('%s', t.created_time) AS INTEGER) "
	"                    THEN r.regime END "
	"        FROM _momentum_regime r "
	"        WHERE r.ts_start <= CAST(strftime('%s', t.created_time) AS INTEGER) "
	"        ORDER BY r.ts_start DESC LIMIT 1) AS regime "
	"FROM trades t JOIN markets m ON t.ticker = m.ticker "
	"WHERE m.series_ticker = ? AND m.result IN ('yes', 'no') "
	"AND t.count > 0 AND t.created_time IS NOT NULL"
)


def _band_scan_clauses(
	series: str,
	cum_volume_range: Optional[tuple[Optional[float], Optional[float]]] = None,
	lifecycle_segment: Optional[tuple[str, int]] = None,
	momentum_regime: Optional[str] = None,
) -> tuple[str, list[str], list[object]]:
	"""Shared FROM/WHERE construction for in-band trade scans.

	Used by _per_trade_band_day_stats (day aggregates) and _per_market_band_stats
	(per-market MC-null rows) so both populations are filtered IDENTICALLY.
	Returns (from_clause, where_parts, params) — band predicates are appended by
	the caller.

	`cum_volume_range` = (gt, le) filters on each print's CAUSAL cumulative
	prior volume (_CAUSAL_CUM_VOLUME_SUBQUERY) — exclusive lower, inclusive
	upper, either side None for unbounded. Membership semantics match the
	tercile-bound quantile convention in VolumeMispricingTest exactly.

	`lifecycle_segment` = ("early"|"late", window_minutes) restricts to trades at
	most / strictly-more-than window minutes after the market's open_time. The
	derived table keeps the cutoff expression in one place, but SQLite's query
	flattener inlines it (simple SELECT, no aggregate), so strftime(open_time)
	still evaluates per joined trade row — the flattened plan keeps the markets
	PK index and benchmarked fine; do not "optimize" assuming a materialized
	per-market hoist happened. Both strftime('%s', ...) values are CAST to
	INTEGER — uncast, SQLite's type affinity compares the TEXT strftime result
	GREATER than any number, which silently empties the early segment. Trades
	with NULL created_time and markets with NULL open_time are excluded (they
	cannot be segmented). Unknown segment names raise.

	`momentum_regime` = "up"|"down"|"flat" restricts to trades whose at-trade
	momentum regime matches (_MOMENTUM_REGIME_SUBQUERY; requires
	temp._momentum_regime to exist on the scanned connection). Unclassified
	prints (regime NULL) never match.

	The three filters are mutually exclusive — at most one per scan.
	"""
	filters_set = sum(
		f is not None for f in (cum_volume_range, lifecycle_segment, momentum_regime)
	)
	if filters_set > 1:
		raise ValueError(
			"cum_volume_range, lifecycle_segment and momentum_regime cannot be combined"
		)

	where_parts = ["m.result IN ('yes', 'no')", "t.count > 0"]
	params: list[object] = []

	if momentum_regime is not None:
		if momentum_regime not in ("up", "down", "flat"):
			raise ValueError(
				f"Unknown momentum regime {momentum_regime!r}: use 'up', 'down' or 'flat'"
			)
		from_clause = (
			f"FROM ({_MOMENTUM_REGIME_SUBQUERY}) t "
			"JOIN markets m ON t.ticker = m.ticker"
		)
		params = [series]
		where_parts.append("t.regime = ?")
		params.append(momentum_regime)
	elif lifecycle_segment is not None:
		segment, window_minutes = lifecycle_segment
		if segment not in ("early", "late"):
			raise ValueError(f"Unknown lifecycle segment {segment!r}: use 'early' or 'late'")
		op = "<=" if segment == "early" else ">"
		from_clause = (
			"FROM trades t JOIN ("
			"SELECT ticker, result, close_time, volume, "
			"       CAST(strftime('%s', open_time) AS INTEGER) + ? * 60 AS cutoff_ts "
			"FROM markets WHERE series_ticker = ? AND open_time IS NOT NULL"
			") m ON t.ticker = m.ticker"
		)
		params = [window_minutes, series]
		where_parts.append("t.created_time IS NOT NULL")
		where_parts.append(
			f"CAST(strftime('%s', t.created_time) AS INTEGER) {op} m.cutoff_ts"
		)
	elif cum_volume_range is not None:
		# The derived trade table carries cum_before; the outer join back to
		# markets supplies result/close_time for the aggregate columns.
		from_clause = (
			f"FROM ({_CAUSAL_CUM_VOLUME_SUBQUERY}) t "
			"JOIN markets m ON t.ticker = m.ticker"
		)
		params = [series]
		cum_gt, cum_le = cum_volume_range
		if cum_gt is not None:
			where_parts.append("t.cum_before > ?")
			params.append(cum_gt)
		if cum_le is not None:
			where_parts.append("t.cum_before <= ?")
			params.append(cum_le)
	else:
		from_clause = "FROM trades t JOIN markets m ON t.ticker = m.ticker"
		where_parts.insert(0, "m.series_ticker = ?")
		params = [series]

	return from_clause, where_parts, params


def _per_market_band_stats(
	cursor: sqlite3.Cursor,
	series: str,
	bands_c: list[tuple[int, int]],
	cum_volume_range: Optional[tuple[Optional[float], Optional[float]]] = None,
	lifecycle_segment: Optional[tuple[str, int]] = None,
	momentum_regime: Optional[str] = None,
) -> list[McMarketRows]:
	"""Per-market in-band aggregates, one row list per band (parallel to bands_c).

	One (close-date day, n_trades, Σ price on 0–1) row per in-band MARKET.
	This single scan feeds BOTH market-level gate baselines — the class (c)/(f)
	per-market mean price (the trade-weighted band mean is a WRONG baseline for
	market-level outcome counts whenever print count correlates with price
	inside the band) — and the class (d) MC null, whose redraw unit is the
	market. Filters are shared verbatim with _per_trade_band_day_stats via
	_band_scan_clauses, so the graded population and the gate populations are
	identical by construction. `day` rides along the GROUP BY exactly because
	each market has one close_time. Disjoint bands (the normal case) are
	answered in ONE scan via a CASE selector, mirroring
	_per_trade_band_day_stats; overlapping bands fall back to one scan each.
	"""
	if not bands_c:
		return []
	from_clause, where_parts, params = _band_scan_clauses(
		series, cum_volume_range, lifecycle_segment, momentum_regime
	)
	agg_columns = (
		"substr(m.close_time, 1, 10) AS day, "
		"COUNT(*) AS n_trades, "
		"SUM(t.yes_price) / 100.0 AS sum_price"
	)
	per_band: list[McMarketRows] = [[] for _ in bands_c]

	if _bands_are_disjoint(bands_c):
		band_case = "CASE " + " ".join(
			f"WHEN t.yes_price >= {lo} AND t.yes_price < {hi} THEN {idx}"
			for idx, (lo, hi) in enumerate(bands_c)
		) + " END"
		min_lo = min(lo for lo, _ in bands_c)
		max_hi = max(hi for _, hi in bands_c)
		sql = (
			f"SELECT {band_case} AS band, {agg_columns} "
			f"{from_clause} "
			f"WHERE {' AND '.join(where_parts)} "
			f"  AND t.yes_price >= {min_lo} AND t.yes_price < {max_hi} "
			"GROUP BY band, t.ticker"
		)
		cursor.execute(sql, params)
		for band, day, n_trades, sum_price in cursor.fetchall():
			if band is None:
				continue  # trade in a gap between non-contiguous bands
			per_band[band].append((day, n_trades, float(sum_price)))
	else:
		for idx, (lo, hi) in enumerate(bands_c):
			sql = (
				f"SELECT {agg_columns} "
				f"{from_clause} "
				f"WHERE {' AND '.join(where_parts)} "
				f"  AND t.yes_price >= {lo} AND t.yes_price < {hi} "
				"GROUP BY t.ticker"
			)
			cursor.execute(sql, params)
			per_band[idx] = [
				(day, n_trades, float(sum_price))
				for day, n_trades, sum_price in cursor.fetchall()
			]

	return per_band


def _attach_per_market_stats(
	cursor: sqlite3.Cursor,
	series: str,
	bucket_results: list[dict],
	cum_volume_range: Optional[tuple[Optional[float], Optional[float]]] = None,
	lifecycle_segment: Optional[tuple[str, int]] = None,
	momentum_regime: Optional[str] = None,
) -> Callable[[dict], McMarketRows]:
	"""One batched per-market scan for every evaluated bucket.

	Annotates each entry with "per_market_mean_price" (mean over in-band markets
	of their per-market mean in-band price — the honest baseline for the class
	(c)/(f) market-level gates) and returns the mc_rows_fn the MC gate reuses,
	so the graded scan, the gate baselines, and the MC null all describe the
	SAME population fetched with the SAME filters, specified once per run.
	"""
	per_band = _per_market_band_stats(
		cursor, series, [_entry_band_c(b) for b in bucket_results],
		cum_volume_range, lifecycle_segment, momentum_regime,
	)
	rows_by_band: dict[tuple[float, float], McMarketRows] = {}
	for b, rows in zip(bucket_results, per_band):
		rows_by_band[(b["bucket_lo"], b["bucket_hi"])] = rows
		if rows:
			b["per_market_mean_price"] = sum(sp / n for _, n, sp in rows) / len(rows)
	return lambda b: rows_by_band[(b["bucket_lo"], b["bucket_hi"])]


def _entry_band_c(bucket_entry: dict) -> tuple[int, int]:
	"""A bucket entry's integer-cent band bounds (delegates to _bands_to_cents so
	the round()-not-int() rationale lives in exactly one place)."""
	return _bands_to_cents([(bucket_entry["bucket_lo"], bucket_entry["bucket_hi"])])[0]


def _row_to_band_day_stats(row: tuple) -> _BandDayStats:
	"""Map one aggregate SQL row (agg_columns order) to _BandDayStats.

	The single place that owns the positional column mapping — both scan
	branches route through it, so a column added to agg_columns cannot be
	unpacked differently by the two.
	"""
	(
		day, n_trades, n_markets, wins, sum_price, market_wins,
		y_n, y_w, y_sp, n_n, n_w, n_sp,
	) = row
	return _BandDayStats(
		day, n_trades, n_markets, wins, float(sum_price), market_wins,
		y_n, y_w, float(y_sp), n_n, n_w, float(n_sp),
	)


def _per_trade_band_day_stats(
	cursor: sqlite3.Cursor,
	series: str,
	bands_c: list[tuple[int, int]],
	cum_volume_range: Optional[tuple[Optional[float], Optional[float]]] = None,
	lifecycle_segment: Optional[tuple[str, int]] = None,
	momentum_regime: Optional[str] = None,
) -> list[list[_BandDayStats]]:
	"""TRUE per-trade calibration aggregates per price band, per day.

	One observation per TRADE ROW whose yes_price falls in the band: (trade price,
	market's settled outcome), clustered by the market's close_date (day). This is
	the method that survives the known-dead controls: pooling per-market rows
	(GROUP BY ticker — one row per market that ever TOUCHED the band) fabricates a
	favorites-overpriced / longshots-underpriced signature, because under a fair
	price process every YES-settler transits the high bands on its way to 99¢ while
	a NO-settler touches them only if it was once genuinely there — conditioning on
	TOUCH, not on the price a trade was actually placed at. Verified 2026-07-02: a
	series with FLAT per-trade calibration graded EDGE_EXISTS under the per-market
	method; per-trade grading kills the artifact.

	Aggregation happens in SQL (real bands hold millions of trade rows): one result
	row per close_date with n_trades, n_markets (distinct tickers — each market has
	exactly one close_date, so summing across days never double-counts), wins
	(trades in YES-settling markets), and Σ trade price. Feed
	(n_trades, wins, sum_price) per day to clustered_z_from_stats for the
	day-clustered z: effective N = #independent days, which absorbs both intraday
	market correlation (~96 correlated 15-min markets/day) and within-market trade
	correlation (all of a market's trades share its close_date cluster). Matches the
	sibling hypotheses/kalshi/price_efficiency.py day-clustering. Observations are
	unweighted by t.count: count-weighting would let single whale prints dominate a
	day; a many-print market still dominates its day's point estimate — accepted,
	it is exactly what the verified control measured, and the day-cluster SE
	absorbs it.

	Only 'yes'/'no' settlements count: any other result (voided/scratched) is
	excluded entirely — counting a void as NO would bias every band.

	Per-trade calibration is unbiased ONLY if the trade price is a fair belief at
	trade time. Systematic in-band price drift toward the outcome CAN surface as a
	real-looking residual; interpreting such a residual as inefficiency vs drift
	needs the known-dead control comparison (see tests: the transit-artifact control
	must grade NO_EDGE).

	Trades with count <= 0 or NULL are placeholder rows, not prints — excluded
	(the old count-weighted SQL excluded them incidentally; here it is deliberate).

	`bands_c` are INTEGER cent bounds from _bands_to_cents. The result list is
	parallel to `bands_c` (one per-day list per band). Multi-GB DBs make each scan
	expensive, so DISJOINT bands (the normal case — grids are partitions) are
	answered with ONE scan via a CASE band selector; overlapping bands fall back to
	one scan per band, because a CASE would assign a trade to only its first
	matching band and silently under-count the others.

	`cum_volume_range` / `lifecycle_segment` / `momentum_regime` filter
	semantics live in _band_scan_clauses (shared with the MC null's per-market
	scan so both populations filter identically). A volume tercile is a range predicate, NOT
	a ticker IN(...) list — a tercile of a long-history series exceeds SQLite's
	bound-variable limit and crashes.

	A NULL close_time yields a single NULL day (one pooled cluster), matching
	clustered_z's "__no_key__" pooling.
	"""
	if not bands_c:
		return []
	from_clause, where_parts, scan_params = _band_scan_clauses(
		series, cum_volume_range, lifecycle_segment, momentum_regime
	)

	agg_columns = (
		"substr(m.close_time, 1, 10) AS day, "
		"COUNT(*) AS n_trades, "
		"COUNT(DISTINCT t.ticker) AS n_markets, "
		"SUM(CASE WHEN m.result = 'yes' THEN 1 ELSE 0 END) AS wins, "
		"SUM(t.yes_price) / 100.0 AS sum_price, "
		"COUNT(DISTINCT CASE WHEN m.result = 'yes' THEN t.ticker END) AS market_wins, "
		"SUM(CASE WHEN t.taker_side = 'yes' THEN 1 ELSE 0 END) AS yes_n, "
		"SUM(CASE WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN 1 ELSE 0 END) AS yes_wins, "
		"SUM(CASE WHEN t.taker_side = 'yes' THEN t.yes_price ELSE 0 END) / 100.0 AS yes_sum_price, "
		"SUM(CASE WHEN t.taker_side = 'no' THEN 1 ELSE 0 END) AS no_n, "
		"SUM(CASE WHEN t.taker_side = 'no' AND m.result = 'yes' THEN 1 ELSE 0 END) AS no_wins, "
		"SUM(CASE WHEN t.taker_side = 'no' THEN t.yes_price ELSE 0 END) / 100.0 AS no_sum_price"
	)

	def _run(sql: str, params: list[object]) -> sqlite3.Cursor:
		cursor.execute(sql, params)
		return cursor

	per_band: list[list[_BandDayStats]] = [[] for _ in bands_c]

	if _bands_are_disjoint(bands_c):
		# Single pass: the CASE selector is exact because bands cannot overlap.
		# Bounds are validated integers, safe to inline (keeps bind params tiny).
		band_case = "CASE " + " ".join(
			f"WHEN t.yes_price >= {lo} AND t.yes_price < {hi} THEN {idx}"
			for idx, (lo, hi) in enumerate(bands_c)
		) + " END"
		min_lo = min(lo for lo, _ in bands_c)
		max_hi = max(hi for _, hi in bands_c)
		sql = (
			f"SELECT {band_case} AS band, {agg_columns} "
			f"{from_clause} "
			f"WHERE {' AND '.join(where_parts)} "
			f"  AND t.yes_price >= {min_lo} AND t.yes_price < {max_hi} "
			"GROUP BY band, day"
		)
		for row in _run(sql, scan_params).fetchall():
			band = row[0]
			if band is None:
				continue  # trade in a gap between non-contiguous bands
			per_band[band].append(_row_to_band_day_stats(row[1:]))
	else:
		for idx, (lo, hi) in enumerate(bands_c):
			sql = (
				f"SELECT {agg_columns} "
				f"{from_clause} "
				f"WHERE {' AND '.join(where_parts)} "
				f"  AND t.yes_price >= {lo} AND t.yes_price < {hi} "
				"GROUP BY day"
			)
			per_band[idx] = [
				_row_to_band_day_stats(row) for row in _run(sql, scan_params).fetchall()
			]

	return per_band


class _BandSummary(NamedTuple):
	"""Bucket-level reduction of per-day aggregates + the day-clustered z."""

	n_trades: int
	n_markets: int
	wins: int
	sum_price: float
	mean_price: float
	win_rate: float
	edge: float
	z: float
	p: float
	n_clusters: int
	market_wins: int


def _summarize_band(day_stats: list[_BandDayStats]) -> _BandSummary:
	"""Reduce per-day aggregates to bucket stats.

	Summing n_markets (and market_wins) across days is exact: each market has one
	close_date, so it appears in exactly one day row. An empty band returns an
	all-zero summary (p = 1.0) so callers can floor-check without special-casing.
	"""
	n_trades = sum(s.n_trades for s in day_stats)
	if n_trades == 0:
		return _BandSummary(0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0, 0)
	n_markets = sum(s.n_markets for s in day_stats)
	wins = sum(s.wins for s in day_stats)
	sum_price = sum(s.sum_price for s in day_stats)
	mean_price = sum_price / n_trades
	win_rate = wins / n_trades
	z, p, n_clusters = clustered_z_from_stats(
		[(s.n_trades, s.wins, s.sum_price) for s in day_stats]
	)
	return _BandSummary(
		n_trades, n_markets, wins, sum_price,
		mean_price, win_rate, win_rate - mean_price,
		float(z), float(p), n_clusters,
		sum(s.market_wins for s in day_stats),
	)


class _SideSummary(NamedTuple):
	"""Per-taker-side reduction of a band's day aggregates (artifact class (b))."""

	n_trades: int
	wins: int
	mean_price: float
	win_rate: float
	edge: float
	z: float
	p_t: float
	n_clusters: int


def _summarize_side(day_stats: list[_BandDayStats], side: str) -> _SideSummary:
	"""Day-clustered stats over one taker side's prints in a band.

	Days with no prints on the side contribute no cluster (clustered_z_from_stats
	drops n == 0 rows); the side's effective k can therefore be smaller than the
	band's. An empty side returns an all-zero summary with p_t = 1.0.
	"""
	if side == "yes":
		rows = [(s.yes_n, s.yes_wins, s.yes_sum_price) for s in day_stats if s.yes_n > 0]
	else:
		rows = [(s.no_n, s.no_wins, s.no_sum_price) for s in day_stats if s.no_n > 0]
	n = sum(r[0] for r in rows)
	if n == 0:
		return _SideSummary(0, 0, 0.0, 0.0, 0.0, 0.0, 1.0, 0)
	wins = sum(r[1] for r in rows)
	sum_price = sum(r[2] for r in rows)
	z, _p, k = clustered_z_from_stats(rows)
	return _SideSummary(
		n, wins, sum_price / n, wins / n, wins / n - sum_price / n,
		float(z), t_pvalue(z, k - 1), k,
	)


def _taker_side_entry_fields(day_stats: list[_BandDayStats], pooled_edge: float) -> dict:
	"""Bucket-entry fields for the taker-side composition gate (class (b)).

	The exploit side is the side of the book a TAKER must hit to capture the
	pooled edge: a positive edge (band underpriced) is captured by buying YES —
	the taker='yes' prints show what that realizes; a negative edge (overpriced)
	is captured by buying NO — the taker='no' prints. _bucket_bonferroni_verdict
	requires the exploit side to independently clear the BASE alpha with a
	matching sign before a bucket can drive EDGE_EXISTS.
	"""
	yes_s = _summarize_side(day_stats, "yes")
	no_s = _summarize_side(day_stats, "no")
	if pooled_edge > 0:
		exploit_side: Optional[str] = "yes"
	elif pooled_edge < 0:
		exploit_side = "no"
	else:
		exploit_side = None
	exploit = yes_s if exploit_side == "yes" else no_s if exploit_side == "no" else None

	def _block(s: _SideSummary) -> dict:
		return {
			"n_trades": s.n_trades, "n_clusters": s.n_clusters,
			"mean_price": s.mean_price, "win_rate": s.win_rate,
			"edge": s.edge, "z": s.z, "p_t": s.p_t,
		}

	# Prints whose taker_side is neither 'yes' nor 'no' (adapter default ''
	# when the API omits the field; 'unknown' on other venues) count in the
	# pooled totals but in neither side split. Coverage lets the verdict
	# distinguish "no side data captured" from a diagnosed one-sided artifact.
	total_n = sum(s.n_trades for s in day_stats)
	sided_n = yes_s.n_trades + no_s.n_trades

	return {
		"taker_yes": _block(yes_s),
		"taker_no": _block(no_s),
		"taker_side_coverage": sided_n / total_n if total_n else 0.0,
		"exploit_side": exploit_side,
		"exploit_n_trades": exploit.n_trades if exploit else 0,
		"exploit_edge": exploit.edge if exploit else 0.0,
		"exploit_z": exploit.z if exploit else 0.0,
		"exploit_p_t": exploit.p_t if exploit else 1.0,
		"exploit_n_clusters": exploit.n_clusters if exploit else 0,
	}


def _meets_min_n(summary: _BandSummary, min_n: int) -> bool:
	"""Dual observation floor: BOTH n_trades AND n_markets must clear min_n.

	A trades-only floor is vestigial in the high-frequency regime (2 markets ×
	5,000 in-band prints would pass it carrying ~2 independent observations);
	requiring n_markets too preserves the pre-per-trade market-count floor.
	Clamped to at least 1 so a config-generated min_n <= 0 cannot admit an empty
	band into the divide-by-n stats.
	"""
	floor = max(min_n, 1)
	return summary.n_trades >= floor and summary.n_markets >= floor


def _count_decided_markets(cursor: sqlite3.Cursor, series: str) -> int:
	"""Count the series' decided (yes/no) markets — the shared graceful-exit guard
	and the population baseline reported in each test's detail."""
	cursor.execute(
		"SELECT COUNT(*) FROM markets WHERE series_ticker = ? AND result IN ('yes', 'no')",
		(series,),
	)
	row = cursor.fetchone()
	return int(row[0]) if row else 0


def _driver_band(driver: Optional[dict]) -> Optional[tuple[float, float]]:
	"""The (lo, hi) band of the verdict-driving bucket, for the detail dict."""
	if driver is None:
		return None
	return (driver["bucket_lo"], driver["bucket_hi"])


def _bonferroni_z_threshold(z_threshold: float, k: int) -> float:
	"""Bonferroni-correct a z-threshold for K simultaneously evaluated buckets.

	Converts the per-test z-threshold to its two-sided alpha, divides alpha by K,
	and returns the z-threshold for that corrected alpha. Conservative and simple
	(no assumption about bucket independence beyond Bonferroni's worst case).
	Reported in the detail for reference; the VERDICT compares the bucket's
	t(k−1) p-value against _bonferroni_alpha (see _bucket_bonferroni_verdict).
	The alpha formula lives ONLY in _bonferroni_alpha so the reported threshold
	and the deciding alpha cannot drift apart.
	"""
	from scipy.stats import norm

	if k <= 1:
		return z_threshold
	alpha_corr = _bonferroni_alpha(z_threshold, k)
	if alpha_corr <= 0.0:
		return z_threshold
	return float(norm.ppf(1.0 - alpha_corr / 2.0))


def _bonferroni_alpha(z_threshold: float, k: int) -> float:
	"""The Bonferroni-corrected two-sided alpha for K evaluated buckets.

	The configured z-threshold defines the per-test alpha on the normal scale
	(the scale thresholds have always been configured in); the verdict then
	compares each bucket's t(k−1) p-value against alpha/K. An absurdly high
	threshold yields alpha 0.0 and rejects everything — fail-closed.
	"""
	from scipy.stats import norm

	alpha = float(2.0 * (1.0 - norm.cdf(z_threshold)))
	return alpha / k if k > 1 else alpha


def _bucket_bonferroni_verdict(
	bucket_results: list[dict],
	z_threshold: float,
	min_fee_adj: float,
	any_bucket_met_min_n: bool,
	mc_rows_fn: Optional[Callable[[dict], McMarketRows]] = None,
) -> tuple[str, Optional[dict], float, float]:
	"""Un-pooled per-bucket verdict with Bonferroni multiple-testing correction.

	Each entry in `bucket_results` must carry "z" (day-clustered z), "n_clusters"
	(independent days behind it) and "fee_adj" (fee_adjusted_edge_curve). K =
	number of evaluated buckets (those that met min_n).

	Significance is graded on the STUDENT-T reference (artifact class (e)): the
	clustered statistic is a mean/SE over k day excesses, t-distributed with k−1
	df under the null, so each bucket's two-sided t(k−1) p-value must clear the
	Bonferroni alpha (per-test alpha of the configured z-threshold, divided by
	K). The normal reference overstates small-k significance — real refutations
	killed findings that cleared it by hairs and failed under t. Each evaluated
	bucket is annotated in place with "p_t" and "significant" for the detail.

	TAKER-SIDE GATE (artifact class (b)): entries carrying "exploit_side" fields
	(from _taker_side_entry_fields) must show the taker-replicable side clearing
	the BASE alpha (uncorrected — a corroboration check on a lower-powered
	subset, not the primary inference) with an edge whose sign matches the pooled
	edge. A significant bucket failing the gate is bid-ask bounce / adverse
	selection, not capturable mispricing: it is flagged "taker_side_fragile" and
	downgraded to EDGE_NOT_TRADEABLE. Entries without the fields (direct unit
	tests) skip the gate.

	DEGENERATE-OUTCOME GATE (artifact class (c)): entries carrying "market_wins"
	(with "n_markets"/"mean_price") must have market-level expected wins AND
	losses of at least MIN_EXPECTED_MARKET_OUTCOMES — below that the clustered z
	can be pure price dispersion (wins == 0 or == n in every cluster). A bucket
	under the floor survives only if the exact binomial on its market-level
	outcome count independently clears the Bonferroni alpha. Failing buckets are
	NOT honestly significant → they fall through to NO_EDGE, not
	EDGE_NOT_TRADEABLE.

	MC NULL GATE (artifact class (d)): when `mc_rows_fn` is supplied, every
	bucket that survives all other gates is re-tested against the Monte-Carlo
	null (market outcomes redrawn ~ Bernoulli(in-band traded price), day
	clusters kept — mc_null_pvalue). EDGE_EXISTS requires mc_p <= the Bonferroni
	alpha; a failing bucket's nominal significance was rare-event inflation →
	NO_EDGE. Evaluated lazily (qualifiers only) because each run is
	MC_NULL_SIMS simulations over the bucket's per-market rows.

	PER-MARKET SENSITIVITY GATE (artifact class (f)): entries with "market_wins"
	are annotated with "per_market_edge" (one-obs-per-market view) and
	"per_market_sign_flip". A flip — the market-level calibration going the
	OTHER way (or exactly zero) against the per-trade edge — is the print-count
	endogeneity signature: many-print markets dominate the per-trade stat and
	print counts are outcome-endogenous in play, so the per-trade direction is
	not what a market-level position realizes. Sign-flipped buckets cannot drive
	EDGE_EXISTS → EDGE_NOT_TRADEABLE. (Flag-only proved insufficient: a local
	real-data control re-graded a hand-killed series EDGE_EXISTS through a
	sign-flipped bucket that passed every other gate.)

	A bucket *qualifies* iff it is significant AND fee_adj > max(min_fee_adj,
	0.0) AND its taker and degenerate-outcome gates pass. The floor is clamped
	at 0 so EDGE_EXISTS always requires a genuinely net-positive fee-adjusted
	edge even if a config passes a negative `min_fee_adjusted_edge`. Verdict:
	  - EDGE_EXISTS        if ≥1 bucket qualifies;
	  - EDGE_NOT_TRADEABLE if ≥1 significant, non-degenerate bucket is fee-walled
	                       (fee_adj <= 0) or taker-side fragile;
	  - NO_EDGE            if ≥1 bucket met min_n but none qualifies;
	  - INSUFFICIENT_DATA  if no bucket met min_n.

	Does not pool opposite-sign buckets, so a +edge longshot and a −edge favorite
	cannot cancel. Returns (verdict, driver_bucket, z_stat, fee_adjusted_edge),
	where the driver is the qualifying bucket with the largest |fee_adj| (most
	economically meaningful), or the max-|z| bucket if none qualifies.
	"""
	if not any_bucket_met_min_n or not bucket_results:
		return (INSUFFICIENT_DATA, None, 0.0, 0.0)

	alpha_corr = _bonferroni_alpha(z_threshold, len(bucket_results))
	alpha_base = _bonferroni_alpha(z_threshold, 1)

	def _taker_gate_ok(b: dict) -> bool:
		if "exploit_side" not in b:
			return True  # side data not supplied — gate not evaluated
		if b["exploit_side"] is None or b["exploit_n_trades"] == 0:
			return False  # the signal lives entirely on the non-replicable side
		if b["exploit_edge"] * b["edge"] <= 0:
			return False  # the replicable side realizes the OPPOSITE of the claim
		return b["exploit_p_t"] <= alpha_base

	def _degenerate_gate_ok(b: dict) -> bool:
		if "market_wins" not in b:
			return True  # outcome-count data not supplied — gate not evaluated
		# Market-level gates need the PER-MARKET baseline (mean of per-market
		# in-band mean prices). The trade-weighted band mean is wrong whenever
		# print count correlates with price inside the band; it remains only a
		# fallback for direct unit tests that don't supply the batched scan.
		base = b.get("per_market_mean_price", b["mean_price"])
		expected_wins = b["n_markets"] * base
		expected_losses = b["n_markets"] * (1.0 - base)
		b["expected_market_wins"] = expected_wins
		b["expected_market_losses"] = expected_losses
		if (
			expected_wins >= MIN_EXPECTED_MARKET_OUTCOMES
			and expected_losses >= MIN_EXPECTED_MARKET_OUTCOMES
		):
			return True
		b["exact_binom_p"] = exact_binom_pvalue(
			b["market_wins"], b["n_markets"], base
		)
		# bool(): keep detail flags JSON-serializable (never numpy bools).
		return bool(b["exact_binom_p"] <= alpha_corr)

	fee_floor = max(min_fee_adj, 0.0)
	for b in bucket_results:
		b["p_t"] = t_pvalue(b["z"], b["n_clusters"] - 1)
		b["significant"] = b["p_t"] <= alpha_corr
		b["taker_gate_ok"] = _taker_gate_ok(b)
		if "exploit_side" in b:
			# "fragile" asserts a MEASURED one-sided composition; a capture with
			# no side metadata at all is "unavailable" instead (the gate still
			# refuses EDGE_EXISTS — replicability is unverifiable either way).
			no_side_data = b.get("taker_side_coverage", 1.0) == 0.0
			b["taker_side_unavailable"] = bool(no_side_data)
			b["taker_side_fragile"] = bool(
				b["significant"] and not b["taker_gate_ok"] and not no_side_data
			)
		b["degenerate_gate_ok"] = _degenerate_gate_ok(b)
		if "market_wins" in b:
			# Class (f) sensitivity: the one-obs-per-market edge against the
			# PER-MARKET mean price (trade-weighted fallback for unit tests).
			base = b.get("per_market_mean_price", b["mean_price"])
			per_market_edge = b["market_wins"] / b["n_markets"] - base
			b["per_market_edge"] = per_market_edge
			b["per_market_sign_flip"] = bool(
				b["edge"] != 0 and per_market_edge * b["edge"] <= 0
			)

	qualifying = [
		b for b in bucket_results
		if b["significant"] and b["fee_adj"] > fee_floor
		and b["taker_gate_ok"] and b["degenerate_gate_ok"]
		and not b.get("per_market_sign_flip", False)
	]
	significant_not_tradeable = [
		b for b in bucket_results
		if b["significant"] and b["degenerate_gate_ok"]
		and (
			b["fee_adj"] <= 0
			or not b["taker_gate_ok"]
			or b.get("per_market_sign_flip", False)
		)
	]

	if mc_rows_fn is not None:
		# Scale sims so the MC p floor (1/(n_sims+1)) sits an order of magnitude
		# below alpha_corr — a fixed count makes the gate unpassable for tighter
		# alphas (see MC_NULL_SIMS_MAX comment).
		mc_sims = MC_NULL_SIMS
		if alpha_corr > 0:
			mc_sims = min(
				MC_NULL_SIMS_MAX, max(MC_NULL_SIMS, math.ceil(10.0 / alpha_corr))
			)
		survivors = []
		for b in qualifying:
			b["mc_p"] = mc_null_pvalue(mc_rows_fn(b), b["z"], mc_sims)
			b["mc_n_sims"] = mc_sims
			b["mc_gate_ok"] = b["mc_p"] <= alpha_corr
			if b["mc_gate_ok"]:
				survivors.append(b)
		qualifying = survivors

	if qualifying:
		driver = max(qualifying, key=lambda b: abs(b["fee_adj"]))
		verdict = EDGE_EXISTS
	elif significant_not_tradeable:
		driver = max(significant_not_tradeable, key=lambda b: abs(b["z"]))
		verdict = EDGE_NOT_TRADEABLE
	else:
		driver = max(bucket_results, key=lambda b: abs(b["z"]))
		verdict = NO_EDGE

	return (verdict, driver, float(driver["z"]), float(driver["fee_adj"]))


@dataclass
class TestResult:
	"""Result of a statistical hypothesis test."""

	# Tell pytest this is a domain class, not a test class. Without this,
	# pytest tries to collect TestResult (and its `__init__`) wherever it's
	# imported into a test module and emits a PytestCollectionWarning.
	__test__ = False

	verdict: str
	z_stat: float
	fee_adjusted_edge: float
	detail: dict


class StatisticalTest:
	"""Base class for all statistical test types."""
	name: ClassVar[str] = ""

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		raise NotImplementedError


def _ohlc_epoch_seconds(value: object) -> Optional[int]:
	"""Normalize an OHLC timestamp cell to epoch seconds.

	Capture DBs (btc.db / ohlc.db) store INTEGER epoch seconds; legacy fixtures
	store ISO-8601 TEXT (naive means UTC). Numeric strings are epochs. Returns
	None for anything unparseable — the candle is dropped, never guessed at.
	"""
	if isinstance(value, bool):
		return None
	if isinstance(value, (int, float)):
		return int(value)
	if isinstance(value, bytes):
		try:
			value = value.decode()
		except UnicodeDecodeError:
			return None
	if isinstance(value, str):
		try:
			return int(float(value))
		except ValueError:
			pass
		try:
			dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
		except ValueError:
			return None
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=timezone.utc)
		return int(dt.timestamp())
	return None


def _momentum_regime_intervals(
	ohlc_cursor: sqlite3.Cursor,
	ohlc_table: str,
	lookback: int,
) -> tuple[list[tuple[int, int, str]], dict]:
	"""Build the (ts_start, ts_end, regime) lookup intervals from OHLC candles.

	Candle i's regime is the sign of its lookback return (close_i vs
	close_{i-lookback}, an index offset — the original OFFSET semantics),
	thresholded at ±FLAT_MOMENTUM_THRESHOLD. The regime is current from the
	candle's timestamp until the next candle's, capped at REGIME_STALENESS_GAPS
	median inter-candle gaps: past that horizon the label is stale history, not
	current momentum, and trades there stay unclassified. Timestamps normalize
	through _ohlc_epoch_seconds (INTEGER-epoch capture DBs and ISO-TEXT fixtures
	alike); duplicate timestamps keep the last row. The first `lookback` candles
	have no return and open no interval. Returns the intervals (non-overlapping,
	ascending) plus the coverage meta dict reported in the test detail.
	"""
	ohlc_cursor.execute(f'SELECT timestamp, close FROM "{ohlc_table}"')
	candles: list[tuple[int, float]] = []
	for ts_raw, close in ohlc_cursor.fetchall():
		ts = _ohlc_epoch_seconds(ts_raw)
		if ts is None or close is None:
			continue
		candles.append((ts, float(close)))
	candles.sort(key=lambda c: c[0])
	deduped: list[tuple[int, float]] = []
	for ts, close in candles:
		if deduped and deduped[-1][0] == ts:
			deduped[-1] = (ts, close)
		else:
			deduped.append((ts, close))

	coverage: dict = {
		"n_candles": len(deduped),
		"n_classifiable_candles": 0,
		"staleness_cap_seconds": None,
		"coverage_start": None,
		"coverage_end": None,
	}
	if len(deduped) <= lookback:
		return [], coverage

	gaps = [b[0] - a[0] for a, b in zip(deduped, deduped[1:])]
	stale_cap = REGIME_STALENESS_GAPS * int(statistics.median(gaps))
	coverage["staleness_cap_seconds"] = stale_cap

	intervals: list[tuple[int, int, str]] = []
	for i in range(lookback, len(deduped)):
		ts_i, close_i = deduped[i]
		ref_close = deduped[i - lookback][1]
		if ref_close == 0:
			continue
		momentum = (close_i - ref_close) / ref_close
		if momentum > FLAT_MOMENTUM_THRESHOLD:
			regime = "up"
		elif momentum < -FLAT_MOMENTUM_THRESHOLD:
			regime = "down"
		else:
			regime = "flat"
		ts_end = ts_i + stale_cap
		if i + 1 < len(deduped):
			ts_end = min(ts_end, deduped[i + 1][0])
		intervals.append((ts_i, ts_end, regime))

	coverage["n_classifiable_candles"] = len(intervals)
	if intervals:
		coverage["coverage_start"] = intervals[0][0]
		coverage["coverage_end"] = intervals[-1][1]
	return intervals, coverage


class PriceBucketBiasTest(StatisticalTest):
	"""Test whether settlement rates deviate from implied probability across price buckets.

	Generalization of the longshot bias analysis in price_efficiency.py.
	Works with any set of probability buckets, not just longshots.

	Calibration is TRUE PER-TRADE (one observation per trade row in the band,
	day-clustered) — see _per_trade_band_day_stats for why per-market aggregation
	fabricates edges. min_n_per_bucket floors BOTH n_trades and n_markets.
	"""
	name: ClassVar[str] = "price_bucket_bias"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		buckets: list[list[float]] = params.get("buckets", [[0.01, 0.30]])
		min_n: int = params.get("min_n_per_bucket", 30)
		min_clusters: int = thresholds.get("min_clusters", 2)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Confirm the series has decided (yes/no) markets at all (graceful exit).
		n_decided_markets = _count_decided_markets(cursor, series)
		if n_decided_markets == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		bucket_tuples = _normalize_buckets(buckets)

		# 2-4. Per bucket: TRUE per-trade calibration (one observation per trade row
		# in the band), clustered by close_date so effective N = #independent days.
		bucket_results: list[dict] = []
		any_bucket_met_min_n = False
		cluster_floor_skipped: list[dict] = []
		total_n = 0
		total_wins = 0
		total_sum_price = 0.0

		per_band = _per_trade_band_day_stats(cursor, series, _bands_to_cents(bucket_tuples))
		for (lo, hi), day_stats in zip(bucket_tuples, per_band):
			s = _summarize_band(day_stats)
			if not _meets_min_n(s, min_n):
				continue

			z_naive, p_naive = proportions_ztest(s.wins, s.n_trades, s.mean_price)
			# FIX A3: charge the fee on the edge MAGNITUDE. edge < 0 means the bucket
			# is overpriced (a short-side edge); the tradeable edge is |edge| - fee,
			# so a fee applied to the signed (negative) edge would wrongly push a real
			# short-side edge further negative and never grade EDGE_EXISTS. Keep the
			# SIGNED edge in the detail for direction.
			fee_adj = fee_adjusted_edge_curve(abs(s.edge), s.mean_price, fee_model)
			# _naive suffix: the CI (like z_stat_naive) treats every trade as
			# independent; with millions of correlated prints it is far narrower than
			# the day-clustered uncertainty the verdict actually uses.
			ci_lo, ci_hi = wilson_ci(s.wins, s.n_trades)

			bucket_entry = {
				"bucket_lo": lo, "bucket_hi": hi,
				"n_trades": s.n_trades, "n_markets": s.n_markets, "n_clusters": s.n_clusters,
				"market_wins": s.market_wins,
				"mean_price": s.mean_price,
				"win_rate": s.win_rate,
				"edge": s.edge,
				"z": s.z,
				"z_stat_naive": float(z_naive),
				"p": s.p,
				"p_value_naive": float(p_naive),
				"fee_adj": fee_adj,
				"ci_lower_naive": ci_lo, "ci_upper_naive": ci_hi,
				**_taker_side_entry_fields(day_stats, s.edge),
			}

			# FIX A1 min-cluster floor: a bucket is eligible for the verdict only if
			# it clears min_clusters independent days. Buckets below the floor are NOT
			# added to the evaluated results (they can't drive a verdict) but are noted
			# in the detail. Understating the cluster count is exactly what fabricates
			# edges under intraday correlation, so a thin-day bucket must not score.
			if s.n_clusters < min_clusters:
				cluster_floor_skipped.append(bucket_entry)
				continue

			any_bucket_met_min_n = True
			bucket_results.append(bucket_entry)
			total_n += s.n_trades
			total_wins += s.wins
			total_sum_price += s.sum_price

		mc_rows_fn = _attach_per_market_stats(cursor, series, bucket_results)
		verdict, driver, z_stat, fee_adj_result = _bucket_bonferroni_verdict(
			bucket_results, z_threshold, min_fee_adj, any_bucket_met_min_n,
			mc_rows_fn=mc_rows_fn,
		)

		if verdict == INSUFFICIENT_DATA:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={
					"reason": "no_bucket_met_min_n", "buckets": bucket_results,
					"cluster_floor_skipped": cluster_floor_skipped,
				},
			)

		# Aggregate descriptors (back-compat detail keys); the VERDICT is per-bucket.
		total_implied = total_sum_price / total_n

		return TestResult(
			verdict=verdict,
			z_stat=z_stat,
			fee_adjusted_edge=fee_adj_result,
			detail={
				"calibration": "per_trade_day_clustered",
				"n": total_n,
				"n_decided_markets": n_decided_markets,
				"overall_implied": total_implied,
				"overall_win_rate": total_wins / total_n,
				"overall_edge": total_wins / total_n - total_implied,
				"z_threshold_bonferroni": _bonferroni_z_threshold(z_threshold, len(bucket_results)),
				"alpha_bonferroni": _bonferroni_alpha(z_threshold, len(bucket_results)),
				"driver_bucket": driver,
				"driver_bucket_band": _driver_band(driver),
				"per_market_sign_flip": bool(driver.get("per_market_sign_flip", False)) if driver else False,
				"buckets": bucket_results,
				"cluster_floor_skipped": cluster_floor_skipped,
			},
		)


class LifecycleBiasTest(StatisticalTest):
	"""Test for tradeable mispricing in a market's EARLY window (time since open).

	Verdict contract: grades ABSOLUTE early-window per-trade calibration per bucket
	(the economically tradeable quantity — you can only earn the early gap). It does
	NOT by itself establish that the mispricing is lifecycle-SPECIFIC: a static bias
	grades EDGE_EXISTS here too (and on price_bucket_bias — a duplicate signal,
	deduped downstream). Check the per-bucket `differential_z` in the detail — a
	day-clustered paired contrast of early vs late excess over days present in BOTH
	segments — for lifecycle attribution.

	Calibration is TRUE PER-TRADE (one observation per trade row in the band's
	segment, day-clustered) — see _per_trade_band_day_stats. A segment with no trades
	contributes no observations (no last_price fallback — never a synthetic one).
	min_n_per_bucket floors n_trades AND n_markets in BOTH segments.
	"""
	name: ClassVar[str] = "lifecycle_bias"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		lifecycle_window_minutes: int = params.get("lifecycle_window_minutes", 30)
		buckets: list[list[float]] = params.get("buckets", [[0.40, 0.60]])
		min_n: int = params.get("min_n_per_bucket", 30)
		min_clusters: int = thresholds.get("min_clusters", 2)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Confirm the series has decided (yes/no) markets at all (graceful exit).
		n_decided_markets = _count_decided_markets(cursor, series)
		if n_decided_markets == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		bucket_tuples = _normalize_buckets(buckets)
		bands_c = _bands_to_cents(bucket_tuples)

		bucket_results: list[dict] = []
		any_bucket_met_min_n = False
		cluster_floor_skipped: list[dict] = []
		total_early_n = 0
		total_early_wins = 0
		total_early_sum_price = 0.0

		# One scan per segment across ALL bands (not per bucket).
		early_bands = _per_trade_band_day_stats(
			cursor, series, bands_c,
			lifecycle_segment=("early", lifecycle_window_minutes),
		)
		late_bands = _per_trade_band_day_stats(
			cursor, series, bands_c,
			lifecycle_segment=("late", lifecycle_window_minutes),
		)

		for i, (lo, hi) in enumerate(bucket_tuples):
			early_stats = early_bands[i]
			e = _summarize_band(early_stats)
			late = _summarize_band(late_bands[i])

			# Dual floor on BOTH segments (the late segment is required context for
			# the differential; a bucket without it cannot claim a lifecycle read).
			if not (_meets_min_n(e, min_n) and _meets_min_n(late, min_n)):
				continue

			# Paired differential over days present in BOTH segments; days in only
			# one segment are DROPPED (treating them as zero would bias the contrast).
			late_by_day = {s.day: s for s in late_bands[i]}
			diffs: list[float] = []
			for s in early_stats:
				late_day = late_by_day.get(s.day)
				if late_day is None:
					continue
				early_excess = s.wins / s.n_trades - s.sum_price / s.n_trades
				late_excess = (
					late_day.wins / late_day.n_trades
					- late_day.sum_price / late_day.n_trades
				)
				diffs.append(early_excess - late_excess)
			d_z, d_p, d_k = z_over_excesses(diffs)

			# FIX A3 parity with PriceBucketBias: fee on the early edge MAGNITUDE.
			fee_adj = fee_adjusted_edge_curve(abs(e.edge), e.mean_price, fee_model)

			bucket_entry = {
				"bucket_lo": lo, "bucket_hi": hi,
				# Verdict keys (consumed by _bucket_bonferroni_verdict):
				"z": e.z,
				"fee_adj": fee_adj,
				"n_clusters": e.n_clusters,
				"edge": e.edge,
				"n_markets": e.n_markets,
				"mean_price": e.mean_price,
				"market_wins": e.market_wins,
				# Early segment (drives the verdict):
				"early_n_trades": e.n_trades, "early_n_markets": e.n_markets,
				"early_implied": e.mean_price, "early_win_rate": e.win_rate,
				"early_edge": e.edge,
				"early_z_stat": e.z, "early_p_value": e.p,
				# Late segment (context):
				"late_n_trades": late.n_trades, "late_n_markets": late.n_markets,
				"late_n_clusters": late.n_clusters,
				"late_implied": late.mean_price, "late_win_rate": late.win_rate,
				"late_edge": late.edge,
				"late_z_stat": late.z, "late_p_value": late.p,
				# Lifecycle attribution (context, not the verdict driver):
				"edge_differential": e.edge - late.edge,
				"differential_z": float(d_z), "differential_p": float(d_p),
				"differential_n_clusters": d_k,
				# Taker-side gate on the verdict-driving (early) segment.
				**_taker_side_entry_fields(early_stats, e.edge),
			}

			# Min-cluster floor on the verdict-driving (early) segment.
			if e.n_clusters < min_clusters:
				cluster_floor_skipped.append(bucket_entry)
				continue

			any_bucket_met_min_n = True
			bucket_results.append(bucket_entry)
			total_early_n += e.n_trades
			total_early_wins += e.wins
			total_early_sum_price += e.sum_price

		mc_rows_fn = _attach_per_market_stats(
			cursor, series, bucket_results,
			lifecycle_segment=("early", lifecycle_window_minutes),
		)
		verdict, driver, z_stat, fee_adj_result = _bucket_bonferroni_verdict(
			bucket_results, z_threshold, min_fee_adj, any_bucket_met_min_n,
			mc_rows_fn=mc_rows_fn,
		)

		if verdict == INSUFFICIENT_DATA:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={
					"reason": "no_bucket_met_min_n", "buckets": bucket_results,
					"cluster_floor_skipped": cluster_floor_skipped,
				},
			)

		total_implied = total_early_sum_price / total_early_n

		return TestResult(
			verdict=verdict,
			z_stat=z_stat,
			fee_adjusted_edge=fee_adj_result,
			detail={
				"calibration": "per_trade_day_clustered",
				"n_early": total_early_n,
				"n_decided_markets": n_decided_markets,
				"overall_early_implied": total_implied,
				"overall_early_win_rate": total_early_wins / total_early_n,
				"overall_early_edge": total_early_wins / total_early_n - total_implied,
				"lifecycle_window_minutes": lifecycle_window_minutes,
				"z_threshold_bonferroni": _bonferroni_z_threshold(z_threshold, len(bucket_results)),
				"alpha_bonferroni": _bonferroni_alpha(z_threshold, len(bucket_results)),
				"driver_bucket": driver,
				"driver_bucket_band": _driver_band(driver),
				"per_market_sign_flip": bool(driver.get("per_market_sign_flip", False)) if driver else False,
				"buckets": bucket_results,
				"cluster_floor_skipped": cluster_floor_skipped,
			},
		)


class VolumeMispricingTest(StatisticalTest):
	"""Test whether prints placed in THIN-SO-FAR markets are wider mispriced.

	Splits prints into terciles of CAUSAL cumulative volume — the contracts that
	had traded in the market BEFORE each print — and checks whether the
	thin-so-far tercile shows significantly more mispricing than the deeper
	ones. Tercile membership from final settled m.volume is artifact class (a):
	final volume is outcome-endogenous on in-play venues (winners and losers
	accumulate different flow BY settlement), so conditioning on it selects on
	outcome — proven by real adversarial refutations where rebuilding the
	terciles causally collapsed verified kills' z-scores to noise. Capture is
	complete (per-market Σt.count matches m.volume), so the running per-print
	sum is exact, not a proxy.

	Cost note: the tercile bounds (count + two ORDER BY/OFFSET quantiles) and
	the three tercile scans each evaluate the window subquery — six windowed
	scans per run. Correctness over speed; the scans are series-filtered.
	"""
	name: ClassVar[str] = "volume_mispricing"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		buckets: list[list[float]] = params.get("buckets", [[0.40, 0.60]])
		min_n: int = params.get("min_n_per_bucket", 30)
		min_clusters: int = thresholds.get("min_clusters", 2)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Confirm the series has decided (yes/no) markets at all (graceful exit),
		#    then compute the CAUSAL cumulative-volume tercile bounds over every
		#    valid print of those markets (same population the scans filter).
		n_decided_markets = _count_decided_markets(cursor, series)
		if n_decided_markets == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)
		cursor.execute(
			f"SELECT COUNT(*) FROM ({_CAUSAL_CUM_VOLUME_SUBQUERY})", (series,)
		)
		n_total = int(cursor.fetchone()[0])
		if n_total == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_valid_prints", "n": 0},
			)

		def _cum_quantile(offset: int) -> float:
			cursor.execute(
				f"SELECT cum_before FROM ({_CAUSAL_CUM_VOLUME_SUBQUERY}) "
				"ORDER BY cum_before LIMIT 1 OFFSET ?",
				(series, offset),
			)
			return float(cursor.fetchone()[0])

		t1 = _cum_quantile(n_total // 3)      # upper bound of thin-so-far tercile
		t2 = _cum_quantile(2 * n_total // 3)  # upper bound of medium tercile

		bucket_tuples = _normalize_buckets(buckets)
		bands_c = _bands_to_cents(bucket_tuples)

		# 2-3. One scan per tercile across ALL bands. Tercile membership is a
		# range predicate on the print's causal cum_before (low <= t1 < med <= t2
		# < high) — NOT a ticker IN(...) list, which would exceed SQLite's
		# bound-variable limit on long-history series and crash.
		low_bands = _per_trade_band_day_stats(
			cursor, series, bands_c, cum_volume_range=(None, t1))
		med_bands = _per_trade_band_day_stats(
			cursor, series, bands_c, cum_volume_range=(t1, t2))
		high_bands = _per_trade_band_day_stats(
			cursor, series, bands_c, cum_volume_range=(t2, None))

		def _tercile_detail(summary: _BandSummary) -> dict:
			if summary.n_trades == 0:
				return {"n_trades": 0, "n_markets": 0}
			return {
				"n_trades": summary.n_trades, "n_markets": summary.n_markets,
				"n_clusters": summary.n_clusters,
				"mean_price": summary.mean_price,
				"win_rate": summary.win_rate,
				"edge": summary.edge,
				"z_stat_clustered": summary.z,
				"p_value_clustered": summary.p,
			}

		bucket_results: list[dict] = []
		any_bucket_met_min_n = False
		cluster_floor_skipped: list[dict] = []
		total_n = 0
		total_wins = 0
		total_sum_price = 0.0

		for i, (lo, hi) in enumerate(bucket_tuples):
			lv = _summarize_band(low_bands[i])
			hi_summary = _summarize_band(high_bands[i])

			# Dual floor on the verdict-driving LOW tercile (see PriceBucketBiasTest).
			if not _meets_min_n(lv, min_n):
				continue

			lv_z_naive, lv_p_naive = proportions_ztest(lv.wins, lv.n_trades, lv.mean_price)
			# _naive suffix: independence-assuming CI over correlated prints (see
			# PriceBucketBiasTest).
			lv_ci_lo, lv_ci_hi = wilson_ci(lv.wins, lv.n_trades)
			# FIX A3: charge the fee on the edge MAGNITUDE (see PriceBucketBiasTest).
			lv_fee_adj = fee_adjusted_edge_curve(abs(lv.edge), lv.mean_price, fee_model)

			bucket_entry = {
				"bucket_lo": lo, "bucket_hi": hi,
				# Verdict keys (consumed by _bucket_bonferroni_verdict):
				"z": lv.z,
				"fee_adj": lv_fee_adj,
				"n_trades": lv.n_trades,
				"n_markets": lv.n_markets,
				"n_clusters": lv.n_clusters,
				"market_wins": lv.market_wins,
				"mean_price": lv.mean_price,
				"win_rate": lv.win_rate,
				"edge": lv.edge,
				"p": lv.p,
				"ci_lower_naive": lv_ci_lo,
				"ci_upper_naive": lv_ci_hi,
				# Detail block:
				"low_volume": {
					"n_trades": lv.n_trades, "n_markets": lv.n_markets,
					"n_clusters": lv.n_clusters,
					"mean_price": lv.mean_price,
					"win_rate": lv.win_rate,
					"edge": lv.edge,
					"z_stat_naive": float(lv_z_naive),
					"z_stat_clustered": lv.z,
					"p_value_naive": float(lv_p_naive),
					"p_value_clustered": lv.p,
					"fee_adjusted_edge": lv_fee_adj,
					"ci_lower_naive": lv_ci_lo,
					"ci_upper_naive": lv_ci_hi,
				},
				"medium_volume": _tercile_detail(_summarize_band(med_bands[i])),
				"high_volume": _tercile_detail(hi_summary),
				"edge_differential_low_vs_high": (
					lv.edge - hi_summary.edge if hi_summary.n_trades else None
				),
				# Taker-side gate on the verdict-driving (low) tercile.
				**_taker_side_entry_fields(low_bands[i], lv.edge),
			}

			# FIX A1 min-cluster floor: only buckets clearing min_clusters independent
			# days are eligible for the verdict (see PriceBucketBiasTest).
			if lv.n_clusters < min_clusters:
				cluster_floor_skipped.append(bucket_entry)
				continue

			any_bucket_met_min_n = True
			bucket_results.append(bucket_entry)
			total_n += lv.n_trades
			total_wins += lv.wins
			total_sum_price += lv.sum_price

		mc_rows_fn = _attach_per_market_stats(
			cursor, series, bucket_results, cum_volume_range=(None, t1),
		)
		verdict, driver, z_stat, fee_adj_result = _bucket_bonferroni_verdict(
			bucket_results, z_threshold, min_fee_adj, any_bucket_met_min_n,
			mc_rows_fn=mc_rows_fn,
		)

		if verdict == INSUFFICIENT_DATA:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={
					"reason": "no_bucket_met_min_n", "buckets": bucket_results,
					"volume_basis": "at_trade_cumulative",
					"tercile_bounds": (t1, t2),
					"cluster_floor_skipped": cluster_floor_skipped,
				},
			)

		# Aggregate descriptors (back-compat detail keys); the VERDICT is per-bucket.
		total_implied = total_sum_price / total_n

		return TestResult(
			verdict=verdict,
			z_stat=z_stat,
			fee_adjusted_edge=fee_adj_result,
			detail={
				"calibration": "per_trade_day_clustered",
				"n_low_volume": total_n,
				"n_decided_markets": n_decided_markets,
				"overall_implied": total_implied,
				"overall_win_rate": total_wins / total_n,
				"overall_edge": total_wins / total_n - total_implied,
				"z_threshold_bonferroni": _bonferroni_z_threshold(z_threshold, len(bucket_results)),
				"alpha_bonferroni": _bonferroni_alpha(z_threshold, len(bucket_results)),
				"driver_bucket": driver,
				"driver_bucket_band": _driver_band(driver),
				"per_market_sign_flip": bool(driver.get("per_market_sign_flip", False)) if driver else False,
				"volume_basis": "at_trade_cumulative",
				"tercile_bounds": (t1, t2),
				"buckets": bucket_results,
				"cluster_floor_skipped": cluster_floor_skipped,
			},
		)


class MomentumAlignmentTest(StatisticalTest):
	"""Test whether contract prices lag external spot price movements.

	Each TRADE is classified into a momentum regime (up/down/flat) from the
	OHLC candle state at its own timestamp, and every (regime × price band)
	cell is graded with the same TRUE per-trade day-clustered calibration and
	hardened gate stack — (b) taker-side composition, (c) degenerate outcomes,
	(d) MC null, (e) t-reference, (f) per-market sensitivity — as
	PriceBucketBiasTest, in ONE Bonferroni family across all evaluated cells.
	"flat" trades are counted in the detail but never graded (no directional
	hypothesis); the driving cell's regime rides in detail["driver_regime"].

	Ported 2026-07-10 from the per-market lifetime-VWAP method, which had two
	proven defects: (1) one observation per MARKET — the edge-fabricating
	aggregation class _per_trade_band_day_stats documents; and (2) an SQLite
	type-affinity look-ahead — binding the TEXT ISO trade time against the
	INTEGER epoch `timestamp` column of the real capture DBs matched every row
	(an INTEGER sorts before any TEXT), so every market classified by the LAST
	candle in the DB, and trades entirely outside OHLC coverage inherited that
	frozen regime label. Post-port, trades outside coverage or past the
	staleness horizon (REGIME_STALENESS_GAPS) are excluded and reported in
	detail["regime_trade_counts"]["unclassified"].

	Requires params["ohlc_config"] = {"db_path": str, "table": str, "asset": str}.
	Falls back to INSUFFICIENT_DATA gracefully if OHLC data is unavailable.
	"""
	name: ClassVar[str] = "momentum_alignment"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		ohlc_config: Optional[dict] = params.get("ohlc_config")
		# Momentum over zero candles is meaningless; clamp so a degenerate
		# config cannot define every candle as "flat".
		lookback: int = max(1, params.get("lookback_candles", 5))
		buckets: list[list[float]] = params.get("buckets", [[0.30, 0.70]])
		min_n: int = params.get("min_n_per_bucket", 30)
		min_clusters: int = thresholds.get("min_clusters", 2)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		# 1. Validate ohlc_config and open OHLC connection
		if not ohlc_config:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_ohlc_config"},
			)

		ohlc_db_path: str = ohlc_config.get("db_path", "")
		ohlc_table: str = ohlc_config.get("table", "")

		import os
		# #2: data_source_config.ohlc_for_series hands back a BARE db_file
		# ("kalshi-altcrypto.db") but the actual file lives under data/. If the path
		# doesn't exist as given, fall back to data/<path> (the convention in
		# data_source_resolver.py). Keep this local to the test (smallest change).
		if not os.path.exists(ohlc_db_path):
			data_dir_candidate = os.path.join("data", ohlc_db_path)
			if os.path.exists(data_dir_candidate):
				ohlc_db_path = data_dir_candidate
			else:
				# Still missing — stay graceful, reporting the RESOLVED candidate path.
				return TestResult(
					verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
					detail={"reason": "ohlc_db_not_found", "db_path": data_dir_candidate},
				)

		try:
			ohlc_conn = sqlite3.connect(ohlc_db_path)
			ohlc_cursor = ohlc_conn.cursor()
			# Check the table exists
			ohlc_cursor.execute(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				(ohlc_table,),
			)
			if ohlc_cursor.fetchone() is None:
				ohlc_conn.close()
				return TestResult(
					verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
					detail={"reason": "ohlc_table_not_found", "table": ohlc_table},
				)
		except Exception as exc:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "ohlc_open_error", "error": str(exc)},
			)

		try:
			return self._run_with_ohlc(
				conn=conn,
				ohlc_conn=ohlc_conn,
				ohlc_table=ohlc_table,
				series=series,
				lookback=lookback,
				buckets=buckets,
				min_n=min_n,
				min_clusters=min_clusters,
				fee_model=fee_model,
				z_threshold=z_threshold,
				min_fee_adj=min_fee_adj,
			)
		finally:
			ohlc_conn.close()

	def _run_with_ohlc(
		self,
		conn: sqlite3.Connection,
		ohlc_conn: sqlite3.Connection,
		ohlc_table: str,
		series: str,
		lookback: int,
		buckets: list[list[float]],
		min_n: int,
		min_clusters: int,
		fee_model: FeeModel,
		z_threshold: float,
		min_fee_adj: float,
	) -> TestResult:
		cursor = conn.cursor()

		n_decided_markets = _count_decided_markets(cursor, series)
		if n_decided_markets == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		bucket_tuples = _normalize_buckets(buckets)

		intervals, coverage = _momentum_regime_intervals(
			ohlc_conn.cursor(), ohlc_table, lookback
		)
		if not intervals:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={
					"reason": "no_classifiable_candles",
					"momentum_coverage": coverage,
					"lookback_candles": lookback,
				},
			)

		# The regime lookup table _MOMENTUM_REGIME_SUBQUERY joins against. TEMP
		# schema: private to this connection, never touches the data file;
		# dropped in finally so a same-connection rerun starts clean.
		cursor.execute("DROP TABLE IF EXISTS temp._momentum_regime")
		cursor.execute(
			"CREATE TEMP TABLE _momentum_regime ("
			"ts_start INTEGER NOT NULL, ts_end INTEGER NOT NULL, regime TEXT NOT NULL)"
		)
		try:
			cursor.executemany(
				"INSERT INTO _momentum_regime VALUES (?, ?, ?)", intervals
			)
			cursor.execute(
				"CREATE INDEX temp.idx_momentum_regime_ts "
				"ON _momentum_regime (ts_start, ts_end)"
			)

			# Whole-population regime census (one scan): up/down/flat plus the
			# prints no interval covers — the honesty stat that exposes a run
			# whose trades mostly fall outside OHLC coverage.
			regime_trade_counts = {"up": 0, "down": 0, "flat": 0, "unclassified": 0}
			cursor.execute(
				f"SELECT t.regime, COUNT(*) FROM ({_MOMENTUM_REGIME_SUBQUERY}) t "
				"GROUP BY t.regime",
				[series],
			)
			for regime, n in cursor.fetchall():
				regime_trade_counts["unclassified" if regime is None else regime] = n

			bands_c = _bands_to_cents(bucket_tuples)
			bucket_results: list[dict] = []
			cluster_floor_skipped: list[dict] = []
			any_bucket_met_min_n = False
			total_n = 0
			total_wins = 0
			total_sum_price = 0.0
			mc_fns: dict[str, Callable[[dict], McMarketRows]] = {}

			# "flat" is never graded (no directional hypothesis). Both graded
			# regimes' cells enter ONE Bonferroni family below.
			for regime in ("up", "down"):
				per_band = _per_trade_band_day_stats(
					cursor, series, bands_c, momentum_regime=regime
				)
				regime_entries: list[dict] = []
				for (lo, hi), day_stats in zip(bucket_tuples, per_band):
					s = _summarize_band(day_stats)
					if not _meets_min_n(s, min_n):
						continue

					z_naive, p_naive = proportions_ztest(s.wins, s.n_trades, s.mean_price)
					# Fee on the edge MAGNITUDE (same rationale as PriceBucketBiasTest
					# FIX A3: a short-side edge must not be double-charged).
					fee_adj = fee_adjusted_edge_curve(abs(s.edge), s.mean_price, fee_model)
					ci_lo, ci_hi = wilson_ci(s.wins, s.n_trades)

					bucket_entry = {
						"regime": regime,
						"bucket_lo": lo, "bucket_hi": hi,
						"n_trades": s.n_trades, "n_markets": s.n_markets,
						"n_clusters": s.n_clusters,
						"market_wins": s.market_wins,
						"mean_price": s.mean_price,
						"win_rate": s.win_rate,
						"edge": s.edge,
						"z": s.z,
						"z_stat_naive": float(z_naive),
						"p": s.p,
						"p_value_naive": float(p_naive),
						"fee_adj": fee_adj,
						"ci_lower_naive": ci_lo, "ci_upper_naive": ci_hi,
						**_taker_side_entry_fields(day_stats, s.edge),
					}

					# Same min-cluster floor as PriceBucketBiasTest (FIX A1): a
					# thin-day cell must not score.
					if s.n_clusters < min_clusters:
						cluster_floor_skipped.append(bucket_entry)
						continue

					any_bucket_met_min_n = True
					regime_entries.append(bucket_entry)
					total_n += s.n_trades
					total_wins += s.wins
					total_sum_price += s.sum_price

				if regime_entries:
					# One batched per-market scan per regime; the composed fn
					# below dispatches each entry to its own regime's scan so the
					# gate/MC population always matches the graded population.
					mc_fns[regime] = _attach_per_market_stats(
						cursor, series, regime_entries, momentum_regime=regime
					)
				bucket_results.extend(regime_entries)

			verdict, driver, z_stat, fee_adj_result = _bucket_bonferroni_verdict(
				bucket_results, z_threshold, min_fee_adj, any_bucket_met_min_n,
				mc_rows_fn=(lambda b: mc_fns[b["regime"]](b)) if mc_fns else None,
			)

			if verdict == INSUFFICIENT_DATA:
				return TestResult(
					verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
					detail={
						"reason": "no_bucket_met_min_n",
						"regime_trade_counts": regime_trade_counts,
						"momentum_coverage": coverage,
						"lookback_candles": lookback,
						"buckets": bucket_results,
						"cluster_floor_skipped": cluster_floor_skipped,
					},
				)

			# Aggregate descriptors (back-compat detail keys); the VERDICT is
			# per-cell.
			total_implied = total_sum_price / total_n

			return TestResult(
				verdict=verdict,
				z_stat=z_stat,
				fee_adjusted_edge=fee_adj_result,
				detail={
					"calibration": "per_trade_day_clustered",
					"n": total_n,
					"n_decided_markets": n_decided_markets,
					"overall_implied": total_implied,
					"overall_win_rate": total_wins / total_n,
					"overall_edge": total_wins / total_n - total_implied,
					"lookback_candles": lookback,
					"regime_trade_counts": regime_trade_counts,
					"momentum_coverage": coverage,
					"z_threshold_bonferroni": _bonferroni_z_threshold(z_threshold, len(bucket_results)),
					"alpha_bonferroni": _bonferroni_alpha(z_threshold, len(bucket_results)),
					"driver_bucket": driver,
					"driver_bucket_band": _driver_band(driver),
					"driver_regime": driver.get("regime") if driver else None,
					"per_market_sign_flip": bool(driver.get("per_market_sign_flip", False)) if driver else False,
					"buckets": bucket_results,
					"cluster_floor_skipped": cluster_floor_skipped,
				},
			)
		finally:
			cursor.execute("DROP TABLE IF EXISTS temp._momentum_regime")


class TestRunner:
	"""Dispatches hypothesis configs to the appropriate StatisticalTest."""

	# Tell pytest this is a domain class, not a test class.
	__test__ = False

	def __init__(self) -> None:
		self.test_types: dict[str, StatisticalTest] = {}
		self._register_defaults()

	def _register_defaults(self) -> None:
		"""Register built-in test types. Called during __init__."""
		self.register("price_bucket_bias", PriceBucketBiasTest())
		self.register("lifecycle_bias", LifecycleBiasTest())
		self.register("volume_mispricing", VolumeMispricingTest())
		self.register("momentum_alignment", MomentumAlignmentTest())

	def register(self, name: str, test: StatisticalTest) -> None:
		self.test_types[name] = test

	def run(
		self,
		test_type: str,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		if test_type not in self.test_types:
			raise ValueError(f"Unknown test type: {test_type}. Available: {list(self.test_types.keys())}")
		return self.test_types[test_type].run(conn, series, params, thresholds)
