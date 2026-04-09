"""
Tutorial hypothesis: Longshot bias in Kalshi prediction markets.

This module is a fully-implemented, heavily-commented reference for building
your own hypotheses. It tests the "longshot bias" — a well-documented behavioral
pattern where bettors systematically overestimate the probability of unlikely
events, leading to overpriced low-probability contracts.

WHAT IS LONGSHOT BIAS?
In horse racing, sports betting, and prediction markets, longshots (contracts
priced near 0) tend to be systematically overpriced. Bettors over-bet unlikely
outcomes because the potential payoff is exciting. This creates negative expected
value for longshot buyers — and potentially positive expected value for sellers.

WHAT THIS HYPOTHESIS TESTS:
Contracts priced between 1–30 cents. If efficient, a contract priced at X cents
should settle YES exactly X% of the time. Longshot bias predicts actual win rates
will be LOWER than implied — confirming buyers overpay for unlikely outcomes.

EXPECTED RESULT: EDGE_NOT_TRADEABLE or NO_EDGE
Longshot bias is real and well-documented, but Kalshi's fees make it difficult
to profit from even if the bias exists. This is a good example of why raw edge
≠ tradeable edge.

HOW TO USE THIS AS A STARTING POINT:
1. Copy this file to edge_catcher/hypotheses/local/my_hypothesis.py
2. Change HYPOTHESIS_ID to a unique string
3. Register it in config.local/hypotheses.yaml
4. Modify the query and bucket range to fit your idea
5. Run: python -m edge_catcher analyze --hypothesis my_hypothesis

KEY STATISTICAL CONCEPTS USED HERE:
- VWAP (volume-weighted average price): better price signal than last_price
- Proportions z-test: tests whether actual win rate differs from implied
- Clustered standard errors: contracts expiring same day are correlated
- Harvey-Liu-Zhu threshold: use z > 3.0 to avoid false positives
- Fee adjustment: raw edge minus Kalshi's percent-of-profit fee
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from statsmodels.stats.proportion import proportions_ztest

from edge_catcher.research.stats_utils import (
	clustered_z, wilson_ci, fee_adjusted_edge,
)
from edge_catcher.storage.models import BucketResult, HypothesisResult

# ── Identity ──────────────────────────────────────────────────────────────────

# This string must match the key in your hypotheses.yaml config entry.
HYPOTHESIS_ID = "longshot_bias_tutorial"

# ── Verdict constants ─────────────────────────────────────────────────────────

INSUFFICIENT_DATA  = "INSUFFICIENT_DATA"
NO_EDGE            = "NO_EDGE"
INCONCLUSIVE       = "INCONCLUSIVE"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"
EDGE_EXISTS        = "EDGE_EXISTS"

# ── Probability buckets ───────────────────────────────────────────────────────

# Longshot bias focuses on the low end of the probability range.
# We test contracts priced between 1–30 cents — the "longshot" zone.
# Buckets are narrow at the extreme low end where bias tends to be strongest.
BUCKETS = [
    (0.01, 0.03),   # deepest longshots (1–3 cents)
    (0.03, 0.05),   # 3–5 cents
    (0.05, 0.08),   # 5–8 cents
    (0.08, 0.12),   # 8–12 cents
    (0.12, 0.18),   # 12–18 cents
    (0.18, 0.25),   # 18–25 cents
    (0.25, 0.30),   # 25–30 cents
]

# ── Helper functions ──────────────────────────────────────────────────────────

def _load_configs(config_path: Path) -> tuple[dict, dict]:
    """Load hypothesis config and fee model from YAML files."""
    hyp_path = config_path / "hypotheses.yaml"
    with open(hyp_path) as f:
        hyp_config = yaml.safe_load(f)["hypotheses"][HYPOTHESIS_ID]

    fee_model_key = hyp_config.get("fee_model", "kalshi")
    fees_path = config_path / "fees.yaml"
    with open(fees_path) as f:
        fee_config = yaml.safe_load(f)[fee_model_key]

    return hyp_config, fee_config


def _compute_vwap(cursor, ticker: str, last_price: Optional[float]) -> Optional[float]:
    """Return volume-weighted average price (0–1 scale) for a market.

    VWAP is a better price signal than last_price because it reflects the
    actual average price paid across all trades, not just the final tick.
    Falls back to last_price when no trades exist.
    Returns None when no price signal is available — those markets are skipped.
    """
    cursor.execute(
        "SELECT SUM(CAST(yes_price AS REAL) * count) / SUM(count) FROM trades WHERE ticker = ?",
        (ticker,),
    )
    row = cursor.fetchone()
    vwap_cents = row[0] if row else None

    if vwap_cents is not None:
        return vwap_cents / 100.0          # convert cents → probability

    if last_price is not None and last_price > 0:
        return last_price / 100.0

    return None


def _bucket_for(prob: float) -> Optional[tuple]:
    """Assign a probability to its bucket, or None if outside the longshot range."""
    for lo, hi in BUCKETS:
        if lo <= prob < hi:
            return (lo, hi)
    return None



# ── Main run function ─────────────────────────────────────────────────────────

def run(db_conn, config_path: Path = Path("config")) -> HypothesisResult:
    """Test for longshot bias in Kalshi prediction markets (1–30 cent contracts).

    ALGORITHM:
    1. Query all settled markets
    2. Compute VWAP per market (fallback: last_price)
    3. Keep only contracts in the 1–30 cent (longshot) range
    4. Per bucket: run naive z-test and clustered z-test
    5. Apply fee adjustment — fees are especially punishing for longshots
    6. Verdict: if actual win rates are significantly BELOW implied → bias confirmed

    INTERPRETATION:
    - Negative z-stat = actual win rate below implied = longshots overpriced (bias confirmed)
    - EDGE_NOT_TRADEABLE = bias exists but fees eat the profit from selling
    - EDGE_EXISTS = could profit by selling (shorting) overpriced longshots
    """
    # ── Load config ────────────────────────────────────────────────────────────
    local_config = config_path.parent / "config.local"
    if local_config.exists() and (local_config / "hypotheses.yaml").exists():
        try:
            hyp_config, fee_config = _load_configs(local_config)
        except KeyError:
            hyp_config, fee_config = _load_configs(config_path)
    else:
        hyp_config, fee_config = _load_configs(config_path)

    thresholds   = hyp_config.get("thresholds", {})
    min_n        = thresholds.get("min_n_per_bucket", 30)
    min_clusters = thresholds.get("min_independent_obs", 80)
    z_threshold  = thresholds.get("t_stat", 3.0)
    maker_fee    = fee_config.get("maker", 0.0175)

    cursor = db_conn.cursor()

    # ── Fetch settled markets ──────────────────────────────────────────────────
    cursor.execute("""
        SELECT ticker, result, last_price, close_time
        FROM markets
        WHERE result IN ('yes', 'no')
    """)
    markets = cursor.fetchall()
    total_markets = len(markets)

    if total_markets == 0:
        return HypothesisResult(
            hypothesis_id=HYPOTHESIS_ID, run_id=str(uuid.uuid4()),
            run_timestamp=datetime.now(timezone.utc), market="kalshi",
            status=hyp_config.get("status", "exploratory"),
            naive_n=0, naive_z_stat=0.0, naive_p_value=1.0, naive_edge=0.0,
            clustered_n=0, clustered_z_stat=0.0, clustered_p_value=1.0,
            clustered_edge=0.0, fee_adjusted_edge=0.0,
            confidence_interval_low=0.0, confidence_interval_high=0.0,
            verdict=INSUFFICIENT_DATA,
            warnings=["No settled markets found — run the download first"],
            total_markets_seen=0,
        )

    # ── Bucket markets (longshot range only) ───────────────────────────────────
    # Markets outside 1–30 cents are ignored — we're only testing longshots here.
    # This is intentional: if you tested the full range, high-probability
    # contracts would dominate and obscure the longshot signal.
    bucket_data: dict[tuple, list] = {b: [] for b in BUCKETS}
    warnings: list[str] = []
    longshot_count = 0

    for ticker, result, last_price, close_time in markets:
        implied = _compute_vwap(cursor, ticker, last_price)
        if implied is None:
            continue
        bucket = _bucket_for(implied)
        if bucket is None:
            continue   # outside 1–30 cent range, skip
        longshot_count += 1
        won = (result == "yes")
        close_date = close_time[:10] if close_time else None
        bucket_data[bucket].append((implied, won, close_date))

    warnings.append(
        f"{longshot_count:,} longshot contracts analyzed out of {total_markets:,} total"
    )

    # ── Per-bucket statistics ──────────────────────────────────────────────────
    bucket_results: list[BucketResult] = []
    total_wins = total_n = 0
    total_implied_sum = 0.0
    total_clusters = 0

    for (lo, hi), rows in bucket_data.items():
        n = len(rows)
        if n < min_n:
            if n > 0:
                warnings.append(f"Bucket [{lo:.2f},{hi:.2f}): {n} obs < min {min_n}, skipped")
            continue

        wins = sum(1 for _, won, _ in rows if won)
        implied_vals = [imp for imp, _, _ in rows]
        mean_implied = sum(implied_vals) / len(implied_vals)
        actual_win_rate = wins / n
        edge = actual_win_rate - mean_implied

        z_naive, p_naive = proportions_ztest(wins, n, mean_implied)
        z_clust, p_clust, n_clust = clustered_z(rows)
        fee_adj = fee_adjusted_edge(edge, mean_implied, maker_fee)
        ci_lo, ci_hi = wilson_ci(wins, n)

        bucket_results.append(BucketResult(
            bucket_lo=lo, bucket_hi=hi, n=n, n_clustered=n_clust,
            implied_prob=mean_implied, actual_win_rate=actual_win_rate,
            edge=edge, z_stat=float(z_naive), z_stat_clustered=float(z_clust),
            p_value=float(p_naive), p_value_clustered=float(p_clust),
            fee_adjusted_edge=fee_adj, ci_lower=ci_lo, ci_upper=ci_hi,
        ))

        total_wins += wins
        total_n += n
        total_implied_sum += mean_implied * n
        total_clusters += n_clust

    if not bucket_results:
        return HypothesisResult(
            hypothesis_id=HYPOTHESIS_ID, run_id=str(uuid.uuid4()),
            run_timestamp=datetime.now(timezone.utc), market="kalshi",
            status=hyp_config.get("status", "exploratory"),
            naive_n=0, naive_z_stat=0.0, naive_p_value=1.0, naive_edge=0.0,
            clustered_n=0, clustered_z_stat=0.0, clustered_p_value=1.0,
            clustered_edge=0.0, fee_adjusted_edge=0.0,
            confidence_interval_low=0.0, confidence_interval_high=0.0,
            verdict=INSUFFICIENT_DATA,
            warnings=warnings + ["No buckets met minimum sample size"],
            total_markets_seen=total_markets,
        )

    # ── Aggregate overall statistics ───────────────────────────────────────────
    overall_implied = total_implied_sum / total_n
    overall_win_rate = total_wins / total_n
    overall_edge = overall_win_rate - overall_implied
    overall_z_naive, overall_p_naive = proportions_ztest(total_wins, total_n, overall_implied)

    all_rows = [row for b in bucket_results for row in bucket_data[(b.bucket_lo, b.bucket_hi)]]
    overall_z_clust, overall_p_clust, _ = clustered_z(all_rows)
    overall_fee_adj = fee_adjusted_edge(overall_edge, overall_implied, maker_fee)
    ci_lo, ci_hi = wilson_ci(total_wins, total_n)

    # ── Verdict ────────────────────────────────────────────────────────────────
    if total_clusters < min_clusters:
        verdict = INSUFFICIENT_DATA
        warnings.append(f"Only {total_clusters} clusters (< min {min_clusters})")
    elif abs(overall_z_clust) < z_threshold:
        verdict = NO_EDGE
    elif overall_fee_adj <= 0:
        verdict = EDGE_NOT_TRADEABLE
    else:
        verdict = EDGE_EXISTS

    return HypothesisResult(
        hypothesis_id=HYPOTHESIS_ID,
        run_id=str(uuid.uuid4()),
        run_timestamp=datetime.now(timezone.utc),
        market="kalshi",
        status=hyp_config.get("status", "exploratory"),
        naive_n=total_n,
        naive_z_stat=float(overall_z_naive),
        naive_p_value=float(overall_p_naive),
        naive_edge=overall_edge,
        clustered_n=total_clusters,
        clustered_z_stat=float(overall_z_clust),
        clustered_p_value=float(overall_p_clust),
        clustered_edge=overall_edge,
        fee_adjusted_edge=overall_fee_adj,
        confidence_interval_low=ci_lo,
        confidence_interval_high=ci_hi,
        verdict=verdict,
        warnings=warnings,
        total_markets_seen=total_markets,
        raw_bucket_data=json.dumps([
            {
                "bucket_lo": b.bucket_lo, "bucket_hi": b.bucket_hi,
                "n": b.n, "n_clustered": b.n_clustered,
                "implied_prob": b.implied_prob,
                "actual_win_rate": b.actual_win_rate,
                "edge": b.edge,
                "z_stat": b.z_stat, "z_stat_clustered": b.z_stat_clustered,
                "p_value": b.p_value, "p_value_clustered": b.p_value_clustered,
                "fee_adjusted_edge": b.fee_adjusted_edge,
                "ci_lower": b.ci_lower, "ci_upper": b.ci_upper,
            }
            for b in bucket_results
        ]),
    )
