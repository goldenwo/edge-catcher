"""
Example hypothesis module — copy this as a starting point for your own research.

This demonstrates the standard structure for an edge-catcher hypothesis.
Implement your own statistical test in the `run()` function.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from edge_catcher.storage.models import HypothesisResult

# Unique identifier — must match the key in config/hypotheses.yaml
HYPOTHESIS_ID = "example_hypothesis"

# Verdict constants
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
NO_EDGE = "NO_EDGE"
INCONCLUSIVE = "INCONCLUSIVE"
EDGE_EXISTS = "EDGE_EXISTS"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"


def _load_config(config_path: Path):
    hyp_path = config_path / "hypotheses.yaml"
    with open(hyp_path) as f:
        hyp_config = yaml.safe_load(f)["hypotheses"][HYPOTHESIS_ID]
    return hyp_config


def run(
    db_conn,
    config_path: Path = Path("config"),
) -> HypothesisResult:
    """
    Run your hypothesis against settled markets in the database.

    Steps:
    1. Query settled markets from db_conn
    2. Compute a price signal per market (VWAP from trades, or last_price fallback)
    3. Bucket contracts by implied probability
    4. Run proportions_ztest per bucket
    5. Cluster standard errors by expiration date
    6. Return HypothesisResult with verdict + per-bucket stats

    Args:
        db_conn: sqlite3 connection to the local market database
        config_path: path to the config/ directory

    Returns:
        HypothesisResult with per-bucket statistics and an overall verdict
    """
    hyp_config = _load_config(config_path)
    thresholds = hyp_config.get("thresholds", {})
    min_n = thresholds.get("min_n_per_bucket", 30)

    # Query settled markets
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM markets
        WHERE result IN ('yes', 'no') AND last_price > 0 AND volume > 0
    """)
    total = cursor.fetchone()[0]

    if total < min_n:
        return HypothesisResult(
            hypothesis_id=HYPOTHESIS_ID,
            run_id=str(uuid.uuid4()),
            run_timestamp=datetime.now(timezone.utc),
            market="kalshi",
            status=hyp_config.get("status", "exploratory"),
            naive_n=total,
            naive_z_stat=0.0,
            naive_p_value=1.0,
            naive_edge=0.0,
            clustered_n=0,
            clustered_z_stat=0.0,
            clustered_p_value=1.0,
            clustered_edge=0.0,
            fee_adjusted_edge=0.0,
            confidence_interval_low=0.0,
            confidence_interval_high=0.0,
            verdict=INSUFFICIENT_DATA,
            warnings=["Not enough data — implement your test logic here"],
            total_markets_seen=total,
        )

    # TODO: implement your statistical test here.
    # See edge_catcher/hypotheses/kalshi/ for reference patterns:
    #   - Bucket markets by implied probability (yes_price / 100)
    #   - For each bucket: run proportions_ztest(wins, n, implied_mid)
    #   - Cluster by expiration date for independent observations
    #   - Apply fee model from config/fees.yaml
    #   - Return EDGE_EXISTS if clustered z-stat > threshold and fee_adjusted_edge > 0

    return HypothesisResult(
        hypothesis_id=HYPOTHESIS_ID,
        run_id=str(uuid.uuid4()),
        run_timestamp=datetime.now(timezone.utc),
        market="kalshi",
        status=hyp_config.get("status", "exploratory"),
        naive_n=total,
        naive_z_stat=0.0,
        naive_p_value=1.0,
        naive_edge=0.0,
        clustered_n=0,
        clustered_z_stat=0.0,
        clustered_p_value=1.0,
        clustered_edge=0.0,
        fee_adjusted_edge=0.0,
        confidence_interval_low=0.0,
        confidence_interval_high=0.0,
        verdict=INCONCLUSIVE,
        warnings=["Example hypothesis — implement your test logic here"],
        total_markets_seen=total,
    )
