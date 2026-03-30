from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class Market:
    ticker: str
    event_ticker: str
    series_ticker: str
    title: str
    status: str              # "open", "closed", "settled"
    result: Optional[str]    # "yes", "no", None
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    last_price: Optional[float]
    open_interest: Optional[int]
    volume: Optional[int]
    expiration_time: Optional[datetime]
    close_time: Optional[datetime]
    created_time: Optional[datetime]
    settled_time: Optional[datetime]
    open_time: Optional[datetime]
    notional_value: Optional[float]
    floor_strike: Optional[float]
    cap_strike: Optional[float]
    raw_data: Optional[str] = None  # JSON blob of original API response


@dataclass
class Trade:
    trade_id: str
    ticker: str
    yes_price: int              # cents (0-100)
    no_price: int               # cents (0-100)
    count: int                  # contracts traded
    taker_side: str             # "yes" or "no"
    created_time: datetime
    raw_data: Optional[str] = None


@dataclass
class HypothesisResult:
    hypothesis_id: str
    run_id: str                          # UUID
    run_timestamp: datetime
    market: str
    status: str                          # "exploratory" | "confirmatory"

    # Naive results (contracts treated as independent)
    naive_n: int
    naive_z_stat: float
    naive_p_value: float
    naive_edge: float

    # Clustered results (clustered by expiration date) — primary for decisions
    clustered_n: int                     # number of independent observation clusters
    clustered_z_stat: float
    clustered_p_value: float
    clustered_edge: float

    fee_adjusted_edge: float
    confidence_interval_low: float
    confidence_interval_high: float

    verdict: str                         # INSUFFICIENT_DATA | NO_EDGE | INCONCLUSIVE | EDGE_EXISTS | EDGE_NOT_TRADEABLE
    warnings: list[str] = field(default_factory=list)

    # Survivorship
    total_markets_seen: int = 0
    delisted_or_cancelled: int = 0

    raw_bucket_data: Optional[str] = None  # JSON with per-bucket stats


@dataclass
class BucketResult:
    bucket_lo: float
    bucket_hi: float
    n: int                      # naive count (all contracts in bucket)
    n_clustered: int            # clustered count (unique expiration dates)
    implied_prob: float         # mean yes_price / 100 in bucket
    actual_win_rate: float      # fraction that resolved YES
    edge: float                 # actual_win_rate - implied_prob
    z_stat: float               # naive proportions_ztest z-statistic
    z_stat_clustered: float     # clustered z-statistic
    p_value: float              # naive p-value (two-sided)
    p_value_clustered: float    # clustered p-value (two-sided)
    fee_adjusted_edge: float    # edge minus maker fee cost
    ci_lower: float             # binomial CI lower (fraction of n)
    ci_upper: float             # binomial CI upper (fraction of n)
