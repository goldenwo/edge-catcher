"""Hypothesis Formalizer: natural language → YAML config + stub module."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import yaml

from .client import LLMClient

_PROMPTS_DIR = Path(__file__).parent / "prompts"


# ── prompt builders ───────────────────────────────────────────────────────────

def _load_system_prompt() -> str:
    return (_PROMPTS_DIR / "formalizer_system.txt").read_text()


def _build_user_prompt(description: str) -> str:
    return f"Formalize this market hypothesis:\n\n{description}"


# ── response parsing ──────────────────────────────────────────────────────────

def _parse_llm_response(response: str) -> tuple[str, dict]:
    """
    Extract hypothesis_id and config dict from an LLM response.

    Expects a ```yaml ... ``` fenced block whose top-level structure is either::

        hypotheses:
          <hypothesis_id>:
            ...

    or (bare dict)::

        <hypothesis_id>:
          ...

    Returns: (hypothesis_id, hyp_config_dict)
    Raises: ValueError if no YAML block is found or the structure is unexpected.
    """
    yaml_match = re.search(r"```yaml\n(.*?)```", response, re.DOTALL)
    if not yaml_match:
        raise ValueError(f"No YAML block found in LLM response:\n{response}")

    raw_yaml = yaml_match.group(1)
    parsed = yaml.safe_load(raw_yaml)

    # Normalize: accept both {"hypotheses": {"id": {...}}} and {"id": {...}}
    if isinstance(parsed, dict) and "hypotheses" in parsed:
        entries = parsed["hypotheses"]
    else:
        entries = parsed

    if not isinstance(entries, dict) or len(entries) == 0:
        raise ValueError(f"Expected a non-empty hypothesis dict, got: {parsed!r}")

    hypothesis_id = next(iter(entries))
    hyp_config = entries[hypothesis_id]
    return hypothesis_id, hyp_config


# ── stub generation ───────────────────────────────────────────────────────────

def _stub_content(hypothesis_id: str, name: str) -> str:
    return f'''"""
{name} - auto-generated hypothesis module.

Queries settled markets, buckets by implied probability, runs proportions
z-tests with clustered standard errors, and applies fee adjustment.
Buckets and thresholds are read from the YAML config at runtime.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from statsmodels.stats.proportion import proportions_ztest

from edge_catcher.research.stats_utils import clustered_z, wilson_ci, fee_adjusted_edge
from edge_catcher.storage.models import BucketResult, HypothesisResult

HYPOTHESIS_ID = "{hypothesis_id}"

INSUFFICIENT_DATA  = "INSUFFICIENT_DATA"
NO_EDGE            = "NO_EDGE"
INCONCLUSIVE       = "INCONCLUSIVE"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"
EDGE_EXISTS        = "EDGE_EXISTS"


def _load_configs(config_path: Path):
\t"""Load hypothesis config and fee model, checking config.local first."""
\tfrom edge_catcher.hypotheses.registry import _load_hypothesis_configs
\tall_configs = _load_hypothesis_configs(config_path)
\thyp_config = all_configs[HYPOTHESIS_ID]

\tfee_model_key = hyp_config.get("fee_model", "kalshi")
\tfor cfg_dir in [config_path.parent / (config_path.name + ".local"), config_path]:
\t\tfees_path = cfg_dir / "fees.yaml"
\t\tif fees_path.exists():
\t\t\twith open(fees_path) as f:
\t\t\t\tfee_config = yaml.safe_load(f).get(fee_model_key, {{}})
\t\t\tif fee_config:
\t\t\t\treturn hyp_config, fee_config
\treturn hyp_config, {{}}


def _compute_vwap(cursor, ticker, last_price):
\t"""Volume-weighted average price (0-1 scale), falling back to last_price."""
\tcursor.execute(
\t\t"SELECT SUM(CAST(yes_price AS REAL) * count) / SUM(count) FROM trades WHERE ticker = ?",
\t\t(ticker,),
\t)
\trow = cursor.fetchone()
\tvwap_cents = row[0] if row else None
\tif vwap_cents is not None:
\t\treturn vwap_cents / 100.0
\tif last_price is not None and last_price > 0:
\t\treturn last_price / 100.0
\treturn None


def run(db_conn, config_path: Path = Path("config")) -> HypothesisResult:
\t"""Run the {name} hypothesis against settled markets."""
\thyp_config, fee_config = _load_configs(config_path)
\tthresholds = hyp_config.get("thresholds", {{}})
\tmin_n = thresholds.get("min_n_per_bucket", 30)
\tmin_clusters = thresholds.get("min_independent_obs", 80)
\tz_threshold = thresholds.get("t_stat", 3.0)
\tmaker_fee = fee_config.get("maker", 0.0175)

\tbuckets = [tuple(b) for b in hyp_config.get("buckets", [(0.01, 0.99)])]

\tcursor = db_conn.cursor()
\tcursor.execute("""
\t\tSELECT ticker, result, last_price, close_time
\t\tFROM markets WHERE result IN ('yes', 'no')
\t""")
\tmarkets = cursor.fetchall()
\ttotal_markets = len(markets)

\tif total_markets == 0:
\t\treturn HypothesisResult(
\t\t\thypothesis_id=HYPOTHESIS_ID, run_id=str(uuid.uuid4()),
\t\t\trun_timestamp=datetime.now(timezone.utc), market="kalshi",
\t\t\tstatus=hyp_config.get("status", "exploratory"),
\t\t\tnaive_n=0, naive_z_stat=0.0, naive_p_value=1.0, naive_edge=0.0,
\t\t\tclustered_n=0, clustered_z_stat=0.0, clustered_p_value=1.0,
\t\t\tclustered_edge=0.0, fee_adjusted_edge=0.0,
\t\t\tconfidence_interval_low=0.0, confidence_interval_high=0.0,
\t\t\tverdict=INSUFFICIENT_DATA,
\t\t\twarnings=["No settled markets found"],
\t\t\ttotal_markets_seen=0,
\t\t)

\tbucket_data: dict[tuple, list] = {{b: [] for b in buckets}}
\twarnings: list[str] = []

\tfor ticker, result, last_price, close_time in markets:
\t\timplied = _compute_vwap(cursor, ticker, last_price)
\t\tif implied is None:
\t\t\tcontinue
\t\tfor lo, hi in buckets:
\t\t\tif lo <= implied < hi:
\t\t\t\tbucket_data[(lo, hi)].append((
\t\t\t\t\timplied, result == "yes", close_time[:10] if close_time else None
\t\t\t\t))
\t\t\t\tbreak

\tbucket_results: list[BucketResult] = []
\ttotal_wins = total_n = 0
\ttotal_implied_sum = 0.0
\ttotal_clusters = 0

\tfor (lo, hi), rows in bucket_data.items():
\t\tn = len(rows)
\t\tif n < min_n:
\t\t\tif n > 0:
\t\t\t\twarnings.append(f"Bucket [{{lo:.2f}},{{hi:.2f}}): {{n}} obs < min {{min_n}}, skipped")
\t\t\tcontinue
\t\twins = sum(1 for _, won, _ in rows if won)
\t\timplied_vals = [imp for imp, _, _ in rows]
\t\tmean_implied = sum(implied_vals) / len(implied_vals)
\t\tactual_win_rate = wins / n
\t\tedge = actual_win_rate - mean_implied

\t\tz_naive, p_naive = proportions_ztest(wins, n, mean_implied)
\t\tz_clust, p_clust, n_clust = clustered_z(rows)
\t\tfee_adj = fee_adjusted_edge(edge, mean_implied, maker_fee)
\t\tci_lo, ci_hi = wilson_ci(wins, n)

\t\tbucket_results.append(BucketResult(
\t\t\tbucket_lo=lo, bucket_hi=hi, n=n, n_clustered=n_clust,
\t\t\timplied_prob=mean_implied, actual_win_rate=actual_win_rate,
\t\t\tedge=edge, z_stat=float(z_naive), z_stat_clustered=float(z_clust),
\t\t\tp_value=float(p_naive), p_value_clustered=float(p_clust),
\t\t\tfee_adjusted_edge=fee_adj, ci_lower=ci_lo, ci_upper=ci_hi,
\t\t))
\t\ttotal_wins += wins
\t\ttotal_n += n
\t\ttotal_implied_sum += mean_implied * n
\t\ttotal_clusters += n_clust

\tif not bucket_results:
\t\treturn HypothesisResult(
\t\t\thypothesis_id=HYPOTHESIS_ID, run_id=str(uuid.uuid4()),
\t\t\trun_timestamp=datetime.now(timezone.utc), market="kalshi",
\t\t\tstatus=hyp_config.get("status", "exploratory"),
\t\t\tnaive_n=0, naive_z_stat=0.0, naive_p_value=1.0, naive_edge=0.0,
\t\t\tclustered_n=0, clustered_z_stat=0.0, clustered_p_value=1.0,
\t\t\tclustered_edge=0.0, fee_adjusted_edge=0.0,
\t\t\tconfidence_interval_low=0.0, confidence_interval_high=0.0,
\t\t\tverdict=INSUFFICIENT_DATA,
\t\t\twarnings=warnings + ["No buckets met minimum sample size"],
\t\t\ttotal_markets_seen=total_markets,
\t\t)

\toverall_implied = total_implied_sum / total_n
\toverall_win_rate = total_wins / total_n
\toverall_edge = overall_win_rate - overall_implied
\toverall_z_naive, overall_p_naive = proportions_ztest(total_wins, total_n, overall_implied)

\tall_rows = [row for b in bucket_results for row in bucket_data[(b.bucket_lo, b.bucket_hi)]]
\toverall_z_clust, overall_p_clust, _ = clustered_z(all_rows)
\toverall_fee_adj = fee_adjusted_edge(overall_edge, overall_implied, maker_fee)
\tci_lo, ci_hi = wilson_ci(total_wins, total_n)

\tif total_clusters < min_clusters:
\t\tverdict = INSUFFICIENT_DATA
\t\twarnings.append(f"Only {{total_clusters}} clusters (< min {{min_clusters}})")
\telif abs(overall_z_clust) < z_threshold:
\t\tverdict = NO_EDGE
\telif overall_fee_adj <= 0:
\t\tverdict = EDGE_NOT_TRADEABLE
\telse:
\t\tverdict = EDGE_EXISTS

\treturn HypothesisResult(
\t\thypothesis_id=HYPOTHESIS_ID, run_id=str(uuid.uuid4()),
\t\trun_timestamp=datetime.now(timezone.utc), market="kalshi",
\t\tstatus=hyp_config.get("status", "exploratory"),
\t\tnaive_n=total_n, naive_z_stat=float(overall_z_naive),
\t\tnaive_p_value=float(overall_p_naive), naive_edge=overall_edge,
\t\tclustered_n=total_clusters, clustered_z_stat=float(overall_z_clust),
\t\tclustered_p_value=float(overall_p_clust), clustered_edge=overall_edge,
\t\tfee_adjusted_edge=overall_fee_adj,
\t\tconfidence_interval_low=ci_lo, confidence_interval_high=ci_hi,
\t\tverdict=verdict, warnings=warnings, total_markets_seen=total_markets,
\t\traw_bucket_data=json.dumps([
\t\t\t{{
\t\t\t\t"bucket_lo": b.bucket_lo, "bucket_hi": b.bucket_hi,
\t\t\t\t"n": b.n, "n_clustered": b.n_clustered,
\t\t\t\t"implied_prob": b.implied_prob,
\t\t\t\t"actual_win_rate": b.actual_win_rate,
\t\t\t\t"edge": b.edge,
\t\t\t\t"z_stat": b.z_stat, "z_stat_clustered": b.z_stat_clustered,
\t\t\t\t"p_value": b.p_value, "p_value_clustered": b.p_value_clustered,
\t\t\t\t"fee_adjusted_edge": b.fee_adjusted_edge,
\t\t\t\t"ci_lower": b.ci_lower, "ci_upper": b.ci_upper,
\t\t\t}}
\t\t\tfor b in bucket_results
\t\t]),
\t)
'''


# ── path helpers ──────────────────────────────────────────────────────────────

def _module_str_to_file(module_str: str) -> Path:
    """``edge_catcher.hypotheses.custom.foo`` → ``edge_catcher/hypotheses/custom/foo.py``"""
    return Path(module_str.replace(".", "/") + ".py")


def _module_str_to_relative_file(module_str: str, hypotheses_base: Path) -> Path:
    """Map a module string to a path relative to *hypotheses_base*."""
    parts = module_str.split(".")
    try:
        idx = parts.index("hypotheses")
        sub_parts = parts[idx + 1:]
    except ValueError:
        sub_parts = parts[-2:]  # fallback: last two segments
    return hypotheses_base / Path("/".join(sub_parts) + ".py")


# ── public API ────────────────────────────────────────────────────────────────

def formalize(
    description: str,
    client: LLMClient,
    config_path: Optional[Path] = None,
    hypotheses_base: Optional[Path] = None,
) -> dict:
    """
    Convert a plain-English hypothesis description into config + stub module.

    Args:
        description: Natural-language hypothesis.
        client: LLMClient instance (caller is responsible for authentication).
        config_path: Path to ``hypotheses.yaml`` (default: ``config.local/hypotheses.yaml``).
        hypotheses_base: Override base directory for stub file creation.
            When ``None`` the module string is mapped to a project-root path.
            Pass ``tmp_path / "hypotheses"`` in tests to keep them hermetic.

    Returns:
        On success: ``{"hypothesis_id", "config_path", "module_path", "message"}``.
        On parse failure: ``{"error": True, "raw_response": str}``.
    """
    config_path = config_path or Path("config.local/hypotheses.yaml")

    system_prompt = _load_system_prompt()
    user_prompt = _build_user_prompt(description)
    response = client.complete(system_prompt, user_prompt, task="formalizer")

    try:
        hypothesis_id, hyp_config = _parse_llm_response(response)
    except ValueError:
        print("Could not parse LLM response. Raw output:\n")
        print(response)
        print("\nPlease manually create the config entry and module.")
        return {"error": True, "raw_response": response}

    # ── append to hypotheses.yaml ─────────────────────────────────────────────
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if config_path.exists():
        with open(config_path) as f:
            existing = yaml.safe_load(f) or {}
    existing.setdefault("hypotheses", {})[hypothesis_id] = hyp_config
    with open(config_path, "w") as f:
        yaml.dump(existing, f, default_flow_style=False, allow_unicode=True)

    # ── resolve stub file path ────────────────────────────────────────────────
    module_str = hyp_config.get(
        "module", f"edge_catcher.hypotheses.local.{hypothesis_id}"
    )
    if hypotheses_base:
        module_file = _module_str_to_relative_file(module_str, hypotheses_base)
    else:
        module_file = _module_str_to_file(module_str)

    # ── create parent dirs + __init__.py sentinels ────────────────────────────
    module_file.parent.mkdir(parents=True, exist_ok=True)
    for parent in reversed(list(module_file.parents)):
        init_file = parent / "__init__.py"
        if parent.exists() and not init_file.exists():
            try:
                init_file.touch()
            except OSError:
                pass

    # ── write stub ────────────────────────────────────────────────────────────
    name = hyp_config.get("name", hypothesis_id)
    module_file.write_text(_stub_content(hypothesis_id, name))

    message = (
        f"Created hypothesis '{hypothesis_id}'.\n"
        f"  Config: {config_path}\n"
        f"  Module: {module_file}\n\n"
        f"Edit the module, then run:\n"
        f"  python -m edge_catcher analyze --hypothesis {hypothesis_id}"
    )
    return {
        "hypothesis_id": hypothesis_id,
        "config_path": config_path,
        "module_path": module_file,
        "message": message,
    }
