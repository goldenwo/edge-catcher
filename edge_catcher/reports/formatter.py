"""Format hypothesis results as human-readable tables."""

import json
from pathlib import Path
from typing import List

from edge_catcher.storage.models import BucketResult, HypothesisResult


def format_bucket_table(bucket_results: List[BucketResult]) -> str:
    """Render bucket results as a fixed-width table.

    Columns: Bucket, N, N_Clustered, Implied, Actual, Edge,
             z-stat, z-stat(C), p-val, Fee(M), Net(M), CI
    """
    if not bucket_results:
        return "No bucket results to display."

    col_fmt = (
        "{bucket:12s} {n:>6s} {nc:>10s} {imp:>8s} {act:>8s} {edge:>8s} "
        "{z:>8s} {zc:>10s} {pv:>8s} {fee:>8s} {net:>8s} {ci}"
    )
    header = col_fmt.format(
        bucket="Bucket",
        n="N",
        nc="N_Clustered",
        imp="Implied",
        act="Actual",
        edge="Edge",
        z="z-stat",
        zc="z-stat(C)",
        pv="p-val",
        fee="Fee(M)",
        net="Net(M)",
        ci="CI",
    )
    separator = "-" * len(header)

    rows = [header, separator]
    for r in bucket_results:
        bucket_label = f"{r.bucket_lo:.2f}-{r.bucket_hi:.2f}"
        fee_maker = 0.0175 * r.implied_prob * (1 - r.implied_prob)
        net_maker = r.edge - fee_maker
        ci_str = f"[{r.ci_lower:.3f},{r.ci_upper:.3f}]"
        rows.append(
            col_fmt.format(
                bucket=bucket_label,
                n=str(r.n),
                nc=str(r.n_clustered),
                imp=f"{r.implied_prob:.3f}",
                act=f"{r.actual_win_rate:.3f}",
                edge=f"{r.edge:+.3f}",
                z=f"{r.z_stat:.2f}",
                zc=f"{r.z_stat_clustered:.2f}",
                pv=f"{r.p_value:.4f}",
                fee=f"{fee_maker:.4f}",
                net=f"{net_maker:+.4f}",
                ci=ci_str,
            )
        )

    return "\n".join(rows)


def format_hypothesis_result(result: HypothesisResult) -> str:
    """Format a HypothesisResult as a complete readable report."""
    ts = (
        result.run_timestamp.isoformat()
        if hasattr(result.run_timestamp, "isoformat")
        else str(result.run_timestamp)
    )
    lines = [
        f"\n{'=' * 60}",
        f"  {result.hypothesis_id}",
        f"{'=' * 60}",
        f"Run ID   : {result.run_id}",
        f"Timestamp: {ts}",
        f"Verdict  : {result.verdict}",
        f"Markets  : {result.total_markets_seen} seen, "
        f"{result.delisted_or_cancelled} delisted/cancelled",
        "",
    ]

    if result.raw_bucket_data:
        try:
            buckets_data = json.loads(result.raw_bucket_data)
            bucket_results = [_dict_to_bucket(b) for b in buckets_data]
            lines.append(format_bucket_table(bucket_results))
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            lines.append(f"[Could not render bucket table: {e}]")

    if result.warnings:
        lines.append("\nWarnings:")
        for w in result.warnings:
            lines.append(f"  • {w}")

    return "\n".join(lines)


def format_json_file(json_path) -> str:
    """Read a JSON results file and format all hypotheses it contains."""
    with open(json_path) as f:
        data = json.load(f)

    sections = []
    for hyp_id, result_data in data.items():
        if "error" in result_data:
            sections.append(f"\n[{hyp_id}] ERROR: {result_data['error']}")
            continue

        lines = [
            f"\n{'=' * 60}",
            f"  {hyp_id}",
            f"{'=' * 60}",
            f"Verdict  : {result_data.get('verdict', 'N/A')}",
            f"Markets  : {result_data.get('total_markets_seen', 0)} seen, "
            f"{result_data.get('delisted_or_cancelled', 0)} delisted",
            "",
        ]

        raw = result_data.get("raw_bucket_data")
        if raw:
            try:
                buckets_data = json.loads(raw)
                bucket_results = [_dict_to_bucket(b) for b in buckets_data]
                lines.append(format_bucket_table(bucket_results))
            except (json.JSONDecodeError, KeyError, ValueError):
                pass

        warnings = result_data.get("warnings", [])
        if isinstance(warnings, str):
            try:
                warnings = json.loads(warnings)
            except json.JSONDecodeError:
                warnings = []
        if warnings:
            lines.append("\nWarnings:")
            for w in warnings:
                lines.append(f"  • {w}")

        sections.append("\n".join(lines))

    return "\n".join(sections)


def _dict_to_bucket(b: dict) -> BucketResult:
    """Convert a raw_bucket_data dict entry to a BucketResult."""
    bucket_str = b["bucket"]  # e.g. "0.50-0.70"
    parts = bucket_str.split("-")
    return BucketResult(
        bucket_lo=float(parts[0]),
        bucket_hi=float(parts[1]),
        n=b["n"],
        n_clustered=b["n_clustered"],
        implied_prob=b["implied_prob"],
        actual_win_rate=b["actual_win_rate"],
        edge=b["edge"],
        z_stat=b["z_stat"],
        z_stat_clustered=b["z_stat_clustered"],
        p_value=b["p_value"],
        p_value_clustered=b["p_value_clustered"],
        fee_adjusted_edge=b["fee_adjusted_edge"],
        ci_lower=b["ci_lower"],
        ci_upper=b["ci_upper"],
    )
