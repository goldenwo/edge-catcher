"""Backtest runner: load hypothesis from registry, run against DB, save results."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from edge_catcher.hypotheses.registry import discover, run_hypothesis
from edge_catcher.storage.db import get_db, init_db, save_analysis_result

logger = logging.getLogger(__name__)


def _result_to_dict(result) -> dict:
    """Serialize a HypothesisResult (or error dict) to a JSON-safe dict."""
    if isinstance(result, dict):
        return result
    d = {}
    for k, v in result.__dict__.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        else:
            d[k] = v
    return d


def run_backtest(
    hypothesis_id: Optional[str] = None,
    db_path: Path = Path("data/kalshi.db"),
    config_path: Path = Path("config"),
    output_path: Path = Path("reports/latest_analysis.json"),
) -> dict:
    """Run hypothesis analysis, persist to DB, write JSON output.

    Args:
        hypothesis_id: run a single hypothesis by ID, or None to run all
        db_path: path to SQLite database (must exist — run 'download' first)
        config_path: directory containing *.yaml config files
        output_path: where to write the JSON results file

    Returns:
        dict of {hypothesis_id: serialized_result}

    Raises:
        FileNotFoundError: if db_path does not exist
    """
    db_path = Path(db_path)
    config_path = Path(config_path)
    output_path = Path(output_path)

    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. Run 'download' first."
        )

    results = {}
    with get_db(db_path) as conn:
        if hypothesis_id:
            result = run_hypothesis(hypothesis_id, conn, config_path)
            results[hypothesis_id] = result
        else:
            modules = discover(config_path)
            for hyp_id in modules:
                try:
                    results[hyp_id] = run_hypothesis(hyp_id, conn, config_path)
                except Exception as e:
                    logger.error("Hypothesis %s failed: %s", hyp_id, e)
                    results[hyp_id] = {"error": str(e)}

        # Persist to DB (only results with a run_id)
        for hyp_id, result in results.items():
            if hasattr(result, "run_id"):
                save_analysis_result(conn, result)
                logger.info("Saved result for %s (run_id=%s)", hyp_id, result.run_id)

    # Serialize and write JSON
    output_data = {hyp_id: _result_to_dict(r) for hyp_id, r in results.items()}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2, default=str)

    logger.info("Analysis written to %s", output_path)
    return output_data


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run hypothesis backtests")
    parser.add_argument("--hypothesis", default=None, help="Hypothesis ID to run")
    parser.add_argument("--db-path", default="data/kalshi.db")
    parser.add_argument(
        "--output", default="reports/latest_analysis.json"
    )
    args = parser.parse_args()

    run_backtest(
        hypothesis_id=args.hypothesis,
        db_path=Path(args.db_path),
        output_path=Path(args.output),
    )
    print(f"Analysis saved to {args.output}")


if __name__ == "__main__":
    main()
