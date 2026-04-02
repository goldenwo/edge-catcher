"""Output formatting and alerting."""
from pathlib import Path

# ── Centralized report paths ─────────────────────────────────────────────────
# Change REPORTS_DIR to relocate all reports. Subdirectories are created
# automatically by Reporter.save() and the backtest runner.
REPORTS_DIR = Path("reports")

ANALYSIS_DIR = REPORTS_DIR / "analysis"
BACKTEST_DIR = REPORTS_DIR / "backtests"
RESEARCH_DIR = REPORTS_DIR / "research"

# Default output paths used by CLI and API
ANALYSIS_OUTPUT = ANALYSIS_DIR / "latest_analysis.json"
BACKTEST_OUTPUT = BACKTEST_DIR / "backtest_result.json"
RESEARCH_OUTPUT = RESEARCH_DIR / "findings"
