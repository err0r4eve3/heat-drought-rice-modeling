"""Command-line entry point for residual-risk report generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.risk_register import generate_risk_action_report  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Generate current residual-risk action report.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Generate residual-risk report and supporting CSV files."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    logger.info("Generating residual-risk action report")
    result = generate_risk_action_report(
        processed_dir=config.data_processed_dir,
        reports_dir=config.project_root / "reports",
        references_dir=config.data_raw_dir / "references",
        target_year_min=config.baseline_years[0],
        target_year_max=int(config.raw.get("panel_policy", {}).get("main_content_years", [2000, 2024])[1]),
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Risk register rows: {}", result.risk_count)
    logger.info("Report: {}", result.report_path)
    logger.info("Risk register: {}", result.risk_register_path)
    logger.info("Coverage summary: {}", result.coverage_summary_path)
    logger.info("Unmatched admin units: {}", result.unmatched_admin_path)
    logger.info("Calibration summary: {}", result.calibration_summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
