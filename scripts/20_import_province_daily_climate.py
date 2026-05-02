"""Command-line entry point for province daily climate QC."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.province_daily_climate import import_province_daily_climate  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Validate province daily climate input.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run province daily climate QC."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    panel_policy = config.raw.get("panel_policy", {})
    main_years = panel_policy.get("main_content_years", [config.baseline_years[0], config.validation_event_year])

    logger.info("Validating province daily climate input.")
    result = import_province_daily_climate(
        interim_dir=config.data_interim_dir,
        output_dir=config.output_dir,
        reports_dir=config.project_root / "reports",
        year_min=int(main_years[0]),
        year_max=int(main_years[1]),
        growth_months=config.rice_growth_months,
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Province daily climate status: {}", result.status)
    logger.info("Rows: {}", result.row_count)
    logger.info("Province count: {}", result.province_count)
    logger.info("QC report: {}", result.outputs["qc_report"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
