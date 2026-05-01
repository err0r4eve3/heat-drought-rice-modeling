"""Command-line entry point for province official model panel construction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.province_panel import build_province_model_panel  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Build province-level official yield model panel.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run province model panel construction."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    panel_policy = config.raw.get("panel_policy", {})
    main_years = panel_policy.get("main_content_years", [config.baseline_years[0], config.validation_event_year])

    logger.info("Building province model panel.")
    result = build_province_model_panel(
        processed_dir=config.data_processed_dir,
        reports_dir=config.project_root / "reports",
        main_year_min=int(main_years[0]),
        main_year_max=int(main_years[1]),
        baseline_years=config.baseline_years,
        min_valid_observations=config.min_valid_observations,
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Province model panel status: {}", result.status)
    logger.info("Rows: {}", result.row_count)
    logger.info("Outcome: {}", result.outcome_type)
    logger.info("Allowed claim strength: {}", result.allowed_claim_strength)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
