"""Command-line entry point for province CHD panel construction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.province_chd import build_province_chd_panel  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Build province-level CHD panel.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run province CHD panel construction."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    panel_policy = config.raw.get("panel_policy", {})
    main_years = panel_policy.get("main_content_years", [config.baseline_years[0], config.validation_event_year])
    region_policy = config.raw.get("study_region_policy", {})
    highlighted = region_policy.get("highlighted_region", region_policy.get("default_region", ""))
    region = region_policy.get("regions", {}).get(highlighted, {})

    logger.info("Building province CHD panel.")
    result = build_province_chd_panel(
        processed_dir=config.data_processed_dir,
        interim_dir=config.data_interim_dir,
        reports_dir=config.project_root / "reports",
        main_year_min=int(main_years[0]),
        main_year_max=int(main_years[1]),
        main_event_year=config.main_event_year,
        highlighted_provinces=[str(value) for value in region.get("provinces", [])],
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Province CHD status: {}", result.status)
    logger.info("Rows: {}", result.row_count)
    logger.info("chd_annual coverage: {:.6f}", result.chd_coverage_rate)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
