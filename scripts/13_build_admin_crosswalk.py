"""Command-line entry point for administrative crosswalk construction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.admin_crosswalk import build_admin_crosswalk  # noqa: E402
from src.config import ensure_project_dirs, load_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Build admin code/name crosswalk for the main model years; 2025 is optional background."
    )
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--admin-codes-dir", default=None, help="Directory containing admin-code source files.")
    parser.add_argument("--manual-crosswalk", default=None, help="Optional manually curated crosswalk CSV/XLSX/JSON.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Build the admin crosswalk."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    admin_codes_dir = Path(args.admin_codes_dir).resolve() if args.admin_codes_dir else config.data_raw_dir / "admin_codes"
    target_year_max = int(config.raw.get("panel_policy", {}).get("main_content_years", [2000, 2024])[1])
    target_boundary_year = int(config.raw.get("admin_crosswalk_policy", {}).get("target_boundary_year", 2022))
    logger.info("Building admin crosswalk from {}", admin_codes_dir)
    result = build_admin_crosswalk(
        admin_codes_dir=admin_codes_dir,
        processed_dir=config.data_processed_dir,
        reports_dir=config.project_root / "reports",
        year_min=config.baseline_years[0],
        year_max=target_year_max,
        target_boundary_year=target_boundary_year,
        manual_crosswalk_path=args.manual_crosswalk,
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Crosswalk status: {}", result.status)
    logger.info("Rows: {}", result.row_count)
    logger.info("Low-confidence rows: {}", result.low_confidence_count)
    logger.info("Output: {}", result.output_path)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
