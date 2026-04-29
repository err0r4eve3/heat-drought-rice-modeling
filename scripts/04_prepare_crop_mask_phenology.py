"""Command-line entry point for crop-mask and phenology preparation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.crop import prepare_crop_mask_phenology  # noqa: E402
from src.staging import load_external_data_paths  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Prepare crop mask and phenology MVP outputs.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--crop-mask-dir", default=None, help="Override raw crop-mask directory.")
    parser.add_argument("--phenology-dir", default=None, help="Override raw phenology directory.")
    parser.add_argument("--processed-dir", default=None, help="Override processed output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--target-year", type=int, default=None, help="Override target event year.")
    parser.add_argument(
        "--rice-growth-months",
        default=None,
        help="Override rice growth months as comma-separated values, for example 6,7,8,9.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run crop-mask and phenology preparation."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    crop_mask_dir = Path(args.crop_mask_dir).resolve() if args.crop_mask_dir else config.data_raw_dir / "crop_mask"
    phenology_dir = Path(args.phenology_dir).resolve() if args.phenology_dir else config.data_raw_dir / "phenology"
    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    target_year = args.target_year if args.target_year is not None else config.main_event_year
    rice_growth_months = (
        _parse_months(args.rice_growth_months) if args.rice_growth_months else config.rice_growth_months
    )
    external_crop_mask_files = [] if args.crop_mask_dir else load_external_data_paths(config.project_root, "crop_mask")
    external_phenology_files = [] if args.phenology_dir else load_external_data_paths(config.project_root, "phenology")

    logger.info("Starting crop-mask and phenology preparation: {}", crop_mask_dir)
    if external_crop_mask_files:
        logger.info("Using {} external crop-mask references from data/raw/references.", len(external_crop_mask_files))
    if external_phenology_files:
        logger.info("Using {} external phenology references from data/raw/references.", len(external_phenology_files))
    result = prepare_crop_mask_phenology(
        crop_mask_dir=crop_mask_dir,
        phenology_dir=phenology_dir,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        study_bbox=config.study_bbox,
        target_year=target_year,
        rice_growth_months=rice_growth_months,
        crs_wgs84=config.crs_wgs84,
        crs_equal_area=config.crs_equal_area,
        external_crop_mask_files=external_crop_mask_files,
        external_phenology_files=external_phenology_files,
        admin_units_path=processed_dir / "admin_units.parquet",
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Crop-mask and phenology status: {}", result.status)
    logger.info("Crop mask candidate files: {}", len(result.crop_mask_files))
    logger.info("Phenology candidate files: {}", len(result.phenology_files))
    if result.selected_mask_path:
        logger.info("Selected crop mask: {}", result.selected_mask_path)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


def _parse_months(value: str) -> list[int]:
    """Parse comma-separated month values."""

    months = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not months:
        raise ValueError("--rice-growth-months must contain at least one month")
    return months


if __name__ == "__main__":
    raise SystemExit(main())
