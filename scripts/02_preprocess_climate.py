"""Command-line entry point for climate preprocessing."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.climate import preprocess_climate  # noqa: E402
from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.staging import load_external_data_paths  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Preprocess NetCDF climate data.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--climate-dir", default=None, help="Override raw climate directory.")
    parser.add_argument("--interim-dir", default=None, help="Override interim output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run climate preprocessing."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    climate_dir = Path(args.climate_dir).resolve() if args.climate_dir else config.data_raw_dir / "climate"
    interim_dir = Path(args.interim_dir).resolve() if args.interim_dir else config.data_interim_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    external_files = [] if args.climate_dir else load_external_data_paths(config.project_root, "climate")

    logger.info("Starting climate preprocessing: {}", climate_dir)
    if external_files:
        logger.info("Using {} external climate references from data/raw/references.", len(external_files))
    result = preprocess_climate(
        climate_dir=climate_dir,
        interim_dir=interim_dir,
        reports_dir=reports_dir,
        study_bbox=config.study_bbox,
        baseline_years=config.baseline_years,
        main_event_year=config.main_event_year,
        recovery_years=config.recovery_years,
        validation_event_year=config.validation_event_year,
        rice_growth_months=config.rice_growth_months,
        heat_threshold_quantile=config.heat_threshold_quantile,
        drought_threshold_quantile=config.drought_threshold_quantile,
        external_files=external_files,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Climate preprocessing status: {}", result.status)
    logger.info("Processed files: {}/{}", len(result.processed_files), result.file_count)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
