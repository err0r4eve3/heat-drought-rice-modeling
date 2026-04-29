"""Command-line entry point for core index construction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.indices import build_indices  # noqa: E402
from src.study_region import enrich_and_filter_model_panel  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Build yield, climate, and remote-sensing indices.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--yield-panel", default=None, help="Override yield panel CSV/Parquet path.")
    parser.add_argument("--climate-panel", default=None, help="Override climate panel CSV/Parquet path.")
    parser.add_argument("--remote-sensing-panel", default=None, help="Override remote-sensing panel CSV/Parquet path.")
    parser.add_argument("--interim-dir", default=None, help="Override interim data directory.")
    parser.add_argument("--processed-dir", default=None, help="Override processed data directory.")
    parser.add_argument("--output-dir", default=None, help="Override output data directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run index construction."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    interim_dir = Path(args.interim_dir).resolve() if args.interim_dir else config.data_interim_dir
    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    output_dir = Path(args.output_dir).resolve() if args.output_dir else config.output_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    logger.info("Starting index construction.")
    result = build_indices(
        yield_panel=args.yield_panel,
        climate_panel=args.climate_panel,
        remote_sensing_panel=args.remote_sensing_panel,
        interim_dir=interim_dir,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        output_dir=output_dir,
        baseline_years=config.baseline_years,
        min_valid_observations=config.min_valid_observations,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Index construction status: {}", result.status)
    logger.info("Rows: {}", result.row_count)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)

    logger.info("Enriching model panel with event exposure fields and study-region filter.")
    study_result = enrich_and_filter_model_panel(
        model_panel=result.outputs["model_panel"],
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        study_region_policy=config.raw.get("study_region_policy", {}),
        main_event_year=config.main_event_year,
        validation_event_year=config.validation_event_year,
    )
    for warning in study_result.warnings:
        logger.warning(warning)
    logger.info("Study-region panel rows: {}", study_result.output_rows)
    logger.info("Study-region report: {}", study_result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
