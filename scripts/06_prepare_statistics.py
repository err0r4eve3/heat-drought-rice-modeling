"""Command-line entry point for agricultural statistics cleaning."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.statistics import prepare_statistics  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Prepare agricultural statistics yield panel.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--statistics-dir", default=None, help="Override raw statistics directory.")
    parser.add_argument("--processed-dir", default=None, help="Override processed output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run agricultural statistics cleaning."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    statistics_dir = Path(args.statistics_dir).resolve() if args.statistics_dir else config.data_raw_dir / "statistics"
    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    logger.info("Starting agricultural statistics cleaning: {}", statistics_dir)
    main_years = config.raw.get("panel_policy", {}).get(
        "main_content_years",
        [config.baseline_years[0], config.validation_event_year],
    )
    expected_years = list(range(int(main_years[0]), int(main_years[1]) + 1))
    result = prepare_statistics(
        statistics_dir=statistics_dir,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        expected_years=expected_years,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Statistics cleaning status: {}", result.status)
    logger.info("Processed files: {}/{}", len(result.processed_files), result.file_count)
    logger.info("Output rows: {}", result.row_count)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
