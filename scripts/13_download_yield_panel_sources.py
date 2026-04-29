"""Command-line entry point for external yield source download and normalization."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.yield_sources import download_and_build_yield_sources  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Download and normalize external yield panel sources.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--statistics-dir", default=None, help="Override raw statistics directory.")
    parser.add_argument("--references-dir", default=None, help="Override raw references directory.")
    parser.add_argument("--processed-dir", default=None, help="Override processed output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--force", action="store_true", help="Re-download files even if they already exist.")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="HTTP timeout per source.")
    parser.add_argument("--year-min", type=int, default=2000, help="Minimum output year.")
    parser.add_argument("--year-max", type=int, default=None, help="Maximum output year; defaults to panel_policy main end year.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run external yield source acquisition."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    statistics_dir = Path(args.statistics_dir).resolve() if args.statistics_dir else config.data_raw_dir / "statistics"
    references_dir = Path(args.references_dir).resolve() if args.references_dir else config.data_raw_dir / "references"
    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    main_years = config.raw.get("panel_policy", {}).get("main_content_years", [args.year_min, config.validation_event_year])
    year_max = int(args.year_max if args.year_max is not None else main_years[1])

    logger.info("Starting external yield source acquisition.")
    result = download_and_build_yield_sources(
        statistics_dir=statistics_dir,
        references_dir=references_dir,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        force=args.force,
        timeout_seconds=args.timeout_seconds,
        year_min=args.year_min,
        year_max=year_max,
    )

    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Yield source status: {}", result.status)
    logger.info("Normalized panel rows: {}", result.panel_rows)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
