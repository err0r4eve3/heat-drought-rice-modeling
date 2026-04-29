"""Command-line entry point for remote-sensing preprocessing."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.remote_sensing import preprocess_remote_sensing  # noqa: E402
from src.staging import load_external_data_paths  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Preprocess remote-sensing data.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--remote-sensing-dir", default=None, help="Override raw remote-sensing directory.")
    parser.add_argument("--interim-dir", default=None, help="Override interim output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run remote-sensing preprocessing."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    remote_sensing_dir = (
        Path(args.remote_sensing_dir).resolve()
        if args.remote_sensing_dir
        else config.data_raw_dir / "remote_sensing"
    )
    interim_dir = Path(args.interim_dir).resolve() if args.interim_dir else config.data_interim_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    external_files = [] if args.remote_sensing_dir else load_external_data_paths(config.project_root, "remote_sensing")

    logger.info("Starting remote-sensing preprocessing: {}", remote_sensing_dir)
    if external_files:
        logger.info("Using {} external remote-sensing references from data/raw/references.", len(external_files))
    result = preprocess_remote_sensing(
        remote_sensing_dir=remote_sensing_dir,
        interim_dir=interim_dir,
        reports_dir=reports_dir,
        study_bbox=config.study_bbox,
        baseline_years=config.baseline_years,
        rice_growth_months=config.rice_growth_months,
        crs_wgs84=config.crs_wgs84,
        crs_equal_area=config.crs_equal_area,
        external_files=external_files,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Remote-sensing preprocessing status: {}", result.status)
    logger.info("Processed files: {}/{}", len(result.processed_files), result.file_count)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
