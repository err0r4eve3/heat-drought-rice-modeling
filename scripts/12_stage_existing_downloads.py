"""Command-line entry point for staging selected existing downloaded files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.staging import stage_existing_downloads  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Stage selected files from heat_drought_download_package.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument(
        "--package-root",
        default="heat_drought_download_package",
        help="Path to the existing download package.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite already staged files.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run data staging."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    package_root = Path(args.package_root).resolve()
    logger.info("Starting data staging from {}", package_root)
    result = stage_existing_downloads(
        package_root=package_root,
        project_root=config.project_root,
        overwrite=args.overwrite,
    )

    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Data staging status: {}", result.status)
    logger.info("Copied files: {}", result.copied_count)
    logger.info("Skipped files: {}", result.skipped_count)
    logger.info("External indexed files: {}", result.external_index_count)
    if result.external_index_csv:
        logger.info("External index CSV: {}", result.external_index_csv)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
