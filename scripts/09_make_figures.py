"""Command-line entry point for figure generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.figures import make_figures  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Generate project figures.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run figure generation."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    config = load_config(args.config)
    ensure_project_dirs(config)

    logger.info("Starting figure generation")
    result = make_figures(
        processed_dir=config.data_processed_dir,
        output_dir=config.output_dir,
        reports_dir=config.project_root / "reports",
        main_event_year=config.main_event_year,
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Figure generation status: {}", result.status)
    logger.info("Generated files: {}", len(result.figures))
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
