"""Command-line entry point for final Markdown summary generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_report_core import generate_final_report  # noqa: E402
from src.config import ensure_project_dirs, load_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Generate final analysis summary.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Generate final report."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    config = load_config(args.config)
    ensure_project_dirs(config)

    logger.info("Generating final analysis summary")
    main_years = config.raw.get("panel_policy", {}).get("main_content_years", [config.baseline_years[0], config.validation_event_year])
    report_path = generate_final_report(
        processed_dir=config.data_processed_dir,
        output_dir=config.output_dir,
        reports_dir=config.project_root / "reports",
        main_event_year=config.main_event_year,
        main_year_min=int(main_years[0]),
        main_year_max=int(main_years[1]),
    )
    logger.info("Report: {}", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
