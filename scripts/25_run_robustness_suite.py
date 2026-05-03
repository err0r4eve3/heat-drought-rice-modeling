"""Command-line entry point for province-panel robustness checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.result_quality_core import run_robustness_suite  # noqa: E402
from src.config import ensure_project_dirs, load_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Run robustness suite for province annual CHD model.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run robustness suite."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    config = load_config(args.config)
    ensure_project_dirs(config)
    output_csv, report_path = run_robustness_suite(config)
    logger.info("Robustness CSV: {}", output_csv)
    logger.info("Robustness report: {}", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
