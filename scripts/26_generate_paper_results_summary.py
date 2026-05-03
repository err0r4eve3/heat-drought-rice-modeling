"""Command-line entry point for paper-facing result summary generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.result_quality_core import generate_paper_results_summary  # noqa: E402
from src.config import ensure_project_dirs, load_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Generate paper-facing results summary with claim guardrails.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Generate paper results summary."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    config = load_config(args.config)
    ensure_project_dirs(config)
    report_path = generate_paper_results_summary(config)
    logger.info("Paper results summary: {}", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
