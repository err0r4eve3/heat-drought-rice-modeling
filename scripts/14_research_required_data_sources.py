"""Command-line entry point for required data-source research outputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.data_sources import write_data_source_outputs  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Write deep data-source search catalog and report.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--references-dir", default=None, help="Override raw references directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Generate required data-source outputs."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    config = load_config(args.config)
    ensure_project_dirs(config)

    references_dir = Path(args.references_dir).resolve() if args.references_dir else config.data_raw_dir / "references"
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    logger.info("Writing required data-source catalog.")
    outputs = write_data_source_outputs(references_dir=references_dir, reports_dir=reports_dir)
    for key, path in outputs.items():
        logger.info("{}: {}", key, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
