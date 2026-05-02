"""Command-line entry point for 2008-2015 province grain backfill import."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.province_grain_backfill import (  # noqa: E402
    create_province_grain_backfill_template,
    import_province_grain_backfill,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Import province grain backfill rows for 2008-2015.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--template-path", default=None, help="Override backfill template CSV path.")
    parser.add_argument("--create-template", action="store_true", help="Create an empty template before importing.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run province grain backfill import."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    template_path = (
        Path(args.template_path).expanduser().resolve()
        if args.template_path
        else config.project_root / "data" / "manual_templates" / "province_grain_backfill_2008_2015.csv"
    )
    if args.create_template:
        create_province_grain_backfill_template(template_path)
        logger.info("Template written: {}", template_path)

    logger.info("Importing province grain backfill template.")
    result = import_province_grain_backfill(
        template_path=template_path,
        processed_dir=config.data_processed_dir,
        reports_dir=config.project_root / "reports",
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Province grain backfill status: {}", result.status)
    logger.info("Rows: {} input, {} output", result.input_rows, result.output_rows)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
