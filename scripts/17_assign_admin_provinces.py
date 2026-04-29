"""Command-line entry point for assigning province names to admin units."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.admin_province import assign_admin_provinces  # noqa: E402
from src.config import ensure_project_dirs, load_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Assign province names to prepared admin units.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--admin-path", default=None, help="Prepared admin units path.")
    parser.add_argument("--province-path", default=None, help="Province boundary vector path.")
    parser.add_argument("--output-path", default=None, help="Output GPKG path.")
    parser.add_argument("--parquet-path", default=None, help="Output parquet path.")
    parser.add_argument("--reports-dir", default=None, help="Reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run province assignment."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    config = load_config(args.config)
    ensure_project_dirs(config)

    processed = config.data_processed_dir
    admin_path = Path(args.admin_path).resolve() if args.admin_path else processed / "admin_units.gpkg"
    province_path = (
        Path(args.province_path).resolve()
        if args.province_path
        else config.data_raw_dir / "references" / "boundary" / "geoBoundaries-CHN-ADM1_simplified.geojson"
    )
    output_path = Path(args.output_path).resolve() if args.output_path else processed / "admin_units_with_province.gpkg"
    parquet_path = (
        Path(args.parquet_path).resolve() if args.parquet_path else processed / "admin_units_with_province.parquet"
    )
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    logger.info("Assigning province names using {}", province_path)
    result = assign_admin_provinces(
        admin_path=admin_path,
        province_path=province_path,
        output_path=output_path,
        parquet_path=parquet_path,
        report_path=reports_dir / "admin_province_assignment_summary.md",
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Status: {}", result.status)
    logger.info("Matched rows: {}/{}", result.matched_rows, result.input_rows)
    logger.info("Output: {}", result.output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
