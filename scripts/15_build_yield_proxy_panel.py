"""Command-line entry point for county-level yield proxy panel construction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.yield_proxy import build_yield_proxy_panel  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Build county-level yield proxy panel from open gridded rasters.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--proxy-dir", default=None, help="Directory containing yield proxy GeoTIFFs.")
    parser.add_argument("--admin-path", default=None, help="Prepared admin units path.")
    parser.add_argument("--crop-summary-path", default=None, help="Crop mask summary path.")
    parser.add_argument("--official-yield-path", default=None, help="Official yield panel path.")
    parser.add_argument("--output-dir", default=None, help="Output directory for proxy panel files.")
    parser.add_argument("--reports-dir", default=None, help="Reports directory.")
    parser.add_argument("--year-min", type=int, default=2000, help="Minimum target year.")
    parser.add_argument("--year-max", type=int, default=2020, help="Maximum target year.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run yield proxy panel construction."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    processed = config.data_processed_dir
    proxy_dir = Path(args.proxy_dir).resolve() if args.proxy_dir else config.data_raw_dir / "statistics" / "yield_proxy"
    admin_path = _first_existing(
        args.admin_path,
        [
            processed / "admin_units_with_province.gpkg",
            processed / "admin_units_with_province.parquet",
            processed / "admin_units.gpkg",
            processed / "admin_units.parquet",
        ],
    )
    crop_summary_path = Path(args.crop_summary_path).resolve() if args.crop_summary_path else processed / "crop_mask_summary_by_admin.csv"
    official_yield_path = Path(args.official_yield_path).resolve() if args.official_yield_path else processed / "yield_panel_combined.csv"
    output_dir = Path(args.output_dir).resolve() if args.output_dir else processed / "yield_proxy"
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    logger.info("Building yield proxy panel from {}", proxy_dir)
    result = build_yield_proxy_panel(
        proxy_dir=proxy_dir,
        admin_path=admin_path,
        crop_summary_path=crop_summary_path,
        official_yield_path=official_yield_path,
        output_dir=output_dir,
        reports_dir=reports_dir,
        target_years=(args.year_min, args.year_max),
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Yield proxy status: {}", result.status)
    logger.info("Raster count: {}", result.raster_count)
    logger.info("Panel rows: {}", result.row_count)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


def _first_existing(override: str | None, candidates: list[Path]) -> Path:
    """Return an override path or first existing candidate."""

    if override:
        return Path(override).resolve()
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


if __name__ == "__main__":
    raise SystemExit(main())
