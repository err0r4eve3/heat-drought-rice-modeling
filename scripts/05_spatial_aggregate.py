"""Command-line entry point for spatial/table panel aggregation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.panel import aggregate_netcdf_to_province_bounds, spatial_aggregate  # noqa: E402
from src.staging import load_external_data_paths  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Aggregate source panels to administrative units.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--processed-dir", default=None, help="Override processed data directory.")
    parser.add_argument("--interim-dir", default=None, help="Override interim data directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--admin-units", default=None, help="Override prepared admin_units.gpkg path.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run spatial/table panel aggregation."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    interim_dir = Path(args.interim_dir).resolve() if args.interim_dir else config.data_interim_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    admin_units = Path(args.admin_units).resolve() if args.admin_units else None

    logger.info("Starting spatial/table panel aggregation.")
    crop_mask_refs = load_external_data_paths(config.project_root, "crop_mask")
    if crop_mask_refs:
        climate_refs = _prefer_compact_netcdf_files(load_external_data_paths(config.project_root, "climate"))
        remote_refs = _prefer_compact_netcdf_files(load_external_data_paths(config.project_root, "remote_sensing"))
        if climate_refs:
            climate_output, climate_warnings = aggregate_netcdf_to_province_bounds(
                netcdf_paths=climate_refs,
                reference_raster_paths=crop_mask_refs,
                output_path=interim_dir / "climate_province_growing_season.parquet",
                rice_growth_months=config.rice_growth_months,
                category="climate",
            )
            for warning in climate_warnings:
                logger.warning(warning)
            logger.info("province_climate_growing_season: {}", climate_output)
        if remote_refs:
            remote_output, remote_warnings = aggregate_netcdf_to_province_bounds(
                netcdf_paths=remote_refs,
                reference_raster_paths=crop_mask_refs,
                output_path=interim_dir / "remote_sensing_province_growing_season.parquet",
                rice_growth_months=config.rice_growth_months,
                category="remote_sensing",
            )
            for warning in remote_warnings:
                logger.warning(warning)
            logger.info("province_remote_sensing_growing_season: {}", remote_output)

    result = spatial_aggregate(
        processed_dir=processed_dir,
        interim_dir=interim_dir,
        reports_dir=reports_dir,
        admin_units_path=admin_units,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Spatial aggregation status: {}", result.status)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


def _prefer_compact_netcdf_files(paths: list[Path]) -> list[Path]:
    """Prefer clipped NetCDF files for providers where clipped files exist."""

    netcdf_paths = [path for path in paths if path.suffix.lower() in {".nc", ".nc4", ".cdf"}]
    if not netcdf_paths:
        return []
    text_by_path = {path: str(path).replace("\\", "/").lower() for path in netcdf_paths}
    compact: list[Path] = []
    for path in netcdf_paths:
        text = text_by_path[path]
        if "chirps" in text and any("chirps" in other and "/clipped/" in other for other in text_by_path.values()):
            if "/clipped/" not in text:
                continue
        if "gleam" in text and any("gleam" in other and "/clipped/" in other for other in text_by_path.values()):
            if "/clipped/" not in text:
                continue
        compact.append(path)
    return compact


if __name__ == "__main__":
    raise SystemExit(main())
