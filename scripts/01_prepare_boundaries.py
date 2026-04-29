"""Command-line entry point for administrative-boundary preparation."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from loguru import logger
except ImportError:  # pragma: no cover - used by bundled geospatial venv without loguru
    import logging

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger = logging.getLogger(__name__)
    logger.remove = lambda: None
    logger.add = lambda *args, **kwargs: None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.spatial import prepare_boundaries  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Prepare administrative boundaries for modeling.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--boundary-dir", default=None, help="Override raw boundary directory.")
    parser.add_argument("--processed-dir", default=None, help="Override processed output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run administrative-boundary preparation."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    reexec = _geospatial_python_if_needed()
    if reexec is not None:
        command = [str(reexec), str(Path(__file__).resolve()), *sys.argv[1:]]
        logger.info(f"Re-running boundary preparation with geospatial Python: {reexec}")
        return subprocess.run(command, check=False).returncode

    config = load_config(args.config)
    ensure_project_dirs(config)

    boundary_dir = Path(args.boundary_dir).resolve() if args.boundary_dir else config.data_raw_dir / "boundary"
    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    logger.info(f"Starting boundary preparation: {boundary_dir}")
    result = prepare_boundaries(
        boundary_dir=boundary_dir,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        study_bbox=config.study_bbox,
        crs_wgs84=config.crs_wgs84,
        crs_equal_area=config.crs_equal_area,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info(f"Boundary preparation status: {result.status}")
    logger.info(f"Feature count: {result.feature_count}")
    for key, path in result.outputs.items():
        logger.info(f"{key}: {path}")
    logger.info(f"Report: {result.report_path}")
    return 0


def _geospatial_python_if_needed() -> Path | None:
    """Return bundled geospatial Python if current interpreter lacks geopandas."""

    try:
        import geopandas  # noqa: F401
        return None
    except ImportError:
        candidate = PROJECT_ROOT / "heat_drought_download_package" / ".venv" / "Scripts" / "python.exe"
        current = Path(sys.executable).resolve()
        if candidate.exists() and candidate.resolve() != current:
            return candidate
    return None


if __name__ == "__main__":
    raise SystemExit(main())
