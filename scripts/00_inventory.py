"""Command-line entry point for raw-data inventory scanning."""

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
from src.inventory import build_inventory, write_inventory_outputs  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Scan data/raw and create data inventory outputs.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--raw-dir", default=None, help="Override raw data directory.")
    parser.add_argument("--processed-dir", default=None, help="Override processed output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run inventory scanning from the command line."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    raw_dir = Path(args.raw_dir).resolve() if args.raw_dir else config.data_raw_dir
    processed_dir = Path(args.processed_dir).resolve() if args.processed_dir else config.data_processed_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"

    reexec = _geospatial_python_if_needed(raw_dir)
    if reexec is not None:
        command = [str(reexec), str(Path(__file__).resolve()), *sys.argv[1:]]
        logger.info(f"Re-running inventory with geospatial Python: {reexec}")
        return subprocess.run(command, check=False).returncode

    logger.info(f"Starting raw data inventory: {raw_dir}")
    inventory = build_inventory(raw_dir)
    outputs = write_inventory_outputs(inventory, processed_dir, reports_dir)

    for warning in inventory.warnings:
        logger.warning(warning)

    logger.info(f"Inventory complete. Files scanned: {len(inventory.records)}")
    logger.info(f"CSV: {outputs['csv']}")
    logger.info(f"JSON: {outputs['json']}")
    logger.info(f"Report: {outputs['report']}")
    return 0


def _geospatial_python_if_needed(raw_dir: Path) -> Path | None:
    """Return bundled geospatial Python if vector inventory needs geopandas."""

    vector_suffixes = {".shp", ".gpkg", ".geojson"}
    has_vector = raw_dir.exists() and any(
        path.is_file() and path.suffix.lower() in vector_suffixes for path in raw_dir.rglob("*")
    )
    if not has_vector:
        return None
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
