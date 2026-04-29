"""Command-line entry point for yield-proxy raster manifesting and downloads."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.yield_proxy_download import download_proxy_sources  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Download open gridded yield proxy rasters.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--output-dir", default=None, help="Where proxy rasters should be stored.")
    parser.add_argument("--references-dir", default=None, help="Where manifests should be written.")
    parser.add_argument("--reports-dir", default=None, help="Reports directory.")
    parser.add_argument("--sources", default="asia,ggcp10", help="Comma-separated sources: asia,ggcp10.")
    parser.add_argument("--year-min", type=int, default=2010, help="Minimum GGCP10 rice year.")
    parser.add_argument("--year-max", type=int, default=2020, help="Maximum GGCP10 rice year.")
    parser.add_argument("--download", action="store_true", help="Actually download files; default is manifest only.")
    parser.add_argument("--no-extract", action="store_true", help="Do not extract downloaded zip archives.")
    parser.add_argument("--force", action="store_true", help="Re-download files even if they already exist.")
    parser.add_argument("--timeout-seconds", type=int, default=300, help="HTTP timeout per request.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run yield-proxy source acquisition."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    output_dir = Path(args.output_dir).resolve() if args.output_dir else config.data_raw_dir / "statistics" / "yield_proxy"
    references_dir = Path(args.references_dir).resolve() if args.references_dir else config.data_raw_dir / "references"
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    sources = [source.strip() for source in args.sources.split(",") if source.strip()]

    logger.info("Preparing yield-proxy downloads for sources: {}", ", ".join(sources))
    result = download_proxy_sources(
        output_dir=output_dir,
        references_dir=references_dir,
        reports_dir=reports_dir,
        sources=sources,
        year_min=args.year_min,
        year_max=args.year_max,
        execute_download=args.download,
        extract_archives=not args.no_extract,
        force=args.force,
        timeout_seconds=args.timeout_seconds,
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Status: {}", result.status)
    logger.info("Manifest rows: {}", result.manifest_count)
    logger.info("Downloaded/existing files: {}", result.downloaded_count)
    logger.info("Extracted archive members: {}", result.extracted_count)
    logger.info("Manifest CSV: {}", result.manifest_csv)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
