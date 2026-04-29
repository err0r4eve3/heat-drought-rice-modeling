"""Command-line entry point for exposure coverage diagnostics."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.exposure_diagnostics import diagnose_exposure_coverage  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Diagnose exposure coverage in model_panel.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--model-panel", default=None, help="Override model panel CSV/Parquet path.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run exposure coverage diagnostics."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    policy = config.raw.get("study_region_policy", {})
    region = policy.get("regions", {}).get(policy.get("default_region", ""), {})
    panel_policy = config.raw.get("panel_policy", {})
    main_years = panel_policy.get("main_content_years", [config.baseline_years[0], config.validation_event_year])

    model_panel = Path(args.model_panel).resolve() if args.model_panel else config.data_processed_dir / "model_panel.csv"
    logger.info("Diagnosing exposure coverage from: {}", model_panel)
    result = diagnose_exposure_coverage(
        model_panel=model_panel,
        processed_dir=config.data_processed_dir,
        interim_dir=config.data_interim_dir,
        output_dir=config.output_dir,
        reports_dir=config.project_root / "reports",
        main_event_year=config.main_event_year,
        main_year_min=int(main_years[0]),
        main_year_max=int(main_years[1]),
        study_provinces=[str(value) for value in region.get("provinces", [])],
    )
    for warning in result.warnings:
        logger.warning(warning)
    logger.info("Exposure coverage status: {}", result.exposure_coverage_status)
    logger.info("Exposure non-missing: {}/{}", result.exposure_nonmissing, result.model_rows)
    logger.info("Likely causes: {}", ", ".join(result.likely_causes))
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
