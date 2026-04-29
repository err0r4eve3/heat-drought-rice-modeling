"""Command-line entry point for MVP statistical modeling."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.models import run_modeling  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Run MVP descriptive modeling from model_panel.csv.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--model-panel", default=None, help="Override model panel CSV path.")
    parser.add_argument("--output-dir", default=None, help="Override model output directory.")
    parser.add_argument("--reports-dir", default=None, help="Override reports directory.")
    parser.add_argument("--outcome-field", default="yield_anomaly", help="Outcome column name.")
    parser.add_argument("--x-vars", nargs="*", default=None, help="Predictor column names.")
    parser.add_argument("--exposure-field", default="chd_annual", help="Exposure column for treatment.")
    parser.add_argument("--year-field", default="year", help="Year column name.")
    parser.add_argument("--admin-field", default="admin_id", help="Administrative unit column name.")
    parser.add_argument("--event-window", type=int, default=3, help="Event-study window in years.")
    parser.add_argument("--treatment-quantile", type=float, default=0.5, help="Treatment exposure quantile.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run modeling from the command line."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)

    default_panel = config.data_processed_dir / "model_panel_study_region.csv"
    if not default_panel.exists():
        default_panel = config.data_processed_dir / "model_panel.csv"
    model_panel = Path(args.model_panel).resolve() if args.model_panel else default_panel
    output_dir = Path(args.output_dir).resolve() if args.output_dir else config.output_dir
    reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else config.project_root / "reports"
    panel_policy = config.raw.get("panel_policy", {})
    main_years = panel_policy.get("main_content_years", [config.baseline_years[0], config.validation_event_year])
    min_year = int(main_years[0])
    max_year = None if panel_policy.get("use_2025_in_main_model", False) else int(main_years[1])

    logger.info("Starting MVP modeling from: {}", model_panel)
    result = run_modeling(
        model_panel=model_panel,
        output_dir=output_dir,
        reports_dir=reports_dir,
        event_year=config.main_event_year,
        processed_dir=config.data_processed_dir,
        outcome_field=args.outcome_field,
        x_vars=args.x_vars,
        exposure_field=args.exposure_field,
        year_field=args.year_field,
        admin_field=args.admin_field,
        treatment_quantile=args.treatment_quantile,
        event_window=args.event_window,
        min_year=min_year,
        max_year=max_year,
    )

    for warning in result.warnings:
        logger.warning(warning)

    logger.info("Modeling status: {}", result.status)
    logger.info("Rows read: {}", result.n_rows)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
