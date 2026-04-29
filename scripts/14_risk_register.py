"""Command-line entry point for external access and residual-risk checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402
from src.risk_register import generate_risk_action_report, write_external_access_check  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Generate risk register and external-access check outputs.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Run residual-risk and access checks."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    reports_dir = config.project_root / "reports"
    target_year_max = int(config.raw.get("panel_policy", {}).get("main_content_years", [2000, 2024])[1])

    logger.info("Generating residual risk register")
    risk_result = generate_risk_action_report(
        processed_dir=config.data_processed_dir,
        reports_dir=reports_dir,
        references_dir=config.data_raw_dir / "references",
        target_year_min=config.baseline_years[0],
        target_year_max=target_year_max,
    )
    logger.info("Risk register: {}", risk_result.risk_register_path)

    logger.info("Checking external data access configuration")
    access_config = dict(config.raw.get("external_data_access", {}))
    access_config["_policy"] = config.raw.get("external_access_policy", {})
    access_result = write_external_access_check(
        access_config,
        processed_dir=config.data_processed_dir,
        reports_dir=reports_dir,
    )
    logger.info("External access report: {}", access_result["report_path"])
    logger.info("External access CSV: {}", access_result["csv_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
