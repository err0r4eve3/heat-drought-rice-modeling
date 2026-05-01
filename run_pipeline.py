"""One-command pipeline runner for the heat-drought rice modeling project."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from loguru import logger


STEP_ORDER = [
    "inventory",
    "boundaries",
    "climate",
    "remote-sensing",
    "crop",
    "aggregate",
    "statistics",
    "validation-2024",
    "admin-crosswalk",
    "risk-register",
    "yield-sources",
    "source-search",
    "yield-proxy-download",
    "admin-provinces",
    "yield-proxy",
    "exposure-diagnosis",
    "annual-exposure",
    "manual-yield",
    "province-chd",
    "province-panel",
    "indices",
    "modeling",
    "figures",
    "diagnostics",
    "risk-report",
    "report",
]


def build_step_commands(config_path: str) -> dict[str, list[str]]:
    """Build base commands for every pipeline step."""

    del config_path
    python = sys.executable
    return {
        "inventory": [python, "scripts/00_inventory.py"],
        "boundaries": [python, "scripts/01_prepare_boundaries.py"],
        "climate": [python, "scripts/02_preprocess_climate.py"],
        "remote-sensing": [python, "scripts/03_preprocess_remote_sensing.py"],
        "crop": [python, "scripts/04_prepare_crop_mask_phenology.py"],
        "aggregate": [python, "scripts/05_spatial_aggregate.py"],
        "statistics": [python, "scripts/06_prepare_statistics.py"],
        "validation-2024": [python, "scripts/12_validate_2024_event.py"],
        "admin-crosswalk": [python, "scripts/13_build_admin_crosswalk.py"],
        "risk-register": [python, "scripts/14_risk_register.py"],
        "yield-sources": [python, "scripts/13_download_yield_panel_sources.py"],
        "source-search": [python, "scripts/14_research_required_data_sources.py"],
        "yield-proxy-download": [python, "scripts/16_download_yield_proxy_rasters.py"],
        "admin-provinces": [python, "scripts/17_assign_admin_provinces.py"],
        "yield-proxy": [python, "scripts/15_build_yield_proxy_panel.py"],
        "exposure-diagnosis": [python, "scripts/15_diagnose_exposure_coverage.py"],
        "annual-exposure": [python, "scripts/16_build_annual_exposure_panel.py"],
        "manual-yield": [python, "scripts/17_import_manual_yield_panel.py"],
        "province-chd": [python, "scripts/19_build_province_chd_panel.py"],
        "province-panel": [python, "scripts/18_build_province_panel.py"],
        "indices": [python, "scripts/07_build_indices.py"],
        "modeling": [python, "scripts/08_modeling.py"],
        "figures": [python, "scripts/09_make_figures.py"],
        "diagnostics": [python, "scripts/10_diagnostics.py"],
        "risk-report": [python, "scripts/18_generate_risk_action_report.py"],
        "report": [python, "scripts/11_generate_report.py"],
    }


def write_pipeline_summary(reports_dir: str | Path, results: list[dict[str, object]]) -> Path:
    """Write Markdown pipeline run summary."""

    reports = Path(reports_dir).expanduser().resolve()
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / "pipeline_run_summary.md"
    lines = [
        "# Pipeline Run Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "| step | status | returncode | duration_seconds | error |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for result in results:
        lines.append(
            "| {step} | {status} | {returncode} | {duration_seconds:.2f} | {error} |".format(
                step=result["step"],
                status=result["status"],
                returncode=result["returncode"],
                duration_seconds=float(result["duration_seconds"]),
                error=str(result["error"]).replace("|", "\\|"),
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Run one or all project pipeline steps.")
    parser.add_argument("--step", default="all", choices=["all", *STEP_ORDER], help="Pipeline step to run.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--force", action="store_true", help="Reserved for steps that support forced recompute.")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue when one step fails.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging in child steps.")
    return parser.parse_args()


def main() -> int:
    """Run selected pipeline steps."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    commands = build_step_commands(args.config)
    selected_steps = STEP_ORDER if args.step == "all" else [args.step]
    results: list[dict[str, object]] = []

    for step in selected_steps:
        command = [*commands[step], "--config", args.config]
        if args.verbose:
            command.append("--verbose")
        logger.info("Starting step {}: {}", step, " ".join(command))
        started = time.perf_counter()
        completed = subprocess.run(command, cwd=Path(__file__).resolve().parent, check=False)
        duration = time.perf_counter() - started
        status = "ok" if completed.returncode == 0 else "error"
        results.append(
            {
                "step": step,
                "status": status,
                "returncode": completed.returncode,
                "duration_seconds": duration,
                "error": "" if completed.returncode == 0 else "step failed",
            }
        )
        logger.info("Finished step {} with status {}", step, status)
        if completed.returncode != 0 and not args.continue_on_error:
            break

    summary_path = write_pipeline_summary(Path("reports"), results)
    logger.info("Pipeline summary: {}", summary_path)
    return 0 if all(result["returncode"] == 0 for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
