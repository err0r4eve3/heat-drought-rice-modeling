from pathlib import Path
import sys

from run_pipeline import build_step_commands, write_pipeline_summary


def test_build_step_commands_contains_core_steps() -> None:
    commands = build_step_commands("config/config.yaml")

    assert commands["inventory"][-1] == "scripts/00_inventory.py"
    assert "boundaries" in commands
    assert "climate" in commands
    assert "diagnostics" in commands
    assert commands["source-search"][-1] == "scripts/14_research_required_data_sources.py"
    assert commands["validation-2024"][-1] == "scripts/12_validate_2024_event.py"
    assert commands["admin-crosswalk"][-1] == "scripts/13_build_admin_crosswalk.py"
    assert commands["risk-register"][-1] == "scripts/14_risk_register.py"
    assert commands["yield-proxy-download"][-1] == "scripts/16_download_yield_proxy_rasters.py"
    assert commands["admin-provinces"][-1] == "scripts/17_assign_admin_provinces.py"
    assert commands["yield-proxy"][-1] == "scripts/15_build_yield_proxy_panel.py"
    assert commands["exposure-diagnosis"][-1] == "scripts/15_diagnose_exposure_coverage.py"
    assert commands["annual-exposure"][-1] == "scripts/16_build_annual_exposure_panel.py"
    assert commands["manual-yield"][-1] == "scripts/17_import_manual_yield_panel.py"
    assert commands["risk-report"][-1] == "scripts/18_generate_risk_action_report.py"


def test_build_step_commands_use_current_python_interpreter() -> None:
    commands = build_step_commands("config/config.yaml")

    assert all(command[0] == sys.executable for command in commands.values())


def test_write_pipeline_summary_creates_report(tmp_path: Path) -> None:
    summary_path = write_pipeline_summary(
        reports_dir=tmp_path,
        results=[
            {
                "step": "inventory",
                "status": "ok",
                "returncode": 0,
                "duration_seconds": 1.2,
                "error": "",
            }
        ],
    )

    assert summary_path.exists()
    assert "inventory" in summary_path.read_text(encoding="utf-8")
