from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd

from src.annual_exposure import build_annual_exposure_panel


def test_build_annual_exposure_panel_from_province_sources(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    interim = tmp_path / "data" / "interim"
    reports = tmp_path / "reports"
    interim.mkdir(parents=True)
    pd.DataFrame(
        [
            {"province": "江苏", "year": 2022, "variable": "growing_season_mean_temperature", "value": 30.0},
            {"province": "浙江", "year": 2022, "variable": "growing_season_mean_temperature", "value": 28.0},
            {"province": "江苏", "year": 2022, "variable": "growing_season_precipitation_sum", "value": 100.0},
            {"province": "浙江", "year": 2022, "variable": "growing_season_precipitation_sum", "value": 200.0},
        ]
    ).to_parquet(interim / "climate_province_growing_season.parquet", index=False)

    result = build_annual_exposure_panel(
        processed_dir=processed,
        interim_dir=interim,
        reports_dir=reports,
        study_provinces=["江苏省", "浙江省"],
    )

    panel = pd.read_csv(processed / "annual_exposure_panel.csv")
    assert result.row_count == 2
    assert "chd_annual" in panel.columns
    assert "chd_2022_intensity" in panel.columns
    assert "event_time_2022" in panel.columns
    assert panel["chd_annual"].notna().any()
    assert panel["chd_2022_intensity"].notna().any()
    assert result.report_path.exists()
    assert (reports / "chd_panel_status.md").exists()


def test_build_annual_exposure_panel_writes_empty_outputs_without_sources(tmp_path: Path) -> None:
    result = build_annual_exposure_panel(
        processed_dir=tmp_path / "processed",
        interim_dir=tmp_path / "interim",
        reports_dir=tmp_path / "reports",
    )

    assert result.status == "empty"
    assert result.outputs["csv"].exists()
    assert result.outputs["parquet"].exists()


def test_annual_exposure_cli_uses_main_model_region_not_default_highlight(tmp_path: Path) -> None:
    interim = tmp_path / "data" / "interim"
    interim.mkdir(parents=True)
    pd.DataFrame(
        [
            {"province": "Alpha", "year": 2022, "variable": "tmax", "value": 30.0},
            {"province": "Alpha", "year": 2022, "variable": "precipitation", "value": 100.0},
            {"province": "Beta", "year": 2022, "variable": "tmax", "value": 35.0},
            {"province": "Beta", "year": 2022, "variable": "precipitation", "value": 80.0},
        ]
    ).to_parquet(interim / "climate_province_growing_season.parquet", index=False)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"project_root: {tmp_path.as_posix()}",
                "data_raw_dir: data/raw",
                "data_interim_dir: data/interim",
                "data_processed_dir: data/processed",
                "output_dir: data/outputs",
                "study_area_name: test",
                "study_bbox: [105, 24, 123, 35]",
                "target_admin_level: province",
                "crs_wgs84: EPSG:4326",
                "crs_equal_area: EPSG:6933",
                "baseline_years: [2000, 2021]",
                "main_event_year: 2022",
                "validation_event_year: 2024",
                "recovery_years: [2023, 2024, 2025]",
                "main_event_months: [6, 7, 8, 9, 10]",
                "rice_growth_months: [6, 7, 8, 9]",
                "heat_threshold_quantile: 0.90",
                "drought_threshold_quantile: 0.10",
                "min_valid_observations: 3",
                "output_formats: [csv, parquet, markdown]",
                "panel_policy:",
                "  main_content_years: [2000, 2024]",
                "study_region_policy:",
                "  default_region: yangtze_middle_lower",
                "  main_model_region: national_control",
                "  regions:",
                "    yangtze_middle_lower:",
                "      provinces: [Alpha]",
                "    national_control:",
                "      provinces: all",
            ]
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "scripts/16_build_annual_exposure_panel.py", "--config", str(config_path)],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    panel = pd.read_csv(tmp_path / "data" / "processed" / "annual_exposure_panel.csv")

    assert completed.returncode == 0, completed.stderr
    assert set(panel["province"]) == {"Alpha", "Beta"}
