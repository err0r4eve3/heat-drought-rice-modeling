from __future__ import annotations

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
