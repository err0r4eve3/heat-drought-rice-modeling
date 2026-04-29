from __future__ import annotations

import math
from pathlib import Path

from src import indices


def test_calculate_yield_anomaly_returns_abs_pct_and_zscore() -> None:
    result = indices.calculate_yield_anomaly(
        actual_yield=110,
        trend_yield=100,
        baseline_mean=95,
        baseline_std=5,
    )

    assert result["abs"] == 10.0
    assert result["pct"] == 10.0
    assert result["zscore"] == 3.0


def test_calculate_yield_anomaly_handles_missing_and_zero_denominators() -> None:
    missing = indices.calculate_yield_anomaly(None, 100)
    zero_trend = indices.calculate_yield_anomaly(100, 0, baseline_mean=90, baseline_std=0)

    assert missing == {"abs": None, "pct": None, "zscore": None}
    assert zero_trend["abs"] == 100.0
    assert zero_trend["pct"] is None
    assert zero_trend["zscore"] is None


def test_calculate_rolling_cv_uses_last_valid_window_and_requires_enough_samples() -> None:
    result = indices.calculate_rolling_cv([1, None, 2, 3], window=3)

    assert math.isclose(result, 0.5)
    assert indices.calculate_rolling_cv([1, None, 2], window=3) is None
    assert indices.calculate_rolling_cv([0, 0, 0], window=3) is None


def test_calculate_chd_intensity_accumulates_only_compound_hot_dry_periods() -> None:
    result = indices.calculate_chd_intensity(
        hot_zscores=[2.0, 3.0, None, 4.0],
        dry_zscores=[-1.5, -2.0, -3.0, None],
        hot_flags=[True, True, True, False],
        dry_flags=[True, False, True, True],
    )

    assert result == 3.0


def test_calculate_max_duration_returns_longest_true_run() -> None:
    assert indices.calculate_max_duration([True, True, False, True, True, True]) == 3
    assert indices.calculate_max_duration([False, None, False]) == 0


def test_calculate_vhi_combines_vci_and_tci() -> None:
    assert indices.calculate_vhi(
        ndvi=0.5,
        ndvi_min=0.2,
        ndvi_max=0.8,
        lst=30,
        lst_min=20,
        lst_max=40,
    ) == 50.0
    assert indices.calculate_vhi(0.5, 0.5, 0.5, 30, 20, 40) is None


def test_build_indices_writes_empty_fallback_when_required_inputs_are_missing(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    reports_dir = tmp_path / "reports"

    result = indices.build_indices(
        yield_panel=tmp_path / "missing_yield.csv",
        climate_panel=tmp_path / "missing_climate.csv",
        remote_sensing_panel=tmp_path / "missing_remote.csv",
        processed_dir=processed_dir,
        reports_dir=reports_dir,
    )

    model_panel = processed_dir / "model_panel.csv"
    report = reports_dir / "index_construction_summary.md"

    assert result.status == "missing"
    assert result.row_count == 0
    assert model_panel.exists()
    assert model_panel.read_text(encoding="utf-8").splitlines() == ["admin_code,year"]
    assert report.exists()
    report_text = report.read_text(encoding="utf-8")
    assert "Missing required input" in report_text
    assert str(tmp_path / "missing_yield.csv") in report_text


def test_build_indices_keeps_yield_rows_when_optional_panels_are_missing(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)
    reports_dir = tmp_path / "reports"
    yield_panel = processed_dir / "yield_panel.csv"
    yield_panel.write_text(
        "\n".join(
            [
                "admin_code,province,year,yield_kg_per_hectare",
                "320000,江苏,2024,6958.4",
                "330000,浙江,2024,6210.2",
            ]
        ),
        encoding="utf-8",
    )

    result = indices.build_indices(
        yield_panel=yield_panel,
        climate_panel=tmp_path / "missing_climate.csv",
        remote_sensing_panel=tmp_path / "missing_remote.csv",
        processed_dir=processed_dir,
        reports_dir=reports_dir,
    )

    model_panel = processed_dir / "model_panel.csv"
    lines = model_panel.read_text(encoding="utf-8").splitlines()

    assert result.status == "partial"
    assert result.row_count == 2
    assert "Optional input missing" in result.report_path.read_text(encoding="utf-8")
    assert lines[0] == "admin_code,province,year,yield_kg_per_hectare"
    assert "320000,江苏,2024,6958.4" in lines


def test_build_indices_prefers_parquet_and_pivots_long_climate_over_stale_csv_fallback(tmp_path: Path) -> None:
    import pandas as pd

    processed_dir = tmp_path / "data" / "processed"
    interim_dir = tmp_path / "data" / "interim"
    processed_dir.mkdir(parents=True)
    interim_dir.mkdir(parents=True)
    yield_panel = processed_dir / "yield_panel.csv"
    yield_panel.write_text(
        "\n".join(
            [
                "admin_code,province,year,yield_kg_per_hectare",
                "320000,江苏,2022,6000",
            ]
        ),
        encoding="utf-8",
    )
    (interim_dir / "climate_growing_season.csv").write_text(
        "source_file,aggregation,variable,time,year,month,value\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "source_file": "era5.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_mean_temperature",
                "time": "",
                "year": 2022,
                "month": "",
                "value": 26.5,
            },
            {
                "source_file": "chirps.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_precipitation_sum",
                "time": "",
                "year": 2022,
                "month": "",
                "value": 450.0,
            },
            {
                "source_file": "era5_second.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_mean_temperature",
                "time": "",
                "year": 2022,
                "month": "",
                "value": 27.5,
            },
        ]
    ).to_parquet(interim_dir / "climate_growing_season.parquet", index=False)

    result = indices.build_indices(
        yield_panel=yield_panel,
        interim_dir=interim_dir,
        processed_dir=processed_dir,
        reports_dir=tmp_path / "reports",
    )

    model_text = (processed_dir / "model_panel.csv").read_text(encoding="utf-8")

    assert result.row_count == 1
    report_text = result.report_path.read_text(encoding="utf-8")
    assert "duplicate join keys" not in report_text
    assert "climate_growing_season_mean_temperature" in model_text
    assert "climate_growing_season_precipitation_sum" in model_text
    assert "27.0" in model_text


def test_build_indices_adds_trend_anomaly_and_exposure_index(tmp_path: Path) -> None:
    import csv
    import pandas as pd

    processed_dir = tmp_path / "data" / "processed"
    interim_dir = tmp_path / "data" / "interim"
    processed_dir.mkdir(parents=True)
    interim_dir.mkdir(parents=True)
    yield_panel = processed_dir / "yield_panel.csv"
    yield_panel.write_text(
        "\n".join(
            [
                "province,crop,year,yield_kg_per_hectare",
                "Alpha,rice,2000,100",
                "Alpha,rice,2001,110",
                "Alpha,rice,2002,90",
                "Beta,rice,2000,200",
                "Beta,rice,2001,220",
                "Beta,rice,2002,180",
            ]
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "source_file": "heat.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_mean_temperature",
                "time": "",
                "year": 2002,
                "month": "",
                "province": "Alpha",
                "value": 32.0,
            },
            {
                "source_file": "rain.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_precipitation_sum",
                "time": "",
                "year": 2002,
                "month": "",
                "province": "Alpha",
                "value": 100.0,
            },
            {
                "source_file": "heat.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_mean_temperature",
                "time": "",
                "year": 2002,
                "month": "",
                "province": "Beta",
                "value": 28.0,
            },
            {
                "source_file": "rain.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_precipitation_sum",
                "time": "",
                "year": 2002,
                "month": "",
                "province": "Beta",
                "value": 300.0,
            },
        ]
    ).to_parquet(interim_dir / "climate_growing_season.parquet", index=False)

    result = indices.build_indices(
        yield_panel=yield_panel,
        interim_dir=interim_dir,
        processed_dir=processed_dir,
        reports_dir=tmp_path / "reports",
        baseline_years=(2000, 2001),
        min_valid_observations=2,
    )
    rows = list(csv.DictReader((processed_dir / "model_panel.csv").open("r", encoding="utf-8")))
    alpha_2002 = next(row for row in rows if row["province"] == "Alpha" and row["year"] == "2002")
    beta_2002 = next(row for row in rows if row["province"] == "Beta" and row["year"] == "2002")

    assert result.status == "partial"
    assert float(alpha_2002["trend_yield"]) == 120.0
    assert float(alpha_2002["yield_anomaly_abs"]) == -30.0
    assert float(alpha_2002["yield_anomaly_pct"]) == -25.0
    assert alpha_2002["exposure_index"]
    assert float(alpha_2002["exposure_index"]) > float(beta_2002["exposure_index"])
