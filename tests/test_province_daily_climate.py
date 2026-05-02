from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.province_daily_climate import (
    build_chd_from_daily_climate,
    import_province_daily_climate,
    validate_province_daily_climate,
)


def test_validate_daily_climate_reports_missing_years_and_growth_month_gaps() -> None:
    frame = pd.DataFrame(
        [
            {
                "province": "Alpha",
                "province_code": "110000",
                "date": "2000-06-01",
                "year": 2000,
                "month": 6,
                "tmax_c": 30.0,
                "precipitation_mm": 2.0,
            },
            {
                "province": "Alpha",
                "province_code": "110000",
                "date": "2022-07-01",
                "year": 2022,
                "month": 7,
                "tmax_c": 303.0,
                "precipitation_mm": -1.0,
            },
        ]
    )

    cleaned, qc, warnings = validate_province_daily_climate(frame, year_min=2000, year_max=2024)

    assert not cleaned.empty
    assert set(qc["year"]) == {2000, 2022}
    assert any("Missing daily climate years" in warning for warning in warnings)
    assert any("tmax_c values do not look like Celsius" in warning for warning in warnings)
    assert any("precipitation_mm contains negative values" in warning for warning in warnings)
    assert qc["growth_season_complete"].eq(False).all()


def test_import_daily_climate_writes_qc_report_without_source(tmp_path: Path) -> None:
    result = import_province_daily_climate(
        interim_dir=tmp_path / "data" / "interim",
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
    )

    assert result.status == "missing"
    assert result.row_count == 0
    assert result.outputs["qc_csv"].exists()
    assert result.outputs["qc_report"].exists()
    assert "Province Daily Climate QC" in result.outputs["qc_report"].read_text(encoding="utf-8")


def test_build_chd_missing_source_does_not_overwrite_existing_annual_report(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    reports.mkdir(parents=True)
    existing_csv = processed / "annual_exposure_panel.csv"
    existing_report = reports / "annual_exposure_panel_summary.md"
    existing_csv.write_text("province,year,chd_annual\nAlpha,2022,1\n", encoding="utf-8")
    existing_report.write_text("existing annual report", encoding="utf-8")

    result = build_chd_from_daily_climate(
        interim_dir=tmp_path / "data" / "interim",
        processed_dir=processed,
        reports_dir=reports,
    )

    assert result.status == "missing"
    assert existing_csv.read_text(encoding="utf-8") == "province,year,chd_annual\nAlpha,2022,1\n"
    assert existing_report.read_text(encoding="utf-8") == "existing annual report"
    assert result.report_path.name == "province_daily_climate_chd_summary.md"


def test_build_chd_from_daily_climate_counts_rolling30_compound_days(tmp_path: Path) -> None:
    interim = tmp_path / "data" / "interim"
    processed = tmp_path / "data" / "processed"
    reports = tmp_path / "reports"
    interim.mkdir(parents=True)
    source = _daily_climate_rows()
    source.to_parquet(interim / "province_daily_climate_2000_2024.parquet", index=False)

    result = build_chd_from_daily_climate(
        interim_dir=interim,
        processed_dir=processed,
        reports_dir=reports,
        year_min=2000,
        year_max=2022,
        baseline_years=(2000, 2021),
        growth_months=[6, 7, 8, 9],
        event_year=2022,
    )

    panel = pd.read_csv(processed / "annual_exposure_panel.csv")
    event = panel[(panel["province"] == "Alpha") & (panel["year"] == 2022)].iloc[0]

    assert result.status == "ok"
    assert result.row_count == 23
    assert int(event["hot_days"]) == 122
    assert int(event["dry_days"]) > 80
    assert int(event["compound_hot_dry_days"]) == int(event["chd_annual"])
    assert float(event["chd_2022_intensity"]) == float(event["chd_annual"])
    assert int(event["post_2022"]) == 1
    assert (processed / "annual_exposure_panel.parquet").exists()
    assert "30-day rolling precipitation" in (reports / "annual_exposure_panel_summary.md").read_text(
        encoding="utf-8"
    )


def _daily_climate_rows() -> pd.DataFrame:
    rows = []
    for date in pd.date_range("2000-05-01", "2022-09-30", freq="D"):
        if date.month not in {5, 6, 7, 8, 9}:
            continue
        is_event_year = date.year == 2022
        rows.append(
            {
                "province": "Alpha",
                "province_code": "110000",
                "date": date.strftime("%Y-%m-%d"),
                "year": date.year,
                "month": date.month,
                "tmax_c": 36.0 if is_event_year and date.month in {6, 7, 8, 9} else 30.0,
                "precipitation_mm": 0.0 if is_event_year else 10.0,
            }
        )
    return pd.DataFrame(rows)
