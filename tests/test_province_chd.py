from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.province_chd import build_province_chd_panel


def test_build_province_chd_panel_aggregates_annual_exposure_to_province(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "province": "江苏省",
                "province_code": "320000",
                "county": "A",
                "year": 2021,
                "chd_annual": 1.0,
                "tmax_anomaly": 0.2,
                "rice_area_ha": 10,
            },
            {
                "province": "江苏省",
                "province_code": "320000",
                "county": "B",
                "year": 2021,
                "chd_annual": 3.0,
                "tmax_anomaly": 0.4,
                "rice_area_ha": 30,
            },
            {
                "province": "江苏省",
                "province_code": "320000",
                "county": "A",
                "year": 2022,
                "chd_annual": 8.0,
                "tmax_anomaly": 1.0,
                "rice_area_ha": 10,
            },
            {
                "province": "浙江省",
                "province_code": "330000",
                "county": "C",
                "year": 2022,
                "chd_annual": 2.0,
                "tmax_anomaly": 0.5,
                "rice_area_ha": 20,
            },
        ]
    ).to_parquet(processed / "annual_exposure_panel.parquet", index=False)

    result = build_province_chd_panel(
        processed_dir=processed,
        interim_dir=tmp_path / "data" / "interim",
        reports_dir=reports,
        main_year_min=2021,
        main_year_max=2022,
        highlighted_provinces=["江苏省"],
    )

    panel = pd.read_csv(processed / "province_chd_panel.csv")
    jiangsu_2021 = panel[(panel["province"] == "江苏省") & (panel["year"] == 2021)].iloc[0]
    jiangsu_2022 = panel[(panel["province"] == "江苏省") & (panel["year"] == 2022)].iloc[0]

    assert result.row_count == 3
    assert abs(float(jiangsu_2021["chd_annual"]) - 2.5) < 1e-9
    assert float(jiangsu_2022["chd_2022_intensity"]) == 8.0
    assert int(jiangsu_2022["post_2022"]) == 1
    assert "Province CHD Panel Summary" in (reports / "province_chd_panel_summary.md").read_text(encoding="utf-8")
    assert "Highlighted-region coverage rate" in (reports / "province_chd_panel_summary.md").read_text(encoding="utf-8")


def test_build_province_chd_panel_writes_empty_outputs_without_sources(tmp_path: Path) -> None:
    result = build_province_chd_panel(
        processed_dir=tmp_path / "data" / "processed",
        interim_dir=tmp_path / "data" / "interim",
        reports_dir=tmp_path / "reports",
    )

    panel = pd.read_csv(tmp_path / "data" / "processed" / "province_chd_panel.csv")
    report_text = (tmp_path / "reports" / "province_chd_panel_summary.md").read_text(encoding="utf-8")

    assert result.status == "empty"
    assert panel.empty
    assert "No annual exposure or climate source table was found" in report_text
