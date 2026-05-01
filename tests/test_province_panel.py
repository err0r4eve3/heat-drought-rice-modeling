from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.province_panel import build_province_model_panel


def test_build_province_model_panel_prefers_rice_and_excludes_2025(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {"province": "江苏省", "province_code": "320000", "year": 2020, "crop": "rice", "yield_kg_per_hectare": 6000},
            {"province": "江苏省", "province_code": "320000", "year": 2021, "crop": "rice", "yield_kg_per_hectare": 6200},
            {"province": "江苏省", "province_code": "320000", "year": 2022, "crop": "rice", "yield_kg_per_hectare": 5600},
            {"province": "浙江省", "province_code": "330000", "year": 2020, "crop": "rice", "yield_kg_per_hectare": 5000},
            {"province": "浙江省", "province_code": "330000", "year": 2021, "crop": "rice", "yield_kg_per_hectare": 5200},
            {"province": "浙江省", "province_code": "330000", "year": 2022, "crop": "rice", "yield_kg_per_hectare": 4700},
            {"province": "浙江省", "province_code": "330000", "year": 2025, "crop": "rice", "yield_kg_per_hectare": 9999},
        ]
    ).to_csv(processed / "yield_panel_combined.csv", index=False)
    pd.DataFrame(
        [
            {"province": "江苏省", "province_code": "320000", "year": 2020, "chd_annual": 0.1},
            {"province": "江苏省", "province_code": "320000", "year": 2021, "chd_annual": 0.2},
            {"province": "江苏省", "province_code": "320000", "year": 2022, "chd_annual": 1.5, "chd_2022_intensity": 1.5, "chd_2022_treated_p75": 1},
            {"province": "浙江省", "province_code": "330000", "year": 2022, "chd_annual": 0.4, "chd_2022_intensity": 0.4, "chd_2022_treated_p75": 0},
        ]
    ).to_csv(processed / "province_chd_panel.csv", index=False)

    result = build_province_model_panel(
        processed_dir=processed,
        reports_dir=reports,
        main_year_min=2020,
        main_year_max=2024,
        baseline_years=(2020, 2021),
        min_valid_observations=2,
    )

    panel = pd.read_csv(processed / "province_model_panel.csv")
    report_text = (reports / "province_panel_summary.md").read_text(encoding="utf-8")

    assert result.status == "ok"
    assert set(panel["year"]) == {2020, 2021, 2022}
    assert result.outcome_type == "province_rice_yield_anomaly"
    assert "province_rice_yield_anomaly" in panel.columns
    assert "province_grain_yield_anomaly" in panel.columns
    assert panel.loc[panel["year"].eq(2022), "chd_2022_intensity"].notna().any()
    assert "Current outcome type: province_rice_yield_anomaly" in report_text


def test_build_province_model_panel_falls_back_to_grain_and_keeps_empty_chd_fields(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {"province": "安徽省", "province_code": "340000", "year": 2020, "crop": "grain", "yield_kg_per_hectare": 5000},
            {"province": "安徽省", "province_code": "340000", "year": 2021, "crop": "grain", "yield_kg_per_hectare": 5200},
            {"province": "安徽省", "province_code": "340000", "year": 2022, "crop": "grain", "yield_kg_per_hectare": 4800},
        ]
    ).to_csv(processed / "yield_panel.csv", index=False)

    result = build_province_model_panel(
        processed_dir=processed,
        reports_dir=reports,
        main_year_min=2020,
        main_year_max=2024,
        baseline_years=(2020, 2021),
        min_valid_observations=2,
    )

    panel = pd.read_csv(processed / "province_model_panel.csv")
    report_text = (reports / "province_panel_summary.md").read_text(encoding="utf-8")

    assert result.status == "ok"
    assert result.outcome_type == "province_grain_yield_anomaly"
    assert "chd_annual" in panel.columns
    assert panel["chd_annual"].isna().all()
    assert "Province CHD panel not found; created empty CHD fields" in report_text
