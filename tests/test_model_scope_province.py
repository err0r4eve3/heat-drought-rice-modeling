from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_complete_province_panel_allows_fixed_effects_candidate(tmp_path: Path) -> None:
    from src.models import run_modeling

    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)

    rows = []
    provinces = [("Alpha", "110000"), ("Beta", "120000"), ("Gamma", "130000"), ("Delta", "140000")]
    for province_index, (province, code) in enumerate(provinces):
        for year in range(2000, 2025):
            chd = ((year - 2000) % 5) + province_index * 0.25
            outcome = 0.3 * chd + province_index + (year - 2000) * 0.05
            rows.append(
                {
                    "province": province,
                    "province_code": code,
                    "year": year,
                    "outcome_type": "province_grain_yield_anomaly",
                    "province_grain_yield_anomaly": outcome,
                    "yield_anomaly_pct": outcome,
                    "chd_annual": chd,
                    "chd_2022_intensity": 10 + province_index if year == 2022 else "",
                }
            )
    pd.DataFrame(rows).to_csv(processed / "province_model_panel.csv", index=False)

    result = run_modeling(
        model_panel=processed / "province_model_panel.csv",
        output_dir=outputs,
        reports_dir=reports,
        event_year=2022,
        processed_dir=processed,
        outcome_field="yield_anomaly",
        exposure_field="chd_annual",
        admin_field="province",
        event_window=1,
    )

    scope_text = (reports / "model_scope_decision.md").read_text(encoding="utf-8")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert "province_fixed_effects_and_event_study_candidate" in scope_text
    assert "descriptive_correlation_only" not in scope_text
    assert "Event study allowed: True" in scope_text
    assert "Conclusion strength: `impact_assessment`" in scope_text
    assert "province_fixed_effects_and_event_study_candidate" in report_text


def test_modeling_cli_defaults_to_province_model_panel(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {"province": "江苏省", "province_code": "320000", "year": 2020, "province_grain_yield_anomaly": 0.1, "chd_annual": 0.1},
            {"province": "浙江省", "province_code": "330000", "year": 2020, "province_grain_yield_anomaly": 0.2, "chd_annual": 0.2},
            {"province": "安徽省", "province_code": "340000", "year": 2020, "province_grain_yield_anomaly": 0.3, "chd_annual": 0.3},
            {"province": "湖北省", "province_code": "420000", "year": 2020, "province_grain_yield_anomaly": 0.4, "chd_annual": 0.4},
        ]
    ).to_parquet(processed / "province_model_panel.parquet", index=False)
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
            ]
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "scripts/08_modeling.py", "--config", str(config_path)],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert (outputs / "model_coefficients.csv").exists()
    report_text = (tmp_path / "reports" / "model_results.md").read_text(encoding="utf-8")
    assert "province_model_panel" in report_text
    assert "省级粮食单产异常" in report_text
