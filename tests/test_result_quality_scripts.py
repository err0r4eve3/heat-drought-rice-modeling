from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_freeze_data_version_records_hashes(tmp_path: Path) -> None:
    _write_config(tmp_path)
    _write_minimal_project_outputs(tmp_path)

    completed = _run_script("scripts/23_freeze_data_version.py", tmp_path)

    assert completed.returncode == 0, completed.stderr
    hashes = pd.read_csv(tmp_path / "data" / "outputs" / "data_file_hashes.csv")
    report = (tmp_path / "reports" / "data_version_freeze.md").read_text(encoding="utf-8")

    assert {"path", "exists", "rows", "columns", "key_variable_coverage", "sha256"}.issubset(hashes.columns)
    assert hashes["sha256"].dropna().astype(str).str.len().ge(64).any()
    assert "province_model_panel.csv" in report


def test_audit_model_results_reports_fixed_effects_and_pretrend(tmp_path: Path) -> None:
    _write_config(tmp_path)
    _write_minimal_project_outputs(tmp_path)
    pd.DataFrame(
        [
            {"model": "descriptive_ols", "term": "chd_annual", "estimate": -0.1, "n": 4, "r2": 0.1},
            {
                "model": "province_two_way_fixed_effects",
                "term": "chd_annual",
                "estimate": -0.2,
                "standard_error": 0.1,
                "p_value": 0.08,
                "n": 4,
                "r2": 0.7,
                "adjusted_r2": 0.6,
            },
        ]
    ).to_csv(tmp_path / "data" / "outputs" / "model_coefficients.csv", index=False)
    pd.DataFrame(
        [
            {"model": "event_study_candidate", "term": "event_time_m2", "estimate": 0.02, "standard_error": 1.0, "p_value": 0.9, "n": 4},
            {"model": "event_study_candidate", "term": "event_time_0", "estimate": -0.5, "standard_error": 1.0, "p_value": 0.6, "n": 4},
        ]
    ).to_csv(tmp_path / "data" / "outputs" / "event_study_coefficients.csv", index=False)

    completed = _run_script("scripts/24_audit_model_results.py", tmp_path)

    assert completed.returncode == 0, completed.stderr
    audit = pd.read_csv(tmp_path / "data" / "outputs" / "model_result_audit.csv")
    report = (tmp_path / "reports" / "model_result_audit.md").read_text(encoding="utf-8")

    assert audit.loc[audit["check"].eq("has_province_two_way_fixed_effects"), "status"].iloc[0] == "passed"
    assert "FE implementation audit: passed" in report
    assert "pretrend_test_passed: True" in report


def test_robustness_suite_outputs_required_specs(tmp_path: Path) -> None:
    _write_config(tmp_path)
    _write_synthetic_panel(tmp_path / "data" / "processed" / "province_model_panel.csv")
    _write_synthetic_daily_climate(tmp_path / "data" / "interim" / "province_daily_climate_2000_2024.csv")

    completed = _run_script("scripts/25_run_robustness_suite.py", tmp_path)

    assert completed.returncode == 0, completed.stderr
    robustness = pd.read_csv(tmp_path / "data" / "outputs" / "robustness_results.csv")
    report = (tmp_path / "reports" / "robustness_summary.md").read_text(encoding="utf-8")

    assert {
        "heat_p90_drought_p10",
        "heat_p95_drought_p10",
        "growth_months_7_9",
        "exclude_backfill",
        "highlighted_region_only",
        "national_control",
    }.issubset(set(robustness["spec_id"]))
    assert {"chd_coefficient", "direction", "model_scope", "claim_strength_allowed"}.issubset(robustness.columns)
    assert "direction-consistent specs" in report


def test_paper_results_summary_uses_grain_title_and_limits_claims(tmp_path: Path) -> None:
    _write_config(tmp_path)
    _write_minimal_project_outputs(tmp_path)
    pd.DataFrame(
        [
            {
                "spec_id": "national_control",
                "sample_size": 4,
                "chd_coefficient": -0.2,
                "p_value": 0.08,
                "direction": "negative",
                "model_scope": "province_fixed_effects_and_event_study_candidate",
                "claim_strength_allowed": "impact_assessment",
            }
        ]
    ).to_csv(tmp_path / "data" / "outputs" / "robustness_results.csv", index=False)
    (tmp_path / "reports" / "model_result_audit.md").write_text(
        "\n".join(
            [
                "# Model Result Audit",
                "- FE implementation audit: passed",
                "- pretrend_test_passed: True",
                "- chd_annual coefficient: -0.2",
                "- chd_annual p_value: 0.08",
            ]
        ),
        encoding="utf-8",
    )

    completed = _run_script("scripts/26_generate_paper_results_summary.py", tmp_path)

    assert completed.returncode == 0, completed.stderr
    report = (tmp_path / "reports" / "paper_results_summary.md").read_text(encoding="utf-8")

    assert "粮食单产异常" in report
    assert "稻谷单产异常与稳定性" not in report
    assert "当前 CHD 暴露为省域平均暴露，不是稻田加权暴露。" in report
    assert "2008-2010 省级粮食回填仍缺，但主模型覆盖率已超过门控阈值。" in report
    assert "最高结论强度：impact_assessment" in report


def _run_script(script: str, project_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, script, "--config", str(project_root / "config.yaml")],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )


def _write_config(project_root: Path) -> None:
    for directory in [
        project_root / "data" / "interim",
        project_root / "data" / "processed",
        project_root / "data" / "outputs",
        project_root / "reports",
    ]:
        directory.mkdir(parents=True, exist_ok=True)
    (project_root / "config.yaml").write_text(
        "\n".join(
            [
                f"project_root: {project_root.as_posix()}",
                "data_raw_dir: data/raw",
                "data_interim_dir: data/interim",
                "data_processed_dir: data/processed",
                "output_dir: data/outputs",
                "study_area_name: test",
                "study_bbox: [105, 24, 123, 35]",
                "target_admin_level: province",
                "study_region_policy:",
                "  default_region: highlighted",
                "  highlighted_region: highlighted",
                "  main_model_region: national_control",
                "  regions:",
                "    highlighted:",
                "      provinces: [Alpha, Beta]",
                "    national_control:",
                "      provinces: all",
                "crs_wgs84: EPSG:4326",
                "crs_equal_area: EPSG:6933",
                "baseline_years: [2000, 2021]",
                "main_event_year: 2022",
                "validation_event_year: 2024",
                "recovery_years: [2023, 2024, 2025]",
                "panel_policy:",
                "  main_content_years: [2000, 2024]",
                "main_event_months: [6, 7, 8, 9, 10]",
                "rice_growth_months: [6, 7, 8, 9]",
                "heat_threshold_quantile: 0.90",
                "drought_threshold_quantile: 0.10",
                "min_valid_observations: 3",
                "output_formats: [csv, parquet, markdown]",
            ]
        ),
        encoding="utf-8",
    )


def _write_minimal_project_outputs(project_root: Path) -> None:
    processed = project_root / "data" / "processed"
    outputs = project_root / "data" / "outputs"
    interim = project_root / "data" / "interim"
    daily = pd.DataFrame(
        [
            {"province": "Alpha", "province_code": "110000", "date": "2022-06-01", "year": 2022, "month": 6, "tmax_c": 35, "precipitation_mm": 0},
            {"province": "Beta", "province_code": "120000", "date": "2022-06-01", "year": 2022, "month": 6, "tmax_c": 30, "precipitation_mm": 5},
        ]
    )
    daily.to_csv(interim / "province_daily_climate_2000_2024.csv", index=False)
    pd.DataFrame(
        [
            {"province": "Alpha", "province_code": "110000", "year": 2022, "chd_annual": 2.0},
            {"province": "Beta", "province_code": "120000", "year": 2022, "chd_annual": 0.0},
        ]
    ).to_csv(processed / "annual_exposure_panel.csv", index=False)
    pd.DataFrame(
        [
            {"province": "Alpha", "province_code": "110000", "year": 2022, "chd_annual": 2.0, "chd_2022_intensity": 2.0, "chd_2022_treated_p75": 1},
            {"province": "Beta", "province_code": "120000", "year": 2022, "chd_annual": 0.0, "chd_2022_intensity": 0.0, "chd_2022_treated_p75": 0},
        ]
    ).to_csv(processed / "province_chd_panel.csv", index=False)
    pd.DataFrame(
        [
            {"province": "Alpha", "province_code": "110000", "year": 2021, "outcome_type": "province_grain_yield_anomaly", "crop": "grain", "yield_anomaly_pct": 1.0, "province_grain_yield_anomaly": 1.0, "chd_annual": 1.0, "chd_2022_treated_p75": 1, "is_backfill": False},
            {"province": "Alpha", "province_code": "110000", "year": 2022, "outcome_type": "province_grain_yield_anomaly", "crop": "grain", "yield_anomaly_pct": -2.0, "province_grain_yield_anomaly": -2.0, "chd_annual": 2.0, "chd_2022_treated_p75": 1, "is_backfill": False},
            {"province": "Beta", "province_code": "120000", "year": 2021, "outcome_type": "province_grain_yield_anomaly", "crop": "grain", "yield_anomaly_pct": 0.5, "province_grain_yield_anomaly": 0.5, "chd_annual": 0.0, "chd_2022_treated_p75": 0, "is_backfill": False},
            {"province": "Beta", "province_code": "120000", "year": 2022, "outcome_type": "province_grain_yield_anomaly", "crop": "grain", "yield_anomaly_pct": 0.2, "province_grain_yield_anomaly": 0.2, "chd_annual": 0.0, "chd_2022_treated_p75": 0, "is_backfill": True},
        ]
    ).to_csv(processed / "province_model_panel.csv", index=False)
    pd.DataFrame(
        [{"province": "Alpha", "province_code": "110000", "year": 2011, "yield_kg_per_hectare": 5000.0}]
    ).to_csv(processed / "province_grain_backfill_2008_2015_cleaned.csv", index=False)
    pd.DataFrame(
        [{"model": "province_two_way_fixed_effects", "term": "chd_annual", "estimate": -0.2, "standard_error": 0.1, "p_value": 0.08, "n": 4, "r2": 0.7}]
    ).to_csv(outputs / "model_coefficients.csv", index=False)
    pd.DataFrame(
        [{"model": "event_study_candidate", "term": "event_time_0", "estimate": -0.5, "standard_error": 1.0, "p_value": 0.6, "n": 4}]
    ).to_csv(outputs / "event_study_coefficients.csv", index=False)


def _write_synthetic_panel(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for province_index, province in enumerate(["Alpha", "Beta", "Gamma", "Delta"]):
        for year in range(2000, 2025):
            exposure = float((year - 2000) % 5 + province_index)
            rows.append(
                {
                    "province": province,
                    "province_code": str(110000 + province_index * 10000),
                    "year": year,
                    "outcome_type": "province_grain_yield_anomaly",
                    "crop": "grain",
                    "yield_anomaly_pct": -0.2 * exposure + province_index,
                    "province_grain_yield_anomaly": -0.2 * exposure + province_index,
                    "chd_annual": exposure,
                    "chd_2022_intensity": exposure if year == 2022 else "",
                    "chd_2022_treated_p75": int(province_index >= 2),
                    "is_backfill": year in {2011, 2012},
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_synthetic_daily_climate(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for province_index, province in enumerate(["Alpha", "Beta", "Gamma", "Delta"]):
        for year in range(2000, 2025):
            for month in [6, 7, 8, 9]:
                for day in range(1, 31):
                    rows.append(
                        {
                            "province": province,
                            "province_code": str(110000 + province_index * 10000),
                            "date": f"{year}-{month:02d}-{day:02d}",
                            "year": year,
                            "month": month,
                            "tmax_c": 28 + province_index + (day % 7),
                            "precipitation_mm": float((day + month + province_index) % 12),
                        }
                    )
    pd.DataFrame(rows).to_csv(path, index=False)
