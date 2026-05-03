from pathlib import Path

import pandas as pd

from scripts.generate_report_core import generate_final_report


def test_generate_final_report_handles_missing_outputs(tmp_path: Path) -> None:
    report_path = generate_final_report(
        processed_dir=tmp_path / "processed",
        output_dir=tmp_path / "outputs",
        reports_dir=tmp_path / "reports",
        main_event_year=2022,
    )

    assert report_path.exists()
    content = report_path.read_text(encoding="utf-8")
    assert "2022 年 CHD 暴露概况" in content
    assert "主要数据缺口" in content


def test_generate_final_report_summarizes_outputs_and_risk_register(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    interim = tmp_path / "interim"
    outputs = tmp_path / "outputs"
    reports = tmp_path / "reports"
    processed.mkdir()
    interim.mkdir()
    (outputs / "figures").mkdir(parents=True)
    (outputs / "figures" / "study_area_map.png").write_text("png", encoding="utf-8")
    pd.DataFrame([{"path": "a"}, {"path": "b"}]).to_csv(processed / "data_inventory.csv", index=False)
    pd.DataFrame([{"admin_id": "a"}, {"admin_id": "b"}]).to_parquet(processed / "admin_units.parquet", index=False)
    pd.DataFrame(
        [
            {"admin_id": "a", "status": "zonal_stats", "crop_area_ha": 10},
            {"admin_id": "b", "status": "no_overlap", "crop_area_ha": 0},
        ]
    ).to_csv(processed / "crop_mask_summary_by_admin.csv", index=False)
    pd.DataFrame(
        [
            {"admin_id": "a", "status": "zonal_stats"},
            {"admin_id": "b", "status": "default"},
        ]
    ).to_csv(processed / "phenology_by_admin.csv", index=False)
    pd.DataFrame(
        [
            {"province": "安徽", "year": 2022, "variable": "growing_season_mean_temperature", "value": 30.0},
            {"province": "江苏", "year": 2022, "variable": "growing_season_precipitation_sum", "value": 100.0},
        ]
    ).to_parquet(interim / "climate_province_growing_season.parquet", index=False)
    pd.DataFrame(
        [
            {"province": "安徽", "year": 2022, "variable": "et", "value": 400.0},
        ]
    ).to_parquet(interim / "remote_sensing_province_growing_season.parquet", index=False)
    pd.DataFrame(
        [
            {"province": "安徽", "year": 2022, "admin_level": "province", "crop": "grain"},
            {"province": "安徽", "year": 2023, "admin_level": "province", "crop": "grain"},
        ]
    ).to_csv(processed / "yield_panel_combined.csv", index=False)
    pd.DataFrame(
        [
            {
                "province": "安徽",
                "year": 2022,
                "yield_anomaly_pct": -10.0,
                "exposure_index": 2.0,
            },
            {
                "province": "江苏",
                "year": 2022,
                "yield_anomaly_pct": 5.0,
                "exposure_index": -1.0,
            },
        ]
    ).to_csv(processed / "model_panel.csv", index=False)
    pd.DataFrame(
        [{"model": "descriptive_ols", "term": "exposure_index", "estimate": -4.0, "n": 2, "r2": 0.5}]
    ).to_csv(outputs / "model_coefficients.csv", index=False)
    pd.DataFrame(
        [{"model": "event_study", "term": "event_time_0", "estimate": -2.0, "n": 2, "r2": 0.4}]
    ).to_csv(outputs / "event_study_coefficients.csv", index=False)
    references = tmp_path / "raw" / "references"
    references.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "source_id": "china_county_statistical_yearbook",
                "category": "yield_panel",
                "access_level": "manual_or_paid",
                "status": "candidate",
                "priority": "high",
            }
        ]
    ).to_csv(references / "deep_required_data_sources.csv", index=False)
    yield_proxy = processed / "yield_proxy"
    yield_proxy.mkdir()
    pd.DataFrame(
        [
            {
                "admin_id": "a",
                "year": 2020,
                "source": "ggcp10",
                "raw_proxy_yield": 5000,
                "calibrated_yield": 5100,
                "calibration_status": "calibrated",
            }
        ]
    ).to_csv(yield_proxy / "county_yield_proxy_panel.csv", index=False)
    pd.DataFrame(
        [
            {"admin_id": "a", "year": 2020, "status": "available"},
            {"admin_id": "b", "year": 2020, "status": "missing"},
        ]
    ).to_csv(yield_proxy / "yield_proxy_gap_report.csv", index=False)

    report_path = generate_final_report(
        processed_dir=processed,
        output_dir=outputs,
        reports_dir=reports,
        main_event_year=2022,
    )

    content = report_path.read_text(encoding="utf-8")
    risk_report = reports / "project_risk_assessment.md"

    assert risk_report.exists()
    assert "稻田掩膜真实叠加：1/2 个行政单元" in content
    assert "省级气象暴露面板：2 行" in content
    assert "yield_anomaly_pct 非空：2/2 行" in content
    assert "exposure_index 非空：2/2 行" in content
    assert "风险状态与处理结果" in content
    assert "后续路线图" in content
    assert "深度数据源检索" in content
    assert "县级单产代理面板" in content
    assert "代理面板行数：1" in content
    assert "县/市级 2000-2024 官方水稻单产面板" in risk_report.read_text(encoding="utf-8")


def test_generate_final_report_uses_current_chd_coverage_for_claim_strength(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    outputs = tmp_path / "outputs"
    reports = tmp_path / "reports"
    processed.mkdir()
    outputs.mkdir()
    rows = []
    for province in ["Alpha", "Beta"]:
        for year in [2021, 2022]:
            rows.append(
                {
                    "province": province,
                    "year": year,
                    "crop": "grain",
                    "outcome_type": "province_grain_yield_anomaly",
                    "yield_anomaly_pct": 1.0,
                    "province_grain_yield_anomaly": 1.0,
                    "chd_annual": 2.0,
                }
            )
    pd.DataFrame(rows).to_csv(processed / "province_model_panel.csv", index=False)
    pd.DataFrame(
        [
            {"province": province, "year": year, "chd_annual": 2.0}
            for province in ["Alpha", "Beta"]
            for year in [2021, 2022]
        ]
    ).to_csv(processed / "province_chd_panel.csv", index=False)

    report_path = generate_final_report(
        processed_dir=processed,
        output_dir=outputs,
        reports_dir=reports,
        main_event_year=2022,
        main_year_min=2021,
        main_year_max=2022,
    )

    content = report_path.read_text(encoding="utf-8")
    risk_content = (reports / "project_risk_assessment.md").read_text(encoding="utf-8")

    assert "`impact_assessment`" in content
    assert "`ok_for_province_fixed_effects`" in content
    assert "chd_annual" in content
    assert "当前 CHD 暴露为省域平均暴露，不是稻田加权暴露。" in content
    assert "2008-2010 省级粮食回填仍缺，但主模型覆盖率已超过门控阈值。" in content
    assert "exposure_index 非空 0/4" not in risk_content
