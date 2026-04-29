from pathlib import Path

import pandas as pd

from src.risk_register import generate_risk_action_report, write_external_access_check


def test_generate_risk_action_report_writes_outputs(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    references = tmp_path / "references"
    reports = tmp_path / "reports"
    (processed / "yield_proxy").mkdir(parents=True)
    references.mkdir(parents=True)

    pd.DataFrame(
        [
            {"admin_id": "a1", "shapeName": "County A", "shapeGroup": "CHN", "shapeType": "ADM3", "province_name": "Zhejiang"},
            {"admin_id": "a2", "shapeName": "County B", "shapeGroup": "CHN", "shapeType": "ADM3", "province_name": ""},
        ]
    ).to_csv(processed / "admin_units_with_province.csv", index=False)
    pd.DataFrame(
        [
            {"admin_id": "a1", "status": "zonal_stats"},
            {"admin_id": "a2", "status": "no_overlap"},
        ]
    ).to_csv(processed / "crop_mask_summary_by_admin.csv", index=False)
    pd.DataFrame(
        [
            {"admin_id": "a1", "status": "zonal_stats"},
            {"admin_id": "a2", "status": "default"},
        ]
    ).to_csv(processed / "phenology_by_admin.csv", index=False)
    pd.DataFrame(
        [
            {"year": 2022, "yield_anomaly_pct": -0.1, "exposure_index": 1.2},
            {"year": 2023, "yield_anomaly_pct": 0.0, "exposure_index": None},
        ]
    ).to_csv(processed / "model_panel.csv", index=False)
    pd.DataFrame(
        [
            {
                "admin_id": "a1",
                "year": 2020,
                "source": "asia_rice_yield_4km",
                "calibration_status": "calibrated",
                "calibrated_yield": 7000.0,
            },
            {
                "admin_id": "a2",
                "year": 2020,
                "source": "ggcp10",
                "calibration_status": "missing_official_or_proxy",
                "calibrated_yield": None,
            },
        ]
    ).to_csv(processed / "yield_proxy" / "county_yield_proxy_panel.csv", index=False)
    pd.DataFrame(
        [
            {"admin_id": "a1", "year": 2020, "status": "available"},
            {"admin_id": "a2", "year": 2020, "status": "available"},
        ]
    ).to_csv(processed / "yield_proxy" / "yield_proxy_gap_report.csv", index=False)
    pd.DataFrame(
        [{"source_id": "s1", "category": "yield_panel", "status": "not_publicly_complete"}]
    ).to_csv(references / "deep_required_data_sources.csv", index=False)

    result = generate_risk_action_report(processed, reports, references)

    assert result.report_path.exists()
    assert result.risk_register_path.exists()
    assert result.coverage_summary_path.exists()
    assert result.unmatched_admin_path.exists()
    assert result.calibration_summary_path.exists()
    assert result.data_gap_report_path.exists()
    assert result.model_scope_decision_path.exists()
    assert result.data_source_decision_path.exists()
    assert result.yield_panel_feasibility_path.exists()
    assert result.admin_crosswalk_decision_path.exists()
    assert result.model_claim_scope_path.exists()
    assert result.external_access_status_path.exists()
    assert result.risk_count >= 9

    risks = pd.read_csv(result.risk_register_path)
    assert "R02_official_county_rice_panel_missing" in set(risks["risk_id"])
    unmatched = pd.read_csv(result.unmatched_admin_path)
    assert len(unmatched) == 1
    assert "实在不可避免" in result.report_path.read_text(encoding="utf-8")
    assert "Main model content years: 2000-2024" in result.data_source_decision_path.read_text(encoding="utf-8")
    assert "Default claim: impact assessment / association" in result.model_claim_scope_path.read_text(encoding="utf-8")


def test_write_external_access_check_does_not_read_secret_values(tmp_path: Path) -> None:
    result = write_external_access_check(
        {
            "cds": {"required_for": ["ERA5-Land"], "local_secret_file": str(tmp_path / ".cdsapirc")},
            "earthdata": {"required_for": ["MODIS"]},
            "cma": {"required_for": ["station_observation"]},
        },
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
    )

    rows = pd.read_csv(result["csv_path"])
    report = result["report_path"].read_text(encoding="utf-8")
    assert {"cds", "earthdata", "cma"}.issubset(set(rows["provider"]))
    assert "Secret files are checked only for existence" in report
