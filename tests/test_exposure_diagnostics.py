from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.exposure_diagnostics import diagnose_exposure_coverage


def test_diagnose_exposure_coverage_detects_event_year_only(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    interim = tmp_path / "data" / "interim"
    outputs = tmp_path / "data" / "outputs"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    interim.mkdir(parents=True)
    model = processed / "model_panel.csv"
    pd.DataFrame(
        [
            {"province": "江苏", "year": 2021, "yield_anomaly_pct": 1.0, "exposure_index": ""},
            {"province": "江苏", "year": 2022, "yield_anomaly_pct": -5.0, "exposure_index": 2.0},
            {"province": "浙江", "year": 2022, "yield_anomaly_pct": -2.0, "exposure_index": 1.0},
            {"province": "浙江", "year": 2023, "yield_anomaly_pct": 1.0, "exposure_index": ""},
        ]
    ).to_csv(model, index=False)

    result = diagnose_exposure_coverage(
        model_panel=model,
        processed_dir=processed,
        interim_dir=interim,
        output_dir=outputs,
        reports_dir=reports,
        main_event_year=2022,
        study_provinces=["江苏省", "浙江省"],
    )

    assert result.exposure_coverage_status == "ok_only_for_2022_cross_section"
    assert "only_2022_event_exposure" in result.likely_causes
    assert (outputs / "exposure_coverage_diagnosis.csv").exists()
    assert "exposure_coverage_status" in (reports / "exposure_coverage_diagnosis.md").read_text(encoding="utf-8")


def test_diagnose_exposure_coverage_handles_missing_panel(tmp_path: Path) -> None:
    result = diagnose_exposure_coverage(
        model_panel=tmp_path / "missing.csv",
        processed_dir=tmp_path / "processed",
        interim_dir=tmp_path / "interim",
        output_dir=tmp_path / "outputs",
        reports_dir=tmp_path / "reports",
    )

    assert result.status == "empty"
    assert result.exposure_coverage_status == "not_usable_until_fixed"
    assert result.report_path.exists()
