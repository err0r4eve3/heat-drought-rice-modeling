from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from scripts.generate_descriptive_evidence_audit import (
    build_descriptive_evidence_audit,
    render_descriptive_evidence_report,
)


def test_descriptive_evidence_audit_skips_cleanly_without_artifacts(tmp_path: Path) -> None:
    interim = tmp_path / "data" / "interim"
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    interim.mkdir(parents=True)
    processed.mkdir(parents=True)
    outputs.mkdir(parents=True)

    audit = build_descriptive_evidence_audit(interim, processed, outputs)
    report = render_descriptive_evidence_report(audit)

    assert audit["sample_flow"]["status"] == "skipped"
    assert audit["proxy_official_consistency"]["status"] == "skipped"
    assert audit["consistency_2024"]["status"] == "skipped"
    assert "no quasi-causal" in report
    assert "subprovince official yield-loss" in report


def test_descriptive_evidence_audit_computes_sample_flow_and_2024(tmp_path: Path) -> None:
    interim, processed, outputs = _make_dirs(tmp_path)
    pd.DataFrame(
        [
            {"province": "Alpha", "year": 2022, "yield_anomaly_pct": 1.0, "chd_annual": 2.0},
            {"province": "Alpha", "year": 2024, "yield_anomaly_pct": None, "chd_annual": 3.0},
            {"province": "Beta", "year": 2024, "yield_anomaly_pct": -1.0, "chd_annual": None},
        ]
    ).to_csv(processed / "province_model_panel.csv", index=False)
    pd.DataFrame(
        [
            {"province": "Alpha", "year": 2024, "chd_annual": 3.0},
            {"province": "Beta", "year": 2024, "chd_annual": 1.0},
        ]
    ).to_csv(processed / "annual_exposure_panel.csv", index=False)
    pd.DataFrame([{"admin_id": "a", "province": "Alpha"}, {"admin_id": "b", "province": None}]).to_csv(
        processed / "admin_units_with_province.csv", index=False
    )

    audit = build_descriptive_evidence_audit(interim, processed, outputs)

    assert audit["sample_flow"]["status"] == "present"
    assert audit["sample_flow"]["model_panel_rows"] == 3
    assert audit["sample_flow"]["yield_anomaly_nonmissing"] == 2
    assert audit["sample_flow"]["exposure_nonmissing"] == 2
    assert audit["sample_flow"]["complete_model_rows"] == 1
    assert audit["sample_flow"]["admin_province_matched"] == 1
    assert audit["consistency_2024"]["status"] == "present"
    assert audit["consistency_2024"]["model_2024_rows"] == 2
    assert audit["consistency_2024"]["does_not_validate_causal_effects"] is True


def test_descriptive_evidence_audit_computes_proxy_consistency_when_artifacts_exist(tmp_path: Path) -> None:
    interim, processed, outputs = _make_dirs(tmp_path)
    (processed / "yield_proxy").mkdir()
    pd.DataFrame(
        [
            {"province": "Alpha", "year": 2022, "proxy_yield": 95.0},
            {"province": "Alpha", "year": 2022, "proxy_yield": 105.0},
            {"province": "Beta", "year": 2022, "proxy_yield": 80.0},
        ]
    ).to_csv(processed / "yield_proxy" / "county_yield_proxy_panel.csv", index=False)
    pd.DataFrame(
        [
            {"province": "Alpha", "year": 2022, "official_yield": 100.0},
            {"province": "Beta", "year": 2022, "official_yield": 100.0},
        ]
    ).to_csv(processed / "province_model_panel.csv", index=False)

    audit = build_descriptive_evidence_audit(interim, processed, outputs)

    consistency = audit["proxy_official_consistency"]
    assert consistency["status"] == "present"
    assert consistency["matched_province_year_rows"] == 2
    assert consistency["warning_rows_abs_bias_gt_5pct"] == 1
    assert consistency["does_not_correct_official_statistics"] is True


def test_descriptive_evidence_audit_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    interim, processed, outputs = _make_dirs(tmp_path)
    report = tmp_path / "reports" / "descriptive_evidence_audit.md"
    output = tmp_path / "outputs" / "descriptive_evidence_audit.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/generate_descriptive_evidence_audit.py",
            "--interim-dir",
            str(interim),
            "--processed-dir",
            str(processed),
            "--outputs-dir",
            str(outputs),
            "--output-json",
            str(output),
            "--report",
            str(report),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["evidence_type"] == "descriptive"
    assert "2024 Descriptive External Consistency" in report.read_text(encoding="utf-8")


def _make_dirs(tmp_path: Path) -> tuple[Path, Path, Path]:
    interim = tmp_path / "data" / "interim"
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    interim.mkdir(parents=True)
    processed.mkdir(parents=True)
    outputs.mkdir(parents=True)
    return interim, processed, outputs
