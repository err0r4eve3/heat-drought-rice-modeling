from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from scripts.summarize_generated_artifacts import summarize_artifacts


def test_summarize_generated_artifacts_reports_missing_without_crashing(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    processed.mkdir(parents=True)
    outputs.mkdir(parents=True)

    summary = summarize_artifacts(processed, outputs)

    assert summary["artifacts"]["province_model_panel"]["status"] == "missing"
    assert "cannot verify generated claim" in summary["artifacts"]["province_model_panel"]["message"]


def test_summarize_generated_artifacts_calculates_coverage(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    processed.mkdir(parents=True)
    outputs.mkdir(parents=True)
    pd.DataFrame(
        [
            {"province": "Alpha", "year": 2021, "yield_anomaly_pct": 1.0, "chd_annual": 2.0},
            {"province": "Alpha", "year": 2022, "yield_anomaly_pct": None, "chd_annual": 3.0},
            {"province": "Beta", "year": 2022, "yield_anomaly_pct": -1.0, "chd_annual": None},
        ]
    ).to_csv(processed / "province_model_panel.csv", index=False)

    summary = summarize_artifacts(processed, outputs)
    panel = summary["artifacts"]["province_model_panel"]

    assert panel["rows"] == 3
    assert panel["year_min"] == 2021
    assert panel["year_max"] == 2022
    assert panel["province_count"] == 2
    assert panel["coverage"]["yield_anomaly_pct"]["rate"] == 2 / 3
    assert panel["coverage"]["chd_annual"]["rate"] == 2 / 3


def test_summarize_generated_artifacts_cli_writes_json(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    output = tmp_path / "artifact_audit.json"
    processed.mkdir(parents=True)
    outputs.mkdir(parents=True)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/summarize_generated_artifacts.py",
            "--processed-dir",
            str(processed),
            "--outputs-dir",
            str(outputs),
            "--output",
            str(output),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["artifacts"]["annual_exposure_panel"]["status"] == "missing"
