from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from src.models import fit_two_way_fixed_effects, run_modeling


def test_fit_two_way_fixed_effects_estimates_exposure_with_admin_and_year_fe() -> None:
    rows = _synthetic_province_panel()

    result = fit_two_way_fixed_effects(
        rows,
        outcome_field="yield_anomaly_pct",
        exposure_field="chd_annual",
        admin_field="province",
        year_field="year",
    )

    assert result["model_name"] == "province_two_way_fixed_effects"
    assert result["n"] == 100
    assert result["coefficients"]["chd_annual"] == result["coefficient"]
    assert abs(result["coefficient"] - 1.25) < 1e-6
    assert result["standard_error"] is not None
    assert result["p_value"] is not None
    assert result["conf_int_low"] is not None
    assert result["conf_int_high"] is not None
    assert result["r2"] > 0.99


def test_run_modeling_writes_province_fixed_effects_separately(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    pd.DataFrame(_synthetic_province_panel()).to_csv(processed / "province_model_panel.csv", index=False)

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

    coefficients = _read_csv(outputs / "model_coefficients.csv")
    report_text = result.report_path.read_text(encoding="utf-8")
    scope_text = (reports / "model_scope_decision.md").read_text(encoding="utf-8")

    assert result.status == "ok"
    assert "province_fixed_effects_and_event_study_candidate" in scope_text
    assert "descriptive_correlation_only" not in scope_text
    assert "descriptive_ols" in {row["model"] for row in coefficients}
    assert "province_two_way_fixed_effects" in {row["model"] for row in coefficients}
    assert "## Descriptive OLS" in report_text
    assert "## Province Two-Way Fixed Effects" in report_text


def test_run_modeling_uses_quasi_causal_evidence_only_after_all_claim_gates_pass(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    outputs = tmp_path / "data" / "outputs"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    pd.DataFrame(_synthetic_province_panel()).to_csv(processed / "province_model_panel.csv", index=False)
    pd.DataFrame(
        [
            {"gate": "yield_panel_coverage_gte_0_75", "passed": "true"},
            {"gate": "pretrend_test_passed", "passed": "true"},
            {"gate": "placebo_tests_passed", "passed": "true"},
            {"gate": "robustness_direction_stable", "passed": "true"},
            {"gate": "admin_crosswalk_match_rate_gte_0_90", "passed": "true"},
            {"gate": "mechanism_remote_sensing_consistent", "passed": "true"},
        ]
    ).to_csv(processed / "causal_claim_gate_status.csv", index=False)

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

    assert result.status == "ok"
    assert "Conclusion strength: `quasi_causal_evidence`" in scope_text


def _synthetic_province_panel() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    provinces = [("Alpha", "110000"), ("Beta", "120000"), ("Gamma", "130000"), ("Delta", "140000")]
    for province_index, (province, code) in enumerate(provinces):
        for year in range(2000, 2025):
            exposure = ((year - 2000) * (province_index + 2)) % 7 + province_index * 0.1
            outcome = 1.25 * exposure + province_index * 2.0 + (year - 2000) * 0.35
            rows.append(
                {
                    "province": province,
                    "province_code": code,
                    "year": year,
                    "outcome_type": "province_grain_yield_anomaly",
                    "province_grain_yield_anomaly": outcome,
                    "yield_anomaly_pct": outcome,
                    "chd_annual": exposure,
                    "chd_2022_intensity": 20.0 + province_index if year == 2022 else "",
                }
            )
    return rows


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))
