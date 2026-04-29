import csv
import subprocess
import sys
from pathlib import Path

import src.models as models


def test_fit_simple_ols_estimates_intercept_and_slope() -> None:
    assert hasattr(models, "fit_simple_ols")
    rows = [
        {"yield_anomaly": 1.0, "exposure_index": 0.0},
        {"yield_anomaly": 3.0, "exposure_index": 1.0},
        {"yield_anomaly": 5.0, "exposure_index": 2.0},
        {"yield_anomaly": 7.0, "exposure_index": 3.0},
    ]

    result = models.fit_simple_ols(rows, y="yield_anomaly", x_vars=["exposure_index"])

    assert result["n"] == 4
    assert abs(result["coefficients"]["intercept"] - 1.0) < 1e-9
    assert abs(result["coefficients"]["exposure_index"] - 2.0) < 1e-9
    assert abs(result["r2"] - 1.0) < 1e-9
    assert abs(result["adjusted_r2"] - 1.0) < 1e-9


def test_assign_treatment_uses_event_year_quantile_by_admin() -> None:
    assert hasattr(models, "assign_treatment")
    rows = [
        {"admin_id": "a", "year": 2021, "exposure_index": 4.0},
        {"admin_id": "a", "year": 2022, "exposure_index": 10.0},
        {"admin_id": "b", "year": 2022, "exposure_index": 20.0},
        {"admin_id": "c", "year": 2022, "exposure_index": 30.0},
        {"admin_id": "c", "year": 2023, "exposure_index": 6.0},
    ]

    assigned = models.assign_treatment(
        rows,
        exposure_field="exposure_index",
        year_field="year",
        admin_field="admin_id",
        event_year=2022,
        quantile=0.5,
    )

    by_key = {(row["admin_id"], row["year"]): row["treatment"] for row in assigned}
    assert by_key[("a", 2021)] == 0
    assert by_key[("a", 2022)] == 0
    assert by_key[("b", 2022)] == 1
    assert by_key[("c", 2022)] == 1
    assert by_key[("c", 2023)] == 1
    assert "treatment" not in rows[0]


def test_build_event_study_terms_marks_treated_relative_years() -> None:
    assert hasattr(models, "build_event_study_terms")
    rows = [
        {"admin_id": "a", "year": 2021, "treatment": 1},
        {"admin_id": "a", "year": 2022, "treatment": 1},
        {"admin_id": "a", "year": 2023, "treatment": 1},
        {"admin_id": "b", "year": 2023, "treatment": 0},
    ]

    expanded = models.build_event_study_terms(
        rows,
        treatment_field="treatment",
        year_field="year",
        event_year=2022,
        window=1,
    )

    assert expanded[0]["event_time"] == -1
    assert expanded[0]["event_time_m1"] == 1
    assert expanded[0]["event_time_0"] == 0
    assert expanded[0]["event_time_p1"] == 0
    assert expanded[1]["event_time_0"] == 1
    assert expanded[2]["event_time_p1"] == 1
    assert expanded[3]["event_time_p1"] == 0
    assert "event_time" not in rows[0]


def test_run_modeling_missing_panel_writes_empty_outputs(tmp_path: Path) -> None:
    assert hasattr(models, "run_modeling")
    result = models.run_modeling(
        model_panel=tmp_path / "data" / "processed" / "model_panel.csv",
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
    )

    assert result.status == "missing"
    assert result.report_path == tmp_path / "reports" / "model_results.md"
    assert result.report_path.exists()
    assert "No model panel CSV found" in result.report_path.read_text(encoding="utf-8")

    coefficient_path = tmp_path / "data" / "outputs" / "model_coefficients.csv"
    prediction_path = tmp_path / "data" / "outputs" / "model_predictions.csv"
    event_path = tmp_path / "data" / "outputs" / "event_study_coefficients.csv"
    assert _read_csv(coefficient_path) == []
    assert _read_csv(prediction_path) == []
    assert _read_csv(event_path) == []


def test_run_modeling_existing_panel_writes_descriptive_ols(tmp_path: Path) -> None:
    assert hasattr(models, "run_modeling")
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    _write_csv(
        panel_path,
        [
            {"admin_id": "a", "year": 2020, "yield_anomaly": 1.0, "exposure_index": 0.0},
            {"admin_id": "b", "year": 2020, "yield_anomaly": 3.0, "exposure_index": 1.0},
            {"admin_id": "c", "year": 2020, "yield_anomaly": 5.0, "exposure_index": 2.0},
            {"admin_id": "d", "year": 2020, "yield_anomaly": 7.0, "exposure_index": 3.0},
        ],
    )

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
        x_vars=["exposure_index"],
        exposure_field="exposure_index",
    )

    coefficients = _read_csv(tmp_path / "data" / "outputs" / "model_coefficients.csv")
    predictions = _read_csv(tmp_path / "data" / "outputs" / "model_predictions.csv")

    assert result.status == "ok"
    assert coefficients[0]["term"] == "intercept"
    assert abs(float(coefficients[0]["estimate"]) - 1.0) < 1e-9
    assert coefficients[1]["term"] == "exposure_index"
    assert abs(float(coefficients[1]["estimate"]) - 2.0) < 1e-9
    assert len(predictions) == 4
    assert abs(float(predictions[-1]["prediction"]) - 7.0) < 1e-9
    assert "Status: ok" in result.report_path.read_text(encoding="utf-8")


def test_run_modeling_falls_back_to_available_yield_outcome(tmp_path: Path) -> None:
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    _write_csv(
        panel_path,
        [
            {"province": "a", "year": 2024, "yield_kg_per_hectare": 5000, "sown_area_hectare": 100},
            {"province": "b", "year": 2024, "yield_kg_per_hectare": 5200, "sown_area_hectare": 200},
            {"province": "c", "year": 2024, "yield_kg_per_hectare": 5400, "sown_area_hectare": 300},
            {"province": "d", "year": 2024, "yield_kg_per_hectare": 5600, "sown_area_hectare": 400},
        ],
    )

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
    )

    coefficients = _read_csv(tmp_path / "data" / "outputs" / "model_coefficients.csv")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert coefficients
    assert "Outcome field `yield_anomaly` not found; using `yield_kg_per_hectare`." in report_text
    assert "Outcome: `yield_kg_per_hectare`" in report_text


def test_run_modeling_filters_optional_background_years(tmp_path: Path) -> None:
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    _write_csv(
        panel_path,
        [
            {"admin_id": "a", "year": 2024, "yield_anomaly": 1.0, "exposure_index": 0.0},
            {"admin_id": "b", "year": 2024, "yield_anomaly": 3.0, "exposure_index": 1.0},
            {"admin_id": "c", "year": 2025, "yield_anomaly": 99.0, "exposure_index": 99.0},
        ],
    )

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
        x_vars=["exposure_index"],
        min_year=2000,
        max_year=2024,
    )

    predictions = _read_csv(tmp_path / "data" / "outputs" / "model_predictions.csv")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert len(predictions) == 2
    assert "removed 1 optional/background rows" in report_text


def test_run_modeling_writes_event_study_when_exposure_and_anomaly_exist(tmp_path: Path) -> None:
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    rows = []
    for admin_id, exposure, base in [("treated", 10.0, -10.0), ("control", 1.0, 0.0)]:
        for year, effect in [(2021, 0.0), (2022, -5.0), (2023, -2.0)]:
            rows.append(
                {
                    "admin_id": admin_id,
                    "year": year,
                    "yield_anomaly_pct": base + effect,
                    "exposure_index": exposure if year == 2022 else 0.0,
                }
            )
    _write_csv(panel_path, rows)

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
        exposure_field="exposure_index",
        event_window=1,
    )

    event_rows = _read_csv(tmp_path / "data" / "outputs" / "event_study_coefficients.csv")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert event_rows
    assert {row["model"] for row in event_rows} == {"event_study"}
    assert "Outcome: `yield_anomaly_pct`" in report_text


def test_run_modeling_uses_province_as_admin_field_when_admin_id_missing(tmp_path: Path) -> None:
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    rows = []
    for province, exposure, base in [("High", 10.0, -10.0), ("Low", 1.0, 0.0)]:
        for year, effect in [(2021, 0.0), (2022, -5.0), (2023, -2.0)]:
            rows.append(
                {
                    "province": province,
                    "year": year,
                    "yield_anomaly_pct": base + effect,
                    "exposure_index": exposure if year == 2022 else 0.0,
                }
            )
    _write_csv(panel_path, rows)

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
        exposure_field="exposure_index",
        event_window=1,
    )

    event_rows = _read_csv(tmp_path / "data" / "outputs" / "event_study_coefficients.csv")
    predictions = _read_csv(tmp_path / "data" / "outputs" / "model_predictions.csv")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert event_rows
    assert {row["admin_id"] for row in predictions} == {"High", "Low"}
    assert "Admin field `admin_id` not found; using `province`." in report_text


def test_run_modeling_skips_constant_exposure_predictor(tmp_path: Path) -> None:
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    _write_csv(
        panel_path,
        [
            {"admin_id": "a", "year": 2020, "yield_anomaly_pct": 1.0, "exposure_index": 0.0, "rice_share": 0.1},
            {"admin_id": "b", "year": 2020, "yield_anomaly_pct": 2.0, "exposure_index": 0.0, "rice_share": 0.2},
            {"admin_id": "c", "year": 2020, "yield_anomaly_pct": 3.0, "exposure_index": 0.0, "rice_share": 0.3},
            {"admin_id": "d", "year": 2020, "yield_anomaly_pct": 4.0, "exposure_index": 0.0, "rice_share": 0.4},
        ],
    )

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
    )

    coefficients = _read_csv(tmp_path / "data" / "outputs" / "model_coefficients.csv")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert any(row["term"] == "rice_share" for row in coefficients)
    assert "exposure_index" not in {row["term"] for row in coefficients}
    assert "Predictors: `rice_share`" in report_text


def test_run_modeling_skips_event_study_when_treatment_has_no_variation(tmp_path: Path) -> None:
    panel_path = tmp_path / "data" / "processed" / "model_panel.csv"
    panel_path.parent.mkdir(parents=True)
    _write_csv(
        panel_path,
        [
            {"admin_id": "a", "year": 2021, "yield_anomaly_pct": 1.0, "exposure_index": 0.0, "rice_share": 0.1},
            {"admin_id": "a", "year": 2022, "yield_anomaly_pct": 2.0, "exposure_index": 0.0, "rice_share": 0.2},
            {"admin_id": "b", "year": 2021, "yield_anomaly_pct": 3.0, "exposure_index": 0.0, "rice_share": 0.3},
            {"admin_id": "b", "year": 2022, "yield_anomaly_pct": 4.0, "exposure_index": 0.0, "rice_share": 0.4},
        ],
    )

    result = models.run_modeling(
        model_panel=panel_path,
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        event_year=2022,
        outcome_field="yield_anomaly",
        exposure_field="exposure_index",
        event_window=1,
    )

    event_rows = _read_csv(tmp_path / "data" / "outputs" / "event_study_coefficients.csv")
    report_text = result.report_path.read_text(encoding="utf-8")

    assert result.status == "ok"
    assert event_rows == []
    assert "Skipped event-study coefficients because treatment has no variation." in report_text


def test_modeling_cli_supports_config_and_verbose(tmp_path: Path) -> None:
    script_path = Path("scripts/08_modeling.py")
    assert script_path.exists()
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
                "target_admin_level: county",
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
                "output_formats: [csv, markdown]",
            ]
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, str(script_path), "--config", str(config_path), "--verbose"],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "reports" / "model_results.md").exists()
    assert (tmp_path / "data" / "outputs" / "model_coefficients.csv").exists()


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))
