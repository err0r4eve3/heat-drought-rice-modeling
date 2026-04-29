from pathlib import Path

from src.diagnostics import correlation_matrix, detect_outliers, missing_rate, run_diagnostics


def test_missing_rate_by_field() -> None:
    rows = [{"a": 1, "b": None}, {"a": "", "b": 2}, {"a": 3, "b": 4}]

    rates = missing_rate(rows, ["a", "b", "c"])

    assert rates == {"a": 1 / 3, "b": 1 / 3, "c": 1.0}


def test_detect_outliers_uses_z_threshold() -> None:
    result = detect_outliers([10, 10, 10, 10, 100], z_threshold=1.5)

    assert result == [False, False, False, False, True]


def test_correlation_matrix_computes_pairwise_values() -> None:
    rows = [{"x": 1, "y": 2}, {"x": 2, "y": 4}, {"x": 3, "y": 6}]

    matrix = correlation_matrix(rows, ["x", "y"])

    assert matrix["x"]["x"] == 1.0
    assert matrix["x"]["y"] == 1.0


def test_run_diagnostics_handles_missing_panel(tmp_path: Path) -> None:
    result = run_diagnostics(
        processed_dir=tmp_path / "processed",
        output_dir=tmp_path / "outputs",
        reports_dir=tmp_path / "reports",
        main_event_year=2022,
    )

    assert result.status == "missing"
    assert result.outputs["robustness"].exists()
    assert result.outputs["placebo"].exists()
    assert result.report_path.exists()
    assert "Model panel not found" in result.report_path.read_text(encoding="utf-8")
