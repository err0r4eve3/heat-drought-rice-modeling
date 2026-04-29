from pathlib import Path

from src.figures import expected_figure_names, make_figures


def test_expected_figure_names_contains_required_outputs() -> None:
    names = expected_figure_names()

    assert len(names) == 10
    assert "study_area_map" in names
    assert "event_study_plot" in names
    assert "timeline_2022_event" in names


def test_make_figures_creates_png_and_svg_outputs_for_empty_data(tmp_path: Path) -> None:
    result = make_figures(
        processed_dir=tmp_path / "processed",
        output_dir=tmp_path / "outputs",
        reports_dir=tmp_path / "reports",
        main_event_year=2022,
    )

    assert result.status in {"ok", "partial"}
    assert result.report_path.exists()
    for name in expected_figure_names():
        assert (tmp_path / "outputs" / "figures" / f"{name}.png").exists()
        assert (tmp_path / "outputs" / "figures" / f"{name}.svg").exists()
