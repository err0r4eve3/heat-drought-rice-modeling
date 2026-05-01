from __future__ import annotations

from pathlib import Path

from scripts.generate_report_core import generate_final_report
from src.figures import expected_figure_names


FORBIDDEN_OUTPUT_TOKENS = {
    "county_yield_loss_map",
    "prefecture_yield_loss_claim",
    "county_level_official_yield_loss_map",
    "prefecture_level_yield_loss_claim",
    "city_county_causal_claim",
}


def test_expected_figures_do_not_include_subprovince_yield_loss_outputs() -> None:
    figure_names = set(expected_figure_names())

    assert not (figure_names & FORBIDDEN_OUTPUT_TOKENS)
    assert "county_or_grid_exposure_detail" in figure_names
    assert "remote_sensing_growth_anomaly" in figure_names


def test_final_report_declares_subprovince_outputs_are_exposure_only(tmp_path: Path) -> None:
    report_path = generate_final_report(
        processed_dir=tmp_path / "data" / "processed",
        output_dir=tmp_path / "data" / "outputs",
        reports_dir=tmp_path / "reports",
        main_event_year=2022,
    )

    report_text = report_path.read_text(encoding="utf-8")

    assert "为什么采用省级产量口径" in report_text
    assert "县域和栅格尺度结果仅用于热旱暴露和遥感响应分析" in report_text
    assert "县级产量损失地图" not in report_text
    assert "地级市产量损失地图" not in report_text
