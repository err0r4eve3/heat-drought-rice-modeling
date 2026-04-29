from pathlib import Path

import pandas as pd

from src.data_sources import build_required_data_sources, write_data_source_outputs


def test_build_required_data_sources_contains_core_categories() -> None:
    sources = build_required_data_sources()

    categories = {source.category for source in sources}
    source_ids = {source.source_id for source in sources}

    assert "yield_panel" in categories
    assert "yield_proxy" in categories
    assert "climate" in categories
    assert "remote_sensing" in categories
    assert "boundary" in categories
    assert "nbs_grain_announcements" in source_ids
    assert "faostat_qcl" in source_ids
    assert "ggcp10" in source_ids


def test_write_data_source_outputs_creates_catalog_and_report(tmp_path: Path) -> None:
    outputs = write_data_source_outputs(tmp_path / "references", tmp_path / "reports")

    csv_path = outputs["csv"]
    json_path = outputs["json"]
    report_path = outputs["report"]

    assert csv_path.exists()
    assert json_path.exists()
    assert report_path.exists()

    catalog = pd.read_csv(csv_path)
    report = report_path.read_text(encoding="utf-8")

    assert {"source_id", "category", "access_level", "status", "url"}.issubset(catalog.columns)
    assert "未找到完整公开县/市级 2000-2024 内容年份水稻单产面板" in report
    assert "ERA5-Land" in report
    assert "CHIRPS" in report
    assert "中国县域统计年鉴" in report
    assert "AsiaRiceYield4km" in report
    assert "栅格产量/单产代理" in report
