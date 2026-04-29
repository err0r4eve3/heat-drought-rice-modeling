import csv
from pathlib import Path

import pytest

import src.statistics as statistics


def require_api(name: str):
    assert hasattr(statistics, name), f"src.statistics must expose {name}"
    return getattr(statistics, name)


def test_find_statistics_files_scans_supported_table_files(tmp_path: Path) -> None:
    statistics_dir = tmp_path / "statistics"
    nested_dir = statistics_dir / "nested"
    nested_dir.mkdir(parents=True)
    (statistics_dir / "county_yield.csv").write_text("year,county\n", encoding="utf-8")
    (nested_dir / "prefecture_yield.xlsx").write_text("", encoding="utf-8")
    (nested_dir / "legacy_yield.xls").write_text("", encoding="utf-8")
    (statistics_dir / "notes.txt").write_text("ignore", encoding="utf-8")

    files = require_api("find_statistics_files")(statistics_dir)

    assert [path.name for path in files] == [
        "county_yield.csv",
        "legacy_yield.xls",
        "prefecture_yield.xlsx",
    ]


def test_find_statistics_files_ignores_external_yield_source_downloads(tmp_path: Path) -> None:
    statistics_dir = tmp_path / "statistics"
    external_dir = statistics_dir / "external_yield_sources"
    external_dir.mkdir(parents=True)
    (statistics_dir / "county_yield.csv").write_text("year,county\n", encoding="utf-8")
    (external_dir / "ers_china_provincialdata.xls").write_text("", encoding="utf-8")

    files = require_api("find_statistics_files")(statistics_dir)

    assert files == [statistics_dir / "county_yield.csv"]


def test_identify_statistics_fields_matches_english_and_chinese_candidates() -> None:
    columns = [
        "年份",
        "province_name",
        "地级市",
        "区县",
        "行政区划代码",
        "作物名称",
        "播种面积(亩)",
        "收获面积(公顷)",
        "总产量(公斤)",
        "单产(公斤/亩)",
        "水稻单产",
        "粮食单产",
    ]

    mapping = require_api("identify_statistics_fields")(columns)

    assert mapping == {
        "year": "年份",
        "province": "province_name",
        "prefecture": "地级市",
        "county": "区县",
        "admin_code": "行政区划代码",
        "crop": "作物名称",
        "sown_area": "播种面积(亩)",
        "harvested_area": "收获面积(公顷)",
        "production": "总产量(公斤)",
        "yield": "单产(公斤/亩)",
        "rice_yield": "水稻单产",
        "grain_yield": "粮食单产",
    }


def test_identify_statistics_fields_matches_unit_encoded_nbs_columns() -> None:
    mapping = require_api("identify_statistics_fields")(
        ["year", "region", "sown_area_1000ha", "production_10000t", "yield_kg_per_ha"]
    )

    assert mapping["year"] == "year"
    assert mapping["province"] == "region"
    assert mapping["sown_area"] == "sown_area_1000ha"
    assert mapping["production"] == "production_10000t"
    assert mapping["yield"] == "yield_kg_per_ha"


def test_unit_conversions_normalize_production_area_and_yield_units() -> None:
    convert_production_to_ton = require_api("convert_production_to_ton")
    convert_area_to_hectare = require_api("convert_area_to_hectare")
    convert_yield_to_kg_per_hectare = require_api("convert_yield_to_kg_per_hectare")

    assert convert_production_to_ton(1000, "kg") == pytest.approx(1.0)
    assert convert_production_to_ton(2000, "斤") == pytest.approx(1.0)
    assert convert_production_to_ton(3, "万吨") == pytest.approx(30000.0)

    assert convert_area_to_hectare(15, "亩") == pytest.approx(1.0)
    assert convert_area_to_hectare(10000, "平方米") == pytest.approx(1.0)
    assert convert_area_to_hectare(2, "ha") == pytest.approx(2.0)

    assert convert_yield_to_kg_per_hectare(400, "kg/mu") == pytest.approx(6000.0)
    assert convert_yield_to_kg_per_hectare(800, "斤/亩") == pytest.approx(6000.0)
    assert convert_yield_to_kg_per_hectare(6, "吨/公顷") == pytest.approx(6000.0)
    assert convert_yield_to_kg_per_hectare(6000, "kg/ha") == pytest.approx(6000.0)


def test_clean_admin_name_removes_common_suffixes() -> None:
    clean_admin_name = require_api("clean_admin_name")

    assert clean_admin_name("长沙市") == "长沙"
    assert clean_admin_name("西湖区") == "西湖"
    assert clean_admin_name("城步苗族自治县") == "城步苗族"
    assert clean_admin_name("  南昌县  ") == "南昌"


def test_compute_yield_returns_kg_per_hectare_and_handles_invalid_area() -> None:
    compute_yield = require_api("compute_yield")

    assert compute_yield(10, 2) == pytest.approx(5000.0)
    assert compute_yield(10, 0) is None
    assert compute_yield(None, 2) is None


def test_prepare_statistics_empty_directory_writes_fallback_outputs(tmp_path: Path) -> None:
    statistics_dir = tmp_path / "raw" / "statistics"
    processed_dir = tmp_path / "data" / "processed"
    reports_dir = tmp_path / "reports"
    statistics_dir.mkdir(parents=True)

    result = require_api("prepare_statistics")(statistics_dir, processed_dir, reports_dir)

    panel_path = processed_dir / "yield_panel.csv"
    qc_path = processed_dir / "yield_panel_qc.csv"
    report_path = reports_dir / "statistics_cleaning_summary.md"

    assert result.status == "missing"
    assert result.file_count == 0
    assert result.outputs == {
        "panel": panel_path,
        "qc": qc_path,
        "coverage": processed_dir / "yield_coverage_report.csv",
    }
    assert result.report_path == report_path
    assert result.warnings == [f"No agricultural statistics files found under {statistics_dir.resolve()}."]

    with panel_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        rows = list(reader)
    assert reader.fieldnames == require_api("YIELD_PANEL_COLUMNS")
    assert rows == []

    with qc_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        qc_rows = list(reader)
    assert reader.fieldnames == [
        "source_file",
        "source_row",
        "issue",
        "message",
    ]
    assert qc_rows == []

    assert "No agricultural statistics files found" in report_path.read_text(encoding="utf-8")
    assert (processed_dir / "yield_coverage_report.csv").exists()
    assert (reports_dir / "yield_coverage_report.md").exists()
    assert (processed_dir / "yield_data_tier_report.csv").exists()


def test_prepare_statistics_reads_csv_and_standardizes_basic_fields(tmp_path: Path) -> None:
    statistics_dir = tmp_path / "raw" / "statistics"
    processed_dir = tmp_path / "data" / "processed"
    reports_dir = tmp_path / "reports"
    statistics_dir.mkdir(parents=True)
    (statistics_dir / "county_yield.csv").write_text(
        "\n".join(
            [
                "年份,区县,作物名称,播种面积(亩),总产量(公斤),单产(公斤/亩)",
                "2022,长沙市,水稻,150,90000,600",
            ]
        ),
        encoding="utf-8",
    )

    result = require_api("prepare_statistics")(statistics_dir, processed_dir, reports_dir)

    assert result.status == "ok"
    assert result.row_count == 1
    with (processed_dir / "yield_panel.csv").open("r", encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))
    with (processed_dir / "yield_coverage_report.csv").open("r", encoding="utf-8", newline="") as file_obj:
        coverage_rows = list(csv.DictReader(file_obj))

    assert rows == [
        {
            "year": "2022",
            "province": "",
            "prefecture": "",
            "county": "长沙市",
            "admin_code": "",
            "admin_name_clean": "长沙",
            "crop": "水稻",
            "sown_area_hectare": "10.0",
            "harvested_area_hectare": "",
            "production_ton": "90.0",
            "yield_kg_per_hectare": "9000.0",
            "rice_yield_kg_per_hectare": "",
            "grain_yield_kg_per_hectare": "",
            "source_file": "county_yield.csv",
        }
    ]
    assert coverage_rows[0]["admin_level"] == "county"
    assert coverage_rows[0]["crop_type"] == "rice"
    assert coverage_rows[0]["recommendation"] == "use_county_rice_main_model"
