from src.yield_sources import (
    build_yield_source_catalog,
    combine_yield_panel_rows,
    normalize_ers_provincial_records,
    parse_nbs_grain_announcement_text,
)


def test_build_yield_source_catalog_includes_local_yearbook_leads() -> None:
    catalog = build_yield_source_catalog()
    by_id = {record["source_id"]: record for record in catalog}

    assert "hubei_statistical_yearbook_index" in by_id
    assert by_id["hubei_statistical_yearbook_index"]["usable_tier"] == "tier_1_or_tier_2_candidate_after_table_extraction"
    assert by_id["yougis_county_yearbook_lead"]["usable_tier"] == "search_clue_only"


def test_normalize_ers_provincial_records_builds_rice_and_grain_yield_rows() -> None:
    rows = [
        {
            "Geography_Desc": "Anhui",
            "Year_Desc": 2000,
            "Category": "Rice production",
            "Commodity_Desc": "rice",
            "Amount": 10.0,
            "Unit_Desc": "1000 tons",
        },
        {
            "Geography_Desc": "Anhui",
            "Year_Desc": 2000,
            "Category": "Rice sown area",
            "Commodity_Desc": "rice",
            "Amount": 2.0,
            "Unit_Desc": "1000 hectares",
        },
        {
            "Geography_Desc": "Anhui",
            "Year_Desc": 2000,
            "Category": "Grain production",
            "Commodity_Desc": "total grain",
            "Amount": 30.0,
            "Unit_Desc": "1000 tons",
        },
        {
            "Geography_Desc": "Anhui",
            "Year_Desc": 2000,
            "Category": "Grain sown area",
            "Commodity_Desc": "total grain",
            "Amount": 5.0,
            "Unit_Desc": "1000 hectares",
        },
    ]

    panel = normalize_ers_provincial_records(rows)

    assert panel == [
        {
            "year": 2000,
            "province": "安徽",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "admin_level": "province",
            "crop": "grain",
            "sown_area_hectare": 5000.0,
            "harvested_area_hectare": "",
            "production_ton": 30000.0,
            "yield_kg_per_hectare": 6000.0,
            "source": "USDA_ERS_China_Provincial_Data",
            "source_file": "",
            "quality_flag": "official_derived_yield",
        },
        {
            "year": 2000,
            "province": "安徽",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "admin_level": "province",
            "crop": "rice",
            "sown_area_hectare": 2000.0,
            "harvested_area_hectare": "",
            "production_ton": 10000.0,
            "yield_kg_per_hectare": 5000.0,
            "source": "USDA_ERS_China_Provincial_Data",
            "source_file": "",
            "quality_flag": "official_derived_yield",
        },
    ]


def test_parse_nbs_grain_announcement_text_extracts_province_rows() -> None:
    text = """
    表
    2
    2024 年全国及各省（区、市）粮食产量
    播 种 面 积
    （千公顷）
    总 产 量
    （万吨）
    单位面积产量
    （公斤/公顷）
    全国总计
    119319.1
    70649.9
    5921.1
    北　　京
    94.2
    57.6
    6115.5
    江　　苏
    5475.5
    3810.1
    6958.4
    注：此表中部分数据因四舍五入
    """

    rows = parse_nbs_grain_announcement_text(text, year=2024, source_url="https://example.test")

    assert rows == [
        {
            "year": 2024,
            "province": "全国总计",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "admin_level": "national",
            "crop": "grain",
            "sown_area_hectare": 119319100.0,
            "harvested_area_hectare": "",
            "production_ton": 706499000.0,
            "yield_kg_per_hectare": 5921.1,
            "source": "NBS_grain_announcement",
            "source_file": "https://example.test",
            "quality_flag": "official_reported",
        },
        {
            "year": 2024,
            "province": "北京",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "admin_level": "province",
            "crop": "grain",
            "sown_area_hectare": 94200.0,
            "harvested_area_hectare": "",
            "production_ton": 576000.0,
            "yield_kg_per_hectare": 6115.5,
            "source": "NBS_grain_announcement",
            "source_file": "https://example.test",
            "quality_flag": "official_reported",
        },
        {
            "year": 2024,
            "province": "江苏",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "admin_level": "province",
            "crop": "grain",
            "sown_area_hectare": 5475500.0,
            "harvested_area_hectare": "",
            "production_ton": 38101000.0,
            "yield_kg_per_hectare": 6958.4,
            "source": "NBS_grain_announcement",
            "source_file": "https://example.test",
            "quality_flag": "official_reported",
        },
    ]


def test_combine_yield_panel_rows_keeps_external_and_local_statistics() -> None:
    external_rows = [
        {
            "year": 2024,
            "province": "江苏",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "admin_level": "province",
            "crop": "grain",
            "sown_area_hectare": 10.0,
            "harvested_area_hectare": "",
            "production_ton": 20.0,
            "yield_kg_per_hectare": 2000.0,
            "source": "NBS_grain_announcement",
            "source_file": "nbs",
            "quality_flag": "official_reported",
        }
    ]
    local_rows = [
        {
            "year": "2025",
            "province": "江苏",
            "prefecture": "",
            "county": "",
            "admin_code": "",
            "crop": "",
            "sown_area_hectare": "11",
            "harvested_area_hectare": "",
            "production_ton": "22",
            "yield_kg_per_hectare": "2000",
            "source_file": "local.csv",
        }
    ]

    combined = combine_yield_panel_rows(external_rows, local_rows)

    assert [row["year"] for row in combined] == [2024, 2025]
    assert [row["source"] for row in combined] == [
        "NBS_grain_announcement",
        "local_statistics_cleaned",
    ]
    assert combined[1]["crop"] == "grain"
    assert combined[1]["admin_level"] == "province"
