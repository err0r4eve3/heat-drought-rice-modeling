from pathlib import Path

import pandas as pd

from src.manual_yield_import import (
    TEMPLATE_COLUMNS,
    create_yield_panel_template,
    import_manual_yield_panel,
    normalize_manual_yield_frame,
)


def test_create_template_writes_required_columns(tmp_path: Path) -> None:
    template = create_yield_panel_template(tmp_path / "manual" / "yield_panel_template.csv")

    frame = pd.read_csv(template)

    assert frame.empty
    assert frame.columns.tolist() == TEMPLATE_COLUMNS


def test_import_empty_template_writes_empty_outputs(tmp_path: Path) -> None:
    template = tmp_path / "yield_panel_template.csv"
    template.write_text(",".join(TEMPLATE_COLUMNS) + "\n", encoding="utf-8")

    result = import_manual_yield_panel(
        template_path=template,
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
    )

    assert result.status == "empty"
    assert result.output_rows == 0
    assert result.outputs["csv"].exists()
    assert result.outputs["parquet"].exists()
    assert result.report_path.exists()
    cleaned = pd.read_csv(result.outputs["csv"])
    assert cleaned.empty


def test_import_zero_byte_template_succeeds(tmp_path: Path) -> None:
    template = tmp_path / "yield_panel_template.csv"
    template.write_text("", encoding="utf-8")

    result = import_manual_yield_panel(
        template_path=template,
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
    )

    assert result.status == "empty"
    assert result.output_rows == 0
    assert any("template is empty" in warning for warning in result.warnings)


def test_normalize_manual_yield_units_and_derives_yield() -> None:
    frame = pd.DataFrame(
        [
            {
                "source_id": "hubei-2024",
                "source_name": "Hubei Statistical Yearbook",
                "source_url_or_reference": "manual excerpt",
                "source_type": "yearbook",
                "content_year": "2023",
                "yearbook_year": "2024",
                "province": "湖北",
                "prefecture": "武汉",
                "county": "",
                "admin_code": "420100",
                "admin_level": "prefecture",
                "crop": "rice",
                "area_value": "2",
                "area_unit": "ten_thousand_mu",
                "production_value": "9000",
                "production_unit": "ton",
                "yield_value": "",
                "yield_unit": "",
                "notes": "test row",
            },
            {
                "source_id": "zhejiang-2024",
                "source_name": "Zhejiang Statistical Yearbook",
                "source_url_or_reference": "manual excerpt",
                "source_type": "yearbook",
                "content_year": "2023",
                "yearbook_year": "2024",
                "province": "浙江",
                "prefecture": "",
                "county": "",
                "admin_code": "330000",
                "admin_level": "province",
                "crop": "wheat",
                "area_value": "",
                "area_unit": "",
                "production_value": "",
                "production_unit": "",
                "yield_value": "600",
                "yield_unit": "kg_per_mu",
                "notes": "",
            },
        ],
        columns=TEMPLATE_COLUMNS,
    )

    cleaned, warnings = normalize_manual_yield_frame(frame)

    assert warnings == []
    assert cleaned["area_ha"].iloc[0] == 20000 / 15
    assert cleaned["area_hectare"].iloc[0] == 20000 / 15
    assert round(cleaned["yield_kg_ha"].iloc[0], 6) == 6750.0
    assert round(cleaned["yield_kg_per_hectare"].iloc[0], 6) == 6750.0
    assert cleaned["yield_source"].iloc[0] == "derived_from_area_and_production"
    assert cleaned["yield_kg_per_hectare"].iloc[1] == 9000.0


def test_normalize_manual_yield_keeps_highest_priority_duplicate() -> None:
    rows = []
    for source_type, yield_value in [("third_party", "500"), ("official", "600")]:
        rows.append(
            {
                "source_id": source_type,
                "source_name": source_type,
                "source_url_or_reference": "",
                "source_type": source_type,
                "content_year": "2023",
                "yearbook_year": "2024",
                "province": "湖北",
                "prefecture": "",
                "county": "",
                "admin_code": "420000",
                "admin_level": "province",
                "crop": "grain",
                "area_value": "",
                "area_unit": "",
                "production_value": "",
                "production_unit": "",
                "yield_value": yield_value,
                "yield_unit": "kg_per_mu",
                "notes": "",
            }
        )
    cleaned, warnings = normalize_manual_yield_frame(pd.DataFrame(rows, columns=TEMPLATE_COLUMNS))

    assert warnings == []
    assert len(cleaned) == 1
    assert cleaned["source_type"].iloc[0] == "official"
    assert cleaned["yield_kg_ha"].iloc[0] == 9000.0


def test_import_skips_unsupported_crop_without_failing(tmp_path: Path) -> None:
    template = tmp_path / "yield_panel_template.csv"
    pd.DataFrame(
        [
            {
                "source_id": "bad",
                "source_name": "Bad",
                "source_url_or_reference": "",
                "source_type": "manual",
                "content_year": "2023",
                "yearbook_year": "2024",
                "province": "湖北",
                "prefecture": "",
                "county": "",
                "admin_code": "420000",
                "admin_level": "province",
                "crop": "soy",
                "area_value": "1",
                "area_unit": "hectare",
                "production_value": "",
                "production_unit": "",
                "yield_value": "",
                "yield_unit": "",
                "notes": "",
            }
        ],
        columns=TEMPLATE_COLUMNS,
    ).to_csv(template, index=False)

    result = import_manual_yield_panel(
        template_path=template,
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
    )

    assert result.status == "empty_after_validation"
    assert result.skipped_rows == 1
    assert "unsupported crop" in result.warnings[0]


def test_import_matches_missing_admin_code_from_crosswalk(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir()
    pd.DataFrame(
        [
            {
                "year": "2023",
                "admin_code_standard": "420100",
                "admin_name_standard": "武汉市",
                "province_standard": "湖北省",
                "prefecture_standard": "武汉市",
                "county_standard": "",
                "match_confidence": "1.0",
            }
        ]
    ).to_csv(processed / "admin_crosswalk_2000_2025.csv", index=False)
    template = tmp_path / "yield_panel_template.csv"
    pd.DataFrame(
        [
            {
                "source_id": "hubei",
                "source_name": "湖北统计年鉴",
                "source_url_or_reference": "manual",
                "source_type": "yearbook",
                "content_year": "2023",
                "yearbook_year": "2024",
                "province": "湖北省",
                "prefecture": "武汉市",
                "county": "",
                "admin_code": "",
                "admin_level": "prefecture",
                "crop": "grain",
                "area_value": "",
                "area_unit": "",
                "production_value": "",
                "production_unit": "",
                "yield_value": "500",
                "yield_unit": "kg_per_mu",
                "notes": "",
            }
        ],
        columns=TEMPLATE_COLUMNS,
    ).to_csv(template, index=False)

    result = import_manual_yield_panel(
        template_path=template,
        processed_dir=processed,
        reports_dir=tmp_path / "reports",
    )
    cleaned = pd.read_csv(result.outputs["csv"], dtype=str)
    low_confidence = pd.read_csv(result.outputs["low_confidence"], dtype=str)

    assert result.status == "ok"
    assert cleaned["admin_code"].iloc[0] == "420100"
    assert cleaned["match_confidence"].astype(float).iloc[0] >= 0.85
    assert low_confidence.empty
