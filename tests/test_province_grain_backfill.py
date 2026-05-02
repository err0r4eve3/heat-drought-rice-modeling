from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.province_grain_backfill import (
    BACKFILL_TEMPLATE_COLUMNS,
    import_province_grain_backfill,
    normalize_province_grain_backfill,
)


def test_normalize_grain_backfill_converts_reported_yield_to_kg_ha() -> None:
    frame = pd.DataFrame(
        [
            {
                "source_id": "alpha-2008",
                "source_name": "",
                "source_url_or_reference": "China Statistical Yearbook 2009 table 12-11",
                "content_year": "2008",
                "yearbook_year": "",
                "province": "Alpha",
                "province_code": "110000",
                "admin_level": "",
                "crop": "",
                "yield_value": "450",
                "yield_unit": "kg_per_mu",
                "production_value": "",
                "production_unit": "",
                "area_value": "",
                "area_unit": "",
                "notes": "",
            }
        ],
        columns=BACKFILL_TEMPLATE_COLUMNS,
    )

    cleaned, warnings = normalize_province_grain_backfill(frame)

    assert warnings == []
    assert cleaned["content_year"].iloc[0] == 2008
    assert cleaned["year"].iloc[0] == 2008
    assert cleaned["yearbook_year"].iloc[0] == 2009
    assert cleaned["source_name"].iloc[0] == "China Statistical Yearbook 2009"
    assert cleaned["source_type"].iloc[0] == "official_yearbook"
    assert cleaned["admin_level"].iloc[0] == "province"
    assert cleaned["crop"].iloc[0] == "grain"
    assert cleaned["is_backfill"].iloc[0] is True
    assert cleaned["yield_kg_ha"].iloc[0] == 6750.0
    assert cleaned["yield_source"].iloc[0] == "reported"


def test_import_grain_backfill_derives_yield_and_writes_outputs(tmp_path: Path) -> None:
    template = tmp_path / "data" / "manual_templates" / "province_grain_backfill_2008_2015.csv"
    template.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "source_id": "beta-2015",
                "source_name": "China Statistical Yearbook 2016",
                "source_url_or_reference": "table 12-10 and 12-11",
                "content_year": "2015",
                "yearbook_year": "2016",
                "province": "Beta",
                "province_code": "120000",
                "admin_level": "province",
                "crop": "grain",
                "yield_value": "",
                "yield_unit": "",
                "production_value": "9",
                "production_unit": "ten_thousand_ton",
                "area_value": "10",
                "area_unit": "thousand_hectare",
                "notes": "",
            }
        ],
        columns=BACKFILL_TEMPLATE_COLUMNS,
    ).to_csv(template, index=False)

    result = import_province_grain_backfill(
        template_path=template,
        processed_dir=tmp_path / "data" / "processed",
        reports_dir=tmp_path / "reports",
    )
    cleaned = pd.read_csv(result.outputs["csv"])

    assert result.status == "ok"
    assert result.input_rows == 1
    assert result.output_rows == 1
    assert cleaned["yield_kg_ha"].iloc[0] == 9000.0
    assert cleaned["yield_source"].iloc[0] == "derived_from_area_and_production"
    assert cleaned["is_backfill"].iloc[0] == True
    assert result.outputs["parquet"].exists()
    assert "Province Grain Backfill Summary" in result.report_path.read_text(encoding="utf-8")
