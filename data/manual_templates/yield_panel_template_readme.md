# Manual Yield Panel Template

This template is for manually curated official area, production, and yield data.

## Required Columns

`source_id, source_name, source_url_or_reference, source_type, content_year, yearbook_year, province, prefecture, county, admin_code, admin_level, crop, area_value, area_unit, production_value, production_unit, yield_value, yield_unit, notes`

## Supported Values

- `crop`: `rice`, `early_rice`, `single_rice`, `middle_rice`, `late_rice`, `grain`, `wheat`, `maize`
- `area_unit`: `hectare`, `mu`, `thousand_hectare`, `ten_thousand_mu`
- `production_unit`: `ton`, `kg`, `jin`, `ten_thousand_ton`, `ten_thousand_jin`
- `yield_unit`: `kg_per_hectare`, `kg_per_mu`, `jin_per_mu`, `ton_per_hectare`
- `admin_level`: `national`, `province`, `prefecture`, `county`

Rows with only area, only production, or only yield are accepted. If area and production are provided but yield is blank, the importer derives `yield_kg_per_hectare`.

Outputs are written to:

- `data/processed/manual_yield_panel_cleaned.parquet`
- `data/processed/manual_yield_panel_cleaned.csv`
- `reports/manual_yield_import_summary.md`
