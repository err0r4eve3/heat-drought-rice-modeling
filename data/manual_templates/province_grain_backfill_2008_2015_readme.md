# Province Grain Backfill 2008-2015

Use `province_grain_backfill_2008_2015.csv` to enter official province-level grain yield rows from China Statistical Yearbook 2009-2016.

Rules:

- `content_year` must be 2008-2015.
- `yearbook_year` must be `content_year + 1`.
- `admin_level` must be `province`.
- `crop` must be `grain`.
- `source_type` is added by the import script as `official_yearbook`.
- Prefer reported yield from the yearbook table when available.
- If reported yield is unavailable, enter production and area so the importer can calculate `yield_kg_ha`.

Supported units:

- Yield: `kg_per_hectare`, `kg_per_ha`, `kg_per_mu`, `jin_per_mu`, `ton_per_hectare`.
- Production: `ton`, `kg`, `ten_thousand_ton`.
- Area: `hectare`, `ha`, `mu`, `thousand_hectare`, `ten_thousand_mu`.

Suggested source names:

- `China Statistical Yearbook 2009` for content year 2008.
- `China Statistical Yearbook 2010` for content year 2009.
- `China Statistical Yearbook 2011` for content year 2010.
- `China Statistical Yearbook 2012` for content year 2011.
- `China Statistical Yearbook 2013` for content year 2012.
- `China Statistical Yearbook 2014` for content year 2013.
- `China Statistical Yearbook 2015` for content year 2014.
- `China Statistical Yearbook 2016` for content year 2015.

Run:

```powershell
uv run python scripts/22_import_province_grain_backfill.py --config config/config.yaml
```

Outputs:

- `data/processed/province_grain_backfill_2008_2015_cleaned.csv`
- `data/processed/province_grain_backfill_2008_2015_cleaned.parquet`
- `reports/province_grain_backfill_summary.md`
