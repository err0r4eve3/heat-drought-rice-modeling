# GEE Province Daily Climate Export

Goal: export one row per `province x date` for 2000-2024, then place the CSV at:

```text
data/interim/province_daily_climate_2000_2024.csv
```

Required fields:

```text
province
province_code
date
year
month
tmax_c
precipitation_mm
```

Recommended GEE script:

```text
gee/gee_export_province_daily_climate_2000_2024.js
```

Data sources:

- Temperature: `ECMWF/ERA5_LAND/DAILY_AGGR`, band `temperature_2m_max`, converted from K to Celsius.
- Precipitation: `UCSB-CHG/CHIRPS/DAILY`, band `precipitation`, already in mm/day.

Current source note:

- The older GEE ERA5 Daily collection `ECMWF/ERA5/DAILY` includes `maximum_2m_air_temperature`, but the current catalog availability shown for that collection does not cover the full 2000-2024 target window. Use ERA5-Land Daily Aggregated for the first full-window export, or use CDS ERA5-Land daily statistics as an alternative.

Official references:

- GEE ERA5-Land Daily Aggregated: https://developers.google.com/earth-engine/datasets/catalog/ECMWF_ERA5_LAND_DAILY_AGGR
- GEE ERA5 Daily Aggregates: https://developers.google.com/earth-engine/datasets/catalog/ECMWF_ERA5_DAILY
- GEE CHIRPS Daily: https://developers.google.com/earth-engine/datasets/catalog/UCSB-CHG_CHIRPS_DAILY
- CDS ERA5-Land daily statistics: https://cds.climate.copernicus.eu/datasets/derived-era5-land-daily-statistics

After export:

```powershell
uv run python scripts/20_import_province_daily_climate.py --config config/config.yaml
uv run python scripts/21_build_province_chd_from_daily_climate.py --config config/config.yaml
```

The CHD build uses:

- baseline years: 2000-2021
- growth months: June-September
- `hot_day = tmax_c > province-month baseline P90`
- `dry_condition = rolling30_precip_mm < province-month baseline P10`
- `chd_annual = June-September compound_hot_dry_day count`
