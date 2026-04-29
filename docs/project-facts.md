# Project Facts

Generated for NotebookLM ingestion on 2026-04-28.

## Project Scope

- Project theme: compound heat-drought shock effects on rice yield and yield stability in the middle and lower Yangtze River region.
- Main event: 2022 extreme heat and drought.
- Validation event: 2024 summer-autumn drought.
- Target statistical unit in configuration: county or prefecture, with current config targeting county workflows.
- Study bbox: `[105, 24, 123, 35]`.
- CRS defaults: WGS84 `EPSG:4326`; equal-area analysis `EPSG:6933`.
- Baseline years: 2000-2021.
- Main event months: June-October.
- Rice growth months: June-September.
- Heat threshold: 0.90 quantile.
- Drought threshold: 0.10 quantile.

## Current Data State

- Data inventory records: 24.
- Processed administrative units: 1242.
- Rice/crop-mask zonal statistics: 999 of 1242 administrative units have zonal-stat results.
- Rice area proxy total: about 12.86 million hectares.
- Phenology zonal windows: 1160 of 1242 administrative units have phenology zonal-stat results.
- Province-level growing-season climate panel: 291 rows.
- Province-level remote-sensing growing-season panel: 45 rows.
- Combined yield panel: 871 rows for 2000-2025.
- Current yield panel administrative levels: 857 province-level rows and 14 national rows.
- Current yield panel crop types: 630 grain rows and 241 rice rows.
- Current model panel: 871 rows for 2000-2025 across 32 province-level units.
- Nonmissing `yield_anomaly_pct`: 596 of 871 rows.
- Nonmissing `exposure_index`: 19 of 871 rows.
- County-level yield proxy framework has been added as a pipeline step named `yield-proxy`.
- AsiaRiceYield4km Version1 and GGCP10 Rice GeoTIFFs for 2010-2020 have been downloaded under `data/raw/statistics/yield_proxy`.
- The yield-proxy download manifest contains 12 planned/downloaded source files: one AsiaRiceYield4km archive and eleven GGCP10 rice GeoTIFFs.
- AsiaRiceYield4km archive extraction produced 126 GeoTIFF members.
- Province assignment used geoBoundaries ADM1 and matched 1236 of 1242 administrative units.
- Current county-level yield proxy panel has 33534 rows for 1242 administrative units over 2000-2020.
- Current yield-proxy panel sources: 19872 rows from `asia_rice_yield_4km` and 13662 rows from `ggcp10`.
- Current yield-proxy gap report covers 26082 admin-year cells and currently has 0 missing admin-year cells.
- Province-level rice calibration is available for 2460 proxy rows. Remaining rows are uncalibrated because compatible official rice statistics or valid coverage checks are unavailable.

## Current 2022 Event Outputs

- 2022 `exposure_index`: 19 nonmissing rows, mean 0.2385, minimum -2.439, maximum 2.07.
- Highest current 2022 exposure rows are grain observations for Henan, Ningxia, Jilin, Liaoning, and Yunnan.
- 2022 `yield_anomaly_pct`: 29 nonmissing rows, mean 28.71, minimum 3.174, maximum 79.46.
- Lowest current 2022 yield-anomaly rows are grain observations for Henan, Shandong, Sichuan, Hebei, and Jilin.

## Model Outputs

- Model coefficient rows: 2.
- Main exposure coefficient currently reported as `estimate=-4.37291, n=19, R2=0.15516`.
- Event-study `event_time_0` currently reported as `estimate=16.6278, n=596, R2=0.014822`.
- Event-study coefficient rows: 8.
- Robustness result rows: 3.
- Placebo result rows: 4.

## Fixed or Reduced Risks

- Crop mask processing now performs administrative-unit zonal statistics instead of only metadata checks.
- Phenology processing now aggregates ChinaRiceCalendar-style DOY rasters where available and keeps default month fallback.
- ERS English province names are normalized to Chinese province names for joining with NBS-style province panels.
- Index construction now creates trend yield, yield anomaly, stability indicators, and exposure index.
- Modeling handles constant predictors and province-level panels without `admin_id`.
- Deep data-source search is codified as machine-readable CSV/JSON and Markdown report.
- Yield-proxy panel construction now writes an empty schema, gap report, and Markdown diagnostics when open gridded yield/proxy rasters are missing.

## Remaining High-Value Risks

- No complete public direct-download 2000-2025 county/prefecture rice area-production-yield panel has been found.
- Current outcome panel is mainly province-level grain/rice, while the intended target unit is county/prefecture; county-level causal interpretation is not supported yet.
- Current exposure coverage is sparse: `exposure_index` is nonmissing for 19 of 871 model rows.
- 2024 validation event still lacks complete processed climate, remote-sensing, and yield outcomes.
- County-level paddy-weighted exposure aggregation remains a priority.
- County-level yield-proxy aggregation now has open gridded inputs, but it remains a proxy panel and still requires official county/prefecture rice statistics for main causal outcome claims.
- Cross-year administrative-code and name mapping is still needed for county/prefecture panels.

## Local Source Files

- `reports/final_analysis_summary.md`
- `reports/project_risk_assessment.md`
- `reports/yield_proxy_panel_summary.md`
- `reports/yield_proxy_download_summary.md`
- `reports/admin_province_assignment_summary.md`
- `data/processed/yield_proxy/county_yield_proxy_panel.csv`
- `data/processed/yield_proxy/yield_proxy_gap_report.csv`
- `reports/deep_data_search_report.md`
- `data/raw/references/deep_required_data_sources.csv`
- `data/raw/references/deep_required_data_sources.json`
