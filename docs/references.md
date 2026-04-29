# References And Evidence Notes

Generated for NotebookLM ingestion on 2026-04-28.

## Core Outcome Data References

### National Bureau of Statistics grain output announcements

- URL: https://www.stats.gov.cn/sj/zxfb/202412/t20241213_1957744.html
- Role: official recent grain area, production, and yield benchmark.
- Scale: national and provincial grain; national rice details appear in some announcements.
- Limitation: not a complete county/prefecture rice panel.

### National Bureau of Statistics online yearbooks

- URL: https://www.stats.gov.cn/sj/ndsj/
- Role: official annual statistical definitions and national/provincial agriculture tables.
- Scale: mainly national and provincial.
- Limitation: not a full county/prefecture rice panel.

### China County Statistical Yearbook official description

- URL: https://www.stats.gov.cn/zs/tjwh/tjkw/tjzl/202302/t20230215_1908004.html
- Role: authoritative yearbook family for county-scale socio-economic and agriculture data.
- Evidence note: the NBS description says the county/city volume covers more than 2000 county units and agriculture-related data.
- Limitation: access is manual/paid/library/subscription, and rice variables must be checked volume by volume.

### CNKI / CSYD China Statistical Yearbooks

- URL: https://www.eastview.com/resources/e-collections/csyd/
- Role: subscription route to official statistical yearbook tables.
- Coverage: described as a large repository of official China statistical yearbooks from 1949 onward.
- Limitation: requires institutional access and field-level verification for county/prefecture rice variables.

### USDA ERS China Agricultural and Economic Data

- URL: https://www.ers.usda.gov/data-products/china-agricultural-and-economic-data/download-the-data
- Documentation: https://www.ers.usda.gov/data-products/china-agricultural-and-economic-data/documentation
- Role: historical province/national crop panel seed.
- Limitation: not county-level and not complete through 2025.

### FAOSTAT Crops and Livestock Products

- URL: https://www.fao.org/faostat/en/#data/QCL
- Release note: https://www.fao.org/statistics/highlights-archive/highlights-detail/agricultural-production-statistics-2010-2024/en
- Role: national-level rice/grain benchmark and international comparison.
- Limitation: national scale only; does not solve province/county panel.

## Climate And Drought References

### ERA5-Land

- URL: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
- Role: primary heat and precipitation exposure source for 2000-2025.
- Relevant variables: 2m temperature, derived daily maximum temperature, total precipitation, land-surface variables.
- Limitation: CDS registration/API key required; hourly data volume is large and must be chunked by year/month/bbox.

### CHIRPS daily precipitation

- URL: https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/
- Repository homepage: https://data.chc.ucsb.edu/
- Role: independent precipitation/dryness source.
- Coverage: 1981-present daily precipitation.
- Limitation: 0.05-degree daily global files are large; monthly or 0.25-degree files may be used for quick checks.

### TerraClimate

- URL: https://www.climatologylab.org/terraclimate.html
- Role: monthly climate and water-balance robustness source.
- Variables: precipitation, maximum/minimum temperature, PET, AET, soil moisture, PDSI-like indicators.
- Limitation: monthly only; daily hot/dry day counts still require daily data.

### CMA China Meteorological Data Service

- URL: https://data.cma.cn/
- Role: official China station/grid benchmark.
- Limitation: registration, approval, and product-specific access constraints may apply.

## Remote Sensing And Water References

### MOD13Q1

- URL: https://lpdaac.usgs.gov/products/mod13q1v061/
- Role: NDVI/EVI anomaly source.
- Coverage: MODIS 16-day vegetation indices, 250 m.
- Limitation: requires QA masking, scale factors, mosaicking, and Earthdata/AppEEARS access.

### MOD11A2

- URL: https://lpdaac.usgs.gov/products/mod11a2v061/
- Role: land-surface-temperature stress and VHI/TCI component.
- Limitation: cloud/QA gaps; LST is not air temperature.

### MOD16A2

- URL: https://lpdaac.usgs.gov/products/mod16a2v061/
- Role: evapotranspiration anomaly source.
- Limitation: algorithmic assumptions; should be checked against GLEAM/TerraClimate.

### GLEAM

- URL: https://www.gleam.eu/#downloads
- Role: evapotranspiration and soil moisture source.
- Limitation: may lag latest validation year depending on release.

### SMAP

- URL: https://smap.jpl.nasa.gov/data/
- Role: soil-moisture comparison for 2022/2024.
- Limitation: begins in 2015, so it cannot support full 2000-2014 baseline quantiles.

### GRACE Tellus

- URL: https://grace.jpl.nasa.gov/data/get-data/
- Role: monthly terrestrial water storage anomaly robustness covariate.
- Limitation: coarse spatial resolution and mission gaps.

## Crop Mask, Phenology, And Boundary References

### CCD-Rice

- URL: https://essd.copernicus.org/articles/17/2193/2025/
- Role: annual paddy-rice distribution candidate for rice-mask weighting.
- Limitation: mask only; no official yield outcome.

### ChinaRiceCalendar

- URL: https://doi.org/10.6084/m9.figshare.24930018
- Role: phenology-specific exposure windows.
- Limitation: variable names and units must be checked against local files.

### IFPRI SPAM / MapSPAM

- URL: https://www.mapspam.info/
- Role: benchmark crop area, production, yield, and irrigated/rainfed splits for selected years.
- Limitation: benchmark years only, not annual 2000-2025.

### GADM

- URL: https://gadm.org/data.html
- Role: fallback administrative boundary data.
- Limitation: non-commercial/academic license constraints and potential mismatch with Chinese statistical codes.

### National Geomatics Center county boundary vector map repost

- URL: https://figshare.com/articles/code/China_s_vector_map_of_administrative_boundary_at_county_level/13019930
- Role: candidate China county boundary vector source.
- Limitation: provenance, redistribution terms, and code matching must be verified before use.

## Yield Proxy References

### AsiaRiceYield4km

- URL: https://essd.copernicus.org/articles/15/791/2023/
- Data DOI: https://doi.org/10.5281/zenodo.6901968
- Role: gridded seasonal rice-yield proxy for historical robustness or method comparison.
- Coverage: 1995-2015.
- Limitation: estimated product, not official statistics; no 2016-2025 coverage.

### GGCP10

- URL: https://www.nature.com/articles/s41597-024-04248-2
- Data DOI: https://doi.org/10.7910/DVN/G1HBNK
- Role: rice production proxy for spatial comparison and gap diagnostics.
- Coverage: 2010-2020, 10 km global grid.
- Limitation: estimated production, not official county yields; no 2021-2025 coverage.

### GDHY

- URL: https://www.nature.com/articles/s41597-020-0433-7
- Data DOI: https://doi.org/10.1594/PANGAEA.909132
- Role: coarse long-run yield anomaly comparison.
- Coverage: 1981-2016, 0.5-degree grid.
- Limitation: too coarse for county-level attribution and no 2017-2025 coverage.

