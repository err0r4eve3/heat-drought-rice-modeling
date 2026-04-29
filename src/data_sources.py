"""Catalog required external data sources for the heat-drought project."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RequiredDataSource:
    """Metadata for one candidate data source."""

    source_id: str
    title: str
    category: str
    priority: str
    access_level: str
    status: str
    scale: str
    coverage_years: str
    variables: list[str]
    url: str
    license_or_access: str
    fit_for_project: str
    limitations: str
    next_action: str
    evidence_note: str


CSV_FIELDS = [
    "source_id",
    "title",
    "category",
    "priority",
    "access_level",
    "status",
    "scale",
    "coverage_years",
    "variables",
    "url",
    "license_or_access",
    "fit_for_project",
    "limitations",
    "next_action",
    "evidence_note",
]


def build_required_data_sources() -> list[RequiredDataSource]:
    """Return a curated catalog of required data sources and acquisition status."""

    return [
        RequiredDataSource(
            source_id="china_county_statistical_yearbook",
            title="中国县域统计年鉴 / 地方统计年鉴",
            category="yield_panel",
            priority="critical",
            access_level="manual_or_paid",
            status="not_publicly_complete",
            scale="county/prefecture/province",
            coverage_years="2000-2024 main content years; 2025 optional background",
            variables=["rice_sown_area", "rice_production", "rice_yield", "grain_area", "grain_production", "grain_yield"],
            url="https://data.cnki.net/yearbook",
            license_or_access="usually institutional subscription or manual PDF/Excel collection",
            fit_for_project="Best source family for county/prefecture yield outcomes.",
            limitations="No complete open direct-download panel found for 2000-2024 county/prefecture rice yield.",
            next_action="Prioritize target provinces and collect yearbooks/PDF tables into data/raw/statistics.",
            evidence_note="Search found yearbook-derived data references but no authoritative full public download.",
        ),
        RequiredDataSource(
            source_id="nbs_grain_announcements",
            title="国家统计局粮食产量公告",
            category="yield_panel",
            priority="high",
            access_level="open_html",
            status="existing_partial",
            scale="national/province",
            coverage_years="recent annual announcements; useful for 2016-2025 extensions",
            variables=["grain_sown_area", "grain_production", "grain_yield", "rice_national"],
            url="https://www.stats.gov.cn/sj/zxfb/202412/t20241213_1957744.html",
            license_or_access="public NBS web pages",
            fit_for_project="Useful for provincial/national grain panel and national rice checks.",
            limitations="Does not provide complete county/prefecture rice panel.",
            next_action="Keep automated downloader and use as benchmark totals.",
            evidence_note="NBS publishes official area, production, and yield tables for annual grain output.",
        ),
        RequiredDataSource(
            source_id="nbs_data_portal",
            title="国家统计局国家数据",
            category="yield_panel",
            priority="high",
            access_level="dynamic_web",
            status="blocked_or_manual",
            scale="national/province",
            coverage_years="varies by indicator",
            variables=["rice_production", "grain_production", "sown_area", "yield"],
            url="https://data.stats.gov.cn/",
            license_or_access="public portal with dynamic/API constraints",
            fit_for_project="Can cross-check province and national series.",
            limitations="Programmatic access may be blocked or require dynamic query handling.",
            next_action="Use manually exported CSV/XLSX when portal blocks scripted access.",
            evidence_note="Portal is official but not a complete county yield panel source.",
        ),
        RequiredDataSource(
            source_id="nbs_china_statistical_yearbook",
            title="中国统计年鉴 / 国家统计局在线年鉴",
            category="yield_panel",
            priority="medium",
            access_level="open_html_pdf",
            status="candidate",
            scale="national/province",
            coverage_years="online annual yearbooks; mainly 1999-present",
            variables=["agricultural_output", "grain", "rice", "sown_area", "production", "yield"],
            url="https://www.stats.gov.cn/sj/ndsj/",
            license_or_access="public NBS annual yearbook pages",
            fit_for_project="Official source for national/provincial agriculture checks.",
            limitations="Online yearbooks are not a complete county/prefecture rice panel.",
            next_action="Use as benchmark totals and metadata for statistical definitions.",
            evidence_note="NBS online yearbooks mainly summarize national and provincial tables.",
        ),
        RequiredDataSource(
            source_id="nbs_early_rice_announcements",
            title="国家统计局早稻产量公告",
            category="yield_panel",
            priority="medium",
            access_level="open_html",
            status="candidate",
            scale="national/province",
            coverage_years="recent annual early-rice announcements",
            variables=["early_rice_sown_area", "early_rice_production", "early_rice_yield"],
            url="https://www.stats.gov.cn/sj/zxfb/202408/t20240823_1956083.html",
            license_or_access="public NBS web pages",
            fit_for_project="Useful for southern double-cropping-rice robustness and 2024 checks.",
            limitations="Only early rice, not total annual rice; no county/prefecture panel.",
            next_action="Use for validation-event context, not as main outcome panel.",
            evidence_note="NBS publishes early-rice output announcements for recent years.",
        ),
        RequiredDataSource(
            source_id="usda_ers_china_ag_econ_data",
            title="USDA ERS China Agricultural and Economic Data",
            category="yield_panel",
            priority="medium",
            access_level="open_xls",
            status="existing_partial",
            scale="province/national",
            coverage_years="historical; discontinued after older update years",
            variables=["crop_area", "crop_production", "crop_yield"],
            url="https://www.ers.usda.gov/data-products/china-agricultural-and-economic-data/download-the-data",
            license_or_access="public ERS XLS files",
            fit_for_project="Useful historical province-level crop panel seed.",
            limitations="Not county-level and not complete through 2025.",
            next_action="Retain as secondary historical source and document coverage gaps.",
            evidence_note="ERS documentation says provincial data include crop production and related series.",
        ),
        RequiredDataSource(
            source_id="faostat_qcl",
            title="FAOSTAT Crops and Livestock Products",
            category="yield_panel",
            priority="low",
            access_level="open_api_bulk",
            status="candidate",
            scale="national",
            coverage_years="1961-2024 in current production release",
            variables=["harvested_area", "production", "yield"],
            url="https://www.fao.org/faostat/en/#data/QCL",
            license_or_access="public FAOSTAT API/bulk download",
            fit_for_project="National-level rice/grain benchmark and international comparison.",
            limitations="National scale only; cannot solve province/county panel.",
            next_action="Use only for national consistency checks.",
            evidence_note="FAOSTAT production domain covers production volumes, harvested areas, and yields up to 2024.",
        ),
        RequiredDataSource(
            source_id="china_county_statistical_yearbook_official",
            title="中国县域统计年鉴（县市卷、乡镇卷）官方说明",
            category="yield_panel",
            priority="critical",
            access_level="manual_or_paid",
            status="required_collection",
            scale="county/township",
            coverage_years="annual volumes; example 2021 volume contains 2020 data",
            variables=["county_basic_conditions", "agriculture", "economic_indicators"],
            url="https://www.stats.gov.cn/zs/tjwh/tjkw/tjzl/202302/t20230215_1908004.html",
            license_or_access="official printed/statistical yearbook; access via purchase/library/subscription",
            fit_for_project="Authoritative yearbook family for county-scale statistical units.",
            limitations="May not expose all rice area/production/yield variables in every county/year.",
            next_action="Inspect county/city volumes for study-area provinces and build extraction templates.",
            evidence_note="NBS describes the county/city volume as covering 2000+ county units and agriculture-related data.",
        ),
        RequiredDataSource(
            source_id="cnki_csyd",
            title="CNKI / CSYD 中国经济社会大数据研究平台",
            category="yield_panel",
            priority="high",
            access_level="subscription",
            status="candidate",
            scale="yearbook-dependent",
            coverage_years="1949-present for many statistical yearbooks",
            variables=["yearbook_tables", "agriculture", "crop_area", "production", "yield"],
            url="https://www.eastview.com/resources/e-collections/csyd/",
            license_or_access="institutional subscription; table export allowed by platform terms",
            fit_for_project="Best semi-structured route to many official statistical yearbook tables.",
            limitations="Requires subscription and field-level verification; not guaranteed complete for rice outcomes.",
            next_action="If access is available, query target provinces/cities/counties and export tables.",
            evidence_note="CSYD is described as a large repository of official China statistical yearbooks.",
        ),
        RequiredDataSource(
            source_id="eps_data_platform",
            title="EPS 数据平台",
            category="yield_panel",
            priority="medium",
            access_level="subscription_or_trial",
            status="candidate",
            scale="database-dependent",
            coverage_years="platform-dependent",
            variables=["agriculture", "rural", "crop_statistics"],
            url="https://olap.epsnet.com.cn/index.html",
            license_or_access="subscription/trial required",
            fit_for_project="Potential structured statistics source if institutional access exists.",
            limitations="Need live query to confirm county/prefecture rice three-variable coverage.",
            next_action="Check subscribed databases and export coverage report before relying on it.",
            evidence_note="Search found EPS platform references to agriculture/rural statistical databases.",
        ),
        RequiredDataSource(
            source_id="acadcn_county_crop_area",
            title="ACADCN 中国县域尺度农作物播种面积面板数据集",
            category="yield_panel",
            priority="medium",
            access_level="paid_small_dataset",
            status="secondary_candidate",
            scale="county",
            coverage_years="2000-2022",
            variables=["crop_type", "sown_area"],
            url="https://acadcn.cn/7343.html",
            license_or_access="paid download; license must be checked",
            fit_for_project="May help rice area denominators and crop structure.",
            limitations="Area only; does not solve yield or production panel by itself.",
            next_action="Verify provenance/license before use; do not treat as official outcome source.",
            evidence_note="Search result describes county crop sown area observations from provincial yearbooks.",
        ),
        RequiredDataSource(
            source_id="local_statistical_yearbooks_pdf",
            title="地方统计年鉴与统计手册 PDF/Excel",
            category="yield_panel",
            priority="critical",
            access_level="manual_scrape",
            status="required_collection",
            scale="county/prefecture",
            coverage_years="varies; often latest years available as PDF",
            variables=["crop_sown_area", "production", "yield"],
            url="https://www.stats.gov.cn/sj/ndsj/",
            license_or_access="public pages vary by locality; table extraction needed",
            fit_for_project="Only practical open path for city/county validation years if no database is purchased.",
            limitations="Fragmented, inconsistent table layouts and units.",
            next_action="Build province-by-province download/extraction list for study-area counties/cities.",
            evidence_note="Search found examples of local yearbook PDFs containing crop area, production, and yield tables.",
        ),
        RequiredDataSource(
            source_id="asia_rice_yield_4km",
            title="AsiaRiceYield4km",
            category="yield_proxy",
            priority="medium",
            access_level="open_research_data",
            status="candidate",
            scale="4 km grid",
            coverage_years="1995-2015",
            variables=["seasonal_rice_yield"],
            url="https://essd.copernicus.org/articles/15/791/2023/",
            license_or_access="research data via Zenodo; cite source and verify license",
            fit_for_project="Gridded rice yield proxy for historical robustness or method comparison.",
            limitations="Estimated product, not official statistics; no 2016-2025 coverage.",
            next_action="Use only as proxy/benchmark, not as main administrative outcome.",
            evidence_note="ESSD paper documents seasonal rice yield in Asia from 1995 to 2015.",
        ),
        RequiredDataSource(
            source_id="ggcp10",
            title="GGCP10 Global Gridded Crop Production",
            category="yield_proxy",
            priority="medium",
            access_level="open_research_data",
            status="candidate",
            scale="10 km grid",
            coverage_years="2010-2020",
            variables=["rice_production", "maize_production", "wheat_production", "soybean_production"],
            url="https://www.nature.com/articles/s41597-024-04248-2",
            license_or_access="public through Harvard Dataverse DOI; verify reuse license",
            fit_for_project="Rice production proxy for spatial comparison and gap diagnostics.",
            limitations="Ends in 2020 and estimates production, not official county yields.",
            next_action="Use as gridded production proxy after administrative panel is documented.",
            evidence_note="Scientific Data describes annual 10 km gridded production for rice and other crops from 2010 to 2020.",
        ),
        RequiredDataSource(
            source_id="gdhy",
            title="GDHY Global Dataset of Historical Yields",
            category="yield_proxy",
            priority="low",
            access_level="open_research_data",
            status="candidate",
            scale="0.5 degree grid",
            coverage_years="1981-2016",
            variables=["rice_yield", "maize_yield", "wheat_yield", "soybean_yield"],
            url="https://www.nature.com/articles/s41597-020-0433-7",
            license_or_access="PANGAEA research data; verify citation/license",
            fit_for_project="Coarse long-run yield anomaly comparison.",
            limitations="Resolution too coarse for county analysis and no 2017-2025 coverage.",
            next_action="Use only for national/regional sensitivity checks.",
            evidence_note="Scientific Data describes annual crop yield NetCDF files for 1981-2016.",
        ),
        RequiredDataSource(
            source_id="era5_land",
            title="ERA5-Land",
            category="climate",
            priority="critical",
            access_level="requires_registration_api",
            status="ready_for_download",
            scale="grid ~9 km",
            coverage_years="1950-present",
            variables=["t2m", "tmax_derived", "total_precipitation", "soil_temperature", "evaporation"],
            url="https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land",
            license_or_access="Copernicus CDS account and API key",
            fit_for_project="Primary source for daily/hourly heat and precipitation exposure 2000-2024; 2025 is optional background.",
            limitations="Large hourly data volume; needs chunked CDS requests and derived daily maxima.",
            next_action="Implement year/month/bbox CDS requests into data/raw/climate.",
            evidence_note="Official CDS product provides land reanalysis variables suitable for heat metrics.",
        ),
        RequiredDataSource(
            source_id="chirps_daily",
            title="CHIRPS Daily Precipitation",
            category="climate",
            priority="critical",
            access_level="open_http_ftp_rsync",
            status="ready_for_download",
            scale="grid 0.05 degree / 0.25 degree",
            coverage_years="1981-present",
            variables=["precipitation"],
            url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/",
            license_or_access="public CHC data repository; FTP/rsync recommended by CHC",
            fit_for_project="Primary independent precipitation/dryness source for 2000-2024; 2025 is optional background.",
            limitations="p05 daily global files are large; p25 or monthly subsets may be used for MVP checks.",
            next_action="Download by year/month and crop to study_bbox before aggregation.",
            evidence_note="CHC repository lists daily NetCDF files by year and resolution.",
        ),
        RequiredDataSource(
            source_id="terraclimate",
            title="TerraClimate",
            category="climate",
            priority="high",
            access_level="open_netcdf",
            status="ready_for_download",
            scale="grid 1/24 degree monthly",
            coverage_years="1958-present",
            variables=["ppt", "tmax", "tmin", "pet", "aet", "soil", "pdsi", "def"],
            url="https://www.climatologylab.org/terraclimate.html",
            license_or_access="public annual NetCDF files; check current license terms",
            fit_for_project="Monthly climate/water-balance fallback and robustness source.",
            limitations="Monthly only; not suitable for daily hot/dry days without ERA5/CHIRPS.",
            next_action="Use for monthly robustness and drought indicators.",
            evidence_note="TerraClimate publishes individual NetCDF files by variable and year.",
        ),
        RequiredDataSource(
            source_id="gleam",
            title="GLEAM Evaporation and Soil Moisture",
            category="remote_sensing",
            priority="high",
            access_level="requires_registration",
            status="ready_for_download",
            scale="grid 0.1 degree",
            coverage_years="1980-near present; annual extensions",
            variables=["evapotranspiration", "surface_soil_moisture", "root_zone_soil_moisture"],
            url="https://www.gleam.eu/#downloads",
            license_or_access="free download after registration; CC BY noted for GLEAM4 publication",
            fit_for_project="Strong ET and soil-moisture source for drought exposure.",
            limitations="Current releases may lag the latest validation year.",
            next_action="Register/download China subset or annual global files, then crop.",
            evidence_note="GLEAM4 publication reports daily 0.1 degree data from 1980 to near present.",
        ),
        RequiredDataSource(
            source_id="mod13q1",
            title="MODIS MOD13Q1 Vegetation Indices",
            category="remote_sensing",
            priority="critical",
            access_level="requires_earthdata",
            status="ready_for_download",
            scale="250 m 16-day",
            coverage_years="2000-present",
            variables=["NDVI", "EVI", "quality_flags"],
            url="https://lpdaac.usgs.gov/products/mod13q1v061/",
            license_or_access="NASA Earthdata/LP DAAC",
            fit_for_project="Primary NDVI/EVI anomaly source.",
            limitations="Needs QA masking, scale factor, mosaicking, and time compositing.",
            next_action="Use AppEEARS or Earthdata bulk download for study bbox and months.",
            evidence_note="MOD13Q1 provides global 16-day vegetation indices at 250 m.",
        ),
        RequiredDataSource(
            source_id="mod11a2",
            title="MODIS MOD11A2 Land Surface Temperature",
            category="remote_sensing",
            priority="high",
            access_level="requires_earthdata",
            status="ready_for_download",
            scale="1 km 8-day",
            coverage_years="2000-present",
            variables=["LST_Day", "LST_Night", "quality_flags"],
            url="https://lpdaac.usgs.gov/products/mod11a2v061/",
            license_or_access="NASA Earthdata/LP DAAC",
            fit_for_project="Remote-sensing heat stress and VHI TCI component.",
            limitations="Cloud/QA gaps and LST is not air temperature.",
            next_action="Download with QA bands and apply scale factor 0.02 Kelvin.",
            evidence_note="MOD11A2 is the standard 8-day MODIS LST product.",
        ),
        RequiredDataSource(
            source_id="mod16a2",
            title="MODIS MOD16A2 Evapotranspiration",
            category="remote_sensing",
            priority="medium",
            access_level="requires_earthdata",
            status="candidate",
            scale="500 m 8-day",
            coverage_years="2000-present",
            variables=["ET", "PET", "LE", "PLE"],
            url="https://lpdaac.usgs.gov/products/mod16a2v061/",
            license_or_access="NASA Earthdata/LP DAAC",
            fit_for_project="ET anomaly and water-stress robustness.",
            limitations="Algorithm assumptions; validate against GLEAM/TerraClimate.",
            next_action="Use after MOD13Q1/MOD11A2 are stable.",
            evidence_note="LP DAAC distributes MOD16A2 Collection 6.1 products.",
        ),
        RequiredDataSource(
            source_id="smap",
            title="SMAP Soil Moisture",
            category="remote_sensing",
            priority="medium",
            access_level="requires_earthdata",
            status="candidate",
            scale="9-36 km",
            coverage_years="2015-present",
            variables=["surface_soil_moisture", "freeze_thaw"],
            url="https://smap.jpl.nasa.gov/data/",
            license_or_access="NASA NSIDC/ASF data centers",
            fit_for_project="Useful for 2022/2024 validation and soil-moisture comparison.",
            limitations="Does not cover the 2000-2014 baseline period.",
            next_action="Use for event-year validation, not baseline quantiles.",
            evidence_note="SMAP science data products are distributed via NASA-designated data centers.",
        ),
        RequiredDataSource(
            source_id="grace_tellus",
            title="GRACE / GRACE-FO Tellus Mascon TWS",
            category="water",
            priority="medium",
            access_level="open",
            status="candidate",
            scale="1 degree monthly",
            coverage_years="2002-present with mission gaps",
            variables=["terrestrial_water_storage_anomaly"],
            url="https://grace.jpl.nasa.gov/data/get-data/",
            license_or_access="NASA/JPL public products",
            fit_for_project="Regional water-storage anomaly robustness indicator.",
            limitations="Coarse resolution and mission gap; weak for county attribution.",
            next_action="Aggregate to province/basin scale as a robustness covariate.",
            evidence_note="GRACE Tellus data portal lists monthly land water storage grids.",
        ),
        RequiredDataSource(
            source_id="ifpri_spam",
            title="IFPRI SPAM / MapSPAM",
            category="crop_mask",
            priority="medium",
            access_level="open_download",
            status="candidate",
            scale="grid ~5 arcmin",
            coverage_years="benchmark years such as 2000/2005/2010/2017/2020 depending release",
            variables=["crop_area", "production", "yield", "irrigated_rainfed_split"],
            url="https://www.mapspam.info/",
            license_or_access="public data with citation/license conditions",
            fit_for_project="Global gridded rice area/production benchmark and irrigation split.",
            limitations="Benchmark years only; not annual 2000-2024 official panel.",
            next_action="Use as cross-check for rice area proxy and exposure weights.",
            evidence_note="SPAM provides gridded crop area, production, and yield by crop.",
        ),
        RequiredDataSource(
            source_id="ccd_rice",
            title="CCD-Rice / China yearly paddy rice distribution products",
            category="crop_mask",
            priority="high",
            access_level="open_research_data",
            status="candidate",
            scale="30 m",
            coverage_years="1986-2023 in published product",
            variables=["paddy_rice_extent", "cropping_intensity"],
            url="https://essd.copernicus.org/articles/17/2193/2025/",
            license_or_access="research data; verify repository license",
            fit_for_project="Strong candidate for annual rice mask weighting.",
            limitations="Mask only; does not provide statistical yield outcomes.",
            next_action="Download selected years around 2022/2024 and compare with existing mask.",
            evidence_note="ESSD article documents China's yearly paddy rice distribution dataset.",
        ),
        RequiredDataSource(
            source_id="china_rice_calendar",
            title="ChinaRiceCalendar / rice phenology products",
            category="phenology",
            priority="high",
            access_level="open_research_data",
            status="partially_existing",
            scale="grid",
            coverage_years="product-specific",
            variables=["transplanting_doy", "heading_doy", "maturity_doy"],
            url="https://doi.org/10.6084/m9.figshare.24930018",
            license_or_access="research repository; verify license",
            fit_for_project="Supports phenology-specific exposure windows.",
            limitations="May require variable-name mapping and unit checks.",
            next_action="Keep current aggregation and add source-specific metadata parser.",
            evidence_note="Current project already detects phenology rasters when present.",
        ),
        RequiredDataSource(
            source_id="gadm_boundaries",
            title="GADM China Administrative Boundaries",
            category="boundary",
            priority="medium",
            access_level="open_noncommercial",
            status="candidate",
            scale="admin 0-3",
            coverage_years="current version snapshots",
            variables=["geometry", "admin_names"],
            url="https://gadm.org/data.html",
            license_or_access="free for academic/non-commercial use; redistribution restricted",
            fit_for_project="Usable fallback boundary for maps and zonal stats.",
            limitations="May not match Chinese statistical codes or annual boundary changes.",
            next_action="Use with explicit license notes and add admin-code crosswalk.",
            evidence_note="GADM provides downloadable country administrative spatial data.",
        ),
        RequiredDataSource(
            source_id="ngcc_county_boundary",
            title="National Geomatics Center of China county boundary vector map",
            category="boundary",
            priority="high",
            access_level="open_reposted_or_catalog",
            status="candidate",
            scale="county",
            coverage_years="snapshot",
            variables=["geometry", "admin_code", "admin_name"],
            url="https://figshare.com/articles/code/China_s_vector_map_of_administrative_boundary_at_county_level/13019930",
            license_or_access="license and original catalog terms must be verified",
            fit_for_project="Closer to China county-code workflows than GADM if licensing is acceptable.",
            limitations="May be a repost; verify provenance before redistribution.",
            next_action="Use only after confirming license and matching statistical codes.",
            evidence_note="Figshare record attributes the vector map to National Geomatics Center of China.",
        ),
        RequiredDataSource(
            source_id="cma_data",
            title="中国气象数据网 / CMA",
            category="climate",
            priority="medium",
            access_level="requires_registration",
            status="candidate",
            scale="station/grid",
            coverage_years="varies by product",
            variables=["temperature", "precipitation", "drought_indices", "station_observations"],
            url="https://data.cma.cn/",
            license_or_access="registration and product-specific access limits",
            fit_for_project="Official China station/grid benchmark for event validation.",
            limitations="Access may require approval and station extraction work.",
            next_action="Use for validation once account access is available.",
            evidence_note="CMA is the official meteorological data portal for China.",
        ),
        RequiredDataSource(
            source_id="viirs_nightlights",
            title="VIIRS Nighttime Lights",
            category="covariates",
            priority="low",
            access_level="requires_registration",
            status="candidate",
            scale="grid",
            coverage_years="2012-present",
            variables=["nightlight_radiance"],
            url="https://eogdata.mines.edu/products/vnl/",
            license_or_access="registration required",
            fit_for_project="Socioeconomic covariate and robustness control.",
            limitations="Not available for full 2000-2011 baseline.",
            next_action="Add only after core exposure/outcome data are complete.",
            evidence_note="EOG distributes VIIRS nighttime light products.",
        ),
        RequiredDataSource(
            source_id="soilgrids",
            title="SoilGrids",
            category="soil",
            priority="low",
            access_level="open_api",
            status="candidate",
            scale="250 m",
            coverage_years="static covariates",
            variables=["soil_texture", "organic_carbon", "bulk_density", "available_water"],
            url="https://soilgrids.org/",
            license_or_access="open data/API; cite ISRIC",
            fit_for_project="Static soil controls and drought sensitivity heterogeneity.",
            limitations="Static global predictions; not annual moisture observations.",
            next_action="Add after yield and exposure coverage risks are reduced.",
            evidence_note="SoilGrids provides global gridded soil property predictions.",
        ),
    ]


def write_data_source_outputs(references_dir: str | Path, reports_dir: str | Path) -> dict[str, Path]:
    """Write the data-source catalog CSV/JSON and Markdown report."""

    references = Path(references_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    references.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    sources = build_required_data_sources()
    csv_path = references / "deep_required_data_sources.csv"
    json_path = references / "deep_required_data_sources.json"
    report_path = reports / "deep_data_search_report.md"

    rows = [_csv_row(source) for source in sources]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "conclusion": "未找到完整公开县/市级 2000-2024 内容年份水稻单产面板；需年鉴/地方 PDF/付费库补齐。",
        "sources": [_json_row(source) for source in sources],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(render_data_search_report(sources), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "report": report_path}


def render_data_search_report(sources: list[RequiredDataSource]) -> str:
    """Render a Markdown report from the source catalog."""

    counts_by_category = _count_by(sources, "category")
    counts_by_access = _count_by(sources, "access_level")
    yield_sources = [source for source in sources if source.category == "yield_panel"]
    yield_proxy_sources = [source for source in sources if source.category == "yield_proxy"]
    direct_yield = [
        source
        for source in yield_sources
        if source.access_level in {"open_html", "open_xls", "open_api_bulk", "open_html_pdf"}
        and source.status in {"existing_partial", "ready_for_download", "candidate"}
    ]
    critical_sources = [source for source in sources if source.priority == "critical"]

    lines = [
        "# Deep Data Search Report",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Catalog size: {len(sources)} sources",
        "",
        "## 结论",
        "",
        "- 未找到完整公开县/市级 2000-2024 内容年份水稻单产面板。",
        "- 当前可自动补齐的是省级/全国粮食或历史省级作物面板；县/市级水稻单产仍需中国县域统计年鉴、地方统计年鉴、统计公报 PDF/Excel、CNKI/EPS 等年鉴库或人工整理。",
        "- 2000-2024 气象、降水、遥感、稻田掩膜和边界数据有可执行来源；2025 只作为背景或补充更新。",
        "",
        "## 数据源覆盖",
        "",
        "| 分组 | 数量 |",
        "| --- | ---: |",
        *_format_counts(counts_by_category),
        "",
        "## 访问方式",
        "",
        "| 访问方式 | 数量 |",
        "| --- | ---: |",
        *_format_counts(counts_by_access),
        "",
        "## 产量面板重点判断",
        "",
        f"- yield_panel 候选源：{len(yield_sources)} 个。",
        f"- 公开且可直接脚本化的产量源：{len(direct_yield)} 个，但均不是完整县/市级水稻面板。",
        f"- 栅格产量/单产代理源：{len(yield_proxy_sources)} 个，只可用于稳健性、空间对照或缺口诊断。",
        "- 关键缺口：`rice_sown_area`、`rice_production`、`rice_yield` 的县/市级 2000-2024 内容年份连续序列。",
        "",
        "| source_id | 尺度 | 年份 | 访问 | 状态 | 主要限制 | 下一步 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for source in yield_sources:
        lines.append(
            f"| {source.source_id} | {source.scale} | {source.coverage_years} | "
            f"{source.access_level} | {source.status} | {source.limitations} | {source.next_action} |"
        )

    lines.extend(
        [
            "",
            "## 栅格产量/单产代理",
            "",
            "| source_id | title | 尺度 | 年份 | 变量 | 主要限制 | 用法 |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for source in yield_proxy_sources:
        lines.append(
            f"| {source.source_id} | {source.title} | {source.scale} | {source.coverage_years} | "
            f"{'; '.join(source.variables)} | {source.limitations} | {source.fit_for_project} |"
        )

    lines.extend(
        [
            "",
            "## 关键数据源清单",
            "",
            "| source_id | category | priority | access | status | url |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for source in critical_sources:
        lines.append(
            f"| {source.source_id} | {source.category} | {source.priority} | "
            f"{source.access_level} | {source.status} | {source.url} |"
        )

    lines.extend(
        [
            "",
            "## 建议执行顺序",
            "",
            "1. 先采购或人工整理研究区县/市级水稻产量、播种面积、单产面板，并保留原始年鉴/PDF 到 `data/raw/statistics/`。",
            "2. 用 ERA5-Land + CHIRPS 补齐 2000-2024 日尺度热旱暴露，按 `study_bbox` 分年分月下载，避免一次性拉取全球大文件；2025 只作背景补充。",
            "3. 用 MOD13Q1/MOD11A2/GLEAM 构造遥感胁迫和土壤水分/ET 交叉验证。",
            "4. 用 CCD-Rice 或已有稻田掩膜做县级稻田像元加权聚合；SPAM 只作为面积/产量基准年交叉检查。",
            "5. 建立行政区划代码与名称跨年映射，记录撤县设区和市辖区口径变化。",
            "",
            "## 机器可读输出",
            "",
            "- `data/raw/references/deep_required_data_sources.csv`",
            "- `data/raw/references/deep_required_data_sources.json`",
            "",
        ]
    )
    return "\n".join(lines)


def summarize_sources_from_csv(path: str | Path) -> dict[str, Any]:
    """Summarize a written source catalog CSV."""

    csv_path = Path(path)
    if not csv_path.exists():
        return {"rows": 0, "categories": {}, "access_levels": {}, "critical": 0}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))
    return {
        "rows": len(rows),
        "categories": _count_rows(rows, "category"),
        "access_levels": _count_rows(rows, "access_level"),
        "critical": sum(1 for row in rows if row.get("priority") == "critical"),
    }


def _csv_row(source: RequiredDataSource) -> dict[str, str]:
    """Convert a source to a CSV-safe row."""

    row = asdict(source)
    row["variables"] = "; ".join(source.variables)
    return {field: str(row.get(field, "")) for field in CSV_FIELDS}


def _json_row(source: RequiredDataSource) -> dict[str, Any]:
    """Convert a source to a JSON row."""

    return asdict(source)


def _count_by(sources: list[RequiredDataSource], field: str) -> dict[str, int]:
    """Count sources by a dataclass field."""

    counts: dict[str, int] = {}
    for source in sources:
        value = str(getattr(source, field))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _count_rows(rows: list[dict[str, str]], field: str) -> dict[str, int]:
    """Count CSV rows by a field."""

    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(field, "")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _format_counts(counts: dict[str, int]) -> list[str]:
    """Format count rows for Markdown."""

    return [f"| {key} | {value} |" for key, value in sorted(counts.items())]
