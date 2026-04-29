"""Download and normalize external agricultural yield sources."""

from __future__ import annotations

import csv
import json
import re
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable


ERS_PROVINCIAL_URL = "https://www.ers.usda.gov/sites/default/files/images/provincialdata.xls"
ERS_NATIONAL_URL = "https://www.ers.usda.gov/sites/default/files/images/nationaldata.xls"

NBS_GRAIN_ANNOUNCEMENT_URLS: dict[int, str] = {
    2016: "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1899351.html",
    2017: "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1899753.html",
    2018: "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1900173.html",
    2019: "https://www.stats.gov.cn/xxgk/sjfb/zxfb2020/201912/t20191206_1767560.html",
    2020: "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1900927.html",
    2021: "https://www.stats.gov.cn/xxgk/sjfb/zxfb2020/202112/t20211206_1825071.html",
    2022: "https://tjj.shaanxi.gov.cn/tjsj/tjxx/qg/202212/t20221214_2268894.html",
    2023: "https://www.stats.gov.cn/sj/zxfb/202312/t20231211_1945417.html",
    2024: "https://www.stats.gov.cn/sj/zxfb/202412/t20241213_1957744.html",
    2025: "https://www.stats.gov.cn/sj/zxfb/202512/t20251212_1962049.html",
}

YIELD_SOURCE_COLUMNS = [
    "source_id",
    "title",
    "url",
    "agency",
    "province",
    "scale",
    "coverage_years",
    "variables",
    "access_method",
    "access_type",
    "usable_tier",
    "limitations",
    "local_path",
    "status",
    "error_message",
]

EXTERNAL_YIELD_PANEL_COLUMNS = [
    "year",
    "province",
    "prefecture",
    "county",
    "admin_code",
    "admin_level",
    "crop",
    "sown_area_hectare",
    "harvested_area_hectare",
    "production_ton",
    "yield_kg_per_hectare",
    "source",
    "source_file",
    "quality_flag",
]

NBS_PROVINCE_NAMES = [
    "全国总计",
    "北京",
    "天津",
    "河北",
    "山西",
    "内蒙古",
    "辽宁",
    "吉林",
    "黑龙江",
    "上海",
    "江苏",
    "浙江",
    "安徽",
    "福建",
    "江西",
    "山东",
    "河南",
    "湖北",
    "湖南",
    "广东",
    "广西",
    "海南",
    "重庆",
    "四川",
    "贵州",
    "云南",
    "西藏",
    "陕西",
    "甘肃",
    "青海",
    "宁夏",
    "新疆",
]

PROVINCE_EN_TO_ZH = {
    "anhui": "安徽",
    "beijing": "北京",
    "chongqing": "重庆",
    "fujian": "福建",
    "gansu": "甘肃",
    "guangdong": "广东",
    "guangxi": "广西",
    "guizhou": "贵州",
    "hainan": "海南",
    "hebei": "河北",
    "heilongjiang": "黑龙江",
    "henan": "河南",
    "hubei": "湖北",
    "hunan": "湖南",
    "inner mongolia": "内蒙古",
    "jiangsu": "江苏",
    "jiangxi": "江西",
    "jilin": "吉林",
    "liaoning": "辽宁",
    "ningxia": "宁夏",
    "qinghai": "青海",
    "shaanxi": "陕西",
    "shandong": "山东",
    "shanghai": "上海",
    "shanxi": "山西",
    "sichuan": "四川",
    "tianjin": "天津",
    "tibet": "西藏",
    "xinjiang": "新疆",
    "yunnan": "云南",
    "zhejiang": "浙江",
}

LOCAL_YEARBOOK_SOURCE_LEADS: list[dict[str, Any]] = [
    {
        "source_id": "nbs_statistical_yearbook_index",
        "title": "National Bureau of Statistics China Statistical Yearbook index",
        "url": "https://www.stats.gov.cn/sj/ndsj/",
        "agency": "国家统计局",
        "province": "全国",
        "scale": "national/province",
        "coverage_years": "1999-2025 publication years; content year depends on table",
        "variables": "national/provincial agriculture tables; definitions and macro benchmarks",
        "access_method": "HTML index cache",
        "access_type": "official_free",
        "usable_tier": "tier_3_or_background",
        "limitations": "not a county/prefecture rice panel; use for definitions, national/provincial checks",
        "local_path": "",
        "status": "planned",
        "error_message": "",
    },
    {
        "source_id": "zhejiang_statistical_yearbook_2024_index",
        "title": "Zhejiang Statistical Yearbook 2024 online index",
        "url": "https://zjjcmspublic.oss-cn-hangzhou-zwynet-d01-a.internet.cloud.zj.gov.cn/jcms_files/jcms1/web3077/site/flash/tjj/Reports1/2024%E6%B5%99%E6%B1%9F%E7%BB%9F%E8%AE%A1%E5%B9%B4%E9%89%B4/indexcn.html",
        "agency": "浙江省统计局",
        "province": "浙江",
        "scale": "province/prefecture",
        "coverage_years": "2024 yearbook; content year must be verified per table",
        "variables": "major crop area, production, yield; city grain area and production",
        "access_method": "HTML index cache",
        "access_type": "official_free",
        "usable_tier": "tier_2_candidate",
        "limitations": "city grain tables are usable leads; city rice area/production/yield must be verified before tier_1",
        "local_path": "",
        "status": "planned",
        "error_message": "",
    },
    {
        "source_id": "hubei_statistical_yearbook_index",
        "title": "Hubei statistical yearbook index",
        "url": "https://tjj.hubei.gov.cn/tjsj/sjkscx/tjnj/qstjnj/",
        "agency": "湖北省统计局",
        "province": "湖北",
        "scale": "province/prefecture/county candidate",
        "coverage_years": "2016-2025 publication years on official index",
        "variables": "province yearbooks and city/county yearbook leads for grain/rice tables",
        "access_method": "HTML index cache",
        "access_type": "official_free",
        "usable_tier": "tier_1_or_tier_2_candidate_after_table_extraction",
        "limitations": "requires downloading/OCRing city/county PDFs or archives and verifying content years",
        "local_path": "",
        "status": "planned",
        "error_message": "",
    },
    {
        "source_id": "hunan_statistical_yearbook_index",
        "title": "Hunan statistical yearbook and city-yearbook index",
        "url": "https://tjj.hunan.gov.cn/hntj/tjfx/hntjnj/hntjnjwlb/index.html",
        "agency": "湖南省统计局",
        "province": "湖南",
        "scale": "province/prefecture/county candidate",
        "coverage_years": "2008-2025 publication years on official index",
        "variables": "province and city yearbook leads for grain/rice area and production tables",
        "access_method": "HTML index cache",
        "access_type": "official_free",
        "usable_tier": "tier_1_or_tier_2_candidate_after_table_extraction",
        "limitations": "city pages vary by site; rice-specific fields must be verified before main model",
        "local_path": "",
        "status": "planned",
        "error_message": "",
    },
    {
        "source_id": "csyd_subscription_yearbook_database",
        "title": "CNKI/CSYD China statistical yearbook database",
        "url": "https://www.eastview.com/resources/e-collections/csyd/",
        "agency": "CNKI/CSYD via institutional access",
        "province": "多省",
        "scale": "county/prefecture/province depending export",
        "coverage_years": "1949-present stated coverage; export table years must be verified",
        "variables": "official yearbook tables exportable as XLS when subscription is available",
        "access_method": "subscription metadata cache",
        "access_type": "subscription",
        "usable_tier": "tier_1_or_tier_2_candidate_after_export_qc",
        "limitations": "requires institutional subscription; exported tables must keep source metadata and units",
        "local_path": "",
        "status": "planned",
        "error_message": "",
    },
    {
        "source_id": "yougis_county_yearbook_lead",
        "title": "YouGIS county statistical yearbook compiled data lead",
        "url": "https://blog.yougis.com.cn/archives/China-County-Statistical-Yearbook-Data-2000-2023-Summary-21-Categories-of-County-Data-Free-Access",
        "agency": "third-party lead",
        "province": "全国",
        "scale": "county",
        "coverage_years": "claimed 2000-2023",
        "variables": "crop sown area and product output fields claimed by third party",
        "access_method": "third-party metadata cache",
        "access_type": "third_party_lead",
        "usable_tier": "search_clue_only",
        "limitations": "not official source; 2024 missing; do not use as main model source without official-table backtrace",
        "local_path": "",
        "status": "planned",
        "error_message": "",
    },
]


@dataclass(frozen=True)
class YieldSourceDownloadResult:
    """Result metadata for external yield source download and normalization."""

    status: str
    downloaded_files: list[Path] = field(default_factory=list)
    panel_rows: int = 0
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/yield_sources_summary.md")


def build_yield_source_catalog() -> list[dict[str, Any]]:
    """Build a reproducible catalog of external yield panel sources."""

    records = [
        {
            "source_id": "ers_china_provincial",
            "title": "USDA ERS China Agricultural and Economic Data - Provincial Data",
            "url": ERS_PROVINCIAL_URL,
            "agency": "USDA ERS",
            "province": "全国分省",
            "scale": "province",
            "coverage_years": "1963-2007; crop-specific availability varies",
            "variables": "rice/grain production and sown area; derived yield",
            "access_method": "direct XLS download",
            "access_type": "public_research_data",
            "usable_tier": "tier_3_or_background",
            "limitations": "discontinued; does not cover 2008-2025",
            "local_path": "",
            "status": "planned",
            "error_message": "",
        },
        {
            "source_id": "ers_china_national",
            "title": "USDA ERS China Agricultural and Economic Data - National Data",
            "url": ERS_NATIONAL_URL,
            "agency": "USDA ERS",
            "province": "全国",
            "scale": "national",
            "coverage_years": "1949-2008; item-specific",
            "variables": "national crop production and macro/agricultural indicators",
            "access_method": "direct XLS download",
            "access_type": "public_research_data",
            "usable_tier": "background_only",
            "limitations": "national scale only; discontinued",
            "local_path": "",
            "status": "planned",
            "error_message": "",
        },
    ]
    for year, url in sorted(NBS_GRAIN_ANNOUNCEMENT_URLS.items()):
        records.append(
            {
                "source_id": f"nbs_grain_announcement_{year}",
                "title": f"NBS grain production announcement {year}",
                "url": url,
                "agency": "国家统计局",
                "province": "全国分省",
                "scale": "province",
                "coverage_years": str(year),
                "variables": "grain sown area, production, yield",
                "access_method": "HTML scrape with text parser",
                "access_type": "official_free",
                "usable_tier": "tier_3_or_background",
                "limitations": "province table is grain total, not province-level rice by variety",
                "local_path": "",
                "status": "planned",
                "error_message": "",
            }
        )
    records.extend(dict(record) for record in LOCAL_YEARBOOK_SOURCE_LEADS)
    return records


def download_and_build_yield_sources(
    statistics_dir: str | Path,
    references_dir: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    force: bool = False,
    timeout_seconds: int = 120,
    year_min: int = 2000,
    year_max: int = 2024,
) -> YieldSourceDownloadResult:
    """Download small external yield sources and build normalized province panels."""

    statistics_root = Path(statistics_dir).expanduser().resolve()
    references = Path(references_dir).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    source_dir = statistics_root / "external_yield_sources"
    lead_dir = statistics_root / "local_yearbook_leads"
    for directory in (source_dir, lead_dir, references, processed, reports):
        directory.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    catalog = build_yield_source_catalog()
    downloaded_files: list[Path] = []

    ers_provincial = source_dir / "ers_china_provincialdata.xls"
    ers_national = source_dir / "ers_china_nationaldata.xls"
    _download_file(ERS_PROVINCIAL_URL, ers_provincial, force, timeout_seconds, downloaded_files, warnings)
    _download_file(ERS_NATIONAL_URL, ers_national, force, timeout_seconds, downloaded_files, warnings)
    _update_catalog_local_status(catalog, "ers_china_provincial", ers_provincial)
    _update_catalog_local_status(catalog, "ers_china_national", ers_national)

    nbs_rows: list[dict[str, Any]] = []
    for year, url in sorted(NBS_GRAIN_ANNOUNCEMENT_URLS.items()):
        html_path = source_dir / f"nbs_grain_announcement_{year}.html"
        _download_file(url, html_path, force, timeout_seconds, downloaded_files, warnings)
        _update_catalog_local_status(catalog, f"nbs_grain_announcement_{year}", html_path)
        if html_path.exists():
            text = html_to_text(html_path.read_text(encoding="utf-8", errors="replace"))
            parsed_rows = parse_nbs_grain_announcement_text(text, year=year, source_url=url)
            if parsed_rows:
                nbs_rows.extend(parsed_rows)
            else:
                warnings.append(f"No NBS province grain rows parsed for {year}: {url}")

    ers_rows: list[dict[str, Any]] = []
    if ers_provincial.exists():
        ers_rows = normalize_ers_provincial_workbook(ers_provincial)

    for lead in LOCAL_YEARBOOK_SOURCE_LEADS:
        lead_path = lead_dir / f"{lead['source_id']}.html"
        _download_file(str(lead["url"]), lead_path, force, timeout_seconds, downloaded_files, warnings)
        _update_catalog_local_status(catalog, str(lead["source_id"]), lead_path)

    panel_rows = sorted(
        [
            row
            for row in [*ers_rows, *nbs_rows]
            if year_min <= int(row["year"]) <= year_max
        ],
        key=lambda row: (int(row["year"]), str(row["source"]), str(row["province"]), str(row["crop"])),
    )
    panel_csv = processed / "yield_panel_external_province.csv"
    panel_parquet = processed / "yield_panel_external_province.parquet"
    combined_csv = processed / "yield_panel_combined.csv"
    combined_parquet = processed / "yield_panel_combined.parquet"
    catalog_csv = references / "agri_stats_sources.csv"
    catalog_json = references / "agri_stats_sources.json"
    local_rows = _read_existing_yield_panel(processed / "yield_panel.csv", warnings)
    combined_rows = combine_yield_panel_rows(panel_rows, local_rows)
    _write_csv_rows(panel_rows, EXTERNAL_YIELD_PANEL_COLUMNS, panel_csv)
    _write_parquet_if_possible(panel_rows, EXTERNAL_YIELD_PANEL_COLUMNS, panel_parquet, warnings)
    _write_csv_rows(combined_rows, EXTERNAL_YIELD_PANEL_COLUMNS, combined_csv)
    _write_parquet_if_possible(combined_rows, EXTERNAL_YIELD_PANEL_COLUMNS, combined_parquet, warnings)
    _write_csv_rows(catalog, YIELD_SOURCE_COLUMNS, catalog_csv)
    catalog_json.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "record_count": len(catalog),
                "records": catalog,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    report_path = reports / "yield_sources_summary.md"
    result = YieldSourceDownloadResult(
        status="ok" if panel_rows else "empty",
        downloaded_files=downloaded_files,
        panel_rows=len(panel_rows),
        outputs={
            "panel_csv": panel_csv,
            "panel_parquet": panel_parquet,
            "combined_csv": combined_csv,
            "combined_parquet": combined_parquet,
            "catalog_csv": catalog_csv,
            "catalog_json": catalog_json,
        },
        warnings=warnings,
        report_path=report_path,
    )
    _write_yield_sources_report(result, catalog)
    return result


def normalize_ers_provincial_workbook(path: str | Path) -> list[dict[str, Any]]:
    """Normalize the ERS provincial workbook into a province yield panel."""

    import pandas as pd

    frame = pd.read_excel(path, engine="xlrd")
    rows = normalize_ers_provincial_records(frame.to_dict(orient="records"))
    source_file = str(Path(path).resolve())
    return [{**row, "source_file": source_file} for row in rows]


def normalize_ers_provincial_records(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize ERS long records for rice and grain into one row per province-year-crop."""

    grouped: dict[tuple[str, int, str], dict[str, float]] = {}
    for row in rows:
        crop = _ers_crop(row)
        measure = _ers_measure(row)
        if crop is None or measure is None:
            continue
        province = normalize_province_name(row.get("Geography_Desc"))
        year = _as_int(row.get("Year_Desc"))
        amount = _as_float(row.get("Amount"))
        if not province or year is None or amount is None:
            continue
        key = (province, year, crop)
        grouped.setdefault(key, {})
        if measure == "production_ton":
            grouped[key][measure] = amount * 1000.0
        elif measure == "sown_area_hectare":
            grouped[key][measure] = amount * 1000.0

    normalized: list[dict[str, Any]] = []
    for (province, year, crop), values in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0], item[0][2])):
        production = values.get("production_ton")
        area = values.get("sown_area_hectare")
        if production is None and area is None:
            continue
        yield_value = production * 1000.0 / area if production is not None and area and area > 0 else ""
        normalized.append(
            {
                "year": year,
                "province": province,
                "prefecture": "",
                "county": "",
                "admin_code": "",
                "admin_level": "province",
                "crop": crop,
                "sown_area_hectare": area if area is not None else "",
                "harvested_area_hectare": "",
                "production_ton": production if production is not None else "",
                "yield_kg_per_hectare": yield_value,
                "source": "USDA_ERS_China_Provincial_Data",
                "source_file": "",
                "quality_flag": "official_derived_yield",
            }
        )
    return normalized


def normalize_province_name(value: Any) -> str:
    """Normalize English or Chinese province names to canonical Chinese names."""

    province = str(value or "").strip()
    if not province:
        return ""
    normalized = re.sub(r"\s+", " ", province).strip().lower()
    return PROVINCE_EN_TO_ZH.get(normalized, province)


def combine_yield_panel_rows(
    external_rows: Iterable[dict[str, Any]],
    local_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Combine normalized external rows with existing cleaned local statistics rows."""

    combined: list[dict[str, Any]] = []
    for row in external_rows:
        combined.append({column: row.get(column, "") for column in EXTERNAL_YIELD_PANEL_COLUMNS})
    for row in local_rows:
        combined.append(_standardize_local_yield_row(row))
    return _deduplicate_combined_rows(combined)


def _read_existing_yield_panel(path: Path, warnings: list[str]) -> list[dict[str, Any]]:
    """Read the existing cleaned yield panel if available."""

    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
            return list(csv.DictReader(file_obj))
    except Exception as exc:  # noqa: BLE001 - combined output should still be written
        warnings.append(f"Could not read existing yield panel {path}: {type(exc).__name__}: {exc}")
        return []


def _standardize_local_yield_row(row: dict[str, Any]) -> dict[str, Any]:
    """Map an existing statistics-cleaning row to the external panel schema."""

    province = str(row.get("province") or "").strip()
    prefecture = str(row.get("prefecture") or "").strip()
    county = str(row.get("county") or "").strip()
    crop = str(row.get("crop") or "").strip().lower() or "grain"
    return {
        "year": _as_int(row.get("year")) or "",
        "province": province,
        "prefecture": prefecture,
        "county": county,
        "admin_code": str(row.get("admin_code") or "").strip(),
        "admin_level": _infer_admin_level(province, prefecture, county),
        "crop": crop,
        "sown_area_hectare": _as_float(row.get("sown_area_hectare")) or "",
        "harvested_area_hectare": _as_float(row.get("harvested_area_hectare")) or "",
        "production_ton": _as_float(row.get("production_ton")) or "",
        "yield_kg_per_hectare": _first_numeric(
            row.get("yield_kg_per_hectare"),
            row.get("grain_yield_kg_per_hectare"),
            row.get("rice_yield_kg_per_hectare"),
        )
        or "",
        "source": "local_statistics_cleaned",
        "source_file": str(row.get("source_file") or "").strip(),
        "quality_flag": "cleaned_existing_statistics",
    }


def _infer_admin_level(province: str, prefecture: str, county: str) -> str:
    """Infer administrative level from populated name columns."""

    if county:
        return "county"
    if prefecture:
        return "prefecture"
    if province in {"全国总计", "China"}:
        return "national"
    return "province" if province else ""


def _first_numeric(*values: Any) -> float | None:
    """Return the first parseable numeric value."""

    for value in values:
        number = _as_float(value)
        if number is not None:
            return number
    return None


def _deduplicate_combined_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate combined rows, preferring later local rows for exact source duplicates."""

    index: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("year"),
            row.get("province"),
            row.get("prefecture"),
            row.get("county"),
            row.get("admin_code"),
            row.get("crop"),
            row.get("source"),
            row.get("source_file"),
        )
        index[key] = row
    return sorted(
        index.values(),
        key=lambda row: (
            int(row["year"]) if str(row.get("year") or "").isdigit() else 9999,
            str(row.get("province") or ""),
            str(row.get("crop") or ""),
            str(row.get("source") or ""),
        ),
    )


def parse_nbs_grain_announcement_text(text: str, year: int, source_url: str) -> list[dict[str, Any]]:
    """Parse province grain table rows from an NBS announcement text dump."""

    tokens = _announcement_tokens(text)
    rows: list[dict[str, Any]] = []
    index = 0
    while index < len(tokens):
        province = _match_province(tokens, index)
        if province is None:
            index += 1
            continue
        next_index = index + _province_token_length(province)
        numbers: list[float] = []
        while next_index < len(tokens) and len(numbers) < 3:
            value = _as_float(tokens[next_index])
            if value is not None:
                numbers.append(value)
            elif _match_province(tokens, next_index) is not None:
                break
            next_index += 1
        if len(numbers) == 3:
            area_1000ha, production_10000t, yield_kg_ha = numbers
            rows.append(
                {
                    "year": int(year),
                    "province": province,
                    "prefecture": "",
                    "county": "",
                    "admin_code": "",
                    "admin_level": "national" if province == "全国总计" else "province",
                    "crop": "grain",
                    "sown_area_hectare": area_1000ha * 1000.0,
                    "harvested_area_hectare": "",
                    "production_ton": production_10000t * 10000.0,
                    "yield_kg_per_hectare": yield_kg_ha,
                    "source": "NBS_grain_announcement",
                    "source_file": source_url,
                    "quality_flag": "official_reported",
                }
            )
            index = next_index
        else:
            index += 1
    return _deduplicate_nbs_rows(rows)


def html_to_text(html: str) -> str:
    """Convert simple HTML into newline-separated visible text using stdlib parsing."""

    parser = _TextExtractor()
    parser.feed(html)
    return "\n".join(parser.parts)


def _download_file(
    url: str,
    output_path: Path,
    force: bool,
    timeout_seconds: int,
    downloaded_files: list[Path],
    warnings: list[str],
) -> None:
    """Download a URL to disk unless it already exists."""

    if output_path.exists() and not force:
        return
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            output_path.write_bytes(response.read())
        downloaded_files.append(output_path)
    except Exception as exc:  # noqa: BLE001 - source failures are recorded, not fatal
        warnings.append(f"Could not download {url}: {type(exc).__name__}: {exc}")


def _update_catalog_local_status(catalog: list[dict[str, Any]], source_id: str, path: Path) -> None:
    """Update one catalog record with local path and file status."""

    for record in catalog:
        if record["source_id"] != source_id:
            continue
        record["local_path"] = str(path)
        record["status"] = "downloaded" if path.exists() else "missing"
        return


def _ers_crop(row: dict[str, Any]) -> str | None:
    """Return the canonical crop represented by an ERS row."""

    category = str(row.get("Category") or "").strip().lower()
    commodity = str(row.get("Commodity_Desc") or "").strip().lower()
    if "rice" in category or commodity == "rice":
        return "rice"
    if "grain" in category or "total grain" in commodity:
        return "grain"
    return None


def _ers_measure(row: dict[str, Any]) -> str | None:
    """Return the canonical measure represented by an ERS row."""

    category = str(row.get("Category") or "").strip().lower()
    if "production" in category:
        return "production_ton"
    if "sown area" in category:
        return "sown_area_hectare"
    return None


def _announcement_tokens(text: str) -> list[str]:
    """Tokenize announcement text while normalizing Chinese spacing."""

    normalized = (
        text.replace("\u3000", "")
        .replace("\xa0", " ")
        .replace(",", "")
        .replace("，", "")
        .replace("\r", "\n")
    )
    return [token.strip() for token in re.split(r"\s+", normalized) if token.strip()]


def _match_province(tokens: list[str], index: int) -> str | None:
    """Match a province name at a token position."""

    for province in NBS_PROVINCE_NAMES:
        length = _province_token_length(province)
        candidate = "".join(tokens[index : index + length])
        if _clean_province_name(candidate) == province:
            return province
    return None


def _province_token_length(province: str) -> int:
    """Return token length for split province names often seen in NBS HTML."""

    if province in {"内蒙古", "黑龙江"}:
        return 2
    return 1


def _clean_province_name(value: str) -> str:
    """Normalize province text from NBS announcements."""

    return re.sub(r"\s+", "", value).replace("\u3000", "").replace(" ", "")


def _deduplicate_nbs_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one row per year/province/crop from repeated mobile/desktop page text."""

    seen: set[tuple[int, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = (int(row["year"]), str(row["province"]), str(row["crop"]))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _write_csv_rows(rows: list[dict[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write rows to UTF-8 CSV with a stable schema."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_parquet_if_possible(
    rows: list[dict[str, Any]],
    columns: list[str],
    output_path: Path,
    warnings: list[str],
) -> None:
    """Write Parquet output when pandas/pyarrow are available."""

    try:
        import pandas as pd

        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(rows, columns=columns).replace({"": None})
        frame.to_parquet(output_path, index=False)
    except Exception as exc:  # noqa: BLE001 - CSV remains the required fallback
        warnings.append(f"Could not write Parquet {output_path}: {type(exc).__name__}: {exc}")


def _write_yield_sources_report(result: YieldSourceDownloadResult, catalog: list[dict[str, Any]]) -> None:
    """Write a Markdown summary for external yield source acquisition."""

    lines = [
        "# Yield Source Acquisition Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Downloaded this run: {len(result.downloaded_files)}",
        f"- Normalized panel rows: {result.panel_rows}",
        "",
        "## Outputs",
        "",
    ]
    lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
    external_catalog = [record for record in catalog if not str(record["source_id"]).endswith("_lead") and record["source_id"] not in {lead["source_id"] for lead in LOCAL_YEARBOOK_SOURCE_LEADS}]
    lead_catalog = [record for record in catalog if record["source_id"] in {lead["source_id"] for lead in LOCAL_YEARBOOK_SOURCE_LEADS}]
    lines.extend(["", "## Source Catalog", "", "| source | scale | years | status | limitation |"])
    lines.append("| --- | --- | --- | --- | --- |")
    for record in external_catalog:
        lines.append(
            (
                f"| {record['source_id']} | {record['scale']} | {record['coverage_years']} | "
                f"{record['status']} | {record['limitations']} |"
            )
        )
    lines.extend(["", "## Local Yearbook And Subscription Leads", "", "| source | agency | province | tier/use | status | limitation |"])
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for record in lead_catalog:
        lines.append(
            (
                f"| {record['source_id']} | {record['agency']} | {record['province']} | "
                f"{record['usable_tier']} | {record['status']} | {record['limitations']} |"
            )
        )
    lines.append("")
    lines.extend(
        [
            "## Known Gaps and Candidate Supplements",
            "",
            (
                "- No directly downloadable complete county/prefecture official rice-yield panel for "
                "2000-2024 main content years was found in the automated public-source workflow."
            ),
            (
                "- NBS annual grain announcements provide province-level grain totals for recent years; "
                "they do not provide province-level rice-by-variety panels."
            ),
            (
                "- NBS early-rice announcements report national or province survey outputs for early rice, "
                "but not a complete all-rice administrative panel."
            ),
            (
                "- CCD-Rice and related gridded rice products can support spatial exposure/rice-area work, "
                "but they are distribution products rather than official yield panels."
            ),
            (
                "- A complete county/city yield panel will likely require local statistical yearbooks, "
                "paid yearbook databases, or manually curated county statistical bulletins."
            ),
            "",
        ]
    )
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _as_float(value: Any) -> float | None:
    """Parse a float from common numeric text."""

    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "-", "--", "—"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _as_int(value: Any) -> int | None:
    """Parse an integer from a value."""

    number = _as_float(value)
    if number is None:
        return None
    return int(number)


class _TextExtractor(HTMLParser):
    """Collect visible-ish text from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        """Append nonblank data chunks."""

        text = data.strip()
        if text:
            self.parts.append(text)
