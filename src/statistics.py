"""Agricultural statistics cleaning utilities."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


SUPPORTED_STATISTICS_SUFFIXES = (".csv", ".xlsx", ".xls")
IGNORED_STATISTICS_DIR_NAMES = {"external_yield_sources", "local_yearbook_leads", "yield_proxy"}

STATISTICS_FIELD_CANDIDATES: dict[str, list[str]] = {
    "year": ["year", "年份", "年度", "统计年份"],
    "province": ["province", "province_name", "region", "地区", "省", "省份", "省级"],
    "prefecture": ["prefecture", "city", "city_name", "市", "地级市", "州", "盟"],
    "county": ["county", "county_name", "district", "区县", "县", "县市区", "县级", "行政区"],
    "admin_code": ["admin_code", "adcode", "code", "county_code", "行政区划代码", "行政代码", "区划代码"],
    "crop": ["crop", "crop_name", "作物", "作物名称", "品种", "类别"],
    "sown_area": ["sown_area", "sownarea", "sown_area_1000ha", "planted_area", "播种面积", "农作物播种面积"],
    "harvested_area": ["harvested_area", "harvestedarea", "harvest_area", "收获面积", "实际收获面积"],
    "production": ["production", "production_10000t", "output", "yield_total", "total_production", "产量", "总产量"],
    "yield": ["yield", "yield_kg_per_ha", "unit_yield", "yield_per_area", "单产", "平均单产", "每公顷产量"],
    "rice_yield": ["rice_yield", "riceyield", "水稻单产", "稻谷单产"],
    "grain_yield": ["grain_yield", "grainyield", "粮食单产", "谷物单产"],
}

YIELD_PANEL_COLUMNS = [
    "year",
    "province",
    "prefecture",
    "county",
    "admin_code",
    "admin_name_clean",
    "crop",
    "sown_area_hectare",
    "harvested_area_hectare",
    "production_ton",
    "yield_kg_per_hectare",
    "rice_yield_kg_per_hectare",
    "grain_yield_kg_per_hectare",
    "source_file",
]

YIELD_PANEL_QC_COLUMNS = [
    "source_file",
    "source_row",
    "issue",
    "message",
]

YIELD_COVERAGE_COLUMNS = [
    "source_file",
    "admin_level",
    "crop_type",
    "coverage_year_start",
    "coverage_year_end",
    "expected_years",
    "observed_years",
    "year_coverage_rate",
    "admin_unit_count",
    "balanced_panel_unit_count",
    "missing_rate_by_year",
    "missing_rate_by_admin",
    "yield_unit_detected",
    "area_unit_detected",
    "production_unit_detected",
    "suspicious_value_count",
    "duplicate_record_count",
    "match_rate_to_admin_boundary",
    "recommendation",
]


@dataclass(frozen=True)
class StatisticsPreparationResult:
    """Result metadata for agricultural statistics cleaning."""

    status: str
    file_count: int
    processed_files: list[Path]
    row_count: int
    field_mapping: dict[str, dict[str, str]] = field(default_factory=dict)
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/statistics_cleaning_summary.md")


def find_statistics_files(statistics_dir: str | Path) -> list[Path]:
    """Find supported agricultural statistics tables."""

    root = Path(statistics_dir).expanduser().resolve()
    if not root.exists():
        return []

    files = [
        path.resolve()
        for path in root.rglob("*")
        if path.is_file()
        and path.suffix.lower() in SUPPORTED_STATISTICS_SUFFIXES
        and not _is_ignored_statistics_path(path, root)
    ]
    return sorted(files, key=lambda path: str(path))


def _is_ignored_statistics_path(path: Path, root: Path) -> bool:
    """Return True for raw downloads handled by specialized ingestion scripts."""

    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return any(part in IGNORED_STATISTICS_DIR_NAMES for part in relative.parts[:-1])


def identify_statistics_fields(columns: list[str] | Any) -> dict[str, str]:
    """Identify common agricultural statistics fields from table columns."""

    column_list = [str(column) for column in columns]
    normalized_columns = [(_normalize_field_name(column), column) for column in column_list]
    mapping: dict[str, str] = {}

    for role, candidates in STATISTICS_FIELD_CANDIDATES.items():
        match = _match_field(role, candidates, normalized_columns)
        if match is not None:
            mapping[role] = match

    return mapping


def convert_production_to_ton(values: Any, units: str | None) -> Any:
    """Convert production values to metric tons."""

    unit_text = _normalize_unit_text(units)
    if _contains_any(unit_text, ["万吨", "10000t", "10kt"]):
        factor = 10000.0
    elif _contains_any(unit_text, ["公斤", "千克", "kg", "kilogram"]):
        factor = 0.001
    elif _contains_any(unit_text, ["斤", "jin", "catty"]):
        factor = 0.0005
    elif _contains_any(unit_text, ["克", "g", "gram"]):
        factor = 0.000001
    else:
        factor = 1.0
    return _map_numeric(values, lambda value: value * factor)


def convert_area_to_hectare(values: Any, units: str | None) -> Any:
    """Convert area values to hectares."""

    unit_text = _normalize_unit_text(units)
    if _contains_any(unit_text, ["1000ha", "千公顷"]):
        factor = 1000.0
    elif _contains_any(unit_text, ["平方公里", "km2", "km^2", "sqkm"]):
        factor = 100.0
    elif _contains_any(unit_text, ["平方米", "m2", "m^2", "squaremeter"]):
        factor = 0.0001
    elif _contains_any(unit_text, ["亩", "mu"]):
        factor = 1.0 / 15.0
    else:
        factor = 1.0
    return _map_numeric(values, lambda value: value * factor)


def convert_yield_to_kg_per_hectare(values: Any, units: str | None) -> Any:
    """Convert yield values to kg/ha."""

    unit_text = _normalize_unit_text(units)
    compact = unit_text.replace("per", "/").replace("每", "/")
    if _contains_any(compact, ["公斤/亩", "千克/亩", "kg/mu"]):
        factor = 15.0
    elif _contains_any(compact, ["斤/亩", "jin/mu", "catty/mu"]):
        factor = 7.5
    elif _contains_any(compact, ["吨/公顷", "吨/ha", "t/ha", "ton/ha", "tons/ha"]):
        factor = 1000.0
    else:
        factor = 1.0
    return _map_numeric(values, lambda value: value * factor)


def clean_admin_name(name: Any) -> str:
    """Remove common administrative suffixes for fuzzy matching."""

    text = str(name or "").strip()
    if not text:
        return ""

    suffixes = [
        "特别行政区",
        "自治县",
        "自治州",
        "自治旗",
        "自治区",
        "市辖区",
        "县级市",
        "新区",
        "地区",
        "林区",
        "市",
        "县",
        "区",
        "旗",
        "州",
        "盟",
    ]
    for suffix in suffixes:
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)].strip()
    return text


def compute_yield(production_ton: Any, area_hectare: Any) -> float | None:
    """Compute yield as kg/ha from production in tons and area in hectares."""

    production_value = _parse_number(production_ton)
    area_value = _parse_number(area_hectare)
    if production_value is None or area_value is None or area_value <= 0:
        return None
    return production_value * 1000.0 / area_value


def prepare_statistics(
    statistics_dir: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    expected_years: list[int] | None = None,
) -> StatisticsPreparationResult:
    """Clean agricultural statistics tables into a yield panel CSV."""

    statistics_root = Path(statistics_dir).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    panel_path = processed / "yield_panel.csv"
    qc_path = processed / "yield_panel_qc.csv"
    coverage_path = processed / "yield_coverage_report.csv"
    report_path = reports / "statistics_cleaning_summary.md"
    coverage_report_path = reports / "yield_coverage_report.md"
    outputs = {"panel": panel_path, "qc": qc_path, "coverage": coverage_path}

    files = find_statistics_files(statistics_root)
    warnings: list[str] = []
    if not files:
        warnings.append(f"No agricultural statistics files found under {statistics_root}.")
        _write_csv_rows([], YIELD_PANEL_COLUMNS, panel_path)
        _write_csv_rows([], YIELD_PANEL_QC_COLUMNS, qc_path)
        fallback_rows = _read_existing_yield_panel_rows(processed)
        coverage_rows = build_yield_coverage_report(fallback_rows, expected_years=expected_years)
        _write_csv_rows(coverage_rows, YIELD_COVERAGE_COLUMNS, coverage_path)
        _write_yield_coverage_report(coverage_rows, coverage_report_path)
        _write_tier_report(fallback_rows, processed, reports, expected_years)
        result = StatisticsPreparationResult(
            status="missing",
            file_count=0,
            processed_files=[],
            row_count=0,
            field_mapping={},
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
        )
        _write_statistics_report(result, statistics_root)
        return result

    panel_rows: list[dict[str, Any]] = []
    qc_rows: list[dict[str, Any]] = []
    processed_files: list[Path] = []
    field_mappings: dict[str, dict[str, str]] = {}

    for path in files:
        try:
            columns, rows = _read_statistics_table(path)
            mapping = identify_statistics_fields(columns)
            field_mappings[path.name] = mapping
            if not mapping:
                warnings.append(f"Skipped {path}: no recognizable statistics fields.")
                continue

            for source_row, row in enumerate(rows, start=2):
                cleaned = _clean_statistics_row(row, mapping, path.name, source_row, qc_rows)
                panel_rows.append(cleaned)
            processed_files.append(path)
        except Exception as exc:  # noqa: BLE001 - one bad file must not abort the pipeline
            warnings.append(f"Skipped {path}: {type(exc).__name__}: {exc}")

    _write_csv_rows(panel_rows, YIELD_PANEL_COLUMNS, panel_path)
    _write_csv_rows(qc_rows, YIELD_PANEL_QC_COLUMNS, qc_path)
    tier_rows = panel_rows if panel_rows else _read_existing_yield_panel_rows(processed)
    coverage_rows = build_yield_coverage_report(tier_rows, expected_years=expected_years)
    _write_csv_rows(coverage_rows, YIELD_COVERAGE_COLUMNS, coverage_path)
    _write_yield_coverage_report(coverage_rows, coverage_report_path)
    _write_tier_report(tier_rows, processed, reports, expected_years)

    if panel_rows:
        status = "partial" if warnings else "ok"
    elif processed_files:
        status = "empty"
    else:
        status = "error"

    result = StatisticsPreparationResult(
        status=status,
        file_count=len(files),
        processed_files=processed_files,
        row_count=len(panel_rows),
        field_mapping=field_mappings,
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_statistics_report(result, statistics_root)
    return result


def build_yield_coverage_report(
    rows: list[dict[str, Any]] | Any,
    expected_years: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Build candidate-panel coverage diagnostics for yield data."""

    import pandas as pd

    frame = pd.DataFrame(rows)
    if frame.empty:
        return [_empty_coverage_row("all")]

    frame = frame.copy()
    if "source_file" not in frame.columns:
        frame["source_file"] = "unknown"
    frame["_year"] = pd.to_numeric(frame.get("year"), errors="coerce")
    frame["_admin_level"] = frame.apply(_infer_admin_level, axis=1)
    frame["_crop_type"] = frame.apply(_infer_crop_type, axis=1)
    frame["_admin_key"] = frame.apply(_admin_key, axis=1)
    if expected_years is None:
        valid_years = frame["_year"].dropna().astype(int)
        expected_years = list(range(int(valid_years.min()), int(valid_years.max()) + 1)) if not valid_years.empty else []
    expected_year_set = {int(year) for year in expected_years}

    group_columns = ["source_file", "_admin_level", "_crop_type"]
    coverage_rows: list[dict[str, Any]] = []
    for (source_file, admin_level, crop_type), group in frame.groupby(group_columns, dropna=False):
        years = set(group["_year"].dropna().astype(int).tolist())
        admin_units = {value for value in group["_admin_key"].dropna().astype(str).tolist() if value}
        observed = len(years & expected_year_set) if expected_year_set else len(years)
        expected_count = len(expected_year_set) if expected_year_set else len(years)
        coverage_rate = observed / expected_count if expected_count else 0.0
        balanced_count = _balanced_unit_count(group, expected_year_set)
        coverage_rows.append(
            {
                "source_file": source_file or "unknown",
                "admin_level": admin_level or "unknown",
                "crop_type": crop_type or "unknown",
                "coverage_year_start": min(years) if years else "",
                "coverage_year_end": max(years) if years else "",
                "expected_years": expected_count,
                "observed_years": observed,
                "year_coverage_rate": round(float(coverage_rate), 4),
                "admin_unit_count": len(admin_units),
                "balanced_panel_unit_count": balanced_count,
                "missing_rate_by_year": round(float(1.0 - coverage_rate), 4),
                "missing_rate_by_admin": round(_missing_rate_by_admin(group, expected_year_set), 4),
                "yield_unit_detected": "kg/ha_standardized" if _has_any_numeric(group, ["yield_kg_per_hectare", "rice_yield_kg_per_hectare", "grain_yield_kg_per_hectare"]) else "missing",
                "area_unit_detected": "hectare_standardized" if _has_any_numeric(group, ["sown_area_hectare", "harvested_area_hectare"]) else "missing",
                "production_unit_detected": "ton_standardized" if _has_any_numeric(group, ["production_ton"]) else "missing",
                "suspicious_value_count": _suspicious_value_count(group),
                "duplicate_record_count": _duplicate_record_count(group),
                "match_rate_to_admin_boundary": "",
                "recommendation": _coverage_recommendation(admin_level, crop_type, coverage_rate),
            }
        )
    return coverage_rows


def _write_tier_report(panel_rows: list[dict[str, Any]], processed: Path, reports: Path, expected_years: list[int] | None) -> None:
    """Write yield data tier report if the tier module is available."""

    from src.data_tiers import write_yield_data_tier_report

    write_yield_data_tier_report(panel_rows, processed, reports, expected_years=expected_years)


def _read_existing_yield_panel_rows(processed: Path) -> list[dict[str, Any]]:
    """Read an existing combined yield panel for coverage fallback."""

    try:
        import pandas as pd
    except ImportError:
        return []
    for path in (
        processed / "yield_panel_combined.parquet",
        processed / "yield_panel_combined.csv",
        processed / "yield_panel_external_province.parquet",
        processed / "yield_panel_external_province.csv",
    ):
        if not path.exists():
            continue
        try:
            frame = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
        except Exception:
            continue
        if not frame.empty:
            return frame.to_dict(orient="records")
    return []


def _write_yield_coverage_report(rows: list[dict[str, Any]], report_path: Path) -> None:
    """Write Markdown summary for yield coverage diagnostics."""

    lines = [
        "# Yield Coverage Report",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "| source_file | admin_level | crop_type | years | coverage | units | recommendation |",
        "| --- | --- | --- | --- | ---: | ---: | --- |",
    ]
    for row in rows:
        years = f"{row.get('coverage_year_start', '')}-{row.get('coverage_year_end', '')}"
        lines.append(
            "| {source_file} | {admin_level} | {crop_type} | {years} | {coverage} | {units} | {recommendation} |".format(
                source_file=row.get("source_file", ""),
                admin_level=row.get("admin_level", ""),
                crop_type=row.get("crop_type", ""),
                years=years,
                coverage=row.get("year_coverage_rate", ""),
                units=row.get("admin_unit_count", ""),
                recommendation=row.get("recommendation", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Automatic Rule",
            "",
            "- county rice coverage >= 0.75: use county rice main model.",
            "- prefecture rice coverage >= 0.75: use prefecture rice main model.",
            "- rice coverage insufficient but grain coverage >= 0.75: downgrade to grain yield anomaly.",
            "- official yield coverage insufficient: use remote-sensing growth anomaly and risk-exposure analysis.",
            "",
        ]
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def _empty_coverage_row(source_file: str) -> dict[str, Any]:
    """Return an empty coverage diagnostic row."""

    return {
        "source_file": source_file,
        "admin_level": "unknown",
        "crop_type": "unknown",
        "coverage_year_start": "",
        "coverage_year_end": "",
        "expected_years": 0,
        "observed_years": 0,
        "year_coverage_rate": 0.0,
        "admin_unit_count": 0,
        "balanced_panel_unit_count": 0,
        "missing_rate_by_year": 1.0,
        "missing_rate_by_admin": 1.0,
        "yield_unit_detected": "missing",
        "area_unit_detected": "missing",
        "production_unit_detected": "missing",
        "suspicious_value_count": 0,
        "duplicate_record_count": 0,
        "match_rate_to_admin_boundary": "",
        "recommendation": "official_yield_coverage_insufficient_use_remote_sensing_growth_analysis",
    }


def _infer_admin_level(row: Any) -> str:
    """Infer admin level from normalized statistics fields."""

    explicit = _clean_text(row.get("admin_level")).lower()
    if explicit in {"county", "prefecture", "province"}:
        return explicit
    if _clean_text(row.get("county")):
        return "county"
    if _clean_text(row.get("prefecture")):
        return "prefecture"
    if _clean_text(row.get("province")):
        return "province"
    return "unknown"


def _infer_crop_type(row: Any) -> str:
    """Infer crop type from crop labels and specialized fields."""

    text = " ".join(_clean_text(row.get(column)).lower() for column in ("crop", "source_file", "source"))
    if any(keyword in text for keyword in ("早稻", "early rice")):
        return "early_rice"
    if any(keyword in text for keyword in ("中稻", "一季稻", "单季稻", "single rice")):
        return "single_rice"
    if any(keyword in text for keyword in ("晚稻", "late rice")):
        return "late_rice"
    if any(keyword in text for keyword in ("水稻", "稻谷", "rice")) or _clean_text(row.get("rice_yield_kg_per_hectare")):
        return "rice"
    if any(keyword in text for keyword in ("粮食", "谷物", "grain")) or _clean_text(row.get("grain_yield_kg_per_hectare")):
        return "grain"
    return "unknown"


def _admin_key(row: Any) -> str:
    """Build an admin key from code or names."""

    return (
        _clean_text(row.get("admin_code"))
        or "|".join(_clean_text(row.get(column)) for column in ("province", "prefecture", "county") if _clean_text(row.get(column)))
        or _clean_text(row.get("admin_name_clean"))
    )


def _balanced_unit_count(group: Any, expected_years: set[int]) -> int:
    """Count admin units observed in every expected year."""

    if not expected_years or group.empty:
        return 0
    count = 0
    for _, unit_group in group.groupby("_admin_key"):
        years = set(unit_group["_year"].dropna().astype(int).tolist())
        if expected_years.issubset(years):
            count += 1
    return count


def _missing_rate_by_admin(group: Any, expected_years: set[int]) -> float:
    """Calculate missing admin-year share."""

    units = {value for value in group["_admin_key"].dropna().astype(str).tolist() if value}
    if not units or not expected_years:
        return 1.0
    observed_pairs = {
        (str(row["_admin_key"]), int(row["_year"]))
        for _, row in group.dropna(subset=["_year"]).iterrows()
        if str(row["_admin_key"])
    }
    expected_pairs = len(units) * len(expected_years)
    return 1.0 - (len(observed_pairs) / expected_pairs if expected_pairs else 0.0)


def _has_any_numeric(group: Any, columns: list[str]) -> bool:
    """Return True when any candidate numeric column is present."""

    import pandas as pd

    return any(column in group.columns and pd.to_numeric(group[column], errors="coerce").notna().any() for column in columns)


def _suspicious_value_count(group: Any) -> int:
    """Count basic suspicious values in standardized yield rows."""

    import pandas as pd

    count = 0
    for column in ("yield_kg_per_hectare", "rice_yield_kg_per_hectare", "grain_yield_kg_per_hectare"):
        if column in group.columns:
            values = pd.to_numeric(group[column], errors="coerce")
            count += int(((values.notna()) & ((values < 500) | (values > 15000))).sum())
    for column in ("sown_area_hectare", "harvested_area_hectare"):
        if column in group.columns:
            values = pd.to_numeric(group[column], errors="coerce")
            count += int(((values.notna()) & (values <= 0)).sum())
    if "production_ton" in group.columns:
        values = pd.to_numeric(group["production_ton"], errors="coerce")
        count += int(((values.notna()) & (values < 0)).sum())
    return count


def _duplicate_record_count(group: Any) -> int:
    """Count duplicate admin-year-crop records."""

    key_columns = [column for column in ["_admin_key", "_year", "_crop_type"] if column in group.columns]
    if not key_columns:
        return 0
    return int(group.duplicated(key_columns).sum())


def _coverage_recommendation(admin_level: str, crop_type: str, coverage_rate: float) -> str:
    """Return an automatic model-scope recommendation."""

    if admin_level == "county" and crop_type == "rice" and coverage_rate >= 0.75:
        return "use_county_rice_main_model"
    if admin_level == "prefecture" and crop_type == "rice" and coverage_rate >= 0.75:
        return "use_prefecture_rice_main_model"
    if crop_type == "grain" and admin_level in {"county", "prefecture"} and coverage_rate >= 0.75:
        return "downgrade_main_model_to_grain_yield"
    if coverage_rate >= 0.5 and admin_level in {"county", "prefecture"}:
        return "run_fixed_effects_and_exploratory_event_study_with_caution"
    return "official_yield_coverage_insufficient_use_remote_sensing_growth_analysis"


def _clean_statistics_row(
    row: dict[str, Any],
    mapping: dict[str, str],
    source_file: str,
    source_row: int,
    qc_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build one normalized yield panel row."""

    province = _clean_text(_get_field(row, mapping, "province"))
    prefecture = _clean_text(_get_field(row, mapping, "prefecture"))
    county = _clean_text(_get_field(row, mapping, "county"))
    admin_source = county or prefecture or province

    sown_area = _convert_mapped_value(row, mapping, "sown_area", convert_area_to_hectare)
    harvested_area = _convert_mapped_value(row, mapping, "harvested_area", convert_area_to_hectare)
    production = _convert_mapped_value(row, mapping, "production", convert_production_to_ton)
    yield_value = _convert_mapped_value(row, mapping, "yield", convert_yield_to_kg_per_hectare)
    rice_yield = _convert_mapped_value(row, mapping, "rice_yield", convert_yield_to_kg_per_hectare)
    grain_yield = _convert_mapped_value(row, mapping, "grain_yield", convert_yield_to_kg_per_hectare)

    if yield_value is None:
        yield_value = compute_yield(production, harvested_area if harvested_area is not None else sown_area)

    if mapping.get("year") and _clean_year(_get_field(row, mapping, "year")) == "":
        qc_rows.append(
            {
                "source_file": source_file,
                "source_row": source_row,
                "issue": "missing_year",
                "message": "Year field is empty or invalid.",
            }
        )

    return {
        "year": _clean_year(_get_field(row, mapping, "year")),
        "province": province,
        "prefecture": prefecture,
        "county": county,
        "admin_code": _clean_text(_get_field(row, mapping, "admin_code")),
        "admin_name_clean": clean_admin_name(admin_source),
        "crop": _clean_text(_get_field(row, mapping, "crop")),
        "sown_area_hectare": _format_output_value(sown_area),
        "harvested_area_hectare": _format_output_value(harvested_area),
        "production_ton": _format_output_value(production),
        "yield_kg_per_hectare": _format_output_value(yield_value),
        "rice_yield_kg_per_hectare": _format_output_value(rice_yield),
        "grain_yield_kg_per_hectare": _format_output_value(grain_yield),
        "source_file": source_file,
    }


def _convert_mapped_value(
    row: dict[str, Any],
    mapping: dict[str, str],
    role: str,
    converter: Callable[[Any, str | None], Any],
) -> float | None:
    """Convert a mapped numeric field using units inferred from the column name."""

    column = mapping.get(role)
    if not column:
        return None
    units = _extract_unit(column) or column
    return converter(_get_field(row, mapping, role), units)


def _get_field(row: dict[str, Any], mapping: dict[str, str], role: str) -> Any:
    """Get a row value by canonical role."""

    column = mapping.get(role)
    if not column:
        return None
    return row.get(column)


def _read_statistics_table(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    """Read a supported statistics table."""

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv_table(path)
    if suffix in {".xlsx", ".xls"}:
        return _read_excel_table(path)
    raise ValueError(f"Unsupported statistics file type: {suffix}")


def _read_csv_table(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    """Read a CSV table with common encodings."""

    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "latin1"):
        try:
            with path.open("r", encoding=encoding, newline="") as file_obj:
                reader = csv.DictReader(file_obj)
                columns = [str(column) for column in (reader.fieldnames or [])]
                rows = [dict(row) for row in reader]
            return columns, rows
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise ValueError(f"CSV could not be read: {path}")


def _read_excel_table(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    """Read an Excel table with pandas loaded only when needed."""

    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required to read Excel statistics files") from exc

    frame = pd.read_excel(path)
    columns = [str(column) for column in frame.columns]
    return columns, frame.to_dict(orient="records")


def _write_csv_rows(rows: list[dict[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write rows as UTF-8 CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_statistics_report(result: StatisticsPreparationResult, statistics_dir: Path) -> None:
    """Write a Markdown summary for statistics cleaning."""

    lines = [
        "# Statistics Cleaning Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Statistics directory: `{statistics_dir}`",
        f"- Candidate files: {result.file_count}",
        f"- Processed files: {len(result.processed_files)}",
        f"- Output rows: {result.row_count}",
        "",
    ]

    if result.status == "missing":
        lines.extend(["No agricultural statistics files found.", ""])

    if result.outputs:
        lines.extend(["## Outputs", ""])
        lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
        lines.append("")

    if result.field_mapping:
        lines.extend(["## Field Mapping", ""])
        for source_file, mapping in result.field_mapping.items():
            lines.append(f"### {source_file}")
            if mapping:
                lines.extend(f"- {role}: `{column}`" for role, column in mapping.items())
            else:
                lines.append("- No recognized fields.")
            lines.append("")

    if result.processed_files:
        lines.extend(["## Processed Files", ""])
        lines.extend(f"- `{path}`" for path in result.processed_files)
        lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _match_field(
    role: str,
    candidates: list[str],
    normalized_columns: list[tuple[str, str]],
) -> str | None:
    """Match a role to the first suitable source column."""

    normalized_candidates = [_normalize_field_name(candidate) for candidate in candidates]
    for candidate in normalized_candidates:
        for normalized_column, original_column in normalized_columns:
            if normalized_column == candidate and _field_allowed_for_role(role, normalized_column):
                return original_column

    for candidate in normalized_candidates:
        for normalized_column, original_column in normalized_columns:
            if candidate and candidate in normalized_column and _field_allowed_for_role(role, normalized_column):
                return original_column
    return None


def _field_allowed_for_role(role: str, normalized_column: str) -> bool:
    """Avoid assigning specialized rice/grain yield fields to generic yield."""

    if role != "yield":
        return True
    specialized_markers = ("rice", "grain", "水稻", "稻谷", "粮食", "谷物")
    return not any(marker in normalized_column for marker in specialized_markers)


def _normalize_field_name(name: str) -> str:
    """Normalize field names for candidate matching."""

    return re.sub(r"[\s_\-/\\()（）\[\]【】{}]", "", str(name).strip().lower())


def _normalize_unit_text(units: str | None) -> str:
    """Normalize unit strings for conversion matching."""

    return (
        str(units or "")
        .strip()
        .lower()
        .replace("（", "(")
        .replace("）", ")")
        .replace("／", "/")
        .replace(" ", "")
        .replace("_", "")
        .replace("·", "")
    )


def _contains_any(text: str, patterns: list[str]) -> bool:
    """Return True if any normalized pattern is in text."""

    return any(_normalize_unit_text(pattern) in text for pattern in patterns)


def _extract_unit(column_name: str) -> str:
    """Extract a unit from parentheses in a source column name."""

    matches = re.findall(r"[（(]([^()（）]+)[）)]", str(column_name))
    if matches:
        return matches[-1]
    return ""


def _map_numeric(values: Any, converter: Callable[[float], float]) -> Any:
    """Apply a scalar numeric converter to common value containers."""

    if isinstance(values, list):
        return [_map_numeric(value, converter) for value in values]
    if isinstance(values, tuple):
        return tuple(_map_numeric(value, converter) for value in values)

    value = _parse_number(values)
    if value is None:
        return None
    return converter(value)


def _parse_number(value: Any) -> float | None:
    """Parse a numeric value, preserving missing values as None."""

    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "na", "n/a", "--", "—", "-"}:
        return None
    text = text.replace(",", "").replace("，", "")
    try:
        return float(text)
    except ValueError:
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if match:
            return float(match.group(0))
    return None


def _clean_text(value: Any) -> str:
    """Normalize text values for CSV output."""

    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _clean_year(value: Any) -> str:
    """Normalize year values to an integer-like string."""

    number = _parse_number(value)
    if number is None:
        return ""
    return str(int(number))


def _format_output_value(value: Any) -> str:
    """Format optional values for stable CSV output."""

    if value is None:
        return ""
    return str(float(value))
