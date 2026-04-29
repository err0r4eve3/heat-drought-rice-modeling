"""Administrative-code crosswalk construction for main-period panels."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


SUPPORTED_ADMIN_CODE_SUFFIXES = (".csv", ".json", ".xlsx", ".xls")

ADMIN_CROSSWALK_COLUMNS = [
    "year",
    "admin_code_original",
    "admin_name_original",
    "admin_code_standard",
    "admin_name_standard",
    "province_standard",
    "prefecture_standard",
    "county_standard",
    "match_confidence",
    "match_method",
    "notes",
]

FIELD_CANDIDATES = {
    "year": ["year", "年份", "年度"],
    "old_code": ["old_code", "旧代码", "原代码", "before_code", "former_code"],
    "new_code": ["new_code", "新代码", "现代码", "after_code", "standard_code"],
    "code": ["code", "admin_code", "行政区划代码", "区划代码", "行政代码"],
    "name": ["name", "admin_name", "名称", "行政区名称", "地区"],
    "province": ["province", "省", "省份", "province_name"],
    "prefecture": ["prefecture", "city", "市", "地级市", "州", "盟"],
    "county": ["county", "district", "县", "区县", "县市区"],
    "status": ["status", "状态"],
    "change_type": ["change_type", "变更类型", "调整类型"],
}


@dataclass(frozen=True)
class AdminCrosswalkResult:
    """Result metadata for crosswalk construction."""

    status: str
    input_files: list[Path]
    row_count: int
    low_confidence_count: int
    output_path: Path
    low_confidence_path: Path
    report_path: Path
    warnings: list[str] = field(default_factory=list)


def normalize_admin_name(name: Any) -> str:
    """Normalize Chinese administrative names for auxiliary matching."""

    text = str(name or "").strip()
    if not text or text.lower() in {"nan", "none"}:
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
        "省",
        "市",
        "县",
        "区",
        "州",
        "盟",
        "旗",
    ]
    for suffix in suffixes:
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)].strip()
    return text


def find_admin_code_files(admin_codes_dir: str | Path) -> list[Path]:
    """Find supported admin-code and crosswalk source files."""

    root = Path(admin_codes_dir).expanduser().resolve()
    if not root.exists():
        return []
    return sorted(
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_ADMIN_CODE_SUFFIXES
    )


def identify_admin_code_fields(columns: list[str] | Any) -> dict[str, str]:
    """Identify standard admin-code fields from input columns."""

    normalized = [(_normalize_field_name(column), str(column)) for column in columns]
    mapping: dict[str, str] = {}
    for role, candidates in FIELD_CANDIDATES.items():
        normalized_candidates = [_normalize_field_name(candidate) for candidate in candidates]
        for candidate in normalized_candidates:
            match = next((original for normal, original in normalized if normal == candidate), None)
            if match is not None:
                mapping[role] = match
                break
        if role not in mapping:
            for candidate in normalized_candidates:
                match = next((original for normal, original in normalized if candidate and candidate in normal), None)
                if match is not None:
                    mapping[role] = match
                    break
    return mapping


def build_admin_crosswalk(
    admin_codes_dir: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    year_min: int = 2000,
    year_max: int = 2025,
    target_boundary_year: int = 2022,
    manual_crosswalk_path: str | Path | None = None,
) -> AdminCrosswalkResult:
    """Build a standard administrative crosswalk and low-confidence review table."""

    import pandas as pd

    source_root = Path(admin_codes_dir).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    output_path = processed / "admin_crosswalk_2000_2025.csv"
    low_path = processed / "admin_crosswalk_low_confidence.csv"
    report_path = reports / "admin_crosswalk_summary.md"
    warnings: list[str] = []

    files = find_admin_code_files(source_root)
    if manual_crosswalk_path:
        manual_path = Path(manual_crosswalk_path).expanduser().resolve()
        if manual_path.exists():
            files.insert(0, manual_path)
        else:
            warnings.append(f"Manual crosswalk not found: {manual_path}")

    rows: list[dict[str, Any]] = []
    for path in files:
        try:
            frame = _read_source_table(path)
            mapping = identify_admin_code_fields(list(frame.columns))
            if _is_areacodes_result(frame):
                rows.extend(_standardize_areacodes_result(frame, path.name, year_min, year_max, target_boundary_year))
            else:
                rows.extend(_standardize_rows(frame, mapping, path.name, year_min, year_max))
        except Exception as exc:  # noqa: BLE001 - keep report generation alive
            warnings.append(f"Skipped {path}: {type(exc).__name__}: {exc}")

    crosswalk = pd.DataFrame(rows, columns=ADMIN_CROSSWALK_COLUMNS)
    if not crosswalk.empty:
        crosswalk = crosswalk.drop_duplicates(
            ["year", "admin_code_original", "admin_name_original", "admin_code_standard", "admin_name_standard"]
        )
    low_confidence = crosswalk[pd.to_numeric(crosswalk.get("match_confidence"), errors="coerce") < 0.85].copy()

    crosswalk.to_csv(output_path, index=False, encoding="utf-8-sig")
    low_confidence.to_csv(low_path, index=False, encoding="utf-8-sig")
    status = "missing" if not files else ("empty" if crosswalk.empty else "ok")
    if not files:
        warnings.append(f"No admin-code source files found under {source_root}.")
    result = AdminCrosswalkResult(
        status=status,
        input_files=files,
        row_count=int(len(crosswalk)),
        low_confidence_count=int(len(low_confidence)),
        output_path=output_path,
        low_confidence_path=low_path,
        report_path=report_path,
        warnings=warnings,
    )
    _write_report(result, source_root, year_min, year_max, target_boundary_year)
    return result


def _is_areacodes_result(frame: Any) -> bool:
    """Return True for yescallop/areacodes result.csv tables."""

    required = {"代码", "一级行政区", "二级行政区", "名称", "级别", "状态", "启用时间", "变更/弃用时间", "新代码"}
    return required.issubset(set(map(str, frame.columns)))


def _standardize_areacodes_result(
    frame: Any,
    source_name: str,
    year_min: int,
    year_max: int,
    target_boundary_year: int,
) -> list[dict[str, Any]]:
    """Expand yescallop/areacodes result.csv records into annual crosswalk rows."""

    records = [_areacodes_record(row) for _, row in frame.iterrows()]
    records = [record for record in records if record["code"] and record["start_year"] is not None]
    records_by_code: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        records_by_code.setdefault(record["code"], []).append(record)
    for code_records in records_by_code.values():
        code_records.sort(key=lambda item: int(item["start_year"]))

    rows: list[dict[str, Any]] = []
    for record in records:
        start_year = max(int(record["start_year"]), int(year_min))
        end_year = _active_end_year(record, int(year_max))
        if start_year > end_year:
            continue
        standard_codes = _resolve_areacodes_standard_codes(record["code"], records_by_code, int(target_boundary_year), set())
        method, confidence = _areacodes_match_method(record, standard_codes, records_by_code, int(target_boundary_year))
        standard_names = [_active_record_name(code, records_by_code, int(target_boundary_year)) for code in standard_codes]
        standard_names = [name for name in standard_names if name]
        for year in range(start_year, end_year + 1):
            rows.append(
                {
                    "year": year,
                    "admin_code_original": record["code"],
                    "admin_name_original": record["name"],
                    "admin_code_standard": ";".join(standard_codes),
                    "admin_name_standard": ";".join(standard_names) if standard_names else record["name"],
                    "province_standard": record["province"],
                    "prefecture_standard": record["prefecture"],
                    "county_standard": record["name"] if record["level"] == "县级" else "",
                    "match_confidence": confidence,
                    "match_method": method,
                    "notes": (
                        f"source={source_name}; level={record['level']}; status={record['status']}; "
                        f"start={record['start_year']}; end={record['end_year'] or ''}; target_boundary_year={target_boundary_year}"
                    ),
                }
            )
    return rows


def _areacodes_record(row: Any) -> dict[str, Any]:
    """Normalize a yescallop/areacodes result.csv row."""

    return {
        "code": _clean_text(row.get("代码")),
        "province": _clean_text(row.get("一级行政区")),
        "prefecture": _clean_text(row.get("二级行政区")),
        "name": _clean_text(row.get("名称")),
        "level": _clean_text(row.get("级别")),
        "status": _clean_text(row.get("状态")),
        "start_year": _clean_year(row.get("启用时间")),
        "end_year": _clean_year(row.get("变更/弃用时间")),
        "new_codes": _parse_areacodes_new_codes(row.get("新代码")),
    }


def _active_end_year(record: dict[str, Any], fallback_year_max: int) -> int:
    """Return inclusive active end year within the output window."""

    end_year = record.get("end_year")
    if end_year is None:
        return int(fallback_year_max)
    return min(int(end_year) - 1, int(fallback_year_max))


def _parse_areacodes_new_codes(value: Any) -> list[str]:
    """Parse semicolon-separated new-code references, ignoring bracketed years."""

    text = _clean_text(value)
    if not text:
        return []
    return re.findall(r"\d{6,12}", text)


def _resolve_areacodes_standard_codes(
    code: str,
    records_by_code: dict[str, list[dict[str, Any]]],
    target_year: int,
    visited: set[str],
) -> list[str]:
    """Resolve a historical code to the best available target-boundary code."""

    if code in visited:
        return [code]
    visited.add(code)
    if _active_record_for_year(code, records_by_code, target_year):
        return [code]
    records = records_by_code.get(code, [])
    new_codes: list[str] = []
    for record in sorted(records, key=lambda item: int(item["start_year"] or 0), reverse=True):
        new_codes.extend(record.get("new_codes", []))
        if new_codes:
            break
    if not new_codes:
        return [code]
    resolved: list[str] = []
    for new_code in new_codes:
        resolved.extend(_resolve_areacodes_standard_codes(new_code, records_by_code, target_year, set(visited)))
    return _unique_preserve_order(resolved)


def _areacodes_match_method(
    record: dict[str, Any],
    standard_codes: list[str],
    records_by_code: dict[str, list[dict[str, Any]]],
    target_year: int,
) -> tuple[str, float]:
    """Classify areacodes target-boundary mapping confidence."""

    if standard_codes == [record["code"]] and _active_record_for_year(record["code"], records_by_code, target_year):
        return "target_boundary_exact_code", 1.0
    if len(standard_codes) == 1 and standard_codes[0] != record["code"]:
        return "target_boundary_explicit_new_code_chain", 0.95
    if len(standard_codes) > 1:
        return "target_boundary_many_to_many_new_code_chain", 0.75
    start_year = record.get("start_year")
    if start_year is not None and int(start_year) > int(target_year):
        return "future_record_after_target_boundary", 0.7
    return "unresolved_to_target_boundary", 0.6


def _active_record_for_year(
    code: str,
    records_by_code: dict[str, list[dict[str, Any]]],
    year: int,
) -> dict[str, Any] | None:
    """Return the record for a code that is active in a year."""

    for record in records_by_code.get(code, []):
        start_year = record.get("start_year")
        end_year = record.get("end_year")
        if start_year is None:
            continue
        if int(start_year) <= int(year) and (end_year is None or int(year) < int(end_year)):
            return record
    return None


def _active_record_name(code: str, records_by_code: dict[str, list[dict[str, Any]]], year: int) -> str:
    """Return target-year record name for a standard code if available."""

    record = _active_record_for_year(code, records_by_code, year)
    return _clean_text(record.get("name")) if record else ""


def _unique_preserve_order(values: list[str]) -> list[str]:
    """Return unique values without changing their order."""

    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            output.append(value)
    return output


def _standardize_rows(frame: Any, mapping: dict[str, str], source_name: str, year_min: int, year_max: int) -> list[dict[str, Any]]:
    """Convert source rows to the standard crosswalk schema."""

    rows: list[dict[str, Any]] = []
    if not mapping:
        return rows
    for _, row in frame.iterrows():
        year = _clean_year(_get(row, mapping, "year"))
        if year is None:
            year = _extract_year(source_name)
        if year is not None and not (int(year_min) <= int(year) <= int(year_max)):
            continue
        original_code = _clean_text(_get(row, mapping, "old_code")) or _clean_text(_get(row, mapping, "code"))
        standard_code = _clean_text(_get(row, mapping, "new_code")) or _clean_text(_get(row, mapping, "code")) or original_code
        province = _clean_text(_get(row, mapping, "province"))
        prefecture = _clean_text(_get(row, mapping, "prefecture"))
        county = _clean_text(_get(row, mapping, "county"))
        name = _clean_text(_get(row, mapping, "name")) or county or prefecture or province
        standard_name = county or prefecture or province or name
        method, confidence = _match_method(original_code, standard_code, name, standard_name, bool(_clean_text(_get(row, mapping, "new_code"))))
        rows.append(
            {
                "year": "" if year is None else int(year),
                "admin_code_original": original_code,
                "admin_name_original": name,
                "admin_code_standard": standard_code,
                "admin_name_standard": standard_name,
                "province_standard": province,
                "prefecture_standard": prefecture,
                "county_standard": county,
                "match_confidence": confidence,
                "match_method": method,
                "notes": _notes(row, mapping),
            }
        )
    return rows


def _match_method(
    original_code: str,
    standard_code: str,
    original_name: str,
    standard_name: str,
    has_explicit_new_code: bool,
) -> tuple[str, float]:
    """Assign a match method and confidence from available evidence."""

    if original_code and standard_code and original_code == standard_code and _looks_like_admin_code(original_code):
        return "exact_admin_code_match", 1.0
    if original_code and standard_code and has_explicit_new_code:
        return "explicit_old_new_crosswalk", 0.95
    if original_name and standard_name and original_name == standard_name:
        return "exact_name_match", 0.9
    normalized_original = normalize_admin_name(original_name)
    normalized_standard = normalize_admin_name(standard_name)
    if normalized_original and normalized_standard:
        ratio = SequenceMatcher(None, normalized_original, normalized_standard).ratio()
        return "normalized_name_fuzzy_match", round(float(ratio), 3)
    return "unmatched", 0.0


def _looks_like_admin_code(value: str) -> bool:
    """Return True when a value looks like an administrative code."""

    return bool(re.fullmatch(r"\d{6,12}", str(value).strip()))


def _read_source_table(path: Path) -> Any:
    """Read CSV, JSON, or Excel admin-code source tables."""

    import pandas as pd

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, list):
                    data = value
                    break
        return pd.DataFrame(data)
    raise ValueError(f"Unsupported admin-code source file: {path}")


def _get(row: Any, mapping: dict[str, str], role: str) -> Any:
    """Get a mapped role value from a pandas row."""

    column = mapping.get(role)
    return row.get(column) if column else None


def _notes(row: Any, mapping: dict[str, str]) -> str:
    """Build source notes from status and change-type fields."""

    values = []
    for role in ("status", "change_type"):
        value = _clean_text(_get(row, mapping, role))
        if value:
            values.append(f"{role}={value}")
    return "; ".join(values)


def _write_report(
    result: AdminCrosswalkResult,
    source_root: Path,
    year_min: int,
    year_max: int,
    target_boundary_year: int,
) -> None:
    """Write a Markdown summary for crosswalk construction."""

    lines = [
        "# Admin Crosswalk Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Source directory: `{source_root}`",
        f"- Target years: {year_min}-{year_max}",
        f"- Target boundary year: {target_boundary_year}",
        f"- Input files: {len(result.input_files)}",
        f"- Crosswalk rows: {result.row_count}",
        f"- Low-confidence rows: {result.low_confidence_count}",
        f"- Output: `{result.output_path}`",
        f"- Low-confidence review table: `{result.low_confidence_path}`",
        "",
        "## Boundary Policy",
        "",
        "- Main models map historical statistics to the 2022 event boundary when stable cross-year boundary data are unavailable.",
        "- Rows with confidence below 0.85 require manual review before automatic merging.",
        "",
        "## Warnings",
        "",
    ]
    if result.warnings:
        lines.extend(f"- {warning}" for warning in result.warnings)
    else:
        lines.append("- None.")
    lines.append("")
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _normalize_field_name(name: Any) -> str:
    """Normalize field names for matching."""

    return re.sub(r"[\s_\-/\\()（）\[\]【】{}]", "", str(name or "").strip().lower())


def _clean_text(value: Any) -> str:
    """Normalize string values."""

    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "na"} else text


def _clean_year(value: Any) -> int | None:
    """Parse a year value."""

    text = _clean_text(value)
    if not text:
        return None
    match = re.search(r"(19|20)\d{2}", text)
    return int(match.group(0)) if match else None


def _extract_year(name: str) -> int | None:
    """Extract a year from a source filename."""

    match = re.search(r"(19|20)\d{2}", str(name))
    return int(match.group(0)) if match else None
