"""Manual official yield panel template import and normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger


TEMPLATE_COLUMNS = [
    "source_id",
    "source_name",
    "source_url_or_reference",
    "source_type",
    "content_year",
    "yearbook_year",
    "province",
    "prefecture",
    "county",
    "admin_code",
    "admin_level",
    "crop",
    "area_value",
    "area_unit",
    "production_value",
    "production_unit",
    "yield_value",
    "yield_unit",
    "notes",
]

SUPPORTED_CROPS = {
    "rice",
    "early_rice",
    "single_rice",
    "middle_rice",
    "late_rice",
    "grain",
    "wheat",
    "maize",
}

AREA_TO_HECTARE = {
    "hectare": 1.0,
    "mu": 1.0 / 15.0,
    "thousand_hectare": 1000.0,
    "ten_thousand_mu": 10000.0 / 15.0,
}

PRODUCTION_TO_TON = {
    "ton": 1.0,
    "kg": 0.001,
    "jin": 0.0005,
    "ten_thousand_ton": 10000.0,
    "ten_thousand_jin": 5.0,
}

YIELD_TO_KG_PER_HECTARE = {
    "kg_per_hectare": 1.0,
    "kg_per_mu": 15.0,
    "jin_per_mu": 7.5,
    "ton_per_hectare": 1000.0,
}

CLEANED_COLUMNS = [
    *TEMPLATE_COLUMNS,
    "year",
    "admin_id",
    "source_priority",
    "match_confidence",
    "match_method",
    "area_ha",
    "area_hectare",
    "production_ton",
    "yield_kg_ha",
    "yield_kg_per_hectare",
    "yield_source",
    "quality_flag",
    "validation_notes",
]


@dataclass(frozen=True)
class ManualYieldImportResult:
    """Result metadata for the manual yield import step."""

    status: str
    input_rows: int
    output_rows: int
    skipped_rows: int
    outputs: dict[str, Path]
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/manual_yield_import_summary.md")


def create_yield_panel_template(template_path: str | Path) -> Path:
    """Create an empty manual yield template with the required columns."""

    path = Path(template_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=TEMPLATE_COLUMNS).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def import_manual_yield_panel(
    template_path: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
) -> ManualYieldImportResult:
    """Import, validate, and normalize a manually curated official yield panel."""

    input_path = Path(template_path).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = processed / "manual_yield_panel_cleaned.csv"
    parquet_path = processed / "manual_yield_panel_cleaned.parquet"
    low_confidence_path = processed / "manual_yield_panel_low_confidence.csv"
    report_path = reports / "manual_yield_import_summary.md"
    outputs = {"csv": csv_path, "parquet": parquet_path, "low_confidence": low_confidence_path}
    warnings: list[str] = []

    raw = read_manual_yield_template(input_path, warnings)
    crosswalk = _load_crosswalk(processed, warnings)
    cleaned, row_warnings = normalize_manual_yield_frame(raw, crosswalk=crosswalk)
    warnings.extend(row_warnings)

    low_confidence = _low_confidence_rows(cleaned)
    _write_low_confidence(low_confidence, low_confidence_path)
    cleaned = cleaned[~cleaned.index.isin(low_confidence.index)].copy() if not cleaned.empty else cleaned
    _write_cleaned_outputs(cleaned, csv_path, parquet_path, warnings)
    status = _status(input_path, raw, cleaned, warnings)
    result = ManualYieldImportResult(
        status=status,
        input_rows=len(raw),
        output_rows=len(cleaned),
        skipped_rows=max(len(raw) - len(cleaned), 0),
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_summary_report(result, input_path)
    logger.info("Manual yield import status: {}", result.status)
    logger.info("Manual yield rows: {} input, {} output", result.input_rows, result.output_rows)
    return result


def read_manual_yield_template(path: str | Path, warnings: list[str] | None = None) -> pd.DataFrame:
    """Read the manual template, returning an empty template frame for missing or empty files."""

    warning_sink = warnings if warnings is not None else []
    input_path = Path(path).expanduser().resolve()
    if not input_path.exists():
        warning_sink.append(f"Manual yield template not found: {input_path}")
        return pd.DataFrame(columns=TEMPLATE_COLUMNS)

    try:
        frame = pd.read_csv(input_path, dtype=str, keep_default_na=False, low_memory=False)
    except pd.errors.EmptyDataError:
        warning_sink.append(f"Manual yield template is empty: {input_path}")
        return pd.DataFrame(columns=TEMPLATE_COLUMNS)

    missing = [column for column in TEMPLATE_COLUMNS if column not in frame.columns]
    if missing:
        warning_sink.append(f"Manual yield template missing columns: {', '.join(missing)}")
        for column in missing:
            frame[column] = ""
    return frame[TEMPLATE_COLUMNS].fillna("")


def normalize_manual_yield_frame(
    frame: pd.DataFrame,
    crosswalk: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Normalize manual yield rows to canonical hectare, ton, and kg/hectare metrics."""

    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame(columns=CLEANED_COLUMNS), warnings

    for index, raw_row in frame.fillna("").iterrows():
        row_number = int(index) + 2
        row = {column: _clean_text(raw_row.get(column, "")) for column in TEMPLATE_COLUMNS}
        if not row["admin_code"] and crosswalk is not None and not crosswalk.empty:
            match = _match_crosswalk(row, crosswalk)
            if match:
                row["admin_code"] = match["admin_code"]
                row.setdefault("_match_confidence", match["confidence"])
                row.setdefault("_match_method", match["method"])
        normalized, row_notes = _normalize_row(row)
        if normalized is None:
            warnings.append(f"Row {row_number} skipped: {'; '.join(row_notes)}")
            continue
        if row_notes:
            normalized["validation_notes"] = "; ".join(row_notes)
        rows.append(normalized)

    cleaned = pd.DataFrame(rows, columns=CLEANED_COLUMNS)
    return _deduplicate_by_priority(cleaned), warnings


def _normalize_row(row: dict[str, str]) -> tuple[dict[str, Any] | None, list[str]]:
    notes: list[str] = []
    crop = row["crop"].lower()
    if crop not in SUPPORTED_CROPS:
        return None, [f"unsupported crop {row['crop']!r}"]
    row["crop"] = crop

    content_year = _parse_int(row["content_year"])
    if content_year is None:
        return None, ["content_year is required and must be an integer"]
    yearbook_year = _parse_int(row["yearbook_year"])

    if not row["province"] and not row["admin_code"]:
        return None, ["province or admin_code is required"]
    if row["admin_level"] and row["admin_level"] not in {"national", "province", "prefecture", "county"}:
        notes.append(f"unrecognized admin_level {row['admin_level']!r}")

    area_hectare = _convert_optional(row["area_value"], row["area_unit"], AREA_TO_HECTARE, "area", notes)
    production_ton = _convert_optional(
        row["production_value"],
        row["production_unit"],
        PRODUCTION_TO_TON,
        "production",
        notes,
    )
    yield_kg_per_hectare = _convert_optional(
        row["yield_value"],
        row["yield_unit"],
        YIELD_TO_KG_PER_HECTARE,
        "yield",
        notes,
    )
    yield_source = "reported"

    if yield_kg_per_hectare is None and area_hectare and production_ton is not None:
        yield_kg_per_hectare = production_ton * 1000.0 / area_hectare
        yield_source = "derived_from_area_and_production"

    if area_hectare is None and production_ton is None and yield_kg_per_hectare is None:
        return None, ["at least one area, production, or yield metric is required"]

    admin_id = row["admin_code"] or "|".join([row["province"], row["prefecture"], row["county"]]).strip("|")
    source_priority = _source_priority(row["source_type"], row["source_name"])
    parsed_confidence = _parse_float(row.get("_match_confidence", ""))
    match_confidence = parsed_confidence if parsed_confidence is not None else (1.0 if row.get("admin_code") else 0.0)
    match_method = row.get("_match_method", "provided_admin_code" if row.get("admin_code") else "name_unmatched")
    normalized: dict[str, Any] = {
        **row,
        "content_year": content_year,
        "yearbook_year": yearbook_year,
        "year": content_year,
        "admin_id": admin_id,
        "source_priority": source_priority,
        "match_confidence": match_confidence,
        "match_method": match_method,
        "area_ha": area_hectare,
        "area_hectare": area_hectare,
        "production_ton": production_ton,
        "yield_kg_ha": yield_kg_per_hectare,
        "yield_kg_per_hectare": yield_kg_per_hectare,
        "yield_source": yield_source if yield_kg_per_hectare is not None else "",
        "quality_flag": "manual_official_normalized",
        "validation_notes": "",
    }
    return normalized, notes


def _load_crosswalk(processed: Path, warnings: list[str]) -> pd.DataFrame:
    """Load generated admin crosswalk for manual yield matching."""

    path = processed / "admin_crosswalk_2000_2025.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        columns = [
            "year",
            "admin_code_standard",
            "admin_name_standard",
            "province_standard",
            "prefecture_standard",
            "county_standard",
            "match_confidence",
        ]
        frame = pd.read_csv(path, dtype=str, usecols=lambda column: column in columns, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not read admin crosswalk for manual yield matching: {type(exc).__name__}: {exc}")
        return pd.DataFrame()
    return _prepare_crosswalk(frame)


def _prepare_crosswalk(frame: pd.DataFrame) -> pd.DataFrame:
    """Prepare normalized name columns for matching."""

    if frame.empty:
        return frame
    output = frame.copy().fillna("")
    for source, target in [
        ("province_standard", "_province_norm"),
        ("prefecture_standard", "_prefecture_norm"),
        ("county_standard", "_county_norm"),
        ("admin_name_standard", "_name_norm"),
    ]:
        output[target] = output[source].map(_normalize_place_name) if source in output.columns else ""
    return output.drop_duplicates()


def _match_crosswalk(row: dict[str, str], crosswalk: pd.DataFrame) -> dict[str, Any] | None:
    """Match a manual row to crosswalk by names with confidence."""

    if crosswalk.empty:
        return None
    year = row.get("content_year", "")
    candidates = crosswalk
    if year and "year" in candidates.columns:
        same_year = candidates[candidates["year"].astype(str) == str(year)]
        if not same_year.empty:
            candidates = same_year
    province = _normalize_place_name(row.get("province", ""))
    prefecture = _normalize_place_name(row.get("prefecture", ""))
    county = _normalize_place_name(row.get("county", ""))

    if province:
        candidates = candidates[candidates["_province_norm"].eq(province) | candidates["_name_norm"].eq(province)]
    if prefecture:
        exact_pref = candidates[candidates["_prefecture_norm"].eq(prefecture) | candidates["_name_norm"].eq(prefecture)]
        if not exact_pref.empty:
            candidates = exact_pref
    if county:
        exact_county = candidates[candidates["_county_norm"].eq(county) | candidates["_name_norm"].eq(county)]
        if not exact_county.empty:
            candidates = exact_county
    if candidates.empty:
        return None

    target_name = county or prefecture or province
    best_row = None
    best_score = 0.0
    for _, candidate in candidates.head(5000).iterrows():
        names = [
            str(candidate.get("_county_norm", "")),
            str(candidate.get("_prefecture_norm", "")),
            str(candidate.get("_province_norm", "")),
            str(candidate.get("_name_norm", "")),
        ]
        score = max((SequenceMatcher(None, target_name, name).ratio() for name in names if name), default=0.0)
        if score > best_score:
            best_score = score
            best_row = candidate
    if best_row is None:
        return None
    return {
        "admin_code": str(best_row.get("admin_code_standard", "")),
        "confidence": round(best_score, 6),
        "method": "crosswalk_exact_or_fuzzy_name",
    }


def _normalize_place_name(value: Any) -> str:
    """Normalize Chinese administrative place names for auxiliary matching."""

    text = _clean_text(value)
    suffixes = ["壮族自治区", "回族自治区", "维吾尔自治区", "自治县", "自治州", "地区", "省", "市", "县", "区", "盟"]
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if text.endswith(suffix):
                text = text[: -len(suffix)]
                changed = True
                break
    return text


def _low_confidence_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return rows requiring manual review before entering cleaned panel."""

    if frame.empty or "match_confidence" not in frame.columns:
        return frame.iloc[0:0].copy()
    confidence = pd.to_numeric(frame["match_confidence"], errors="coerce")
    return frame[confidence.notna() & (confidence < 0.85)].copy()


def _write_low_confidence(frame: pd.DataFrame, path: Path) -> None:
    """Write low-confidence manual yield matches."""

    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def _convert_optional(
    value: str,
    unit: str,
    factors: dict[str, float],
    label: str,
    notes: list[str],
) -> float | None:
    if not value and not unit:
        return None
    numeric = _parse_float(value)
    if numeric is None:
        notes.append(f"{label}_value is not numeric")
        return None
    normalized_unit = unit.lower()
    if normalized_unit not in factors:
        notes.append(f"unsupported {label}_unit {unit!r}")
        return None
    return numeric * factors[normalized_unit]


def _source_priority(source_type: str, source_name: str) -> int:
    """Infer source priority when the manual template has duplicate observations."""

    text = f"{source_type} {source_name}".lower()
    if any(token in text for token in ["official", "统计局", "调查总队", "government"]):
        return 100
    if any(token in text for token in ["yearbook", "年鉴"]):
        return 90
    if any(token in text for token in ["cnki", "csyd", "eps"]):
        return 80
    if any(token in text for token in ["third", "第三方", "tjnj", "yougis"]):
        return 30
    return 50


def _deduplicate_by_priority(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep highest-priority source for duplicate admin-year-crop rows."""

    if frame.empty:
        return frame
    work = frame.copy()
    keys = [key for key in ["admin_id", "year", "crop"] if key in work.columns]
    if len(keys) < 3:
        return work
    work["_priority_sort"] = pd.to_numeric(work["source_priority"], errors="coerce").fillna(0)
    work = work.sort_values(["_priority_sort"], ascending=False)
    work = work.drop_duplicates(subset=keys, keep="first")
    return work.drop(columns=["_priority_sort"])[CLEANED_COLUMNS]


def _write_cleaned_outputs(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.replace({"": None}).to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001 - CSV remains readable if Parquet support is unavailable
        warnings.append(f"Could not write Parquet {parquet_path}: {type(exc).__name__}: {exc}")
        if parquet_path.exists():
            parquet_path.unlink()


def _write_summary_report(result: ManualYieldImportResult, input_path: Path) -> None:
    lines = [
        "# Manual Yield Import Summary",
        "",
        f"- Status: {result.status}",
        f"- Input template: `{input_path}`",
        f"- Input rows: {result.input_rows}",
        f"- Output rows: {result.output_rows}",
        f"- Skipped rows: {result.skipped_rows}",
        f"- CSV output: `{result.outputs['csv']}`",
        f"- Parquet output: `{result.outputs['parquet']}`",
        f"- Low-confidence review output: `{result.outputs['low_confidence']}`",
        "",
        "## Supported Codes",
        "",
        f"- Crops: {', '.join(sorted(SUPPORTED_CROPS))}",
        f"- Area units: {', '.join(AREA_TO_HECTARE)}",
        f"- Production units: {', '.join(PRODUCTION_TO_TON)}",
        f"- Yield units: {', '.join(YIELD_TO_KG_PER_HECTARE)}",
    ]
    if result.warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _status(input_path: Path, raw: pd.DataFrame, cleaned: pd.DataFrame, warnings: list[str]) -> str:
    if not input_path.exists():
        return "missing"
    if raw.empty:
        return "empty"
    if cleaned.empty:
        return "empty_after_validation"
    if warnings:
        return "ok_with_warnings"
    return "ok"


def _parse_float(value: str) -> float | None:
    text = _clean_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: str) -> int | None:
    number = _parse_float(value)
    if number is None:
        return None
    if not float(number).is_integer():
        return None
    return int(number)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
