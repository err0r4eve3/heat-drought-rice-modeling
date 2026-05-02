"""Province grain yield backfill import for 2008-2015 yearbook gaps."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


BACKFILL_TEMPLATE_COLUMNS = [
    "source_id",
    "source_name",
    "source_url_or_reference",
    "content_year",
    "yearbook_year",
    "province",
    "province_code",
    "admin_level",
    "crop",
    "yield_value",
    "yield_unit",
    "production_value",
    "production_unit",
    "area_value",
    "area_unit",
    "notes",
]

AREA_TO_HECTARE = {
    "hectare": 1.0,
    "ha": 1.0,
    "mu": 1.0 / 15.0,
    "thousand_hectare": 1000.0,
    "ten_thousand_mu": 10000.0 / 15.0,
}

PRODUCTION_TO_TON = {
    "ton": 1.0,
    "kg": 0.001,
    "ten_thousand_ton": 10000.0,
}

YIELD_TO_KG_HA = {
    "kg_per_hectare": 1.0,
    "kg_per_ha": 1.0,
    "kg_per_mu": 15.0,
    "jin_per_mu": 7.5,
    "ton_per_hectare": 1000.0,
}

CLEANED_COLUMNS = [
    *BACKFILL_TEMPLATE_COLUMNS,
    "year",
    "admin_id",
    "admin_code",
    "source_type",
    "area_ha",
    "area_hectare",
    "production_ton",
    "yield_kg_ha",
    "yield_kg_per_hectare",
    "yield_source",
    "is_backfill",
    "backfill_period",
    "quality_flag",
    "validation_notes",
]


@dataclass(frozen=True)
class ProvinceGrainBackfillResult:
    """Result metadata for province grain backfill import."""

    status: str
    input_rows: int
    output_rows: int
    skipped_rows: int
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/province_grain_backfill_summary.md")


def create_province_grain_backfill_template(path: str | Path) -> Path:
    """Create an empty 2008-2015 province grain backfill template."""

    output = Path(path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=BACKFILL_TEMPLATE_COLUMNS).to_csv(output, index=False, encoding="utf-8-sig")
    return output


def import_province_grain_backfill(
    template_path: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
) -> ProvinceGrainBackfillResult:
    """Import and normalize the province grain backfill template."""

    template = Path(template_path).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = processed / "province_grain_backfill_2008_2015_cleaned.csv"
    parquet_path = processed / "province_grain_backfill_2008_2015_cleaned.parquet"
    report_path = reports / "province_grain_backfill_summary.md"
    outputs = {"csv": csv_path, "parquet": parquet_path}
    warnings: list[str] = []
    raw = read_province_grain_backfill_template(template, warnings)
    cleaned, row_warnings = normalize_province_grain_backfill(raw)
    warnings.extend(row_warnings)
    _write_outputs(cleaned, csv_path, parquet_path, warnings)
    status = _status(template, raw, cleaned, warnings)
    result = ProvinceGrainBackfillResult(
        status=status,
        input_rows=len(raw),
        output_rows=len(cleaned),
        skipped_rows=max(len(raw) - len(cleaned), 0),
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_report(result, template)
    return result


def read_province_grain_backfill_template(path: str | Path, warnings: list[str] | None = None) -> pd.DataFrame:
    """Read the backfill template, returning an empty frame if it is missing or empty."""

    warning_sink = warnings if warnings is not None else []
    template = Path(path).expanduser().resolve()
    if not template.exists():
        warning_sink.append(f"Province grain backfill template not found: {template}")
        return pd.DataFrame(columns=BACKFILL_TEMPLATE_COLUMNS)
    try:
        frame = pd.read_csv(template, dtype=str, keep_default_na=False, low_memory=False)
    except pd.errors.EmptyDataError:
        warning_sink.append(f"Province grain backfill template is empty: {template}")
        return pd.DataFrame(columns=BACKFILL_TEMPLATE_COLUMNS)
    missing = [column for column in BACKFILL_TEMPLATE_COLUMNS if column not in frame.columns]
    if missing:
        warning_sink.append(f"Province grain backfill template missing columns: {', '.join(missing)}")
        for column in missing:
            frame[column] = ""
    return frame[BACKFILL_TEMPLATE_COLUMNS].fillna("")


def normalize_province_grain_backfill(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Normalize template rows into province official grain yield records."""

    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame(columns=CLEANED_COLUMNS), warnings
    for index, raw_row in frame.fillna("").iterrows():
        row_number = int(index) + 2
        row = {column: _clean_text(raw_row.get(column, "")) for column in BACKFILL_TEMPLATE_COLUMNS}
        normalized, notes = _normalize_row(row)
        if normalized is None:
            warnings.append(f"Row {row_number} skipped: {'; '.join(notes)}")
            continue
        if notes:
            normalized["validation_notes"] = "; ".join(notes)
        rows.append(normalized)
    cleaned = pd.DataFrame(rows, columns=CLEANED_COLUMNS)
    if not cleaned.empty:
        cleaned["is_backfill"] = cleaned["is_backfill"].astype(object)
    return _deduplicate(cleaned), warnings


def _normalize_row(row: dict[str, str]) -> tuple[dict[str, Any] | None, list[str]]:
    notes: list[str] = []
    content_year = _parse_int(row["content_year"])
    if content_year is None:
        return None, ["content_year is required and must be an integer"]
    if content_year < 2008 or content_year > 2015:
        return None, ["content_year must be in 2008-2015"]

    yearbook_year = _parse_int(row["yearbook_year"])
    expected_yearbook = content_year + 1
    if yearbook_year is None:
        yearbook_year = expected_yearbook
    elif yearbook_year != expected_yearbook:
        return None, [f"yearbook_year must equal content_year + 1 ({expected_yearbook})"]

    if not row["source_name"]:
        row["source_name"] = f"China Statistical Yearbook {yearbook_year}"
    if not row["province"] and not row["province_code"]:
        return None, ["province or province_code is required"]
    row["admin_level"] = row["admin_level"] or "province"
    if row["admin_level"] != "province":
        return None, ["admin_level must be province"]
    row["crop"] = (row["crop"] or "grain").lower()
    if row["crop"] != "grain":
        return None, ["crop must be grain for this backfill template"]

    area_ha = _convert_optional(row["area_value"], row["area_unit"], AREA_TO_HECTARE, "area", notes)
    production_ton = _convert_optional(
        row["production_value"],
        row["production_unit"],
        PRODUCTION_TO_TON,
        "production",
        notes,
    )
    yield_kg_ha = _convert_optional(row["yield_value"], row["yield_unit"], YIELD_TO_KG_HA, "yield", notes)
    yield_source = "reported"
    if yield_kg_ha is None and area_ha and production_ton is not None:
        yield_kg_ha = production_ton * 1000.0 / area_ha
        yield_source = "derived_from_area_and_production"
    if yield_kg_ha is None:
        return None, ["yield_value or production_value plus area_value is required"]

    row["yearbook_year"] = yearbook_year
    row["content_year"] = content_year
    admin_id = row["province_code"] or row["province"]
    normalized: dict[str, Any] = {
        **row,
        "year": content_year,
        "admin_id": admin_id,
        "admin_code": row["province_code"],
        "source_type": "official_yearbook",
        "area_ha": area_ha,
        "area_hectare": area_ha,
        "production_ton": production_ton,
        "yield_kg_ha": yield_kg_ha,
        "yield_kg_per_hectare": yield_kg_ha,
        "yield_source": yield_source,
        "is_backfill": True,
        "backfill_period": "2008-2015",
        "quality_flag": "official_yearbook_backfill",
        "validation_notes": "",
    }
    return normalized, notes


def _convert_optional(
    value: str,
    unit: str,
    factors: dict[str, float],
    label: str,
    notes: list[str],
) -> float | None:
    if not value and not unit:
        return None
    number = _parse_float(value)
    if number is None:
        notes.append(f"{label}_value is not numeric")
        return None
    normalized_unit = unit.lower()
    if normalized_unit not in factors:
        notes.append(f"unsupported {label}_unit {unit!r}")
        return None
    return number * factors[normalized_unit]


def _deduplicate(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    keys = ["province", "province_code", "year", "crop"]
    return frame.drop_duplicates(subset=keys, keep="last")[CLEANED_COLUMNS]


def _write_outputs(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.replace({"": None}).to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not write province grain backfill parquet: {type(exc).__name__}: {exc}")


def _write_report(result: ProvinceGrainBackfillResult, template: Path) -> None:
    lines = [
        "# Province Grain Backfill Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Template: `{template}`",
        f"- Status: {result.status}",
        f"- Input rows: {result.input_rows}",
        f"- Output rows: {result.output_rows}",
        f"- Skipped rows: {result.skipped_rows}",
        f"- CSV output: `{result.outputs['csv']}`",
        f"- Parquet output: `{result.outputs['parquet']}`",
        "",
        "## Rules",
        "",
        "- content_year must be 2008-2015.",
        "- yearbook_year must equal content_year + 1.",
        "- admin_level is province.",
        "- crop is grain.",
        "- source_type is official_yearbook.",
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _status(template: Path, raw: pd.DataFrame, cleaned: pd.DataFrame, warnings: list[str]) -> str:
    if not template.exists():
        return "missing"
    if raw.empty:
        return "empty"
    if cleaned.empty:
        return "empty_after_validation"
    if warnings:
        return "ok_with_warnings"
    return "ok"


def _parse_float(value: Any) -> float | None:
    text = _clean_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    number = _parse_float(value)
    if number is None or not float(number).is_integer():
        return None
    return int(number)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
