"""Diagnostics for exposure coverage in the model panel."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


DIAGNOSIS_COLUMNS = ["section", "metric", "value"]
CAUSES = [
    "no_annual_exposure_panel",
    "only_2022_event_exposure",
    "study_region_mismatch",
    "national_yield_but_regional_exposure",
    "admin_join_mismatch",
    "year_join_mismatch",
    "climate_data_missing",
    "aggregation_failed",
    "unknown",
]


@dataclass(frozen=True)
class ExposureDiagnosisResult:
    """Result metadata for exposure coverage diagnostics."""

    status: str
    exposure_coverage_status: str
    model_rows: int
    exposure_nonmissing: int
    exposure_rate: float
    likely_causes: list[str] = field(default_factory=list)
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/exposure_coverage_diagnosis.md")


def diagnose_exposure_coverage(
    model_panel: str | Path,
    processed_dir: str | Path,
    interim_dir: str | Path,
    output_dir: str | Path,
    reports_dir: str | Path,
    main_event_year: int = 2022,
    main_year_min: int = 2000,
    main_year_max: int = 2024,
    study_provinces: list[str] | None = None,
) -> ExposureDiagnosisResult:
    """Diagnose why annual exposure coverage is low."""

    panel_path = Path(model_panel).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve()
    outputs = Path(output_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    outputs.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = outputs / "exposure_coverage_diagnosis.csv"
    report_path = reports / "exposure_coverage_diagnosis.md"
    warnings: list[str] = []

    model = _read_optional_table(panel_path, warnings)
    model = _filter_year_range(model, main_year_min, main_year_max)
    study_model = _read_optional_table(processed / "model_panel_study_region.csv", warnings)
    study_model = _filter_year_range(study_model, main_year_min, main_year_max)
    climate = _read_first_existing(
        [
            processed / "annual_exposure_panel.parquet",
            processed / "annual_exposure_panel.csv",
            processed / "admin_climate_panel.parquet",
            processed / "admin_climate_panel.csv",
            interim / "climate_province_growing_season.parquet",
            interim / "climate_province_growing_season.csv",
        ],
        warnings,
    )
    remote = _read_first_existing(
        [
            processed / "admin_remote_sensing_panel.parquet",
            processed / "admin_remote_sensing_panel.csv",
            interim / "remote_sensing_province_growing_season.parquet",
            interim / "remote_sensing_province_growing_season.csv",
        ],
        warnings,
    )

    exposure_field = _resolve_exposure_field(model)
    exposure_nonmissing = _non_missing_count(model, exposure_field) if exposure_field else 0
    exposure_rate = exposure_nonmissing / len(model) if len(model) else 0.0
    causes = _infer_causes(
        model=model,
        study_model=study_model,
        climate=climate,
        remote=remote,
        exposure_field=exposure_field,
        main_event_year=main_event_year,
        exposure_rate=exposure_rate,
        study_provinces=study_provinces or [],
    )
    coverage_status = _coverage_status(model, exposure_field, exposure_rate, main_event_year)

    rows = _diagnostic_rows(
        model=model,
        study_model=study_model,
        climate=climate,
        remote=remote,
        exposure_field=exposure_field,
        exposure_nonmissing=exposure_nonmissing,
        exposure_rate=exposure_rate,
        likely_causes=causes,
        coverage_status=coverage_status,
        main_event_year=main_event_year,
    )
    pd.DataFrame(rows, columns=DIAGNOSIS_COLUMNS).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = ExposureDiagnosisResult(
        status="ok" if len(model) else "empty",
        exposure_coverage_status=coverage_status,
        model_rows=len(model),
        exposure_nonmissing=exposure_nonmissing,
        exposure_rate=exposure_rate,
        likely_causes=causes,
        outputs={"diagnosis_csv": csv_path},
        warnings=warnings,
        report_path=report_path,
    )
    _write_report(result, panel_path, model, study_model, climate, remote, exposure_field, rows)
    return result


def _read_first_existing(paths: list[Path], warnings: list[str]) -> pd.DataFrame:
    """Read the first existing optional table from candidate paths."""

    for path in paths:
        if path.exists():
            return _read_optional_table(path, warnings)
    warnings.append(f"No candidate table found among: {', '.join(str(path) for path in paths)}")
    return pd.DataFrame()


def _read_optional_table(path: Path, warnings: list[str]) -> pd.DataFrame:
    """Read CSV or Parquet, returning an empty frame on failure."""

    if not path.exists():
        warnings.append(f"Missing table: {path}")
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as exc:  # noqa: BLE001 - diagnostics must not stop the pipeline
        warnings.append(f"Could not read {path}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


def _filter_year_range(frame: pd.DataFrame, min_year: int, max_year: int) -> pd.DataFrame:
    """Filter rows to the main content-year window when a year column exists."""

    if frame.empty or "year" not in frame.columns:
        return frame
    years = pd.to_numeric(frame["year"], errors="coerce")
    return frame[(years >= int(min_year)) & (years <= int(max_year))].copy()


def _resolve_exposure_field(frame: pd.DataFrame) -> str | None:
    """Return the best available exposure field."""

    for field in ("exposure_index", "chd_annual", "CHD_intensity", "chd_2022_intensity"):
        if field in frame.columns:
            return field
    return None


def _non_missing_count(frame: pd.DataFrame, column: str | None) -> int:
    """Count non-empty, non-null values in a column."""

    if frame.empty or column is None or column not in frame.columns:
        return 0
    values = frame[column]
    return int(values.notna().sum() - values.astype(str).str.strip().eq("").sum())


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    """Return compact value counts for a column."""

    if frame.empty or column not in frame.columns:
        return {}
    values = frame[column].fillna("").astype(str).str.strip()
    values = values[values.ne("")]
    return {str(key): int(value) for key, value in values.value_counts().head(30).items()}


def _nonmissing_by_column(frame: pd.DataFrame, exposure_field: str | None, group_column: str) -> dict[str, int]:
    """Count non-missing exposure rows by year or province."""

    if frame.empty or exposure_field is None or exposure_field not in frame.columns or group_column not in frame.columns:
        return {}
    mask = frame[exposure_field].notna() & frame[exposure_field].astype(str).str.strip().ne("")
    values = frame.loc[mask, group_column].astype(str).str.strip()
    values = values[values.ne("")]
    return {str(key): int(value) for key, value in values.value_counts().sort_index().items()}


def _table_year_range(frame: pd.DataFrame) -> str:
    """Format table year range."""

    if frame.empty or "year" not in frame.columns:
        return "n/a"
    years = pd.to_numeric(frame["year"], errors="coerce").dropna()
    if years.empty:
        return "n/a"
    return f"{int(years.min())}-{int(years.max())}"


def _infer_admin_level_distribution(frame: pd.DataFrame) -> dict[str, int]:
    """Infer admin level distribution from explicit or available fields."""

    if frame.empty:
        return {}
    if "admin_level" in frame.columns:
        return _value_counts(frame, "admin_level")
    levels: dict[str, int] = {}
    for field, level in [("county", "county"), ("prefecture", "prefecture"), ("province", "province")]:
        if field in frame.columns:
            count = _non_missing_count(frame, field)
            if count:
                levels[level] = count
    if "admin_id" in frame.columns and not levels:
        levels["admin_id"] = _non_missing_count(frame, "admin_id")
    return levels


def _infer_causes(
    model: pd.DataFrame,
    study_model: pd.DataFrame,
    climate: pd.DataFrame,
    remote: pd.DataFrame,
    exposure_field: str | None,
    main_event_year: int,
    exposure_rate: float,
    study_provinces: list[str],
) -> list[str]:
    """Infer likely causes for exposure missingness from observed tables."""

    if model.empty:
        return ["unknown"]

    causes: list[str] = []
    exposure_years = _years_with_nonmissing(model, exposure_field)
    if exposure_years == {int(main_event_year)}:
        causes.append("only_2022_event_exposure")
    if _column_coverage(study_model if not study_model.empty else model, "chd_annual") == 0:
        causes.append("no_annual_exposure_panel")
    if exposure_field is not None and exposure_years and _model_years(model) and not exposure_years & _model_years(model):
        causes.append("year_join_mismatch")

    model_provinces = _normalized_provinces(model)
    exposed = _rows_with_nonmissing(model, exposure_field)
    exposure_provinces = _normalized_provinces(exposed)
    study = {_normalize_province_name(value) for value in study_provinces}
    if len(model_provinces) > max(len(exposure_provinces), 0) and exposure_provinces:
        causes.append("national_yield_but_regional_exposure")
    if study and model_provinces and not model_provinces.issubset(study):
        causes.append("study_region_mismatch")
    if exposure_rate == 0 and climate.empty and remote.empty:
        causes.append("climate_data_missing")
    if exposure_rate == 0 and (not climate.empty or not remote.empty):
        causes.append("aggregation_failed")
    if exposure_rate < 0.75 and _has_admin_code_mismatch(model, exposure_field):
        causes.append("admin_join_mismatch")

    ordered = [cause for cause in CAUSES if cause in set(causes)]
    return ordered or ["unknown"]


def _coverage_status(
    model: pd.DataFrame,
    exposure_field: str | None,
    exposure_rate: float,
    main_event_year: int,
) -> str:
    """Classify whether exposure coverage can support panel modeling."""

    exposure_years = _years_with_nonmissing(model, exposure_field)
    if exposure_rate >= 0.75 and len(exposure_years) >= 5:
        return "ok_for_panel_model"
    if exposure_years == {int(main_event_year)} and exposure_rate > 0:
        return "ok_only_for_2022_cross_section"
    return "not_usable_until_fixed"


def _years_with_nonmissing(frame: pd.DataFrame, field: str | None) -> set[int]:
    """Return years with at least one non-missing field value."""

    if frame.empty or field is None or field not in frame.columns or "year" not in frame.columns:
        return set()
    mask = frame[field].notna() & frame[field].astype(str).str.strip().ne("")
    years = pd.to_numeric(frame.loc[mask, "year"], errors="coerce").dropna()
    return {int(year) for year in years}


def _model_years(frame: pd.DataFrame) -> set[int]:
    """Return model years."""

    if frame.empty or "year" not in frame.columns:
        return set()
    years = pd.to_numeric(frame["year"], errors="coerce").dropna()
    return {int(year) for year in years}


def _rows_with_nonmissing(frame: pd.DataFrame, field: str | None) -> pd.DataFrame:
    """Return rows where a field is non-missing."""

    if frame.empty or field is None or field not in frame.columns:
        return frame.iloc[0:0].copy()
    mask = frame[field].notna() & frame[field].astype(str).str.strip().ne("")
    return frame.loc[mask].copy()


def _normalized_provinces(frame: pd.DataFrame) -> set[str]:
    """Return normalized province names from a table."""

    if frame.empty or "province" not in frame.columns:
        return set()
    values = frame["province"].fillna("").astype(str).map(_normalize_province_name)
    return {value for value in values if value}


def _normalize_province_name(value: Any) -> str:
    """Normalize Chinese province names for diagnostics matching."""

    text = str(value or "").strip()
    for suffix in ("省", "市", "自治区", "壮族自治区", "回族自治区", "维吾尔自治区"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _has_admin_code_mismatch(frame: pd.DataFrame, exposure_field: str | None) -> bool:
    """Heuristic for code mismatch: code exists but exposure is only partially populated."""

    if frame.empty or exposure_field is None or "admin_code" not in frame.columns:
        return False
    code_count = _non_missing_count(frame, "admin_code")
    exposure_count = _non_missing_count(frame, exposure_field)
    return code_count > 0 and exposure_count > 0 and exposure_count < code_count * 0.25


def _diagnostic_rows(
    model: pd.DataFrame,
    study_model: pd.DataFrame,
    climate: pd.DataFrame,
    remote: pd.DataFrame,
    exposure_field: str | None,
    exposure_nonmissing: int,
    exposure_rate: float,
    likely_causes: list[str],
    coverage_status: str,
    main_event_year: int,
) -> list[dict[str, Any]]:
    """Build machine-readable diagnostic rows."""

    rows: list[dict[str, Any]] = [
        {"section": "summary", "metric": "model_panel_rows", "value": len(model)},
        {"section": "summary", "metric": "study_region_rows", "value": len(study_model)},
        {"section": "summary", "metric": "exposure_field", "value": exposure_field or ""},
        {"section": "summary", "metric": "exposure_nonmissing", "value": exposure_nonmissing},
        {"section": "summary", "metric": "exposure_nonmissing_rate", "value": round(exposure_rate, 6)},
        {"section": "summary", "metric": "chd_annual_nonmissing_rate", "value": round(_column_coverage(study_model if not study_model.empty else model, "chd_annual"), 6)},
        {"section": "summary", "metric": "chd_2022_intensity_nonmissing_rate", "value": round(_column_coverage(study_model if not study_model.empty else model, "chd_2022_intensity"), 6)},
        {"section": "summary", "metric": "yield_anomaly_pct_nonmissing_rate", "value": round(_column_coverage(study_model if not study_model.empty else model, "yield_anomaly_pct"), 6)},
        {"section": "summary", "metric": "exposure_coverage_status", "value": coverage_status},
        {"section": "summary", "metric": "likely_causes", "value": ";".join(likely_causes)},
        {"section": "model_panel", "metric": "admin_level_distribution", "value": _format_dict(_infer_admin_level_distribution(model))},
        {"section": "model_panel", "metric": "year_distribution", "value": _format_dict(_value_counts(model, "year"))},
        {"section": "model_panel", "metric": "province_distribution", "value": _format_dict(_value_counts(model, "province"))},
        {"section": "exposure", "metric": "nonmissing_by_year", "value": _format_dict(_nonmissing_by_column(model, exposure_field, "year"))},
        {"section": "exposure", "metric": "nonmissing_by_province", "value": _format_dict(_nonmissing_by_column(model, exposure_field, "province"))},
        {"section": "chd_annual", "metric": "nonmissing_by_year", "value": _format_dict(_nonmissing_by_column(study_model if not study_model.empty else model, "chd_annual", "year"))},
        {"section": "chd_annual", "metric": "nonmissing_by_province", "value": _format_dict(_nonmissing_by_column(study_model if not study_model.empty else model, "chd_annual", "province"))},
        {"section": "yield", "metric": "yield_anomaly_pct_nonmissing_by_year", "value": _format_dict(_nonmissing_by_column(model, "yield_anomaly_pct", "year"))},
        {"section": "yield", "metric": "yield_anomaly_pct_nonmissing_by_province", "value": _format_dict(_nonmissing_by_column(model, "yield_anomaly_pct", "province"))},
        {"section": "climate_or_exposure_source", "metric": "rows", "value": len(climate)},
        {"section": "climate_or_exposure_source", "metric": "year_range", "value": _table_year_range(climate)},
        {"section": "remote_sensing_source", "metric": "rows", "value": len(remote)},
        {"section": "remote_sensing_source", "metric": "year_range", "value": _table_year_range(remote)},
        {"section": "join_loss", "metric": "missing_admin_year_count", "value": _missing_admin_year_count(model, exposure_field)},
        {"section": "join_loss", "metric": "main_event_year", "value": main_event_year},
    ]
    return rows


def _missing_admin_year_count(frame: pd.DataFrame, field: str | None) -> int:
    """Return rows missing exposure as an admin-year loss proxy."""

    if frame.empty or field is None or field not in frame.columns:
        return len(frame)
    return int((frame[field].isna() | frame[field].astype(str).str.strip().eq("")).sum())


def _format_dict(values: dict[str, Any]) -> str:
    """Format a dictionary as a compact semicolon-separated string."""

    return "; ".join(f"{key}={value}" for key, value in values.items())


def _column_coverage(frame: pd.DataFrame, column: str) -> float:
    """Return non-empty coverage rate for one column."""

    if frame.empty or column not in frame.columns:
        return 0.0
    return _non_missing_count(frame, column) / len(frame) if len(frame) else 0.0


def _write_report(
    result: ExposureDiagnosisResult,
    panel_path: Path,
    model: pd.DataFrame,
    study_model: pd.DataFrame,
    climate: pd.DataFrame,
    remote: pd.DataFrame,
    exposure_field: str | None,
    rows: list[dict[str, Any]],
) -> None:
    """Write Markdown exposure coverage diagnostics."""

    lookup = {(row["section"], row["metric"]): row["value"] for row in rows}
    lines = [
        "# Exposure Coverage Diagnosis",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Model panel: `{panel_path}`",
        f"- Status: {result.status}",
        f"- Exposure field: `{exposure_field or ''}`",
        f"- study_region rows: {len(study_model)}",
        f"- exposure_index/chd non-missing: {result.exposure_nonmissing}/{result.model_rows}",
        f"- exposure non-missing rate: {result.exposure_rate:.6f}",
        f"- chd_annual non-missing rate: {_column_coverage(study_model if not study_model.empty else model, 'chd_annual'):.6f}",
        f"- chd_2022_intensity non-missing rate: {_column_coverage(study_model if not study_model.empty else model, 'chd_2022_intensity'):.6f}",
        f"- exposure_coverage_status: `{result.exposure_coverage_status}`",
        f"- likely_missing_causes: `{';'.join(result.likely_causes)}`",
        "",
        "## Key Distributions",
        "",
        f"- admin_level distribution: {lookup.get(('model_panel', 'admin_level_distribution'), '')}",
        f"- year distribution: {lookup.get(('model_panel', 'year_distribution'), '')}",
        f"- exposure non-missing by year: {lookup.get(('exposure', 'nonmissing_by_year'), '')}",
        f"- exposure non-missing by province: {lookup.get(('exposure', 'nonmissing_by_province'), '')}",
        f"- chd_annual non-missing by year: {lookup.get(('chd_annual', 'nonmissing_by_year'), '')}",
        f"- chd_annual non-missing by province: {lookup.get(('chd_annual', 'nonmissing_by_province'), '')}",
        f"- yield_anomaly_pct non-missing by year: {lookup.get(('yield', 'yield_anomaly_pct_nonmissing_by_year'), '')}",
        "",
        "## Candidate Source Tables",
        "",
        f"- climate/exposure rows: {len(climate)}; year range: {_table_year_range(climate)}",
        f"- remote sensing rows: {len(remote)}; year range: {_table_year_range(remote)}",
        "",
        "## Machine Decision",
        "",
        f"exposure_coverage_status: `{result.exposure_coverage_status}`",
        "",
        "## Interpretation",
        "",
        _interpretation(result, model),
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _interpretation(result: ExposureDiagnosisResult, model: pd.DataFrame) -> str:
    """Return a concise human interpretation for the diagnosis."""

    if result.exposure_coverage_status == "ok_for_panel_model":
        return "年度暴露覆盖达到面板模型最低要求。"
    if "only_2022_event_exposure" in result.likely_causes:
        return "暴露值主要只出现在 2022 事件年；当前更适合 2022 横截面强度相关分析，不适合年度固定效应模型。"
    if "national_yield_but_regional_exposure" in result.likely_causes:
        return "产量面板看起来覆盖全国省份，而暴露只覆盖部分省份；主模型应先过滤到研究区。"
    if len(model) == 0:
        return "未找到可诊断的 model_panel。"
    return "暴露覆盖不足，需检查年度气象/遥感聚合、行政区 join 键和研究区过滤。"
