"""Province-level compound heat-drought exposure panel construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.annual_exposure import ANNUAL_EXPOSURE_COLUMNS, build_annual_exposure_panel


PROVINCE_CHD_COLUMNS = [
    "province",
    "province_code",
    "year",
    "tmax_anomaly",
    "precip_anomaly",
    "soil_moisture_anomaly",
    "hot_days",
    "dry_days",
    "compound_hot_dry_days",
    "chd_annual",
    "chd_2022_intensity",
    "chd_2022_treated_p75",
    "event_time_2022",
    "post_2022",
    "lst_anomaly",
    "ndvi_anomaly",
    "evi_anomaly",
    "et_anomaly",
    "aggregation_weight",
    "source_rows",
]

AGGREGATE_NUMERIC_COLUMNS = [
    "tmax_anomaly",
    "precip_anomaly",
    "soil_moisture_anomaly",
    "hot_days",
    "dry_days",
    "compound_hot_dry_days",
    "chd_annual",
    "lst_anomaly",
    "ndvi_anomaly",
    "evi_anomaly",
    "et_anomaly",
]

WEIGHT_COLUMNS = (
    "rice_area_ha",
    "paddy_area_ha",
    "crop_area_ha",
    "admin_area_ha",
    "area_ha",
    "area",
)


@dataclass(frozen=True)
class ProvinceCHDResult:
    """Result metadata for province CHD panel construction."""

    status: str
    row_count: int
    chd_nonmissing: int
    chd_coverage_rate: float
    highlighted_region_coverage_rate: float | None = None
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/province_chd_panel_summary.md")


def build_province_chd_panel(
    processed_dir: str | Path,
    interim_dir: str | Path,
    reports_dir: str | Path,
    main_year_min: int = 2000,
    main_year_max: int = 2024,
    main_event_year: int = 2022,
    highlighted_provinces: list[str] | None = None,
) -> ProvinceCHDResult:
    """Build a province-year CHD panel from annual exposure or climate intermediates."""

    processed = Path(processed_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = processed / "province_chd_panel.csv"
    parquet_path = processed / "province_chd_panel.parquet"
    report_path = reports / "province_chd_panel_summary.md"
    warnings: list[str] = []

    source = _load_annual_or_build_from_climate(
        processed=processed,
        interim=interim,
        reports=reports,
        main_year_min=main_year_min,
        main_year_max=main_year_max,
        main_event_year=main_event_year,
        warnings=warnings,
    )
    if source.empty:
        panel = pd.DataFrame(columns=PROVINCE_CHD_COLUMNS)
    else:
        panel = _aggregate_to_province(source, warnings)
        panel = _filter_year_range(panel, main_year_min, main_year_max)
        panel = _add_event_fields(panel, main_event_year)
        panel = _finalize_columns(panel)

    _write_outputs(panel, csv_path, parquet_path, warnings)
    chd_nonmissing = _non_missing_count(panel, "chd_annual")
    coverage_rate = _coverage_rate(panel, "chd_annual", main_year_min, main_year_max)
    highlighted_rate = _highlighted_coverage_rate(panel, highlighted_provinces or [], main_year_min, main_year_max)
    status = "ok" if chd_nonmissing and coverage_rate >= 0.75 else "partial" if chd_nonmissing else "empty"
    result = ProvinceCHDResult(
        status=status,
        row_count=len(panel),
        chd_nonmissing=chd_nonmissing,
        chd_coverage_rate=coverage_rate,
        highlighted_region_coverage_rate=highlighted_rate,
        outputs={"csv": csv_path, "parquet": parquet_path},
        warnings=warnings,
        report_path=report_path,
    )
    _write_report(result, panel, main_year_min, main_year_max)
    return result


def _load_annual_or_build_from_climate(
    processed: Path,
    interim: Path,
    reports: Path,
    main_year_min: int,
    main_year_max: int,
    main_event_year: int,
    warnings: list[str],
) -> pd.DataFrame:
    """Load annual exposure, or derive it from available climate/remote-sensing sources."""

    for path in [processed / "annual_exposure_panel.parquet", processed / "annual_exposure_panel.csv"]:
        frame = _read_table(path, warnings)
        if not frame.empty:
            return frame

    has_climate_source = any(
        path.exists()
        for path in [
            interim / "climate_province_growing_season.parquet",
            interim / "climate_province_growing_season.csv",
            interim / "remote_sensing_province_growing_season.parquet",
            interim / "remote_sensing_province_growing_season.csv",
            processed / "admin_climate_panel.parquet",
            processed / "admin_climate_panel.csv",
            processed / "admin_remote_sensing_panel.parquet",
            processed / "admin_remote_sensing_panel.csv",
            interim / "climate_growing_season.parquet",
            interim / "remote_sensing_growing_season.parquet",
        ]
    )
    if not has_climate_source:
        warnings.append("No annual exposure or climate source table was found; wrote empty province CHD outputs.")
        return pd.DataFrame(columns=ANNUAL_EXPOSURE_COLUMNS)

    annual = build_annual_exposure_panel(
        processed_dir=processed,
        interim_dir=interim,
        reports_dir=reports,
        main_year_min=main_year_min,
        main_year_max=main_year_max,
        main_event_year=main_event_year,
    )
    warnings.extend(f"annual_exposure: {warning}" for warning in annual.warnings)
    return _read_table(processed / "annual_exposure_panel.parquet", warnings)


def _aggregate_to_province(frame: pd.DataFrame, warnings: list[str]) -> pd.DataFrame:
    """Aggregate any admin-level annual exposure rows to province-year rows."""

    if frame.empty:
        return pd.DataFrame(columns=PROVINCE_CHD_COLUMNS)
    if "province" not in frame.columns or "year" not in frame.columns:
        warnings.append("Annual exposure panel lacks province/year keys; wrote empty province CHD outputs.")
        return pd.DataFrame(columns=PROVINCE_CHD_COLUMNS)

    work = frame.copy()
    work["province"] = work["province"].fillna("").astype(str).str.strip()
    work = work[work["province"].ne("")]
    work["year"] = pd.to_numeric(work["year"], errors="coerce").astype("Int64")
    work = work[work["year"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=PROVINCE_CHD_COLUMNS)

    weight_column = _first_weight_column(work)
    if weight_column is None:
        warnings.append("No rice-area weight was available; province CHD uses unweighted administrative means.")
    rows: list[dict[str, Any]] = []
    group_columns = ["province", "year"]
    for (province, year), group in work.groupby(group_columns, dropna=False):
        row: dict[str, Any] = {
            "province": province,
            "province_code": _first_valid(group, ["province_code", "admin_code"]),
            "year": int(year),
            "aggregation_weight": weight_column or "unweighted_mean",
            "source_rows": int(len(group)),
        }
        for column in AGGREGATE_NUMERIC_COLUMNS:
            row[column] = _weighted_mean(group, column, weight_column)
        rows.append(row)
    return pd.DataFrame(rows)


def _first_weight_column(frame: pd.DataFrame) -> str | None:
    """Return the first usable area-weight column."""

    for column in WEIGHT_COLUMNS:
        if column not in frame.columns:
            continue
        weights = pd.to_numeric(frame[column], errors="coerce")
        if weights.gt(0).any():
            return column
    return None


def _weighted_mean(group: pd.DataFrame, column: str, weight_column: str | None) -> float | None:
    """Compute weighted or unweighted mean for a numeric column."""

    if column not in group.columns:
        return None
    values = pd.to_numeric(group[column], errors="coerce")
    valid = values.notna()
    if not valid.any():
        return None
    if weight_column is None or weight_column not in group.columns:
        return float(values[valid].mean())
    weights = pd.to_numeric(group[weight_column], errors="coerce")
    valid = valid & weights.notna() & weights.gt(0)
    if not valid.any():
        return float(values.dropna().mean())
    return float((values[valid] * weights[valid]).sum() / weights[valid].sum())


def _add_event_fields(frame: pd.DataFrame, event_year: int) -> pd.DataFrame:
    """Add 2022 intensity, treatment, event-time, and post fields."""

    if frame.empty:
        return frame
    output = frame.copy()
    years = pd.to_numeric(output["year"], errors="coerce")
    output["event_time_2022"] = years - int(event_year)
    output["post_2022"] = (years >= int(event_year)).astype("Int64")
    chd = pd.to_numeric(output.get("chd_annual", pd.Series(pd.NA, index=output.index)), errors="coerce")
    event_rows = output.loc[years == int(event_year), ["province"]].copy()
    event_rows["_chd"] = chd.loc[event_rows.index]
    event_map = event_rows.dropna(subset=["_chd"]).groupby("province")["_chd"].mean().to_dict()
    output["chd_2022_intensity"] = output["province"].map(event_map)
    threshold = pd.to_numeric(output["chd_2022_intensity"], errors="coerce").dropna().quantile(0.75)
    output["chd_2022_treated_p75"] = (
        (pd.to_numeric(output["chd_2022_intensity"], errors="coerce") >= threshold).astype("Int64")
        if pd.notna(threshold)
        else pd.Series(pd.NA, index=output.index, dtype="Int64")
    )
    return output


def _filter_year_range(frame: pd.DataFrame, min_year: int, max_year: int) -> pd.DataFrame:
    """Filter rows to the main model content-year range."""

    if frame.empty or "year" not in frame.columns:
        return frame
    years = pd.to_numeric(frame["year"], errors="coerce")
    return frame[(years >= int(min_year)) & (years <= int(max_year))].copy()


def _finalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure stable column order."""

    output = frame.copy()
    for column in PROVINCE_CHD_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    return output[PROVINCE_CHD_COLUMNS]


def _read_table(path: Path, warnings: list[str]) -> pd.DataFrame:
    """Read CSV or Parquet if available."""

    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not read {path}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


def _write_outputs(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    """Write CSV and Parquet outputs."""

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not write province CHD parquet: {type(exc).__name__}: {exc}")


def _write_report(result: ProvinceCHDResult, panel: pd.DataFrame, min_year: int, max_year: int) -> None:
    """Write a province CHD panel summary."""

    years = pd.to_numeric(panel["year"], errors="coerce").dropna() if "year" in panel.columns else pd.Series(dtype=float)
    year_range = "n/a" if years.empty else f"{int(years.min())}-{int(years.max())}"
    lines = [
        "# Province CHD Panel Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Rows: {result.row_count}",
        f"- Year range: {year_range}",
        f"- chd_annual non-missing: {result.chd_nonmissing}",
        f"- chd_annual coverage rate against {min_year}-{max_year}: {result.chd_coverage_rate:.6f}",
        f"- Highlighted-region coverage rate: {_format_optional_rate(result.highlighted_region_coverage_rate)}",
        f"- Province count: {_nunique(panel, 'province')}",
        "",
        "## Coverage diagnostics",
        "",
        "- Main model coverage target: >= 0.75.",
        "- Sub-province rows, when present, are aggregated to province-year exposure only.",
        "- No county or prefecture official yield-loss output is created by this step.",
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _coverage_rate(frame: pd.DataFrame, column: str, min_year: int, max_year: int) -> float:
    """Return non-missing column coverage over province-year cells."""

    if frame.empty or column not in frame.columns:
        return 0.0
    province_count = max(1, _nunique(frame, "province"))
    expected = province_count * (int(max_year) - int(min_year) + 1)
    return _non_missing_count(frame, column) / expected if expected else 0.0


def _highlighted_coverage_rate(frame: pd.DataFrame, provinces: list[str], min_year: int, max_year: int) -> float | None:
    """Return coverage rate for highlighted provinces when configured."""

    if not provinces:
        return None
    if frame.empty or "province" not in frame.columns:
        return 0.0
    normalized = {_normalize_province_name(province) for province in provinces}
    subset = frame[frame["province"].map(_normalize_province_name).isin(normalized)].copy()
    if subset.empty:
        return 0.0
    expected = len(normalized) * (int(max_year) - int(min_year) + 1)
    return _non_missing_count(subset, "chd_annual") / expected if expected else 0.0


def _first_valid(frame: pd.DataFrame, columns: list[str]) -> str:
    """Return the first valid string in candidate columns."""

    for column in columns:
        if column not in frame.columns:
            continue
        values = frame[column].dropna().astype(str).str.strip()
        values = values[values.ne("") & values.str.lower().ne("nan")]
        if not values.empty:
            return str(values.iloc[0])
    return ""


def _non_missing_count(frame: pd.DataFrame, column: str) -> int:
    """Count non-empty values."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column]
    return int((values.notna() & values.astype(str).str.strip().ne("")).sum())


def _nunique(frame: pd.DataFrame, column: str) -> int:
    """Count unique non-empty values."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column].dropna().astype(str).str.strip()
    return int(values[values.ne("")].nunique())


def _normalize_province_name(value: Any) -> str:
    """Normalize province names for matching."""

    text = str(value or "").strip()
    for suffix in ("壮族自治区", "回族自治区", "维吾尔自治区", "自治区", "省", "市"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _format_optional_rate(value: float | None) -> str:
    """Format optional coverage rates."""

    return "n/a" if value is None else f"{value:.6f}"
