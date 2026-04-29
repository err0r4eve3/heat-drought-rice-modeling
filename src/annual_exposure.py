"""Build annual administrative exposure panels from climate and remote-sensing outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ANNUAL_EXPOSURE_COLUMNS = [
    "admin_id",
    "admin_code",
    "province",
    "prefecture",
    "county",
    "admin_level",
    "year",
    "tmax_anomaly",
    "precip_anomaly",
    "soil_moisture_anomaly",
    "lst_anomaly",
    "ndvi_anomaly",
    "evi_anomaly",
    "et_anomaly",
    "hot_days",
    "dry_days",
    "compound_hot_dry_days",
    "chd_annual",
    "chd_2022_intensity",
    "chd_2022_treated_p75",
    "event_time_2022",
    "post_2022",
    "exposure_source",
]


@dataclass(frozen=True)
class AnnualExposureResult:
    """Result metadata for annual exposure panel construction."""

    status: str
    row_count: int
    chd_nonmissing: int
    chd_coverage_rate: float
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/annual_exposure_panel_summary.md")


def build_annual_exposure_panel(
    processed_dir: str | Path,
    interim_dir: str | Path,
    reports_dir: str | Path,
    main_year_min: int = 2000,
    main_year_max: int = 2024,
    baseline_years: tuple[int, int] = (2000, 2021),
    main_event_year: int = 2022,
    rice_growth_months: list[int] | None = None,
    heat_threshold_quantile: float = 0.90,
    drought_threshold_quantile: float = 0.10,
    study_provinces: list[str] | None = None,
) -> AnnualExposureResult:
    """Build a compact annual CHD exposure panel from available intermediate tables."""

    del rice_growth_months, heat_threshold_quantile, drought_threshold_quantile
    processed = Path(processed_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = processed / "annual_exposure_panel.csv"
    parquet_path = processed / "annual_exposure_panel.parquet"
    report_path = reports / "annual_exposure_panel_summary.md"
    warnings: list[str] = []

    source_frames = _load_candidate_frames(processed, interim, warnings)
    if not source_frames:
        panel = pd.DataFrame(columns=ANNUAL_EXPOSURE_COLUMNS)
        warnings.append("No climate or remote-sensing exposure source tables were found.")
    else:
        panel = _merge_source_frames(source_frames, warnings)
        panel = _filter_year_range(panel, main_year_min, main_year_max)
        panel = _filter_study_provinces(panel, study_provinces or [])
        panel = _add_anomaly_fields(panel)
        panel = _add_chd_annual(panel, baseline_years=baseline_years)
        panel = _add_event_fields(panel, event_year=main_event_year)
        panel = _finalize_columns(panel)

    _write_outputs(panel, csv_path, parquet_path, warnings)
    chd_nonmissing = _non_missing_count(panel, "chd_annual")
    expected_cells = _expected_cells(panel, main_year_min, main_year_max)
    coverage_rate = chd_nonmissing / expected_cells if expected_cells else 0.0
    status = "ok" if chd_nonmissing and coverage_rate >= 0.75 else "partial" if chd_nonmissing else "empty"
    result = AnnualExposureResult(
        status=status,
        row_count=len(panel),
        chd_nonmissing=chd_nonmissing,
        chd_coverage_rate=coverage_rate,
        outputs={"csv": csv_path, "parquet": parquet_path},
        warnings=warnings,
        report_path=report_path,
    )
    _write_report(result, panel, source_frames, main_year_min, main_year_max)
    _write_status_report(
        reports / "chd_panel_status.md",
        result=result,
        panel=panel,
        main_year_min=main_year_min,
        main_year_max=main_year_max,
        baseline_years=baseline_years,
        main_event_year=main_event_year,
    )
    return result


def _load_candidate_frames(processed: Path, interim: Path, warnings: list[str]) -> list[pd.DataFrame]:
    """Load available source tables and convert each to wide annual rows."""

    candidates = [
        (interim / "climate_province_growing_season.parquet", "province_climate"),
        (interim / "climate_province_growing_season.csv", "province_climate"),
        (interim / "remote_sensing_province_growing_season.parquet", "province_remote"),
        (interim / "remote_sensing_province_growing_season.csv", "province_remote"),
        (processed / "admin_climate_panel.parquet", "admin_climate"),
        (processed / "admin_climate_panel.csv", "admin_climate"),
        (processed / "admin_remote_sensing_panel.parquet", "admin_remote"),
        (processed / "admin_remote_sensing_panel.csv", "admin_remote"),
        (interim / "climate_growing_season.parquet", "grid_climate"),
        (interim / "remote_sensing_growing_season.parquet", "grid_remote"),
    ]
    seen_labels: set[str] = set()
    frames: list[pd.DataFrame] = []
    for path, label in candidates:
        if label in seen_labels or not path.exists():
            continue
        frame = _read_table(path, warnings)
        if frame.empty:
            continue
        wide = _to_wide_annual(frame, label, warnings)
        if not wide.empty:
            frames.append(wide)
            seen_labels.add(label)
    return frames


def _read_table(path: Path, warnings: list[str]) -> pd.DataFrame:
    """Read CSV or Parquet safely."""

    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not read {path}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


def _to_wide_annual(frame: pd.DataFrame, source_label: str, warnings: list[str]) -> pd.DataFrame:
    """Convert a long variable/value table to one row per admin-year."""

    if "year" not in frame.columns:
        warnings.append(f"Skipped {source_label}: missing year column.")
        return pd.DataFrame()

    work = frame.copy()
    work["year"] = pd.to_numeric(work["year"], errors="coerce").astype("Int64")
    work = work[work["year"].notna()].copy()
    if work.empty:
        warnings.append(f"Skipped {source_label}: no valid year values.")
        return pd.DataFrame()

    key_columns = [column for column in ["admin_id", "admin_code", "province", "prefecture", "county", "admin_level", "year"] if column in work.columns]
    if "admin_id" not in key_columns and "province" not in key_columns:
        warnings.append(f"Skipped {source_label}: no admin_id or province key.")
        return pd.DataFrame()

    value_column = "value" if "value" in work.columns else "mean" if "mean" in work.columns else None
    if value_column is None:
        warnings.append(f"Skipped {source_label}: no value or mean column.")
        return pd.DataFrame()
    if "variable" not in work.columns:
        warnings.append(f"Skipped {source_label}: missing variable column.")
        return pd.DataFrame()

    work[value_column] = pd.to_numeric(work[value_column], errors="coerce")
    work = work[work[value_column].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    index_columns = [column for column in key_columns if column != "year"] + ["year"]
    pivot = (
        work.pivot_table(
            index=index_columns,
            columns="variable",
            values=value_column,
            aggfunc="mean",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    pivot["exposure_source"] = source_label
    return pivot


def _merge_source_frames(frames: list[pd.DataFrame], warnings: list[str]) -> pd.DataFrame:
    """Outer-merge source frames on administrative keys and year."""

    merged = frames[0].copy()
    for frame in frames[1:]:
        keys = _common_keys(merged, frame)
        if not keys:
            warnings.append("Skipped one exposure source merge because no common admin/year keys were found.")
            continue
        value_columns = [column for column in frame.columns if column not in keys]
        frame = frame.rename(columns={column: _dedupe_column(column, merged.columns) for column in value_columns})
        merged = merged.merge(frame, on=keys, how="outer")
    return merged


def _common_keys(left: pd.DataFrame, right: pd.DataFrame) -> list[str]:
    """Return best common admin/year keys for two annual panels."""

    for candidates in (
        ["admin_id", "year"],
        ["admin_code", "year"],
        ["province", "year"],
        ["prefecture", "year"],
    ):
        if all(column in left.columns and column in right.columns for column in candidates):
            return candidates
    return []


def _dedupe_column(column: str, existing: Any) -> str:
    """Return a non-conflicting column name."""

    if column not in set(existing):
        return column
    return f"{column}_remote"


def _filter_year_range(frame: pd.DataFrame, min_year: int, max_year: int) -> pd.DataFrame:
    """Filter annual exposure rows to content-year range."""

    if frame.empty or "year" not in frame.columns:
        return frame
    years = pd.to_numeric(frame["year"], errors="coerce")
    return frame[(years >= int(min_year)) & (years <= int(max_year))].copy()


def _filter_study_provinces(frame: pd.DataFrame, provinces: list[str]) -> pd.DataFrame:
    """Keep study-region provinces when province names are available."""

    if frame.empty or not provinces or "province" not in frame.columns:
        return frame
    normalized = {_normalize_province_name(province) for province in provinces}
    province_values = frame["province"].map(_normalize_province_name)
    return frame[province_values.isin(normalized)].copy()


def _add_anomaly_fields(frame: pd.DataFrame) -> pd.DataFrame:
    """Add anomaly aliases from available annual variables."""

    if frame.empty:
        return frame
    enriched = frame.copy()
    source_map = {
        "tmax_anomaly": ["tmax", "max_temperature", "maximum_temperature", "lst"],
        "precip_anomaly": ["precipitation", "precip", "rain"],
        "soil_moisture_anomaly": ["soil_moisture", "sm"],
        "lst_anomaly": ["lst", "land_surface_temperature"],
        "ndvi_anomaly": ["ndvi"],
        "evi_anomaly": ["evi"],
        "et_anomaly": ["et", "evapotranspiration"],
    }
    group_columns = _admin_group_columns(enriched)
    for target, tokens in source_map.items():
        source = _first_numeric_column(enriched, tokens, exclude={target})
        if source is None:
            enriched[target] = pd.NA
            continue
        enriched[target] = _zscore_within_admin(enriched, source, group_columns)

    for target, tokens in {
        "hot_days": ["hot_days"],
        "dry_days": ["dry_days"],
        "compound_hot_dry_days": ["compound_hot_dry_days", "chd_days"],
    }.items():
        source = _first_numeric_column(enriched, tokens, exclude={target})
        enriched[target] = pd.to_numeric(enriched[source], errors="coerce") if source else pd.NA
    return enriched


def _add_chd_annual(frame: pd.DataFrame, baseline_years: tuple[int, int] = (2000, 2021)) -> pd.DataFrame:
    """Construct annual compound heat-drought index from available components."""

    if frame.empty:
        return frame
    enriched = frame.copy()
    if {"hot_days", "dry_days"}.issubset(enriched.columns):
        hot = pd.to_numeric(enriched["hot_days"], errors="coerce")
        dry = pd.to_numeric(enriched["dry_days"], errors="coerce")
        if hot.notna().sum() and dry.notna().sum():
            if "compound_hot_dry_days" not in enriched.columns or pd.to_numeric(enriched["compound_hot_dry_days"], errors="coerce").notna().sum() == 0:
                enriched["compound_hot_dry_days"] = ((hot > 0) & (dry > 0)).astype("Int64")
            compound = pd.to_numeric(enriched["compound_hot_dry_days"], errors="coerce")
            if compound.notna().sum():
                enriched["chd_annual"] = compound
                return enriched

    threshold_chd = _threshold_chd_from_tmax_precip(enriched, baseline_years)
    if threshold_chd.notna().sum():
        enriched["chd_annual"] = threshold_chd
        return enriched

    direct_source = _first_numeric_column(enriched, ["exposure_index", "chd_intensity", "chd_annual"], exclude={"chd_annual"})
    if direct_source is not None:
        enriched["chd_annual"] = pd.to_numeric(enriched[direct_source], errors="coerce")
        return enriched

    heat = _row_mean(enriched, ["tmax_anomaly", "lst_anomaly"])
    drought_components = []
    for column in ["precip_anomaly", "soil_moisture_anomaly"]:
        if column in enriched.columns:
            drought_components.append(-pd.to_numeric(enriched[column], errors="coerce"))
    drought = pd.concat(drought_components, axis=1).mean(axis=1, skipna=True) if drought_components else pd.Series(pd.NA, index=enriched.index)
    enriched["chd_annual"] = pd.concat([heat, drought], axis=1).sum(axis=1, min_count=1)
    return enriched


def _threshold_chd_from_tmax_precip(frame: pd.DataFrame, baseline_years: tuple[int, int]) -> pd.Series:
    """Build binary annual CHD when tmax and precipitation annual values exist."""

    tmax_source = _first_numeric_column(frame, ["tmax", "max_temperature", "maximum_temperature"], exclude={"tmax_anomaly"})
    precip_source = _first_numeric_column(frame, ["precipitation", "precip", "rain"], exclude={"precip_anomaly"})
    if tmax_source is None or precip_source is None or "year" not in frame.columns:
        return pd.Series(pd.NA, index=frame.index)
    years = pd.to_numeric(frame["year"], errors="coerce")
    baseline_mask = (years >= int(baseline_years[0])) & (years <= int(baseline_years[1]))
    if baseline_mask.sum() < 3:
        return pd.Series(pd.NA, index=frame.index)
    group_columns = _admin_group_columns(frame)
    tmax = pd.to_numeric(frame[tmax_source], errors="coerce")
    precip = pd.to_numeric(frame[precip_source], errors="coerce")
    hot_threshold = _baseline_quantile(frame, tmax, baseline_mask, group_columns, 0.90)
    dry_threshold = _baseline_quantile(frame, precip, baseline_mask, group_columns, 0.10)
    hot = tmax > hot_threshold
    dry = precip < dry_threshold
    return (hot & dry).astype("Int64")


def _baseline_quantile(
    frame: pd.DataFrame,
    values: pd.Series,
    baseline_mask: pd.Series,
    group_columns: list[str],
    quantile: float,
) -> pd.Series:
    """Return group-specific baseline quantile repeated to all rows."""

    if not group_columns:
        threshold = values[baseline_mask].quantile(quantile)
        return pd.Series(threshold, index=frame.index)
    keys = [frame[column].astype(str) for column in group_columns]
    baseline = frame.loc[baseline_mask, group_columns].copy()
    baseline["_value"] = values.loc[baseline_mask]
    grouped = baseline.groupby(group_columns)["_value"].quantile(quantile).reset_index(name="_threshold")
    temp = frame[group_columns].copy()
    temp["_row_id"] = range(len(temp))
    merged = temp.merge(grouped, on=group_columns, how="left").sort_values("_row_id")
    fallback = values[baseline_mask].quantile(quantile)
    return merged["_threshold"].fillna(fallback).reset_index(drop=True)


def _add_event_fields(frame: pd.DataFrame, event_year: int) -> pd.DataFrame:
    """Add event-year treatment fields without overwriting annual exposure."""

    if frame.empty:
        return frame
    enriched = frame.copy()
    years = pd.to_numeric(enriched["year"], errors="coerce") if "year" in enriched.columns else pd.Series(pd.NA, index=enriched.index)
    enriched["event_time_2022"] = years - int(event_year)
    enriched["post_2022"] = (years >= int(event_year)).astype("Int64")
    key = _event_key(enriched)
    if key is None:
        enriched["chd_2022_intensity"] = pd.NA
        enriched["chd_2022_treated_p75"] = pd.NA
        return enriched
    chd = pd.to_numeric(enriched["chd_annual"], errors="coerce") if "chd_annual" in enriched.columns else pd.Series(pd.NA, index=enriched.index)
    event_rows = enriched.loc[years == int(event_year), [key]].copy()
    event_rows["_chd"] = chd.loc[event_rows.index]
    event_map = event_rows.dropna(subset=["_chd"]).groupby(event_rows[key].astype(str))["_chd"].mean().to_dict()
    enriched["chd_2022_intensity"] = enriched[key].astype(str).map(event_map)
    threshold = pd.to_numeric(enriched["chd_2022_intensity"], errors="coerce").quantile(0.75)
    if pd.isna(threshold):
        enriched["chd_2022_treated_p75"] = pd.NA
    else:
        enriched["chd_2022_treated_p75"] = (
            pd.to_numeric(enriched["chd_2022_intensity"], errors="coerce") >= float(threshold)
        ).astype("Int64")
    return enriched


def _event_key(frame: pd.DataFrame) -> str | None:
    """Return the key used to repeat event-year exposure."""

    for column in ("admin_id", "admin_code", "province", "prefecture", "county"):
        if column in frame.columns and _valid_text_count(frame[column]) > 0:
            return column
    return None


def _valid_text_count(series: pd.Series) -> int:
    """Count valid non-null text values."""

    text = series.fillna("").astype(str).str.strip().str.lower()
    return int((text.ne("") & text.ne("nan") & text.ne("none") & text.ne("<na>")).sum())


def _finalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure stable output columns while preserving source variables."""

    if frame.empty:
        return pd.DataFrame(columns=ANNUAL_EXPOSURE_COLUMNS)
    output = frame.copy()
    for column in ANNUAL_EXPOSURE_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    ordered = ANNUAL_EXPOSURE_COLUMNS + [column for column in output.columns if column not in ANNUAL_EXPOSURE_COLUMNS]
    return output[ordered]


def _admin_group_columns(frame: pd.DataFrame) -> list[str]:
    """Return administrative grouping columns for anomalies."""

    for candidates in (["admin_id"], ["admin_code"], ["province"], ["prefecture"]):
        if all(column in frame.columns for column in candidates):
            return candidates
    return []


def _zscore_within_admin(frame: pd.DataFrame, column: str, groups: list[str]) -> pd.Series:
    """Compute z-score anomalies within admin where possible, falling back to year-cross-section."""

    values = pd.to_numeric(frame[column], errors="coerce")
    if groups:
        mean = values.groupby([frame[group] for group in groups]).transform("mean")
        std = values.groupby([frame[group] for group in groups]).transform("std")
        z = (values - mean) / std.replace(0, pd.NA)
        if z.notna().sum():
            return z
    year_groups = frame["year"] if "year" in frame.columns else pd.Series(0, index=frame.index)
    mean = values.groupby(year_groups).transform("mean")
    std = values.groupby(year_groups).transform("std")
    return (values - mean) / std.replace(0, pd.NA)


def _first_numeric_column(frame: pd.DataFrame, tokens: list[str], exclude: set[str] | None = None) -> str | None:
    """Find the first numeric source column whose name contains any token."""

    excluded = exclude or set()
    for column in frame.columns:
        if column in excluded:
            continue
        lower = str(column).lower()
        if any(token in lower for token in tokens) and pd.to_numeric(frame[column], errors="coerce").notna().any():
            return str(column)
    return None


def _row_mean(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    """Compute row mean over available numeric columns."""

    available = [column for column in columns if column in frame.columns]
    if not available:
        return pd.Series(pd.NA, index=frame.index)
    values = [pd.to_numeric(frame[column], errors="coerce") for column in available]
    return pd.concat(values, axis=1).mean(axis=1, skipna=True)


def _normalize_province_name(value: Any) -> str:
    """Normalize province names for filtering."""

    text = str(value or "").strip()
    for suffix in ("省", "市", "壮族自治区", "回族自治区", "维吾尔自治区", "自治区"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _non_missing_count(frame: pd.DataFrame, column: str) -> int:
    """Count non-empty values in a column."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column]
    return int((values.notna() & values.astype(str).str.strip().ne("")).sum())


def _expected_cells(frame: pd.DataFrame, min_year: int, max_year: int) -> int:
    """Estimate expected admin-year cells for coverage reporting."""

    if frame.empty:
        return 0
    key = "admin_id" if "admin_id" in frame.columns and _non_missing_count(frame, "admin_id") else "province" if "province" in frame.columns else None
    if key is None:
        return len(frame)
    units = frame[key].fillna("").astype(str).str.strip()
    unit_count = int(units[units.ne("")].nunique())
    return unit_count * (int(max_year) - int(min_year) + 1)


def _write_outputs(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    """Write CSV and Parquet outputs."""

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not write annual exposure parquet: {type(exc).__name__}: {exc}")


def _write_report(
    result: AnnualExposureResult,
    panel: pd.DataFrame,
    source_frames: list[pd.DataFrame],
    main_year_min: int,
    main_year_max: int,
) -> None:
    """Write annual exposure construction report."""

    years = pd.to_numeric(panel["year"], errors="coerce").dropna() if "year" in panel.columns else pd.Series(dtype=float)
    year_range = "n/a" if years.empty else f"{int(years.min())}-{int(years.max())}"
    lines = [
        "# Annual Exposure Panel Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Source tables used: {len(source_frames)}",
        f"- Rows: {result.row_count}",
        f"- Year range: {year_range}",
        f"- chd_annual non-missing: {result.chd_nonmissing}",
        f"- chd_annual coverage rate against {main_year_min}-{main_year_max}: {result.chd_coverage_rate:.6f}",
        f"- chd_2022_intensity non-missing: {_non_missing_count(panel, 'chd_2022_intensity')}",
        "",
        "## Model Usability",
        "",
        _usability_text(result),
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _write_status_report(
    path: Path,
    result: AnnualExposureResult,
    panel: pd.DataFrame,
    main_year_min: int,
    main_year_max: int,
    baseline_years: tuple[int, int],
    main_event_year: int,
) -> None:
    """Write a dedicated CHD panel readiness report."""

    years = pd.to_numeric(panel["year"], errors="coerce").dropna() if "year" in panel.columns else pd.Series(dtype=float)
    year_range = "n/a" if years.empty else f"{int(years.min())}-{int(years.max())}"
    lines = [
        "# CHD Panel Status",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Target years: {main_year_min}-{main_year_max}",
        f"- Baseline years: {baseline_years[0]}-{baseline_years[1]}",
        f"- Main event year: {main_event_year}",
        f"- Current year range: {year_range}",
        f"- Rows: {result.row_count}",
        f"- chd_annual non-missing: {result.chd_nonmissing}",
        f"- chd_annual coverage rate: {result.chd_coverage_rate:.6f}",
        f"- chd_2022_intensity non-missing: {_non_missing_count(panel, 'chd_2022_intensity')}",
        "",
        "## Readiness",
        "",
        "- annual exposure panel not ready for fixed effects" if result.chd_coverage_rate < 0.75 else "- annual exposure panel meets the minimum coverage gate for fixed effects",
        "- current model should remain association/descriptive" if result.chd_coverage_rate < 0.75 else "- current model may proceed to fixed-effect gate checks",
        "",
        "## Definition",
        "",
        "- First-pass CHD uses tmax and precipitation when both are available across baseline years.",
        "- If daily hot/dry day counts already exist in intermediate tables, they are used directly.",
        "- 2022 event intensity is repeated by admin unit only for treatment intensity; it is not annual exposure.",
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _usability_text(result: AnnualExposureResult) -> str:
    """Return report text for model usability."""

    if result.chd_coverage_rate >= 0.75:
        return "年度暴露覆盖满足主模型最低要求。"
    if result.chd_nonmissing:
        return "当前暴露数据不足以支持完整年度固定效应面板；可用于事件年或局部横截面分析。"
    return "当前没有可用年度 CHD 暴露，模型必须降级。"
