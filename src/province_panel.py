"""Province-level official outcome panel construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


PROVINCE_MODEL_COLUMNS = [
    "province",
    "province_code",
    "year",
    "outcome_type",
    "crop",
    "source_id",
    "source_name",
    "source_type",
    "is_backfill",
    "yield_kg_per_hectare",
    "province_rice_yield_anomaly",
    "province_grain_yield_anomaly",
    "yield_anomaly_pct",
    "yield_anomaly_abs",
    "trend_yield",
    "baseline_mean",
    "baseline_std",
    "chd_annual",
    "chd_2022_intensity",
    "chd_2022_treated_p75",
    "event_time_2022",
    "post_2022",
    "tmax_anomaly",
    "precip_anomaly",
    "soil_moisture_anomaly",
    "ndvi_anomaly",
    "evi_anomaly",
    "lst_anomaly",
    "irrigation",
    "agricultural_input",
]

RICE_CROPS = {"rice", "paddy", "稻谷", "水稻", "早稻", "中稻", "晚稻", "early_rice", "middle_rice", "late_rice"}
GRAIN_CROPS = {"grain", "粮食", "粮食作物"}
YIELD_COLUMNS = (
    "yield_kg_per_hectare",
    "grain_yield_kg_per_hectare",
    "rice_yield_kg_per_hectare",
    "yield",
    "actual_yield",
)


@dataclass(frozen=True)
class ProvincePanelResult:
    """Result metadata for province model panel construction."""

    status: str
    row_count: int
    outcome_type: str
    allowed_claim_strength: str
    yield_coverage_rate: float = 0.0
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/province_panel_summary.md")


def build_province_model_panel(
    processed_dir: str | Path,
    reports_dir: str | Path,
    main_year_min: int = 2000,
    main_year_max: int = 2024,
    main_event_year: int = 2022,
    baseline_years: tuple[int, int] = (2000, 2021),
    min_valid_observations: int = 10,
) -> ProvincePanelResult:
    """Build the province official-yield model panel."""

    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = processed / "province_model_panel.csv"
    parquet_path = processed / "province_model_panel.parquet"
    report_path = reports / "province_panel_summary.md"
    warnings: list[str] = []

    yield_panel = _load_yield_panel(processed, warnings)
    if yield_panel.empty:
        warnings.append("No province official yield source panel was found; wrote empty province model outputs.")
        panel = pd.DataFrame(columns=PROVINCE_MODEL_COLUMNS)
        outcome_type = "province_grain_yield_anomaly"
    else:
        outcome_source, outcome_type = _select_official_outcome(yield_panel, warnings, main_event_year=main_event_year)
        panel = _prepare_outcome_panel(
            outcome_source,
            outcome_type=outcome_type,
            main_year_min=main_year_min,
            main_year_max=main_year_max,
            baseline_years=baseline_years,
            min_valid_observations=min_valid_observations,
            warnings=warnings,
        )
        panel = _merge_province_chd(panel, processed, warnings)
        panel = _finalize_columns(panel)

    _write_outputs(panel, csv_path, parquet_path, warnings)
    yield_coverage_rate = _target_column_coverage(panel, "yield_anomaly_pct", main_year_min, main_year_max)
    allowed_claim_strength = _allowed_claim_strength(panel, main_year_min, main_year_max)
    result = ProvincePanelResult(
        status="ok" if len(panel) else "empty",
        row_count=len(panel),
        outcome_type=outcome_type,
        allowed_claim_strength=allowed_claim_strength,
        yield_coverage_rate=yield_coverage_rate,
        outputs={"csv": csv_path, "parquet": parquet_path},
        warnings=warnings,
        report_path=report_path,
    )
    _write_report(result, panel, main_year_min, main_year_max)
    return result


def _load_yield_panel(processed: Path, warnings: list[str]) -> pd.DataFrame:
    """Load the first available official yield panel."""

    candidates = [
        processed / "manual_yield_panel_cleaned.parquet",
        processed / "manual_yield_panel_cleaned.csv",
        processed / "yield_panel_combined.parquet",
        processed / "yield_panel_combined.csv",
        processed / "yield_panel.parquet",
        processed / "yield_panel.csv",
        processed / "model_panel.parquet",
        processed / "model_panel.csv",
    ]
    primary = pd.DataFrame()
    for path in candidates:
        frame = _read_table(path, warnings)
        if not frame.empty:
            primary = frame
            break
    backfill = _read_first_existing_table(
        [
            processed / "province_grain_backfill_2008_2015_cleaned.parquet",
            processed / "province_grain_backfill_2008_2015_cleaned.csv",
        ],
        warnings,
    )
    if primary.empty:
        return backfill
    if backfill.empty:
        return primary
    return _append_backfill_rows(primary, backfill)


def _append_backfill_rows(primary: pd.DataFrame, backfill: pd.DataFrame) -> pd.DataFrame:
    """Append official province-grain backfill rows while preserving existing observations."""

    left = primary.copy()
    right = backfill.copy()
    if "yield_kg_per_hectare" not in right.columns and "yield_kg_ha" in right.columns:
        right["yield_kg_per_hectare"] = right["yield_kg_ha"]
    if "province_code" not in right.columns and "admin_code" in right.columns:
        right["province_code"] = right["admin_code"]
    if "is_backfill" not in left.columns:
        left["is_backfill"] = False
    if "is_backfill" not in right.columns:
        right["is_backfill"] = True
    for column in ["source_id", "source_name", "source_type"]:
        if column not in left.columns:
            left[column] = pd.NA
        if column not in right.columns:
            right[column] = pd.NA
    columns = sorted(set(left.columns) | set(right.columns))
    combined = pd.concat([left.reindex(columns=columns), right.reindex(columns=columns)], ignore_index=True)
    for column in ["province", "province_code", "year", "crop"]:
        if column not in combined.columns:
            combined[column] = ""
    combined["_has_yield"] = pd.to_numeric(
        combined.get("yield_kg_per_hectare", pd.Series(pd.NA, index=combined.index)),
        errors="coerce",
    ).notna()
    combined["_is_backfill_sort"] = combined["is_backfill"].astype(str).str.lower().isin({"true", "1", "yes"})
    combined = combined.sort_values(["_has_yield", "_is_backfill_sort"], ascending=[False, True])
    combined = combined.drop_duplicates(subset=["province", "province_code", "year", "crop"], keep="first")
    return combined.drop(columns=["_has_yield", "_is_backfill_sort"])


def _select_official_outcome(frame: pd.DataFrame, warnings: list[str], main_event_year: int = 2022) -> tuple[pd.DataFrame, str]:
    """Prefer province rice yield; fall back to province grain yield."""

    work = frame.copy()
    if "admin_level" in work.columns:
        province_rows = work[work["admin_level"].fillna("").astype(str).str.lower().eq("province")].copy()
        if not province_rows.empty:
            work = province_rows
    if "province" not in work.columns:
        warnings.append("Yield panel lacks province key; no official province outcome can be constructed.")
        return work.iloc[0:0].copy(), "province_grain_yield_anomaly"

    crop_text = work.get("crop", pd.Series("", index=work.index)).fillna("").astype(str)
    crop_norm = crop_text.str.lower().str.strip()
    rice = work[crop_norm.isin({crop.lower() for crop in RICE_CROPS}) | crop_text.isin(RICE_CROPS)].copy()
    grain = work[crop_norm.isin({crop.lower() for crop in GRAIN_CROPS}) | crop_text.isin(GRAIN_CROPS)].copy()
    if not rice.empty and _first_yield_column(rice) is not None:
        rice_years = pd.to_numeric(rice.get("year", pd.Series(dtype=float)), errors="coerce")
        if (rice_years == int(main_event_year)).any():
            return rice, "province_rice_yield_anomaly"
        if not grain.empty and _first_yield_column(grain) is not None:
            warnings.append(
                f"Province rice yield panel lacks {main_event_year} event-year coverage; using province grain yield anomaly."
            )
            return grain, "province_grain_yield_anomaly"
        return rice, "province_rice_yield_anomaly"

    if not grain.empty and _first_yield_column(grain) is not None:
        warnings.append("Province rice yield panel not found; using province grain yield anomaly.")
        return grain, "province_grain_yield_anomaly"

    if _first_yield_column(work) is not None:
        warnings.append("Crop type is unavailable; using province grain yield anomaly as fallback.")
        return work, "province_grain_yield_anomaly"

    warnings.append("No usable official yield column was found; wrote empty province model outputs.")
    return work.iloc[0:0].copy(), "province_grain_yield_anomaly"


def _prepare_outcome_panel(
    frame: pd.DataFrame,
    outcome_type: str,
    main_year_min: int,
    main_year_max: int,
    baseline_years: tuple[int, int],
    min_valid_observations: int,
    warnings: list[str],
) -> pd.DataFrame:
    """Clean province outcome rows and compute yield anomalies."""

    if frame.empty:
        return pd.DataFrame(columns=PROVINCE_MODEL_COLUMNS)
    work = frame.copy()
    work["province"] = work["province"].fillna("").astype(str).str.strip()
    work = work[work["province"].ne("")]
    if "province_code" not in work.columns:
        work["province_code"] = work.get("admin_code", "")
    work["year"] = pd.to_numeric(work["year"], errors="coerce").astype("Int64") if "year" in work.columns else pd.Series(pd.NA, index=work.index, dtype="Int64")
    work = work[work["year"].notna()].copy()
    work = work[(work["year"] >= int(main_year_min)) & (work["year"] <= int(main_year_max))].copy()
    yield_column = _first_yield_column(work)
    if yield_column is None:
        return pd.DataFrame(columns=PROVINCE_MODEL_COLUMNS)
    work["yield_kg_per_hectare"] = pd.to_numeric(work[yield_column], errors="coerce")
    work = work[work["yield_kg_per_hectare"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=PROVINCE_MODEL_COLUMNS)

    anomaly_column = "province_rice_yield_anomaly" if outcome_type == "province_rice_yield_anomaly" else "province_grain_yield_anomaly"
    other_anomaly = "province_grain_yield_anomaly" if anomaly_column == "province_rice_yield_anomaly" else "province_rice_yield_anomaly"
    rows: list[pd.DataFrame] = []
    start, end = int(baseline_years[0]), int(baseline_years[1])
    for _province, group in work.groupby("province", dropna=False):
        group = group.sort_values("year").copy()
        baseline = group[(group["year"] >= start) & (group["year"] <= end)]["yield_kg_per_hectare"].dropna()
        if len(baseline) >= max(2, int(min_valid_observations)):
            years = group.loc[group["yield_kg_per_hectare"].notna(), "year"].astype(float)
            values = group.loc[group["yield_kg_per_hectare"].notna(), "yield_kg_per_hectare"].astype(float)
            intercept, slope = _linear_trend(years.tolist(), values.tolist())
            group["trend_yield"] = group["year"].astype(float) * slope + intercept
            group["baseline_mean"] = float(baseline.mean())
            group["baseline_std"] = float(baseline.std(ddof=1)) if len(baseline) > 1 else pd.NA
        else:
            warnings.append(f"Province {_province} lacks enough baseline years for trend anomaly; using baseline mean fallback.")
            group["trend_yield"] = float(baseline.mean()) if len(baseline) else pd.NA
            group["baseline_mean"] = float(baseline.mean()) if len(baseline) else pd.NA
            group["baseline_std"] = float(baseline.std(ddof=1)) if len(baseline) > 1 else pd.NA
        trend = pd.to_numeric(group["trend_yield"], errors="coerce")
        group["yield_anomaly_abs"] = group["yield_kg_per_hectare"] - trend
        group["yield_anomaly_pct"] = group["yield_anomaly_abs"] / trend.replace(0, pd.NA) * 100.0
        group[anomaly_column] = group["yield_anomaly_pct"]
        group[other_anomaly] = pd.NA
        rows.append(group)
    output = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=PROVINCE_MODEL_COLUMNS)
    output["outcome_type"] = outcome_type
    output["crop"] = "rice" if outcome_type == "province_rice_yield_anomaly" else "grain"
    return output


def _merge_province_chd(panel: pd.DataFrame, processed: Path, warnings: list[str]) -> pd.DataFrame:
    """Merge province CHD fields into the official outcome panel."""

    if panel.empty:
        return panel
    chd = _read_first_existing_table(
        [
            processed / "province_chd_panel.parquet",
            processed / "province_chd_panel.csv",
        ],
        warnings,
    )
    if chd.empty:
        warnings.append("Province CHD panel not found; created empty CHD fields.")
        for column in [
            "chd_annual",
            "chd_2022_intensity",
            "chd_2022_treated_p75",
            "event_time_2022",
            "post_2022",
            "tmax_anomaly",
            "precip_anomaly",
            "soil_moisture_anomaly",
            "ndvi_anomaly",
            "evi_anomaly",
            "lst_anomaly",
        ]:
            panel[column] = pd.NA
        panel["event_time_2022"] = pd.to_numeric(panel["year"], errors="coerce") - 2022
        panel["post_2022"] = (pd.to_numeric(panel["year"], errors="coerce") >= 2022).astype("Int64")
        return panel

    left = panel.copy()
    right = chd.copy()
    left["_province_norm"] = left["province"].map(_normalize_province_name)
    right["_province_norm"] = right["province"].map(_normalize_province_name)
    left["year"] = pd.to_numeric(left["year"], errors="coerce").astype("Int64")
    right["year"] = pd.to_numeric(right["year"], errors="coerce").astype("Int64")
    merge_columns = [
        column
        for column in [
            "_province_norm",
            "year",
            "chd_annual",
            "chd_2022_intensity",
            "chd_2022_treated_p75",
            "event_time_2022",
            "post_2022",
            "tmax_anomaly",
            "precip_anomaly",
            "soil_moisture_anomaly",
            "ndvi_anomaly",
            "evi_anomaly",
            "lst_anomaly",
        ]
        if column in right.columns
    ]
    merged = left.merge(right[merge_columns], on=["_province_norm", "year"], how="left", suffixes=("", "_chd"))
    return merged.drop(columns=["_province_norm"])


def _finalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure stable output columns."""

    output = frame.copy()
    for column in PROVINCE_MODEL_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    return output[PROVINCE_MODEL_COLUMNS]


def _write_outputs(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    """Write CSV and Parquet outputs."""

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not write province model parquet: {type(exc).__name__}: {exc}")


def _write_report(result: ProvincePanelResult, panel: pd.DataFrame, min_year: int, max_year: int) -> None:
    """Write province model panel summary."""

    lines = [
        "# Province Model Panel Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Rows: {result.row_count}",
        f"- Target years: {min_year}-{max_year}",
        f"- Current outcome type: {result.outcome_type}",
        f"- Current allowed maximum claim strength: {result.allowed_claim_strength}",
        f"- Province count: {_nunique(panel, 'province')}",
        f"- yield_anomaly_pct coverage rate against {min_year}-{max_year}: {result.yield_coverage_rate:.6f}",
        f"- chd_annual coverage rate against {min_year}-{max_year}: {_target_column_coverage(panel, 'chd_annual', min_year, max_year):.6f}",
        "",
        "## Scope",
        "",
        "- 主模型只使用省级官方产量面板。",
        "- 县域、地级市和栅格尺度只用于热旱暴露、遥感长势响应和机制分析。",
        "- 本步骤不生成市县级官方产量损失结论。",
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _allowed_claim_strength(panel: pd.DataFrame, min_year: int, max_year: int) -> str:
    """Gate the maximum claim strength from available coverage."""

    if panel.empty:
        return "association"
    chd_coverage = _target_column_coverage(panel, "chd_annual", min_year, max_year)
    yield_coverage = _target_column_coverage(panel, "yield_anomaly_pct", min_year, max_year)
    return "impact_assessment" if chd_coverage >= 0.75 and yield_coverage >= 0.75 else "association"


def _first_yield_column(frame: pd.DataFrame) -> str | None:
    """Return first usable official yield column."""

    for column in YIELD_COLUMNS:
        if column in frame.columns and pd.to_numeric(frame[column], errors="coerce").notna().any():
            return column
    return None


def _read_first_existing_table(paths: list[Path], warnings: list[str]) -> pd.DataFrame:
    """Read first existing non-empty table."""

    for path in paths:
        frame = _read_table(path, warnings)
        if not frame.empty:
            return frame
    return pd.DataFrame()


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


def _linear_trend(years: list[float], values: list[float]) -> tuple[float, float]:
    """Fit a simple trend line."""

    if len(years) < 2:
        return (values[0] if values else 0.0), 0.0
    x_mean = sum(years) / len(years)
    y_mean = sum(values) / len(values)
    denominator = sum((year - x_mean) ** 2 for year in years)
    slope = 0.0 if denominator == 0 else sum((year - x_mean) * (value - y_mean) for year, value in zip(years, values, strict=True)) / denominator
    intercept = y_mean - slope * x_mean
    return intercept, slope


def _coverage_rate(frame: pd.DataFrame, column: str) -> float:
    """Return observed-row non-missing coverage for a column."""

    if frame.empty or column not in frame.columns:
        return 0.0
    values = frame[column]
    return float((values.notna() & values.astype(str).str.strip().ne("")).sum() / len(frame)) if len(frame) else 0.0


def _target_column_coverage(frame: pd.DataFrame, column: str, min_year: int, max_year: int) -> float:
    """Return coverage against expected province-year cells for the target window."""

    if frame.empty or column not in frame.columns or "province" not in frame.columns:
        return 0.0
    provinces = frame["province"].dropna().astype(str).str.strip()
    province_count = int(provinces[provinces.ne("")].nunique())
    expected = province_count * (int(max_year) - int(min_year) + 1)
    if expected <= 0:
        return 0.0
    values = frame[column]
    nonmissing = int((values.notna() & values.astype(str).str.strip().ne("")).sum())
    return float(nonmissing / expected)


def _nunique(frame: pd.DataFrame, column: str) -> int:
    """Count unique non-empty values."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column].dropna().astype(str).str.strip()
    return int(values[values.ne("")].nunique())


def _normalize_province_name(value: Any) -> str:
    """Normalize province names for joining."""

    text = str(value or "").strip()
    for suffix in ("壮族自治区", "回族自治区", "维吾尔自治区", "自治区", "省", "市"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text
