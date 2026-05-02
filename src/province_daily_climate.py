"""Province daily climate QC and annual CHD construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.annual_exposure import ANNUAL_EXPOSURE_COLUMNS


DAILY_CLIMATE_COLUMNS = [
    "province",
    "province_code",
    "date",
    "year",
    "month",
    "tmax_c",
    "precipitation_mm",
]

QC_COLUMNS = [
    "province",
    "province_code",
    "year",
    "date_count",
    "growth_month_date_count",
    "expected_growth_days",
    "missing_growth_months",
    "growth_season_complete",
    "tmax_c_nonmissing_rate",
    "precipitation_mm_nonmissing_rate",
    "issue",
]


@dataclass(frozen=True)
class ProvinceDailyClimateImportResult:
    """Result metadata for daily climate import/QC."""

    status: str
    row_count: int
    province_count: int
    year_min: int | None
    year_max: int | None
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ProvinceDailyCHDResult:
    """Result metadata for daily-climate CHD construction."""

    status: str
    row_count: int
    province_count: int
    chd_nonmissing: int
    chd_coverage_rate: float
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/annual_exposure_panel_summary.md")


def import_province_daily_climate(
    interim_dir: str | Path,
    output_dir: str | Path,
    reports_dir: str | Path,
    year_min: int = 2000,
    year_max: int = 2024,
    growth_months: list[int] | None = None,
) -> ProvinceDailyClimateImportResult:
    """Read the province daily climate table when available and write QC outputs."""

    interim = Path(interim_dir).expanduser().resolve()
    outputs = Path(output_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    outputs.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    qc_csv = outputs / "province_daily_climate_qc.csv"
    qc_report = reports / "province_daily_climate_qc.md"
    warnings: list[str] = []

    source_path = _find_daily_climate_source(interim)
    if source_path is None:
        warnings.append(
            "Province daily climate input not found; expected "
            "province_daily_climate_2000_2024.parquet or .csv in data/interim."
        )
        empty_qc = pd.DataFrame(columns=QC_COLUMNS)
        _write_qc_outputs(empty_qc, qc_csv, qc_report, warnings, None, year_min, year_max)
        return ProvinceDailyClimateImportResult(
            status="missing",
            row_count=0,
            province_count=0,
            year_min=None,
            year_max=None,
            outputs={"qc_csv": qc_csv, "qc_report": qc_report},
            warnings=warnings,
        )

    raw = _read_table(source_path, warnings)
    cleaned, qc, validation_warnings = validate_province_daily_climate(
        raw,
        year_min=year_min,
        year_max=year_max,
        growth_months=growth_months,
    )
    warnings.extend(validation_warnings)
    _write_qc_outputs(qc, qc_csv, qc_report, warnings, source_path, year_min, year_max)
    years = pd.to_numeric(cleaned.get("year", pd.Series(dtype=float)), errors="coerce").dropna()
    status = "ok" if not cleaned.empty and not _has_critical_qc_gap(qc) else "partial" if not cleaned.empty else "empty"
    return ProvinceDailyClimateImportResult(
        status=status,
        row_count=len(cleaned),
        province_count=_nunique(cleaned, "province"),
        year_min=int(years.min()) if not years.empty else None,
        year_max=int(years.max()) if not years.empty else None,
        outputs={"qc_csv": qc_csv, "qc_report": qc_report},
        warnings=warnings,
    )


def validate_province_daily_climate(
    frame: pd.DataFrame,
    year_min: int = 2000,
    year_max: int = 2024,
    growth_months: list[int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Validate and normalize the daily province climate input table."""

    months = sorted(growth_months or [6, 7, 8, 9])
    warnings: list[str] = []
    if frame.empty:
        return pd.DataFrame(columns=DAILY_CLIMATE_COLUMNS), pd.DataFrame(columns=QC_COLUMNS), warnings

    missing = [column for column in DAILY_CLIMATE_COLUMNS if column not in frame.columns]
    if missing:
        warnings.append(f"Province daily climate table missing columns: {', '.join(missing)}")
        return pd.DataFrame(columns=DAILY_CLIMATE_COLUMNS), pd.DataFrame(columns=QC_COLUMNS), warnings

    cleaned = frame[DAILY_CLIMATE_COLUMNS].copy()
    cleaned["province"] = cleaned["province"].fillna("").astype(str).str.strip()
    cleaned["province_code"] = cleaned["province_code"].fillna("").astype(str).str.strip()
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned["year"] = pd.to_numeric(cleaned["year"], errors="coerce").astype("Int64")
    cleaned["month"] = pd.to_numeric(cleaned["month"], errors="coerce").astype("Int64")
    cleaned["tmax_c"] = pd.to_numeric(cleaned["tmax_c"], errors="coerce")
    cleaned["precipitation_mm"] = pd.to_numeric(cleaned["precipitation_mm"], errors="coerce")
    cleaned = cleaned[cleaned["province"].ne("") & cleaned["date"].notna()].copy()
    if cleaned.empty:
        return pd.DataFrame(columns=DAILY_CLIMATE_COLUMNS), pd.DataFrame(columns=QC_COLUMNS), warnings

    parsed_year = cleaned["date"].dt.year.astype("Int64")
    parsed_month = cleaned["date"].dt.month.astype("Int64")
    if not cleaned["year"].equals(parsed_year):
        warnings.append("Year values were normalized from the date column.")
    if not cleaned["month"].equals(parsed_month):
        warnings.append("Month values were normalized from the date column.")
    cleaned["year"] = parsed_year
    cleaned["month"] = parsed_month
    cleaned = cleaned.drop_duplicates(subset=["province", "province_code", "date"]).sort_values(
        ["province", "date"]
    )

    observed_years = set(cleaned["year"].dropna().astype(int))
    expected_years = set(range(int(year_min), int(year_max) + 1))
    missing_years = sorted(expected_years - observed_years)
    if missing_years:
        warnings.append(f"Missing daily climate years in {year_min}-{year_max}: {_format_year_spans(missing_years)}")

    tmax = pd.to_numeric(cleaned["tmax_c"], errors="coerce")
    if tmax.notna().any() and (tmax.max() > 70.0 or tmax.min() < -90.0):
        warnings.append("tmax_c values do not look like Celsius; verify Kelvin-to-Celsius conversion.")
    precipitation = pd.to_numeric(cleaned["precipitation_mm"], errors="coerce")
    if (precipitation < 0).any():
        warnings.append("precipitation_mm contains negative values.")
    positive_precip = precipitation[precipitation > 0]
    if not positive_precip.empty and positive_precip.max() <= 1.0:
        warnings.append("precipitation_mm values are very small; verify they are millimeters, not meters.")

    qc = _build_qc_rows(cleaned, year_min, year_max, months)
    return cleaned.reset_index(drop=True), qc, warnings


def build_chd_from_daily_climate(
    interim_dir: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    year_min: int = 2000,
    year_max: int = 2024,
    baseline_years: tuple[int, int] = (2000, 2021),
    growth_months: list[int] | None = None,
    event_year: int = 2022,
    heat_threshold_quantile: float = 0.90,
    drought_threshold_quantile: float = 0.10,
) -> ProvinceDailyCHDResult:
    """Build annual province CHD exposure from daily tmax and precipitation."""

    interim = Path(interim_dir).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    csv_path = processed / "annual_exposure_panel.csv"
    parquet_path = processed / "annual_exposure_panel.parquet"
    report_path = reports / "annual_exposure_panel_summary.md"
    warnings: list[str] = []
    source_path = _find_daily_climate_source(interim)
    if source_path is None:
        warnings.append(
            "Province daily climate input not found; daily CHD build skipped without overwriting annual exposure outputs."
        )
        missing_report_path = reports / "province_daily_climate_chd_summary.md"
        result = ProvinceDailyCHDResult(
            status="missing",
            row_count=0,
            province_count=0,
            chd_nonmissing=0,
            chd_coverage_rate=0.0,
            outputs={"csv": csv_path, "parquet": parquet_path},
            warnings=warnings,
            report_path=missing_report_path,
        )
        _write_chd_report(result, pd.DataFrame(columns=ANNUAL_EXPOSURE_COLUMNS), source_path, year_min, year_max)
        return result

    raw = _read_table(source_path, warnings)
    daily, _qc, validation_warnings = validate_province_daily_climate(
        raw,
        year_min=year_min,
        year_max=year_max,
        growth_months=growth_months,
    )
    warnings.extend(validation_warnings)
    if daily.empty:
        panel = pd.DataFrame(columns=ANNUAL_EXPOSURE_COLUMNS)
    else:
        panel = _daily_to_annual_chd(
            daily,
            year_min=year_min,
            year_max=year_max,
            baseline_years=baseline_years,
            growth_months=growth_months or [6, 7, 8, 9],
            event_year=event_year,
            heat_threshold_quantile=heat_threshold_quantile,
            drought_threshold_quantile=drought_threshold_quantile,
        )
    _write_annual_outputs(panel, csv_path, parquet_path, warnings)

    chd_nonmissing = _non_missing_count(panel, "chd_annual")
    province_count = _nunique(panel, "province")
    expected = province_count * (int(year_max) - int(year_min) + 1)
    coverage = chd_nonmissing / expected if expected else 0.0
    status = "ok" if chd_nonmissing and coverage >= 0.75 else "partial" if chd_nonmissing else "empty"
    result = ProvinceDailyCHDResult(
        status=status,
        row_count=len(panel),
        province_count=province_count,
        chd_nonmissing=chd_nonmissing,
        chd_coverage_rate=coverage,
        outputs={"csv": csv_path, "parquet": parquet_path},
        warnings=warnings,
        report_path=report_path,
    )
    _write_chd_report(result, panel, source_path, year_min, year_max)
    return result


def _daily_to_annual_chd(
    daily: pd.DataFrame,
    year_min: int,
    year_max: int,
    baseline_years: tuple[int, int],
    growth_months: list[int],
    event_year: int,
    heat_threshold_quantile: float,
    drought_threshold_quantile: float,
) -> pd.DataFrame:
    work = daily.copy().sort_values(["province", "date"]).reset_index(drop=True)
    work = work[(work["year"] >= int(year_min)) & (work["year"] <= int(year_max))].copy()
    if work.empty:
        return pd.DataFrame(columns=ANNUAL_EXPOSURE_COLUMNS)

    work["rolling30_precip_mm"] = (
        work.groupby("province", group_keys=False)["precipitation_mm"]
        .rolling(window=30, min_periods=30)
        .sum()
        .reset_index(level=0, drop=True)
    )
    baseline_mask = (work["year"] >= int(baseline_years[0])) & (work["year"] <= int(baseline_years[1]))
    baseline = work[baseline_mask].copy()
    thresholds = (
        baseline.groupby(["province", "month"], dropna=False)
        .agg(
            tmax_p90=("tmax_c", lambda values: values.quantile(float(heat_threshold_quantile))),
            rolling30_precip_p10=(
                "rolling30_precip_mm",
                lambda values: values.dropna().quantile(float(drought_threshold_quantile))
                if not values.dropna().empty
                else pd.NA,
            ),
        )
        .reset_index()
    )
    work = work.merge(thresholds, on=["province", "month"], how="left")
    work["hot_day"] = pd.to_numeric(work["tmax_c"], errors="coerce") > pd.to_numeric(
        work["tmax_p90"], errors="coerce"
    )
    work["dry_condition"] = pd.to_numeric(work["rolling30_precip_mm"], errors="coerce") < pd.to_numeric(
        work["rolling30_precip_p10"], errors="coerce"
    )
    work["compound_hot_dry_day"] = work["hot_day"].fillna(False) & work["dry_condition"].fillna(False)

    growth = work[work["month"].isin([int(month) for month in growth_months])].copy()
    annual = (
        growth.groupby(["province", "province_code", "year"], dropna=False)
        .agg(
            tmax_mean=("tmax_c", "mean"),
            precip_sum=("precipitation_mm", "sum"),
            hot_days=("hot_day", "sum"),
            dry_days=("dry_condition", "sum"),
            compound_hot_dry_days=("compound_hot_dry_day", "sum"),
        )
        .reset_index()
    )
    baseline_annual = annual[
        (annual["year"] >= int(baseline_years[0])) & (annual["year"] <= int(baseline_years[1]))
    ]
    baseline_stats = (
        baseline_annual.groupby("province", dropna=False)
        .agg(
            baseline_tmax_mean=("tmax_mean", "mean"),
            baseline_precip_sum=("precip_sum", "mean"),
        )
        .reset_index()
    )
    annual = annual.merge(baseline_stats, on="province", how="left")
    annual["tmax_anomaly"] = annual["tmax_mean"] - annual["baseline_tmax_mean"]
    annual["precip_anomaly"] = annual["precip_sum"] - annual["baseline_precip_sum"]
    annual["chd_annual"] = annual["compound_hot_dry_days"]
    annual = _add_event_fields(annual, event_year)
    return _finalize_annual_panel(annual)


def _add_event_fields(frame: pd.DataFrame, event_year: int) -> pd.DataFrame:
    output = frame.copy()
    years = pd.to_numeric(output["year"], errors="coerce")
    output["event_time_2022"] = years - int(event_year)
    output["post_2022"] = (years >= int(event_year)).astype("Int64")
    event_rows = output.loc[years == int(event_year), ["province", "chd_annual"]].dropna()
    event_map = event_rows.groupby("province")["chd_annual"].mean().to_dict()
    output["chd_2022_intensity"] = output["province"].map(event_map)
    threshold = pd.to_numeric(output["chd_2022_intensity"], errors="coerce").dropna().quantile(0.75)
    output["chd_2022_treated_p75"] = (
        (pd.to_numeric(output["chd_2022_intensity"], errors="coerce") >= threshold).astype("Int64")
        if pd.notna(threshold)
        else pd.Series(pd.NA, index=output.index, dtype="Int64")
    )
    return output


def _finalize_annual_panel(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["admin_id"] = output["province_code"].where(
        output["province_code"].fillna("").astype(str).str.strip().ne(""),
        output["province"],
    )
    output["admin_code"] = output["province_code"]
    output["admin_level"] = "province"
    output["exposure_source"] = "province_daily_climate"
    output["soil_moisture_anomaly"] = pd.NA
    output["lst_anomaly"] = pd.NA
    output["ndvi_anomaly"] = pd.NA
    output["evi_anomaly"] = pd.NA
    output["et_anomaly"] = pd.NA
    output["prefecture"] = pd.NA
    output["county"] = pd.NA
    for column in ANNUAL_EXPOSURE_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    return output[ANNUAL_EXPOSURE_COLUMNS]


def _build_qc_rows(cleaned: pd.DataFrame, year_min: int, year_max: int, growth_months: list[int]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    main = cleaned[(cleaned["year"] >= int(year_min)) & (cleaned["year"] <= int(year_max))].copy()
    for (province, province_code, year), group in main.groupby(["province", "province_code", "year"], dropna=False):
        growth = group[group["month"].isin(growth_months)].copy()
        months_present = set(growth["month"].dropna().astype(int))
        missing_months = [month for month in growth_months if month not in months_present]
        expected_growth_days = _expected_growth_days(int(year), growth_months)
        date_count = int(group["date"].nunique())
        growth_count = int(growth["date"].nunique())
        issue_parts = []
        if missing_months:
            issue_parts.append(f"missing growth months {','.join(str(month) for month in missing_months)}")
        if growth_count < expected_growth_days:
            issue_parts.append("incomplete growth-season daily coverage")
        if pd.to_numeric(group["tmax_c"], errors="coerce").isna().any():
            issue_parts.append("missing tmax_c")
        if pd.to_numeric(group["precipitation_mm"], errors="coerce").isna().any():
            issue_parts.append("missing precipitation_mm")
        rows.append(
            {
                "province": province,
                "province_code": province_code,
                "year": int(year),
                "date_count": date_count,
                "growth_month_date_count": growth_count,
                "expected_growth_days": expected_growth_days,
                "missing_growth_months": ",".join(str(month) for month in missing_months),
                "growth_season_complete": bool(not missing_months and growth_count >= expected_growth_days),
                "tmax_c_nonmissing_rate": _nonmissing_rate(group, "tmax_c"),
                "precipitation_mm_nonmissing_rate": _nonmissing_rate(group, "precipitation_mm"),
                "issue": "; ".join(issue_parts),
            }
        )
    return pd.DataFrame(rows, columns=QC_COLUMNS)


def _expected_growth_days(year: int, months: list[int]) -> int:
    return int(
        sum(
            pd.Period(f"{int(year)}-{int(month):02d}").days_in_month
            for month in months
        )
    )


def _find_daily_climate_source(interim: Path) -> Path | None:
    for name in [
        "province_daily_climate_2000_2024.parquet",
        "province_daily_climate_2000_2024.csv",
    ]:
        path = interim / name
        if path.exists():
            return path
    return None


def _read_table(path: Path, warnings: list[str]) -> pd.DataFrame:
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not read {path}: {type(exc).__name__}: {exc}")
        return pd.DataFrame(columns=DAILY_CLIMATE_COLUMNS)


def _write_qc_outputs(
    qc: pd.DataFrame,
    qc_csv: Path,
    qc_report: Path,
    warnings: list[str],
    source_path: Path | None,
    year_min: int,
    year_max: int,
) -> None:
    qc_csv.parent.mkdir(parents=True, exist_ok=True)
    qc.to_csv(qc_csv, index=False, encoding="utf-8-sig")
    complete_rate = _nonmissing_bool_rate(qc, "growth_season_complete")
    lines = [
        "# Province Daily Climate QC",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Input: `{source_path}`" if source_path else "- Input: not found",
        f"- Target years: {year_min}-{year_max}",
        f"- QC rows: {len(qc)}",
        f"- Growth-season complete rate: {complete_rate:.6f}",
        f"- QC CSV: `{qc_csv}`",
        "",
        "## Required Fields",
        "",
        "- province",
        "- province_code",
        "- date",
        "- year",
        "- month",
        "- tmax_c",
        "- precipitation_mm",
        "",
    ]
    if warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")
    qc_report.parent.mkdir(parents=True, exist_ok=True)
    qc_report.write_text("\n".join(lines), encoding="utf-8")


def _write_annual_outputs(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not write annual exposure parquet: {type(exc).__name__}: {exc}")


def _write_chd_report(
    result: ProvinceDailyCHDResult,
    panel: pd.DataFrame,
    source_path: Path | None,
    year_min: int,
    year_max: int,
) -> None:
    years = pd.to_numeric(panel.get("year", pd.Series(dtype=float)), errors="coerce").dropna()
    year_range = "n/a" if years.empty else f"{int(years.min())}-{int(years.max())}"
    lines = [
        "# Annual Exposure Panel Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Source: `{source_path}`" if source_path else "- Source: not found",
        "- Method: province daily climate with monthly tmax P90 and 30-day rolling precipitation P10.",
        f"- Target years: {year_min}-{year_max}",
        f"- Status: {result.status}",
        f"- Rows: {result.row_count}",
        f"- Province count: {result.province_count}",
        f"- Year range: {year_range}",
        f"- chd_annual non-missing: {result.chd_nonmissing}",
        f"- chd_annual coverage rate against {year_min}-{year_max}: {result.chd_coverage_rate:.6f}",
        "",
        "## Definition",
        "",
        "- Baseline thresholds are calculated by province and calendar month.",
        "- hot_day = tmax_c > baseline monthly P90.",
        "- dry_condition = 30-day rolling precipitation < baseline monthly P10.",
        "- chd_annual = June-September compound_hot_dry_day count.",
        "- 2022 event fields are repeated by province for treatment-intensity modeling only.",
        "",
    ]
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _has_critical_qc_gap(qc: pd.DataFrame) -> bool:
    if qc.empty or "growth_season_complete" not in qc.columns:
        return True
    return not bool(qc["growth_season_complete"].all())


def _nonmissing_rate(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").notna().sum() / len(frame))


def _nonmissing_bool_rate(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(frame[column].astype(bool).sum() / len(frame))


def _non_missing_count(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column]
    return int((values.notna() & values.astype(str).str.strip().ne("")).sum())


def _nunique(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column].dropna().astype(str).str.strip()
    return int(values[values.ne("")].nunique())


def _format_year_spans(years: list[int]) -> str:
    if not years:
        return ""
    spans: list[str] = []
    start = previous = int(years[0])
    for year in [int(value) for value in years[1:]]:
        if year == previous + 1:
            previous = year
            continue
        spans.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = year
    spans.append(str(start) if start == previous else f"{start}-{previous}")
    return ", ".join(spans)
