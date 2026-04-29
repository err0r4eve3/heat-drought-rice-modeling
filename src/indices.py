"""Index construction utilities for yield anomalies and heat-drought exposure."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


MODEL_PANEL_COLUMNS = ["admin_code", "year"]
JOIN_KEY_CANDIDATES = (
    "admin_code",
    "county_code",
    "region_id",
    "province",
    "city",
    "county",
    "year",
)
YIELD_ACTUAL_COLUMNS = (
    "actual_yield",
    "yield",
    "yield_actual",
    "rice_yield",
    "yield_kg_per_hectare",
    "grain_yield_kg_per_hectare",
    "rice_yield_kg_per_hectare",
)
YIELD_TREND_COLUMNS = ("trend_yield", "yield_trend")
BASELINE_MEAN_COLUMNS = ("baseline_mean", "yield_baseline_mean")
BASELINE_STD_COLUMNS = ("baseline_std", "yield_baseline_std")


@dataclass(frozen=True)
class IndexBuildResult:
    """Result metadata for index construction."""

    status: str
    row_count: int
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/index_construction_summary.md")


@dataclass(frozen=True)
class _Table:
    rows: list[dict[str, str]]
    columns: list[str]


def calculate_yield_anomaly(
    actual_yield: Any,
    trend_yield: Any,
    baseline_mean: Any | None = None,
    baseline_std: Any | None = None,
) -> dict[str, float | None]:
    """Calculate absolute, percent, and baseline z-score yield anomalies."""

    actual = _to_float(actual_yield)
    trend = _to_float(trend_yield)
    if actual is None or trend is None:
        return {"abs": None, "pct": None, "zscore": None}

    absolute = actual - trend
    percent = None if trend == 0 else absolute / trend * 100.0

    mean = _to_float(baseline_mean)
    std = _to_float(baseline_std)
    zscore = None
    if mean is not None and std is not None and std > 0:
        zscore = (actual - mean) / std

    return {"abs": absolute, "pct": percent, "zscore": zscore}


def calculate_rolling_cv(values: Iterable[Any], window: int) -> float | None:
    """Calculate the coefficient of variation over the last valid window."""

    if window <= 0:
        return None

    valid_values = [_to_float(value) for value in values]
    valid_values = [value for value in valid_values if value is not None]
    if len(valid_values) < window:
        return None

    sample = valid_values[-window:]
    mean = sum(sample) / len(sample)
    if mean == 0:
        return None

    if len(sample) == 1:
        return 0.0

    variance = sum((value - mean) ** 2 for value in sample) / (len(sample) - 1)
    return math.sqrt(variance) / abs(mean)


def calculate_chd_intensity(
    hot_zscores: Iterable[Any],
    dry_zscores: Iterable[Any],
    hot_flags: Iterable[Any],
    dry_flags: Iterable[Any],
) -> float:
    """Accumulate compound hot-dry intensity for periods flagged hot and dry."""

    total = 0.0
    for hot_z, dry_z, hot_flag, dry_flag in zip(
        hot_zscores,
        dry_zscores,
        hot_flags,
        dry_flags,
        strict=False,
    ):
        if not hot_flag or not dry_flag:
            continue
        hot_value = _to_float(hot_z)
        dry_value = _to_float(dry_z)
        if hot_value is None or dry_value is None:
            continue
        total += hot_value * abs(dry_value)
    return total


def calculate_max_duration(flags: Iterable[Any]) -> int:
    """Return the maximum consecutive truthy flag duration."""

    longest = 0
    current = 0
    for flag in flags:
        if flag:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def calculate_vhi(
    ndvi: Any,
    ndvi_min: Any,
    ndvi_max: Any,
    lst: Any,
    lst_min: Any,
    lst_max: Any,
) -> float | None:
    """Calculate vegetation health index from NDVI and land-surface temperature."""

    ndvi_value = _to_float(ndvi)
    ndvi_min_value = _to_float(ndvi_min)
    ndvi_max_value = _to_float(ndvi_max)
    lst_value = _to_float(lst)
    lst_min_value = _to_float(lst_min)
    lst_max_value = _to_float(lst_max)

    if (
        ndvi_value is None
        or ndvi_min_value is None
        or ndvi_max_value is None
        or lst_value is None
        or lst_min_value is None
        or lst_max_value is None
    ):
        return None

    ndvi_range = ndvi_max_value - ndvi_min_value
    lst_range = lst_max_value - lst_min_value
    if ndvi_range <= 0 or lst_range <= 0:
        return None

    vci = (ndvi_value - ndvi_min_value) / ndvi_range * 100.0
    tci = (lst_max_value - lst_value) / lst_range * 100.0
    return round((vci + tci) / 2.0, 12)


def build_indices(
    yield_panel: str | Path | None = None,
    climate_panel: str | Path | None = None,
    remote_sensing_panel: str | Path | None = None,
    interim_dir: str | Path | None = None,
    processed_dir: str | Path = "data/processed",
    reports_dir: str | Path = "reports",
    output_dir: str | Path | None = None,
    baseline_years: tuple[int, int] | list[int] = (2000, 2021),
    min_valid_observations: int = 10,
) -> IndexBuildResult:
    """Build the MVP model panel from available yield, climate, and remote-sensing panels."""

    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve() if interim_dir is not None else _infer_interim_dir(processed)
    output = Path(output_dir).expanduser().resolve() if output_dir is not None else None

    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    if output is not None:
        output.mkdir(parents=True, exist_ok=True)

    yield_path = _resolve_panel_path(
        yield_panel,
        [
            processed / "manual_yield_panel_cleaned.parquet",
            processed / "manual_yield_panel_cleaned.csv",
            processed / "yield_panel_combined.parquet",
            processed / "yield_panel_combined.csv",
            processed / "yield_panel.csv",
            processed / "yield_panel.parquet",
            processed / "crop_yield_panel.csv",
            processed / "crop_yield_panel.parquet",
        ],
    )
    climate_path = _resolve_panel_path(
        climate_panel,
        [
            interim / "climate_province_growing_season.parquet",
            interim / "climate_province_growing_season.csv",
            interim / "climate_growing_season.parquet",
            interim / "climate_growing_season.csv",
            interim / "climate_extremes_grid.parquet",
            interim / "climate_extremes_grid.csv",
        ],
    )
    remote_path = _resolve_panel_path(
        remote_sensing_panel,
        [
            interim / "remote_sensing_province_growing_season.parquet",
            interim / "remote_sensing_province_growing_season.csv",
            interim / "remote_sensing_growing_season.parquet",
            interim / "remote_sensing_growing_season.csv",
            interim / "remote_sensing_monthly.parquet",
            interim / "remote_sensing_monthly.csv",
            interim / "remote_sensing_panel.csv",
            interim / "remote_sensing_panel.parquet",
            interim / "remote_sensing_indices.csv",
            interim / "remote_sensing_indices.parquet",
        ],
    )

    model_panel_path = processed / "model_panel.csv"
    report_path = reports / "index_construction_summary.md"
    warnings: list[str] = []
    outputs = {"model_panel": model_panel_path}

    if not yield_path.exists():
        warnings.append(f"Missing required input: {yield_path}")
        _write_csv_rows([], MODEL_PANEL_COLUMNS, model_panel_path)
        result = IndexBuildResult(
            status="missing",
            row_count=0,
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
        )
        _write_index_report(
            result=result,
            yield_panel=yield_path,
            climate_panel=climate_path,
            remote_sensing_panel=remote_path,
            output_dir=output,
        )
        return result

    if not climate_path.exists():
        warnings.append(f"Optional input missing: {climate_path}")
    if not remote_path.exists():
        warnings.append(f"Optional input missing: {remote_path}")

    yield_table = _read_table(yield_path, warnings)
    climate_table = (
        _pivot_variable_value_table(_read_table(climate_path, warnings))
        if climate_path.exists()
        else _Table(rows=[], columns=[])
    )
    remote_table = (
        _pivot_variable_value_table(_read_table(remote_path, warnings))
        if remote_path.exists()
        else _Table(rows=[], columns=[])
    )

    model_rows, columns = _join_tables(yield_table, climate_table, "climate", warnings)
    model_rows, columns = _join_tables(_Table(model_rows, columns), remote_table, "remote", warnings)
    model_rows = _add_group_yield_trends(
        model_rows,
        baseline_years=baseline_years,
        min_valid_observations=min_valid_observations,
        warnings=warnings,
    )
    model_rows = [_add_derived_indices(row) for row in model_rows]
    model_rows = _add_exposure_indices(model_rows)
    columns = _collect_columns(model_rows, columns)

    _write_csv_rows(model_rows, columns or MODEL_PANEL_COLUMNS, model_panel_path)
    status = "partial" if model_rows and warnings else "ok" if model_rows else "empty"
    result = IndexBuildResult(
        status=status,
        row_count=len(model_rows),
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_index_report(
        result=result,
        yield_panel=yield_path,
        climate_panel=climate_path,
        remote_sensing_panel=remote_path,
        output_dir=output,
    )
    return result


def _infer_interim_dir(processed_dir: Path) -> Path:
    """Infer data/interim next to data/processed."""

    if processed_dir.name == "processed":
        return processed_dir.parent / "interim"
    return processed_dir / "interim"


def _resolve_panel_path(value: str | Path | None, candidates: list[Path]) -> Path:
    """Resolve an explicit panel path or choose the first existing default candidate."""

    if value is not None:
        return Path(value).expanduser().resolve()
    for candidate in candidates:
        if candidate.exists() and _path_has_data_rows(candidate):
            return candidate.resolve()
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def _path_has_data_rows(path: Path) -> bool:
    """Return True if a candidate CSV/Parquet appears to contain data rows."""

    if path.suffix.lower() == ".csv":
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                next(file_obj, None)
                return next(file_obj, None) is not None
        except OSError:
            return False
    if path.suffix.lower() == ".parquet":
        try:
            import pandas as pd

            return bool(pd.read_parquet(path, columns=[]).shape[0])
        except Exception:
            return path.stat().st_size > 0
    return path.stat().st_size > 0


def _missing_input_warnings(paths: list[Path]) -> list[str]:
    """Return missing input warnings for required MVP panel files."""

    return [f"Missing required input: {path}" for path in paths if not path.exists()]


def _read_table(path: Path, warnings: list[str]) -> _Table:
    """Read a CSV or Parquet table into rows while keeping imports lazy."""

    if path.suffix.lower() == ".csv":
        return _read_csv_table(path)
    if path.suffix.lower() == ".parquet":
        try:
            import pandas as pd

            frame = pd.read_parquet(path)
            rows = [
                {str(column): _stringify_cell(value) for column, value in row.items()}
                for row in frame.to_dict(orient="records")
            ]
            return _Table(rows=rows, columns=[str(column) for column in frame.columns])
        except Exception as exc:  # noqa: BLE001 - fallback keeps pipeline runnable
            warnings.append(f"Could not read {path}: {type(exc).__name__}: {exc}")
            return _Table(rows=[], columns=[])

    warnings.append(f"Unsupported panel format: {path}")
    return _Table(rows=[], columns=[])


def _read_csv_table(path: Path) -> _Table:
    """Read a CSV table with DictReader."""

    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        columns = [str(column) for column in (reader.fieldnames or [])]
        rows = [
            {column: _stringify_cell(row.get(column)) for column in columns}
            for row in reader
        ]
    return _Table(rows=rows, columns=columns)


def _join_tables(
    left: _Table,
    right: _Table,
    prefix: str,
    warnings: list[str],
) -> tuple[list[dict[str, str]], list[str]]:
    """Left-join two tables on common administrative/year keys."""

    if not left.rows:
        return left.rows, left.columns
    if not right.rows:
        warnings.append(f"Skipped {prefix} join because the panel has no rows.")
        return left.rows, left.columns

    keys = [
        key
        for key in JOIN_KEY_CANDIDATES
        if key in left.columns and key in right.columns
    ]
    if not keys:
        warnings.append(f"Skipped {prefix} join because no common join keys were found.")
        return left.rows, left.columns

    index: dict[tuple[str, ...], dict[str, str]] = {}
    duplicate_keys = 0
    for row in right.rows:
        key = tuple(row.get(column, "") for column in keys)
        if key in index:
            duplicate_keys += 1
        index[key] = row

    if duplicate_keys:
        warnings.append(f"{prefix} panel had {duplicate_keys} duplicate join keys; last row was used.")

    rows: list[dict[str, str]] = []
    for row in left.rows:
        merged = dict(row)
        right_row = index.get(tuple(row.get(column, "") for column in keys))
        if right_row is None:
            rows.append(merged)
            continue
        for column, value in right_row.items():
            if column in keys:
                continue
            output_column = f"{prefix}_{column}"
            merged[output_column] = value
        rows.append(merged)

    return rows, _collect_columns(rows, left.columns)


def _pivot_variable_value_table(table: _Table) -> _Table:
    """Pivot long `variable`/`value` panels to one row per join key."""

    if "variable" not in table.columns or "value" not in table.columns:
        return table

    key_columns = [key for key in JOIN_KEY_CANDIDATES if key in table.columns]
    if not key_columns:
        return table

    grouped_values: dict[tuple[str, ...], dict[str, list[float]]] = {}
    for row in table.rows:
        variable = row.get("variable", "").strip()
        value = _to_float(row.get("value"))
        if not variable or value is None:
            continue
        key = tuple(row.get(column, "") for column in key_columns)
        grouped_values.setdefault(key, {}).setdefault(variable, []).append(value)

    if not grouped_values:
        return _Table(rows=[], columns=key_columns)

    variable_columns = sorted(
        {variable for values_by_variable in grouped_values.values() for variable in values_by_variable}
    )
    rows: list[dict[str, str]] = []
    for key, values_by_variable in sorted(grouped_values.items()):
        output_row = {column: value for column, value in zip(key_columns, key, strict=True)}
        for variable in variable_columns:
            values = values_by_variable.get(variable, [])
            if values:
                output_row[variable] = _stringify_cell(sum(values) / len(values))
            else:
                output_row[variable] = ""
        rows.append(output_row)

    return _Table(rows=rows, columns=[*key_columns, *variable_columns])


def _add_derived_indices(row: dict[str, str]) -> dict[str, str]:
    """Add row-level derived indices when required source columns are present."""

    enriched = dict(row)
    actual_column = _first_present(enriched, YIELD_ACTUAL_COLUMNS)
    trend_column = _first_present(enriched, YIELD_TREND_COLUMNS)
    if actual_column is not None and trend_column is not None:
        anomaly = calculate_yield_anomaly(
            actual_yield=enriched.get(actual_column),
            trend_yield=enriched.get(trend_column),
            baseline_mean=enriched.get(_first_present(enriched, BASELINE_MEAN_COLUMNS) or ""),
            baseline_std=enriched.get(_first_present(enriched, BASELINE_STD_COLUMNS) or ""),
        )
        enriched["yield_anomaly_abs"] = _stringify_cell(anomaly["abs"])
        enriched["yield_anomaly_pct"] = _stringify_cell(anomaly["pct"])
        enriched["yield_anomaly_zscore"] = _stringify_cell(anomaly["zscore"])

    if all(column in enriched for column in ("ndvi", "ndvi_min", "ndvi_max", "lst", "lst_min", "lst_max")):
        enriched["vhi"] = _stringify_cell(
            calculate_vhi(
                enriched["ndvi"],
                enriched["ndvi_min"],
                enriched["ndvi_max"],
                enriched["lst"],
                enriched["lst_min"],
                enriched["lst_max"],
            )
        )

    if all(column in enriched for column in ("hot_zscore", "dry_zscore", "hot_flag", "dry_flag")):
        enriched["chd_intensity"] = _stringify_cell(
            calculate_chd_intensity(
                [enriched["hot_zscore"]],
                [enriched["dry_zscore"]],
                [_to_bool(enriched["hot_flag"])],
                [_to_bool(enriched["dry_flag"])],
            )
        )

    return enriched


def _add_group_yield_trends(
    rows: list[dict[str, str]],
    baseline_years: tuple[int, int] | list[int],
    min_valid_observations: int,
    warnings: list[str],
) -> list[dict[str, str]]:
    """Add trend and stability columns by admin/province crop group."""

    if not rows:
        return rows
    start_year, end_year = _baseline_bounds(baseline_years)
    grouped: dict[tuple[str, ...], list[dict[str, str]]] = {}
    for row in rows:
        actual_column = _first_present(row, YIELD_ACTUAL_COLUMNS)
        if actual_column is None:
            continue
        key = _yield_group_key(row)
        grouped.setdefault(key, []).append(row)

    output_rows = [dict(row) for row in rows]
    row_lookup = {id(source): target for source, target in zip(rows, output_rows, strict=True)}
    trendable_groups = 0
    for group_rows in grouped.values():
        actual_column = _first_present(group_rows[0], YIELD_ACTUAL_COLUMNS)
        if actual_column is None:
            continue
        baseline_points = [
            (_to_float(row.get("year")), _to_float(row.get(actual_column)))
            for row in group_rows
            if _to_float(row.get("year")) is not None
            and start_year <= int(_to_float(row.get("year")) or 0) <= end_year
            and _to_float(row.get(actual_column)) is not None
        ]
        if len(baseline_points) < max(2, int(min_valid_observations)):
            continue
        years = [float(year) for year, _value in baseline_points if year is not None]
        values = [float(value) for _year, value in baseline_points if value is not None]
        intercept, slope = _linear_trend(years, values)
        baseline_mean = sum(values) / len(values)
        baseline_std = _sample_std(values)
        trendable_groups += 1
        sorted_group = sorted(group_rows, key=lambda item: _to_float(item.get("year")) or -10**9)
        history: list[float] = []
        for row in sorted_group:
            target = row_lookup[id(row)]
            year = _to_float(row.get("year"))
            actual = _to_float(row.get(actual_column))
            if year is not None:
                target["trend_yield"] = _stringify_cell(intercept + slope * year)
            target["baseline_mean"] = _stringify_cell(baseline_mean)
            target["baseline_std"] = _stringify_cell(baseline_std)
            if actual is not None:
                history.append(actual)
            target["yield_cv_5yr"] = _stringify_cell(calculate_rolling_cv(history, 5))
            target["yield_cv_10yr"] = _stringify_cell(calculate_rolling_cv(history, 10))
            trend = _to_float(target.get("trend_yield"))
            if actual is not None and trend is not None:
                target["downside_risk"] = _stringify_cell(max(0.0, trend - actual))
                if int(year or -1) == 2022:
                    target["shock_loss_2022"] = _stringify_cell(max(0.0, trend - actual))
                if int(year or -1) in {2023, 2024} and trend != 0:
                    target[f"recovery_{int(year)}"] = _stringify_cell(actual / trend)

    if grouped and trendable_groups == 0:
        warnings.append(
            "No yield groups had enough baseline observations to estimate trend yield."
        )
    return output_rows


def _add_exposure_indices(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Add heat, drought, and compound exposure fields from available variables."""

    if not rows:
        return rows
    columns = _collect_columns(rows, [])
    heat_columns = [
        column
        for column in columns
        if _is_heat_column(column) and _column_has_numeric(rows, column)
    ]
    dry_columns = [
        column
        for column in columns
        if _is_dry_column(column) and _column_has_numeric(rows, column)
    ]
    if not heat_columns and not dry_columns:
        return rows

    z_by_column = {column: _z_scores([_to_float(row.get(column)) for row in rows]) for column in [*heat_columns, *dry_columns]}
    enriched_rows: list[dict[str, str]] = []
    for index, row in enumerate(rows):
        enriched = dict(row)
        heat_values = [z_by_column[column][index] for column in heat_columns if z_by_column[column][index] is not None]
        dry_values = [
            -z_by_column[column][index]
            for column in dry_columns
            if z_by_column[column][index] is not None
        ]
        heat_component = _mean_or_none(heat_values)
        drought_component = _mean_or_none(dry_values)
        exposure_values = [value for value in [heat_component, drought_component] if value is not None]
        if heat_component is not None:
            enriched["heat_component"] = _stringify_cell(heat_component)
        if drought_component is not None:
            enriched["drought_component"] = _stringify_cell(drought_component)
        if exposure_values:
            enriched["exposure_index"] = _stringify_cell(sum(exposure_values))
            enriched["CHD_intensity"] = _stringify_cell(sum(exposure_values))
        enriched_rows.append(enriched)
    return enriched_rows


def _first_present(row: dict[str, str], candidates: Iterable[str]) -> str | None:
    """Return the first candidate column present in a row."""

    for candidate in candidates:
        if candidate in row:
            return candidate
    return None


def _yield_group_key(row: dict[str, str]) -> tuple[str, ...]:
    """Build a stable group key for yield-trend estimation."""

    crop = row.get("crop", "")
    admin_code = row.get("admin_code", "")
    if admin_code and admin_code.strip():
        return ("admin_code", admin_code.strip(), crop)
    admin_id = row.get("admin_id", "")
    if admin_id and admin_id.strip():
        return ("admin_id", admin_id.strip(), crop)
    return (
        "names",
        row.get("province", "").strip(),
        row.get("prefecture", "").strip(),
        row.get("county", "").strip(),
        crop,
    )


def _baseline_bounds(baseline_years: tuple[int, int] | list[int]) -> tuple[int, int]:
    """Normalize baseline year config to inclusive bounds."""

    if len(baseline_years) != 2:
        raise ValueError("baseline_years must contain exactly two values")
    start, end = int(baseline_years[0]), int(baseline_years[1])
    return (min(start, end), max(start, end))


def _linear_trend(years: list[float], values: list[float]) -> tuple[float, float]:
    """Fit a simple linear trend y = intercept + slope * year."""

    if len(set(years)) <= 1:
        return (sum(values) / len(values), 0.0)
    mean_x = sum(years) / len(years)
    mean_y = sum(values) / len(values)
    denominator = sum((year - mean_x) ** 2 for year in years)
    if denominator == 0:
        return (mean_y, 0.0)
    slope = sum((year - mean_x) * (value - mean_y) for year, value in zip(years, values, strict=True)) / denominator
    intercept = mean_y - slope * mean_x
    return intercept, slope


def _sample_std(values: list[float]) -> float | None:
    """Return sample standard deviation for baseline values."""

    if len(values) < 2:
        return None
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _is_heat_column(column: str) -> bool:
    """Return True for heat-related columns."""

    text = column.lower()
    return any(token in text for token in ["temperature", "tmax", "lst", "hot"])


def _is_dry_column(column: str) -> bool:
    """Return True for moisture deficit columns."""

    text = column.lower()
    return any(token in text for token in ["precipitation", "soil_moisture", "spei", "drought"])


def _column_has_numeric(rows: list[dict[str, str]], column: str) -> bool:
    """Return True when a column has at least one numeric value."""

    return any(_to_float(row.get(column)) is not None for row in rows)


def _z_scores(values: list[float | None]) -> list[float | None]:
    """Compute z-scores, preserving missing values."""

    valid_values = [value for value in values if value is not None]
    if len(valid_values) < 2:
        return [0.0 if value is not None else None for value in values]
    mean_value = sum(valid_values) / len(valid_values)
    std_value = _sample_std(valid_values)
    if std_value is None or std_value <= 0:
        return [0.0 if value is not None else None for value in values]
    return [None if value is None else (value - mean_value) / std_value for value in values]


def _mean_or_none(values: list[float]) -> float | None:
    """Return the arithmetic mean for a non-empty list."""

    if not values:
        return None
    return sum(values) / len(values)


def _collect_columns(rows: list[dict[str, str]], seed_columns: Iterable[str]) -> list[str]:
    """Collect columns in stable insertion order."""

    columns: list[str] = []
    seen: set[str] = set()
    for column in seed_columns:
        if column not in seen:
            columns.append(column)
            seen.add(column)
    for row in rows:
        for column in row:
            if column not in seen:
                columns.append(column)
                seen.add(column)
    return columns


def _write_csv_rows(rows: list[dict[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write rows as CSV and create parent directories first."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_index_report(
    result: IndexBuildResult,
    yield_panel: Path,
    climate_panel: Path,
    remote_sensing_panel: Path,
    output_dir: Path | None,
) -> None:
    """Write a Markdown index construction summary."""

    lines = [
        "# Index Construction Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Rows: {result.row_count}",
        f"- Yield panel: `{yield_panel}`",
        f"- Climate panel: `{climate_panel}`",
        f"- Remote sensing panel: `{remote_sensing_panel}`",
    ]
    if output_dir is not None:
        lines.append(f"- Output dir: `{output_dir}`")
    lines.extend(["", "## Outputs", ""])
    lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
    lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _to_float(value: Any) -> float | None:
    """Convert a value to float, preserving missing values as None."""

    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _to_bool(value: Any) -> bool:
    """Parse common truthy strings for row-level flags."""

    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def _stringify_cell(value: Any) -> str:
    """Convert cell values to CSV-friendly strings."""

    if value is None:
        return ""
    try:
        if math.isnan(float(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value)
