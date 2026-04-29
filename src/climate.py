"""Climate preprocessing utilities."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable


SUPPORTED_CLIMATE_SUFFIXES = (".nc", ".nc4", ".cdf")
CLIMATE_SUFFIX_PRIORITY = {suffix: index for index, suffix in enumerate(SUPPORTED_CLIMATE_SUFFIXES)}

COORDINATE_CANDIDATES: dict[str, list[str]] = {
    "time": ["time", "valid_time"],
    "lat": ["lat", "latitude", "y"],
    "lon": ["lon", "longitude", "x"],
}

CLIMATE_VARIABLE_CANDIDATES: dict[str, list[str]] = {
    "temperature": ["t2m", "2m_temperature", "temperature", "temp", "tas"],
    "tmax": ["tmax", "maximum_temperature", "max_temperature", "mx2t", "tasmax"],
    "precipitation": ["prec", "precip", "precipitation", "tp", "total_precipitation", "pr"],
    "soil_moisture": [
        "soil_moisture",
        "soilmoisture",
        "sm",
        "swvl1",
        "volumetric_soil_water_layer_1",
    ],
    "evapotranspiration": ["et", "evapotranspiration", "aet", "actual_evapotranspiration"],
    "potential_evapotranspiration": ["pet", "pev", "potential_evapotranspiration"],
    "drought_index": ["spei", "pdsi", "scpdsi", "drought_index"],
}

CLIMATE_ROW_COLUMNS = ["source_file", "aggregation", "variable", "time", "year", "month", "value"]
EXTREME_ROW_COLUMNS = ["source_file", "aggregation", "variable", "year", "value"]


@dataclass(frozen=True)
class ClimatePreprocessResult:
    """Result metadata for climate preprocessing."""

    status: str
    file_count: int
    processed_files: list[Path]
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/climate_preprocess_summary.md")


def find_climate_files(
    climate_dir: str | Path,
    external_files: Iterable[str | Path] | None = None,
) -> list[Path]:
    """Find supported NetCDF climate files."""

    root = Path(climate_dir).expanduser().resolve()
    files: list[Path] = []
    if root.exists():
        files.extend(
            path.resolve()
            for path in root.rglob("*")
            if path.is_file() and path.suffix.lower() in SUPPORTED_CLIMATE_SUFFIXES
        )
    if not files:
        files.extend(_supported_external_files(external_files, SUPPORTED_CLIMATE_SUFFIXES))
    return sorted(_unique_paths(files), key=lambda path: (CLIMATE_SUFFIX_PRIORITY[path.suffix.lower()], str(path)))


def identify_coordinate_names(names: list[str] | Any) -> dict[str, str]:
    """Identify common time, latitude, and longitude coordinate names."""

    name_list = [str(name) for name in names]
    normalized = {_normalize_name(name): name for name in name_list}
    mapping: dict[str, str] = {}

    for role, candidates in COORDINATE_CANDIDATES.items():
        for candidate in candidates:
            matched = normalized.get(_normalize_name(candidate))
            if matched is not None:
                mapping[role] = matched
                break
    return mapping


def identify_climate_variables(variable_names: list[str] | Any) -> dict[str, str]:
    """Identify available climate variables from dataset variable names."""

    names = [str(name) for name in variable_names]
    normalized = {_normalize_name(name): name for name in names}
    mapping: dict[str, str] = {}

    for role, candidates in CLIMATE_VARIABLE_CANDIDATES.items():
        for candidate in candidates:
            matched = normalized.get(_normalize_name(candidate))
            if matched is not None:
                mapping[role] = matched
                break
    return mapping


def build_analysis_years(
    baseline_years: tuple[int, int],
    main_event_year: int,
    recovery_years: list[int],
    validation_event_year: int,
) -> list[int]:
    """Build sorted unique years needed for baseline, event, recovery, and validation."""

    start_year, end_year = baseline_years
    years = set(range(int(start_year), int(end_year) + 1))
    years.add(int(main_event_year))
    years.add(int(validation_event_year))
    years.update(int(year) for year in recovery_years)
    return sorted(years)


def convert_temperature_values(values: Any, units: str | None) -> Any:
    """Convert temperature values to Celsius when source units are Kelvin."""

    normalized_units = _normalize_units(units)
    if normalized_units in {"k", "kelvin"}:
        return _map_values(values, lambda value: value - 273.15)
    return values


def convert_precipitation_values(
    values: Any,
    units: str | None,
    time_step_seconds: float | None = None,
) -> Any:
    """Convert precipitation values to millimeters."""

    normalized_units = _normalize_units(units)
    if _is_precip_rate_unit(normalized_units):
        if time_step_seconds is None:
            raise ValueError("time_step_seconds is required for precipitation rate units")
        return _map_values(values, lambda value: value * time_step_seconds)
    if normalized_units in {"m", "meter", "metre", "meters", "metres"}:
        return _map_values(values, lambda value: value * 1000.0)
    return values


def preprocess_climate(
    climate_dir: str | Path,
    interim_dir: str | Path,
    reports_dir: str | Path,
    study_bbox: list[float],
    baseline_years: tuple[int, int],
    main_event_year: int,
    recovery_years: list[int],
    validation_event_year: int,
    rice_growth_months: list[int],
    heat_threshold_quantile: float,
    drought_threshold_quantile: float,
    external_files: Iterable[str | Path] | None = None,
) -> ClimatePreprocessResult:
    """Preprocess NetCDF climate files into monthly, growing-season, and extremes tables."""

    climate_root = Path(climate_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    interim.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    report_path = reports / "climate_preprocess_summary.md"

    files = find_climate_files(climate_root, external_files=external_files)
    warnings: list[str] = []
    outputs: dict[str, Path] = {}

    if not files:
        warnings.append(f"No climate NetCDF files found under {climate_root}.")
        outputs.update(_write_empty_climate_outputs(interim, warnings))
        result = ClimatePreprocessResult(
            status="missing",
            file_count=0,
            processed_files=[],
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
        )
        _write_climate_report(result, study_bbox, baseline_years, rice_growth_months)
        return result

    monthly_rows: list[dict[str, Any]] = []
    growing_rows: list[dict[str, Any]] = []
    extreme_rows: list[dict[str, Any]] = []
    processed_files: list[Path] = []
    analysis_years = build_analysis_years(
        baseline_years=baseline_years,
        main_event_year=main_event_year,
        recovery_years=recovery_years,
        validation_event_year=validation_event_year,
    )

    for path in files:
        try:
            file_rows = _process_climate_file(
                path=path,
                study_bbox=study_bbox,
                analysis_years=analysis_years,
                baseline_years=baseline_years,
                rice_growth_months=rice_growth_months,
                heat_threshold_quantile=heat_threshold_quantile,
                drought_threshold_quantile=drought_threshold_quantile,
            )
            monthly_rows.extend(file_rows["monthly"])
            growing_rows.extend(file_rows["growing"])
            extreme_rows.extend(file_rows["extremes"])
            processed_files.append(path)
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the batch
            warnings.append(f"Skipped {path}: {type(exc).__name__}: {exc}")

    outputs["monthly"] = _write_table(
        monthly_rows,
        interim / "climate_monthly.parquet",
        CLIMATE_ROW_COLUMNS,
        warnings,
    )
    outputs["growing_season"] = _write_table(
        growing_rows,
        interim / "climate_growing_season.parquet",
        CLIMATE_ROW_COLUMNS,
        warnings,
    )
    outputs["extremes"] = _write_table(
        extreme_rows,
        interim / "climate_extremes_grid.parquet",
        EXTREME_ROW_COLUMNS,
        warnings,
    )

    if processed_files:
        status = "ok" if len(processed_files) == len(files) else "partial"
    else:
        status = "error"

    result = ClimatePreprocessResult(
        status=status,
        file_count=len(files),
        processed_files=processed_files,
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_climate_report(result, study_bbox, baseline_years, rice_growth_months)
    return result


def _process_climate_file(
    path: Path,
    study_bbox: list[float],
    analysis_years: list[int],
    baseline_years: tuple[int, int],
    rice_growth_months: list[int],
    heat_threshold_quantile: float,
    drought_threshold_quantile: float,
) -> dict[str, list[dict[str, Any]]]:
    """Read and aggregate one NetCDF climate file."""

    xr = _import_xarray()
    with xr.open_dataset(path, chunks="auto", decode_times=True) as dataset:
        coord_names = identify_coordinate_names(list(dataset.coords) + list(dataset.dims))
        if not {"time", "lat", "lon"}.issubset(coord_names):
            raise ValueError(f"Could not identify time/lat/lon coordinates in {path.name}")

        dataset = _standardize_coordinates(dataset, coord_names)
        dataset = _clip_to_bbox(dataset, study_bbox)
        dataset = _filter_years(dataset, analysis_years)

        variable_map = identify_climate_variables(list(dataset.data_vars))
        if not variable_map:
            raise ValueError(f"Could not identify climate variables in {path.name}")

        dataset = _standardize_variable_units(dataset, variable_map)
        monthly_rows = _build_monthly_rows(dataset, variable_map, path.name)
        growing_rows = _build_growing_season_rows(dataset, variable_map, path.name, rice_growth_months)
        extreme_rows = _build_extreme_rows(
            dataset=dataset,
            variable_map=variable_map,
            source_file=path.name,
            baseline_years=baseline_years,
            rice_growth_months=rice_growth_months,
            heat_threshold_quantile=heat_threshold_quantile,
            drought_threshold_quantile=drought_threshold_quantile,
        )
        return {"monthly": monthly_rows, "growing": growing_rows, "extremes": extreme_rows}


def _standardize_coordinates(dataset: Any, coord_names: dict[str, str]) -> Any:
    """Rename coordinates to time, lat, and lon."""

    rename_map = {
        source: target
        for target, source in coord_names.items()
        if source != target and source in dataset
    }
    return dataset.rename(rename_map)


def _clip_to_bbox(dataset: Any, study_bbox: list[float]) -> Any:
    """Clip an xarray dataset to a lon/lat bounding box."""

    west, south, east, north = [float(value) for value in study_bbox]
    lon_values = dataset["lon"]
    lat_values = dataset["lat"]
    lon_ascending = float(lon_values.values[0]) <= float(lon_values.values[-1])
    lat_ascending = float(lat_values.values[0]) <= float(lat_values.values[-1])
    lon_slice = slice(west, east) if lon_ascending else slice(east, west)
    lat_slice = slice(south, north) if lat_ascending else slice(north, south)
    return dataset.sel(lon=lon_slice, lat=lat_slice)


def _filter_years(dataset: Any, analysis_years: list[int]) -> Any:
    """Filter dataset time range to the analysis years."""

    if not analysis_years:
        return dataset
    start = min(analysis_years)
    end = max(analysis_years)
    return dataset.sel(time=slice(f"{start}-01-01", f"{end}-12-31"))


def _standardize_variable_units(dataset: Any, variable_map: dict[str, str]) -> Any:
    """Convert common temperature and precipitation units in an xarray dataset."""

    for role in ("temperature", "tmax"):
        variable = variable_map.get(role)
        if variable and variable in dataset:
            units = dataset[variable].attrs.get("units")
            dataset[variable] = convert_temperature_values(dataset[variable], units)
            dataset[variable].attrs["units"] = "degC"

    precipitation = variable_map.get("precipitation")
    if precipitation and precipitation in dataset:
        units = dataset[precipitation].attrs.get("units")
        time_step = _infer_time_step_seconds(dataset)
        dataset[precipitation] = convert_precipitation_values(dataset[precipitation], units, time_step)
        dataset[precipitation].attrs["units"] = "mm"

    return dataset


def _build_monthly_rows(dataset: Any, variable_map: dict[str, str], source_file: str) -> list[dict[str, Any]]:
    """Build long-format monthly climate rows."""

    rows: list[dict[str, Any]] = []
    aggregation_plan = [
        ("temperature", "monthly_mean_temperature", "mean"),
        ("tmax", "monthly_max_temperature", "max"),
        ("precipitation", "monthly_precipitation_sum", "sum"),
        ("soil_moisture", "monthly_mean_soil_moisture", "mean"),
        ("evapotranspiration", "monthly_evapotranspiration_sum", "sum"),
        ("potential_evapotranspiration", "monthly_potential_evapotranspiration_sum", "sum"),
        ("drought_index", "monthly_drought_index_mean", "mean"),
    ]

    for role, output_variable, reducer in aggregation_plan:
        source_variable = variable_map.get(role)
        if not source_variable or source_variable not in dataset:
            continue
        data = getattr(dataset[source_variable].resample(time="MS"), reducer)()
        rows.extend(_dataarray_to_spatial_mean_rows(data, source_file, "monthly", output_variable, include_month=True))

    return rows


def _build_growing_season_rows(
    dataset: Any,
    variable_map: dict[str, str],
    source_file: str,
    rice_growth_months: list[int],
) -> list[dict[str, Any]]:
    """Build long-format growing-season climate rows."""

    rows: list[dict[str, Any]] = []
    season_dataset = dataset.sel(time=dataset["time"].dt.month.isin(rice_growth_months))
    if season_dataset.sizes.get("time", 0) == 0:
        return rows
    aggregation_plan = [
        ("temperature", "growing_season_mean_temperature", "mean"),
        ("tmax", "growing_season_max_temperature", "max"),
        ("precipitation", "growing_season_precipitation_sum", "sum"),
        ("soil_moisture", "growing_season_mean_soil_moisture", "mean"),
        ("evapotranspiration", "growing_season_evapotranspiration_sum", "sum"),
        ("potential_evapotranspiration", "growing_season_potential_evapotranspiration_sum", "sum"),
        ("drought_index", "growing_season_drought_index_mean", "mean"),
    ]

    for role, output_variable, reducer in aggregation_plan:
        source_variable = variable_map.get(role)
        if not source_variable or source_variable not in season_dataset:
            continue
        data = getattr(season_dataset[source_variable].groupby("time.year"), reducer)()
        rows.extend(_dataarray_to_spatial_mean_rows(data, source_file, "growing_season", output_variable))

    return rows


def _build_extreme_rows(
    dataset: Any,
    variable_map: dict[str, str],
    source_file: str,
    baseline_years: tuple[int, int],
    rice_growth_months: list[int],
    heat_threshold_quantile: float,
    drought_threshold_quantile: float,
) -> list[dict[str, Any]]:
    """Build grid-mean hot, dry, and compound timestep-count rows."""

    tmax_variable = variable_map.get("tmax") or variable_map.get("temperature")
    dry_variable = variable_map.get("soil_moisture") or variable_map.get("precipitation")
    if not tmax_variable or not dry_variable or tmax_variable not in dataset or dry_variable not in dataset:
        return []

    season = dataset.sel(time=dataset["time"].dt.month.isin(rice_growth_months))
    baseline = season.sel(time=slice(f"{baseline_years[0]}-01-01", f"{baseline_years[1]}-12-31"))
    if baseline.sizes.get("time", 0) == 0:
        return []

    hot_threshold = baseline[tmax_variable].quantile(heat_threshold_quantile, dim="time")
    very_hot_threshold = baseline[tmax_variable].quantile(0.95, dim="time")
    dry_threshold = baseline[dry_variable].quantile(drought_threshold_quantile, dim="time")

    hot = season[tmax_variable] > hot_threshold
    very_hot = season[tmax_variable] > very_hot_threshold
    dry = season[dry_variable] < dry_threshold
    compound = hot & dry

    rows: list[dict[str, Any]] = []
    for variable_name, data in (
        ("hot_days", hot),
        ("very_hot_days", very_hot),
        ("dry_days", dry),
        ("compound_hot_dry_days", compound),
    ):
        annual = data.groupby("time.year").sum(dim="time")
        rows.extend(_dataarray_to_spatial_mean_rows(annual, source_file, "extremes", variable_name))
    return rows


def _dataarray_to_spatial_mean_rows(
    data: Any,
    source_file: str,
    aggregation: str,
    variable: str,
    include_month: bool = False,
) -> list[dict[str, Any]]:
    """Convert an xarray DataArray to long rows after spatial averaging."""

    spatial_dims = [dim for dim in ("lat", "lon") if dim in data.dims]
    if spatial_dims:
        data = data.mean(dim=spatial_dims, skipna=True)

    rows: list[dict[str, Any]] = []
    if "time" in data.dims:
        for timestamp, value in zip(data["time"].values, data.values, strict=False):
            py_time = _to_python_datetime(timestamp)
            rows.append(
                {
                    "source_file": source_file,
                    "aggregation": aggregation,
                    "variable": variable,
                    "time": py_time.date().isoformat(),
                    "year": py_time.year,
                    "month": py_time.month if include_month else "",
                    "value": _safe_float(value),
                }
            )
    elif "year" in data.dims:
        for year, value in zip(data["year"].values, data.values, strict=False):
            rows.append(
                {
                    "source_file": source_file,
                    "aggregation": aggregation,
                    "variable": variable,
                    "time": "",
                    "year": int(year),
                    "month": "",
                    "value": _safe_float(value),
                }
            )
    return rows


def _write_empty_climate_outputs(interim_dir: Path, warnings: list[str]) -> dict[str, Path]:
    """Write empty fallback CSV outputs when climate data are missing."""

    return {
        "monthly": _write_table([], interim_dir / "climate_monthly.parquet", CLIMATE_ROW_COLUMNS, warnings),
        "growing_season": _write_table(
            [],
            interim_dir / "climate_growing_season.parquet",
            CLIMATE_ROW_COLUMNS,
            warnings,
        ),
        "extremes": _write_table(
            [],
            interim_dir / "climate_extremes_grid.parquet",
            EXTREME_ROW_COLUMNS,
            warnings,
        ),
    }


def _write_table(
    rows: list[dict[str, Any]],
    parquet_path: Path,
    columns: list[str],
    warnings: list[str],
) -> Path:
    """Write rows to Parquet, falling back to CSV if pandas/pyarrow are unavailable."""

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd

        frame = pd.DataFrame(rows, columns=columns)
        frame.to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception as exc:  # noqa: BLE001 - fallback keeps pipeline runnable in lean envs
        csv_path = parquet_path.with_suffix(".csv")
        _write_csv_rows(rows, columns, csv_path)
        warnings.append(f"Wrote CSV fallback for {parquet_path.name}: {type(exc).__name__}: {exc}")
        return csv_path


def _write_csv_rows(rows: list[dict[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write rows as CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_climate_report(
    result: ClimatePreprocessResult,
    study_bbox: list[float],
    baseline_years: tuple[int, int],
    rice_growth_months: list[int],
) -> None:
    """Write a Markdown climate preprocessing summary."""

    lines = [
        "# Climate Preprocess Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Candidate files: {result.file_count}",
        f"- Processed files: {len(result.processed_files)}",
        f"- Study bbox: {study_bbox}",
        f"- Baseline years: {baseline_years[0]}-{baseline_years[1]}",
        f"- Rice growth months: {rice_growth_months}",
        "",
        "Outputs use long format with `source_file`, `aggregation`, `variable`, `year`, `month`, and `value`.",
        "",
    ]

    if result.status == "missing":
        lines.extend(["No climate NetCDF files found.", ""])

    if result.outputs:
        lines.extend(["## Outputs", ""])
        lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
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


def _supported_external_files(
    external_files: Iterable[str | Path] | None,
    suffixes: tuple[str, ...],
) -> list[Path]:
    """Normalize supported external reference paths."""

    if external_files is None:
        return []
    files: list[Path] = []
    for value in external_files:
        path = Path(value).expanduser()
        if path.is_file() and path.suffix.lower() in suffixes:
            files.append(path.resolve())
    return files


def _unique_paths(paths: Iterable[Path]) -> list[Path]:
    """Return unique paths in first-seen order."""

    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(path.resolve())
    return unique


def _map_values(values: Any, converter: Callable[[float], float]) -> Any:
    """Apply a numeric converter to scalars, sequences, or array-like objects."""

    if isinstance(values, list):
        return [converter(float(value)) for value in values]
    if isinstance(values, tuple):
        return tuple(converter(float(value)) for value in values)
    try:
        return values - 273.15 if converter(273.15) == 0.0 else values * converter(1.0)
    except Exception:
        return converter(float(values))


def _normalize_name(name: str) -> str:
    """Normalize names for candidate matching."""

    return str(name).strip().lower().replace("_", "").replace("-", "").replace(" ", "")


def _normalize_units(units: str | None) -> str:
    """Normalize unit strings."""

    return str(units or "").strip().lower().replace("−", "-")


def _is_precip_rate_unit(units: str) -> bool:
    """Return True if units look like mass-flux precipitation rate."""

    compact = units.replace(" ", "")
    return "kgm-2s-1" in compact or "kg/m2/s" in compact or "kgm**-2s**-1" in compact


def _infer_time_step_seconds(dataset: Any) -> float | None:
    """Infer seconds between the first two timesteps."""

    if "time" not in dataset or dataset.sizes.get("time", 0) < 2:
        return None
    values = dataset["time"].values
    delta = values[1] - values[0]
    try:
        return float(delta / _numpy_timedelta64_one_second())
    except Exception:
        return None


def _numpy_timedelta64_one_second() -> Any:
    """Return numpy one-second timedelta without importing numpy at module import time."""

    import numpy as np

    return np.timedelta64(1, "s")


def _to_python_datetime(value: Any) -> datetime:
    """Convert numpy/cftime/pandas timestamps to Python datetime."""

    try:
        import pandas as pd

        return pd.Timestamp(value).to_pydatetime()
    except Exception:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value)[:19])


def _safe_float(value: Any) -> float | None:
    """Convert array scalar to float, preserving missing values as None."""

    try:
        result = float(value)
    except Exception:
        return None
    if result != result:
        return None
    return result


def _import_xarray() -> Any:
    """Import xarray lazily."""

    try:
        import xarray as xr
    except ImportError as exc:
        raise ImportError("xarray is required for NetCDF climate preprocessing") from exc
    return xr
