"""Panel assembly and spatial aggregation utilities."""

from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import median, pstdev
from typing import Any, Iterable, Mapping


PANEL_OUTPUT_COLUMNS = [
    "admin_id",
    "year",
    "month",
    "variable",
    "mean",
    "median",
    "min",
    "max",
    "std",
    "valid_pixel_count",
    "source_panel",
]

FALLBACK_OUTPUT_NAMES = {
    "climate": "admin_climate_panel.csv",
    "remote_sensing": "admin_remote_sensing_panel.csv",
    "water": "admin_water_panel.csv",
    "covariates": "admin_covariates_panel.csv",
}

DEFAULT_INPUT_PANEL_CANDIDATES = {
    "climate": (
        "climate_growing_season.parquet",
        "climate_growing_season.csv",
        "climate_monthly.parquet",
        "climate_monthly.csv",
    ),
    "remote_sensing": (
        "remote_sensing_panel.parquet",
        "remote_sensing_panel.csv",
        "remote_sensing_growing_season.parquet",
        "remote_sensing_growing_season.csv",
        "remote_sensing_monthly.parquet",
        "remote_sensing_monthly.csv",
    ),
    "water": ("water_panel.parquet", "water_panel.csv"),
    "covariates": ("covariates_panel.parquet", "covariates_panel.csv"),
}


@dataclass(frozen=True)
class SpatialAggregationResult:
    """Result metadata for spatial/table panel aggregation."""

    status: str
    admin_units_path: Path
    input_panels: dict[str, Path | None]
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/spatial_aggregation_qc.md")


def calculate_missing_rates(
    rows: Iterable[Mapping[str, Any]],
    variables: Iterable[str],
) -> dict[str, float]:
    """Calculate missing-value rates for variables across row mappings."""

    row_list = list(rows)
    variable_list = [str(variable) for variable in variables]
    if not row_list:
        return {variable: 1.0 for variable in variable_list}

    denominator = len(row_list)
    rates: dict[str, float] = {}
    for variable in variable_list:
        missing_count = sum(1 for row in row_list if _is_missing(row.get(variable)))
        rates[variable] = missing_count / denominator
    return rates


def calculate_valid_observations(
    rows: Iterable[Mapping[str, Any]],
    id_field: str,
    variable: str,
) -> dict[Any, int]:
    """Count non-missing observations for one variable grouped by an ID field."""

    counts: dict[Any, int] = {}
    for row in rows:
        admin_id = row.get(id_field)
        if _is_missing(admin_id):
            continue
        counts.setdefault(admin_id, 0)
        if not _is_missing(row.get(variable)):
            counts[admin_id] += 1
    return counts


def aggregate_values(values: Iterable[Any]) -> dict[str, float | int | None]:
    """Aggregate numeric values while ignoring None, NaN, and blank values."""

    valid_values = [_as_valid_float(value) for value in values]
    numbers = [value for value in valid_values if value is not None]
    count = len(numbers)
    if count == 0:
        return {
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "std": None,
            "valid_pixel_count": 0,
        }

    return {
        "mean": sum(numbers) / count,
        "median": float(median(numbers)),
        "min": min(numbers),
        "max": max(numbers),
        "std": pstdev(numbers) if count > 1 else 0.0,
        "valid_pixel_count": count,
    }


def spatial_aggregate(
    processed_dir: str | Path,
    interim_dir: str | Path,
    reports_dir: str | Path,
    admin_units_path: str | Path | None = None,
    input_panels: Mapping[str, str | Path] | None = None,
) -> SpatialAggregationResult:
    """Aggregate source panels to administrative units, with empty fallback outputs.

    The MVP keeps the pipeline runnable in lean environments. If the prepared
    administrative units or any expected input panel is missing, it writes empty
    output CSVs and a QC report instead of raising.
    """

    processed = Path(processed_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    interim.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    admin_path = (
        Path(admin_units_path).expanduser().resolve()
        if admin_units_path is not None
        else processed / "admin_units.gpkg"
    )
    panel_paths = _resolve_input_panels(interim, input_panels)
    warnings = _missing_input_warnings(admin_path, panel_paths)
    report_path = reports / "spatial_aggregation_qc.md"

    if not admin_path.exists():
        outputs = _write_empty_fallback_outputs(processed)
        result = SpatialAggregationResult(
            status="missing",
            admin_units_path=admin_path,
            input_panels=panel_paths,
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
        )
        _write_spatial_aggregation_report(result)
        return result

    admin_rows = _read_admin_units(admin_path, warnings)
    existing_panel_paths = {name: path for name, path in panel_paths.items() if path is not None and path.exists()}
    if not admin_rows or not existing_panel_paths:
        outputs = _write_empty_fallback_outputs(processed)
        result = SpatialAggregationResult(
            status="missing",
            admin_units_path=admin_path,
            input_panels=panel_paths,
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
        )
        _write_spatial_aggregation_report(result)
        return result

    warnings.append(
        "Full raster overlay is not implemented yet; existing regional source panels were broadcast to admin units."
    )
    outputs = _write_panel_outputs(processed, admin_rows, existing_panel_paths, warnings)
    for panel_name in FALLBACK_OUTPUT_NAMES:
        if panel_name not in outputs:
            path = processed / FALLBACK_OUTPUT_NAMES[panel_name]
            _write_csv_rows([], PANEL_OUTPUT_COLUMNS, path)
            outputs[panel_name] = path
    result = SpatialAggregationResult(
        status="partial" if warnings else "ok",
        admin_units_path=admin_path,
        input_panels=panel_paths,
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_spatial_aggregation_report(result)
    return result


def aggregate_netcdf_to_province_bounds(
    netcdf_paths: Iterable[str | Path],
    reference_raster_paths: Iterable[str | Path],
    output_path: str | Path,
    rice_growth_months: Iterable[int],
    category: str = "climate",
) -> tuple[Path, list[str]]:
    """Aggregate NetCDF grids to province extents inferred from reference rasters."""

    import pandas as pd
    import xarray as xr

    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    province_bounds = _province_bounds_from_reference_rasters(reference_raster_paths, warnings)
    rows: list[dict[str, Any]] = []
    months = {int(month) for month in rice_growth_months}
    if not province_bounds:
        warnings.append("No province reference raster bounds were available for NetCDF aggregation.")
        _write_dataframe(rows, output)
        return output, warnings

    for raw_path in netcdf_paths:
        path = Path(raw_path).expanduser().resolve()
        if path.suffix.lower() not in {".nc", ".nc4", ".cdf"}:
            continue
        try:
            with xr.open_dataset(path) as dataset:
                coord_names = _identify_netcdf_coords(dataset)
                variable_map = _identify_netcdf_variables(dataset, category)
                if not {"lat", "lon"}.issubset(coord_names) or not variable_map:
                    warnings.append(f"Skipped NetCDF without usable coordinates or variables: {path}")
                    continue
                for role, variable_name in variable_map.items():
                    data_array = dataset[variable_name]
                    if coord_names.get("time") in data_array.dims:
                        time_name = coord_names["time"]
                        month_mask = data_array[time_name].dt.month.isin(sorted(months)).values
                        data_array = data_array.isel({time_name: month_mask})
                        years = sorted({int(value) for value in data_array[time_name].dt.year.values})
                    else:
                        time_name = None
                        years = [_extract_year_from_path(path) or 0]
                    data_array = _convert_netcdf_units(data_array, role)
                    for year in years:
                        year_array = (
                            data_array.isel({time_name: (data_array[time_name].dt.year == year).values})
                            if time_name
                            else data_array
                        )
                        if year_array.size == 0:
                            continue
                        for province, bounds in province_bounds.items():
                            subset = _subset_data_array_to_bounds(year_array, coord_names["lat"], coord_names["lon"], bounds)
                            value = _aggregate_data_array_value(subset, role, time_name)
                            if value is None:
                                continue
                            rows.append(
                                {
                                    "province": province,
                                    "year": int(year),
                                    "variable": _province_variable_name(role),
                                    "value": value,
                                    "source_file": path.name,
                                    "aggregation": "growing_season_province_bounds",
                                    "category": category,
                                }
                            )
        except Exception as exc:  # noqa: BLE001 - one NetCDF must not stop aggregation
            warnings.append(f"Could not aggregate NetCDF {path}: {type(exc).__name__}: {exc}")

    _write_dataframe(rows, output)
    return output, warnings


def _resolve_input_panels(
    interim_dir: Path,
    input_panels: Mapping[str, str | Path] | None,
) -> dict[str, Path | None]:
    """Resolve explicit panel paths or first existing default candidate."""

    resolved: dict[str, Path | None] = {}
    if input_panels is not None:
        for key, value in input_panels.items():
            resolved[key] = Path(value).expanduser().resolve() if value is not None else None
        return resolved

    for key, candidates in DEFAULT_INPUT_PANEL_CANDIDATES.items():
        existing = None
        for candidate in candidates:
            path = interim_dir / candidate
            if path.exists():
                existing = path.resolve()
                break
        resolved[key] = existing or (interim_dir / candidates[0]).resolve()
    return resolved


def _missing_input_warnings(admin_units_path: Path, input_panels: Mapping[str, Path | None]) -> list[str]:
    """Build warnings for missing required aggregation inputs."""

    warnings: list[str] = []
    if not admin_units_path.exists():
        warnings.append(f"Administrative units file missing: {admin_units_path}")

    for panel_name, path in input_panels.items():
        if path is None or not path.exists():
            warnings.append(f"Input panel missing for {panel_name}: {path}")
    return warnings


def _read_admin_units(admin_units_path: Path, warnings: list[str]) -> list[dict[str, Any]]:
    """Read admin IDs and names from Parquet, CSV, or vector admin files."""

    try:
        if admin_units_path.suffix.lower() == ".parquet":
            import pandas as pd

            frame = pd.read_parquet(admin_units_path)
        elif admin_units_path.suffix.lower() == ".csv":
            import pandas as pd

            frame = pd.read_csv(admin_units_path)
        else:
            import geopandas as gpd

            frame = gpd.read_file(admin_units_path)
    except Exception as exc:  # noqa: BLE001 - keep missing-report behavior
        warnings.append(f"Could not read administrative units: {type(exc).__name__}: {exc}")
        return []

    if "admin_id" not in frame.columns:
        frame = frame.copy()
        frame["admin_id"] = [f"admin_{index + 1:06d}" for index in range(len(frame))]
    name_column = _first_existing_column(frame.columns, ["admin_name", "county", "prefecture", "shapeName", "NAME_3"])
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        admin_id = row.get("admin_id")
        if _is_missing(admin_id):
            continue
        rows.append(
            {
                "admin_id": str(admin_id),
                "admin_name": "" if name_column is None or _is_missing(row.get(name_column)) else str(row.get(name_column)),
            }
        )
    return rows


def _write_panel_outputs(
    processed_dir: Path,
    admin_rows: list[dict[str, Any]],
    panel_paths: Mapping[str, Path],
    warnings: list[str],
) -> dict[str, Path]:
    """Broadcast existing regional panel rows to admin IDs and write outputs."""

    outputs: dict[str, Path] = {}
    for panel_name, source_path in panel_paths.items():
        output_stem = Path(FALLBACK_OUTPUT_NAMES.get(panel_name, f"admin_{panel_name}_panel.csv")).stem
        output_path = processed_dir / f"{output_stem}.parquet"
        try:
            source_rows = _read_source_panel(source_path)
            output_rows = _collapse_admin_panel_rows(_broadcast_panel_rows(admin_rows, source_rows, panel_name))
            outputs[panel_name] = _write_panel_table(output_rows, output_path, warnings)
        except Exception as exc:  # noqa: BLE001 - one panel must not stop others
            warnings.append(f"Could not aggregate {panel_name}: {type(exc).__name__}: {exc}")
            fallback_path = processed_dir / FALLBACK_OUTPUT_NAMES.get(panel_name, f"admin_{panel_name}_panel.csv")
            _write_csv_rows([], PANEL_OUTPUT_COLUMNS, fallback_path)
            outputs[panel_name] = fallback_path
    return outputs


def _read_source_panel(path: Path) -> list[dict[str, Any]]:
    """Read a CSV or Parquet source panel as row dictionaries."""

    import pandas as pd

    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_csv(path)
    return frame.to_dict(orient="records")


def _broadcast_panel_rows(
    admin_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    panel_name: str,
) -> list[dict[str, Any]]:
    """Broadcast region-mean panel rows to each administrative unit."""

    output_rows: list[dict[str, Any]] = []
    for source in source_rows:
        stats = _source_row_stats(source)
        for admin in admin_rows:
            output_rows.append(
                {
                    "admin_id": admin["admin_id"],
                    "year": _clean_output_value(source.get("year")),
                    "month": _clean_output_value(source.get("month")),
                    "variable": _clean_output_value(source.get("variable")),
                    "mean": stats["mean"],
                    "median": stats["median"],
                    "min": stats["min"],
                    "max": stats["max"],
                    "std": stats["std"],
                    "valid_pixel_count": stats["valid_pixel_count"],
                    "source_panel": panel_name,
                }
            )
    return output_rows


def _collapse_admin_panel_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse duplicate admin-year-month-variable rows after broadcasting."""

    if not rows:
        return []
    import pandas as pd

    frame = pd.DataFrame(rows, columns=PANEL_OUTPUT_COLUMNS)
    key_columns = ["admin_id", "year", "month", "variable", "source_panel"]
    numeric_columns = ["mean", "median", "min", "max", "std", "valid_pixel_count"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    collapsed = (
        frame.groupby(key_columns, dropna=False, as_index=False)
        .agg(
            mean=("mean", "mean"),
            median=("median", "median"),
            min=("min", "min"),
            max=("max", "max"),
            std=("std", "mean"),
            valid_pixel_count=("valid_pixel_count", "sum"),
        )
        .reset_index(drop=True)
    )
    collapsed["valid_pixel_count"] = collapsed["valid_pixel_count"].fillna(0).astype(int)
    return collapsed[PANEL_OUTPUT_COLUMNS].where(pd.notna(collapsed[PANEL_OUTPUT_COLUMNS]), "").to_dict(orient="records")


def _source_row_stats(row: Mapping[str, Any]) -> dict[str, Any]:
    """Build aggregation statistics from a long-format source row."""

    if "mean" in row and not _is_missing(row.get("mean")):
        value = _as_valid_float(row.get("mean"))
    else:
        value = _as_valid_float(row.get("value"))
    if value is None:
        return {
            "mean": "",
            "median": "",
            "min": "",
            "max": "",
            "std": "",
            "valid_pixel_count": 0,
        }
    return {
        "mean": value,
        "median": _as_valid_float(row.get("median")) if not _is_missing(row.get("median")) else value,
        "min": _as_valid_float(row.get("min")) if not _is_missing(row.get("min")) else value,
        "max": _as_valid_float(row.get("max")) if not _is_missing(row.get("max")) else value,
        "std": _as_valid_float(row.get("std")) if not _is_missing(row.get("std")) else 0.0,
        "valid_pixel_count": int(_as_valid_float(row.get("valid_pixel_count")) or 1),
    }


def _write_panel_table(rows: list[dict[str, Any]], parquet_path: Path, warnings: list[str]) -> Path:
    """Write an admin panel to Parquet and a small CSV companion when possible."""

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd

        frame = pd.DataFrame(rows, columns=PANEL_OUTPUT_COLUMNS)
        frame.to_parquet(parquet_path, index=False)
        frame.to_csv(parquet_path.with_suffix(".csv"), index=False, encoding="utf-8-sig")
        return parquet_path
    except Exception as exc:  # noqa: BLE001 - CSV fallback keeps the pipeline runnable
        csv_path = parquet_path.with_suffix(".csv")
        _write_csv_rows(rows, PANEL_OUTPUT_COLUMNS, csv_path)
        warnings.append(f"Wrote CSV fallback for {parquet_path.name}: {type(exc).__name__}: {exc}")
        return csv_path


def _first_existing_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    """Return the first candidate column present in a column collection."""

    column_set = {str(column): str(column) for column in columns}
    for candidate in candidates:
        if candidate in column_set:
            return column_set[candidate]
    return None


def _clean_output_value(value: Any) -> Any:
    """Return a CSV/Parquet friendly value for panel fields."""

    if _is_missing(value):
        return ""
    return value


def _write_empty_fallback_outputs(processed_dir: Path) -> dict[str, Path]:
    """Write empty fallback CSVs for all expected administrative panels."""

    outputs: dict[str, Path] = {}
    for key, file_name in FALLBACK_OUTPUT_NAMES.items():
        path = processed_dir / file_name
        _write_csv_rows([], PANEL_OUTPUT_COLUMNS, path)
        outputs[key] = path
    return outputs


def _write_csv_rows(rows: Iterable[Mapping[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write rows as UTF-8 CSV with a stable header."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_spatial_aggregation_report(result: SpatialAggregationResult) -> None:
    """Write Markdown QC report for the aggregation step."""

    lines = [
        "# Spatial Aggregation QC",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Admin units: `{result.admin_units_path}`",
        "",
        "## Input Panels",
        "",
        "| panel | path | exists |",
        "| --- | --- | --- |",
    ]
    for panel_name, path in result.input_panels.items():
        exists = bool(path and path.exists())
        lines.append(f"| {panel_name} | `{path}` | {exists} |")

    lines.extend(["", "## Outputs", ""])
    lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
    lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _province_bounds_from_reference_rasters(
    reference_raster_paths: Iterable[str | Path],
    warnings: list[str],
) -> dict[str, tuple[float, float, float, float]]:
    """Infer WGS84 province bounds from crop-mask raster filenames and metadata."""

    import rasterio
    from rasterio.warp import transform_bounds

    bounds_by_province: dict[str, tuple[float, float, float, float]] = {}
    for raw_path in reference_raster_paths:
        path = Path(raw_path).expanduser().resolve()
        if path.suffix.lower() not in {".tif", ".tiff"}:
            continue
        province = _province_from_reference_raster_name(path)
        if not province:
            continue
        try:
            with rasterio.open(path) as src:
                if src.crs is None:
                    warnings.append(f"Reference raster has no CRS and was skipped: {path}")
                    continue
                bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds, densify_pts=21)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Could not read reference raster bounds {path}: {type(exc).__name__}: {exc}")
            continue
        existing = bounds_by_province.get(province)
        if existing is None:
            bounds_by_province[province] = tuple(float(value) for value in bounds)
        else:
            bounds_by_province[province] = (
                min(existing[0], float(bounds[0])),
                min(existing[1], float(bounds[1])),
                max(existing[2], float(bounds[2])),
                max(existing[3], float(bounds[3])),
            )
    return bounds_by_province


def _province_from_reference_raster_name(path: Path) -> str:
    """Parse and normalize a province name from a crop-mask filename."""

    from src.yield_sources import normalize_province_name

    match = re.search(r"classified-(.+?)-(?:(?:19|20)\d{2})", path.name, flags=re.IGNORECASE)
    if not match:
        return ""
    return normalize_province_name(match.group(1).replace("_", " "))


def _identify_netcdf_coords(dataset: Any) -> dict[str, str]:
    """Identify time, latitude, and longitude coordinate names in a dataset."""

    candidates = {
        "time": ["time", "valid_time"],
        "lat": ["lat", "latitude", "y"],
        "lon": ["lon", "longitude", "x"],
    }
    names = [str(name) for name in [*dataset.coords, *dataset.dims]]
    normalized = {_normalize_panel_name(name): name for name in names}
    result: dict[str, str] = {}
    for role, values in candidates.items():
        for value in values:
            matched = normalized.get(_normalize_panel_name(value))
            if matched is not None:
                result[role] = matched
                break
    return result


def _identify_netcdf_variables(dataset: Any, category: str) -> dict[str, str]:
    """Identify standard variable roles from NetCDF data variables."""

    if category == "remote_sensing":
        candidates = {
            "et": ["e", "et", "evapotranspiration"],
            "soil_moisture": ["sms", "smrz", "soil_moisture"],
            "lst": ["lst", "lst_day", "lst_day_1km"],
            "ndvi": ["ndvi"],
            "evi": ["evi"],
            "tws": ["tws", "lwe_thickness"],
        }
    else:
        candidates = {
            "temperature": ["t2m", "2m_temperature", "temperature", "temp", "tas"],
            "tmax": ["tmax", "maximum_temperature", "max_temperature", "mx2t", "tasmax"],
            "precipitation": ["prec", "precip", "precipitation", "tp", "total_precipitation", "pr"],
            "soil_moisture": ["soil_moisture", "soilmoisture", "sm", "swvl1", "volumetric_soil_water_layer_1"],
            "evapotranspiration": ["et", "e", "evapotranspiration", "aet"],
            "potential_evapotranspiration": ["pet", "pev", "potential_evapotranspiration"],
            "drought_index": ["spei", "pdsi", "scpdsi", "drought_index"],
        }
    normalized = {_normalize_panel_name(name): str(name) for name in dataset.data_vars}
    result: dict[str, str] = {}
    for role, values in candidates.items():
        for value in values:
            matched = normalized.get(_normalize_panel_name(value))
            if matched is not None:
                result[role] = matched
                break
    return result


def _convert_netcdf_units(data_array: Any, role: str) -> Any:
    """Apply lightweight temperature and precipitation unit conversion."""

    units = str(data_array.attrs.get("units") or "").strip().lower()
    if role in {"temperature", "tmax", "lst"} and units in {"k", "kelvin"}:
        return data_array - 273.15
    if role == "precipitation" and units in {"m", "meter", "metre", "meters", "metres"}:
        return data_array * 1000.0
    return data_array


def _subset_data_array_to_bounds(
    data_array: Any,
    lat_name: str,
    lon_name: str,
    bounds: tuple[float, float, float, float],
) -> Any:
    """Subset a DataArray to WGS84 bounds."""

    minx, miny, maxx, maxy = bounds
    lat_values = data_array[lat_name].values
    lon_values = data_array[lon_name].values
    lat_slice = slice(miny, maxy) if lat_values[0] <= lat_values[-1] else slice(maxy, miny)
    lon_slice = slice(minx, maxx) if lon_values[0] <= lon_values[-1] else slice(maxx, minx)
    return data_array.sel({lat_name: lat_slice, lon_name: lon_slice})


def _aggregate_data_array_value(data_array: Any, role: str, time_name: str | None) -> float | None:
    """Aggregate a subset DataArray to a single growing-season value."""

    import numpy as np

    if data_array.size == 0:
        return None
    spatial_dims = [dim for dim in data_array.dims if dim != time_name]
    if role in {"precipitation", "evapotranspiration", "potential_evapotranspiration", "et"} and time_name:
        value = data_array.mean(dim=spatial_dims, skipna=True).sum(dim=time_name, skipna=True)
    else:
        dims = list(data_array.dims)
        value = data_array.mean(dim=dims, skipna=True)
    number = float(value.values)
    if not np.isfinite(number):
        return None
    return number


def _province_variable_name(role: str) -> str:
    """Return model-panel compatible variable names for province aggregates."""

    if role == "temperature":
        return "growing_season_mean_temperature"
    if role == "tmax":
        return "growing_season_max_temperature"
    if role == "precipitation":
        return "growing_season_precipitation_sum"
    if role == "soil_moisture":
        return "growing_season_mean_soil_moisture"
    if role == "evapotranspiration":
        return "growing_season_evapotranspiration_sum"
    if role == "potential_evapotranspiration":
        return "growing_season_potential_evapotranspiration_sum"
    return str(role)


def _extract_year_from_path(path: Path) -> int | None:
    """Extract the first four-digit year from a filename."""

    match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", path.name)
    return int(match.group(1)) if match else None


def _write_dataframe(rows: list[dict[str, Any]], output_path: Path) -> None:
    """Write rows to Parquet or CSV based on output suffix."""

    import pandas as pd

    columns = ["province", "year", "variable", "value", "source_file", "aggregation", "category"]
    frame = pd.DataFrame(rows, columns=columns)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".parquet":
        frame.to_parquet(output_path, index=False)
    else:
        frame.to_csv(output_path, index=False)


def _normalize_panel_name(name: str) -> str:
    """Normalize NetCDF names for candidate matching."""

    return str(name).strip().lower().replace("_", "").replace("-", "").replace(" ", "")


def _is_missing(value: Any) -> bool:
    """Return True for None, NaN, and blank string values."""

    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        return bool(math.isnan(float(value)))
    except (TypeError, ValueError):
        return False


def _as_valid_float(value: Any) -> float | None:
    """Convert a value to float, returning None for missing or non-numeric values."""

    if _is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number
