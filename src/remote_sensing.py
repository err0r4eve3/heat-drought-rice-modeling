"""Remote sensing preprocessing utilities."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable


SUPPORTED_REMOTE_SENSING_SUFFIXES = (".tif", ".tiff", ".nc", ".nc4", ".hdf", ".h5")
REMOTE_SENSING_SUFFIX_PRIORITY = {
    suffix: index for index, suffix in enumerate(SUPPORTED_REMOTE_SENSING_SUFFIXES)
}

REMOTE_SENSING_ROW_COLUMNS = [
    "source_file",
    "product",
    "variable",
    "aggregation",
    "time",
    "year",
    "month",
    "value",
    "crs",
]

PRODUCT_FILENAME_CANDIDATES: dict[str, list[str]] = {
    "MOD13Q1": ["mod13q1"],
    "MYD13Q1": ["myd13q1"],
    "MOD11A2": ["mod11a2"],
    "MOD16A2": ["mod16a2"],
    "SMAP": ["smap"],
    "GLEAM": ["gleam"],
    "GRACE": ["grace", "grctellus", "mascon"],
}

REMOTE_SENSING_VARIABLE_CANDIDATES: dict[str, list[str]] = {
    "ndvi": ["ndvi", "250m_16_days_ndvi", "1_km_16_days_ndvi"],
    "evi": ["evi", "250m_16_days_evi", "1_km_16_days_evi"],
    "lst": ["lst", "lst_day_1km", "lst_night_1km", "land_surface_temperature"],
    "et": ["e", "et", "et_500m", "evapotranspiration", "aet", "actual_evapotranspiration"],
    "soil_moisture": ["sms", "smrz", "soil_moisture", "soilmoisture", "sm", "smap_sm", "rootzone_sm"],
    "evaporative_stress": ["s", "stress", "evaporative_stress"],
    "tws": ["tws", "twsa", "lwe", "lwe_thickness", "water_storage", "equivalent_water_thickness"],
}

VARIABLE_PRODUCT_HINTS: dict[str, str] = {
    "ndvi": "MOD13Q1",
    "evi": "MOD13Q1",
    "lst": "MOD11A2",
    "et": "MOD16A2",
    "soil_moisture": "SMAP",
    "tws": "GRACE",
}


@dataclass(frozen=True)
class RemoteSensingPreprocessResult:
    """Result metadata for remote-sensing preprocessing."""

    status: str
    file_count: int
    processed_files: list[Path]
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/remote_sensing_summary.md")
    metadata: list[dict[str, Any]] = field(default_factory=list)


def find_remote_sensing_files(
    remote_sensing_dir: str | Path,
    external_files: Iterable[str | Path] | None = None,
) -> list[Path]:
    """Find supported remote-sensing files."""

    root = Path(remote_sensing_dir).expanduser().resolve()
    files: list[Path] = []
    if root.exists():
        files.extend(
            path.resolve()
            for path in root.rglob("*")
            if path.is_file() and path.suffix.lower() in SUPPORTED_REMOTE_SENSING_SUFFIXES
        )
    if not files:
        files.extend(_supported_external_files(external_files, SUPPORTED_REMOTE_SENSING_SUFFIXES))
    return sorted(
        _unique_paths(files),
        key=lambda path: (REMOTE_SENSING_SUFFIX_PRIORITY[path.suffix.lower()], str(path)),
    )


def scale_modis_ndvi(values: Any) -> Any:
    """Scale MODIS NDVI raw values by 0.0001 and mask fill/invalid values."""

    return _scale_values(values, scale=0.0001, offset=0.0, min_raw=-3000.0, max_raw=10000.0)


def scale_modis_evi(values: Any) -> Any:
    """Scale MODIS EVI raw values by 0.0001 and mask fill/invalid values."""

    return _scale_values(values, scale=0.0001, offset=0.0, min_raw=-3000.0, max_raw=10000.0)


def scale_modis_lst_celsius(values: Any) -> Any:
    """Scale MODIS LST raw values by 0.02 and convert Kelvin to Celsius."""

    return _scale_values(
        values,
        scale=0.02,
        offset=-273.15,
        min_raw=7501.0,
        max_raw=65535.0,
        round_digits=2,
    )


def identify_remote_sensing_product(
    file_name: str | Path | None = None,
    variable_names: list[str] | Any | None = None,
) -> dict[str, Any]:
    """Identify a coarse remote-sensing product and known variables."""

    variables = identify_remote_sensing_variables(variable_names or [])
    product = _identify_product_from_filename(file_name)
    if product is None and variables:
        product = VARIABLE_PRODUCT_HINTS.get(next(iter(variables)))

    return {"product": product, "variables": variables}


def identify_remote_sensing_variables(variable_names: list[str] | Any) -> dict[str, str]:
    """Identify remote-sensing variables from common variable names."""

    names = [str(name) for name in variable_names]
    normalized = {_normalize_name(name): name for name in names}
    mapping: dict[str, str] = {}

    for role, candidates in REMOTE_SENSING_VARIABLE_CANDIDATES.items():
        for candidate in candidates:
            matched = normalized.get(_normalize_name(candidate))
            if matched is not None:
                mapping[role] = matched
                break
    return mapping


def preprocess_remote_sensing(
    remote_sensing_dir: str | Path,
    interim_dir: str | Path,
    reports_dir: str | Path,
    study_bbox: list[float],
    baseline_years: tuple[int, int],
    rice_growth_months: list[int],
    crs_wgs84: str = "EPSG:4326",
    crs_equal_area: str = "EPSG:6933",
    external_files: Iterable[str | Path] | None = None,
) -> RemoteSensingPreprocessResult:
    """Preprocess remote-sensing files into MVP monthly and growing-season tables."""

    remote_root = Path(remote_sensing_dir).expanduser().resolve()
    interim = Path(interim_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    interim.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    report_path = reports / "remote_sensing_summary.md"

    files = find_remote_sensing_files(remote_root, external_files=external_files)
    warnings: list[str] = []
    metadata: list[dict[str, Any]] = []
    processed_files: list[Path] = []
    monthly_rows: list[dict[str, Any]] = []
    growing_rows: list[dict[str, Any]] = []

    if not files:
        warnings.append(f"No remote sensing files found under {remote_root}.")
        outputs = _write_remote_sensing_outputs(interim, None, None, warnings)
        result = RemoteSensingPreprocessResult(
            status="missing",
            file_count=0,
            processed_files=[],
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
            metadata=[],
        )
        _write_remote_sensing_report(result, study_bbox, baseline_years, rice_growth_months, crs_wgs84, crs_equal_area)
        return result

    for path in files:
        try:
            record = _read_basic_metadata(path)
            metadata.append(record)
            extracted = _extract_remote_sensing_rows(
                path=path,
                metadata=record,
                study_bbox=study_bbox,
                rice_growth_months=rice_growth_months,
                crs_wgs84=crs_wgs84,
            )
            monthly_rows.extend(extracted["monthly"])
            growing_rows.extend(extracted["growing"])
            processed_files.append(path)
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the batch
            warnings.append(f"Skipped {path}: {type(exc).__name__}: {exc}")

    outputs = _write_remote_sensing_outputs(interim, monthly_rows, growing_rows, warnings)
    if processed_files:
        status = "ok" if len(processed_files) == len(files) else "partial"
    else:
        status = "error"

    result = RemoteSensingPreprocessResult(
        status=status,
        file_count=len(files),
        processed_files=processed_files,
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
        metadata=metadata,
    )
    _write_remote_sensing_report(result, study_bbox, baseline_years, rice_growth_months, crs_wgs84, crs_equal_area)
    return result


def _read_basic_metadata(path: Path) -> dict[str, Any]:
    """Read lightweight metadata from a supported file."""

    suffix = path.suffix.lower()
    if suffix in {".nc", ".nc4"}:
        return _read_netcdf_metadata(path)
    if suffix in {".tif", ".tiff"}:
        return _read_raster_metadata(path)
    if suffix in {".hdf", ".h5"}:
        return _read_hdf_metadata(path)
    raise ValueError(f"Unsupported remote sensing suffix: {suffix}")


def _extract_remote_sensing_rows(
    path: Path,
    metadata: dict[str, Any],
    study_bbox: list[float],
    rice_growth_months: list[int],
    crs_wgs84: str,
) -> dict[str, list[dict[str, Any]]]:
    """Extract lightweight region-mean time series rows from readable grids."""

    if metadata.get("format") != "netcdf":
        return {"monthly": [], "growing": []}
    return _extract_netcdf_time_series_rows(path, study_bbox, rice_growth_months, crs_wgs84)


def _read_netcdf_metadata(path: Path) -> dict[str, Any]:
    """Read basic NetCDF metadata with xarray."""

    xr = _import_xarray()
    with xr.open_dataset(path, decode_times=False) as dataset:
        variable_names = [str(name) for name in dataset.data_vars]
        product_info = identify_remote_sensing_product(path.name, variable_names)
        return {
            "source_file": path.name,
            "path": str(path),
            "format": "netcdf",
            "product": product_info["product"],
            "variables": product_info["variables"],
            "dimensions": {str(name): int(size) for name, size in dataset.sizes.items()},
            "crs": _extract_xarray_crs(dataset),
        }


def _extract_netcdf_time_series_rows(
    path: Path,
    study_bbox: list[float],
    rice_growth_months: list[int],
    crs_wgs84: str,
) -> dict[str, list[dict[str, Any]]]:
    """Read NetCDF variables into monthly and growing-season spatial means."""

    xr = _import_xarray()
    with xr.open_dataset(path, decode_times=True, chunks="auto") as dataset:
        coord_names = _identify_spatiotemporal_coords(list(dataset.coords) + list(dataset.dims))
        if not {"time", "lat", "lon"}.issubset(coord_names):
            return {"monthly": [], "growing": []}
        dataset = _standardize_coordinates(dataset, coord_names)
        dataset = _clip_to_bbox(dataset, study_bbox)
        variable_map = identify_remote_sensing_variables(list(dataset.data_vars))
        if not variable_map:
            return {"monthly": [], "growing": []}

        product = identify_remote_sensing_product(path.name, list(dataset.data_vars)).get("product")
        crs = _extract_xarray_crs(dataset) or crs_wgs84
        monthly_rows: list[dict[str, Any]] = []
        growing_rows: list[dict[str, Any]] = []

        for role, variable_name in variable_map.items():
            if variable_name not in dataset:
                continue
            data = _scale_dataarray_for_role(dataset[variable_name], role)
            monthly_rows.extend(
                _dataarray_to_remote_rows(
                    data=data,
                    source_file=path.name,
                    product=product,
                    variable=role,
                    aggregation="monthly",
                    crs=crs,
                    include_month=True,
                )
            )
            season = _spatial_mean(data).sel(time=data["time"].dt.month.isin(rice_growth_months))
            if season.sizes.get("time", 0) == 0:
                continue
            reducer = "sum" if role in {"et"} else "mean"
            annual = getattr(season.groupby("time.year"), reducer)(dim="time", skipna=True)
            growing_rows.extend(
                _dataarray_to_remote_rows(
                    data=annual,
                    source_file=path.name,
                    product=product,
                    variable=role,
                    aggregation="growing_season",
                    crs=crs,
                    include_month=False,
                )
            )
        return {"monthly": monthly_rows, "growing": growing_rows}


def _identify_spatiotemporal_coords(names: list[str]) -> dict[str, str]:
    """Identify common time, latitude, and longitude coordinate names."""

    normalized = {_normalize_name(name): name for name in names}
    candidates = {
        "time": ["time", "valid_time"],
        "lat": ["lat", "latitude", "y"],
        "lon": ["lon", "longitude", "x"],
    }
    mapping: dict[str, str] = {}
    for role, role_candidates in candidates.items():
        for candidate in role_candidates:
            matched = normalized.get(_normalize_name(candidate))
            if matched is not None:
                mapping[role] = matched
                break
    return mapping


def _standardize_coordinates(dataset: Any, coord_names: dict[str, str]) -> Any:
    """Rename coordinates to time, lat, and lon when needed."""

    rename_map = {
        source: target
        for target, source in coord_names.items()
        if source != target and source in dataset
    }
    return dataset.rename(rename_map)


def _clip_to_bbox(dataset: Any, study_bbox: list[float]) -> Any:
    """Clip a dataset to the configured lon/lat bounding box."""

    west, south, east, north = [float(value) for value in study_bbox]
    lon_values = dataset["lon"]
    lat_values = dataset["lat"]
    lon_ascending = float(lon_values.values[0]) <= float(lon_values.values[-1])
    lat_ascending = float(lat_values.values[0]) <= float(lat_values.values[-1])
    lon_slice = slice(west, east) if lon_ascending else slice(east, west)
    lat_slice = slice(south, north) if lat_ascending else slice(north, south)
    return dataset.sel(lon=lon_slice, lat=lat_slice)


def _scale_dataarray_for_role(data: Any, role: str) -> Any:
    """Apply known MODIS scale factors to array data by canonical variable role."""

    if role == "ndvi":
        return scale_modis_ndvi(data)
    if role == "evi":
        return scale_modis_evi(data)
    if role == "lst":
        return scale_modis_lst_celsius(data)
    return data


def _dataarray_to_remote_rows(
    data: Any,
    source_file: str,
    product: str | None,
    variable: str,
    aggregation: str,
    crs: str | None,
    include_month: bool,
) -> list[dict[str, Any]]:
    """Convert an xarray DataArray to long remote-sensing rows."""

    data = _spatial_mean(data)

    rows: list[dict[str, Any]] = []
    if "time" in data.dims:
        for timestamp, value in zip(data["time"].values, data.values, strict=False):
            py_time = _to_python_datetime(timestamp)
            rows.append(
                {
                    "source_file": source_file,
                    "product": product or "",
                    "variable": variable,
                    "aggregation": aggregation,
                    "time": py_time.date().isoformat(),
                    "year": py_time.year,
                    "month": py_time.month if include_month else "",
                    "value": _safe_float(value),
                    "crs": crs or "",
                }
            )
    elif "year" in data.dims:
        for year, value in zip(data["year"].values, data.values, strict=False):
            rows.append(
                {
                    "source_file": source_file,
                    "product": product or "",
                    "variable": variable,
                    "aggregation": aggregation,
                    "time": "",
                    "year": int(year),
                    "month": "",
                    "value": _safe_float(value),
                    "crs": crs or "",
                }
            )
    return rows


def _spatial_mean(data: Any) -> Any:
    """Return a spatial mean over lat/lon when those dimensions exist."""

    spatial_dims = [dim for dim in ("lat", "lon") if dim in data.dims]
    if spatial_dims:
        return data.mean(dim=spatial_dims, skipna=True)
    return data


def _read_raster_metadata(path: Path) -> dict[str, Any]:
    """Read basic GeoTIFF metadata with rasterio."""

    rasterio = _import_rasterio()
    with rasterio.open(path) as dataset:
        product_info = identify_remote_sensing_product(path.name, [dataset.descriptions[0] or path.stem])
        crs = dataset.crs.to_string() if dataset.crs else None
        return {
            "source_file": path.name,
            "path": str(path),
            "format": "raster",
            "product": product_info["product"],
            "variables": product_info["variables"],
            "dimensions": {"bands": dataset.count, "height": dataset.height, "width": dataset.width},
            "crs": crs,
        }


def _read_hdf_metadata(path: Path) -> dict[str, Any]:
    """Read basic HDF5 metadata with h5py."""

    h5py = _import_h5py()
    try:
        with h5py.File(path, "r") as dataset:
            variable_names = _collect_hdf_dataset_names(dataset)
            product_info = identify_remote_sensing_product(path.name, variable_names)
            return {
                "source_file": path.name,
                "path": str(path),
                "format": "hdf",
                "product": product_info["product"],
                "variables": product_info["variables"],
                "dimensions": {},
                "crs": _extract_hdf_crs(dataset),
            }
    except OSError as exc:
        fallback = _read_hdf4_metadata_limited(path, exc)
        if fallback is not None:
            return fallback
        raise


def _read_hdf4_metadata_limited(path: Path, error: OSError) -> dict[str, Any] | None:
    """Return filename-based metadata for MODIS HDF4 files when HDF4 IO is unavailable."""

    product_info = identify_remote_sensing_product(path.name, [])
    product = product_info["product"]
    if path.suffix.lower() != ".hdf" or product not in {"MOD13Q1", "MYD13Q1", "MOD11A2", "MOD16A2"}:
        return None
    return {
        "source_file": path.name,
        "path": str(path),
        "format": "hdf4_metadata_limited",
        "product": product,
        "variables": product_info["variables"],
        "dimensions": {},
        "crs": None,
        "metadata_warning": f"HDF4 driver unavailable or unsupported; used filename metadata only: {error}",
    }


def _write_remote_sensing_outputs(
    interim_dir: Path,
    monthly_rows: list[dict[str, Any]] | None = None,
    growing_rows: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Path]:
    """Write remote-sensing outputs, using Parquet when rows are available."""

    if monthly_rows is None or growing_rows is None:
        monthly_path = interim_dir / "remote_sensing_monthly.csv"
        growing_path = interim_dir / "remote_sensing_growing_season.csv"
        _write_csv_rows([], REMOTE_SENSING_ROW_COLUMNS, monthly_path)
        _write_csv_rows([], REMOTE_SENSING_ROW_COLUMNS, growing_path)
        return {"monthly": monthly_path, "growing_season": growing_path}

    monthly_path = _write_table(monthly_rows, interim_dir / "remote_sensing_monthly.parquet", warnings)
    growing_path = _write_table(growing_rows, interim_dir / "remote_sensing_growing_season.parquet", warnings)
    return {"monthly": monthly_path, "growing_season": growing_path}


def _write_table(
    rows: list[dict[str, Any]],
    parquet_path: Path,
    warnings: list[str] | None,
) -> Path:
    """Write rows to Parquet, falling back to CSV on optional dependency errors."""

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd

        pd.DataFrame(rows, columns=REMOTE_SENSING_ROW_COLUMNS).to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception as exc:  # noqa: BLE001 - CSV fallback keeps the pipeline runnable
        csv_path = parquet_path.with_suffix(".csv")
        _write_csv_rows(rows, REMOTE_SENSING_ROW_COLUMNS, csv_path)
        if warnings is not None:
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


def _write_remote_sensing_report(
    result: RemoteSensingPreprocessResult,
    study_bbox: list[float],
    baseline_years: tuple[int, int],
    rice_growth_months: list[int],
    crs_wgs84: str,
    crs_equal_area: str,
) -> None:
    """Write a Markdown remote-sensing preprocessing summary."""

    lines = [
        "# Remote Sensing Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Candidate files: {result.file_count}",
        f"- Processed files: {len(result.processed_files)}",
        f"- Study bbox: {study_bbox}",
        f"- Study CRS: {crs_wgs84}",
        f"- Area/statistics CRS: {crs_equal_area}",
        f"- Baseline years: {baseline_years[0]}-{baseline_years[1]}",
        f"- Rice growth months: {rice_growth_months}",
        "",
        "Outputs use long format with `source_file`, `product`, `variable`, `year`, `month`, and `value`.",
        "",
    ]

    if result.status == "missing":
        lines.extend(["No remote sensing files found.", ""])

    if result.outputs:
        lines.extend(["## Outputs", ""])
        lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
        lines.append("")

    if result.metadata:
        lines.extend(["## Metadata", ""])
        for record in result.metadata:
            lines.extend(
                [
                    f"- `{record['source_file']}`",
                    f"  - format: {record.get('format')}",
                    f"  - product: {record.get('product')}",
                    f"  - variables: {record.get('variables')}",
                    f"  - dimensions: {record.get('dimensions')}",
                    f"  - crs: {record.get('crs')}",
                ]
            )
            if record.get("metadata_warning"):
                lines.append(f"  - warning: {record.get('metadata_warning')}")
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
    return _unique_paths(files)


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


def _scale_values(
    values: Any,
    scale: float,
    offset: float,
    min_raw: float,
    max_raw: float,
    round_digits: int = 6,
) -> Any:
    """Apply a MODIS scale to scalars, lists, tuples, or array-like objects."""

    converter = lambda value: round((value * scale) + offset, round_digits)
    validator = lambda value: min_raw <= value <= max_raw

    if isinstance(values, list):
        return [_scale_scalar(value, converter, validator) for value in values]
    if isinstance(values, tuple):
        return tuple(_scale_scalar(value, converter, validator) for value in values)

    array_result = _try_scale_array(values, scale, offset, min_raw, max_raw)
    if array_result is not None:
        return array_result

    return _scale_scalar(values, converter, validator)


def _scale_scalar(
    value: Any,
    converter: Callable[[float], float],
    validator: Callable[[float], bool],
) -> float | None:
    """Scale one numeric value, returning None for fill/invalid values."""

    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None

    if raw != raw or raw in {-32768.0, -9999.0} or not validator(raw):
        return None
    return converter(raw)


def _try_scale_array(values: Any, scale: float, offset: float, min_raw: float, max_raw: float) -> Any | None:
    """Scale array-like values when numpy or xarray-like APIs are available."""

    if hasattr(values, "where") and hasattr(values, "astype"):
        raw = values.astype("float64")
        valid = (raw >= min_raw) & (raw <= max_raw) & (raw != -32768.0) & (raw != -9999.0)
        return ((raw * scale) + offset).where(valid)

    try:
        import numpy as np
    except ImportError:
        return None

    try:
        array = np.asarray(values, dtype=float)
    except (TypeError, ValueError):
        return None

    if array.shape == ():
        return None

    valid = (array >= min_raw) & (array <= max_raw) & (array != -32768.0) & (array != -9999.0)
    scaled = (array * scale) + offset
    return np.where(valid, scaled, np.nan)


def _identify_product_from_filename(file_name: str | Path | None) -> str | None:
    """Identify product from filename text."""

    if file_name is None:
        return None
    normalized = _normalize_name(Path(str(file_name)).name)
    for product, candidates in PRODUCT_FILENAME_CANDIDATES.items():
        if any(_normalize_name(candidate) in normalized for candidate in candidates):
            return product
    return None


def _normalize_name(name: str) -> str:
    """Normalize names for coarse matching."""

    return str(name).strip().lower().replace("_", "").replace("-", "").replace(" ", "")


def _collect_hdf_dataset_names(dataset: Any, limit: int = 100) -> list[str]:
    """Collect a bounded list of HDF dataset names."""

    names: list[str] = []

    def visitor(name: str, obj: Any) -> None:
        if len(names) >= limit:
            return
        if hasattr(obj, "shape"):
            names.append(str(name).split("/")[-1])

    dataset.visititems(visitor)
    return names


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
    """Convert scalar-like values to float while preserving missing values."""

    try:
        result = float(value)
    except Exception:
        return None
    if result != result:
        return None
    return result


def _extract_xarray_crs(dataset: Any) -> str | None:
    """Extract CRS metadata from common xarray attributes."""

    for attr_name in ("crs", "spatial_ref"):
        value = dataset.attrs.get(attr_name)
        if value:
            return str(value)
    for variable_name in ("crs", "spatial_ref"):
        if variable_name in dataset:
            value = dataset[variable_name].attrs.get("crs_wkt") or dataset[variable_name].attrs.get("spatial_ref")
            if value:
                return str(value)
    return None


def _extract_hdf_crs(dataset: Any) -> str | None:
    """Extract CRS metadata from common HDF root attributes."""

    for attr_name in ("crs", "spatial_ref", "projection"):
        value = dataset.attrs.get(attr_name)
        if value is not None:
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="replace")
            return str(value)
    return None


def _import_xarray() -> Any:
    """Import xarray lazily."""

    try:
        import xarray as xr
    except ImportError as exc:
        raise ImportError("xarray is required for NetCDF remote-sensing metadata") from exc
    return xr


def _import_rasterio() -> Any:
    """Import rasterio lazily."""

    try:
        import rasterio
    except ImportError as exc:
        raise ImportError("rasterio is required for GeoTIFF remote-sensing metadata") from exc
    return rasterio


def _import_h5py() -> Any:
    """Import h5py lazily."""

    try:
        import h5py
    except ImportError as exc:
        raise ImportError("h5py is required for HDF/H5 remote-sensing metadata") from exc
    return h5py
