"""Spatial processing utilities for administrative units and rasters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


SUPPORTED_BOUNDARY_SUFFIXES = (".gpkg", ".geojson", ".shp", ".parquet")
BOUNDARY_SUFFIX_PRIORITY = {suffix: index for index, suffix in enumerate(SUPPORTED_BOUNDARY_SUFFIXES)}

ADMIN_FIELD_CANDIDATES: dict[str, list[str]] = {
    "province": ["province", "prov", "省", "省份", "NAME_1"],
    "prefecture": ["city", "prefecture", "市", "地级市", "NAME_2"],
    "county": ["county", "district", "县", "区县", "NAME_3"],
    "code": ["code", "adcode", "gbcode", "行政区划代码"],
}


@dataclass(frozen=True)
class BoundaryPreparationResult:
    """Result metadata for administrative-boundary preparation."""

    status: str
    source_path: Path | None
    feature_count: int
    field_mapping: dict[str, str]
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/boundary_summary.md")
    crs: str | None = None
    bounds: list[float] | None = None


def find_boundary_files(boundary_dir: str | Path) -> list[Path]:
    """Find supported boundary files under a boundary directory."""

    root = Path(boundary_dir).expanduser().resolve()
    if not root.exists():
        return []

    files = [
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_BOUNDARY_SUFFIXES
    ]
    return sorted(files, key=lambda path: (BOUNDARY_SUFFIX_PRIORITY[path.suffix.lower()], str(path)))


def identify_admin_fields(columns: list[str] | Any) -> dict[str, str]:
    """Identify province, prefecture, county, and code fields from column names."""

    column_list = [str(column) for column in columns]
    normalized = {_normalize_field_name(column): column for column in column_list}
    mapping: dict[str, str] = {}

    for role, candidates in ADMIN_FIELD_CANDIDATES.items():
        for candidate in candidates:
            matched = normalized.get(_normalize_field_name(candidate))
            if matched is not None:
                mapping[role] = matched
                break

    return mapping


def prepare_boundaries(
    boundary_dir: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    study_bbox: list[float],
    crs_wgs84: str,
    crs_equal_area: str,
) -> BoundaryPreparationResult:
    """Prepare administrative units from raw boundary files."""

    boundary_root = Path(boundary_dir).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    report_path = reports / "boundary_summary.md"

    boundary_files = find_boundary_files(boundary_root)
    if not boundary_files:
        warnings = [f"No boundary files found under {boundary_root}."]
        result = BoundaryPreparationResult(
            status="missing",
            source_path=None,
            feature_count=0,
            field_mapping={},
            warnings=warnings,
            report_path=report_path,
        )
        _write_boundary_report(result, study_bbox, crs_wgs84, crs_equal_area, [])
        return result

    warnings: list[str] = []
    last_error = ""
    for source_path in boundary_files:
        try:
            frame = _read_boundary_file(source_path)
            result = _prepare_boundary_frame(
                frame=frame,
                source_path=source_path,
                processed_dir=processed,
                reports_dir=reports,
                report_path=report_path,
                study_bbox=study_bbox,
                crs_wgs84=crs_wgs84,
                crs_equal_area=crs_equal_area,
                warnings=warnings,
            )
            _write_boundary_report(result, study_bbox, crs_wgs84, crs_equal_area, boundary_files)
            return result
        except Exception as exc:  # noqa: BLE001 - try all candidate files and report failures
            last_error = f"{source_path}: {type(exc).__name__}: {exc}"
            warnings.append(last_error)

    result = BoundaryPreparationResult(
        status="error",
        source_path=None,
        feature_count=0,
        field_mapping={},
        warnings=warnings or [last_error],
        report_path=report_path,
    )
    _write_boundary_report(result, study_bbox, crs_wgs84, crs_equal_area, boundary_files)
    return result


def _prepare_boundary_frame(
    frame: Any,
    source_path: Path,
    processed_dir: Path,
    reports_dir: Path,
    report_path: Path,
    study_bbox: list[float],
    crs_wgs84: str,
    crs_equal_area: str,
    warnings: list[str],
) -> BoundaryPreparationResult:
    """Clean, clip, and write one successfully loaded boundary frame."""

    del reports_dir
    gpd = _import_geopandas()
    box = _import_box()

    if frame.empty:
        warnings.append(f"Boundary file is empty: {source_path}")

    source_crs = str(frame.crs) if frame.crs else None
    if frame.crs is None:
        warnings.append(f"Boundary file has no CRS; assuming {crs_wgs84}: {source_path}")
        frame = frame.set_crs(crs_wgs84)

    frame = frame.to_crs(crs_wgs84)
    frame = _repair_geometries(frame)
    frame = frame[frame.geometry.notna() & ~frame.geometry.is_empty].copy()

    bbox_geom = box(*[float(value) for value in study_bbox])
    frame = frame[frame.geometry.intersects(bbox_geom)].copy()
    if not frame.empty:
        frame.geometry = frame.geometry.intersection(bbox_geom)
        frame = frame[frame.geometry.notna() & ~frame.geometry.is_empty].copy()

    field_mapping = identify_admin_fields([column for column in frame.columns if column != "geometry"])
    frame = _add_standard_admin_columns(frame, field_mapping)

    outputs: dict[str, Path] = {}
    gpkg_path = processed_dir / "admin_units.gpkg"
    parquet_path = processed_dir / "admin_units.parquet"
    equal_area_path = processed_dir / "admin_units_equal_area.gpkg"

    _try_write(lambda: frame.to_file(gpkg_path, driver="GPKG"), gpkg_path, outputs, "gpkg", warnings)
    _try_write(lambda: frame.to_parquet(parquet_path, index=False), parquet_path, outputs, "parquet", warnings)

    equal_area = gpd.GeoDataFrame(frame.copy(), geometry="geometry", crs=crs_wgs84).to_crs(crs_equal_area)
    _try_write(
        lambda: equal_area.to_file(equal_area_path, driver="GPKG"),
        equal_area_path,
        outputs,
        "equal_area_gpkg",
        warnings,
    )

    bounds = [float(value) for value in frame.total_bounds] if not frame.empty else None
    crs = str(frame.crs) if frame.crs else source_crs
    status = "ok" if len(frame) > 0 else "empty_after_clip"
    if status != "ok":
        warnings.append("Boundary data loaded, but no features remain after bbox clipping.")

    return BoundaryPreparationResult(
        status=status,
        source_path=source_path,
        feature_count=int(len(frame)),
        field_mapping=field_mapping,
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
        crs=crs,
        bounds=bounds,
    )


def _read_boundary_file(path: Path) -> Any:
    """Read a supported boundary file with geopandas."""

    gpd = _import_geopandas()
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return gpd.read_parquet(path)
    return gpd.read_file(path)


def _repair_geometries(frame: Any) -> Any:
    """Repair invalid geometries using available GeoPandas/Shapely methods."""

    invalid = ~frame.geometry.is_valid
    if invalid.any():
        geometry = frame.geometry
        if hasattr(geometry, "make_valid"):
            frame.geometry = geometry.make_valid()
        else:
            frame.geometry = geometry.buffer(0)
    return frame


def _add_standard_admin_columns(frame: Any, field_mapping: dict[str, str]) -> Any:
    """Add normalized admin columns for downstream panel joins."""

    frame = frame.copy()
    if "admin_id" not in frame.columns:
        code_field = field_mapping.get("code")
        if code_field:
            frame["admin_id"] = frame[code_field].astype(str).str.strip()
        else:
            frame["admin_id"] = [f"admin_{index + 1:06d}" for index in range(len(frame))]

    for role, output_column in (
        ("province", "province_name"),
        ("prefecture", "prefecture_name"),
        ("county", "county_name"),
    ):
        source_column = field_mapping.get(role)
        if source_column and output_column not in frame.columns:
            frame[output_column] = frame[source_column].astype(str).str.strip()

    return frame


def _try_write(
    writer: Any,
    output_path: Path,
    outputs: dict[str, Path],
    key: str,
    warnings: list[str],
) -> None:
    """Write an output file and record warning instead of aborting on failure."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        writer()
        outputs[key] = output_path
    except Exception as exc:  # noqa: BLE001 - optional formats may lack local engines
        warnings.append(f"Failed to write {output_path}: {type(exc).__name__}: {exc}")


def _write_boundary_report(
    result: BoundaryPreparationResult,
    study_bbox: list[float],
    crs_wgs84: str,
    crs_equal_area: str,
    candidates: list[Path],
) -> None:
    """Write a Markdown summary for boundary preparation."""

    lines = [
        "# Boundary Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Source path: `{result.source_path}`" if result.source_path else "- Source path: not available",
        f"- Candidate files: {len(candidates)}",
        f"- Target CRS: `{crs_wgs84}`",
        f"- Equal-area CRS: `{crs_equal_area}`",
        f"- Study bbox: {study_bbox}",
        f"- Feature count after clipping: {result.feature_count}",
        f"- Output CRS: `{result.crs}`" if result.crs else "- Output CRS: not available",
        f"- Bounds: {result.bounds}" if result.bounds else "- Bounds: not available",
        "",
    ]

    if not candidates:
        lines.extend(["No boundary files found.", ""])

    if result.field_mapping:
        lines.extend(["## Field Mapping", "", "| role | source field |", "| --- | --- |"])
        for role, column in result.field_mapping.items():
            lines.append(f"| {role} | {column} |")
        lines.append("")

    if result.outputs:
        lines.extend(["## Outputs", ""])
        lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
        lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _normalize_field_name(name: str) -> str:
    """Normalize field names for candidate matching."""

    return (
        str(name)
        .strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )


def _import_geopandas() -> Any:
    """Import geopandas lazily so non-spatial tests can run without it."""

    try:
        import geopandas as gpd
    except ImportError as exc:
        raise ImportError("geopandas is required for boundary file processing") from exc
    return gpd


def _import_box() -> Any:
    """Import shapely.box lazily."""

    try:
        from shapely.geometry import box
    except ImportError as exc:
        raise ImportError("shapely is required for boundary clipping") from exc
    return box
