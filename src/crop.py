"""Crop mask and phenology utilities."""

from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator


SUPPORTED_CROP_MASK_SUFFIXES = (
    ".tif",
    ".tiff",
    ".nc",
    ".hdf",
    ".h5",
    ".gpkg",
    ".geojson",
    ".shp",
    ".parquet",
)
SUPPORTED_PHENOLOGY_SUFFIXES = (".csv", ".xlsx", ".json", ".parquet", ".tif", ".tiff")
CROP_MASK_SUFFIX_PRIORITY = {
    suffix: index for index, suffix in enumerate(SUPPORTED_CROP_MASK_SUFFIXES)
}
PHENOLOGY_SUFFIX_PRIORITY = {
    suffix: index for index, suffix in enumerate(SUPPORTED_PHENOLOGY_SUFFIXES)
}

CROP_SUMMARY_COLUMNS = [
    "admin_id",
    "admin_name",
    "target_year",
    "crop_area_ha",
    "crop_fraction",
    "source_file",
    "status",
]
PHENOLOGY_COLUMNS = [
    "admin_id",
    "admin_name",
    "target_year",
    "start_month",
    "end_month",
    "months",
    "source",
    "status",
]
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")


@dataclass(frozen=True)
class CropFileDiscovery:
    """Supported crop-mask and phenology files found in raw directories."""

    crop_mask_files: list[Path]
    phenology_files: list[Path]

    @property
    def mask_files(self) -> list[Path]:
        """Alias for crop-mask files."""

        return self.crop_mask_files

    def __iter__(self) -> Iterator[list[Path]]:
        """Allow tuple unpacking as mask_files, phenology_files."""

        yield self.crop_mask_files
        yield self.phenology_files

    def __getitem__(self, key: str) -> list[Path]:
        """Allow dictionary-style access for simple callers."""

        aliases = {
            "crop_mask": self.crop_mask_files,
            "crop_mask_files": self.crop_mask_files,
            "mask_files": self.crop_mask_files,
            "phenology": self.phenology_files,
            "phenology_files": self.phenology_files,
        }
        try:
            return aliases[key]
        except KeyError as exc:
            raise KeyError(f"Unknown crop file discovery key: {key}") from exc


@dataclass(frozen=True)
class CropMaskPhenologyResult:
    """Result metadata for crop-mask and phenology preparation."""

    status: str
    crop_mask_files: list[Path]
    phenology_files: list[Path]
    selected_mask_path: Path | None
    default_phenology_window: dict[str, Any]
    selected_mask_paths: list[Path] = field(default_factory=list)
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/crop_mask_phenology_summary.md")


def find_crop_files(
    crop_mask_dir: str | Path,
    phenology_dir: str | Path,
    external_crop_mask_files: Iterable[str | Path] | None = None,
    external_phenology_files: Iterable[str | Path] | None = None,
) -> CropFileDiscovery:
    """Find supported crop-mask and phenology files under raw-data directories."""

    crop_mask_root = Path(crop_mask_dir).expanduser().resolve()
    phenology_root = Path(phenology_dir).expanduser().resolve()

    crop_mask_files = _find_supported_files(
        crop_mask_root,
        SUPPORTED_CROP_MASK_SUFFIXES,
        CROP_MASK_SUFFIX_PRIORITY,
    )
    if not crop_mask_files:
        crop_mask_files.extend(
            _supported_external_files(
                external_crop_mask_files,
                SUPPORTED_CROP_MASK_SUFFIXES,
                CROP_MASK_SUFFIX_PRIORITY,
            )
        )
    phenology_files = _find_supported_files(
        phenology_root,
        SUPPORTED_PHENOLOGY_SUFFIXES,
        PHENOLOGY_SUFFIX_PRIORITY,
    )
    if not phenology_files:
        phenology_files.extend(
            _supported_external_files(
                external_phenology_files,
                SUPPORTED_PHENOLOGY_SUFFIXES,
                PHENOLOGY_SUFFIX_PRIORITY,
            )
        )
    return CropFileDiscovery(
        crop_mask_files=_sort_unique_files(crop_mask_files, CROP_MASK_SUFFIX_PRIORITY),
        phenology_files=_sort_unique_files(phenology_files, PHENOLOGY_SUFFIX_PRIORITY),
    )


def select_mask_for_year(files: list[Path] | Iterable[Path], target_year: int) -> Path | None:
    """Select the mask file with the filename year closest to the target year."""

    file_list = list(files)
    if not file_list:
        return None

    candidates: list[tuple[int, int, int, str, Path]] = []
    for path in file_list:
        years = _extract_filename_years(path)
        for year in years:
            candidates.append((_mask_source_rank(path), abs(year - int(target_year)), year, str(path), path))

    if not candidates:
        return file_list[0]

    return min(candidates)[4]


def select_masks_for_year(files: list[Path] | Iterable[Path], target_year: int) -> list[Path]:
    """Select all same-priority mask files nearest to the target year."""

    file_list = list(files)
    if not file_list:
        return []

    candidates: list[tuple[int, int, int, str, Path]] = []
    for path in file_list:
        years = _extract_filename_years(path)
        for year in years:
            candidates.append((_mask_source_rank(path), abs(year - int(target_year)), year, str(path), path))

    if not candidates:
        return [file_list[0]]

    best_rank, best_distance = min((rank, distance) for rank, distance, *_ in candidates)
    selected = [
        path
        for rank, distance, _year, _name, path in candidates
        if rank == best_rank and distance == best_distance
    ]
    return _sort_unique_files(selected, CROP_MASK_SUFFIX_PRIORITY)


def default_phenology(months: Iterable[int] | None = None) -> dict[str, Any]:
    """Return the default rice growing-season phenology window."""

    month_values = _normalize_months(months)
    if not month_values:
        month_values = [6, 7, 8, 9]

    start_month = min(month_values)
    end_month = max(month_values)
    return {
        "source": "default",
        "months": month_values,
        "start_month": start_month,
        "end_month": end_month,
        "windows": [
            {
                "stage": "rice_growth_window",
                "start_month": start_month,
                "end_month": end_month,
                "months": month_values,
            }
        ],
    }


def aggregate_crop_masks_to_admin_units(
    mask_paths: Iterable[str | Path],
    admin_units_path: str | Path,
    target_year: int,
    crs_equal_area: str,
    max_cells_per_raster: int = 3_000_000,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Aggregate binary crop-mask rasters to administrative units."""

    warnings: list[str] = []
    gpd = _import_geopandas()
    admin_path = Path(admin_units_path).expanduser().resolve()
    if not admin_path.exists():
        return [], [f"Administrative units file not found for crop-mask aggregation: {admin_path}"]

    try:
        admin_frame = _read_admin_frame(admin_path)
    except Exception as exc:  # noqa: BLE001 - caller can fall back to metadata rows
        return [], [f"Could not read admin units for crop-mask aggregation: {type(exc).__name__}: {exc}"]

    if admin_frame.empty:
        return [], ["Administrative units file is empty for crop-mask aggregation."]
    if admin_frame.crs is None:
        warnings.append("Administrative units have no CRS for crop-mask aggregation; assuming EPSG:4326.")
        admin_frame = admin_frame.set_crs("EPSG:4326")

    admin_frame = admin_frame.copy()
    if "admin_id" not in admin_frame.columns:
        admin_frame["admin_id"] = [f"admin_{index + 1:06d}" for index in range(len(admin_frame))]
    name_column = _first_existing_column(
        admin_frame.columns,
        ["admin_name", "shapeName", "county_name", "county", "prefecture_name", "NAME_3"],
    )

    admin_area_ha = _admin_area_ha(admin_frame, crs_equal_area, warnings)
    crop_area_by_admin = {str(row.get("admin_id")): 0.0 for _, row in admin_frame.iterrows()}
    overlap_by_admin: set[str] = set()
    sources_by_admin: dict[str, set[str]] = {admin_id: set() for admin_id in crop_area_by_admin}

    for raw_path in mask_paths:
        path = Path(raw_path).expanduser().resolve()
        if path.suffix.lower() not in {".tif", ".tiff"}:
            warnings.append(f"Skipped unsupported crop-mask format for zonal stats: {path}")
            continue
        if not path.exists():
            warnings.append(f"Crop-mask raster not found: {path}")
            continue
        try:
            raster_crop_area, raster_overlap = _aggregate_one_binary_raster(
                raster_path=path,
                admin_frame=admin_frame,
                max_cells=max_cells_per_raster,
            )
        except Exception as exc:  # noqa: BLE001 - one bad mask must not stop the module
            warnings.append(f"Could not aggregate crop mask {path}: {type(exc).__name__}: {exc}")
            continue
        overlap_by_admin.update(raster_overlap)
        for admin_id, crop_area_ha in raster_crop_area.items():
            crop_area_by_admin[admin_id] = crop_area_by_admin.get(admin_id, 0.0) + crop_area_ha
            if crop_area_ha > 0:
                sources_by_admin.setdefault(admin_id, set()).add(path.name)

    rows: list[dict[str, Any]] = []
    for _, row in admin_frame.iterrows():
        admin_id = _clean_output_text(row.get("admin_id"))
        crop_area = crop_area_by_admin.get(admin_id, 0.0)
        total_area = admin_area_ha.get(admin_id)
        crop_fraction = ""
        if total_area and total_area > 0:
            crop_fraction = _round_number(max(0.0, min(1.0, crop_area / total_area)))
        status = "zonal_stats" if crop_area > 0 else "no_crop_observed" if admin_id in overlap_by_admin else "no_overlap"
        rows.append(
            {
                "admin_id": admin_id,
                "admin_name": _clean_output_text(row.get(name_column)) if name_column else "",
                "target_year": int(target_year),
                "crop_area_ha": _round_number(crop_area) if crop_area > 0 else 0.0,
                "crop_fraction": crop_fraction,
                "source_file": ";".join(sorted(sources_by_admin.get(admin_id, set()))),
                "status": status,
            }
        )

    if not any(_as_float(row["crop_area_ha"]) and _as_float(row["crop_area_ha"]) > 0 for row in rows):
        warnings.append("Crop-mask zonal stats completed, but no positive crop pixels were found in admin units.")
    del gpd
    return rows, warnings


def aggregate_phenology_to_admin_units(
    phenology_paths: Iterable[str | Path],
    admin_units_path: str | Path,
    target_year: int,
    default_months: Iterable[int],
    max_cells_per_raster: int = 3_000_000,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Aggregate phenology DOY rasters to admin-level month windows."""

    warnings: list[str] = []
    admin_path = Path(admin_units_path).expanduser().resolve()
    if not admin_path.exists():
        return [], [f"Administrative units file not found for phenology aggregation: {admin_path}"]
    try:
        admin_frame = _read_admin_frame(admin_path)
    except Exception as exc:  # noqa: BLE001 - caller can fall back to default phenology
        return [], [f"Could not read admin units for phenology aggregation: {type(exc).__name__}: {exc}"]
    if admin_frame.empty:
        return [], ["Administrative units file is empty for phenology aggregation."]
    if admin_frame.crs is None:
        warnings.append("Administrative units have no CRS for phenology aggregation; assuming EPSG:4326.")
        admin_frame = admin_frame.set_crs("EPSG:4326")
    if "admin_id" not in admin_frame.columns:
        admin_frame = admin_frame.copy()
        admin_frame["admin_id"] = [f"admin_{index + 1:06d}" for index in range(len(admin_frame))]

    selected_files = _select_phenology_stage_files(phenology_paths, target_year)
    if not selected_files:
        return [], ["No phenology rasters matched the target year and required stages."]

    stage_values: dict[str, dict[str, list[float]]] = {}
    sources: dict[str, set[str]] = {}
    for stage, path in selected_files:
        try:
            means = _aggregate_one_numeric_raster_mean(path, admin_frame, max_cells_per_raster)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Could not aggregate phenology raster {path}: {type(exc).__name__}: {exc}")
            continue
        for admin_id, value in means.items():
            stage_values.setdefault(admin_id, {}).setdefault(stage, []).append(value)
            sources.setdefault(admin_id, set()).add(path.name)

    name_column = _first_existing_column(
        admin_frame.columns,
        ["admin_name", "shapeName", "county_name", "county", "prefecture_name", "NAME_3"],
    )
    default_window = default_phenology(default_months)
    rows: list[dict[str, Any]] = []
    for _, row in admin_frame.iterrows():
        admin_id = _clean_output_text(row.get("admin_id"))
        values = stage_values.get(admin_id, {})
        transplanting = values.get("transplanting", [])
        maturity = values.get("maturity", [])
        if transplanting and maturity:
            start_month = _doy_to_month(min(transplanting), int(target_year))
            end_month = _doy_to_month(max(maturity), int(target_year))
            if end_month < start_month:
                start_month = int(default_window["start_month"])
                end_month = int(default_window["end_month"])
            months = list(range(start_month, end_month + 1))
            source = ";".join(sorted(sources.get(admin_id, set())))
            status = "zonal_stats"
        else:
            start_month = int(default_window["start_month"])
            end_month = int(default_window["end_month"])
            months = list(default_window["months"])
            source = default_window["source"]
            status = "default"
        rows.append(
            {
                "admin_id": admin_id,
                "admin_name": _clean_output_text(row.get(name_column)) if name_column else "",
                "target_year": int(target_year),
                "start_month": start_month,
                "end_month": end_month,
                "months": ",".join(str(month) for month in months),
                "source": source,
                "status": status,
            }
        )
    return rows, warnings


def prepare_crop_mask_phenology(
    crop_mask_dir: str | Path,
    phenology_dir: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    study_bbox: list[float],
    target_year: int,
    rice_growth_months: list[int],
    crs_wgs84: str = "EPSG:4326",
    crs_equal_area: str = "EPSG:6933",
    external_crop_mask_files: Iterable[str | Path] | None = None,
    external_phenology_files: Iterable[str | Path] | None = None,
    admin_units_path: str | Path | None = None,
) -> CropMaskPhenologyResult:
    """Prepare crop-mask and phenology MVP outputs.

    The MVP intentionally avoids raster/vector overlay work. It records candidate
    file metadata, writes empty admin-level fallback tables, and reports warnings.
    """

    crop_mask_root = Path(crop_mask_dir).expanduser().resolve()
    phenology_root = Path(phenology_dir).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    report_path = reports / "crop_mask_phenology_summary.md"

    discovery = find_crop_files(
        crop_mask_root,
        phenology_root,
        external_crop_mask_files=external_crop_mask_files,
        external_phenology_files=external_phenology_files,
    )
    warnings: list[str] = []

    if not discovery.crop_mask_files:
        warnings.append(f"No crop mask files found under {crop_mask_root}.")
    if not discovery.phenology_files:
        warnings.append(f"No phenology files found under {phenology_root}; using default months.")

    selected_mask_path = select_mask_for_year(discovery.crop_mask_files, target_year)
    if discovery.crop_mask_files and _all_files_lack_filename_years(discovery.crop_mask_files):
        warnings.append("No year found in crop mask filenames; selected the first discovered mask.")

    selected_mask_paths = select_masks_for_year(discovery.crop_mask_files, target_year)
    if discovery.phenology_files:
        warnings.append("Phenology rasters will be aggregated when matching county-level DOY files are available.")

    admin_rows = _read_admin_units(admin_units_path, processed, warnings)
    crop_rows: list[dict[str, Any]] | None = None
    if selected_mask_paths and admin_units_path is not None:
        crop_rows, crop_warnings = aggregate_crop_masks_to_admin_units(
            mask_paths=selected_mask_paths,
            admin_units_path=admin_units_path,
            target_year=target_year,
            crs_equal_area=crs_equal_area,
        )
        warnings.extend(crop_warnings)
        if not crop_rows:
            crop_rows = None
            warnings.append("Crop mask zonal aggregation produced no rows; using metadata fallback.")
    elif discovery.crop_mask_files:
        warnings.append(
            "Crop mask processing is metadata-only because administrative units were unavailable."
        )

    phenology_rows: list[dict[str, Any]] | None = None
    if discovery.phenology_files and admin_units_path is not None:
        phenology_rows, phenology_warnings = aggregate_phenology_to_admin_units(
            phenology_paths=discovery.phenology_files,
            admin_units_path=admin_units_path,
            target_year=target_year,
            default_months=rice_growth_months,
        )
        warnings.extend(phenology_warnings)
        if not phenology_rows:
            phenology_rows = None
            warnings.append("Phenology aggregation produced no rows; using default month fallback.")

    outputs = _write_outputs(
        processed,
        admin_rows,
        target_year,
        rice_growth_months,
        selected_mask_path,
        crop_rows,
        phenology_rows,
    )
    phenology_window = default_phenology(rice_growth_months)
    has_crop_stats = bool(crop_rows and any(row.get("status") == "zonal_stats" for row in crop_rows))
    has_phenology_stats = bool(phenology_rows and any(row.get("status") == "zonal_stats" for row in phenology_rows))
    if has_crop_stats or has_phenology_stats:
        status = "ok"
    else:
        status = "metadata_only" if discovery.crop_mask_files else "missing"

    result = CropMaskPhenologyResult(
        status=status,
        crop_mask_files=discovery.crop_mask_files,
        phenology_files=discovery.phenology_files,
        selected_mask_path=selected_mask_path,
        selected_mask_paths=selected_mask_paths,
        default_phenology_window=phenology_window,
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_crop_report(
        result=result,
        crop_mask_root=crop_mask_root,
        phenology_root=phenology_root,
        study_bbox=study_bbox,
        target_year=int(target_year),
        crs_wgs84=crs_wgs84,
        crs_equal_area=crs_equal_area,
    )
    return result


def _find_supported_files(root: Path, suffixes: tuple[str, ...], priority: dict[str, int]) -> list[Path]:
    """Find files with supported suffixes under a directory."""

    if not root.exists():
        return []

    files = [
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in suffixes
    ]
    return sorted(files, key=lambda path: (priority[path.suffix.lower()], str(path)))


def _supported_external_files(
    external_files: Iterable[str | Path] | None,
    suffixes: tuple[str, ...],
    priority: dict[str, int],
) -> list[Path]:
    """Normalize supported external reference paths."""

    if external_files is None:
        return []
    files: list[Path] = []
    for value in external_files:
        path = Path(value).expanduser()
        if path.is_file() and path.suffix.lower() in suffixes:
            files.append(path.resolve())
    return _sort_unique_files(files, priority)


def _sort_unique_files(files: Iterable[Path], priority: dict[str, int]) -> list[Path]:
    """Return unique supported files sorted by suffix priority and path."""

    seen: set[str] = set()
    unique: list[Path] = []
    for path in files:
        resolved = path.resolve()
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return sorted(unique, key=lambda path: (priority[path.suffix.lower()], str(path)))


def _extract_filename_years(path: Path) -> list[int]:
    """Extract plausible four-digit years from a filename."""

    return [int(value) for value in YEAR_PATTERN.findall(Path(path).name)]


def _mask_source_rank(path: Path) -> int:
    """Rank rice masks before generic cropland masks."""

    text = str(path).replace("\\", "/").lower()
    rice_keywords = ("rice", "single_season_rice", "middle_rice", "early_rice", "late_rice", "稻")
    cropland_keywords = ("clcd", "cropland", "worldcover", "landcover", "crop_land")
    if any(keyword in text for keyword in rice_keywords):
        return 0
    if any(keyword in text for keyword in cropland_keywords):
        return 1
    return 2


def _all_files_lack_filename_years(files: list[Path]) -> bool:
    """Return True when none of the candidate files has a filename year."""

    return all(not _extract_filename_years(path) for path in files)


def _normalize_months(months: Iterable[int] | None) -> list[int]:
    """Normalize month values while preserving first-seen order."""

    if months is None:
        return [6, 7, 8, 9]

    normalized: list[int] = []
    for value in months:
        month = int(value)
        if month < 1 or month > 12:
            raise ValueError(f"Invalid month value: {value}")
        if month not in normalized:
            normalized.append(month)
    return normalized


def _read_admin_units(
    admin_units_path: str | Path | None,
    processed_dir: Path,
    warnings: list[str],
) -> list[dict[str, str]]:
    """Read admin IDs and names for default crop and phenology outputs."""

    candidates: list[Path] = []
    if admin_units_path is not None:
        candidates.append(Path(admin_units_path).expanduser().resolve())
    else:
        candidates.extend(
            [
                processed_dir / "admin_units.parquet",
                processed_dir / "admin_units.gpkg",
                processed_dir / "admin_units.csv",
            ]
        )

    path = next((candidate for candidate in candidates if candidate.exists()), None)
    if path is None:
        return []

    try:
        if path.suffix.lower() == ".parquet":
            import pandas as pd

            frame = pd.read_parquet(path)
        elif path.suffix.lower() == ".csv":
            import pandas as pd

            frame = pd.read_csv(path)
        else:
            import geopandas as gpd

            frame = gpd.read_file(path)
    except Exception as exc:  # noqa: BLE001 - keep fallback outputs available
        warnings.append(f"Could not read admin units for crop defaults: {type(exc).__name__}: {exc}")
        return []

    if "admin_id" not in frame.columns:
        frame = frame.copy()
        frame["admin_id"] = [f"admin_{index + 1:06d}" for index in range(len(frame))]
    name_column = _first_existing_column(frame.columns, ["admin_name", "shapeName", "county", "prefecture", "NAME_3"])
    rows: list[dict[str, str]] = []
    for _, row in frame.iterrows():
        admin_id = _clean_output_text(row.get("admin_id"))
        if not admin_id:
            continue
        rows.append(
            {
                "admin_id": admin_id,
                "admin_name": _clean_output_text(row.get(name_column)) if name_column else "",
            }
        )
    return rows


def _write_outputs(
    processed_dir: Path,
    admin_rows: list[dict[str, str]],
    target_year: int,
    rice_growth_months: list[int],
    selected_mask_path: Path | None,
    crop_rows: list[dict[str, Any]] | None = None,
    phenology_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Path]:
    """Write admin-level crop and phenology fallback/default outputs."""

    crop_summary = processed_dir / "crop_mask_summary_by_admin.csv"
    phenology_summary = processed_dir / "phenology_by_admin.csv"
    if crop_rows is None:
        crop_rows = _default_crop_rows(admin_rows, target_year, selected_mask_path)
    if phenology_rows is None:
        phenology_rows = _default_phenology_rows(admin_rows, target_year, rice_growth_months)
    _write_csv_rows(crop_rows, CROP_SUMMARY_COLUMNS, crop_summary)
    _write_csv_rows(phenology_rows, PHENOLOGY_COLUMNS, phenology_summary)
    return {
        "crop_mask_summary_by_admin": crop_summary.resolve(),
        "phenology_by_admin": phenology_summary.resolve(),
    }


def _default_crop_rows(
    admin_rows: list[dict[str, str]],
    target_year: int,
    selected_mask_path: Path | None,
) -> list[dict[str, Any]]:
    """Build per-admin crop rows when spatial mask aggregation is unavailable."""

    if not admin_rows:
        return []
    status = "metadata_only" if selected_mask_path else "missing_mask"
    source_file = str(selected_mask_path) if selected_mask_path else ""
    return [
        {
            "admin_id": row["admin_id"],
            "admin_name": row["admin_name"],
            "target_year": int(target_year),
            "crop_area_ha": "",
            "crop_fraction": "",
            "source_file": source_file,
            "status": status,
        }
        for row in admin_rows
    ]


def _default_phenology_rows(
    admin_rows: list[dict[str, str]],
    target_year: int,
    rice_growth_months: list[int],
) -> list[dict[str, Any]]:
    """Build per-admin default phenology rows from configured growth months."""

    if not admin_rows:
        return []
    window = default_phenology(rice_growth_months)
    month_text = ",".join(str(month) for month in window["months"])
    return [
        {
            "admin_id": row["admin_id"],
            "admin_name": row["admin_name"],
            "target_year": int(target_year),
            "start_month": window["start_month"],
            "end_month": window["end_month"],
            "months": month_text,
            "source": window["source"],
            "status": "default",
        }
        for row in admin_rows
    ]


def _write_csv_rows(rows: list[dict[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write CSV rows with a stable header."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_crop_report(
    result: CropMaskPhenologyResult,
    crop_mask_root: Path,
    phenology_root: Path,
    study_bbox: list[float],
    target_year: int,
    crs_wgs84: str,
    crs_equal_area: str,
) -> None:
    """Write a Markdown summary for crop-mask and phenology preparation."""

    window = result.default_phenology_window
    selected = f"`{result.selected_mask_path}`" if result.selected_mask_path else "not available"
    selected_count = len(result.selected_mask_paths)
    lines = [
        "# Crop Mask and Phenology Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Crop mask directory: `{crop_mask_root}`",
        f"- Phenology directory: `{phenology_root}`",
        f"- Crop mask candidate files: {len(result.crop_mask_files)}",
        f"- Phenology candidate files: {len(result.phenology_files)}",
        f"- Selected mask: {selected}",
        f"- Selected mask count for aggregation: {selected_count}",
        f"- Target year: {target_year}",
        f"- Study bbox: {study_bbox}",
        f"- Default CRS: `{crs_wgs84}`",
        f"- Equal-area CRS for area statistics: `{crs_equal_area}`",
        f"- Default phenology months: {window['months']}",
        f"- Default phenology window: {window['start_month']}-{window['end_month']}",
        "",
        (
            "This MVP records file metadata, writes default admin phenology rows when admin units exist, "
            "and does not force raster/vector processing."
        ),
        "",
    ]

    if not result.crop_mask_files:
        lines.extend(["No crop mask files found.", ""])
    if not result.phenology_files:
        lines.extend(["No phenology files found; default growth months are used for reporting.", ""])

    _append_metadata_table(lines, "Crop Mask Candidates", result.crop_mask_files)
    _append_metadata_table(lines, "Phenology Candidates", result.phenology_files)

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


def _append_metadata_table(lines: list[str], title: str, files: list[Path]) -> None:
    """Append lightweight filesystem metadata to report lines."""

    if not files:
        return

    lines.extend(
        [
            f"## {title}",
            "",
            "| file | suffix | size_mb | modified_time | filename_years |",
            "| --- | --- | ---: | --- | --- |",
        ]
    )
    for path in files:
        metadata = _file_metadata(path)
        years = ", ".join(str(year) for year in _extract_filename_years(path)) or ""
        lines.append(
            (
                f"| `{path}` | {path.suffix.lower()} | {metadata['size_mb']} | "
                f"{metadata['modified_time']} | {years} |"
            )
        )
    lines.append("")


def _file_metadata(path: Path) -> dict[str, Any]:
    """Return basic filesystem metadata without opening heavy data formats."""

    try:
        stat = path.stat()
        return {
            "size_mb": round(stat.st_size / (1024 * 1024), 4),
            "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        }
    except OSError as exc:
        return {"size_mb": "", "modified_time": f"{type(exc).__name__}: {exc}"}


def _first_existing_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    """Return the first candidate column present in a collection."""

    column_set = {str(column): str(column) for column in columns}
    for candidate in candidates:
        if candidate in column_set:
            return column_set[candidate]
    return None


def _clean_output_text(value: Any) -> str:
    """Convert optional table values to stable text."""

    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _read_admin_frame(path: Path) -> Any:
    """Read administrative units into a GeoDataFrame."""

    gpd = _import_geopandas()
    if path.suffix.lower() == ".parquet":
        return gpd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        import pandas as pd
        from shapely import wkt

        frame = pd.read_csv(path)
        if "geometry" not in frame.columns:
            raise ValueError("CSV admin units require a geometry WKT column.")
        frame["geometry"] = frame["geometry"].apply(wkt.loads)
        return gpd.GeoDataFrame(frame, geometry="geometry", crs="EPSG:4326")
    return gpd.read_file(path)


def _admin_area_ha(frame: Any, crs_equal_area: str, warnings: list[str]) -> dict[str, float]:
    """Calculate administrative-unit area in hectares."""

    try:
        area_frame = frame.to_crs(crs_equal_area)
        return {
            _clean_output_text(row.get("admin_id")): float(row.geometry.area) / 10000.0
            for _, row in area_frame.iterrows()
        }
    except Exception as exc:  # noqa: BLE001 - fallback keeps crop aggregation available
        warnings.append(f"Could not calculate equal-area admin areas: {type(exc).__name__}: {exc}")
        return {_clean_output_text(row.get("admin_id")): 0.0 for _, row in frame.iterrows()}


def _aggregate_one_binary_raster(
    raster_path: Path,
    admin_frame: Any,
    max_cells: int,
) -> tuple[dict[str, float], set[str]]:
    """Aggregate one binary raster to admin IDs using a rasterized admin label grid."""

    import numpy as np
    import rasterio
    from affine import Affine
    from rasterio.enums import Resampling
    from rasterio.features import rasterize
    from rasterio.windows import Window, from_bounds
    from shapely.geometry import box

    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError("Crop-mask raster has no CRS.")
        admin_src = admin_frame.to_crs(src.crs)
        raster_box = box(*src.bounds)
        admin_src = admin_src[admin_src.geometry.notna() & admin_src.geometry.intersects(raster_box)].copy()
        if admin_src.empty:
            return {}, set()

        minx, miny, maxx, maxy = admin_src.total_bounds
        minx = max(float(minx), float(src.bounds.left))
        miny = max(float(miny), float(src.bounds.bottom))
        maxx = min(float(maxx), float(src.bounds.right))
        maxy = min(float(maxy), float(src.bounds.top))
        if minx >= maxx or miny >= maxy:
            return {}, set()

        window = from_bounds(minx, miny, maxx, maxy, src.transform).round_offsets().round_lengths()
        window = _clip_window_to_raster(window, src.width, src.height)
        if window.width <= 0 or window.height <= 0:
            return {}, set()

        scale = max(1.0, math.sqrt((window.width * window.height) / max(1, int(max_cells))))
        out_width = max(1, int(math.ceil(window.width / scale)))
        out_height = max(1, int(math.ceil(window.height / scale)))
        data = src.read(
            1,
            window=window,
            out_shape=(out_height, out_width),
            resampling=Resampling.nearest,
            masked=True,
        )
        array = np.asarray(data.filled(0) if hasattr(data, "filled") else data)
        transform = src.window_transform(window) * Affine.scale(window.width / out_width, window.height / out_height)

        features = [
            (geometry, index)
            for index, geometry in enumerate(admin_src.geometry, start=1)
            if geometry is not None and not geometry.is_empty
        ]
        if not features:
            return {}, set()
        label_grid = rasterize(
            features,
            out_shape=(out_height, out_width),
            transform=transform,
            fill=0,
            all_touched=True,
            dtype="int32",
        )

        crop_mask = np.isfinite(array.astype("float64")) & (array > 0) & (label_grid > 0)
        overlap_labels = np.unique(label_grid[label_grid > 0])
        overlap_ids = {
            _clean_output_text(admin_src.iloc[int(label) - 1].get("admin_id"))
            for label in overlap_labels
        }
        if not crop_mask.any():
            return {}, overlap_ids

        rows, _cols = np.where(crop_mask)
        labels = label_grid[crop_mask]
        weights = _pixel_area_weights_ha(transform, src.crs, rows)
        sums = np.bincount(labels.astype("int64"), weights=weights, minlength=len(admin_src) + 1)
        crop_area_by_admin: dict[str, float] = {}
        for label in np.nonzero(sums)[0]:
            if label == 0:
                continue
            admin_id = _clean_output_text(admin_src.iloc[int(label) - 1].get("admin_id"))
            crop_area_by_admin[admin_id] = float(sums[label])
        return crop_area_by_admin, overlap_ids


def _aggregate_one_numeric_raster_mean(
    raster_path: Path,
    admin_frame: Any,
    max_cells: int,
) -> dict[str, float]:
    """Aggregate one numeric raster to admin-level mean values."""

    import numpy as np
    import rasterio
    from affine import Affine
    from rasterio.enums import Resampling
    from rasterio.features import rasterize
    from rasterio.windows import from_bounds
    from shapely.geometry import box

    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError("Phenology raster has no CRS.")
        admin_src = admin_frame.to_crs(src.crs)
        raster_box = box(*src.bounds)
        admin_src = admin_src[admin_src.geometry.notna() & admin_src.geometry.intersects(raster_box)].copy()
        if admin_src.empty:
            return {}

        minx, miny, maxx, maxy = admin_src.total_bounds
        minx = max(float(minx), float(src.bounds.left))
        miny = max(float(miny), float(src.bounds.bottom))
        maxx = min(float(maxx), float(src.bounds.right))
        maxy = min(float(maxy), float(src.bounds.top))
        if minx >= maxx or miny >= maxy:
            return {}

        window = from_bounds(minx, miny, maxx, maxy, src.transform).round_offsets().round_lengths()
        window = _clip_window_to_raster(window, src.width, src.height)
        if window.width <= 0 or window.height <= 0:
            return {}

        scale = max(1.0, math.sqrt((window.width * window.height) / max(1, int(max_cells))))
        out_width = max(1, int(math.ceil(window.width / scale)))
        out_height = max(1, int(math.ceil(window.height / scale)))
        data = src.read(
            1,
            window=window,
            out_shape=(out_height, out_width),
            resampling=Resampling.average,
            masked=True,
        )
        array = np.asarray(data.filled(np.nan) if hasattr(data, "filled") else data, dtype="float64")
        transform = src.window_transform(window) * Affine.scale(window.width / out_width, window.height / out_height)

        features = [
            (geometry, index)
            for index, geometry in enumerate(admin_src.geometry, start=1)
            if geometry is not None and not geometry.is_empty
        ]
        if not features:
            return {}
        label_grid = rasterize(
            features,
            out_shape=(out_height, out_width),
            transform=transform,
            fill=0,
            all_touched=True,
            dtype="int32",
        )
        valid = np.isfinite(array) & (label_grid > 0)
        if src.nodata is not None and math.isfinite(float(src.nodata)):
            valid &= array != float(src.nodata)
        valid &= (array > 0) & (array <= 366)
        if not valid.any():
            return {}

        labels = label_grid[valid].astype("int64")
        values = array[valid]
        sums = np.bincount(labels, weights=values, minlength=len(admin_src) + 1)
        counts = np.bincount(labels, minlength=len(admin_src) + 1)
        means: dict[str, float] = {}
        for label in np.nonzero(counts)[0]:
            if label == 0:
                continue
            admin_id = _clean_output_text(admin_src.iloc[int(label) - 1].get("admin_id"))
            means[admin_id] = float(sums[label] / counts[label])
        return means


def _select_phenology_stage_files(
    files: Iterable[str | Path],
    target_year: int,
) -> list[tuple[str, Path]]:
    """Select target-year phenology rasters by stage, preferring county-level files."""

    candidates: dict[tuple[str, str], list[tuple[int, int, str, Path]]] = {}
    for raw_path in files:
        path = Path(raw_path).expanduser().resolve()
        if path.suffix.lower() not in {".tif", ".tiff"}:
            continue
        stage = _phenology_stage(path)
        rice_type = _rice_type(path)
        if stage is None or rice_type is None:
            continue
        years = _extract_filename_years(path)
        if len(years) >= 2:
            start_year, end_year = min(years), max(years)
        elif len(years) == 1:
            start_year = end_year = years[0]
        else:
            start_year = end_year = int(target_year)
        if not (start_year <= int(target_year) <= end_year):
            continue
        span = end_year - start_year
        level_rank = 0 if "county_level" in path.name.lower() else 1
        candidates.setdefault((rice_type, stage), []).append((level_rank, span, str(path), path))

    selected: list[tuple[str, Path]] = []
    for (_season, stage), values in candidates.items():
        selected.append((stage, min(values)[3]))
    return sorted(selected, key=lambda item: str(item[1]))


def _phenology_stage(path: Path) -> str | None:
    """Infer phenology stage from filename."""

    name = path.name.lower()
    if "transplant" in name:
        return "transplanting"
    if "maturity" in name:
        return "maturity"
    if "heading" in name:
        return "heading"
    return None


def _rice_type(path: Path) -> str | None:
    """Infer rice season type from filename."""

    name = path.name.lower()
    if "early_rice" in name:
        return "early"
    if "middle_rice" in name:
        return "middle"
    if "late_rice" in name:
        return "late"
    if "rice" in name:
        return "rice"
    return None


def _doy_to_month(day_of_year: float, year: int) -> int:
    """Convert day-of-year to calendar month."""

    day = max(1, min(366, int(round(float(day_of_year)))))
    date_value = datetime(int(year), 1, 1) + timedelta(days=day - 1)
    return int(date_value.month)


def _clip_window_to_raster(window: Any, width: int, height: int) -> Any:
    """Clip a rasterio window to raster dimensions."""

    from rasterio.windows import Window

    col_off = max(0, int(window.col_off))
    row_off = max(0, int(window.row_off))
    end_col = min(int(math.ceil(window.col_off + window.width)), int(width))
    end_row = min(int(math.ceil(window.row_off + window.height)), int(height))
    return Window(col_off, row_off, max(0, end_col - col_off), max(0, end_row - row_off))


def _pixel_area_weights_ha(transform: Any, crs: Any, rows: Any) -> Any:
    """Return hectare weights for raster rows under projected or geographic CRS."""

    import numpy as np

    x_size = abs(float(transform.a))
    y_size = abs(float(transform.e))
    if crs and not crs.is_geographic:
        return np.full(len(rows), x_size * y_size / 10000.0, dtype="float64")

    center_lats = float(transform.f) + (rows.astype("float64") + 0.5) * float(transform.e)
    meters_per_degree_lat = 111_320.0
    meters_per_degree_lon = 111_320.0 * np.cos(np.deg2rad(center_lats))
    return np.abs(x_size * meters_per_degree_lon * y_size * meters_per_degree_lat) / 10000.0


def _round_number(value: float) -> float:
    """Round numeric outputs for stable CSV values."""

    return round(float(value), 12)


def _as_float(value: Any) -> float | None:
    """Convert a value to float or return None."""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _import_geopandas() -> Any:
    """Import geopandas lazily."""

    try:
        import geopandas as gpd
    except ImportError as exc:
        raise ImportError("geopandas is required for crop-mask aggregation") from exc
    return gpd
