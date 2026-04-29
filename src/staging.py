"""Stage selected existing download-package files into project raw-data folders."""

from __future__ import annotations

import csv
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


BOUNDARY_FILES = [
    "geoBoundaries-CHN-ADM3_simplified.shp",
    "geoBoundaries-CHN-ADM3_simplified.dbf",
    "geoBoundaries-CHN-ADM3_simplified.shx",
    "geoBoundaries-CHN-ADM3_simplified.prj",
    "geoBoundaries-CHN-ADM3-metaData.json",
    "CITATION-AND-USE-geoBoundaries.txt",
]

STATISTICS_FILES = [
    "nbs_grain_province_2024_2025.csv",
    "nbs_grain_focus_provinces_2024_2025.csv",
]

EXTERNAL_INDEX_COLUMNS = [
    "category",
    "path",
    "relative_path",
    "file_name",
    "suffix",
    "size_mb",
    "modified_time",
    "source_group",
]
EXTERNAL_SCAN_ROOTS = ("downloads/raw", "downloads/clipped")
CLIMATE_SUFFIXES = {".nc", ".nc4", ".cdf"}
REMOTE_SENSING_SUFFIXES = {".tif", ".tiff", ".nc", ".nc4", ".hdf", ".h5"}
CROP_MASK_SUFFIXES = {".tif", ".tiff", ".nc", ".hdf", ".h5", ".gpkg", ".geojson", ".shp", ".parquet"}
PHENOLOGY_SUFFIXES = {".csv", ".xlsx", ".json", ".parquet", ".tif", ".tiff"}


@dataclass(frozen=True)
class StagingResult:
    """Result metadata for staging existing downloads."""

    status: str
    copied_count: int
    skipped_count: int
    copied_files: list[Path] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/data_staging_summary.md")
    external_index_count: int = 0
    external_index_csv: Path | None = None
    external_index_json: Path | None = None


@dataclass(frozen=True)
class ExternalDataIndexResult:
    """Result metadata for external large-data references."""

    status: str
    indexed_count: int
    csv_path: Path
    json_path: Path
    report_path: Path
    warnings: list[str] = field(default_factory=list)


def stage_existing_downloads(
    package_root: str | Path,
    project_root: str | Path,
    overwrite: bool = False,
) -> StagingResult:
    """Copy selected boundary/statistics files from the existing download package."""

    package = Path(package_root).expanduser().resolve()
    project = Path(project_root).expanduser().resolve()
    reports = project / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    report_path = reports / "data_staging_summary.md"

    if not package.exists():
        references = project / "data" / "raw" / "references"
        result = StagingResult(
            status="missing",
            copied_count=0,
            skipped_count=0,
            warnings=[f"Download package not found: {package}"],
            report_path=report_path,
            external_index_count=0,
            external_index_csv=references / "external_data_sources.csv",
            external_index_json=references / "external_data_sources.json",
        )
        _write_staging_report(result, package, project)
        return result

    copied: list[Path] = []
    skipped: list[str] = []
    warnings: list[str] = []

    _copy_known_files(
        source_dir=package / "downloads" / "raw" / "geoboundaries" / "CHN_ADM3",
        target_dir=project / "data" / "raw" / "boundary",
        filenames=BOUNDARY_FILES,
        copied=copied,
        skipped=skipped,
        overwrite=overwrite,
    )
    _copy_known_files(
        source_dir=package / "metadata" / "statistics",
        target_dir=project / "data" / "raw" / "statistics",
        filenames=STATISTICS_FILES,
        copied=copied,
        skipped=skipped,
        overwrite=overwrite,
    )
    index_result = build_external_data_index(package_root=package, project_root=project)
    warnings.extend(index_result.warnings)

    if not copied and index_result.indexed_count == 0:
        warnings.append("No staged files were copied; check whether known source files exist.")

    result = StagingResult(
        status="ok" if copied or index_result.indexed_count else "empty",
        copied_count=len(copied),
        skipped_count=len(skipped),
        copied_files=copied,
        skipped_files=skipped,
        warnings=warnings,
        report_path=report_path,
        external_index_count=index_result.indexed_count,
        external_index_csv=index_result.csv_path,
        external_index_json=index_result.json_path,
    )
    _write_staging_report(result, package, project)
    return result


def build_external_data_index(
    package_root: str | Path,
    project_root: str | Path,
) -> ExternalDataIndexResult:
    """Index large external files without copying them into the project."""

    package = Path(package_root).expanduser().resolve()
    project = Path(project_root).expanduser().resolve()
    references_dir = project / "data" / "raw" / "references"
    reports_dir = project / "reports"
    references_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    csv_path = references_dir / "external_data_sources.csv"
    json_path = references_dir / "external_data_sources.json"
    report_path = reports_dir / "external_data_index.md"

    warnings: list[str] = []
    records: list[dict[str, Any]] = []
    if not package.exists():
        warnings.append(f"Download package not found: {package}")
        _write_external_index_outputs(records, csv_path, json_path)
        result = ExternalDataIndexResult("missing", 0, csv_path, json_path, report_path, warnings)
        _write_external_index_report(result, package, records)
        return result

    for relative_root in EXTERNAL_SCAN_ROOTS:
        scan_root = package / Path(relative_root)
        if not scan_root.exists():
            warnings.append(f"External scan root not found: {scan_root}")
            continue
        for path in scan_root.rglob("*"):
            if not path.is_file():
                continue
            category = guess_external_data_category(path, package)
            if category is None:
                continue
            records.append(_external_index_record(path, package, category, relative_root))

    records = sorted(records, key=lambda item: (str(item["category"]), str(item["relative_path"])))
    _write_external_index_outputs(records, csv_path, json_path)
    status = "ok" if records else "empty"
    result = ExternalDataIndexResult(status, len(records), csv_path, json_path, report_path, warnings)
    _write_external_index_report(result, package, records)
    return result


def load_external_data_paths(project_root: str | Path, category: str) -> list[Path]:
    """Load external paths for one category from the generated index."""

    index_path = Path(project_root).expanduser().resolve() / "data" / "raw" / "references" / "external_data_sources.csv"
    if not index_path.exists():
        return []

    paths: list[Path] = []
    with index_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            if row.get("category") != category:
                continue
            path_text = row.get("path") or ""
            path = Path(path_text).expanduser()
            if path.exists():
                paths.append(path.resolve())
    return _unique_paths(paths)


def guess_external_data_category(path: str | Path, package_root: str | Path | None = None) -> str | None:
    """Guess the project raw-data category for an external file path."""

    file_path = Path(path)
    suffix = file_path.suffix.lower()
    text_path = _normalize_path_text(file_path)
    if package_root is not None:
        try:
            text_path = _normalize_path_text(file_path.resolve().relative_to(Path(package_root).resolve()))
        except ValueError:
            text_path = _normalize_path_text(file_path)

    if "chinaricecalendar" in text_path and suffix in PHENOLOGY_SUFFIXES:
        return "phenology"
    if (
        "china_single_season_rice" in text_path
        or "single_season_rice" in text_path
        or "/clcd/" in text_path
    ) and suffix in CROP_MASK_SUFFIXES:
        return "crop_mask"
    if any(keyword in text_path for keyword in ["era5_land", "cmfd", "chirps", "terraclimate"]) and suffix in CLIMATE_SUFFIXES:
        return "climate"
    if (
        any(
            keyword in text_path
            for keyword in ["nasa", "mod13q1", "myd13q1", "mod11a2", "mod16a2", "smap", "spl3smp", "grace", "gleam"]
        )
        and suffix in REMOTE_SENSING_SUFFIXES
    ):
        return "remote_sensing"
    return None


def _copy_known_files(
    source_dir: Path,
    target_dir: Path,
    filenames: list[str],
    copied: list[Path],
    skipped: list[str],
    overwrite: bool,
) -> None:
    """Copy known filenames from one directory into a target directory."""

    target_dir.mkdir(parents=True, exist_ok=True)
    for filename in filenames:
        source = source_dir / filename
        target = target_dir / filename
        if not source.exists():
            skipped.append(f"missing: {source}")
            continue
        if target.exists() and not overwrite:
            skipped.append(f"exists: {target}")
            continue
        shutil.copy2(source, target)
        copied.append(target)


def _external_index_record(path: Path, package_root: Path, category: str, source_group: str) -> dict[str, Any]:
    """Build one external-index CSV/JSON record."""

    stat = path.stat()
    try:
        relative_path = path.resolve().relative_to(package_root)
    except ValueError:
        relative_path = path.name
    return {
        "category": category,
        "path": str(path.resolve()),
        "relative_path": str(relative_path),
        "file_name": path.name,
        "suffix": path.suffix.lower(),
        "size_mb": round(stat.st_size / (1024 * 1024), 6),
        "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "source_group": source_group.replace("\\", "/"),
    }


def _write_external_index_outputs(records: list[dict[str, Any]], csv_path: Path, json_path: Path) -> None:
    """Write external index records to CSV and JSON."""

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=EXTERNAL_INDEX_COLUMNS)
        writer.writeheader()
        for record in records:
            writer.writerow({column: record.get(column, "") for column in EXTERNAL_INDEX_COLUMNS})

    json_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "record_count": len(records),
        "category_counts": _count_by_category(records),
        "records": records,
    }
    json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_external_index_report(
    result: ExternalDataIndexResult,
    package_root: Path,
    records: list[dict[str, Any]],
) -> None:
    """Write a Markdown report for external large-data references."""

    category_counts = _count_by_category(records)
    lines = [
        "# External Data Index",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Download package: `{package_root}`",
        f"- Indexed files: {result.indexed_count}",
        f"- CSV: `{result.csv_path}`",
        f"- JSON: `{result.json_path}`",
        "",
        "Large raster/NetCDF/HDF files are referenced by absolute path and are not copied into `data/raw`.",
        "",
        "## Category Counts",
        "",
    ]
    if category_counts:
        lines.extend(f"- {category}: {count}" for category, count in sorted(category_counts.items()))
    else:
        lines.append("- No external data files indexed.")
    lines.append("")

    if records:
        lines.extend(
            [
                "## Indexed Files",
                "",
                "| category | file | size_mb | source_group |",
                "| --- | --- | ---: | --- |",
            ]
        )
        for record in records:
            lines.append(
                (
                    f"| {record['category']} | `{record['relative_path']}` | "
                    f"{record['size_mb']} | {record['source_group']} |"
                )
            )
        lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _write_staging_report(result: StagingResult, package_root: Path, project_root: Path) -> None:
    """Write a Markdown report for staged data."""

    lines = [
        "# Data Staging Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Download package: `{package_root}`",
        f"- Project root: `{project_root}`",
        f"- Copied files: {result.copied_count}",
        f"- Skipped files: {result.skipped_count}",
        f"- External indexed files: {result.external_index_count}",
        "",
        "Default staging copies only selected small/essential boundary and statistics files.",
        "Large raster/NetCDF/HDF files remain in the download package and are referenced by index.",
        "",
    ]

    if result.external_index_csv or result.external_index_json:
        lines.extend(
            [
                "## External Index",
                "",
                f"- CSV: `{result.external_index_csv}`",
                f"- JSON: `{result.external_index_json}`",
                "",
            ]
        )

    if result.copied_files:
        lines.extend(["## Copied", ""])
        lines.extend(f"- `{path}`" for path in result.copied_files)
        lines.append("")

    if result.skipped_files:
        lines.extend(["## Skipped", ""])
        lines.extend(f"- {item}" for item in result.skipped_files)
        lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _count_by_category(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Count records by category."""

    counts: dict[str, int] = {}
    for record in records:
        category = str(record.get("category") or "unknown")
        counts[category] = counts.get(category, 0) + 1
    return counts


def _normalize_path_text(path: Path) -> str:
    """Normalize a path for case-insensitive keyword matching."""

    return str(path).replace("\\", "/").lower()


def _unique_paths(paths: Iterable[Path]) -> list[Path]:
    """Return paths in first-seen order without duplicates."""

    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(path.resolve())
    return unique
