"""Project configuration loading and path normalization."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


RAW_CATEGORIES = (
    "climate",
    "remote_sensing",
    "crop_mask",
    "phenology",
    "statistics",
    "boundary",
    "admin_codes",
    "irrigation",
    "soil",
    "water",
    "references",
)


@dataclass(frozen=True)
class ProjectConfig:
    """Normalized project configuration values."""

    project_root: Path
    data_raw_dir: Path
    data_interim_dir: Path
    data_processed_dir: Path
    output_dir: Path
    study_area_name: str
    study_bbox: list[float]
    target_admin_level: str
    crs_wgs84: str
    crs_equal_area: str
    baseline_years: tuple[int, int]
    main_event_year: int
    validation_event_year: int
    recovery_years: list[int]
    main_event_months: list[int]
    rice_growth_months: list[int]
    heat_threshold_quantile: float
    drought_threshold_quantile: float
    min_valid_observations: int
    output_formats: list[str]
    raw: dict[str, Any]


def load_config(config_path: str | Path = "config/config.yaml") -> ProjectConfig:
    """Load a YAML config file and resolve project paths."""

    path = Path(config_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as file_obj:
        raw = yaml.safe_load(file_obj) or {}

    project_root = _resolve_project_root(raw.get("project_root"), path)

    return ProjectConfig(
        project_root=project_root,
        data_raw_dir=_resolve_path(raw["data_raw_dir"], project_root),
        data_interim_dir=_resolve_path(raw["data_interim_dir"], project_root),
        data_processed_dir=_resolve_path(raw["data_processed_dir"], project_root),
        output_dir=_resolve_path(raw["output_dir"], project_root),
        study_area_name=str(raw["study_area_name"]),
        study_bbox=[float(value) if "." in str(value) else int(value) for value in raw["study_bbox"]],
        target_admin_level=str(raw["target_admin_level"]),
        crs_wgs84=str(raw["crs_wgs84"]),
        crs_equal_area=str(raw["crs_equal_area"]),
        baseline_years=_parse_year_range(raw["baseline_years"]),
        main_event_year=int(raw["main_event_year"]),
        validation_event_year=int(raw["validation_event_year"]),
        recovery_years=[int(year) for year in raw["recovery_years"]],
        main_event_months=[int(month) for month in raw["main_event_months"]],
        rice_growth_months=[int(month) for month in raw["rice_growth_months"]],
        heat_threshold_quantile=float(raw["heat_threshold_quantile"]),
        drought_threshold_quantile=float(raw["drought_threshold_quantile"]),
        min_valid_observations=int(raw["min_valid_observations"]),
        output_formats=[str(fmt) for fmt in raw["output_formats"]],
        raw=dict(raw),
    )


def ensure_project_dirs(config: ProjectConfig) -> None:
    """Create configured output directories and raw-data category folders."""

    directories = [
        config.data_raw_dir,
        config.data_interim_dir,
        config.data_processed_dir,
        config.output_dir,
        config.project_root / "reports",
        config.project_root / "scripts",
        config.project_root / "src",
        config.project_root / "tests",
        config.project_root / "notebooks",
    ]
    directories.extend(config.data_raw_dir / category for category in RAW_CATEGORIES)

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def _resolve_project_root(value: Any, config_path: Path) -> Path:
    """Resolve project root relative to the config file parent."""

    if value is None:
        return config_path.parent.parent.resolve()

    expanded = os.path.expandvars(str(value))
    candidate = Path(expanded).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (config_path.parent.parent / candidate).resolve()


def _resolve_path(value: Any, project_root: Path) -> Path:
    """Resolve a path value relative to project root."""

    expanded = os.path.expandvars(str(value))
    candidate = Path(expanded).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (project_root / candidate).resolve()


def _parse_year_range(value: Any) -> tuple[int, int]:
    """Parse inclusive baseline years from a string, list, tuple, or mapping."""

    if isinstance(value, str):
        parts = value.replace(" ", "").split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid baseline_years string: {value}")
        return int(parts[0]), int(parts[1])

    if isinstance(value, dict):
        return int(value["start"]), int(value["end"])

    if isinstance(value, (list, tuple)) and len(value) == 2:
        return int(value[0]), int(value[1])

    raise ValueError(f"Invalid baseline_years value: {value}")
