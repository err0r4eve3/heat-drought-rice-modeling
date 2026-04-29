from pathlib import Path

from src.climate import (
    _build_growing_season_rows,
    build_analysis_years,
    find_climate_files,
    identify_coordinate_names,
    identify_climate_variables,
    preprocess_climate,
)


def test_find_climate_files_returns_netcdf_formats(tmp_path: Path) -> None:
    climate_dir = tmp_path / "climate"
    climate_dir.mkdir()
    first = climate_dir / "era5.nc"
    second = climate_dir / "chirps.nc4"
    ignored = climate_dir / "readme.txt"
    first.write_text("x", encoding="utf-8")
    second.write_text("x", encoding="utf-8")
    ignored.write_text("x", encoding="utf-8")

    assert find_climate_files(climate_dir) == [first.resolve(), second.resolve()]


def test_find_climate_files_includes_external_index_paths(tmp_path: Path) -> None:
    climate_dir = tmp_path / "raw" / "climate"
    external_file = tmp_path / "external" / "era5_land" / "tmax_2022.nc"
    ignored_external = tmp_path / "external" / "notes.txt"
    external_file.parent.mkdir(parents=True)
    external_file.write_text("x", encoding="utf-8")
    ignored_external.write_text("x", encoding="utf-8")

    assert find_climate_files(climate_dir, external_files=[external_file, ignored_external]) == [
        external_file.resolve()
    ]


def test_find_climate_files_prefers_local_files_over_external_index_paths(tmp_path: Path) -> None:
    climate_dir = tmp_path / "raw" / "climate"
    climate_dir.mkdir(parents=True)
    local_file = climate_dir / "local_tmax_2022.nc"
    external_file = tmp_path / "external" / "era5_land" / "external_tmax_2022.nc"
    local_file.write_text("x", encoding="utf-8")
    external_file.parent.mkdir(parents=True)
    external_file.write_text("x", encoding="utf-8")

    assert find_climate_files(climate_dir, external_files=[external_file]) == [local_file.resolve()]


def test_identify_coordinate_names_matches_common_names() -> None:
    matched = identify_coordinate_names(["time", "latitude", "longitude", "valid_time"])

    assert matched == {"time": "time", "lat": "latitude", "lon": "longitude"}


def test_identify_coordinate_names_matches_era5_valid_time() -> None:
    matched = identify_coordinate_names(["valid_time", "latitude", "longitude"])

    assert matched == {"time": "valid_time", "lat": "latitude", "lon": "longitude"}


def test_identify_climate_variables_matches_common_names() -> None:
    matched = identify_climate_variables(["t2m", "tmax", "tp", "swvl1", "pev", "spei"])

    assert matched["temperature"] == "t2m"
    assert matched["tmax"] == "tmax"
    assert matched["precipitation"] == "tp"
    assert matched["soil_moisture"] == "swvl1"
    assert matched["potential_evapotranspiration"] == "pev"
    assert matched["drought_index"] == "spei"


def test_identify_climate_variables_matches_cmfd_precipitation() -> None:
    matched = identify_climate_variables(["prec"])

    assert matched["precipitation"] == "prec"


def test_build_analysis_years_deduplicates_and_sorts() -> None:
    years = build_analysis_years((2000, 2002), 2022, [2023, 2024, 2025], 2024)

    assert years == [2000, 2001, 2002, 2022, 2023, 2024, 2025]


def test_build_growing_season_rows_returns_empty_when_file_has_no_growth_months() -> None:
    import numpy as np
    import xarray as xr

    dataset = xr.Dataset(
        {"t2m": (("time", "lat", "lon"), np.ones((2, 1, 1)))},
        coords={
            "time": np.array(["2022-10-01", "2022-10-02"], dtype="datetime64[D]"),
            "lat": [30.0],
            "lon": [110.0],
        },
    )

    rows = _build_growing_season_rows(dataset, {"temperature": "t2m"}, "october.nc", [6, 7, 8, 9])

    assert rows == []


def test_preprocess_climate_handles_empty_input(tmp_path: Path) -> None:
    result = preprocess_climate(
        climate_dir=tmp_path / "raw" / "climate",
        interim_dir=tmp_path / "interim",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        baseline_years=(2000, 2021),
        main_event_year=2022,
        recovery_years=[2023, 2024, 2025],
        validation_event_year=2024,
        rice_growth_months=[6, 7, 8, 9],
        heat_threshold_quantile=0.90,
        drought_threshold_quantile=0.10,
    )

    assert result.status == "missing"
    assert result.file_count == 0
    assert result.report_path.exists()
    assert "No climate NetCDF files found" in result.report_path.read_text(encoding="utf-8")
