from pathlib import Path

from src.staging import build_external_data_index, load_external_data_paths, stage_existing_downloads


def test_stage_existing_downloads_copies_boundary_and_statistics(tmp_path: Path) -> None:
    package_root = tmp_path / "heat_drought_download_package"
    boundary_source = package_root / "downloads" / "raw" / "geoboundaries" / "CHN_ADM3"
    stats_source = package_root / "metadata" / "statistics"
    boundary_source.mkdir(parents=True)
    stats_source.mkdir(parents=True)

    for suffix in [".shp", ".dbf", ".shx", ".prj"]:
        (boundary_source / f"geoBoundaries-CHN-ADM3_simplified{suffix}").write_text("x", encoding="utf-8")
    (stats_source / "nbs_grain_province_2024_2025.csv").write_text("year,region\n2024,湖北\n", encoding="utf-8")

    result = stage_existing_downloads(package_root=package_root, project_root=tmp_path / "project")

    assert result.status == "ok"
    assert result.copied_count == 5
    assert (tmp_path / "project" / "data" / "raw" / "boundary" / "geoBoundaries-CHN-ADM3_simplified.shp").exists()
    assert (tmp_path / "project" / "data" / "raw" / "statistics" / "nbs_grain_province_2024_2025.csv").exists()


def test_stage_existing_downloads_reports_missing_package(tmp_path: Path) -> None:
    result = stage_existing_downloads(package_root=tmp_path / "missing", project_root=tmp_path / "project")

    assert result.status == "missing"
    assert result.copied_count == 0
    assert result.report_path.exists()
    assert "Download package not found" in result.report_path.read_text(encoding="utf-8")


def test_build_external_data_index_references_large_files_without_copying(tmp_path: Path) -> None:
    package_root = tmp_path / "heat_drought_download_package"
    project_root = tmp_path / "project"
    climate_file = package_root / "downloads" / "raw" / "era5_land" / "t2m_2022.nc"
    clipped_climate_file = package_root / "downloads" / "clipped" / "chirps" / "chirps_2022.nc"
    remote_file = package_root / "downloads" / "raw" / "nasa" / "MOD13Q1" / "MOD13Q1_2022.hdf"
    crop_file = (
        package_root
        / "downloads"
        / "raw"
        / "china_single_season_rice_2017_2023"
        / "rice_mask_2021.tif"
    )
    phenology_file = package_root / "downloads" / "raw" / "chinaricecalendar" / "Middle_rice_heading_2022.tif"
    for path in [climate_file, clipped_climate_file, remote_file, crop_file, phenology_file]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    result = build_external_data_index(package_root=package_root, project_root=project_root)

    assert result.status == "ok"
    assert result.indexed_count == 5
    assert result.csv_path.exists()
    assert result.json_path.exists()
    assert result.report_path.exists()
    assert not (project_root / "data" / "raw" / "climate" / climate_file.name).exists()
    assert climate_file.resolve() in load_external_data_paths(project_root, "climate")
    assert clipped_climate_file.resolve() in load_external_data_paths(project_root, "climate")
    assert remote_file.resolve() in load_external_data_paths(project_root, "remote_sensing")
    assert crop_file.resolve() in load_external_data_paths(project_root, "crop_mask")
    assert phenology_file.resolve() in load_external_data_paths(project_root, "phenology")
