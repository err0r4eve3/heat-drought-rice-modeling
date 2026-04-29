import csv
from pathlib import Path

from src.crop import (
    aggregate_crop_masks_to_admin_units,
    aggregate_phenology_to_admin_units,
    default_phenology,
    find_crop_files,
    prepare_crop_mask_phenology,
    select_mask_for_year,
)


def test_find_crop_files_returns_supported_mask_and_phenology_formats(tmp_path: Path) -> None:
    crop_mask_dir = tmp_path / "raw" / "crop_mask"
    phenology_dir = tmp_path / "raw" / "phenology"
    nested_mask_dir = crop_mask_dir / "nested"
    crop_mask_dir.mkdir(parents=True)
    nested_mask_dir.mkdir()
    phenology_dir.mkdir(parents=True)

    mask_tif = crop_mask_dir / "rice_mask_2020.tif"
    mask_vector = nested_mask_dir / "crop_land_2022.gpkg"
    mask_ignored = crop_mask_dir / "notes.txt"
    phenology_csv = phenology_dir / "rice_calendar.csv"
    phenology_json = phenology_dir / "phenology_2022.json"
    phenology_tif = phenology_dir / "ChinaRiceCalendar_heading.tif"
    for path in [mask_tif, mask_vector, mask_ignored, phenology_csv, phenology_json, phenology_tif]:
        path.write_text("x", encoding="utf-8")

    files = find_crop_files(crop_mask_dir, phenology_dir)

    assert files.crop_mask_files == [mask_tif.resolve(), mask_vector.resolve()]
    assert files.phenology_files == [phenology_csv.resolve(), phenology_json.resolve(), phenology_tif.resolve()]


def test_find_crop_files_includes_external_index_paths(tmp_path: Path) -> None:
    crop_mask_dir = tmp_path / "raw" / "crop_mask"
    phenology_dir = tmp_path / "raw" / "phenology"
    external_mask = tmp_path / "external" / "china_single_season_rice" / "rice_mask_2021.tif"
    external_phenology = tmp_path / "external" / "chinaricecalendar" / "Middle_rice_heading_2022.tif"
    ignored_external = tmp_path / "external" / "notes.txt"
    for path in [external_mask, external_phenology, ignored_external]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    files = find_crop_files(
        crop_mask_dir,
        phenology_dir,
        external_crop_mask_files=[external_mask, ignored_external],
        external_phenology_files=[external_phenology, ignored_external],
    )

    assert files.crop_mask_files == [external_mask.resolve()]
    assert files.phenology_files == [external_phenology.resolve()]


def test_find_crop_files_prefers_local_files_over_external_index_paths(tmp_path: Path) -> None:
    crop_mask_dir = tmp_path / "raw" / "crop_mask"
    phenology_dir = tmp_path / "raw" / "phenology"
    crop_mask_dir.mkdir(parents=True)
    phenology_dir.mkdir(parents=True)
    local_mask = crop_mask_dir / "local_rice_mask_2022.tif"
    local_phenology = phenology_dir / "local_calendar_2022.csv"
    external_mask = tmp_path / "external" / "rice_mask_2021.tif"
    external_phenology = tmp_path / "external" / "Middle_rice_heading_2022.tif"
    for path in [local_mask, local_phenology, external_mask, external_phenology]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    files = find_crop_files(
        crop_mask_dir,
        phenology_dir,
        external_crop_mask_files=[external_mask],
        external_phenology_files=[external_phenology],
    )

    assert files.crop_mask_files == [local_mask.resolve()]
    assert files.phenology_files == [local_phenology.resolve()]


def test_select_mask_for_year_chooses_nearest_filename_year(tmp_path: Path) -> None:
    masks = [
        tmp_path / "rice_mask_2018.tif",
        tmp_path / "rice_mask_2021.tif",
        tmp_path / "rice_mask_2025.tif",
    ]

    assert select_mask_for_year(masks, 2022) == masks[1]


def test_select_mask_for_year_prefers_rice_mask_over_cropland_proxy(tmp_path: Path) -> None:
    rice_mask = tmp_path / "classified-Hubei-2021-middle_rice-WGS84-v1.tif"
    clcd_mask = tmp_path / "CLCD_v01_2022_albert.tif"

    assert select_mask_for_year([clcd_mask, rice_mask], 2022) == rice_mask


def test_select_mask_for_year_returns_first_file_when_no_years(tmp_path: Path) -> None:
    masks = [tmp_path / "a_mask.tif", tmp_path / "b_mask.tif"]

    assert select_mask_for_year(masks, 2022) == masks[0]


def test_default_phenology_returns_default_growth_window() -> None:
    phenology = default_phenology([6, 7, 8, 9])

    assert phenology["source"] == "default"
    assert phenology["months"] == [6, 7, 8, 9]
    assert phenology["start_month"] == 6
    assert phenology["end_month"] == 9
    assert phenology["windows"] == [
        {
            "stage": "rice_growth_window",
            "start_month": 6,
            "end_month": 9,
            "months": [6, 7, 8, 9],
        }
    ]


def test_prepare_crop_mask_phenology_handles_empty_input(tmp_path: Path) -> None:
    result = prepare_crop_mask_phenology(
        crop_mask_dir=tmp_path / "raw" / "crop_mask",
        phenology_dir=tmp_path / "raw" / "phenology",
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        target_year=2022,
        rice_growth_months=[6, 7, 8, 9],
        crs_wgs84="EPSG:4326",
        crs_equal_area="EPSG:6933",
    )

    crop_summary = tmp_path / "processed" / "crop_mask_summary_by_admin.csv"
    phenology_summary = tmp_path / "processed" / "phenology_by_admin.csv"

    assert result.status == "missing"
    assert result.selected_mask_path is None
    assert result.outputs["crop_mask_summary_by_admin"] == crop_summary.resolve()
    assert result.outputs["phenology_by_admin"] == phenology_summary.resolve()
    assert result.report_path.exists()
    assert "No crop mask files found" in result.report_path.read_text(encoding="utf-8")

    with crop_summary.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        assert reader.fieldnames == [
            "admin_id",
            "admin_name",
            "target_year",
            "crop_area_ha",
            "crop_fraction",
            "source_file",
            "status",
        ]
        assert list(reader) == []

    with phenology_summary.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        assert reader.fieldnames == [
            "admin_id",
            "admin_name",
            "target_year",
            "start_month",
            "end_month",
            "months",
            "source",
            "status",
        ]
        assert list(reader) == []


def test_prepare_crop_mask_phenology_writes_default_phenology_for_admin_units(tmp_path: Path) -> None:
    import pandas as pd

    admin_path = tmp_path / "processed" / "admin_units.parquet"
    admin_path.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {"admin_id": "admin_1", "shapeName": "Alpha County"},
            {"admin_id": "admin_2", "shapeName": "Beta County"},
        ]
    ).to_parquet(admin_path, index=False)

    result = prepare_crop_mask_phenology(
        crop_mask_dir=tmp_path / "raw" / "crop_mask",
        phenology_dir=tmp_path / "raw" / "phenology",
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        target_year=2022,
        rice_growth_months=[6, 7, 8, 9],
        crs_wgs84="EPSG:4326",
        crs_equal_area="EPSG:6933",
        admin_units_path=admin_path,
    )

    phenology_rows = list(
        csv.DictReader(
            (tmp_path / "processed" / "phenology_by_admin.csv").open(
                "r",
                encoding="utf-8",
                newline="",
            )
        )
    )
    crop_rows = list(
        csv.DictReader(
            (tmp_path / "processed" / "crop_mask_summary_by_admin.csv").open(
                "r",
                encoding="utf-8",
                newline="",
            )
        )
    )

    assert result.status == "missing"
    assert [row["admin_id"] for row in phenology_rows] == ["admin_1", "admin_2"]
    assert {row["months"] for row in phenology_rows} == {"6,7,8,9"}
    assert {row["status"] for row in phenology_rows} == {"default"}
    assert [row["admin_id"] for row in crop_rows] == ["admin_1", "admin_2"]
    assert {row["status"] for row in crop_rows} == {"missing_mask"}


def test_aggregate_crop_masks_to_admin_units_counts_binary_rice_pixels(tmp_path: Path) -> None:
    import geopandas as gpd
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin
    from shapely.geometry import box

    raster_path = tmp_path / "rice_mask_2022.tif"
    admin_path = tmp_path / "admin_units.gpkg"
    data = np.array(
        [
            [1, 0, 0, 0],
            [1, 1, 0, 0],
            [0, 0, 1, 0],
            [0, 0, 1, 1],
        ],
        dtype="uint8",
    )
    with rasterio.open(
        raster_path,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=from_origin(0, 4, 1, 1),
        nodata=0,
    ) as dst:
        dst.write(data, 1)

    admins = gpd.GeoDataFrame(
        [
            {"admin_id": "left", "admin_name": "Left", "geometry": box(0, 0, 2, 4)},
            {"admin_id": "right", "admin_name": "Right", "geometry": box(2, 0, 4, 4)},
        ],
        crs="EPSG:3857",
    )
    admins.to_file(admin_path, driver="GPKG")

    rows, warnings = aggregate_crop_masks_to_admin_units(
        mask_paths=[raster_path],
        admin_units_path=admin_path,
        target_year=2022,
        crs_equal_area="EPSG:3857",
        max_cells_per_raster=100,
    )

    by_admin = {row["admin_id"]: row for row in rows}

    assert warnings == []
    assert set(by_admin) == {"left", "right"}
    assert by_admin["left"]["status"] == "zonal_stats"
    assert by_admin["right"]["status"] == "zonal_stats"
    assert float(by_admin["left"]["crop_area_ha"]) == 0.0003
    assert float(by_admin["right"]["crop_area_ha"]) == 0.0003
    assert float(by_admin["left"]["crop_fraction"]) == 0.375
    assert float(by_admin["right"]["crop_fraction"]) == 0.375


def test_aggregate_phenology_to_admin_units_converts_doy_to_month_window(tmp_path: Path) -> None:
    import geopandas as gpd
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin
    from shapely.geometry import box

    admin_path = tmp_path / "admin_units.gpkg"
    gpd.GeoDataFrame(
        [{"admin_id": "admin_1", "admin_name": "Alpha", "geometry": box(0, 0, 2, 2)}],
        crs="EPSG:3857",
    ).to_file(admin_path, driver="GPKG")
    for name, value in [
        ("Middle_rice_transplanting_dates_2018_2022_county_level.tif", 150),
        ("Middle_rice_maturity_dates_2018_2022_county_level.tif", 250),
    ]:
        with rasterio.open(
            tmp_path / name,
            "w",
            driver="GTiff",
            height=2,
            width=2,
            count=1,
            dtype="float32",
            crs="EPSG:3857",
            transform=from_origin(0, 2, 1, 1),
            nodata=-9999,
        ) as dst:
            dst.write(np.full((2, 2), value, dtype="float32"), 1)

    rows, warnings = aggregate_phenology_to_admin_units(
        phenology_paths=list(tmp_path.glob("*.tif")),
        admin_units_path=admin_path,
        target_year=2022,
        default_months=[6, 7, 8, 9],
        max_cells_per_raster=100,
    )

    assert warnings == []
    assert rows == [
        {
            "admin_id": "admin_1",
            "admin_name": "Alpha",
            "target_year": 2022,
            "start_month": 5,
            "end_month": 9,
            "months": "5,6,7,8,9",
            "source": (
                "Middle_rice_maturity_dates_2018_2022_county_level.tif;"
                "Middle_rice_transplanting_dates_2018_2022_county_level.tif"
            ),
            "status": "zonal_stats",
        }
    ]
