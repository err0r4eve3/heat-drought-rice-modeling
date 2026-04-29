from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from src.yield_proxy import (
    apply_province_calibration,
    build_yield_proxy_panel,
    parse_yield_proxy_metadata,
    zonal_aggregate_proxy_raster,
)


def test_parse_yield_proxy_metadata_detects_common_sources() -> None:
    ggcp = parse_yield_proxy_metadata(Path("GGCP10_Production_2020_Rice.tif"))
    asia = parse_yield_proxy_metadata(Path("asia_rice_yield_4km") / "Version1" / "Double_Early2012.tif")

    assert ggcp.source == "ggcp10"
    assert ggcp.year == 2020
    assert ggcp.variable == "production"
    assert ggcp.crop == "rice"
    assert asia.source == "asia_rice_yield_4km"
    assert asia.year == 2012
    assert asia.variable == "yield"
    assert asia.crop == "rice"


def test_zonal_aggregate_proxy_raster_returns_admin_values(tmp_path: Path) -> None:
    raster_path = tmp_path / "AsiaRiceYield4km_2010_rice.tif"
    data = np.array([[1000.0, 2000.0], [3000.0, 4000.0]], dtype="float32")
    with rasterio.open(
        raster_path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999,
    ) as dataset:
        dataset.write(data, 1)

    admin = gpd.GeoDataFrame(
        [
            {"admin_id": "left", "province_name": "A", "county_name": "L", "geometry": box(0, 0, 1, 2)},
            {"admin_id": "right", "province_name": "A", "county_name": "R", "geometry": box(1, 0, 2, 2)},
        ],
        crs="EPSG:4326",
    )

    result = zonal_aggregate_proxy_raster(raster_path, admin)

    values = dict(zip(result["admin_id"], result["raw_proxy_yield"], strict=True))
    assert values["left"] == 2000.0
    assert values["right"] == 3000.0
    assert set(result["valid_pixel_count"]) == {2}


def test_apply_province_calibration_matches_official_production() -> None:
    proxy = pd.DataFrame(
        [
            {
                "admin_id": "a",
                "province": "P",
                "year": 2020,
                "crop": "rice",
                "raw_proxy_yield": 5000.0,
                "rice_area_proxy": 10.0,
            },
            {
                "admin_id": "b",
                "province": "P",
                "year": 2020,
                "crop": "rice",
                "raw_proxy_yield": 6000.0,
                "rice_area_proxy": 20.0,
            },
        ]
    )
    official = pd.DataFrame(
        [
            {
                "province": "P",
                "year": 2020,
                "crop": "rice",
                "production_ton": 340.0,
            }
        ]
    )

    calibrated = apply_province_calibration(proxy, official)

    assert calibrated["calibration_coefficient"].round(6).tolist() == [2.0, 2.0]
    assert calibrated["calibrated_yield"].tolist() == [10000.0, 12000.0]
    assert calibrated["calibration_status"].tolist() == ["calibrated", "calibrated"]


def test_apply_province_calibration_does_not_use_grain_for_rice_proxy() -> None:
    proxy = pd.DataFrame(
        [
            {
                "admin_id": "a",
                "province": "P",
                "year": 2020,
                "crop": "rice",
                "raw_proxy_yield": 5000.0,
                "rice_area_proxy": 10.0,
            }
        ]
    )
    official = pd.DataFrame(
        [
            {
                "province": "P",
                "year": 2020,
                "crop": "grain",
                "production_ton": 100.0,
            }
        ]
    )

    calibrated = apply_province_calibration(proxy, official)

    assert calibrated["calibration_status"].tolist() == ["missing_official_or_proxy"]
    assert calibrated["calibrated_yield"].isna().all()


def test_build_yield_proxy_panel_writes_gap_report_when_rasters_missing(tmp_path: Path) -> None:
    admin_path = tmp_path / "admin.gpkg"
    crop_path = tmp_path / "crop.csv"
    official_path = tmp_path / "yield.csv"
    proxy_dir = tmp_path / "proxy"
    output_dir = tmp_path / "processed" / "yield_proxy"
    reports_dir = tmp_path / "reports"
    proxy_dir.mkdir()

    admin = gpd.GeoDataFrame(
        [{"admin_id": "a", "province_name": "P", "county_name": "C", "geometry": box(0, 0, 1, 1)}],
        crs="EPSG:4326",
    )
    admin.to_file(admin_path, driver="GPKG")
    pd.DataFrame([{"admin_id": "a", "crop_area_ha": 10.0, "status": "zonal_stats"}]).to_csv(crop_path, index=False)
    pd.DataFrame([{"province": "P", "year": 2020, "crop": "rice", "production_ton": 100.0}]).to_csv(
        official_path,
        index=False,
    )

    result = build_yield_proxy_panel(
        proxy_dir=proxy_dir,
        admin_path=admin_path,
        crop_summary_path=crop_path,
        official_yield_path=official_path,
        output_dir=output_dir,
        reports_dir=reports_dir,
        target_years=(2010, 2020),
    )

    assert result.status == "missing"
    assert result.outputs["panel"].exists()
    assert result.outputs["gap_report"].exists()
    assert result.report_path.exists()
    assert "No yield proxy rasters found" in result.report_path.read_text(encoding="utf-8")
