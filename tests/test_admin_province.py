from pathlib import Path

import geopandas as gpd
from shapely.geometry import box

from src.admin_province import assign_admin_provinces, normalize_geoboundaries_province_name


def test_normalize_geoboundaries_province_name_to_chinese() -> None:
    assert normalize_geoboundaries_province_name("Guangxi Zhuang Autonomous Region") == "广西"
    assert normalize_geoboundaries_province_name("Shanghai Municipality") == "上海"
    assert normalize_geoboundaries_province_name("Hubei Province") == "湖北"
    assert normalize_geoboundaries_province_name("Guangzhou Province") == "广东"


def test_assign_admin_provinces_spatial_join(tmp_path: Path) -> None:
    admin_path = tmp_path / "admin.gpkg"
    province_path = tmp_path / "province.geojson"
    output_path = tmp_path / "admin_with_province.gpkg"

    admin = gpd.GeoDataFrame(
        [
            {"admin_id": "a", "county_name": "A", "geometry": box(0, 0, 1, 1)},
            {"admin_id": "b", "county_name": "B", "geometry": box(2, 0, 3, 1)},
        ],
        crs="EPSG:4326",
    )
    province = gpd.GeoDataFrame(
        [
            {"shapeName": "Hubei Province", "geometry": box(-1, -1, 1.5, 2)},
            {"shapeName": "Hunan Province", "geometry": box(1.5, -1, 4, 2)},
        ],
        crs="EPSG:4326",
    )
    admin.to_file(admin_path, driver="GPKG")
    province.to_file(province_path, driver="GeoJSON")

    result = assign_admin_provinces(admin_path, province_path, output_path)

    assert result.status == "ok"
    joined = gpd.read_file(output_path)
    assert dict(zip(joined["admin_id"], joined["province_name"], strict=True)) == {"a": "湖北", "b": "湖南"}
