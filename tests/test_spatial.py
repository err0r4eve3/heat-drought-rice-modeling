from pathlib import Path

import pytest

from src.spatial import (
    ADMIN_FIELD_CANDIDATES,
    find_boundary_files,
    identify_admin_fields,
    prepare_boundaries,
)


def test_find_boundary_files_returns_supported_formats(tmp_path: Path) -> None:
    boundary_dir = tmp_path / "boundary"
    boundary_dir.mkdir()
    supported = boundary_dir / "admin.geojson"
    unsupported = boundary_dir / "notes.txt"
    supported.write_text("{}", encoding="utf-8")
    unsupported.write_text("ignore", encoding="utf-8")

    files = find_boundary_files(boundary_dir)

    assert files == [supported.resolve()]


def test_identify_admin_fields_matches_candidates() -> None:
    columns = ["省份", "地级市", "区县", "行政区划代码", "geometry"]

    matched = identify_admin_fields(columns)

    assert matched == {
        "province": "省份",
        "prefecture": "地级市",
        "county": "区县",
        "code": "行政区划代码",
    }
    assert "NAME_1" in ADMIN_FIELD_CANDIDATES["province"]


def test_prepare_boundaries_handles_empty_input(tmp_path: Path) -> None:
    result = prepare_boundaries(
        boundary_dir=tmp_path / "raw" / "boundary",
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        crs_wgs84="EPSG:4326",
        crs_equal_area="EPSG:6933",
    )

    assert result.status == "missing"
    assert result.feature_count == 0
    assert result.report_path.exists()
    assert "No boundary files found" in result.report_path.read_text(encoding="utf-8")


def test_prepare_boundaries_clips_and_writes_outputs_when_geopandas_available(tmp_path: Path) -> None:
    gpd = pytest.importorskip("geopandas")
    box = pytest.importorskip("shapely.geometry").box

    boundary_dir = tmp_path / "raw" / "boundary"
    boundary_dir.mkdir(parents=True)
    source = boundary_dir / "admin.geojson"
    frame = gpd.GeoDataFrame(
        {
            "省份": ["省A", "省B"],
            "地级市": ["市A", "市B"],
            "区县": ["县A", "县B"],
            "行政区划代码": ["1001", "1002"],
        },
        geometry=[box(106, 25, 107, 26), box(130, 40, 131, 41)],
        crs="EPSG:4326",
    )
    frame.to_file(source, driver="GeoJSON")

    result = prepare_boundaries(
        boundary_dir=boundary_dir,
        processed_dir=tmp_path / "processed",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        crs_wgs84="EPSG:4326",
        crs_equal_area="EPSG:6933",
    )

    assert result.status == "ok"
    assert result.feature_count == 1
    assert result.outputs["gpkg"].exists()
    assert result.outputs["equal_area_gpkg"].exists()
    assert result.field_mapping["county"] == "区县"
