from pathlib import Path

import src.remote_sensing as remote_sensing


def _read_table(path: Path):
    import pandas as pd

    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def test_find_remote_sensing_files_returns_supported_formats(tmp_path: Path) -> None:
    remote_dir = tmp_path / "remote_sensing"
    nested_dir = remote_dir / "nested"
    nested_dir.mkdir(parents=True)
    tif = remote_dir / "MOD13Q1_NDVI.tif"
    tiff = nested_dir / "MOD11A2_LST.tiff"
    nc = remote_dir / "SMAP_soil_moisture.nc"
    nc4 = remote_dir / "GLEAM_sm.nc4"
    hdf = remote_dir / "MOD16A2_ET.hdf"
    h5 = remote_dir / "GRACE_TWS.h5"
    ignored = remote_dir / "readme.txt"
    for path in (tif, tiff, nc, nc4, hdf, h5, ignored):
        path.write_text("x", encoding="utf-8")

    assert remote_sensing.find_remote_sensing_files(remote_dir) == [
        tif.resolve(),
        tiff.resolve(),
        nc.resolve(),
        nc4.resolve(),
        hdf.resolve(),
        h5.resolve(),
    ]


def test_find_remote_sensing_files_includes_external_index_paths(tmp_path: Path) -> None:
    remote_dir = tmp_path / "raw" / "remote_sensing"
    external_hdf = tmp_path / "external" / "nasa" / "MOD13Q1" / "MOD13Q1_2022.hdf"
    external_tif = tmp_path / "external" / "ndvi_2022.tif"
    ignored_external = tmp_path / "external" / "readme.txt"
    for path in [external_hdf, external_tif, ignored_external]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    assert remote_sensing.find_remote_sensing_files(
        remote_dir,
        external_files=[external_hdf, external_tif, ignored_external],
    ) == [external_tif.resolve(), external_hdf.resolve()]


def test_find_remote_sensing_files_prefers_local_files_over_external_index_paths(tmp_path: Path) -> None:
    remote_dir = tmp_path / "raw" / "remote_sensing"
    remote_dir.mkdir(parents=True)
    local_file = remote_dir / "local_MOD13Q1_2022.tif"
    external_file = tmp_path / "external" / "nasa" / "MOD13Q1" / "external_MOD13Q1_2022.hdf"
    local_file.write_text("x", encoding="utf-8")
    external_file.parent.mkdir(parents=True)
    external_file.write_text("x", encoding="utf-8")

    assert remote_sensing.find_remote_sensing_files(remote_dir, external_files=[external_file]) == [
        local_file.resolve()
    ]


def test_modis_scale_helpers_convert_fill_values_to_none() -> None:
    assert remote_sensing.scale_modis_ndvi([10000, 0, -3000, -3001, -32768]) == [
        1.0,
        0.0,
        -0.3,
        None,
        None,
    ]
    assert remote_sensing.scale_modis_evi([5000, -2000, 10001, -32768]) == [0.5, -0.2, None, None]
    assert remote_sensing.scale_modis_lst_celsius([15000, 13000, 7500, 0, -32768]) == [
        26.85,
        -13.15,
        None,
        None,
        None,
    ]


def test_identify_remote_sensing_product_from_filename_and_variables() -> None:
    mod13 = remote_sensing.identify_remote_sensing_product(
        "MOD13Q1.A2022177.h27v05.tif",
        ["250m_16_days_NDVI", "250m_16_days_EVI"],
    )
    assert mod13["product"] == "MOD13Q1"
    assert mod13["variables"] == {"ndvi": "250m_16_days_NDVI", "evi": "250m_16_days_EVI"}

    myd13 = remote_sensing.identify_remote_sensing_product(
        "MYD13Q1.A2022177.h27v05.hdf",
        ["250m_16_days_NDVI", "250m_16_days_EVI"],
    )
    assert myd13["product"] == "MYD13Q1"
    assert myd13["variables"] == {"ndvi": "250m_16_days_NDVI", "evi": "250m_16_days_EVI"}

    mod11 = remote_sensing.identify_remote_sensing_product("MOD11A2_LST_Day_1km.hdf", ["LST_Day_1km"])
    assert mod11["product"] == "MOD11A2"
    assert mod11["variables"] == {"lst": "LST_Day_1km"}

    mod16 = remote_sensing.identify_remote_sensing_product("MOD16A2_ET_500m.hdf", ["ET_500m"])
    assert mod16["product"] == "MOD16A2"
    assert mod16["variables"] == {"et": "ET_500m"}

    smap = remote_sensing.identify_remote_sensing_product("SMAP_L3_soil_moisture.nc", ["soil_moisture"])
    assert smap["product"] == "SMAP"
    assert smap["variables"] == {"soil_moisture": "soil_moisture"}

    grace = remote_sensing.identify_remote_sensing_product("GRACE_TWSA.h5", ["lwe_thickness"])
    assert grace["product"] == "GRACE"
    assert grace["variables"] == {"tws": "lwe_thickness"}


def test_preprocess_remote_sensing_uses_metadata_limited_fallback_for_modis_hdf4(
    tmp_path: Path,
) -> None:
    remote_dir = tmp_path / "raw" / "remote_sensing"
    remote_dir.mkdir(parents=True)
    hdf4_like = remote_dir / "MOD13Q1.A2022177.h27v05.061.fake.hdf"
    hdf4_like.write_bytes(b"not an hdf5 file")

    result = remote_sensing.preprocess_remote_sensing(
        remote_sensing_dir=remote_dir,
        interim_dir=tmp_path / "interim",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        baseline_years=(2000, 2021),
        rice_growth_months=[6, 7, 8, 9],
    )

    assert result.status == "ok"
    assert result.file_count == 1
    assert result.processed_files == [hdf4_like.resolve()]
    assert result.metadata[0]["format"] == "hdf4_metadata_limited"
    assert result.metadata[0]["product"] == "MOD13Q1"
    assert "metadata_warning" in result.metadata[0]


def test_preprocess_remote_sensing_extracts_monthly_and_growing_values_from_netcdf(
    tmp_path: Path,
) -> None:
    import numpy as np
    import pandas as pd
    import xarray as xr

    remote_dir = tmp_path / "raw" / "remote_sensing"
    remote_dir.mkdir(parents=True)
    path = remote_dir / "SMs_2022_GLEAM_v4.2b_MO.nc"
    dataset = xr.Dataset(
        {
            "SMs": (
                ("time", "lat", "lon"),
                np.array(
                    [
                        [[0.2, 0.4], [0.6, np.nan]],
                        [[0.3, 0.5], [0.7, 0.9]],
                    ],
                    dtype=float,
                ),
            )
        },
        coords={
            "time": pd.to_datetime(["2022-06-30", "2022-07-31"]),
            "lat": [31.0, 30.0],
            "lon": [110.0, 111.0],
        },
    )
    dataset.to_netcdf(path)

    result = remote_sensing.preprocess_remote_sensing(
        remote_sensing_dir=remote_dir,
        interim_dir=tmp_path / "interim",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        baseline_years=(2000, 2021),
        rice_growth_months=[6, 7, 8, 9],
    )

    monthly = _read_table(result.outputs["monthly"])
    growing = _read_table(result.outputs["growing_season"])

    assert result.status == "ok"
    assert result.outputs["monthly"].suffix == ".parquet"
    assert result.outputs["growing_season"].suffix == ".parquet"
    assert monthly["variable"].tolist() == ["soil_moisture", "soil_moisture"]
    assert monthly["month"].tolist() == [6, 7]
    assert monthly["value"].round(6).tolist() == [0.4, 0.6]
    assert growing["variable"].tolist() == ["soil_moisture"]
    assert growing["year"].tolist() == [2022]
    assert growing["value"].round(6).tolist() == [0.5]


def test_preprocess_remote_sensing_uses_metadata_limited_fallback_for_myd13_hdf4(
    tmp_path: Path,
) -> None:
    remote_dir = tmp_path / "raw" / "remote_sensing"
    remote_dir.mkdir(parents=True)
    hdf4_like = remote_dir / "MYD13Q1.A2022177.h27v05.061.fake.hdf"
    hdf4_like.write_bytes(b"not an hdf5 file")

    result = remote_sensing.preprocess_remote_sensing(
        remote_sensing_dir=remote_dir,
        interim_dir=tmp_path / "interim",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        baseline_years=(2000, 2021),
        rice_growth_months=[6, 7, 8, 9],
    )

    assert result.status == "ok"
    assert result.processed_files == [hdf4_like.resolve()]
    assert result.metadata[0]["format"] == "hdf4_metadata_limited"
    assert result.metadata[0]["product"] == "MYD13Q1"


def test_preprocess_remote_sensing_handles_empty_input_with_csv_fallback(tmp_path: Path) -> None:
    result = remote_sensing.preprocess_remote_sensing(
        remote_sensing_dir=tmp_path / "raw" / "remote_sensing",
        interim_dir=tmp_path / "interim",
        reports_dir=tmp_path / "reports",
        study_bbox=[105, 24, 123, 35],
        baseline_years=(2000, 2021),
        rice_growth_months=[6, 7, 8, 9],
    )

    assert result.status == "missing"
    assert result.file_count == 0
    assert result.processed_files == []
    assert result.outputs["monthly"].name == "remote_sensing_monthly.csv"
    assert result.outputs["growing_season"].name == "remote_sensing_growing_season.csv"
    assert result.outputs["monthly"].exists()
    assert result.outputs["growing_season"].exists()
    assert result.report_path.exists()
    report_text = result.report_path.read_text(encoding="utf-8")
    assert "No remote sensing files found" in report_text
    assert "EPSG:4326" in report_text
