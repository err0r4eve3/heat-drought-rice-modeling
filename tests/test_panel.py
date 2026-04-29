import csv
import math
from pathlib import Path

from src import panel


def _panel_attr(name: str):
    assert hasattr(panel, name), f"src.panel.{name} is missing"
    return getattr(panel, name)


def test_aggregate_values_ignores_none_and_nan() -> None:
    aggregate_values = _panel_attr("aggregate_values")

    result = aggregate_values([1, 3, None, float("nan"), 5])

    assert result["mean"] == 3.0
    assert result["median"] == 3.0
    assert result["min"] == 1.0
    assert result["max"] == 5.0
    assert math.isclose(result["std"], math.sqrt(8 / 3))
    assert result["valid_pixel_count"] == 3


def test_aggregate_values_returns_empty_stats_when_all_values_missing() -> None:
    aggregate_values = _panel_attr("aggregate_values")

    result = aggregate_values([None, float("nan")])

    assert result == {
        "mean": None,
        "median": None,
        "min": None,
        "max": None,
        "std": None,
        "valid_pixel_count": 0,
    }


def test_calculate_missing_rates_counts_none_nan_and_absent_keys() -> None:
    calculate_missing_rates = _panel_attr("calculate_missing_rates")

    rows = [
        {"temperature": 25.0, "rain": None},
        {"temperature": float("nan"), "rain": 2.0},
        {"temperature": 30.0},
    ]

    rates = calculate_missing_rates(rows, ["temperature", "rain", "soil"])

    assert math.isclose(rates["temperature"], 1 / 3)
    assert math.isclose(rates["rain"], 2 / 3)
    assert rates["soil"] == 1.0


def test_calculate_valid_observations_counts_non_missing_values_by_id() -> None:
    calculate_valid_observations = _panel_attr("calculate_valid_observations")

    rows = [
        {"admin_id": "A", "rain": 1.0},
        {"admin_id": "A", "rain": float("nan")},
        {"admin_id": "A", "rain": 3.0},
        {"admin_id": "B", "rain": None},
        {"rain": 9.0},
    ]

    counts = calculate_valid_observations(rows, "admin_id", "rain")

    assert counts == {"A": 2, "B": 0}


def test_spatial_aggregate_writes_empty_outputs_when_inputs_are_missing(tmp_path: Path) -> None:
    spatial_aggregate = _panel_attr("spatial_aggregate")
    fallback_output_names = _panel_attr("FALLBACK_OUTPUT_NAMES")

    processed_dir = tmp_path / "processed"
    interim_dir = tmp_path / "interim"
    reports_dir = tmp_path / "reports"

    result = spatial_aggregate(
        processed_dir=processed_dir,
        interim_dir=interim_dir,
        reports_dir=reports_dir,
    )

    assert result.status == "missing"
    assert result.report_path == reports_dir / "spatial_aggregation_qc.md"
    assert result.report_path.exists()
    assert "admin_units.gpkg" in result.report_path.read_text(encoding="utf-8")

    assert set(result.outputs) == set(fallback_output_names)
    for key, file_name in fallback_output_names.items():
        path = processed_dir / file_name
        assert result.outputs[key] == path
        assert path.exists()
        with path.open("r", encoding="utf-8", newline="") as file_obj:
            rows = list(csv.reader(file_obj))
        assert rows == [
            [
                "admin_id",
                "year",
                "month",
                "variable",
                "mean",
                "median",
                "min",
                "max",
                "std",
                "valid_pixel_count",
                "source_panel",
            ]
        ]


def test_spatial_aggregate_broadcasts_existing_source_panels_to_admin_units(tmp_path: Path) -> None:
    import pandas as pd

    spatial_aggregate = _panel_attr("spatial_aggregate")

    processed_dir = tmp_path / "processed"
    interim_dir = tmp_path / "interim"
    reports_dir = tmp_path / "reports"
    processed_dir.mkdir()
    interim_dir.mkdir()
    admin_path = processed_dir / "admin_units.parquet"
    climate_path = interim_dir / "climate_growing_season.parquet"
    pd.DataFrame(
        [
            {"admin_id": "A", "admin_name": "Alpha"},
            {"admin_id": "B", "admin_name": "Beta"},
        ]
    ).to_parquet(admin_path, index=False)
    pd.DataFrame(
        [
            {
                "source_file": "climate.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_mean_temperature",
                "time": "",
                "year": 2022,
                "month": "",
                "value": 28.5,
            },
            {
                "source_file": "climate_duplicate.nc",
                "aggregation": "growing_season",
                "variable": "growing_season_mean_temperature",
                "time": "",
                "year": 2022,
                "month": "",
                "value": 30.5,
            }
        ]
    ).to_parquet(climate_path, index=False)

    result = spatial_aggregate(
        processed_dir=processed_dir,
        interim_dir=interim_dir,
        reports_dir=reports_dir,
        admin_units_path=admin_path,
        input_panels={"climate": climate_path},
    )

    output = processed_dir / "admin_climate_panel.parquet"
    rows = pd.read_parquet(output).sort_values("admin_id")
    csv_output = processed_dir / "admin_climate_panel.csv"
    csv_rows = pd.read_csv(csv_output).sort_values("admin_id")

    assert result.status == "partial"
    assert result.outputs["climate"] == output
    assert csv_output.exists()
    assert rows["admin_id"].tolist() == ["A", "B"]
    assert rows["variable"].tolist() == [
        "growing_season_mean_temperature",
        "growing_season_mean_temperature",
    ]
    assert rows["mean"].tolist() == [29.5, 29.5]
    assert rows["valid_pixel_count"].tolist() == [2, 2]
    assert csv_rows["mean"].tolist() == [29.5, 29.5]


def test_aggregate_netcdf_to_province_bounds_uses_reference_raster_extents(tmp_path: Path) -> None:
    import numpy as np
    import pandas as pd
    import rasterio
    import xarray as xr
    from rasterio.transform import from_origin

    aggregate_netcdf_to_province_bounds = _panel_attr("aggregate_netcdf_to_province_bounds")
    nc_path = tmp_path / "temperature_2022.nc"
    output_path = tmp_path / "province_climate.parquet"
    anhui_mask = tmp_path / "classified-Anhui-2022-middle_rice-WGS84-v1.tif"
    jiangsu_mask = tmp_path / "classified-Jiangsu-2022-middle_rice-WGS84-v1.tif"
    for raster_path, origin_x in [(anhui_mask, 0), (jiangsu_mask, 1)]:
        with rasterio.open(
            raster_path,
            "w",
            driver="GTiff",
            height=1,
            width=1,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(origin_x, 2, 1, 2),
        ) as dst:
            dst.write(np.ones((1, 1), dtype="uint8"), 1)
    ds = xr.Dataset(
        {
            "t2m": (
                ("time", "lat", "lon"),
                np.array([[[300.0, 310.0]], [[302.0, 312.0]]]),
                {"units": "K"},
            )
        },
        coords={
            "time": pd.to_datetime(["2022-06-15", "2022-07-15"]),
            "lat": [1.0],
            "lon": [0.5, 1.5],
        },
    )
    ds.to_netcdf(nc_path)

    result_path, warnings = aggregate_netcdf_to_province_bounds(
        netcdf_paths=[nc_path],
        reference_raster_paths=[anhui_mask, jiangsu_mask],
        output_path=output_path,
        rice_growth_months=[6, 7, 8, 9],
        category="climate",
    )
    rows = pd.read_parquet(result_path).sort_values("province")

    assert warnings == []
    assert rows["province"].tolist() == ["安徽", "江苏"]
    assert rows["variable"].tolist() == [
        "growing_season_mean_temperature",
        "growing_season_mean_temperature",
    ]
    assert rows["value"].round(2).tolist() == [27.85, 37.85]
