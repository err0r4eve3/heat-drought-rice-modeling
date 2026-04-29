"""County-level yield proxy panel construction from gridded open products."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


SUPPORTED_PROXY_RASTER_SUFFIXES = (".tif", ".tiff")

YIELD_PROXY_PANEL_COLUMNS = [
    "admin_id",
    "province",
    "prefecture",
    "county",
    "year",
    "crop",
    "source",
    "source_file",
    "proxy_variable",
    "raw_proxy_value",
    "raw_proxy_yield",
    "raw_proxy_production_ton",
    "rice_area_proxy",
    "valid_pixel_count",
    "calibration_coefficient",
    "calibrated_yield",
    "calibrated_production_ton",
    "calibration_status",
]


@dataclass(frozen=True)
class YieldProxyMetadata:
    """Parsed metadata for one yield-proxy raster."""

    path: Path
    source: str
    year: int | None
    crop: str
    variable: str
    value_to_ton_factor: float | None


@dataclass(frozen=True)
class YieldProxyBuildResult:
    """Result metadata for the yield-proxy build step."""

    status: str
    raster_count: int
    row_count: int
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/yield_proxy_panel_summary.md")


def parse_yield_proxy_metadata(path: str | Path) -> YieldProxyMetadata:
    """Parse source, year, crop, and variable hints from a proxy raster filename."""

    raster_path = Path(path)
    name = raster_path.name.lower()
    path_text = str(raster_path).replace("\\", "/").lower()
    year_match = re.search(r"(19|20)\d{2}", name)
    year = int(year_match.group(0)) if year_match else None

    if "ggcp10" in path_text:
        source = "ggcp10"
    elif "asiariceyield" in path_text or "asia_rice_yield" in path_text:
        source = "asia_rice_yield_4km"
    elif "gdhy" in name:
        source = "gdhy"
    else:
        source = "unknown_yield_proxy"

    crop = "rice" if "rice" in path_text else "unknown"
    if "production" in name or "prod" in name:
        variable = "production"
    elif "yield" in name or source in {"asia_rice_yield_4km", "gdhy"}:
        variable = "yield"
    else:
        variable = "value"

    value_to_ton_factor = 1000.0 if source == "ggcp10" and variable == "production" else None
    return YieldProxyMetadata(
        path=raster_path,
        source=source,
        year=year,
        crop=crop,
        variable=variable,
        value_to_ton_factor=value_to_ton_factor,
    )


def find_yield_proxy_rasters(proxy_dir: str | Path) -> list[Path]:
    """Find supported proxy GeoTIFF rasters under a directory."""

    root = Path(proxy_dir).expanduser().resolve()
    if not root.exists():
        return []
    return sorted(
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_PROXY_RASTER_SUFFIXES
    )


def zonal_aggregate_proxy_raster(raster_path: str | Path, admin_units: Any) -> Any:
    """Aggregate one proxy GeoTIFF to administrative polygons."""

    import numpy as np
    import pandas as pd
    import rasterio
    from rasterio.mask import mask

    metadata = parse_yield_proxy_metadata(raster_path)
    admin = _standardize_admin_frame(admin_units)
    rows: list[dict[str, Any]] = []

    with rasterio.open(metadata.path) as dataset:
        raster_crs = dataset.crs
        if raster_crs and admin.crs and str(admin.crs) != str(raster_crs):
            admin = admin.to_crs(raster_crs)

        nodata = dataset.nodata
        for _, feature in admin.iterrows():
            row = _base_admin_row(feature)
            row.update(
                {
                    "year": metadata.year,
                    "crop": metadata.crop,
                    "source": metadata.source,
                    "source_file": str(metadata.path),
                    "proxy_variable": metadata.variable,
                    "raw_proxy_value": None,
                    "raw_proxy_yield": None,
                    "raw_proxy_production_ton": None,
                    "valid_pixel_count": 0,
                    "raster_crs": str(raster_crs) if raster_crs else "",
                }
            )
            try:
                out_image, _ = mask(dataset, [feature.geometry], crop=True, filled=False)
            except ValueError:
                rows.append(row)
                continue

            values = np.asarray(out_image[0], dtype="float64")
            if np.ma.isMaskedArray(out_image):
                valid = values[~np.asarray(out_image[0].mask)]
            else:
                valid = values.ravel()
            if nodata is not None:
                valid = valid[valid != nodata]
            valid = valid[np.isfinite(valid)]
            if valid.size == 0:
                rows.append(row)
                continue

            row["valid_pixel_count"] = int(valid.size)
            if metadata.variable == "production":
                production = float(valid.sum())
                if metadata.value_to_ton_factor is not None:
                    production *= metadata.value_to_ton_factor
                row["raw_proxy_value"] = float(valid.sum())
                row["raw_proxy_production_ton"] = production
            else:
                row["raw_proxy_value"] = float(valid.mean())
                row["raw_proxy_yield"] = float(valid.mean())
            rows.append(row)

    return pd.DataFrame(rows)


def apply_province_calibration(proxy_panel: Any, official_yield_panel: Any) -> Any:
    """Calibrate proxy yields or production to province-level official production totals."""

    import numpy as np
    import pandas as pd

    if proxy_panel is None or len(proxy_panel) == 0:
        return _empty_panel_frame()
    panel = proxy_panel.copy()
    official = official_yield_panel.copy() if official_yield_panel is not None else pd.DataFrame()

    for column in YIELD_PROXY_PANEL_COLUMNS:
        if column not in panel.columns:
            panel[column] = np.nan

    panel["rice_area_proxy"] = pd.to_numeric(panel["rice_area_proxy"], errors="coerce")
    panel["raw_proxy_yield"] = pd.to_numeric(panel["raw_proxy_yield"], errors="coerce")
    panel["raw_proxy_production_ton"] = pd.to_numeric(panel["raw_proxy_production_ton"], errors="coerce")
    panel["proxy_production_ton"] = panel["raw_proxy_production_ton"]
    derived = panel["raw_proxy_yield"] * panel["rice_area_proxy"] / 1000.0
    panel["proxy_production_ton"] = panel["proxy_production_ton"].where(panel["proxy_production_ton"].notna(), derived)

    panel["calibration_coefficient"] = np.nan
    panel["calibrated_yield"] = np.nan
    panel["calibrated_production_ton"] = np.nan
    panel["calibration_status"] = "missing_official_or_proxy"

    if official.empty or "production_ton" not in official.columns:
        return panel[YIELD_PROXY_PANEL_COLUMNS]

    official = official.copy()
    official["year"] = pd.to_numeric(official.get("year"), errors="coerce")
    official["production_ton"] = pd.to_numeric(official["production_ton"], errors="coerce")
    if "crop" in official.columns:
        official = official[official["crop"].fillna("").astype(str).str.lower() == "rice"]
    official = official[official["province"].notna() & official["year"].notna()]

    if official.empty:
        return panel[YIELD_PROXY_PANEL_COLUMNS]

    official["_crop_priority"] = 0
    official = official.sort_values(["province", "year", "_crop_priority"])
    official_totals = (
        official.groupby(["province", "year"], as_index=False)
        .first()[["province", "year", "production_ton"]]
        .rename(columns={"production_ton": "official_production_ton"})
    )
    official_area_column = _official_area_column(official)
    if official_area_column:
        official_areas = (
            official.groupby(["province", "year"], as_index=False)
            .first()[["province", "year", official_area_column]]
            .rename(columns={official_area_column: "official_area_hectare"})
        )
        official_areas["official_area_hectare"] = pd.to_numeric(
            official_areas["official_area_hectare"],
            errors="coerce",
        )
        official_totals = official_totals.merge(official_areas, on=["province", "year"], how="left")

    proxy_totals = (
        panel.groupby(["province", "year"], dropna=False, as_index=False)
        .agg(proxy_total_production_ton=("proxy_production_ton", lambda values: values.sum(min_count=1)), proxy_area_hectare=("rice_area_proxy", "sum"))
    )
    calibration = official_totals.merge(proxy_totals, on=["province", "year"], how="inner")
    calibration["calibration_coefficient"] = calibration["official_production_ton"] / calibration[
        "proxy_total_production_ton"
    ]
    calibration = calibration.replace([np.inf, -np.inf], np.nan)
    if "official_area_hectare" in calibration.columns:
        calibration["proxy_area_ratio"] = calibration["proxy_area_hectare"] / calibration["official_area_hectare"]
        calibration = calibration[
            calibration["proxy_area_ratio"].isna()
            | ((calibration["proxy_area_ratio"] >= 0.8) & (calibration["proxy_area_ratio"] <= 1.2))
        ]
    calibration = calibration[calibration["calibration_coefficient"].notna()]

    panel = panel.merge(
        calibration[["province", "year", "calibration_coefficient"]],
        on=["province", "year"],
        how="left",
        suffixes=("", "_new"),
    )
    if "calibration_coefficient_new" in panel.columns:
        panel["calibration_coefficient"] = panel["calibration_coefficient_new"]
        panel = panel.drop(columns=["calibration_coefficient_new"])

    has_coeff = panel["calibration_coefficient"].notna() & panel["proxy_production_ton"].notna()
    panel.loc[has_coeff, "calibrated_production_ton"] = (
        panel.loc[has_coeff, "proxy_production_ton"] * panel.loc[has_coeff, "calibration_coefficient"]
    )
    panel.loc[has_coeff & panel["raw_proxy_yield"].notna(), "calibrated_yield"] = (
        panel.loc[has_coeff & panel["raw_proxy_yield"].notna(), "raw_proxy_yield"]
        * panel.loc[has_coeff & panel["raw_proxy_yield"].notna(), "calibration_coefficient"]
    )
    needs_yield = has_coeff & panel["calibrated_yield"].isna() & (panel["rice_area_proxy"] > 0)
    panel.loc[needs_yield, "calibrated_yield"] = (
        panel.loc[needs_yield, "calibrated_production_ton"] * 1000.0 / panel.loc[needs_yield, "rice_area_proxy"]
    )
    panel.loc[has_coeff, "calibration_status"] = "calibrated"
    return panel[YIELD_PROXY_PANEL_COLUMNS]


def _official_area_column(frame: Any) -> str | None:
    """Return an official area column usable for coverage checks."""

    for column in ("sown_area_hectare", "harvested_area_hectare"):
        if column in frame.columns:
            return column
    return None


def build_yield_proxy_panel(
    proxy_dir: str | Path,
    admin_path: str | Path,
    crop_summary_path: str | Path,
    official_yield_path: str | Path,
    output_dir: str | Path,
    reports_dir: str | Path,
    target_years: tuple[int, int] = (2000, 2020),
) -> YieldProxyBuildResult:
    """Build a county-level yield proxy panel or a clear gap report if rasters are missing."""

    import pandas as pd

    output = Path(output_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    panel_path = output / "county_yield_proxy_panel.csv"
    parquet_path = output / "county_yield_proxy_panel.parquet"
    gap_path = output / "yield_proxy_gap_report.csv"
    report_path = reports / "yield_proxy_panel_summary.md"
    outputs = {"panel": panel_path, "parquet": parquet_path, "gap_report": gap_path}
    warnings: list[str] = []

    admin = _read_admin_units(admin_path, warnings)
    crop = _read_optional_table(crop_summary_path)
    official = _read_optional_table(official_yield_path)
    admin_for_targets = _admin_target_frame(admin)

    rasters = _filter_rasters_by_year(find_yield_proxy_rasters(proxy_dir), target_years)
    if not rasters:
        warnings.append(f"No yield proxy rasters found under {Path(proxy_dir).expanduser().resolve()}.")
        panel = _empty_panel_frame()
        gap = _build_gap_report(admin_for_targets, target_years, panel)
        _write_tables(panel, panel_path, parquet_path)
        gap.to_csv(gap_path, index=False, encoding="utf-8-sig")
        result = YieldProxyBuildResult(
            status="missing",
            raster_count=0,
            row_count=0,
            outputs=outputs,
            warnings=warnings,
            report_path=report_path,
        )
        _write_yield_proxy_report(result, proxy_dir, target_years, gap)
        return result

    aggregated_frames = []
    for raster in rasters:
        try:
            aggregated_frames.append(zonal_aggregate_proxy_raster(raster, admin))
        except Exception as exc:  # noqa: BLE001 - one bad raster must not abort the step
            warnings.append(f"Skipped {raster}: {type(exc).__name__}: {exc}")

    if aggregated_frames:
        panel = pd.concat(aggregated_frames, ignore_index=True)
    else:
        panel = _empty_panel_frame()
    panel = _collapse_proxy_rows(panel)
    panel = _attach_crop_area(panel, crop)
    panel = apply_province_calibration(panel, official)
    gap = _build_gap_report(admin_for_targets, target_years, panel)

    _write_tables(panel, panel_path, parquet_path)
    gap.to_csv(gap_path, index=False, encoding="utf-8-sig")

    status = "ok" if len(panel) else "empty"
    result = YieldProxyBuildResult(
        status=status,
        raster_count=len(rasters),
        row_count=int(len(panel)),
        outputs=outputs,
        warnings=warnings,
        report_path=report_path,
    )
    _write_yield_proxy_report(result, proxy_dir, target_years, gap)
    return result


def _standardize_admin_frame(frame: Any) -> Any:
    """Return a GeoDataFrame with standard admin columns."""

    import geopandas as gpd

    admin = gpd.GeoDataFrame(frame.copy(), geometry="geometry", crs=getattr(frame, "crs", None))
    if admin.crs is None:
        admin = admin.set_crs("EPSG:4326")
    for column in ("admin_id", "province_name", "prefecture_name", "county_name"):
        if column not in admin.columns:
            admin[column] = ""
    return admin


def _read_admin_units(path: str | Path, warnings: list[str]) -> Any:
    """Read administrative units from GPKG, GeoJSON, Shapefile, or Parquet."""

    import geopandas as gpd

    admin_path = Path(path).expanduser().resolve()
    if not admin_path.exists():
        warnings.append(f"Admin units file not found: {admin_path}")
        return gpd.GeoDataFrame(columns=["admin_id", "province_name", "prefecture_name", "county_name", "geometry"], crs="EPSG:4326")
    if admin_path.suffix.lower() == ".parquet":
        frame = gpd.read_parquet(admin_path)
    else:
        frame = gpd.read_file(admin_path)
    return _standardize_admin_frame(frame)


def _read_optional_table(path: str | Path) -> Any:
    """Read an optional CSV or Parquet table."""

    import pandas as pd

    table_path = Path(path).expanduser().resolve()
    if not table_path.exists():
        return pd.DataFrame()
    if table_path.suffix.lower() == ".parquet":
        return pd.read_parquet(table_path)
    return pd.read_csv(table_path)


def _base_admin_row(feature: Any) -> dict[str, Any]:
    """Extract standard admin fields from one feature."""

    return {
        "admin_id": str(feature.get("admin_id", "")),
        "province": str(feature.get("province_name", feature.get("province", "")) or ""),
        "prefecture": str(feature.get("prefecture_name", feature.get("prefecture", "")) or ""),
        "county": str(feature.get("county_name", feature.get("county", "")) or feature.get("shapeName", "") or ""),
    }


def _attach_crop_area(panel: Any, crop: Any) -> Any:
    """Attach rice-area proxy from crop-mask summary."""

    import numpy as np

    if panel.empty:
        panel["rice_area_proxy"] = np.nan
        return panel
    if crop.empty or "admin_id" not in crop.columns:
        panel["rice_area_proxy"] = np.nan
        return panel
    area_column = "crop_area_ha" if "crop_area_ha" in crop.columns else None
    if area_column is None:
        panel["rice_area_proxy"] = np.nan
        return panel
    area = crop[["admin_id", area_column]].copy().rename(columns={area_column: "rice_area_proxy"})
    area["admin_id"] = area["admin_id"].astype(str)
    panel["admin_id"] = panel["admin_id"].astype(str)
    return panel.merge(area, on="admin_id", how="left")


def _collapse_proxy_rows(panel: Any) -> Any:
    """Collapse multiple seasonal rasters to one admin-year-source proxy row."""

    if panel.empty:
        return panel
    group_columns = ["admin_id", "province", "prefecture", "county", "year", "crop", "source", "proxy_variable"]
    aggregations = {
        "source_file": lambda values: "; ".join(sorted({str(value) for value in values if str(value)})),
        "raw_proxy_value": "mean",
        "raw_proxy_yield": "mean",
        "raw_proxy_production_ton": "sum",
        "valid_pixel_count": "sum",
    }
    collapsed = panel.groupby(group_columns, dropna=False, as_index=False).agg(aggregations)
    collapsed.loc[collapsed["raw_proxy_production_ton"] == 0, "raw_proxy_production_ton"] = None
    return collapsed


def _filter_rasters_by_year(rasters: list[Path], target_years: tuple[int, int]) -> list[Path]:
    """Filter rasters to the configured inclusive target-year range."""

    selected: list[Path] = []
    year_min, year_max = int(target_years[0]), int(target_years[1])
    for raster in rasters:
        year = parse_yield_proxy_metadata(raster).year
        if year is None or year_min <= year <= year_max:
            selected.append(raster)
    return selected


def _admin_target_frame(admin: Any) -> Any:
    """Return non-geometry admin columns for gap-report targets."""

    import pandas as pd

    if admin.empty:
        return pd.DataFrame(columns=["admin_id", "province", "prefecture", "county"])
    rows = [_base_admin_row(row) for _, row in admin.iterrows()]
    return pd.DataFrame(rows)


def _build_gap_report(admin_targets: Any, target_years: tuple[int, int], panel: Any) -> Any:
    """Build admin-year availability report for proxy yields."""

    import pandas as pd

    years = list(range(int(target_years[0]), int(target_years[1]) + 1))
    if admin_targets.empty:
        return pd.DataFrame(columns=["admin_id", "province", "prefecture", "county", "year", "proxy_observation_count", "status"])

    target = admin_targets.merge(pd.DataFrame({"year": years}), how="cross")
    if panel.empty:
        target["proxy_observation_count"] = 0
        target["status"] = "missing_proxy_raster"
        return target

    counts = panel.groupby(["admin_id", "year"], as_index=False).size().rename(columns={"size": "proxy_observation_count"})
    gap = target.merge(counts, on=["admin_id", "year"], how="left")
    gap["proxy_observation_count"] = gap["proxy_observation_count"].fillna(0).astype(int)
    gap["status"] = gap["proxy_observation_count"].map(lambda value: "available" if value > 0 else "missing")
    return gap


def _empty_panel_frame() -> Any:
    """Return an empty yield-proxy panel."""

    import pandas as pd

    return pd.DataFrame(columns=YIELD_PROXY_PANEL_COLUMNS)


def _write_tables(panel: Any, csv_path: Path, parquet_path: Path) -> None:
    """Write panel as CSV and Parquet when possible."""

    panel.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        panel.to_parquet(parquet_path, index=False)
    except Exception:
        if parquet_path.exists():
            parquet_path.unlink()


def _write_yield_proxy_report(
    result: YieldProxyBuildResult,
    proxy_dir: str | Path,
    target_years: tuple[int, int],
    gap: Any,
) -> None:
    """Write Markdown report for yield-proxy panel construction."""

    missing_count = int((gap["status"] != "available").sum()) if not gap.empty and "status" in gap.columns else 0
    available_count = int((gap["status"] == "available").sum()) if not gap.empty and "status" in gap.columns else 0
    lines = [
        "# Yield Proxy Panel Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Proxy directory: `{Path(proxy_dir).expanduser().resolve()}`",
        f"- Target years: {target_years[0]}-{target_years[1]}",
        f"- Raster count: {result.raster_count}",
        f"- Panel rows: {result.row_count}",
        f"- Available admin-year cells: {available_count}",
        f"- Missing admin-year cells: {missing_count}",
        "",
        "## Outputs",
        "",
    ]
    for key, path in result.outputs.items():
        lines.append(f"- {key}: `{path}`")
    lines.extend(["", "## Warnings", ""])
    if result.warnings:
        lines.extend(f"- {warning}" for warning in result.warnings)
    else:
        lines.append("- None.")
    if result.status == "missing":
        lines.extend(
            [
                "",
                "## Data Gap",
                "",
                "- No yield proxy rasters found, so only an empty panel and a gap report were generated.",
                "- Expected open proxy sources include AsiaRiceYield4km and GGCP10 rice GeoTIFFs.",
                "- Add downloaded proxy rasters to the configured proxy directory and rerun this step.",
            ]
        )
    lines.append("")
    result.report_path.write_text("\n".join(lines), encoding="utf-8")
