"""Assign province names to administrative units using province boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


PROVINCE_NAME_MAP = {
    "anhui": "安徽",
    "beijing": "北京",
    "chongqing": "重庆",
    "fujian": "福建",
    "gansu": "甘肃",
    "guangdong": "广东",
    "guangzhou": "广东",
    "guangxi": "广西",
    "guizhou": "贵州",
    "hainan": "海南",
    "hebei": "河北",
    "heilongjiang": "黑龙江",
    "henan": "河南",
    "hubei": "湖北",
    "hunan": "湖南",
    "inner mongolia": "内蒙古",
    "jiangsu": "江苏",
    "jiangxi": "江西",
    "jilin": "吉林",
    "liaoning": "辽宁",
    "ningxia": "宁夏",
    "qinghai": "青海",
    "shaanxi": "陕西",
    "shandong": "山东",
    "shanghai": "上海",
    "shanxi": "山西",
    "sichuan": "四川",
    "tianjin": "天津",
    "tibet": "西藏",
    "xinjiang": "新疆",
    "yunnan": "云南",
    "zhejiang": "浙江",
    "taiwan": "台湾",
    "hong kong": "香港",
    "macao": "澳门",
}


@dataclass(frozen=True)
class AdminProvinceAssignmentResult:
    """Result metadata for province assignment."""

    status: str
    input_rows: int
    matched_rows: int
    output_path: Path
    parquet_path: Path | None = None
    report_path: Path | None = None
    warnings: list[str] = field(default_factory=list)


def normalize_geoboundaries_province_name(name: str) -> str:
    """Normalize an English geoBoundaries province name to Chinese when possible."""

    text = str(name or "").strip()
    lowered = text.lower()
    for suffix in (
        " special administrative region",
        " zhuang autonomous region",
        " hui autonomous region",
        " uyghur autonomous region",
        " autonomous region",
        " municipality",
        " province",
    ):
        lowered = lowered.replace(suffix, "")
    lowered = " ".join(lowered.split())
    return PROVINCE_NAME_MAP.get(lowered, text)


def assign_admin_provinces(
    admin_path: str | Path,
    province_path: str | Path,
    output_path: str | Path,
    parquet_path: str | Path | None = None,
    report_path: str | Path | None = None,
) -> AdminProvinceAssignmentResult:
    """Assign `province_name` to admin units with a point-in-polygon spatial join."""

    import geopandas as gpd

    admin_file = Path(admin_path).expanduser().resolve()
    province_file = Path(province_path).expanduser().resolve()
    output_file = Path(output_path).expanduser().resolve()
    parquet_file = Path(parquet_path).expanduser().resolve() if parquet_path else output_file.with_suffix(".parquet")
    report_file = Path(report_path).expanduser().resolve() if report_path else None
    output_file.parent.mkdir(parents=True, exist_ok=True)
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    if report_file:
        report_file.parent.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    admin = _read_vector(admin_file)
    provinces = _read_vector(province_file)
    if admin.crs is None:
        warnings.append(f"Admin file has no CRS; assuming EPSG:4326: {admin_file}")
        admin = admin.set_crs("EPSG:4326")
    if provinces.crs is None:
        warnings.append(f"Province file has no CRS; assuming EPSG:4326: {province_file}")
        provinces = provinces.set_crs("EPSG:4326")
    provinces = provinces.to_crs(admin.crs)

    province_name_column = _province_name_column(provinces)
    provinces = provinces[[province_name_column, "geometry"]].copy()
    provinces["province_name"] = provinces[province_name_column].map(normalize_geoboundaries_province_name)
    provinces = provinces.drop(columns=[province_name_column])

    points = admin.copy()
    points["geometry"] = points.geometry.representative_point()
    joined = gpd.sjoin(points[["admin_id", "geometry"]], provinces, how="left", predicate="within")
    joined = joined.drop(columns=[column for column in ("index_right",) if column in joined.columns])
    joined = joined[["admin_id", "province_name"]].drop_duplicates("admin_id")

    admin = admin.drop(columns=["province_name"], errors="ignore").merge(joined, on="admin_id", how="left")
    matched_rows = int(admin["province_name"].notna().sum()) if "province_name" in admin.columns else 0
    status = "ok" if matched_rows == len(admin) else "partial"
    if matched_rows < len(admin):
        warnings.append(f"Province assignment matched {matched_rows}/{len(admin)} admin units.")

    admin.to_file(output_file, driver="GPKG")
    try:
        admin.to_parquet(parquet_file, index=False)
    except Exception as exc:  # noqa: BLE001 - GPKG is the primary output
        warnings.append(f"Could not write parquet output: {type(exc).__name__}: {exc}")
        parquet_file = None

    result = AdminProvinceAssignmentResult(
        status=status,
        input_rows=int(len(admin)),
        matched_rows=matched_rows,
        output_path=output_file,
        parquet_path=parquet_file,
        report_path=report_file,
        warnings=warnings,
    )
    if report_file:
        _write_report(result, admin_file, province_file)
    return result


def _read_vector(path: Path):
    """Read a vector file with GeoPandas."""

    import geopandas as gpd

    if path.suffix.lower() == ".parquet":
        return gpd.read_parquet(path)
    return gpd.read_file(path)


def _province_name_column(frame) -> str:
    """Choose a province-name column."""

    for column in ("province_name", "shapeName", "NAME_1", "name", "Name"):
        if column in frame.columns:
            return column
    raise ValueError("Province boundary has no recognizable name column.")


def _write_report(result: AdminProvinceAssignmentResult, admin_path: Path, province_path: Path) -> None:
    """Write a Markdown report for province assignment."""

    assert result.report_path is not None
    lines = [
        "# Admin Province Assignment Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Admin input: `{admin_path}`",
        f"- Province input: `{province_path}`",
        f"- Input rows: {result.input_rows}",
        f"- Matched rows: {result.matched_rows}",
        f"- Output: `{result.output_path}`",
        f"- Parquet: `{result.parquet_path}`",
        "",
        "## Warnings",
        "",
    ]
    if result.warnings:
        lines.extend(f"- {warning}" for warning in result.warnings)
    else:
        lines.append("- None.")
    lines.append("")
    result.report_path.write_text("\n".join(lines), encoding="utf-8")
