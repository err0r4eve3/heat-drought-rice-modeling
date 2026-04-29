"""Study-region filtering and model-panel exposure enrichment."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


EVENT_FIELD_COLUMNS = [
    "chd_annual",
    "chd_2022_intensity",
    "chd_2022_treated_p75",
    "event_time_2022",
    "post_2022",
    "chd_validation_2024",
]


@dataclass(frozen=True)
class StudyRegionFilterResult:
    """Result metadata for study-region filtering."""

    status: str
    input_rows: int
    output_rows: int
    region_name: str
    provinces: list[str]
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/study_region_filter_summary.md")


def enrich_and_filter_model_panel(
    model_panel: str | Path,
    processed_dir: str | Path,
    reports_dir: str | Path,
    study_region_policy: dict[str, Any] | None,
    main_event_year: int = 2022,
    validation_event_year: int = 2024,
) -> StudyRegionFilterResult:
    """Add annual/event CHD fields and filter the model panel to the configured study region."""

    model_path = Path(model_panel).expanduser().resolve()
    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    full_csv = processed / "model_panel.csv"
    full_parquet = processed / "model_panel.parquet"
    study_csv = processed / "model_panel_study_region.csv"
    study_parquet = processed / "model_panel_study_region.parquet"
    report_path = reports / "study_region_filter_summary.md"
    warnings: list[str] = []

    model = _read_model_panel(model_path, warnings)
    policy = study_region_policy or {}
    region_name, provinces = _resolve_region(policy)
    annual = _read_annual_exposure(processed, warnings)
    enriched = _merge_annual_exposure(model, annual, warnings)
    enriched = add_event_exposure_fields(enriched, main_event_year, validation_event_year)
    _write_table(enriched, full_csv, full_parquet, warnings)

    study = _filter_region(enriched, provinces) if policy.get("filter_model_panel_to_study_region", True) else enriched.copy()
    _write_table(study, study_csv, study_parquet, warnings)
    _write_yield_gap_action_plan(reports / "yield_data_gap_action_plan.md", study, provinces)

    result = StudyRegionFilterResult(
        status="ok" if len(study) else "empty",
        input_rows=len(model),
        output_rows=len(study),
        region_name=region_name,
        provinces=provinces,
        outputs={
            "model_panel_csv": full_csv,
            "model_panel_parquet": full_parquet,
            "study_region_csv": study_csv,
            "study_region_parquet": study_parquet,
        },
        warnings=warnings,
        report_path=report_path,
    )
    _write_report(result, enriched, study, annual)
    return result


def add_event_exposure_fields(
    frame: pd.DataFrame,
    main_event_year: int = 2022,
    validation_event_year: int = 2024,
) -> pd.DataFrame:
    """Add annual/event CHD fields without fabricating annual exposure."""

    if frame.empty:
        for column in EVENT_FIELD_COLUMNS:
            frame[column] = pd.Series(dtype="object")
        return frame

    enriched = frame.copy()
    if "year" in enriched.columns:
        enriched["event_time_2022"] = pd.to_numeric(enriched["year"], errors="coerce") - int(main_event_year)
        enriched["post_2022"] = (pd.to_numeric(enriched["year"], errors="coerce") >= int(main_event_year)).astype("Int64")
    else:
        enriched["event_time_2022"] = pd.NA
        enriched["post_2022"] = pd.NA

    if "chd_annual" not in enriched.columns:
        source = _first_numeric_column(enriched, ["exposure_index", "CHD_intensity", "chd_intensity"])
        enriched["chd_annual"] = pd.to_numeric(enriched[source], errors="coerce") if source else pd.NA

    key = _admin_key_column(enriched)
    if key is None or "year" not in enriched.columns:
        enriched["chd_2022_intensity"] = pd.NA
        enriched["chd_validation_2024"] = pd.NA
    else:
        years = pd.to_numeric(enriched["year"], errors="coerce")
        chd = pd.to_numeric(enriched["chd_annual"], errors="coerce")
        event_values = _event_values(enriched, key, years, chd, main_event_year)
        validation_values = _event_values(enriched, key, years, chd, validation_event_year)
        enriched["chd_2022_intensity"] = enriched[key].astype(str).map(event_values)
        enriched["chd_validation_2024"] = enriched[key].astype(str).map(validation_values)

    threshold = pd.to_numeric(enriched["chd_2022_intensity"], errors="coerce").quantile(0.75)
    if pd.isna(threshold):
        enriched["chd_2022_treated_p75"] = pd.NA
    else:
        enriched["chd_2022_treated_p75"] = (
            pd.to_numeric(enriched["chd_2022_intensity"], errors="coerce") >= float(threshold)
        ).astype("Int64")

    if "exposure_index" not in enriched.columns:
        enriched["exposure_index"] = enriched["chd_annual"]
    else:
        enriched["exposure_index"] = enriched["exposure_index"].where(
            enriched["exposure_index"].notna() & enriched["exposure_index"].astype(str).str.strip().ne(""),
            enriched["chd_annual"],
        )
    return enriched


def _read_model_panel(path: Path, warnings: list[str]) -> pd.DataFrame:
    """Read the model panel CSV or return an empty frame."""

    if not path.exists():
        warnings.append(f"Model panel not found: {path}")
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not read model panel {path}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


def _read_annual_exposure(processed: Path, warnings: list[str]) -> pd.DataFrame:
    """Read annual exposure panel if available."""

    for path in [processed / "annual_exposure_panel.parquet", processed / "annual_exposure_panel.csv"]:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".parquet":
                return pd.read_parquet(path)
            return pd.read_csv(path, dtype=str, low_memory=False)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Could not read annual exposure panel {path}: {type(exc).__name__}: {exc}")
    warnings.append("Annual exposure panel not found; event fields will use model_panel exposure_index where available.")
    return pd.DataFrame()


def _merge_annual_exposure(model: pd.DataFrame, annual: pd.DataFrame, warnings: list[str]) -> pd.DataFrame:
    """Merge annual exposure columns into model panel on the best available key."""

    if model.empty or annual.empty:
        return model.copy()
    left = model.copy()
    right = annual.copy()
    if "province" in left.columns and "province" in right.columns:
        left["_province_norm"] = left["province"].map(_normalize_province_name)
        right["_province_norm"] = right["province"].map(_normalize_province_name)
    keys = _merge_keys(left, right)
    if not keys:
        warnings.append("Could not merge annual exposure panel into model_panel: no common admin/year keys.")
        return model.copy()
    for key in keys:
        left[key] = left[key].astype(str)
        right[key] = right[key].astype(str)

    exposure_columns = [column for column in right.columns if column in EVENT_FIELD_COLUMNS or column.endswith("_anomaly") or column in {"hot_days", "dry_days", "compound_hot_dry_days"}]
    merge_columns = [*keys, *[column for column in exposure_columns if column not in keys]]
    right = right[merge_columns].drop_duplicates(subset=keys)
    merged = left.merge(right, on=keys, how="left", suffixes=("", "_annual"))
    if "_province_norm" in merged.columns:
        merged = merged.drop(columns=["_province_norm"])
    for column in EVENT_FIELD_COLUMNS:
        annual_column = f"{column}_annual"
        if annual_column in merged.columns:
            if column in merged.columns:
                merged[column] = merged[column].where(
                    merged[column].notna() & merged[column].astype(str).str.strip().ne(""),
                    merged[annual_column],
                )
                merged = merged.drop(columns=[annual_column])
            else:
                merged[column] = merged.pop(annual_column)
    return merged


def _merge_keys(left: pd.DataFrame, right: pd.DataFrame) -> list[str]:
    """Return merge keys for model and exposure panels."""

    for keys in (["admin_id", "year"], ["admin_code", "year"], ["_province_norm", "year"], ["province", "year"]):
        if all(key in left.columns and key in right.columns for key in keys) and _keys_have_values(left, right, keys):
            return keys
    return []


def _keys_have_values(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> bool:
    """Return True when non-year merge keys have usable values on both sides."""

    for key in keys:
        if key == "year":
            continue
        if _valid_text_mask(left[key]).sum() == 0 or _valid_text_mask(right[key]).sum() == 0:
            return False
    return True


def _resolve_region(policy: dict[str, Any]) -> tuple[str, list[str]]:
    """Resolve configured study region name and provinces."""

    region_name = str(policy.get("default_region", "yangtze_middle_lower"))
    regions = policy.get("regions", {})
    region = regions.get(region_name, {}) if isinstance(regions, dict) else {}
    provinces = [str(value) for value in region.get("provinces", [])]
    return region_name, provinces


def _filter_region(frame: pd.DataFrame, provinces: list[str]) -> pd.DataFrame:
    """Filter a model panel to configured provinces."""

    if frame.empty or not provinces or "province" not in frame.columns:
        return frame.copy()
    allowed = {_normalize_province_name(province) for province in provinces}
    province_values = frame["province"].map(_normalize_province_name)
    return frame[province_values.isin(allowed)].copy()


def _normalize_province_name(value: Any) -> str:
    """Normalize province names for stable study-region matching."""

    text = str(value or "").strip()
    for suffix in ("省", "市", "壮族自治区", "回族自治区", "维吾尔自治区", "自治区"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _first_numeric_column(frame: pd.DataFrame, tokens: list[str]) -> str | None:
    """Find first numeric column containing any token."""

    for column in frame.columns:
        lower = str(column).lower()
        if any(token.lower() in lower for token in tokens) and pd.to_numeric(frame[column], errors="coerce").notna().any():
            return str(column)
    return None


def _admin_key_column(frame: pd.DataFrame) -> str | None:
    """Return best administrative key column."""

    for column in ("admin_id", "admin_code", "province", "prefecture", "county"):
        if column in frame.columns and _valid_text_mask(frame[column]).sum() >= max(1, len(frame) * 0.5):
            return column
    return None


def _valid_text_mask(series: pd.Series) -> pd.Series:
    """Return mask for non-empty text excluding textual nulls."""

    values = series.fillna("").astype(str).str.strip().str.lower()
    return values.ne("") & values.ne("nan") & values.ne("none") & values.ne("<na>")


def _event_values(frame: pd.DataFrame, key: str, years: pd.Series, chd: pd.Series, event_year: int) -> dict[str, float]:
    """Return admin-level event-year CHD values."""

    event_rows = frame.loc[years == int(event_year), [key]].copy()
    event_rows["_chd"] = chd.loc[event_rows.index]
    event_rows = event_rows.dropna(subset=["_chd"])
    if event_rows.empty:
        return {}
    grouped = event_rows.groupby(event_rows[key].astype(str))["_chd"].mean()
    return {str(index): float(value) for index, value in grouped.items()}


def _write_table(frame: pd.DataFrame, csv_path: Path, parquet_path: Path, warnings: list[str]) -> None:
    """Write CSV and Parquet outputs."""

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        parquet_frame = frame.copy()
        for column in parquet_frame.columns:
            if parquet_frame[column].dtype == "object":
                parquet_frame[column] = parquet_frame[column].astype(str).replace({"nan": "", "<NA>": ""})
        parquet_frame.to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Could not write parquet {parquet_path}: {type(exc).__name__}: {exc}")


def _write_report(
    result: StudyRegionFilterResult,
    full_panel: pd.DataFrame,
    study_panel: pd.DataFrame,
    annual: pd.DataFrame,
) -> None:
    """Write study-region filtering summary."""

    lines = [
        "# Study Region Filter Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Region: `{result.region_name}`",
        f"- Configured provinces: {', '.join(result.provinces) if result.provinces else 'n/a'}",
        f"- Input rows: {result.input_rows}",
        f"- Study-region rows: {result.output_rows}",
        f"- Full panel province count: {_nunique(full_panel, 'province')}",
        f"- Study-region province count: {_nunique(study_panel, 'province')}",
        f"- Annual exposure rows available: {len(annual)}",
        f"- chd_annual non-missing in study region: {_non_missing_count(study_panel, 'chd_annual')}/{len(study_panel)}",
        f"- chd_2022_intensity non-missing in study region: {_non_missing_count(study_panel, 'chd_2022_intensity')}/{len(study_panel)}",
        "",
        "## Rule",
        "",
        "- 主模型只使用配置中 default_region 对应省份；全国省级数据仅作背景或宏观对照。",
        "- 若出现全国省级面板，已输出 `model_panel_study_region.csv` 作为主模型候选输入。",
        "",
        "## Outputs",
        "",
    ]
    lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
    lines.append("")
    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _write_yield_gap_action_plan(path: Path, panel: pd.DataFrame, provinces: list[str]) -> None:
    """Write a practical manual yield-data gap action plan."""

    levels = _value_counts(panel, "admin_level")
    crops = _value_counts(panel, "crop")
    years = pd.to_numeric(panel["year"], errors="coerce").dropna() if "year" in panel.columns else pd.Series(dtype=float)
    year_range = "n/a" if years.empty else f"{int(years.min())}-{int(years.max())}"
    current_level = next(iter(levels), "unknown")
    current_crop = next(iter(crops), "unknown")
    target_order = ["湖北省", "湖南省", "江西省", "安徽省", "江苏省", "浙江省", "上海市"]
    missing_provinces = [province for province in target_order if _normalize_province_name(province) not in {_normalize_province_name(value) for value in panel.get("province", pd.Series(dtype=str)).dropna().unique()}]
    lines = [
        "# Yield Data Gap Action Plan",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- 当前使用的产量层级：`{current_level}`；分布：{_format_counts(levels)}",
        f"- 当前使用的作物：`{current_crop}`；分布：{_format_counts(crops)}",
        f"- 当前年份覆盖：{year_range}",
        f"- 主研究区省份：{', '.join(provinces)}",
        f"- 仍需优先补齐省份：{', '.join(missing_provinces) if missing_provinces else '当前研究区省份已有省级记录，仍需补市/县级稻谷。'}",
        "",
        "## 当前主模型缺口",
        "",
        "- 年度 CHD 暴露仍未覆盖 2000-2024 × 研究区行政单元，固定效应模型暂不应运行。",
        "- 官方产量面板仍停留在省级粮食口径，不能写成市县级稻谷单产。",
        "",
        "## 补数目标分档",
        "",
        "- 最小可用补数目标：地级市 × 粮食 × 2000-2024。",
        "- 较优补数目标：地级市 × 稻谷/水稻 × 2000-2024。",
        "- 增强补数目标：县级 × 稻谷/水稻 × 2000-2024。",
        "",
        "## 推荐人工补数顺序",
        "",
        "1. 湖北",
        "2. 湖南",
        "3. 江西",
        "4. 安徽",
        "5. 江苏",
        "6. 浙江",
        "7. 上海",
        "",
        "## 每省优先数据源",
        "",
        "- 统计年鉴",
        "- 调查年鉴",
        "- 农村统计年鉴",
        "- 市州统计公报",
        "- 地方统计局 Excel/PDF",
        "- CNKI/CSYD/EPS",
        "",
        "第三方年鉴站只能作为线索，最终引用应优先回到官方年鉴或统计局来源。",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _non_missing_count(frame: pd.DataFrame, column: str) -> int:
    """Count non-empty values in a column."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column]
    return int((values.notna() & values.astype(str).str.strip().ne("")).sum())


def _nunique(frame: pd.DataFrame, column: str) -> int:
    """Return number of unique non-empty values."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column].dropna().astype(str).str.strip()
    return int(values[values.ne("")].nunique())


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    """Return compact value counts."""

    if frame.empty or column not in frame.columns:
        return {}
    values = frame[column].fillna("").astype(str).str.strip()
    values = values[values.ne("")]
    return {str(key): int(value) for key, value in values.value_counts().items()}


def _format_counts(counts: dict[str, int]) -> str:
    """Format value-count dictionary."""

    return "; ".join(f"{key}={value}" for key, value in counts.items()) if counts else "n/a"
