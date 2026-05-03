"""Core final-report generation helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def generate_final_report(
    processed_dir: str | Path,
    output_dir: str | Path,
    reports_dir: str | Path,
    main_event_year: int,
    main_year_min: int = 2000,
    main_year_max: int = 2024,
) -> Path:
    """Generate final analysis and risk-assessment Markdown reports."""

    processed = Path(processed_dir).expanduser().resolve()
    outputs = Path(output_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    interim = processed.parent / "interim" if processed.name == "processed" else processed / "interim"
    reports.mkdir(parents=True, exist_ok=True)

    tables = _load_project_tables(processed, interim, outputs)
    summary = _build_summary(tables, main_event_year, main_year_min, main_year_max)

    report_path = reports / "final_analysis_summary.md"
    risk_path = reports / "project_risk_assessment.md"
    report_path.write_text(_render_final_report(summary, outputs, main_event_year), encoding="utf-8")
    risk_path.write_text(_render_risk_report(summary), encoding="utf-8")
    return report_path


def _load_project_tables(processed: Path, interim: Path, outputs: Path) -> dict[str, Any]:
    """Load optional output tables into a dictionary of pandas DataFrames."""

    references = processed.parent / "raw" / "references"
    return {
        "inventory": _read_table(processed / "data_inventory.csv"),
        "admin": _read_first_existing_table(
            [
                processed / "admin_units.parquet",
                processed / "admin_units.csv",
            ]
        ),
        "crop": _read_table(processed / "crop_mask_summary_by_admin.csv"),
        "phenology": _read_table(processed / "phenology_by_admin.csv"),
        "climate_province": _read_first_existing_table(
            [
                interim / "climate_province_growing_season.parquet",
                interim / "climate_province_growing_season.csv",
            ]
        ),
        "remote_province": _read_first_existing_table(
            [
                interim / "remote_sensing_province_growing_season.parquet",
                interim / "remote_sensing_province_growing_season.csv",
            ]
        ),
        "yield": _read_first_existing_table(
            [
                processed / "yield_panel_combined.parquet",
                processed / "yield_panel_combined.csv",
                processed / "yield_panel.csv",
            ]
        ),
        "yield_proxy": _read_first_existing_table(
            [
                processed / "yield_proxy" / "county_yield_proxy_panel.parquet",
                processed / "yield_proxy" / "county_yield_proxy_panel.csv",
            ]
        ),
        "yield_proxy_gap": _read_table(processed / "yield_proxy" / "yield_proxy_gap_report.csv"),
        "yield_tier": _read_table(processed / "yield_data_tier_report.csv"),
        "yield_coverage": _read_table(processed / "yield_coverage_report.csv"),
        "admin_crosswalk": _read_table(processed / "admin_crosswalk_2000_2025.csv"),
        "model": _read_first_existing_table(
            [
                processed / "province_model_panel.parquet",
                processed / "province_model_panel.csv",
                processed / "model_panel.csv",
            ]
        ),
        "model_study_region": _read_table(processed / "model_panel_study_region.csv"),
        "annual_exposure": _read_first_existing_table(
            [
                processed / "province_chd_panel.parquet",
                processed / "province_chd_panel.csv",
                processed / "annual_exposure_panel.parquet",
                processed / "annual_exposure_panel.csv",
            ]
        ),
        "exposure_diagnosis": _read_table(outputs / "exposure_coverage_diagnosis.csv"),
        "coefficients": _read_table(outputs / "model_coefficients.csv"),
        "event": _read_table(outputs / "event_study_coefficients.csv"),
        "robustness": _read_table(outputs / "robustness_results.csv"),
        "placebo": _read_table(outputs / "placebo_results.csv"),
        "data_sources": _read_table(references / "deep_required_data_sources.csv"),
        "agri_sources": _read_table(references / "agri_stats_sources.csv"),
    }


def _build_summary(tables: dict[str, Any], main_event_year: int, main_year_min: int, main_year_max: int) -> dict[str, Any]:
    """Build scalar summary metrics from available tables."""

    model = tables["model"]
    model_main = _filter_year_range(model, main_year_min, main_year_max)
    crop = tables["crop"]
    phenology = tables["phenology"]
    yield_panel = tables["yield"]
    yield_proxy = tables["yield_proxy"]
    yield_proxy_gap = tables["yield_proxy_gap"]
    yield_tier = tables["yield_tier"]
    yield_coverage = tables["yield_coverage"]
    admin_crosswalk = tables["admin_crosswalk"]
    climate = tables["climate_province"]
    remote = tables["remote_province"]
    coefficients = tables["coefficients"]
    event = tables["event"]
    model_study_region = tables["model_study_region"]
    annual_exposure = tables["annual_exposure"]
    exposure_diagnosis = tables["exposure_diagnosis"]
    data_sources = tables["data_sources"]
    agri_sources = tables["agri_sources"]
    agri_source_leads = _filter_agri_source_leads(agri_sources)

    event_model = _filter_year(model, main_event_year)
    province_chd_coverage_rate = _coverage_rate(model_main, "chd_annual", main_year_min, main_year_max)
    yield_anomaly_coverage_rate = _coverage_rate(model_main, "yield_anomaly_pct", main_year_min, main_year_max)
    exposure_status = _current_exposure_status(
        province_chd_coverage_rate,
        _diagnosis_metric(exposure_diagnosis, "exposure_coverage_status"),
    )
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inventory_rows": len(tables["inventory"]),
        "admin_rows": len(tables["admin"]),
        "crop_rows": len(crop),
        "crop_zonal_rows": _status_count(crop, "zonal_stats"),
        "crop_positive_rows": _positive_count(crop, "crop_area_ha"),
        "crop_area_sum": _sum_numeric(crop, "crop_area_ha"),
        "phenology_rows": len(phenology),
        "phenology_zonal_rows": _status_count(phenology, "zonal_stats"),
        "climate_rows": len(climate),
        "climate_variables": _unique_text(climate, "variable"),
        "remote_rows": len(remote),
        "remote_variables": _unique_text(remote, "variable"),
        "yield_rows": len(yield_panel),
        "yield_year_min": _min_numeric(yield_panel, "year"),
        "yield_year_max": _max_numeric(yield_panel, "year"),
        "yield_admin_levels": _value_counts(yield_panel, "admin_level"),
        "yield_crops": _value_counts(yield_panel, "crop"),
        "yield_proxy_rows": len(yield_proxy),
        "yield_proxy_sources": _value_counts(yield_proxy, "source"),
        "yield_proxy_calibrated_rows": _value_count(yield_proxy, "calibration_status", "calibrated"),
        "yield_proxy_gap_rows": len(yield_proxy_gap),
        "yield_proxy_available_cells": _value_count(yield_proxy_gap, "status", "available"),
        "yield_proxy_missing_cells": _non_available_count(yield_proxy_gap, "status", "available"),
        "yield_tier": _first_row(yield_tier),
        "yield_coverage_rows": len(yield_coverage),
        "yield_best_coverage": _max_numeric(yield_coverage, "year_coverage_rate"),
        "admin_crosswalk_rows": len(admin_crosswalk),
        "model_rows": len(model),
        "model_year_min": _min_numeric(model, "year"),
        "model_year_max": _max_numeric(model, "year"),
        "model_main_rows": len(model_main),
        "model_main_year_min": _min_numeric(model_main, "year"),
        "model_main_year_max": _max_numeric(model_main, "year"),
        "model_province_count": _nunique(model, "province"),
        "yield_anomaly_nonmissing": _non_missing_count(model, "yield_anomaly_pct"),
        "exposure_nonmissing": _non_missing_count(model, "exposure_index"),
        "model_main_yield_anomaly_nonmissing": _non_missing_count(model_main, "yield_anomaly_pct"),
        "model_main_exposure_nonmissing": _non_missing_count(model_main, "exposure_index"),
        "province_model_rows": len(model),
        "province_model_outcome_type": _first_text(model, "outcome_type"),
        "province_rice_anomaly_nonmissing": _non_missing_count(model_main, "province_rice_yield_anomaly"),
        "province_grain_anomaly_nonmissing": _non_missing_count(model_main, "province_grain_yield_anomaly"),
        "province_chd_nonmissing": _non_missing_count(model_main, "chd_annual"),
        "model_study_region_rows": len(model_study_region),
        "model_study_region_province_count": _nunique(model_study_region, "province"),
        "model_study_region_chd_annual_nonmissing": _non_missing_count(model_study_region, "chd_annual"),
        "model_study_region_chd_2022_nonmissing": _non_missing_count(model_study_region, "chd_2022_intensity"),
        "annual_exposure_rows": len(annual_exposure),
        "annual_exposure_year_min": _min_numeric(annual_exposure, "year"),
        "annual_exposure_year_max": _max_numeric(annual_exposure, "year"),
        "annual_exposure_chd_nonmissing": _non_missing_count(annual_exposure, "chd_annual"),
        "annual_exposure_chd_coverage_rate": _coverage_rate(annual_exposure, "chd_annual", main_year_min, main_year_max),
        "province_chd_coverage_rate": province_chd_coverage_rate,
        "yield_anomaly_coverage_rate": yield_anomaly_coverage_rate,
        "report_conclusion_strength": _conclusion_strength_from_coverage(
            province_chd_coverage_rate,
            yield_anomaly_coverage_rate,
        ),
        "exposure_coverage_status": exposure_status,
        "exposure_likely_causes": _current_exposure_causes(
            exposure_status,
            _diagnosis_metric(exposure_diagnosis, "likely_causes"),
        ),
        "event_exposure_summary": _numeric_summary(event_model, "chd_annual"),
        "event_yield_anomaly_summary": _numeric_summary(event_model, "yield_anomaly_pct"),
        "top_exposure": _top_rows(event_model, "chd_annual", ["province", "crop", "chd_annual"], 5),
        "bottom_yield_anomaly": _bottom_rows(event_model, "yield_anomaly_pct", ["province", "crop", "yield_anomaly_pct"], 5),
        "coefficient_rows": len(coefficients),
        "event_rows": len(event),
        "main_coefficient": _main_coefficient(coefficients),
        "event_time_0": _event_time_zero(event),
        "robustness_rows": len(tables["robustness"]),
        "placebo_rows": len(tables["placebo"]),
        "source_rows": len(data_sources),
        "source_categories": _value_counts(data_sources, "category"),
        "source_access_levels": _value_counts(data_sources, "access_level"),
        "source_statuses": _value_counts(data_sources, "status"),
        "yield_source_rows": _value_count(data_sources, "category", "yield_panel"),
        "critical_source_rows": _value_count(data_sources, "priority", "critical"),
        "agri_source_rows": len(agri_sources),
        "agri_source_statuses": _value_counts(agri_sources, "status"),
        "local_yearbook_lead_rows": len(agri_source_leads),
        "local_yearbook_lead_statuses": _value_counts(agri_source_leads, "status"),
    }
    summary["risk_items"] = _risk_items(summary)
    return summary


def _render_final_report(summary: dict[str, Any], outputs: Path, main_event_year: int) -> str:
    """Render the final analysis Markdown report."""

    figures = sorted((outputs / "figures").glob("*.png")) if (outputs / "figures").exists() else []
    lines = [
        "# Final Analysis Summary",
        "",
        f"- Generated at: {summary['generated_at']}",
        "",
        "## 1. 数据清单摘要",
        "",
        f"- 清单记录数：{_fmt_int(summary['inventory_rows'])}",
        "",
        "## 2. 研究区摘要",
        "",
        f"- 行政单元数：{_fmt_int(summary['admin_rows'])}",
        f"- 稻田掩膜真实叠加：{_fmt_int(summary['crop_zonal_rows'])}/{_fmt_int(summary['crop_rows'])} 个行政单元",
        f"- 稻田面积代理合计：{_fmt_float(summary['crop_area_sum'])} ha",
        f"- 物候栅格窗口：{_fmt_int(summary['phenology_zonal_rows'])}/{_fmt_int(summary['phenology_rows'])} 个行政单元",
        "",
        "## 3. 暴露与遥感数据摘要",
        "",
        f"- 省级气象暴露面板：{_fmt_int(summary['climate_rows'])} 行；变量：{_join_or_none(summary['climate_variables'])}",
        f"- 省级遥感暴露面板：{_fmt_int(summary['remote_rows'])} 行；变量：{_join_or_none(summary['remote_variables'])}",
        "",
        "## 4. 研究口径与数据限制",
        "",
        *_format_scope_statement(summary),
        "",
        "## 4b. 为什么采用省级产量口径",
        "",
        "- 地级市/县级官方稻谷或粮食 2000-2024 连续面板不可得，不能再作为主模型依赖。",
        "- 本文主模型使用省级官方产量统计，官方产量结论和单产异常结论均限定在省级尺度。",
        "- 县域和栅格尺度结果仅用于热旱暴露和遥感响应分析，不用于官方产量损失结论。",
        "- 研究结论默认为影响评估或相关性分析，不作强因果表述。",
        f"- 当前自动题目建议：{_suggest_title(summary)}",
        "",
        "## 5. 产量面板摘要",
        "",
        f"- 合并产量面板行数：{_fmt_int(summary['yield_rows'])}",
        f"- 合并数据年份范围（含背景/可选年份）：{_format_content_year_range(summary['yield_year_min'], summary['yield_year_max'])}",
        f"- 行政层级：{_format_counts(summary['yield_admin_levels'])}",
        f"- 作物类型：{_format_counts(summary['yield_crops'])}",
        "",
        "## 6. 县级单产代理面板",
        "",
        f"- 代理面板行数：{_fmt_int(summary['yield_proxy_rows'])}",
        f"- 代理数据源：{_format_counts(summary['yield_proxy_sources'])}",
        f"- 已完成省级校准行数：{_fmt_int(summary['yield_proxy_calibrated_rows'])}",
        f"- 代理缺口报告行数：{_fmt_int(summary['yield_proxy_gap_rows'])}",
        f"- 可用 admin-year 单元：{_fmt_int(summary['yield_proxy_available_cells'])}",
        f"- 缺失 admin-year 单元：{_fmt_int(summary['yield_proxy_missing_cells'])}",
        "",
        "## 7. 模型面板与核心变量",
        "",
        f"- `province_model_panel` 行数（含背景/可选年份）：{_fmt_int(summary['model_rows'])}",
        f"- 主模型年份过滤后行数：{_fmt_int(summary['model_main_rows'])}",
        f"- `model_panel_study_region.csv` 行数：{_fmt_int(summary['model_study_region_rows'])}",
        f"- 研究区省级单元数：{_fmt_int(summary['model_study_region_province_count'])}",
        f"- 模型面板样本年份（含背景/可选年份）：{_format_content_year_range(summary['model_year_min'], summary['model_year_max'])}",
        f"- 省级单元数：{_fmt_int(summary['model_province_count'])}",
        f"- 省级稻谷单产异常非空：{_fmt_int(summary['province_rice_anomaly_nonmissing'])}/{_fmt_int(summary['model_main_rows'])} 行",
        f"- 省级粮食单产异常非空：{_fmt_int(summary['province_grain_anomaly_nonmissing'])}/{_fmt_int(summary['model_main_rows'])} 行",
        f"- yield_anomaly_pct 非空：{_fmt_int(summary['model_main_yield_anomaly_nonmissing'])}/{_fmt_int(summary['model_main_rows'])} 行",
        f"- 主模型 chd_annual 非空：{_fmt_int(summary['province_chd_nonmissing'])}/{_fmt_int(summary['model_main_rows'])} 行",
        f"- 旧 exposure_index 非空：{_fmt_int(summary['model_main_exposure_nonmissing'])}/{_fmt_int(summary['model_main_rows'])} 行",
        f"- 研究区 chd_annual 非空：{_fmt_int(summary['model_study_region_chd_annual_nonmissing'])}/{_fmt_int(summary['model_study_region_rows'])} 行",
        f"- 研究区 chd_2022_intensity 非空：{_fmt_int(summary['model_study_region_chd_2022_nonmissing'])}/{_fmt_int(summary['model_study_region_rows'])} 行",
        f"- 暴露覆盖机器判定：`{summary['exposure_coverage_status'] or 'n/a'}`",
        f"- 暴露缺失疑似原因：`{summary['exposure_likely_causes'] or 'n/a'}`",
        "",
        "## 7b. 年度 CHD 暴露面板",
        "",
        f"- annual_exposure_panel 行数：{_fmt_int(summary['annual_exposure_rows'])}",
        f"- 年份范围：{_format_content_year_range(summary['annual_exposure_year_min'], summary['annual_exposure_year_max'])}",
        f"- chd_annual 非空：{_fmt_int(summary['annual_exposure_chd_nonmissing'])}",
        f"- chd_annual 覆盖率（相对 2000-2024 admin-year）：{_fmt_float(summary['annual_exposure_chd_coverage_rate'])}",
        "",
        f"## 8. {main_event_year} 年 CHD 暴露概况",
        "",
        _format_numeric_summary(summary["event_exposure_summary"], "chd_annual"),
        "",
        "高暴露样本前 5：",
        "",
        *_format_small_table(summary["top_exposure"]),
        "",
        f"## 9. {main_event_year} 年单产异常概况",
        "",
        _format_numeric_summary(summary["event_yield_anomaly_summary"], "yield_anomaly_pct"),
        "",
        "单产异常最低样本前 5：",
        "",
        *_format_small_table(summary["bottom_yield_anomaly"]),
        "",
        "## 10. 模型结果摘要",
        "",
        f"- 系数行数：{_fmt_int(summary['coefficient_rows'])}",
        f"- 主模型 CHD 系数：{summary['main_coefficient']}",
        f"- 事件研究 event_time_0：{summary['event_time_0']}",
        f"- 事件研究系数行数：{_fmt_int(summary['event_rows'])}",
        "",
        "## 11. 稳健性检查摘要",
        "",
        f"- robustness_results.csv 行数：{_fmt_int(summary['robustness_rows'])}",
        f"- placebo_results.csv 行数：{_fmt_int(summary['placebo_rows'])}",
        "",
        "## 12. 已生成图表列表",
        "",
    ]
    lines.extend(f"- `{figure}`" for figure in figures) if figures else lines.append("- 暂无 PNG 图表。")
    lines.extend(
        [
            "",
            "## 13. 深度数据源检索",
            "",
            f"- 数据源目录行数：{_fmt_int(summary['source_rows'])}",
            f"- 数据源类别：{_format_counts(summary['source_categories'])}",
            f"- 访问方式：{_format_counts(summary['source_access_levels'])}",
            f"- 产量面板候选源：{_fmt_int(summary['yield_source_rows'])}",
            f"- critical 优先级源：{_fmt_int(summary['critical_source_rows'])}",
            "- 结论：未找到完整公开县/市级 2000-2024 内容年份水稻单产面板；本项目已降级为省级官方产量主模型。",
            "- 详细检索报告见 `reports/deep_data_search_report.md`；机器可读目录见 `data/raw/references/deep_required_data_sources.csv`。",
            "",
            "## 13b. 农业统计来源缓存",
            "",
            f"- 农业统计来源目录行数：{_fmt_int(summary['agri_source_rows'])}",
            f"- 本地年鉴/订阅/第三方线索数：{_fmt_int(summary['local_yearbook_lead_rows'])}",
            f"- 线索缓存状态：{_format_counts(summary['local_yearbook_lead_statuses'])}",
            "- 已缓存的官方线索包括浙江、湖北、湖南和国家统计局年鉴入口；CSYD/CNKI 作为订阅导出路线；YouGIS 仅作第三方线索。",
            "",
            "## 14. 风险状态与处理结果",
            "",
            *_format_risk_table(summary["risk_items"]),
            "",
            "## 15. 主要数据缺口",
            "",
            "- 县/市级 2000-2024 官方水稻单产面板仍不足，当前不再追补为主模型依赖；2025 只作全国/省级背景或补充说明。",
            "- 2024 伏秋旱验证事件仍需补齐官方统计产量；年度 CHD 气象暴露已覆盖到 2024。",
            "- 县级稻田像元加权暴露仍需替代当前省级近似。",
            "",
            "## 16. 后续路线图",
            "",
            "1. 维护省级官方稻谷/粮食产量面板，并将 2024 统计结果作为外部验证。",
            "2. 扩展 2000-2024 气象与遥感网格，并做县域或栅格稻田加权暴露聚合。",
            "3. 将县域/栅格输出限定为暴露、长势响应和机制分析。",
            "4. 在省级面板、平行趋势、安慰剂和稳健性检查通过后，最多表述为准因果证据。",
            "5. 将模型输出升级为论文表格格式，并为每个结果附样本筛选说明和结论强度门控。",
            "",
            "详细风险报告见 `reports/project_risk_assessment.md`。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_risk_report(summary: dict[str, Any]) -> str:
    """Render a standalone risk-assessment report."""

    lines = [
        "# Project Risk Assessment",
        "",
        f"- Generated at: {summary['generated_at']}",
        "",
        "## 已处理风险",
        "",
        "- 稻田掩膜已从 metadata-only 升级为行政单元 zonal stats 输出。",
        "- 物候窗口已从固定 6-9 月升级为 ChinaRiceCalendar DOY 栅格聚合，并保留默认 fallback。",
        "- ERS 英文省名已归一化为中文省名，可与 NBS 近年公告按省衔接。",
        "- 模型面板已自动生成 trend_yield、yield_anomaly_pct、稳定性指标和 chd_annual；旧 exposure_index 仅作为历史字段保留。",
        "- 建模模块已处理常数预测变量和缺少 admin_id 的省级面板场景。",
        f"- 深度数据源检索已生成 {summary['source_rows']} 条候选源；结果见 `reports/deep_data_search_report.md`。",
        "",
        "## 剩余风险登记表",
        "",
        *_format_risk_table(summary["risk_items"]),
        "",
        "## 关键数据缺口",
        "",
        "- 县/市级 2000-2024 官方水稻单产面板：自动公开源未找到完整可下载数据。",
        "- 2024 伏秋旱验证事件：年度 CHD 气象暴露已覆盖到 2024，仍缺 2024 官方产量作为外部验证。",
        "- 县级暴露：当前建模优先使用省级 NetCDF 暴露面板，县级稻田像元加权暴露仍需扩展。",
        "- 行政区划跨年一致性：还没有撤并调整后的代码映射表。",
        "",
        "## 建议采购或整理的数据",
        "",
        "- 省、市、县统计年鉴中的水稻播种面积、产量、单产，主模型至少覆盖 2000-2024 内容年份。",
        "- 统计公报 PDF/HTML 批量抓取结果，配套字段映射和单位校验表。",
        "- ERA5-Land/CHIRPS 的 2000-2024 省级日尺度 CHD 输入已落地；若需要机制或像元加权暴露，再整理 GLEAM/MODIS 和稻田掩膜。",
        "- 历年行政区划代码表和名称变更表。",
        "- 已固化的数据源目录：`data/raw/references/deep_required_data_sources.csv`。",
        "",
    ]
    return "\n".join(lines)


def _read_first_existing_table(paths: list[Path]) -> Any:
    """Read the first existing table in a path list."""

    for path in paths:
        frame = _read_table(path)
        if not frame.empty:
            return frame
    return _empty_frame()


def _read_table(path: Path) -> Any:
    """Read CSV or Parquet if available, otherwise return an empty DataFrame."""

    import pandas as pd

    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()


def _empty_frame() -> Any:
    """Return an empty pandas DataFrame."""

    import pandas as pd

    return pd.DataFrame()


def _filter_year(frame: Any, year: int) -> Any:
    """Filter a DataFrame by integer year if possible."""

    if frame.empty or "year" not in frame.columns:
        return frame.iloc[0:0] if not frame.empty else frame
    years = _numeric_series(frame, "year")
    return frame[years == int(year)]


def _filter_year_range(frame: Any, year_min: int, year_max: int) -> Any:
    """Filter a DataFrame to the main content-year window."""

    if frame.empty or "year" not in frame.columns:
        return frame.iloc[0:0] if not frame.empty else frame
    years = _numeric_series(frame, "year")
    return frame[(years >= int(year_min)) & (years <= int(year_max))]


def _filter_agri_source_leads(frame: Any) -> Any:
    """Return cached local yearbook, subscription, and third-party lead rows."""

    if frame.empty or "source_id" not in frame.columns:
        return _empty_frame()
    lead_ids = {
        "nbs_statistical_yearbook_index",
        "zhejiang_statistical_yearbook_2024_index",
        "hubei_statistical_yearbook_index",
        "hunan_statistical_yearbook_index",
        "csyd_subscription_yearbook_database",
        "yougis_county_yearbook_lead",
    }
    return frame[frame["source_id"].astype(str).isin(lead_ids)].copy()


def _status_count(frame: Any, status: str) -> int:
    """Count rows with a given status."""

    if frame.empty or "status" not in frame.columns:
        return 0
    return int((frame["status"].astype(str) == status).sum())


def _positive_count(frame: Any, column: str) -> int:
    """Count positive numeric values in a column."""

    values = _numeric_series(frame, column)
    return int((values > 0).sum()) if values is not None else 0


def _sum_numeric(frame: Any, column: str) -> float | None:
    """Sum a numeric column."""

    values = _numeric_series(frame, column)
    return None if values is None else float(values.sum(skipna=True))


def _min_numeric(frame: Any, column: str) -> float | None:
    """Return numeric minimum."""

    values = _numeric_series(frame, column)
    return None if values is None or values.dropna().empty else float(values.min(skipna=True))


def _max_numeric(frame: Any, column: str) -> float | None:
    """Return numeric maximum."""

    values = _numeric_series(frame, column)
    return None if values is None or values.dropna().empty else float(values.max(skipna=True))


def _non_missing_count(frame: Any, column: str) -> int:
    """Count non-missing values in a column."""

    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column]
    return int((values.notna() & values.astype(str).str.strip().ne("")).sum())


def _coverage_rate(frame: Any, column: str, year_min: int, year_max: int) -> float | None:
    """Estimate column coverage against admin-year cells."""

    if frame.empty or column not in frame.columns:
        return None
    key = "admin_id" if "admin_id" in frame.columns and _non_missing_count(frame, "admin_id") else "province" if "province" in frame.columns else None
    if key is None:
        expected = len(frame)
    else:
        expected = max(1, _nunique(frame, key) * (int(year_max) - int(year_min) + 1))
    return _non_missing_count(frame, column) / expected if expected else None


def _conclusion_strength_from_coverage(chd_coverage: float | None, yield_coverage: float | None) -> str:
    """Return the report conclusion strength implied by current model inputs."""

    if chd_coverage is not None and yield_coverage is not None and chd_coverage >= 0.75 and yield_coverage >= 0.75:
        return "impact_assessment"
    return "association"


def _current_exposure_status(chd_coverage: float | None, fallback_status: str) -> str:
    """Prefer current CHD panel coverage over stale exposure-index diagnostics."""

    if chd_coverage is not None and chd_coverage >= 0.75:
        return "ok_for_province_fixed_effects"
    return fallback_status


def _current_exposure_causes(exposure_status: str, fallback_causes: str) -> str:
    """Return exposure limitation text consistent with the active CHD panel."""

    if exposure_status == "ok_for_province_fixed_effects":
        return "n/a"
    return fallback_causes


def _diagnosis_metric(frame: Any, metric: str) -> str:
    """Read a metric value from exposure diagnosis CSV."""

    if frame.empty or not {"metric", "value"}.issubset(frame.columns):
        return ""
    rows = frame[frame["metric"].astype(str) == metric]
    if rows.empty:
        return ""
    return str(rows.iloc[0]["value"])


def _nunique(frame: Any, column: str) -> int:
    """Count unique non-missing text values."""

    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].dropna().astype(str).nunique())


def _value_counts(frame: Any, column: str) -> dict[str, int]:
    """Return value counts for a text column."""

    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].fillna("").astype(str).value_counts().items() if str(key)}


def _value_count(frame: Any, column: str, value: str) -> int:
    """Count rows where a text column equals a target value."""

    if frame.empty or column not in frame.columns:
        return 0
    return int((frame[column].fillna("").astype(str) == value).sum())


def _non_available_count(frame: Any, column: str, available_value: str) -> int:
    """Count rows where a status column is not the available value."""

    if frame.empty or column not in frame.columns:
        return 0
    return int((frame[column].fillna("").astype(str) != available_value).sum())


def _unique_text(frame: Any, column: str) -> list[str]:
    """Return sorted unique text values."""

    if frame.empty or column not in frame.columns:
        return []
    return sorted(str(value) for value in frame[column].dropna().unique())


def _numeric_summary(frame: Any, column: str) -> dict[str, float | int | None]:
    """Summarize a numeric column."""

    values = _numeric_series(frame, column)
    if values is None:
        return {"count": 0, "mean": None, "min": None, "max": None}
    valid = values.dropna()
    if valid.empty:
        return {"count": 0, "mean": None, "min": None, "max": None}
    return {
        "count": int(valid.shape[0]),
        "mean": float(valid.mean()),
        "min": float(valid.min()),
        "max": float(valid.max()),
    }


def _top_rows(frame: Any, sort_column: str, columns: list[str], n: int) -> list[dict[str, Any]]:
    """Return top rows by a numeric column."""

    values = _numeric_series(frame, sort_column)
    if frame.empty or values is None or values.dropna().empty:
        return []
    temp = frame.copy()
    temp[sort_column] = values
    for column in columns:
        if column not in temp.columns:
            temp[column] = ""
    return temp.sort_values(sort_column, ascending=False)[columns].head(n).to_dict(orient="records")


def _bottom_rows(frame: Any, sort_column: str, columns: list[str], n: int) -> list[dict[str, Any]]:
    """Return bottom rows by a numeric column."""

    values = _numeric_series(frame, sort_column)
    if frame.empty or values is None or values.dropna().empty:
        return []
    temp = frame.copy()
    temp[sort_column] = values
    for column in columns:
        if column not in temp.columns:
            temp[column] = ""
    return temp.sort_values(sort_column, ascending=True)[columns].head(n).to_dict(orient="records")


def _main_coefficient(frame: Any) -> str:
    """Format the main exposure coefficient if available."""

    if frame.empty or "term" not in frame.columns:
        return "缺失"
    match = frame[frame["term"].astype(str).isin(["chd_annual", "chd_2022_intensity", "exposure_index"])]
    if match.empty:
        return "未估计"
    row = match.iloc[0]
    return f"estimate={_fmt_number(row.get('estimate'))}, n={_fmt_number(row.get('n'))}, R2={_fmt_number(row.get('r2'))}"


def _event_time_zero(frame: Any) -> str:
    """Format event-time-zero coefficient if available."""

    if frame.empty or "term" not in frame.columns:
        return "缺失"
    match = frame[frame["term"].astype(str) == "event_time_0"]
    if match.empty:
        return "未估计"
    row = match.iloc[0]
    return f"estimate={_fmt_number(row.get('estimate'))}, n={_fmt_number(row.get('n'))}, R2={_fmt_number(row.get('r2'))}"


def _first_row(frame: Any) -> dict[str, Any]:
    """Return the first row of a DataFrame as a dictionary."""

    if frame.empty:
        return {}
    return dict(frame.iloc[0].to_dict())


def _first_text(frame: Any, column: str) -> str:
    """Return first non-empty text value from a DataFrame column."""

    if frame.empty or column not in frame.columns:
        return ""
    values = frame[column].dropna().astype(str).str.strip()
    values = values[values.ne("")]
    return "" if values.empty else str(values.iloc[0])


def _suggest_title(summary: dict[str, Any]) -> str:
    """Return the report title suggested by the active outcome type."""

    outcome_type = str(summary.get("province_model_outcome_type") or "")
    if outcome_type == "province_rice_yield_anomaly" or summary.get("province_rice_anomaly_nonmissing", 0):
        return "省域尺度复合热旱暴露对长江中下游稻谷单产异常与稳定性的影响——以 2022 年极端高温干旱事件为例"
    if outcome_type == "province_grain_yield_anomaly" or summary.get("province_grain_anomaly_nonmissing", 0):
        return "省域尺度复合热旱暴露对长江中下游粮食单产异常与稳定性的影响——以 2022 年极端高温干旱事件为例"
    return "2022 年复合热旱事件下长江中下游稻作区遥感长势异常响应研究"


def _format_scope_statement(summary: dict[str, Any]) -> list[str]:
    """Render research scope and data limitation statements."""

    tier = summary.get("yield_tier", {}) or {}
    admin_level = tier.get("admin_level") or _first_count_key(summary.get("yield_admin_levels", {})) or "unknown"
    crop_type = tier.get("crop_type") or _first_count_key(summary.get("yield_crops", {})) or "unknown"
    official = tier.get("is_official_statistics", "unknown")
    coverage = summary.get("yield_anomaly_coverage_rate")
    if coverage is None:
        coverage = tier.get("year_coverage_rate", summary.get("yield_best_coverage"))
    conclusion_strength = summary.get("report_conclusion_strength") or tier.get("conclusion_strength", "impact_assessment")
    forbidden = tier.get("forbidden_claim", "strong_causal_claim_without_valid_identification")
    uses_proxy = summary.get("yield_proxy_rows", 0) > 0
    return [
        "- 主口径：省级官方产量面板 + 高分辨率遥感/气象暴露聚合 + 2022 事件影响评估。",
        "- 主模型内容年份：2000-2024；2025 只作全国/省级背景或补充说明。",
        "- 默认统计单元：省份 × 年份；全国省级面板作为主模型样本，长江中下游作为重点展示区域。",
        "- 县域、地级市和栅格尺度只用于暴露差异、遥感长势响应和机制分析。",
        f"- 当前产量数据层级：`{admin_level}`；作物口径：`{crop_type}`。",
        f"- 产量数据是否官方统计：{official}。",
        f"- 当前合并产量数据年份范围（含背景/可选年份）：{_format_content_year_range(summary.get('yield_year_min'), summary.get('yield_year_max'))}；主模型覆盖率按 2000-2024 计算：{_fmt_number(coverage)}。",
        f"- 产量覆盖报告行数：{_fmt_int(summary.get('yield_coverage_rows'))}。",
        f"- 行政区划跨年映射记录数：{_fmt_int(summary.get('admin_crosswalk_rows'))}；默认映射到 2022 年事件边界。",
        f"- 是否使用遥感代理长势分析：{uses_proxy}；遥感代理不得写成官方产量损失。",
        "- 2024 年只作为外部一致性验证或描述性对照，不作为主因果识别事件。",
        f"- 当前允许结论强度：`{conclusion_strength}`。",
        f"- 禁止表述：`{forbidden}`；不得写“证明 2022 热旱导致单产下降”。",
    ]


def _risk_items(summary: dict[str, Any]) -> list[dict[str, str]]:
    """Build a risk register from summary metrics."""

    chd_coverage = summary.get("province_chd_coverage_rate")
    exposure_coverage = (
        float(chd_coverage)
        if chd_coverage is not None
        else _coverage(summary["model_main_exposure_nonmissing"], summary["model_main_rows"])
    )
    anomaly_coverage_value = summary.get("yield_anomaly_coverage_rate")
    anomaly_coverage = (
        float(anomaly_coverage_value)
        if anomaly_coverage_value is not None
        else _coverage(summary["model_main_yield_anomaly_nonmissing"], summary["model_main_rows"])
    )
    county_panel_available = "county" in summary["yield_admin_levels"] or "prefecture" in summary["yield_admin_levels"]
    source_search_evidence = (
        f"已完成深度检索目录 {summary['source_rows']} 条，yield_panel 候选 {summary['yield_source_rows']} 条；"
        "未发现完整公开县/市级 2000-2024 内容年份水稻单产直链。"
        if summary["source_rows"]
        else "当前合并产量面板以省级公开源为主。"
    )
    exposure_evidence = (
        f"主模型 chd_annual 非空 {summary['province_chd_nonmissing']}/{summary['model_main_rows']} 行；"
        f"按 province-year 目标覆盖率 {_fmt_number(exposure_coverage)}。"
    )
    exposure_next = (
        "保持省域平均 CHD 口径；若论文需要稻田加权暴露，再扩展县级或像元加权聚合。"
        if exposure_coverage >= 0.75
        else "按数据源目录下载 ERA5-Land、CHIRPS、MODIS、GLEAM 并扩展县级聚合。"
    )
    annual_exposure_covers_2024 = (
        summary.get("annual_exposure_year_max") is not None
        and float(summary.get("annual_exposure_year_max")) >= 2024
        and exposure_coverage >= 0.75
    )
    risks = [
        {
            "risk": "县/市级 2000-2024 官方水稻单产面板不足",
            "level": "高",
            "status": "已定位来源，数据缺口未解决",
            "evidence": source_search_evidence if not county_panel_available else "已有部分县/市级记录，但覆盖仍需检查。",
            "next": "按 deep_data_search_report.md 采购/整理年鉴、统计公报或授权数据库。",
        },
        {
            "risk": "暴露变量覆盖不足",
            "level": "低" if exposure_coverage >= 0.75 else "高" if exposure_coverage < 0.5 else "中",
            "status": "已缓解" if exposure_coverage >= 0.75 else "部分缓解",
            "evidence": exposure_evidence,
            "next": exposure_next,
        },
        {
            "risk": "产量异常覆盖不足",
            "level": "中" if anomaly_coverage < 0.8 else "低",
            "status": "部分缓解",
            "evidence": f"主模型 yield_anomaly_pct 非空 {summary['model_main_yield_anomaly_nonmissing']}/{summary['model_main_rows']} 行。",
            "next": "补更长基线年份和更细行政单元产量。",
        },
        {
            "risk": "县级气象/遥感聚合仍未完全替代 MVP fallback",
            "level": "中",
            "status": "部分缓解",
            "evidence": f"已生成省级气象 {summary['climate_rows']} 行、遥感 {summary['remote_rows']} 行。",
            "next": "实现县级稻田像元加权 NetCDF/GeoTIFF zonal stats。",
        },
        _yield_proxy_risk(summary),
        {
            "risk": "2024 验证事件证据不足",
            "level": "中",
            "status": "部分缓解" if annual_exposure_covers_2024 else "已定位数据源，处理结果未补齐",
            "evidence": (
                "年度 CHD 暴露面板已覆盖到 2024；仍需收集 2024 官方产量用于外部验证。"
                if annual_exposure_covers_2024
                else "模型面板含 2025 背景/可选年份行，但主模型已过滤到 2000-2024；当前暴露面板仍主要由已有 2022 数据驱动。"
            ),
            "next": "收集 2024 地方统计产量，并将其作为外部一致性验证。"
            if annual_exposure_covers_2024
            else "用 ERA5-Land/CHIRPS/MODIS/GLEAM 补 2024，并收集地方统计产量。",
        },
    ]
    return risks


def _yield_proxy_risk(summary: dict[str, Any]) -> dict[str, str]:
    """Build the yield-proxy risk row from proxy-panel coverage."""

    if summary["yield_proxy_rows"] == 0:
        return {
            "risk": "县级单产代理面板缺少栅格输入",
            "level": "高",
            "status": "框架已建立，等待数据",
            "evidence": (
                f"县级单产代理面板 {summary['yield_proxy_rows']} 行，"
                f"可用 admin-year 单元 {summary['yield_proxy_available_cells']}。"
            ),
            "next": "下载 AsiaRiceYield4km/GGCP10 rice GeoTIFF 后运行 yield-proxy 步骤。",
        }
    if summary["yield_proxy_calibrated_rows"] == 0:
        return {
            "risk": "县级单产代理面板尚未完成省级校准",
            "level": "中",
            "status": "部分缓解",
            "evidence": (
                f"代理面板 {summary['yield_proxy_rows']} 行，"
                f"但省级校准行数为 {summary['yield_proxy_calibrated_rows']}。"
            ),
            "next": "补齐省份映射、官方 rice 口径统计和面积覆盖校验后再校准。",
        }
    return {
        "risk": "县级单产代理面板仍需官方县级统计验证",
        "level": "中",
        "status": "部分缓解",
        "evidence": (
            f"代理面板 {summary['yield_proxy_rows']} 行，"
            f"已校准 {summary['yield_proxy_calibrated_rows']} 行，"
            f"可用 admin-year 单元 {summary['yield_proxy_available_cells']}。"
        ),
        "next": "继续补官方县/市级水稻统计，用代理面板做稳健性或缺口诊断，不替代官方因变量。",
    }


def _numeric_series(frame: Any, column: str) -> Any | None:
    """Return a pandas numeric series for a column, or None."""

    import pandas as pd

    if frame.empty or column not in frame.columns:
        return None
    return pd.to_numeric(frame[column], errors="coerce")


def _coverage(numerator: int, denominator: int) -> float:
    """Calculate simple coverage ratio."""

    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _format_counts(counts: dict[str, int]) -> str:
    """Format value counts for Markdown."""

    if not counts:
        return "缺失"
    return "，".join(f"{key}: {value}" for key, value in counts.items())


def _first_count_key(counts: dict[str, int]) -> str:
    """Return the first key from a value-count dictionary."""

    if not counts:
        return ""
    return next(iter(counts))


def _format_numeric_summary(summary: dict[str, Any], label: str) -> str:
    """Format a numeric summary sentence."""

    if not summary or int(summary.get("count") or 0) == 0:
        return f"- `{label}`：无可用数值。"
    return (
        f"- `{label}`：count={summary['count']}，mean={_fmt_float(summary['mean'])}，"
        f"min={_fmt_float(summary['min'])}，max={_fmt_float(summary['max'])}"
    )


def _format_small_table(rows: list[dict[str, Any]]) -> list[str]:
    """Format a compact Markdown table from rows."""

    if not rows:
        return ["- 无可用样本。"]
    columns = list(rows[0])
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return lines


def _format_risk_table(risks: list[dict[str, str]]) -> list[str]:
    """Format the risk register as a Markdown table."""

    lines = [
        "| 风险 | 等级 | 状态 | 证据 | 下一步 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for risk in risks:
        lines.append(
            f"| {risk['risk']} | {risk['level']} | {risk['status']} | {risk['evidence']} | {risk['next']} |"
        )
    return lines


def _join_or_none(values: list[str]) -> str:
    """Join list values or return missing text."""

    return "、".join(values) if values else "缺失"


def _fmt_int(value: Any) -> str:
    """Format integer-like values."""

    if value is None:
        return "缺失"
    return str(int(value))


def _fmt_float(value: Any) -> str:
    """Format float-like values."""

    if value is None:
        return "缺失"
    try:
        return f"{float(value):.4g}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_number(value: Any) -> str:
    """Format general numeric values."""

    if value is None:
        return "缺失"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6g}"


def _format_content_year_range(year_min: Any, year_max: Any) -> str:
    """Format data-year range while preserving the main/background-year policy."""

    start = _fmt_number(year_min)
    end = _fmt_number(year_max)
    try:
        start_number = int(float(year_min))
        end_number = int(float(year_max))
    except (TypeError, ValueError):
        return f"{start}-{end}"
    if start_number <= 2000 and end_number >= 2025:
        return "2000-2024 主模型内容年份；2025 背景/可选年份"
    return f"{start}-{end}"
