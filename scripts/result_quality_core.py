"""Result quality-control helpers for data freeze, audits, robustness, and paper summaries."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import ProjectConfig
from src.models import fit_two_way_fixed_effects
from src.province_daily_climate import _daily_to_annual_chd, validate_province_daily_climate


FREEZE_FILES = [
    "data/interim/province_daily_climate_2000_2024.csv",
    "data/processed/annual_exposure_panel.csv",
    "data/processed/province_chd_panel.csv",
    "data/processed/province_model_panel.csv",
    "data/processed/province_grain_backfill_2008_2015_cleaned.csv",
    "data/outputs/model_coefficients.csv",
    "data/outputs/event_study_coefficients.csv",
]

FORBIDDEN_OUTPUT_TOKENS = {
    "county_yield_loss_map",
    "prefecture_yield_loss_claim",
    "county_level_official_yield_loss_map",
    "prefecture_level_yield_loss_claim",
    "city_county_causal_claim",
    "市县级产量损失",
    "县级产量损失",
}


def freeze_data_version(config: ProjectConfig) -> tuple[Path, Path]:
    """Write file hashes and a Markdown data-version freeze report."""

    rows = [_freeze_file_row(config.project_root, relative_path) for relative_path in FREEZE_FILES]
    output_csv = config.output_dir / "data_file_hashes.csv"
    report_path = config.project_root / "reports" / "data_version_freeze.md"
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_csv, index=False, encoding="utf-8-sig")
    _write_freeze_report(report_path, rows)
    return output_csv, report_path


def audit_model_results(config: ProjectConfig) -> tuple[Path, Path]:
    """Audit model outputs for FE implementation, event-study pretrend, and claim limits."""

    outputs = config.output_dir
    reports = config.project_root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    coefs = _read_table(outputs / "model_coefficients.csv")
    event = _read_table(outputs / "event_study_coefficients.csv")
    panel = _read_first_existing(
        [
            config.data_processed_dir / "province_model_panel.parquet",
            config.data_processed_dir / "province_model_panel.csv",
        ]
    )
    checks = _build_model_audit_checks(coefs, event, panel, config)
    output_csv = outputs / "model_result_audit.csv"
    report_path = reports / "model_result_audit.md"
    pd.DataFrame(checks).to_csv(output_csv, index=False, encoding="utf-8-sig")
    _write_model_audit_report(report_path, checks, coefs, event, panel, config)
    return output_csv, report_path


def run_robustness_suite(config: ProjectConfig) -> tuple[Path, Path]:
    """Run a bounded robustness suite from the frozen province model panel."""

    panel = _read_first_existing(
        [
            config.data_processed_dir / "province_model_panel.parquet",
            config.data_processed_dir / "province_model_panel.csv",
        ]
    )
    daily = _load_daily_climate(config)
    specs = _robustness_specs(config)
    rows = [
        _run_robustness_spec(spec, panel, daily, config)
        for spec in specs
    ]
    output_csv = config.output_dir / "robustness_results.csv"
    report_path = config.project_root / "reports" / "robustness_summary.md"
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_csv, index=False, encoding="utf-8-sig")
    _write_robustness_report(report_path, rows)
    return output_csv, report_path


def generate_paper_results_summary(config: ProjectConfig) -> Path:
    """Generate a paper-facing results summary with claim-strength guardrails."""

    panel = _read_first_existing(
        [
            config.data_processed_dir / "province_model_panel.parquet",
            config.data_processed_dir / "province_model_panel.csv",
        ]
    )
    annual = _read_first_existing(
        [
            config.data_processed_dir / "annual_exposure_panel.parquet",
            config.data_processed_dir / "annual_exposure_panel.csv",
        ]
    )
    chd = _read_first_existing(
        [
            config.data_processed_dir / "province_chd_panel.parquet",
            config.data_processed_dir / "province_chd_panel.csv",
        ]
    )
    coefs = _read_table(config.output_dir / "model_coefficients.csv")
    event = _read_table(config.output_dir / "event_study_coefficients.csv")
    robustness = _read_table(config.output_dir / "robustness_results.csv")
    audit = _read_table(config.output_dir / "model_result_audit.csv")
    scope_text = _read_text(config.project_root / "reports" / "model_scope_decision.md")
    report_path = config.project_root / "reports" / "paper_results_summary.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        _render_paper_summary(panel, annual, chd, coefs, event, robustness, audit, scope_text, config),
        encoding="utf-8",
    )
    return report_path


def _freeze_file_row(root: Path, relative_path: str) -> dict[str, Any]:
    path = root / relative_path
    frame = _read_table(path) if path.exists() else pd.DataFrame()
    stat = path.stat() if path.exists() else None
    return {
        "path": relative_path,
        "exists": bool(path.exists()),
        "rows": len(frame) if path.exists() else 0,
        "columns": len(frame.columns) if path.exists() else 0,
        "year_min": _min_year(frame),
        "year_max": _max_year(frame),
        "province_count": _nunique(frame, "province"),
        "key_variable_coverage": _key_variable_coverage(relative_path, frame),
        "sha256": _sha256(path) if path.exists() else "",
        "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds") if stat else "",
        "generated_time": datetime.now().isoformat(timespec="seconds"),
    }


def _write_freeze_report(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Data Version Freeze",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "- Purpose: lock the current model-ready data snapshot for reproducible paper tables and figures.",
        "",
        "| path | exists | rows | columns | years | provinces | key coverage | sha256 |",
        "| --- | ---: | ---: | ---: | --- | ---: | --- | --- |",
    ]
    for row in rows:
        years = _format_year_range(row["year_min"], row["year_max"])
        lines.append(
            f"| {row['path']} | {row['exists']} | {row['rows']} | {row['columns']} | "
            f"{years} | {row['province_count']} | {row['key_variable_coverage']} | {row['sha256']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_model_audit_checks(
    coefs: pd.DataFrame,
    event: pd.DataFrame,
    panel: pd.DataFrame,
    config: ProjectConfig,
) -> list[dict[str, Any]]:
    has_descriptive = _has_model(coefs, "descriptive_ols")
    has_fe = _has_model(coefs, "province_two_way_fixed_effects")
    has_event = _has_model(event, "event_study_candidate")
    fe_row = _coefficient_row(coefs, "province_two_way_fixed_effects", "chd_annual")
    chd_coef = _to_float(fe_row.get("estimate")) if fe_row else None
    p_value = _to_float(fe_row.get("p_value")) if fe_row else None
    n_obs = _to_float(fe_row.get("n_obs") or fe_row.get("n")) if fe_row else None
    r2 = _to_float(fe_row.get("r2")) if fe_row else None
    adjusted_r2 = _to_float(fe_row.get("adjusted_r2")) if fe_row else None
    province_count = _nunique(panel, "province")
    year_count = _nunique(panel, "year")
    contains_province_fe = bool(has_fe and province_count >= 2)
    contains_year_fe = bool(has_fe and year_count >= 2)
    pretrend = _pretrend_passed(event)
    treated, control = _treatment_counts(panel, config.main_event_year)
    missing_concentration = _missing_year_concentration(panel)
    forbidden = _forbidden_outputs_detected(config.project_root)
    return [
        _check("has_descriptive_ols", has_descriptive, str(has_descriptive)),
        _check("has_province_two_way_fixed_effects", has_fe, str(has_fe)),
        _check("has_event_study_candidate", has_event, str(has_event)),
        _metric("chd_annual_coefficient", chd_coef, _direction(chd_coef)),
        _metric("chd_annual_p_value", p_value, _significance_label(p_value)),
        _metric("sample_size", n_obs, ""),
        _metric("r2", r2, ""),
        _metric("adjusted_r2", adjusted_r2, ""),
        _check("contains_province_fe", contains_province_fe, str(contains_province_fe)),
        _check("contains_year_fe", contains_year_fe, str(contains_year_fe)),
        _check("pretrend_test_passed", pretrend, str(pretrend)),
        _metric("treated_province_count", treated, ""),
        _metric("control_province_count", control, ""),
        _check("forbidden_subprovince_yield_claims_absent", not forbidden, str(not forbidden)),
        _metric("missing_rows_concentrated_in_2008_2010", missing_concentration, ""),
    ]


def _write_model_audit_report(
    path: Path,
    checks: list[dict[str, Any]],
    coefs: pd.DataFrame,
    event: pd.DataFrame,
    panel: pd.DataFrame,
    config: ProjectConfig,
) -> None:
    lookup = {row["check"]: row for row in checks}
    fe_passed = (
        lookup["has_province_two_way_fixed_effects"]["status"] == "passed"
        and lookup["contains_province_fe"]["status"] == "passed"
        and lookup["contains_year_fe"]["status"] == "passed"
    )
    fe_row = _coefficient_row(coefs, "province_two_way_fixed_effects", "chd_annual")
    desc_row = _coefficient_row(coefs, "descriptive_ols", "chd_annual")
    pretrend = lookup["pretrend_test_passed"]["value"]
    treated = lookup["treated_province_count"]["value"]
    control = lookup["control_province_count"]["value"]
    lines = [
        "# Model Result Audit",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- FE implementation audit: {'passed' if fe_passed else 'failed'}",
        f"- has_descriptive_ols: {lookup['has_descriptive_ols']['value']}",
        f"- has_province_two_way_fixed_effects: {lookup['has_province_two_way_fixed_effects']['value']}",
        f"- has_event_study_candidate: {lookup['has_event_study_candidate']['value']}",
        f"- chd_annual coefficient: {_format_number(fe_row.get('estimate') if fe_row else None)}",
        f"- chd_annual p_value: {_format_number(fe_row.get('p_value') if fe_row else None)}",
        f"- descriptive chd_annual coefficient: {_format_number(desc_row.get('estimate') if desc_row else None)}",
        f"- pretrend_test_passed: {pretrend}",
        f"- treated_province_count: {treated}",
        f"- control_province_count: {control}",
        "",
    ]
    if not fe_passed:
        lines.append("当前固定效应模型未通过实现审计，不能在论文中称为双向固定效应。")
        lines.append("")
    lines.extend(
        [
            "## Checks",
            "",
            "| check | status | value | notes |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in checks:
        lines.append(f"| {row['check']} | {row['status']} | {row['value']} | {row.get('notes', '')} |")
    lines.extend(
        [
            "",
            "## Scope Guardrails",
            "",
            "- 当前 outcome 是省级 grain / 粮食单产异常，不是稻谷单产。",
            "- 当前 CHD 暴露为省域平均暴露，不是稻田加权暴露。",
            "- 当前最高结论强度为 impact_assessment；quasi_causal_evidence 仍需所有门控通过。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _robustness_specs(config: ProjectConfig) -> list[dict[str, Any]]:
    return [
        {"spec_id": "heat_p90_drought_p10", "heat_threshold": 0.90, "drought_threshold": 0.10, "growth_months": [6, 7, 8, 9]},
        {"spec_id": "heat_p95_drought_p10", "heat_threshold": 0.95, "drought_threshold": 0.10, "growth_months": [6, 7, 8, 9]},
        {"spec_id": "heat_p90_drought_p20", "heat_threshold": 0.90, "drought_threshold": 0.20, "growth_months": [6, 7, 8, 9]},
        {"spec_id": "growth_months_6_9", "heat_threshold": config.heat_threshold_quantile, "drought_threshold": config.drought_threshold_quantile, "growth_months": [6, 7, 8, 9]},
        {"spec_id": "growth_months_7_9", "heat_threshold": config.heat_threshold_quantile, "drought_threshold": config.drought_threshold_quantile, "growth_months": [7, 8, 9]},
        {"spec_id": "growth_months_6_8", "heat_threshold": config.heat_threshold_quantile, "drought_threshold": config.drought_threshold_quantile, "growth_months": [6, 7, 8]},
        {"spec_id": "exclude_backfill", "exclude_backfill": True},
        {"spec_id": "highlighted_region_only", "highlighted_region_only": True},
        {"spec_id": "national_control", "national_control": True},
    ]


def _run_robustness_spec(
    spec: dict[str, Any],
    source_panel: pd.DataFrame,
    daily: pd.DataFrame,
    config: ProjectConfig,
) -> dict[str, Any]:
    panel = source_panel.copy()
    if not daily.empty and {"heat_threshold", "drought_threshold", "growth_months"}.issubset(spec):
        annual = _daily_to_annual_chd(
            daily,
            year_min=_main_year_min(config),
            year_max=_main_year_max(config),
            baseline_years=config.baseline_years,
            growth_months=spec["growth_months"],
            event_year=config.main_event_year,
            heat_threshold_quantile=spec["heat_threshold"],
            drought_threshold_quantile=spec["drought_threshold"],
        )
        panel = _merge_variant_chd(panel, annual)
    if spec.get("exclude_backfill") and "is_backfill" in panel.columns:
        panel = panel[~panel["is_backfill"].map(_truthy)].copy()
    if spec.get("highlighted_region_only"):
        panel = _filter_highlighted(panel, config)
    fit_frame = _complete_model_frame(panel)
    outcome = _outcome_field(fit_frame)
    fit = fit_two_way_fixed_effects(
        rows=fit_frame.to_dict(orient="records"),
        outcome_field=outcome,
        exposure_field="chd_annual",
        admin_field="province",
        year_field="year",
    )
    coefficient = fit.get("coefficient")
    standard_error = fit.get("standard_error")
    p_value = fit.get("p_value")
    chd_cov = _target_coverage(fit_frame, "chd_annual", _main_year_min(config), _main_year_max(config))
    yield_cov = _target_coverage(fit_frame, "yield_anomaly_pct", _main_year_min(config), _main_year_max(config))
    fe_available = not fit.get("unavailable")
    claim = "impact_assessment" if fe_available and chd_cov >= 0.75 and yield_cov >= 0.75 else "association"
    return {
        "spec_id": spec["spec_id"],
        "sample_size": int(fit.get("n_obs") or fit.get("n") or len(fit_frame)),
        "chd_coefficient": coefficient,
        "standard_error": standard_error,
        "p_value": p_value,
        "direction": _direction(coefficient),
        "significant_at_10pct": bool(p_value is not None and p_value < 0.10),
        "significant_at_5pct": bool(p_value is not None and p_value < 0.05),
        "model_scope": "province_fixed_effects_and_event_study_candidate" if fe_available else "fixed_effects_unavailable",
        "claim_strength_allowed": claim,
    }


def _write_robustness_report(path: Path, rows: list[dict[str, Any]]) -> None:
    valid = [row for row in rows if row.get("direction") not in {"unavailable", "zero"}]
    reference = next((row.get("direction") for row in rows if row.get("spec_id") == "national_control"), None)
    consistent = sum(1 for row in valid if reference and row.get("direction") == reference)
    lines = [
        "# Robustness Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- specs: {len(rows)}",
        f"- direction-consistent specs: {consistent}/{len(valid)}",
        "",
        "| spec_id | sample_size | coefficient | p_value | direction | claim_strength_allowed |",
        "| --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['spec_id']} | {row['sample_size']} | {_format_number(row['chd_coefficient'])} | "
            f"{_format_number(row['p_value'])} | {row['direction']} | {row['claim_strength_allowed']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _render_paper_summary(
    panel: pd.DataFrame,
    annual: pd.DataFrame,
    chd: pd.DataFrame,
    coefs: pd.DataFrame,
    event: pd.DataFrame,
    robustness: pd.DataFrame,
    audit: pd.DataFrame,
    scope_text: str,
    config: ProjectConfig,
) -> str:
    outcome_type = _first_text(panel, "outcome_type")
    title = (
        "省域尺度复合热旱暴露对稻谷单产异常与稳定性的影响评估——以 2022 年长江流域极端高温干旱事件为例"
        if outcome_type == "province_rice_yield_anomaly"
        else "省域尺度复合热旱暴露对粮食单产异常与稳定性的影响评估——以 2022 年长江流域极端高温干旱事件为例"
    )
    fe_row = _coefficient_row(coefs, "province_two_way_fixed_effects", "chd_annual")
    event_row = _coefficient_row(event, "event_study_candidate", "event_time_0")
    robust_valid = robustness[robustness.get("direction", pd.Series(dtype=str)).astype(str).ne("unavailable")] if not robustness.empty and "direction" in robustness.columns else pd.DataFrame()
    reference_direction = _direction(_to_float(fe_row.get("estimate")) if fe_row else None)
    direction_consistent = int((robust_valid["direction"].astype(str) == reference_direction).sum()) if not robust_valid.empty else 0
    pretrend_value = _audit_value(audit, "pretrend_test_passed")
    conclusion = _paper_conclusion_strength(panel, robustness, scope_text, config)
    lines = [
        "# Paper Results Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- 推荐论文题目：{title}",
        f"- 最高结论强度：{conclusion}",
        "",
        "## 1. 数据覆盖情况",
        "",
        f"- province_daily_climate: {_rows(config.data_interim_dir / 'province_daily_climate_2000_2024.csv')} 行。",
        f"- annual_exposure_panel: {len(annual)} 行，chd_annual 覆盖率 {_target_coverage(annual, 'chd_annual', _main_year_min(config), _main_year_max(config)):.6f}。",
        f"- province_chd_panel: {len(chd)} 行。",
        f"- province_model_panel: {len(panel)} 行，yield_anomaly_pct 覆盖率 {_target_coverage(panel, 'yield_anomaly_pct', _main_year_min(config), _main_year_max(config)):.6f}。",
        "- 2008-2010 省级粮食回填仍缺，但主模型覆盖率已超过门控阈值。",
        "",
        "## 2. CHD 暴露描述",
        "",
        "- 当前 CHD 暴露为省域平均暴露，不是稻田加权暴露。",
        f"- 2022 年 chd_annual 描述：{_numeric_summary(_filter_year(panel, config.main_event_year), 'chd_annual')}。",
        "",
        "## 3. 2022 事件空间分布",
        "",
        _top_event_exposure(panel, config.main_event_year),
        "",
        "## 4. 省级粮食单产异常描述",
        "",
        f"- 2022 年 yield_anomaly_pct 描述：{_numeric_summary(_filter_year(panel, config.main_event_year), 'yield_anomaly_pct')}。",
        "",
        "## 5. 主模型结果",
        "",
        f"- province_two_way_fixed_effects chd_annual 系数：{_format_number(fe_row.get('estimate') if fe_row else None)}。",
        f"- p 值：{_format_number(fe_row.get('p_value') if fe_row else None)}。",
        f"- 方向：{reference_direction}。",
        "",
        "## 6. 事件研究结果",
        "",
        f"- event_time_0 系数：{_format_number(event_row.get('estimate') if event_row else None)}。",
        f"- event_time_0 p 值：{_format_number(event_row.get('p_value') if event_row else None)}。",
        f"- pretrend_test_passed: {pretrend_value or 'unknown'}。",
        "",
        "## 7. 稳健性检验",
        "",
        f"- 方向一致设定：{direction_consistent}/{len(robust_valid)}。",
        "",
        "## 8. 当前结论强度",
        "",
        f"- 当前模型门控允许写到：{conclusion}。",
        "- 当前可以写“省级年度面板下的复合热旱影响评估”。",
        "",
        "## 9. 禁止表述",
        "",
        "- 不能写准因果证据。",
        "- 不能写县/市级产量损失。",
        "- 当前 outcome 是省级 grain / 粮食单产异常，不能写稻谷单产结论。",
        "- 不能写“证明导致”或“因果效应”。",
        "",
        "## 10. 推荐论文题目",
        "",
        title,
        "",
    ]
    return "\n".join(lines)


def _load_daily_climate(config: ProjectConfig) -> pd.DataFrame:
    for path in [
        config.data_interim_dir / "province_daily_climate_2000_2024.parquet",
        config.data_interim_dir / "province_daily_climate_2000_2024.csv",
    ]:
        frame = _read_table(path)
        if not frame.empty:
            cleaned, _qc, _warnings = validate_province_daily_climate(
                frame,
                year_min=_main_year_min(config),
                year_max=_main_year_max(config),
                growth_months=config.rice_growth_months,
            )
            return cleaned
    return pd.DataFrame()


def _paper_conclusion_strength(
    panel: pd.DataFrame,
    robustness: pd.DataFrame,
    scope_text: str,
    config: ProjectConfig,
) -> str:
    if "impact_assessment" in scope_text:
        return "impact_assessment"
    if not robustness.empty and "claim_strength_allowed" in robustness.columns:
        claims = set(robustness["claim_strength_allowed"].dropna().astype(str))
        if "impact_assessment" in claims:
            return "impact_assessment"
    chd_coverage = _target_coverage(panel, "chd_annual", _main_year_min(config), _main_year_max(config))
    yield_coverage = _target_coverage(panel, "yield_anomaly_pct", _main_year_min(config), _main_year_max(config))
    return "impact_assessment" if chd_coverage >= 0.75 and yield_coverage >= 0.75 else "association"


def _merge_variant_chd(panel: pd.DataFrame, annual: pd.DataFrame) -> pd.DataFrame:
    if panel.empty or annual.empty:
        return panel
    left = panel.copy()
    right = annual.copy()
    for column in ["province", "province_code", "year", "chd_annual", "chd_2022_intensity", "chd_2022_treated_p75"]:
        if column not in right.columns:
            right[column] = pd.NA
    right = right[["province", "province_code", "admin_code", "year", "chd_annual", "chd_2022_intensity", "chd_2022_treated_p75"]].copy() if "admin_code" in right.columns else right[["province", "province_code", "year", "chd_annual", "chd_2022_intensity", "chd_2022_treated_p75"]].copy()
    left["_code"] = _code_series(left)
    right["_code"] = _code_series(right)
    left["year"] = pd.to_numeric(left["year"], errors="coerce").astype("Int64")
    right["year"] = pd.to_numeric(right["year"], errors="coerce").astype("Int64")
    left = left.drop(columns=[column for column in ["chd_annual", "chd_2022_intensity", "chd_2022_treated_p75"] if column in left.columns])
    merged = left.merge(right[["_code", "year", "chd_annual", "chd_2022_intensity", "chd_2022_treated_p75"]], on=["_code", "year"], how="left")
    return merged.drop(columns=["_code"])


def _complete_model_frame(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel
    work = panel.copy()
    for column in ["year", "chd_annual", "yield_anomaly_pct", "province_grain_yield_anomaly", "province_rice_yield_anomaly"]:
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    outcome = _outcome_field(work)
    required = ["province", "year", "chd_annual", outcome]
    for column in required:
        if column not in work.columns:
            return work.iloc[0:0].copy()
    return work.dropna(subset=required).copy()


def _outcome_field(frame: pd.DataFrame) -> str:
    outcome_type = _first_text(frame, "outcome_type")
    for candidate in [outcome_type, "province_grain_yield_anomaly", "province_rice_yield_anomaly", "yield_anomaly_pct"]:
        if candidate and candidate in frame.columns and pd.to_numeric(frame[candidate], errors="coerce").notna().any():
            return candidate
    return "yield_anomaly_pct"


def _filter_highlighted(panel: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    policy = config.raw.get("study_region_policy", {})
    region_name = policy.get("highlighted_region") or policy.get("default_region")
    region = policy.get("regions", {}).get(region_name, {}) if isinstance(policy.get("regions", {}), dict) else {}
    provinces = region.get("provinces", [])
    if not provinces or "province" not in panel.columns:
        return panel
    allowed = {_normalize_province(value) for value in provinces}
    return panel[panel["province"].map(_normalize_province).isin(allowed)].copy()


def _read_first_existing(paths: list[Path]) -> pd.DataFrame:
    for path in paths:
        frame = _read_table(path)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _key_variable_coverage(relative_path: str, frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    candidates = {
        "province_daily_climate": ["tmax_c", "precipitation_mm"],
        "annual_exposure_panel": ["chd_annual"],
        "province_chd_panel": ["chd_annual", "chd_2022_intensity"],
        "province_model_panel": ["yield_anomaly_pct", "chd_annual"],
        "province_grain_backfill": ["yield_kg_per_hectare", "yield_kg_ha"],
        "model_coefficients": ["estimate"],
        "event_study_coefficients": ["estimate"],
    }
    variables: list[str] = []
    for token, names in candidates.items():
        if token in relative_path:
            variables = names
            break
    parts = []
    for variable in variables:
        if variable in frame.columns:
            parts.append(f"{variable}={_observed_coverage(frame, variable):.6f}")
    return ";".join(parts)


def _observed_coverage(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    values = frame[column]
    return float((values.notna() & values.astype(str).str.strip().ne("")).sum() / len(frame))


def _target_coverage(frame: pd.DataFrame, column: str, min_year: int, max_year: int) -> float:
    if frame.empty or column not in frame.columns or "province" not in frame.columns:
        return 0.0
    province_count = _nunique(frame, "province")
    expected = province_count * (int(max_year) - int(min_year) + 1)
    if expected <= 0:
        return 0.0
    values = frame[column]
    nonmissing = int((values.notna() & values.astype(str).str.strip().ne("")).sum())
    return float(nonmissing / expected)


def _has_model(frame: pd.DataFrame, model: str) -> bool:
    return not frame.empty and "model" in frame.columns and frame["model"].astype(str).eq(model).any()


def _coefficient_row(frame: pd.DataFrame, model: str, term: str) -> dict[str, Any]:
    if frame.empty or not {"model", "term"}.issubset(frame.columns):
        return {}
    match = frame[frame["model"].astype(str).eq(model) & frame["term"].astype(str).eq(term)]
    return {} if match.empty else dict(match.iloc[0].to_dict())


def _pretrend_passed(event: pd.DataFrame) -> bool:
    if event.empty or "term" not in event.columns:
        return False
    pre = event[event["term"].astype(str).str.startswith("event_time_m")].copy()
    if pre.empty:
        return False
    for _, row in pre.iterrows():
        estimate = _to_float(row.get("estimate"))
        standard_error = _to_float(row.get("standard_error"))
        if estimate is None:
            return False
        if standard_error is not None and standard_error > 0:
            if abs(estimate) > 1.96 * standard_error:
                return False
        elif abs(estimate) > 0.1:
            return False
    return True


def _treatment_counts(panel: pd.DataFrame, event_year: int) -> tuple[int, int]:
    if panel.empty or "province" not in panel.columns or "year" not in panel.columns:
        return 0, 0
    event = _filter_year(panel, event_year)
    if event.empty:
        return 0, 0
    if "chd_2022_treated_p75" in event.columns:
        flags = event.groupby("province")["chd_2022_treated_p75"].first().map(_truthy)
    elif "chd_2022_intensity" in event.columns:
        scores = pd.to_numeric(event.groupby("province")["chd_2022_intensity"].mean(), errors="coerce")
        threshold = scores.quantile(0.75)
        flags = scores >= threshold
    else:
        return 0, 0
    treated = int(flags.sum())
    return treated, int(flags.shape[0] - treated)


def _missing_year_concentration(panel: pd.DataFrame) -> float:
    if panel.empty or "province" not in panel.columns or "year" not in panel.columns:
        return 0.0
    provinces = panel["province"].dropna().astype(str).str.strip()
    province_count = int(provinces[provinces.ne("")].nunique())
    years = set(range(2000, 2025))
    present = {int(value) for value in pd.to_numeric(panel["year"], errors="coerce").dropna()}
    missing_cells = {year: province_count for year in years - present}
    total_missing = sum(missing_cells.values())
    if total_missing == 0:
        return 0.0
    return float(sum(missing_cells.get(year, 0) for year in [2008, 2009, 2010]) / total_missing)


def _forbidden_outputs_detected(root: Path) -> bool:
    search_paths = [root / "reports", root / "data" / "outputs"]
    for base in search_paths:
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file() and any(token in path.name for token in FORBIDDEN_OUTPUT_TOKENS):
                return True
            if path.suffix.lower() in {".md", ".csv"}:
                try:
                    text = path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                if any(token in text for token in FORBIDDEN_OUTPUT_TOKENS):
                    return True
    return False


def _check(check: str, passed: bool, value: str, notes: str = "") -> dict[str, Any]:
    return {"check": check, "status": "passed" if passed else "failed", "value": value, "notes": notes}


def _metric(check: str, value: Any, notes: str = "") -> dict[str, Any]:
    return {"check": check, "status": "info", "value": "" if value is None else value, "notes": notes}


def _filter_year(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    if frame.empty or "year" not in frame.columns:
        return frame.iloc[0:0].copy()
    years = pd.to_numeric(frame["year"], errors="coerce")
    return frame[years.eq(int(year))].copy()


def _numeric_summary(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return "count=0"
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return "count=0"
    return f"count={values.shape[0]}, mean={values.mean():.3g}, min={values.min():.3g}, max={values.max():.3g}"


def _top_event_exposure(panel: pd.DataFrame, event_year: int) -> str:
    event = _filter_year(panel, event_year)
    if event.empty or "chd_annual" not in event.columns:
        return "- no event-year CHD exposure rows."
    temp = event.copy()
    temp["chd_annual"] = pd.to_numeric(temp["chd_annual"], errors="coerce")
    rows = temp.dropna(subset=["chd_annual"]).sort_values("chd_annual", ascending=False).head(5)
    if rows.empty:
        return "- no event-year CHD exposure rows."
    return "\n".join(f"- {row.get('province')}: chd_annual={row.get('chd_annual'):.3g}" for _, row in rows.iterrows())


def _rows(path: Path) -> int:
    return len(_read_table(path))


def _audit_value(audit: pd.DataFrame, check: str) -> str:
    if audit.empty or not {"check", "value"}.issubset(audit.columns):
        return ""
    match = audit[audit["check"].astype(str).eq(check)]
    return "" if match.empty else str(match.iloc[0]["value"])


def _code_series(frame: pd.DataFrame) -> pd.Series:
    output = pd.Series("", index=frame.index, dtype="object")
    for column in ["province_code", "admin_code", "province"]:
        if column not in frame.columns:
            continue
        values = frame[column].map(_normalize_code)
        missing = output.eq("")
        output.loc[missing] = values.loc[missing]
    return output


def _normalize_code(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "<na>"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text if text.isdigit() else ""


def _normalize_province(value: Any) -> str:
    text = str(value or "").strip()
    for suffix in ("壮族自治区", "回族自治区", "维吾尔自治区", "自治区", "省", "市"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y", "treated"}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _direction(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return "unavailable"
    if number < 0:
        return "negative"
    if number > 0:
        return "positive"
    return "zero"


def _significance_label(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return "unavailable"
    if number < 0.05:
        return "significant_at_5pct"
    if number < 0.10:
        return "significant_at_10pct"
    return "not_significant"


def _format_number(value: Any) -> str:
    number = _to_float(value)
    return "n/a" if number is None else f"{number:.6g}"


def _first_text(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return ""
    values = frame[column].dropna().astype(str).str.strip()
    values = values[values.ne("")]
    return "" if values.empty else str(values.iloc[0])


def _nunique(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column].dropna().astype(str).str.strip()
    return int(values[values.ne("")].nunique())


def _min_year(frame: pd.DataFrame) -> int | None:
    if frame.empty or "year" not in frame.columns:
        return None
    values = pd.to_numeric(frame["year"], errors="coerce").dropna()
    return int(values.min()) if not values.empty else None


def _max_year(frame: pd.DataFrame) -> int | None:
    if frame.empty or "year" not in frame.columns:
        return None
    values = pd.to_numeric(frame["year"], errors="coerce").dropna()
    return int(values.max()) if not values.empty else None


def _format_year_range(year_min: Any, year_max: Any) -> str:
    return "n/a" if year_min is None or year_max is None or year_min == "" or year_max == "" else f"{year_min}-{year_max}"


def _main_year_min(config: ProjectConfig) -> int:
    years = config.raw.get("panel_policy", {}).get("main_content_years", [config.baseline_years[0], config.validation_event_year])
    return int(years[0])


def _main_year_max(config: ProjectConfig) -> int:
    years = config.raw.get("panel_policy", {}).get("main_content_years", [config.baseline_years[0], config.validation_event_year])
    return int(years[1])
