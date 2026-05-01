"""Statistical modeling utilities."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence


COEFFICIENT_COLUMNS = ["model", "term", "estimate", "n", "r2", "adjusted_r2"]
PREDICTION_COLUMNS = ["row_index", "admin_id", "year", "observed", "prediction", "residual"]
OUTCOME_FALLBACK_CANDIDATES = (
    "province_rice_yield_anomaly",
    "province_grain_yield_anomaly",
    "yield_anomaly_pct",
    "yield_anomaly",
    "yield_kg_per_hectare",
    "grain_yield_kg_per_hectare",
    "rice_yield_kg_per_hectare",
    "actual_yield",
    "yield",
)


@dataclass(frozen=True)
class ModelingResult:
    """Result metadata for the modeling step."""

    status: str
    n_rows: int
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/model_results.md")


def fit_simple_ols(rows: Iterable[dict[str, Any]], y: str, x_vars: Sequence[str]) -> dict[str, Any]:
    """Fit an ordinary least squares model with an intercept using pure Python."""

    fit = _fit_ols_with_predictions(list(rows), y, list(x_vars))
    return {
        "n": fit["n"],
        "coefficients": fit["coefficients"],
        "r2": fit["r2"],
        "adjusted_r2": fit["adjusted_r2"],
    }


def assign_treatment(
    rows: Iterable[dict[str, Any]],
    exposure_field: str,
    year_field: str,
    admin_field: str,
    event_year: int,
    quantile: float = 0.5,
) -> list[dict[str, Any]]:
    """Assign treatment by event-year exposure quantile and return copied rows."""

    if not 0.0 <= float(quantile) <= 1.0:
        raise ValueError("quantile must be between 0 and 1")

    row_list = [dict(row) for row in rows]
    exposure_by_admin: dict[str, list[float]] = {}
    for row in row_list:
        if _coerce_int(row.get(year_field)) != int(event_year):
            continue
        admin_value = row.get(admin_field)
        exposure = _coerce_float(row.get(exposure_field))
        if admin_value is None or exposure is None:
            continue
        exposure_by_admin.setdefault(str(admin_value), []).append(exposure)

    admin_scores = {
        admin: sum(values) / len(values)
        for admin, values in exposure_by_admin.items()
        if values
    }
    threshold = _quantile(list(admin_scores.values()), float(quantile))

    for row in row_list:
        admin = row.get(admin_field)
        score = admin_scores.get(str(admin)) if admin is not None else None
        row["treatment"] = int(threshold is not None and score is not None and score >= threshold)
    return row_list


def build_event_study_terms(
    rows: Iterable[dict[str, Any]],
    treatment_field: str,
    year_field: str,
    event_year: int,
    window: int,
) -> list[dict[str, Any]]:
    """Add treated relative-year event-study indicators and return copied rows."""

    if int(window) < 0:
        raise ValueError("window must be non-negative")

    terms = [_event_term_name(offset) for offset in range(-int(window), int(window) + 1)]
    expanded: list[dict[str, Any]] = []
    for source_row in rows:
        row = dict(source_row)
        year = _coerce_int(row.get(year_field))
        event_time = None if year is None else year - int(event_year)
        treated = _is_truthy(row.get(treatment_field))
        row["event_time"] = event_time if event_time is not None else ""
        for offset, term in zip(range(-int(window), int(window) + 1), terms, strict=True):
            row[term] = int(treated and event_time == offset)
        expanded.append(row)
    return expanded


def run_modeling(
    model_panel: str | Path,
    output_dir: str | Path,
    reports_dir: str | Path,
    event_year: int,
    processed_dir: str | Path | None = None,
    outcome_field: str = "yield_anomaly",
    x_vars: Sequence[str] | None = None,
    exposure_field: str = "exposure_index",
    year_field: str = "year",
    admin_field: str = "admin_id",
    treatment_quantile: float = 0.5,
    event_window: int = 3,
    min_year: int | None = None,
    max_year: int | None = None,
) -> ModelingResult:
    """Run the MVP modeling step from a model-panel CSV using pure Python fallback."""

    panel_path = Path(model_panel).expanduser().resolve()
    outputs = Path(output_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    outputs.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    report_path = reports / "model_results.md"
    scope_report_path = reports / "model_scope_decision.md"
    coefficient_path = outputs / "model_coefficients.csv"
    prediction_path = outputs / "model_predictions.csv"
    event_path = outputs / "event_study_coefficients.csv"
    output_paths = {
        "coefficients": coefficient_path,
        "predictions": prediction_path,
        "event_study_coefficients": event_path,
    }

    warnings: list[str] = []
    if not panel_path.exists() or panel_path.stat().st_size == 0:
        warnings.append(f"No model panel CSV found at {panel_path}.")
        _write_empty_model_outputs(coefficient_path, prediction_path, event_path)
        result = ModelingResult(
            status="missing",
            n_rows=0,
            outputs=output_paths,
            warnings=warnings,
            report_path=report_path,
        )
        scope = _load_model_scope(processed_dir or panel_path.parent)
        _write_model_report(result, panel_path, outcome_field, [], None, scope)
        _write_model_scope_report(scope_report_path, scope)
        return result

    try:
        rows = _read_table_rows(panel_path)
    except Exception as exc:  # noqa: BLE001 - report and empty outputs keep the pipeline moving
        warnings.append(f"Could not read model panel CSV {panel_path}: {type(exc).__name__}: {exc}")
        _write_empty_model_outputs(coefficient_path, prediction_path, event_path)
        result = ModelingResult(
            status="error",
            n_rows=0,
            outputs=output_paths,
            warnings=warnings,
            report_path=report_path,
        )
        scope = _load_model_scope(processed_dir or panel_path.parent)
        _write_model_report(result, panel_path, outcome_field, [], None, scope)
        _write_model_scope_report(scope_report_path, scope)
        return result

    if not rows:
        warnings.append(f"Model panel table is empty: {panel_path}.")
        _write_empty_model_outputs(coefficient_path, prediction_path, event_path)
        result = ModelingResult(
            status="empty",
            n_rows=0,
            outputs=output_paths,
            warnings=warnings,
            report_path=report_path,
        )
        scope = _load_model_scope(processed_dir or panel_path.parent)
        _write_model_report(result, panel_path, outcome_field, [], None, scope)
        _write_model_scope_report(scope_report_path, scope)
        return result

    original_row_count = len(rows)
    rows = _filter_rows_by_year(rows, year_field, min_year, max_year)
    if len(rows) < original_row_count:
        warnings.append(
            f"Filtered model panel to {year_field} between "
            f"{min_year if min_year is not None else '-inf'} and {max_year if max_year is not None else '+inf'}; "
            f"removed {original_row_count - len(rows)} optional/background rows."
        )
    if not rows:
        warnings.append("No model rows remain after year filtering.")
        _write_empty_model_outputs(coefficient_path, prediction_path, event_path)
        result = ModelingResult(
            status="empty",
            n_rows=0,
            outputs=output_paths,
            warnings=warnings,
            report_path=report_path,
        )
        scope = _load_model_scope(processed_dir or panel_path.parent)
        _write_model_report(result, panel_path, outcome_field, [], None, scope)
        _write_model_scope_report(scope_report_path, scope)
        return result

    scope = _load_model_scope(processed_dir or panel_path.parent)
    model_rows = _select_model_rows_for_scope(rows, year_field, event_year, scope, warnings)
    resolved_outcome_field = _resolve_outcome_field(model_rows, outcome_field, warnings)
    resolved_admin_field = _resolve_admin_field(model_rows, admin_field, warnings)
    resolved_exposure_field = _resolve_exposure_field_for_scope(model_rows, exposure_field, scope, warnings)
    selected_x_vars = (
        list(x_vars)
        if x_vars
        else _infer_x_vars(model_rows, resolved_outcome_field, resolved_exposure_field, year_field, resolved_admin_field)
    )
    if not selected_x_vars:
        warnings.append("No usable numeric predictor fields found for descriptive OLS.")
        _write_empty_model_outputs(coefficient_path, prediction_path, event_path)
        result = ModelingResult(
            status="no_model",
            n_rows=len(rows),
            outputs=output_paths,
            warnings=warnings,
            report_path=report_path,
        )
        _write_model_report(result, panel_path, outcome_field, [], None, scope)
        _write_model_scope_report(scope_report_path, scope)
        return result

    try:
        descriptive_fit = _fit_ols_with_predictions(model_rows, resolved_outcome_field, selected_x_vars)
        if descriptive_fit["n"] == 0:
            raise ValueError("No complete numeric rows are available for OLS.")
        _write_coefficient_rows(
            _coefficient_rows(scope.get("primary_model_name", "descriptive_ols"), descriptive_fit),
            coefficient_path,
        )
        _write_prediction_rows(
            descriptive_fit["predictions"],
            prediction_path,
            admin_field=resolved_admin_field,
            year_field=year_field,
        )
        if scope["run_event_study"]:
            event_fit = _fit_event_study(
                rows=rows,
                outcome_field=resolved_outcome_field,
                exposure_field=resolved_exposure_field,
                year_field=year_field,
                admin_field=resolved_admin_field,
                event_year=event_year,
                quantile=treatment_quantile,
                window=event_window,
                warnings=warnings,
            )
        else:
            warnings.append(f"Skipped event-study coefficients because model scope is `{scope['model_scope']}`.")
            event_fit = None
        if event_fit is None:
            _write_coefficient_rows([], event_path)
        else:
            _write_coefficient_rows(_coefficient_rows("event_study", event_fit), event_path)
        status = "ok"
    except Exception as exc:  # noqa: BLE001 - modeling failures should not stop the pipeline
        warnings.append(f"Descriptive OLS failed: {type(exc).__name__}: {exc}")
        _write_empty_model_outputs(coefficient_path, prediction_path, event_path)
        descriptive_fit = None
        status = "error"

    result = ModelingResult(
        status=status,
        n_rows=len(rows),
        outputs=output_paths,
        warnings=warnings,
        report_path=report_path,
    )
    _write_model_report(result, panel_path, resolved_outcome_field, selected_x_vars, descriptive_fit, scope)
    _write_model_scope_report(scope_report_path, scope)
    return result


def _resolve_outcome_field(rows: list[dict[str, Any]], requested_field: str, warnings: list[str]) -> str:
    """Resolve the requested outcome or fall back to an available yield field."""

    if _field_has_numeric_value(rows, requested_field):
        return requested_field

    for candidate in OUTCOME_FALLBACK_CANDIDATES:
        if candidate == requested_field:
            continue
        if _field_has_numeric_value(rows, candidate):
            warnings.append(f"Outcome field `{requested_field}` not found; using `{candidate}`.")
            return candidate

    warnings.append(f"Outcome field `{requested_field}` not found and no fallback yield field is usable.")
    return requested_field


def _filter_rows_by_year(
    rows: list[dict[str, Any]],
    year_field: str,
    min_year: int | None,
    max_year: int | None,
) -> list[dict[str, Any]]:
    """Filter model rows to the configured content-year window."""

    if min_year is None and max_year is None:
        return rows
    filtered: list[dict[str, Any]] = []
    for row in rows:
        year = _coerce_int(row.get(year_field))
        if year is None:
            continue
        if min_year is not None and year < int(min_year):
            continue
        if max_year is not None and year > int(max_year):
            continue
        filtered.append(row)
    return filtered


def _select_model_rows_for_scope(
    rows: list[dict[str, Any]],
    year_field: str,
    event_year: int,
    scope: dict[str, Any],
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Select rows for the allowed model scope."""

    if scope.get("model_scope") != "cross_section_2022_intensity":
        return rows
    selected = [row for row in rows if _coerce_int(row.get(year_field)) == int(event_year)]
    warnings.append(
        "Annual chd_annual coverage is insufficient; modeling is restricted to "
        f"{event_year} cross-sectional intensity association."
    )
    return selected


def _resolve_exposure_field_for_scope(
    rows: list[dict[str, Any]],
    requested_field: str,
    scope: dict[str, Any],
    warnings: list[str],
) -> str:
    """Resolve exposure field according to annual/event coverage gates."""

    preferred: list[str] = []
    if scope.get("model_scope") == "cross_section_2022_intensity":
        preferred.extend(["chd_2022_intensity", requested_field, "exposure_index"])
    elif scope.get("chd_annual_coverage_rate", 0.0) >= 0.75:
        preferred.extend(["chd_annual", requested_field, "exposure_index"])
    else:
        preferred.extend([requested_field, "chd_annual", "exposure_index", "chd_2022_intensity"])
    seen: set[str] = set()
    for field in preferred:
        if field in seen:
            continue
        seen.add(field)
        if _field_has_numeric_variation(rows, field):
            if field != requested_field:
                warnings.append(f"Exposure field `{requested_field}` is not usable for this scope; using `{field}`.")
            return field
    return requested_field


def _load_model_scope(processed_dir: str | Path) -> dict[str, Any]:
    """Load tier and coverage reports and decide which model scope is allowed."""

    import pandas as pd

    processed = Path(processed_dir).expanduser().resolve()
    tier_path = processed / "yield_data_tier_report.csv"
    coverage_path = processed / "yield_coverage_report.csv"
    tier = _read_first_row_csv(tier_path)
    coverage = _read_coverage_rows(coverage_path)
    province_scope = _load_province_outcome_scope(processed)
    if province_scope:
        tier = {
            "tier": province_scope["tier"],
            "tier_name": province_scope["tier_name"],
            "admin_level": province_scope["admin_level"],
            "crop_type": province_scope["crop_type"],
            "year_coverage_rate": province_scope["year_coverage_rate"],
        }
    tier_id = str(tier.get("tier", "unknown"))
    coverage_rate = _coerce_float(tier.get("year_coverage_rate"))
    if coverage_rate is None:
        coverage_rate = _best_official_coverage(coverage)
    claim_gate = _evaluate_causal_claim_gate(processed, coverage_rate)
    exposure_scope = _load_exposure_scope(processed)

    if tier_id == "tier_4":
        model_scope = "remote_sensing_growth_anomaly_analysis"
        run_event_study = False
        conclusion_strength = "descriptive"
    elif (
        tier_id in {"tier_1", "tier_2"}
        and coverage_rate >= 0.75
        and exposure_scope["chd_annual_coverage_rate"] >= 0.75
        and exposure_scope["yield_anomaly_coverage_rate"] >= 0.75
    ):
        model_scope = "fixed_effects_and_event_study"
        run_event_study = True
        conclusion_strength = "quasi_causal" if claim_gate["passed"] else "impact_assessment"
    elif exposure_scope["has_2022_intensity"] and exposure_scope["chd_annual_coverage_rate"] < 0.75:
        model_scope = "cross_section_2022_intensity"
        run_event_study = False
        conclusion_strength = "association"
    elif tier_id in {"tier_1", "tier_2"} and coverage_rate >= 0.5:
        model_scope = "fixed_effects_with_exploratory_event_study"
        run_event_study = True
        conclusion_strength = "impact_assessment"
    elif tier_id in {"tier_1", "tier_2", "tier_3"}:
        model_scope = "descriptive_correlation_only"
        run_event_study = False
        conclusion_strength = "association"
    else:
        model_scope = "legacy_or_unclassified_descriptive"
        run_event_study = True
        conclusion_strength = "impact_assessment"

    return {
        "tier": tier_id,
        "tier_name": tier.get("tier_name", ""),
        "admin_level": tier.get("admin_level", "unknown"),
        "crop_type": tier.get("crop_type", "unknown"),
        "year_coverage_rate": coverage_rate,
        "model_scope": model_scope,
        "run_event_study": run_event_study,
        "conclusion_strength": conclusion_strength,
        "causal_claim_gate_passed": claim_gate["passed"],
        "causal_claim_gate_details": claim_gate["details"],
        "causal_claim_gate_missing": claim_gate["missing"],
        "allowed_language": _allowed_language(conclusion_strength),
        "forbidden_language": "证明 2022 热旱导致单产下降；用遥感代理直接声称官方产量损失",
        "coverage_rows": len(coverage) if hasattr(coverage, "__len__") else 0,
        "chd_annual_coverage_rate": exposure_scope["chd_annual_coverage_rate"],
        "yield_anomaly_coverage_rate": exposure_scope["yield_anomaly_coverage_rate"],
        "has_2022_intensity": exposure_scope["has_2022_intensity"],
        "exposure_coverage_status": exposure_scope["exposure_coverage_status"],
        "primary_model_name": _primary_model_name(model_scope, tier.get("admin_level", "unknown"), tier.get("crop_type", "unknown")),
        "outcome_label": _outcome_label(tier.get("admin_level", "unknown"), tier.get("crop_type", "unknown"), tier_id),
    }


def _read_first_row_csv(path: Path) -> dict[str, Any]:
    """Read the first row from a CSV file."""

    import pandas as pd

    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception:
        return {}
    if frame.empty:
        return {}
    return dict(frame.iloc[0].to_dict())


def _load_province_outcome_scope(processed: Path) -> dict[str, Any]:
    """Load province model-panel metadata when the project has downgraded to province outcomes."""

    frame = _read_first_existing_scope_table(
        [
            processed / "province_model_panel.parquet",
            processed / "province_model_panel.csv",
        ]
    )
    if frame.empty and not {
        "outcome_type",
        "province_rice_yield_anomaly",
        "province_grain_yield_anomaly",
    }.intersection(set(frame.columns)):
        return {}
    outcome_type = ""
    if "outcome_type" in frame.columns:
        values = frame["outcome_type"].dropna().astype(str).str.strip()
        if not values.empty:
            outcome_type = str(values.iloc[0])
    if not outcome_type:
        if _column_coverage(frame, "province_rice_yield_anomaly") > 0:
            outcome_type = "province_rice_yield_anomaly"
        else:
            outcome_type = "province_grain_yield_anomaly"
    crop_type = "rice" if outcome_type == "province_rice_yield_anomaly" else "grain"
    coverage_rate = _column_coverage(frame, outcome_type)
    if coverage_rate == 0.0:
        coverage_rate = _column_coverage(frame, "yield_anomaly_pct")
    return {
        "tier": "tier_3",
        "tier_name": "provincial_official_yield_panel",
        "admin_level": "province",
        "crop_type": crop_type,
        "year_coverage_rate": coverage_rate,
        "outcome_type": outcome_type,
    }


def _read_coverage_rows(path: Path) -> Any:
    """Read yield coverage report CSV."""

    import pandas as pd

    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _best_official_coverage(frame: Any) -> float:
    """Return best official county/prefecture coverage from coverage report."""

    if frame is None or len(frame) == 0 or "year_coverage_rate" not in frame.columns:
        return 1.0
    if "admin_level" in frame.columns:
        candidates = frame[frame["admin_level"].astype(str).isin(["county", "prefecture"])]
    else:
        candidates = frame.iloc[0:0]
    if candidates.empty:
        candidates = frame
    values = []
    for value in candidates["year_coverage_rate"]:
        number = _coerce_float(value)
        if number is not None:
            values.append(number)
    return max(values) if values else 0.0


def _load_exposure_scope(processed: Path) -> dict[str, Any]:
    """Load exposure coverage facts for model gating."""

    import pandas as pd

    panel = _read_first_existing_scope_table(
        [
            processed / "province_model_panel.parquet",
            processed / "province_model_panel.csv",
            processed / "model_panel_study_region.csv",
            processed / "model_panel.csv",
        ]
    )
    if panel.empty:
        return {
            "chd_annual_coverage_rate": 0.0,
            "yield_anomaly_coverage_rate": 0.0,
            "has_2022_intensity": False,
            "exposure_coverage_status": "not_usable_until_fixed",
        }
    years = pd.to_numeric(panel.get("year", pd.Series(dtype=float)), errors="coerce")
    main = panel[(years >= 2000) & (years <= 2024)].copy()
    if main.empty:
        main = panel
    chd_rate = _column_coverage(main, "chd_annual")
    yield_rate = max(
        _column_coverage(main, "yield_anomaly_pct"),
        _column_coverage(main, "province_rice_yield_anomaly"),
        _column_coverage(main, "province_grain_yield_anomaly"),
    )
    if chd_rate == 0.0:
        chd_rate = _column_coverage(main, "exposure_index")
    has_2022 = _has_event_exposure(main, "chd_2022_intensity", 2022) or _has_event_exposure(main, "exposure_index", 2022)
    diagnosis = _read_exposure_diagnosis(processed.parent / "outputs" / "exposure_coverage_diagnosis.csv")
    status = diagnosis or ("ok_for_panel_model" if chd_rate >= 0.75 else "ok_only_for_2022_cross_section" if has_2022 else "not_usable_until_fixed")
    return {
        "chd_annual_coverage_rate": chd_rate,
        "yield_anomaly_coverage_rate": yield_rate,
        "has_2022_intensity": has_2022,
        "exposure_coverage_status": status,
    }


def _read_first_existing_scope_table(paths: list[Path]) -> Any:
    """Read first available scope table."""

    import pandas as pd

    for path in paths:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".parquet":
                return pd.read_parquet(path)
            return pd.read_csv(path, dtype=str, low_memory=False)
        except Exception:
            continue
    return pd.DataFrame()


def _column_coverage(frame: Any, column: str) -> float:
    """Return non-empty coverage rate for a frame column."""

    if frame is None or len(frame) == 0 or column not in frame.columns:
        return 0.0
    values = frame[column]
    nonmissing = values.notna() & values.astype(str).str.strip().ne("")
    return float(nonmissing.sum() / len(frame)) if len(frame) else 0.0


def _has_event_exposure(frame: Any, column: str, event_year: int) -> bool:
    """Return True when event-year exposure exists."""

    if frame is None or len(frame) == 0 or column not in frame.columns or "year" not in frame.columns:
        return False
    years = [_coerce_int(value) for value in frame["year"]]
    mask = [year == int(event_year) for year in years]
    if not any(mask):
        return False
    values = frame.loc[mask, column]
    return bool((values.notna() & values.astype(str).str.strip().ne("")).any())


def _read_exposure_diagnosis(path: Path) -> str | None:
    """Read machine exposure coverage status from diagnostics output."""

    import pandas as pd

    if not path.exists():
        return None
    try:
        frame = pd.read_csv(path, dtype=str, low_memory=False)
    except Exception:
        return None
    if frame.empty or not {"metric", "value"}.issubset(frame.columns):
        return None
    match = frame[frame["metric"].astype(str).eq("exposure_coverage_status")]
    if match.empty:
        return None
    return str(match.iloc[0]["value"])


def _primary_model_name(model_scope: str, admin_level: Any, crop_type: Any) -> str:
    """Return coefficient model name consistent with data scope."""

    level = str(admin_level)
    crop = str(crop_type)
    if model_scope == "cross_section_2022_intensity":
        if level == "province" and crop == "grain":
            return "province_grain_2022_cross_section"
        return "chd_2022_cross_section"
    if model_scope == "fixed_effects_and_event_study":
        return "two_way_fixed_effects_candidate"
    return "descriptive_ols"


def _outcome_label(admin_level: Any, crop_type: Any, tier_id: str) -> str:
    """Return human-readable outcome label for reports."""

    level = str(admin_level or "unknown")
    crop = str(crop_type or "unknown")
    if tier_id == "tier_4":
        return "遥感代理长势异常"
    if level == "province" and crop == "grain":
        return "省级粮食单产异常"
    if level == "province" and crop in {"rice", "early_rice", "single_rice", "middle_rice", "late_rice"}:
        return "省级稻谷单产异常"
    if crop in {"rice", "early_rice", "single_rice", "middle_rice", "late_rice"}:
        return f"{level} 稻谷/水稻单产异常"
    if crop == "grain":
        return f"{level} 粮食单产异常"
    return "单产异常"


def _evaluate_causal_claim_gate(processed: Path, coverage_rate: float) -> dict[str, Any]:
    """Evaluate conservative gates before allowing quasi-causal wording."""

    details: dict[str, bool] = {
        "yield_panel_coverage_gte_0_75": coverage_rate >= 0.75,
        "pretrend_test_passed": False,
        "placebo_tests_passed": False,
        "robustness_direction_stable": False,
        "admin_crosswalk_match_rate_gte_0_90": _admin_crosswalk_match_rate(processed) >= 0.90,
        "mechanism_remote_sensing_consistent": False,
    }
    details.update(_read_claim_gate_overrides(processed))
    missing = [key for key, passed in details.items() if not passed]
    return {"passed": not missing, "details": details, "missing": missing}


def _admin_crosswalk_match_rate(processed: Path) -> float:
    """Estimate high-confidence crosswalk rate from the generated crosswalk."""

    import pandas as pd

    for name in ("admin_crosswalk_2000_2025.csv", "admin_crosswalk_2000_2024.csv"):
        path = processed / name
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path, dtype=str, low_memory=False)
        except Exception:
            continue
        if frame.empty or "match_confidence" not in frame.columns:
            continue
        confidence = pd.to_numeric(frame["match_confidence"], errors="coerce").dropna()
        if confidence.empty:
            continue
        return float((confidence >= 0.85).sum() / len(confidence))
    return 0.0


def _read_claim_gate_overrides(processed: Path) -> dict[str, bool]:
    """Read optional diagnostics gate statuses without requiring them for MVP."""

    import pandas as pd

    paths = [
        processed / "causal_claim_gate_status.csv",
        processed.parent / "outputs" / "causal_claim_gate_status.csv",
    ]
    overrides: dict[str, bool] = {}
    for path in paths:
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if {"gate", "passed"}.issubset(frame.columns):
            for _, row in frame.iterrows():
                gate = str(row.get("gate", "")).strip()
                if gate:
                    overrides[gate] = _coerce_bool(row.get("passed"))
        elif not frame.empty:
            for column in frame.columns:
                overrides[str(column)] = _coerce_bool(frame.iloc[0][column])
    return overrides


def _coerce_bool(value: Any) -> bool:
    """Coerce common CSV boolean spellings."""

    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "passed", "pass"}


def _allowed_language(strength: str) -> str:
    """Return allowed model-result language for the current conclusion strength."""

    if strength == "quasi_causal":
        return "事件研究结果可表述为准因果识别支持，但必须附数据覆盖和区划匹配限制。"
    if strength == "impact_assessment":
        return "复合热旱暴露与单产异常下降显著相关；固定效应结果可作为影响评估。"
    if strength == "association":
        return "复合热旱暴露与单产异常存在相关关系。"
    return "仅可描述遥感长势异常或暴露格局。"


def _resolve_admin_field(rows: list[dict[str, Any]], requested_field: str, warnings: list[str]) -> str:
    """Resolve the panel unit field for admin/province-level modeling."""

    if _field_has_value(rows, requested_field):
        return requested_field
    for candidate in ("admin_code", "province", "county", "prefecture"):
        if _field_has_value(rows, candidate):
            warnings.append(f"Admin field `{requested_field}` not found; using `{candidate}`.")
            return candidate
    warnings.append(f"Admin field `{requested_field}` not found and no fallback panel unit field is usable.")
    return requested_field


def _fit_event_study(
    rows: list[dict[str, Any]],
    outcome_field: str,
    exposure_field: str,
    year_field: str,
    admin_field: str,
    event_year: int,
    quantile: float,
    window: int,
    warnings: list[str],
) -> dict[str, Any] | None:
    """Fit a minimal event-study OLS when enough treated relative-year variation exists."""

    if not _fields_exist(rows, [outcome_field, exposure_field, year_field, admin_field]):
        warnings.append("Skipped event-study coefficients because required fields are missing.")
        return None

    treated_rows = assign_treatment(
        rows,
        exposure_field=exposure_field,
        year_field=year_field,
        admin_field=admin_field,
        event_year=event_year,
        quantile=quantile,
    )
    treatment_values = {
        int(row.get("treatment", 0))
        for row in treated_rows
        if _coerce_int(row.get(year_field)) == int(event_year)
    }
    if len(treatment_values) < 2:
        warnings.append("Skipped event-study coefficients because treatment has no variation.")
        return None

    event_rows = build_event_study_terms(
        treated_rows,
        treatment_field="treatment",
        year_field=year_field,
        event_year=event_year,
        window=window,
    )
    terms = [_event_term_name(offset) for offset in range(-int(window), int(window) + 1)]
    varying_terms = [term for term in terms if any(_coerce_float(row.get(term)) for row in event_rows)]
    if not varying_terms:
        warnings.append("Skipped event-study coefficients because no treated event-window observations were found.")
        return None

    try:
        fit = _fit_ols_with_predictions(event_rows, outcome_field, varying_terms)
    except Exception as exc:  # noqa: BLE001 - event study is optional in the MVP
        warnings.append(f"Skipped event-study coefficients: {type(exc).__name__}: {exc}")
        return None
    if fit["n"] == 0:
        warnings.append("Skipped event-study coefficients because no complete rows were available.")
        return None
    return fit


def _fit_ols_with_predictions(
    rows: list[dict[str, Any]],
    y: str,
    x_vars: list[str],
) -> dict[str, Any]:
    """Fit OLS and keep row-level predictions for complete rows."""

    y_values: list[float] = []
    x_values: list[list[float]] = []
    complete_rows: list[tuple[int, dict[str, Any]]] = []
    for index, row in enumerate(rows):
        y_value = _coerce_float(row.get(y))
        x_row = [_coerce_float(row.get(name)) for name in x_vars]
        if y_value is None or any(value is None for value in x_row):
            continue
        y_values.append(y_value)
        x_values.append([1.0] + [float(value) for value in x_row if value is not None])
        complete_rows.append((index, row))

    coefficient_names = ["intercept", *x_vars]
    empty_coefficients = {name: None for name in coefficient_names}
    if not y_values:
        return {
            "n": 0,
            "coefficients": empty_coefficients,
            "r2": None,
            "adjusted_r2": None,
            "predictions": [],
        }

    beta = _least_squares_coefficients(x_values, y_values)
    predictions = []
    fitted_values: list[float] = []
    for (index, row), x_row, observed in zip(complete_rows, x_values, y_values, strict=True):
        prediction = sum(beta_value * x_value for beta_value, x_value in zip(beta, x_row, strict=True))
        fitted_values.append(prediction)
        predictions.append(
            {
                "row_index": index,
                "source": row,
                "observed": observed,
                "prediction": prediction,
                "residual": observed - prediction,
            }
        )

    r2 = _r_squared(y_values, fitted_values)
    adjusted_r2 = _adjusted_r_squared(r2, n=len(y_values), predictor_count=len(x_vars))
    return {
        "n": len(y_values),
        "coefficients": dict(zip(coefficient_names, beta, strict=True)),
        "r2": r2,
        "adjusted_r2": adjusted_r2,
        "predictions": predictions,
    }


def _least_squares_coefficients(x_values: list[list[float]], y_values: list[float]) -> list[float]:
    """Solve OLS normal equations using Gauss-Jordan elimination."""

    column_count = len(x_values[0])
    xtx = [[0.0 for _ in range(column_count)] for _ in range(column_count)]
    xty = [0.0 for _ in range(column_count)]
    for x_row, y_value in zip(x_values, y_values, strict=True):
        for row_index in range(column_count):
            xty[row_index] += x_row[row_index] * y_value
            for column_index in range(column_count):
                xtx[row_index][column_index] += x_row[row_index] * x_row[column_index]
    return _solve_linear_system(xtx, xty)


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    """Solve a square linear system with partial pivoting."""

    size = len(vector)
    augmented = [list(row) + [float(value)] for row, value in zip(matrix, vector, strict=True)]

    for column in range(size):
        pivot_row = max(range(column, size), key=lambda row_index: abs(augmented[row_index][column]))
        pivot_value = augmented[pivot_row][column]
        if abs(pivot_value) < 1e-12:
            raise ValueError("OLS design matrix is singular.")
        if pivot_row != column:
            augmented[column], augmented[pivot_row] = augmented[pivot_row], augmented[column]

        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row_index in range(size):
            if row_index == column:
                continue
            factor = augmented[row_index][column]
            if factor == 0.0:
                continue
            augmented[row_index] = [
                current - factor * pivot
                for current, pivot in zip(augmented[row_index], augmented[column], strict=True)
            ]

    return [augmented[row_index][-1] for row_index in range(size)]


def _r_squared(y_values: list[float], fitted_values: list[float]) -> float:
    """Compute R-squared, handling constant outcomes."""

    mean_y = sum(y_values) / len(y_values)
    total_sum_squares = sum((value - mean_y) ** 2 for value in y_values)
    residual_sum_squares = sum(
        (observed - fitted) ** 2
        for observed, fitted in zip(y_values, fitted_values, strict=True)
    )
    if total_sum_squares <= 1e-12:
        return 1.0 if residual_sum_squares <= 1e-12 else 0.0
    return 1.0 - residual_sum_squares / total_sum_squares


def _adjusted_r_squared(r2: float | None, n: int, predictor_count: int) -> float | None:
    """Compute adjusted R-squared when the denominator is defined."""

    if r2 is None or n <= predictor_count + 1:
        return None
    return 1.0 - (1.0 - r2) * (n - 1) / (n - predictor_count - 1)


def _infer_x_vars(
    rows: list[dict[str, Any]],
    outcome_field: str,
    exposure_field: str,
    year_field: str,
    admin_field: str,
) -> list[str]:
    """Infer a conservative single predictor for the descriptive MVP model."""

    if not rows:
        return []
    if _field_has_numeric_variation(rows, exposure_field) and exposure_field != outcome_field:
        return [exposure_field]

    excluded = {outcome_field, year_field, admin_field}
    for candidate in rows[0].keys():
        if candidate in excluded:
            continue
        if _field_has_numeric_variation(rows, candidate):
            return [candidate]
    return []


def _field_has_numeric_value(rows: list[dict[str, Any]], field_name: str) -> bool:
    """Return True when at least one row has a numeric value for a field."""

    return any(_coerce_float(row.get(field_name)) is not None for row in rows)


def _field_has_value(rows: list[dict[str, Any]], field_name: str) -> bool:
    """Return True when at least one row has a non-blank value for a field."""

    return any(str(row.get(field_name) or "").strip() for row in rows)


def _field_has_numeric_variation(rows: list[dict[str, Any]], field_name: str) -> bool:
    """Return True when a field has at least two distinct numeric values."""

    values = {
        value
        for row in rows
        for value in [_coerce_float(row.get(field_name))]
        if value is not None
    }
    return len(values) >= 2


def _fields_exist(rows: list[dict[str, Any]], fields: list[str]) -> bool:
    """Return True when all fields appear in at least one row."""

    keys = set().union(*(row.keys() for row in rows)) if rows else set()
    return all(field in keys for field in fields)


def _quantile(values: list[float], quantile: float) -> float | None:
    """Compute a linear-interpolated quantile for a list of floats."""

    if not values:
        return None
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def _event_term_name(offset: int) -> str:
    """Build a stable event-study term name."""

    if offset < 0:
        return f"event_time_m{abs(offset)}"
    if offset > 0:
        return f"event_time_p{offset}"
    return "event_time_0"


def _coerce_float(value: Any) -> float | None:
    """Coerce CSV or Python values to finite floats."""

    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    text = str(value).strip()
    if text == "":
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _coerce_int(value: Any) -> int | None:
    """Coerce a value to int when it represents a numeric year."""

    number = _coerce_float(value)
    if number is None:
        return None
    return int(number)


def _is_truthy(value: Any) -> bool:
    """Interpret common treatment flags."""

    number = _coerce_float(value)
    if number is not None:
        return number != 0.0
    return str(value).strip().lower() in {"true", "yes", "y", "treated"}


def _read_table_rows(path: Path) -> list[dict[str, Any]]:
    """Read a CSV or Parquet model panel into dictionaries."""

    if path.suffix.lower() == ".parquet":
        import pandas as pd

        frame = pd.read_parquet(path)
        return [
            {str(column): _format_csv_value(value) for column, value in row.items()}
            for row in frame.to_dict(orient="records")
        ]
    return _read_csv_rows(path)


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    """Read a CSV file into dictionaries without pandas."""

    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        return [dict(row) for row in reader]


def _write_empty_model_outputs(coefficient_path: Path, prediction_path: Path, event_path: Path) -> None:
    """Write empty modeling CSV artifacts with headers."""

    _write_coefficient_rows([], coefficient_path)
    _write_prediction_rows([], prediction_path)
    _write_coefficient_rows([], event_path)


def _coefficient_rows(model_name: str, fit: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert an OLS fit dictionary to coefficient CSV rows."""

    return [
        {
            "model": model_name,
            "term": term,
            "estimate": estimate,
            "n": fit["n"],
            "r2": fit["r2"],
            "adjusted_r2": fit["adjusted_r2"],
        }
        for term, estimate in fit["coefficients"].items()
        if estimate is not None
    ]


def _write_coefficient_rows(rows: list[dict[str, Any]], output_path: Path) -> None:
    """Write coefficient rows to CSV."""

    _write_csv_rows(rows, COEFFICIENT_COLUMNS, output_path)


def _write_prediction_rows(
    rows: list[dict[str, Any]],
    output_path: Path,
    admin_field: str = "admin_id",
    year_field: str = "year",
) -> None:
    """Write row-level prediction diagnostics to CSV."""

    csv_rows = [
        {
            "row_index": row["row_index"],
            "admin_id": row["source"].get(admin_field, ""),
            "year": row["source"].get(year_field, ""),
            "observed": row["observed"],
            "prediction": row["prediction"],
            "residual": row["residual"],
        }
        for row in rows
    ]
    _write_csv_rows(csv_rows, PREDICTION_COLUMNS, output_path)


def _write_csv_rows(rows: list[dict[str, Any]], columns: list[str], output_path: Path) -> None:
    """Write rows as CSV, creating parent directories first."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _format_csv_value(row.get(column, "")) for column in columns})


def _format_csv_value(value: Any) -> Any:
    """Format floats compactly while preserving CSV-friendly strings."""

    if isinstance(value, float):
        return format(value, ".15g")
    if value is None:
        return ""
    return value


def _write_model_report(
    result: ModelingResult,
    panel_path: Path,
    outcome_field: str,
    x_vars: list[str],
    fit: dict[str, Any] | None,
    scope: dict[str, Any],
) -> None:
    """Write a Markdown summary for the modeling step."""

    lines = [
        "# Model Results",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Model panel: `{panel_path}`",
        f"- Rows read: {result.n_rows}",
        f"- Outcome: `{outcome_field}`",
        f"- Predictors: {', '.join(f'`{name}`' for name in x_vars) if x_vars else 'none'}",
        "",
        "## Model Scope Decision",
        "",
        f"- Yield data tier: `{scope.get('tier')}`",
        f"- Outcome label: `{scope.get('outcome_label')}`",
        f"- Model scope: `{scope.get('model_scope')}`",
        f"- Conclusion strength: `{scope.get('conclusion_strength')}`",
        f"- Event study allowed: {scope.get('run_event_study')}",
        f"- chd_annual coverage rate: {_format_report_number(scope.get('chd_annual_coverage_rate'))}",
        f"- yield_anomaly_pct coverage rate: {_format_report_number(scope.get('yield_anomaly_coverage_rate'))}",
        f"- exposure coverage status: `{scope.get('exposure_coverage_status')}`",
        f"- Allowed language: {scope.get('allowed_language')}",
        f"- Forbidden language: {scope.get('forbidden_language')}",
        "",
    ]

    if result.status == "missing":
        lines.extend(["No model panel CSV found.", ""])
    elif result.status == "empty":
        lines.extend(["Model panel table is empty.", ""])

    if fit is not None:
        lines.extend(
            [
                "## Descriptive OLS",
                "",
                f"- Complete rows: {fit['n']}",
                f"- R-squared: {_format_report_number(fit['r2'])}",
                f"- Adjusted R-squared: {_format_report_number(fit['adjusted_r2'])}",
                "",
                "| Term | Estimate |",
                "| --- | ---: |",
            ]
        )
        lines.extend(
            f"| `{term}` | {_format_report_number(estimate)} |"
            for term, estimate in fit["coefficients"].items()
        )
        lines.append("")

    if result.outputs:
        lines.extend(["## Outputs", ""])
        lines.extend(f"- {key}: `{path}`" for key, path in result.outputs.items())
        lines.append("")

    if result.warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
        lines.append("")

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _write_model_scope_report(path: Path, scope: dict[str, Any]) -> None:
    """Write the standalone model-scope decision report."""

    lines = [
        "# Model Scope Decision",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Yield data tier: `{scope.get('tier')}`",
        f"- Tier name: `{scope.get('tier_name')}`",
        f"- Admin level: `{scope.get('admin_level')}`",
        f"- Crop type: `{scope.get('crop_type')}`",
        f"- Outcome label: `{scope.get('outcome_label')}`",
        f"- Year coverage rate: {_format_report_number(scope.get('year_coverage_rate'))}",
        f"- chd_annual coverage rate: {_format_report_number(scope.get('chd_annual_coverage_rate'))}",
        f"- yield_anomaly_pct coverage rate: {_format_report_number(scope.get('yield_anomaly_coverage_rate'))}",
        f"- Exposure coverage status: `{scope.get('exposure_coverage_status')}`",
        f"- Has 2022 event intensity: {scope.get('has_2022_intensity')}",
        f"- Model scope: `{scope.get('model_scope')}`",
        f"- Event study allowed: {scope.get('run_event_study')}",
        f"- Conclusion strength: `{scope.get('conclusion_strength')}`",
        f"- Quasi-causal claim gate passed: {scope.get('causal_claim_gate_passed')}",
        "",
        "## Allowed Language",
        "",
        f"- {scope.get('allowed_language')}",
        "- 若准因果门控未通过，只能写“复合热旱暴露与单产异常下降存在显著关联”。",
        "- 若准因果门控未通过，必须写“本文结果属于影响评估，不构成强因果识别”。",
        "",
        "## Forbidden Language",
        "",
        f"- {scope.get('forbidden_language')}",
        "- 任何情况下都不得输出“证明 2022 热旱导致单产下降”。",
        "- 不得写“完全识别因果效应”。",
        "",
        "## Causal Claim Gate",
        "",
        *_format_claim_gate(scope.get("causal_claim_gate_details", {})),
        "",
        "## 2024 Validation Rule",
        "",
        "- 2024 年只作为外部一致性验证或描述性对照；若省级官方粮食/稻谷数据可得，可用于交叉验证。",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines)
    path.write_text(content, encoding="utf-8")
    if path.name != "model_claim_scope.md":
        (path.parent / "model_claim_scope.md").write_text(content, encoding="utf-8")


def _format_claim_gate(details: Any) -> list[str]:
    """Format claim-gate details for Markdown."""

    if not isinstance(details, dict) or not details:
        return ["- No causal claim gate diagnostics found; quasi-causal wording is not allowed."]
    lines = ["| Gate | Passed |", "| --- | --- |"]
    for key, passed in details.items():
        lines.append(f"| {key} | {bool(passed)} |")
    return lines


def _format_report_number(value: Any) -> str:
    """Format numbers for Markdown reports."""

    if isinstance(value, float):
        return format(value, ".6g")
    if value is None:
        return "n/a"
    return str(value)
