"""Risk register generation for the heat-drought rice modeling project."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RiskReportResult:
    """Generated risk-report outputs and warnings."""

    report_path: Path
    risk_register_path: Path
    coverage_summary_path: Path
    unmatched_admin_path: Path
    calibration_summary_path: Path
    data_gap_report_path: Path
    model_scope_decision_path: Path
    data_source_decision_path: Path
    yield_panel_feasibility_path: Path
    admin_crosswalk_decision_path: Path
    model_claim_scope_path: Path
    external_access_status_path: Path
    risk_count: int
    warnings: list[str] = field(default_factory=list)


def generate_risk_action_report(
    processed_dir: str | Path,
    reports_dir: str | Path,
    references_dir: str | Path | None = None,
    target_year_min: int = 2000,
    target_year_max: int = 2024,
) -> RiskReportResult:
    """Generate current residual-risk tables and a Markdown action report."""

    import pandas as pd

    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    references = Path(references_dir).expanduser().resolve() if references_dir else None
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    summary = _collect_summary(processed, references, warnings, target_year_min, target_year_max)
    unmatched = summary["unmatched_admin_units"]
    calibration_summary = summary["yield_proxy_calibration_summary"]
    coverage_summary = _build_coverage_summary(summary)
    risks = _build_risks(summary)

    unmatched_path = processed / "admin_unmatched_province_units.csv"
    coverage_path = processed / "risk_coverage_summary.csv"
    risk_register_path = processed / "risk_register.csv"
    calibration_path = processed / "yield_proxy" / "calibration_status_summary.csv"
    report_path = reports / "unavoidable_risks_for_research.md"
    data_gap_path = reports / "data_gap_report.md"
    scope_path = reports / "model_scope_decision.md"
    data_source_decision_path = reports / "data_source_decision.md"
    yield_panel_feasibility_path = reports / "yield_panel_feasibility.md"
    admin_crosswalk_decision_path = reports / "admin_crosswalk_decision.md"
    model_claim_scope_path = reports / "model_claim_scope.md"
    external_access_status_path = reports / "external_access_status.md"

    unmatched_path.parent.mkdir(parents=True, exist_ok=True)
    calibration_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched.to_csv(unmatched_path, index=False, encoding="utf-8-sig")
    calibration_summary.to_csv(calibration_path, index=False, encoding="utf-8-sig")
    coverage_summary.to_csv(coverage_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(risks).to_csv(risk_register_path, index=False, encoding="utf-8-sig")
    _write_markdown_report(report_path, summary, coverage_summary, risks, warnings)
    _write_data_gap_report(data_gap_path, summary, risks)
    _write_model_scope_decision(scope_path, processed, summary, risks)
    _write_decision_reports(
        paths={
            "data_source": data_source_decision_path,
            "yield_feasibility": yield_panel_feasibility_path,
            "admin_crosswalk": admin_crosswalk_decision_path,
            "model_claim": model_claim_scope_path,
            "external_access": external_access_status_path,
        },
        summary=summary,
        risks=risks,
        target_year_min=target_year_min,
        target_year_max=target_year_max,
    )

    return RiskReportResult(
        report_path=report_path,
        risk_register_path=risk_register_path,
        coverage_summary_path=coverage_path,
        unmatched_admin_path=unmatched_path,
        calibration_summary_path=calibration_path,
        data_gap_report_path=data_gap_path,
        model_scope_decision_path=scope_path,
        data_source_decision_path=data_source_decision_path,
        yield_panel_feasibility_path=yield_panel_feasibility_path,
        admin_crosswalk_decision_path=admin_crosswalk_decision_path,
        model_claim_scope_path=model_claim_scope_path,
        external_access_status_path=external_access_status_path,
        risk_count=len(risks),
        warnings=warnings,
    )


def write_external_access_check(
    access_config: dict[str, Any],
    processed_dir: str | Path,
    reports_dir: str | Path,
) -> dict[str, Path]:
    """Check external data-access setup without reading secret contents."""

    import importlib.util
    import pandas as pd

    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    csv_path = processed / "external_access_check.csv"
    report_path = reports / "external_access_check.md"

    rows: list[dict[str, Any]] = []
    cds_config = access_config.get("cds", {}) if isinstance(access_config, dict) else {}
    cds_path = Path(str(cds_config.get("local_secret_file", "~/.cdsapirc"))).expanduser()
    rows.append(
        {
            "provider": "cds",
            "required_for": "; ".join(cds_config.get("required_for", [])),
            "check": "local_secret_file_exists",
            "configured": cds_path.exists(),
            "details": str(cds_path),
        }
    )
    earthdata_config = access_config.get("earthdata", {}) if isinstance(access_config, dict) else {}
    rows.append(
        {
            "provider": "earthdata",
            "required_for": "; ".join(earthdata_config.get("required_for", [])),
            "check": "earthaccess_importable",
            "configured": importlib.util.find_spec("earthaccess") is not None,
            "details": "module import check only",
        }
    )
    netrc_path = Path("~/.netrc").expanduser()
    rows.append(
        {
            "provider": "earthdata",
            "required_for": "; ".join(earthdata_config.get("required_for", [])),
            "check": "netrc_exists",
            "configured": netrc_path.exists(),
            "details": str(netrc_path),
        }
    )
    for provider in ("cma", "cnki", "eps"):
        provider_config = access_config.get(provider, {}) if isinstance(access_config, dict) else {}
        rows.append(
            {
                "provider": provider,
                "required_for": "; ".join(provider_config.get("required_for", [])),
                "check": "manual_or_subscription_required",
                "configured": False,
                "details": "external account/subscription must be verified outside this codebase",
            }
        )

    frame = pd.DataFrame(rows)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    _write_external_access_markdown(report_path, rows)
    return {"csv_path": csv_path, "report_path": report_path}


def _collect_summary(
    processed: Path,
    references: Path | None,
    warnings: list[str],
    target_year_min: int,
    target_year_max: int,
) -> dict[str, Any]:
    """Collect compact facts from current generated outputs."""

    admin = _read_first_table(
        [
            processed / "admin_units_with_province.parquet",
            processed / "admin_units_with_province.csv",
            processed / "admin_units.parquet",
            processed / "admin_units.csv",
        ],
        warnings,
        "admin units",
    )
    crop = _read_optional_table(processed / "crop_mask_summary_by_admin.csv", warnings, "crop mask summary")
    phenology = _read_optional_table(processed / "phenology_by_admin.csv", warnings, "phenology summary")
    model_panel = _read_optional_table(processed / "model_panel.csv", warnings, "model panel")
    admin_crosswalk = _read_optional_table(processed / "admin_crosswalk_2000_2025.csv", warnings, "admin crosswalk")
    proxy_panel = _read_first_table(
        [
            processed / "yield_proxy" / "county_yield_proxy_panel.parquet",
            processed / "yield_proxy" / "county_yield_proxy_panel.csv",
        ],
        warnings,
        "yield proxy panel",
    )
    proxy_gap = _read_optional_table(processed / "yield_proxy" / "yield_proxy_gap_report.csv", warnings, "yield proxy gap report")
    source_catalog = _read_optional_table(
        (references / "deep_required_data_sources.csv") if references else processed / "__missing_references__.csv",
        warnings,
        "deep required data sources",
    )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "admin": _summarize_admin(admin),
        "crop": _summarize_status_table(crop, "status"),
        "phenology": _summarize_status_table(phenology, "status"),
        "model_panel": _summarize_model_panel(model_panel, target_year_min, target_year_max),
        "admin_crosswalk": _summarize_admin_crosswalk(admin_crosswalk),
        "yield_proxy": _summarize_yield_proxy(proxy_panel, proxy_gap, target_year_min, target_year_max),
        "climate_panel": _summarize_spatial_panel_pair(processed, "admin_climate_panel", warnings),
        "remote_sensing_panel": _summarize_spatial_panel_pair(processed, "admin_remote_sensing_panel", warnings),
        "source_catalog": _summarize_source_catalog(source_catalog),
        "unmatched_admin_units": _extract_unmatched_admin(admin),
        "yield_proxy_calibration_summary": _summarize_proxy_calibration(proxy_panel),
    }


def _read_first_table(paths: list[Path], warnings: list[str], label: str) -> Any:
    """Read the first existing table from candidate paths."""

    for path in paths:
        if path.exists():
            return _read_table(path, warnings, label)
    warnings.append(f"Missing {label}: no candidate file exists.")
    return _empty_frame()


def _read_optional_table(path: Path, warnings: list[str], label: str) -> Any:
    """Read an optional table, returning an empty frame when absent."""

    if not path.exists():
        warnings.append(f"Missing {label}: {path}")
        return _empty_frame()
    return _read_table(path, warnings, label)


def _read_table(path: Path, warnings: list[str], label: str) -> Any:
    """Read CSV or Parquet into a pandas DataFrame."""

    import pandas as pd

    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as exc:  # noqa: BLE001 - reporting must survive partial outputs
        warnings.append(f"Unable to read {label} at {path}: {type(exc).__name__}: {exc}")
        return _empty_frame()


def _empty_frame() -> Any:
    """Return an empty pandas DataFrame without importing pandas at module import."""

    import pandas as pd

    return pd.DataFrame()


def _summarize_admin(admin: Any) -> dict[str, Any]:
    """Summarize administrative boundary/province assignment coverage."""

    total = int(len(admin)) if admin is not None else 0
    unmatched = _extract_unmatched_admin(admin)
    province_values = _value_counts(admin, "province_name") if admin is not None and len(admin) else {}
    non_chinese = [
        name
        for name in province_values
        if name != "missing" and any(("A" <= character <= "Z") or ("a" <= character <= "z") for character in name)
    ]
    return {
        "admin_units": total,
        "province_matched": total - int(len(unmatched)),
        "province_unmatched": int(len(unmatched)),
        "non_chinese_province_names": sorted(non_chinese),
    }


def _extract_unmatched_admin(admin: Any) -> Any:
    """Return admin units without an assigned province."""

    import pandas as pd

    if admin is None or len(admin) == 0:
        return pd.DataFrame(columns=["admin_id", "shapeName", "shapeGroup", "shapeType", "province_name"])
    frame = admin.copy()
    province_column = "province_name" if "province_name" in frame.columns else "province"
    if province_column not in frame.columns:
        frame["province_name"] = ""
        province_column = "province_name"
    mask = frame[province_column].isna() | (frame[province_column].astype(str).str.strip() == "")
    columns = [column for column in ["admin_id", "shapeName", "shapeGroup", "shapeType", "province_name", "province"] if column in frame.columns]
    if "geometry" in columns:
        columns.remove("geometry")
    return frame.loc[mask, columns].copy()


def _summarize_status_table(table: Any, status_column: str) -> dict[str, Any]:
    """Summarize rows by a status column."""

    if table is None or len(table) == 0:
        return {"rows": 0, "status_counts": {}}
    if status_column not in table.columns:
        return {"rows": int(len(table)), "status_counts": {}}
    counts = table[status_column].fillna("missing").astype(str).value_counts().to_dict()
    return {"rows": int(len(table)), "status_counts": {str(key): int(value) for key, value in counts.items()}}


def _summarize_model_panel(model_panel: Any, target_year_min: int, target_year_max: int) -> dict[str, Any]:
    """Summarize model-panel availability and exposure coverage."""

    if model_panel is None or len(model_panel) == 0:
        return {
            "rows": 0,
            "main_rows": 0,
            "year_min": None,
            "year_max": None,
            "yield_anomaly_pct_nonmissing": 0,
            "exposure_index_nonmissing": 0,
            "main_yield_anomaly_pct_nonmissing": 0,
            "main_exposure_index_nonmissing": 0,
            "exposure_year_counts": {},
        }
    frame = model_panel.copy()
    years = _numeric_column(frame, "year")
    exposure = _numeric_column(frame, "exposure_index")
    yield_anomaly = _numeric_column(frame, "yield_anomaly_pct")
    main = frame.loc[(years >= int(target_year_min)) & (years <= int(target_year_max))].copy()
    main_exposure = _numeric_column(main, "exposure_index")
    main_yield_anomaly = _numeric_column(main, "yield_anomaly_pct")
    if "year" in frame.columns and "exposure_index" in frame.columns:
        exposed = frame.loc[exposure.notna()].copy()
        exposure_year_counts = exposed.groupby("year").size().to_dict() if len(exposed) else {}
    else:
        exposure_year_counts = {}
    return {
        "rows": int(len(frame)),
        "main_rows": int(len(main)),
        "year_min": int(years.min()) if years.notna().any() else None,
        "year_max": int(years.max()) if years.notna().any() else None,
        "yield_anomaly_pct_nonmissing": int(yield_anomaly.notna().sum()),
        "exposure_index_nonmissing": int(exposure.notna().sum()),
        "main_yield_anomaly_pct_nonmissing": int(main_yield_anomaly.notna().sum()),
        "main_exposure_index_nonmissing": int(main_exposure.notna().sum()),
        "exposure_year_counts": {str(key): int(value) for key, value in exposure_year_counts.items()},
    }


def _summarize_admin_crosswalk(crosswalk: Any) -> dict[str, Any]:
    """Summarize administrative crosswalk confidence."""

    if crosswalk is None or len(crosswalk) == 0:
        return {"rows": 0, "high_confidence_rows": 0, "low_confidence_rows": 0, "high_confidence_rate": 0.0}
    confidence = _numeric_column(crosswalk, "match_confidence")
    high_confidence = int((confidence >= 0.85).sum())
    low_confidence = int((confidence < 0.85).sum())
    rows = int(len(crosswalk))
    return {
        "rows": rows,
        "high_confidence_rows": high_confidence,
        "low_confidence_rows": low_confidence,
        "high_confidence_rate": high_confidence / rows if rows else 0.0,
    }


def _summarize_yield_proxy(proxy_panel: Any, proxy_gap: Any, target_year_min: int, target_year_max: int) -> dict[str, Any]:
    """Summarize county-level gridded yield proxy coverage and calibration."""

    if proxy_panel is None or len(proxy_panel) == 0:
        panel_summary = {
            "rows": 0,
            "admin_units": 0,
            "year_min": None,
            "year_max": None,
            "source_counts": {},
            "calibration_status_counts": {},
            "calibrated_yield_nonmissing": 0,
        }
    else:
        years = _numeric_column(proxy_panel, "year")
        panel_summary = {
            "rows": int(len(proxy_panel)),
            "admin_units": int(proxy_panel["admin_id"].nunique()) if "admin_id" in proxy_panel.columns else 0,
            "year_min": int(years.min()) if years.notna().any() else None,
            "year_max": int(years.max()) if years.notna().any() else None,
            "source_counts": _value_counts(proxy_panel, "source"),
            "calibration_status_counts": _value_counts(proxy_panel, "calibration_status"),
            "calibrated_yield_nonmissing": int(_numeric_column(proxy_panel, "calibrated_yield").notna().sum()),
        }

    target_years = set(range(int(target_year_min), int(target_year_max) + 1))
    if proxy_gap is None or len(proxy_gap) == 0:
        panel_summary.update(
            {
                "available_admin_year_cells": 0,
                "missing_admin_year_cells": 0,
                "target_admin_year_cells": int(panel_summary["admin_units"] * len(target_years)),
                "missing_admin_year_cells_target": int(panel_summary["admin_units"] * len(target_years)),
            }
        )
    else:
        status = proxy_gap["status"].astype(str) if "status" in proxy_gap.columns else None
        available = int((status == "available").sum()) if status is not None else 0
        admin_count = int(proxy_gap["admin_id"].nunique()) if "admin_id" in proxy_gap.columns else int(panel_summary["admin_units"])
        expected = admin_count * len(target_years)
        available_target = available
        if {"year", "status"}.issubset(proxy_gap.columns):
            years = _numeric_column(proxy_gap, "year")
            available_target = int(((status == "available") & years.isin(target_years)).sum())
        panel_summary.update(
            {
                "available_admin_year_cells": available,
                "missing_admin_year_cells": int(len(proxy_gap) - available),
                "target_admin_year_cells": int(expected),
                "missing_admin_year_cells_target": int(max(expected - available_target, 0)),
            }
        )
    return panel_summary


def _summarize_spatial_panel_pair(processed: Path, stem: str, warnings: list[str]) -> dict[str, Any]:
    """Summarize CSV/Parquet consistency and duplicate keys for a spatial panel."""

    parquet_path = processed / f"{stem}.parquet"
    csv_path = processed / f"{stem}.csv"
    parquet = _read_table(parquet_path, warnings, stem) if parquet_path.exists() else _empty_frame()
    csv = _read_table(csv_path, warnings, stem) if csv_path.exists() else _empty_frame()
    analysis = parquet if len(parquet) else csv
    key_columns = [column for column in ["admin_id", "year", "month", "variable"] if column in analysis.columns]
    duplicate_key_rows = int(analysis.duplicated(key_columns).sum()) if key_columns else 0
    unique_key_rows = int(len(analysis.drop_duplicates(key_columns))) if key_columns else int(len(analysis))
    years = _numeric_column(analysis, "year")
    return {
        "parquet_rows": int(len(parquet)),
        "csv_rows": int(len(csv)),
        "analysis_rows": int(len(analysis)),
        "duplicate_key_rows": duplicate_key_rows,
        "unique_key_rows": unique_key_rows,
        "year_min": int(years.min()) if years.notna().any() else None,
        "year_max": int(years.max()) if years.notna().any() else None,
        "variable_counts": _value_counts(analysis, "variable"),
    }


def _summarize_proxy_calibration(proxy_panel: Any) -> Any:
    """Build a compact calibration status table by source."""

    import pandas as pd

    columns = ["source", "calibration_status", "row_count", "calibrated_yield_nonmissing"]
    if proxy_panel is None or len(proxy_panel) == 0:
        return pd.DataFrame(columns=columns)
    frame = proxy_panel.copy()
    if "source" not in frame.columns:
        frame["source"] = "unknown"
    if "calibration_status" not in frame.columns:
        frame["calibration_status"] = "unknown"
    frame["_calibrated_yield_present"] = _numeric_column(frame, "calibrated_yield").notna().astype(int)
    summary = (
        frame.groupby(["source", "calibration_status"], dropna=False)
        .agg(row_count=("source", "size"), calibrated_yield_nonmissing=("_calibrated_yield_present", "sum"))
        .reset_index()
    )
    return summary[columns]


def _summarize_source_catalog(source_catalog: Any) -> dict[str, Any]:
    """Summarize researched external data-source catalog."""

    if source_catalog is None or len(source_catalog) == 0:
        return {"rows": 0, "category_counts": {}, "status_counts": {}}
    return {
        "rows": int(len(source_catalog)),
        "category_counts": _value_counts(source_catalog, "category"),
        "status_counts": _value_counts(source_catalog, "status"),
    }


def _numeric_column(frame: Any, column: str) -> Any:
    """Return a numeric Series for a possibly missing column."""

    import pandas as pd

    if frame is None or column not in frame.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _value_counts(frame: Any, column: str) -> dict[str, int]:
    """Return stringified value counts for a column."""

    if frame is None or len(frame) == 0 or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].fillna("missing").astype(str).value_counts().to_dict().items()}


def _build_coverage_summary(summary: dict[str, Any]) -> Any:
    """Build a flat coverage summary table."""

    import pandas as pd

    rows = [
        {"metric": "admin_units", "value": summary["admin"]["admin_units"]},
        {"metric": "admin_province_matched", "value": summary["admin"]["province_matched"]},
        {"metric": "admin_province_unmatched", "value": summary["admin"]["province_unmatched"]},
        {"metric": "crop_rows", "value": summary["crop"]["rows"]},
        {"metric": "crop_zonal_stats", "value": summary["crop"]["status_counts"].get("zonal_stats", 0)},
        {"metric": "crop_no_crop_observed", "value": summary["crop"]["status_counts"].get("no_crop_observed", 0)},
        {"metric": "crop_no_overlap", "value": summary["crop"]["status_counts"].get("no_overlap", 0)},
        {"metric": "phenology_rows", "value": summary["phenology"]["rows"]},
        {"metric": "phenology_zonal_stats", "value": summary["phenology"]["status_counts"].get("zonal_stats", 0)},
        {"metric": "phenology_default", "value": summary["phenology"]["status_counts"].get("default", 0)},
        {"metric": "model_panel_rows", "value": summary["model_panel"]["rows"]},
        {"metric": "model_panel_main_period_rows", "value": summary["model_panel"]["main_rows"]},
        {"metric": "model_panel_exposure_nonmissing", "value": summary["model_panel"]["exposure_index_nonmissing"]},
        {"metric": "model_panel_yield_anomaly_nonmissing", "value": summary["model_panel"]["yield_anomaly_pct_nonmissing"]},
        {"metric": "model_panel_main_period_exposure_nonmissing", "value": summary["model_panel"]["main_exposure_index_nonmissing"]},
        {"metric": "model_panel_main_period_yield_anomaly_nonmissing", "value": summary["model_panel"]["main_yield_anomaly_pct_nonmissing"]},
        {"metric": "admin_crosswalk_rows", "value": summary["admin_crosswalk"]["rows"]},
        {"metric": "admin_crosswalk_high_confidence_rate", "value": summary["admin_crosswalk"]["high_confidence_rate"]},
        {"metric": "admin_crosswalk_low_confidence_rows", "value": summary["admin_crosswalk"]["low_confidence_rows"]},
        {"metric": "yield_proxy_rows", "value": summary["yield_proxy"]["rows"]},
        {"metric": "yield_proxy_calibrated_rows", "value": summary["yield_proxy"]["calibrated_yield_nonmissing"]},
        {"metric": "yield_proxy_available_admin_year_cells", "value": summary["yield_proxy"]["available_admin_year_cells"]},
        {"metric": "yield_proxy_missing_admin_year_cells", "value": summary["yield_proxy"]["missing_admin_year_cells"]},
        {"metric": "yield_proxy_target_admin_year_cells_main_period", "value": summary["yield_proxy"]["target_admin_year_cells"]},
        {"metric": "yield_proxy_missing_admin_year_cells_main_period", "value": summary["yield_proxy"]["missing_admin_year_cells_target"]},
        {"metric": "admin_climate_panel_parquet_rows", "value": summary["climate_panel"]["parquet_rows"]},
        {"metric": "admin_climate_panel_csv_rows", "value": summary["climate_panel"]["csv_rows"]},
        {"metric": "admin_climate_panel_duplicate_key_rows", "value": summary["climate_panel"]["duplicate_key_rows"]},
        {"metric": "admin_remote_sensing_panel_parquet_rows", "value": summary["remote_sensing_panel"]["parquet_rows"]},
        {"metric": "admin_remote_sensing_panel_csv_rows", "value": summary["remote_sensing_panel"]["csv_rows"]},
        {"metric": "admin_remote_sensing_panel_duplicate_key_rows", "value": summary["remote_sensing_panel"]["duplicate_key_rows"]},
        {"metric": "deep_source_catalog_rows", "value": summary["source_catalog"]["rows"]},
    ]
    return pd.DataFrame(rows)


def _build_risks(summary: dict[str, Any]) -> list[dict[str, str]]:
    """Build the current risk register from observed evidence."""

    model = summary["model_panel"]
    proxy = summary["yield_proxy"]
    admin = summary["admin"]
    crop = summary["crop"]
    phenology = summary["phenology"]
    sources = summary["source_catalog"]
    crosswalk = summary["admin_crosswalk"]
    climate_panel = summary["climate_panel"]
    remote_panel = summary["remote_sensing_panel"]

    risks = [
        {
            "risk_id": "R01_proxy_panel_built_but_not_official",
            "category": "outcome_data",
            "severity": "high",
            "status": "partly_mitigated",
            "owner": "user_research",
            "evidence": (
                f"Yield proxy panel has {proxy['rows']} rows, {proxy['available_admin_year_cells']} available admin-year cells, "
                f"{proxy['calibrated_yield_nonmissing']} calibrated rows, and {proxy['missing_admin_year_cells_target']} missing "
                "target admin-year cells for the main content period."
            ),
            "next_action": "Treat gridded proxy as robustness or gap diagnosis until official county/prefecture rice statistics are obtained.",
        },
        {
            "risk_id": "R02_official_county_rice_panel_missing",
            "category": "outcome_data",
            "severity": "critical",
            "status": "unavoidable_external_research",
            "owner": "user_research",
            "evidence": (
                f"Deep source catalog has {sources['rows']} reviewed sources; current conclusion is no complete public direct-download "
                "2000-2024 county/prefecture rice yield panel."
            ),
            "next_action": "Research China County Statistical Yearbook, local yearbooks, CNKI/CSYD/EPS tables, and agricultural bulletins for rice area/production/yield.",
        },
        {
            "risk_id": "R03_2024_county_validation_yield_lag",
            "category": "validation_event",
            "severity": "high",
            "status": "unavoidable_external_research",
            "owner": "user_research",
            "evidence": "The project validation event is 2024; county/prefecture official yield statistics may lag yearbook publication.",
            "next_action": "Check 2024 local statistical bulletins and agricultural department reports; otherwise validate with remote-sensing stress indicators only.",
        },
        {
            "risk_id": "R04_exposure_panel_coverage_low",
            "category": "climate_remote_sensing",
            "severity": "high",
            "status": "code_and_data_open",
            "owner": "code",
            "evidence": (
                f"main-period model_panel exposure_index nonmissing is {model['main_exposure_index_nonmissing']}/{model['main_rows']}; "
                f"year counts: {model['exposure_year_counts']}."
            ),
            "next_action": "Extend ERA5-Land/CHIRPS/soil-moisture processing and admin aggregation for 2000-2024 before relying on panel models.",
        },
        {
            "risk_id": "R05_yield_proxy_2021_2024_gap",
            "category": "yield_proxy",
            "severity": "medium",
            "status": "code_and_data_open",
            "owner": "shared",
            "evidence": (
                f"Yield proxy target cells are {proxy['target_admin_year_cells']} for the 2000-2024 main period; "
                f"missing target cells are {proxy['missing_admin_year_cells_target']}."
            ),
            "next_action": "Research or add proxy products covering 2021-2024, then rerun yield-proxy aggregation and calibration.",
        },
        {
            "risk_id": "R06_admin_province_assignment_gaps",
            "category": "admin_boundary",
            "severity": "medium",
            "status": "partly_mitigated",
            "owner": "code",
            "evidence": f"Province assignment matched {admin['province_matched']}/{admin['admin_units']} admin units; unmatched units are exported.",
            "next_action": "Inspect admin_unmatched_province_units.csv and add deterministic name/code overrides for the remaining units if they affect the study sample.",
        },
        {
            "risk_id": "R07_crop_phenology_partial_defaults",
            "category": "crop_mask_phenology",
            "severity": "medium",
            "status": "code_and_data_open",
            "owner": "shared",
            "evidence": (
                f"Crop statuses: {crop['status_counts']}; phenology statuses: {phenology['status_counts']}."
            ),
            "next_action": "Confirm whether no-crop/no-overlap units are outside rice area; replace default phenology where ChinaRiceCalendar or local crop calendars are available.",
        },
        {
            "risk_id": "R08_spatial_panel_duplicate_keys",
            "category": "climate_remote_sensing",
            "severity": "medium",
            "status": (
                "partly_mitigated"
                if climate_panel["duplicate_key_rows"] == 0 and remote_panel["duplicate_key_rows"] == 0
                else "code_and_data_open"
            ),
            "owner": "code",
            "evidence": (
                f"Climate duplicate key rows: {climate_panel['duplicate_key_rows']}; "
                f"remote-sensing duplicate key rows: {remote_panel['duplicate_key_rows']}."
            ),
            "next_action": (
                "Keep duplicate-key checks in the risk report after every aggregate rerun."
                if climate_panel["duplicate_key_rows"] == 0 and remote_panel["duplicate_key_rows"] == 0
                else "Collapse duplicate admin-year-month-variable rows deterministically before merging into model_panel."
            ),
        },
        {
            "risk_id": "R09_spatial_panel_csv_parquet_mismatch",
            "category": "pipeline_outputs",
            "severity": "medium",
            "status": (
                "partly_mitigated"
                if climate_panel["csv_rows"] == climate_panel["parquet_rows"] and remote_panel["csv_rows"] == remote_panel["parquet_rows"]
                else "code_and_data_open"
            ),
            "owner": "code",
            "evidence": (
                f"Climate CSV/Parquet rows: {climate_panel['csv_rows']}/{climate_panel['parquet_rows']}; "
                f"remote CSV/Parquet rows: {remote_panel['csv_rows']}/{remote_panel['parquet_rows']}."
            ),
            "next_action": (
                "Keep CSV/Parquet row-count checks in the risk report after every aggregate rerun."
                if climate_panel["csv_rows"] == climate_panel["parquet_rows"] and remote_panel["csv_rows"] == remote_panel["parquet_rows"]
                else "Regenerate CSV summaries from Parquet or stop writing empty CSV companions to avoid accidental use of empty panels."
            ),
        },
        {
            "risk_id": "R10_admin_crosswalk_2000_2025",
            "category": "admin_boundary",
            "severity": "high",
            "status": "manual_review_required" if crosswalk["rows"] else "unavoidable_external_research",
            "owner": "shared",
            "evidence": (
                f"Crosswalk rows: {crosswalk['rows']}; high-confidence rate: {crosswalk['high_confidence_rate']:.3f}; "
                f"low-confidence rows: {crosswalk['low_confidence_rows']}."
            )
            if crosswalk["rows"]
            else "Annual county code/name changes across 2000-2024 cannot be inferred safely from one boundary snapshot; 2025 is optional background.",
            "next_action": "Review admin_crosswalk_low_confidence.csv before county-level main models; prefecture/province models may proceed with documented mapping policy.",
        },
        {
            "risk_id": "R11_causal_identification_scope",
            "category": "modeling_claim",
            "severity": "critical",
            "status": "unavoidable_research_design",
            "owner": "user_research",
            "evidence": "Proxy outcome data, incomplete exposure history, and nonrandom drought exposure limit causal claims.",
            "next_action": "Define conservative wording, treatment thresholds, placebo years, and whether the paper claims association, impact estimation, or causal identification.",
        },
        {
            "risk_id": "R12_provider_access_and_volume",
            "category": "data_access",
            "severity": "medium",
            "status": "unavoidable_external_setup",
            "owner": "user_research",
            "evidence": "CDS/Earthdata/CMA and large raster products may require accounts, licenses, API keys, bandwidth, and storage.",
            "next_action": "Prepare accounts/credentials and decide priority products before expanding downloads to full 2000-2024 coverage.",
        },
    ]
    return risks


def _write_markdown_report(
    path: Path,
    summary: dict[str, Any],
    coverage_summary: Any,
    risks: list[dict[str, str]],
    warnings: list[str],
) -> None:
    """Write the human-readable risk action report."""

    path.parent.mkdir(parents=True, exist_ok=True)
    mitigated = [risk for risk in risks if risk["status"].startswith("partly")]
    local = [risk for risk in risks if risk["status"] == "code_and_data_open"]
    unavoidable = [risk for risk in risks if risk["status"].startswith("unavoidable")]

    lines = [
        "# Residual Risk Action Report",
        "",
        f"- Generated at: {summary['generated_at']}",
        "- Purpose: separate risks already reduced by code/data work from risks that need external research or research-design decisions.",
        "",
        "## Current Evidence Snapshot",
        "",
    ]
    for row in coverage_summary.to_dict("records"):
        lines.append(f"- {row['metric']}: {row['value']}")

    lines.extend(
        [
            "",
            "## Already Mitigated Or Partly Mitigated",
            "",
            _risk_table(mitigated),
            "",
            "## Still Code/Data Addressable",
            "",
            _risk_table(local),
            "",
            "## 实在不可避免、需要外部调研的风险",
            "",
            _risk_table(unavoidable),
            "",
            "## Safe Claim Boundary",
            "",
            "- The current county-level yield proxy panel is useful for robustness checks, coverage diagnostics, and spatial pattern comparison.",
            "- It should not replace official county/prefecture rice yield statistics as the main causal outcome.",
            "- Fixed-effect and event-study outputs remain exploratory until exposure history and official outcome coverage are expanded.",
            "",
            "## Warnings",
            "",
        ]
    )
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_data_gap_report(path: Path, summary: dict[str, Any], risks: list[dict[str, str]]) -> None:
    """Write a consolidated data-gap report."""

    unresolved = [risk for risk in risks if not risk["status"].startswith("partly")]
    lines = [
        "# Data Gap Report",
        "",
        f"- Generated at: {summary['generated_at']}",
        "",
        "## High-Priority Gaps",
        "",
        _risk_table(unresolved),
        "",
        "## Quantitative Coverage Snapshot",
        "",
        f"- main-period model_panel exposure_index nonmissing: {summary['model_panel']['main_exposure_index_nonmissing']}/{summary['model_panel']['main_rows']}",
        f"- main-period model_panel yield_anomaly_pct nonmissing: {summary['model_panel']['main_yield_anomaly_pct_nonmissing']}/{summary['model_panel']['main_rows']}",
        f"- yield proxy main-period missing admin-year cells: {summary['yield_proxy']['missing_admin_year_cells_target']}",
        f"- crop statuses: {summary['crop']['status_counts']}",
        f"- phenology statuses: {summary['phenology']['status_counts']}",
        "",
        "## Data-Gap Rule",
        "",
        "- Missing official yield panels do not stop the pipeline.",
        "- Missing county/prefecture rice outcomes trigger a downgrade to grain, province, or proxy analysis depending on available data.",
        "- Proxy outputs must not be used as official yield-loss claims.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_model_scope_decision(path: Path, processed: Path, summary: dict[str, Any], risks: list[dict[str, str]]) -> None:
    """Write current model-scope downgrade decision."""

    try:
        from src.data_tiers import read_yield_tier_decision
    except Exception:  # pragma: no cover - import should normally work
        read_yield_tier_decision = lambda _processed: {}  # type: ignore[assignment]

    tier = read_yield_tier_decision(processed)
    if not tier:
        tier = {
            "tier": "unknown",
            "admin_level": "unknown",
            "crop_type": "unknown",
            "conclusion_strength": "descriptive",
            "recommended_scope": "data_gap_only",
            "forbidden_claim": "strong_causal_claim_without_valid_identification",
            "downgrade_reason": "yield_data_tier_report_missing",
        }
    exposure_rows = summary["model_panel"]["main_exposure_index_nonmissing"]
    model_rows = summary["model_panel"]["main_rows"]
    exposure_rate = exposure_rows / model_rows if model_rows else 0.0
    event_study_allowed = exposure_rate >= 0.5 and str(tier.get("tier")) in {"tier_1", "tier_2"}
    quasi_causal_gate_passed = False
    try:
        tier_coverage_rate = float(tier.get("year_coverage_rate", 0) or 0)
    except (TypeError, ValueError):
        tier_coverage_rate = 0.0
    yield_coverage_gate = str(tier.get("tier")) in {"tier_1", "tier_2"} and tier_coverage_rate >= 0.75
    admin_crosswalk_gate = summary["admin_crosswalk"]["high_confidence_rate"] >= 0.90
    lines = [
        "# Model Scope Decision",
        "",
        f"- Generated at: {summary['generated_at']}",
        "- Main claim: impact assessment with quasi-causal attempt only when data and identification diagnostics support it.",
        "- Strong causal claims are forbidden.",
        "- Main model content years: 2000-2024; 2025 is background/optional only.",
        "",
        "## Current Data Tier",
        "",
        f"- Tier: `{tier.get('tier')}`",
        f"- Admin level: `{tier.get('admin_level')}`",
        f"- Crop type: `{tier.get('crop_type')}`",
        f"- Recommended scope: `{tier.get('recommended_scope')}`",
        f"- Allowed conclusion strength: `{tier.get('conclusion_strength')}`",
        f"- Downgrade reason: `{tier.get('downgrade_reason')}`",
        f"- Forbidden claim: `{tier.get('forbidden_claim')}`",
        "",
        "## Model Gate",
        "",
        f"- main-period exposure_index coverage: {exposure_rows}/{model_rows} ({exposure_rate:.3f})",
        f"- Main event study allowed now: {event_study_allowed}",
        f"- Quasi-causal claim gate passed: {quasi_causal_gate_passed}",
        "- 2024 is external validation or descriptive comparison unless reliable county/prefecture yield data are available.",
        "",
        "## Causal Claim Gate",
        "",
        "| Gate | Current status |",
        "| --- | --- |",
        f"| Official county/prefecture yield coverage >= 0.75 | {yield_coverage_gate} |",
        "| Pretrend test passed | False |",
        "| Placebo tests passed | False |",
        "| Robustness direction stable | False |",
        f"| Admin crosswalk match rate >= 0.90 | {admin_crosswalk_gate} |",
        "| Remote-sensing mechanism consistent | False |",
        "",
        "## Allowed Language",
        "",
        "- When the gate is not passed: 复合热旱暴露与单产异常下降存在显著关联。",
        "- When the gate is not passed: 本文结果属于影响评估，不构成强因果识别。",
        "- Only if every gate passes: 事件研究结果提供了准因果证据，但结论仍受统计口径、行政区划映射和未观测农业管理差异限制。",
        "",
        "## Forbidden Language",
        "",
        "- Do not write that the model proves 2022 heat-drought caused yield decline.",
        "- Do not write that the model fully identifies a causal effect.",
        "- Do not present remote-sensing proxy outcomes as official yield losses.",
        "",
    ]
    lines.extend(["## Blocking Risks", "", _risk_table([risk for risk in risks if risk["status"].startswith("unavoidable")]), ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_external_access_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write external access check Markdown."""

    lines = [
        "# External Access Check",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "- Secret files are checked only for existence; contents are never read or printed.",
        "",
        "| provider | required_for | check | configured | details |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {provider} | {required_for} | {check} | {configured} | {details} |".format(
                provider=row["provider"],
                required_for=_escape_markdown_cell(row["required_for"]),
                check=row["check"],
                configured=row["configured"],
                details=_escape_markdown_cell(row["details"]),
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_decision_reports(
    paths: dict[str, Path],
    summary: dict[str, Any],
    risks: list[dict[str, str]],
    target_year_min: int,
    target_year_max: int,
) -> None:
    """Write short decision reports for data, crosswalk, claims, and access."""

    _write_data_source_decision(paths["data_source"], target_year_min, target_year_max)
    _write_yield_panel_feasibility(paths["yield_feasibility"], summary, target_year_min, target_year_max)
    _write_admin_crosswalk_decision(paths["admin_crosswalk"], summary, target_year_min, target_year_max)
    _write_model_claim_scope(paths["model_claim"], summary, risks)
    _write_external_access_status(paths["external_access"])


def _write_data_source_decision(path: Path, year_min: int, year_max: int) -> None:
    """Write the data-source decision memo."""

    lines = [
        "# Data Source Decision",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Main model content years: {year_min}-{year_max}.",
        "- 2025 is optional national/provincial background only, not required for the main city/county panel.",
        "- Preferred outcome path: official prefecture/county rice panel.",
        "- Downgrade path: official prefecture/county grain panel.",
        "- Fallback path: provincial official data plus city/county exposure and remote-sensing response.",
        "- Third-party yearbook sites are search clues; final evidence should return to official yearbooks, official bulletins, or licensed databases.",
        "- Remote-sensing proxies are allowed for vegetation-growth anomaly and mechanism analysis, not official yield-loss claims.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_yield_panel_feasibility(path: Path, summary: dict[str, Any], year_min: int, year_max: int) -> None:
    """Write the yield-panel feasibility memo."""

    lines = [
        "# Yield Panel Feasibility",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Target main content years: {year_min}-{year_max}.",
        "- Minimum coverage for main model: 0.75.",
        "- Minimum coverage for exploratory model: 0.50.",
        f"- Current main-period model_panel yield anomaly nonmissing: {summary['model_panel']['main_yield_anomaly_pct_nonmissing']}/{summary['model_panel']['main_rows']}.",
        f"- Current yield proxy main-period missing admin-year cells: {summary['yield_proxy']['missing_admin_year_cells_target']}.",
        "- Decision: do not block MVP on a 2025 city/county rice panel.",
        "- Decision: if rice panel is unavailable, downgrade to grain yield anomaly before using remote-sensing proxy outcomes.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_admin_crosswalk_decision(path: Path, summary: dict[str, Any], year_min: int, year_max: int) -> None:
    """Write the admin-crosswalk decision memo."""

    lines = [
        "# Admin Crosswalk Decision",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Main statistical years to map: {year_min}-{year_max}.",
        "- Target boundary year: 2022.",
        "- Computational source: yescallop/areacodes for historical code mapping.",
        "- Official validation source: Ministry of Civil Affairs and National Geographical Names Information Database.",
        "- Matching priority: exact admin_code, explicit old-new crosswalk, exact province/prefecture/county name, normalized fuzzy name, manual override.",
        "- Fuzzy matches below 0.85 confidence must go to manual review.",
        "- Paper wording: based on public historical administrative-code crosswalk, current codes checked against official MCA/National Geographical Names sources.",
        "- Forbidden wording: yescallop/areacodes is an official crosswalk.",
        f"- Current province assignment: {summary['admin']['province_matched']}/{summary['admin']['admin_units']} matched.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_model_claim_scope(path: Path, summary: dict[str, Any], risks: list[dict[str, str]]) -> None:
    """Write the model-claim scope memo."""

    exposure_rows = summary["model_panel"]["main_exposure_index_nonmissing"]
    model_rows = summary["model_panel"]["main_rows"]
    exposure_rate = exposure_rows / model_rows if model_rows else 0.0
    gate_passed = False
    lines = [
        "# Model Claim Scope",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "- Default claim: impact assessment / association.",
        f"- Current main-period exposure coverage: {exposure_rows}/{model_rows} ({exposure_rate:.3f}).",
        f"- Quasi-causal gate passed: {gate_passed}.",
        "",
        "## Allowed When Gate Is Not Passed",
        "",
        "- 复合热旱暴露与单产异常下降存在显著关联。",
        "- 本文结果属于影响评估，不构成强因果识别。",
        "",
        "## Allowed Only If Gate Passes",
        "",
        "- 事件研究结果提供了准因果证据。",
        "- 必须同时写：结论仍受统计口径、行政区划映射和未观测农业管理差异限制。",
        "",
        "## Proxy Outcome Rule",
        "",
        "- 如果只使用遥感代理因变量，必须写：本文识别的是热旱暴露与遥感长势异常之间的空间响应关系，不直接声称官方产量损失。",
        "",
        "## Forbidden",
        "",
        "- 证明2022热旱导致减产。",
        "- 完全识别因果效应。",
        "- 没有官方县域产量时输出县域官方产量损失图。",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_external_access_status(path: Path) -> None:
    """Write a stable external-access policy memo."""

    lines = [
        "# External Access Status",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "- CDS: configured if `~/.cdsapirc` exists; continue using ERA5-Land after accepting dataset terms.",
        "- Earthdata: optional for MVP; install `earthaccess` and persist login to `.netrc` before large MODIS/SMAP/GRACE downloads.",
        "- CMA: not required for MVP; use for station validation or robustness.",
        "- CNKI/EPS: not required for MVP; use if institutional subscription is available for official yearbook panels.",
        "- Fallback climate/remote-sensing sources: ERA5-Land, CHIRPS, CMFD, GLEAM, MODIS, SMAP.",
        "- Secret contents must never be read or printed.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _risk_table(risks: list[dict[str, str]]) -> str:
    """Format risk rows as a Markdown table."""

    if not risks:
        return "No risks in this group."
    lines = [
        "| risk_id | severity | owner | evidence | next_action |",
        "| --- | --- | --- | --- | --- |",
    ]
    for risk in risks:
        lines.append(
            "| {risk_id} | {severity} | {owner} | {evidence} | {next_action} |".format(
                risk_id=risk["risk_id"],
                severity=risk["severity"],
                owner=risk["owner"],
                evidence=_escape_markdown_cell(risk["evidence"]),
                next_action=_escape_markdown_cell(risk["next_action"]),
            )
        )
    return "\n".join(lines)


def _escape_markdown_cell(value: str) -> str:
    """Escape pipe characters in a Markdown table cell."""

    return str(value).replace("|", "\\|").replace("\n", " ")
