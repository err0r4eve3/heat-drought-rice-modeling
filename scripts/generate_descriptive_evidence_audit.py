"""Generate descriptive evidence audits from local aggregate artifacts."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger


MODEL_PANEL_CANDIDATES = ("province_model_panel.parquet", "province_model_panel.csv", "model_panel.parquet", "model_panel.csv")
ANNUAL_EXPOSURE_CANDIDATES = ("annual_exposure_panel.parquet", "annual_exposure_panel.csv")
ADMIN_PROVINCE_CANDIDATES = ("admin_units_with_province.parquet", "admin_units_with_province.csv", "admin_units_with_province.gpkg")
PROXY_PANEL_CANDIDATES = (
    "yield_proxy/county_yield_proxy_panel.parquet",
    "yield_proxy/county_yield_proxy_panel.csv",
    "county_yield_proxy_panel.parquet",
    "county_yield_proxy_panel.csv",
)
OFFICIAL_PANEL_CANDIDATES = (
    "province_model_panel.parquet",
    "province_model_panel.csv",
    "yield_panel_combined.parquet",
    "yield_panel_combined.csv",
)

EXPOSURE_COLUMNS = ("chd_annual", "exposure_index")
PROXY_VALUE_COLUMNS = ("proxy_yield", "yield_proxy", "yield_proxy_value", "proxy_yield_kg_per_hectare", "yield_kg_per_hectare")
OFFICIAL_VALUE_COLUMNS = ("official_yield", "official_yield_kg_per_hectare", "yield_kg_per_hectare", "yield_anomaly_pct")
BIAS_WARNING_THRESHOLD = 5.0


def build_descriptive_evidence_audit(interim_dir: Path, processed_dir: Path, outputs_dir: Path) -> dict[str, Any]:
    """Build a no-raw-data descriptive evidence audit payload."""

    model_path = _first_existing(processed_dir, MODEL_PANEL_CANDIDATES)
    annual_path = _first_existing(processed_dir, ANNUAL_EXPOSURE_CANDIDATES)
    admin_path = _first_existing(processed_dir, ADMIN_PROVINCE_CANDIDATES)
    proxy_path = _first_existing(processed_dir, PROXY_PANEL_CANDIDATES)
    official_path = _first_existing(processed_dir, OFFICIAL_PANEL_CANDIDATES)

    model = _read_optional_table(model_path)
    annual = _read_optional_table(annual_path)
    admin = _read_optional_table(admin_path)
    proxy = _read_optional_table(proxy_path)
    official = _read_optional_table(official_path)

    payload = {
        "status": "ok",
        "evidence_type": "descriptive",
        "raw_data_required": False,
        "scope_guardrails": {
            "official_outcome_scope": "province_official_grain_yield_anomaly",
            "chd_scope": "province_average",
            "causal_claim_allowed": False,
            "subprovince_official_yield_loss_claim_allowed": False,
            "validation_2024_scope": "descriptive_external_consistency_only",
        },
        "inputs": {
            "interim_dir": str(interim_dir),
            "processed_dir": str(processed_dir),
            "outputs_dir": str(outputs_dir),
            "model_panel": _path_status(model_path),
            "annual_exposure_panel": _path_status(annual_path),
            "admin_units_with_province": _path_status(admin_path),
            "yield_proxy_panel": _path_status(proxy_path),
            "official_yield_panel": _path_status(official_path),
        },
        "sample_flow": _audit_sample_flow(model, annual, admin),
        "proxy_official_consistency": _audit_proxy_official_consistency(proxy, official),
        "consistency_2024": _audit_2024_consistency(model, annual),
    }
    return payload


def render_descriptive_evidence_report(audit: dict[str, Any]) -> str:
    """Render the descriptive audit payload as Markdown."""

    lines = [
        "# Descriptive Evidence Audit",
        "",
        "- Evidence type: descriptive",
        "- Raw data required: false",
        "- Outcome scope: province-level official grain yield anomaly",
        "- CHD scope: province-average exposure",
        "- Claim boundary: no quasi-causal, treatment-effect, or subprovince official yield-loss claims.",
        "",
        "## Sample Flow And Attrition",
        "",
    ]
    sample = audit["sample_flow"]
    lines.extend(_render_section_status(sample))
    if sample["status"] == "present":
        lines.extend(
            [
                "| metric | value |",
                "| --- | ---: |",
                f"| model_panel_rows | {sample['model_panel_rows']} |",
                f"| year_min | {_fmt(sample['year_min'])} |",
                f"| year_max | {_fmt(sample['year_max'])} |",
                f"| province_count | {_fmt(sample['province_count'])} |",
                f"| yield_anomaly_nonmissing | {_fmt(sample['yield_anomaly_nonmissing'])} |",
                f"| exposure_nonmissing | {_fmt(sample['exposure_nonmissing'])} |",
                f"| complete_model_rows | {_fmt(sample['complete_model_rows'])} |",
            ]
        )
        if sample.get("annual_exposure_rows") is not None:
            lines.append(f"| annual_exposure_rows | {_fmt(sample['annual_exposure_rows'])} |")
        if sample.get("admin_rows") is not None:
            lines.append(f"| admin_rows | {_fmt(sample['admin_rows'])} |")
        lines.extend(
            [
                "",
                "Interpretation: this is a sample-construction audit only. It does not claim national representativeness or causal identification.",
                "",
            ]
        )

    lines.extend(["## Proxy Vs Official Consistency", ""])
    proxy = audit["proxy_official_consistency"]
    lines.extend(_render_section_status(proxy))
    if proxy["status"] == "present":
        lines.extend(
            [
                "| metric | value |",
                "| --- | ---: |",
                f"| matched_province_year_rows | {proxy['matched_province_year_rows']} |",
                f"| mean_percentage_bias | {_fmt(proxy['mean_percentage_bias'])} |",
                f"| max_abs_percentage_bias | {_fmt(proxy['max_abs_percentage_bias'])} |",
                f"| warning_rows_abs_bias_gt_5pct | {proxy['warning_rows_abs_bias_gt_5pct']} |",
                "",
                "Interpretation: proxy values are descriptive robustness diagnostics only; they do not correct official statistics.",
                "",
            ]
        )

    lines.extend(["## 2024 Descriptive External Consistency", ""])
    consistency = audit["consistency_2024"]
    lines.extend(_render_section_status(consistency))
    if consistency["status"] == "present":
        lines.extend(
            [
                "| metric | value |",
                "| --- | ---: |",
                f"| model_2024_rows | {consistency['model_2024_rows']} |",
                f"| yield_anomaly_nonmissing_2024 | {_fmt(consistency['yield_anomaly_nonmissing_2024'])} |",
                f"| exposure_nonmissing_2024 | {_fmt(consistency['exposure_nonmissing_2024'])} |",
                f"| annual_exposure_rows_2024 | {_fmt(consistency['annual_exposure_rows_2024'])} |",
                "",
                "Interpretation: 2024 is descriptive external consistency context only, not an independent validation of causal effects.",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_descriptive_evidence_outputs(audit: dict[str, Any], output_json: Path, report_path: Path) -> tuple[Path, Path]:
    """Write descriptive evidence audit JSON and Markdown."""

    output_json.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(render_descriptive_evidence_report(audit), encoding="utf-8")
    return output_json, report_path


def _audit_sample_flow(model: pd.DataFrame | None, annual: pd.DataFrame | None, admin: pd.DataFrame | None) -> dict[str, Any]:
    if model is None:
        return _skipped("missing_model_panel", "model panel artifact is missing; cannot audit sample attrition")

    exposure_nonmissing = _row_nonmissing_any(model, EXPOSURE_COLUMNS)
    return {
        "status": "present",
        "evidence_type": "descriptive",
        "raw_data_required": False,
        "model_panel_rows": int(len(model)),
        "year_min": _min_value(model, "year"),
        "year_max": _max_value(model, "year"),
        "province_count": _nunique(model, "province"),
        "yield_anomaly_nonmissing": _nonmissing(model, "yield_anomaly_pct"),
        "exposure_nonmissing": exposure_nonmissing,
        "complete_model_rows": _complete_rows(model, ("yield_anomaly_pct",), EXPOSURE_COLUMNS),
        "annual_exposure_rows": int(len(annual)) if annual is not None else None,
        "annual_exposure_chd_nonmissing": _nonmissing(annual, "chd_annual") if annual is not None else None,
        "admin_rows": int(len(admin)) if admin is not None else None,
        "admin_province_matched": _nonmissing(admin, "province") if admin is not None else None,
        "does_not_assert_representative": True,
    }


def _audit_proxy_official_consistency(proxy: pd.DataFrame | None, official: pd.DataFrame | None) -> dict[str, Any]:
    if proxy is None:
        return _skipped("missing_proxy_panel", "yield proxy artifact is missing; proxy consistency audit skipped")
    if official is None:
        return _skipped("missing_official_panel", "official province panel artifact is missing; proxy consistency audit skipped")
    required = {"province", "year"}
    if not required.issubset(proxy.columns) or not required.issubset(official.columns):
        return _skipped("missing_join_keys", "proxy or official panel lacks province/year join keys")

    proxy_value = _first_column(proxy, PROXY_VALUE_COLUMNS)
    official_value = _first_column(official, OFFICIAL_VALUE_COLUMNS)
    if proxy_value is None or official_value is None:
        return _skipped("missing_value_columns", "proxy or official panel lacks comparable numeric value columns")

    proxy_grouped = (
        proxy.assign(_proxy_value=pd.to_numeric(proxy[proxy_value], errors="coerce"))
        .groupby(["province", "year"], as_index=False)["_proxy_value"]
        .mean()
    )
    official_grouped = (
        official.assign(_official_value=pd.to_numeric(official[official_value], errors="coerce"))
        .groupby(["province", "year"], as_index=False)["_official_value"]
        .mean()
    )
    merged = proxy_grouped.merge(official_grouped, on=["province", "year"], how="inner")
    merged = merged.dropna(subset=["_proxy_value", "_official_value"])
    if merged.empty:
        return _skipped("no_comparable_rows", "proxy and official panels have no comparable nonmissing province-year rows")

    merged = merged[merged["_official_value"].ne(0)]
    if merged.empty:
        return _skipped("zero_official_values", "official comparable values are zero, so percentage bias cannot be computed")

    bias = (merged["_proxy_value"] - merged["_official_value"]) / merged["_official_value"] * 100
    return {
        "status": "present",
        "evidence_type": "descriptive",
        "raw_data_required": False,
        "proxy_value_column": proxy_value,
        "official_value_column": official_value,
        "matched_province_year_rows": int(len(merged)),
        "mean_percentage_bias": _safe_float(bias.mean()),
        "max_abs_percentage_bias": _safe_float(bias.abs().max()),
        "warning_rows_abs_bias_gt_5pct": int(bias.abs().gt(BIAS_WARNING_THRESHOLD).sum()),
        "does_not_correct_official_statistics": True,
    }


def _audit_2024_consistency(model: pd.DataFrame | None, annual: pd.DataFrame | None) -> dict[str, Any]:
    if model is None:
        return _skipped("missing_model_panel", "model panel artifact is missing; 2024 descriptive consistency skipped")
    if "year" not in model.columns:
        return _skipped("missing_year_column", "model panel lacks year column; 2024 descriptive consistency skipped")

    model_2024 = model[pd.to_numeric(model["year"], errors="coerce").eq(2024)]
    if model_2024.empty:
        return _skipped("missing_2024_rows", "model panel has no 2024 rows; descriptive consistency skipped")

    annual_2024 = pd.DataFrame()
    if annual is not None and "year" in annual.columns:
        annual_2024 = annual[pd.to_numeric(annual["year"], errors="coerce").eq(2024)]

    return {
        "status": "present",
        "evidence_type": "descriptive",
        "raw_data_required": False,
        "scope": "descriptive_external_consistency_only",
        "model_2024_rows": int(len(model_2024)),
        "yield_anomaly_nonmissing_2024": _nonmissing(model_2024, "yield_anomaly_pct"),
        "exposure_nonmissing_2024": _row_nonmissing_any(model_2024, EXPOSURE_COLUMNS),
        "annual_exposure_rows_2024": int(len(annual_2024)) if annual is not None else None,
        "does_not_validate_causal_effects": True,
    }


def _read_optional_table(path: Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    if path.suffix == ".csv":
        return pd.read_csv(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".gpkg":
        try:
            import geopandas as gpd
        except ImportError:
            logger.warning("geopandas unavailable; skipping {}", path)
            return None
        return pd.DataFrame(gpd.read_file(path))
    raise ValueError(f"Unsupported artifact type: {path}")


def _first_existing(root: Path, candidates: tuple[str, ...]) -> Path | None:
    for candidate in candidates:
        path = root / candidate
        if path.exists():
            return path
    return None


def _path_status(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"status": "missing"}
    return {"status": "present", "path": str(path)}


def _skipped(reason: str, message: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "reason": reason,
        "message": message,
        "evidence_type": "descriptive",
        "raw_data_required": False,
    }


def _row_nonmissing_any(frame: pd.DataFrame, columns: tuple[str, ...]) -> int | None:
    present = [column for column in columns if column in frame.columns]
    if not present:
        return None
    return int(frame[present].notna().any(axis=1).sum())


def _complete_rows(frame: pd.DataFrame, required_columns: tuple[str, ...], any_columns: tuple[str, ...]) -> int | None:
    required_present = [column for column in required_columns if column in frame.columns]
    any_present = [column for column in any_columns if column in frame.columns]
    if not required_present or not any_present:
        return None
    mask = frame[required_present].notna().all(axis=1) & frame[any_present].notna().any(axis=1)
    return int(mask.sum())


def _nonmissing(frame: pd.DataFrame | None, column: str) -> int | None:
    if frame is None or column not in frame.columns:
        return None
    return int(frame[column].notna().sum())


def _nunique(frame: pd.DataFrame, column: str) -> int | None:
    if column not in frame.columns:
        return None
    return int(frame[column].dropna().nunique())


def _min_value(frame: pd.DataFrame, column: str) -> int | None:
    if column not in frame.columns or frame.empty:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return int(values.min()) if not values.empty else None


def _max_value(frame: pd.DataFrame, column: str) -> int | None:
    if column not in frame.columns or frame.empty:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return int(values.max()) if not values.empty else None


def _first_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def _fmt(value: Any) -> str:
    if value is None:
        return "NA"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _render_section_status(section: dict[str, Any]) -> list[str]:
    if section["status"] == "present":
        return []
    return [
        f"- Status: {section['status']}",
        f"- Reason: {section.get('reason', '')}",
        f"- Message: {section.get('message', '')}",
        "",
    ]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Generate descriptive evidence audit from local aggregate artifacts.")
    parser.add_argument("--interim-dir", type=Path, default=Path("data/interim"), help="Directory containing interim artifacts.")
    parser.add_argument("--processed-dir", type=Path, default=Path("data/processed"), help="Directory containing processed artifacts.")
    parser.add_argument("--outputs-dir", type=Path, default=Path("data/outputs"), help="Directory containing output artifacts.")
    parser.add_argument("--output-json", type=Path, default=None, help="Audit JSON path. Defaults to <outputs-dir>/descriptive_evidence_audit.json.")
    parser.add_argument("--report", type=Path, default=Path("reports/descriptive_evidence_audit.md"), help="Markdown report path.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Generate descriptive evidence audit files."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    output_json = args.output_json or args.outputs_dir / "descriptive_evidence_audit.json"
    audit = build_descriptive_evidence_audit(args.interim_dir, args.processed_dir, args.outputs_dir)
    write_descriptive_evidence_outputs(audit, output_json, args.report)
    logger.info("Descriptive evidence audit JSON: {}", output_json)
    logger.info("Descriptive evidence audit report: {}", args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
