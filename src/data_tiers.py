"""Yield-data tier classification and downgrade decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


TIER_METADATA = {
    "tier_1": {
        "name": "official_county_or_prefecture_rice_panel",
        "description": "Official county or prefecture rice yield panel.",
        "usable_for": "main_model; fixed_effects; event_study",
    },
    "tier_2": {
        "name": "official_county_or_prefecture_grain_panel",
        "description": "Official county or prefecture grain yield panel.",
        "usable_for": "main_model_with_crop_scope_downgrade; fixed_effects",
    },
    "tier_3": {
        "name": "provincial_rice_panel",
        "description": "Official provincial rice panel.",
        "usable_for": "robustness; background; macro_validation",
    },
    "tier_4": {
        "name": "remote_sensing_yield_proxy",
        "description": "Remote-sensing or gridded yield proxy.",
        "usable_for": "descriptive_analysis; robustness; mechanism_analysis",
        "not_usable_for": "official_yield_loss_claim; strong_causal_claim",
    },
    "missing": {
        "name": "missing_yield_data",
        "description": "No usable yield panel was found.",
        "usable_for": "data_gap_report",
    },
}


@dataclass(frozen=True)
class TierReportResult:
    """Output paths for yield-data tier reporting."""

    report_path: Path
    csv_path: Path
    decision: dict[str, Any]


def classify_yield_data(df: Any, expected_years: list[int] | None = None) -> dict[str, Any]:
    """Classify a yield panel into an analysis tier."""

    import pandas as pd

    frame = pd.DataFrame() if df is None else pd.DataFrame(df).copy()
    if frame.empty:
        return _decision("missing", 0, 0.0, "unknown", "unknown", False, "descriptive", "no_yield_panel")

    frame["_admin_level_inferred"] = frame.apply(_infer_admin_level_from_row, axis=1)
    frame["_crop_type_inferred"] = frame.apply(_infer_crop_type_from_row, axis=1)
    frame["_is_proxy"] = frame.apply(_is_proxy_row, axis=1)
    frame["_has_yield"] = _has_numeric_any(frame, ["yield_kg_per_hectare", "rice_yield_kg_per_hectare", "grain_yield_kg_per_hectare"])
    frame["_has_area"] = _has_numeric_any(frame, ["sown_area_hectare", "harvested_area_hectare", "rice_area_proxy"])
    frame["_has_production"] = _has_numeric_any(frame, ["production_ton", "calibrated_production_ton", "raw_proxy_production_ton"])

    year_values = _numeric_series(frame, "year").dropna().astype(int).unique().tolist()
    observed_years = len(set(year_values))
    if expected_years is None:
        if year_values:
            expected_years = list(range(min(year_values), max(year_values) + 1))
        else:
            expected_years = []
    year_coverage_rate = observed_years / len(set(expected_years)) if expected_years else 0.0

    official = frame[~frame["_is_proxy"]].copy()
    if not official.empty:
        candidate = _best_official_candidate(official)
        if candidate is not None:
            tier, admin_level, crop_type = candidate
            return _decision(
                tier=tier,
                observed_years=observed_years,
                year_coverage_rate=year_coverage_rate,
                admin_level=admin_level,
                crop_type=crop_type,
                official=True,
                conclusion_strength=_conclusion_strength(tier, year_coverage_rate),
                downgrade_reason=_downgrade_reason(tier, year_coverage_rate),
            )

    if frame["_is_proxy"].any() or _looks_like_proxy_panel(frame):
        return _decision(
            "tier_4",
            observed_years,
            year_coverage_rate,
            _dominant_value(frame, "_admin_level_inferred") or "county",
            "proxy",
            False,
            "descriptive",
            "remote_sensing_or_gridded_proxy_only",
        )

    return _decision("missing", observed_years, year_coverage_rate, "unknown", "unknown", False, "descriptive", "no_classifiable_yield_panel")


def write_yield_data_tier_report(
    df: Any,
    processed_dir: str | Path,
    reports_dir: str | Path,
    expected_years: list[int] | None = None,
) -> TierReportResult:
    """Classify yield data and write CSV/Markdown tier reports."""

    import pandas as pd

    processed = Path(processed_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    processed.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    decision = classify_yield_data(df, expected_years=expected_years)
    csv_path = processed / "yield_data_tier_report.csv"
    report_path = reports / "yield_data_tier_report.md"
    pd.DataFrame([decision]).to_csv(csv_path, index=False, encoding="utf-8-sig")
    _write_tier_markdown(report_path, decision)
    return TierReportResult(report_path=report_path, csv_path=csv_path, decision=decision)


def read_yield_tier_decision(processed_dir: str | Path) -> dict[str, Any]:
    """Read the current yield tier decision if available."""

    import pandas as pd

    path = Path(processed_dir).expanduser().resolve() / "yield_data_tier_report.csv"
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception:
        return {}
    if frame.empty:
        return {}
    return dict(frame.iloc[0].to_dict())


def _decision(
    tier: str,
    observed_years: int,
    year_coverage_rate: float,
    admin_level: str,
    crop_type: str,
    official: bool,
    conclusion_strength: str,
    downgrade_reason: str,
) -> dict[str, Any]:
    """Build a normalized tier decision dictionary."""

    metadata = _metadata_for_decision(tier, crop_type)
    return {
        "tier": tier,
        "tier_name": metadata["name"],
        "tier_description": metadata["description"],
        "usable_for": metadata.get("usable_for", ""),
        "not_usable_for": metadata.get("not_usable_for", ""),
        "admin_level": admin_level,
        "crop_type": crop_type,
        "is_official_statistics": bool(official),
        "observed_years": int(observed_years),
        "year_coverage_rate": float(year_coverage_rate),
        "recommended_scope": _recommended_scope(tier, admin_level, crop_type),
        "conclusion_strength": conclusion_strength,
        "downgrade_reason": downgrade_reason,
        "forbidden_claim": _forbidden_claim(tier, year_coverage_rate),
    }


def _metadata_for_decision(tier: str, crop_type: str) -> dict[str, str]:
    """Return tier metadata adjusted for crop-scope downgrades."""

    metadata = dict(TIER_METADATA[tier])
    if tier == "tier_3" and crop_type == "grain":
        metadata["name"] = "provincial_grain_panel"
        metadata["description"] = "Official provincial grain panel."
    return metadata


def _best_official_candidate(frame: Any) -> tuple[str, str, str] | None:
    """Return the best official tier candidate by configured priority."""

    candidates = [
        ("tier_1", {"prefecture", "county"}, {"rice", "early_rice", "single_rice", "late_rice"}),
        ("tier_2", {"prefecture", "county"}, {"grain"}),
        ("tier_3", {"province"}, {"rice", "early_rice", "single_rice", "late_rice", "grain"}),
    ]
    for tier, admin_levels, crop_types in candidates:
        mask = frame["_admin_level_inferred"].isin(admin_levels) & frame["_crop_type_inferred"].isin(crop_types)
        mask = mask & (frame["_has_yield"] | (frame["_has_area"] & frame["_has_production"]))
        if mask.any():
            subset = frame[mask]
            admin_level = _dominant_value(subset, "_admin_level_inferred") or sorted(admin_levels)[0]
            crop_type = _dominant_value(subset, "_crop_type_inferred") or sorted(crop_types)[0]
            return tier, admin_level, crop_type
    return None


def _infer_admin_level_from_row(row: Any) -> str:
    """Infer administrative level from explicit or location fields."""

    explicit = _clean_text(row.get("admin_level")).lower()
    if explicit in {"county", "prefecture", "province"}:
        return explicit
    if _clean_text(row.get("county")) or _clean_text(row.get("county_name")):
        return "county"
    if _clean_text(row.get("prefecture")) or _clean_text(row.get("city")) or _clean_text(row.get("city_name")):
        return "prefecture"
    if _clean_text(row.get("province")) or _clean_text(row.get("province_name")):
        return "province"
    return "unknown"


def _infer_crop_type_from_row(row: Any) -> str:
    """Infer crop type from crop labels and specialized yield fields."""

    text = " ".join(
        _clean_text(row.get(column)).lower()
        for column in ("crop", "crop_name", "category", "variable", "source", "source_file")
    )
    if any(keyword in text for keyword in ("early rice", "早稻")):
        return "early_rice"
    if any(keyword in text for keyword in ("single rice", "中稻", "一季稻", "单季稻")):
        return "single_rice"
    if any(keyword in text for keyword in ("late rice", "晚稻")):
        return "late_rice"
    if any(keyword in text for keyword in ("rice", "水稻", "稻谷")) or _clean_text(row.get("rice_yield_kg_per_hectare")):
        return "rice"
    if any(keyword in text for keyword in ("grain", "粮食", "谷物")) or _clean_text(row.get("grain_yield_kg_per_hectare")):
        return "grain"
    return "unknown"


def _is_proxy_row(row: Any) -> bool:
    """Return True when a row appears to come from proxy or remote-sensing data."""

    text = " ".join(_clean_text(row.get(column)).lower() for column in ("source", "source_file", "tier", "proxy_variable"))
    return any(keyword in text for keyword in ("proxy", "remote", "ndvi", "evi", "ggcp", "asiarice", "yield_proxy"))


def _looks_like_proxy_panel(frame: Any) -> bool:
    """Detect proxy panels by columns when source labels are absent."""

    return any(column in frame.columns for column in ("calibrated_yield", "raw_proxy_yield", "proxy_variable"))


def _has_numeric_any(frame: Any, columns: list[str]) -> Any:
    """Return a boolean Series for any numeric value across candidate columns."""

    import pandas as pd

    mask = pd.Series(False, index=frame.index)
    for column in columns:
        if column in frame.columns:
            mask = mask | pd.to_numeric(frame[column], errors="coerce").notna()
    return mask


def _numeric_series(frame: Any, column: str) -> Any:
    """Return numeric values for a column or an empty Series."""

    import pandas as pd

    if column not in frame.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _dominant_value(frame: Any, column: str) -> str:
    """Return the most frequent non-empty text value."""

    if column not in frame.columns:
        return ""
    values = frame[column].dropna().astype(str)
    values = values[values.str.strip() != ""]
    if values.empty:
        return ""
    return str(values.value_counts().idxmax())


def _conclusion_strength(tier: str, coverage: float) -> str:
    """Map tier and coverage to allowed conclusion strength."""

    if tier == "tier_1" and coverage >= 0.75:
        return "quasi_causal"
    if tier in {"tier_1", "tier_2"} and coverage >= 0.5:
        return "impact_assessment"
    if tier == "tier_3":
        return "association"
    return "descriptive"


def _downgrade_reason(tier: str, coverage: float) -> str:
    """Return a concise downgrade reason."""

    if tier == "tier_1" and coverage >= 0.75:
        return "none"
    if tier == "tier_2":
        return "rice_panel_missing_use_grain_panel"
    if tier == "tier_3":
        return "county_or_prefecture_panel_missing_use_province_panel"
    if tier == "tier_4":
        return "official_yield_panel_missing_use_remote_sensing_proxy"
    if coverage < 0.75:
        return "year_coverage_below_0_75"
    return "no_usable_official_panel"


def _recommended_scope(tier: str, admin_level: str, crop_type: str) -> str:
    """Return the recommended analysis scope."""

    if tier == "tier_4":
        return "remote_sensing_growth_anomaly_analysis"
    if tier == "missing":
        return "data_gap_only"
    return f"{admin_level}_{crop_type}_yield_anomaly"


def _forbidden_claim(tier: str, coverage: float) -> str:
    """Return claims that must be avoided under the tier."""

    if tier == "tier_4":
        return "official_yield_loss_claim; strong_causal_claim"
    if tier == "missing":
        return "yield_impact_claim"
    if coverage < 0.75:
        return "strong_causal_claim_without_valid_identification"
    return "strong_causal_claim_without_parallel_trends_and_coverage_checks"


def _write_tier_markdown(path: Path, decision: dict[str, Any]) -> None:
    """Write a Markdown tier decision report."""

    lines = [
        "# Yield Data Tier Report",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Tier: `{decision['tier']}` ({decision['tier_name']})",
        f"- Admin level: `{decision['admin_level']}`",
        f"- Crop type: `{decision['crop_type']}`",
        f"- Official statistics: {decision['is_official_statistics']}",
        f"- Observed years: {decision['observed_years']}",
        f"- Year coverage rate: {decision['year_coverage_rate']:.3f}",
        f"- Recommended scope: `{decision['recommended_scope']}`",
        f"- Allowed conclusion strength: `{decision['conclusion_strength']}`",
        f"- Downgrade reason: `{decision['downgrade_reason']}`",
        f"- Forbidden claim: `{decision['forbidden_claim']}`",
        "",
        "## Interpretation",
        "",
        _interpretation(decision),
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _interpretation(decision: dict[str, Any]) -> str:
    """Build human-readable interpretation for the current tier."""

    tier = decision["tier"]
    if tier == "tier_1":
        return "Official city/county rice data can support the main impact-assessment model; quasi-causal language still requires identification checks."
    if tier == "tier_2":
        return "Rice outcome coverage is insufficient, so the main outcome is downgraded to grain yield anomaly."
    if tier == "tier_3":
        if decision.get("crop_type") == "grain":
            return "Only provincial official grain data are available; use for macro validation or descriptive background, not city/county rice impact claims."
        return "Only provincial official rice data are available; use for robustness, background, or macro validation."
    if tier == "tier_4":
        return "Only proxy yield or vegetation-growth data are available; do not claim official yield losses."
    return "No usable yield panel is available; produce data-gap reports and avoid yield-impact claims."


def _clean_text(value: Any) -> str:
    """Normalize scalar text values."""

    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none"} else text
