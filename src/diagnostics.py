"""Diagnostics and robustness-check utilities."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DiagnosticsResult:
    """Result metadata for diagnostics."""

    status: str
    outputs: dict[str, Path] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    report_path: Path = Path("reports/diagnostics.md")


def missing_rate(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, float]:
    """Calculate missing rate for each field."""

    if not rows:
        return {field: 1.0 for field in fields}
    result: dict[str, float] = {}
    for field in fields:
        missing = sum(1 for row in rows if _is_missing(row.get(field)))
        result[field] = missing / len(rows)
    return result


def detect_outliers(values: list[Any], z_threshold: float = 3.0) -> list[bool]:
    """Detect outliers by absolute z-score."""

    numeric = [_to_float(value) for value in values]
    valid = [value for value in numeric if value is not None]
    if len(valid) < 2:
        return [False for _ in values]
    mean_value = sum(valid) / len(valid)
    variance = sum((value - mean_value) ** 2 for value in valid) / len(valid)
    std = math.sqrt(variance)
    if std == 0:
        return [False for _ in values]
    return [False if value is None else abs((value - mean_value) / std) > z_threshold for value in numeric]


def correlation_matrix(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, dict[str, float | None]]:
    """Calculate a simple Pearson correlation matrix."""

    return {
        left: {right: _pearson(_field_values(rows, left), _field_values(rows, right)) for right in fields}
        for left in fields
    }


def calculate_vif(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, float | None]:
    """Calculate a conservative VIF fallback using pairwise correlations."""

    matrix = correlation_matrix(rows, fields)
    result: dict[str, float | None] = {}
    for field in fields:
        max_r2 = 0.0
        for other in fields:
            if other == field:
                continue
            corr = matrix[field][other]
            if corr is not None:
                max_r2 = max(max_r2, corr * corr)
        result[field] = None if max_r2 >= 1 else 1 / (1 - max_r2)
    return result


def run_diagnostics(
    processed_dir: str | Path,
    output_dir: str | Path,
    reports_dir: str | Path,
    main_event_year: int,
) -> DiagnosticsResult:
    """Run diagnostics and robustness placeholders."""

    processed = Path(processed_dir).expanduser().resolve()
    outputs_root = Path(output_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    outputs_root.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    model_panel = processed / "model_panel.csv"
    if not model_panel.exists():
        warnings.append(f"Model panel not found: {model_panel}")
        rows: list[dict[str, Any]] = []
        status = "missing"
    else:
        rows = _read_csv_rows(model_panel)
        status = "ok" if rows else "empty"

    robustness_path = outputs_root / "robustness_results.csv"
    placebo_path = outputs_root / "placebo_results.csv"
    _write_csv(robustness_path, ["check", "outcome", "status"], _robustness_rows(status))
    _write_csv(placebo_path, ["placebo_year", "status"], _placebo_rows([2018, 2019, 2020, 2021], status))

    report_path = reports / "diagnostics.md"
    _write_report(report_path, status, warnings, rows, main_event_year, robustness_path, placebo_path)

    return DiagnosticsResult(
        status=status,
        outputs={"robustness": robustness_path, "placebo": placebo_path},
        warnings=warnings,
        report_path=report_path,
    )


def _write_report(
    report_path: Path,
    status: str,
    warnings: list[str],
    rows: list[dict[str, Any]],
    main_event_year: int,
    robustness_path: Path,
    placebo_path: Path,
) -> None:
    """Write diagnostics report."""

    fields = sorted(rows[0].keys()) if rows else []
    rates = missing_rate(rows, fields) if fields else {}
    lines = [
        "# Diagnostics",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {status}",
        f"- Main event year: {main_event_year}",
        f"- Rows inspected: {len(rows)}",
        f"- Robustness output: `{robustness_path}`",
        f"- Placebo output: `{placebo_path}`",
        "",
    ]
    if rates:
        lines.extend(["## Missing Rates", "", "| field | missing_rate |", "| --- | ---: |"])
        lines.extend(f"| {field} | {rate:.4f} |" for field, rate in rates.items())
        lines.append("")
    if warnings:
        lines.extend(["## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")


def _robustness_rows(status: str) -> list[dict[str, Any]]:
    """Build placeholder robustness result rows."""

    return [
        {"check": "heat_threshold_90_95", "outcome": "not_run_without_model_panel", "status": status},
        {"check": "drought_threshold_10_20", "outcome": "not_run_without_model_panel", "status": status},
        {"check": "exposure_window_6_8_7_9_6_9", "outcome": "not_run_without_model_panel", "status": status},
    ]


def _placebo_rows(years: list[int], status: str) -> list[dict[str, Any]]:
    """Build placeholder placebo result rows."""

    return [{"placebo_year": year, "status": status} for year in years]


def _field_values(rows: list[dict[str, Any]], field: str) -> list[float | None]:
    """Extract numeric values for a field."""

    return [_to_float(row.get(field)) for row in rows]


def _pearson(left: list[float | None], right: list[float | None]) -> float | None:
    """Calculate Pearson correlation with pairwise complete observations."""

    pairs = [(x, y) for x, y in zip(left, right, strict=False) if x is not None and y is not None]
    if len(pairs) < 2:
        return None
    xs, ys = zip(*pairs, strict=False)
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if denom_x == 0 or denom_y == 0:
        return None
    return round(numerator / (denom_x * denom_y), 12)


def _to_float(value: Any) -> float | None:
    """Convert value to float or None."""

    if _is_missing(value):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _is_missing(value: Any) -> bool:
    """Return True for empty or NaN-like values."""

    return value is None or value == "" or (isinstance(value, float) and math.isnan(value))


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    """Read CSV rows."""

    with path.open("r", encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    """Write CSV rows."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
