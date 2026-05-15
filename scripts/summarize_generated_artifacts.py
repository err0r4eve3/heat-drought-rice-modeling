"""Summarize generated model artifacts when they exist locally."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger


EXPECTED_ARTIFACTS = {
    "annual_exposure_panel": {
        "candidates": ("annual_exposure_panel.parquet", "annual_exposure_panel.csv"),
        "directory": "processed",
        "coverage_columns": ("chd_annual", "exposure_index"),
    },
    "province_chd_panel": {
        "candidates": ("province_chd_panel.parquet", "province_chd_panel.csv"),
        "directory": "processed",
        "coverage_columns": ("chd_annual", "compound_hot_dry_days"),
    },
    "province_model_panel": {
        "candidates": ("province_model_panel.parquet", "province_model_panel.csv"),
        "directory": "processed",
        "coverage_columns": ("yield_anomaly_pct", "chd_annual", "exposure_index"),
    },
    "model_coefficients": {
        "candidates": ("model_coefficients.csv",),
        "directory": "outputs",
        "coverage_columns": ("estimate", "p_value"),
    },
    "event_study_coefficients": {
        "candidates": ("event_study_coefficients.csv",),
        "directory": "outputs",
        "coverage_columns": ("estimate", "p_value"),
    },
    "robustness_results": {
        "candidates": ("robustness_results.csv",),
        "directory": "outputs",
        "coverage_columns": ("chd_coefficient", "p_value", "direction"),
    },
}


def summarize_artifacts(processed_dir: Path, outputs_dir: Path) -> dict[str, Any]:
    """Summarize expected generated artifacts without requiring raw data."""

    roots = {"processed": processed_dir, "outputs": outputs_dir}
    artifacts: dict[str, Any] = {}
    for name, spec in EXPECTED_ARTIFACTS.items():
        root = roots[str(spec["directory"])]
        path = _first_existing(root, tuple(spec["candidates"]))
        if path is None:
            artifacts[name] = {
                "status": "missing",
                "message": "artifact missing; cannot verify generated claim",
                "expected_paths": [str(root / candidate) for candidate in spec["candidates"]],
            }
            continue
        frame = _read_table(path)
        artifacts[name] = _summarize_frame(path, frame, tuple(spec["coverage_columns"]))

    return {
        "status": "ok",
        "processed_dir": str(processed_dir),
        "outputs_dir": str(outputs_dir),
        "artifacts": artifacts,
    }


def _first_existing(root: Path, candidates: tuple[str, ...]) -> Path | None:
    for candidate in candidates:
        path = root / candidate
        if path.exists():
            return path
    return None


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported artifact type: {path}")


def _summarize_frame(path: Path, frame: pd.DataFrame, coverage_columns: tuple[str, ...]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "present",
        "path": str(path),
        "rows": int(len(frame)),
        "columns": int(len(frame.columns)),
        "year_min": _min_value(frame, "year"),
        "year_max": _max_value(frame, "year"),
        "province_count": _nunique(frame, "province"),
        "coverage": {},
    }
    for column in coverage_columns:
        if column in frame.columns:
            summary["coverage"][column] = _coverage(frame[column])
    return summary


def _coverage(series: pd.Series) -> dict[str, Any]:
    total = int(len(series))
    nonmissing = int(series.notna().sum())
    return {
        "nonmissing": nonmissing,
        "total": total,
        "rate": float(nonmissing / total) if total else None,
    }


def _min_value(frame: pd.DataFrame, column: str) -> Any:
    if column not in frame.columns or frame.empty:
        return None
    value = pd.to_numeric(frame[column], errors="coerce").dropna()
    return int(value.min()) if not value.empty else None


def _max_value(frame: pd.DataFrame, column: str) -> Any:
    if column not in frame.columns or frame.empty:
        return None
    value = pd.to_numeric(frame[column], errors="coerce").dropna()
    return int(value.max()) if not value.empty else None


def _nunique(frame: pd.DataFrame, column: str) -> int | None:
    if column not in frame.columns:
        return None
    return int(frame[column].dropna().nunique())


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Summarize locally generated project artifacts.")
    parser.add_argument("--processed-dir", type=Path, default=Path("data/processed"), help="Directory for processed artifacts.")
    parser.add_argument("--outputs-dir", type=Path, default=Path("data/outputs"), help="Directory for model output artifacts.")
    parser.add_argument("--output", type=Path, default=None, help="JSON output path. Defaults to <outputs-dir>/artifact_audit.json.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Write generated artifact audit JSON."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    output = args.output or args.outputs_dir / "artifact_audit.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    summary = summarize_artifacts(args.processed_dir, args.outputs_dir)
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logger.info("Artifact audit JSON: {}", output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
