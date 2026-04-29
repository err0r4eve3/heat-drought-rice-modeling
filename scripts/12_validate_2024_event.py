"""Generate downgraded validation outputs for the 2024 event."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ensure_project_dirs, load_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Validate 2024 event with downgraded evidence rules.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> int:
    """Generate 2024 validation summaries."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    config = load_config(args.config)
    ensure_project_dirs(config)
    output_dir = config.output_dir
    reports_dir = config.project_root / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    climate = _read_first_existing([config.data_processed_dir / "admin_climate_panel.parquet", config.data_processed_dir / "admin_climate_panel.csv"])
    remote = _read_first_existing(
        [config.data_processed_dir / "admin_remote_sensing_panel.parquet", config.data_processed_dir / "admin_remote_sensing_panel.csv"]
    )
    yield_panel = _read_first_existing([config.data_processed_dir / "yield_panel_combined.parquet", config.data_processed_dir / "yield_panel_combined.csv"])

    validation_year = int(config.validation_event_year)
    exposure_summary = _summarize_panel(climate, validation_year)
    remote_summary = _summarize_panel(remote, validation_year)
    yield_status = _yield_status(yield_panel, validation_year)

    exposure_path = output_dir / "validation_2024_exposure_summary.csv"
    remote_path = output_dir / "validation_2024_remote_sensing_summary.csv"
    report_path = reports_dir / "validation_2024_summary.md"
    exposure_summary.to_csv(exposure_path, index=False, encoding="utf-8-sig")
    remote_summary.to_csv(remote_path, index=False, encoding="utf-8-sig")
    _write_report(report_path, exposure_path, remote_path, yield_status, validation_year)

    logger.info("Exposure summary: {}", exposure_path)
    logger.info("Remote-sensing summary: {}", remote_path)
    logger.info("Report: {}", report_path)
    return 0


def _read_first_existing(paths: list[Path]) -> pd.DataFrame:
    """Read the first existing CSV/Parquet table."""

    for path in paths:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".parquet":
                return pd.read_parquet(path)
            return pd.read_csv(path)
        except Exception:
            continue
    return pd.DataFrame()


def _summarize_panel(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    """Summarize long-format panel values for a validation year."""

    columns = ["year", "variable", "row_count", "admin_unit_count", "mean", "min", "max"]
    if frame.empty or "year" not in frame.columns:
        return pd.DataFrame(columns=columns)
    data = frame[pd.to_numeric(frame["year"], errors="coerce") == year].copy()
    if data.empty:
        return pd.DataFrame(columns=columns)
    value_column = "mean" if "mean" in data.columns else ("value" if "value" in data.columns else None)
    if value_column is None:
        return pd.DataFrame(columns=columns)
    data["_value"] = pd.to_numeric(data[value_column], errors="coerce")
    if "variable" not in data.columns:
        data["variable"] = "value"
    if "admin_id" not in data.columns:
        data["admin_id"] = ""
    summary = (
        data.groupby("variable", dropna=False)
        .agg(
            row_count=("_value", "size"),
            admin_unit_count=("admin_id", "nunique"),
            mean=("_value", "mean"),
            min=("_value", "min"),
            max=("_value", "max"),
        )
        .reset_index()
    )
    summary.insert(0, "year", year)
    return summary[columns]


def _yield_status(frame: pd.DataFrame, year: int) -> dict[str, object]:
    """Check whether official 2024 yield validation is available."""

    if frame.empty or "year" not in frame.columns:
        return {"available": False, "level": "missing", "rows": 0}
    data = frame[pd.to_numeric(frame["year"], errors="coerce") == year].copy()
    if data.empty:
        return {"available": False, "level": "missing", "rows": 0}
    if "admin_level" in data.columns:
        levels = sorted(str(value) for value in data["admin_level"].dropna().unique())
    else:
        levels = []
    if any(level in {"county", "prefecture"} for level in levels):
        level = "county_or_prefecture"
    elif "province" in levels or "province" in data.columns:
        level = "province"
    else:
        level = "unknown"
    return {"available": True, "level": level, "rows": int(len(data))}


def _write_report(report_path: Path, exposure_path: Path, remote_path: Path, yield_status: dict[str, object], year: int) -> None:
    """Write the 2024 validation Markdown report."""

    lines = [
        "# 2024 Validation Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Validation year: {year}",
        "- 2024 年数据用于外部一致性验证，不作为主因果识别事件。",
        f"- Exposure summary: `{exposure_path}`",
        f"- Remote-sensing summary: `{remote_path}`",
        f"- Official yield availability: {yield_status['available']}",
        f"- Official yield level: {yield_status['level']}",
        f"- Official yield rows: {yield_status['rows']}",
        "",
    ]
    if not yield_status["available"]:
        lines.extend(
            [
                "## Downgrade Decision",
                "",
                "- 官方县/市级产量缺失：只输出暴露和遥感响应。",
                "- 不输出县/市级产量损失估计。",
                "- 不输出 causal_validation 或 recovery_conclusion。",
                "",
            ]
        )
    elif yield_status["level"] == "province":
        lines.extend(["## Downgrade Decision", "", "- 只有省级产量：仅做省级交叉验证。", ""])
    report_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
