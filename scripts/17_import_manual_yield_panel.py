"""Command-line entry point for manual official yield panel import."""

from __future__ import annotations

import sys
from pathlib import Path

import typer
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config  # noqa: E402
from src.manual_yield_import import create_yield_panel_template, import_manual_yield_panel  # noqa: E402


app = typer.Typer(help="Import the manually curated official yield panel template.")


@app.command()
def main(
    config: Path = typer.Option(Path("config/config.yaml"), help="Path to config YAML."),
    template_path: Path | None = typer.Option(None, help="Manual yield template CSV path."),
    processed_dir: Path | None = typer.Option(None, help="Processed output directory."),
    reports_dir: Path | None = typer.Option(None, help="Reports output directory."),
    create_template: bool = typer.Option(False, help="Create the template before importing."),
    verbose: bool = typer.Option(False, help="Enable debug logging."),
) -> None:
    """Run manual official yield panel import."""

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "INFO")

    project_config = load_config(config)
    resolved_template = (
        template_path.expanduser().resolve()
        if template_path
        else project_config.project_root / "data" / "manual_templates" / "yield_panel_template.csv"
    )
    resolved_processed = processed_dir.expanduser().resolve() if processed_dir else project_config.data_processed_dir
    resolved_reports = reports_dir.expanduser().resolve() if reports_dir else project_config.project_root / "reports"

    if create_template:
        create_yield_panel_template(resolved_template)
        logger.info("Template written: {}", resolved_template)

    result = import_manual_yield_panel(
        template_path=resolved_template,
        processed_dir=resolved_processed,
        reports_dir=resolved_reports,
    )
    for warning in result.warnings:
        logger.warning(warning)
    for key, path in result.outputs.items():
        logger.info("{}: {}", key, path)
    logger.info("Report: {}", result.report_path)


if __name__ == "__main__":
    app()
