import json
import csv
from pathlib import Path

from src.inventory import build_inventory, write_inventory_outputs


def test_build_inventory_scans_supported_files(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    stats_dir = raw_dir / "statistics"
    stats_dir.mkdir(parents=True)
    csv_path = stats_dir / "yield.csv"
    csv_path.write_text("year,county,yield\n2022,A,6100\n", encoding="utf-8")

    inventory = build_inventory(raw_dir)

    assert len(inventory.records) == 1
    record = inventory.records[0]
    assert record.file_name == "yield.csv"
    assert record.suffix == ".csv"
    assert record.guessed_category == "statistics"
    assert record.readable is True
    assert record.details["columns"] == ["year", "county", "yield"]
    assert record.details["row_count"] == 1


def test_write_inventory_outputs_handles_empty_raw_dir(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    reports_dir = tmp_path / "reports"
    raw_dir.mkdir()

    inventory = build_inventory(raw_dir)
    outputs = write_inventory_outputs(inventory, processed_dir, reports_dir)

    csv_path = outputs["csv"]
    json_path = outputs["json"]
    report_path = outputs["report"]

    assert csv_path.exists()
    assert json_path.exists()
    assert report_path.exists()

    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        rows = list(reader)

    assert reader.fieldnames == [
        "path",
        "file_name",
        "suffix",
        "size_mb",
        "modified_time",
        "guessed_category",
        "readable",
        "error_message",
    ]
    assert rows == []

    detailed = json.loads(json_path.read_text(encoding="utf-8"))
    assert detailed["summary"]["total_files"] == 0
    assert "No raw data files found" in report_path.read_text(encoding="utf-8")
