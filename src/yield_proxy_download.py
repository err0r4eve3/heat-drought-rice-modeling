"""Download manifests and files for open gridded yield proxy datasets."""

from __future__ import annotations

import csv
import json
import shutil
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ASIA_RICE_RECORD_ID = "11443978"
ASIA_RICE_API_URL = f"https://zenodo.org/api/records/{ASIA_RICE_RECORD_ID}"
GGCP10_DATASET_API_URL = "https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId=doi:10.7910/DVN/G1HBNK"
DATAVERSE_FILE_DOWNLOAD_BASE = "https://dataverse.harvard.edu/api/access/datafile"


@dataclass(frozen=True)
class ProxyDownloadRecord:
    """One downloadable yield-proxy source file."""

    source_id: str
    file_name: str
    url: str
    relative_path: str
    year: int | None
    size_bytes: int | None
    expected_status: str = "planned"


@dataclass(frozen=True)
class ProxyDownloadResult:
    """Result metadata for yield-proxy download/acquisition."""

    status: str
    manifest_count: int
    downloaded_count: int
    extracted_count: int
    output_dir: Path
    manifest_csv: Path
    manifest_json: Path
    report_path: Path
    warnings: list[str]


def build_asia_rice_manifest(versions: list[str] | None = None) -> list[ProxyDownloadRecord]:
    """Build static Zenodo download records for AsiaRiceYield4km archives."""

    selected = set(versions or ["Version1"])
    records = [
        ProxyDownloadRecord(
            source_id="asia_rice_yield_4km",
            file_name="Version1.zip",
            url=f"https://zenodo.org/api/records/{ASIA_RICE_RECORD_ID}/files/Version1.zip/content",
            relative_path="asia_rice_yield_4km/Version1.zip",
            year=None,
            size_bytes=16165366,
        ),
        ProxyDownloadRecord(
            source_id="asia_rice_yield_4km",
            file_name="Version2.zip",
            url=f"https://zenodo.org/api/records/{ASIA_RICE_RECORD_ID}/files/Version2.zip/content",
            relative_path="asia_rice_yield_4km/Version2.zip",
            year=None,
            size_bytes=4508480,
        ),
    ]
    return [record for record in records if Path(record.file_name).stem in selected]


def build_dataverse_file_download_url(file_id: int) -> str:
    """Build a Harvard Dataverse direct file-download API URL."""

    return f"{DATAVERSE_FILE_DOWNLOAD_BASE}/{int(file_id)}"


def filter_ggcp10_rice_files(files: list[dict[str, Any]], year_min: int, year_max: int) -> list[ProxyDownloadRecord]:
    """Convert Dataverse file metadata into GGCP10 rice records."""

    records: list[ProxyDownloadRecord] = []
    for file_info in files:
        data_file = file_info.get("dataFile", {})
        filename = str(data_file.get("filename", ""))
        if not filename.lower().endswith("_rice.tif") or "ggcp10_production_" not in filename.lower():
            continue
        year = _parse_year(filename)
        if year is None or year < int(year_min) or year > int(year_max):
            continue
        file_id = data_file.get("id")
        if file_id is None:
            continue
        records.append(
            ProxyDownloadRecord(
                source_id="ggcp10",
                file_name=filename,
                url=build_dataverse_file_download_url(int(file_id)),
                relative_path=f"ggcp10/{filename}",
                year=year,
                size_bytes=int(data_file["filesize"]) if data_file.get("filesize") is not None else None,
            )
        )
    return sorted(records, key=lambda record: (record.year or 0, record.file_name))


def fetch_ggcp10_rice_manifest(year_min: int, year_max: int, timeout_seconds: int = 120) -> list[ProxyDownloadRecord]:
    """Fetch GGCP10 file metadata from Harvard Dataverse and return rice records."""

    payload = _read_json_url(GGCP10_DATASET_API_URL, timeout_seconds)
    files = payload.get("data", {}).get("latestVersion", {}).get("files", [])
    return filter_ggcp10_rice_files(files, year_min, year_max)


def build_proxy_download_manifest(
    sources: list[str],
    year_min: int,
    year_max: int,
    timeout_seconds: int = 120,
) -> tuple[list[ProxyDownloadRecord], list[str]]:
    """Build a combined proxy download manifest for selected sources."""

    selected = {source.strip().lower() for source in sources}
    warnings: list[str] = []
    records: list[ProxyDownloadRecord] = []

    if "asia" in selected or "asia_rice_yield_4km" in selected:
        records.extend(build_asia_rice_manifest(["Version1"]))
    if "ggcp10" in selected:
        try:
            records.extend(fetch_ggcp10_rice_manifest(year_min, year_max, timeout_seconds))
        except Exception as exc:  # noqa: BLE001 - keep Asia manifest if Dataverse is unavailable
            warnings.append(f"Could not fetch GGCP10 Dataverse manifest: {type(exc).__name__}: {exc}")
    return records, warnings


def download_proxy_sources(
    output_dir: str | Path,
    references_dir: str | Path,
    reports_dir: str | Path,
    sources: list[str],
    year_min: int,
    year_max: int,
    execute_download: bool = False,
    extract_archives: bool = True,
    force: bool = False,
    timeout_seconds: int = 120,
) -> ProxyDownloadResult:
    """Write a proxy download manifest and optionally download/extract files."""

    output = Path(output_dir).expanduser().resolve()
    references = Path(references_dir).expanduser().resolve()
    reports = Path(reports_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    references.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    manifest_csv = references / "yield_proxy_download_manifest.csv"
    manifest_json = references / "yield_proxy_download_manifest.json"
    report_path = reports / "yield_proxy_download_summary.md"

    records, warnings = build_proxy_download_manifest(sources, year_min, year_max, timeout_seconds)
    statuses = [_manifest_row(record, output) for record in records]
    downloaded_count = 0
    extracted_count = 0

    if execute_download:
        for row, record in zip(statuses, records, strict=True):
            try:
                target = output / record.relative_path
                status = _download_file(record.url, target, record.size_bytes, force, timeout_seconds)
                row["status"] = status
                if status in {"downloaded", "exists"}:
                    downloaded_count += 1
                if extract_archives and target.suffix.lower() == ".zip" and target.exists():
                    extracted_count += _extract_zip(target, target.with_suffix(""))
            except Exception as exc:  # noqa: BLE001 - continue downloading the rest
                row["status"] = "error"
                row["error_message"] = f"{type(exc).__name__}: {exc}"
                warnings.append(f"Failed to download {record.file_name}: {type(exc).__name__}: {exc}")
    else:
        for row in statuses:
            row["status"] = "manifest_only"

    _write_manifest_outputs(statuses, manifest_csv, manifest_json)
    status = "ok" if records and not warnings else "warning" if records else "empty"
    if execute_download and downloaded_count == 0:
        status = "warning"
    result = ProxyDownloadResult(
        status=status,
        manifest_count=len(records),
        downloaded_count=downloaded_count,
        extracted_count=extracted_count,
        output_dir=output,
        manifest_csv=manifest_csv,
        manifest_json=manifest_json,
        report_path=report_path,
        warnings=warnings,
    )
    _write_download_report(result, statuses, execute_download)
    return result


def _read_json_url(url: str, timeout_seconds: int) -> dict[str, Any]:
    """Read a JSON URL with urllib."""

    request = urllib.request.Request(url, headers={"User-Agent": "heat-drought-rice-modeling/0.1"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _download_file(url: str, target: Path, expected_size: int | None, force: bool, timeout_seconds: int) -> str:
    """Download one file if needed and return status."""

    if target.exists() and not force:
        if expected_size is None or target.stat().st_size == expected_size:
            return "exists"

    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(target.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "heat-drought-rice-modeling/0.1"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response, temp_path.open("wb") as file_obj:
        shutil.copyfileobj(response, file_obj)
    if expected_size is not None and temp_path.stat().st_size != expected_size:
        raise OSError(f"Downloaded size mismatch for {target.name}: {temp_path.stat().st_size} != {expected_size}")
    temp_path.replace(target)
    return "downloaded"


def _extract_zip(zip_path: Path, extract_dir: Path) -> int:
    """Safely extract a zip archive and return extracted file count."""

    extract_dir.mkdir(parents=True, exist_ok=True)
    extracted = 0
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            target = (extract_dir / member.filename).resolve()
            if not str(target).startswith(str(extract_dir.resolve())):
                raise OSError(f"Unsafe zip member path: {member.filename}")
            if target.exists() and target.stat().st_size == member.file_size:
                extracted += 1
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            extracted += 1
    return extracted


def _manifest_row(record: ProxyDownloadRecord, output_dir: Path) -> dict[str, Any]:
    """Convert one record to a manifest row."""

    row = asdict(record)
    row["target_path"] = str(output_dir / record.relative_path)
    row["status"] = record.expected_status
    row["error_message"] = ""
    return row


def _write_manifest_outputs(rows: list[dict[str, Any]], csv_path: Path, json_path: Path) -> None:
    """Write CSV and JSON manifest outputs."""

    fieldnames = [
        "source_id",
        "file_name",
        "url",
        "relative_path",
        "target_path",
        "year",
        "size_bytes",
        "status",
        "error_message",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "records": rows}
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_download_report(result: ProxyDownloadResult, rows: list[dict[str, Any]], execute_download: bool) -> None:
    """Write a Markdown report for yield-proxy source acquisition."""

    lines = [
        "# Yield Proxy Download Summary",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Status: {result.status}",
        f"- Execute download: {execute_download}",
        f"- Output directory: `{result.output_dir}`",
        f"- Manifest rows: {result.manifest_count}",
        f"- Downloaded/existing files: {result.downloaded_count}",
        f"- Extracted archive members: {result.extracted_count}",
        "",
        "## Outputs",
        "",
        f"- manifest_csv: `{result.manifest_csv}`",
        f"- manifest_json: `{result.manifest_json}`",
        "",
        "## Manifest Status Counts",
        "",
    ]
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status", ""))
        counts[status] = counts.get(status, 0) + 1
    if counts:
        lines.extend(f"- {key}: {value}" for key, value in sorted(counts.items()))
    else:
        lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    if result.warnings:
        lines.extend(f"- {warning}" for warning in result.warnings)
    else:
        lines.append("- None.")
    lines.append("")
    result.report_path.write_text("\n".join(lines), encoding="utf-8")


def _parse_year(text: str) -> int | None:
    """Parse the first four-digit year from text."""

    import re

    match = re.search(r"(19|20)\d{2}", text)
    return int(match.group(0)) if match else None
