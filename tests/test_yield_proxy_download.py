from src.yield_proxy_download import (
    build_asia_rice_manifest,
    build_dataverse_file_download_url,
    filter_ggcp10_rice_files,
)


def test_build_asia_rice_manifest_filters_requested_versions() -> None:
    manifest = build_asia_rice_manifest(["Version1"])

    assert len(manifest) == 1
    assert manifest[0].source_id == "asia_rice_yield_4km"
    assert manifest[0].file_name == "Version1.zip"
    assert manifest[0].url.endswith("/Version1.zip/content")


def test_filter_ggcp10_rice_files_keeps_year_range() -> None:
    payload_files = [
        {"dataFile": {"filename": "GGCP10_Production_2010_Rice.tif", "id": 1, "filesize": 10}},
        {"dataFile": {"filename": "GGCP10_Production_2010_Maize.tif", "id": 2, "filesize": 10}},
        {"dataFile": {"filename": "GGCP10_Production_2020_Rice.tif", "id": 3, "filesize": 10}},
        {"dataFile": {"filename": "README.txt", "id": 4, "filesize": 1}},
    ]

    records = filter_ggcp10_rice_files(payload_files, 2010, 2015)

    assert len(records) == 1
    assert records[0].file_name == "GGCP10_Production_2010_Rice.tif"
    assert records[0].year == 2010
    assert records[0].source_id == "ggcp10"


def test_build_dataverse_file_download_url() -> None:
    assert build_dataverse_file_download_url(7340013).endswith("/api/access/datafile/7340013")
