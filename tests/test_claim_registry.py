from __future__ import annotations

import re
from pathlib import Path

import yaml


REGISTRY_PATH = Path("reports/claim_registry.yml")
REPORT_PATH = Path("reports/paper_draft.md")


def test_claim_registry_records_all_key_paper_numbers() -> None:
    registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    report = REPORT_PATH.read_text(encoding="utf-8")
    registered_values = {str(item["value"]) for item in registry["claims"]}
    report_numbers = {
        "283092",
        "775",
        "666",
        "0.859355",
        "-0.236536",
        "0.138256",
        "9/9",
    }

    assert report_numbers.issubset(registered_values)
    assert report_numbers.issubset(set(re.findall(r"-?\d+(?:\.\d+)?(?:/\d+)?", report)))


def test_untracked_artifact_claims_require_reproducibility_qualifier() -> None:
    registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    untracked = [item for item in registry["claims"] if not item["artifact_tracked"]]

    assert untracked
    for item in untracked:
        qualifier = item["required_qualifier"]
        assert any(token in qualifier for token in ["生成结果", "复现", "本地流程"])
        assert item["source_artifact_expected"].startswith(("data/interim/", "data/processed/", "data/outputs/"))
