from __future__ import annotations

from pathlib import Path

import yaml


CLAIMS_PATH = Path("reports/descriptive_evidence_claims.yml")


def test_descriptive_evidence_claims_are_registry_compatible() -> None:
    payload = yaml.safe_load(CLAIMS_PATH.read_text(encoding="utf-8"))
    required = {
        "claim_id",
        "artifact_id",
        "evidence_type",
        "raw_data_required",
        "report_section",
        "allowed_language",
        "forbidden_language",
    }

    assert payload["claims"]
    for claim in payload["claims"]:
        assert required.issubset(claim)
        assert claim["evidence_type"] == "descriptive"
        assert claim["raw_data_required"] is False
        assert "causal" in claim["forbidden_language"] or "yield loss" in claim["forbidden_language"]
