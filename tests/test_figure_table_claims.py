from __future__ import annotations

from pathlib import Path

import yaml


CLAIMS_PATH = Path("reports/figure_table_claims.yml")


def test_figure_table_claims_have_required_schema() -> None:
    payload = yaml.safe_load(CLAIMS_PATH.read_text(encoding="utf-8"))
    required = {
        "id",
        "title",
        "scale",
        "official_outcome_scope",
        "required_label",
        "forbidden_interpretation",
        "official_yield_loss_claim_allowed",
    }

    assert payload["items"]
    for item in payload["items"]:
        assert required.issubset(item)
        if item["scale"] in {"county", "grid"}:
            assert item["official_yield_loss_claim_allowed"] is False


def test_chd_items_require_province_average_label() -> None:
    payload = yaml.safe_load(CLAIMS_PATH.read_text(encoding="utf-8"))
    chd_items = [item for item in payload["items"] if "CHD" in item["title"] or "复合热旱" in item["title"]]

    assert chd_items
    for item in chd_items:
        assert "省域平均" in item["required_label"]
