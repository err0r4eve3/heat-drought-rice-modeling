from pathlib import Path

import pandas as pd

from src.data_tiers import classify_yield_data, write_yield_data_tier_report


def test_classify_prefecture_rice_as_tier_1() -> None:
    df = pd.DataFrame(
        [
            {
                "year": 2020,
                "province": "湖北",
                "prefecture": "武汉市",
                "crop": "稻谷",
                "yield_kg_per_hectare": 7000,
                "production_ton": 100,
                "sown_area_hectare": 10,
                "source": "official_yearbook",
            }
        ]
    )

    decision = classify_yield_data(df, expected_years=[2020])

    assert decision["tier"] == "tier_1"
    assert decision["admin_level"] == "prefecture"
    assert decision["crop_type"] == "rice"
    assert decision["conclusion_strength"] == "quasi_causal"


def test_classify_grain_panel_as_tier_2_downgrade() -> None:
    df = pd.DataFrame(
        [
            {
                "year": 2020,
                "province": "湖北",
                "prefecture": "武汉市",
                "crop": "粮食",
                "yield_kg_per_hectare": 6000,
                "source": "official_yearbook",
            }
        ]
    )

    decision = classify_yield_data(df, expected_years=[2020, 2021])

    assert decision["tier"] == "tier_2"
    assert decision["downgrade_reason"] == "rice_panel_missing_use_grain_panel"
    assert decision["conclusion_strength"] == "impact_assessment"


def test_classify_proxy_as_tier_4() -> None:
    decision = classify_yield_data(
        pd.DataFrame([{"admin_id": "a", "year": 2020, "source": "yield_proxy", "calibrated_yield": 7000}]),
        expected_years=[2020],
    )

    assert decision["tier"] == "tier_4"
    assert decision["recommended_scope"] == "remote_sensing_growth_anomaly_analysis"
    assert "official_yield_loss_claim" in decision["forbidden_claim"]


def test_classify_provincial_grain_names_scope_explicitly() -> None:
    df = pd.DataFrame(
        [
            {
                "year": 2024,
                "province": "湖北",
                "crop": "粮食",
                "yield_kg_per_hectare": 6000,
                "source": "official_nbs",
            }
        ]
    )

    decision = classify_yield_data(df, expected_years=[2024])

    assert decision["tier"] == "tier_3"
    assert decision["tier_name"] == "provincial_grain_panel"
    assert decision["recommended_scope"] == "province_grain_yield_anomaly"


def test_write_yield_data_tier_report(tmp_path: Path) -> None:
    result = write_yield_data_tier_report([], tmp_path / "processed", tmp_path / "reports", expected_years=[2020])

    assert result.csv_path.exists()
    assert result.report_path.exists()
    assert result.decision["tier"] == "missing"
