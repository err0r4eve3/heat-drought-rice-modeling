from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.study_region import add_event_exposure_fields, enrich_and_filter_model_panel


def test_add_event_exposure_fields_repeats_event_intensity_without_faking_annual() -> None:
    frame = pd.DataFrame(
        [
            {"province": "江苏", "year": 2021, "chd_annual": ""},
            {"province": "江苏", "year": 2022, "chd_annual": "3.0"},
            {"province": "江苏", "year": 2023, "chd_annual": ""},
            {"province": "浙江", "year": 2022, "chd_annual": "1.0"},
        ]
    )

    enriched = add_event_exposure_fields(frame)

    jiangsu_2021 = enriched[(enriched["province"] == "江苏") & (enriched["year"] == 2021)].iloc[0]
    assert jiangsu_2021["chd_annual"] == ""
    assert float(jiangsu_2021["chd_2022_intensity"]) == 3.0
    assert int(enriched.loc[enriched["province"] == "江苏", "chd_2022_treated_p75"].iloc[0]) == 1


def test_enrich_and_filter_model_panel_outputs_study_region(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    reports = tmp_path / "reports"
    processed.mkdir(parents=True)
    model = processed / "model_panel.csv"
    pd.DataFrame(
        [
            {"province": "江苏", "year": 2022, "yield_anomaly_pct": -5.0},
            {"province": "北京", "year": 2022, "yield_anomaly_pct": 1.0},
        ]
    ).to_csv(model, index=False)
    pd.DataFrame(
        [
            {"province": "江苏", "year": 2022, "chd_annual": 2.0},
            {"province": "北京", "year": 2022, "chd_annual": 0.1},
        ]
    ).to_parquet(processed / "annual_exposure_panel.parquet", index=False)

    result = enrich_and_filter_model_panel(
        model_panel=model,
        processed_dir=processed,
        reports_dir=reports,
        study_region_policy={
            "default_region": "yangtze_middle_lower",
            "regions": {"yangtze_middle_lower": {"provinces": ["江苏省", "浙江省"]}},
            "filter_model_panel_to_study_region": True,
        },
    )

    study = pd.read_csv(processed / "model_panel_study_region.csv")
    assert result.output_rows == 1
    assert study["province"].tolist() == ["江苏"]
    assert "chd_2022_intensity" in study.columns
    assert (reports / "yield_data_gap_action_plan.md").exists()
