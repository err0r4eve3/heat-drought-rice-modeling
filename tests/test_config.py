from pathlib import Path

from src.config import ProjectConfig, ensure_project_dirs, load_config


def test_default_config_loads() -> None:
    config = load_config(Path("config/config.yaml"))

    assert isinstance(config, ProjectConfig)
    assert config.project_root == Path.cwd().resolve()
    assert config.study_area_name
    assert config.study_bbox == [105, 24, 123, 35]
    assert config.main_event_year == 2022
    assert config.validation_event_year == 2024
    assert config.data_raw_dir.name == "raw"


def test_ensure_project_dirs_creates_expected_directories(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "\n".join(
            [
                f"project_root: {tmp_path.as_posix()}",
                "data_raw_dir: data/raw",
                "data_interim_dir: data/interim",
                "data_processed_dir: data/processed",
                "output_dir: data/outputs",
                "study_area_name: test",
                "study_bbox: [105, 24, 123, 35]",
                "target_admin_level: county",
                "crs_wgs84: EPSG:4326",
                "crs_equal_area: EPSG:6933",
                "baseline_years: [2000, 2021]",
                "main_event_year: 2022",
                "validation_event_year: 2024",
                "recovery_years: [2023, 2024, 2025]",
                "main_event_months: [6, 7, 8, 9, 10]",
                "rice_growth_months: [6, 7, 8, 9]",
                "heat_threshold_quantile: 0.90",
                "drought_threshold_quantile: 0.10",
                "min_valid_observations: 3",
                "output_formats: [csv, parquet, geopackage, geotiff, png, markdown]",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(config_file)
    ensure_project_dirs(config)

    assert config.data_raw_dir.exists()
    assert config.data_interim_dir.exists()
    assert config.data_processed_dir.exists()
    assert config.output_dir.exists()
