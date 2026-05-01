# 省域复合热旱暴露与长江中下游粮食/稻谷单产异常

本项目用于统计建模大赛的可复现数据处理与建模框架。当前研究口径已降级为：省级官方产量面板负责产量结论，县域/栅格遥感和气象数据负责暴露、长势和机制分析；MVP 聚焦 2022 年长江中下游极端高温干旱事件，并预留 2024 年伏秋旱作为外部验证事件。

## 项目目标

- 扫描 `data/raw/`，生成可检查的数据清单。
- 将气象、遥感、稻田掩膜和农业统计数据整理为省级主模型面板。
- 构造省级稻田加权复合热旱暴露、省级官方单产异常和稳定性指标。
- 县域、地级市和栅格尺度只输出暴露差异、遥感长势响应和机制分析，不输出官方产量损失结论。
- 输出描述性相关模型、固定效应候选、事件研究候选、图表和 Markdown 摘要；结论强度默认是 impact assessment / association。

## 目录

- `config/config.yaml`：路径、年份、研究区、阈值和输出格式配置。
- `data/raw/`：只读原始数据目录，按主题分子目录。
- `data/interim/`：中间结果，便于排错。
- `data/processed/`：清洗后的面板、空间数据和指标。
- `data/outputs/`：模型结果、图表和可提交输出。
- `scripts/`：命令行脚本。
- `src/`：可测试的功能模块。
- `tests/`：轻量测试，不依赖真实大文件。
- `reports/`：自动生成的 Markdown 报告。

## 环境

推荐 Windows + `uv`，项目会在根目录创建 `.venv`：

```powershell
uv sync
```

Conda 配置仍保留为备选：

```powershell
conda env create -f environment.yml
conda activate heat-drought-rice
```

## 当前 MVP 运行顺序

```powershell
uv run python -m pytest
uv run python scripts/12_stage_existing_downloads.py --config config/config.yaml --package-root heat_drought_download_package
uv run python run_pipeline.py --step all --config config/config.yaml --continue-on-error
```

也可以逐步运行：

```powershell
uv run python scripts/12_stage_existing_downloads.py --config config/config.yaml --package-root heat_drought_download_package
uv run python scripts/00_inventory.py --config config/config.yaml
uv run python scripts/01_prepare_boundaries.py --config config/config.yaml
uv run python scripts/02_preprocess_climate.py --config config/config.yaml
uv run python scripts/03_preprocess_remote_sensing.py --config config/config.yaml
uv run python scripts/04_prepare_crop_mask_phenology.py --config config/config.yaml
uv run python scripts/05_spatial_aggregate.py --config config/config.yaml
uv run python scripts/06_prepare_statistics.py --config config/config.yaml
uv run python scripts/12_validate_2024_event.py --config config/config.yaml
uv run python scripts/13_build_admin_crosswalk.py --config config/config.yaml
uv run python scripts/14_risk_register.py --config config/config.yaml
uv run python scripts/13_download_yield_panel_sources.py --config config/config.yaml
uv run python scripts/14_research_required_data_sources.py --config config/config.yaml
uv run python scripts/16_download_yield_proxy_rasters.py --config config/config.yaml
uv run python scripts/17_assign_admin_provinces.py --config config/config.yaml
uv run python scripts/15_build_yield_proxy_panel.py --config config/config.yaml
uv run python scripts/16_build_annual_exposure_panel.py --config config/config.yaml
uv run python scripts/17_import_manual_yield_panel.py --config config/config.yaml
uv run python scripts/19_build_province_chd_panel.py --config config/config.yaml
uv run python scripts/18_build_province_panel.py --config config/config.yaml
uv run python scripts/07_build_indices.py --config config/config.yaml
uv run python scripts/08_modeling.py --config config/config.yaml
uv run python scripts/09_make_figures.py --config config/config.yaml
uv run python scripts/10_diagnostics.py --config config/config.yaml
uv run python scripts/18_generate_risk_action_report.py --config config/config.yaml
uv run python scripts/11_generate_report.py --config config/config.yaml
```

`run_pipeline.py` 会把当前解释器传给子步骤；因此使用 `uv run python run_pipeline.py ...` 时，所有步骤都在项目 `.venv` 中执行。
`12_stage_existing_downloads.py` 只复制边界和统计等小文件；气象、遥感、稻田掩膜、物候等大文件会写入外部索引
`data/raw/references/external_data_sources.csv` 和 `data/raw/references/external_data_sources.json`，后续脚本在本地 raw 子目录为空时会读取这些外部路径。

## 当前输出

- `data/processed/data_inventory.csv`
- `data/processed/data_inventory_detailed.json`
- `data/raw/references/external_data_sources.csv`
- `data/raw/references/external_data_sources.json`
- `data/raw/references/deep_required_data_sources.csv`
- `data/raw/references/deep_required_data_sources.json`
- `data/processed/admin_units.gpkg`
- `data/processed/admin_units_equal_area.gpkg`
- `data/interim/climate_monthly.parquet` 或 CSV fallback
- `data/interim/climate_growing_season.parquet` 或 CSV fallback
- `data/interim/climate_extremes_grid.parquet` 或 CSV fallback
- `data/interim/remote_sensing_monthly.parquet` 或 CSV fallback
- `data/interim/remote_sensing_growing_season.parquet` 或 CSV fallback
- `data/processed/crop_mask_summary_by_admin.csv`
- `data/processed/phenology_by_admin.csv`
- `data/processed/admin_climate_panel.parquet` 或 CSV fallback
- `data/processed/admin_remote_sensing_panel.parquet` 或 CSV fallback
- `data/processed/yield_panel.csv`
- `data/processed/yield_coverage_report.csv`
- `data/processed/yield_data_tier_report.csv`
- `data/processed/yield_panel_external_province.csv`
- `data/processed/yield_panel_combined.csv`
- `data/processed/yield_proxy/county_yield_proxy_panel.csv`
- `data/processed/yield_proxy/yield_proxy_gap_report.csv`
- `data/processed/yield_proxy/calibration_status_summary.csv`
- `data/processed/risk_register.csv`
- `data/processed/risk_coverage_summary.csv`
- `data/processed/external_access_check.csv`
- `data/processed/admin_unmatched_province_units.csv`
- `data/processed/admin_crosswalk_2000_2025.csv`
- `data/processed/admin_crosswalk_low_confidence.csv`
- `data/raw/references/yield_proxy_download_manifest.csv`
- `data/processed/admin_units_with_province.gpkg`
- `data/processed/province_chd_panel.csv`
- `data/processed/province_chd_panel.parquet`
- `data/processed/province_model_panel.csv`
- `data/processed/province_model_panel.parquet`
- `data/processed/model_panel.csv`
- `data/outputs/model_coefficients.csv`
- `data/outputs/event_study_coefficients.csv`
- `data/outputs/figures/*.png`
- `data/outputs/figures/*.svg`
- `reports/data_inventory.md`
- `reports/data_staging_summary.md`
- `reports/boundary_summary.md`
- `reports/climate_preprocess_summary.md`
- `reports/remote_sensing_summary.md`
- `reports/crop_mask_phenology_summary.md`
- `reports/spatial_aggregation_qc.md`
- `reports/statistics_cleaning_summary.md`
- `reports/yield_coverage_report.md`
- `reports/yield_data_tier_report.md`
- `reports/validation_2024_summary.md`
- `reports/admin_crosswalk_summary.md`
- `reports/external_access_check.md`
- `reports/external_access_status.md`
- `reports/data_gap_report.md`
- `reports/data_source_decision.md`
- `reports/yield_panel_feasibility.md`
- `reports/admin_crosswalk_decision.md`
- `reports/model_claim_scope.md`
- `reports/model_scope_decision.md`
- `reports/index_construction_summary.md`
- `reports/model_results.md`
- `reports/figure_summary.md`
- `reports/diagnostics.md`
- `reports/deep_data_search_report.md`
- `reports/yield_proxy_panel_summary.md`
- `reports/yield_proxy_download_summary.md`
- `reports/admin_province_assignment_summary.md`
- `reports/province_chd_panel_summary.md`
- `reports/province_panel_summary.md`
- `reports/project_risk_assessment.md`
- `reports/unavoidable_risks_for_research.md`
- `reports/pipeline_run_summary.md`
- `reports/final_analysis_summary.md`

## 当前完成度

MVP 已覆盖完整 pipeline 的空数据可运行路径，并已固化数据源深度检索目录。当前关键事实是地级市/县级 2000-2024 内容年份官方稻谷或粮食单产连续面板不可得，因此主模型不再依赖市县级官方产量。正式产量结论限定在省级；2025 只作全国/省级背景或补充说明。

## 数据风险与降级策略

当前研究口径固定为“省级官方产量面板 + 高分辨率遥感/气象暴露聚合 + 2022 事件影响评估”，不默认强因果。模型和报告会读取 `province_model_panel`、`province_chd_panel`、`yield_data_tier_report.csv`、`yield_coverage_report.csv` 和 `risk_register.csv` 自动决定能写到什么强度。

主模型路径：省级官方稻谷/水稻单产异常优先；若省级稻谷数据不可得，则使用省级粮食单产异常。主模型覆盖 2000-2024 内容年份；2025 仅用于全国/省级背景或补充说明。

空间暴露路径：ERA5-Land、CHIRPS、MODIS、SMAP、GLEAM 等栅格或县域结果按稻田面积优先加权到省级；没有稻田面积时退到行政面积或非加权均值，并在报告中说明。

子省级路径：县域、地级市和栅格尺度只用于热旱暴露差异、遥感长势异常和机制分析，不作为官方产量损失结论。

禁止路径：不再构建 prefecture_yield_main_model、county_yield_main_model、county_level_yield_loss_map 或 prefecture_event_study；也不得用遥感代理直接声称官方产量损失。

行政区划：主模型默认映射到 2022 年事件边界。`admin_crosswalk_2000_2025.csv` 用于统计数据跨年匹配，`admin_crosswalk_low_confidence.csv` 中 `match_confidence < 0.85` 的记录必须人工复核。

2024 验证：默认只作为外部一致性验证或描述性对照；若省级官方粮食/稻谷数据可得，可用于交叉验证，不做市县级官方产量损失结论。

结论强度：默认影响评估或相关性；省级面板覆盖、处理组定义、平行趋势、安慰剂和稳健性检验满足条件时，最高写“准因果证据”；任何情况下都不写“证明 2022 热旱导致单产下降”。

## 数据约束

`data/raw/` 只读，不在处理流程中修改。所有中间结果写入 `data/interim/`，所有可复用结果写入 `data/processed/`，所有图表和模型输出写入 `data/outputs/`。
