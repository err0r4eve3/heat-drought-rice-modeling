# Final Figures and Tables Plan

## 论文题目

省域尺度复合热旱暴露对粮食单产异常与稳定性的影响评估——以 2022 年长江流域极端高温干旱事件为例

## 图件计划

| 编号 | 标题 | 核心用途 | 主要数据 | 当前处理要求 |
| --- | --- | --- | --- | --- |
| Figure 1 | 研究区与 2022 CHD 暴露地图 | 展示 2022 年复合热旱事件的空间分布，并突出长江中下游及邻近省份 | `data/processed/province_chd_panel.csv`、省级边界 | 使用 `chd_annual` 或 `chd_2022_intensity`；图注说明 CHD 为省域平均暴露 |
| Figure 2 | 2022 省级粮食单产异常地图 | 展示产量侧异常的省级空间差异 | `data/processed/province_model_panel.csv` | 使用 `yield_anomaly_pct`；图注说明 outcome 为省级粮食单产异常 |
| Figure 3 | CHD 与单产异常散点图 | 展示复合热旱暴露与粮食单产异常之间的描述性关系 | `data/processed/province_model_panel.csv` | 可区分 2022 和非 2022 年；避免把散点相关写成因果 |
| Figure 4 | 固定效应模型系数图 | 展示主模型 `chd_annual` 系数、置信区间和 p 值 | `data/outputs/model_coefficients.csv` | 优先使用 `province_two_way_fixed_effects` 行；注明 p=0.138256 |
| Figure 5 | 事件研究图 | 展示事件前后动态和前趋势检验 | `data/outputs/event_study_coefficients.csv` | 图注说明仅作为辅助识别尝试；不写强因果结论 |
| Figure 6 | 稳健性系数方向图 | 展示 9 组稳健性设定方向均为负 | `data/outputs/robustness_results.csv` | 标出每组 p 值和方向；说明 9/9 方向一致不等于全部显著 |

## 表格计划

| 编号 | 标题 | 核心用途 | 主要字段 | 当前处理要求 |
| --- | --- | --- | --- | --- |
| Table 1 | 数据来源与变量说明 | 说明气象、CHD、产量和模型变量来源 | 数据源、时间范围、空间尺度、变量名、单位 | 明确 CHD 是省域平均暴露，产量为省级官方粮食统计 |
| Table 2 | 数据覆盖率 | 说明数据链完整性和门控依据 | 行数、省份数、年份范围、覆盖率 | 写入 `province_daily_climate=283092`、`annual_exposure_panel=775`、`province_model_panel=666` |
| Table 3 | 主模型结果 | 展示 FE 主模型估计 | 模型、系数、标准误、p 值、样本量、R2、FE | 主行使用 `province_two_way_fixed_effects`；结论写负向但未达常用显著性水平 |
| Table 4 | 稳健性检验 | 汇总 9 组稳健性结果 | `spec_id`、样本量、系数、标准误、p 值、方向 | 强调 9/9 方向一致；避免把方向一致写成强识别 |
| Table 5 | 研究限制与解释边界 | 固化论文口径边界 | 限制项、影响、本文处理方式 | 必列省级尺度、非稻田加权、2008-2010 缺失、非强因果 |

## 图表口径检查清单

- 图题和表题统一使用“粮食单产异常”，不使用更窄作物口径。
- 所有产量侧图表均限定为省级统计口径。
- 所有 CHD 图表均标注“省域平均暴露”。
- 主模型图表必须显示 `chd_annual=-0.236536` 与 `p=0.138256`。
- 事件研究图表只能写作辅助识别尝试。
- 稳健性图表必须同时显示方向一致性和显著性不足的限制。
- 不把任何县域、栅格或遥感响应图解释为官方产量损失。

## 建议优先级

1. 先完成 Table 2 和 Table 3，锁定数据覆盖与主模型口径。
2. 再完成 Figure 1、Figure 2 和 Figure 4，支撑核心结果章节。
3. 最后补 Figure 5、Figure 6、Table 4 和 Table 5，用于稳健性、识别边界和答辩说明。
