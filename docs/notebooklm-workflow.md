# NotebookLM Workflow

Generated on 2026-04-28.

## Purpose

NotebookLM is used as the project knowledge base for source-grounded summaries, citation checks, and research memos. Repository code, tests, and generated outputs remain the source of truth for implementation behavior.

## Sources To Keep In NotebookLM

- `docs/project-facts.md`
- `docs/references.md`
- `reports/deep_data_search_report.md`
- `reports/final_analysis_summary.md`
- `reports/project_risk_assessment.md`
- `reports/yield_proxy_panel_summary.md`
- `reports/yield_proxy_download_summary.md`
- `reports/admin_province_assignment_summary.md`
- `reports/unavoidable_risks_for_research.md`
- `data/raw/references/deep_required_data_sources.csv`

## Query Rules

- Use NotebookLM for literature/source summaries, external data-source comparisons, and citation-supported writing.
- Do not use NotebookLM as a substitute for reading current source code and tests.
- Treat NotebookLM answers as source-grounded summaries; verify any implementation claim against local code.
- When NotebookLM cites a source, prefer the underlying source title/URL and record uncertainty if the source is incomplete.
- Do not add secrets, credentials, cookies, private keys, or sensitive raw data to NotebookLM.

## Notebook Record

- Notebook title target: `统计建模-热旱稻作项目知识库`
- Notebook ID: `ceacc8dc-a317-44dd-bfb6-c3cefafacd1f`
- Notebook URL: https://notebooklm.google.com/notebook/ceacc8dc-a317-44dd-bfb6-c3cefafacd1f
- Imported source count on 2026-04-28: 23.
- Initial summary note ID: `131e1a72-d064-49bb-b119-2e480505f75e`.
- Yield-proxy execution note ID: `7249d8de-4a1a-41ce-bf4d-9014ab99b830`.
- Yield-proxy latest status source ID: `458ec861-7f08-4d06-b2eb-3dbba0031862`.
- Yield-proxy completion note ID: `cfde6b9b-3860-4a4f-82e7-888bcfc91d13`.
- Residual-risk report source ID: `809e37f6-e45a-4370-a791-4c4277e42b61`.
- Residual-risk report note ID: `2851d162-2dae-4993-abf3-0f285d5d20c0`.

## Recommended Queries

- `请基于项目来源，总结当前研究事实、数据源、限制和下一步。`
- `请列出能支持“县/市级水稻单产面板缺失”这一判断的来源和证据。`
- `请按论文写作口径，给出气象、遥感、稻田掩膜、产量统计各自的数据来源、用途和限制。`
- `请生成答辩用的风险说明，重点解释为什么当前不能声称县级产量因果识别。`
- `请基于 yield_proxy_panel_summary.md 解释县级单产代理框架当前完成了什么，还缺哪些栅格输入。`
- `请基于 unavoidable_risks_for_research.md 只列出当前真正需要外部调研或研究设计决策的风险。`
