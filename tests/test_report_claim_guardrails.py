from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.validate_report_claims import validate_report_paths


def test_report_claim_guardrail_rejects_unbounded_causal_claim(tmp_path: Path) -> None:
    report = tmp_path / "report.md"
    report.write_text("本文识别出因果效应，并证明县级产量损失。\n", encoding="utf-8")

    issues = validate_report_paths([report])

    assert {issue.rule for issue in issues} >= {"causal_overclaim", "subprovince_yield_loss_overclaim"}


def test_report_claim_guardrail_allows_bounded_language(tmp_path: Path) -> None:
    report = tmp_path / "report.md"
    report.write_text(
        "\n".join(
            [
                "# 省域平均 CHD 暴露结果",
                "",
                "本文为省域尺度影响评估，不作准因果识别，不能解释为县级产量损失。",
                "2024 年仅作为描述性外部一致性辅助材料。",
                "当前 CHD 暴露为省域平均暴露，不是稻田加权暴露。",
            ]
        ),
        encoding="utf-8",
    )

    assert validate_report_paths([report]) == []


def test_report_claim_guardrail_requires_chd_scope_in_table_labels(tmp_path: Path) -> None:
    report = tmp_path / "report.md"
    report.write_text(
        "\n".join(
            [
                "# 结果",
                "",
                "本文为影响评估。",
                "",
                "| 编号 | 标题 |",
                "| --- | --- |",
                "| Figure 1 | CHD 暴露地图 |",
            ]
        ),
        encoding="utf-8",
    )

    issues = validate_report_paths([report])

    assert any(issue.rule == "chd_scope_label_missing" for issue in issues)


def test_report_claim_guardrail_cli_passes_current_reports() -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/validate_report_claims.py"],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
