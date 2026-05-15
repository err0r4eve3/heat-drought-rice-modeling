"""Validate paper/report wording against conservative claim guardrails."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from loguru import logger


DEFAULT_REPORTS = (
    Path("reports/paper_draft.md"),
    Path("reports/defense_qa.md"),
    Path("reports/final_figures_tables_plan.md"),
)

CAUSAL_TERMS = (
    "准因果",
    "quasi_causal",
    "quasi-causal",
    "因果识别",
    "因果效应",
    "causal identification",
    "causal effect",
)

SUBPROVINCE_YIELD_LOSS_TERMS = (
    "县域官方减产",
    "县级官方减产",
    "地市官方减产",
    "市县级产量损失",
    "县级产量损失",
    "地级市产量损失",
    "county official yield loss",
    "prefecture official yield loss",
)

PADDY_WEIGHTED_TERMS = (
    "paddy-weighted",
    "paddy weighted",
    "稻田像元加权暴露",
    "稻田加权暴露",
    "稻田暴露",
)

BALANCED_PANEL_TERMS = (
    "完全平衡面板",
    "完美平衡",
    "perfectly balanced",
)

TREATMENT_EFFECT_TERMS = (
    "处理效应",
    "treatment effect",
    "导致减产",
    "归因于",
    "attributed to",
)

SAFE_NEGATION_TERMS = (
    "不",
    "不能",
    "不可",
    "不得",
    "未",
    "非",
    "不是",
    "不作",
    "不做",
    "不写",
    "不构成",
    "不等同",
    "禁止",
    "避免",
    "限制",
    "仅",
    "只",
    "rather than",
    "not ",
    "cannot",
    "should not",
)

IMPACT_SCOPE_TERMS = (
    "impact_assessment",
    "影响评估",
    "风险识别",
    "统计关联",
)

CHD_TOKENS = (
    "CHD",
    "chd_annual",
    "复合热旱暴露",
    "暴露指数",
)

CHD_SCOPE_TERMS = (
    "省域平均",
    "省级",
    "省域尺度",
    "province-average",
    "province average",
)

VALIDATION_2024_TERMS = (
    "验证",
    "validation",
    "检验",
)

VALIDATION_2024_QUALIFIERS = (
    "描述性",
    "外部一致性",
    "辅助",
    "对照",
    "不作为",
    "external consistency",
    "descriptive",
)


@dataclass(frozen=True)
class ClaimIssue:
    """A single report-claim guardrail violation."""

    path: Path
    line: int
    rule: str
    message: str
    excerpt: str


def validate_report_paths(paths: list[Path]) -> list[ClaimIssue]:
    """Return all report claim issues for the given Markdown files."""

    issues: list[ClaimIssue] = []
    existing_paths = [path for path in paths if path.exists()]
    missing_paths = [path for path in paths if not path.exists()]
    for path in missing_paths:
        issues.append(
            ClaimIssue(
                path=path,
                line=0,
                rule="missing_report",
                message="Expected report file is missing.",
                excerpt="",
            )
        )

    for path in existing_paths:
        text = path.read_text(encoding="utf-8")
        issues.extend(_validate_high_risk_terms(path, text))
        issues.extend(_validate_chd_scope_labels(path, text))
        issues.extend(_validate_2024_validation_scope(path, text))
        issues.extend(_validate_claim_strength_marker(path, text))
    return issues


def _validate_high_risk_terms(path: Path, text: str) -> list[ClaimIssue]:
    issues: list[ClaimIssue] = []
    term_groups = [
        ("causal_overclaim", CAUSAL_TERMS),
        ("subprovince_yield_loss_overclaim", SUBPROVINCE_YIELD_LOSS_TERMS),
        ("paddy_weighted_overclaim", PADDY_WEIGHTED_TERMS),
        ("balanced_panel_overclaim", BALANCED_PANEL_TERMS),
        ("treatment_effect_overclaim", TREATMENT_EFFECT_TERMS),
    ]
    for line_number, line in enumerate(text.splitlines(), start=1):
        normalized = line.lower()
        for rule, terms in term_groups:
            matched = [term for term in terms if term.lower() in normalized]
            if matched and not _has_safe_qualifier(line):
                issues.append(
                    ClaimIssue(
                        path=path,
                        line=line_number,
                        rule=rule,
                        message=f"High-risk term must be negated or bounded: {', '.join(matched)}",
                        excerpt=line.strip(),
                    )
                )
    return issues


def _validate_chd_scope_labels(path: Path, text: str) -> list[ClaimIssue]:
    issues: list[ClaimIssue] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not _looks_like_title_or_table_row(line):
            continue
        if not _contains_any(line, CHD_TOKENS):
            continue
        if _contains_any(line, CHD_SCOPE_TERMS):
            continue
        issues.append(
            ClaimIssue(
                path=path,
                line=line_number,
                rule="chd_scope_label_missing",
                message="CHD figure/table/headline labels must state province-average or province-scale scope.",
                excerpt=line.strip(),
            )
        )
    return issues


def _validate_2024_validation_scope(path: Path, text: str) -> list[ClaimIssue]:
    issues: list[ClaimIssue] = []
    for start_line, paragraph in _paragraphs_with_start_lines(text):
        if "2024" not in paragraph:
            continue
        if not _contains_any(paragraph, VALIDATION_2024_TERMS):
            continue
        if _contains_any(paragraph, VALIDATION_2024_QUALIFIERS):
            continue
        issues.append(
            ClaimIssue(
                path=path,
                line=start_line,
                rule="validation_2024_scope_missing",
                message="2024 validation language must be framed as descriptive external consistency or auxiliary evidence.",
                excerpt=_single_line(paragraph),
            )
        )
    return issues


def _validate_claim_strength_marker(path: Path, text: str) -> list[ClaimIssue]:
    if _contains_any(text, IMPACT_SCOPE_TERMS):
        return []
    return [
        ClaimIssue(
            path=path,
            line=1,
            rule="impact_scope_missing",
            message="Report should explicitly state the current conclusion strength as impact assessment / risk identification.",
            excerpt="",
        )
    ]


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    lower = text.lower()
    return any(term.lower() in lower for term in terms)


def _has_safe_qualifier(text: str) -> bool:
    return _contains_any(text, SAFE_NEGATION_TERMS) or _contains_any(text, IMPACT_SCOPE_TERMS)


def _looks_like_title_or_table_row(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("#") or stripped.startswith("| Figure") or stripped.startswith("| Table") or re.match(r"^\|\s*(Figure|Table|\d+)", stripped) is not None


def _paragraphs_with_start_lines(text: str) -> list[tuple[int, str]]:
    paragraphs: list[tuple[int, str]] = []
    start = 1
    current: list[str] = []
    current_start = 1
    for line_number, line in enumerate(text.splitlines(), start=1):
        if line.strip():
            if not current:
                current_start = line_number
            current.append(line)
            continue
        if current:
            paragraphs.append((current_start, "\n".join(current)))
            current = []
        start = line_number + 1
    if current:
        paragraphs.append((current_start, "\n".join(current)))
    elif start == 1 and not text:
        paragraphs.append((1, ""))
    return paragraphs


def _single_line(text: str) -> str:
    return " ".join(part.strip() for part in text.splitlines() if part.strip())


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Validate paper/report claim guardrails.")
    parser.add_argument("paths", nargs="*", type=Path, help="Markdown report files or directories to validate.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def _expand_paths(paths: list[Path]) -> list[Path]:
    if not paths:
        return list(DEFAULT_REPORTS)

    expanded: list[Path] = []
    for path in paths:
        if path.is_dir():
            expanded.extend(sorted(path.glob("*.md")))
        else:
            expanded.append(path)
    return expanded


def main() -> int:
    """Run report claim validation."""

    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")
    paths = _expand_paths(args.paths)
    issues = validate_report_paths(paths)
    if issues:
        for issue in issues:
            location = f"{issue.path}:{issue.line}" if issue.line else str(issue.path)
            logger.error("{} [{}] {} {}", location, issue.rule, issue.message, issue.excerpt)
        return 1
    logger.info("Report claim validation passed for {} file(s).", len(paths))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
