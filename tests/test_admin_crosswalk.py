from pathlib import Path

import pandas as pd

from src.admin_crosswalk import build_admin_crosswalk, identify_admin_code_fields, normalize_admin_name


def test_normalize_admin_name_removes_suffixes() -> None:
    assert normalize_admin_name("长沙市") == "长沙"
    assert normalize_admin_name("西湖区") == "西湖"
    assert normalize_admin_name("广西壮族自治区") == "广西壮族"


def test_identify_admin_code_fields_matches_chinese_columns() -> None:
    mapping = identify_admin_code_fields(["年份", "旧代码", "新代码", "省份", "地级市", "区县", "变更类型"])

    assert mapping["year"] == "年份"
    assert mapping["old_code"] == "旧代码"
    assert mapping["new_code"] == "新代码"
    assert mapping["province"] == "省份"
    assert mapping["prefecture"] == "地级市"
    assert mapping["county"] == "区县"


def test_build_admin_crosswalk_writes_review_outputs(tmp_path: Path) -> None:
    admin_codes = tmp_path / "raw" / "admin_codes"
    processed = tmp_path / "processed"
    reports = tmp_path / "reports"
    admin_codes.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "year": 2022,
                "old_code": "420100",
                "new_code": "420100",
                "province": "湖北省",
                "city": "武汉市",
                "county": "",
                "change_type": "unchanged",
            },
            {
                "year": 2021,
                "old_code": "old",
                "new_code": "",
                "name": "旧县",
                "county": "新区",
            },
        ]
    ).to_csv(admin_codes / "codes.csv", index=False)

    result = build_admin_crosswalk(admin_codes, processed, reports, year_min=2020, year_max=2022)

    rows = pd.read_csv(result.output_path)
    low = pd.read_csv(result.low_confidence_path)
    assert result.status == "ok"
    assert len(rows) == 2
    assert result.report_path.exists()
    assert len(low) == 1
    assert rows.iloc[0]["match_method"] == "exact_admin_code_match"


def test_build_admin_crosswalk_expands_areacodes_result_to_target_boundary(tmp_path: Path) -> None:
    admin_codes = tmp_path / "raw" / "admin_codes"
    processed = tmp_path / "processed"
    reports = tmp_path / "reports"
    admin_codes.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "代码": "110000",
                "一级行政区": "北京市",
                "二级行政区": "",
                "名称": "北京市",
                "级别": "省级",
                "状态": "在用",
                "启用时间": "1981",
                "变更/弃用时间": "",
                "新代码": "",
            },
            {
                "代码": "110103",
                "一级行政区": "北京市",
                "二级行政区": "直辖",
                "名称": "崇文区",
                "级别": "县级",
                "状态": "弃用",
                "启用时间": "1981",
                "变更/弃用时间": "2010",
                "新代码": "110101",
            },
            {
                "代码": "110101",
                "一级行政区": "北京市",
                "二级行政区": "直辖",
                "名称": "东城区",
                "级别": "县级",
                "状态": "在用",
                "启用时间": "1981",
                "变更/弃用时间": "",
                "新代码": "",
            },
        ]
    ).to_csv(admin_codes / "result.csv", index=False)

    result = build_admin_crosswalk(
        admin_codes,
        processed,
        reports,
        year_min=2009,
        year_max=2011,
        target_boundary_year=2022,
    )

    rows = pd.read_csv(result.output_path)
    old_row = rows[(rows["year"] == 2009) & (rows["admin_code_original"] == 110103)].iloc[0]

    assert result.status == "ok"
    assert int(old_row["admin_code_standard"]) == 110101
    assert old_row["match_method"] == "target_boundary_explicit_new_code_chain"
    assert rows["year"].min() == 2009
    assert rows["year"].max() == 2011
