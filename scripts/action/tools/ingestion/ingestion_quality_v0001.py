# ============================================================
# 文件名: ingestion_quality_v0001.py
# 中文名: 正文质量与来源覆盖检查
# 版本号: v0001
#
# 主层级: action
# 层级: tools / ingestion / ingestion_quality_gate
# 脚本定位: 正文质量与来源覆盖检查的独立执行边界
#
# 职责说明:
# - 正文质量与来源覆盖检查，提供明确输入输出
#
# 本脚本做什么:
# - 执行本模块声明的接入操作并返回真实状态
#
# 本脚本不做什么:
# - 不执行未知代码，不调用收费模型，不写核心数据库
#
# 制度边界声明:
# - 输入原件只读；运行产物仅写显式受管目录，追加发布，不覆盖已有证据
# - 失败明确返回；导入无写入副作用；测试仅使用 temp 下隔离数据
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingestion_quality_v0001
# family: ingestion_quality
# role: ingestion_quality_gate
# version: v0001
# status: active
# entry_point: scripts/action/tools/ingestion/ingestion_quality_v0001.py
# input:
#   - explicit paths and versioned ingestion configuration
# output:
#   - validated results with provenance and classified errors
# depends_on:
#   - Python stdlib
# used_by:
#   - unified_ingestion_pipeline_v0001
# ============================================================

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "ingestion_quality"
SCRIPT_NAME = "ingestion_quality_v0001"
SCRIPT_VERSION = "v0001"
POLICY_VERSION = "verbatim_text_quality_v0002"


# ============================================================
# 数据结构
# ============================================================

class Quality(TypedDict):
    status: str | None
    policy_version: str
    checks: dict
    reasons: list
    unchecked: list


# ============================================================
# 核心业务组件
# ============================================================

def assess(path: Path, extraction: dict) -> Quality:
    report = Quality(status=None, policy_version=POLICY_VERSION, checks={},
                     reasons=[], unchecked=["factual_truth", "visual_layout", "embedded_nontext"])
    if extraction["parse_status"] != "extracted":
        report["reasons"] = [extraction["parse_status"]]
        return report
    try:
        original = path.read_bytes().decode("utf-8-sig")
    except (OSError, UnicodeDecodeError):
        report.update(status="failed", reasons=["independent_decode_failed"])
        return report
    units = extraction["units"]
    matches = bool(units)
    offset = 0
    for unit in units:
        locator = unit.get("source_locator", {})
        text = unit.get("text", "")
        start, end = locator.get("char_start"), locator.get("char_end")
        if (type(start) is not int or type(end) is not int or start != offset
                or end < start or original[start:end] != text):
            matches = False
            break
        offset = end
    controls = any(ord(c) < 32 and c not in "\n\r\t" for c in original)
    report["checks"] = {"exact_source_ranges": matches, "complete_coverage": offset == len(original),
                        "nonempty": bool(original.strip()), "no_control_noise": not controls,
                        "no_replacement_character": "\ufffd" not in original}
    if not matches or controls or "\ufffd" in original or not original.strip():
        report.update(status="failed", reasons=["text_integrity_or_quality_failed"])
    elif offset != len(original):
        report.update(status="partial", reasons=["incomplete_coverage"])
    else:
        report.update(status="passed", reasons=[])
    return report
