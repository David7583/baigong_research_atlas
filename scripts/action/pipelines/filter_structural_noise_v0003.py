#!/usr/bin/env python3
# filename: filter_structural_noise_v0003.py
# 中文名: 结构噪声过滤脚本
# version: v0003
# layer: execution
# main_layer: action
# 可更新: True
#
# 职责说明:
# 本脚本用于在行动层中对全局候选结构单元执行"结构噪声"过滤判断。
# 它仅基于形式与结构特征判断 unit 是否为结构残渣，不涉及任何语义理解。
#
# 本脚本做什么:
# - 读取 extract_frequent_units v0003 的全局聚合输出（N:1 单文件）
# - 根据 policy 判定 unit 是否为结构噪声
# - 输出 KEEP / DROP 决策及噪声类型枚举
# - 流式逐行写入，不全量加载到内存
#
# 本脚本不做什么:
# - 不进行语义判断
# - 不进行重要性评估
# - 不修改原始 unit 数据
# - 不参与制度裁决（仅防御性过滤）
# - 不回到实例层读取 per-conversation 文件
#
# v0002 相对 v0001 的变更:
# - 输入从 per-conversation normalize_variants JSONL 改为
#   extract_frequent v0002 的全局聚合单文件
# - 噪声判断字段从 normalized_unit_text 改为 unit_text
#   （extract_frequent 输出的主键即 unit_text）
# - 全量内存 list 改为流式逐行写入
# - 新增 --verbose 进度输出
# - 新增 --dry-run 模式下的 counters 完整输出
# - run_meta 格式对齐 v0002 管线规范
# - v0001 保留不动，SQL 入库管线仍可使用


# ============================================================
# ALIAS_META
# ============================================================
# alias: filter_structural_noise
# family: filter_structural_noise
# role: execution
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/filter_structural_noise_v0003.py
# input:
#   - extract_frequent_units v0003 全局聚合 JSONL（单文件）
# output:
#   - unit_structural_noise_decisions.jsonl（全局单文件）
# depends_on:
#   - extract_frequent_units_v0003.py
# used_by:
#   - validate_unit_boundaries_v0003.py
#   - decide_unit_prominence_v0004.py


# ============================================================
# 制度与职责边界声明
# ============================================================
# - 本脚本进行的是结构噪声判断（防御性）
# - 不进行制度裁决，不决定"是否重要"
# - 所有判断规则必须来自 policy
# - 输出结果为判断记录，不修改系统对象


# ============================================================
# Imports
# ============================================================
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None


# ============================================================
# 常量与全局配置
# ============================================================
SCRIPT_NAME = "filter_structural_noise_v0003.py"
SCRIPT_VERSION = "v0003"
NOISE_KEEP = "KEEP"
NOISE_DROP = "DROP"
PROGRESS_INTERVAL = 50000


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_policy(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load policy YAML. Install: pip install pyyaml")
    with path.open("r", encoding="utf-8") as f:
        policy = yaml.safe_load(f)
    if not isinstance(policy, dict):
        raise ValueError("Policy YAML root must be a mapping object.")
    return policy


def is_numeric_only(text: str) -> bool:
    """纯数字（含空串返回 False）"""
    return len(text) > 0 and text.isdigit()


def is_punctuation_only(text: str, punctuation_chars: str) -> bool:
    """全部由指定标点字符构成"""
    return len(text) > 0 and all(ch in punctuation_chars for ch in text)


def is_symbol_only(text: str, symbol_chars: str) -> bool:
    """全部由指定符号字符构成"""
    return len(text) > 0 and all(ch in symbol_chars for ch in text)


def match_regex_rules(text: str, rules: List[Dict[str, str]]) -> List[str]:
    """匹配正则规则列表，返回命中的 reason 列表"""
    matched = []
    for rule in rules:
        pattern = rule.get("pattern", "")
        reason = rule.get("reason", "regex_match")
        try:
            if re.match(pattern, text):
                matched.append(reason)
        except re.error:
            # 无效正则跳过，不中断管线
            pass
    return matched


# ============================================================
# 核心业务逻辑区
# ============================================================
def evaluate_noise(
    record: Dict[str, Any],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    """
    对单条全局候选记录执行噪声判断。
    v0002 读取 unit_text 字段（extract_frequent 输出的主键）。
    """
    # extract_frequent 输出字段是 unit_text，不是 normalized_unit_text
    unit_text = record.get("unit_text", "")
    reasons: List[str] = []

    # 1. 空值与长度判断
    if not unit_text:
        if not policy.get("allow_empty_after_normalize", False):
            reasons.append("empty_unit_text")

    if len(unit_text) < policy.get("min_normalized_length", 0):
        reasons.append("below_min_length")

    # 2. 字符类别判断
    if policy.get("filter_numeric_only", False) and is_numeric_only(unit_text):
        reasons.append("numeric_only")

    if policy.get("filter_punctuation_only", False) and is_punctuation_only(
        unit_text, policy.get("punctuation_chars", "")
    ):
        reasons.append("pure_punctuation")

    if policy.get("filter_symbol_only", False) and is_symbol_only(
        unit_text, policy.get("symbol_chars", "")
    ):
        reasons.append("symbol_only")

    # 3. 正则规则判断
    if policy.get("enable_regex_rules", False):
        regex_reasons = match_regex_rules(
            unit_text, policy.get("regex_rules", [])
        )
        reasons.extend(regex_reasons)

    decision = NOISE_DROP if reasons else NOISE_KEEP

    # 只追加字段，不修改原始记录的任何既有字段
    output = dict(record)
    output["noise_decision"] = decision
    output["noise_reasons"] = reasons
    output["noise_evidence"] = {
        "unit_text_length": len(unit_text),
    }

    return output


# ============================================================
# CLI / main 接口区
# ============================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Filter structural noise from global frequent unit candidates. "
                    "v0003: reads extract_frequent v0003 N:1 global output."
    )
    p.add_argument(
        "--input", required=True, type=Path,
        help="Input JSONL: extract_frequent v0003 全局聚合输出（单文件）",
    )
    p.add_argument(
        "--output", type=Path,
        default="actioning/pipelines/filter_structural_noise_action/unit_structural_noise_decisions.jsonl",
        help="Output JSONL: 全局噪声判定结果（单文件）",
    )
    p.add_argument(
        "--rules", default="config/action/config/filter_structural_noise_v0001.yml", type=Path,
        help="Policy YAML file",
    )
    p.add_argument(
        "--run-meta", type=Path,
        default="actioning/pipelines/filter_structural_noise_action/run_meta.json",
        help="Run meta JSON output path",
    )
    p.add_argument("--dry-run", action="store_true", help="Dry run: 不写 output 文件")
    p.add_argument("--verbose", action="store_true", help="输出处理进度到 stderr")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_path: Path = args.input
    output_path: Path = args.output
    policy_path: Path = args.rules
    run_meta_path: Path = args.run_meta
    dry_run: bool = args.dry_run
    verbose: bool = args.verbose

    started_at = utc_now_iso()

    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    if not policy_path.exists():
        print(f"[ERROR] Policy file not found: {policy_path}", file=sys.stderr)
        sys.exit(1)

    policy = load_policy(policy_path)
    policy_version = policy.get("policy_version", "unknown")

    counters = {
        "rows_read": 0,
        "rows_keep": 0,
        "rows_drop": 0,
        "rows_written": 0,
    }

    # 按 noise_reason 分类统计
    reason_counts: Dict[str, int] = {}

    output_f = None
    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        output_f = tmp_path.open("w", encoding="utf-8", newline="\n")

    try:
        with input_path.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue

                counters["rows_read"] += 1
                record = json.loads(s)
                evaluated = evaluate_noise(record, policy)

                if evaluated["noise_decision"] == NOISE_DROP:
                    counters["rows_drop"] += 1
                    for reason in evaluated["noise_reasons"]:
                        reason_counts[reason] = reason_counts.get(reason, 0) + 1
                else:
                    counters["rows_keep"] += 1

                if not dry_run and output_f is not None:
                    output_f.write(json.dumps(evaluated, ensure_ascii=False) + "\n")
                    counters["rows_written"] += 1

                if verbose and counters["rows_read"] % PROGRESS_INTERVAL == 0:
                    print(
                        f"[FILTER_NOISE] Progress: {counters['rows_read']} rows, "
                        f"{counters['rows_keep']} keep, {counters['rows_drop']} drop",
                        file=sys.stderr,
                    )

    finally:
        if output_f is not None:
            output_f.close()

    # 原子替换
    if not dry_run:
        os.replace(str(tmp_path), str(output_path))

    if verbose:
        print(
            f"[FILTER_NOISE] Done: {counters['rows_read']} rows read, "
            f"{counters['rows_keep']} keep, {counters['rows_drop']} drop",
            file=sys.stderr,
        )

    # Run meta
    run_meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": utc_now_iso(),
        "policy_version": policy_version,
        "input": str(input_path),
        "output": str(output_path),
        "counters": counters,
        "reason_counts": reason_counts,
        "known_limitations": [
            "v0003 reads extract_frequent v0003 global output; noise judgment is on unit_text (not normalized_unit_text).",
            "v0003 does not access instance-level data; all judgments are form-based on the candidate text.",
            "v0003 does not perform importance or semantic evaluation.",
        ],
    }

    run_meta_path.parent.mkdir(parents=True, exist_ok=True)
    with run_meta_path.open("w", encoding="utf-8") as f:
        json.dump(run_meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(run_meta, ensure_ascii=False, indent=2))


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    main()
