# filename: filter_structural_noise_v0001.py
# 中文名: 结构噪声过滤脚本
# version: v0001
# layer: execution
# main_layer: understand
# 可更新: True
#
# 职责说明:
# 本脚本用于在理解层中对结构化文本单元执行“结构噪声”过滤判断。
# 它仅基于形式与结构特征判断 unit 是否为结构残渣，不涉及任何语义理解。
#
# 本脚本做什么:
# - 读取 normalize_unit_variants 的输出 JSONL
# - 根据 policy 判定 unit 是否为结构噪声
# - 输出 KEEP / DROP 决策及噪声类型枚举
#
# 本脚本不做什么:
# - 不进行语义判断
# - 不进行重要性评估
# - 不修改原始 unit 数据
# - 不参与制度裁决（仅防御性过滤）


# ============================================================
# ALIAS_META
# ============================================================
# alias: filter_structural_noise
# family: filter_structural_noise
# role: execution
# version: v0001
# status: active
# entry_point: filter_structural_noise_v0001.py
# input:
#   - unit_variants_normalized.jsonl
# output:
#   - unit_structural_noise_decisions.jsonl
# depends_on:
#   - normalize_unit_variants_v0001.py
# used_by:
#   - validate_unit_boundaries_v0001.py
#   - decide_unit_prominence_v0001.py


# ============================================================
# 制度与职责边界声明
# ============================================================
# - 本脚本进行的是结构噪声判断（防御性）
# - 不进行制度裁决，不决定“是否重要”
# - 所有判断规则必须来自 policy
# - 输出结果为判断记录，不修改系统对象


# ============================================================
# Imports
# ============================================================
import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Any

import yaml


# ============================================================
# 常量与全局配置
# ============================================================
NOISE_KEEP = "KEEP"
NOISE_DROP = "DROP"


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def load_policy(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        policy = yaml.safe_load(f)
    return policy


def is_numeric_only(text: str) -> bool:
    return text.isdigit()


def is_punctuation_only(text: str, punctuation_chars: str) -> bool:
    return all(ch in punctuation_chars for ch in text)


def is_symbol_only(text: str, symbol_chars: str) -> bool:
    return all(ch in symbol_chars for ch in text)


def match_regex_rules(text: str, rules: List[Dict[str, str]]) -> List[str]:
    matched = []
    for rule in rules:
        if re.match(rule["pattern"], text):
            matched.append(rule["reason"])
    return matched


# ============================================================
# 核心业务逻辑区
# ============================================================
def evaluate_noise(
    record: Dict[str, Any],
    policy: Dict[str, Any]
) -> Dict[str, Any]:
    normalized_text = record.get("normalized_unit_text", "")
    reasons: List[str] = []

    # 1. 空值与长度判断
    if not normalized_text:
        if not policy.get("allow_empty_after_normalize", False):
            reasons.append("empty_after_normalize")

    if len(normalized_text) < policy.get("min_normalized_length", 0):
        reasons.append("below_min_length")

    # 2. 字符类别判断
    if policy.get("filter_numeric_only", False) and is_numeric_only(normalized_text):
        reasons.append("numeric_only")

    if policy.get("filter_punctuation_only", False) and is_punctuation_only(
        normalized_text, policy.get("punctuation_chars", "")
    ):
        reasons.append("pure_punctuation")

    if policy.get("filter_symbol_only", False) and is_symbol_only(
        normalized_text, policy.get("symbol_chars", "")
    ):
        reasons.append("symbol_only")

    # 3. 正则规则判断
    if policy.get("enable_regex_rules", False):
        regex_reasons = match_regex_rules(
            normalized_text, policy.get("regex_rules", [])
        )
        reasons.extend(regex_reasons)

    decision = NOISE_DROP if reasons else NOISE_KEEP

    output = dict(record)
    output.update(
        {
            "noise_decision": decision,
            "noise_reasons": reasons,
            "evidence": {
                "normalized_length": len(normalized_text),
            },
        }
    )
    return output


# ============================================================
# CLI / main 接口区
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Filter structural noise units")
    parser.add_argument("--input", required=True, help="Input JSONL file")
    parser.add_argument("--output", required=True, help="Output JSONL file")
    parser.add_argument("--rules", required=True, help="Policy YAML file")
    parser.add_argument("--run-meta", required=True, help="Run meta output path")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    policy_path = Path(args.rules)
    run_meta_path = Path(args.run_meta)

    policy = load_policy(policy_path)

    counters = {
        "rows_read": 0,
        "rows_written": 0,
        "rows_dropped": 0,
    }

    results: List[Dict[str, Any]] = []

    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            counters["rows_read"] += 1
            record = json.loads(line)
            evaluated = evaluate_noise(record, policy)
            results.append(evaluated)

            if evaluated["noise_decision"] == NOISE_DROP:
                counters["rows_dropped"] += 1
            else:
                counters["rows_written"] += 1

    if not args.dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as out:
            for r in results:
                out.write(json.dumps(r, ensure_ascii=False) + "\n")

    run_meta = {
        "status": "ok",
        "script_name": "filter_structural_noise_v0001.py",
        "script_version": "v0001",
        "policy_version": policy.get("policy_version"),
        "input": str(input_path),
        "output": str(output_path),
        "counters": counters,
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
