# ============================================================
# 文件名: analyze_structural_statistics_v0003.py
# 中文名: 结构统计分析脚本
# 版本号: v0003
#
# 主层级: action
# 子层级: analysis / structural
# 脚本定位: action 层前置分析执行脚本（端底层）
#
# 职责说明:
# - 本脚本用于对已完成制度化标准化的文本单元进行纯结构统计分析
# - 生成长度、分布、重复率等结构性统计事实
#
# 明确不做的事情:
# - 不进行语义分析
# - 不抽取关键词
# - 不构建关系网络
# - 不进行情感判断或分类
# - 不进入理解登记系统
#
# 制度边界声明:
# - 本脚本不进行任何制度判断
# - 本脚本不修改任何系统对象
# - 本脚本输出结果允许被丢弃、重算、否定
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: analyze_structural_statistics_v0003
# family: understanding_analysis
# role: structural_statistics_generator
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/analyze_structural_statistics_v0003.py
# layer: action
# input:
#   - normalized_text_units_jsonl (from normalize_text_units_v0002.py)
# output:
#   - structural_statistics_summary.json
#   - structural_health_report.json
#   - run_meta.json
# depends_on:
#   - python_stdlib
# used_by:
#   - (none - independent analysis output)

import json
import argparse
import statistics
import datetime
import os
from collections import Counter
from typing import Iterable, Dict, Any, Optional


# ------------------------------
# 启发式常量（非制度阈值，仅用于健康提示）
# ------------------------------

EXTREME_LENGTH_TAIL_RATIO = 10        # P99 / median 倍数提示线（informational）
HIGH_REPETITION_WARNING_RATIO = 0.2   # 实例层重复率提示线（informational）


# ------------------------------
# 基础工具函数
# ------------------------------

def iter_text_units(input_path: str) -> Iterable[Dict[str, Any]]:
    """逐行读取 JSONL 结构单元。单条失败不终止整体流程。"""
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


# ------------------------------
# 统计收集逻辑（未改动）
# ------------------------------

def collect_statistics(units: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    char_lengths = []
    token_counts = []
    empty_count = 0
    near_empty_count = 0
    total_count = 0

    text_counter = Counter()

    for unit in units:
        total_count += 1

        text = unit.get("unit_text", "")
        if not isinstance(text, str):
            text = ""

        char_len = len(text)
        char_lengths.append(char_len)

        if char_len == 0:
            empty_count += 1
        if char_len < 3:
            near_empty_count += 1

        stats = unit.get("stats", {})
        token_count = safe_int(stats.get("token_count"))
        if token_count is not None:
            token_counts.append(token_count)

        if text:
            text_counter[text] += 1

    return {
        "total_units": total_count,
        "char_lengths": char_lengths,
        "token_counts": token_counts,
        "empty_count": empty_count,
        "near_empty_count": near_empty_count,
        "text_counter": text_counter
    }


# ------------------------------
# 数值分布汇总
# ------------------------------

def summarize_numeric_distribution(values: list) -> Dict[str, Any]:
    if not values:
        return {}

    values_sorted = sorted(values)
    n = len(values_sorted)

    p90_idx = min(int(n * 0.9), n - 1)
    p99_idx = min(int(n * 0.99), n - 1)

    return {
        "count": n,
        "mean": statistics.mean(values_sorted),
        "median": statistics.median(values_sorted),
        "p90": values_sorted[p90_idx],
        "p99": values_sorted[p99_idx],
        "max": values_sorted[-1]
    }


# ------------------------------
# 重复率计算（实例层语义明确）
# ------------------------------

def compute_repetition_stats(text_counter: Counter, total_units: int) -> Dict[str, Any]:
    if total_units == 0:
        return {}

    repeated_unit_instances = sum(
        count for count in text_counter.values() if count > 1
    )

    return {
        "repeated_unit_instance_ratio": repeated_unit_instances / total_units
    }


# ------------------------------
# 健康评估（仅结构提示）
# ------------------------------

def evaluate_health(summary: Dict[str, Any]) -> Dict[str, Any]:
    flags = []
    notes = []

    char_stats = summary.get("char_length", {})
    if char_stats:
        p99 = char_stats.get("p99")
        median = char_stats.get("median")
        if p99 and median and p99 > median * EXTREME_LENGTH_TAIL_RATIO:
            flags.append("extreme_length_tail")
            notes.append("P99 char length significantly exceeds median")

    repetition = summary.get("repetition", {})
    if repetition.get("repeated_unit_instance_ratio", 0) > HIGH_REPETITION_WARNING_RATIO:
        flags.append("high_repetition_ratio")
        notes.append("High proportion of repeated text unit instances detected")

    overall = "ok" if not flags else "warning"

    return {
        "overall_health": overall,
        "flags": flags,
        "notes": notes
    }


# ------------------------------
# 输出工具
# ------------------------------

def write_json(path: str, data: Dict[str, Any]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ------------------------------
# 主流程
# ------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Structural statistics analyzer (action layer, v0003)"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input normalized_text_units.jsonl path (from normalize_text_units_v0002.py)"
    )
    parser.add_argument("--output-root", default="actioning/pipelines/analyze_structural_statistics_action", help="Root output directory")
    parser.add_argument("--version", default="v0003", help="Script version")
    parser.add_argument("--run-id", default="auto", help="Run ID or 'auto'")
    args = parser.parse_args()

    if args.run_id == "auto":
        run_id = datetime.datetime.now(datetime.timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    else:
        run_id = args.run_id

    output_dir = os.path.join(args.output_root, args.version, run_id)
    os.makedirs(output_dir, exist_ok=True)

    units = list(iter_text_units(args.input))
    stats_raw = collect_statistics(units)

    summary = {
        "total_units": stats_raw["total_units"],
        "char_length": summarize_numeric_distribution(stats_raw["char_lengths"]),
        "token_count": summarize_numeric_distribution(stats_raw["token_counts"]),
        "empty_unit_ratio": (
            stats_raw["empty_count"] / stats_raw["total_units"]
            if stats_raw["total_units"] else 0
        ),
        "near_empty_unit_ratio": (
            stats_raw["near_empty_count"] / stats_raw["total_units"]
            if stats_raw["total_units"] else 0
        ),
        "repetition": compute_repetition_stats(
            stats_raw["text_counter"],
            stats_raw["total_units"]
        )
    }

    health_report = evaluate_health(summary)

    run_meta = {
        "script": "analyze_structural_statistics_v0003",
        "version": args.version,
        "layer": "action",
        "input": args.input,
        "run_id": run_id,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "record_count": stats_raw["total_units"]
    }

    write_json(os.path.join(output_dir, "structural_statistics_summary.json"), summary)
    write_json(os.path.join(output_dir, "structural_health_report.json"), health_report)
    write_json(os.path.join(output_dir, "run_meta.json"), run_meta)


if __name__ == "__main__":
    main()
