# filename: profile_string_values_v0001.py
# 中文名: 值级字符串观测脚本
# version: v0001
# layer: execution
# main_layer: understanding
# 可更新: True
#
# 职责说明
# 本脚本在路径与节点类型已知的前提下，对 string 类型值进行物理与形式层面的观测
# 本脚本只输出可复算的观测事实，不输出任何语义判断或解析结论
#
# 禁止事项
# 不判断是否语言、不判断是否代码、不判断是否可解析
# 不进行分词、语言识别、关键词、主题或语义分析
# 不修改、不清洗、不重写任何原文数据资产


# =========================
# ALIAS_META
# =========================
# alias: profile_string_values_v0001
# family: profile_string_values
# role: value_observation_string_only
# version: v0001
# status: active
# entry_point: profile_string_values_v0001.py
# input:
#   - structure_report_json
#   - json_assets
# output:
#   - observations_jsonl
# depends_on:
#   - python_stdlib_only
# used_by:
#   - eligibility_decision_family_planned
# notes:
#   - Observations only. No semantic or eligibility judgment.


from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import random
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# =========================
# 制度护栏
# =========================

FORBIDDEN_KEYS = {
    "language",
    "noise",
    "like",
    "confidence",
    "probability",
    "risk",
    "decision",
    "allow",
    "deny",
    "freeze",
}

PAIRED_SYMBOLS = ("{", "}", "[", "]", "(", ")")
SEPARATORS = (",", ".", ":", ";", "=")


# =========================
# 参数结构
# =========================

@dataclass(frozen=True)
class Limits:
    max_samples_per_path: int
    max_chars_per_sample: int
    max_total_chars: int
    entropy_window_size: int
    random_seed: int


# =========================
# 工具函数
# =========================

def _now_utc() -> str:
    return _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _validate_no_forbidden_keys(obj: Any) -> None:
    stack = [obj]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(k, str):
                    kl = k.lower()
                    for bad in FORBIDDEN_KEYS:
                        if bad in kl:
                            raise ValueError(f"Forbidden key detected: {k}")
                stack.append(v)
        elif isinstance(node, list):
            stack.extend(node)


# =========================
# 结构报告解析（迭代 DFS）
# =========================

def extract_string_paths(structure_report: Any) -> List[str]:
    paths: List[str] = []
    stack = [structure_report]

    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            path = node.get("path") or node.get("path_pattern")
            ntype = node.get("node_type") or node.get("type")
            if isinstance(path, str) and isinstance(ntype, str):
                if ntype.lower() == "string":
                    paths.append(path)
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)

    return sorted(set(paths))


def compile_path_patterns(paths: List[str]) -> List[re.Pattern]:
    compiled: List[re.Pattern] = []
    for p in paths:
        segs = []
        for part in p.strip("/").split("/"):
            if part == "*":
                segs.append(r"[^/]+")
            else:
                segs.append(re.escape(part))
        regex = "^/" + "/".join(segs) + "$"
        compiled.append(re.compile(regex))
    return compiled


# =========================
# JSON 遍历
# =========================

def iter_string_values(obj: Any, base: List[str]) -> Iterable[Tuple[str, str]]:
    if isinstance(obj, str):
        yield "/" + "/".join(base), obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str):
                yield from iter_string_values(v, base + [k])
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_string_values(item, base + ["*"])


# =========================
# 观测计算
# =========================

def shannon_entropy(text: str, window: int) -> float:
    if not text:
        return 0.0
    if window > 0:
        text = text[:window]
    counts = Counter(text)
    total = len(text)
    return -sum((c / total) * math.log2(c / total) for c in counts.values())


def compute_observations(samples: List[str], limits: Limits) -> Dict[str, Any]:
    chars = "".join(samples)
    total = len(chars)

    entropy_vals = [shannon_entropy(s, limits.entropy_window_size) for s in samples]

    obs = {
        "length": {
            "min": min(len(s) for s in samples) if samples else 0,
            "max": max(len(s) for s in samples) if samples else 0,
            "avg": sum(len(s) for s in samples) / len(samples) if samples else 0.0,
        },
        "unicode": {
            "ascii_ratio": sum(ord(c) < 128 for c in chars) / total if total else 0.0,
            "digit_ratio": sum(unicodedata.category(c) == "Nd" for c in chars) / total if total else 0.0,
            "punct_ratio": sum(unicodedata.category(c).startswith("P") for c in chars) / total if total else 0.0,
            "control_ratio": sum(unicodedata.category(c) == "Cc" for c in chars) / total if total else 0.0,
        },
        "symbols": {
            "paired_total": sum(c in PAIRED_SYMBOLS for c in chars),
            "separators_total": sum(c in SEPARATORS for c in chars),
        },
        "entropy": {
            "min": min(entropy_vals) if entropy_vals else 0.0,
            "max": max(entropy_vals) if entropy_vals else 0.0,
            "avg": sum(entropy_vals) / len(entropy_vals) if entropy_vals else 0.0,
        },
    }

    _validate_no_forbidden_keys(obs)
    return obs


# =========================
# 主流程
# =========================

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--structure-report", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-samples-per-path", type=int, default=200)
    parser.add_argument("--max-chars-per-sample", type=int, default=8000)
    parser.add_argument("--max-total-chars", type=int, default=2_000_000)
    parser.add_argument("--entropy-window-size", type=int, default=1024)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args(argv)

    limits = Limits(
        max_samples_per_path=args.max_samples_per_path,
        max_chars_per_sample=args.max_chars_per_sample,
        max_total_chars=args.max_total_chars,
        entropy_window_size=args.entropy_window_size,
        random_seed=args.seed,
    )

    rng = random.Random(limits.random_seed)

    structure = _read_json(Path(args.structure_report))
    allowed_paths = extract_string_paths(structure)
    patterns = compile_path_patterns(allowed_paths)

    data = _read_json(Path(args.data))

    collected: Dict[str, List[str]] = {}

    for path, val in iter_string_values(data, []):
        if any(p.match(path) for p in patterns):
            collected.setdefault(path, []).append(val)

    rows = []
    for path, values in collected.items():
        rng.shuffle(values)
        samples = []
        total_chars = 0
        for v in values:
            v = v[: limits.max_chars_per_sample]
            if total_chars + len(v) > limits.max_total_chars:
                break
            samples.append(v)
            total_chars += len(v)
            if len(samples) >= limits.max_samples_per_path:
                break

        obs = compute_observations(samples, limits)

        row = {
            "asset_id": Path(args.data).stem,
            "path": path,
            "node_type": "string",
            "observations": obs,
        }

        _validate_no_forbidden_keys(row)
        rows.append(row)

    with open(args.output, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(json.dumps(
        {"status": "ok", "rows": len(rows), "version": "v0001"},
        ensure_ascii=False,
        indent=2
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
