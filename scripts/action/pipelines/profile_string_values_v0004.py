# filename: profile_string_values_v0004.py
# 中文名: 值级字符串观测脚本
# version: v0004
# layer: execution
# main_layer: action
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
#
# v0003 变更说明
# - compile_path_patterns 新增对深度通配符 (**) 的正则表达式支持，将其映射为 `.*`。
# - 解决动态 UUID 导致深层会话节点无法被观测扫描的问题，实现跨层穿透。
#
# v0002 变更说明
# - compute_observations 新增五个观测维度:
#   1. sampling: 采样元信息（sample_count, sample_total_chars）
#   2. unicode_blocks: Unicode 区块字符分布（物理形式观测）
#   3. entropy_by_block: 按主要 Unicode 区块分别计算熵值
#   4. line_structure: 行级物理结构指标
#   5. block_transitions: Unicode 区块切换密度
# - unicode 维度新增 control_ratio_strict（排除换行、回车、制表符的控制字符比例）
# - 原有 length / unicode / symbols / entropy 四个维度完全保留，不做任何修改
# - 新增观测维度均为物理形式层面，不引入语义判断
# - 新增工具函数: classify_unicode_block, compute_block_distribution,
#   compute_entropy_by_block, compute_line_structure, compute_block_transitions


# =========================
# ALIAS_META
# =========================
# alias: profile_string_values_v0004
# family: profile_string_values
# role: value_observation_string_only
# version: v0004
# status: active
# entry_point: scripts/action/pipelines/profile_string_values_v0004.py
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
#   - v0002 adds sampling, unicode_blocks, entropy_by_block, line_structure, block_transitions.
#   - v0002 adds control_ratio_strict to unicode observations.


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
# Unicode 区块范围定义（物理形式分类，非语义分类）
# =========================
# 每个区块以 (start, end, label) 表示
# 仅覆盖 GPT 对话中高频出现的区块，其余归入 "other"
# 区块定义来自 Unicode 标准，此处仅做范围映射，不做语义推断

UNICODE_BLOCK_RANGES: List[Tuple[int, int, str]] = [
    (0x0000, 0x007F, "basic_latin"),
    (0x0080, 0x00FF, "latin_supplement"),
    (0x0100, 0x024F, "latin_extended"),
    (0x0370, 0x03FF, "greek"),
    (0x0400, 0x04FF, "cyrillic"),
    (0x2000, 0x206F, "general_punctuation"),
    (0x2070, 0x209F, "superscripts_subscripts"),
    (0x20A0, 0x20CF, "currency_symbols"),
    (0x2100, 0x214F, "letterlike_symbols"),
    (0x2150, 0x218F, "number_forms"),
    (0x2190, 0x21FF, "arrows"),
    (0x2200, 0x22FF, "mathematical_operators"),
    (0x2300, 0x23FF, "misc_technical"),
    (0x2500, 0x257F, "box_drawing"),
    (0x2580, 0x259F, "block_elements"),
    (0x25A0, 0x25FF, "geometric_shapes"),
    (0x2600, 0x26FF, "misc_symbols"),
    (0x2700, 0x27BF, "dingbats"),
    (0x27C0, 0x27EF, "misc_math_symbols_a"),
    (0x2980, 0x29FF, "misc_math_symbols_b"),
    (0x2A00, 0x2AFF, "supplemental_math_operators"),
    (0x3000, 0x303F, "cjk_symbols_punctuation"),
    (0x3040, 0x309F, "hiragana"),
    (0x30A0, 0x30FF, "katakana"),
    (0x3100, 0x312F, "bopomofo"),
    (0x3400, 0x4DBF, "cjk_extension_a"),
    (0x4E00, 0x9FFF, "cjk_unified"),
    (0xF900, 0xFAFF, "cjk_compatibility"),
    (0xFE30, 0xFE4F, "cjk_compatibility_forms"),
    (0xFF00, 0xFFEF, "halfwidth_fullwidth"),
    (0x1F600, 0x1F64F, "emoticons"),
    (0x1F300, 0x1F5FF, "misc_symbols_pictographs"),
]

# 将区块按 start 排序，用于线性扫描
_BLOCK_SORTED = sorted(UNICODE_BLOCK_RANGES, key=lambda t: t[0])


def classify_unicode_block(cp: int) -> str:
    """
    将单个 codepoint 映射到 Unicode 区块标签。
    纯物理映射，不含语义判断。
    使用线性扫描（区块数量 < 40，无需二分）。
    """
    for start, end, label in _BLOCK_SORTED:
        if start <= cp <= end:
            return label
    return "other"


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
                    if k.lower() in FORBIDDEN_KEYS:
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
            if part == "**":
                segs.append(r".*")
            elif part == "*":
                segs.append(r"[^/]+")
            else:
                segs.append(re.escape(part))
        regex = "^/" + "/".join(segs).replace("//", "/") + "$"
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
# 观测计算 — 基础工具（v0001 保留）
# =========================

def shannon_entropy(text: str, window: int) -> float:
    if not text:
        return 0.0
    if window > 0:
        text = text[:window]
    counts = Counter(text)
    total = len(text)
    return -sum((c / total) * math.log2(c / total) for c in counts.values())


# =========================
# 观测计算 — v0002 新增工具函数
# =========================

def compute_block_distribution(chars: str) -> Dict[str, float]:
    """
    统计字符在各 Unicode 区块中的分布比例。
    纯物理形式观测：只做 codepoint → 区块标签的映射与计数。
    返回: {block_label: ratio} ，仅包含 ratio > 0 的区块。
    """
    if not chars:
        return {}
    counts: Counter = Counter()
    for c in chars:
        block = classify_unicode_block(ord(c))
        counts[block] += 1
    total = len(chars)
    return {k: v / total for k, v in sorted(counts.items())}


def compute_entropy_by_block(
    chars: str, window: int
) -> Dict[str, float]:
    """
    按 Unicode 区块分别计算 Shannon 熵。
    解决混合内容（如中英混杂）导致的聚合熵值失真问题。

    策略：将字符按区块分桶，对每个桶内字符独立计算熵值。
    仅对字符数 >= 10 的桶计算（样本过小无统计意义）。
    返回: {block_label: entropy_value}
    """
    if not chars:
        return {}

    buckets: Dict[str, List[str]] = {}
    for c in chars:
        block = classify_unicode_block(ord(c))
        buckets.setdefault(block, []).append(c)

    result: Dict[str, float] = {}
    for block, block_chars in buckets.items():
        if len(block_chars) < 10:
            continue
        text = "".join(block_chars)
        result[block] = shannon_entropy(text, window)

    return result


def compute_line_structure(samples: List[str]) -> Dict[str, Any]:
    """
    行级物理结构观测。
    不做内容判断，仅统计行的物理形式特征。

    观测指标:
    - total_lines: 总行数
    - avg_line_length: 平均行长（字符数）
    - empty_line_ratio: 空行占比
    - indented_line_ratio: 以空白字符开头的非空行占比（缩进率）
    - paired_symbol_line_ratio: 包含配对符号的行占比
    - short_line_ratio: 长度 <= 5 的非空行占比
    """
    all_lines: List[str] = []
    for s in samples:
        all_lines.extend(s.split("\n"))

    total = len(all_lines)
    if total == 0:
        return {
            "total_lines": 0,
            "avg_line_length": 0.0,
            "empty_line_ratio": 0.0,
            "indented_line_ratio": 0.0,
            "paired_symbol_line_ratio": 0.0,
            "short_line_ratio": 0.0,
        }

    non_empty = [l for l in all_lines if l.strip()]
    non_empty_count = len(non_empty)

    lengths = [len(l) for l in all_lines]

    indented = sum(1 for l in non_empty if l[0] in (" ", "\t"))
    has_paired = sum(
        1 for l in all_lines
        if any(c in PAIRED_SYMBOLS for c in l)
    )
    short = sum(1 for l in non_empty if len(l.strip()) <= 5)

    return {
        "total_lines": total,
        "avg_line_length": sum(lengths) / total,
        "empty_line_ratio": (total - non_empty_count) / total,
        "indented_line_ratio": indented / non_empty_count if non_empty_count else 0.0,
        "paired_symbol_line_ratio": has_paired / total,
        "short_line_ratio": short / non_empty_count if non_empty_count else 0.0,
    }


def compute_block_transitions(chars: str) -> Dict[str, Any]:
    """
    Unicode 区块切换密度观测。
    统计相邻字符跨越不同 Unicode 区块的频率。

    返回:
    - transition_count: 切换次数
    - transition_density: 切换次数 / (总字符数 - 1)
    - top_transitions: 最频繁的切换对及次数（前 10）
    """
    if len(chars) < 2:
        return {
            "transition_count": 0,
            "transition_density": 0.0,
            "top_transitions": [],
        }

    prev_block = classify_unicode_block(ord(chars[0]))
    transition_count = 0
    transition_pairs: Counter = Counter()

    for i in range(1, len(chars)):
        curr_block = classify_unicode_block(ord(chars[i]))
        if curr_block != prev_block:
            transition_count += 1
            pair_key = f"{prev_block}->{curr_block}"
            transition_pairs[pair_key] += 1
        prev_block = curr_block

    density = transition_count / (len(chars) - 1)

    top = [
        {"pair": k, "count": v}
        for k, v in transition_pairs.most_common(10)
    ]

    return {
        "transition_count": transition_count,
        "transition_density": density,
        "top_transitions": top,
    }


# =========================
# 观测计算 — 主函数
# =========================

def compute_observations(samples: List[str], limits: Limits) -> Dict[str, Any]:
    chars = "".join(samples)
    total = len(chars)

    entropy_vals = [shannon_entropy(s, limits.entropy_window_size) for s in samples]

    # ---- 采样元信息（v0002 新增） ----

    obs: Dict[str, Any] = {
        "sampling": {
            "sample_count": len(samples),
            "sample_total_chars": total,
        },
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
            "control_ratio_strict": sum(
                1 for c in chars
                if unicodedata.category(c) == "Cc" and c not in ("\n", "\r", "\t")
            ) / total if total else 0.0,
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

    # ---- v0002 新增四个维度 ----

    # 1. Unicode 区块分布
    obs["unicode_blocks"] = compute_block_distribution(chars)

    # 2. 按区块分别计算熵值
    obs["entropy_by_block"] = compute_entropy_by_block(
        chars, limits.entropy_window_size
    )

    # 3. 行级物理结构
    obs["line_structure"] = compute_line_structure(samples)

    # 4. 区块切换密度
    obs["block_transitions"] = compute_block_transitions(chars)

    _validate_no_forbidden_keys(obs)
    return obs


# =========================
# 主流程
# =========================

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--structure-report", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", default="actioning/pipelines/profile_string_values_action/observations.jsonl")
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

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(json.dumps(
        {"status": "ok", "rows": len(rows), "version": "v0004"},
        ensure_ascii=False,
        indent=2
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())