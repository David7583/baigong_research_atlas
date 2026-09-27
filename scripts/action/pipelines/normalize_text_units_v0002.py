# ============================================================
# filename: normalize_text_units_v0002.py
# 中文名: 文本单元制度化标准化脚本
# version: v0002
# layer: execution
# main_layer: action
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ------------------------------------------------------------
# alias: normalize_text_units
# family: normalize_text_units
# role: execution
# version: v0002
# status: active
# entry_point: scripts/action/pipelines/normalize_text_units_v0002.py
#
# input:
#   - language_parse_lite_v0003 输出的 jsonl（语言解析单元）
#
# output:
#   - text_units.jsonl（行动层标准文本单元）
#   - run_meta.json
#
# depends_on:
#   - language_parse_lite_v0003
#
# used_by:
#   - analyze_structural_adjacency_v0004
#   - analyze_structural_cooccurrence_v0004
#   - analyze_structural_statistics_v0003
# ============================================================

# ============================================================
# 制度与职责说明
# ------------------------------------------------------------
# 本脚本属于行动层（action）的执行型脚本。
#
# 制度边界声明：
# - 本脚本不进行语义解释
# - 本脚本不进行筛选、裁决或合并
# - 本脚本不清洗、不改写文本内容
# - 本脚本不引入对话或交互结构假设
#
# 本脚本做什么：
# - 将 language_parse_lite 的语言解析产物
#   制度化为行动层可引用的 text_unit 对象
# - 为每个语言单元生成稳定、确定、可重放的 unit_id
# - 统一语言内容字段命名为 unit_text
# - 保留语言结构坐标用于后续结构分析
#
# 本脚本不做什么：
# - 不生成 conversation_id 或 message_id
# - 不推导任何社会或交互上下文
# - 不生成结构关系（层级、邻接、共现）
# - 不引入外部状态
# ============================================================

from __future__ import annotations

# ============================================================
# Imports 区
# ============================================================

import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Set

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 本脚本仅在人工或 flow 明确触发下运行
# - 不在 import 阶段执行任何逻辑
# - 输出结果完全由输入决定，具有确定性
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================

WARNINGS_LIMIT = 20

REQUIRED_INPUT_KEYS_MIN = (
    "asset_id",
    "path",
    "text",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
)

# ============================================================
# 数据结构定义区
# ============================================================

@dataclass
class RunCounters:
    rows_read: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    units_distinct: int = 0
    warnings_count: int = 0

# ============================================================
# 工具函数区（无副作用）
# ============================================================

def utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_jsonl(path: Path, encoding: str) -> Iterator[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding=encoding) as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            yield lineno, json.loads(line)


def stable_hash_hex(parts: List[str], algo: str = "sha1") -> str:
    joined = "|".join(parts)
    h = hashlib.sha1() if algo.lower() == "sha1" else hashlib.sha256()
    h.update(joined.encode("utf-8", errors="strict"))
    return h.hexdigest()


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def extract_required_fields(
    obj: Dict[str, Any]
) -> Optional[Tuple[str, str, str, int, int, int, int, Optional[int]]]:
    try:
        asset_id = str(obj["asset_id"])
        path = str(obj["path"])
        unit_text = str(obj["text"])
        segment_index = safe_int(obj.get("segment_index"))
        sentence_index = safe_int(obj.get("sentence_index"))
        char_start = safe_int(obj.get("char_start"))
        char_end = safe_int(obj.get("char_end"))
        value_index = safe_int(obj.get("value_index"))

        if (
            segment_index is None
            or sentence_index is None
            or char_start is None
            or char_end is None
        ):
            return None

        if not unit_text.strip():
            return None

        return (
            asset_id,
            path,
            unit_text,
            segment_index,
            sentence_index,
            char_start,
            char_end,
            value_index,
        )
    except Exception:
        return None


def derive_unit_id(
    asset_id: str,
    path: str,
    segment_index: int,
    sentence_index: int,
    char_start: int,
    char_end: int,
    value_index: Optional[int],
) -> str:
    parts = [
        asset_id,
        path,
        str(value_index) if value_index is not None else "",
        str(segment_index),
        str(sentence_index),
        str(char_start),
        str(char_end),
    ]
    return stable_hash_hex(parts, algo="sha1")

# ============================================================
# 核心业务逻辑区
# ============================================================

def normalize(
    input_path: Path,
    output_path: Path,
    run_meta_path: Path,
    encoding: str,
    dry_run: bool,
) -> None:
    counters = RunCounters()
    warnings: List[Dict[str, Any]] = []
    unit_ids_seen: Set[str] = set()
    out_rows: List[Dict[str, Any]] = []

    if input_path.exists():
        for lineno, obj in load_jsonl(input_path, encoding):
            counters.rows_read += 1

            missing = [k for k in REQUIRED_INPUT_KEYS_MIN if k not in obj]
            if missing:
                counters.rows_skipped += 1
                if len(warnings) < WARNINGS_LIMIT:
                    warnings.append({
                        "lineno": lineno,
                        "type": "missing_required_fields",
                        "missing_keys": missing,
                    })
                continue

            extracted = extract_required_fields(obj)
            if extracted is None:
                counters.rows_skipped += 1
                continue

            (
                asset_id,
                path,
                unit_text,
                segment_index,
                sentence_index,
                char_start,
                char_end,
                value_index,
            ) = extracted

            unit_id = derive_unit_id(
                asset_id,
                path,
                segment_index,
                sentence_index,
                char_start,
                char_end,
                value_index,
            )

            unit_ids_seen.add(unit_id)

            out_rows.append({
                "unit_id": unit_id,
                "unit_text": unit_text,
                "unit_anchor": "language_parse_lite",
                "segment_index": segment_index,
                "sentence_index": sentence_index,
                "char_start": char_start,
                "char_end": char_end,
                "value_index": value_index,
                "asset_id": asset_id,
                "path": path,
            })

            counters.rows_written += 1

    counters.units_distinct = len(unit_ids_seen)
    counters.warnings_count = len(warnings)

    out_rows_sorted = sorted(
        out_rows,
        key=lambda r: (
            r["asset_id"],
            r["path"],
            r.get("value_index") if r.get("value_index") is not None else -1,
            r["segment_index"],
            r["sentence_index"],
            r["char_start"],
        ),
    )

    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding=encoding) as f:
            for row in out_rows_sorted:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    run_meta_path.parent.mkdir(parents=True, exist_ok=True)
    with run_meta_path.open("w", encoding=encoding) as f:
        json.dump({
            "status": "ok",
            "script_name": "normalize_text_units_v0002.py",
            "script_version": "v0002",
            "generated_at": utc_now_iso_z(),
            "encoding": encoding,
            "input_path": str(input_path),
            "output_path": str(output_path),
            "run_meta_path": str(run_meta_path),
            "counters": asdict(counters),
            "warnings_sample": warnings,
            "known_limitations": [
                "v0002 produces language-only text units without conversation or message context.",
                "v0002 does not attempt to normalize or clean unit_text.",
                "v0002 does not handle unit_id hash collisions.",
            ],
        }, f, ensure_ascii=False, indent=2)

# ============================================================
# CLI / main 接口区
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="actioning/pipelines/normalize_text_units_action/text_units.jsonl")
    parser.add_argument("--run-meta", default="actioning/pipelines/normalize_text_units_action/run_meta.json")
    parser.add_argument("--encoding", default="utf-8")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    normalize(
        input_path=Path(args.input),
        output_path=Path(args.output),
        run_meta_path=Path(args.run_meta),
        encoding=args.encoding,
        dry_run=args.dry_run,
    )

# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    main()
