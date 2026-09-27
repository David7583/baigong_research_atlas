# ============================================================
# filename: extract_frequent_units_v0003.py
# 中文名: 高频结构单元提取脚本
# version: v0003
# layer: execution
# main_layer: action
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ------------------------------------------------------------
# alias: extract_frequent_units
# family: extract_frequent_units
# role: frequent_unit_candidate_extractor
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/extract_frequent_units_v0003.py
#
# input:
#   - normalized_text_units_jsonl: single file (--input) or directory (--input-dir)
#
# output:
#   - frequent_units_jsonl
#   - run_meta_json
#
# depends_on:
#   - python_stdlib_only
#   - normalize_text_units_v0002
#
# used_by:
#   - profile_unit_structure_v0003.py
#   - decide_unit_prominence_v0004.py (via downstream profiling outputs)
#
# notes:
#   - Extracts frequent unit_text candidates by deterministic frequency thresholding.
#   - No semantic inference, no variant normalization, no noise filtering, no institutional judgement.
# ============================================================

# ============================================================
# 制度与职责说明
# ------------------------------------------------------------
# 本脚本属于行动层（action）执行型脚本，位于“阶段二：显著结构单元提取”。
#
# 制度边界声明：
# - 本脚本不进行语义分析、主题判断、立场判断、逻辑判断
# - 本脚本不进行变体归并、同义合并、去噪过滤（这些属于阶段三脚本职责）
# - 本脚本不进行制度裁决，不输出 ALLOW / DELAY / FREEZE
# - 本脚本不修改任何系统对象，不覆盖输入文件，不写回上游目录
# - 本脚本只从结构单元全集中按频率阈值提取“候选显著结构单元”
#
# 回指能力声明：
# - 本脚本输出中保留 example_unit 的身份锚点字段（asset_id/path/value_index/.../char_start/char_end）
# - 该锚点可用于回指最原始 JSON 资产的具体字段与字符区间（通过上游制度化链路完成）
# ============================================================

from __future__ import annotations

# ============================================================
# Imports 区
# ============================================================

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 仅消费 normalize_text_units_v0002 的制度化输出（normalized_text_units_jsonl）
# - 仅基于 unit_text 的严格等值做聚合，不做任何“看起来像”的合并
# - 仅基于 min_occurrence 做阈值筛选，不做重要性判断
# - 输出为确定性、可重放的新文件，允许被丢弃、重算、否定
# - 不读取 adjacency/cooccurrence/statistics/hierarchy 的任何产物作为条件
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================

DEFAULT_ENCODING = "utf-8"

SCRIPT_NAME = "extract_frequent_units_v0003.py"
SCRIPT_VERSION = "v0003"

PROGRESS_INTERVAL = 100

# normalize_text_units_v0002 的输出通常包含 unit_text 与单位身份锚点字段。
# 为了保持制度稳健性，本脚本对 unit_text 字段名也做最小兼容（unit_text/text）。
REQUIRED_ANCHOR_FIELDS_MIN = (
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
)

MAX_WARNING_SAMPLES = 200


# ============================================================
# 工具函数区（无副作用）
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def load_jsonl_stream(path: Path, encoding: str) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding=encoding) as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    obj["__line_no__"] = line_no
                    yield obj
                else:
                    yield {"__non_dict_row__": True, "__line_no__": line_no}
            except Exception:
                yield {"__json_parse_error__": True, "__line_no__": line_no}


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    """Collect all .jsonl files in a directory, sorted for determinism."""
    return sorted(dir_path.glob("*.jsonl"))


def get_unit_text(row: Dict[str, Any]) -> Optional[str]:
    # 制度化输出应为 unit_text，但保留 text 作为最小兼容入口
    v = row.get("unit_text", None)
    if isinstance(v, str):
        return v
    v2 = row.get("text", None)
    if isinstance(v2, str):
        return v2
    return None


def anchor_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # anchor 字段用于回指，不在此处做语义推断，仅做类型与基本约束检查
    if not all(k in row for k in REQUIRED_ANCHOR_FIELDS_MIN):
        return None

    value_index = safe_int(row.get("value_index"))
    segment_index = safe_int(row.get("segment_index"))
    sentence_index = safe_int(row.get("sentence_index"))
    char_start = safe_int(row.get("char_start"))
    char_end = safe_int(row.get("char_end"))

    if None in (value_index, segment_index, sentence_index, char_start, char_end):
        return None
    if char_end < char_start:
        return None

    anchor = {
        "asset_id": str(row.get("asset_id")),
        "path": str(row.get("path")),
        "value_index": value_index,
        "segment_index": segment_index,
        "sentence_index": sentence_index,
        "char_start": char_start,
        "char_end": char_end,
    }

    # unit_id 若存在，可作为附加锚点，但不作为必需字段
    if "unit_id" in row and isinstance(row.get("unit_id"), str):
        anchor["unit_id"] = row.get("unit_id")

    return anchor


# ============================================================
# 核心业务逻辑区
# ============================================================

@dataclass
class UnitAgg:
    unit_text: str
    occurrence_count: int
    asset_ids: Set[str]
    paths: Set[str]
    total_char_length: int
    example_anchor: Dict[str, Any]

    def add(self, asset_id: str, path: str, char_len: int) -> None:
        self.occurrence_count += 1
        self.asset_ids.add(asset_id)
        self.paths.add(path)
        self.total_char_length += char_len

    def as_output_row(self, max_paths: int) -> Dict[str, Any]:
        paths_sorted = sorted(self.paths)
        if max_paths > 0:
            paths_sorted = paths_sorted[:max_paths]

        avg_len = 0.0
        if self.occurrence_count > 0:
            avg_len = self.total_char_length / float(self.occurrence_count)

        return {
            "unit_text": self.unit_text,
            "occurrence_count": self.occurrence_count,
            "asset_coverage": len(self.asset_ids),
            "paths": paths_sorted,
            "avg_char_length": avg_len,
            "example_unit": dict(self.example_anchor),
        }


def extract_frequent_units(
    input_files: List[Path],
    encoding: str,
    min_occurrence: int,
    max_paths: int,
    max_unique_units_guard: int,
    verbose: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:

    warnings: List[Dict[str, Any]] = []

    total_rows_read = 0
    rows_skipped = 0
    units_ok = 0
    files_processed = 0

    aggs: Dict[str, UnitAgg] = {}

    for file_idx, fpath in enumerate(input_files):
        for row in load_jsonl_stream(fpath, encoding):
            total_rows_read += 1
            line_no = row.get("__line_no__", None)

            if "__json_parse_error__" in row or "__non_dict_row__" in row:
                rows_skipped += 1
                if len(warnings) < MAX_WARNING_SAMPLES:
                    warnings.append({"type": "parse_error", "file": fpath.name, "line_no": line_no})
                continue

            unit_text = get_unit_text(row)
            anchor = anchor_from_row(row)

            if unit_text is None or anchor is None:
                rows_skipped += 1
                if len(warnings) < MAX_WARNING_SAMPLES:
                    warnings.append({"type": "missing_or_invalid_fields", "file": fpath.name, "line_no": line_no})
                continue

            units_ok += 1

            asset_id = str(anchor["asset_id"])
            path = str(anchor["path"])
            char_len = len(unit_text)

            if unit_text not in aggs:
                if max_unique_units_guard > 0 and len(aggs) >= max_unique_units_guard:
                    rows_skipped += 1
                    if len(warnings) < MAX_WARNING_SAMPLES:
                        warnings.append(
                            {
                                "type": "unique_units_guard_triggered",
                                "file": fpath.name,
                                "line_no": line_no,
                                "max_unique_units_guard": max_unique_units_guard,
                            }
                        )
                    continue

                aggs[unit_text] = UnitAgg(
                    unit_text=unit_text,
                    occurrence_count=1,
                    asset_ids={asset_id},
                    paths={path},
                    total_char_length=char_len,
                    example_anchor=anchor,
                )
            else:
                aggs[unit_text].add(asset_id=asset_id, path=path, char_len=char_len)

        files_processed += 1

        if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            print(
                f"[FREQUENT] Progress: {file_idx + 1}/{len(input_files)} files, "
                f"{total_rows_read} rows read, "
                f"{len(aggs)} unique units so far",
                file=sys.stderr,
            )

    if verbose:
        print(
            f"[FREQUENT] Scan done: {files_processed} files, "
            f"{total_rows_read} rows read, "
            f"{units_ok} units ok, "
            f"{len(aggs)} unique unit_texts",
            file=sys.stderr,
        )

    # 频率阈值筛选（唯一筛选规则）
    frequent: List[UnitAgg] = [a for a in aggs.values() if a.occurrence_count >= min_occurrence]

    # 输出排序保持确定性：先按频次降序，再按 unit_text 字典序
    frequent_sorted = sorted(frequent, key=lambda a: (-a.occurrence_count, a.unit_text))

    output_rows = [a.as_output_row(max_paths=max_paths) for a in frequent_sorted]

    if verbose:
        print(
            f"[FREQUENT] Filter: {len(aggs)} unique → {len(output_rows)} frequent (min_occurrence={min_occurrence})",
            file=sys.stderr,
        )

    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "generated_at": utc_now_iso(),
        "policy": {
            "min_occurrence": min_occurrence,
            "max_paths_per_unit": max_paths,
            "max_unique_units_guard": max_unique_units_guard,
        },
        "counters": {
            "rows_read": total_rows_read,
            "units_ok": units_ok,
            "rows_skipped": rows_skipped,
            "files_processed": files_processed,
            "unique_unit_texts": len(aggs),
            "frequent_units": len(output_rows),
            "warnings_count": len(warnings),
        },
        "warnings_sample": warnings,
        "known_limitations": [
            "v0003 uses exact match on unit_text; no variant normalization is performed here.",
            "v0003 does not perform structural noise filtering; this is delegated to stage-3 scripts.",
            "v0003 outputs a candidate list, not an institutional decision or importance ranking.",
        ],
    }

    return output_rows, meta


def write_jsonl(rows: List[Dict[str, Any]], path: Path, encoding: str) -> int:
    rows_written = 0
    with path.open("w", encoding=encoding) as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            rows_written += 1
    return rows_written


def write_json(meta: Dict[str, Any], path: Path, encoding: str) -> None:
    with path.open("w", encoding=encoding) as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


# ============================================================
# CLI / main 接口区
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract frequent unit_text candidates from normalized_text_units JSONL (v0003)."
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input",
        default=None,
        help="Path to a single normalized_text_units JSONL file.",
    )
    input_group.add_argument(
        "--input-dir",
        default=None,
        help="Path to directory containing normalized_text_units JSONL files.",
    )
    parser.add_argument(
        "--output",
        default="actioning/pipelines/extract_frequent_units_action/frequent_units.jsonl",
        help="Path to frequent_units JSONL output.",
    )
    parser.add_argument(
        "--run-meta",
        default="actioning/pipelines/extract_frequent_units_action/run_meta.json",
        help="Path to run_meta JSON output.",
    )
    parser.add_argument(
        "--min-occurrence",
        type=int,
        default=3,
        help="Minimum occurrence count to be considered frequent (default: 3).",
    )
    parser.add_argument(
        "--max-paths",
        type=int,
        default=50,
        help="Max number of distinct paths to keep per unit (default: 50). Use 0 for unlimited.",
    )
    parser.add_argument(
        "--max-unique-units-guard",
        type=int,
        default=0,
        help="Guardrail for max unique unit_text in memory (default: 0 = disabled).",
    )
    parser.add_argument("--encoding", default=DEFAULT_ENCODING)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.min_occurrence <= 0:
        print(
            json.dumps(
                {"status": "error", "error": "invalid_min_occurrence", "min_occurrence": args.min_occurrence},
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 2

    if args.max_paths < 0:
        print(
            json.dumps(
                {"status": "error", "error": "invalid_max_paths", "max_paths": args.max_paths},
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 2

    if args.max_unique_units_guard < 0:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": "invalid_max_unique_units_guard",
                    "max_unique_units_guard": args.max_unique_units_guard,
                },
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 2

    # Resolve input files
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(
                json.dumps({"status": "error", "error": "input_not_found", "input": args.input}, ensure_ascii=False, indent=2),
                file=sys.stderr,
            )
            return 1
        input_files = [input_path]
        input_mode = "single_file"
        input_source = str(input_path)
    elif args.input_dir:
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            print(
                json.dumps({"status": "error", "error": "input_dir_not_found", "input_dir": args.input_dir}, ensure_ascii=False, indent=2),
                file=sys.stderr,
            )
            return 1
        input_files = _glob_jsonl_files(input_dir)
        if not input_files:
            print(
                json.dumps({"status": "error", "error": "no_jsonl_files", "input_dir": args.input_dir}, ensure_ascii=False, indent=2),
                file=sys.stderr,
            )
            return 1
        input_mode = "directory"
        input_source = str(input_dir)
    else:
        print(json.dumps({"status": "error", "error": "no_input_specified"}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2

    verbose = bool(args.verbose)

    if verbose:
        print(f"[FREQUENT] Input mode: {input_mode}, files: {len(input_files)}", file=sys.stderr)

    rows, meta = extract_frequent_units(
        input_files=input_files,
        encoding=args.encoding,
        min_occurrence=int(args.min_occurrence),
        max_paths=int(args.max_paths),
        max_unique_units_guard=int(args.max_unique_units_guard),
        verbose=verbose,
    )

    # Record input source in meta
    meta["input_mode"] = input_mode
    meta["input_source"] = input_source
    meta["input_files_count"] = len(input_files)

    if args.dry_run:
        print(json.dumps(meta, ensure_ascii=False, indent=2))
        return 0

    output_path = Path(args.output)
    run_meta_path = Path(args.run_meta)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run_meta_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = write_jsonl(rows, output_path, args.encoding)
    meta["counters"]["rows_written"] = rows_written
    meta["output_path"] = str(output_path)
    meta["run_meta_path"] = str(run_meta_path)

    write_json(meta, run_meta_path, args.encoding)

    print(
        json.dumps(
            {
                "status": "ok",
                "input_mode": input_mode,
                "input_files": len(input_files),
                "unique_unit_texts": meta["counters"]["unique_unit_texts"],
                "frequent_units": meta["counters"]["frequent_units"],
                "rows_written": rows_written,
                "output": str(output_path),
                "run_meta": str(run_meta_path),
                "version": SCRIPT_VERSION,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
