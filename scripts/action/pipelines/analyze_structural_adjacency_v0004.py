#!/usr/bin/env python3
# ============================================================
# File: analyze_structural_adjacency_v0004.py
# 中文名: 结构邻接分析脚本
# Version: v0004
# Layer: execution
# Main Layer: action
# Updatable: True
#
# Purpose
#
# 基于 normalize_text_units 产出的 normalized_text_units JSONL，
# 将"结构单元在同一物理序列中相邻出现"的事实显性化为邻接关系集合。
#
#
# What it does
#
# 1 读取 normalized_text_units JSONL（单文件或整个目录）
# 2 以 segment 作为邻接容器边界，构造 container_key
# 3 在每个 container 内按稳定规则排序，生成相邻（distance=1）的有向邻接边（forward）
# 4 输出带 unit_id + unit_text 的结构邻接事实（JSONL）与 run_meta（JSON）
#
#
# What it does NOT do
#
# 1 不读取原始 JSON 数据资产
# 2 不消费 cooccurrence / statistics 的输出作为过滤条件
# 3 不进行语义分析、关键词判断、逻辑判断
# 4 不引入 AI、embedding、概率模型
# 5 不跨容器建立顺序，不做 distance > 1 的邻接扩展
# 6 不做对象级聚合（聚合是 builder 的职责）
#
#
# Design decision
#
# v0003 核心变更
#
# 1 UnitIdentity 扩展：新增 unit_id 和 unit_text 字段
#   输出同时包含扁平的 unit_id/unit_text（供 builder 聚合）
#   和完整的 location dict（供审计溯源回指原始文件）
#
# 2 支持目录输入：--input-dir 自动 glob 所有 *.jsonl
#   --input-dir + --output-dir + --run-meta-dir → 1:1 per-file output
#   每个输入文件产出一个 adjacency JSONL + 一个 run_meta JSON
#   另产出一个 global_run_meta.json 汇总全局统计
#   逐文件处理，container_key 含 asset_id 不会跨文件冲突
#
# 3 逐 container 流式写入：
#   每完成一个 container 立即写入输出并释放内存
#   不维护全局 edge_counts dict
#   同一 unit 对在不同 container 出现会输出多行，聚合交给 builder
#
# 4 REQUIRED_FIELDS 新增 unit_id 和 unit_text
#   缺少这两个字段的行记为 skipped
#
#
# 制度声明
#
# 本脚本生成"结构邻接事实"，不生成"统计结论"与"理解判断"
# v0003 中 direction 恒为 "forward"，distance 恒为 1
# 本脚本不会修改任何系统对象，仅生成新的输出文件（可审计、可回放）
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: analyze_structural_adjacency_v0004
# family: analyze_structural_adjacency
# role: structural_adjacency_analysis
# version: v0004
# status: active
# entry_point: scripts/action/pipelines/analyze_structural_adjacency_v0004.py
# input:
#   - normalized_text_units_jsonl (from normalize_text_units_v0002.py)
#   - supports single file (--input) or directory (--input-dir)
# output:
#   - structural_adjacency_jsonl (with unit_id, unit_text, location)
#   - run_meta_json (per-file + global_run_meta in directory mode)
# depends_on:
#   - python_stdlib_only
# used_by:
#   - build_unit_adjacency_graph
# notes:
#   - v0003 outputs unit_id + unit_text for downstream builder aggregation
#   - v0003 writes per-container edges (no global aggregation)
#   - v0003 supports directory input for batch processing
# ============================================================


from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

DEFAULT_ENCODING = "utf-8"
SUPPORTED_CONTAINER_LEVELS = {"segment"}

SCRIPT_NAME = "analyze_structural_adjacency_v0004.py"
SCRIPT_VERSION = "v0004"

DEFAULT_DIRECTION = "forward"
DEFAULT_DISTANCE = 1

REQUIRED_FIELDS = (
    "unit_id",
    "unit_text",
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
)

MAX_WARNING_SAMPLES = 200

# Progress reporting interval (files)
PROGRESS_INTERVAL = 100


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def _load_jsonl_stream(path: Path, encoding: str) -> Iterable[Dict[str, Any]]:
    """Stream JSONL rows from a file. Yields dicts or error markers."""
    with path.open("r", encoding=encoding) as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"__non_dict_row__": True, "__line_no__": line_no}
            except Exception:
                yield {"__json_parse_error__": True, "__line_no__": line_no}


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    """Collect all .jsonl files in a directory, sorted for determinism."""
    return sorted(dir_path.glob("*.jsonl"))


# ============================================================
# UnitIdentity (v0003: extended with unit_id and unit_text)
# ============================================================

@dataclass(frozen=True)
class UnitIdentity:
    unit_id: str
    unit_text: str
    asset_id: str
    path: str
    value_index: int
    segment_index: int
    sentence_index: int
    char_start: int
    char_end: int

    def sort_key(self) -> Tuple[int, int, int]:
        return (self.sentence_index, self.char_start, self.char_end)

    def full_key(self) -> Tuple[Any, ...]:
        return (
            self.asset_id,
            self.path,
            self.value_index,
            self.segment_index,
            self.sentence_index,
            self.char_start,
            self.char_end,
        )

    def location_dict(self) -> Dict[str, Any]:
        """Location coordinates for audit trail / back-reference to source."""
        return {
            "asset_id": self.asset_id,
            "path": self.path,
            "value_index": self.value_index,
            "segment_index": self.segment_index,
            "sentence_index": self.sentence_index,
            "char_start": self.char_start,
            "char_end": self.char_end,
        }

    def container_key(self) -> Tuple[str, str, int, int]:
        """Container boundary for adjacency: same asset+path+value+segment."""
        return (self.asset_id, self.path, self.value_index, self.segment_index)


def _unit_from_row(row: Dict[str, Any]) -> Optional[UnitIdentity]:
    """Parse a normalized text unit row into UnitIdentity.

    Returns None if required fields are missing or invalid.
    """
    if not all(k in row for k in REQUIRED_FIELDS):
        return None

    unit_id = row.get("unit_id")
    unit_text = row.get("unit_text")
    if not isinstance(unit_id, str) or not unit_id.strip():
        return None
    if not isinstance(unit_text, str):
        return None

    value_index = _safe_int(row.get("value_index"))
    segment_index = _safe_int(row.get("segment_index"))
    sentence_index = _safe_int(row.get("sentence_index"))
    char_start = _safe_int(row.get("char_start"))
    char_end = _safe_int(row.get("char_end"))

    if None in (value_index, segment_index, sentence_index, char_start, char_end):
        return None
    if char_end < char_start:
        return None

    return UnitIdentity(
        unit_id=unit_id.strip(),
        unit_text=unit_text,
        asset_id=str(row.get("asset_id", "")),
        path=str(row.get("path", "")),
        value_index=value_index,
        segment_index=segment_index,
        sentence_index=sentence_index,
        char_start=char_start,
        char_end=char_end,
    )


# ============================================================
# Core: Single File Processing (stream per container)
# ============================================================

def _process_single_file(
    input_path: Path,
    out_file,
    encoding: str,
    counters: Dict[str, int],
    warnings: List[Dict[str, Any]],
    verbose: bool = False,
) -> None:
    """Process one JSONL file: group by container, generate adjacent pairs, write immediately.

    Edges are written per-container and memory is released after each container.
    Adjacency is linear (n-1 edges per container of n units), much lighter than cooccurrence.
    """
    # Group units by container within this file
    containers: Dict[Tuple[str, str, int, int], List[UnitIdentity]] = {}

    for row in _load_jsonl_stream(input_path, encoding):
        counters["rows_read"] += 1

        if "__json_parse_error__" in row or "__non_dict_row__" in row:
            counters["rows_skipped"] += 1
            if len(warnings) < MAX_WARNING_SAMPLES:
                warnings.append({
                    "type": row.get("__json_parse_error__", row.get("__non_dict_row__", "unknown")),
                    "file": input_path.name,
                    "line_no": row.get("__line_no__"),
                })
            continue

        unit = _unit_from_row(row)
        if unit is None:
            counters["rows_skipped"] += 1
            if len(warnings) < MAX_WARNING_SAMPLES:
                warnings.append({
                    "type": "invalid_or_missing_fields",
                    "file": input_path.name,
                })
            continue

        counters["units_ok"] += 1
        ckey = unit.container_key()
        containers.setdefault(ckey, []).append(unit)

    # Process each container: generate adjacent pairs, write immediately
    # NOTE: Memory for this function is bounded by the size of a single input file,
    # not by global edge count. With per-conversation JSONL files (~1000 lines each),
    # this is well within acceptable limits.
    file_edges_written = 0

    for ckey, units in containers.items():
        counters["containers_total"] += 1
        n = len(units)

        if n > counters.get("max_units_in_container", 0):
            counters["max_units_in_container"] = n

        if n < 2:
            continue

        counters["containers_with_edges"] += 1

        # Sort for deterministic output
        units_sorted = sorted(units, key=lambda u: (u.sort_key(), u.full_key()))

        # Generate adjacent pairs (linear: n-1 edges)
        for i in range(n - 1):
            u_from = units_sorted[i]
            u_to = units_sorted[i + 1]

            # Defensive: skip self-loop edges caused by duplicate units
            # at the same position (e.g. upstream parsing anomaly)
            if u_from.unit_id == u_to.unit_id:
                counters["self_loops_skipped"] = counters.get("self_loops_skipped", 0) + 1
                continue

            out_file.write(json.dumps({
                "unit_id_from": u_from.unit_id,
                "unit_id_to": u_to.unit_id,
                "unit_text_from": u_from.unit_text,
                "unit_text_to": u_to.unit_text,
                "location_from": u_from.location_dict(),
                "location_to": u_to.location_dict(),
                "container_level": "segment",
                "direction": DEFAULT_DIRECTION,
                "distance": DEFAULT_DISTANCE,
                "adjacency_count": 1,
            }, ensure_ascii=False) + "\n")

            file_edges_written += 1
            counters["edges_written"] += 1

    if verbose and file_edges_written > 0:
        print(
            f"  [{input_path.name}] {counters['units_ok']} units, "
            f"{len(containers)} containers, {file_edges_written} edges",
            file=sys.stderr,
        )

    # Memory released when containers dict goes out of scope


# ============================================================
# Core: Analyze (single file or directory)
# ============================================================

def analyze_adjacency(
    input_path: Optional[Path],
    input_dir: Optional[Path],
    output_path: Path,
    run_meta_path: Path,
    encoding: str,
    dry_run: bool,
    verbose: bool,
) -> Dict[str, Any]:
    """Main analysis logic.

    Processes single file or directory of JSONL files.
    Writes adjacency edges to output JSONL.
    Returns run_meta dict.
    """
    started_at = _utc_iso()

    counters: Dict[str, int] = {
        "rows_read": 0,
        "rows_skipped": 0,
        "units_ok": 0,
        "containers_total": 0,
        "containers_with_edges": 0,
        "edges_written": 0,
        "max_units_in_container": 0,
        "files_processed": 0,
    }
    warnings: List[Dict[str, Any]] = []

    # Resolve input files
    if input_dir is not None:
        if not input_dir.exists():
            return {
                "status": "error",
                "error": f"Input directory not found: {input_dir}",
            }
        input_files = _glob_jsonl_files(input_dir)
        input_mode = "directory"
        if not input_files:
            return {
                "status": "error",
                "error": f"No .jsonl files found in: {input_dir}",
            }
    elif input_path is not None:
        if not input_path.exists():
            return {
                "status": "error",
                "error": f"Input file not found: {input_path}",
            }
        input_files = [input_path]
        input_mode = "single_file"
    else:
        return {
            "status": "error",
            "error": "No input specified. Use --input or --input-dir.",
        }

    if verbose:
        print(
            f"[ADJACENCY] Input mode: {input_mode}, files: {len(input_files)}",
            file=sys.stderr,
        )

    # Dry run: scan and report without writing
    if dry_run:
        for fpath in input_files:
            for row in _load_jsonl_stream(fpath, encoding):
                counters["rows_read"] += 1
                if "__json_parse_error__" not in row and "__non_dict_row__" not in row:
                    unit = _unit_from_row(row)
                    if unit is not None:
                        counters["units_ok"] += 1
                    else:
                        counters["rows_skipped"] += 1
                else:
                    counters["rows_skipped"] += 1
            counters["files_processed"] += 1

        return {
            "status": "dry-run",
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "input_mode": input_mode,
            "started_at": started_at,
            "counters": counters,
        }

    # Normal mode: process and write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run_meta_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding=encoding) as out_file:
        for file_idx, fpath in enumerate(input_files):
            _process_single_file(
                input_path=fpath,
                out_file=out_file,
                encoding=encoding,
                counters=counters,
                warnings=warnings,
                verbose=verbose,
            )
            counters["files_processed"] += 1

            if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
                print(
                    f"[ADJACENCY] Progress: {file_idx + 1}/{len(input_files)} files, "
                    f"{counters['edges_written']} edges written so far",
                    file=sys.stderr,
                )

    if verbose:
        print(
            f"[ADJACENCY] Done: {counters['files_processed']} files, "
            f"{counters['units_ok']} units, "
            f"{counters['edges_written']} edges written",
            file=sys.stderr,
        )

    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "container_level": "segment",
        "input_mode": input_mode,
        "input_path": str(input_dir or input_path),
        "output_path": str(output_path),
        "run_meta_path": str(run_meta_path),
        "started_at": started_at,
        "generated_at": _utc_iso(),
        "counters": counters,
        "warnings_sample": warnings,
        "known_limitations": [
            "v0004 outputs per-container edges without global aggregation; aggregation is builder's responsibility.",
            "v0004 requires unit_id and unit_text in input; rows missing these fields are skipped.",
            "v0004 adjacency_count is always 1 per output row (one edge per container occurrence).",
            "v0004 direction is always 'forward', distance is always 1.",
        ],
        "v0003_changes": [
            "UnitIdentity extended with unit_id and unit_text from input",
            "Output includes flat unit_id/unit_text fields for builder aggregation",
            "Output includes location dicts for audit trail back-reference",
            "Supports --input-dir for batch processing of multiple JSONL files",
            "Per-container streaming write: no global edge_counts dict, bounded memory",
            "Added --verbose progress reporting",
        ],
    }

    # Write run_meta
    with run_meta_path.open("w", encoding=encoding) as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# ============================================================
# Core: Directory Mode (1:1 per-file output)
# ============================================================

def _derive_output_name(input_stem: str, suffix: str, ext: str) -> str:
    """Derive output filename from input file stem."""
    return f"{input_stem}{suffix}{ext}"


def analyze_adjacency_directory(
    input_dir: Path,
    output_dir: Path,
    run_meta_dir: Path,
    encoding: str,
    dry_run: bool,
    verbose: bool,
) -> Dict[str, Any]:
    """Directory mode: process each input JSONL → one output JSONL + one run_meta JSON.

    Output filenames are derived from input filenames:
      {input_stem}_adjacency.jsonl
      {input_stem}_adjacency_run_meta.json
    """
    started_at = _utc_iso()

    # Global counters (aggregated across all files)
    counters: Dict[str, int] = {
        "rows_read": 0,
        "rows_skipped": 0,
        "units_ok": 0,
        "containers_total": 0,
        "containers_with_edges": 0,
        "edges_written": 0,
        "self_loops_skipped": 0,
        "max_units_in_container": 0,
        "files_processed": 0,
        "files_empty_output": 0,
    }
    global_warnings: List[Dict[str, Any]] = []

    if not input_dir.exists():
        return {
            "status": "error",
            "error": f"Input directory not found: {input_dir}",
        }

    input_files = _glob_jsonl_files(input_dir)
    if not input_files:
        return {
            "status": "error",
            "error": f"No .jsonl files found in: {input_dir}",
        }

    if verbose:
        print(
            f"[ADJACENCY] Directory mode: {len(input_files)} files",
            file=sys.stderr,
        )

    # Dry run: scan all files, report global stats
    if dry_run:
        for fpath in input_files:
            for row in _load_jsonl_stream(fpath, encoding):
                counters["rows_read"] += 1
                if "__json_parse_error__" not in row and "__non_dict_row__" not in row:
                    unit = _unit_from_row(row)
                    if unit is not None:
                        counters["units_ok"] += 1
                    else:
                        counters["rows_skipped"] += 1
                else:
                    counters["rows_skipped"] += 1
            counters["files_processed"] += 1

        return {
            "status": "dry-run",
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "input_mode": "directory_per_file",
            "started_at": started_at,
            "counters": counters,
        }

    # Normal mode: per-file processing
    output_dir.mkdir(parents=True, exist_ok=True)
    run_meta_dir.mkdir(parents=True, exist_ok=True)

    for file_idx, fpath in enumerate(input_files):
        stem = fpath.stem
        out_path = output_dir / _derive_output_name(stem, "_adjacency", ".jsonl")
        meta_path = run_meta_dir / _derive_output_name(stem, "_adjacency_run_meta", ".json")

        # Per-file counters
        file_counters: Dict[str, int] = {
            "rows_read": 0,
            "rows_skipped": 0,
            "units_ok": 0,
            "containers_total": 0,
            "containers_with_edges": 0,
            "edges_written": 0,
            "max_units_in_container": 0,
            "files_processed": 0,
        }
        file_warnings: List[Dict[str, Any]] = []

        with out_path.open("w", encoding=encoding) as out_file:
            _process_single_file(
                input_path=fpath,
                out_file=out_file,
                encoding=encoding,
                counters=file_counters,
                warnings=file_warnings,
                verbose=False,
            )
            file_counters["files_processed"] = 1

        # Write per-file run_meta
        file_meta = {
            "status": "ok",
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "container_level": "segment",
            "input_mode": "directory_per_file",
            "input_file": str(fpath),
            "output_file": str(out_path),
            "run_meta_file": str(meta_path),
            "started_at": started_at,
            "generated_at": _utc_iso(),
            "counters": file_counters,
            "warnings_sample": file_warnings,
        }
        with meta_path.open("w", encoding=encoding) as f:
            json.dump(file_meta, f, ensure_ascii=False, indent=2)

        # Accumulate into global counters
        for k in ("rows_read", "rows_skipped", "units_ok",
                   "containers_total", "containers_with_edges", "edges_written",
                   "self_loops_skipped"):
            counters[k] += file_counters.get(k, 0)
        counters["max_units_in_container"] = max(
            counters["max_units_in_container"],
            file_counters.get("max_units_in_container", 0),
        )
        counters["files_processed"] += 1
        if file_counters["edges_written"] == 0:
            counters["files_empty_output"] += 1

        global_warnings.extend(file_warnings)
        if len(global_warnings) > MAX_WARNING_SAMPLES:
            global_warnings = global_warnings[:MAX_WARNING_SAMPLES]

        if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            print(
                f"[ADJACENCY] Progress: {file_idx + 1}/{len(input_files)} files, "
                f"{counters['edges_written']} edges written so far",
                file=sys.stderr,
            )

    if verbose:
        print(
            f"[ADJACENCY] Done: {counters['files_processed']} files, "
            f"{counters['units_ok']} units, "
            f"{counters['edges_written']} edges written, "
            f"{counters['files_empty_output']} files with empty output",
            file=sys.stderr,
        )

    # Write global summary run_meta
    global_meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "container_level": "segment",
        "input_mode": "directory_per_file",
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "run_meta_dir": str(run_meta_dir),
        "started_at": started_at,
        "generated_at": _utc_iso(),
        "counters": counters,
        "warnings_sample": global_warnings,
        "known_limitations": [
            "v0004 outputs per-container edges without global aggregation; aggregation is builder's responsibility.",
            "v0004 requires unit_id and unit_text in input; rows missing these fields are skipped.",
            "v0004 adjacency_count is always 1 per output row (one edge per container occurrence).",
            "v0004 direction is always 'forward', distance is always 1.",
        ],
        "v0003_changes": [
            "UnitIdentity extended with unit_id and unit_text from input",
            "Output includes flat unit_id/unit_text fields for builder aggregation",
            "Output includes location dicts for audit trail back-reference",
            "Supports --input-dir + --output-dir for 1:1 per-file output",
            "Per-container streaming write: no global edge_counts dict, bounded memory",
            "Added --verbose progress reporting",
        ],
    }

    global_meta_path = run_meta_dir / "global_run_meta.json"
    with global_meta_path.open("w", encoding=encoding) as f:
        json.dump(global_meta, f, ensure_ascii=False, indent=2)

    return global_meta


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze structural adjacency at segment level (v0004).",
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

    parser.add_argument("--output", default="actioning/pipelines/analyze_structural_adjacency_action/structural_adjacency.jsonl", help="Output adjacency JSONL path (single-file mode).")
    parser.add_argument("--output-dir", default="actioning/pipelines/analyze_structural_adjacency_action", help="Output directory for per-file adjacency JSONL (directory mode).")
    parser.add_argument("--run-meta", default="actioning/pipelines/analyze_structural_adjacency_action/run_meta.json", help="Run meta JSON output path (single-file mode).")
    parser.add_argument("--run-meta-dir", default="actioning/pipelines/analyze_structural_adjacency_action", help="Output directory for per-file run_meta JSON (directory mode).")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help=f"File encoding. Default: {DEFAULT_ENCODING}")
    parser.add_argument("--container-level", default="segment", help="Container level. v0004 only supports: segment")
    parser.add_argument("--dry-run", action="store_true", help="Scan input and report stats without writing output.")
    parser.add_argument("--verbose", action="store_true", help="Print progress to stderr.")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.container_level not in SUPPORTED_CONTAINER_LEVELS:
        print(json.dumps({
            "status": "error",
            "error": "unsupported_container_level",
            "supported": list(SUPPORTED_CONTAINER_LEVELS),
        }, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2

    input_path = Path(args.input) if args.input else None
    input_dir = Path(args.input_dir) if args.input_dir else None

    # Parameter validation: --input requires --output + --run-meta
    #                       --input-dir requires --output-dir + --run-meta-dir
    if input_path is not None:
        if not args.output or not args.run_meta:
            print(json.dumps({
                "status": "error",
                "error": "Single-file mode (--input) requires --output and --run-meta.",
            }, ensure_ascii=False, indent=2), file=sys.stderr)
            return 2

        meta = analyze_adjacency(
            input_path=input_path,
            input_dir=None,
            output_path=Path(args.output),
            run_meta_path=Path(args.run_meta),
            encoding=args.encoding,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        print(json.dumps({
            "status": meta.get("status", "error"),
            "version": SCRIPT_VERSION,
            "files_processed": meta.get("counters", {}).get("files_processed", 0),
            "units_ok": meta.get("counters", {}).get("units_ok", 0),
            "edges_written": meta.get("counters", {}).get("edges_written", 0),
            "output": str(args.output),
            "run_meta": str(args.run_meta),
        }, ensure_ascii=False, indent=2))

        return 0 if meta.get("status") in ("ok", "dry-run") else 1

    elif input_dir is not None:
        if not args.output_dir or not args.run_meta_dir:
            print(json.dumps({
                "status": "error",
                "error": "Directory mode (--input-dir) requires --output-dir and --run-meta-dir.",
            }, ensure_ascii=False, indent=2), file=sys.stderr)
            return 2

        meta = analyze_adjacency_directory(
            input_dir=input_dir,
            output_dir=Path(args.output_dir),
            run_meta_dir=Path(args.run_meta_dir),
            encoding=args.encoding,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        print(json.dumps({
            "status": meta.get("status", "error"),
            "version": SCRIPT_VERSION,
            "files_processed": meta.get("counters", {}).get("files_processed", 0),
            "units_ok": meta.get("counters", {}).get("units_ok", 0),
            "edges_written": meta.get("counters", {}).get("edges_written", 0),
            "output_dir": str(args.output_dir),
            "run_meta_dir": str(args.run_meta_dir),
        }, ensure_ascii=False, indent=2))

        return 0 if meta.get("status") in ("ok", "dry-run") else 1

    else:
        print(json.dumps({
            "status": "error",
            "error": "No input specified. Use --input or --input-dir.",
        }, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
