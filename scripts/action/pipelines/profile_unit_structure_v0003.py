#!/usr/bin/env python3
# ============================================================
# filename: profile_unit_structure_v0003.py
# 中文名: 结构单元结构画像脚本
# version: v0003
# layer: execution
# main_layer: action
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ------------------------------------------------------------
# alias: profile_unit_structure
# family: profile_unit_structure
# role: unit_structure_profiler
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/profile_unit_structure_v0003.py
#
# input:
#   - frequent_units_jsonl: output from extract_frequent_units_v0003.py (single file)
#   - normalized_text_units_jsonl: single file (--normalized) or directory (--normalized-dir)
#
# output:
#   - unit_structure_profiles_jsonl (N:1, one file for all candidates)
#   - run_meta_json
#
# depends_on:
#   - python_stdlib_only
#   - extract_frequent_units_v0003
#   - normalize_text_units_v0002
#
# used_by:
#   - validate_unit_boundaries
#   - decide_unit_prominence
#
# notes:
#   - Profiles structural distribution facts for frequent unit_text candidates.
#   - No semantic inference, no importance judgement, no filtering.
#   - Reads full normalized units to compute distribution evidence for candidates.
#   - Two-pass scan: pass 1 = segment_sentence_max, pass 2 = profile aggregation.
#
# v0002 changes:
#   - Supports --normalized-dir for directory input (2171 files)
#   - Added --verbose with per-pass progress reporting
#   - Enhanced run_meta with input_mode, files_processed, per-pass counters
# ============================================================

# ============================================================
# 制度与职责说明
# ------------------------------------------------------------
# 本脚本属于行动层（action）执行型脚本，位于"阶段三：结构画像（profile）"。
#
# 制度边界声明：
# - 本脚本不进行语义分析、主题判断、立场判断、逻辑判断
# - 本脚本不进行变体归并、同义合并、不做去噪过滤
# - 本脚本不进行制度裁决，不输出 ALLOW / DELAY / FREEZE
# - 本脚本不引入 AI、embedding、概率模型
# - 本脚本不消费 adjacency / cooccurrence / statistics 的产物作为过滤条件
# - 本脚本不修改任何系统对象，不覆盖输入文件，不写回上游目录
#
# 画像目标：
# - 仅对 extract_frequent_units 输出的候选 unit_text 集合做结构画像
# - 画像内容是结构事实，例如路径集中度、段落覆盖、位置分布等
# ============================================================

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# ============================================================
# Constants
# ============================================================

DEFAULT_ENCODING = "utf-8"
SCRIPT_NAME = "profile_unit_structure_v0003.py"
SCRIPT_VERSION = "v0003"

REQUIRED_ANCHOR_FIELDS_MIN = (
    "asset_id", "path", "value_index",
    "segment_index", "sentence_index",
    "char_start", "char_end",
)

MAX_WARNING_SAMPLES = 200
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
    with path.open("r", encoding=encoding) as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"__parse_failed__": True}
            except Exception:
                yield {"__parse_failed__": True}


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    return sorted(dir_path.glob("*.jsonl"))


def _get_unit_text(row: Dict[str, Any]) -> Optional[str]:
    v = row.get("unit_text")
    if isinstance(v, str):
        return v
    v2 = row.get("text")
    if isinstance(v2, str):
        return v2
    return None


def _anchor_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not all(k in row for k in REQUIRED_ANCHOR_FIELDS_MIN):
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

    anchor = {
        "asset_id": str(row.get("asset_id")),
        "path": str(row.get("path")),
        "value_index": value_index,
        "segment_index": segment_index,
        "sentence_index": sentence_index,
        "char_start": char_start,
        "char_end": char_end,
    }
    uid = row.get("unit_id")
    if isinstance(uid, str):
        anchor["unit_id"] = uid
    return anchor


def _container_key(anchor: Dict[str, Any]) -> Tuple[str, str, int, int]:
    return (
        str(anchor["asset_id"]),
        str(anchor["path"]),
        int(anchor["value_index"]),
        int(anchor["segment_index"]),
    )


def _bucket_position(
    sentence_index: int,
    sentence_max: int,
    early_ratio: float,
    late_ratio: float,
) -> str:
    if sentence_max <= 0:
        return "early"
    r = float(sentence_index) / float(sentence_max)
    if r <= early_ratio:
        return "early"
    if r >= late_ratio:
        return "late"
    return "middle"


# ============================================================
# Multi-file stream helper
# ============================================================

def _stream_normalized_files(
    input_files: List[Path],
    encoding: str,
    verbose: bool,
    pass_label: str,
) -> Iterable[Tuple[Dict[str, Any], int]]:
    """Yield (row, file_idx) from all input files with progress reporting.
    
    file_idx is 0-based index into input_files.
    """
    for file_idx, fpath in enumerate(input_files):
        for row in _load_jsonl_stream(fpath, encoding):
            yield row, file_idx

        if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            print(
                f"[PROFILE {pass_label}] Progress: {file_idx + 1}/{len(input_files)} files",
                file=sys.stderr,
            )


# ============================================================
# Core: Read candidates
# ============================================================

@dataclass
class CandidateInfo:
    unit_text: str
    occurrence_count: int
    paths: List[str]
    example_unit: Dict[str, Any]


def _read_candidates(
    frequent_path: Path,
    encoding: str,
) -> Tuple[Dict[str, CandidateInfo], Dict[str, Any]]:
    warnings: List[Dict[str, Any]] = []
    total_read = 0
    skipped = 0
    candidates: Dict[str, CandidateInfo] = {}

    for row in _load_jsonl_stream(frequent_path, encoding):
        total_read += 1

        if row.get("__parse_failed__"):
            skipped += 1
            continue

        unit_text = row.get("unit_text")
        occurrence_count = row.get("occurrence_count")
        paths = row.get("paths")
        example_unit = row.get("example_unit")

        if not isinstance(unit_text, str):
            skipped += 1
            continue

        occ = _safe_int(occurrence_count)
        if occ is None or occ <= 0:
            skipped += 1
            continue

        if not isinstance(example_unit, dict):
            skipped += 1
            continue

        paths_clean = [p for p in paths if isinstance(p, str)] if isinstance(paths, list) else []

        candidates[unit_text] = CandidateInfo(
            unit_text=unit_text,
            occurrence_count=occ,
            paths=paths_clean,
            example_unit=example_unit,
        )

    meta = {
        "rows_read": total_read,
        "rows_skipped": skipped,
        "candidates_loaded": len(candidates),
    }
    return candidates, meta


# ============================================================
# Core: Pass 1 - Segment sentence max
# ============================================================

def _compute_segment_sentence_max(
    input_files: List[Path],
    encoding: str,
    verbose: bool,
) -> Tuple[Dict[Tuple[str, str, int, int], int], Dict[str, int]]:
    seg_max: Dict[Tuple[str, str, int, int], int] = {}
    counters = {"rows_read": 0, "rows_skipped": 0, "units_ok": 0}

    for row, file_idx in _stream_normalized_files(input_files, encoding, verbose, "PASS1"):
        counters["rows_read"] += 1

        if row.get("__parse_failed__"):
            counters["rows_skipped"] += 1
            continue

        anchor = _anchor_from_row(row)
        if anchor is None:
            counters["rows_skipped"] += 1
            continue

        counters["units_ok"] += 1
        ck = _container_key(anchor)
        si = int(anchor["sentence_index"])
        prev = seg_max.get(ck)
        if prev is None or si > prev:
            seg_max[ck] = si

    if verbose:
        print(
            f"[PROFILE PASS1] Done: {counters['rows_read']} rows, "
            f"{len(seg_max)} containers, "
            f"{len(input_files)} files",
            file=sys.stderr,
        )

    return seg_max, counters


# ============================================================
# Core: Pass 2 - Profile aggregation
# ============================================================

@dataclass
class ProfileAgg:
    unit_text: str
    occurrence_count_expected: int
    seen_occurrences: int
    segment_keys: Set[Tuple[str, str, int, int]]
    paths_count: Dict[str, int]
    position_profile: Dict[str, int]

    def __init__(self, unit_text: str, occurrence_count_expected: int) -> None:
        self.unit_text = unit_text
        self.occurrence_count_expected = occurrence_count_expected
        self.seen_occurrences = 0
        self.segment_keys = set()
        self.paths_count = {}
        self.position_profile = {"early": 0, "middle": 0, "late": 0}

    def add_occurrence(self, anchor: Dict[str, Any], position_bucket: str) -> None:
        self.seen_occurrences += 1
        self.segment_keys.add(_container_key(anchor))
        p = str(anchor["path"])
        self.paths_count[p] = self.paths_count.get(p, 0) + 1
        if position_bucket not in self.position_profile:
            self.position_profile[position_bucket] = 0
        self.position_profile[position_bucket] += 1

    def finalize(self, dominant_path_threshold: float) -> Dict[str, Any]:
        occurrence = self.seen_occurrences
        segment_coverage = len(self.segment_keys)
        path_diversity = len(self.paths_count)

        dominant_path = None
        dominant_path_ratio = 0.0
        if occurrence > 0 and self.paths_count:
            dominant_path, dominant_cnt = max(self.paths_count.items(), key=lambda kv: (kv[1], kv[0]))
            dominant_path_ratio = float(dominant_cnt) / float(occurrence)

        # Structural role derivation (pure rules, no semantics)
        if path_diversity >= 3 and segment_coverage >= 5:
            structural_role = "stable"
        elif path_diversity == 1 and segment_coverage <= 2:
            structural_role = "incidental"
        else:
            structural_role = "diffuse"

        flags = {
            "high_path_concentration": bool(dominant_path is not None and dominant_path_ratio >= dominant_path_threshold),
            "single_segment_bias": bool(segment_coverage == 1 and occurrence > 0),
        }

        return {
            "unit_text": self.unit_text,
            "occurrence_count": occurrence,
            "segment_coverage": segment_coverage,
            "path_diversity": path_diversity,
            "dominant_path": dominant_path,
            "dominant_path_ratio": dominant_path_ratio,
            "structural_role": structural_role,
            "position_profile": dict(self.position_profile),
            "structural_flags": flags,
        }


def _profile_structure(
    candidates: Dict[str, CandidateInfo],
    input_files: List[Path],
    seg_sentence_max: Dict[Tuple[str, str, int, int], int],
    encoding: str,
    dominant_path_threshold: float,
    early_ratio: float,
    late_ratio: float,
    verbose: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:

    counters = {"rows_read": 0, "rows_skipped": 0, "units_ok": 0, "candidate_hits": 0}

    aggs: Dict[str, ProfileAgg] = {
        ut: ProfileAgg(ut, c.occurrence_count) for ut, c in candidates.items()
    }

    for row, file_idx in _stream_normalized_files(input_files, encoding, verbose, "PASS2"):
        counters["rows_read"] += 1

        if row.get("__parse_failed__"):
            counters["rows_skipped"] += 1
            continue

        unit_text = _get_unit_text(row)
        anchor = _anchor_from_row(row)
        if unit_text is None or anchor is None:
            counters["rows_skipped"] += 1
            continue

        counters["units_ok"] += 1

        if unit_text not in aggs:
            continue

        counters["candidate_hits"] += 1

        ck = _container_key(anchor)
        sentence_max = seg_sentence_max.get(ck, 0)
        si = int(anchor["sentence_index"])
        pos_bucket = _bucket_position(si, sentence_max, early_ratio, late_ratio)
        aggs[unit_text].add_occurrence(anchor=anchor, position_bucket=pos_bucket)

    if verbose:
        print(
            f"[PROFILE PASS2] Done: {counters['rows_read']} rows, "
            f"{counters['candidate_hits']} candidate hits, "
            f"{len(input_files)} files",
            file=sys.stderr,
        )

    # Finalize and build output rows
    rows: List[Dict[str, Any]] = []
    for ut, agg in aggs.items():
        base = agg.finalize(dominant_path_threshold=dominant_path_threshold)
        base["example_unit"] = dict(candidates[ut].example_unit)
        base["candidate_occurrence_count"] = int(candidates[ut].occurrence_count)
        rows.append(base)

    rows_sorted = sorted(rows, key=lambda r: (-int(r.get("occurrence_count", 0)), str(r.get("unit_text", ""))))
    return rows_sorted, counters


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Profile structural distribution facts for frequent unit_text candidates (v0003)."
    )
    p.add_argument("--frequent", required=True, help="Path to frequent_units JSONL (single file).")

    norm_group = p.add_mutually_exclusive_group(required=True)
    norm_group.add_argument("--normalized", default=None, help="Path to a single normalized_text_units JSONL file.")
    norm_group.add_argument("--normalized-dir", default=None, help="Path to directory containing normalized_text_units JSONL files.")

    p.add_argument("--output", default="actioning/pipelines/profile_unit_structure_action/unit_structure_profiles.jsonl", help="Path to unit_structure_profiles JSONL output.")
    p.add_argument("--run-meta", default="actioning/pipelines/profile_unit_structure_action/run_meta.json", help="Path to run_meta JSON output.")

    p.add_argument("--dominant-path-threshold", type=float, default=0.8, help="Dominant path ratio threshold (default: 0.8).")
    p.add_argument("--position-early-ratio", type=float, default=0.33, help="Early position ratio boundary (default: 0.33).")
    p.add_argument("--position-late-ratio", type=float, default=0.67, help="Late position ratio boundary (default: 0.67).")
    p.add_argument("--encoding", default=DEFAULT_ENCODING)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    return p.parse_args(argv)


# ============================================================
# Main
# ============================================================

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    started_at = _utc_iso()
    verbose = bool(args.verbose)

    # Validate thresholds
    for name, val in [
        ("dominant_path_threshold", args.dominant_path_threshold),
        ("position_early_ratio", args.position_early_ratio),
        ("position_late_ratio", args.position_late_ratio),
    ]:
        if not (0.0 <= val <= 1.0):
            print(json.dumps({"status": "error", "error": f"invalid_{name}", name: val}, ensure_ascii=False), file=sys.stderr)
            return 2

    if args.position_early_ratio > args.position_late_ratio:
        print(json.dumps({"status": "error", "error": "position_ratio_conflict"}, ensure_ascii=False), file=sys.stderr)
        return 2

    # Resolve frequent units (single file)
    frequent_path = Path(args.frequent)
    if not frequent_path.exists():
        print(json.dumps({"status": "error", "error": f"frequent not found: {frequent_path}"}, ensure_ascii=False), file=sys.stderr)
        return 1

    # Resolve normalized input files
    if args.normalized:
        norm_path = Path(args.normalized)
        if not norm_path.exists():
            print(json.dumps({"status": "error", "error": f"normalized not found: {norm_path}"}, ensure_ascii=False), file=sys.stderr)
            return 1
        input_files = [norm_path]
        input_mode = "single_file"
        input_source = str(norm_path)
    elif args.normalized_dir:
        norm_dir = Path(args.normalized_dir)
        if not norm_dir.exists():
            print(json.dumps({"status": "error", "error": f"normalized dir not found: {norm_dir}"}, ensure_ascii=False), file=sys.stderr)
            return 1
        input_files = _glob_jsonl_files(norm_dir)
        if not input_files:
            print(json.dumps({"status": "error", "error": f"No .jsonl files in: {norm_dir}"}, ensure_ascii=False), file=sys.stderr)
            return 1
        input_mode = "directory"
        input_source = str(norm_dir)
    else:
        print(json.dumps({"status": "error", "error": "No normalized input specified."}, ensure_ascii=False), file=sys.stderr)
        return 2

    if verbose:
        print(f"[PROFILE] Input mode: {input_mode}, files: {len(input_files)}", file=sys.stderr)

    # 1) Read candidates
    candidates, cand_meta = _read_candidates(frequent_path, args.encoding)
    if verbose:
        print(f"[PROFILE] Candidates loaded: {len(candidates)}", file=sys.stderr)

    # 2) Pass 1: compute segment sentence max
    if verbose:
        print(f"[PROFILE] Pass 1: computing segment_sentence_max...", file=sys.stderr)
    seg_max, pass1_counters = _compute_segment_sentence_max(input_files, args.encoding, verbose)

    # 3) Pass 2: profile aggregation
    if verbose:
        print(f"[PROFILE] Pass 2: profiling candidates...", file=sys.stderr)
    rows, pass2_counters = _profile_structure(
        candidates=candidates,
        input_files=input_files,
        seg_sentence_max=seg_max,
        encoding=args.encoding,
        dominant_path_threshold=float(args.dominant_path_threshold),
        early_ratio=float(args.position_early_ratio),
        late_ratio=float(args.position_late_ratio),
        verbose=verbose,
    )

    # Build run_meta
    meta: Dict[str, Any] = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": _utc_iso(),
        "input_mode": input_mode,
        "input_source": input_source,
        "input_files_count": len(input_files),
        "frequent_units_path": str(frequent_path),
        "policy": {
            "dominant_path_threshold": args.dominant_path_threshold,
            "position_early_ratio": args.position_early_ratio,
            "position_late_ratio": args.position_late_ratio,
            "position_container_level": "segment",
        },
        "candidates_meta": cand_meta,
        "pass1_counters": pass1_counters,
        "pass2_counters": pass2_counters,
        "profiles_count": len(rows),
    }

    if args.dry_run:
        print(json.dumps(meta, ensure_ascii=False, indent=2))
        return 0

    output_path = Path(args.output)
    run_meta_path = Path(args.run_meta)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run_meta_path.parent.mkdir(parents=True, exist_ok=True)

    # Write profiles
    rows_written = 0
    with output_path.open("w", encoding=args.encoding) as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            rows_written += 1

    meta["rows_written"] = rows_written
    meta["output_path"] = str(output_path)
    meta["run_meta_path"] = str(run_meta_path)

    with run_meta_path.open("w", encoding=args.encoding) as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps({
        "status": "ok",
        "version": SCRIPT_VERSION,
        "candidates": len(candidates),
        "profiles_written": rows_written,
        "output": str(output_path),
        "run_meta": str(run_meta_path),
    }, ensure_ascii=False, indent=2))

    return 0


# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
