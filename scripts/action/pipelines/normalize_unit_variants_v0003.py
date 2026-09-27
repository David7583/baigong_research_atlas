#!/usr/bin/env python3
# ============================================================
# filename: normalize_unit_variants_v0003.py
# 中文名: 结构单元形式变体规范化脚本
# version: v0003
# layer: execution
# main_layer: action
# 可更新: True
#
# 职责说明
# 本脚本对结构化文本单元中的 unit_text 执行"形式层"规范化处理，
# 用于消除大小写、空白、标点、Unicode 形态等造成的表面差异。
#
# 本脚本做什么
# - 统一 Unicode 形态（NFKC）
# - 规范全角与半角字符
# - 压缩多余空白
# - 规范标点形式
# - 在实例层生成稳定的 variant_group_id
#
# 本脚本不做什么
# - 不进行语义合并
# - 不进行同义词判断
# - 不进行词形还原或停用词处理
# - 不减少实例行数（输入 N 行 → 输出 N 行）
# - 不进行任何制度裁决
#
# v0002 changes
# 1 支持 --input-dir + --output-dir（1:1 per-file output）
# 2 policy loader 使用 PyYAML，未安装时优雅降级到内置默认值
# 3 --rules 变为 optional，不传则使用内置默认 policy
# 4 加入 archive 机制：输出目录下旧文件自动 revoke 到 archive/
# 5 加入 --verbose 进度报告
# 6 run_meta 补全审计字段（rows_read, rows_skipped, files_processed 等）
# 7 新增 global_run_meta.json 汇总全局统计（目录模式）
#
# 制度与职责边界声明
# - 本脚本属于 execution 型底层脚本
# - 不进行合规判断、制度判断
# - 不修改系统对象，只生成派生字段
# - 规范化规则来自外部 policy 文件（YAML），未提供时使用内置默认值
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================
# alias: normalize_unit_variants_v0003
# family: normalize_unit_variants
# role: form_variant_normalizer
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/normalize_unit_variants_v0003.py
# input:
#   - normalized_text_units_jsonl (single file or directory)
# output:
#   - unit_variants_normalized_jsonl (1:1 per-file in directory mode)
#   - run_meta_json (per-file + global in directory mode)
# depends_on:
#   - pyyaml (for policy loading; graceful fallback to defaults if missing)
#   - normalize_text_units_v0002
# used_by:
#   - filter_structural_noise_v0003.py


from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

DEFAULT_ENCODING = "utf-8"
SCRIPT_NAME = "normalize_unit_variants_v0003.py"
SCRIPT_VERSION = "v0003"

PROGRESS_INTERVAL = 100
MAX_WARNING_SAMPLES = 200

ARCHIVE_DIRNAME = "archive"

DEFAULT_PUNCTUATION_MAP: Dict[str, str] = {
    "\uff0c": ",", "\u3002": ".", "\uff1b": ";", "\uff1a": ":",
    "\uff1f": "?", "\uff01": "!", "\uff08": "(", "\uff09": ")",
    "\u3010": "[", "\u3011": "]", "\u300a": "<", "\u300b": ">",
    "\u201c": '"', "\u201d": '"', "\u2018": "'", "\u2019": "'",
    "\u3001": ",", "\u2014": "-", "\uff0d": "-", "\uff5e": "~",
    "\u2026": "...",
}

WHITESPACE_RE = re.compile(r"\s+")

DEFAULT_POLICY: Dict[str, Any] = {
    "policy_version": "v0001",
    "unicode_nfkc": True,
    "strip": True,
    "collapse_whitespace": True,
    "normalize_punctuation": True,
    "collapse_punctuation": True,
    "collapse_punct_chars": "!?.,;:",
    "lowercase": True,
}


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _blake2b_hex(text: str) -> str:
    h = hashlib.blake2b(text.encode("utf-8"), digest_size=16)
    return h.hexdigest()


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    return sorted(dir_path.glob("*.jsonl"))


def _read_jsonl_stream(path: Path, encoding: str) -> Iterable[Dict[str, Any]]:
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


# ============================================================
# Policy Loader (PyYAML)
# ============================================================

def _load_policy(policy_path: Optional[Path]) -> Dict[str, Any]:
    """Load policy from YAML file using PyYAML. Falls back to defaults if not provided."""
    if policy_path is None or not policy_path.exists():
        return dict(DEFAULT_POLICY)

    try:
        import yaml
    except ImportError:
        print("[WARN] PyYAML not installed, using built-in defaults.", file=sys.stderr)
        return dict(DEFAULT_POLICY)

    try:
        with policy_path.open("r", encoding=DEFAULT_ENCODING) as f:
            loaded = yaml.safe_load(f)
        if not isinstance(loaded, dict):
            return dict(DEFAULT_POLICY)

        # Merge loaded values into defaults (only overwrite known keys)
        policy = dict(DEFAULT_POLICY)
        for k, v in loaded.items():
            if k in policy:
                policy[k] = v
        return policy
    except Exception:
        return dict(DEFAULT_POLICY)


# ============================================================
# Archive
# ============================================================

def _archive_if_exists(file_path: Path, archive_dir: Path, run_id: str) -> Optional[str]:
    """Move existing file to archive with revoked suffix. Returns archived name or None."""
    if not file_path.exists():
        return None
    _safe_mkdir(archive_dir)
    archived_name = f"{file_path.stem}__revoked__{run_id}{file_path.suffix}"
    dst = archive_dir / archived_name
    shutil.move(str(file_path), str(dst))
    return archived_name


# ============================================================
# Core: Normalize text form
# ============================================================

def _normalize_text_form(text: str, policy: Dict[str, Any]) -> Tuple[str, List[str]]:
    applied: List[str] = []
    s = text

    if policy.get("unicode_nfkc", True):
        ns = unicodedata.normalize("NFKC", s)
        if ns != s:
            s = ns
            applied.append("unicode_nfkc")

    if policy.get("strip", True):
        ns = s.strip()
        if ns != s:
            s = ns
            applied.append("strip")

    if policy.get("collapse_whitespace", True):
        ns = WHITESPACE_RE.sub(" ", s)
        if ns != s:
            s = ns
            applied.append("collapse_whitespace")

    if policy.get("normalize_punctuation", True):
        for k, v in DEFAULT_PUNCTUATION_MAP.items():
            s = s.replace(k, v)
        applied.append("normalize_punctuation")

    if policy.get("collapse_punctuation", True):
        chars = re.escape(policy.get("collapse_punct_chars", "!?.,;:"))
        ns = re.sub(rf"([{chars}])\1+", r"\1", s)
        if ns != s:
            s = ns
            applied.append("collapse_punctuation")

    if policy.get("lowercase", True):
        ns = s.lower()
        if ns != s:
            s = ns
            applied.append("lowercase")

    return s, applied


# ============================================================
# Core: Process single file
# ============================================================

def _process_single_file(
    input_path: Path,
    output_path: Path,
    policy: Dict[str, Any],
    encoding: str,
) -> Dict[str, int]:
    """Process one JSONL file, write 1:1 output. Returns counters."""
    counters = {
        "rows_read": 0,
        "rows_written": 0,
        "rows_skipped": 0,
    }

    policy_version = policy.get("policy_version", "v0001")

    _safe_mkdir(output_path.parent)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")

    with tmp.open("w", encoding=encoding) as out_f:
        for row in _read_jsonl_stream(input_path, encoding):
            counters["rows_read"] += 1

            if row.get("__parse_failed__"):
                counters["rows_skipped"] += 1
                continue

            unit_text = row.get("unit_text")
            if not isinstance(unit_text, str):
                counters["rows_skipped"] += 1
                continue

            normalized_text, applied = _normalize_text_form(unit_text, policy)
            variant_group_id = "vgrp_" + _blake2b_hex(
                normalized_text + "\n" + policy_version
            )

            out = dict(row)
            out["original_unit_text"] = unit_text
            out["normalized_unit_text"] = normalized_text
            out["variant_group_id"] = variant_group_id
            out["normalization_applied"] = applied

            out_f.write(json.dumps(out, ensure_ascii=False) + "\n")
            counters["rows_written"] += 1

    os.replace(str(tmp), str(output_path))
    return counters


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Normalize unit_text form variants at instance level (v0003)."
    )

    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--input", default=None, help="Path to a single normalized_text_units JSONL file.")
    input_group.add_argument("--input-dir", default=None, help="Path to directory containing normalized_text_units JSONL files.")

    output_group = p.add_mutually_exclusive_group()
    output_group.add_argument("--output", default="actioning/pipelines/normalize_unit_variants_action/unit_variants_normalized.jsonl", help="Output JSONL path (single-file mode).")
    output_group.add_argument("--output-dir", default="actioning/pipelines/normalize_unit_variants_action", help="Output directory for per-file JSONL (directory mode).")

    p.add_argument("--rules", default="config/action/config/normalize_unit_variants_policy_v0001.yml", help="Policy file path. If omitted, uses built-in defaults.")
    p.add_argument("--run-meta", default="actioning/pipelines/normalize_unit_variants_action/run_meta.json", help="Run meta JSON output path (single-file mode).")
    p.add_argument("--run-meta-dir", default="actioning/pipelines/normalize_unit_variants_action", help="Run meta directory (directory mode).")
    p.add_argument("--encoding", default=DEFAULT_ENCODING)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    return p.parse_args(argv)


# ============================================================
# Main
# ============================================================

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    run_id = _utc_compact()
    started_at = _utc_iso()
    verbose = bool(args.verbose)
    dry_run = bool(args.dry_run)
    encoding = args.encoding

    # Load policy
    policy_path = Path(args.rules) if args.rules else None
    policy = _load_policy(policy_path)

    # ---- Single file mode ----
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(json.dumps({"status": "error", "error": f"Input not found: {input_path}"}, ensure_ascii=False), file=sys.stderr)
            return 1
        if not args.output:
            print(json.dumps({"status": "error", "error": "Single-file mode requires --output."}, ensure_ascii=False), file=sys.stderr)
            return 2

        output_path = Path(args.output)

        if verbose:
            print(f"[VARIANTS] Single file mode: {input_path.name}", file=sys.stderr)

        if not dry_run:
            counters = _process_single_file(input_path, output_path, policy, encoding)
        else:
            # dry run: just count
            counters = {"rows_read": 0, "rows_written": 0, "rows_skipped": 0}
            for row in _read_jsonl_stream(input_path, encoding):
                counters["rows_read"] += 1

        meta = {
            "status": "ok" if not dry_run else "dry-run",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "started_at": started_at,
            "generated_at": _utc_iso(),
            "input_mode": "single_file",
            "input_file": str(input_path),
            "output_file": str(output_path),
            "policy_version": policy.get("policy_version", "v0001"),
            "policy_path": str(policy_path) if policy_path else "",
            "counters": counters,
        }

        run_meta_path = Path(args.run_meta) if args.run_meta else output_path.parent / "run_meta.json"
        _safe_mkdir(run_meta_path.parent)
        with run_meta_path.open("w", encoding=encoding) as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        print(json.dumps({
            "status": meta["status"],
            "version": SCRIPT_VERSION,
            "rows_written": counters.get("rows_written", 0),
            "output": str(output_path),
            "run_meta": str(run_meta_path),
        }, ensure_ascii=False, indent=2))
        return 0

    # ---- Directory mode ----
    elif args.input_dir:
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            print(json.dumps({"status": "error", "error": f"Input dir not found: {input_dir}"}, ensure_ascii=False), file=sys.stderr)
            return 1
        if not args.output_dir:
            print(json.dumps({"status": "error", "error": "Directory mode requires --output-dir."}, ensure_ascii=False), file=sys.stderr)
            return 2

        output_dir = Path(args.output_dir)
        run_meta_dir = Path(args.run_meta_dir) if args.run_meta_dir else output_dir / "_run_meta"
        archive_dir = output_dir / ARCHIVE_DIRNAME

        input_files = _glob_jsonl_files(input_dir)
        if not input_files:
            print(json.dumps({"status": "error", "error": f"No .jsonl files in: {input_dir}"}, ensure_ascii=False), file=sys.stderr)
            return 1

        _safe_mkdir(output_dir)
        _safe_mkdir(run_meta_dir)

        if verbose:
            print(f"[VARIANTS] Directory mode: {len(input_files)} files", file=sys.stderr)

        global_counters: Dict[str, int] = {
            "rows_read": 0,
            "rows_written": 0,
            "rows_skipped": 0,
            "files_processed": 0,
            "files_archived": 0,
        }

        for file_idx, fpath in enumerate(input_files):
            stem = fpath.stem
            out_path = output_dir / f"{stem}_variants.jsonl"
            meta_path = run_meta_dir / f"{stem}_variants_run_meta.json"

            # Archive existing output if present
            archived = None
            if not dry_run:
                archived = _archive_if_exists(out_path, archive_dir, run_id)
                if archived:
                    global_counters["files_archived"] += 1

            if not dry_run:
                file_counters = _process_single_file(fpath, out_path, policy, encoding)
            else:
                file_counters = {"rows_read": 0, "rows_written": 0, "rows_skipped": 0}
                for row in _read_jsonl_stream(fpath, encoding):
                    file_counters["rows_read"] += 1

            # Per-file run_meta
            file_meta = {
                "status": "ok" if not dry_run else "dry-run",
                "script": SCRIPT_NAME,
                "version": SCRIPT_VERSION,
                "input_file": str(fpath),
                "output_file": str(out_path),
                "archived_previous": archived or "",
                "counters": file_counters,
            }
            if not dry_run:
                _safe_mkdir(meta_path.parent)
                with meta_path.open("w", encoding=encoding) as f:
                    json.dump(file_meta, f, ensure_ascii=False, indent=2)

            # Accumulate
            for k in ("rows_read", "rows_written", "rows_skipped"):
                global_counters[k] += file_counters.get(k, 0)
            global_counters["files_processed"] += 1

            if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
                print(
                    f"[VARIANTS] Progress: {file_idx + 1}/{len(input_files)} files, "
                    f"{global_counters['rows_written']} rows written so far",
                    file=sys.stderr,
                )

        if verbose:
            print(
                f"[VARIANTS] Done: {global_counters['files_processed']} files, "
                f"{global_counters['rows_read']} rows read, "
                f"{global_counters['rows_written']} rows written, "
                f"{global_counters['files_archived']} files archived",
                file=sys.stderr,
            )

        # Global run_meta
        global_meta = {
            "status": "ok" if not dry_run else "dry-run",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "started_at": started_at,
            "generated_at": _utc_iso(),
            "input_mode": "directory_per_file",
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "run_meta_dir": str(run_meta_dir),
            "policy_version": policy.get("policy_version", "v0001"),
            "policy_path": str(policy_path) if policy_path else "",
            "input_files_count": len(input_files),
            "counters": global_counters,
        }

        global_meta_path = run_meta_dir / "global_run_meta.json"
        if not dry_run:
            with global_meta_path.open("w", encoding=encoding) as f:
                json.dump(global_meta, f, ensure_ascii=False, indent=2)

        print(json.dumps({
            "status": global_meta["status"],
            "version": SCRIPT_VERSION,
            "files_processed": global_counters["files_processed"],
            "rows_written": global_counters["rows_written"],
            "files_archived": global_counters["files_archived"],
            "output_dir": str(output_dir),
            "run_meta": str(global_meta_path),
        }, ensure_ascii=False, indent=2))
        return 0

    else:
        print(json.dumps({"status": "error", "error": "No input specified."}, ensure_ascii=False), file=sys.stderr)
        return 2


# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
