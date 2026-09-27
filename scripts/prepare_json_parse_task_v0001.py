#!/usr/bin/env python3
# ============================================================
# File: prepare_json_parse_task_v0001.py
# 中文名: JSON 解析任务准备脚本
# Version: v0001
# Layer: execution
# Main Layer: Data
# Updatable: True
#
# Purpose
# 接收“文件类型识别脚本”生成的识别清单（snapshot），在清单内进行选择，
# 创建解析任务工作区，并复制原始 JSON 文件为解析副本，生成 manifest。
#
# Notes (v0001 最终冻结语义)
# - snapshot 必须位于 data_root 内
# - verbose 仅用于描述计算过程，不改变行为
# - dry_run = 只运行、不产出（data_root 必须 0 变化）
# - task_id 冲突视为非法行为，不自动修复
# ============================================================

# ===========================
# ALIAS_META
# ===========================
# alias: prepare_json_parse_task
# family: prepare_json_parse_task
# role: data_task_prepare
# version: v0001
# status: active
# entry_point: prepare_json_parse_task_v0001.py

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


# =========================
# 数据结构
# =========================

@dataclass
class SnapshotRecord:
    relative_path: str
    file_type: str
    size_bytes: Optional[int] = None
    ext: Optional[str] = None
    method: Optional[str] = None
    signals: Optional[Dict[str, Any]] = None


# =========================
# 工具函数
# =========================

def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log(msg: str, verbose: bool) -> None:
    if verbose:
        print(msg)


def _coerce_record(obj: Any) -> SnapshotRecord:
    if not isinstance(obj, dict):
        raise ValueError("Snapshot record must be an object")
    rp = obj.get("relative_path") or obj.get("path")
    ft = obj.get("file_type") or obj.get("kind")
    if not rp or not ft:
        raise ValueError("Snapshot record missing relative_path/path or file_type/kind")
    return SnapshotRecord(
        relative_path=str(rp),
        file_type=str(ft),
        size_bytes=obj.get("size_bytes"),
        ext=obj.get("ext"),
        method=obj.get("method"),
        signals=obj.get("signals"),
    )


def _load_snapshot(snapshot_path: Path) -> List[SnapshotRecord]:
    text = snapshot_path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        raise ValueError("Snapshot is empty")

    records: List[SnapshotRecord] = []

    if text.lstrip().startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("Snapshot JSON must be a list")
        for obj in data:
            records.append(_coerce_record(obj))
        return records

    for i, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception as e:
            raise ValueError(f"Invalid JSONL at line {i}: {e}") from e
        records.append(_coerce_record(obj))
    return records


def _normalize_rel(rel: str) -> str:
    return str(Path(rel))


def _build_index(records: Sequence[SnapshotRecord]) -> Dict[str, int]:
    index: Dict[str, int] = {}
    for idx, r in enumerate(records):
        key = _normalize_rel(r.relative_path)
        if key not in index:
            index[key] = idx
    return index


def _parse_int_list(values: Sequence[str]) -> List[int]:
    out: List[int] = []
    for v in values:
        s = v.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except ValueError as e:
            raise ValueError(f"Invalid index value: {v}") from e
    return out


def _read_select_file(path: Path) -> List[str]:
    if not path.exists():
        raise SystemExit(f"select-file not found: {path}")
    items: List[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if s:
            items.append(s)
    return items


def _select_records(
    records: Sequence[SnapshotRecord],
    select_paths: Sequence[str],
    select_indexes: Sequence[int],
    expected_type: str,
) -> List[SnapshotRecord]:
    index = _build_index(records)
    selected: List[SnapshotRecord] = []
    seen: Set[str] = set()

    for p in select_paths:
        key = _normalize_rel(p)
        if key not in index:
            raise ValueError(f"Selected path not found in snapshot: {p}")
        r = records[index[key]]
        if r.file_type != expected_type:
            raise ValueError(f"Selected path is not type '{expected_type}': {p}")
        if key not in seen:
            selected.append(r)
            seen.add(key)

    for idx in select_indexes:
        if idx < 0 or idx >= len(records):
            raise ValueError(f"Selected index out of range: {idx}")
        r = records[idx]
        key = _normalize_rel(r.relative_path)
        if r.file_type != expected_type:
            raise ValueError(f"Selected index is not type '{expected_type}': {idx}")
        if key not in seen:
            selected.append(r)
            seen.add(key)

    if not selected:
        raise ValueError("No files selected")

    return selected


def _ensure_under_root(root: Path, rel_path: str) -> Path:
    candidate = (root / rel_path).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as e:
        raise ValueError(f"Path escapes data_root: {rel_path}") from e
    return candidate


def _rel_from_root(root: Path, abs_path: Path) -> str:
    return str(abs_path.resolve().relative_to(root.resolve()))


def _copy_with_structure(
    root: Path,
    task_dir: Path,
    record: SnapshotRecord,
    dry_run: bool,
    verbose: bool,
) -> Tuple[str, str]:
    original_rel = _normalize_rel(record.relative_path)
    original_abs = _ensure_under_root(root, original_rel)

    if not original_abs.exists() or not original_abs.is_file():
        raise FileNotFoundError(f"Original file not found: {original_rel}")

    dest_abs = task_dir / Path(original_rel)
    _log(
        f"[{'DRY-RUN' if dry_run else 'EXEC'}] "
        f"{'would copy' if dry_run else 'copying'} {original_rel} -> {dest_abs}",
        verbose,
    )

    if not dry_run:
        dest_abs.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original_abs, dest_abs)

    return original_rel, _rel_from_root(root, dest_abs)


# =========================
# 主流程
# =========================

def main() -> int:
    ap = argparse.ArgumentParser(description="Prepare a JSON parse task from an identified snapshot.")
    ap.add_argument("--data-root", required=True, help="Data root directory.")
    ap.add_argument("--snapshot", required=True, help="Snapshot path (must be under data_root).")
    ap.add_argument("--select", action="append", default=[], help="Select by relative_path.")
    ap.add_argument("--select-index", action="append", default=[], help="Select by snapshot index (0-based).")
    ap.add_argument("--select-file", default="", help="File with relative_path per line.")
    ap.add_argument("--workspace-root", default="parse_workspace", help="Workspace root under data_root.")
    ap.add_argument("--task-id", default="", help="Optional task_id override.")
    ap.add_argument("--expected-type", default="json", help="Expected file_type label.")
    ap.add_argument("--manifest-name", default="manifest.json", help="Manifest filename.")
    ap.add_argument("--dry-run", action="store_true", help="Run without producing any output under data_root.")
    ap.add_argument("--verbose", action="store_true", help="Verbose output (stdout only).")
    args = ap.parse_args()

    data_root = Path(args.data_root).resolve()
    if not data_root.exists() or not data_root.is_dir():
        raise SystemExit(f"Invalid data_root: {data_root}")

    snapshot_path = Path(args.snapshot)
    if not snapshot_path.is_absolute():
        snapshot_path = (data_root / snapshot_path).resolve()

    try:
        snapshot_path.resolve().relative_to(data_root.resolve())
    except ValueError:
        raise SystemExit("Snapshot must be under data_root")

    _log(f"Loading snapshot: {_rel_from_root(data_root, snapshot_path)}", args.verbose)
    records = _load_snapshot(snapshot_path)

    select_paths: List[str] = list(args.select) if args.select else []
    if args.select_file:
        sf = Path(args.select_file)
        if not sf.is_absolute():
            sf = (data_root / sf).resolve()
        select_paths.extend(_read_select_file(sf))

    select_indexes = _parse_int_list(args.select_index)

    selected = _select_records(records, select_paths, select_indexes, args.expected_type)
    _log(f"Selected {len(selected)} file(s)", args.verbose)

    task_id = args.task_id or f"task_{_utc_stamp()}"
    task_dir = data_root / args.workspace_root / args.expected_type / task_id

    _log(
        f"[{'DRY-RUN' if args.dry_run else 'EXEC'}] "
        f"{'would create' if args.dry_run else 'creating'} task_dir: {_rel_from_root(data_root, task_dir)}",
        args.verbose,
    )

    if not args.dry_run:
        try:
            task_dir.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            raise SystemExit(f"task_id already exists: {_rel_from_root(data_root, task_dir)}")

    original_files: List[str] = []
    working_copies: List[str] = []
    mappings: List[Dict[str, str]] = []

    for r in selected:
        o, c = _copy_with_structure(data_root, task_dir, r, args.dry_run, args.verbose)
        original_files.append(o)
        working_copies.append(c)
        mappings.append({"original": o, "copy": c})

    manifest = {
        "task_id": task_id,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "created_by": {"script": "prepare_json_parse_task_v0001.py", "version": "v0001"},
        "source_type": args.expected_type,
        "snapshot_path": _rel_from_root(data_root, snapshot_path),
        "task_dir": _rel_from_root(data_root, task_dir),
        "original_files": original_files,
        "working_copies": working_copies,
        "mappings": mappings,
        "dry_run": bool(args.dry_run),
    }

    _log(
        f"[{'DRY-RUN' if args.dry_run else 'EXEC'}] "
        f"{'would write' if args.dry_run else 'writing'} manifest: {_rel_from_root(data_root, task_dir / args.manifest_name)}",
        args.verbose,
    )

    if not args.dry_run:
        manifest_path = task_dir / args.manifest_name
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "task_id": task_id,
        "task_dir": _rel_from_root(data_root, task_dir),
        "count": len(working_copies),
        "dry_run": bool(args.dry_run),
    }, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
