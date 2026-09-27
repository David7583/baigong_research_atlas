#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: coarse_slice_conversations_v0003.py
# 中文名: 规范对话数据集粗切片脚本
# 版本号: v0003
#
# 主层级: data
# 层级: scripts / data_preparation / slicing
# 脚本定位: 将 canonical_conversation_ingress_v0001 按对话粒度保真切片
#
# 职责说明:
# - 从规范载荷的 conversations 数组生成逐对话切片和清单
# - 保留数据集、信封、对话身份和原始序号追溯
#
# 本脚本做什么:
# - 使用 Python 标准库逐对话原子生成 JSON-per-conversation 或 JSONL 切片
# - 对话对象原样放入切片信封，不展平节点图
#
# 本脚本不做什么:
# - 不依赖 messages、mapping 或 Provider 字段，不修改对话内容
# - 不写正式数据库，不执行语言解析或标注
#
# 制度边界声明:
# - 输出数必须与规范 conversations 数守恒，空对话也必须保留
# - dry-run 校验完整输入和输出计划，但不建目录、切片或清单
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: coarse_slice_conversations_v0003
# family: coarse_slice_conversations
# role: canonical_conversation_coarse_slicer
# version: v0003
# status: experimental
# entry_point: scripts/coarse_slice_conversations_v0003.py
# input:
#   - canonical_conversation_ingress_v0001 JSON under a parse workspace task
# output:
#   - conversation-level JSON slices and slice manifest
# depends_on:
#   - Python standard library
#   - scripts/json_stream_v0001.py
# used_by:
#   - data_discovery_parse_preparation_pipeline_v0003
# ============================================================

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "coarse_slice_conversations"
SCRIPT_NAME = "coarse_slice_conversations_v0003.py"
SCRIPT_VERSION = "v0003"
CANONICAL_SCHEMA_VERSION = "canonical_conversation_ingress_v0001"
DEFAULT_OUT_SUBDIR = "slices"
DEFAULT_MODE = "jsonl"
SAFE_ID = re.compile(r"[^A-Za-z0-9._-]+")


# ============================================================
# 异常类型
# ============================================================

class CanonicalSliceError(RuntimeError):
    """Raised when canonical conversations cannot be sliced safely."""


# ============================================================
# 数据结构
# ============================================================

# Slice records remain JSON mappings compatible with the existing importer.


# ============================================================
# 工具函数区
# ============================================================

def _utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_canonical(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise CanonicalSliceError(f"canonical input is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanonicalSliceError(f"canonical input is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != CANONICAL_SCHEMA_VERSION:
        raise CanonicalSliceError(f"input schema_version must be {CANONICAL_SCHEMA_VERSION}")
    conversations = payload.get("conversations")
    if not isinstance(conversations, list) or not conversations:
        raise CanonicalSliceError("canonical conversations must be a non-empty list")
    return payload


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding=DEFAULT_ENCODING, newline="\n") as handle:
            handle.write(json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary_name).replace(path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def _safe_id(value: str) -> str:
    rendered = SAFE_ID.sub("_", value).strip("._-")
    return rendered[:48] or "unknown"


# ============================================================
# 默认映射
# ============================================================

# Canonical slicing uses only the contract fields declared above.


# ============================================================
# 核心切片组件
# ============================================================

def run_slice(
    *,
    task_dir: Path,
    input_file: Path,
    out_subdir: Path,
    mode: str,
    start_index: int,
    limit: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    task_root = task_dir.resolve()
    input_path = (task_root / input_file).resolve()
    output_dir = (task_root / out_subdir).resolve()
    try:
        input_path.relative_to(task_root)
        output_dir.relative_to(task_root)
    except ValueError as exc:
        raise CanonicalSliceError("input and output must remain inside task-dir") from exc
    canonical = _load_canonical(input_path)
    conversations = canonical["conversations"]
    if start_index < 0 or start_index > len(conversations):
        raise CanonicalSliceError("start-index is outside the conversation range")
    selected = conversations[start_index:]
    if limit is not None:
        if limit < 1:
            raise CanonicalSliceError("limit must be at least 1")
        selected = selected[:limit]
    if not selected:
        raise CanonicalSliceError("slice selection is empty")
    records: List[Dict[str, Any]] = []
    for offset, conversation in enumerate(selected):
        index = start_index + offset
        if not isinstance(conversation, dict):
            raise CanonicalSliceError(f"conversation {index} must be an object")
        conversation_id = conversation.get("conversation_id")
        if not isinstance(conversation_id, str) or not conversation_id:
            raise CanonicalSliceError(f"conversation {index} has no stable conversation_id")
        records.append({
            "_meta": {
                "schema_version": "canonical_conversation_slice_v0001",
                "source_file": input_path.name,
                "dataset_id": canonical["dataset_id"],
                "source_envelope_id": canonical["source_envelope_id"],
                "conversation_index": index,
                "conversation_id": conversation_id,
            },
            "conversation": conversation,
        })
    if dry_run:
        return {
            "status": "dry_run",
            "task_dir": str(task_root),
            "input_file": input_file.as_posix(),
            "output_subdir": out_subdir.as_posix(),
            "mode": mode,
            "written": len(records),
        }
    if output_dir.exists() and any(output_dir.iterdir()):
        raise CanonicalSliceError(f"output directory must be absent or empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    if mode == "jsonl":
        text = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n" for row in records)
        (output_dir / "conversations.jsonl").write_text(text, encoding=DEFAULT_ENCODING, newline="\n")
    else:
        for row in records:
            index = int(row["_meta"]["conversation_index"])
            filename = f"conv_{index:06d}.json"
            _atomic_write_json(output_dir / filename, row)
    manifest = {
        "schema_version": "canonical_conversation_slice_manifest_v0001",
        "source_file": input_path.name,
        "dataset_id": canonical["dataset_id"],
        "source_envelope_id": canonical["source_envelope_id"],
        "slice_count": len(records),
        "mode": mode,
        "sliced_at": _utc_now_z(),
        "sliced_by": SCRIPT_NAME,
        "task_dir": str(task_root),
        "output_subdir": out_subdir.as_posix(),
    }
    _atomic_write_json(output_dir / "slice_manifest.json", manifest)
    return {
        "status": "completed",
        "task_dir": str(task_root),
        "input_file": input_file.as_posix(),
        "output_subdir": out_subdir.as_posix(),
        "mode": mode,
        "written": len(records),
    }


def _load_stream_module() -> Any:
    for parent in Path(__file__).resolve().parents:
        candidates = [parent / "scripts" / "json_stream_v0001.py"]
        if parent.name == "scripts":
            candidates.append(parent / "json_stream_v0001.py")
        for candidate in candidates:
            if not candidate.is_file():
                continue
            spec = importlib.util.spec_from_file_location("json_stream_slicer_runtime_v0001", candidate)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            return module
    raise CanonicalSliceError("json_stream_v0001.py cannot be located")


def run_slice_streaming(
    *,
    task_dir: Path,
    input_file: Path,
    out_subdir: Path,
    mode: str,
    start_index: int,
    limit: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    task_root = task_dir.resolve()
    input_path = (task_root / input_file).resolve()
    output_dir = (task_root / out_subdir).resolve()
    try:
        input_path.relative_to(task_root)
        output_dir.relative_to(task_root)
    except ValueError as exc:
        raise CanonicalSliceError("input and output must remain inside task-dir") from exc
    stream_module = _load_stream_module()
    inspection = stream_module.inspect_object_array(input_path, "conversations")
    metadata = inspection["metadata"]
    if metadata.get("schema_version") != CANONICAL_SCHEMA_VERSION:
        raise CanonicalSliceError(f"input schema_version must be {CANONICAL_SCHEMA_VERSION}")
    dataset_id = metadata.get("dataset_id")
    source_envelope_id = metadata.get("source_envelope_id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise CanonicalSliceError("canonical dataset_id is missing")
    if not isinstance(source_envelope_id, str) or not source_envelope_id:
        raise CanonicalSliceError("canonical source_envelope_id is missing")
    conversation_count = int(inspection["count"])
    if start_index < 0 or start_index > conversation_count:
        raise CanonicalSliceError("start-index is outside the conversation range")
    if limit is not None and limit < 1:
        raise CanonicalSliceError("limit must be at least 1")
    selected_count = conversation_count - start_index
    if limit is not None:
        selected_count = min(selected_count, limit)
    if selected_count < 1:
        raise CanonicalSliceError("slice selection is empty")
    if dry_run:
        return {
            "status": "dry_run",
            "task_dir": str(task_root),
            "input_file": input_file.as_posix(),
            "output_subdir": out_subdir.as_posix(),
            "mode": mode,
            "written": selected_count,
            "streaming": True,
        }
    if output_dir.exists() and any(output_dir.iterdir()):
        raise CanonicalSliceError(f"output directory must be absent or empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_temporary = output_dir / ".conversations.jsonl.tmp"
    jsonl_stream: Any = None
    written = 0
    try:
        if mode == "jsonl":
            jsonl_stream = jsonl_temporary.open("x", encoding=DEFAULT_ENCODING, newline="\n")
        for index, conversation in stream_module.iter_object_array(input_path, "conversations"):
            if index < start_index:
                continue
            if written >= selected_count:
                break
            if not isinstance(conversation, dict):
                raise CanonicalSliceError(f"conversation {index} must be an object")
            conversation_id = conversation.get("conversation_id")
            if not isinstance(conversation_id, str) or not conversation_id:
                raise CanonicalSliceError(f"conversation {index} has no stable conversation_id")
            row = {
                "_meta": {
                    "schema_version": "canonical_conversation_slice_v0001",
                    "source_file": input_path.name,
                    "dataset_id": dataset_id,
                    "source_envelope_id": source_envelope_id,
                    "conversation_index": index,
                    "conversation_id": conversation_id,
                },
                "conversation": conversation,
            }
            if jsonl_stream is not None:
                jsonl_stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
                jsonl_stream.write("\n")
            else:
                _atomic_write_json(output_dir / f"conv_{index:06d}.json", row)
            written += 1
        if jsonl_stream is not None:
            jsonl_stream.flush()
            os.fsync(jsonl_stream.fileno())
            jsonl_stream.close()
            jsonl_stream = None
            jsonl_temporary.replace(output_dir / "conversations.jsonl")
        if written != selected_count:
            raise CanonicalSliceError(
                f"slice conservation failed: expected={selected_count} written={written}"
            )
        manifest = {
            "schema_version": "canonical_conversation_slice_manifest_v0001",
            "source_file": input_path.name,
            "dataset_id": dataset_id,
            "source_envelope_id": source_envelope_id,
            "slice_count": written,
            "mode": mode,
            "sliced_at": _utc_now_z(),
            "sliced_by": SCRIPT_NAME,
            "task_dir": str(task_root),
            "output_subdir": out_subdir.as_posix(),
            "streaming": True,
        }
        _atomic_write_json(output_dir / "slice_manifest.json", manifest)
    except Exception:
        if jsonl_stream is not None:
            jsonl_stream.close()
        jsonl_temporary.unlink(missing_ok=True)
        shutil.rmtree(output_dir, ignore_errors=True)
        raise
    return {
        "status": "completed",
        "task_dir": str(task_root),
        "input_file": input_file.as_posix(),
        "output_subdir": out_subdir.as_posix(),
        "mode": mode,
        "written": written,
        "streaming": True,
    }


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def slicer_contract() -> Dict[str, Any]:
    return {
        "input_schema": CANONICAL_SCHEMA_VERSION,
        "output_schema": "canonical_conversation_slice_v0001",
        "conversation_field": "conversations",
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Slice canonical conversation ingress data without provider-specific fields.")
    parser.add_argument("--task-dir", required=True)
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--out-subdir", default=DEFAULT_OUT_SUBDIR)
    parser.add_argument("--mode", choices=("jsonl", "files"), default=DEFAULT_MODE)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = run_slice_streaming(
            task_dir=Path(args.task_dir),
            input_file=Path(args.input_file),
            out_subdir=Path(args.out_subdir),
            mode=args.mode,
            start_index=args.start_index,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except CanonicalSliceError as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
