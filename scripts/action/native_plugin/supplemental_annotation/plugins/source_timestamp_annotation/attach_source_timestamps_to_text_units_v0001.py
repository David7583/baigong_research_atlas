#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: attach_source_timestamps_to_text_units_v0001.py
# 中文名: 原始消息时间挂接脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_timestamp
# 脚本定位: 将来源消息时间按稳定解析坐标追加到 language 或 normalized text units
#
# 职责说明:
# - 用 asset_id、path、value_index 关联来源时间标注并保持全部原字段不变
#
# 本脚本做什么:
# - 对 language units 使用 source_value_sha1 二次核验
# - 原子写出逐行增强 JSONL 与挂接 manifest
#
# 本脚本不做什么:
# - 不提取或解释时间，不重新计算 unit_id，不写数据库
# - 不覆盖输入，不把运行时间或 created_at 当成事件时间
#
# 制度边界声明:
# - 每个输入行必须在输出中原序保留且只追加受控字段
# - 冲突 annotation 不静默选择；挂接结果显式标为 ambiguous
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: attach_source_timestamps_to_text_units_v0001
# family: attach_source_timestamps_to_text_units
# role: source_timestamp_text_unit_attacher
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_timestamp_annotation/attach_source_timestamps_to_text_units_v0001.py
# input:
#   - language or normalized text units JSONL
#   - source message timestamp annotations JSONL
# output:
#   - timestamp enriched text units JSONL
#   - timestamp attachment manifest JSON
# depends_on:
#   - Python standard library
#   - extract_source_message_timestamps_v0001
# used_by:
#   - validate_source_timestamp_attachment_v0001
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "attach_source_timestamps_to_text_units"
SCRIPT_NAME = "attach_source_timestamps_to_text_units_v0001.py"
SCRIPT_VERSION = "v0001"
ATTACHMENT_FIELDS = (
    "event_time",
    "event_time_raw",
    "event_time_kind",
    "event_time_source_path",
    "event_time_status",
    "event_time_rule_version",
    "mapping_node_id",
    "message_id",
    "source_timestamp_annotation_id",
)


# ============================================================
# 异常类型
# ============================================================

class SourceTimestampAttachmentError(RuntimeError):
    """Raised when source timestamp annotations cannot be attached safely."""


# ============================================================
# 数据结构
# ============================================================

Coordinate = Tuple[str, str, int]


# ============================================================
# 工具函数区
# ============================================================

def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(text, encoding=DEFAULT_ENCODING, newline="\n")
    os.replace(temporary, path)


def _read_jsonl(path: Path, *, label: str) -> Iterator[Tuple[int, Dict[str, Any]]]:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SourceTimestampAttachmentError(f"{label} line {line_number} is invalid JSON: {exc}") from exc
                if not isinstance(payload, dict):
                    raise SourceTimestampAttachmentError(f"{label} line {line_number} must be an object")
                yield line_number, payload
    except FileNotFoundError as exc:
        raise SourceTimestampAttachmentError(f"{label} not found: {path}") from exc


def _safe_index(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise SourceTimestampAttachmentError(f"{label} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SourceTimestampAttachmentError(f"{label} must be an integer") from exc
    if parsed < 0:
        raise SourceTimestampAttachmentError(f"{label} must be non-negative")
    return parsed


def _coordinate(record: Mapping[str, Any], *, label: str) -> Coordinate:
    asset_id = record.get("asset_id")
    path = record.get("target_path") if label == "annotation" else record.get("path")
    if not isinstance(asset_id, str) or not asset_id or not isinstance(path, str) or not path:
        raise SourceTimestampAttachmentError(f"{label} requires non-empty asset_id and path")
    return asset_id, path, _safe_index(record.get("value_index"), label=f"{label}.value_index")


def _annotation_index(path: Path) -> Tuple[DefaultDict[Coordinate, List[Dict[str, Any]]], int]:
    index: DefaultDict[Coordinate, List[Dict[str, Any]]] = defaultdict(list)
    count = 0
    for _, record in _read_jsonl(path, label="timestamp annotations"):
        index[_coordinate(record, label="annotation")].append(record)
        count += 1
    return index, count


def _attachment_payload(annotation: Optional[Mapping[str, Any]], *, status: str) -> Dict[str, Any]:
    if annotation is None:
        return {
            "event_time": None,
            "event_time_raw": None,
            "event_time_kind": None,
            "event_time_source_path": None,
            "event_time_status": status,
            "event_time_rule_version": None,
            "mapping_node_id": None,
            "message_id": None,
            "source_timestamp_annotation_id": None,
        }
    return {
        "event_time": annotation.get("event_time"),
        "event_time_raw": annotation.get("event_time_raw"),
        "event_time_kind": annotation.get("event_time_kind"),
        "event_time_source_path": annotation.get("event_time_source_path"),
        "event_time_status": status,
        "event_time_rule_version": annotation.get("event_time_rule_version"),
        "mapping_node_id": annotation.get("mapping_node_id"),
        "message_id": annotation.get("message_id"),
        "source_timestamp_annotation_id": annotation.get("annotation_id"),
    }


# ============================================================
# 默认映射
# ============================================================

VALID_SOURCE_STATUSES = {"resolved", "missing", "invalid", "ambiguous"}


# ============================================================
# 核心业务组件
# ============================================================

def attach_timestamps(units_path: Path, annotations_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int], int]:
    annotations, annotations_count = _annotation_index(annotations_path)
    rows: List[Dict[str, Any]] = []
    counts: Counter[str] = Counter()
    matched_annotation_ids: set[str] = set()
    for line_number, unit in _read_jsonl(units_path, label="text units"):
        overlap = sorted(set(unit).intersection(ATTACHMENT_FIELDS))
        if overlap:
            raise SourceTimestampAttachmentError(f"text units line {line_number} already contains attachment fields: {overlap}")
        coordinate = _coordinate(unit, label="text unit")
        candidates = annotations.get(coordinate, [])
        selected: Optional[Mapping[str, Any]] = None
        status = "not_applicable"
        if len(candidates) > 1:
            unique = {_canonical_json({key: candidate.get(key) for key in ATTACHMENT_FIELDS if key != "source_timestamp_annotation_id"}) for candidate in candidates}
            if len(unique) > 1:
                status = "ambiguous"
            else:
                selected = candidates[0]
        elif len(candidates) == 1:
            selected = candidates[0]
        if selected is not None:
            status = str(selected.get("event_time_status"))
            if status not in VALID_SOURCE_STATUSES:
                raise SourceTimestampAttachmentError(f"annotation has unsupported event_time_status: {status}")
            source_sha1 = selected.get("source_value_sha1")
            unit_sha1 = unit.get("source_value_sha1")
            if unit_sha1 is not None and source_sha1 != unit_sha1:
                status = "ambiguous"
            annotation_id = selected.get("annotation_id")
            if isinstance(annotation_id, str):
                matched_annotation_ids.add(annotation_id)
        enriched = dict(unit)
        enriched.update(_attachment_payload(selected, status=status))
        rows.append(enriched)
        counts[status] += 1
    unused = 0
    for candidates in annotations.values():
        for annotation in candidates:
            if annotation.get("annotation_id") not in matched_annotation_ids:
                unused += 1
    return rows, dict(sorted(counts.items())), unused


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def _render_jsonl(rows: Iterable[Mapping[str, Any]]) -> str:
    return "".join(_canonical_json(dict(row)) + "\n" for row in rows)


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Attach source message timestamps to text units.")
    parser.add_argument("--units", required=True)
    parser.add_argument("--annotations", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        units_path = Path(args.units).resolve()
        annotations_path = Path(args.annotations).resolve()
        output_path = Path(args.output).resolve()
        manifest_path = Path(args.manifest).resolve()
        if output_path == units_path or output_path == annotations_path or manifest_path in {units_path, annotations_path, output_path}:
            raise SourceTimestampAttachmentError("input, output and manifest paths must be distinct")
        rows, status_counts, unused_annotations = attach_timestamps(units_path, annotations_path)
        manifest = {
            "schema_version": "source_timestamp_attachment_manifest_v0001",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "run_id": args.run_id,
            "status": "dry_run" if args.dry_run else "completed",
            "units_input": str(units_path),
            "units_input_sha256": _sha256_file(units_path),
            "annotations_input": str(annotations_path),
            "annotations_input_sha256": _sha256_file(annotations_path),
            "output": str(output_path),
            "input_records": len(rows),
            "output_records": len(rows),
            "status_counts": status_counts,
            "unused_annotations": unused_annotations,
        }
        if not args.dry_run:
            _atomic_write_text(output_path, _render_jsonl(rows))
            manifest["output_sha256"] = _sha256_file(output_path)
            _atomic_write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        print(_canonical_json(manifest))
        return 0
    except SourceTimestampAttachmentError as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2
    except Exception as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

