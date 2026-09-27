#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: validate_source_timestamp_attachment_v0001.py
# 中文名: 原始消息时间挂接守恒校验脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_timestamp
# 脚本定位: 在增强 text units 进入数据库或下游以前验证字段、身份和时间契约
#
# 职责说明:
# - 对原输入与增强输出逐行验证原字段守恒和新增时间字段合法性
#
# 本脚本做什么:
# - 验证行数、原字段、坐标、unit_id、状态、UTC 时间和来源证据
# - 原子写出通过或失败的结构化 validation report
#
# 本脚本不做什么:
# - 不修复输入，不重新挂接，不写数据库
# - 不把 missing、invalid 或 not_applicable 伪装为 resolved
#
# 制度边界声明:
# - 任一原字段漂移或非法 resolved 时间均使验证失败并返回非零退出码
# - report 记录全部错误数量和受限样本，不覆盖输入或增强文件
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: validate_source_timestamp_attachment_v0001
# family: validate_source_timestamp_attachment
# role: source_timestamp_attachment_validator
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_timestamp_annotation/validate_source_timestamp_attachment_v0001.py
# input:
#   - original text units JSONL
#   - timestamp enriched text units JSONL
# output:
#   - timestamp attachment validation report JSON
# depends_on:
#   - Python standard library
#   - attach_source_timestamps_to_text_units_v0001
# used_by:
#   - data_action_chain_pipeline_v0007
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "validate_source_timestamp_attachment"
SCRIPT_NAME = "validate_source_timestamp_attachment_v0001.py"
SCRIPT_VERSION = "v0001"
ATTACHMENT_FIELDS = {
    "event_time",
    "event_time_raw",
    "event_time_kind",
    "event_time_source_path",
    "event_time_status",
    "event_time_rule_version",
    "mapping_node_id",
    "message_id",
    "source_timestamp_annotation_id",
}
VALID_STATUSES = {"resolved", "missing", "invalid", "ambiguous", "not_applicable"}
ERROR_SAMPLE_LIMIT = 100


# ============================================================
# 异常类型
# ============================================================

class SourceTimestampValidationError(RuntimeError):
    """Raised when the validation operation itself cannot be completed."""


# ============================================================
# 数据结构
# ============================================================

# Validation records are represented by versioned JSON objects in the report.


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
                    raise SourceTimestampValidationError(f"{label} line {line_number} is invalid JSON: {exc}") from exc
                if not isinstance(payload, dict):
                    raise SourceTimestampValidationError(f"{label} line {line_number} must be an object")
                yield line_number, payload
    except FileNotFoundError as exc:
        raise SourceTimestampValidationError(f"{label} not found: {path}") from exc


def _valid_utc(value: Any) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return True


def _append_error(errors: List[Dict[str, Any]], *, line: int, kind: str, detail: str) -> None:
    if len(errors) < ERROR_SAMPLE_LIMIT:
        errors.append({"line": line, "kind": kind, "detail": detail})


# ============================================================
# 默认映射
# ============================================================

IDENTITY_FIELDS = ("unit_id", "asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end")


# ============================================================
# 核心业务组件
# ============================================================

def validate_attachment(original_path: Path, enriched_path: Path) -> Dict[str, Any]:
    originals = [record for _, record in _read_jsonl(original_path, label="original units")]
    enriched = [record for _, record in _read_jsonl(enriched_path, label="enriched units")]
    errors: List[Dict[str, Any]] = []
    total_errors = 0
    status_counts: Counter[str] = Counter()
    if len(originals) != len(enriched):
        total_errors += 1
        _append_error(errors, line=0, kind="row_count_mismatch", detail=f"{len(originals)} != {len(enriched)}")
    for index, (source, output) in enumerate(zip(originals, enriched), 1):
        for key, value in source.items():
            if key not in output or output[key] != value:
                total_errors += 1
                _append_error(errors, line=index, kind="original_field_changed", detail=key)
        unexpected = sorted(set(output) - set(source) - ATTACHMENT_FIELDS)
        missing = sorted(ATTACHMENT_FIELDS - set(output))
        if unexpected:
            total_errors += 1
            _append_error(errors, line=index, kind="unexpected_fields", detail=",".join(unexpected))
        if missing:
            total_errors += 1
            _append_error(errors, line=index, kind="missing_attachment_fields", detail=",".join(missing))
        for key in IDENTITY_FIELDS:
            if key in source and output.get(key) != source.get(key):
                total_errors += 1
                _append_error(errors, line=index, kind="identity_changed", detail=key)
        status = output.get("event_time_status")
        if status not in VALID_STATUSES:
            total_errors += 1
            _append_error(errors, line=index, kind="invalid_status", detail=str(status))
            continue
        status_counts[str(status)] += 1
        if status == "resolved":
            if not _valid_utc(output.get("event_time")):
                total_errors += 1
                _append_error(errors, line=index, kind="invalid_resolved_time", detail=str(output.get("event_time")))
            if not isinstance(output.get("event_time_source_path"), str) or not output.get("event_time_source_path"):
                total_errors += 1
                _append_error(errors, line=index, kind="missing_source_path", detail="resolved record lacks source path")
        elif status == "not_applicable" and any(output.get(key) is not None for key in ATTACHMENT_FIELDS - {"event_time_status"}):
            total_errors += 1
            _append_error(errors, line=index, kind="not_applicable_has_source_data", detail="source fields must be null")
    return {
        "schema_version": "source_timestamp_attachment_validation_v0001",
        "status": "passed" if total_errors == 0 else "failed",
        "original_records": len(originals),
        "enriched_records": len(enriched),
        "status_counts": dict(sorted(status_counts.items())),
        "error_count": total_errors,
        "error_samples": errors,
        "original_sha256": _sha256_file(original_path),
        "enriched_sha256": _sha256_file(enriched_path),
    }


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def _require_distinct(paths: Sequence[Path]) -> None:
    if len({path.resolve() for path in paths}) != len(paths):
        raise SourceTimestampValidationError("original, enriched and report paths must be distinct")


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate source timestamp attachment conservation.")
    parser.add_argument("--original", required=True)
    parser.add_argument("--enriched", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        original_path = Path(args.original).resolve()
        enriched_path = Path(args.enriched).resolve()
        report_path = Path(args.report).resolve()
        _require_distinct((original_path, enriched_path, report_path))
        report = validate_attachment(original_path, enriched_path)
        report.update({"script": SCRIPT_NAME, "script_version": SCRIPT_VERSION, "run_id": args.run_id, "dry_run": bool(args.dry_run)})
        if not args.dry_run:
            _atomic_write_text(report_path, json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(_canonical_json(report))
        return 0 if report["status"] == "passed" else 2
    except SourceTimestampValidationError as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2
    except Exception as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

