#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: validate_source_timestamp_data_db_v0002.py
# 中文名: 增量原始时间戳数据库对账脚本
# 版本号: v0002
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_timestamp
# 脚本定位: 对账当前增量文本单元与可包含历史记录的规范表及时间戳旁表
#
# 职责说明:
# - 逐条验证当前输入坐标在规范表和时间戳旁表中的存在性与载荷一致性
# - 区分当前输入验证数量与数据库历史旁表总量
#
# 本脚本做什么:
# - 使用完整坐标键执行只读 SQLite 精确查询
# - 输出当前输入、已验证记录、旁表总记录和错误样本
#
# 本脚本不做什么:
# - 不要求增量输入数量等于历史旁表总量
# - 不写入、删除或重建数据库，不修改输入 JSONL
#
# 制度边界声明:
# - 空输入、重复输入键、缺失规范记录、缺失旁表记录或载荷不一致均失败
# - 数据库可包含先前批次记录；这些历史记录不属于当前输入错误
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: validate_source_timestamp_data_db_v0002
# family: source_timestamp_annotation_validation
# role: source_timestamp_database_validator
# version: v0002
# status: experimental
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_timestamp_annotation/validate_source_timestamp_data_db_v0002.py
# input:
#   - timestamp enriched language text units JSONL
#   - data.db
# output:
#   - validation report JSON
# depends_on:
#   - Python standard library
#   - write_source_timestamp_annotations_v0001
# used_by:
#   - data_action_chain_pipeline_v0011
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence, Tuple


# ============================================================
# 默认配置
# ============================================================

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "source_timestamp_annotation_validation"
SCRIPT_NAME = "validate_source_timestamp_data_db_v0002.py"
SCRIPT_VERSION = "v0002"
CANONICAL_TABLE = "data_text_units"
SIDE_TABLE = "data_text_unit_source_timestamps"
KEY_FIELDS = ("asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end")
DB_PAYLOAD_FIELDS = (
    "event_time", "event_time_raw_json", "event_time_kind", "event_time_source_path",
    "event_time_status", "event_time_rule_version", "mapping_node_id", "message_id",
    "source_timestamp_annotation_id",
)


# ============================================================
# 异常类型
# ============================================================

class SourceTimestampDatabaseValidationError(RuntimeError):
    """Raised when database validation cannot be executed."""


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


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(content, encoding=DEFAULT_ENCODING, newline="\n")
    os.replace(temporary, path)


def _read_jsonl(path: Path) -> Iterator[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding=DEFAULT_ENCODING) as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SourceTimestampDatabaseValidationError(f"invalid JSON at line {line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise SourceTimestampDatabaseValidationError(f"line {line_number} is not an object")
            yield line_number, value


def _expected(record: Mapping[str, Any], line_number: int) -> Tuple[Tuple[Any, ...], Tuple[Any, ...]]:
    try:
        key = (
            str(record["asset_id"]), str(record["path"]), int(record["value_index"]),
            int(record["segment_index"]), int(record["sentence_index"]),
            int(record["char_start"]), int(record["char_end"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceTimestampDatabaseValidationError(f"invalid key at line {line_number}") from exc
    raw = record.get("event_time_raw")
    payload = (
        record.get("event_time"), None if raw is None else _canonical_json(raw),
        record.get("event_time_kind"), record.get("event_time_source_path"),
        record.get("event_time_status"), record.get("event_time_rule_version"),
        record.get("mapping_node_id"), record.get("message_id"),
        record.get("source_timestamp_annotation_id"),
    )
    return key, payload


# ============================================================
# 核心验证组件
# ============================================================

def validate_database(units_path: Path, db_path: Path) -> Dict[str, Any]:
    if not db_path.is_file():
        raise SourceTimestampDatabaseValidationError(f"database not found: {db_path}")
    connection = sqlite3.connect(str(db_path), timeout=30)
    errors = []
    status_counts: Counter[str] = Counter()
    input_records = 0
    validated_side_records = 0
    seen_keys = set()
    try:
        canonical_sql = f"SELECT 1 FROM {CANONICAL_TABLE} WHERE " + " AND ".join(f"{field}=?" for field in KEY_FIELDS)
        side_sql = f"SELECT {', '.join(DB_PAYLOAD_FIELDS)} FROM {SIDE_TABLE} WHERE " + " AND ".join(f"{field}=?" for field in KEY_FIELDS)
        for line_number, record in _read_jsonl(units_path):
            input_records += 1
            key, expected = _expected(record, line_number)
            if key in seen_keys:
                errors.append({"line": line_number, "kind": "duplicate_input_key", "key": list(key)})
                continue
            seen_keys.add(key)
            if connection.execute(canonical_sql, key).fetchone() is None:
                errors.append({"line": line_number, "kind": "canonical_row_missing", "key": list(key)})
                continue
            actual = connection.execute(side_sql, key).fetchone()
            if actual is None:
                errors.append({"line": line_number, "kind": "side_row_missing", "key": list(key)})
            elif actual != expected:
                errors.append({"line": line_number, "kind": "side_payload_mismatch", "key": list(key)})
            else:
                validated_side_records += 1
            status_counts[str(record.get("event_time_status"))] += 1
        side_count = connection.execute(f"SELECT COUNT(*) FROM {SIDE_TABLE}").fetchone()[0]
        if input_records == 0:
            errors.append({"line": 0, "kind": "empty_input"})
    except sqlite3.Error as exc:
        raise SourceTimestampDatabaseValidationError(str(exc)) from exc
    finally:
        connection.close()
    return {
        "schema_version": "source_timestamp_data_db_validation_v0002",
        "status": "passed" if not errors else "failed",
        "input_records": input_records,
        "side_table_records": side_count,
        "validated_side_records": validated_side_records,
        "status_counts": dict(sorted(status_counts.items())),
        "error_count": len(errors),
        "error_samples": errors[:20],
        "input_sha256": _sha256_file(units_path),
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate source timestamp annotations persisted in data.db.")
    parser.add_argument("--units", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        report = validate_database(Path(args.units).resolve(), Path(args.db).resolve())
        report.update({"script": SCRIPT_NAME, "script_version": SCRIPT_VERSION, "run_id": args.run_id, "dry_run": bool(args.dry_run)})
        if not args.dry_run:
            _atomic_write(Path(args.report).resolve(), json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(_canonical_json(report))
        return 0 if report["status"] == "passed" else 2
    except Exception as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
