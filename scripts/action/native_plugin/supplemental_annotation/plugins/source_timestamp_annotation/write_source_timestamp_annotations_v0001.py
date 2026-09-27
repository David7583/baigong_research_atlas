#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: write_source_timestamp_annotations_v0001.py
# 中文名: 原始时间戳旁表写入脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_timestamp
# 脚本定位: 将已挂接的原始时间字段按规范文本单元坐标写入 SQLite 旁表
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: write_source_timestamp_annotations_v0001
# family: source_timestamp_annotation_persistence
# role: source_timestamp_side_table_writer
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_timestamp_annotation/write_source_timestamp_annotations_v0001.py
# input:
#   - timestamp enriched language text units JSONL
#   - initialized data.db
# output:
#   - rows in data_text_unit_source_timestamps
#   - write manifest JSON
# depends_on:
#   - Python standard library
#   - init_source_timestamp_annotation_schema_v0001
# used_by:
#   - validate_source_timestamp_data_db_v0001
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple


SCRIPT_NAME = "write_source_timestamp_annotations_v0001.py"
SCRIPT_VERSION = "v0001"
DEFAULT_ENCODING = "utf-8"
CANONICAL_TABLE = "data_text_units"
SIDE_TABLE = "data_text_unit_source_timestamps"
KEY_FIELDS = ("asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end")
SOURCE_FIELDS = (
    "event_time", "event_time_raw", "event_time_kind", "event_time_source_path",
    "event_time_status", "event_time_rule_version", "mapping_node_id", "message_id",
    "source_timestamp_annotation_id",
)
VALID_STATUSES = {"resolved", "missing", "invalid", "ambiguous", "not_applicable"}


class SourceTimestampWriteError(RuntimeError):
    """Raised when annotation rows cannot be persisted without ambiguity."""


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
                raise SourceTimestampWriteError(f"invalid JSON at line {line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise SourceTimestampWriteError(f"line {line_number} is not an object")
            yield line_number, value


def _key(record: Mapping[str, Any], line_number: int) -> Tuple[Any, ...]:
    missing = [field for field in KEY_FIELDS if record.get(field) is None]
    if missing:
        raise SourceTimestampWriteError(f"line {line_number} missing key fields: {missing}")
    try:
        return (
            str(record["asset_id"]), str(record["path"]), int(record["value_index"]),
            int(record["segment_index"]), int(record["sentence_index"]),
            int(record["char_start"]), int(record["char_end"]),
        )
    except (TypeError, ValueError) as exc:
        raise SourceTimestampWriteError(f"line {line_number} has invalid key values") from exc


def _payload(record: Mapping[str, Any], line_number: int) -> Tuple[Any, ...]:
    status = record.get("event_time_status")
    if status not in VALID_STATUSES:
        raise SourceTimestampWriteError(f"line {line_number} has invalid event_time_status: {status}")
    raw = record.get("event_time_raw")
    raw_json = None if raw is None else _canonical_json(raw)
    return (
        record.get("event_time"), raw_json, record.get("event_time_kind"),
        record.get("event_time_source_path"), status, record.get("event_time_rule_version"),
        record.get("mapping_node_id"), record.get("message_id"),
        record.get("source_timestamp_annotation_id"),
    )


def write_annotations(units_path: Path, db_path: Path, run_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
    rows: List[Tuple[Any, ...]] = []
    seen: set[Tuple[Any, ...]] = set()
    for line_number, record in _read_jsonl(units_path):
        key = _key(record, line_number)
        if key in seen:
            raise SourceTimestampWriteError(f"duplicate text unit coordinate at line {line_number}: {key}")
        seen.add(key)
        rows.append(key + _payload(record, line_number))
    if dry_run:
        return {"status": "dry_run", "input_records": len(rows), "input_sha256": _sha256_file(units_path)}
    if not db_path.is_file():
        raise SourceTimestampWriteError(f"database not found: {db_path}")
    annotated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted = 0
    duplicates = 0
    connection = sqlite3.connect(str(db_path), timeout=30)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        side_exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (SIDE_TABLE,)).fetchone()
        if side_exists is None:
            raise SourceTimestampWriteError(f"required table not found: {SIDE_TABLE}")
        canonical_sql = f"SELECT 1 FROM {CANONICAL_TABLE} WHERE " + " AND ".join(f"{field}=?" for field in KEY_FIELDS)
        insert_columns = KEY_FIELDS + (
            "event_time", "event_time_raw_json", "event_time_kind", "event_time_source_path",
            "event_time_status", "event_time_rule_version", "mapping_node_id", "message_id",
            "source_timestamp_annotation_id", "annotation_run_id", "annotated_at",
        )
        insert_sql = f"INSERT OR IGNORE INTO {SIDE_TABLE} ({', '.join(insert_columns)}) VALUES ({', '.join('?' for _ in insert_columns)})"
        compare_sql = f"SELECT {', '.join(insert_columns[7:16])} FROM {SIDE_TABLE} WHERE " + " AND ".join(f"{field}=?" for field in KEY_FIELDS)
        for row in rows:
            key = row[:7]
            if connection.execute(canonical_sql, key).fetchone() is None:
                raise SourceTimestampWriteError(f"canonical text unit not found: {key}")
            cursor = connection.execute(insert_sql, row + (run_id, annotated_at))
            if cursor.rowcount:
                inserted += 1
            else:
                existing_payload = connection.execute(compare_sql, key).fetchone()
                if existing_payload != row[7:16]:
                    raise SourceTimestampWriteError(f"conflicting existing annotation for: {key}")
                duplicates += 1
        connection.commit()
        return {
            "status": "completed", "input_records": len(rows), "inserted_records": inserted,
            "unchanged_duplicates": duplicates, "input_sha256": _sha256_file(units_path),
            "table": SIDE_TABLE,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write source timestamp annotations to the SQLite side table.")
    parser.add_argument("--units", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        units_path = Path(args.units).resolve()
        db_path = Path(args.db).resolve()
        manifest_path = Path(args.manifest).resolve()
        result = write_annotations(units_path, db_path, args.run_id, dry_run=args.dry_run)
        result.update({"script": SCRIPT_NAME, "script_version": SCRIPT_VERSION, "run_id": args.run_id, "db": str(db_path)})
        if not args.dry_run:
            _atomic_write(manifest_path, json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(_canonical_json(result))
        return 0
    except Exception as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
