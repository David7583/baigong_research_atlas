#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: init_source_timestamp_annotation_schema_v0001.py
# 中文名: 原始时间戳旁表初始化脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_timestamp
# 脚本定位: 在既有 data.db 中追加原始时间戳旁表，不改变规范文本单元表
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: init_source_timestamp_annotation_schema_v0001
# family: source_timestamp_annotation_schema
# role: source_timestamp_side_table_initializer
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_timestamp_annotation/init_source_timestamp_annotation_schema_v0001.py
# input:
#   - existing data.db containing data_text_units
# output:
#   - data_text_unit_source_timestamps side table
# depends_on:
#   - Python standard library
# used_by:
#   - write_source_timestamp_annotations_v0001
# ============================================================

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


SCRIPT_NAME = "init_source_timestamp_annotation_schema_v0001.py"
SCRIPT_VERSION = "v0001"
CANONICAL_TABLE = "data_text_units"
SIDE_TABLE = "data_text_unit_source_timestamps"
EXPECTED_COLUMNS = (
    "asset_id", "path", "value_index", "segment_index", "sentence_index",
    "char_start", "char_end", "event_time", "event_time_raw_json",
    "event_time_kind", "event_time_source_path", "event_time_status",
    "event_time_rule_version", "mapping_node_id", "message_id",
    "source_timestamp_annotation_id", "annotation_run_id", "annotated_at",
)


class SourceTimestampSchemaError(RuntimeError):
    """Raised when the database cannot safely host the side table."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def initialize_schema(db_path: Path, *, dry_run: bool = False) -> Dict[str, Any]:
    if not db_path.is_file():
        raise SourceTimestampSchemaError(f"database not found: {db_path}")
    connection = sqlite3.connect(str(db_path), timeout=30)
    try:
        canonical_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (CANONICAL_TABLE,)
        ).fetchone()
        if canonical_exists is None:
            raise SourceTimestampSchemaError(f"required table not found: {CANONICAL_TABLE}")
        existing = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (SIDE_TABLE,)
        ).fetchone()
        if dry_run:
            return {"status": "dry_run", "table": SIDE_TABLE, "already_exists": existing is not None}
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(f"""
CREATE TABLE IF NOT EXISTS {SIDE_TABLE} (
    asset_id                       TEXT    NOT NULL,
    path                           TEXT    NOT NULL,
    value_index                    INTEGER NOT NULL,
    segment_index                  INTEGER NOT NULL,
    sentence_index                 INTEGER NOT NULL,
    char_start                     INTEGER NOT NULL,
    char_end                       INTEGER NOT NULL,
    event_time                     TEXT,
    event_time_raw_json            TEXT,
    event_time_kind                TEXT,
    event_time_source_path         TEXT,
    event_time_status              TEXT    NOT NULL,
    event_time_rule_version        TEXT,
    mapping_node_id                TEXT,
    message_id                     TEXT,
    source_timestamp_annotation_id TEXT,
    annotation_run_id              TEXT    NOT NULL,
    annotated_at                   TEXT    NOT NULL,
    PRIMARY KEY (asset_id, path, value_index, segment_index, sentence_index, char_start, char_end),
    FOREIGN KEY (asset_id, path, value_index, segment_index, sentence_index, char_start, char_end)
        REFERENCES {CANONICAL_TABLE} (asset_id, path, value_index, segment_index, sentence_index, char_start, char_end)
);
CREATE INDEX IF NOT EXISTS idx_{SIDE_TABLE}_event_time
    ON {SIDE_TABLE} (event_time);
CREATE INDEX IF NOT EXISTS idx_{SIDE_TABLE}_status
    ON {SIDE_TABLE} (event_time_status);
""")
        columns = tuple(row[1] for row in connection.execute(f"PRAGMA table_info({SIDE_TABLE})"))
        if columns != EXPECTED_COLUMNS:
            raise SourceTimestampSchemaError(f"unexpected existing schema for {SIDE_TABLE}: {columns}")
        connection.commit()
        return {"status": "completed", "table": SIDE_TABLE, "already_existed": existing is not None, "columns": list(columns)}
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialize the source timestamp annotation side table.")
    parser.add_argument("--db", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = initialize_schema(Path(args.db).resolve(), dry_run=args.dry_run)
        result.update({"script": SCRIPT_NAME, "script_version": SCRIPT_VERSION})
        print(_canonical_json(result))
        return 0
    except Exception as exc:
        print(_canonical_json({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
