#!/usr/bin/env python3
# ============================================================
# File: init_data_schema_v0001.py
# 中文名: 数据层 SQL 数据库初始化与结构校验脚本
# Version: v0001
#
# Layer: infrastructure
# Script Type: Schema Management
# Updatable: True
#
# Purpose
#
# 创建 sql/data.db 数据库文件与数据层核心表结构
# 或校验已有数据库的表结构与预期是否一致
#
# What it does
#
# 1 创建数据库文件与两张表（data_text_units / data_run_log）
# 2 创建索引
# 3 设置并验证 PRAGMA（WAL 模式 / synchronous）
# 4 校验已有数据库的表结构、字段类型、约束、索引完整性
#
# What it does NOT do
#
# 1 不写入任何业务数据
# 2 不执行迁移或 ALTER TABLE
# 3 不删除或重建已有表
# 4 不操作理解层或行动层的数据库
#
# Design decision
#
# 幂等性保证
#
# 所有 CREATE TABLE 使用 IF NOT EXISTS
# 所有 CREATE INDEX 使用 IF NOT EXISTS
# 重复运行不会破坏已有数据
#
# 数据层独立
#
# 本脚本只管 data.db，不读写 understand.db 或 action.db
# 数据层有自己的审计表 data_run_log
#
# 字段名安全
#
# 所有表名与字段名通过合法性检查（仅允许字母、数字、下划线）
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: init_data_schema
# family: init_data_schema
# role: schema_initializer
# version: v0001
# status: active
# entry_point: scripts/tools/init_data_schema_v0001.py
#
# depends_on:
#   - Python stdlib: sqlite3, json, argparse, pathlib, datetime, typing, re
#
# used_by:
#   - manual invocation before first data ingest
#   - ingest_data_text_units_v0001.py (expects tables to exist)
#   - verify_data_anchor_v0001.py (queries data_text_units)
# ============================================================


from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "init_data_schema"
SCRIPT_VERSION = "v0001"

MAX_ROOT_SEARCH_DEPTH = 10

DEFAULT_DB_PATH = Path("sql") / "data.db"

DEFAULT_TABLES = {
    "text_units": "data_text_units",
    "run_log": "data_run_log",
}

DEFAULT_FIELDS = {
    "text_units": {
        "asset_id": "asset_id",
        "path": "path",
        "value_index": "value_index",
        "segment_index": "segment_index",
        "sentence_index": "sentence_index",
        "char_start": "char_start",
        "char_end": "char_end",
        "text": "text",
        "source_value_sha1": "source_value_sha1",
        "parse_version": "parse_version",
        "unit_type": "unit_type",
        "ingested_at": "ingested_at",
    },
    "run_log": {
        "run_id": "run_id",
        "script_name": "script_name",
        "script_version": "script_version",
        "started_at": "started_at",
        "finished_at": "finished_at",
        "input_hash": "input_hash",
        "record_count": "record_count",
        "error_summary": "error_summary",
    },
}

DEFAULT_PRAGMAS = {
    "journal_mode": "WAL",
    "foreign_keys": False,  # 数据层无外键引用
    "synchronous": "NORMAL",
}

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


def _validate_identifier(name: str, context: str) -> None:
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe SQL identifier from config ({context}): '{name}'. "
            f"Only letters, digits, and underscores are allowed."
        )


def _validate_all_identifiers(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> None:
    for key, tbl_name in tables.items():
        _validate_identifier(tbl_name, f"tables.{key}")
    for group_key, field_map in fields.items():
        for fkey, fname in field_map.items():
            _validate_identifier(fname, f"fields.{group_key}.{fkey}")


# ============================================================
# Schema Definition
# ============================================================

def _build_create_statements(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[Tuple[str, str]]:

    stmts: List[Tuple[str, str]] = []

    tt = tables["text_units"]
    tr = tables["run_log"]
    ft = fields["text_units"]
    fr = fields["run_log"]

    # -------------------------------------------------------
    # 1. data_text_units
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {tt}", f"""
CREATE TABLE IF NOT EXISTS {tt} (
    {ft['asset_id']}            TEXT    NOT NULL,
    {ft['path']}                TEXT    NOT NULL,
    {ft['value_index']}         INTEGER NOT NULL,
    {ft['segment_index']}       INTEGER NOT NULL,
    {ft['sentence_index']}      INTEGER NOT NULL,
    {ft['char_start']}          INTEGER NOT NULL,
    {ft['char_end']}            INTEGER NOT NULL,
    {ft['text']}                TEXT    NOT NULL,
    {ft['source_value_sha1']}   TEXT,
    {ft['parse_version']}       TEXT,
    {ft['unit_type']}           TEXT,
    {ft['ingested_at']}         TEXT    NOT NULL,
    PRIMARY KEY (
        {ft['asset_id']},
        {ft['path']},
        {ft['value_index']},
        {ft['segment_index']},
        {ft['sentence_index']},
        {ft['char_start']},
        {ft['char_end']}
    )
);
"""))

    # verify_data_anchor 查询用索引（5 字段）
    idx_name = f"idx_{tt}_anchor"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {tt} ({ft['asset_id']}, {ft['path']}, {ft['segment_index']}, {ft['char_start']}, {ft['char_end']});
"""))

    # 按 asset 查全量
    idx_name = f"idx_{tt}_asset"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {tt} ({ft['asset_id']});
"""))

    # -------------------------------------------------------
    # 2. data_run_log
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {tr}", f"""
CREATE TABLE IF NOT EXISTS {tr} (
    {fr['run_id']}              TEXT PRIMARY KEY,
    {fr['script_name']}         TEXT NOT NULL,
    {fr['script_version']}      TEXT NOT NULL,
    {fr['started_at']}          TEXT NOT NULL,
    {fr['finished_at']}         TEXT,
    {fr['input_hash']}          TEXT,
    {fr['record_count']}        INTEGER,
    {fr['error_summary']}       TEXT
);
"""))

    idx_name = f"idx_{tr}_{fr['started_at']}"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {tr} ({fr['started_at']});
"""))

    return stmts


# ============================================================
# Expected column specs for validation
# ============================================================

def _build_expected_columns(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, List[Tuple[str, str, bool, bool]]]:

    ft = fields["text_units"]
    fr = fields["run_log"]

    specs: Dict[str, List[Tuple[str, str, bool, bool]]] = {}

    # data_text_units
    specs[tables["text_units"]] = [
        (ft["asset_id"],          "TEXT",    True,  False),
        (ft["path"],              "TEXT",    True,  False),
        (ft["value_index"],       "INTEGER", True,  False),
        (ft["segment_index"],     "INTEGER", True,  False),
        (ft["sentence_index"],    "INTEGER", True,  False),
        (ft["char_start"],        "INTEGER", True,  False),
        (ft["char_end"],          "INTEGER", True,  False),
        (ft["text"],              "TEXT",    True,  False),
        (ft["source_value_sha1"], "TEXT",    False, False),
        (ft["parse_version"],     "TEXT",    False, False),
        (ft["unit_type"],         "TEXT",    False, False),
        (ft["ingested_at"],       "TEXT",    True,  False),
    ]

    # data_run_log
    specs[tables["run_log"]] = [
        (fr["run_id"],          "TEXT",    True,  True),
        (fr["script_name"],     "TEXT",    True,  False),
        (fr["script_version"],  "TEXT",    True,  False),
        (fr["started_at"],      "TEXT",    True,  False),
        (fr["finished_at"],     "TEXT",    False, False),
        (fr["input_hash"],      "TEXT",    False, False),
        (fr["record_count"],    "INTEGER", False, False),
        (fr["error_summary"],   "TEXT",    False, False),
    ]

    return specs


def _build_expected_index_names(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[str]:
    tt = tables["text_units"]
    tr = tables["run_log"]
    fr = fields["run_log"]

    return [
        f"idx_{tt}_anchor",
        f"idx_{tt}_asset",
        f"idx_{tr}_{fr['started_at']}",
    ]


# ============================================================
# Init
# ============================================================

def init_database(
    db_path: Path,
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    pragmas: Dict[str, Any],
    *,
    verbose: bool = False,
    audit: bool = False,
) -> Dict[str, Any]:

    _validate_all_identifiers(tables, fields)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    current_label = "(connection)"
    pragma_results: Dict[str, str] = {}

    try:
        # Apply PRAGMAs
        jm = pragmas.get("journal_mode", "WAL")
        current_label = f"PRAGMA journal_mode = {jm}"
        row = conn.execute(f"PRAGMA journal_mode = {jm};").fetchone()
        actual_jm = str(row[0]).upper() if row else "unknown"
        pragma_results["journal_mode"] = actual_jm
        if actual_jm != str(jm).upper():
            raise RuntimeError(
                f"PRAGMA journal_mode expected {jm}, got {actual_jm}"
            )

        sync = pragmas.get("synchronous", "NORMAL")
        current_label = f"PRAGMA synchronous = {sync}"
        conn.execute(f"PRAGMA synchronous = {sync};")
        pragma_results["synchronous"] = str(sync)

        stmts = _build_create_statements(tables, fields)

        for label, stmt in stmts:
            current_label = label
            if verbose:
                print(f"[INIT] {label}")
            conn.execute(stmt)

        conn.commit()

        # Optional audit record
        audit_written = False
        if audit:
            try:
                tr = tables["run_log"]
                fr = fields["run_log"]
                run_id = f"init_data_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
                now = _utc_iso()
                cols = [
                    fr["run_id"], fr["script_name"], fr["script_version"],
                    fr["started_at"], fr["finished_at"],
                ]
                vals = [run_id, SCRIPT_NAME, SCRIPT_VERSION, now, now]
                sql = f"INSERT INTO {tr} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
                conn.execute(sql, vals)
                conn.commit()
                audit_written = True
            except Exception:
                pass

        return {
            "status": "ok",
            "action": "init",
            "db_path": str(db_path),
            "tables_created": list(tables.values()),
            "pragma_results": pragma_results,
            "audit_written": audit_written,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            "status": "error",
            "action": "init",
            "db_path": str(db_path),
            "failed_at": current_label,
            "error": str(e)[:500],
            "timestamp": _utc_iso(),
        }

    finally:
        conn.close()


# ============================================================
# Validate
# ============================================================

def validate_schema(
    db_path: Path,
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:

    if not db_path.exists():
        return {
            "status": "error",
            "action": "validate",
            "db_path": str(db_path),
            "error": "database file does not exist",
            "timestamp": _utc_iso(),
        }

    _validate_all_identifiers(tables, fields)

    conn = sqlite3.connect(str(db_path))
    issues: List[str] = []

    try:
        cur = conn.cursor()

        expected_columns = _build_expected_columns(tables, fields)

        for config_key, table_name in tables.items():
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (table_name,),
            )
            if cur.fetchone() is None:
                issues.append(f"missing table: {table_name}")
                continue

            cur.execute(f"PRAGMA table_info({table_name});")
            actual_cols: Dict[str, Tuple[str, bool, bool]] = {}
            for row in cur.fetchall():
                col_name = row[1]
                col_type = str(row[2]).upper() if row[2] else ""
                col_notnull = bool(row[3])
                col_pk = bool(row[5])
                actual_cols[col_name] = (col_type, col_notnull, col_pk)

            if table_name in expected_columns:
                for (exp_name, exp_type, exp_notnull, exp_pk) in expected_columns[table_name]:
                    if exp_name not in actual_cols:
                        issues.append(
                            f"table {table_name}: missing column: {exp_name}"
                        )
                        continue

                    act_type, act_notnull, act_pk = actual_cols[exp_name]

                    if act_type != exp_type.upper():
                        issues.append(
                            f"table {table_name}.{exp_name}: "
                            f"type expected {exp_type}, got {act_type}"
                        )

                    if exp_notnull and not act_notnull and not exp_pk:
                        issues.append(
                            f"table {table_name}.{exp_name}: "
                            f"expected NOT NULL but got nullable"
                        )

                    if exp_pk and not act_pk:
                        issues.append(
                            f"table {table_name}.{exp_name}: "
                            f"expected PRIMARY KEY but not marked as pk"
                        )

        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='index';",
        )
        actual_indexes = {row[0] for row in cur.fetchall()}

        expected_indexes = _build_expected_index_names(tables, fields)
        for idx_name in expected_indexes:
            if idx_name not in actual_indexes:
                issues.append(f"missing index: {idx_name}")

        row = conn.execute("PRAGMA journal_mode;").fetchone()
        if row and str(row[0]).upper() != "WAL":
            issues.append(f"journal_mode is {row[0]}, expected WAL")

        status = "ok" if not issues else "issues_found"

        return {
            "status": status,
            "action": "validate",
            "db_path": str(db_path),
            "tables_checked": list(tables.values()),
            "indexes_expected": expected_indexes,
            "issues": issues,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "action": "validate",
            "db_path": str(db_path),
            "error": str(e)[:500],
            "timestamp": _utc_iso(),
        }

    finally:
        conn.close()


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Initialize or validate data layer schema (sql/data.db)."
    )
    ap.add_argument(
        "--db-path",
        default=None,
        help="Override database file path. If omitted, uses default (sql/data.db relative to project root).",
    )

    group = ap.add_mutually_exclusive_group()
    group.add_argument(
        "--init",
        action="store_true",
        help="Create database and tables (idempotent).",
    )
    group.add_argument(
        "--validate",
        action="store_true",
        help="Validate existing database schema.",
    )

    ap.add_argument(
        "--audit",
        action="store_true",
        help="Write a data_run_log record after successful init.",
    )
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    if args.db_path:
        db_path = Path(args.db_path).resolve()
    else:
        db_path = (project_root / DEFAULT_DB_PATH).resolve()

    if not args.init and not args.validate:
        args.validate = True

    if args.init:
        result = init_database(
            db_path, DEFAULT_TABLES, DEFAULT_FIELDS, DEFAULT_PRAGMAS,
            verbose=bool(args.verbose),
            audit=bool(args.audit),
        )
    else:
        result = validate_schema(db_path, DEFAULT_TABLES, DEFAULT_FIELDS)

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result["status"] == "error":
        return 1
    if result.get("issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
