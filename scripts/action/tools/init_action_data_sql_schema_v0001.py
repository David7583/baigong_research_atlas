#!/usr/bin/env python3
# ============================================================
# File: init_action_data_sql_schema_v0001.py
# 中文名: 行动端数据四库 SQL 数据库初始化与结构校验脚本
# Version: v0001
#
# Layer: infrastructure
# Main Layer: action
# Sub Layer: action_data
# Script Type: Schema Management
# Updatable: True
#
# Purpose
#
# 创建 sql/action_data.db 数据库文件与四张核心表结构
# 或校验已有数据库的表结构与预期是否一致
#
# 本脚本是行动端【数据四库】的 SQL 原生层建库脚本
# 与理解端 init_sql_schema_v0001 的骨架完全一致
# 表结构在初始阶段与理解端完全同构（仓库先建好，演化交给以后）
#
# 行动端两套四库的区分
# - 脚本四库：承载脚本/功能/功能块/任务的元数据（已存在）
# - 数据四库：承载行动产生的实际数据（本脚本服务于此）
#
#
# What it does
#
# 1 读取 action_data_sql_schema_config YAML 获取数据库路径与表名/字段名映射
# 2 创建数据库文件与四张表（concept_units / instance_units / unit_attributes / ingestion_run_log）
# 3 创建对应的索引与约束
# 4 设置并验证 PRAGMA（WAL 模式 / 外键约束）
# 5 校验已有数据库的表结构、字段类型、约束、索引完整性
#
#
# What it does NOT do
#
# 1 不写入任何业务数据
# 2 不执行迁移或 ALTER TABLE
# 3 不删除或重建已有表
# 4 不操作行动端脚本四库或理解端任何数据库
# 5 不修改 config 文件
#
#
# Design decision
#
# 幂等性保证
#
# 所有 CREATE TABLE 使用 IF NOT EXISTS
# 所有 CREATE INDEX 使用 IF NOT EXISTS
# 重复运行不会破坏已有数据
#
# Config 驱动
#
# 表名与字段名从 YAML config 中读取
# 若 config 文件不存在 则使用内置默认值
#
# 与理解端的对称性
#
# 表结构、字段、约束、PRAGMA 在初始阶段与理解端完全一致
# 唯一区别是数据库文件路径（sql/action_data.db 而非 sql/data.db）
# 各端的库会在各自的演化中逐渐分化（仓库先建好，演化交给以后）
#
# 字段名安全
#
# 所有从 config 读取的表名与字段名
# 必须通过合法性检查（仅允许字母 数字 下划线）
# 防止配置文件被污染时执行任意 SQL
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: init_action_data_sql_schema
# family: init_action_data_sql_schema
# role: schema_initializer
# version: v0001
# status: active
# entry_point: scripts/action/tools/init_action_data_sql_schema_v0001.py
#
# depends_on:
#   - Python stdlib: sqlite3, json, argparse, pathlib, datetime, typing, re
#   - Optional: PyYAML (yaml) for config loading
#
# used_by:
#   - manual invocation before first action-data ingest
#   - (future) action_data writer scripts (expect tables to exist)
# ============================================================


from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "init_action_data_sql_schema"
SCRIPT_VERSION = "v0001"

MAX_ROOT_SEARCH_DEPTH = 10

# Default database path (relative to project root)
# 与理解端的 sql/data.db 平行,放在同一个 sql/ 目录下,以文件名区分
DEFAULT_DB_PATH = Path("sql") / "action_data.db"

# Default table names (与理解端完全一致)
DEFAULT_TABLES = {
    "concept": "concept_units",
    "instance": "instance_units",
    "attribute": "unit_attributes",
    "run_log": "ingestion_run_log",
}

# Default field names (与理解端完全一致)
DEFAULT_FIELDS = {
    "concept": {
        "id": "unit_text_id",
        "text": "unit_text",
        "hash": "content_hash",
        "first_seen_instance": "first_seen_instance_id",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "instance": {
        "id": "instance_id",
        "concept_id": "unit_text_id",
        "asset_id": "asset_id",
        "path": "path",
        "value_index": "value_index",
        "segment_index": "segment_index",
        "sentence_index": "sentence_index",
        "char_start": "char_start",
        "char_end": "char_end",
        "content": "content",
        "content_hash": "content_hash",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "attribute": {
        "id": "attr_id",
        "object_type": "object_type",
        "object_id": "object_id",
        "attr_scope": "attr_scope",
        "attr_key": "attr_key",
        "attr_value": "attr_value",
        "attr_type": "attr_type",
        "attr_state": "attr_state",
        "evidence_ref": "evidence_ref",
        "created_at": "created_at",
        "created_by": "created_by",
        "run_id": "run_id",
    },
    "run_log": {
        "run_id": "run_id",
        "script_name": "script_name",
        "script_version": "script_version",
        "started_at": "started_at",
        "finished_at": "finished_at",
        "input_hash": "input_hash",
        "output_hash": "output_hash",
        "error_summary": "error_summary",
        "environment_fingerprint": "environment_fingerprint",
    },
}

DEFAULT_PRAGMAS = {
    "journal_mode": "WAL",
    "foreign_keys": True,
    "synchronous": "NORMAL",
}

# Regex for validating SQL identifiers from config
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
    """Ensure a table or field name contains only safe characters."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe SQL identifier from config ({context}): '{name}'. "
            f"Only letters, digits, and underscores are allowed."
        )


def _validate_all_identifiers(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> None:
    """Validate all table names and field names from config."""
    for key, tbl_name in tables.items():
        _validate_identifier(tbl_name, f"tables.{key}")
    for group_key, field_map in fields.items():
        for fkey, fname in field_map.items():
            _validate_identifier(fname, f"fields.{group_key}.{fkey}")


def _load_config(
    config_path: Path,
    project_root: Path,
) -> Tuple[Path, Dict[str, str], Dict[str, Dict[str, str]], Dict[str, Any]]:
    """Load action_data sql schema config YAML. Returns (db_path, tables, fields, pragmas)."""
    try:
        import yaml
    except ImportError:
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_FIELDS, DEFAULT_PRAGMAS

    if not config_path.exists():
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_FIELDS, DEFAULT_PRAGMAS

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_FIELDS, DEFAULT_PRAGMAS

    conn = raw.get("connection", {}) or {}
    sqlite_cfg = conn.get("sqlite", {}) or {}
    db_path_str = sqlite_cfg.get("path", str(DEFAULT_DB_PATH))
    pragmas = sqlite_cfg.get("pragmas", {}) or DEFAULT_PRAGMAS

    tables = raw.get("tables", {}) or DEFAULT_TABLES
    fields = raw.get("fields", {}) or DEFAULT_FIELDS

    # Always resolve relative to project_root
    db_path = (project_root / db_path_str).resolve()

    return db_path, tables, fields, pragmas


# ============================================================
# Schema Definition
# ============================================================

def _build_create_statements(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[Tuple[str, str]]:
    """Build CREATE TABLE and CREATE INDEX statements from config mappings."""

    stmts: List[Tuple[str, str]] = []

    # --- table aliases ---
    tc = tables["concept"]
    ti = tables["instance"]
    ta = tables["attribute"]
    tr = tables["run_log"]

    fc = fields["concept"]
    fi = fields["instance"]
    fa = fields["attribute"]
    fr = fields["run_log"]

    # -------------------------------------------------------
    # 1. concept_units
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {tc}", f"""
CREATE TABLE IF NOT EXISTS {tc} (
    {fc['id']}                    TEXT PRIMARY KEY,
    {fc['text']}                  TEXT NOT NULL,
    {fc['hash']}                  TEXT NOT NULL,
    {fc['first_seen_instance']}   TEXT,
    {fc['created_at']}            TEXT NOT NULL,
    {fc['schema_version']}        TEXT NOT NULL,
    {fc['run_id']}                TEXT NOT NULL
);
"""))

    idx_name = f"idx_{tc}_{fc['hash']}"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {tc} ({fc['hash']});
"""))

    # -------------------------------------------------------
    # 2. instance_units
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {ti}", f"""
CREATE TABLE IF NOT EXISTS {ti} (
    {fi['id']}                TEXT PRIMARY KEY,
    {fi['concept_id']}        TEXT NOT NULL,
    {fi['asset_id']}          TEXT NOT NULL,
    {fi['path']}              TEXT NOT NULL,
    {fi['value_index']}       INTEGER,
    {fi['segment_index']}     INTEGER NOT NULL,
    {fi['sentence_index']}    INTEGER,
    {fi['char_start']}        INTEGER,
    {fi['char_end']}          INTEGER,
    {fi['content']}           TEXT NOT NULL,
    {fi['content_hash']}      TEXT NOT NULL,
    {fi['created_at']}        TEXT NOT NULL,
    {fi['schema_version']}    TEXT NOT NULL,
    {fi['run_id']}            TEXT NOT NULL,
    FOREIGN KEY ({fi['concept_id']}) REFERENCES {tc} ({fc['id']})
);
"""))

    idx_name = f"idx_{ti}_{fi['concept_id']}"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {ti} ({fi['concept_id']});
"""))

    idx_name = f"idx_{ti}_asset_path_seg"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {ti} ({fi['asset_id']}, {fi['path']}, {fi['segment_index']});
"""))

    idx_name = f"idx_{ti}_{fi['content_hash']}"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {ti} ({fi['content_hash']});
"""))

    # -------------------------------------------------------
    # 3. unit_attributes
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {ta}", f"""
CREATE TABLE IF NOT EXISTS {ta} (
    {fa['id']}                TEXT PRIMARY KEY,
    {fa['object_type']}       TEXT NOT NULL,
    {fa['object_id']}         TEXT NOT NULL,
    {fa['attr_scope']}        TEXT,
    {fa['attr_key']}          TEXT NOT NULL,
    {fa['attr_value']}        TEXT,
    {fa['attr_type']}         TEXT,
    {fa['attr_state']}        TEXT NOT NULL DEFAULT 'active',
    {fa['evidence_ref']}      TEXT,
    {fa['created_at']}        TEXT NOT NULL,
    {fa['created_by']}        TEXT NOT NULL,
    {fa['run_id']}            TEXT NOT NULL
);
"""))

    idx_name = f"idx_{ta}_obj_key"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {ta} ({fa['object_type']}, {fa['object_id']}, {fa['attr_key']});
"""))

    idx_name = f"idx_{ta}_{fa['run_id']}"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {ta} ({fa['run_id']});
"""))

    # -------------------------------------------------------
    # 4. ingestion_run_log
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {tr}", f"""
CREATE TABLE IF NOT EXISTS {tr} (
    {fr['run_id']}                   TEXT PRIMARY KEY,
    {fr['script_name']}              TEXT NOT NULL,
    {fr['script_version']}           TEXT NOT NULL,
    {fr['started_at']}               TEXT NOT NULL,
    {fr['finished_at']}              TEXT,
    {fr['input_hash']}               TEXT,
    {fr['output_hash']}              TEXT,
    {fr['error_summary']}            TEXT,
    {fr['environment_fingerprint']}  TEXT
);
"""))

    idx_name = f"idx_{tr}_{fr['started_at']}"
    stmts.append((f"CREATE INDEX {idx_name}", f"""
CREATE INDEX IF NOT EXISTS {idx_name}
    ON {tr} ({fr['started_at']});
"""))

    return stmts


def _build_expected_index_names(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[str]:
    """Build the exact list of expected index names for validation."""
    tc = tables["concept"]
    ti = tables["instance"]
    ta = tables["attribute"]
    tr = tables["run_log"]

    fc = fields["concept"]
    fi = fields["instance"]
    fa = fields["attribute"]
    fr = fields["run_log"]

    return [
        f"idx_{tc}_{fc['hash']}",
        f"idx_{ti}_{fi['concept_id']}",
        f"idx_{ti}_asset_path_seg",
        f"idx_{ti}_{fi['content_hash']}",
        f"idx_{ta}_obj_key",
        f"idx_{ta}_{fa['run_id']}",
        f"idx_{tr}_{fr['started_at']}",
    ]


# ============================================================
# Expected column specs for validation
# ============================================================

def _build_expected_columns(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, List[Tuple[str, str, bool, bool]]]:
    """Build expected column specs: (col_name, type, notnull, is_pk)."""

    fc = fields["concept"]
    fi = fields["instance"]
    fa = fields["attribute"]
    fr = fields["run_log"]

    specs: Dict[str, List[Tuple[str, str, bool, bool]]] = {}

    # concept_units
    specs[tables["concept"]] = [
        (fc["id"],                    "TEXT", True,  True),
        (fc["text"],                  "TEXT", True,  False),
        (fc["hash"],                  "TEXT", True,  False),
        (fc["first_seen_instance"],   "TEXT", False, False),
        (fc["created_at"],            "TEXT", True,  False),
        (fc["schema_version"],        "TEXT", True,  False),
        (fc["run_id"],                "TEXT", True,  False),
    ]

    # instance_units
    specs[tables["instance"]] = [
        (fi["id"],              "TEXT",    True,  True),
        (fi["concept_id"],      "TEXT",    True,  False),
        (fi["asset_id"],        "TEXT",    True,  False),
        (fi["path"],            "TEXT",    True,  False),
        (fi["value_index"],     "INTEGER", False, False),
        (fi["segment_index"],   "INTEGER", True,  False),
        (fi["sentence_index"],  "INTEGER", False, False),
        (fi["char_start"],      "INTEGER", False, False),
        (fi["char_end"],        "INTEGER", False, False),
        (fi["content"],         "TEXT",    True,  False),
        (fi["content_hash"],    "TEXT",    True,  False),
        (fi["created_at"],      "TEXT",    True,  False),
        (fi["schema_version"],  "TEXT",    True,  False),
        (fi["run_id"],          "TEXT",    True,  False),
    ]

    # unit_attributes
    specs[tables["attribute"]] = [
        (fa["id"],            "TEXT", True,  True),
        (fa["object_type"],   "TEXT", True,  False),
        (fa["object_id"],     "TEXT", True,  False),
        (fa["attr_scope"],    "TEXT", False, False),
        (fa["attr_key"],      "TEXT", True,  False),
        (fa["attr_value"],    "TEXT", False, False),
        (fa["attr_type"],     "TEXT", False, False),
        (fa["attr_state"],    "TEXT", True,  False),
        (fa["evidence_ref"],  "TEXT", False, False),
        (fa["created_at"],    "TEXT", True,  False),
        (fa["created_by"],    "TEXT", True,  False),
        (fa["run_id"],        "TEXT", True,  False),
    ]

    # ingestion_run_log
    specs[tables["run_log"]] = [
        (fr["run_id"],                   "TEXT", True,  True),
        (fr["script_name"],              "TEXT", True,  False),
        (fr["script_version"],           "TEXT", True,  False),
        (fr["started_at"],               "TEXT", True,  False),
        (fr["finished_at"],              "TEXT", False, False),
        (fr["input_hash"],               "TEXT", False, False),
        (fr["output_hash"],              "TEXT", False, False),
        (fr["error_summary"],            "TEXT", False, False),
        (fr["environment_fingerprint"],  "TEXT", False, False),
    ]

    return specs


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
    """Create database file and tables. Idempotent."""

    _validate_all_identifiers(tables, fields)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    current_label = "(connection)"
    pragma_results: Dict[str, str] = {}

    try:
        # Apply and verify PRAGMAs
        if pragmas.get("foreign_keys") is True:
            conn.execute("PRAGMA foreign_keys = ON;")
            row = conn.execute("PRAGMA foreign_keys;").fetchone()
            actual = str(row[0]) if row else "unknown"
            pragma_results["foreign_keys"] = actual
            if actual != "1":
                raise RuntimeError(
                    f"PRAGMA foreign_keys expected ON, got {actual}"
                )

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
                run_id = f"init_action_data_sql_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
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
                # Audit failure should not break init
                pass

        return {
            "status": "ok",
            "action": "init",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
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
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
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
    """Validate that existing database matches expected schema."""

    if not db_path.exists():
        return {
            "status": "error",
            "action": "validate",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "error": "database file does not exist",
            "hint": "run with --init first",
            "timestamp": _utc_iso(),
        }

    _validate_all_identifiers(tables, fields)

    conn = sqlite3.connect(str(db_path))
    issues: List[str] = []

    try:
        # Enable foreign keys for accurate foreign_key_check
        conn.execute("PRAGMA foreign_keys = ON;")

        cur = conn.cursor()

        # Build expected column specs
        expected_columns = _build_expected_columns(tables, fields)

        # Check each table exists and validate column structure
        for config_key, table_name in tables.items():
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (table_name,),
            )
            if cur.fetchone() is None:
                issues.append(f"missing table: {table_name}")
                continue

            # Get actual columns via PRAGMA table_info
            # Returns: (cid, name, type, notnull, dflt_value, pk)
            cur.execute(f"PRAGMA table_info({table_name});")
            actual_cols: Dict[str, Tuple[str, bool, bool]] = {}
            for row in cur.fetchall():
                col_name = row[1]
                col_type = str(row[2]).upper() if row[2] else ""
                col_notnull = bool(row[3])
                col_pk = bool(row[5])
                actual_cols[col_name] = (col_type, col_notnull, col_pk)

            # Compare against expected
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

                    # SQLite: PRIMARY KEY columns are implicitly NOT NULL
                    # but PRAGMA table_info may report notnull=0 for PK columns
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

        # Check indexes by exact name
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='index';",
        )
        actual_indexes = {row[0] for row in cur.fetchall()}

        expected_indexes = _build_expected_index_names(tables, fields)
        for idx_name in expected_indexes:
            if idx_name not in actual_indexes:
                issues.append(f"missing index: {idx_name}")

        # Check foreign key integrity
        cur.execute("PRAGMA foreign_key_check;")
        fk_issues = cur.fetchall()
        if fk_issues:
            issues.append(f"foreign key violations: {len(fk_issues)}")

        # Check PRAGMA states
        row = conn.execute("PRAGMA journal_mode;").fetchone()
        if row and str(row[0]).upper() != "WAL":
            issues.append(f"journal_mode is {row[0]}, expected WAL")

        status = "ok" if not issues else "issues_found"

        return {
            "status": status,
            "action": "validate",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
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
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
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
        description="Initialize or validate sql/action_data.db schema (行动端数据四库 SQL 原生层)."
    )
    ap.add_argument(
        "--config",
        default=None,
        help="Path to action_data_sql_schema_config YAML. If omitted, uses built-in defaults.",
    )
    ap.add_argument(
        "--db-path",
        default=None,
        help="Override database file path. If omitted, uses config or default (sql/action_data.db).",
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
        help="Write a run_log record after successful init (optional).",
    )
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    # Resolve project root
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    # Load config (always relative to project_root)
    if args.config:
        config_path = Path(args.config).resolve()
    else:
        config_path = project_root / "config" / "action" / "init_schema" / "action_data_sql_schema_config_v0001.yml"

    db_path, tables, fields, pragmas = _load_config(config_path, project_root)

    # Override db_path if specified via CLI
    if args.db_path:
        db_path = Path(args.db_path).resolve()

    # Default action: validate
    if not args.init and not args.validate:
        args.validate = True

    if args.init:
        result = init_database(
            db_path, tables, fields, pragmas,
            verbose=bool(args.verbose),
            audit=bool(args.audit),
        )
    else:
        result = validate_schema(db_path, tables, fields)

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result["status"] == "error":
        return 1
    if result.get("issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())