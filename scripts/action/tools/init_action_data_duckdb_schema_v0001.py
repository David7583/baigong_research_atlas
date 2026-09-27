#!/usr/bin/env python3
# ============================================================
# File: init_action_data_duckdb_schema_v0001.py
# 中文名: 行动端数据四库 DuckDB 数据库初始化与结构校验脚本
# Version: v0001
# Layer: infrastructure
# Main Layer: action
# Sub Layer: action_data
# Script Type: Schema Management
# Updatable: True
#
# Purpose
#
# 创建 DuckDB 数据库文件与表结构（行动端数据四库 派生层）
# 或校验已有数据库的表结构与预期是否一致
#
# 本脚本是行动端【数据四库】的 DuckDB 派生层建库脚本
# 基于理解端 init_duckdb_schema_v0002 的骨架改造而来
# 表结构在初始阶段与理解端完全同构（仓库先建好，演化交给以后）
#
# 行动端两套四库的区分
# - 脚本四库：承载脚本/功能/功能块/任务的元数据（已存在）
# - 数据四库：承载行动产生的实际数据（本脚本服务于此）
#
#
# What it does
#
# 1 读取 action_data_duckdb_schema_config YAML 获取数据库路径与表名/字段名映射
# 2 创建目录结构（derivation/action/duckdb/, derivation/action/embedding/）
# 3 创建数据库文件与四张表（_sync_log / instance_units_sync / concept_units_sync / attribute_units_sync）
# 4 创建三个视图（instance_units_latest / concept_units_latest / attribute_units_latest）
# 5 校验已有数据库的表结构、字段、视图完整性
# 6 报告已有表的行数（--validate 模式下）
#
#
# What it does NOT do
#
# 1 不写入任何业务数据
# 2 不执行同步操作
# 3 不做分析计算
# 4 不修改 config 文件
# 5 不创建理解端的数据库（那是另一个脚本的职责）
# 6 不操作行动端脚本四库
# 7 不删除或重建已有表
#
#
# Design decision
#
# 幂等性保证
#
# 所有 CREATE TABLE 使用 IF NOT EXISTS
# 所有 CREATE VIEW 使用 CREATE OR REPLACE
# 重复运行不会破坏已有数据
#
# Config 驱动
#
# 表名与字段名从 YAML config 中读取
# 若 config 文件不存在，则使用内置默认值
#
# 与理解端的对称性
#
# 表结构、字段、视图在初始阶段与理解端完全一致
# 唯一区别是数据库文件路径和派生目录
#   理解端: derivation/understand/duckdb/understand_analysis.duckdb
#   行动端: derivation/action/duckdb/action_data_analysis.duckdb
# 各端的库会在各自的演化中逐渐分化（仓库先建好，演化交给以后）
#
# 字段名安全
#
# 所有从 config 读取的表名与字段名
# 必须通过合法性检查（仅允许字母、数字、下划线）
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: init_action_data_duckdb_schema
# family: init_action_data_duckdb_schema
# role: schema_initializer
# version: v0001
# status: active
# entry_point: scripts/action/tools/init_action_data_duckdb_schema_v0001.py
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, re, sys, traceback
#   - Third-party: duckdb, PyYAML (yaml)
#
# used_by:
#   - manual invocation before first action-data sync
#   - (future) action_data sync scripts (expect tables to exist)
# ============================================================


from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "init_action_data_duckdb_schema_v0001.py"
SCRIPT_VERSION = "v0001"

MAX_ROOT_SEARCH_DEPTH = 10

# Default database path (relative to project root)
# 与理解端 derivation/understand/duckdb/understand_analysis.duckdb 平行
DEFAULT_DB_PATH = Path("derivation") / "action" / "duckdb" / "action_data_analysis.duckdb"

# Default table names (与理解端完全一致)
DEFAULT_TABLES = {
    "sync_log": "_sync_log",
    "instance_sync": "instance_units_sync",
    "concept_sync": "concept_units_sync",
    "attribute_sync": "attribute_units_sync",
}

# Default view names (与理解端完全一致)
DEFAULT_VIEWS = {
    "instance_latest": "instance_units_latest",
    "concept_latest": "concept_units_latest",
    "attribute_latest": "attribute_units_latest",
}

# Default field names (与理解端完全一致)
DEFAULT_FIELDS = {
    "sync_log": {
        "sync_id": "sync_id",
        "source_db": "source_db",
        "source_table": "source_table",
        "record_count": "record_count",
        "started_at": "started_at",
        "finished_at": "finished_at",
        "script_name": "script_name",
        "script_version": "script_version",
        "source_db_hash": "source_db_hash",
    },
    "instance_sync": {
        "sync_id": "sync_id",
        "synced_at": "synced_at",
        "instance_id": "instance_id",
        "unit_text_id": "unit_text_id",
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
    "concept_sync": {
        "sync_id": "sync_id",
        "synced_at": "synced_at",
        "unit_text_id": "unit_text_id",
        "unit_text": "unit_text",
        "content_hash": "content_hash",
        "first_seen_instance_id": "first_seen_instance_id",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "attribute_sync": {
        "sync_id": "sync_id",
        "synced_at": "synced_at",
        "attr_id": "attr_id",
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
}

# Directory structure to create
# 本脚本只创建行动端自己的目录,不再创建理解端目录(那是理解端脚本的职责)
DEFAULT_DIRECTORIES = [
    Path("derivation") / "action" / "duckdb",
    Path("derivation") / "action" / "embedding",
]

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
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"Empty or non-string SQL identifier from config ({context})."
        )
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe SQL identifier from config ({context}): '{name}'. "
            f"Only letters, digits, and underscores are allowed."
        )


def _validate_all_identifiers(
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> None:
    """Validate all table, view, and field names from config."""
    for key, tbl_name in tables.items():
        _validate_identifier(tbl_name, f"tables.{key}")
    for key, view_name in views.items():
        _validate_identifier(view_name, f"views.{key}")
    for group_key, field_map in fields.items():
        for fkey, fname in field_map.items():
            _validate_identifier(fname, f"fields.{group_key}.{fkey}")


def _check_required_field_groups(fields: Dict[str, Dict[str, str]]) -> List[str]:
    """Check that all required field groups are present and non-empty."""
    issues = []
    required_groups = ["sync_log", "instance_sync", "concept_sync", "attribute_sync"]
    for group in required_groups:
        if group not in fields:
            issues.append(f"missing required field group: {group}")
        elif not fields[group]:
            issues.append(f"empty field group: {group}")
    return issues


def _deep_copy_fields(fields: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """Deep copy the fields dict to prevent mutation of defaults."""
    return {k: dict(v) for k, v in fields.items()}


def _load_config(
    config_path: Path,
    project_root: Path,
    verbose: bool = False,
) -> Tuple[Path, Dict[str, str], Dict[str, str], Dict[str, Dict[str, str]], List[str]]:
    """Load action_data_duckdb schema config YAML.

    Returns (db_path, tables, views, fields, warnings).
    Falls back to defaults on missing config. Raises on malformed config.
    """
    warnings: List[str] = []

    try:
        import yaml
    except ImportError:
        warnings.append("PyYAML not installed, using built-in defaults for all config values.")
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES.copy(), DEFAULT_VIEWS.copy(), _deep_copy_fields(DEFAULT_FIELDS), warnings

    if not config_path.exists():
        warnings.append(f"Config file not found: {config_path}. Using built-in defaults.")
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES.copy(), DEFAULT_VIEWS.copy(), _deep_copy_fields(DEFAULT_FIELDS), warnings

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to parse config YAML at {config_path}: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError(f"Config file must be a YAML mapping (dict), got {type(raw).__name__} at {config_path}")

    if verbose:
        print(f"[CONFIG] Loaded: {config_path}")

    # --- connection ---
    conn = raw.get("connection", {}) or {}
    duckdb_cfg = conn.get("duckdb", {}) or {}
    db_path_str = duckdb_cfg.get("path", str(DEFAULT_DB_PATH))

    # --- tables (merge with defaults to ensure new keys are present) ---
    tables = DEFAULT_TABLES.copy()
    raw_tables = raw.get("tables")
    if isinstance(raw_tables, dict):
        tables.update(raw_tables)
    elif raw_tables is not None:
        warnings.append(f"Config 'tables' is not a mapping ({type(raw_tables).__name__}), using defaults.")

    # --- views (merge with defaults) ---
    views = DEFAULT_VIEWS.copy()
    views_cfg = raw.get("views")
    if isinstance(views_cfg, dict):
        for key, view_def in views_cfg.items():
            if isinstance(view_def, dict) and "name" in view_def:
                views[key] = view_def["name"]
            elif isinstance(view_def, str):
                views[key] = view_def
            else:
                warnings.append(f"Config views.{key} has unexpected format, using default.")
    elif views_cfg is not None:
        warnings.append(f"Config 'views' is not a mapping ({type(views_cfg).__name__}), using defaults.")

    # --- fields (merge with defaults) ---
    fields = _deep_copy_fields(DEFAULT_FIELDS)
    raw_fields = raw.get("fields")
    if isinstance(raw_fields, dict):
        for group_key, group_val in raw_fields.items():
            if isinstance(group_val, dict):
                if group_key not in fields:
                    fields[group_key] = {}
                fields[group_key].update(group_val)
            else:
                warnings.append(f"Config fields.{group_key} is not a mapping, skipping.")
    elif raw_fields is not None:
        warnings.append(f"Config 'fields' is not a mapping ({type(raw_fields).__name__}), using defaults.")

    # Always resolve relative to project_root
    db_path = (project_root / db_path_str).resolve()

    return db_path, tables, views, fields, warnings


def _ensure_directories(project_root: Path, verbose: bool = False) -> List[str]:
    """Create the action_data derivation directory structure."""
    created = []
    for rel_dir in DEFAULT_DIRECTORIES:
        abs_dir = project_root / rel_dir
        if not abs_dir.exists():
            abs_dir.mkdir(parents=True, exist_ok=True)
            created.append(str(rel_dir))
            if verbose:
                print(f"[DIR] Created: {rel_dir}")

        # Add .gitkeep to empty directories
        gitkeep = abs_dir / ".gitkeep"
        if not gitkeep.exists():
            try:
                if not any(abs_dir.iterdir()):
                    gitkeep.write_text("# Placeholder for empty directory\n", encoding="utf-8")
            except OSError:
                pass

    return created


# ============================================================
# Schema Definition
# ============================================================

def _build_create_statements(
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[Tuple[str, str]]:
    """Build CREATE TABLE and CREATE VIEW statements from config mappings."""

    stmts: List[Tuple[str, str]] = []

    # --- table aliases ---
    t_sync_log = tables["sync_log"]
    t_instance = tables["instance_sync"]
    t_concept = tables["concept_sync"]
    t_attribute = tables["attribute_sync"]

    f_sync = fields["sync_log"]
    f_inst = fields["instance_sync"]
    f_conc = fields["concept_sync"]
    f_attr = fields["attribute_sync"]

    v_inst_latest = views["instance_latest"]
    v_conc_latest = views["concept_latest"]
    v_attr_latest = views["attribute_latest"]

    # -------------------------------------------------------
    # 1. _sync_log
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_sync_log}", f"""
CREATE TABLE IF NOT EXISTS {t_sync_log} (
    {f_sync['sync_id']}           VARCHAR PRIMARY KEY,
    {f_sync['source_db']}         VARCHAR NOT NULL,
    {f_sync['source_table']}      VARCHAR NOT NULL,
    {f_sync['record_count']}      INTEGER NOT NULL,
    {f_sync['started_at']}        TIMESTAMP NOT NULL,
    {f_sync['finished_at']}       TIMESTAMP,
    {f_sync['script_name']}       VARCHAR NOT NULL,
    {f_sync['script_version']}    VARCHAR NOT NULL,
    {f_sync['source_db_hash']}    VARCHAR
);
"""))

    # -------------------------------------------------------
    # 2. instance_units_sync
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_instance}", f"""
CREATE TABLE IF NOT EXISTS {t_instance} (
    {f_inst['sync_id']}           VARCHAR NOT NULL,
    {f_inst['synced_at']}         TIMESTAMP NOT NULL,
    {f_inst['instance_id']}       VARCHAR NOT NULL,
    {f_inst['unit_text_id']}      VARCHAR NOT NULL,
    {f_inst['asset_id']}          VARCHAR NOT NULL,
    {f_inst['path']}              VARCHAR NOT NULL,
    {f_inst['value_index']}       INTEGER,
    {f_inst['segment_index']}     INTEGER NOT NULL,
    {f_inst['sentence_index']}    INTEGER,
    {f_inst['char_start']}        INTEGER,
    {f_inst['char_end']}          INTEGER,
    {f_inst['content']}           VARCHAR NOT NULL,
    {f_inst['content_hash']}      VARCHAR NOT NULL,
    {f_inst['created_at']}        VARCHAR NOT NULL,
    {f_inst['schema_version']}    VARCHAR NOT NULL,
    {f_inst['run_id']}            VARCHAR NOT NULL,
    PRIMARY KEY ({f_inst['sync_id']}, {f_inst['instance_id']})
);
"""))

    # -------------------------------------------------------
    # 3. concept_units_sync
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_concept}", f"""
CREATE TABLE IF NOT EXISTS {t_concept} (
    {f_conc['sync_id']}                 VARCHAR NOT NULL,
    {f_conc['synced_at']}               TIMESTAMP NOT NULL,
    {f_conc['unit_text_id']}            VARCHAR NOT NULL,
    {f_conc['unit_text']}               VARCHAR NOT NULL,
    {f_conc['content_hash']}            VARCHAR NOT NULL,
    {f_conc['first_seen_instance_id']}  VARCHAR,
    {f_conc['created_at']}              VARCHAR NOT NULL,
    {f_conc['schema_version']}          VARCHAR NOT NULL,
    {f_conc['run_id']}                  VARCHAR NOT NULL,
    PRIMARY KEY ({f_conc['sync_id']}, {f_conc['unit_text_id']})
);
"""))

    # -------------------------------------------------------
    # 4. attribute_units_sync
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_attribute}", f"""
CREATE TABLE IF NOT EXISTS {t_attribute} (
    {f_attr['sync_id']}           VARCHAR NOT NULL,
    {f_attr['synced_at']}         TIMESTAMP NOT NULL,
    {f_attr['attr_id']}           VARCHAR NOT NULL,
    {f_attr['object_type']}       VARCHAR NOT NULL,
    {f_attr['object_id']}         VARCHAR NOT NULL,
    {f_attr['attr_scope']}        VARCHAR,
    {f_attr['attr_key']}          VARCHAR NOT NULL,
    {f_attr['attr_value']}        VARCHAR,
    {f_attr['attr_type']}         VARCHAR,
    {f_attr['attr_state']}        VARCHAR NOT NULL,
    {f_attr['evidence_ref']}      VARCHAR,
    {f_attr['created_at']}        VARCHAR NOT NULL,
    {f_attr['created_by']}        VARCHAR NOT NULL,
    {f_attr['run_id']}            VARCHAR NOT NULL,
    PRIMARY KEY ({f_attr['sync_id']}, {f_attr['attr_id']})
);
"""))

    # -------------------------------------------------------
    # 5. instance_units_latest view
    # -------------------------------------------------------
    stmts.append((f"CREATE VIEW {v_inst_latest}", f"""
CREATE OR REPLACE VIEW {v_inst_latest} AS
SELECT * FROM {t_instance}
WHERE {f_inst['sync_id']} = (
    SELECT {f_sync['sync_id']}
    FROM {t_sync_log}
    WHERE {f_sync['source_table']} = 'instance_units'
    ORDER BY {f_sync['finished_at']} DESC
    LIMIT 1
);
"""))

    # -------------------------------------------------------
    # 6. concept_units_latest view
    # -------------------------------------------------------
    stmts.append((f"CREATE VIEW {v_conc_latest}", f"""
CREATE OR REPLACE VIEW {v_conc_latest} AS
SELECT * FROM {t_concept}
WHERE {f_conc['sync_id']} = (
    SELECT {f_sync['sync_id']}
    FROM {t_sync_log}
    WHERE {f_sync['source_table']} = 'concept_units'
    ORDER BY {f_sync['finished_at']} DESC
    LIMIT 1
);
"""))

    # -------------------------------------------------------
    # 7. attribute_units_latest view
    # -------------------------------------------------------
    stmts.append((f"CREATE VIEW {v_attr_latest}", f"""
CREATE OR REPLACE VIEW {v_attr_latest} AS
SELECT * FROM {t_attribute}
WHERE {f_attr['sync_id']} = (
    SELECT {f_sync['sync_id']}
    FROM {t_sync_log}
    WHERE {f_sync['source_table']} = 'unit_attributes'
    ORDER BY {f_sync['finished_at']} DESC
    LIMIT 1
);
"""))

    return stmts


# ============================================================
# Init
# ============================================================

def init_database(
    db_path: Path,
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    project_root: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Initialize DuckDB database with schema. Idempotent and safe on existing databases."""

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "action": "init",
            "error": "duckdb module not installed. Run: pip install duckdb",
            "timestamp": _utc_iso(),
        }

    current_label = "setup"
    conn = None

    try:
        # Validate identifiers before touching anything
        _validate_all_identifiers(tables, views, fields)

        # Check required field groups
        group_issues = _check_required_field_groups(fields)
        if group_issues:
            return {
                "status": "error",
                "action": "init",
                "error": f"Config validation failed: {'; '.join(group_issues)}",
                "timestamp": _utc_iso(),
            }

        # Create directory structure
        dirs_created = _ensure_directories(project_root, verbose=verbose)

        # Ensure database directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Detect if database already exists (for reporting)
        db_existed = db_path.exists()

        # Build SQL statements
        statements = _build_create_statements(tables, views, fields)

        # Connect and execute
        current_label = "connect"
        conn = duckdb.connect(str(db_path))

        executed = []
        failed: List[Dict[str, str]] = []

        for label, sql in statements:
            current_label = label
            if verbose:
                print(f"[SQL] {label}")
            try:
                conn.execute(sql)
                executed.append(label)
            except Exception as e:
                error_msg = str(e)[:300]
                failed.append({"label": label, "error": error_msg})
                if verbose:
                    print(f"[WARN] {label} failed: {error_msg}")

        # Post-init: count rows in each table for sanity check
        row_counts = {}
        for config_key, table_name in tables.items():
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_counts[table_name] = result[0] if result else 0
            except Exception:
                row_counts[table_name] = -1

        conn.close()
        conn = None

        status = "ok" if not failed else "partial"

        return {
            "status": status,
            "action": "init",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "db_existed_before": db_existed,
            "tables_expected": list(tables.values()),
            "views_expected": list(views.values()),
            "statements_executed": executed,
            "statements_failed": failed,
            "row_counts": row_counts,
            "directories_created": dirs_created,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "action": "init",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "failed_at": current_label,
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-800:],
            "timestamp": _utc_iso(),
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# Validate
# ============================================================

def validate_schema(
    db_path: Path,
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    verbose: bool = False,
) -> Dict[str, Any]:
    """Validate that existing database matches expected schema.

    Reports missing tables, missing columns, missing views, and row counts.
    """

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "action": "validate",
            "error": "duckdb module not installed. Run: pip install duckdb",
            "timestamp": _utc_iso(),
        }

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

    try:
        _validate_all_identifiers(tables, views, fields)
    except ValueError as e:
        return {
            "status": "error",
            "action": "validate",
            "error": f"Identifier validation failed: {e}",
            "timestamp": _utc_iso(),
        }

    issues: List[str] = []
    row_counts: Dict[str, int] = {}
    conn = None

    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        # Get list of tables
        result = conn.execute("SHOW TABLES").fetchall()
        actual_tables = {row[0] for row in result}

        if verbose:
            print(f"[VALIDATE] Tables in database: {sorted(actual_tables)}")

        # Check each expected table exists and has expected columns
        for config_key, table_name in tables.items():
            if table_name not in actual_tables:
                issues.append(f"missing table: {table_name}")
                row_counts[table_name] = -1
                continue

            # Get actual columns
            try:
                col_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                actual_cols = {row[0] for row in col_result}
            except Exception as e:
                issues.append(f"table {table_name}: DESCRIBE failed: {str(e)[:100]}")
                row_counts[table_name] = -1
                continue

            # Check expected columns exist
            if config_key in fields:
                for fkey, fname in fields[config_key].items():
                    if fname not in actual_cols:
                        issues.append(f"table {table_name}: missing column: {fname}")

                # Report unexpected columns (informational, not an issue)
                expected_cols = set(fields[config_key].values())
                extra_cols = actual_cols - expected_cols
                if extra_cols and verbose:
                    print(f"[INFO] table {table_name}: extra columns (not in config): {sorted(extra_cols)}")

            # Row count
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_counts[table_name] = cnt[0] if cnt else 0
            except Exception:
                row_counts[table_name] = -1

        # Check views using duckdb_views() system table
        try:
            view_result = conn.execute(
                "SELECT view_name FROM duckdb_views() WHERE NOT internal"
            ).fetchall()
            actual_views = {row[0] for row in view_result}
        except Exception:
            # Fallback: try SELECT LIMIT 0 on each view
            actual_views = set()
            for view_key, view_name in views.items():
                try:
                    conn.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    actual_views.add(view_name)
                except Exception:
                    pass

        for view_key, view_name in views.items():
            if view_name not in actual_views:
                issues.append(f"missing or invalid view: {view_name}")
            elif verbose:
                try:
                    cnt = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
                    print(f"[VIEW] {view_name}: {cnt[0] if cnt else 0} rows")
                except Exception:
                    print(f"[VIEW] {view_name}: query failed (view might reference empty sync_log)")

        conn.close()
        conn = None

        status = "ok" if not issues else "issues_found"

        return {
            "status": status,
            "action": "validate",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "tables_checked": list(tables.values()),
            "views_checked": list(views.values()),
            "row_counts": row_counts,
            "issues": issues,
            "issues_count": len(issues),
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
            "traceback": traceback.format_exc()[-800:],
            "timestamp": _utc_iso(),
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Initialize or validate DuckDB schema (行动端数据四库 派生层)."
    )
    ap.add_argument(
        "--config",
        default=None,
        help="Path to action_data_duckdb_schema_config YAML. If omitted, uses built-in defaults.",
    )
    ap.add_argument(
        "--db-path",
        default=None,
        help="Override database file path. If omitted, uses config or default.",
    )

    group = ap.add_mutually_exclusive_group()
    group.add_argument(
        "--init",
        action="store_true",
        help="Create database and tables (idempotent, safe on existing databases).",
    )
    group.add_argument(
        "--validate",
        action="store_true",
        help="Validate existing database schema and report row counts.",
    )

    ap.add_argument("--verbose", action="store_true", help="Print detailed progress.")
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
        config_path = project_root / "config" / "action" / "init_schema" / "action_data_duckdb_schema_config_v0001.yml"

    try:
        db_path, tables, views, fields, config_warnings = _load_config(
            config_path, project_root, verbose=bool(args.verbose)
        )
    except (ValueError, OSError) as e:
        print(json.dumps({
            "status": "error",
            "action": "load_config",
            "error": str(e)[:500],
            "config_path": str(config_path),
            "timestamp": _utc_iso(),
        }, ensure_ascii=False, indent=2))
        return 1

    # Override db_path if specified via CLI
    if args.db_path:
        db_path = Path(args.db_path).resolve()

    # Default action: validate
    if not args.init and not args.validate:
        args.validate = True

    if args.init:
        result = init_database(
            db_path, tables, views, fields, project_root,
            verbose=bool(args.verbose),
        )
    else:
        result = validate_schema(
            db_path, tables, views, fields,
            verbose=bool(args.verbose),
        )

    # Attach config warnings to result
    if config_warnings:
        result["config_warnings"] = config_warnings

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result["status"] == "error":
        return 1
    if result.get("issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())