#!/usr/bin/env python3
# ============================================================
# File: init_duckdb_schema_v0001.py
# 中文名: DuckDB 派生层理解端数据库初始化与结构校验脚本
# Version: v0001
#
# Layer: infrastructure
# Main Layer: understand
# Script Type: Schema Management
# Updatable: True
#
# Purpose
#
# 创建 DuckDB 数据库文件与表结构（派生层理解端分析子系统）
# 或校验已有数据库的表结构与预期是否一致
#
# 本脚本遵循派生层手册 v0003 的设计原则
# 并与 duckdb_schema_config_v0001.yml 的字段映射保持对齐
#
#
# What it does
#
# 1 读取 duckdb_schema_config YAML 获取数据库路径与表名/字段名映射
# 2 创建目录结构（derivation/understand/duckdb/, derivation/understand/embedding/）
# 3 创建数据库文件与三张表（_sync_log / instance_units_sync / concept_units_sync）
# 4 创建两个视图（instance_units_latest / concept_units_latest）
# 5 校验已有数据库的表结构、字段、视图完整性
#
#
# What it does NOT do
#
# 1 不写入任何业务数据
# 2 不执行同步操作
# 3 不做分析计算
# 4 不修改 config 文件
# 5 不创建 action 端的数据库（那是另一个脚本的职责）
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
# 表名与字段名从 duckdb_schema_config YAML 中读取
# 确保建库脚本与同步脚本使用完全相同的名称
# 若 config 文件不存在，则使用内置默认值
#
# 路径解析
#
# 始终以 _find_project_root 找到的 project_root 为基准
# config 中的 duckdb path 作为相对 project_root 的路径解释
#
# 字段名安全
#
# 所有从 config 读取的表名与字段名
# 必须通过合法性检查（仅允许字母、数字、下划线）
# 防止配置文件被污染时执行任意 SQL
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: init_duckdb_schema
# family: init_duckdb_schema
# role: schema_initializer
# version: v0001
# status: active
# entry_point: scripts/tools/init_duckdb_schema_v0001.py
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, re
#   - Third-party: duckdb, PyYAML (yaml)
#
# used_by:
#   - manual invocation before first sync
#   - sync_sql_to_duckdb_v0001.py (expects tables to exist)
# ============================================================


from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "init_duckdb_schema_v0001.py"
SCRIPT_VERSION = "v0001"

MAX_ROOT_SEARCH_DEPTH = 10

# Default database path (relative to project root)
DEFAULT_DB_PATH = Path("derivation") / "understand" / "duckdb" / "understand_analysis.duckdb"

# Default table names
DEFAULT_TABLES = {
    "sync_log": "_sync_log",
    "instance_sync": "instance_units_sync",
    "concept_sync": "concept_units_sync",
}

# Default view names
DEFAULT_VIEWS = {
    "instance_latest": "instance_units_latest",
    "concept_latest": "concept_units_latest",
}

# Default field names
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
}

# Directory structure to create
DEFAULT_DIRECTORIES = [
    Path("derivation") / "understand" / "duckdb",
    Path("derivation") / "understand" / "embedding",
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
) -> Tuple[Path, Dict[str, str], Dict[str, str], Dict[str, Dict[str, str]]]:
    """Load duckdb_schema config YAML. Returns (db_path, tables, views, fields)."""
    try:
        import yaml
    except ImportError:
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_VIEWS, DEFAULT_FIELDS

    if not config_path.exists():
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_VIEWS, DEFAULT_FIELDS

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_VIEWS, DEFAULT_FIELDS

    conn = raw.get("connection", {}) or {}
    duckdb_cfg = conn.get("duckdb", {}) or {}
    db_path_str = duckdb_cfg.get("path", str(DEFAULT_DB_PATH))

    tables = raw.get("tables", {}) or DEFAULT_TABLES
    
    # Extract view names from views config
    views_cfg = raw.get("views", {}) or {}
    views = {}
    for key, view_def in views_cfg.items():
        if isinstance(view_def, dict) and "name" in view_def:
            views[key] = view_def["name"]
        else:
            views[key] = DEFAULT_VIEWS.get(key, f"{key}_view")
    if not views:
        views = DEFAULT_VIEWS

    fields = raw.get("fields", {}) or DEFAULT_FIELDS

    # Always resolve relative to project_root
    db_path = (project_root / db_path_str).resolve()

    return db_path, tables, views, fields


def _ensure_directories(project_root: Path, verbose: bool = False) -> List[str]:
    """Create the derivation directory structure."""
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
        if not gitkeep.exists() and not any(abs_dir.iterdir()):
            gitkeep.write_text("# Placeholder for empty directory\n", encoding="utf-8")
    
    return created


# ============================================================
# Schema Definition
# ============================================================

def _build_create_statements(
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[Tuple[str, str]]:
    """Build CREATE TABLE and CREATE VIEW statements from config mappings.

    Returns list of (label, sql) tuples for diagnostics.
    """

    stmts: List[Tuple[str, str]] = []

    # --- table aliases ---
    t_sync_log = tables["sync_log"]
    t_instance = tables["instance_sync"]
    t_concept = tables["concept_sync"]

    f_sync = fields["sync_log"]
    f_inst = fields["instance_sync"]
    f_conc = fields["concept_sync"]

    v_inst_latest = views["instance_latest"]
    v_conc_latest = views["concept_latest"]

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
    # 4. instance_units_latest view
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
    # 5. concept_units_latest view
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
    """Initialize DuckDB database with schema."""

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

    try:
        _validate_all_identifiers(tables, fields)

        # Create directory structure
        dirs_created = _ensure_directories(project_root, verbose=verbose)

        # Ensure database directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Build SQL statements
        statements = _build_create_statements(tables, views, fields)

        # Connect and execute
        conn = duckdb.connect(str(db_path))

        executed = []
        for label, sql in statements:
            current_label = label
            if verbose:
                print(f"[SQL] {label}")
            conn.execute(sql)
            executed.append(label)

        conn.close()

        return {
            "status": "ok",
            "action": "init",
            "db_path": str(db_path),
            "tables_created": list(tables.values()),
            "views_created": list(views.values()),
            "directories_created": dirs_created,
            "statements_executed": executed,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "action": "init",
            "db_path": str(db_path),
            "failed_at": current_label,
            "error": str(e)[:500],
            "timestamp": _utc_iso(),
        }


# ============================================================
# Validate
# ============================================================

def validate_schema(
    db_path: Path,
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """Validate that existing database matches expected schema."""

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
            "db_path": str(db_path),
            "error": "database file does not exist",
            "hint": "run with --init first",
            "timestamp": _utc_iso(),
        }

    _validate_all_identifiers(tables, fields)

    issues: List[str] = []

    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        # Get list of tables
        result = conn.execute("SHOW TABLES").fetchall()
        actual_tables = {row[0] for row in result}

        # Check each expected table exists
        for config_key, table_name in tables.items():
            if table_name not in actual_tables:
                issues.append(f"missing table: {table_name}")
                continue

            # Get actual columns
            col_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
            actual_cols = {row[0] for row in col_result}

            # Check expected columns exist
            if config_key in fields:
                for fkey, fname in fields[config_key].items():
                    if fname not in actual_cols:
                        issues.append(f"table {table_name}: missing column: {fname}")

        # Check views
        # DuckDB: views are also in SHOW TABLES or we can query duckdb_views()
        for view_key, view_name in views.items():
            try:
                conn.execute(f"SELECT * FROM {view_name} LIMIT 0")
            except Exception:
                issues.append(f"missing or invalid view: {view_name}")

        conn.close()

        status = "ok" if not issues else "issues_found"

        return {
            "status": status,
            "action": "validate",
            "db_path": str(db_path),
            "tables_checked": list(tables.values()),
            "views_checked": list(views.values()),
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


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Initialize or validate DuckDB schema (派生层理解端分析子系统)."
    )
    ap.add_argument(
        "--config",
        default=None,
        help="Path to duckdb_schema_config YAML. If omitted, uses built-in defaults.",
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
        help="Create database and tables (idempotent).",
    )
    group.add_argument(
        "--validate",
        action="store_true",
        help="Validate existing database schema.",
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
        config_path = project_root / "config" / "duckdb_schema_config_v0001.yml"

    db_path, tables, views, fields = _load_config(config_path, project_root)

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
        result = validate_schema(db_path, tables, views, fields)

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result["status"] == "error":
        return 1
    if result.get("issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
