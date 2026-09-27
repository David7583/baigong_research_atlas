#!/usr/bin/env python3
# ============================================================
# File: init_action_schema_v0002.py
# 中文名: 行动端建表脚本
# Version: v0002
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose
# 在已存在的 action.db（容器层）上建立四组共 13 张表的 schema。
# Schema 完全由四份声明型 config 驱动，不从扫描数据派生。
# 派生型字段漂移检测留给入库 writer 在运行时报 warning。
#
# What it does
# 1) 加载四份 group config（script / function / function_block / task）
# 2) 校验 config 结构（每张表的必填字段、主键、索引声明）
# 3) 按声明顺序对 13 张表进行 schema 落地：
#    - 表不存在：CREATE TABLE
#    - 表已存在：检测列差异，缺失列 ALTER TABLE ADD COLUMN（只增不减）
# 4) 创建声明的索引（IF NOT EXISTS）
# 5) 执行每张表的 post_create_actions（如有），目前支持 insert_initial_records
# 6) 全部 DDL 用同一个事务包裹，要么全成功要么全回滚
# 7) 输出 run_meta 审计摘要
#
# What it does NOT do
# 1) 不建库（这是 init_action_db_v0001 的事）
# 2) 不写业务数据（writer 们的事）
# 3) 不删表、不删列（只增不减）
# 4) 不修改已存在列的类型（不可逆变更不在范围内）
# 5) 不解析 ALIAS_META（schema 与 ALIAS_META 解耦）
# 6) 不开启外键约束（外键策略由 PRAGMA 单独决定，本脚本不改 PRAGMA）
# 7) 不做交互
#
# Non-v0001 targets (deferred, still recorded here)
# 1) schema 漂移检测脚本（detect_schema_drift）：
#    对照 config 与 db 实际状态，报告差异
# 2) 列类型变更通道（drop+create 模式 + 数据迁移），需要明确制度审批
# 3) 外键约束的统一管理（开关、级联策略）
# 4) 索引使用情况统计与无效索引清理
#
# Notes
# - 本脚本是建表器，不是建库器，也不是业务 writer
# - Schema 是制度声明，不是从扫描数据派生的副产物
#   派生型字段漂移留给入库 writer 在 run_meta 里报 warning
# - 仅依赖 PyYAML 与 Python 标准库
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: init_action_schema
# family: init_action_schema
# role: schema_initializer
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/init_action_schema_v0002.py
# input:
#   - config/action/init_schema/script_group.yml
#   - config/action/init_schema/function_group.yml
#   - config/action/init_schema/function_block_group.yml
#   - config/action/init_schema/task_group.yml
#   - --by (optional, CLI)
#   - --reason (optional, CLI)
#   - --dry-run (optional, CLI)
# output:
#   - sql/action.db (DDL applied)
#   - logs/action_schema_init.jsonl (append-only event log)
#   - actioning/init_action_schema/run_meta.json
# depends_on:
#   - init_action_db_v0001
# used_by:
#   - register_script_v0001 (future)
#   - register_function_v0001 (future)
#   - register_function_block_v0001 (future)
#   - register_task_v0001 (future)
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 本脚本只负责建表与加列，不写入业务数据
# - 只增不减：从不删表、从不删列、从不改列类型
# - DDL 用单一事务包裹，要么全成功要么全回滚
# - dry-run 下不得修改数据库或写任何文件
# - schema 完全由 config 声明，不读 JSONL 或其他扫描数据
# - 单次运行幂等：重复运行同一份 config 不应产生 schema 变化
# - 表/列名严格校验为合法标识符，避免 SQL 注入与意外字符
# ============================================================

from __future__ import annotations

# ============================================================
# Imports
# ============================================================
import argparse
import json
import os
import re
import socket
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 第三方依赖: PyYAML
# - dry-run 下不得执行任何 DDL 或写任何文件
# - 不删表、不删列、不修改列类型
# - 表名、列名、索引名严格校验为标识符（防 SQL 注入兜底）
# - DDL 在单一事务中执行；失败即回滚

# ============================================================
# 路径常量区（项目级约定，不通过 config 暴露）
# ============================================================
ACTION_DB_PATH = Path("sql/action.db")
INIT_SCHEMA_CONFIG_DIR = Path("config/action/init_schema")
GROUP_CONFIG_FILES = [
    "script_group.yml",
    "function_group.yml",
    "function_block_group.yml",
    "task_group.yml",
]
EVENT_LOG_PATH = Path("logs/action_schema_init.jsonl")
RUN_META_PATH = Path("actioning/init_action_schema/run_meta.json")

# ============================================================
# 常量与全局配置区
# ============================================================
SCRIPT_NAME = "init_action_schema_v0002.py"
SCRIPT_VERSION = "v0002"
MAIN_LAYER = "action"

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

# 标识符合法性正则（表名、列名、索引名共用）
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

# 允许的列类型（白名单，避免随手写出奇怪类型）
ALLOWED_COLUMN_TYPES = {"TEXT", "INTEGER", "REAL", "BLOB", "NUMERIC"}

# 支持的 post_create_action 类型
SUPPORTED_POST_ACTIONS = {"insert_initial_records"}


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def dumps_json_compact(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def get_machine_name() -> str:
    return socket.gethostname()


def get_current_user() -> str:
    try:
        return os.getlogin()
    except OSError:
        return "unknown"


def validate_identifier(name: str, kind: str) -> None:
    """校验 SQL 标识符。失败时抛 ValueError。"""
    if not isinstance(name, str) or not IDENTIFIER_PATTERN.match(name):
        raise ValueError(
            f"Invalid {kind} identifier: {name!r}. "
            f"Must match {IDENTIFIER_PATTERN.pattern}"
        )


def validate_column_type(col_type: str, column_name: str) -> None:
    """校验列类型在白名单中。"""
    if col_type not in ALLOWED_COLUMN_TYPES:
        raise ValueError(
            f"Column {column_name!r}: type {col_type!r} not in allowed set "
            f"{sorted(ALLOWED_COLUMN_TYPES)}"
        )


# ============================================================
# Config 加载与校验
# ============================================================
def load_yaml(path: Path) -> Dict[str, Any]:
    """加载 yaml 文件。失败时带文件路径抛 ValueError。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise ValueError(f"Config file not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error in {path}: {e}")

    if data is None:
        raise ValueError(f"Config file is empty: {path}")

    if not isinstance(data, dict):
        raise ValueError(f"Config file root must be a mapping: {path}")

    return data


def validate_table_spec(table: Dict[str, Any], source: str) -> None:
    """
    校验单张表的 spec 结构合法。
    source 用于错误信息定位。
    """
    if not isinstance(table, dict):
        raise ValueError(f"{source}: each table must be a mapping, got {type(table).__name__}")

    # table_name
    name = table.get("table_name")
    if not name:
        raise ValueError(f"{source}: table is missing 'table_name'")
    validate_identifier(name, "table_name")

    # columns
    columns = table.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError(f"{source}/{name}: 'columns' must be a non-empty list")

    seen_columns: Set[str] = set()
    for col in columns:
        if not isinstance(col, dict):
            raise ValueError(f"{source}/{name}: each column must be a mapping")

        col_name = col.get("name")
        if not col_name:
            raise ValueError(f"{source}/{name}: column missing 'name'")
        validate_identifier(col_name, "column name")

        if col_name in seen_columns:
            raise ValueError(f"{source}/{name}: duplicate column name {col_name!r}")
        seen_columns.add(col_name)

        col_type = col.get("type")
        if not col_type:
            raise ValueError(f"{source}/{name}.{col_name}: column missing 'type'")
        validate_column_type(col_type, col_name)

        # nullable 必须是显式声明的 bool，避免歧义
        nullable = col.get("nullable")
        if not isinstance(nullable, bool):
            raise ValueError(
                f"{source}/{name}.{col_name}: 'nullable' must be explicit bool (true/false)"
            )

        # default 是可选的，但若存在必须是字符串（直接拼到 SQL 里）
        if "default" in col and not isinstance(col["default"], str):
            raise ValueError(
                f"{source}/{name}.{col_name}: 'default' must be a string "
                f"(it is inserted verbatim into SQL, e.g. \"'active'\")"
            )

    # primary_key
    pk = table.get("primary_key")
    if not isinstance(pk, list) or not pk:
        raise ValueError(f"{source}/{name}: 'primary_key' must be a non-empty list")
    for pk_col in pk:
        if pk_col not in seen_columns:
            raise ValueError(f"{source}/{name}: primary_key column {pk_col!r} not in columns")

    # indexes（可选）
    indexes = table.get("indexes", []) or []
    if not isinstance(indexes, list):
        raise ValueError(f"{source}/{name}: 'indexes' must be a list if present")

    seen_idx: Set[str] = set()
    for idx in indexes:
        if not isinstance(idx, dict):
            raise ValueError(f"{source}/{name}: each index must be a mapping")
        idx_name = idx.get("name")
        if not idx_name:
            raise ValueError(f"{source}/{name}: index missing 'name'")
        validate_identifier(idx_name, "index name")
        if idx_name in seen_idx:
            raise ValueError(f"{source}/{name}: duplicate index name {idx_name!r}")
        seen_idx.add(idx_name)

        idx_cols = idx.get("columns")
        if not isinstance(idx_cols, list) or not idx_cols:
            raise ValueError(f"{source}/{name}/idx[{idx_name}]: 'columns' must be a non-empty list")
        for idx_col in idx_cols:
            if idx_col not in seen_columns:
                raise ValueError(
                    f"{source}/{name}/idx[{idx_name}]: column {idx_col!r} not in table columns"
                )

    # post_create_actions（可选）
    actions = table.get("post_create_actions", []) or []
    if not isinstance(actions, list):
        raise ValueError(f"{source}/{name}: 'post_create_actions' must be a list if present")

    for action in actions:
        if not isinstance(action, dict):
            raise ValueError(f"{source}/{name}: each post_create_action must be a mapping")
        act_kind = action.get("action")
        if act_kind not in SUPPORTED_POST_ACTIONS:
            raise ValueError(
                f"{source}/{name}: unsupported post_create_action {act_kind!r}. "
                f"Supported: {sorted(SUPPORTED_POST_ACTIONS)}"
            )
        if act_kind == "insert_initial_records":
            records = action.get("records")
            if not isinstance(records, list) or not records:
                raise ValueError(
                    f"{source}/{name}: insert_initial_records requires non-empty 'records' list"
                )
            for rec in records:
                if not isinstance(rec, dict):
                    raise ValueError(f"{source}/{name}: each record must be a mapping")
                for k in rec.keys():
                    if k not in seen_columns:
                        raise ValueError(
                            f"{source}/{name}: initial record references unknown column {k!r}"
                        )


def load_all_group_configs(
    config_dir: Path,
    group_files: List[str],
) -> List[Dict[str, Any]]:
    """
    加载并校验所有 group config，返回扁平化的 table spec 列表，
    保留 config 文件内的声明顺序。
    """
    all_tables: List[Dict[str, Any]] = []
    seen_table_names: Set[str] = set()

    for filename in group_files:
        path = config_dir / filename
        cfg = load_yaml(path)

        tables = cfg.get("tables")
        if not isinstance(tables, list) or not tables:
            raise ValueError(f"{path}: top-level 'tables' must be a non-empty list")

        for table in tables:
            validate_table_spec(table, source=str(path))

            tname = table["table_name"]
            if tname in seen_table_names:
                raise ValueError(
                    f"Duplicate table name {tname!r} across group configs"
                )
            seen_table_names.add(tname)

            # 附加来源信息便于审计
            table_with_source = dict(table)
            table_with_source["_source_config"] = filename
            all_tables.append(table_with_source)

    return all_tables


# ============================================================
# DDL 构造
# ============================================================
def build_create_table_sql(table: Dict[str, Any]) -> str:
    """生成 CREATE TABLE IF NOT EXISTS 语句。"""
    name = table["table_name"]
    columns = table["columns"]
    pk = table["primary_key"]

    col_defs = []
    for col in columns:
        parts = [col["name"], col["type"]]
        if not col["nullable"]:
            parts.append("NOT NULL")
        if "default" in col:
            parts.append(f"DEFAULT {col['default']}")
        col_defs.append("    " + " ".join(parts))

    pk_clause = "    PRIMARY KEY (" + ", ".join(pk) + ")"
    col_defs.append(pk_clause)

    body = ",\n".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {name} (\n{body}\n);"


def build_create_index_sql(table_name: str, index: Dict[str, Any]) -> str:
    cols = ", ".join(index["columns"])
    return f"CREATE INDEX IF NOT EXISTS {index['name']} ON {table_name} ({cols});"


def build_alter_add_column_sql(table_name: str, col: Dict[str, Any]) -> str:
    """
    生成 ALTER TABLE ADD COLUMN 语句。
    SQLite 的 ADD COLUMN 限制：
      - 不能为 NOT NULL 但没有 DEFAULT
      - 不能加 PRIMARY KEY
      - 不能加 UNIQUE
    本脚本不主动加 PK/UNIQUE，但 NOT NULL 需要兜底处理。
    """
    parts = [col["name"], col["type"]]

    has_default = "default" in col
    if not col["nullable"]:
        if not has_default:
            # SQLite 限制：缺省值缺失时不能加 NOT NULL 列
            # 退化为可空列，避免 DDL 失败
            parts_safe = [col["name"], col["type"]]
            return (
                f"ALTER TABLE {table_name} ADD COLUMN " + " ".join(parts_safe) + ";"
            )
        parts.append("NOT NULL")
    if has_default:
        parts.append(f"DEFAULT {col['default']}")

    return f"ALTER TABLE {table_name} ADD COLUMN " + " ".join(parts) + ";"


def build_insert_initial_record_sql(
    table_name: str,
    record: Dict[str, Any],
) -> Tuple[str, Tuple[Any, ...]]:
    """生成 INSERT OR IGNORE 语句和参数 tuple。"""
    cols = list(record.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT OR IGNORE INTO {table_name} ({col_list}) VALUES ({placeholders});"
    params = tuple(record[c] for c in cols)
    return sql, params


# ============================================================
# DB 状态查询
# ============================================================
def get_existing_table_names(conn: sqlite3.Connection) -> Set[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    return {row[0] for row in cursor.fetchall()}


def get_existing_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    """获取已存在表的列名。表不存在时返回空集合。"""
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        return {row[1] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return set()


def get_existing_indexes(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
        (table_name,),
    )
    return {row[0] for row in cursor.fetchall() if row[0] is not None}


# ============================================================
# 单表落地
# ============================================================
def plan_table(
    conn: sqlite3.Connection,
    table: Dict[str, Any],
) -> Dict[str, Any]:
    """
    对一张表生成执行计划：
      - 是新建还是 ALTER
      - 哪些列要加
      - 哪些索引要建
      - 有没有 post_create_actions
    返回的计划字典在 dry-run 下作为预览，在实跑下作为执行依据。
    """
    name = table["table_name"]
    existing_tables = get_existing_table_names(conn)

    plan: Dict[str, Any] = {
        "table_name": name,
        "source_config": table.get("_source_config"),
        "table_existed": name in existing_tables,
        "create_table_sql": None,
        "add_column_sqls": [],
        "skipped_columns_unsafe_not_null": [],  # 已存在表新增 NOT NULL 列时降级的列
        "create_index_sqls": [],
        "skipped_indexes_existing": [],
        "post_create_action_count": 0,
    }

    if not plan["table_existed"]:
        plan["create_table_sql"] = build_create_table_sql(table)
    else:
        existing_cols = get_existing_columns(conn, name)
        for col in table["columns"]:
            if col["name"] not in existing_cols:
                if not col["nullable"] and "default" not in col:
                    plan["skipped_columns_unsafe_not_null"].append(col["name"])
                plan["add_column_sqls"].append(build_alter_add_column_sql(name, col))

    # 索引：以索引名为准，存在就跳过
    existing_indexes = get_existing_indexes(conn, name)
    for idx in table.get("indexes", []) or []:
        if idx["name"] in existing_indexes:
            plan["skipped_indexes_existing"].append(idx["name"])
        else:
            plan["create_index_sqls"].append(build_create_index_sql(name, idx))

    # post_create_actions
    actions = table.get("post_create_actions", []) or []
    if actions:
        # 对新建表才执行 post_create_actions（避免对已有表重复 INSERT 引发歧义）
        # 但 INSERT OR IGNORE 本身是幂等的，所以即使表已存在也是安全的
        plan["post_create_action_count"] = sum(
            len(a.get("records", []))
            for a in actions
            if a.get("action") == "insert_initial_records"
        )

    return plan


def execute_table_plan(
    conn: sqlite3.Connection,
    table: Dict[str, Any],
    plan: Dict[str, Any],
) -> None:
    """按计划执行 DDL + post_create_actions。失败抛异常由上层回滚。"""
    if plan["create_table_sql"]:
        conn.execute(plan["create_table_sql"])

    for sql in plan["add_column_sqls"]:
        conn.execute(sql)

    for sql in plan["create_index_sqls"]:
        conn.execute(sql)

    # post_create_actions
    actions = table.get("post_create_actions", []) or []
    for action in actions:
        if action.get("action") == "insert_initial_records":
            for record in action.get("records", []):
                sql, params = build_insert_initial_record_sql(
                    table["table_name"], record
                )
                conn.execute(sql, params)


# ============================================================
# 事件 log
# ============================================================
def append_event_log(
    log_path: Path,
    log_entry: Dict[str, Any],
    dry_run: bool = False,
) -> None:
    if dry_run:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(dumps_json_compact(log_entry) + "\n")


def write_run_meta(run_meta_path: Path, run_meta: Dict[str, Any]) -> Optional[str]:
    try:
        run_meta_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = run_meta_path.with_suffix(run_meta_path.suffix + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(dumps_json(run_meta))
        tmp_path.replace(run_meta_path)
        return None
    except OSError as e:
        return f"run_meta write failed: {type(e).__name__}: {e}"


# ============================================================
# 骨架与主流程
# ============================================================
def build_skeleton(
    *,
    status: str,
    dry_run: bool,
    timestamp_iso: str,
    initiated_by: str,
    machine: str,
    reason: Optional[str],
) -> Dict[str, Any]:
    return {
        "status": status,
        "dry_run": dry_run,
        "timestamp": timestamp_iso,
        "initiated_by": initiated_by,
        "machine": machine,
        "reason": reason,
        "action_db_path": str(ACTION_DB_PATH),
        "config_dir": str(INIT_SCHEMA_CONFIG_DIR),
        "config_files": list(GROUP_CONFIG_FILES),
        "stage": None,
        "error": None,
        "tables_total": 0,
        "tables_created": 0,
        "tables_altered": 0,
        "tables_unchanged": 0,
        "columns_added_total": 0,
        "indexes_created_total": 0,
        "post_actions_executed_total": 0,
        "table_plans": [],
        "warnings": [],
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
    }


def run_init_schema(
    initiated_by: Optional[str] = None,
    reason: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    timestamp_iso = utc_now()
    machine = get_machine_name()

    if initiated_by is None or not initiated_by.strip():
        initiated_by = get_current_user()
    else:
        initiated_by = initiated_by.strip()

    if reason is not None:
        reason = reason.strip() or None

    meta = build_skeleton(
        status="ok",
        dry_run=dry_run,
        timestamp_iso=timestamp_iso,
        initiated_by=initiated_by,
        machine=machine,
        reason=reason,
    )

    # ----------------------------------------------------------
    # 步骤 1: 前置检查
    # ----------------------------------------------------------
    if not ACTION_DB_PATH.exists():
        meta["status"] = "error"
        meta["stage"] = "precheck"
        meta["error"] = (
            f"action.db not found at {ACTION_DB_PATH}. "
            f"Run init_action_db_v0001 first."
        )
        return meta

    if not INIT_SCHEMA_CONFIG_DIR.exists():
        meta["status"] = "error"
        meta["stage"] = "precheck"
        meta["error"] = f"config dir not found: {INIT_SCHEMA_CONFIG_DIR}"
        return meta

    # ----------------------------------------------------------
    # 步骤 2: 加载并校验 config
    # ----------------------------------------------------------
    try:
        all_tables = load_all_group_configs(
            INIT_SCHEMA_CONFIG_DIR, GROUP_CONFIG_FILES
        )
    except ValueError as e:
        meta["status"] = "error"
        meta["stage"] = "config_load"
        meta["error"] = str(e)
        return meta

    meta["tables_total"] = len(all_tables)

    # ----------------------------------------------------------
    # 步骤 3: 连库 + 规划 + 执行（事务包裹）
    # ----------------------------------------------------------
    try:
        conn = sqlite3.connect(str(ACTION_DB_PATH))
    except sqlite3.Error as e:
        meta["status"] = "error"
        meta["stage"] = "db_connect"
        meta["error"] = f"{type(e).__name__}: {e}"
        return meta

    table_plans: List[Dict[str, Any]] = []

    try:
        # 先全部规划，不执行——这样 dry-run 和实跑共用同一份规划逻辑
        for table in all_tables:
            plan = plan_table(conn, table)
            table_plans.append(plan)

            # 收集警告：在已有表上想加 NOT NULL 但没 DEFAULT 的列
            for unsafe_col in plan["skipped_columns_unsafe_not_null"]:
                meta["warnings"].append(
                    f"{plan['table_name']}.{unsafe_col}: declared NOT NULL without DEFAULT, "
                    f"but adding to existing table; column will be added as NULLABLE. "
                    f"Existing rows would otherwise violate NOT NULL."
                )

        meta["table_plans"] = table_plans

        # 汇总统计
        for plan in table_plans:
            if plan["create_table_sql"]:
                meta["tables_created"] += 1
            elif plan["add_column_sqls"] or plan["create_index_sqls"]:
                # 只有实际改了 schema（加列/加索引）才算 altered
                # post_create_actions 是数据层操作，不影响 schema 分类
                meta["tables_altered"] += 1
            else:
                meta["tables_unchanged"] += 1

            meta["columns_added_total"] += len(plan["add_column_sqls"])
            meta["indexes_created_total"] += len(plan["create_index_sqls"])
            meta["post_actions_executed_total"] += plan["post_create_action_count"]

        # dry-run：到此结束，不执行 DDL
        if dry_run:
            conn.close()
            return meta

        # 实跑：在事务里执行
        conn.execute("BEGIN")
        try:
            for table, plan in zip(all_tables, table_plans):
                execute_table_plan(conn, table, plan)
            conn.execute("COMMIT")
        except sqlite3.Error as e:
            conn.execute("ROLLBACK")
            conn.close()
            meta["status"] = "error"
            meta["stage"] = "ddl_execute"
            meta["error"] = f"{type(e).__name__}: {e}"
            return meta
        except Exception as e:
            conn.execute("ROLLBACK")
            conn.close()
            meta["status"] = "error"
            meta["stage"] = "ddl_execute"
            meta["error"] = f"{type(e).__name__}: {e}"
            return meta

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # ----------------------------------------------------------
    # 步骤 4: 写事件 log
    # ----------------------------------------------------------
    event = {
        "event": "schema_init",
        "timestamp": timestamp_iso,
        "initiated_by": initiated_by,
        "machine": machine,
        "reason": reason,
        "tables_total": meta["tables_total"],
        "tables_created": meta["tables_created"],
        "tables_altered": meta["tables_altered"],
        "tables_unchanged": meta["tables_unchanged"],
        "columns_added_total": meta["columns_added_total"],
        "indexes_created_total": meta["indexes_created_total"],
        "post_actions_executed_total": meta["post_actions_executed_total"],
        "warnings_count": len(meta["warnings"]),
        "dry_run": dry_run,
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
    }
    try:
        append_event_log(EVENT_LOG_PATH, event, dry_run=dry_run)
    except OSError as e:
        meta["warnings"].append(
            f"event log append failed: {type(e).__name__}: {e}"
        )

    # ----------------------------------------------------------
    # 步骤 5: 写 run_meta
    # ----------------------------------------------------------
    if not dry_run:
        warning = write_run_meta(RUN_META_PATH, meta)
        if warning:
            meta["warnings"].append(warning)

    return meta


# ============================================================
# CLI / main 接口区
# ============================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Initialize action.db schema (4 groups, 13 tables) from declarative configs. "
            "Requires action.db to exist (run init_action_db_v0001 first)."
        )
    )
    parser.add_argument(
        "--by",
        default=None,
        help="Who is initiating this schema init (optional, defaults to system username)",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="Why running schema init (optional, written into event log for audit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configs and plan DDL without executing or writing files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary = run_init_schema(
        initiated_by=args.by,
        reason=args.reason,
        dry_run=bool(args.dry_run),
    )

    print(dumps_json(summary))

    if summary.get("status") == "error":
        return 1

    return 0


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
