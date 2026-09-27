#!/usr/bin/env python3
# ============================================================
# File: register_script_v0002.py
# 中文名: 脚本组入库器
# Version: v0002
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose
# 将 scan_alias_meta 产出的 JSONL 入库到 action.db 的脚本组三张表：
# script_registry / script_dependencies / script_consumers。
# 同时实施"派生型影子"——检测 JSONL 中出现但 schema 未声明的新字段，
# 在 run_meta 里报 warning，不阻塞写入。
#
# What it does
# 1) 加载 JSONL（来自 scan_alias_meta）
# 2) 加载脚本组 schema 配置（确认字段映射的目标）
# 3) 对每条记录：
#    a. 跳过 _scan_error 记录（计入 errors 报告）
#    b. 跳过缺 alias / version 的记录（计入 errors 报告）
#    c. 字段映射 → script_registry 一行
#    d. depends_on 列表 → script_dependencies N 行
#    e. used_by 列表 → script_consumers N 行
# 4) 派生型影子：检测未在 schema 中声明的 JSONL 字段，收集到 warnings
# 5) 全部写入用同一事务包裹，INSERT OR IGNORE（幂等追加）
# 6) 输出 run_meta 审计摘要 + 事件 log
#
# What it does NOT do
# 1) 不扫描脚本（scan_alias_meta 的事）
# 2) 不建库、不建表（init_action_db / init_action_schema 的事）
# 3) 不删除任何已入库记录（永远 INSERT OR IGNORE）
# 4) 不修改源 JSONL
# 5) 不入库 header_purpose_raw / header_constraint_raw
#    （这两个字段是大段文本，更适合留在源码里被人读，不入结构化 db）
# 6) 不做交互
# 7) 不做依赖类型校验（script 的 depends_on/used_by 字段不受 dependency_type_registry 约束，
#    因为它们承载的是脚本之间的引用，不是功能层的开放依赖）
# 8) 不验证被引用的脚本是否真实存在（依赖图完整性是 resolve_dependencies 的事）
#
# Non-v0001 targets (deferred, still recorded here)
# 1) 增量入库（基于 source_jsonl + 上次 writer 的水位线）
# 2) 自动给 schema 加新字段（当前只在 warning 里报告，由人工决策是否加）
# 3) JSONL 字段类型推断（当前所有字段当 TEXT 处理）
# 4) 跨 JSONL 的对账（多次扫描产生的 JSONL 之间的差异分析）
#
# Notes
# - 本脚本是声明驱动 + 数据扫描的混合 writer
#   主体是数据扫描（JSONL），但每条记录的接受/拒绝是声明驱动（schema 已知字段）
# - 派生型影子的实现位置：本脚本，不在 init_action_schema 里
#   理由：schema 是制度声明，schema 变化是人工决策，不应被扫描数据触发
# - 入库使用 INSERT OR IGNORE，幂等追加
#   隐含语义：被删除的脚本，其旧记录仍保留在 db 中（历史不可变原则的体现）
# - 仅依赖 PyYAML 与 Python 标准库
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: register_script
# family: register_script
# role: script_writer
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/register_script_v0002.py
# input:
#   - actioning/scan_alias_meta/scan_alias_meta_output.jsonl
#   - config/action/init_schema/script_group.yml (used for schema awareness)
#   - --by (optional, CLI)
#   - --reason (optional, CLI)
#   - --jsonl-path (optional, CLI override)
#   - --dry-run (optional, CLI)
# output:
#   - sql/action.db (script_registry / script_dependencies / script_consumers)
#   - logs/register_script.jsonl (append-only event log)
#   - actioning/register_script/run_meta.json
# depends_on:
#   - scan_alias_meta_v0002
#   - init_action_db_v0001
#   - init_action_schema_v0002
# used_by:
#   - flow_register_script_v0001 (future)
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 本脚本只入库脚本组三张表，不动其他组的表
# - 写入策略：INSERT OR IGNORE，永不更新、永不删除
# - 一次性事务包裹所有写入，失败全回滚
# - 派生型影子：未知字段只报 warning 不阻塞
# - dry-run 下不得修改 db 或写任何文件
# ============================================================

from __future__ import annotations

# ============================================================
# Imports
# ============================================================
import argparse
import json
import os
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
# - dry-run 下不得执行任何 INSERT 或写任何文件
# - 不删除任何记录
# - 写入用同一事务，失败回滚

# ============================================================
# 路径常量区（项目级约定，不通过 config 暴露）
# ============================================================
ACTION_DB_PATH = Path("sql/action.db")
DEFAULT_JSONL_PATH = Path("actioning/scan_alias_meta/scan_alias_meta_output.jsonl")
SCRIPT_GROUP_CONFIG_PATH = Path("config/action/init_schema/script_group.yml")
EVENT_LOG_PATH = Path("logs/register_script.jsonl")
RUN_META_PATH = Path("actioning/register_script/run_meta.json")

# ============================================================
# 常量与全局配置区
# ============================================================
SCRIPT_NAME = "register_script_v0002.py"
SCRIPT_VERSION = "v0002"
MAIN_LAYER = "action"

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

# script_group.yml 中的三张表名（也作为内置常量参照，避免 config 改名时悄悄漂移）
TABLE_SCRIPT_REGISTRY = "script_registry"
TABLE_SCRIPT_DEPENDENCIES = "script_dependencies"
TABLE_SCRIPT_CONSUMERS = "script_consumers"

# JSONL 中的列表字段，会被拆开写入关联表
LIST_FIELD_DEPENDS_ON = "depends_on"
LIST_FIELD_USED_BY = "used_by"
LIST_FIELDS_TO_RELATION_TABLES = {LIST_FIELD_DEPENDS_ON, LIST_FIELD_USED_BY}

# JSONL 中的列表字段，会被序列化成 JSON 字符串存入 TEXT 字段
LIST_FIELDS_TO_TEXT_SERIALIZED = {"input", "output"}

# JSONL 中已知会出现但故意不入库的字段（不报警告）
KNOWN_IGNORED_FIELDS = {
    "header_purpose_raw",
    "header_constraint_raw",
    "file_path",       # 由 writer 单独处理为 entry_point/source_jsonl 上下文
    "scanned_at",      # 由 writer 单独处理为 registered_at
    "_scan_error",     # 错误标记字段，单独处理
}

# JSONL 字段 → script_registry 列名的映射
# 仅列出名字不一致的；同名字段不需要映射条目
JSONL_TO_REGISTRY_RENAME = {
    "input": "input_spec",
    "output": "output_spec",
    "scanned_at": "registered_at",
}


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


# ============================================================
# JSONL 与 schema 加载
# ============================================================
def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """
    流式读 JSONL，单行解析失败抛异常并附带行号。
    返回完整的记录列表。
    """
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                rec = json.loads(stripped)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"JSONL parse error at line {lineno} of {path}: {e}"
                )
            if not isinstance(rec, dict):
                raise ValueError(
                    f"JSONL line {lineno} of {path} is not a JSON object"
                )
            records.append(rec)
    return records


def load_script_group_schema(path: Path) -> Dict[str, Set[str]]:
    """
    从 script_group.yml 提取每张表的列名集合。
    返回 {table_name: {col_name, ...}}.
    用于派生型影子的"已知字段集"。
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "tables" not in cfg:
        raise ValueError(f"Invalid schema config: {path}")

    out: Dict[str, Set[str]] = {}
    for table in cfg["tables"]:
        tname = table.get("table_name")
        if not tname:
            continue
        cols = {c["name"] for c in table.get("columns", []) if "name" in c}
        out[tname] = cols
    return out


# ============================================================
# 派生型影子检测
# ============================================================
def detect_unknown_fields(
    jsonl_record: Dict[str, Any],
    known_registry_columns: Set[str],
) -> List[str]:
    """
    检测 JSONL 记录中出现但 script_registry 未声明的字段。
    排除：
      - KNOWN_IGNORED_FIELDS（已知忽略集）
      - 列表字段（会进关联表，不是 registry 主表的字段）
    返回 sorted 的未知字段名列表。
    """
    unknown: Set[str] = set()
    for key in jsonl_record.keys():
        if key in KNOWN_IGNORED_FIELDS:
            continue
        if key in LIST_FIELDS_TO_RELATION_TABLES:
            continue
        # 应用重命名后再比较
        target_name = JSONL_TO_REGISTRY_RENAME.get(key, key)
        if target_name not in known_registry_columns:
            unknown.add(key)
    return sorted(unknown)


# ============================================================
# 字段映射
# ============================================================
def map_jsonl_to_registry_row(
    record: Dict[str, Any],
    known_columns: Set[str],
    registered_by: str,
    registered_where: str,
    source_jsonl: str,
) -> Dict[str, Any]:
    """
    把一条 JSONL 记录映射成 script_registry 表的一行。
    只取 known_columns 中的字段，未知字段忽略（已在派生型影子里报告）。
    """
    row: Dict[str, Any] = {}

    for jsonl_key, value in record.items():
        if jsonl_key in KNOWN_IGNORED_FIELDS:
            continue
        if jsonl_key in LIST_FIELDS_TO_RELATION_TABLES:
            continue

        target_key = JSONL_TO_REGISTRY_RENAME.get(jsonl_key, jsonl_key)
        if target_key not in known_columns:
            # 未知字段——已在派生型影子里报警，这里直接跳过
            continue

        # 列表字段（input/output）序列化为 JSON 字符串
        if jsonl_key in LIST_FIELDS_TO_TEXT_SERIALIZED and isinstance(value, list):
            row[target_key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, (list, dict)):
            # 其他意外的复合类型也兜底序列化，避免 sqlite 报错
            row[target_key] = json.dumps(value, ensure_ascii=False)
        else:
            row[target_key] = value

    # writer 附加的元字段
    if "registered_by" in known_columns:
        row["registered_by"] = registered_by
    if "registered_where" in known_columns:
        row["registered_where"] = registered_where
    if "source_jsonl" in known_columns:
        row["source_jsonl"] = source_jsonl
    # registered_at 已通过 scanned_at 重命名映射
    # 但若 JSONL 里没有 scanned_at，兜底填当前时间
    if "registered_at" in known_columns and "registered_at" not in row:
        row["registered_at"] = utc_now()

    return row


def extract_relation_rows(
    record: Dict[str, Any],
    alias: str,
    version: str,
) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str]]]:
    """
    从 JSONL 记录中拆出依赖与消费者关联行。
    返回 (depends_on_rows, used_by_rows)，每行是 (alias, version, target)。
    """
    depends_rows: List[Tuple[str, str, str]] = []
    used_by_rows: List[Tuple[str, str, str]] = []

    deps = record.get(LIST_FIELD_DEPENDS_ON)
    if isinstance(deps, list):
        for dep in deps:
            dep_str = str(dep).strip()
            if dep_str:
                depends_rows.append((alias, version, dep_str))
    elif isinstance(deps, str) and deps.strip():
        # JSONL 偶尔会出现单字符串而非列表（解析容错）
        depends_rows.append((alias, version, deps.strip()))

    consumers = record.get(LIST_FIELD_USED_BY)
    if isinstance(consumers, list):
        for c in consumers:
            c_str = str(c).strip()
            if c_str:
                used_by_rows.append((alias, version, c_str))
    elif isinstance(consumers, str) and consumers.strip():
        used_by_rows.append((alias, version, consumers.strip()))

    return depends_rows, used_by_rows


# ============================================================
# 单条记录的可入库性判断
# ============================================================
def classify_record(record: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    对一条 JSONL 记录分类。返回 (status, reason)。

    status:
      - "ok"      : 可入库
      - "scan_error" : JSONL 中带 _scan_error 标记
      - "missing_key" : 缺关键字段（alias / version）
    """
    if "_scan_error" in record:
        return "scan_error", record.get("_scan_error", "unknown scan error")

    alias = record.get("alias")
    if not alias or not isinstance(alias, str) or not alias.strip():
        return "missing_key", "missing or empty 'alias'"

    version = record.get("version")
    if not version or not isinstance(version, str) or not version.strip():
        return "missing_key", "missing or empty 'version'"

    return "ok", None


# ============================================================
# DB 写入
# ============================================================
def insert_registry_row(
    conn: sqlite3.Connection,
    row: Dict[str, Any],
) -> bool:
    """
    INSERT OR IGNORE 一行到 script_registry。
    返回是否真正写入（False 表示因主键冲突被忽略）。
    """
    cols = list(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    sql = (
        f"INSERT OR IGNORE INTO {TABLE_SCRIPT_REGISTRY} "
        f"({col_list}) VALUES ({placeholders})"
    )
    params = tuple(row[c] for c in cols)

    changes_before = conn.total_changes
    conn.execute(sql, params)
    changes_after = conn.total_changes
    return changes_after > changes_before


def insert_relation_rows(
    conn: sqlite3.Connection,
    table_name: str,
    target_col: str,
    rows: List[Tuple[str, str, str]],
) -> int:
    """
    批量 INSERT OR IGNORE 关联行。返回真正写入的条数。
    """
    if not rows:
        return 0
    sql = (
        f"INSERT OR IGNORE INTO {table_name} "
        f"(alias, version, {target_col}) VALUES (?, ?, ?)"
    )
    written = 0
    for r in rows:
        changes_before = conn.total_changes
        conn.execute(sql, r)
        if conn.total_changes > changes_before:
            written += 1
    return written


# ============================================================
# 事件 log 与 run_meta
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
# 骨架
# ============================================================
def build_skeleton(
    *,
    status: str,
    dry_run: bool,
    timestamp_iso: str,
    initiated_by: str,
    machine: str,
    reason: Optional[str],
    jsonl_path: str,
) -> Dict[str, Any]:
    return {
        "status": status,
        "dry_run": dry_run,
        "timestamp": timestamp_iso,
        "initiated_by": initiated_by,
        "machine": machine,
        "reason": reason,
        "action_db_path": str(ACTION_DB_PATH),
        "jsonl_path": jsonl_path,
        "schema_config_path": str(SCRIPT_GROUP_CONFIG_PATH),
        "stage": None,
        "error": None,
        "records_total": 0,
        "records_ok": 0,
        "records_scan_error": 0,
        "records_missing_key": 0,
        "registry_inserted": 0,
        "registry_skipped_existing": 0,
        "depends_on_inserted": 0,
        "depends_on_skipped_existing": 0,
        "used_by_inserted": 0,
        "used_by_skipped_existing": 0,
        "unknown_fields_summary": {},   # field_name -> count of records with this field
        "errored_records": [],          # 详细错误记录
        "warnings": [],
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
    }


# ============================================================
# 主流程
# ============================================================
def run_register_script(
    initiated_by: Optional[str] = None,
    reason: Optional[str] = None,
    jsonl_path: Optional[Path] = None,
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

    actual_jsonl_path = jsonl_path if jsonl_path else DEFAULT_JSONL_PATH

    meta = build_skeleton(
        status="ok",
        dry_run=dry_run,
        timestamp_iso=timestamp_iso,
        initiated_by=initiated_by,
        machine=machine,
        reason=reason,
        jsonl_path=str(actual_jsonl_path),
    )

    # ----------------------------------------------------------
    # 步骤 1: 前置检查
    # ----------------------------------------------------------
    if not ACTION_DB_PATH.exists():
        meta["status"] = "error"
        meta["stage"] = "precheck"
        meta["error"] = (
            f"action.db not found at {ACTION_DB_PATH}. "
            f"Run init_action_db_v0001 + init_action_schema_v0002 first."
        )
        return meta

    if not actual_jsonl_path.exists():
        meta["status"] = "error"
        meta["stage"] = "precheck"
        meta["error"] = (
            f"JSONL not found at {actual_jsonl_path}. "
            f"Run scan_alias_meta_v0002 first."
        )
        return meta

    if not SCRIPT_GROUP_CONFIG_PATH.exists():
        meta["status"] = "error"
        meta["stage"] = "precheck"
        meta["error"] = f"schema config not found at {SCRIPT_GROUP_CONFIG_PATH}"
        return meta

    # ----------------------------------------------------------
    # 步骤 2: 加载 schema 与 JSONL
    # ----------------------------------------------------------
    try:
        schema_columns = load_script_group_schema(SCRIPT_GROUP_CONFIG_PATH)
    except (ValueError, yaml.YAMLError) as e:
        meta["status"] = "error"
        meta["stage"] = "schema_load"
        meta["error"] = f"{type(e).__name__}: {e}"
        return meta

    if TABLE_SCRIPT_REGISTRY not in schema_columns:
        meta["status"] = "error"
        meta["stage"] = "schema_load"
        meta["error"] = (
            f"{TABLE_SCRIPT_REGISTRY} not declared in {SCRIPT_GROUP_CONFIG_PATH}"
        )
        return meta

    registry_columns = schema_columns[TABLE_SCRIPT_REGISTRY]

    try:
        records = load_jsonl(actual_jsonl_path)
    except (ValueError, OSError) as e:
        meta["status"] = "error"
        meta["stage"] = "jsonl_load"
        meta["error"] = f"{type(e).__name__}: {e}"
        return meta

    meta["records_total"] = len(records)

    # ----------------------------------------------------------
    # 步骤 3: 分类记录 + 派生型影子检测
    # ----------------------------------------------------------
    ok_records: List[Dict[str, Any]] = []
    unknown_field_count: Dict[str, int] = {}

    for idx, record in enumerate(records):
        cls, reason_str = classify_record(record)
        if cls == "scan_error":
            meta["records_scan_error"] += 1
            meta["errored_records"].append({
                "index": idx,
                "kind": "scan_error",
                "reason": reason_str,
                "file_path": record.get("file_path"),
            })
            continue
        if cls == "missing_key":
            meta["records_missing_key"] += 1
            meta["errored_records"].append({
                "index": idx,
                "kind": "missing_key",
                "reason": reason_str,
                "file_path": record.get("file_path"),
            })
            continue

        # 派生型影子：检测未知字段
        unknowns = detect_unknown_fields(record, registry_columns)
        for field in unknowns:
            unknown_field_count[field] = unknown_field_count.get(field, 0) + 1

        ok_records.append(record)

    meta["records_ok"] = len(ok_records)
    meta["unknown_fields_summary"] = dict(sorted(unknown_field_count.items()))

    # 派生型影子的 warning（每个未知字段一条 warning）
    for field, count in sorted(unknown_field_count.items()):
        meta["warnings"].append(
            f"Unknown JSONL field {field!r} appeared in {count} record(s); "
            f"not declared in {TABLE_SCRIPT_REGISTRY} schema. "
            f"Consider adding it to script_group.yml if it should be persisted."
        )

    # ----------------------------------------------------------
    # 步骤 4: 连库 + 事务写入
    # ----------------------------------------------------------
    try:
        conn = sqlite3.connect(str(ACTION_DB_PATH))
    except sqlite3.Error as e:
        meta["status"] = "error"
        meta["stage"] = "db_connect"
        meta["error"] = f"{type(e).__name__}: {e}"
        return meta

    source_jsonl_str = str(actual_jsonl_path)

    try:
        # 先把所有要写的内容算好（dry-run 与实跑共用同一份计算逻辑）
        registry_rows: List[Dict[str, Any]] = []
        all_depends: List[Tuple[str, str, str]] = []
        all_used_by: List[Tuple[str, str, str]] = []

        for record in ok_records:
            try:
                row = map_jsonl_to_registry_row(
                    record=record,
                    known_columns=registry_columns,
                    registered_by=initiated_by,
                    registered_where=machine,
                    source_jsonl=source_jsonl_str,
                )
            except Exception as e:
                # 单条记录映射失败不阻塞整体——记录到 errored_records
                meta["errored_records"].append({
                    "index": records.index(record) if record in records else -1,
                    "kind": "map_failed",
                    "reason": f"{type(e).__name__}: {e}",
                    "alias": record.get("alias"),
                    "version": record.get("version"),
                })
                continue

            registry_rows.append(row)

            depends_rows, used_by_rows = extract_relation_rows(
                record=record,
                alias=record["alias"].strip(),
                version=record["version"].strip(),
            )
            all_depends.extend(depends_rows)
            all_used_by.extend(used_by_rows)

        # dry-run：到此返回，不写库
        if dry_run:
            meta["registry_inserted"] = 0  # 无法预知有多少会因主键冲突被忽略
            meta["depends_on_inserted"] = 0
            meta["used_by_inserted"] = 0
            # 但能给出"计划写入数"作为参考
            meta.setdefault("dry_run_planned", {})
            meta["dry_run_planned"] = {
                "registry_rows_planned": len(registry_rows),
                "depends_on_rows_planned": len(all_depends),
                "used_by_rows_planned": len(all_used_by),
            }
            conn.close()
            return meta

        # 实跑：单事务包裹
        conn.execute("BEGIN")
        try:
            registry_inserted = 0
            for row in registry_rows:
                if insert_registry_row(conn, row):
                    registry_inserted += 1

            depends_inserted = insert_relation_rows(
                conn, TABLE_SCRIPT_DEPENDENCIES, "depends_on", all_depends
            )
            used_by_inserted = insert_relation_rows(
                conn, TABLE_SCRIPT_CONSUMERS, "consumer", all_used_by
            )

            conn.execute("COMMIT")

            meta["registry_inserted"] = registry_inserted
            meta["registry_skipped_existing"] = len(registry_rows) - registry_inserted
            meta["depends_on_inserted"] = depends_inserted
            meta["depends_on_skipped_existing"] = len(all_depends) - depends_inserted
            meta["used_by_inserted"] = used_by_inserted
            meta["used_by_skipped_existing"] = len(all_used_by) - used_by_inserted

        except sqlite3.Error as e:
            conn.execute("ROLLBACK")
            meta["status"] = "error"
            meta["stage"] = "db_write"
            meta["error"] = f"{type(e).__name__}: {e}"
            return meta
        except Exception as e:
            conn.execute("ROLLBACK")
            meta["status"] = "error"
            meta["stage"] = "db_write"
            meta["error"] = f"{type(e).__name__}: {e}"
            return meta

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # ----------------------------------------------------------
    # 步骤 5: 事件 log + run_meta
    # ----------------------------------------------------------
    event = {
        "event": "register_script",
        "timestamp": timestamp_iso,
        "initiated_by": initiated_by,
        "machine": machine,
        "reason": reason,
        "jsonl_path": source_jsonl_str,
        "records_total": meta["records_total"],
        "records_ok": meta["records_ok"],
        "records_scan_error": meta["records_scan_error"],
        "records_missing_key": meta["records_missing_key"],
        "registry_inserted": meta["registry_inserted"],
        "depends_on_inserted": meta["depends_on_inserted"],
        "used_by_inserted": meta["used_by_inserted"],
        "unknown_fields_count": len(meta["unknown_fields_summary"]),
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
            "Register scripts into action.db (script_registry/dependencies/consumers) "
            "from scan_alias_meta JSONL. INSERT OR IGNORE — idempotent."
        )
    )
    parser.add_argument(
        "--by",
        default=None,
        help="Who is initiating this writer run (optional, defaults to system username)",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="Why running this writer (optional, written into event log for audit)",
    )
    parser.add_argument(
        "--jsonl-path",
        default=None,
        help=f"Override JSONL path (default: {DEFAULT_JSONL_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan and report without writing to db or files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    jsonl_path = Path(args.jsonl_path) if args.jsonl_path else None

    summary = run_register_script(
        initiated_by=args.by,
        reason=args.reason,
        jsonl_path=jsonl_path,
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
