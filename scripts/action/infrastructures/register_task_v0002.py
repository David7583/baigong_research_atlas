#!/usr/bin/env python3
# ============================================================
# File: register_task_v0002.py
# 中文名: 任务组入库器
# Version: v0002
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose
# 将“若干功能按明确顺序再次编排形成一个任务”的声明写入 action.db。
#
# What it does
# 1) 登记任务身份、初始版本和初始状态；
# 2) 校验每个功能身份真实存在；
# 3) 写入 flow_definition(scope=task) 保存功能顺序；
# 4) 全部数据库写入使用同一事务。
#
# What it does NOT do
# 1) 不登记脚本或功能；
# 2) 不自动创建功能块；
# 3) 不执行任务；
# 4) 不修改既有任务。
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: register_task
# family: register_task
# role: task_writer
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/register_task_v0002.py
# input:
#   - --name (required, CLI)
#   - --purpose (optional, CLI)
#   - --functions (required, ordered comma-separated function_ids)
#   - --version (optional, default v0001)
#   - --change-summary (optional, CLI)
#   - --by (optional, CLI)
#   - --reason (optional, CLI)
#   - --dry-run (optional, CLI)
# output:
#   - sql/action.db (task_registry / task_versions / task_status_log / flow_definition)
#   - logs/register_task.jsonl (append-only event log)
#   - actioning/register_task/run_meta.json
# depends_on:
#   - init_action_db_v0001
#   - init_action_schema_v0002
# used_by:
#   - register_function_block_v0002
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


ACTION_DB_PATH = Path("sql/action.db")
EVENT_LOG_PATH = Path("logs/register_task.jsonl")
RUN_META_PATH = Path("actioning/register_task/run_meta.json")

SCRIPT_NAME = "register_task_v0002.py"
SCRIPT_VERSION = "v0002"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def dumps_json(data: Any, *, compact: bool = False) -> str:
    if compact:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return json.dumps(data, ensure_ascii=False, indent=2)


def current_user() -> str:
    try:
        return os.getlogin()
    except OSError:
        return "unknown"


def parse_csv(raw: Optional[str]) -> List[str]:
    values: List[str] = []
    seen = set()
    for piece in (raw or "").split(","):
        value = piece.strip()
        if value and value not in seen:
            values.append(value)
            seen.add(value)
    return values


def append_jsonl(path: Path, event: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(dumps_json(event, compact=True) + "\n")


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(dumps_json(payload), encoding="utf-8")
    temporary.replace(path)


def run_register_task(
    task_name: str,
    purpose: Optional[str] = None,
    functions_raw: Optional[str] = None,
    version: Optional[str] = None,
    change_summary: Optional[str] = None,
    initiated_by: Optional[str] = None,
    reason: Optional[str] = None,
    dry_run: bool = False,
    *,
    db_path: Path = ACTION_DB_PATH,
    event_log_path: Path = EVENT_LOG_PATH,
    run_meta_path: Path = RUN_META_PATH,
) -> Dict[str, Any]:
    timestamp = utc_now()
    task_name = (task_name or "").strip()
    purpose = purpose.strip() if purpose and purpose.strip() else None
    functions = parse_csv(functions_raw)
    version = (version or "v0001").strip() or "v0001"
    change_summary = change_summary.strip() if change_summary and change_summary.strip() else None
    initiated_by = (initiated_by or current_user()).strip() or "unknown"
    reason = reason.strip() if reason and reason.strip() else None

    result: Dict[str, Any] = {
        "status": "ok",
        "dry_run": dry_run,
        "timestamp": timestamp,
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "task_id": None,
        "task_name": task_name or None,
        "version": version,
        "functions": functions,
        "flow_rows": 0,
        "error": None,
        "stage": None,
        "warnings": [],
    }

    if not task_name:
        result.update(status="error", stage="args", error="task name is required")
        return result
    if not functions:
        result.update(
            status="error",
            stage="args",
            error="at least one ordered function_id is required via --functions",
        )
        return result
    if not version:
        result.update(status="error", stage="args", error="task version cannot be empty")
        return result
    if not db_path.exists():
        result.update(status="error", stage="precheck", error=f"action database not found: {db_path}")
        return result

    task_id = uuid.uuid4().hex
    log_id = uuid.uuid4().hex
    result["task_id"] = task_id

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        result.update(status="error", stage="db_connect", error=f"{type(exc).__name__}: {exc}")
        return result

    try:
        placeholders = ",".join("?" for _ in functions)
        existing = {
            row[0]
            for row in conn.execute(
                f"SELECT function_id FROM function_registry WHERE function_id IN ({placeholders})",
                functions,
            )
        }
        missing = [function_id for function_id in functions if function_id not in existing]
        if missing:
            result.update(
                status="error",
                stage="constraint",
                error=f"function_ids are not registered: {missing}",
            )
            return result

        if dry_run:
            result["flow_rows"] = len(functions)
            return result

        machine = socket.gethostname()
        conn.execute("BEGIN")
        conn.execute(
            """INSERT INTO task_registry
               (task_id, task_name, purpose, registered_at,
                registered_by, registered_where, status)
               VALUES (?, ?, ?, ?, ?, ?, 'pending')""",
            (task_id, task_name, purpose, timestamp, initiated_by, machine),
        )
        conn.execute(
            """INSERT INTO task_versions
               (task_id, version, change_summary, created_at, created_by)
               VALUES (?, ?, ?, ?, ?)""",
            (task_id, version, change_summary, timestamp, initiated_by),
        )
        conn.execute(
            """INSERT INTO task_status_log
               (log_id, task_id, from_status, to_status, reason, changed_at, changed_by)
               VALUES (?, ?, NULL, 'pending', ?, ?, ?)""",
            (log_id, task_id, reason, timestamp, initiated_by),
        )
        for position, function_id in enumerate(functions):
            conn.execute(
                """INSERT INTO flow_definition
                   (flow_id, scope, scope_id, scope_version, element_type,
                    element_id, order_index, declared_at)
                   VALUES (?, 'task', ?, ?, 'function', ?, ?, ?)""",
                (uuid.uuid4().hex, task_id, version, function_id, position, timestamp),
            )
        conn.commit()
        result["flow_rows"] = len(functions)
    except sqlite3.Error as exc:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        result.update(status="error", stage="db_write", error=f"{type(exc).__name__}: {exc}")
        return result
    finally:
        conn.close()

    event = {
        "event": "register_task",
        "timestamp": timestamp,
        "task_id": task_id,
        "task_name": task_name,
        "version": version,
        "function_count": len(functions),
        "initiated_by": initiated_by,
        "reason": reason,
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
    }
    try:
        append_jsonl(event_log_path, event)
        write_json(run_meta_path, result)
    except OSError as exc:
        result["warnings"].append(f"audit artifact write failed: {type(exc).__name__}: {exc}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register one task as an ordered composition of functions."
    )
    parser.add_argument("--name", required=True)
    parser.add_argument("--purpose", default=None)
    parser.add_argument("--functions", required=True, help="Ordered comma-separated function_ids")
    parser.add_argument("--version", default="v0001")
    parser.add_argument("--change-summary", default=None)
    parser.add_argument("--by", default=None)
    parser.add_argument("--reason", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_register_task(
        task_name=args.name,
        purpose=args.purpose,
        functions_raw=args.functions,
        version=args.version,
        change_summary=args.change_summary,
        initiated_by=args.by,
        reason=args.reason,
        dry_run=bool(args.dry_run),
    )
    print(dumps_json(result))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
