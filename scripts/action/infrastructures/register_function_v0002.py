#!/usr/bin/env python3
# ============================================================
# File: register_function_v0002.py
# 中文名: 功能组入库器
# Version: v0002
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose
# 将“若干脚本按明确顺序组成一个目标能力”的声明写入 action.db。
#
# What it does
# 1) 登记一个 function_registry 身份；
# 2) 将有序脚本同时写入 function_dependencies；
# 3) 将脚本顺序写入 flow_definition(scope=function)；
# 4) 登记非脚本依赖；
# 5) 全部数据库写入使用同一事务。
#
# What it does NOT do
# 1) 不登记任务或功能块；
# 2) 不运行被登记脚本；
# 3) 不推测脚本顺序；
# 4) 不更新既有功能。
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: register_function
# family: register_function
# role: function_writer
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/register_function_v0002.py
# input:
#   - --name (required, CLI)
#   - --description (optional, CLI)
#   - --scripts (required, ordered comma-separated script aliases)
#   - --depends-on (optional, non-script dependencies as type:target)
#   - --by (optional, CLI)
#   - --reason (optional, CLI)
#   - --dry-run (optional, CLI)
# output:
#   - sql/action.db (function_registry / function_dependencies / flow_definition)
#   - logs/register_function.jsonl (append-only event log)
#   - actioning/register_function/run_meta.json
# depends_on:
#   - init_action_db_v0001
#   - init_action_schema_v0002
# used_by:
#   - register_task_v0002
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
EVENT_LOG_PATH = Path("logs/register_function.jsonl")
RUN_META_PATH = Path("actioning/register_function/run_meta.json")

SCRIPT_NAME = "register_function_v0002.py"
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


def parse_dependencies(raw: Optional[str]) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    seen = set()
    for piece in (raw or "").split(","):
        piece = piece.strip()
        if not piece:
            continue
        if ":" not in piece:
            raise ValueError(f"Invalid dependency {piece!r}; expected type:target")
        dependency_type, target = (part.strip() for part in piece.split(":", 1))
        if not dependency_type or not target:
            raise ValueError(f"Invalid dependency {piece!r}; type and target are required")
        if dependency_type == "script":
            raise ValueError("Script dependencies must use --scripts so their order is explicit")
        key = (dependency_type, target)
        if key not in seen:
            result.append({"type": dependency_type, "target": target})
            seen.add(key)
    return result


def append_jsonl(path: Path, event: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(dumps_json(event, compact=True) + "\n")


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(dumps_json(payload), encoding="utf-8")
    temporary.replace(path)


def run_register_function(
    function_name: str,
    description: Optional[str] = None,
    scripts_raw: Optional[str] = None,
    depends_on_raw: Optional[str] = None,
    initiated_by: Optional[str] = None,
    reason: Optional[str] = None,
    dry_run: bool = False,
    *,
    db_path: Path = ACTION_DB_PATH,
    event_log_path: Path = EVENT_LOG_PATH,
    run_meta_path: Path = RUN_META_PATH,
) -> Dict[str, Any]:
    timestamp = utc_now()
    function_name = (function_name or "").strip()
    description = description.strip() if description and description.strip() else None
    scripts = parse_csv(scripts_raw)
    initiated_by = (initiated_by or current_user()).strip() or "unknown"
    reason = reason.strip() if reason and reason.strip() else None

    result: Dict[str, Any] = {
        "status": "ok",
        "dry_run": dry_run,
        "timestamp": timestamp,
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "function_id": None,
        "function_name": function_name or None,
        "scripts": scripts,
        "dependencies": [],
        "flow_rows": 0,
        "error": None,
        "stage": None,
        "warnings": [],
    }

    if not function_name:
        result.update(status="error", stage="args", error="function name is required")
        return result
    if not scripts:
        result.update(
            status="error",
            stage="args",
            error="at least one ordered script alias is required via --scripts",
        )
        return result
    try:
        extra_dependencies = parse_dependencies(depends_on_raw)
    except ValueError as exc:
        result.update(status="error", stage="args", error=str(exc))
        return result
    result["dependencies"] = [
        *({"type": "script", "target": alias} for alias in scripts),
        *extra_dependencies,
    ]

    if not db_path.exists():
        result.update(status="error", stage="precheck", error=f"action database not found: {db_path}")
        return result

    function_id = uuid.uuid4().hex
    result["function_id"] = function_id

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        result.update(status="error", stage="db_connect", error=f"{type(exc).__name__}: {exc}")
        return result

    try:
        known_types = {
            row[0] for row in conn.execute("SELECT type_name FROM dependency_type_registry")
        }
        if "script" not in known_types:
            result.update(
                status="error",
                stage="constraint",
                error="dependency type 'script' is not registered",
            )
            return result
        unknown_types = sorted({item["type"] for item in extra_dependencies} - known_types)
        if unknown_types:
            result.update(
                status="error",
                stage="constraint",
                error=f"unknown dependency types: {unknown_types}",
            )
            return result

        placeholders = ",".join("?" for _ in scripts)
        existing_scripts = {
            row[0]
            for row in conn.execute(
                f"SELECT DISTINCT alias FROM script_registry WHERE alias IN ({placeholders})",
                scripts,
            )
        }
        missing_scripts = [alias for alias in scripts if alias not in existing_scripts]
        if missing_scripts:
            result.update(
                status="error",
                stage="constraint",
                error=f"script aliases are not registered: {missing_scripts}",
            )
            return result

        flow_columns = {row[1] for row in conn.execute("PRAGMA table_info(flow_definition)")}
        if "scope" not in flow_columns:
            result.update(status="error", stage="schema_check", error="flow_definition is unavailable")
            return result

        if dry_run:
            result["flow_rows"] = len(scripts)
            return result

        machine = socket.gethostname()
        conn.execute("BEGIN")
        conn.execute(
            """INSERT INTO function_registry
               (function_id, function_name, description, registered_at,
                registered_by, registered_where, status)
               VALUES (?, ?, ?, ?, ?, ?, 'active')""",
            (function_id, function_name, description, timestamp, initiated_by, machine),
        )
        for dependency in result["dependencies"]:
            conn.execute(
                """INSERT OR IGNORE INTO function_dependencies
                   (function_id, dependency_type, dependency_target, declared_at)
                   VALUES (?, ?, ?, ?)""",
                (function_id, dependency["type"], dependency["target"], timestamp),
            )
        for position, alias in enumerate(scripts):
            conn.execute(
                """INSERT INTO flow_definition
                   (flow_id, scope, scope_id, scope_version, element_type,
                    element_id, order_index, declared_at)
                   VALUES (?, 'function', ?, NULL, 'script', ?, ?, ?)""",
                (uuid.uuid4().hex, function_id, alias, position, timestamp),
            )
        conn.commit()
        result["flow_rows"] = len(scripts)
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
        "event": "register_function",
        "timestamp": timestamp,
        "function_id": function_id,
        "function_name": function_name,
        "script_count": len(scripts),
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
        description="Register one function as an ordered composition of scripts."
    )
    parser.add_argument("--name", required=True)
    parser.add_argument("--description", default=None)
    parser.add_argument("--scripts", required=True, help="Ordered comma-separated script aliases")
    parser.add_argument("--depends-on", default=None, help="Non-script dependencies as type:target")
    parser.add_argument("--by", default=None)
    parser.add_argument("--reason", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_register_function(
        function_name=args.name,
        description=args.description,
        scripts_raw=args.scripts,
        depends_on_raw=args.depends_on,
        initiated_by=args.by,
        reason=args.reason,
        dry_run=bool(args.dry_run),
    )
    print(dumps_json(result))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
