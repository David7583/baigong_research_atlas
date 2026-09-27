#!/usr/bin/env python3
# ============================================================
# File: init_action_db_v0001.py
# 中文名: 行动端建库脚本
# Version: v0001
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose
# 建立 action.db 这个数据库文件本身（容器层），不建任何表。
# 如果旧 db 已存在，先归档（改后缀加时间戳）移到归档目录，
# 写一份归档 log，然后创建一个全新的空 action.db 文件。
#
# What it does
# 1) 检测目标位置是否已有 action.db
#    若有：改后缀为 .archived_<UTC_timestamp> 移到归档目录
#    若无：跳过归档步骤
# 2) 写归档 log（JSON 一行追加），记录归档事实
# 3) 创建新的空 action.db 文件（通过 sqlite3.connect 触发隐式建库）
# 4) 写一条 create 事件到同一份 log
# 5) 输出 run_meta 审计摘要
#
# What it does NOT do
# 1) 不建任何表（这是 init_action_schema_v0002 的事）
# 2) 不删除旧 db（只归档，文件保留在归档目录）
# 3) 不做交互
# 4) 不修改任何已归档的 db 文件
# 5) 不验证旧 db 的内容
# 6) 不处理并发（建库是手动单人单机操作，不存在并发场景）
# 7) 不跟随符号链接（若 db_path 是符号链接，直接拒绝并报错）
# 8) 不轮转 log 文件（建库非高频操作，外部工具按需处理）
#
# Non-v0001 targets (deferred, still recorded here)
# 1) 多 db 容器并行管理（understanding.db / data.db 等的统一建库器）
# 2) 归档 db 的索引与可视化检索
# 3) 跨平台权限显式控制（当前以 Windows 单机为主，OS 默认权限即可）
#
# Notes
# - 本脚本是建库器，不是建表器
# - 路径常量直接写在脚本中，不通过 config 暴露
#   理由：action.db 的位置是项目级约定，不应作为可调参数
# - 仅依赖 Python 标准库
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: init_action_db
# family: init_action_db
# role: db_initializer
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/init_action_db_v0001.py
# input:
#   - --by (optional, CLI)
#   - --reason (optional, CLI)
#   - --dry-run (optional, CLI)
# output:
#   - sql/action.db (new empty db file)
#   - sql/archive/action.db.archived_<timestamp> (if old db existed)
#   - logs/action_db_archive.jsonl (append-only event log)
#   - actioning/init_action_db/run_meta.json
# depends_on: []
# used_by:
#   - init_action_schema_v0002
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 本脚本只负责建库（容器层），不建任何表
# - 旧 db 永远归档，不删除（事实保留，可审计）
# - 归档采用改后缀方式：action.db -> action.db.archived_<UTC_timestamp_us>
# - 归档 log 采用 jsonl 追加模式，历史不可变
# - 时间戳精度到微秒，避免同秒多次运行时碰撞
# - dry-run 下不得修改任何文件
# - 符号链接路径直接拒绝，避免误移动链接本身导致数据丢失
# ============================================================

from __future__ import annotations

# ============================================================
# Imports
# ============================================================
import argparse
import json
import os
import shutil
import socket
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 仅使用标准库（无 PyYAML 等第三方依赖）
# - dry-run 下不得创建、移动、写入任何文件
# - 不删除任何文件
# - 不覆盖已归档的 db 文件（微秒时间戳保证唯一）
# - 不跟随符号链接

# ============================================================
# 路径常量区（项目级约定，不通过 config 暴露）
# ============================================================
ACTION_DB_PATH = Path("sql/action.db")
ARCHIVE_DIR = Path("sql/archive")
ARCHIVE_LOG_PATH = Path("logs/action_db_archive.jsonl")
RUN_META_PATH = Path("actioning/init_action_db/run_meta.json")

# ============================================================
# 常量与全局配置区
# ============================================================
SCRIPT_NAME = "init_action_db_v0001.py"
SCRIPT_VERSION = "v0001"
MAIN_LAYER = "action"

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
TIMESTAMP_FMT_US = "%Y%m%dT%H%M%S_%fZ"  # 文件名时间戳，含微秒避免同秒碰撞


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now() -> str:
    """ISO 格式 UTC 时间戳（用于 log 内容字段）。"""
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def utc_now_for_filename() -> str:
    """文件名安全的 UTC 时间戳，含微秒精度。"""
    return datetime.now(timezone.utc).strftime(TIMESTAMP_FMT_US)


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def dumps_json_compact(data: Any) -> str:
    """单行 JSON，用于 jsonl 追加。"""
    return json.dumps(data, ensure_ascii=False)


def get_machine_name() -> str:
    return socket.gethostname()


def get_current_user() -> str:
    """读取系统用户名。失败时返回 unknown。"""
    try:
        return os.getlogin()
    except OSError:
        return "unknown"


def empty_archive_result(reason: str) -> Dict[str, Any]:
    """统一的归档结果空结构。保证返回字段一致性。"""
    return {
        "archived": False,
        "source_path": None,
        "archive_path": None,
        "size_bytes": None,
        "reason": reason,
    }


def empty_create_result(reason: str, path: Optional[Path] = None) -> Dict[str, Any]:
    """统一的创建结果空结构。保证返回字段一致性。"""
    return {
        "created": False,
        "path": str(path) if path else None,
        "size_bytes": None,
        "reason": reason,
    }


# ============================================================
# 核心业务逻辑
# ============================================================
def archive_existing_db(
    db_path: Path,
    archive_dir: Path,
    archive_timestamp: str,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    归档已有的 db 文件。

    返回字典统一结构（即使没归档也填齐字段）：
        archived: bool
        source_path: str | None
        archive_path: str | None
        size_bytes: int | None
        reason: str
    """
    if not db_path.exists():
        return empty_archive_result("no existing db to archive")

    # 拒绝符号链接，避免误移动链接本身
    if db_path.is_symlink():
        raise ValueError(
            f"db_path is a symbolic link, refusing to archive: {db_path}. "
            f"Resolve the link manually before re-running."
        )

    if not db_path.is_file():
        raise ValueError(f"db_path exists but is not a regular file: {db_path}")

    size_bytes = db_path.stat().st_size
    archive_filename = f"{db_path.name}.archived_{archive_timestamp}"
    archive_path = archive_dir / archive_filename

    if archive_path.exists():
        raise FileExistsError(
            f"archive target already exists (timestamp collision?): {archive_path}"
        )

    if dry_run:
        return {
            "archived": False,
            "source_path": str(db_path),
            "archive_path": str(archive_path),
            "size_bytes": size_bytes,
            "reason": "dry_run",
        }

    archive_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(db_path), str(archive_path))

    return {
        "archived": True,
        "source_path": str(db_path),
        "archive_path": str(archive_path),
        "size_bytes": size_bytes,
        "reason": "archived",
    }


def append_event_log(
    log_path: Path,
    log_entry: Dict[str, Any],
    dry_run: bool = False,
) -> None:
    """以 jsonl 追加模式写事件 log。dry_run 下不写入。"""
    if dry_run:
        return

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(dumps_json_compact(log_entry) + "\n")


def create_empty_db(db_path: Path, dry_run: bool = False) -> Dict[str, Any]:
    """
    创建一个全新的空 db 文件。
    通过 sqlite3.connect 触发隐式建库。

    返回字典统一结构：
        created: bool
        path: str
        size_bytes: int | None
        reason: str
    """
    # dry-run 必须先返回，避免与"目标已存在"检查冲突。
    # 因为 dry-run 下 archive 步骤也未实际移动旧 db，
    # 所以 db_path.exists() 在 dry-run 下为 True 是合预期的，不应报错。
    if dry_run:
        return empty_create_result("dry_run", db_path)

    if db_path.exists():
        raise FileExistsError(
            f"db file already exists at target path: {db_path}. "
            f"Archive step should have moved it. This indicates a logic error."
        )

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 隐式建库：连接即创建文件
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("SELECT 1")
        finally:
            conn.close()
    except sqlite3.Error as e:
        # 保留原始异常类型与信息，使用 raise from 保留异常链
        raise RuntimeError(
            f"sqlite3 failed to create db at {db_path}: {type(e).__name__}: {e}"
        ) from e

    if not db_path.exists():
        raise RuntimeError(
            f"sqlite3.connect did not produce expected db file: {db_path}"
        )

    size_bytes = db_path.stat().st_size

    return {
        "created": True,
        "path": str(db_path),
        "size_bytes": size_bytes,
        "reason": "created",
    }


def write_run_meta(run_meta_path: Path, run_meta: Dict[str, Any]) -> Optional[str]:
    """
    原子写出 run_meta。失败时返回 warning 字符串，成功返回 None。
    .tmp + replace 在 Windows 上若目标被占用可能失败，捕获 OSError 不阻塞主流程。
    """
    try:
        run_meta_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = run_meta_path.with_suffix(run_meta_path.suffix + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(dumps_json(run_meta))
        tmp_path.replace(run_meta_path)
        return None
    except OSError as e:
        return f"run_meta write failed: {type(e).__name__}: {e}"


def build_skeleton(
    *,
    status: str,
    dry_run: bool,
    timestamp_iso: str,
    initiated_by: str,
    machine: str,
    reason: Optional[str],
) -> Dict[str, Any]:
    """
    构造 run_meta 的统一骨架。无论成功/失败，所有字段都存在，
    保证下游调用方拿到一致的结构。
    """
    return {
        "status": status,
        "dry_run": dry_run,
        "timestamp": timestamp_iso,
        "initiated_by": initiated_by,
        "machine": machine,
        "reason": reason,
        "action_db_path": str(ACTION_DB_PATH),
        "archive_dir": str(ARCHIVE_DIR),
        "archive_log_path": str(ARCHIVE_LOG_PATH),
        "stage": None,
        "error": None,
        "archive_event": None,
        "create_event": None,
        "archive_result": empty_archive_result("not_attempted"),
        "create_result": empty_create_result("not_attempted"),
        "warnings": [],
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
    }


def run_init_db(
    initiated_by: Optional[str] = None,
    reason: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    执行建库流程。返回统一结构的 run_meta 字典。
    """
    # ----------------------------------------------------------
    # 准备元信息
    # ----------------------------------------------------------
    timestamp_iso = utc_now()
    timestamp_filename = utc_now_for_filename()
    machine = get_machine_name()

    if initiated_by is None or not initiated_by.strip():
        initiated_by = get_current_user()
    else:
        initiated_by = initiated_by.strip()

    if reason is not None:
        reason = reason.strip() or None

    # 初始化骨架
    meta = build_skeleton(
        status="ok",
        dry_run=dry_run,
        timestamp_iso=timestamp_iso,
        initiated_by=initiated_by,
        machine=machine,
        reason=reason,
    )

    # ----------------------------------------------------------
    # 步骤 1: 归档旧 db（如有）
    # ----------------------------------------------------------
    try:
        archive_result = archive_existing_db(
            db_path=ACTION_DB_PATH,
            archive_dir=ARCHIVE_DIR,
            archive_timestamp=timestamp_filename,
            dry_run=dry_run,
        )
        meta["archive_result"] = archive_result
    except (ValueError, FileExistsError, OSError) as e:
        meta["status"] = "error"
        meta["stage"] = "archive"
        meta["error"] = f"{type(e).__name__}: {e}"
        return meta

    # ----------------------------------------------------------
    # 步骤 2: 写归档事件 log
    # ----------------------------------------------------------
    archive_event: Optional[Dict[str, Any]] = None
    if archive_result.get("source_path"):
        archive_event = {
            "event": "archive",
            "timestamp": timestamp_iso,
            "initiated_by": initiated_by,
            "machine": machine,
            "reason": reason,
            "source_path": archive_result["source_path"],
            "archive_path": archive_result["archive_path"],
            "size_bytes": archive_result["size_bytes"],
            "dry_run": dry_run,
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
        }
        meta["archive_event"] = archive_event

        try:
            append_event_log(ARCHIVE_LOG_PATH, archive_event, dry_run=dry_run)
        except OSError as e:
            meta["status"] = "error"
            meta["stage"] = "archive_log"
            meta["error"] = f"archive log write failed: {type(e).__name__}: {e}"
            return meta

    # ----------------------------------------------------------
    # 步骤 3: 创建新的空 db
    # ----------------------------------------------------------
    try:
        create_result = create_empty_db(db_path=ACTION_DB_PATH, dry_run=dry_run)
        meta["create_result"] = create_result
    except (FileExistsError, RuntimeError, OSError) as e:
        meta["status"] = "error"
        meta["stage"] = "create"
        meta["error"] = f"{type(e).__name__}: {e}"
        return meta

    # ----------------------------------------------------------
    # 步骤 4: 写创建事件 log
    # ----------------------------------------------------------
    create_event = {
        "event": "create",
        "timestamp": timestamp_iso,
        "initiated_by": initiated_by,
        "machine": machine,
        "reason": reason,
        "db_path": str(ACTION_DB_PATH),
        "size_bytes": create_result.get("size_bytes"),
        "dry_run": dry_run,
        "script": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
    }
    meta["create_event"] = create_event

    try:
        append_event_log(ARCHIVE_LOG_PATH, create_event, dry_run=dry_run)
    except OSError as e:
        # log 写入失败不阻塞主流程，但要记 warning
        # 此时 db 已经实际建好，主要事实已落地
        meta["warnings"].append(
            f"create event log append failed: {type(e).__name__}: {e}"
        )

    # ----------------------------------------------------------
    # 步骤 5: 写出 run_meta
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
            "Initialize action.db (container layer). "
            "Archives existing db if present, then creates a new empty db file. "
            "Does NOT create any tables."
        )
    )
    parser.add_argument(
        "--by",
        default=None,
        help="Who is initiating this rebuild (optional, defaults to system username)",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="Why rebuilding (optional, written into archive log for audit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report without modifying any files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary = run_init_db(
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
