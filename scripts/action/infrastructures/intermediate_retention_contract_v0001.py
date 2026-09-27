# ============================================================
# 文件名: intermediate_retention_contract_v0001.py
# 中文名: 中间产物处置契约
# 版本号: v0001
#
# 主层级: action
# 层级: infrastructures / retention
# 脚本定位: 提供处置凭证、文件边界和哈希校验的公共契约
#
# 职责说明:
# - 校验版本化 JSON、绝对路径边界与文件内容身份
# 本脚本做什么:
# - 拒绝链接和重解析点，提供原子证据写入
# 本脚本不做什么:
# - 不选择处置策略，不删除文件，不写业务数据库
# 制度边界声明:
# - 原始数据、代码、配置、数据库和状态索引不属于可删除文件
# 可更新: True
# ============================================================
# ALIAS_META
# alias: intermediate_retention_contract_v0001
# family: intermediate_retention_contract
# role: retention_contract
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/intermediate_retention_contract_v0001.py
# input:
#   - versioned retention JSON and file paths
# output:
#   - validated contracts and file hashes
# depends_on:
#   - Python standard library
# used_by:
#   - intermediate_retention_pipeline_v0001
# ============================================================
from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "intermediate_retention_contract"
SCRIPT_NAME = "intermediate_retention_contract_v0001"
SCRIPT_VERSION = "v0001"
SCHEMA_VERSION = "intermediate_retention_v0001"
PROTECTED_PARTS = {"data_raw", "sql", "chroma", "chromadb", "scripts", "config", "registry", "transaction_backup"}
PROTECTED_SUFFIXES = {".db", ".sqlite", ".sqlite3", ".duckdb", ".wal", ".shm"}


class RetentionError(RuntimeError):
    """A retention contract, evidence or path check failed."""


# ============================================================
# 工具函数区
# ============================================================
def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
    if not isinstance(value, dict):
        raise RetentionError(f"JSON object required: {path}")
    return value


def seal(payload: dict) -> dict:
    result = {**payload, "schema_version": SCHEMA_VERSION}
    result.pop("contract_sha256", None)
    result["contract_sha256"] = hashlib.sha256(canonical(result).encode(DEFAULT_ENCODING)).hexdigest()
    return result


def verify(payload: dict, kind: str) -> dict:
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("kind") != kind:
        raise RetentionError(f"Unexpected {kind} schema")
    if seal(payload)["contract_sha256"] != payload.get("contract_sha256"):
        raise RetentionError(f"Changed {kind} contract")
    return payload


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding=DEFAULT_ENCODING) as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2))
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def no_links(path: Path) -> Path:
    absolute = path.absolute()
    for item in (absolute, *absolute.parents):
        if item.exists() or item.is_symlink():
            info = item.lstat()
            if item.is_symlink() or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
                raise RetentionError(f"Links/reparse points are forbidden: {item}")
    return absolute.resolve()


def within(path: Path, root: Path) -> Path:
    root = no_links(root)
    result = no_links(path)
    if result == root or not result.is_relative_to(root):
        raise RetentionError(f"Path is not a child of the authorized root: {result}")
    return result


def eligible(path: Path, root: Path) -> Path:
    path = within(path, root)
    relative = path.relative_to(root.resolve())
    if set(part.lower() for part in relative.parts) & PROTECTED_PARTS:
        raise RetentionError(f"Protected file tree: {path}")
    if path.suffix.lower() in PROTECTED_SUFFIXES or path.name in {"state.jsonl", "active_index.jsonl"}:
        raise RetentionError(f"Protected persistent store: {path}")
    if path.suffix.lower() != ".jsonl":
        raise RetentionError(f"Only explicitly planned reproducible JSONL payloads are eligible: {path}")
    return path


def check_file(row: dict, root: Path) -> Path:
    path = eligible(Path(row["path"]), root)
    if not path.is_file() or path.stat().st_size != row["size"] or sha(path) != row["sha256"]:
        raise RetentionError(f"Planned file changed or is missing: {path}")
    return path


def cli_result(function) -> int:
    try:
        print(canonical(function()))
        return 0
    except Exception as exc:
        print(canonical({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2 if isinstance(exc, RetentionError) else 3
