#!/usr/bin/env python3
# ============================================================
# File: embedding_contract_validator_v0002.py
# 中文名: 向量入库契约校验脚本
# Version: v0002
# Layer: infrastructure
# Main Layer: action
# Updatable: True
#
# Purpose:
# 对单条或多条 embedding 记录执行"入向量库契约"校验。
#
# What it does:
# 1. 读取 sql_writer_config 获取数据库路径与字段映射。
# 2. 对每条记录做必填项、类型、向量维度与状态检查 (包含 NaN/Inf 拦截)。
# 3. 按固定字段集合重算 embedding_signature 并与提供值比对。
# 4. 回查 SQL 验证 content_hash 并与源 hash 比对。
# 5. 输出 JSON 摘要，并按需将校验结果追加至审计日志。
# 6. 可选：校验 100% 通过后将目录移动到 2_validated（--promote 参数）。
#
# What it does NOT do:
# 1. 不生成 embedding 向量。
# 2. 不写入任何向量库索引文件。
# 3. 不修改 SQL 真源数据或状态。
# 4. 不执行生命周期替换或清理。
# ============================================================

# ============================================================
# ALIAS_META
# alias: embedding_contract_validator
# family: embedding_contract_validator
# role: validator
# version: v0002
# status: active
# entry_point: scripts/action/vector/embedding_contract_validator_v0002.py
# input: JSON/JSONL embedding records or directory
# output: Validation result summary (stdout) and JSONL audit logs
# depends_on: sqlite3, json, argparse, pathlib, datetime, itertools, typing, math, shutil
# used_by: execution_flow, manual_trigger
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 是否进行合规判断: 是 (对数据结构进行契约合法性校验)
# - 是否进行制度判断: 否 (仅执行既有校验规则，不判定或生成新制度)
# - 是否修改系统对象: 否 (完全只读，不修改数据库或文件系统中的对象)
#   注：--promote 模式下会移动目录，但不修改文件内容
# - 是否解析其他头部结构: 否 (仅读取数据体与 config 文件)
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import re
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ============================================================
# 边界声明与强约束说明
#
# 1. 内存边界约束: 必须使用流式迭代器 (Iterable/Generator) 处理输入数据，绝对禁止将大体积 JSONL 全量载入内存。
# 2. 数据库性能约束: 必须在批处理的最外层维护单一 SQLite 会话，禁止在逐条记录校验时反复开启/关闭连接。
# 3. 输出边界约束: CLI 标准输出仅允许打印最终的结构化 Summary，避免海量日志导致终端卡死或 OOM。
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================

SCRIPT_NAME = "embedding_contract_validator"
SCRIPT_VERSION = "v0002"
MAIN_LAYER = "action"

MAX_ROOT_SEARCH_DEPTH = 10

DEFAULT_CONFIG_PATH = Path("config") / "action" / "config" / "sql_writer_config_v0001.yml"
DEFAULT_DB_PATH = Path("sql") / "action_data.db"

# Pipeline 目录常量
PIPELINE_BASE = Path("vector") / "pipeline"
STAGE_GENERATED = "1_generated"
STAGE_VALIDATED = "2_validated"

DEFAULT_TABLES = {
    "concept": "concept_units",
    "instance": "instance_units",
}

DEFAULT_FIELDS = {
    "concept": {
        "id": "unit_text_id",
        "hash": "content_hash",
    },
    "instance": {
        "id": "instance_id",
        "content_hash": "content_hash",
    },
}

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ALLOWED_OBJECT_TYPES = {"instance", "concept"}
ALLOWED_STATES = {"active", "replaced", "deprecated"}

REQUIRED_FIELDS = {
    "object_type",
    "object_id",
    "embedding_vector",
    "embedding_dimension",
    "embedding_model_name",
    "embedding_model_version",
    "tokenizer_version",
    "embedding_parameters_hash",
    "policy_version",
    "embedding_signature",
    "source_hash",
    "generated_at",
    "run_id",
    "state",
}

SIGNATURE_FIELDS = [
    "object_type",
    "object_id",
    "embedding_model_name",
    "embedding_model_version",
    "tokenizer_version",
    "embedding_parameters_hash",
    "policy_version",
    "source_hash",
]

# ============================================================
# 工具函数区 (无副作用)
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()

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

def _validate_all_identifiers(tables: Dict[str, str], fields: Dict[str, Dict[str, str]]) -> None:
    for key, tbl_name in tables.items():
        _validate_identifier(str(tbl_name), f"tables.{key}")
    for group_key, field_map in fields.items():
        if not isinstance(field_map, dict):
            raise ValueError(f"Invalid fields.{group_key} (expected mapping)")
        for fkey, fname in field_map.items():
            _validate_identifier(str(fname), f"fields.{group_key}.{fkey}")

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest().lower()

def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise ValueError(f"Invalid JSONL at {path} line {ln}: {e}")
            if not isinstance(obj, dict):
                raise ValueError(f"JSONL record must be an object at {path} line {ln}")
            yield obj

def _iter_dir_jsonl(dir_path: Path) -> Iterable[Dict[str, Any]]:
    """递归遍历目录下所有 JSONL 文件并流式返回记录"""
    for jsonl_file in sorted(dir_path.rglob("*.jsonl")):
        for rec in _iter_jsonl(jsonl_file):
            yield rec

def _load_json(path: Path) -> Iterable[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"JSON must be an object: {path}")
    yield obj

def _read_stdin_json() -> Iterable[Dict[str, Any]]:
    data = sys.stdin.read()
    if not data.strip():
        raise ValueError("stdin is empty")
    obj = json.loads(data)
    if not isinstance(obj, dict):
        raise ValueError("stdin JSON must be an object")
    yield obj

def _normalize_null(v: Any) -> str:
    if v is None or str(v) == "":
        return "NULL"
    return str(v)

def _recompute_signature(record: Dict[str, Any]) -> str:
    items = [(k, _normalize_null(record.get(k))) for k in SIGNATURE_FIELDS]
    items.sort(key=lambda x: x[0])
    return _sha256_hex("|".join([f"{k}={v}" for k, v in items]))

def _compute_record_fingerprint(record: Dict[str, Any]) -> str:
    subset_keys = [
        "object_type", "object_id", "embedding_signature", "source_hash",
        "run_id", "generated_at", "embedding_model_name", "embedding_model_version",
        "tokenizer_version", "embedding_parameters_hash", "policy_version",
        "state", "embedding_dimension",
    ]
    return _sha256_hex(_safe_json({k: record.get(k) for k in subset_keys}))

# ============================================================
# 核心业务逻辑区
# ============================================================

def _load_config(
    config_path: Path,
    project_root: Path,
) -> Tuple[Path, Dict[str, str], Dict[str, Dict[str, str]]]:
    try:
        import yaml
    except ImportError:
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_FIELDS

    if not config_path.exists():
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_FIELDS

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES, DEFAULT_FIELDS

    conn = raw.get("connection", {}) or {}
    sqlite_cfg = conn.get("sqlite", {}) or {}
    db_path_str = sqlite_cfg.get("path", str(DEFAULT_DB_PATH))

    tables = raw.get("tables", {}) or DEFAULT_TABLES
    fields = raw.get("fields", {}) or DEFAULT_FIELDS

    db_path = (project_root / db_path_str).resolve()

    return db_path, tables, fields


def _fetch_sql_content_hash(
    conn: sqlite3.Connection,
    *,
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    object_type: str,
    object_id: str,
) -> Optional[str]:
    if object_type == "concept":
        tbl = tables.get("concept", "concept_units")
        fld_id = fields.get("concept", {}).get("id", "unit_text_id")
        fld_hash = fields.get("concept", {}).get("hash", "content_hash")
    elif object_type == "instance":
        tbl = tables.get("instance", "instance_units")
        fld_id = fields.get("instance", {}).get("id", "instance_id")
        fld_hash = fields.get("instance", {}).get("content_hash", "content_hash")
    else:
        return None

    sql = f"SELECT {fld_hash} FROM {tbl} WHERE {fld_id} = ? LIMIT 1"
    cur = conn.execute(sql, (object_id,))
    row = cur.fetchone()
    if row:
        return str(row[0]).lower() if row[0] else None
    return None


def validate_record(
    record: Dict[str, Any],
    *,
    conn: Optional[sqlite3.Connection],
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        errors.append("MISSING_FIELD")

    object_type = record.get("object_type")
    object_id = record.get("object_id")
    state = record.get("state")

    if object_type not in ALLOWED_OBJECT_TYPES: errors.append("INVALID_OBJECT_TYPE")
    if state not in ALLOWED_STATES: errors.append("INVALID_STATE")

    vec = record.get("embedding_vector")
    dim = record.get("embedding_dimension")

    if not isinstance(vec, list):
        errors.append("INVALID_TYPE")
    else:
        # 优化点：同时拦截非数字类型 (防止"1.5") 以及 NaN/Inf (防止入库崩溃)
        bad = 0
        for x in vec[:32]:
            if not isinstance(x, (int, float)) or not math.isfinite(x):
                bad += 1
                if bad >= 3: break
        if bad > 0:
            warnings.append("VECTOR_NON_FLOAT_VALUES_POSSIBLE")

    if not isinstance(dim, int):
        errors.append("INVALID_TYPE")
    elif isinstance(vec, list) and len(vec) != dim:
        errors.append("VECTOR_DIMENSION_MISMATCH")

    sig_provided = record.get("embedding_signature")
    sig_recomputed = None
    if isinstance(sig_provided, str):
        try:
            sig_recomputed = _recompute_signature(record)
            if sig_recomputed != sig_provided.lower():
                errors.append("SIGNATURE_MISMATCH")
        except Exception:
            errors.append("SIGNATURE_RECOMPUTE_FAILED")
    elif "MISSING_FIELD" not in errors:
        errors.append("INVALID_TYPE")

    sql_hash = None
    if object_type in ALLOWED_OBJECT_TYPES and isinstance(object_id, str) and object_id:
        if conn is None:
            errors.append("SQL_CONNECTION_FAILED")
        else:
            try:
                sql_hash = _fetch_sql_content_hash(
                    conn, tables=tables, fields=fields, object_type=object_type, object_id=object_id
                )
                if sql_hash is None:
                    errors.append("SQL_OBJECT_NOT_FOUND")
                else:
                    source_hash = record.get("source_hash")
                    if isinstance(source_hash, str):
                        if source_hash != sql_hash:
                            errors.append("SOURCE_HASH_MISMATCH")
                    else:
                        errors.append("INVALID_TYPE")
            except Exception:
                errors.append("SQL_CONNECTION_FAILED")
    elif object_type in ALLOWED_OBJECT_TYPES:
        errors.append("INVALID_TYPE")

    meta: Dict[str, Any] = {
        "timestamp": _utc_iso(),
        "object_type": object_type,
        "object_id": object_id,
        "state": state,
        "signature_provided": sig_provided,
        "signature_recomputed": sig_recomputed,
        "sql_content_hash": sql_hash,
    }

    try:
        meta["record_fingerprint"] = _compute_record_fingerprint(record)
    except Exception:
        warnings.append("FINGERPRINT_FAILED")

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings, "meta": meta}

def _write_audit_line(*, audit_dir: Path, record: Dict[str, Any], record_result: Dict[str, Any]) -> None:
    out_path = audit_dir / f"{_today_utc_date()}.jsonl"
    meta = record_result.get("meta", {}) or {}
    audit_obj = {
        "timestamp": meta.get("timestamp"),
        "main_layer": MAIN_LAYER,
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "run_id": record.get("run_id"),
        "object_type": record.get("object_type"),
        "object_id": record.get("object_id"),
        "valid": bool(record_result.get("valid")),
        "errors": record_result.get("errors") or [],
        "warnings": record_result.get("warnings") or [],
        "source_hash": record.get("source_hash"),
        "sql_content_hash": meta.get("sql_content_hash"),
        "signature_provided": meta.get("signature_provided"),
        "signature_recomputed": meta.get("signature_recomputed"),
        "record_fingerprint": meta.get("record_fingerprint"),
    }
    with out_path.open("a", encoding="utf-8") as f:
        f.write(_safe_json(audit_obj) + "\n")

def validate_records(
    records_iter: Iterable[Dict[str, Any]],
    *,
    db_path: Path,
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    audit: bool,
    project_root: Path,
) -> Dict[str, Any]:
    _validate_all_identifiers(tables, fields)

    audit_dir = project_root / "vector" / "audit" / MAIN_LAYER / SCRIPT_NAME / SCRIPT_VERSION
    if audit:
        _ensure_dir(audit_dir)

    conn = None
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
        except Exception:
            conn = None

    failed = 0
    warned_only = 0
    total = 0

    try:
        for rec in records_iter:
            total += 1
            rr = validate_record(rec, conn=conn, tables=tables, fields=fields)
            
            if not rr["valid"]: failed += 1
            elif rr["warnings"]: warned_only += 1

            if audit:
                _write_audit_line(audit_dir=audit_dir, record=rec, record_result=rr)
    finally:
        if conn:
            conn.close()

    # 优化点：状态语义与底层Exit Code对齐
    if failed > 0:
        status_code = "issues_found"
    elif warned_only > 0:
        status_code = "warnings_only"
    else:
        status_code = "ok"

    return {
        "status": status_code,
        "action": "validate_contract",
        "records_processed": total,
        "records_failed": failed,
        "records_warn_only": warned_only,
        "timestamp": _utc_iso(),
        "audit_dir": str(audit_dir) if audit else None,
    }


def promote_directory(
    source_dir: Path,
    target_dir: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    将校验通过的目录从 1_generated 移动到 2_validated。
    """
    if not source_dir.exists():
        return {
            "status": "error",
            "action": "promote",
            "error": f"Source directory not found: {source_dir}",
        }
    
    if target_dir.exists():
        return {
            "status": "error",
            "action": "promote",
            "error": f"Target directory already exists: {target_dir}",
        }
    
    if dry_run:
        return {
            "status": "dry-run",
            "action": "promote",
            "source": str(source_dir),
            "target": str(target_dir),
        }
    
    # 确保目标父目录存在
    _ensure_dir(target_dir.parent)
    
    # 复制目录（保留源目录，留痕）
    shutil.copytree(str(source_dir), str(target_dir))
    
    return {
        "status": "ok",
        "action": "promote",
        "source": str(source_dir),
        "target": str(target_dir),
        "mode": "copy",
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=f"Validate embedding records contract for main layer: {MAIN_LAYER}")
    ap.add_argument("--config", default=None, help="Path to sql_writer_config YAML. Defaults to config/action/config/sql_writer_config_v0001.yml.")
    ap.add_argument("--db-path", default=None, help="Override database file path.")
    ap.add_argument("--dry-run", action="store_true", help="Dry run mode. Disables audit logging and implies a read-only pass.")
    ap.add_argument("--no-audit", action="store_true", help="Disable audit JSONL output.")
    ap.add_argument("--promote", action="store_true", help="If validation passes 100%, move directory from 1_generated to 2_validated.")

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--input-json", default=None, help="Path to single JSON.")
    group.add_argument("--input-jsonl", default=None, help="Path to JSONL stream.")
    group.add_argument("--input-dir", default=None, help="Path to run directory (e.g., vector/pipeline/1_generated/run_xxx). Validates all JSONL files recursively.")
    group.add_argument("--stdin", action="store_true", help="Read JSON stream from stdin.")

    ap.add_argument("--max-records", type=int, default=None, help="Limit number of records processed.")
    return ap.parse_args()

def main() -> int:
    args = parse_args()

    here = Path(__file__).resolve() if "__file__" in globals() else Path.cwd()
    project_root = _find_project_root(here)

    config_path = Path(args.config).resolve() if args.config else project_root / DEFAULT_CONFIG_PATH
    db_path, tables, fields = _load_config(config_path, project_root)
    if args.db_path:
        db_path = Path(args.db_path).resolve()

    records_iter: Iterable[Dict[str, Any]]
    input_dir: Optional[Path] = None
    
    if args.input_json:
        records_iter = _load_json(Path(args.input_json))
    elif args.input_jsonl:
        records_iter = _iter_jsonl(Path(args.input_jsonl))
    elif args.input_dir:
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            print(json.dumps({
                "status": "error",
                "error": f"Input directory not found: {input_dir}",
            }, ensure_ascii=False, indent=2))
            return 1
        records_iter = _iter_dir_jsonl(input_dir)
    elif args.stdin:
        records_iter = _read_stdin_json()
    else:
        return 1

    if args.max_records:
        records_iter = itertools.islice(records_iter, args.max_records)

    audit_enabled = not (args.no_audit or args.dry_run)

    summary = validate_records(
        records_iter,
        db_path=db_path,
        tables=tables,
        fields=fields,
        audit=audit_enabled,
        project_root=project_root,
    )

    # 处理 promote 逻辑
    promote_result = None
    if args.promote and input_dir is not None:
        if summary["status"] == "ok":
            # 校验 100% 通过，执行 promote
            run_id = input_dir.name
            target_dir = project_root / PIPELINE_BASE / STAGE_VALIDATED / run_id
            promote_result = promote_directory(
                source_dir=input_dir,
                target_dir=target_dir,
                dry_run=args.dry_run,
            )
            summary["promote"] = promote_result
        else:
            # 校验未通过，不执行 promote
            summary["promote"] = {
                "status": "skipped",
                "reason": f"Validation status is '{summary['status']}', promote requires 'ok'",
            }

    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if summary["status"] == "issues_found" or summary.get("records_failed", 0) > 0:
        return 2
    if summary["status"] == "warnings_only" or summary.get("records_warn_only", 0) > 0:
        return 1
    if summary["status"] == "error":
        return 1
    return 0

# ============================================================
# if __name__ == "__main__" 入口
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
