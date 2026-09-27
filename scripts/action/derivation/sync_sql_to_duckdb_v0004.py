#!/usr/bin/env python3
# ============================================================
# File: sync_sql_to_duckdb_v0004.py
# 中文名: SQL 到 DuckDB 同步脚本
# Version: v0004
# Layer: derivation
# Main Layer: action
# Script Type: Data Sync
# Updatable: True
#
# Purpose
#
# 将 SQL库层的事实数据追加式同步至 DuckDB 分析子系统
# 每次同步生成独立的 sync_id，不覆盖历史记录
#
# 本脚本遵循派生层手册 v0003 的设计原则
#
#
# What it does
#
# 1 读取 SQL 数据库中的 instance_units / concept_units / unit_attributes 三张表
# 2 为每条记录附加 sync_id 和 synced_at
# 3 使用 executemany + 显式事务批量写入 DuckDB
# 4 写入 _sync_log 同步日志（每张源表一条记录，同一 sync_id）
# 5 输出同步元信息
#
#
# What it does NOT do
#
# 1 不校验字段（信任 SQL 源，数据已在入库时校验过）
# 2 不做分析计算
# 3 不生成 embedding
# 4 不做制度裁决
# 5 不修改 SQL 源数据
# 6 不覆盖 DuckDB 历史记录
#
#
# v0002 变更说明
#
# - 新增 attribute 表（unit_attributes）同步
# - sync_all 使用统一 sync_id（三张表同一个 sync_id，_sync_log 写三行）
# - 逐行插入改为 executemany + 显式事务（BEGIN/COMMIT）
# - 所有数据库连接使用 try/finally 确保必定关闭
# - _load_configs 对 YAML 解析失败做防御处理
# - _load_configs 读取已确认迁移到行动端的 v0001 config
# - verbose 模式下打印每张表的读取行数和写入行数
# - dry-run 模式下的 preview 限制为 3 行
# - 错误输出包含 traceback 尾部便于定位
# - --target 新增 attribute 选项
#
#
# Design decision
#
# 追加式写入
#
# 每次同步生成新的 sync_id
# 历史记录永久保留
# 支持分析策略演化追溯
#
# 全表同步
#
# v0002 仍采用全表快照模式
# 不做增量同步
# 简单可靠，便于审计
# 增量同步属于 v0003 方向（依赖增量更新模块）
#
# 统一 sync_id
#
# sync_all 模式下三张表共享同一个 sync_id
# _sync_log 按 source_table 区分
# 确保同一次同步的三张表来自同一时间点快照
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: sync_sql_to_duckdb
# family: derivation_sync
# role: sql_to_duckdb_syncer
# version: v0004
# status: active
# entry_point: scripts/action/derivation/sync_sql_to_duckdb_v0004.py
#
# input:
#   - sql/action_data.db (instance_units / concept_units / unit_attributes)
#   - config/action/config/duckdb_schema_config_v0001.yml
#
# output:
#   - derivation/action/duckdb/action_data_analysis.duckdb (+ _sync_log)
#   - run_meta JSON (optional, via --run-meta)
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, sqlite3, hashlib, uuid, sys, traceback, time
#   - Third-party: duckdb, PyYAML (yaml)
#   - Internal: init_action_data_duckdb_schema_v0001.py (tables must exist)
#
# used_by:
#   - analysis scripts
#   - manual invocation for periodic sync
# ============================================================


from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "sync_sql_to_duckdb_v0004.py"
SCRIPT_VERSION = "v0004"

MAX_ROOT_SEARCH_DEPTH = 10

# Default paths (relative to project root)
DEFAULT_SOURCE_DB = Path("sql") / "action_data.db"
DEFAULT_TARGET_DB = Path("derivation") / "action" / "duckdb" / "action_data_analysis.duckdb"

# Default SQL table names (from sql_writer_config)
DEFAULT_SQL_TABLES = {
    "instance": "instance_units",
    "concept": "concept_units",
    "attribute": "unit_attributes",
}

# Default DuckDB table names (from duckdb_schema_config)
DEFAULT_DUCKDB_TABLES = {
    "sync_log": "_sync_log",
    "instance_sync": "instance_units_sync",
    "concept_sync": "concept_units_sync",
    "attribute_sync": "attribute_units_sync",
}

# SQL fields to read (aligned with sql_writer_config / init_sql_schema)
SQL_INSTANCE_FIELDS = [
    "instance_id",
    "unit_text_id",
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
    "content",
    "content_hash",
    "created_at",
    "schema_version",
    "run_id",
]

SQL_CONCEPT_FIELDS = [
    "unit_text_id",
    "unit_text",
    "content_hash",
    "first_seen_instance_id",
    "created_at",
    "schema_version",
    "run_id",
]

SQL_ATTRIBUTE_FIELDS = [
    "attr_id",
    "object_type",
    "object_id",
    "attr_scope",
    "attr_key",
    "attr_value",
    "attr_type",
    "attr_state",
    "evidence_ref",
    "created_at",
    "created_by",
    "run_id",
]

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def _utc_timestamp() -> str:
    """Return UTC timestamp for DuckDB TIMESTAMP field."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


def _generate_sync_id(label: str) -> str:
    """Generate unique sync_id for this sync run."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"sync_{label}_{ts}_{suffix}"


def _compute_file_hash(path: Path) -> Optional[str]:
    """Compute SHA256 hash of a file."""
    if not path.exists():
        return None
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _load_configs(
    project_root: Path,
    verbose: bool = False,
) -> Tuple[Path, Path, Dict[str, str], Dict[str, str], List[str]]:
    """Load SQL and DuckDB configs.

    Returns (source_db, target_db, sql_tables, duckdb_tables, warnings).
    Merges config values with defaults so new v0002 keys are always present.
    """
    warnings: List[str] = []

    source_db = (project_root / DEFAULT_SOURCE_DB).resolve()
    target_db = (project_root / DEFAULT_TARGET_DB).resolve()
    sql_tables = DEFAULT_SQL_TABLES.copy()
    duckdb_tables = DEFAULT_DUCKDB_TABLES.copy()

    try:
        import yaml
    except ImportError:
        warnings.append("PyYAML not installed, using built-in defaults.")
        return source_db, target_db, sql_tables, duckdb_tables, warnings

    # --- Load sql_writer_config ---
    sql_config_path = project_root / "config" / "action" / "config" / "sql_writer_config_v0001.yml"
    if sql_config_path.exists():
        try:
            raw = yaml.safe_load(sql_config_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                conn = raw.get("connection", {}) or {}
                sqlite_cfg = conn.get("sqlite", {}) or {}
                if "path" in sqlite_cfg:
                    source_db = (project_root / sqlite_cfg["path"]).resolve()

                tables = raw.get("tables", {}) or {}
                for key in ("instance", "concept", "attribute"):
                    if key in tables:
                        sql_tables[key] = tables[key]

                if verbose:
                    print(f"[CONFIG] SQL config loaded: {sql_config_path}")
            else:
                warnings.append(f"sql_writer_config is not a mapping, using defaults.")
        except Exception as e:
            warnings.append(f"Failed to parse sql_writer_config: {str(e)[:200]}")
    else:
        warnings.append(f"sql_writer_config not found at {sql_config_path}, using defaults for SQL.")

    # --- Load the confirmed action-side duckdb_schema_config ---
    duckdb_config_path = project_root / "config" / "action" / "config" / "duckdb_schema_config_v0001.yml"
    if not duckdb_config_path.exists():
        warnings.append(f"No duckdb_schema_config found, using defaults for DuckDB.")

    if duckdb_config_path.exists():
        try:
            raw = yaml.safe_load(duckdb_config_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                conn = raw.get("connection", {}) or {}
                duckdb_cfg = conn.get("duckdb", {}) or {}
                if "path" in duckdb_cfg:
                    target_db = (project_root / duckdb_cfg["path"]).resolve()

                tables = raw.get("tables", {}) or {}
                for key in ("sync_log", "instance_sync", "concept_sync", "attribute_sync"):
                    if key in tables:
                        duckdb_tables[key] = tables[key]

                if verbose:
                    print(f"[CONFIG] DuckDB config loaded: {duckdb_config_path}")
            else:
                warnings.append(f"duckdb_schema_config is not a mapping, using defaults.")
        except Exception as e:
            warnings.append(f"Failed to parse duckdb_schema_config: {str(e)[:200]}")

    return source_db, target_db, sql_tables, duckdb_tables, warnings


# ============================================================
# Core: chunked streaming sync (read batch → write batch → repeat)
# ============================================================

# Default chunk size: 10000 rows per batch.
# At ~1KB per row this keeps Python memory under ~50MB per chunk.
# Safe for 16GB laptops even with multiple large tables.
DEFAULT_CHUNK_SIZE = 10000


def _count_sql_table(
    source_db: Path,
    table_name: str,
    target_label: str,
) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """Get row count from a SQL table without loading data.

    Returns (count, error_dict). On success error_dict is None.
    """
    if not source_db.exists():
        return None, {
            "status": "error",
            "target": target_label,
            "error": f"source database not found: {source_db}",
        }

    sql_conn = None
    try:
        sql_conn = sqlite3.connect(str(source_db), timeout=30)
        cursor = sql_conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        return count, None

    except Exception as e:
        return None, {
            "status": "error",
            "target": target_label,
            "error": f"failed to count {table_name}: {str(e)[:300]}",
            "traceback": traceback.format_exc()[-500:],
        }

    finally:
        if sql_conn is not None:
            try:
                sql_conn.close()
            except Exception:
                pass


def _preview_sql_table(
    source_db: Path,
    table_name: str,
    fields: List[str],
    limit: int = 3,
) -> List[Dict[str, Any]]:
    """Read a few rows for dry-run preview. Lightweight, no full scan."""
    sql_conn = None
    try:
        sql_conn = sqlite3.connect(str(source_db), timeout=30)
        sql_conn.row_factory = sqlite3.Row
        fields_str = ", ".join(fields)
        cursor = sql_conn.execute(f"SELECT {fields_str} FROM {table_name} LIMIT ?", (limit,))
        return [{f: row[f] for f in fields} for row in cursor.fetchall()]
    except Exception:
        return []
    finally:
        if sql_conn is not None:
            try:
                sql_conn.close()
            except Exception:
                pass


def _chunked_sync(
    source_db: Path,
    target_db: Path,
    sql_table_name: str,
    duckdb_table: str,
    sync_log_table: str,
    sql_fields: List[str],
    sync_id: str,
    synced_at: str,
    source_count: int,
    source_db_hash: Optional[str],
    target_label: str,
    started_at: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Stream rows from SQLite to DuckDB in chunks with per-chunk commits.

    Opens both connections simultaneously.
    Reads chunk_size rows at a time from SQLite via fetchmany().
    Each chunk is written inside its own BEGIN/COMMIT transaction.
    If the process crashes mid-way, all previously committed chunks are safe.
    Memory usage stays constant regardless of source table size.
    """

    try:
        import duckdb as duckdb_mod
    except ImportError:
        return {
            "status": "error",
            "target": target_label,
            "error": "duckdb module not installed. Run: pip install duckdb",
        }

    sql_conn = None
    duck_conn = None
    synced_count = 0
    chunks_written = 0
    chunks_failed = 0

    # Build DuckDB insert statement
    duckdb_fields = ["sync_id", "synced_at"] + sql_fields
    placeholders = ", ".join(["?"] * len(duckdb_fields))
    fields_str = ", ".join(duckdb_fields)
    insert_sql = f"INSERT INTO {duckdb_table} ({fields_str}) VALUES ({placeholders})"

    t0 = time.monotonic()

    try:
        # Open both connections
        sql_conn = sqlite3.connect(str(source_db), timeout=30)
        sql_conn.row_factory = sqlite3.Row

        duck_conn = duckdb_mod.connect(str(target_db))

        # Stream from SQLite
        sql_fields_str = ", ".join(sql_fields)
        cursor = sql_conn.execute(f"SELECT {sql_fields_str} FROM {sql_table_name}")

        while True:
            raw_chunk = cursor.fetchmany(chunk_size)
            if not raw_chunk:
                break

            # Convert chunk to value tuples (no intermediate dict)
            batch = []
            for row in raw_chunk:
                values = [sync_id, synced_at] + [row[f] for f in sql_fields]
                batch.append(values)

            # Per-chunk transaction: BEGIN -> executemany -> COMMIT
            try:
                duck_conn.execute("BEGIN TRANSACTION")
                duck_conn.executemany(insert_sql, batch)
                duck_conn.execute("COMMIT")
                synced_count += len(batch)
                chunks_written += 1
            except Exception as chunk_err:
                # Rollback this chunk only; previously committed chunks are safe
                try:
                    duck_conn.execute("ROLLBACK")
                except Exception:
                    pass
                chunks_failed += 1
                if verbose:
                    print(f"[ERROR] {target_label}: chunk #{chunks_written + chunks_failed} failed: {str(chunk_err)[:200]}")
                continue

            if verbose:
                pct = (synced_count / source_count * 100) if source_count > 0 else 0
                elapsed_so_far = time.monotonic() - t0
                rows_per_sec = synced_count / elapsed_so_far if elapsed_so_far > 0 else 0
                print(f"[CHUNK] {target_label}: {synced_count}/{source_count} ({pct:.1f}%) — chunk #{chunks_written} — {rows_per_sec:.0f} rows/s")

        # Write sync log entry (in its own transaction)
        finished_at = _utc_timestamp()
        log_sql = (
            f"INSERT INTO {sync_log_table} "
            f"(sync_id, source_db, source_table, record_count, started_at, finished_at, "
            f"script_name, script_version, source_db_hash) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        try:
            duck_conn.execute("BEGIN TRANSACTION")
            duck_conn.execute(log_sql, (
                sync_id,
                str(source_db),
                sql_table_name,
                synced_count,
                started_at,
                finished_at,
                SCRIPT_NAME,
                SCRIPT_VERSION,
                source_db_hash,
            ))
            duck_conn.execute("COMMIT")
        except Exception as log_err:
            try:
                duck_conn.execute("ROLLBACK")
            except Exception:
                pass
            if verbose:
                print(f"[WARN] {target_label}: sync_log write failed: {str(log_err)[:200]}")

        elapsed = time.monotonic() - t0

        if verbose:
            print(f"[DONE] {target_label}: {synced_count} rows in {elapsed:.2f}s ({chunks_written} chunks, {chunks_failed} failed)")

        status = "ok" if chunks_failed == 0 else "partial"

        return {
            "status": status,
            "target": target_label,
            "sync_id": sync_id,
            "source_db": str(source_db),
            "target_db": str(target_db),
            "source_table": sql_table_name,
            "source_count": source_count,
            "synced_count": synced_count,
            "chunks_written": chunks_written,
            "chunks_failed": chunks_failed,
            "chunk_size": chunk_size,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": round(elapsed, 3),
            "source_db_hash": source_db_hash,
        }

    except Exception as e:
        return {
            "status": "error",
            "target": target_label,
            "sync_id": sync_id,
            "synced_before_failure": synced_count,
            "chunks_committed_before_failure": chunks_written,
            "error": f"Chunked sync failed for {target_label}: {str(e)[:400]}",
            "traceback": traceback.format_exc()[-500:],
        }

    finally:
        if sql_conn is not None:
            try:
                sql_conn.close()
            except Exception:
                pass
        if duck_conn is not None:
            try:
                duck_conn.close()
            except Exception:
                pass


# ============================================================
# Sync functions (one per table type)
# ============================================================

def _sync_single_table(
    source_db: Path,
    target_db: Path,
    sql_table_name: str,
    duckdb_table: str,
    sync_log_table: str,
    sql_fields: List[str],
    sync_id: str,
    target_label: str,
    dry_run: bool = False,
    verbose: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Dict[str, Any]:
    """Generic sync for a single table. Used by sync_instance/concept/attribute."""

    started_at = _utc_timestamp()
    synced_at = started_at

    # 1. Count source rows (lightweight, no data loaded)
    source_count, count_error = _count_sql_table(source_db, sql_table_name, target_label)
    if count_error is not None:
        return count_error

    if verbose:
        print(f"[READ] {target_label}: {source_count} rows in {sql_table_name}")

    # 2. Dry-run: return count + preview without loading full table
    if dry_run:
        preview = _preview_sql_table(source_db, sql_table_name, sql_fields, limit=3)
        return {
            "status": "dry-run",
            "target": target_label,
            "sync_id": sync_id,
            "source_db": str(source_db),
            "target_db": str(target_db),
            "source_table": sql_table_name,
            "source_count": source_count,
            "preview_first_3": preview,
        }

    # 3. Check target DB exists
    if not target_db.exists():
        return {
            "status": "error",
            "target": target_label,
            "error": f"target database not found: {target_db}",
            "hint": "run init_action_data_duckdb_schema_v0001.py --init first",
        }

    # 4. Compute source hash for audit
    source_db_hash = _compute_file_hash(source_db)

    # 5. Chunked streaming sync
    return _chunked_sync(
        source_db=source_db,
        target_db=target_db,
        sql_table_name=sql_table_name,
        duckdb_table=duckdb_table,
        sync_log_table=sync_log_table,
        sql_fields=sql_fields,
        sync_id=sync_id,
        synced_at=synced_at,
        source_count=source_count,
        source_db_hash=source_db_hash,
        target_label=target_label,
        started_at=started_at,
        chunk_size=chunk_size,
        verbose=verbose,
    )


def sync_instance(source_db, target_db, sql_tables, duckdb_tables, sync_id, dry_run=False, verbose=False):
    return _sync_single_table(
        source_db=source_db,
        target_db=target_db,
        sql_table_name=sql_tables["instance"],
        duckdb_table=duckdb_tables["instance_sync"],
        sync_log_table=duckdb_tables["sync_log"],
        sql_fields=SQL_INSTANCE_FIELDS,
        sync_id=sync_id,
        target_label="instance",
        dry_run=dry_run,
        verbose=verbose,
    )


def sync_concept(source_db, target_db, sql_tables, duckdb_tables, sync_id, dry_run=False, verbose=False):
    return _sync_single_table(
        source_db=source_db,
        target_db=target_db,
        sql_table_name=sql_tables["concept"],
        duckdb_table=duckdb_tables["concept_sync"],
        sync_log_table=duckdb_tables["sync_log"],
        sql_fields=SQL_CONCEPT_FIELDS,
        sync_id=sync_id,
        target_label="concept",
        dry_run=dry_run,
        verbose=verbose,
    )


def sync_attribute(source_db, target_db, sql_tables, duckdb_tables, sync_id, dry_run=False, verbose=False):
    return _sync_single_table(
        source_db=source_db,
        target_db=target_db,
        sql_table_name=sql_tables["attribute"],
        duckdb_table=duckdb_tables["attribute_sync"],
        sync_log_table=duckdb_tables["sync_log"],
        sql_fields=SQL_ATTRIBUTE_FIELDS,
        sync_id=sync_id,
        target_label="attribute",
        dry_run=dry_run,
        verbose=verbose,
    )


# ============================================================
# sync_all: unified sync_id across all tables
# ============================================================

def sync_all(
    source_db: Path,
    target_db: Path,
    sql_tables: Dict[str, str],
    duckdb_tables: Dict[str, str],
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Sync instance + concept + attribute with a unified sync_id."""

    unified_sync_id = _generate_sync_id("all")

    if verbose:
        print(f"[SYNC] Unified sync_id: {unified_sync_id}")

    instance_result = sync_instance(
        source_db, target_db, sql_tables, duckdb_tables, unified_sync_id, dry_run, verbose
    )
    concept_result = sync_concept(
        source_db, target_db, sql_tables, duckdb_tables, unified_sync_id, dry_run, verbose
    )
    attribute_result = sync_attribute(
        source_db, target_db, sql_tables, duckdb_tables, unified_sync_id, dry_run, verbose
    )

    # Determine overall status
    results = [instance_result, concept_result, attribute_result]
    statuses = [r.get("status") for r in results]

    if all(s == "ok" for s in statuses):
        overall_status = "ok"
    elif all(s == "dry-run" for s in statuses):
        overall_status = "dry-run"
    elif all(s == "error" for s in statuses):
        overall_status = "error"
    elif "error" in statuses:
        overall_status = "partial"
    else:
        overall_status = "ok"

    # Compute totals
    total_source = sum(r.get("source_count", 0) for r in results)
    total_synced = sum(r.get("synced_count", 0) for r in results)

    return {
        "status": overall_status,
        "target": "all",
        "sync_id": unified_sync_id,
        "total_source_count": total_source,
        "total_synced_count": total_synced,
        "instance": instance_result,
        "concept": concept_result,
        "attribute": attribute_result,
        "generated_at": _utc_iso(),
    }


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Sync SQL fact data to DuckDB analysis subsystem (append-only). v0003: action config path alignment."
    )
    ap.add_argument(
        "--target",
        choices=["instance", "concept", "attribute", "all"],
        default="all",
        help="Which table(s) to sync. Default: all",
    )
    ap.add_argument(
        "--source-db",
        default=None,
        help="Override source SQL database path.",
    )
    ap.add_argument(
        "--target-db",
        default=None,
        help="Override target DuckDB database path.",
    )
    ap.add_argument(
        "--run-meta",
        default=None,
        help="Output run meta JSON to file.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run: read from SQL but do not write to DuckDB.",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed progress.",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    # Resolve project root
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    # Load configs
    source_db, target_db, sql_tables, duckdb_tables, config_warnings = _load_configs(
        project_root, verbose=bool(args.verbose)
    )

    # Override paths if specified
    if args.source_db:
        source_db = Path(args.source_db).resolve()
    if args.target_db:
        target_db = Path(args.target_db).resolve()

    if args.verbose:
        print(f"[INFO] Source DB: {source_db}")
        print(f"[INFO] Target DB: {target_db}")
        print(f"[INFO] Target: {args.target}")
        print(f"[INFO] Dry run: {args.dry_run}")

    # Execute sync
    if args.target == "all":
        # sync_all generates its own unified sync_id internally
        result = sync_all(source_db, target_db, sql_tables, duckdb_tables, args.dry_run, args.verbose)
    else:
        # Single-table sync: generate a table-specific sync_id
        sync_id = _generate_sync_id(args.target)
        if args.target == "instance":
            result = sync_instance(source_db, target_db, sql_tables, duckdb_tables, sync_id, args.dry_run, args.verbose)
        elif args.target == "concept":
            result = sync_concept(source_db, target_db, sql_tables, duckdb_tables, sync_id, args.dry_run, args.verbose)
        elif args.target == "attribute":
            result = sync_attribute(source_db, target_db, sql_tables, duckdb_tables, sync_id, args.dry_run, args.verbose)

    # Add script metadata
    result["script"] = SCRIPT_NAME
    result["script_version"] = SCRIPT_VERSION

    # Attach config warnings if any
    if config_warnings:
        result["config_warnings"] = config_warnings

    # Output
    output_json = json.dumps(result, ensure_ascii=False, indent=2)
    print(output_json)

    # Write run_meta to file if requested
    if args.run_meta:
        try:
            run_meta_path = Path(args.run_meta)
            run_meta_path.parent.mkdir(parents=True, exist_ok=True)
            run_meta_path.write_text(output_json, encoding="utf-8")
            if args.verbose:
                print(f"[INFO] Run meta written to: {run_meta_path}")
        except Exception as e:
            print(f"[WARN] Failed to write run_meta: {e}", file=sys.stderr)

    # Return code
    status = result.get("status", "error")
    if status == "error":
        return 1
    if status == "partial":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
