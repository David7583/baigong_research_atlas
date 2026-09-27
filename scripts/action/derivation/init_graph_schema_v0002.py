#!/usr/bin/env python3
# ============================================================
# File: init_graph_schema_v0002.py
# 中文名: Neo4j 图数据库 Schema 初始化脚本
# Version: v0002
# Layer: derivation
# Main Layer: action
# Script Type: Infrastructure
# Updatable: True
#
# Purpose
#
# 在 Neo4j 实例中创建约束和索引，确保图数据库结构就绪
# 供后续 sync_to_graph 写入数据
#
# 本脚本遵循派生层手册 v0003 的设计原则
#
#
# What it does
#
# 1 读取 neo4j_connection_config（连接信息）
# 2 读取 neo4j_schema_config（约束与索引定义）
# 3 连接 Neo4j（密码通过 --password / 环境变量 / config 传入）
# 4 查询已有约束和索引（幂等：已存在的不重复创建）
# 5 创建缺失的约束和索引
# 6 输出结构化 JSON 结果与 run_meta
#
#
# What it does NOT do
#
# 1 不写入任何节点或关系数据（那是 sync_to_graph 的活）
# 2 不删除已有约束或索引（只做添加，不做破坏性操作）
# 3 不修改 Neo4j 服务端配置（端口/内存等由 Desktop 管理）
# 4 不调用 LLM
# 5 不连接 SQLite / DuckDB / ChromaDB
#
#
# Design decision
#
# 幂等执行
#
# 每次执行前先 SHOW CONSTRAINTS / SHOW INDEXES
# 对比 config 定义的目标状态，只创建缺失的
# 已存在的记录为 skipped，不报错
# 脚本可安全地反复执行
#
# 密码不落盘
#
# 优先级：--password > NEO4J_PASSWORD 环境变量 > config 中 password 字段
# 建议生产环境使用环境变量，开发环境可用命令行参数
#
# 连接池最小化
#
# 使用单次 session 执行所有 DDL
# 脚本退出前显式关闭 driver
# 不保持长连接，不消耗连接池资源
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: init_graph_schema
# family: graph_schema
# role: graph_schema_initializer
# version: v0002
# status: active
# entry_point: scripts/action/derivation/init_graph_schema_v0002.py
#
# input:
#   - config/action/config/action_data_neo4j_connection_config_v0001.yml
#   - config/action/config/action_data_neo4j_schema_config_v0001.yml
#
# output:
#   - Neo4j constraints & indexes (idempotent)
#   - run_meta JSON (derivation/action/neo4j/run_meta)
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, sys, os, traceback, time
#   - Third-party: neo4j >= 5.0.0 (Python driver, uses execute_query API), PyYAML (yaml)
#
# used_by:
#   - sync_to_graph (requires schema to exist)
#   - manual invocation for initial setup
# ============================================================


from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "init_graph_schema_v0002.py"
SCRIPT_VERSION = "v0002"

MAX_ROOT_SEARCH_DEPTH = 10

# Default config file names (relative to project_root/config/action/config/)
DEFAULT_CONNECTION_CONFIG = "action_data_neo4j_connection_config_v0001.yml"
DEFAULT_SCHEMA_CONFIG = "action_data_neo4j_schema_config_v0001.yml"

# Default run_meta output directory (relative to project_root)
DEFAULT_RUN_META_DIR = Path("derivation") / "action" / "neo4j" / "run_meta"

# Connection defaults (fallback if config missing)
FALLBACK_URI = "bolt://localhost:7688"
FALLBACK_USER = "neo4j"
FALLBACK_DATABASE = "neo4j"

# Timeout for driver connectivity verification (seconds)
CONNECTIVITY_TIMEOUT_SEC = 15

# Maximum retries for transient connection failures
MAX_CONNECT_RETRIES = 3
RETRY_DELAY_SEC = 2.0


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    """UTC timestamp in ISO 8601 format, no microseconds."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _utc_compact() -> str:
    """Compact UTC timestamp for run_id generation."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    """Walk up from start to find project root (has scripts/ and config/ dirs)."""
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


def _pretty_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _safe_mkdir(path: Path) -> None:
    """Create directory and parents if needed, no error if exists."""
    path.mkdir(parents=True, exist_ok=True)


# ============================================================
# YAML Config Loader (defensive)
# ============================================================

def _load_yaml_file(path: Path) -> Dict[str, Any]:
    """Load a YAML file, return empty dict on any failure."""
    if not path.exists():
        return {}

    try:
        import yaml
    except ImportError:
        # Fallback: attempt very basic key-value parsing for flat YAML
        return _load_yaml_fallback(path)

    try:
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def _load_yaml_fallback(path: Path) -> Dict[str, Any]:
    """Minimal fallback parser for simple YAML when PyYAML is not installed.

    Only handles top-level and one-level nested 'key: value' lines.
    Sufficient for connection config; schema config really needs PyYAML.
    """
    result: Dict[str, Any] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return result

    current_section: Optional[str] = None
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        # Detect indentation level
        indent = len(line) - len(line.lstrip())

        if ":" not in stripped:
            continue

        key, _, val = stripped.partition(":")
        key = key.strip()
        val = val.strip().strip('"').strip("'")

        if indent == 0 and not val:
            # Section header like "connection:"
            current_section = key
            if current_section not in result:
                result[current_section] = {}
        elif indent == 0 and val:
            # Top-level key-value
            result[key] = val
        elif indent > 0 and current_section and isinstance(result.get(current_section), dict):
            # Nested key-value
            result[current_section][key] = val

    return result


# ============================================================
# Config Resolution
# ============================================================

def _resolve_connection_config(
    config_path: Optional[str],
    project_root: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Resolve and load connection config.

    Returns merged dict with guaranteed keys: uri, user, database.
    """
    if config_path:
        path = Path(config_path).resolve()
    else:
        path = (project_root / "config" / "action" / "config" / DEFAULT_CONNECTION_CONFIG).resolve()

    if verbose:
        print(f"[CONFIG] Connection config: {path}", file=sys.stderr)

    raw = _load_yaml_file(path)

    # Extract connection block (handle both flat and nested)
    conn = raw.get("connection", {}) if isinstance(raw.get("connection"), dict) else {}

    return {
        "uri": conn.get("uri", FALLBACK_URI),
        "user": conn.get("user", FALLBACK_USER),
        "database": conn.get("database", FALLBACK_DATABASE),
        "password": conn.get("password"),  # may be None
        "config_path": str(path),
        "config_found": path.exists(),
    }


def _resolve_schema_config(
    config_path: Optional[str],
    project_root: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Resolve and load schema config.

    Returns the raw parsed YAML dict. Caller extracts constraints/indexes.
    """
    if config_path:
        path = Path(config_path).resolve()
    else:
        path = (project_root / "config" / "action" / "config" / DEFAULT_SCHEMA_CONFIG).resolve()

    if verbose:
        print(f"[CONFIG] Schema config: {path}", file=sys.stderr)

    raw = _load_yaml_file(path)
    raw["_config_path"] = str(path)
    raw["_config_found"] = path.exists()
    return raw


def _resolve_password(
    cli_password: Optional[str],
    config_password: Optional[str],
) -> Optional[str]:
    """Resolve password with priority: CLI > env > config.

    Returns None if no password found anywhere.
    """
    # 1. CLI argument
    if cli_password and cli_password.strip():
        return cli_password.strip()

    # 2. Environment variable
    env_pw = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env_pw:
        return env_pw

    # 3. Config file
    if config_password and str(config_password).strip():
        return str(config_password).strip()

    return None


# ============================================================
# Schema Target Extraction
# ============================================================

def _extract_target_constraints(schema_config: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract constraint definitions from schema config.

    Returns list of dicts with keys: name, label, property, type.
    Validates each entry defensively.
    """
    constraints_raw = schema_config.get("constraints", [])
    if not isinstance(constraints_raw, list):
        return []

    targets: List[Dict[str, str]] = []
    for item in constraints_raw:
        if not isinstance(item, dict):
            continue

        name = str(item.get("name", "")).strip()
        label = str(item.get("label", "")).strip()
        prop = str(item.get("property", "")).strip()
        ctype = str(item.get("type", "UNIQUENESS")).strip().upper()

        if not name or not label or not prop:
            continue

        targets.append({
            "name": name,
            "label": label,
            "property": prop,
            "type": ctype,
        })

    return targets


def _extract_target_indexes(schema_config: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract index definitions from schema config.

    Returns list of dicts with keys: name, label, property, type.
    Validates each entry defensively.
    """
    indexes_raw = schema_config.get("indexes", [])
    if not isinstance(indexes_raw, list):
        return []

    targets: List[Dict[str, str]] = []
    for item in indexes_raw:
        if not isinstance(item, dict):
            continue

        name = str(item.get("name", "")).strip()
        label = str(item.get("label", "")).strip()
        prop = str(item.get("property", "")).strip()
        itype = str(item.get("type", "RANGE")).strip().upper()

        if not name or not label or not prop:
            continue

        targets.append({
            "name": name,
            "label": label,
            "property": prop,
            "type": itype,
        })

    return targets


# ============================================================
# Neo4j Operations
# ============================================================

def _create_driver(uri: str, user: str, password: str, verbose: bool = False):
    """Create Neo4j driver with retry logic.

    Returns (driver, error_message). error_message is None on success.
    """
    try:
        from neo4j import GraphDatabase
    except ImportError:
        return None, "neo4j Python driver not installed. Run: pip install neo4j"

    last_error = None
    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))

            # Verify connectivity with timeout
            driver.verify_connectivity()

            if verbose:
                print(f"[CONNECT] Connected to {uri} (attempt {attempt})", file=sys.stderr)

            return driver, None

        except Exception as e:
            last_error = e
            if verbose:
                print(
                    f"[CONNECT] Attempt {attempt}/{MAX_CONNECT_RETRIES} failed: {str(e)[:200]}",
                    file=sys.stderr,
                )
            if attempt < MAX_CONNECT_RETRIES:
                time.sleep(RETRY_DELAY_SEC)

    return None, f"Failed to connect after {MAX_CONNECT_RETRIES} attempts: {str(last_error)[:300]}"


def _close_driver(driver, verbose: bool = False) -> None:
    """Close driver safely."""
    if driver is None:
        return
    try:
        driver.close()
        if verbose:
            print("[CONNECT] Driver closed.", file=sys.stderr)
    except Exception:
        pass


def _query_existing_constraints(driver, database: str) -> List[Dict[str, Any]]:
    """Query all existing constraints in the database.

    Returns list of constraint info dicts.
    Memory-safe: constraint count is always small (< 1000 in any real system).
    """
    results = []
    try:
        records, _, _ = driver.execute_query(
            "SHOW CONSTRAINTS",
            database_=database,
        )
        for record in records:
            results.append({
                "name": record.get("name") or "",
                "type": record.get("type") or "",
                "entityType": record.get("entityType") or "",
                "labelsOrTypes": record.get("labelsOrTypes") or [],
                "properties": record.get("properties") or [],
            })
    except Exception:
        # Older Neo4j versions may not support SHOW CONSTRAINTS
        # Fall through with empty list; creation will handle duplicates via IF NOT EXISTS
        pass

    return results


def _query_existing_indexes(driver, database: str) -> List[Dict[str, Any]]:
    """Query all existing indexes in the database.

    Returns list of index info dicts.
    Memory-safe: index count is always small.
    """
    results = []
    try:
        records, _, _ = driver.execute_query(
            "SHOW INDEXES",
            database_=database,
        )
        for record in records:
            results.append({
                "name": record.get("name") or "",
                "type": record.get("type") or "",
                "entityType": record.get("entityType") or "",
                "labelsOrTypes": record.get("labelsOrTypes") or [],
                "properties": record.get("properties") or [],
                "state": record.get("state") or "",
            })
    except Exception:
        pass

    return results


def _constraint_exists(
    target: Dict[str, str],
    existing: List[Dict[str, Any]],
) -> bool:
    """Check if a constraint already exists by name or by label+property+type match."""
    target_name = target["name"]
    target_label = target["label"]
    target_prop = target["property"]
    target_type = target.get("type", "UNIQUENESS").upper()

    # Map config type names to Neo4j SHOW CONSTRAINTS type strings
    type_aliases: Dict[str, set] = {
        "UNIQUENESS": {"UNIQUENESS", "NODE_PROPERTY_UNIQUENESS"},
        "NODE_KEY": {"NODE_KEY"},
        "NOT_NULL": {"NOT_NULL", "NODE_PROPERTY_EXISTENCE"},
    }
    acceptable_types = type_aliases.get(target_type, {target_type})

    for item in existing:
        # Match by name (exact)
        if item.get("name") == target_name:
            return True

        # Match by label + property + type (in case name differs)
        labels = item.get("labelsOrTypes") or []
        props = item.get("properties") or []
        item_type = str(item.get("type") or "").upper()

        if (target_label in labels
                and target_prop in props
                and item_type in acceptable_types):
            return True

    return False


def _index_exists(
    target: Dict[str, str],
    existing: List[Dict[str, Any]],
) -> bool:
    """Check if an index already exists by name or by label+property+type match.

    Special case: uniqueness constraints auto-create a UNIQUENESS-type index.
    If the target is a RANGE index and a UNIQUENESS index already covers the
    same label+property, we still create the RANGE index (they serve different
    query patterns). But if the exact same type already exists, we skip.
    """
    target_name = target["name"]
    target_label = target["label"]
    target_prop = target["property"]
    target_type = target.get("type", "RANGE").upper()

    # Map config type names to Neo4j SHOW INDEXES type strings
    type_aliases: Dict[str, set] = {
        "RANGE": {"RANGE"},
        "TEXT": {"TEXT"},
        "FULLTEXT": {"FULLTEXT"},
    }
    acceptable_types = type_aliases.get(target_type, {target_type})

    for item in existing:
        # Match by name (exact)
        if item.get("name") == target_name:
            return True

        # Match by label + property + type
        labels = item.get("labelsOrTypes") or []
        props = item.get("properties") or []
        item_type = str(item.get("type") or "").upper()

        if (target_label in labels
                and target_prop in props
                and item_type in acceptable_types):
            return True

    return False


def _build_create_constraint_cypher(target: Dict[str, str]) -> str:
    """Build Cypher for creating a constraint.

    Uses IF NOT EXISTS for extra safety (belt + suspenders with pre-check).
    All identifiers are backtick-escaped to handle reserved words and special characters.
    """
    name = target["name"]
    label = target["label"]
    prop = target["property"]
    ctype = target["type"]

    if ctype == "UNIQUENESS":
        return (
            f"CREATE CONSTRAINT `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.`{prop}` IS UNIQUE"
        )
    elif ctype == "NODE_KEY":
        return (
            f"CREATE CONSTRAINT `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.`{prop}` IS NODE KEY"
        )
    elif ctype == "NOT_NULL":
        return (
            f"CREATE CONSTRAINT `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.`{prop}` IS NOT NULL"
        )
    else:
        # Default to uniqueness for unknown types
        return (
            f"CREATE CONSTRAINT `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.`{prop}` IS UNIQUE"
        )


def _build_create_index_cypher(target: Dict[str, str]) -> str:
    """Build Cypher for creating an index.

    Uses IF NOT EXISTS for extra safety.
    All identifiers are backtick-escaped to handle reserved words and special characters.
    """
    name = target["name"]
    label = target["label"]
    prop = target["property"]
    itype = target["type"]

    if itype == "RANGE":
        return (
            f"CREATE INDEX `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) ON (n.`{prop}`)"
        )
    elif itype == "TEXT":
        return (
            f"CREATE TEXT INDEX `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) ON (n.`{prop}`)"
        )
    elif itype == "FULLTEXT":
        # Fulltext index has different syntax
        return (
            f"CREATE FULLTEXT INDEX `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) ON EACH [n.`{prop}`]"
        )
    else:
        # Default to range index
        return (
            f"CREATE INDEX `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) ON (n.`{prop}`)"
        )


def _execute_ddl(
    driver,
    database: str,
    cypher: str,
    verbose: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Execute a single DDL Cypher statement.

    Returns (success, error_message).
    """
    try:
        driver.execute_query(cypher, database_=database)
        if verbose:
            print(f"[DDL] OK: {cypher}", file=sys.stderr)
        return True, None
    except Exception as e:
        err = str(e)[:300]
        # "already exists" is not an error in our logic (IF NOT EXISTS should prevent,
        # but some Neo4j versions may still raise)
        if "already exists" in err.lower() or "equivalent" in err.lower():
            if verbose:
                print(f"[DDL] Already exists (safe): {cypher}", file=sys.stderr)
            return True, None
        return False, err


# ============================================================
# Core Logic
# ============================================================

def init_schema(
    connection_config: Dict[str, Any],
    schema_config: Dict[str, Any],
    password: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Main schema initialization logic.

    Returns structured result dict.
    """
    run_id = _utc_compact()
    started_at = _utc_iso()
    start_time = time.monotonic()

    uri = connection_config.get("uri", FALLBACK_URI)
    user = connection_config.get("user", FALLBACK_USER)
    database = connection_config.get("database", FALLBACK_DATABASE)

    # Extract targets from schema config
    target_constraints = _extract_target_constraints(schema_config)
    target_indexes = _extract_target_indexes(schema_config)

    if verbose:
        print(f"[SCHEMA] Target constraints: {len(target_constraints)}", file=sys.stderr)
        print(f"[SCHEMA] Target indexes: {len(target_indexes)}", file=sys.stderr)

    # Connect
    driver, connect_error = _create_driver(uri, user, password, verbose)
    if connect_error:
        return {
            "status": "error",
            "error": connect_error,
            "run_id": run_id,
            "started_at": started_at,
            "elapsed_sec": round(time.monotonic() - start_time, 3),
        }

    try:
        # Query existing state
        existing_constraints = _query_existing_constraints(driver, database)
        existing_indexes = _query_existing_indexes(driver, database)

        if verbose:
            print(f"[SCHEMA] Existing constraints: {len(existing_constraints)}", file=sys.stderr)
            print(f"[SCHEMA] Existing indexes: {len(existing_indexes)}", file=sys.stderr)

        # Classify constraints
        constraints_to_create: List[Dict[str, str]] = []
        constraints_skipped: List[str] = []

        for target in target_constraints:
            if _constraint_exists(target, existing_constraints):
                constraints_skipped.append(target["name"])
                if verbose:
                    print(f"[SCHEMA] Constraint exists, skip: {target['name']}", file=sys.stderr)
            else:
                constraints_to_create.append(target)

        # Classify indexes
        indexes_to_create: List[Dict[str, str]] = []
        indexes_skipped: List[str] = []

        for target in target_indexes:
            if _index_exists(target, existing_indexes):
                indexes_skipped.append(target["name"])
                if verbose:
                    print(f"[SCHEMA] Index exists, skip: {target['name']}", file=sys.stderr)
            else:
                indexes_to_create.append(target)

        # Dry run: report only
        if dry_run:
            return {
                "status": "dry-run",
                "run_id": run_id,
                "started_at": started_at,
                "connection": {
                    "uri": uri,
                    "user": user,
                    "database": database,
                },
                "existing_constraints": len(existing_constraints),
                "existing_indexes": len(existing_indexes),
                "constraints_to_create": [c["name"] for c in constraints_to_create],
                "constraints_to_skip": constraints_skipped,
                "indexes_to_create": [i["name"] for i in indexes_to_create],
                "indexes_to_skip": indexes_skipped,
                "elapsed_sec": round(time.monotonic() - start_time, 3),
            }

        # Execute constraint creation
        constraints_created: List[str] = []
        constraints_failed: List[Dict[str, str]] = []

        for target in constraints_to_create:
            cypher = _build_create_constraint_cypher(target)
            success, err = _execute_ddl(driver, database, cypher, verbose)
            if success:
                constraints_created.append(target["name"])
            else:
                constraints_failed.append({"name": target["name"], "error": err or "unknown"})

        # Execute index creation
        indexes_created: List[str] = []
        indexes_failed: List[Dict[str, str]] = []

        for target in indexes_to_create:
            cypher = _build_create_index_cypher(target)
            success, err = _execute_ddl(driver, database, cypher, verbose)
            if success:
                indexes_created.append(target["name"])
            else:
                indexes_failed.append({"name": target["name"], "error": err or "unknown"})

        # Determine overall status
        has_failures = len(constraints_failed) > 0 or len(indexes_failed) > 0
        if has_failures and (len(constraints_created) > 0 or len(indexes_created) > 0):
            overall_status = "partial"
        elif has_failures:
            overall_status = "error"
        else:
            overall_status = "ok"

        return {
            "status": overall_status,
            "run_id": run_id,
            "started_at": started_at,
            "connection": {
                "uri": uri,
                "user": user,
                "database": database,
            },
            "constraints_created": constraints_created,
            "constraints_skipped": constraints_skipped,
            "constraints_failed": constraints_failed,
            "indexes_created": indexes_created,
            "indexes_skipped": indexes_skipped,
            "indexes_failed": indexes_failed,
            "elapsed_sec": round(time.monotonic() - start_time, 3),
        }

    except Exception as e:
        return {
            "status": "error",
            "run_id": run_id,
            "started_at": started_at,
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-500:],
            "elapsed_sec": round(time.monotonic() - start_time, 3),
        }

    finally:
        _close_driver(driver, verbose)


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Neo4j 图数据库 Schema 初始化（幂等）。"
            "创建约束和索引，已存在的自动跳过。"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    ap.add_argument(
        "--password",
        default=None,
        help=(
            "Neo4j 密码。优先级：--password > NEO4J_PASSWORD 环境变量 > config 文件。"
        ),
    )
    ap.add_argument(
        "--connection-config",
        default=None,
        help=f"覆盖连接配置路径。默认: config/action/config/{DEFAULT_CONNECTION_CONFIG}",
    )
    ap.add_argument(
        "--schema-config",
        default=None,
        help=f"覆盖 schema 配置路径。默认: config/action/config/{DEFAULT_SCHEMA_CONFIG}",
    )
    ap.add_argument(
        "--run-meta",
        default=None,
        help="指定 run_meta 输出路径。默认: derivation/action/neo4j/run_meta/",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="只检查现状并报告差异，不执行任何创建。",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="打印详细信息到 stderr。",
    )

    return ap.parse_args()


def main() -> int:
    args = parse_args()

    # Resolve project root
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    if args.verbose:
        print(f"[INFO] Project root: {project_root}", file=sys.stderr)

    # Load configs
    connection_config = _resolve_connection_config(
        args.connection_config, project_root, args.verbose,
    )
    schema_config = _resolve_schema_config(
        args.schema_config, project_root, args.verbose,
    )

    # Check schema config found
    if not schema_config.get("_config_found", False):
        print(_pretty_json({
            "status": "error",
            "error": f"Schema config not found: {schema_config.get('_config_path', 'unknown')}",
            "hint": "Create action_data_neo4j_schema_config_v0001.yml in config/action/config/ directory.",
        }))
        return 1

    # Resolve password
    password = _resolve_password(
        cli_password=args.password,
        config_password=connection_config.get("password"),
    )

    if password is None:
        print(_pretty_json({
            "status": "error",
            "error": "No password provided.",
            "hint": (
                "Use --password <pwd>, or set NEO4J_PASSWORD environment variable, "
                "or uncomment password in connection config."
            ),
        }))
        return 1

    # Execute
    result = init_schema(
        connection_config=connection_config,
        schema_config=schema_config,
        password=password,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    # Add script metadata
    result["script"] = SCRIPT_NAME
    result["script_version"] = SCRIPT_VERSION

    # Add config paths for audit
    result["configs"] = {
        "connection": connection_config.get("config_path", ""),
        "connection_found": connection_config.get("config_found", False),
        "schema": schema_config.get("_config_path", ""),
        "schema_found": schema_config.get("_config_found", False),
    }

    # Write run_meta
    if args.run_meta:
        run_meta_path = Path(args.run_meta)
    else:
        run_meta_dir = (project_root / DEFAULT_RUN_META_DIR).resolve()
        _safe_mkdir(run_meta_dir)
        run_meta_path = run_meta_dir / f"init_schema_{result.get('run_id', 'unknown')}.json"

    try:
        _safe_mkdir(run_meta_path.parent)
        run_meta_path.write_text(
            _pretty_json(result), encoding="utf-8",
        )
        result["run_meta_path"] = str(run_meta_path)
        if args.verbose:
            print(f"[INFO] Run meta written to: {run_meta_path}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] Failed to write run_meta: {e}", file=sys.stderr)

    # Output
    print(_pretty_json(result))

    # Return code
    status = result.get("status", "error")
    if status == "error":
        return 1
    if status == "partial":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
