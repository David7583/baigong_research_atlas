# ============================================================
# File: sync_to_graph_v0002.py
# 中文名: 图数据库同步脚本
# Version: v0002
#
# Layer: derivation
# Main Layer: action
# Updatable: True
#
# Purpose
#
# 消费 register（已登记结构单元）和 builder 产出的 edges JSONL，
# 批量写入 Neo4j 图数据库。
#
# What it does
#
# 1 读取 register JSONL，用 UNWIND + MERGE 批量创建 StructuralUnit 节点
#   - MERGE 键：unit_id（sha1，唯一性约束 unique_structural_unit_id）
#   - label 属性：unit_text（RANGE 索引 idx_structural_unit_label）
# 2 读取 cooccurrence edges JSONL，用 UNWIND + MERGE 批量创建 CO_OCCURS_WITH 边
# 3 读取 adjacency edges JSONL，用 UNWIND + MERGE 批量创建 ADJACENT_TO 边（有向）
# 4 支持 --clear 先清库再写入（全量重灌场景）
# 5 支持 --dry-run 只统计不写入
# 6 流式逐行读取 JSONL，按 batch_size 分批提交，无内存硬门槛
# 7 输出 run_meta JSON（写入统计 + 耗时 + 审计信息）
#
# What it does NOT do
#
# 1 不生成或修改 register 或 edges 数据
# 2 不做任何聚合计算——聚合由上游 builder 完成
# 3 不回写任何上游产物
# 4 不处理 CONTAINS 边（待 hierarchy 管线完成后扩展）
#
# Data flow
#
# register JSONL (10.3万节点) ──→ StructuralUnit 节点
#   MERGE on unit_id, SET label = unit_text
# cooccurrence edges JSONL (4万边) ──→ CO_OCCURS_WITH 关系
#   边的 u/v = unit_text → MATCH on label 属性（有索引）
# adjacency edges JSONL (1.4万边) ──→ ADJACENT_TO 关系
#   边的 u/v = unit_text → MATCH on label 属性（有索引）
#
# Neo4j Schema 对齐（action_data_neo4j_schema_config_v0001.yml）
#
# 约束：unique_structural_unit_id → StructuralUnit.unit_id UNIQUENESS
# 索引：idx_structural_unit_label → StructuralUnit.label RANGE
# 关系：CO_OCCURS_WITH（无向存储有向）、ADJACENT_TO（有向）、CONTAINS（待建）
#
# 节点属性映射
#
#   Neo4j 属性        ← register 字段
#   ─────────────────────────────────
#   unit_id           ← sha1_id（对齐 builder 的 SHA1）
#   label             ← unit_text（结构单元文本内容）
#   sha256_id         ← sha256_id（制度身份）
#   prominence        ← prominence（ALLOW/DELAY/FREEZE）
#   sync_id           ← 本次 run_id
#
# 关于 CO_OCCURS_WITH 方向
#
# CO_OCCURS_WITH 是有向关系，保留原文叙事顺序：
# u = 容器内先出现的 unit，v = 后出现的 unit
# analyze v0003 产出 pair 时严格按文本位置排序（sentence_index + char_start）
# builder v0002 直接透传该顺序，不做字典序重排
# 查询时：
#   有向查询 (a)-[:CO_OCCURS_WITH]->(b) = "a 在文本中先于 b 出现"
#   无向查询 (a)-[:CO_OCCURS_WITH]-(b) = "a 和 b 在同一容器内共现"
#
# 密码不落盘
#
# 优先级：--password > NEO4J_PASSWORD 环境变量 > connection config
# 与 init_graph_schema_v0002 保持一致
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================
# alias: sync_to_graph_v0002
# family: sync_to_graph
# role: graph_database_sync
# version: v0002
# status: active
# entry_point: scripts/action/derivation/sync_to_graph_v0002.py
# input:
#   - register_structural_units_jsonl
#   - unit_cooccurrence_edges_jsonl
#   - unit_adjacency_edges_jsonl
#   - neo4j_connection_config_yaml (auto-discovered)
# output:
#   - run_meta_json
# depends_on:
#   - neo4j (pip)
#   - register_structural_units_v0003
#   - build_unit_adjacency_graph_v0003
#   - build_unit_cooccurrence_graph_v0003
# used_by:
#   - query_graph_direct (downstream)
#   - enrich_retrieval_with_graph (downstream)
# notes:
#   - Requires Neo4j server running on bolt://localhost:7688
#   - init_graph_schema_v0002 must have been run first
#   - Schema alignment: unit_id constraint + label index + CO_OCCURS_WITH naming
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable, AuthError
except ImportError:
    print(
        "[ERROR] neo4j package not found. Install with: pip install neo4j",
        file=sys.stderr,
    )
    sys.exit(1)


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "sync_to_graph_v0002"
SCRIPT_VERSION = "v0002"
DEFAULT_BATCH_SIZE = 500  # aligned with neo4j_sync_source_config
DEFAULT_BOLT_URI = "bolt://localhost:7688"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_DATABASE = "neo4j"

# Connection retry (aligned with init_graph_schema_v0002)
MAX_CONNECT_RETRIES = 3
RETRY_DELAY_SEC = 2.0

# Relationship type names (aligned with action_data_neo4j_schema_config_v0001.yml)
REL_COOCCURRENCE = "CO_OCCURS_WITH"
REL_ADJACENCY = "ADJACENT_TO"
REL_HIERARCHY = "CONTAINS"  # reserved for future


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log(msg: str) -> None:
    print(f"[SYNC-GRAPH] {msg}", file=sys.stderr, flush=True)


# ============================================================
# Minimal YAML loader (aligned with init_graph_schema_v0002)
# ============================================================

def _load_yaml_file(path: Path) -> Dict[str, Any]:
    """Load a YAML file. Try PyYAML first, fall back to basic parser."""
    if not path.exists():
        return {}
    try:
        import yaml
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        return data if isinstance(data, dict) else {}
    except ImportError:
        return _load_yaml_fallback(path)
    except Exception:
        return {}


def _load_yaml_fallback(path: Path) -> Dict[str, Any]:
    """Minimal fallback for simple key-value YAML."""
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
        indent = len(line) - len(line.lstrip())
        if ":" not in stripped:
            continue
        key, _, val = stripped.partition(":")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if indent == 0 and not val:
            current_section = key
            if current_section not in result:
                result[current_section] = {}
        elif indent == 0 and val:
            result[key] = val
        elif indent > 0 and current_section and isinstance(result.get(current_section), dict):
            result[current_section][key] = val
    return result


def _resolve_connection(
    config_path: Optional[str],
    project_root: Path,
) -> Dict[str, Any]:
    """Load connection config from YAML, same logic as init_graph_schema."""
    if config_path:
        path = Path(config_path).resolve()
    else:
        path = (project_root / "config" / "action" / "config" / "action_data_neo4j_connection_config_v0001.yml").resolve()

    raw = _load_yaml_file(path)
    conn = raw.get("connection", {}) if isinstance(raw.get("connection"), dict) else {}

    return {
        "uri": conn.get("uri", DEFAULT_BOLT_URI),
        "user": conn.get("user", DEFAULT_NEO4J_USER),
        "database": conn.get("database", DEFAULT_DATABASE),
        "password": conn.get("password"),
        "config_path": str(path),
        "config_found": path.exists(),
    }


def _resolve_password(
    cli_password: Optional[str],
    config_password: Optional[str],
) -> Optional[str]:
    """Resolve password: --password > NEO4J_PASSWORD env > config."""
    if cli_password and cli_password.strip():
        return cli_password.strip()
    env_pw = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env_pw:
        return env_pw
    if config_password and str(config_password).strip():
        return str(config_password).strip()
    return None


def _find_project_root(start: Path) -> Path:
    """Walk up from start to find project root (has scripts/ and config/ dirs)."""
    cur = start.resolve()
    for _ in range(10):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


# ============================================================
# JSONL streaming reader
# ============================================================

def _iter_jsonl(path: Path):
    """Yield parsed JSON objects from a JSONL file, one line at a time."""
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                _log(f"  WARN: skipping malformed JSON at {path.name}:{line_no}")


def _count_lines(path: Path) -> int:
    """Count non-empty lines without loading into memory."""
    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


# ============================================================
# Register node extraction
# ============================================================

def _extract_node_from_register(row: Dict[str, Any], sync_id: str) -> Optional[Dict[str, Any]]:
    """Extract node properties from a register row.

    Maps register fields to Neo4j StructuralUnit properties:
      unit_id    ← sha1_id (builder alignment, constraint key)
      label      ← unit_text (text content, indexed)
      sha256_id  ← sha256_id (institutional identity)
      prominence ← prominence (ALLOW/DELAY/FREEZE)
      sync_id    ← current run_id (audit)

    Returns None if the row is unparseable or missing required fields.
    """
    # unit_text → Neo4j "label" property
    unit_text = None
    for k in ("unit_text", "text", "normalized", "value", "display", "label"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            unit_text = v.strip()
            break

    if not unit_text:
        return None

    # SHA1 ID → Neo4j "unit_id" property (constraint key)
    sha1_id = row.get("sha1_id") or row.get("unit_id") or row.get("id") or ""
    if isinstance(sha1_id, str):
        sha1_id = sha1_id.strip()

    if not sha1_id:
        return None  # must have unit_id for constraint

    # SHA256 ID (institutional identity)
    # register v0002 field name: structural_unit_id (format: SU_v0002_ + SHA256[:12])
    sha256_id = row.get("structural_unit_id") or row.get("sha256_id") or row.get("register_id") or ""
    if isinstance(sha256_id, str):
        sha256_id = sha256_id.strip()

    # Prominence decision
    prominence = row.get("prominence") or row.get("decision") or "ALLOW"
    if isinstance(prominence, str):
        prominence = prominence.strip().upper()

    return {
        "unit_id": sha1_id,
        "label": unit_text,
        "sha256_id": sha256_id,
        "prominence": prominence,
        "sync_id": sync_id,
    }


# ============================================================
# Edge extraction
# ============================================================

def _extract_edge(row: Dict[str, Any], directed: bool) -> Optional[Dict[str, Any]]:
    """Extract edge properties from a builder edges JSONL row.

    Builder v0002 outputs: u, v, u_label, v_label, weight
    where u/v = unit_text.

    Both cooccurrence and adjacency are now directed:
    - cooccurrence: u = earlier in text, v = later in text (narrative order)
    - adjacency: u = from, v = to (physical sequence)
    The directed parameter is retained for future edge types but currently
    all edges preserve original order from builder.

    Returns dict with u, v (= unit_text for MATCH on label), weight.
    """
    u = row.get("u")
    v = row.get("v")

    if not isinstance(u, str) or not isinstance(v, str):
        return None

    u = u.strip()
    v = v.strip()

    if not u or not v:
        return None

    weight = row.get("weight", 1)
    if not isinstance(weight, (int, float)):
        weight = 1

    return {
        "u": u,
        "v": v,
        "weight": int(weight),
    }


# ============================================================
# Neo4j connection (with retry)
# ============================================================

def _connect(uri: str, user: str, password: str, verbose: bool = False):
    """Create and verify Neo4j driver with retry logic."""
    last_error = None
    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            if verbose:
                _log(f"Connected to {uri} (attempt {attempt})")
            return driver
        except AuthError:
            _log(f"ERROR: Authentication failed for user '{user}'. Check password.")
            sys.exit(1)
        except Exception as e:
            last_error = e
            if verbose:
                _log(f"Connection attempt {attempt}/{MAX_CONNECT_RETRIES} failed: {str(e)[:200]}")
            if attempt < MAX_CONNECT_RETRIES:
                time.sleep(RETRY_DELAY_SEC)

    _log(f"ERROR: Cannot connect to Neo4j after {MAX_CONNECT_RETRIES} attempts: {last_error}")
    sys.exit(1)


# ============================================================
# Cypher operations
# ============================================================

def _clear_database(driver, dry_run: bool) -> int:
    """Delete all nodes and relationships. Returns count of deleted nodes."""
    if dry_run:
        with driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) AS cnt")
            cnt = result.single()["cnt"]
            _log(f"  DRY-RUN: would delete {cnt} nodes and all relationships")
            return cnt

    with driver.session() as session:
        total_deleted = 0
        while True:
            result = session.run(
                "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(*) AS deleted"
            )
            deleted = result.single()["deleted"]
            total_deleted += deleted
            if deleted == 0:
                break
            _log(f"  Deleted batch: {deleted} nodes (total: {total_deleted})")

    return total_deleted


def _sync_nodes(
    driver,
    register_path: Path,
    batch_size: int,
    sync_id: str,
    dry_run: bool,
    verbose: bool,
) -> Dict[str, Any]:
    """Read register JSONL and MERGE nodes into Neo4j.

    MERGE on unit_id (constraint key).
    SET label = unit_text (indexed property).
    """
    stats = {
        "rows_read": 0,
        "rows_skipped": 0,
        "nodes_merged": 0,
        "batches": 0,
    }

    if not register_path.exists():
        _log(f"  WARN: register file not found: {register_path}")
        return stats

    batch: List[Dict[str, Any]] = []

    for row in _iter_jsonl(register_path):
        stats["rows_read"] += 1
        node = _extract_node_from_register(row, sync_id)
        if node is None:
            stats["rows_skipped"] += 1
            continue

        batch.append(node)

        if len(batch) >= batch_size:
            if not dry_run:
                _merge_node_batch(driver, batch)
            stats["nodes_merged"] += len(batch)
            stats["batches"] += 1
            if verbose and stats["batches"] % 10 == 0:
                _log(f"  Nodes: {stats['nodes_merged']} merged ({stats['batches']} batches)")
            batch = []

    # Flush remaining
    if batch:
        if not dry_run:
            _merge_node_batch(driver, batch)
        stats["nodes_merged"] += len(batch)
        stats["batches"] += 1

    return stats


def _merge_node_batch(driver, batch: List[Dict[str, Any]]) -> None:
    """UNWIND a batch of nodes and MERGE them.

    MERGE key: unit_id (has UNIQUENESS constraint → fast lookup).
    SET label, sha256_id, prominence, sync_id, timestamps.
    """
    cypher = """
    UNWIND $batch AS row
    MERGE (n:StructuralUnit {unit_id: row.unit_id})
    ON CREATE SET
        n.label = row.label,
        n.sha256_id = row.sha256_id,
        n.prominence = row.prominence,
        n.sync_id = row.sync_id,
        n.created_at = datetime(),
        n.synced_at = datetime()
    ON MATCH SET
        n.label = row.label,
        n.sha256_id = row.sha256_id,
        n.prominence = row.prominence,
        n.sync_id = row.sync_id,
        n.synced_at = datetime()
    """
    with driver.session() as session:
        session.run(cypher, batch=batch)


def _sync_edges(
    driver,
    edges_path: Path,
    edge_type: str,
    batch_size: int,
    sync_id: str,
    dry_run: bool,
    verbose: bool,
) -> Dict[str, Any]:
    """Read edges JSONL and MERGE relationships into Neo4j.

    Edge u/v fields are unit_text. MATCH nodes via label property
    (has RANGE index idx_structural_unit_label).
    """
    directed = (edge_type == REL_ADJACENCY)

    stats = {
        "rows_read": 0,
        "rows_skipped": 0,
        "edges_merged": 0,
        "batches": 0,
    }

    if not edges_path.exists():
        _log(f"  WARN: edges file not found: {edges_path}")
        return stats

    batch: List[Dict[str, Any]] = []

    for row in _iter_jsonl(edges_path):
        stats["rows_read"] += 1
        edge = _extract_edge(row, directed=directed)
        if edge is None:
            stats["rows_skipped"] += 1
            continue

        edge["sync_id"] = sync_id
        batch.append(edge)

        if len(batch) >= batch_size:
            if not dry_run:
                _merge_edge_batch(driver, batch, edge_type)
            stats["edges_merged"] += len(batch)
            stats["batches"] += 1
            if verbose and stats["batches"] % 10 == 0:
                _log(f"  {edge_type}: {stats['edges_merged']} merged ({stats['batches']} batches)")
            batch = []

    # Flush remaining
    if batch:
        if not dry_run:
            _merge_edge_batch(driver, batch, edge_type)
        stats["edges_merged"] += len(batch)
        stats["batches"] += 1

    return stats


def _merge_edge_batch(driver, batch: List[Dict[str, Any]], edge_type: str) -> None:
    """UNWIND a batch of edges and MERGE them.

    Both CO_OCCURS_WITH and ADJACENT_TO use directed MERGE (a)-[r]->(b).
    Nodes matched via label property (= unit_text, indexed).

    For CO_OCCURS_WITH: u/v canonically sorted in _extract_edge.
    Query with undirected MATCH: (a)-[:CO_OCCURS_WITH]-(b).
    """
    if edge_type == REL_COOCCURRENCE:
        cypher = """
        UNWIND $batch AS row
        MATCH (a:StructuralUnit {label: row.u})
        MATCH (b:StructuralUnit {label: row.v})
        MERGE (a)-[r:CO_OCCURS_WITH]->(b)
        ON CREATE SET r.weight = row.weight, r.sync_id = row.sync_id,
                      r.created_at = datetime(), r.synced_at = datetime()
        ON MATCH SET r.weight = row.weight, r.sync_id = row.sync_id,
                     r.synced_at = datetime()
        """
    elif edge_type == REL_ADJACENCY:
        cypher = """
        UNWIND $batch AS row
        MATCH (a:StructuralUnit {label: row.u})
        MATCH (b:StructuralUnit {label: row.v})
        MERGE (a)-[r:ADJACENT_TO]->(b)
        ON CREATE SET r.weight = row.weight, r.sync_id = row.sync_id,
                      r.direction = "forward", r.distance = 1,
                      r.created_at = datetime(), r.synced_at = datetime()
        ON MATCH SET r.weight = row.weight, r.sync_id = row.sync_id,
                     r.synced_at = datetime()
        """
    else:
        raise ValueError(f"Unknown edge type: {edge_type}")

    with driver.session() as session:
        session.run(cypher, batch=batch)


# ============================================================
# Post-sync verification
# ============================================================

def _verify_counts(driver) -> Dict[str, int]:
    """Query Neo4j for node and relationship counts."""
    with driver.session() as session:
        node_count = session.run(
            "MATCH (n:StructuralUnit) RETURN count(n) AS cnt"
        ).single()["cnt"]

        cooccur_count = session.run(
            "MATCH ()-[r:CO_OCCURS_WITH]->() RETURN count(r) AS cnt"
        ).single()["cnt"]

        adjacent_count = session.run(
            "MATCH ()-[r:ADJACENT_TO]->() RETURN count(r) AS cnt"
        ).single()["cnt"]

    return {
        "nodes": node_count,
        "co_occurs_with_edges": cooccur_count,
        "adjacent_to_edges": adjacent_count,
    }


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Sync register nodes + builder edges into Neo4j graph database.\n"
            "Requires init_graph_schema_v0002 to have been run first."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Data sources
    ap.add_argument(
        "--register",
        type=str,
        default=None,
        help="Path to register JSONL file (structural unit registry).",
    )
    ap.add_argument(
        "--cooccurrence-edges",
        type=str,
        default=None,
        help="Path to cooccurrence edges JSONL (from builder).",
    )
    ap.add_argument(
        "--adjacency-edges",
        type=str,
        default=None,
        help="Path to adjacency edges JSONL (from builder).",
    )

    # Neo4j connection
    ap.add_argument(
        "--connection-config",
        default=None,
        help="Path to neo4j_connection_config YAML. Default: config/action/config/action_data_neo4j_connection_config_v0001.yml",
    )
    ap.add_argument(
        "--password",
        default=None,
        help="Neo4j password. Priority: --password > NEO4J_PASSWORD env > connection config.",
    )

    # Behavior
    ap.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size for UNWIND commits (default: {DEFAULT_BATCH_SIZE}).",
    )
    ap.add_argument(
        "--clear",
        action="store_true",
        help="Clear all existing data before sync (for full reload).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Count data without writing to Neo4j.",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress to stderr.",
    )

    # Output
    ap.add_argument(
        "--run-meta",
        type=str,
        default=None,
        help="Path to write run_meta JSON.",
    )

    return ap.parse_args()


# ============================================================
# Main
# ============================================================

def main():
    args = parse_args()
    sync_id = _run_id()
    t0 = time.time()

    _log(f"sync_to_graph {SCRIPT_VERSION} — sync_id: {sync_id}")

    # Validate at least one data source
    if not args.register and not args.cooccurrence_edges and not args.adjacency_edges:
        _log("ERROR: Provide at least one of --register, --cooccurrence-edges, --adjacency-edges")
        sys.exit(1)

    # Resolve project root + connection config
    project_root = _find_project_root(Path(__file__).resolve())
    conn_cfg = _resolve_connection(args.connection_config, project_root)
    password = _resolve_password(args.password, conn_cfg.get("password"))

    if password is None:
        _log("ERROR: No password provided. Use --password, NEO4J_PASSWORD env, or connection config.")
        sys.exit(1)

    uri = conn_cfg["uri"]
    user = conn_cfg["user"]

    if args.verbose:
        _log(f"Project root: {project_root}")
        _log(f"Connection config: {conn_cfg['config_path']} (found: {conn_cfg['config_found']})")
        _log(f"URI: {uri}, User: {user}")

    if args.dry_run:
        _log("DRY-RUN mode: will count data only, no writes")

    _log(f"Connecting to Neo4j at {uri} ...")
    driver = _connect(uri, user, password, args.verbose)
    _log("Connected.")

    # Clear if requested
    cleared = 0
    if args.clear:
        _log("Clearing existing data ...")
        cleared = _clear_database(driver, args.dry_run)
        _log(f"Cleared {cleared} nodes.")

    # Phase 1: Sync nodes from register
    node_stats = {"rows_read": 0, "rows_skipped": 0, "nodes_merged": 0, "batches": 0}
    if args.register:
        register_path = Path(args.register)
        _log(f"Phase 1: Syncing nodes from register: {register_path.name}")
        if args.verbose:
            line_count = _count_lines(register_path)
            _log(f"  Register lines: {line_count}")
        node_stats = _sync_nodes(
            driver, register_path, args.batch_size, sync_id, args.dry_run, args.verbose
        )
        _log(f"  Nodes: {node_stats['nodes_merged']} merged, {node_stats['rows_skipped']} skipped")

    # Phase 2: Sync cooccurrence edges
    cooccur_stats = {"rows_read": 0, "rows_skipped": 0, "edges_merged": 0, "batches": 0}
    if args.cooccurrence_edges:
        cooccur_path = Path(args.cooccurrence_edges)
        _log(f"Phase 2: Syncing cooccurrence edges: {cooccur_path.name}")
        if args.verbose:
            line_count = _count_lines(cooccur_path)
            _log(f"  Edge lines: {line_count}")
        cooccur_stats = _sync_edges(
            driver, cooccur_path, REL_COOCCURRENCE,
            args.batch_size, sync_id, args.dry_run, args.verbose,
        )
        _log(f"  {REL_COOCCURRENCE}: {cooccur_stats['edges_merged']} merged, "
             f"{cooccur_stats['rows_skipped']} skipped")

    # Phase 3: Sync adjacency edges
    adjacent_stats = {"rows_read": 0, "rows_skipped": 0, "edges_merged": 0, "batches": 0}
    if args.adjacency_edges:
        adjacent_path = Path(args.adjacency_edges)
        _log(f"Phase 3: Syncing adjacency edges: {adjacent_path.name}")
        if args.verbose:
            line_count = _count_lines(adjacent_path)
            _log(f"  Edge lines: {line_count}")
        adjacent_stats = _sync_edges(
            driver, adjacent_path, REL_ADJACENCY,
            args.batch_size, sync_id, args.dry_run, args.verbose,
        )
        _log(f"  {REL_ADJACENCY}: {adjacent_stats['edges_merged']} merged, "
             f"{adjacent_stats['rows_skipped']} skipped")

    # Verification
    if not args.dry_run:
        _log("Verifying final counts ...")
        counts = _verify_counts(driver)
        _log(f"  Neo4j: {counts['nodes']} nodes, "
             f"{counts['co_occurs_with_edges']} {REL_COOCCURRENCE}, "
             f"{counts['adjacent_to_edges']} {REL_ADJACENCY}")
    else:
        counts = {
            "nodes": "dry_run",
            "co_occurs_with_edges": "dry_run",
            "adjacent_to_edges": "dry_run",
        }

    driver.close()

    elapsed = round(time.time() - t0, 2)
    _log(f"Done in {elapsed}s.")

    # Build run_meta
    meta = {
        "status": "ok",
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "sync_id": sync_id,
        "dry_run": args.dry_run,
        "clear_before_sync": args.clear,
        "nodes_cleared": cleared,
        "batch_size": args.batch_size,
        "connection": {
            "uri": uri,
            "user": user,
            "config_path": conn_cfg["config_path"],
            "config_found": conn_cfg["config_found"],
        },
        "register": {
            "source": str(args.register) if args.register else None,
            **node_stats,
        },
        "cooccurrence": {
            "source": str(args.cooccurrence_edges) if args.cooccurrence_edges else None,
            "relationship_type": REL_COOCCURRENCE,
            **cooccur_stats,
        },
        "adjacency": {
            "source": str(args.adjacency_edges) if args.adjacency_edges else None,
            "relationship_type": REL_ADJACENCY,
            **adjacent_stats,
        },
        "neo4j_final_counts": counts,
        "elapsed_seconds": elapsed,
        "started_at": _utc_iso(),
    }

    # Print to stdout
    print(json.dumps(meta, ensure_ascii=False, indent=2))

    # Write run_meta file if requested
    if args.run_meta:
        meta_path = Path(args.run_meta)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        _log(f"Run meta written to: {meta_path}")


if __name__ == "__main__":
    main()
