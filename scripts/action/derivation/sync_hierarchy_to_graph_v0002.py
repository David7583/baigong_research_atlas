#!/usr/bin/env python3
# ============================================================
# filename: sync_hierarchy_to_graph_v0002.py
# 中文名: 层级树图数据库同步脚本
# version: v0002
# layer: derivation
# main_layer: action
# 可更新: True
#
# 职责说明:
# - 消费 analyze_structural_hierarchy_v0004 的产出
# - 将消息树结构（Conversation、MappingNode、层级关系）写入 Neo4j
# - 将 MappingNode 与已有 StructuralUnit 节点通过 CONTAINS_UNIT 边关联
#
# 本脚本做什么:
# 1. 逐文件读取 hierarchy JSONL，提取去重后的节点和边信息
# 2. MERGE Conversation 节点（约束键：conversation_id）
# 3. MERGE MappingNode 节点（约束键：composite_id = asset_id::node_id）
# 4. MERGE HAS_NODE 边（Conversation → MappingNode）
# 5. MERGE PARENT_OF 边（MappingNode → MappingNode）
# 6. MERGE CONTAINS_UNIT 边（MappingNode → StructuralUnit）
#    仅当 unit_id 存在于 register 白名单中（--register 提供时）
#    免登记模式下（--register 省略）所有 content unit 均连接
# 7. 支持 --clear-hierarchy 清除已有 hierarchy 节点和边后重灌
# 8. 支持 --dry-run 只统计不写入
# 9. 流式逐文件处理，逐批提交，无内存硬门槛
#
# 本脚本不做什么:
# 1. 不修改已有的 StructuralUnit / CO_OCCURS_WITH / ADJACENT_TO 数据
# 2. 不生成或修改 hierarchy analyze 的产出
# 3. 不进行任何聚合、过滤或语义判断
# 4. 不回写任何上游产物
#
# 密码不落盘:
# 优先级：--password > NEO4J_PASSWORD 环境变量 > connection config
# 与 init_graph_schema_v0002 / sync_to_graph_v0002 保持一致
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: sync_hierarchy_to_graph
# family: sync_to_graph
# role: graph_database_sync
# version: v0002
# status: active
# entry_point: scripts/action/derivation/sync_hierarchy_to_graph_v0002.py
# input:
#   - analyze_structural_hierarchy_v0004 输出目录
#   - register_structural_units_v0003 输出（可选，白名单）
#   - neo4j_connection_config_yaml (auto-discovered)
# output:
#   - run_meta.json
# depends_on:
#   - neo4j (pip)
#   - analyze_structural_hierarchy_v0004.py
# used_by:
#   - query_graph_direct (downstream)
# notes:
#   - Requires Neo4j server running on bolt://localhost:7688
#   - init_graph_schema_v0002 must have been run first
#   - Does NOT touch StructuralUnit / CO_OCCURS_WITH / ADJACENT_TO
# ============================================================


# ============================================================
# Imports
# ============================================================
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

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
# 常量
# ============================================================
SCRIPT_NAME = "sync_hierarchy_to_graph_v0002.py"
SCRIPT_VERSION = "v0002"
DEFAULT_ENCODING = "utf-8"
DEFAULT_BATCH_SIZE = 500
DEFAULT_NEO4J_URI = "bolt://localhost:7688"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_DATABASE = "neo4j"
PROGRESS_INTERVAL = 200  # 每 N 个文件打印进度
MAX_CONNECT_RETRIES = 3
CONNECT_RETRY_DELAY = 2.0

# Config 项目相对路径；从脚本位置向上查找项目根目录。
DEFAULT_CONNECTION_CONFIG = (
    Path("config")
    / "action"
    / "config"
    / "action_data_neo4j_connection_config_v0001.yml"
)


# ============================================================
# 工具函数
# ============================================================
def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    return sorted(dir_path.glob("*.jsonl"))


def _iter_jsonl(path: Path, encoding: str = DEFAULT_ENCODING):
    """Yield parsed JSON objects from a JSONL file."""
    with open(path, "r", encoding=encoding) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                _log(f"  WARN: skipping malformed JSON at {path.name}:{line_no}")


# ============================================================
# Config 加载
# ============================================================
def _discover_config(script_dir: Path) -> Optional[Path]:
    """Auto-discover connection config YAML."""
    for base in (script_dir, *script_dir.parents):
        candidate = base / DEFAULT_CONNECTION_CONFIG
        if candidate.exists():
            return candidate
    return None


def _load_connection_config(config_path: Optional[Path]) -> Dict[str, Any]:
    """Load connection config from YAML. Returns empty dict if unavailable."""
    if config_path is None or not config_path.exists():
        return {}
    if yaml is None:
        _log("[WARN] PyYAML not installed, cannot read connection config.")
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    conn = data.get("connection", data)
    return {
        "uri": conn.get("uri", ""),
        "user": conn.get("user", ""),
        "password": conn.get("password", ""),
        "database": conn.get("database", ""),
    }


def _resolve_password(
    cli_password: Optional[str], config_password: Optional[str]
) -> Optional[str]:
    """三级优先级: CLI > 环境变量 > config。"""
    if cli_password and cli_password.strip():
        return cli_password.strip()
    env_pw = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env_pw:
        return env_pw
    if config_password and str(config_password).strip():
        return str(config_password).strip()
    return None


# ============================================================
# Neo4j 连接
# ============================================================
def _connect_neo4j(
    uri: str, user: str, password: str, database: str, verbose: bool = False
):
    """连接 Neo4j，带重试。返回 (driver, database)。"""
    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            if verbose:
                _log(f"[SYNC-H] Connected to Neo4j at {uri} (attempt {attempt})")
            return driver, database
        except AuthError:
            _log(f"[ERROR] Neo4j authentication failed for user '{user}'.")
            sys.exit(1)
        except ServiceUnavailable:
            if attempt < MAX_CONNECT_RETRIES:
                _log(
                    f"[WARN] Neo4j unavailable, retry {attempt}/{MAX_CONNECT_RETRIES} "
                    f"in {CONNECT_RETRY_DELAY}s..."
                )
                time.sleep(CONNECT_RETRY_DELAY)
            else:
                _log(f"[ERROR] Neo4j unavailable after {MAX_CONNECT_RETRIES} retries.")
                sys.exit(1)
    return None, database  # unreachable


# ============================================================
# Schema 初始化（幂等）
# ============================================================
def _ensure_hierarchy_schema(driver, database: str, verbose: bool = False) -> None:
    """创建 hierarchy 相关的约束和索引（幂等，不影响已有 schema）。"""
    statements = [
        # Conversation 节点约束
        "CREATE CONSTRAINT conversation_id IF NOT EXISTS "
        "FOR (c:Conversation) REQUIRE c.conversation_id IS UNIQUE",
        # MappingNode 节点约束
        "CREATE CONSTRAINT mapping_node_composite_id IF NOT EXISTS "
        "FOR (m:MappingNode) REQUIRE m.composite_id IS UNIQUE",
        # 索引
        "CREATE INDEX mapping_node_role IF NOT EXISTS "
        "FOR (m:MappingNode) ON (m.role)",
        "CREATE INDEX mapping_node_asset_id IF NOT EXISTS "
        "FOR (m:MappingNode) ON (m.asset_id)",
        "CREATE INDEX conversation_asset_id IF NOT EXISTS "
        "FOR (c:Conversation) ON (c.asset_id)",
        # StructuralUnit.label 索引（幂等声明，CONTAINS_UNIT 匹配依赖此索引）
        "CREATE INDEX structural_unit_label IF NOT EXISTS "
        "FOR (s:StructuralUnit) ON (s.label)",
    ]

    with driver.session(database=database) as session:
        for stmt in statements:
            session.run(stmt)

    if verbose:
        _log("[SYNC-H] Hierarchy schema ensured (constraints + indexes).")


# ============================================================
# Register 白名单加载
# ============================================================
def _load_register_whitelist(
    register_path: Optional[Path],
) -> Optional[Set[str]]:
    """加载 register 的 unit_text 集合作为白名单。None 表示免登记模式。

    注意：hierarchy JSONL 的 unit_id 是实例级（normalize 产出），
    而 register 的 unit_id 是对象级（SHA1(unit_text)），两者不匹配。
    因此白名单基于 unit_text 匹配，对应 Neo4j 中 StructuralUnit 的 label 属性。
    """
    if register_path is None:
        return None
    if not register_path.exists():
        _log(f"[WARN] Register file not found: {register_path}, using no-register mode.")
        return None

    whitelist: Set[str] = set()
    for row in _iter_jsonl(register_path):
        ut = row.get("unit_text")
        if isinstance(ut, str) and ut.strip():
            whitelist.add(ut.strip())

    return whitelist


# ============================================================
# 单文件数据提取
# ============================================================
class HierarchyFileData:
    """从一个 hierarchy JSONL 文件中提取的去重数据。"""

    __slots__ = (
        "conversation_id",
        "title",
        "asset_id",
        "mapping_nodes",      # dict: composite_id -> {role, has_message, tree_depth, is_leaf, node_id}
        "parent_of_edges",    # list of (parent_composite_id, child_composite_id)
        "contains_unit_edges",  # list of (composite_id, unit_text)
    )

    def __init__(self) -> None:
        self.conversation_id: Optional[str] = None
        self.title: Optional[str] = None
        self.asset_id: Optional[str] = None
        self.mapping_nodes: Dict[str, Dict[str, Any]] = {}
        self.parent_of_edges: List[Tuple[str, str]] = []
        self.contains_unit_edges: List[Tuple[str, str]] = []


def _extract_file_data(
    file_path: Path,
    register_whitelist: Optional[Set[str]],
    encoding: str = DEFAULT_ENCODING,
) -> Tuple[HierarchyFileData, Dict[str, int]]:
    """从一个 hierarchy JSONL 中提取去重的节点和边信息。

    单遍扫描，内存只存骨架信息（node属性 + 边指向），不存 unit 内容。
    """
    data = HierarchyFileData()
    counters = {
        "rows_read": 0,
        "rows_skipped": 0,
        "conversation_nodes": 0,
        "mapping_nodes": 0,
        "has_node_edges": 0,
        "parent_of_edges": 0,
        "contains_unit_edges": 0,
        "contains_unit_filtered": 0,
    }

    seen_parent_edges: Set[Tuple[str, str]] = set()
    seen_contains: Set[Tuple[str, str]] = set()

    for row in _iter_jsonl(file_path, encoding):
        counters["rows_read"] += 1

        # 跳过 parse failed 行
        if row.get("__hierarchy_parse_failed__"):
            counters["rows_skipped"] += 1
            continue

        asset_id = row.get("asset_id")
        if data.asset_id is None and asset_id:
            data.asset_id = asset_id

        conv_id = row.get("hierarchy_conversation_id")
        if data.conversation_id is None and conv_id:
            data.conversation_id = conv_id

        title = row.get("hierarchy_title")
        if data.title is None and title:
            data.title = title

        node_id = row.get("hierarchy_node_id")
        if node_id is None:
            # 非 mapping 行，跳过
            continue

        # composite_id 避免跨 conversation 冲突
        composite_id = f"{asset_id}::{node_id}"

        # MappingNode 去重收集
        if composite_id not in data.mapping_nodes:
            data.mapping_nodes[composite_id] = {
                "node_id": node_id,
                "role": row.get("hierarchy_role"),
                "has_message": row.get("hierarchy_has_message"),
                "tree_depth": row.get("hierarchy_tree_depth"),
                "is_leaf": row.get("hierarchy_is_leaf"),
                "asset_id": asset_id,
            }

        # PARENT_OF 边
        parent_node_id = row.get("hierarchy_parent_node_id")
        if parent_node_id:
            parent_composite = f"{asset_id}::{parent_node_id}"
            edge_key = (parent_composite, composite_id)
            if edge_key not in seen_parent_edges:
                seen_parent_edges.add(edge_key)
                data.parent_of_edges.append(edge_key)

        # CONTAINS_UNIT 边：仅对 content/parts 行
        path = row.get("path", "")
        if "message/content/parts" in path:
            unit_text = row.get("unit_text")
            if unit_text and isinstance(unit_text, str) and unit_text.strip():
                ut = unit_text.strip()
                # register 白名单过滤（基于 unit_text）
                if register_whitelist is not None and ut not in register_whitelist:
                    counters["contains_unit_filtered"] += 1
                    continue

                edge_key_cu = (composite_id, ut)
                if edge_key_cu not in seen_contains:
                    seen_contains.add(edge_key_cu)
                    data.contains_unit_edges.append(edge_key_cu)

    counters["mapping_nodes"] = len(data.mapping_nodes)
    counters["conversation_nodes"] = 1 if data.conversation_id else 0
    counters["has_node_edges"] = len(data.mapping_nodes)
    counters["parent_of_edges"] = len(data.parent_of_edges)
    counters["contains_unit_edges"] = len(data.contains_unit_edges)

    return data, counters


# ============================================================
# Neo4j 批量写入
# ============================================================
def _batch_write_conversation(
    driver, database: str, data: HierarchyFileData, sync_id: str
) -> None:
    """MERGE Conversation 节点。"""
    if not data.conversation_id:
        return

    query = """
    MERGE (c:Conversation {conversation_id: $conv_id})
    SET c.title = $title,
        c.asset_id = $asset_id,
        c.sync_id = $sync_id
    """
    with driver.session(database=database) as session:
        session.run(
            query,
            conv_id=data.conversation_id,
            title=data.title,
            asset_id=data.asset_id,
            sync_id=sync_id,
        )


def _batch_write_mapping_nodes(
    driver, database: str, data: HierarchyFileData,
    sync_id: str, batch_size: int
) -> int:
    """MERGE MappingNode 节点，返回写入数量。"""
    nodes_list = [
        {
            "composite_id": cid,
            "node_id": props["node_id"],
            "role": props["role"],
            "has_message": props["has_message"],
            "tree_depth": props["tree_depth"],
            "is_leaf": props["is_leaf"],
            "asset_id": props["asset_id"],
            "sync_id": sync_id,
        }
        for cid, props in data.mapping_nodes.items()
    ]

    query = """
    UNWIND $batch AS row
    MERGE (m:MappingNode {composite_id: row.composite_id})
    SET m.node_id = row.node_id,
        m.role = row.role,
        m.has_message = row.has_message,
        m.tree_depth = row.tree_depth,
        m.is_leaf = row.is_leaf,
        m.asset_id = row.asset_id,
        m.sync_id = row.sync_id
    """

    written = 0
    with driver.session(database=database) as session:
        for i in range(0, len(nodes_list), batch_size):
            batch = nodes_list[i : i + batch_size]
            session.run(query, batch=batch)
            written += len(batch)

    return written


def _batch_write_has_node_edges(
    driver, database: str, data: HierarchyFileData,
    sync_id: str, batch_size: int
) -> int:
    """MERGE HAS_NODE 边 (Conversation → MappingNode)。"""
    if not data.conversation_id:
        return 0

    edges = [
        {"conv_id": data.conversation_id, "composite_id": cid}
        for cid in data.mapping_nodes.keys()
    ]

    query = """
    UNWIND $batch AS row
    MATCH (c:Conversation {conversation_id: row.conv_id})
    MATCH (m:MappingNode {composite_id: row.composite_id})
    MERGE (c)-[:HAS_NODE]->(m)
    """

    written = 0
    with driver.session(database=database) as session:
        for i in range(0, len(edges), batch_size):
            batch = edges[i : i + batch_size]
            session.run(query, batch=batch)
            written += len(batch)

    return written


def _batch_write_parent_of_edges(
    driver, database: str, data: HierarchyFileData,
    sync_id: str, batch_size: int
) -> int:
    """MERGE PARENT_OF 边 (MappingNode → MappingNode)。"""
    edges = [
        {"parent_cid": p, "child_cid": c}
        for p, c in data.parent_of_edges
    ]

    query = """
    UNWIND $batch AS row
    MATCH (p:MappingNode {composite_id: row.parent_cid})
    MATCH (c:MappingNode {composite_id: row.child_cid})
    MERGE (p)-[:PARENT_OF]->(c)
    """

    written = 0
    with driver.session(database=database) as session:
        for i in range(0, len(edges), batch_size):
            batch = edges[i : i + batch_size]
            session.run(query, batch=batch)
            written += len(batch)

    return written


def _batch_write_contains_unit_edges(
    driver, database: str, data: HierarchyFileData,
    sync_id: str, batch_size: int
) -> int:
    """MERGE CONTAINS_UNIT 边 (MappingNode → StructuralUnit)。

    StructuralUnit 节点必须已存在（由 sync_to_graph_v0002 创建）。
    匹配基于 unit_text → StructuralUnit.label（两者一致）。
    使用 MATCH 而非 MERGE，避免意外创建不完整的 StructuralUnit 节点。
    """
    edges = [
        {"composite_id": cid, "unit_text": ut}
        for cid, ut in data.contains_unit_edges
    ]

    query = """
    UNWIND $batch AS row
    MATCH (m:MappingNode {composite_id: row.composite_id})
    MATCH (u:StructuralUnit {label: row.unit_text})
    MERGE (m)-[:CONTAINS_UNIT]->(u)
    """

    written = 0
    with driver.session(database=database) as session:
        for i in range(0, len(edges), batch_size):
            batch = edges[i : i + batch_size]
            session.run(query, batch=batch)
            written += len(batch)

    return written


# ============================================================
# 清库（仅清 hierarchy 相关数据）
# ============================================================
def _clear_hierarchy_data(driver, database: str, verbose: bool = False) -> None:
    """删除所有 Conversation、MappingNode 节点及其关系。

    不触碰 StructuralUnit / CO_OCCURS_WITH / ADJACENT_TO。
    使用 DETACH DELETE 确保节点上的所有边（包括未来新增的边类型）一并清除。
    """
    statements = [
        "MATCH (m:MappingNode) DETACH DELETE m",
        "MATCH (c:Conversation) DETACH DELETE c",
    ]

    with driver.session(database=database) as session:
        for stmt in statements:
            session.run(stmt)

    if verbose:
        _log("[SYNC-H] Cleared all hierarchy data (Conversation + MappingNode + edges).")


# ============================================================
# CLI
# ============================================================
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Sync hierarchy tree structure from analyze output into Neo4j. "
            "v0001: Conversation + MappingNode + HAS_NODE + PARENT_OF + CONTAINS_UNIT."
        )
    )

    p.add_argument(
        "--input-dir", required=True,
        help="Directory containing hierarchy JSONL files (analyze output).",
    )
    p.add_argument(
        "--register", default=None,
        help="Path to register_structural_units JSONL (whitelist). "
             "If omitted, all content units are connected.",
    )
    p.add_argument(
        "--connection-config", default=None,
        help="Path to Neo4j connection config YAML. Auto-discovered if omitted.",
    )
    p.add_argument("--password", default=None, help="Neo4j password (highest priority).")
    p.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Batch size for UNWIND writes (default: {DEFAULT_BATCH_SIZE}).",
    )
    p.add_argument(
        "--clear-hierarchy", action="store_true",
        help="Clear existing hierarchy data before sync.",
    )
    p.add_argument("--dry-run", action="store_true", help="Scan only, do not write.")
    p.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    p.add_argument(
        "--output-dir", default=None,
        help="Directory for run_meta output. Defaults to input-dir.",
    )

    return p


# ============================================================
# Main
# ============================================================
def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        _log(f"[ERROR] Input dir not found: {input_dir}")
        sys.exit(1)

    input_files = _glob_jsonl_files(input_dir)
    if not input_files:
        _log(f"[ERROR] No .jsonl files in: {input_dir}")
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else input_dir
    _safe_mkdir(output_dir)

    started_at = _utc_now_iso()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.verbose:
        _log(f"[SYNC-H] Input: {len(input_files)} files from {input_dir}")

    # Load register whitelist
    register_path = Path(args.register) if args.register else None
    register_whitelist = _load_register_whitelist(register_path)
    if args.verbose:
        if register_whitelist is not None:
            _log(f"[SYNC-H] Register whitelist loaded: {len(register_whitelist)} unit_ids")
        else:
            _log("[SYNC-H] No register provided, using no-register mode (all units accepted).")

    # Resolve Neo4j connection
    script_dir = Path(__file__).resolve().parent
    config_path = (
        Path(args.connection_config)
        if args.connection_config
        else _discover_config(script_dir)
    )
    config = _load_connection_config(config_path)

    uri = config.get("uri") or DEFAULT_NEO4J_URI
    user = config.get("user") or DEFAULT_NEO4J_USER
    database = config.get("database") or DEFAULT_NEO4J_DATABASE
    password = _resolve_password(args.password, config.get("password"))

    if not password:
        _log("[ERROR] No Neo4j password provided. Use --password, NEO4J_PASSWORD env, or config.")
        sys.exit(1)

    # Connect (skip in dry-run)
    driver = None
    if not args.dry_run:
        driver, database = _connect_neo4j(uri, user, password, database, args.verbose)

        # Ensure schema
        _ensure_hierarchy_schema(driver, database, args.verbose)

        # Clear if requested
        if args.clear_hierarchy:
            _clear_hierarchy_data(driver, database, args.verbose)

    # Process files
    total = {
        "files_processed": 0,
        "files_error": 0,
        "total_conversations": 0,
        "total_mapping_nodes": 0,
        "total_has_node_edges": 0,
        "total_parent_of_edges": 0,
        "total_contains_unit_edges": 0,
        "total_contains_unit_filtered": 0,
        "total_rows_read": 0,
    }

    for file_idx, fpath in enumerate(input_files):
        try:
            data, counters = _extract_file_data(fpath, register_whitelist)
            total["total_rows_read"] += counters["rows_read"]
            total["total_conversations"] += counters["conversation_nodes"]
            total["total_mapping_nodes"] += counters["mapping_nodes"]
            total["total_parent_of_edges"] += counters["parent_of_edges"]
            total["total_contains_unit_edges"] += counters["contains_unit_edges"]
            total["total_contains_unit_filtered"] += counters["contains_unit_filtered"]
            total["total_has_node_edges"] += counters["has_node_edges"]

            if not args.dry_run and driver is not None:
                _batch_write_conversation(driver, database, data, run_id)
                _batch_write_mapping_nodes(driver, database, data, run_id, args.batch_size)
                _batch_write_has_node_edges(driver, database, data, run_id, args.batch_size)
                _batch_write_parent_of_edges(driver, database, data, run_id, args.batch_size)
                _batch_write_contains_unit_edges(driver, database, data, run_id, args.batch_size)

            total["files_processed"] += 1

        except Exception as e:
            total["files_error"] += 1
            _log(f"[ERROR] {fpath.name}: {e}")

        if args.verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            _log(
                f"[SYNC-H] Progress: {file_idx + 1}/{len(input_files)} files, "
                f"{total['total_mapping_nodes']} nodes"
            )

    # Close driver
    if driver is not None:
        driver.close()

    # Run meta
    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": _utc_now_iso(),
        "run_id": run_id,
        "input_dir": str(input_dir),
        "register": str(register_path) if register_path else None,
        "register_whitelist_size": len(register_whitelist) if register_whitelist else None,
        "neo4j": {
            "uri": uri,
            "user": user,
            "database": database,
        },
        "dry_run": args.dry_run,
        "clear_hierarchy": args.clear_hierarchy,
        "batch_size": args.batch_size,
        "total_files": len(input_files),
        "counters": total,
        "known_limitations": [
            "v0002 reads analyze_structural_hierarchy_v0004 output.",
            "v0002 does not carry timestamp on nodes (pending timestamp annotation script).",
            "v0002 CONTAINS_UNIT uses MATCH on StructuralUnit.label (unit_text), not unit_id, because hierarchy's instance-level unit_id differs from register's object-level unit_id.",
            "v0002 does not touch StructuralUnit / CO_OCCURS_WITH / ADJACENT_TO data.",
            "v0002 MappingNode composite_id = asset_id::node_id for cross-conversation uniqueness.",
        ],
    }

    meta_path = output_dir / "sync_hierarchy_run_meta.json"
    with meta_path.open("w", encoding=DEFAULT_ENCODING) as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if args.verbose:
        _log(
            f"[SYNC-H] Complete: {total['files_processed']} files, "
            f"{total['total_conversations']} conversations, "
            f"{total['total_mapping_nodes']} mapping nodes, "
            f"{total['total_parent_of_edges']} parent_of edges, "
            f"{total['total_contains_unit_edges']} contains_unit edges "
            f"({total['total_contains_unit_filtered']} filtered), "
            f"errors={total['files_error']}"
        )

    print(json.dumps({
        "status": "ok",
        "counters": total,
        "run_meta": str(meta_path),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
