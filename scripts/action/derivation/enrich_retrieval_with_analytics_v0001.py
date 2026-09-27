#!/usr/bin/env python3
# ============================================================
# File: enrich_retrieval_with_analytics_v0001.py
# 中文名: 检索结果分析增强脚本
# Version: v0001
# Layer: derivation
# Main Layer: action
# Script Type: Retrieval Enrichment
# Updatable: True
#
# Purpose
#
# 接收 understand_retriever 的检索结果，回 DuckDB 查询每个候选的
# 频率统计、属性画像、时间分布等结构化信息，生成增强后的 context
# 供 token_budget_calculator → understand_llm_caller 使用
#
# 在 TRIPLEX-RAG 架构中的位置
#
#   understand_retriever (语义轴/ChromaDB)
#         ↓ retrieval results (JSON)
#   enrich_retrieval_with_analytics (事实轴/DuckDB)    ← 本脚本
#         ↓ enriched results (JSON)
#   [future: enrich_with_graph (关系轴/Neo4j)]
#         ↓
#   token_budget_calculator → understand_llm_caller
#
#
# What it does
#
# 1 接收 retriever 输出的 JSON（含 object_id 列表）
# 2 连接 DuckDB（只读）
# 3 对每个 object_id 回查：
#    - occurrence_count（出现次数）
#    - path_diversity（路径多样性）
#    - dominant_path_ratio（最集中路径占比）
#    - 全部 attribute（完整属性画像）
#    - 相关 instance 数量
# 4 输出增强后的结构化 JSON（可直接传给 token_budget_calculator）
# 5 可选留档
#
# What it does NOT do
#
# 1 不做向量检索（那是 retriever 的活）
# 2 不调用 LLM
# 3 不修改 DuckDB 中的任何数据（只读连接）
# 4 不做制度裁决
# 5 不查询 Neo4j（那是未来 enrich_with_graph 的活）
#
# Design decision
#
# 只读连接
# 使用 read_only=True 连接 DuckDB
#
# 批量查询
# 使用 IN (?, ?, ...) 批量查询，不逐个 object_id 查询
# 对于 attribute 表千万行级别的数据，批量查询显著减少 IO 次数
#
# 渐进增强
# 某个 object_id 在 DuckDB 中找不到数据时不报错
# 返回空的 analytics 字段，原始 retriever 结果不受影响
# 这保证了即使 DuckDB 数据不完整也不阻断 RAG 管线
#
# 接口兼容
# 输出格式兼容 token_budget_calculator 的输入格式
# enriched_results 可以直接替代 retriever 原始 results 传入下游
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: enrich_retrieval_with_analytics
# family: duckdb_query
# role: retrieval_enrichment
# version: v0001
# status: active
# entry_point: scripts/action/derivation/enrich_retrieval_with_analytics_v0001.py
#
# input:
#   - retriever results JSON (--input, file path or JSON string)
#   - derivation/action/duckdb/action_data_analysis.duckdb (read-only)
#
# output:
#   - enriched results JSON (stdout)
#   - enrichment log (optional, actioning/enrichment_logs)
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, sys, traceback
#   - Third-party: duckdb
#
# used_by:
#   - flow_understand_rag_chat (between retriever and token_budget_calculator)
#   - manual exploration
# ============================================================


from __future__ import annotations

import argparse
import json
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "enrich_retrieval_with_analytics_v0001.py"
SCRIPT_VERSION = "v0001"

MAX_ROOT_SEARCH_DEPTH = 10
DEFAULT_DB_PATH = Path("derivation") / "action" / "duckdb" / "action_data_analysis.duckdb"
DEFAULT_LOG_DIR = Path("actioning") / "enrichment_logs"

# Maximum number of object_ids to enrich per call (OOM guard)
MAX_OBJECT_IDS = 500

# Attribute keys to highlight in the summary (others still available in full_attributes)
HIGHLIGHT_ATTR_KEYS = [
    "occurrence_count",
    "path_diversity",
    "dominant_path_ratio",
    "structural_role",
    "prominence_decision",
]


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
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


def _generate_enrichment_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"enrich_{ts}_{suffix}"


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# ============================================================
# DuckDB Batch Queries
# ============================================================

def _batch_query_attributes(
    conn,
    object_ids: List[str],
) -> Dict[str, List[Dict[str, str]]]:
    """Batch query all attributes for a list of object_ids.

    Returns {object_id: [{attr_key, attr_value, attr_state, created_by}, ...]}
    """
    if not object_ids:
        return {}

    placeholders = ", ".join(["?"] * len(object_ids))
    sql = f"""
        SELECT object_id, attr_key, attr_value, attr_state, created_by
        FROM attribute_units_latest
        WHERE object_id IN ({placeholders})
        ORDER BY object_id, attr_key
    """

    try:
        result = conn.execute(sql, object_ids)
        rows = result.fetchall()
    except Exception:
        return {}

    attrs: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        oid = row[0]
        if oid not in attrs:
            attrs[oid] = []
        attrs[oid].append({
            "attr_key": row[1],
            "attr_value": row[2],
            "attr_state": row[3],
            "created_by": row[4],
        })

    return attrs


def _batch_query_instance_counts(
    conn,
    concept_ids: List[str],
) -> Dict[str, int]:
    """For concept object_ids, count how many instances exist.

    Returns {unit_text_id: instance_count}
    """
    if not concept_ids:
        return {}

    placeholders = ", ".join(["?"] * len(concept_ids))
    sql = f"""
        SELECT unit_text_id, COUNT(*) AS cnt
        FROM instance_units_latest
        WHERE unit_text_id IN ({placeholders})
        GROUP BY unit_text_id
    """

    try:
        result = conn.execute(sql, concept_ids)
        rows = result.fetchall()
    except Exception:
        return {}

    return {row[0]: row[1] for row in rows}


def _batch_query_concept_texts(
    conn,
    concept_ids: List[str],
) -> Dict[str, str]:
    """Batch query concept unit_text for a list of unit_text_ids.

    Returns {unit_text_id: unit_text}
    """
    if not concept_ids:
        return {}

    placeholders = ", ".join(["?"] * len(concept_ids))
    sql = f"""
        SELECT unit_text_id, unit_text
        FROM concept_units_latest
        WHERE unit_text_id IN ({placeholders})
    """

    try:
        result = conn.execute(sql, concept_ids)
        rows = result.fetchall()
    except Exception:
        return {}

    return {row[0]: row[1] for row in rows}


# ============================================================
# Core Enrichment Logic
# ============================================================

def enrich(
    retrieval_results: List[Dict[str, Any]],
    db_path: Path,
    include_full_attributes: bool = True,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Enrich retriever results with DuckDB analytics.

    Args:
        retrieval_results: List of result items from understand_retriever
            (each has object_type, object_id, fragment, distance, etc.)
        db_path: Path to DuckDB analysis database
        include_full_attributes: Whether to include all attributes or only highlights
        verbose: Print progress to stderr

    Returns:
        Structured enrichment result with enriched_results list
    """

    enrichment_id = _generate_enrichment_id()
    timestamp = _utc_iso()

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "enrichment_id": enrichment_id,
            "error": "duckdb module not installed. Run: pip install duckdb",
            "timestamp": timestamp,
        }

    if not db_path.exists():
        return {
            "status": "error",
            "enrichment_id": enrichment_id,
            "error": f"DuckDB not found: {db_path}",
            "timestamp": timestamp,
        }

    # Guard: limit number of object_ids
    if len(retrieval_results) > MAX_OBJECT_IDS:
        retrieval_results = retrieval_results[:MAX_OBJECT_IDS]
        if verbose:
            print(f"[WARN] Truncated to {MAX_OBJECT_IDS} results for enrichment.", file=sys.stderr)

    # Collect all unique object_ids
    all_object_ids = []
    concept_ids = []
    for item in retrieval_results:
        oid = item.get("object_id", "")
        if oid:
            all_object_ids.append(oid)
            if item.get("object_type") == "concept":
                concept_ids.append(oid)

    if verbose:
        print(f"[ENRICH] {len(all_object_ids)} objects to enrich ({len(concept_ids)} concepts)", file=sys.stderr)

    conn = None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        # Batch queries
        all_attributes = _batch_query_attributes(conn, all_object_ids)
        instance_counts = _batch_query_instance_counts(conn, concept_ids)
        concept_texts = _batch_query_concept_texts(conn, concept_ids)

        if verbose:
            attrs_found = sum(1 for oid in all_object_ids if oid in all_attributes)
            print(f"[ENRICH] Attributes found for {attrs_found}/{len(all_object_ids)} objects", file=sys.stderr)

        # Enrich each result item
        enriched_results = []
        enriched_count = 0

        for item in retrieval_results:
            oid = item.get("object_id", "")
            otype = item.get("object_type", "")

            # Start with original item data (preserve all retriever fields)
            enriched_item = dict(item)

            # Build analytics block
            analytics = {}

            # Highlighted attributes (flat key-value for easy access)
            obj_attrs = all_attributes.get(oid, [])
            for attr in obj_attrs:
                if attr["attr_key"] in HIGHLIGHT_ATTR_KEYS:
                    analytics[attr["attr_key"]] = attr["attr_value"]

            # Instance count (for concepts)
            if otype == "concept" and oid in instance_counts:
                analytics["instance_count"] = instance_counts[oid]

            # Concept text from DuckDB (cross-check with retriever)
            if otype == "concept" and oid in concept_texts:
                analytics["concept_text_duckdb"] = concept_texts[oid]

            # Full attributes (optional, for deep inspection)
            if include_full_attributes and obj_attrs:
                analytics["full_attributes"] = obj_attrs

            if analytics:
                enriched_count += 1

            enriched_item["analytics"] = analytics
            enriched_results.append(enriched_item)

        return {
            "status": "ok",
            "enrichment_id": enrichment_id,
            "input_count": len(retrieval_results),
            "enriched_count": enriched_count,
            "enriched_results": enriched_results,
            "timestamp": timestamp,
        }

    except Exception as e:
        return {
            "status": "error",
            "enrichment_id": enrichment_id,
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-500:],
            "timestamp": timestamp,
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# Log
# ============================================================

def save_log(result: Dict[str, Any], log_dir: Path) -> Path:
    """Save enrichment result to log directory."""
    _ensure_dir(log_dir)
    enrichment_id = result.get("enrichment_id", "enrich_unknown")
    log_file = log_dir / f"{enrichment_id}.json"
    log_file.write_text(_pretty_json(result), encoding="utf-8")
    return log_file


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="用 DuckDB 分析数据增强 retriever 的检索结果。在 TRIPLEX-RAG 中衔接语义轴和事实轴。"
    )
    ap.add_argument(
        "--input",
        required=True,
        help="retriever 输出的 JSON 文件路径或 JSON 字符串。",
    )
    ap.add_argument(
        "--db-path",
        default=None,
        help="覆盖 DuckDB 路径。",
    )
    ap.add_argument(
        "--no-full-attributes",
        action="store_true",
        help="不包含完整属性列表，只保留 highlight 属性。减少输出体积。",
    )
    ap.add_argument(
        "--log-dir",
        default=None,
        help=f"留档目录。默认: {DEFAULT_LOG_DIR}",
    )
    ap.add_argument(
        "--no-log",
        action="store_true",
        help="不留档。",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="打印详细信息到 stderr。",
    )
    return ap.parse_args()


def _parse_input(input_arg: str) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Parse input from file or JSON string.

    Returns (results_list, full_retrieval_output).
    full_retrieval_output is the complete retriever JSON if available (for pass-through fields).
    """
    raw = input_arg.strip()

    # Try as file path first
    if raw.endswith(".json") and Path(raw).exists():
        with open(raw, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = json.loads(raw)

    # Handle both formats: full retriever output or plain list
    if isinstance(data, dict) and "results" in data:
        return data["results"], data
    elif isinstance(data, list):
        return data, None
    else:
        return [], None


# Need Tuple import
from typing import Tuple


def main() -> int:
    args = parse_args()

    # Resolve paths
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    if args.db_path:
        db_path = Path(args.db_path).resolve()
    else:
        db_path = (project_root / DEFAULT_DB_PATH).resolve()

    # Parse input
    try:
        results, full_retrieval = _parse_input(args.input)
    except Exception as e:
        print(json.dumps({
            "status": "error",
            "error": f"Failed to parse input: {str(e)[:300]}",
        }, ensure_ascii=False, indent=2))
        return 1

    if not results:
        print(json.dumps({
            "status": "ok",
            "enrichment_id": _generate_enrichment_id(),
            "input_count": 0,
            "enriched_count": 0,
            "enriched_results": [],
            "note": "No results to enrich.",
            "timestamp": _utc_iso(),
        }, ensure_ascii=False, indent=2))
        return 0

    # Enrich
    result = enrich(
        retrieval_results=results,
        db_path=db_path,
        include_full_attributes=not args.no_full_attributes,
        verbose=args.verbose,
    )

    # Pass through retrieval metadata if available
    if full_retrieval:
        result["retrieval_id"] = full_retrieval.get("retrieval_id")
        result["query"] = full_retrieval.get("query")
        result["retrieval_mode"] = full_retrieval.get("retrieval_mode")

    # Add script metadata
    result["script"] = SCRIPT_NAME
    result["script_version"] = SCRIPT_VERSION

    # Log
    if not args.no_log:
        log_dir_path = Path(args.log_dir) if args.log_dir else (project_root / DEFAULT_LOG_DIR)
        try:
            log_file = save_log(result, log_dir_path)
            result["log_file"] = str(log_file)
        except Exception as e:
            print(f"[WARN] Failed to save log: {e}", file=sys.stderr)

    # Output
    print(_pretty_json(result))

    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
