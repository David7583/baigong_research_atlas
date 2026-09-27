#!/usr/bin/env python3
# ============================================================
# File: query_duckdb_direct_v0001.py
# 中文名: DuckDB 直接查询脚本
# Version: v0001
# Layer: derivation
# Main Layer: action
# Script Type: Query & Analysis
# Updatable: True
#
# Purpose
#
# 对 DuckDB 分析子系统进行直接查询
# 用于数据探索、频率统计、全局分析等场景
# 不涉及 LLM 调用，不涉及向量检索
#
# What it does
#
# 1 连接 DuckDB 分析子系统（action_data_analysis.duckdb）
# 2 执行预定义查询（--preset）或自定义 SQL（--sql）
# 3 分页输出（--limit / --offset），防止大结果集 OOM
# 4 支持 JSONL 导出（--export）
# 5 输出结构化 JSON 结果
#
# What it does NOT do
#
# 1 不修改 DuckDB 中的任何数据（只读连接）
# 2 不调用 LLM
# 3 不做向量检索
# 4 不做制度裁决
# 5 不连接 SQLite 或 ChromaDB
#
# Design decision
#
# 只读连接
# 使用 read_only=True 连接 DuckDB，物理上不可能误写
#
# 预定义查询
# 内置常用分析查询，避免用户手写复杂 SQL
# 每个 preset 有名称、描述、参数化 SQL
#
# 分页与 OOM 防护
# 默认 limit=100，最大 limit=10000
# 大结果集通过 --export 导出 JSONL 文件，逐行写入不占内存
#
# 自定义 SQL 白名单
# --sql 模式下拒绝包含 INSERT/UPDATE/DELETE/DROP/ALTER/CREATE 的语句
# 防止误操作（虽然 read_only 已经保护了，但双保险）
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: query_duckdb_direct
# family: duckdb_query
# role: direct_query
# version: v0001
# status: active
# entry_point: scripts/action/derivation/query_duckdb_direct_v0001.py
#
# input:
#   - derivation/action/duckdb/action_data_analysis.duckdb (read-only)
#   - --preset / --sql (query definition)
#
# output:
#   - structured JSON result (stdout)
#   - JSONL export (optional, via --export)
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, sys, re, traceback
#   - Third-party: duckdb
#
# used_by:
#   - manual data exploration
#   - global_importance_analyzer (future)
# ============================================================


from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "query_duckdb_direct_v0001.py"
SCRIPT_VERSION = "v0001"

MAX_ROOT_SEARCH_DEPTH = 10
DEFAULT_DB_PATH = Path("derivation") / "action" / "duckdb" / "action_data_analysis.duckdb"

DEFAULT_LIMIT = 100
MAX_LIMIT = 10000
DEFAULT_OFFSET = 0

# SQL write keywords to reject in --sql mode (double safety on top of read_only)
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)

# Maximum rows for JSONL export per run (prevent runaway exports)
MAX_EXPORT_ROWS = 500000


# ============================================================
# Preset Queries
# ============================================================

PRESETS: Dict[str, Dict[str, Any]] = {
    "top_concepts": {
        "description": "出现次数最高的 N 个概念（需要 attribute 表中有 occurrence_count）",
        "sql": """
            SELECT c.unit_text, CAST(a.attr_value AS INTEGER) AS occurrence_count
            FROM concept_units_latest c
            JOIN attribute_units_latest a
              ON a.object_id = c.unit_text_id
              AND a.attr_key = 'occurrence_count'
            ORDER BY CAST(a.attr_value AS INTEGER) DESC
            LIMIT {limit} OFFSET {offset}
        """,
    },
    "top_concepts_by_diversity": {
        "description": "路径多样性最高的 N 个概念（分布在最多不同路径中的概念）",
        "sql": """
            SELECT c.unit_text, CAST(a.attr_value AS INTEGER) AS path_diversity
            FROM concept_units_latest c
            JOIN attribute_units_latest a
              ON a.object_id = c.unit_text_id
              AND a.attr_key = 'path_diversity'
            ORDER BY CAST(a.attr_value AS INTEGER) DESC
            LIMIT {limit} OFFSET {offset}
        """,
    },
    "high_value_concepts": {
        "description": "高频且高多样性的概念（occurrence_count >= 10 AND path_diversity >= 5）",
        "sql": """
            SELECT
              c.unit_text,
              CAST(occ.attr_value AS INTEGER) AS occurrence_count,
              CAST(div.attr_value AS INTEGER) AS path_diversity,
              CAST(ratio.attr_value AS DOUBLE) AS dominant_path_ratio
            FROM concept_units_latest c
            JOIN attribute_units_latest occ
              ON occ.object_id = c.unit_text_id AND occ.attr_key = 'occurrence_count'
            JOIN attribute_units_latest div
              ON div.object_id = c.unit_text_id AND div.attr_key = 'path_diversity'
            LEFT JOIN attribute_units_latest ratio
              ON ratio.object_id = c.unit_text_id AND ratio.attr_key = 'dominant_path_ratio'
            WHERE CAST(occ.attr_value AS INTEGER) >= 10
              AND CAST(div.attr_value AS INTEGER) >= 5
            ORDER BY CAST(occ.attr_value AS INTEGER) DESC
            LIMIT {limit} OFFSET {offset}
        """,
    },
    "concept_profile": {
        "description": "查看某个概念的全部属性画像（需要 --param concept_text=xxx）",
        "sql": """
            SELECT a.attr_key, a.attr_value, a.attr_state, a.created_by
            FROM concept_units_latest c
            JOIN attribute_units_latest a
              ON a.object_id = c.unit_text_id
            WHERE c.unit_text = '{concept_text}'
            ORDER BY a.attr_key
            LIMIT {limit} OFFSET {offset}
        """,
        "params": ["concept_text"],
    },
    "concept_instances": {
        "description": "查看某个概念的所有 instance 出现位置（需要 --param concept_text=xxx）",
        "sql": """
            SELECT i.asset_id, i.path, i.segment_index, i.char_start, i.char_end, i.content
            FROM concept_units_latest c
            JOIN instance_units_latest i
              ON i.unit_text_id = c.unit_text_id
            WHERE c.unit_text = '{concept_text}'
            ORDER BY i.asset_id, i.segment_index
            LIMIT {limit} OFFSET {offset}
        """,
        "params": ["concept_text"],
    },
    "attribute_distribution": {
        "description": "各 attr_key 的记录数分布（了解属性类型构成）",
        "sql": """
            SELECT attr_key, COUNT(*) AS count
            FROM attribute_units_latest
            GROUP BY attr_key
            ORDER BY count DESC
            LIMIT {limit} OFFSET {offset}
        """,
    },
    "table_stats": {
        "description": "三张 latest 视图的行数统计",
        "sql": """
            SELECT
              (SELECT COUNT(*) FROM instance_units_latest) AS instance_count,
              (SELECT COUNT(*) FROM concept_units_latest) AS concept_count,
              (SELECT COUNT(*) FROM attribute_units_latest) AS attribute_count
        """,
    },
    "sync_history": {
        "description": "同步日志历史",
        "sql": """
            SELECT sync_id, source_table, record_count, started_at, finished_at, script_version
            FROM _sync_log
            ORDER BY finished_at DESC
            LIMIT {limit} OFFSET {offset}
        """,
    },
    "concept_search": {
        "description": "模糊搜索概念名称（需要 --param keyword=xxx）",
        "sql": """
            SELECT unit_text_id, unit_text
            FROM concept_units_latest
            WHERE unit_text ILIKE '%{keyword}%'
            LIMIT {limit} OFFSET {offset}
        """,
        "params": ["keyword"],
    },
}


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


def _validate_custom_sql(sql: str) -> Optional[str]:
    """Validate custom SQL is read-only. Returns error message or None."""
    match = _WRITE_KEYWORDS.search(sql)
    if match:
        return f"Forbidden keyword detected: '{match.group()}'. Only SELECT queries are allowed."
    return None


def _parse_params(param_list: Optional[List[str]]) -> Dict[str, str]:
    """Parse --param key=value pairs into a dict."""
    params = {}
    if not param_list:
        return params
    for p in param_list:
        if "=" not in p:
            continue
        key, _, value = p.partition("=")
        key = key.strip()
        value = value.strip()
        # Basic SQL injection prevention for param values
        if "'" in value or ";" in value or "--" in value:
            raise ValueError(f"Unsafe characters in param value for '{key}': quotes, semicolons, and comments are not allowed.")
        params[key] = value
    return params


# ============================================================
# Query Execution
# ============================================================

def execute_query(
    db_path: Path,
    sql: str,
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Execute a read-only SQL query against DuckDB.

    Returns structured result with column names, rows, and metadata.
    """

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "error": "duckdb module not installed. Run: pip install duckdb",
            "timestamp": _utc_iso(),
        }

    if not db_path.exists():
        return {
            "status": "error",
            "error": f"database not found: {db_path}",
            "timestamp": _utc_iso(),
        }

    conn = None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        if verbose:
            print(f"[SQL] {sql.strip()[:200]}...", file=sys.stderr)

        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description] if result.description else []
        rows_raw = result.fetchall()

        # Convert to list of dicts
        rows = []
        for row in rows_raw:
            row_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                # Convert non-serializable types to string
                if isinstance(val, (bytes, memoryview)):
                    val = val.hex() if isinstance(val, bytes) else bytes(val).hex()
                elif hasattr(val, 'isoformat'):
                    val = val.isoformat()
                row_dict[col] = val
            rows.append(row_dict)

        return {
            "status": "ok",
            "columns": columns,
            "row_count": len(rows),
            "rows": rows,
            "limit": limit,
            "offset": offset,
            "has_more": len(rows) == limit,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-500:],
            "timestamp": _utc_iso(),
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# JSONL Export
# ============================================================

def export_query_jsonl(
    db_path: Path,
    sql: str,
    output_path: Path,
    max_rows: int = MAX_EXPORT_ROWS,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Execute query and stream results to JSONL file, row by row.

    Memory-safe: never holds more than one row in memory.
    """

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "error": "duckdb module not installed.",
            "timestamp": _utc_iso(),
        }

    if not db_path.exists():
        return {
            "status": "error",
            "error": f"database not found: {db_path}",
            "timestamp": _utc_iso(),
        }

    conn = None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description] if result.description else []

        output_path.parent.mkdir(parents=True, exist_ok=True)
        written = 0

        with open(output_path, "w", encoding="utf-8") as f:
            while True:
                row = result.fetchone()
                if row is None:
                    break
                if written >= max_rows:
                    if verbose:
                        print(f"[EXPORT] Reached max_rows limit: {max_rows}", file=sys.stderr)
                    break

                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, (bytes, memoryview)):
                        val = val.hex() if isinstance(val, bytes) else bytes(val).hex()
                    elif hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    row_dict[col] = val

                f.write(json.dumps(row_dict, ensure_ascii=False) + "\n")
                written += 1

                if verbose and written % 10000 == 0:
                    print(f"[EXPORT] {written} rows written...", file=sys.stderr)

        if verbose:
            print(f"[EXPORT] Done: {written} rows → {output_path}", file=sys.stderr)

        return {
            "status": "ok",
            "rows_exported": written,
            "output_path": str(output_path),
            "max_rows_limit": max_rows,
            "truncated": written >= max_rows,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-500:],
            "timestamp": _utc_iso(),
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="DuckDB 直接查询（只读）。支持预定义查询和自定义 SQL。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_build_preset_help(),
    )

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--preset",
        choices=list(PRESETS.keys()),
        help="执行预定义查询。使用 --list-presets 查看可用查询。",
    )
    group.add_argument(
        "--sql",
        default=None,
        help="执行自定义 SQL（仅允许 SELECT）。",
    )
    group.add_argument(
        "--list-presets",
        action="store_true",
        help="列出所有预定义查询。",
    )

    ap.add_argument("--param", action="append", help="查询参数，格式: key=value。可多次指定。")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help=f"最大返回行数。默认: {DEFAULT_LIMIT}，最大: {MAX_LIMIT}")
    ap.add_argument("--offset", type=int, default=DEFAULT_OFFSET, help=f"跳过前 N 行。默认: {DEFAULT_OFFSET}")
    ap.add_argument("--export", default=None, help="导出结果到 JSONL 文件（流式写入，不受 --limit 限制）。")
    ap.add_argument("--db-path", default=None, help="覆盖 DuckDB 路径。")
    ap.add_argument("--verbose", action="store_true", help="打印详细信息到 stderr。")

    return ap.parse_args()


def _build_preset_help() -> str:
    lines = ["\n可用的预定义查询 (--preset):"]
    for name, info in PRESETS.items():
        params = info.get("params", [])
        param_str = f"  (需要参数: {', '.join(params)})" if params else ""
        lines.append(f"  {name:30s} {info['description']}{param_str}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    # List presets
    if args.list_presets:
        for name, info in PRESETS.items():
            params = info.get("params", [])
            param_str = f" [params: {', '.join(params)}]" if params else ""
            print(f"  {name:30s} {info['description']}{param_str}")
        return 0

    # Resolve paths
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    if args.db_path:
        db_path = Path(args.db_path).resolve()
    else:
        db_path = (project_root / DEFAULT_DB_PATH).resolve()

    # Clamp limit
    limit = max(1, min(args.limit, MAX_LIMIT))
    offset = max(0, args.offset)

    # Parse params
    try:
        params = _parse_params(args.param)
    except ValueError as e:
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False, indent=2))
        return 1

    # Build SQL
    if args.preset:
        preset = PRESETS[args.preset]
        # Check required params
        required_params = preset.get("params", [])
        for rp in required_params:
            if rp not in params:
                print(json.dumps({
                    "status": "error",
                    "error": f"Preset '{args.preset}' requires param: --param {rp}=<value>",
                }, ensure_ascii=False, indent=2))
                return 1

        sql = preset["sql"].format(limit=limit, offset=offset, **params)
    else:
        # Custom SQL
        error = _validate_custom_sql(args.sql)
        if error:
            print(json.dumps({"status": "error", "error": error}, ensure_ascii=False, indent=2))
            return 1
        sql = args.sql

    # Export mode
    if args.export:
        export_path = Path(args.export)
        # For export, remove LIMIT/OFFSET from preset SQL (we want all rows)
        if args.preset:
            export_sql = PRESETS[args.preset]["sql"].format(
                limit=MAX_EXPORT_ROWS, offset=0, **params
            )
        else:
            export_sql = sql

        result = export_query_jsonl(db_path, export_sql, export_path, verbose=args.verbose)
        print(_pretty_json(result))
        return 0 if result["status"] == "ok" else 1

    # Normal query mode
    result = execute_query(db_path, sql, limit=limit, offset=offset, verbose=args.verbose)

    # Add query metadata
    result["script"] = SCRIPT_NAME
    result["script_version"] = SCRIPT_VERSION
    if args.preset:
        result["preset"] = args.preset
        result["preset_description"] = PRESETS[args.preset]["description"]

    print(_pretty_json(result))

    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
