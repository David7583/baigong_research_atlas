# ============================================================
# File: build_unit_adjacency_graph_v0003.py
# 中文名: 对象级邻接关系图构建脚本
# Version: v0003
#
# Layer: execution
# Main Layer: action
# Updatable: True
#
# Purpose
#
# 基于 register（已登记结构单元）与结构邻接事实（adjacency），
# 构建"对象级邻接关系图"。当提供 register 时，节点来源仅限 register；
# 当使用免登记模式（--register 省略）时，节点从边数据中自动发现。
#
# What it does
#
# 1 读取 register JSONL，构建合法节点集合（unit_id → label）
# 2 读取 adjacency JSONL（单文件或目录），流式聚合边权重
# 3 仅保留两端都在 register 中的边
# 4 支持权重裁剪、阈值过滤、图统计
# 5 输出聚合后的 JSONL（供 sync_to_graph 流式消费）+ graph JSON + run_meta
#
# What it does NOT do
#
# 1 不生成或修改 register
# 2 不对邻接事实进行语义解释或重要性判断
# 3 不为下游兜底造节点（register 模式下不允许 adjacency 反向生成对象；免登记模式除外）
# 4 不回写任何上游产物，不修改原始数据资产
#
# v0002 changes
#
# 1 支持 --adjacency-dir 读取目录下所有 JSONL 文件（v0003 analyze 的 1:1 输出）
# 2 聚合使用 dict 累加，内存占用 = 唯一 pair 数 × ~200B，无硬门槛
# 3 新增 --output-jsonl 输出聚合后的 JSONL（流式，供 sync_to_graph 消费）
# 4 _extract_pair 适配 v0003 字段名（unit_id_from/unit_id_to）
# 5 _extract_count 适配 v0003 字段名（adjacency_count）
# 6 进度报告：每处理 100 个文件输出一次 stderr 进度
#
# Design decision
#
# 聚合使用内存 dict 是经过计算的选择：
# 1.8M 行 adjacency → 去重后约 50 万~100 万唯一 pair
# 每个 key (unit_id_from, unit_id_to) 约 100B + value int → 总计 ~100MB~200MB
# 这在 RTX 4060 笔记本上完全可行
# 如果未来数据量增长到这个方案不可行，应切换为 DuckDB 聚合
#
# 制度声明
#
# register 模式下 register 是唯一合法节点来源；免登记模式下节点从边数据自动发现
# register 为空是合法状态，脚本应视为成功运行并输出空图与零值统计
# archive 中的图不参与任何计算，仅用于审计与回溯
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================
# alias: build_unit_adjacency_graph_v0003
# family: build_unit_adjacency_graph
# role: object_graph_builder_adjacency
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/build_unit_adjacency_graph_v0003.py
# input:
#   - register_structural_units_jsonl
#   - adjacency_facts_jsonl (single file or directory of JSONL files)
#   - graph_policy_yaml_optional
# output:
#   - unit_adjacency_graph_json (graph object with nodes + edges)
#   - unit_adjacency_edges.jsonl (aggregated edges for sync_to_graph)
#   - run_meta_json
# depends_on:
#   - python_stdlib_only
# used_by:
#   - sync_to_graph_v0002
# notes:
#   - current only keeps latest; previous graph will be moved to archive (revoked).
#   - v0002 supports directory input for v0003 analyze per-file output.


# ============================================================
# Imports
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# ============================================================
# Constants
# ============================================================

DEFAULT_ENCODING = "utf-8"
NUL_NAMES = {"NUL", "nul"}

GRAPH_FILENAME = "unit_adjacency_graph.json"
EDGES_JSONL_FILENAME = "unit_adjacency_edges.jsonl"
ARCHIVE_DIRNAME = "archive"
CURRENT_DIRNAME = "current"
RUN_META_DIRNAME = "_run_meta"

GRAPH_TYPE = "unit_adjacency"
SCRIPT_NAME = "build_unit_adjacency_graph_v0003"
SCRIPT_VERSION = "v0003"

PROGRESS_INTERVAL = 100

DEFAULT_POLICY = {
    "policy": {"version": "v0001"},
    "weight": {"mode": "count"},
    "pruning": {
        "enabled": True,
        "min_weight": 1,
        "max_edges": 0,
        "top_k_per_node": 0,
    },
    "viz": {"enabled": True},
}


# ============================================================
# Utilities
# ============================================================

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _is_nul_path(p: Optional[str]) -> bool:
    if p is None:
        return False
    s = p.strip()
    if not s:
        return True
    return Path(s).name in NUL_NAMES


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_jsonl_stream(path: Path) -> Iterable[Dict[str, Any]]:
    """Stream JSONL rows. Yields dicts or error markers."""
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"__parse_failed__": True}
            except json.JSONDecodeError:
                yield {"__parse_failed__": True}


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    """Collect all .jsonl files in a directory, sorted for determinism."""
    return sorted(dir_path.glob("*.jsonl"))


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    _safe_mkdir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding=DEFAULT_ENCODING) as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(path))


def _atomic_move(src: Path, dst: Path) -> None:
    _safe_mkdir(dst.parent)
    os.replace(str(src), str(dst))


def _file_sha1(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


# ============================================================
# Lite Policy Loader (stdlib-only YAML-like)
# ============================================================

class LitePolicyLoader:
    def __init__(self, path: Path):
        self.raw = path.read_text(encoding=DEFAULT_ENCODING)

    def _get_line_value(self, key: str) -> Optional[str]:
        pattern = rf"^{re.escape(key)}\s*:\s*(.+)$"
        m = re.search(pattern, self.raw, re.MULTILINE)
        if not m:
            return None
        val = m.group(1).strip()
        if val.startswith('"') and val.endswith('"'):
            return val[1:-1]
        if val.startswith("'") and val.endswith("'"):
            return val[1:-1]
        return val

    def get_bool(self, key: str, default: bool) -> bool:
        v = self._get_line_value(key)
        if v is None:
            return default
        if v.lower() == "true":
            return True
        if v.lower() == "false":
            return False
        return default

    def get_int(self, key: str, default: int) -> int:
        v = self._get_line_value(key)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            return default

    def get_str(self, key: str, default: str) -> str:
        v = self._get_line_value(key)
        return default if v is None else v


def _load_policy(policy_path: Optional[Path]) -> Dict[str, Any]:
    if policy_path is None or not policy_path.exists():
        return json.loads(json.dumps(DEFAULT_POLICY))

    loader = LitePolicyLoader(policy_path)
    policy: Dict[str, Any] = json.loads(json.dumps(DEFAULT_POLICY))

    policy["policy"]["version"] = loader.get_str("version", policy["policy"]["version"])
    policy["pruning"]["enabled"] = loader.get_bool("enabled", policy["pruning"]["enabled"])
    policy["pruning"]["min_weight"] = loader.get_int("min_weight", policy["pruning"]["min_weight"])
    policy["pruning"]["max_edges"] = loader.get_int("max_edges", policy["pruning"]["max_edges"])
    policy["pruning"]["top_k_per_node"] = loader.get_int("top_k_per_node", policy["pruning"]["top_k_per_node"])
    policy["viz"]["enabled"] = loader.get_bool("viz_enabled", policy["viz"]["enabled"])

    return policy


# ============================================================
# Register Parsing
# ============================================================

def _extract_unit_id_from_register_row(row: Dict[str, Any]) -> Optional[str]:
    for k in ("unit_id", "id", "uid", "unit", "name"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    unit_obj = row.get("unit")
    if isinstance(unit_obj, dict):
        v = unit_obj.get("unit_id") or unit_obj.get("id")
        if isinstance(v, str) and v.strip():
            return v.strip()
    for k in ("text", "unit_text", "normalized", "value"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return "u_" + _sha1_text(v.strip())
    return None


def _extract_unit_label_from_register_row(row: Dict[str, Any]) -> str:
    for k in ("unit_text", "text", "normalized", "value", "display", "label"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _build_nodes(register_path: Path) -> Tuple[Dict[str, Dict[str, Any]], int]:
    """Build node map from register. Returns (nodes_map keyed by unit_text, rows_read).

    v0002 fix: use unit_text as key for matching with edge data,
    because edge data's unit_id is instance-level (position-dependent)
    while register's unit_id is object-level (SHA1 of unit_text).
    """
    nodes: Dict[str, Dict[str, Any]] = {}
    rows = 0

    for row in _read_jsonl_stream(register_path):
        rows += 1
        if row.get("__parse_failed__"):
            continue
        label = _extract_unit_label_from_register_row(row)
        if not label:
            continue
        if label not in nodes:
            uid = _extract_unit_id_from_register_row(row)
            nodes[label] = {"id": uid or ("u_" + _sha1_text(label)), "label": label}

    return nodes, rows


# ============================================================
# Adjacency Parsing (v0003 compatible)
# ============================================================

def _extract_pair_from_adjacency_row(row: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Extract (unit_text_from, unit_text_to) from an adjacency row.

    v0002 fix: prefer unit_text fields for matching with register (keyed by unit_text).
    Edge data's unit_id is instance-level and cannot match register's object-level unit_id.
    Falls back to unit_id fields only when unit_text is not available.
    """
    candidates = [
        ("unit_text_from", "unit_text_to"),  # preferred: object-level text matching
        ("unit_id_from", "unit_id_to"),      # fallback: v0003 format (instance-level)
        ("unit_a", "unit_b"),
        ("a", "b"),
        ("u", "v"),
        ("left", "right"),
        ("source", "target"),
    ]
    for k1, k2 in candidates:
        v1 = row.get(k1)
        v2 = row.get(k2)
        if isinstance(v1, str) and isinstance(v2, str) and v1.strip() and v2.strip():
            return v1.strip(), v2.strip()

    pair = row.get("pair")
    if isinstance(pair, dict):
        v1 = pair.get("unit_text_from") or pair.get("from") or pair.get("a") or pair.get("unit_a") or pair.get("unit_id_from")
        v2 = pair.get("unit_text_to") or pair.get("to") or pair.get("b") or pair.get("unit_b") or pair.get("unit_id_to")
        if isinstance(v1, str) and isinstance(v2, str) and v1.strip() and v2.strip():
            return v1.strip(), v2.strip()

    return None


def _extract_count_from_adjacency_row(row: Dict[str, Any]) -> int:
    """Extract count from an adjacency row.

    v0003 field: adjacency_count
    Legacy fields: count, adjacency, freq, etc.
    """
    for k in ("adjacency_count", "count", "adjacency", "freq", "frequency", "n", "weight"):
        v = row.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return 1


# ============================================================
# Core: Streaming Aggregation
# ============================================================

def _aggregate_edges(
    input_files: List[Path],
    node_ids: Optional[Set[str]],
    verbose: bool,
) -> Tuple[Dict[Tuple[str, str], int], Dict[str, str], Dict[str, int]]:
    """Stream through all adjacency files, aggregate by directed (from, to) pair.

    When node_ids is None (no register), all edges are accepted and labels
    are extracted from unit_text_from/unit_text_to fields in the edge data.

    Returns:
        edge_counts: dict mapping (from, to) → total count
        discovered_labels: dict mapping unit_id → unit_text (from edge data)
        stats: counters for audit
    """
    edge_counts: Dict[Tuple[str, str], int] = {}
    discovered_labels: Dict[str, str] = {}
    stats = {
        "rows_read": 0,
        "rows_used": 0,
        "rows_dropped_not_in_register": 0,
        "rows_dropped_parse_failed": 0,
        "rows_dropped_self_loop": 0,
        "files_processed": 0,
    }

    use_register = node_ids is not None and len(node_ids) >= 2

    for file_idx, fpath in enumerate(input_files):
        for row in _read_jsonl_stream(fpath):
            stats["rows_read"] += 1

            if row.get("__parse_failed__"):
                stats["rows_dropped_parse_failed"] += 1
                continue

            pair = _extract_pair_from_adjacency_row(row)
            if not pair:
                stats["rows_dropped_parse_failed"] += 1
                continue

            a, b = pair

            # Self-loop defense
            if a == b:
                stats["rows_dropped_self_loop"] += 1
                continue

            if use_register and (a not in node_ids or b not in node_ids):
                stats["rows_dropped_not_in_register"] += 1
                continue

            cnt = _extract_count_from_adjacency_row(row)

            # Directed: preserve from → to order as-is
            key = (a, b)
            edge_counts[key] = edge_counts.get(key, 0) + cnt
            stats["rows_used"] += 1

            # Collect labels from edge data (first seen wins)
            if a not in discovered_labels:
                label_a = row.get("unit_text_from")
                if isinstance(label_a, str) and label_a.strip():
                    discovered_labels[a] = label_a.strip()
            if b not in discovered_labels:
                label_b = row.get("unit_text_to")
                if isinstance(label_b, str) and label_b.strip():
                    discovered_labels[b] = label_b.strip()

        stats["files_processed"] += 1

        if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            print(
                f"[ADJACENCY-BUILD] Progress: {file_idx + 1}/{len(input_files)} files, "
                f"{stats['rows_read']} rows read, "
                f"{len(edge_counts)} unique pairs so far",
                file=sys.stderr,
            )

    if verbose:
        print(
            f"[ADJACENCY-BUILD] Aggregation done: {stats['files_processed']} files, "
            f"{stats['rows_read']} rows read, "
            f"{stats['rows_used']} rows used, "
            f"{len(edge_counts)} unique pairs",
            file=sys.stderr,
        )

    return edge_counts, discovered_labels, stats


# ============================================================
# Pruning
# ============================================================

def _prune_edges(
    edge_counts: Dict[Tuple[str, str], int],
    policy: Dict[str, Any],
) -> Tuple[Dict[Tuple[str, str], int], Dict[str, Any]]:
    """Apply pruning rules. Returns (pruned_edges, prune_details)."""
    prune_details: Dict[str, Any] = {
        "before": len(edge_counts),
        "below_min_weight": 0,
        "top_k_pruned": 0,
        "max_edges_pruned": 0,
    }

    if not edge_counts:
        prune_details["after"] = 0
        return edge_counts, prune_details

    pruning = policy.get("pruning", {})
    if not pruning.get("enabled", True):
        prune_details["after"] = len(edge_counts)
        return edge_counts, prune_details

    min_w = int(pruning.get("min_weight", 1))
    max_edges = int(pruning.get("max_edges", 0))
    top_k = int(pruning.get("top_k_per_node", 0))

    # Min weight filter
    filtered: Dict[Tuple[str, str], int] = {}
    for key, cnt in edge_counts.items():
        if cnt >= min_w:
            filtered[key] = cnt
        else:
            prune_details["below_min_weight"] += 1

    # Top-k per node
    if top_k > 0 and filtered:
        per_node: Dict[str, List[Tuple[Tuple[str, str], int]]] = {}
        for (u, v), cnt in filtered.items():
            per_node.setdefault(u, []).append(((u, v), cnt))
            per_node.setdefault(v, []).append(((u, v), cnt))

        allow_keys: Set[Tuple[str, str]] = set()
        for nid, entries in per_node.items():
            entries_sorted = sorted(entries, key=lambda x: x[1], reverse=True)
            for edge_key, _ in entries_sorted[:top_k]:
                allow_keys.add(edge_key)

        before_topk = len(filtered)
        filtered = {k: v for k, v in filtered.items() if k in allow_keys}
        prune_details["top_k_pruned"] = before_topk - len(filtered)

    # Max edges (global)
    if max_edges > 0 and len(filtered) > max_edges:
        sorted_edges = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
        before_max = len(filtered)
        filtered = dict(sorted_edges[:max_edges])
        prune_details["max_edges_pruned"] = before_max - len(filtered)

    prune_details["after"] = len(filtered)
    return filtered, prune_details


# ============================================================
# Statistics
# ============================================================

def _compute_graph_statistics(node_count: int, edge_count: int) -> Dict[str, Any]:
    if node_count <= 1:
        density = 0.0
    else:
        # Directed graph density: E / (N * (N-1))
        density = float(edge_count) / (node_count * (node_count - 1))
    return {
        "node_count": int(node_count),
        "edge_count": int(edge_count),
        "density": float(density),
    }


# ============================================================
# Output
# ============================================================

def _prepare_output_dirs(output_root: Path) -> Dict[str, Path]:
    current_dir = output_root / CURRENT_DIRNAME
    archive_dir = output_root / ARCHIVE_DIRNAME
    run_meta_dir = output_root / RUN_META_DIRNAME
    _safe_mkdir(current_dir)
    _safe_mkdir(archive_dir)
    _safe_mkdir(run_meta_dir)
    return {"current": current_dir, "archive": archive_dir, "run_meta": run_meta_dir}


def _archive_previous_if_needed(
    current_path: Path,
    archive_dir: Path,
    run_id: str,
    dry_run: bool,
) -> Optional[str]:
    if not current_path.exists():
        return None
    archived_name = f"{current_path.stem}__revoked__{run_id}{current_path.suffix}"
    if dry_run:
        return archived_name
    dst = archive_dir / archived_name
    _atomic_move(current_path, dst)
    return archived_name


def _write_edges_jsonl(
    path: Path,
    edge_counts: Dict[Tuple[str, str], int],
    nodes_map: Dict[str, Dict[str, Any]],
) -> None:
    """Write aggregated edges as JSONL for sync_to_graph streaming consumption."""
    _safe_mkdir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding=DEFAULT_ENCODING) as f:
        for (u, v), count in sorted(edge_counts.items(), key=lambda x: x[1], reverse=True):
            row = {
                "u": u,
                "v": v,
                "u_label": nodes_map.get(u, {}).get("label", ""),
                "v_label": nodes_map.get(v, {}).get("label", ""),
                "direction": "forward",
                "count": count,
                "weight": count,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    os.replace(str(tmp), str(path))


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build object-level adjacency graph from register + adjacency facts (v0003)."
    )
    p.add_argument("--register", default=None, help="Path to register_structural_units JSONL. If omitted, all units from edges are accepted.")

    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--adjacency",
        default=None,
        help="Path to a single adjacency facts JSONL file.",
    )
    input_group.add_argument(
        "--adjacency-dir",
        default=None,
        help="Path to directory containing adjacency JSONL files (v0003 per-file output).",
    )

    p.add_argument("--output-root", default="actioning/pipelines/build_unit_adjacency_graph_action", help="Output root directory.")
    p.add_argument("--policy", default="", help="Optional policy YAML (lite parsed).")
    p.add_argument("--run-meta", default="", help="Optional run_meta output path.")
    p.add_argument("--dry-run", action="store_true", help="Compute and report, do not write graph.")
    p.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    return p.parse_args(argv)


# ============================================================
# Main
# ============================================================

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    run_id = _now_utc_compact()
    started_at = _now_utc_iso()

    register_path = Path(args.register) if args.register else None
    output_root = Path(args.output_root)
    policy_path = Path(args.policy) if args.policy.strip() else None
    dry_run = bool(args.dry_run)
    verbose = bool(args.verbose)

    # Resolve input files
    if args.adjacency:
        adjacency_path = Path(args.adjacency)
        if not adjacency_path.exists():
            print(f"[ERROR] adjacency not found: {adjacency_path}", file=sys.stderr)
            return 1
        input_files = [adjacency_path]
        input_mode = "single_file"
        input_source = str(adjacency_path)
    elif args.adjacency_dir:
        adjacency_dir = Path(args.adjacency_dir)
        if not adjacency_dir.exists():
            print(f"[ERROR] adjacency directory not found: {adjacency_dir}", file=sys.stderr)
            return 1
        input_files = _glob_jsonl_files(adjacency_dir)
        if not input_files:
            print(f"[ERROR] No .jsonl files found in: {adjacency_dir}", file=sys.stderr)
            return 1
        input_mode = "directory"
        input_source = str(adjacency_dir)
    else:
        print("[ERROR] No adjacency input specified.", file=sys.stderr)
        return 1

    # 1) Build nodes from register (optional)
    if register_path is not None:
        if not register_path.exists():
            print(f"[ERROR] register not found: {register_path}", file=sys.stderr)
            return 1
        nodes_map, register_rows = _build_nodes(register_path)
        node_ids: Optional[Set[str]] = set(nodes_map.keys())
        register_mode = "register"
    else:
        nodes_map = {}
        register_rows = 0
        node_ids = None
        register_mode = "none_all_accepted"

    policy = _load_policy(policy_path)
    dirs = _prepare_output_dirs(output_root)
    current_dir = dirs["current"]
    archive_dir = dirs["archive"]
    run_meta_dir = dirs["run_meta"]

    current_graph_path = current_dir / GRAPH_FILENAME
    current_edges_path = current_dir / EDGES_JSONL_FILENAME

    if verbose:
        print(f"[ADJACENCY-BUILD] Input mode: {input_mode}, files: {len(input_files)}", file=sys.stderr)
        print(f"[ADJACENCY-BUILD] Register mode: {register_mode}, nodes: {len(nodes_map)}", file=sys.stderr)

    # 2) Stream-aggregate edges
    edge_counts, discovered_labels, agg_stats = _aggregate_edges(input_files, node_ids, verbose)

    # Merge discovered labels into nodes_map
    for uid, label in discovered_labels.items():
        if uid not in nodes_map:
            nodes_map[uid] = {"id": uid, "label": label}

    # 3) Prune
    pruned_edges, prune_details = _prune_edges(edge_counts, policy)

    # 4) Statistics
    active_node_ids: Set[str] = set()
    for (u, v) in pruned_edges:
        active_node_ids.add(u)
        active_node_ids.add(v)
    stats = _compute_graph_statistics(len(active_node_ids), len(pruned_edges))

    # 5) Build graph object
    nodes_list = [nodes_map[nid] for nid in sorted(active_node_ids) if nid in nodes_map]

    graph_obj: Dict[str, Any] = {
        "graph_type": GRAPH_TYPE,
        "version": SCRIPT_VERSION,
        "generated_at": started_at,
        "policy_version": policy.get("policy", {}).get("version", "v0001"),
        "nodes": [{"id": n["id"], "label": n.get("label", "")} for n in nodes_list],
        "edges": [
            {"u": u, "v": v, "direction": "forward", "weight": cnt, "count": cnt}
            for (u, v), cnt in sorted(pruned_edges.items(), key=lambda x: x[1], reverse=True)
        ],
        "statistics": stats,
    }

    # 6) Archive & Write
    archived_graph = _archive_previous_if_needed(current_graph_path, archive_dir, run_id, dry_run)
    archived_edges = _archive_previous_if_needed(current_edges_path, archive_dir, run_id, dry_run)

    wrote_graph = False
    wrote_edges = False

    if not dry_run:
        _write_json(current_graph_path, graph_obj)
        wrote_graph = True

        _write_edges_jsonl(current_edges_path, pruned_edges, nodes_map)
        wrote_edges = True

    # 7) Run meta
    input_hashes: Dict[str, Any] = {}
    if register_path is not None:
        input_hashes["register_sha1"] = _file_sha1(register_path)
    if policy_path is not None and policy_path.exists():
        input_hashes["policy_sha1"] = _file_sha1(policy_path)

    if args.run_meta and not _is_nul_path(args.run_meta):
        run_meta_path = Path(args.run_meta)
        _safe_mkdir(run_meta_path.parent)
    else:
        run_meta_path = run_meta_dir / f"run_{run_id}.json"

    run_meta_obj: Dict[str, Any] = {
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "generated_at": _now_utc_iso(),
        "dry_run": dry_run,
        "inputs": {
            "register": str(register_path) if register_path else "",
            "register_mode": register_mode,
            "adjacency_source": input_source,
            "input_mode": input_mode,
            "input_files_count": len(input_files),
            "policy": str(policy_path) if policy_path is not None else "",
            "hashes": input_hashes,
        },
        "output": {
            "output_root": str(output_root),
            "current_graph_path": str(current_graph_path),
            "current_edges_path": str(current_edges_path),
            "archived_graph": archived_graph or "",
            "archived_edges": archived_edges or "",
            "graph_written": wrote_graph,
            "edges_written": wrote_edges,
        },
        "aggregation_stats": {
            "register_rows_read": register_rows,
            "register_node_count": len(node_ids) if node_ids is not None else 0,
            "adjacency_rows_read": agg_stats["rows_read"],
            "adjacency_rows_used": agg_stats["rows_used"],
            "adjacency_rows_dropped_not_in_register": agg_stats["rows_dropped_not_in_register"],
            "adjacency_rows_dropped_parse_failed": agg_stats["rows_dropped_parse_failed"],
            "adjacency_rows_dropped_self_loop": agg_stats["rows_dropped_self_loop"],
            "adjacency_files_processed": agg_stats["files_processed"],
            "unique_pairs_before_pruning": len(edge_counts),
        },
        "pruning": prune_details,
        "result_stats": stats,
        "status": "ok" if not dry_run else "dry-run",
    }

    if not _is_nul_path(str(run_meta_path)):
        try:
            _write_json(run_meta_path, run_meta_obj)
        except Exception as e:
            print(f"[WARN] failed to write run_meta: {e}", file=sys.stderr)

    # Summary to stdout
    print(json.dumps({
        "status": run_meta_obj["status"],
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "register_count": len(node_ids) if node_ids is not None else 0,
        "unique_pairs": len(edge_counts),
        "edges_after_pruning": len(pruned_edges),
        "active_nodes": len(active_node_ids),
        "output_graph": str(current_graph_path),
        "output_edges": str(current_edges_path),
        "run_meta": str(run_meta_path),
    }, ensure_ascii=False, indent=2))

    return 0


# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())