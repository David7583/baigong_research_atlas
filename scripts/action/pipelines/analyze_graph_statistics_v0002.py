# ============================================================
# 文件名: analyze_graph_statistics_v0002.py
# 中文名: 图结构统计分析脚本
# 版本号: v0002
#
# 层级: execution
# 主层级: action
# 可更新: True
#
# 制度定位:
# 本脚本对"当前生效的 graph 对象文件"执行只读结构统计分析，输出统计快照与 run_meta。
# 本脚本不修改、不撤销、不生成任何 graph 对象，不引入 current 或 archive 语义。
#
# 核心制度语义:
# - 输入必须是单一 graph 文件路径，且应来自 graphs/*/current 下的当前对象
# - 一次运行只分析一个 graph 对象
# - 空图是合法状态，输出必须存在且可审计
#
# 输出策略:
# 输出位于 actioning/pipelines/analyze_graph_statistics_action/{graph_type}/ 下
# 每次运行生成一个新的 statistics 文件，不覆盖
# run_meta 输出位于同目录的 _run_meta/ 下
#
# 本脚本不做什么:
# - 不修改 graph
# - 不生成 graph
# - 不撤销 graph
# - 不解释统计含义
# - 不跨图比较
# - 不做趋势判断
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================
# alias: analyze_graph_statistics_v0002
# family: analyze_graph_statistics
# role: graph_readonly_statistics
# version: v0002
# status: active
# entry_point: scripts/action/pipelines/analyze_graph_statistics_v0002.py
# input:
#   - current_graph_json
#   - policy_yaml_optional
# output:
#   - graph_statistics_json
#   - run_meta_json
# depends_on:
#   - python_stdlib_only
# used_by:
#   - aggregate_graph_statistics_v0002
# notes:
#   - one run analyzes one graph file only
#   - output is append-only snapshots, no current, no archive


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


DEFAULT_ENCODING = "utf-8"
NUL_NAMES = {"NUL", "nul"}

SCRIPT_NAME = "analyze_graph_statistics_v0002"
SCRIPT_VERSION = "v0002"

ANALYSIS_TYPE = "graph_statistics"
ANALYSIS_VERSION = "v0001"

RUN_META_DIRNAME = "_run_meta"


DEFAULT_POLICY = {
    "policy": {"version": "v0002"},
    "degree": {
        "enabled": True,
        "histogram": {
            "enabled": True,
            "bucket_size": 5,
            "max_bucket": 200,
        },
    },
    "components": {
        "enabled": True,
        "mode": "weak",  # for directed graphs use undirected projection
    },
    "hierarchy": {
        "enabled": True,
        "max_depth_cap": 0,  # 0 means no cap
    },
    "weights": {
        "enabled": True,
    },
}


# ------------------------------------------------------------
# 基础工具
# ------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _is_nul_path(p: Optional[str]) -> bool:
    if p is None:
        return False
    s = str(p).strip()
    if not s:
        return True
    return Path(s).name in NUL_NAMES


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    _safe_mkdir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding=DEFAULT_ENCODING) as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(path))


def _file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _load_policy(policy_path: Optional[Path]) -> Dict[str, Any]:
    if policy_path is None:
        return dict(DEFAULT_POLICY)
    if not policy_path.exists():
        return dict(DEFAULT_POLICY)

    try:
        raw = policy_path.read_text(encoding=DEFAULT_ENCODING)
    except Exception:
        return dict(DEFAULT_POLICY)

    policy = dict(DEFAULT_POLICY)
    try:
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            k = k.strip()
            v = v.strip().strip("'").strip('"')

            cur: Any = policy
            parts = [p for p in k.split(".") if p]
            if not parts:
                continue
            for p in parts[:-1]:
                if p not in cur or not isinstance(cur[p], dict):
                    cur[p] = {}
                cur = cur[p]
            leaf = parts[-1]

            if re.fullmatch(r"-?\d+", v or ""):
                cur[leaf] = int(v)
            elif v.lower() in ("true", "false"):
                cur[leaf] = (v.lower() == "true")
            else:
                cur[leaf] = v
    except Exception:
        return dict(DEFAULT_POLICY)

    return policy


# ------------------------------------------------------------
# 数据结构
# ------------------------------------------------------------

@dataclass(frozen=True)
class BuildStatus:
    executed: bool
    reason: str


@dataclass(frozen=True)
class ParsedEdge:
    src: str
    dst: str
    weight: int


# ------------------------------------------------------------
# Graph 读取与解析
# ------------------------------------------------------------

def _read_graph_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"graph json root must be dict, got {type(obj).__name__}")
    return obj


def _extract_graph_type(graph: Dict[str, Any]) -> str:
    gt = graph.get("graph_type")
    if isinstance(gt, str) and gt.strip():
        return gt.strip()
    return "unknown_graph_type"


def _extract_graph_version(graph: Dict[str, Any]) -> str:
    v = graph.get("version")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return ""


def _extract_nodes(graph: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    missing: List[str] = []
    nodes_obj = graph.get("nodes")

    if nodes_obj is None:
        missing.append("nodes")
        return [], missing

    node_ids: List[str] = []
    if isinstance(nodes_obj, list):
        for n in nodes_obj:
            if isinstance(n, str) and n.strip():
                node_ids.append(n.strip())
            elif isinstance(n, dict):
                nid = n.get("id") or n.get("unit_id") or n.get("name")
                if isinstance(nid, str) and nid.strip():
                    node_ids.append(nid.strip())
    else:
        missing.append("nodes_not_list")
        return [], missing

    # de-dup preserve order
    seen: Set[str] = set()
    out: List[str] = []
    for x in node_ids:
        if x not in seen:
            out.append(x)
            seen.add(x)

    return out, missing


def _is_directed_graph(graph_type: str) -> bool:
    # unit_cooccurrence and unit_adjacency are both undirected (edges stored with u <= v)
    if graph_type in ("unit_cooccurrence", "unit_adjacency"):
        return False
    if graph_type == "unit_hierarchy":
        return True
    # unknown default
    return True


def _extract_edges(graph: Dict[str, Any], graph_type: str) -> Tuple[List[ParsedEdge], List[str], Dict[str, int]]:
    missing: List[str] = []
    edges_obj = graph.get("edges")

    stats = {
        "edges_rows_read": 0,
        "edges_rows_used": 0,
        "edges_rows_dropped_parse_failed": 0,
        "edges_rows_dropped_missing_endpoints": 0,
    }

    if edges_obj is None:
        missing.append("edges")
        return [], missing, stats

    if not isinstance(edges_obj, list):
        missing.append("edges_not_list")
        return [], missing, stats

    directed = _is_directed_graph(graph_type)
    parsed: List[ParsedEdge] = []

    for e in edges_obj:
        stats["edges_rows_read"] += 1
        if not isinstance(e, dict):
            stats["edges_rows_dropped_parse_failed"] += 1
            continue

        src = ""
        dst = ""

        if graph_type == "unit_hierarchy":
            p = e.get("parent")
            c = e.get("child")
            if isinstance(p, str) and isinstance(c, str):
                src, dst = p.strip(), c.strip()
        else:
            # cooccurrence / adjacency default
            u = e.get("u")
            v = e.get("v")
            if isinstance(u, str) and isinstance(v, str):
                src, dst = u.strip(), v.strip()

            # tolerate alternative keys
            if not src or not dst:
                s = e.get("source")
                t = e.get("target")
                if isinstance(s, str) and isinstance(t, str):
                    src, dst = s.strip(), t.strip()

        if not src or not dst:
            stats["edges_rows_dropped_missing_endpoints"] += 1
            continue

        w = e.get("weight")
        if isinstance(w, int):
            weight = w
        elif isinstance(w, float):
            weight = int(w)
        else:
            cnt = e.get("count")
            if isinstance(cnt, int):
                weight = cnt
            elif isinstance(cnt, float):
                weight = int(cnt)
            else:
                weight = 1

        if weight < 0:
            weight = 0

        if not directed and src > dst:
            src, dst = dst, src

        parsed.append(ParsedEdge(src=src, dst=dst, weight=int(weight)))
        stats["edges_rows_used"] += 1

    return parsed, missing, stats


# ------------------------------------------------------------
# 指标计算
# ------------------------------------------------------------

def _compute_density(node_count: int, edge_count: int, directed: bool) -> float:
    if node_count <= 1:
        return 0.0
    denom = float(node_count * (node_count - 1))
    if directed:
        return float(edge_count) / denom
    return float(2.0 * float(edge_count) / denom)


def _compute_degrees(
    nodes: List[str],
    edges: List[ParsedEdge],
    directed: bool,
) -> Tuple[Dict[str, Any], BuildStatus]:
    if not nodes:
        return {}, BuildStatus(executed=False, reason="empty_graph")

    idx: Set[str] = set(nodes)
    if not edges:
        # degrees exist as zeros
        if directed:
            return {
                "in_degree": {"min": 0, "max": 0, "mean": 0.0},
                "out_degree": {"min": 0, "max": 0, "mean": 0.0},
                "total_degree": {"min": 0, "max": 0, "mean": 0.0},
            }, BuildStatus(executed=True, reason="ok_no_edges")
        return {
            "degree": {"min": 0, "max": 0, "mean": 0.0},
        }, BuildStatus(executed=True, reason="ok_no_edges")

    if directed:
        indeg: Dict[str, int] = {n: 0 for n in nodes}
        outdeg: Dict[str, int] = {n: 0 for n in nodes}
        for e in edges:
            if e.src in idx:
                outdeg[e.src] += 1
            if e.dst in idx:
                indeg[e.dst] += 1

        def _summary(d: Dict[str, int]) -> Dict[str, Any]:
            vals = list(d.values())
            return {"min": int(min(vals)), "max": int(max(vals)), "mean": float(sum(vals) / len(vals))}

        total = {n: indeg[n] + outdeg[n] for n in nodes}
        return {
            "in_degree": _summary(indeg),
            "out_degree": _summary(outdeg),
            "total_degree": _summary(total),
        }, BuildStatus(executed=True, reason="ok")

    deg: Dict[str, int] = {n: 0 for n in nodes}
    for e in edges:
        if e.src in idx:
            deg[e.src] += 1
        if e.dst in idx:
            deg[e.dst] += 1

    vals = list(deg.values())
    return {
        "degree": {"min": int(min(vals)), "max": int(max(vals)), "mean": float(sum(vals) / len(vals))}
    }, BuildStatus(executed=True, reason="ok")


def _degree_histogram(
    nodes: List[str],
    edges: List[ParsedEdge],
    directed: bool,
    bucket_size: int,
    max_bucket: int,
) -> Tuple[Dict[str, Any], BuildStatus]:
    if not nodes:
        return {}, BuildStatus(executed=False, reason="empty_graph")
    if bucket_size <= 0:
        return {}, BuildStatus(executed=False, reason="invalid_bucket_size")

    # compute total degree for directed
    idx = set(nodes)
    deg: Dict[str, int] = {n: 0 for n in nodes}
    if directed:
        for e in edges:
            if e.src in idx:
                deg[e.src] += 1
            if e.dst in idx:
                deg[e.dst] += 1
    else:
        for e in edges:
            if e.src in idx:
                deg[e.src] += 1
            if e.dst in idx:
                deg[e.dst] += 1

    hist: Dict[str, int] = {}
    overflow_key = f">{max_bucket}"
    for v in deg.values():
        if v > max_bucket:
            hist[overflow_key] = hist.get(overflow_key, 0) + 1
            continue
        b0 = (v // bucket_size) * bucket_size
        b1 = b0 + bucket_size - 1
        key = f"{b0}-{b1}"
        hist[key] = hist.get(key, 0) + 1

    return {"bucket_size": int(bucket_size), "max_bucket": int(max_bucket), "histogram": hist}, BuildStatus(executed=True, reason="ok")


def _connected_components_weak(
    nodes: List[str],
    edges: List[ParsedEdge],
) -> Tuple[Dict[str, Any], BuildStatus]:
    if not nodes:
        return {}, BuildStatus(executed=False, reason="empty_graph")
    if not edges:
        return {"components_count": int(len(nodes)), "largest_component_node_count": 1}, BuildStatus(executed=True, reason="ok_no_edges")

    adj: Dict[str, Set[str]] = {n: set() for n in nodes}
    idx = set(nodes)
    for e in edges:
        if e.src in idx and e.dst in idx:
            adj[e.src].add(e.dst)
            adj[e.dst].add(e.src)

    seen: Set[str] = set()
    comp_sizes: List[int] = []
    for n in nodes:
        if n in seen:
            continue
        stack = [n]
        seen.add(n)
        size = 0
        while stack:
            u = stack.pop()
            size += 1
            for v in adj.get(u, set()):
                if v not in seen:
                    seen.add(v)
                    stack.append(v)
        comp_sizes.append(size)

    comp_sizes.sort(reverse=True)
    return {
        "components_count": int(len(comp_sizes)),
        "largest_component_node_count": int(comp_sizes[0]) if comp_sizes else 0,
    }, BuildStatus(executed=True, reason="ok")


def _hierarchy_stats(
    nodes: List[str],
    edges: List[ParsedEdge],
    max_depth_cap: int,
) -> Tuple[Dict[str, Any], BuildStatus, bool]:
    if not nodes:
        return {}, BuildStatus(executed=False, reason="empty_graph"), False

    idx = set(nodes)
    indeg: Dict[str, int] = {n: 0 for n in nodes}
    outdeg: Dict[str, int] = {n: 0 for n in nodes}
    children: Dict[str, List[str]] = {n: [] for n in nodes}

    for e in edges:
        if e.src not in idx or e.dst not in idx:
            continue
        outdeg[e.src] += 1
        indeg[e.dst] += 1
        children[e.src].append(e.dst)

    roots = [n for n in nodes if indeg[n] == 0]
    leaves = [n for n in nodes if outdeg[n] == 0]

    visiting: Set[str] = set()
    visited_depth: Dict[str, int] = {}
    cycle_detected = False

    def dfs(u: str) -> int:
        nonlocal cycle_detected
        if u in visited_depth:
            return visited_depth[u]
        if u in visiting:
            cycle_detected = True
            return 1
        visiting.add(u)
        best = 1
        for v in children.get(u, []):
            best = max(best, 1 + dfs(v))
            if max_depth_cap and best - 1 >= max_depth_cap:
                best = max_depth_cap + 1
                break
        visiting.remove(u)
        visited_depth[u] = best
        return best

    best_all = 1
    for n in nodes:
        best_all = max(best_all, dfs(n))
        if max_depth_cap and best_all - 1 >= max_depth_cap:
            best_all = max_depth_cap + 1
            break

    max_depth = max(0, int(best_all - 1))
    out = {
        "max_depth": int(max_depth),
        "root_count": int(len(roots)),
        "leaf_count": int(len(leaves)),
    }
    return out, BuildStatus(executed=True, reason="ok"), cycle_detected


def _weight_summary(edges: List[ParsedEdge]) -> Tuple[Dict[str, Any], BuildStatus]:
    if not edges:
        return {}, BuildStatus(executed=False, reason="no_edges")
    ws = [int(e.weight) for e in edges]
    if not ws:
        return {}, BuildStatus(executed=False, reason="no_edges")
    return {
        "min_weight": int(min(ws)),
        "max_weight": int(max(ws)),
        "mean_weight": float(sum(ws) / len(ws)),
    }, BuildStatus(executed=True, reason="ok")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read-only graph statistics analyzer (v0002).")
    p.add_argument("--graph", required=True, help="Path to a single current graph json file.")
    p.add_argument("--output-root", default="actioning/pipelines/analyze_graph_statistics_action", help="Output root directory for graph statistics analysis.")
    p.add_argument("--policy", default="", help="Optional policy YAML (lite parsed).")
    p.add_argument("--run-meta", default="", help="Optional run_meta output path. If empty, write to {graph_type}/_run_meta/run_{run_id}.json")
    p.add_argument("--dry-run", action="store_true", help="Compute but do not write outputs. Prints summary json.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    run_id = _now_utc_compact()
    generated_at = _now_utc_iso()

    graph_path = Path(args.graph)
    output_root = Path(args.output_root)
    policy_path = Path(args.policy) if args.policy.strip() else None
    dry_run = bool(args.dry_run)

    if not graph_path.exists() or not graph_path.is_file():
        print(f"[ERROR] graph file not found: {graph_path}", file=sys.stderr)
        return 1

    policy = _load_policy(policy_path)

    # input hashes
    input_hashes: Dict[str, Any] = {}
    try:
        input_hashes["graph_sha1"] = _file_sha1(graph_path)
    except Exception:
        input_hashes["graph_sha1"] = None
    if policy_path is not None and policy_path.exists():
        try:
            input_hashes["policy_sha1"] = _file_sha1(policy_path)
        except Exception:
            input_hashes["policy_sha1"] = None
    else:
        input_hashes["policy_sha1"] = None

    # parse graph
    try:
        graph_obj = _read_graph_json(graph_path)
    except Exception as e:
        print(f"[ERROR] failed to read graph json: {e}", file=sys.stderr)
        return 1

    graph_type = _extract_graph_type(graph_obj)
    graph_version = _extract_graph_version(graph_obj)
    directed = _is_directed_graph(graph_type)

    nodes, missing_nodes = _extract_nodes(graph_obj)
    edges, missing_edges, edge_parse_stats = _extract_edges(graph_obj, graph_type)

    missing_fields: List[str] = []
    missing_fields.extend(missing_nodes)
    missing_fields.extend(missing_edges)

    node_count = int(len(nodes))
    edge_count = int(len(edges))
    empty_graph = bool(node_count == 0 or edge_count == 0)

    # output paths
    out_dir = output_root / graph_type
    run_meta_dir = out_dir / RUN_META_DIRNAME
    stats_path = out_dir / f"graph_statistics__{run_id}.json"
    run_meta_path = Path(args.run_meta) if args.run_meta and not _is_nul_path(args.run_meta) else (run_meta_dir / f"run_{run_id}.json")

    capabilities: Dict[str, Dict[str, Any]] = {}
    inactive_reasons: Dict[str, str] = {}

    # base stats always
    density = _compute_density(node_count, edge_count, directed)
    base_stats = {"node_count": node_count, "edge_count": edge_count, "density": float(density)}
    capabilities["base_stats"] = {"executed": True, "reason": "ok", "empty_graph": bool(empty_graph)}

    # degree stats
    degree_cfg = policy.get("degree", {})
    if not degree_cfg.get("enabled", True):
        capabilities["degree_stats"] = {"executed": False, "reason": "disabled"}
        inactive_reasons["degree_stats"] = "disabled"
        degree_stats = {}
    else:
        degree_stats, st_deg = _compute_degrees(nodes, edges, directed)
        capabilities["degree_stats"] = {"executed": st_deg.executed, "reason": st_deg.reason}
        if not st_deg.executed:
            inactive_reasons["degree_stats"] = st_deg.reason

    # degree histogram
    hist_cfg = degree_cfg.get("histogram", {})
    if not degree_cfg.get("enabled", True) or not hist_cfg.get("enabled", True):
        capabilities["degree_histogram"] = {"executed": False, "reason": "disabled"}
        inactive_reasons["degree_histogram"] = "disabled"
        degree_hist = {}
    else:
        bs = int(hist_cfg.get("bucket_size", 5))
        mb = int(hist_cfg.get("max_bucket", 200))
        degree_hist, st_hist = _degree_histogram(nodes, edges, directed, bs, mb)
        capabilities["degree_histogram"] = {"executed": st_hist.executed, "reason": st_hist.reason}
        if not st_hist.executed:
            inactive_reasons["degree_histogram"] = st_hist.reason

    # components
    comp_cfg = policy.get("components", {})
    if not comp_cfg.get("enabled", True):
        capabilities["components"] = {"executed": False, "reason": "disabled"}
        inactive_reasons["components"] = "disabled"
        comp_stats = {}
    else:
        if node_count == 0:
            comp_stats, st_comp = {}, BuildStatus(executed=False, reason="empty_graph")
        elif edge_count == 0:
            # still valid: each node is isolated
            comp_stats, st_comp = {"components_count": node_count, "largest_component_node_count": 1 if node_count > 0 else 0}, BuildStatus(executed=True, reason="ok_no_edges")
        else:
            comp_stats, st_comp = _connected_components_weak(nodes, edges)
        capabilities["components"] = {"executed": st_comp.executed, "reason": st_comp.reason}
        if not st_comp.executed:
            inactive_reasons["components"] = st_comp.reason

    # hierarchy type specific
    hierarchy_out: Dict[str, Any] = {}
    cycle_detected = False
    if graph_type == "unit_hierarchy":
        hcfg = policy.get("hierarchy", {})
        if not hcfg.get("enabled", True):
            capabilities["hierarchy_stats"] = {"executed": False, "reason": "disabled"}
            inactive_reasons["hierarchy_stats"] = "disabled"
        else:
            cap = int(hcfg.get("max_depth_cap", 0))
            hierarchy_out, st_h, cycle_detected = _hierarchy_stats(nodes, edges, cap)
            capabilities["hierarchy_stats"] = {"executed": st_h.executed, "reason": st_h.reason, "cycle_detected": bool(cycle_detected)}
            if not st_h.executed:
                inactive_reasons["hierarchy_stats"] = st_h.reason
    else:
        capabilities["hierarchy_stats"] = {"executed": False, "reason": "not_applicable"}

    # weights summary
    wcfg = policy.get("weights", {})
    if not wcfg.get("enabled", True):
        capabilities["weight_summary"] = {"executed": False, "reason": "disabled"}
        inactive_reasons["weight_summary"] = "disabled"
        wsum = {}
    else:
        wsum, st_w = _weight_summary(edges)
        capabilities["weight_summary"] = {"executed": st_w.executed, "reason": st_w.reason}
        if not st_w.executed:
            inactive_reasons["weight_summary"] = st_w.reason

    # compile statistics
    statistics: Dict[str, Any] = {}
    statistics.update(base_stats)
    statistics.update(degree_stats)
    if degree_hist:
        statistics["degree_histogram"] = degree_hist
    if comp_stats:
        statistics.update(comp_stats)
    if hierarchy_out:
        statistics.update(hierarchy_out)
    if wsum:
        statistics.update(wsum)

    # stats object
    stats_obj: Dict[str, Any] = {
        "analysis_type": ANALYSIS_TYPE,
        "analysis_version": ANALYSIS_VERSION,
        "run_id": run_id,
        "generated_at": generated_at,
        "graph_type": graph_type,
        "graph_version": graph_version,
        "directed": bool(directed),
        "input_graph": {
            "path": str(graph_path),
            "sha1": input_hashes.get("graph_sha1"),
        },
        "statistics": statistics,
    }

    # run_meta object
    run_meta_obj: Dict[str, Any] = {
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "started_at": generated_at,
        "dry_run": dry_run,
        "inputs": {
            "graph": str(graph_path),
            "output_root": str(output_root),
            "policy": str(policy_path) if policy_path is not None else "",
            "hashes": input_hashes,
        },
        "graph": {
            "graph_type": graph_type,
            "graph_version": graph_version,
            "node_count": node_count,
            "edge_count": edge_count,
            "empty_graph": bool(empty_graph),
            "directed": bool(directed),
        },
        "parse_stats": {
            "missing_fields": missing_fields,
            **edge_parse_stats,
        },
        "capabilities": capabilities,
        "inactive_reasons": inactive_reasons,
        "outputs": {
            "statistics_path": str(stats_path),
            "run_meta_path": str(run_meta_path),
            "written": False,
        },
        "status": "ok" if not dry_run else "dry-run",
    }

    # write
    if not dry_run:
        _safe_mkdir(out_dir)
        _safe_mkdir(run_meta_dir)
        run_meta_obj["outputs"]["written"] = True
        _write_json(stats_path, stats_obj)
        _write_json(run_meta_path, run_meta_obj)

    # print summary
    print(json.dumps(
        {
            "status": run_meta_obj["status"],
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "graph_type": graph_type,
            "node_count": node_count,
            "edge_count": edge_count,
            "empty_graph": bool(empty_graph),
            "statistics": str(stats_path),
            "run_meta": str(run_meta_path),
        },
        ensure_ascii=False,
        indent=2
    ))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
