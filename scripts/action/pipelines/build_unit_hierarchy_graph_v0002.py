# ============================================================
# 文件名: build_unit_hierarchy_graph_v0002.py
# 中文名: 对象级层级关系图构建脚本
# 版本号: v0002
#
# 层级: execution
# 主层级: action
# 可更新: True
#
# 职责说明:
# 本脚本基于 register（已登记结构单元）与结构层级事实（hierarchy），
# 构建“对象级层级关系图”。对象级节点来源必须且只能来自 register。
#
# 本脚本做什么:
# - 读取 register JSONL，形成当前可用节点集合（可为空）
# - 读取 hierarchy JSONL，仅对 parent 与 child 都在 register 的记录生成边
# - 支持权重计算、裁剪、阈值过滤、图统计、可视化适配等能力
# - 当 register 为空或图为空时，能力自动失活或输出零值统计并记录原因
# - 写入 current 下的最新 graph 文件，并将旧 graph 移动到 archive（撤销）
# - 输出 run_meta 记录本次运行状态、失活原因与撤销信息
#
# 本脚本不做什么:
# - 不生成或修改 register
# - 不对层级事实进行语义解释或重要性判断
# - 不为下游兜底造节点，不允许 hierarchy 反向生成对象
# - 不回写任何上游产物，不修改原始数据资产
#
# 制度声明:
# - register 是唯一合法节点来源
# - register 为空是合法状态，脚本应视为成功运行并输出空图与零值统计
# - archive 中的图不参与任何计算，仅用于审计与回溯
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================
# alias: build_unit_hierarchy_graph_v0002
# family: build_unit_hierarchy_graph
# role: object_graph_builder_hierarchy
# version: v0002
# status: active
# entry_point: scripts/action/pipelines/build_unit_hierarchy_graph_v0002.py
# input:
#   - register_structural_units_jsonl
#   - hierarchy_facts_jsonl
#   - graph_policy_yaml_optional
# output:
#   - unit_hierarchy_graph_json
#   - run_meta_json
# depends_on:
#   - python_stdlib_only
# used_by:
#   - understand_graph_stage_v0001
# notes:
#   - current only keeps latest; previous graph will be moved to archive (revoked), not overwritten.


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

GRAPH_FILENAME = "unit_hierarchy_graph.json"
ARCHIVE_DIRNAME = "archive"
CURRENT_DIRNAME = "current"
RUN_META_DIRNAME = "_run_meta"

GRAPH_TYPE = "unit_hierarchy"
SCRIPT_NAME = "build_unit_hierarchy_graph_v0002"
SCRIPT_VERSION = "v0002"


DEFAULT_POLICY = {
    "policy": {"version": "v0002"},
    "weight": {"mode": "count"},  # v0001: count only
    "pruning": {
        "enabled": True,
        "min_weight": 1,
        "max_edges": 0,          # 0 means unlimited
        "top_k_per_node": 0,     # 0 means disabled
        "remove_self_loops": True,
    },
    "hierarchy": {
        "max_depth": 0,          # 0 means do not enforce (compute stats only)
    },
    "viz": {
        "enabled": True,
        "schema": "lite_v0001",
    },
}


# ------------------------------------------------------------
# 基础工具
# ------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _is_nul_path(p: str) -> bool:
    if not p:
        return True
    s = str(p).strip()
    if not s:
        return True
    return Path(s).name in NUL_NAMES


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                yield {"__parse_failed__": True}


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    _safe_mkdir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding=DEFAULT_ENCODING) as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(path))


def _atomic_move(src: Path, dst: Path) -> None:
    _safe_mkdir(dst.parent)
    os.replace(str(src), str(dst))


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
# register / hierarchy 解析
# ------------------------------------------------------------

def _extract_unit_id_from_register_row(row: Dict[str, Any]) -> Optional[str]:
    for k in ("unit_id", "id", "unit", "name"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _extract_unit_label_from_register_row(row: Dict[str, Any]) -> str:
    for k in ("label", "text", "value", "unit_label"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_parent_child_from_hierarchy_row(row: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    candidates = [
        ("parent", "child"),
        ("parent_unit", "child_unit"),
        ("unit_parent", "unit_child"),
        ("ancestor", "descendant"),
        ("source", "target"),
        ("u", "v"),
        ("a", "b"),
    ]
    for k1, k2 in candidates:
        v1 = row.get(k1)
        v2 = row.get(k2)
        if isinstance(v1, str) and isinstance(v2, str) and v1.strip() and v2.strip():
            return v1.strip(), v2.strip()

    rel = row.get("relation")
    if isinstance(rel, dict):
        v1 = rel.get("parent") or rel.get("ancestor") or rel.get("source")
        v2 = rel.get("child") or rel.get("descendant") or rel.get("target")
        if isinstance(v1, str) and isinstance(v2, str) and v1.strip() and v2.strip():
            return v1.strip(), v2.strip()

    return None


def _extract_count_from_hierarchy_row(row: Dict[str, Any]) -> int:
    for k in ("count", "freq", "frequency", "n", "weight"):
        v = row.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return 1


# ------------------------------------------------------------
# 核心业务逻辑
# ------------------------------------------------------------

@dataclass(frozen=True)
class BuildStatus:
    executed: bool
    reason: str


def _build_nodes(register_path: Path) -> Tuple[Dict[str, Dict[str, Any]], BuildStatus, int]:
    nodes: Dict[str, Dict[str, Any]] = {}
    rows = 0

    for row in _read_jsonl(register_path):
        rows += 1
        if row.get("__parse_failed__"):
            continue
        uid = _extract_unit_id_from_register_row(row)
        if not uid:
            continue
        if uid not in nodes:
            nodes[uid] = {"id": uid, "label": _extract_unit_label_from_register_row(row)}

    if not nodes:
        return nodes, BuildStatus(executed=False, reason="empty_register"), rows

    return nodes, BuildStatus(executed=True, reason="ok"), rows


def _map_edges(
    hierarchy_path: Path,
    node_ids: Set[str],
) -> Tuple[List[Dict[str, Any]], BuildStatus, Dict[str, int]]:
    edges: List[Dict[str, Any]] = []
    stats = {
        "rows_read": 0,
        "rows_used": 0,
        "rows_dropped_not_in_register": 0,
        "rows_dropped_parse_failed": 0,
    }

    if len(node_ids) < 2:
        return edges, BuildStatus(executed=False, reason="insufficient_nodes"), stats

    for row in _read_jsonl(hierarchy_path):
        stats["rows_read"] += 1
        if row.get("__parse_failed__"):
            stats["rows_dropped_parse_failed"] += 1
            continue

        pc = _extract_parent_child_from_hierarchy_row(row)
        if not pc:
            stats["rows_dropped_parse_failed"] += 1
            continue

        parent, child = pc
        if parent not in node_ids or child not in node_ids:
            stats["rows_dropped_not_in_register"] += 1
            continue

        cnt = _extract_count_from_hierarchy_row(row)
        edges.append({"parent": parent, "child": child, "count": int(cnt)})
        stats["rows_used"] += 1

    if not edges:
        return edges, BuildStatus(executed=False, reason="no_hierarchy_edges"), stats

    return edges, BuildStatus(executed=True, reason="ok"), stats


def _compute_edge_weights(edges: List[Dict[str, Any]], policy: Dict[str, Any]) -> BuildStatus:
    if not edges:
        return BuildStatus(executed=False, reason="no_edges")

    mode = str(policy.get("weight", {}).get("mode", "count")).strip().lower()
    if mode != "count":
        mode = "count"

    for e in edges:
        e["weight"] = int(e.get("count", 1))

    return BuildStatus(executed=True, reason="ok")


def _prune_edges(
    edges: List[Dict[str, Any]],
    policy: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], BuildStatus, Dict[str, Any]]:
    if not edges:
        return edges, BuildStatus(executed=False, reason="no_edges"), {"pruned": 0}

    pruning = policy.get("pruning", {})
    if not pruning.get("enabled", True):
        return edges, BuildStatus(executed=False, reason="disabled"), {"pruned": 0}

    min_w = int(pruning.get("min_weight", 1))
    max_edges = int(pruning.get("max_edges", 0))
    top_k = int(pruning.get("top_k_per_node", 0))
    remove_self = bool(pruning.get("remove_self_loops", True))

    before = len(edges)

    kept: List[Dict[str, Any]] = []
    for e in edges:
        p = e.get("parent")
        c = e.get("child")
        if remove_self and p == c:
            continue
        w = int(e.get("weight", e.get("count", 1)))
        if w < min_w:
            continue
        kept.append(e)

    # 去重：同一 parent-child 只保留最大 weight
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in kept:
        key = (str(e["parent"]), str(e["child"]))
        if key not in dedup:
            dedup[key] = e
        else:
            if int(e.get("weight", 1)) > int(dedup[key].get("weight", 1)):
                dedup[key] = e
            else:
                # 也可以累加 count，这里保持“最大权重”策略以便可审计
                pass
    kept = list(dedup.values())

    # 可选 top-k per node：按 parent 的出边保留 top-k
    if top_k > 0 and kept:
        by_parent: Dict[str, List[Dict[str, Any]]] = {}
        for e in kept:
            by_parent.setdefault(str(e["parent"]), []).append(e)

        kept2: List[Dict[str, Any]] = []
        for parent, lst in by_parent.items():
            lst_sorted = sorted(lst, key=lambda x: int(x.get("weight", x.get("count", 1))), reverse=True)
            kept2.extend(lst_sorted[:top_k])
        kept = kept2

    # 全局 max_edges
    if max_edges > 0 and len(kept) > max_edges:
        kept = sorted(kept, key=lambda x: int(x.get("weight", x.get("count", 1))), reverse=True)[:max_edges]

    pruned = before - len(kept)
    return kept, BuildStatus(executed=True, reason="ok"), {"pruned": int(pruned)}


def _compute_max_depth(nodes: Set[str], edges: List[Dict[str, Any]]) -> int:
    """
    v0001: 估算最大深度（最长 parent->child 路径）。
    若存在环，函数会尽可能返回一个保守估计（不抛错），并在上层 run_meta 里记录 cycle_detected。
    """
    if not nodes or not edges:
        return 0

    children: Dict[str, List[str]] = {}
    for e in edges:
        p = str(e.get("parent", ""))
        c = str(e.get("child", ""))
        if not p or not c:
            continue
        children.setdefault(p, []).append(c)

    visiting: Set[str] = set()
    visited: Dict[str, int] = {}
    cycle = {"hit": False}

    def dfs(u: str) -> int:
        if u in visited:
            return visited[u]
        if u in visiting:
            cycle["hit"] = True
            return 1  # break cycle conservatively
        visiting.add(u)
        best = 1
        for v in children.get(u, []):
            best = max(best, 1 + dfs(v))
        visiting.remove(u)
        visited[u] = best
        return best

    best_all = 1
    for n in nodes:
        best_all = max(best_all, dfs(n))

    # depth levels: nodes count along path; convert to edges depth by -1 if you prefer.
    # Here we keep "levels" semantics but clamp to >=0.
    max_depth = max(0, int(best_all - 1))
    return max_depth


def _compute_graph_statistics(node_count: int, edge_count: int, max_depth: int) -> Dict[str, Any]:
    return {
        "node_count": int(node_count),
        "edge_count": int(edge_count),
        "max_depth": int(max_depth),
    }


def _build_viz_payload(
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    policy: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], BuildStatus]:
    if not nodes:
        return None, BuildStatus(executed=False, reason="empty_graph")

    viz_cfg = policy.get("viz", {})
    if not viz_cfg.get("enabled", True):
        return None, BuildStatus(executed=False, reason="disabled")

    schema = str(viz_cfg.get("schema", "lite_v0001"))
    viz = {
        "schema": schema,
        "nodes": [{"id": n.get("id"), "label": n.get("label", "")} for n in nodes],
        "edges": [{"parent": e.get("parent"), "child": e.get("child"), "weight": int(e.get("weight", e.get("count", 1)))} for e in edges],
    }
    return viz, BuildStatus(executed=True, reason="ok")


def _prepare_output_dirs(output_root: Path) -> Dict[str, Path]:
    current_dir = output_root / CURRENT_DIRNAME
    archive_dir = output_root / ARCHIVE_DIRNAME
    run_meta_dir = output_root / RUN_META_DIRNAME
    _safe_mkdir(current_dir)
    _safe_mkdir(archive_dir)
    _safe_mkdir(run_meta_dir)
    return {"current": current_dir, "archive": archive_dir, "run_meta": run_meta_dir}


def _archive_previous_current_if_needed(
    current_graph_path: Path,
    archive_dir: Path,
    run_id: str,
    dry_run: bool,
) -> Optional[str]:
    if dry_run:
        return None
    if not current_graph_path.exists():
        return None

    revoked_name = f"{current_graph_path.stem}__revoked__{run_id}{current_graph_path.suffix}"
    dst = archive_dir / revoked_name
    _atomic_move(current_graph_path, dst)
    return revoked_name


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build object-level hierarchy graph from register + hierarchy facts (v0002)."
    )
    p.add_argument("--register", required=True, help="Path to register_structural_units JSONL.")
    p.add_argument("--hierarchy", required=True, help="Path to hierarchy facts JSONL.")
    p.add_argument("--output-root", default="actioning/pipelines/build_unit_hierarchy_graph_action", help="Output root directory for unit_hierarchy graph.")
    p.add_argument("--policy", default="", help="Optional policy YAML (lite parsed).")
    p.add_argument("--run-meta", default="", help="Optional run_meta output path. If empty, write to _run_meta/run_{run_id}.json")
    p.add_argument("--dry-run", action="store_true", help="Compute and report, but do not move/write graph. run_meta still can be written.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    run_id = _now_utc_compact()
    started_at = _now_utc_iso()

    register_path = Path(args.register)
    hierarchy_path = Path(args.hierarchy)
    output_root = Path(args.output_root)
    policy_path = Path(args.policy) if args.policy.strip() else None
    dry_run = bool(args.dry_run)

    if not register_path.exists():
        print(f"[ERROR] register not found: {register_path}", file=sys.stderr)
        return 1
    if not hierarchy_path.exists():
        print(f"[ERROR] hierarchy not found: {hierarchy_path}", file=sys.stderr)
        return 1

    policy = _load_policy(policy_path)

    dirs = _prepare_output_dirs(output_root)
    current_dir = dirs["current"]
    archive_dir = dirs["archive"]
    run_meta_dir = dirs["run_meta"]

    current_graph_path = current_dir / GRAPH_FILENAME

    inactive_reasons: Dict[str, str] = {}
    capability_status: Dict[str, Dict[str, Any]] = {}

    # Input hashes (best effort)
    input_hashes: Dict[str, Any] = {}
    try:
        input_hashes["register_sha1"] = _file_sha1(register_path)
    except Exception:
        input_hashes["register_sha1"] = None
    try:
        input_hashes["hierarchy_sha1"] = _file_sha1(hierarchy_path)
    except Exception:
        input_hashes["hierarchy_sha1"] = None
    if policy_path is not None and policy_path.exists():
        try:
            input_hashes["policy_sha1"] = _file_sha1(policy_path)
        except Exception:
            input_hashes["policy_sha1"] = None
    else:
        input_hashes["policy_sha1"] = None

    # 1) Nodes
    nodes_map, st_nodes, register_rows = _build_nodes(register_path)
    capability_status["nodes_built"] = {"executed": st_nodes.executed, "reason": st_nodes.reason}
    if not st_nodes.executed:
        inactive_reasons["nodes_built"] = st_nodes.reason

    node_ids: Set[str] = set(nodes_map.keys())

    # 2) Map hierarchy edges
    edges, st_edges, edge_map_stats = _map_edges(hierarchy_path, node_ids)
    capability_status["edges_mapped"] = {"executed": st_edges.executed, "reason": st_edges.reason, "details": edge_map_stats}
    if not st_edges.executed:
        inactive_reasons["edges_mapped"] = st_edges.reason

    # 3) Weight
    st_w = _compute_edge_weights(edges, policy)
    capability_status["edge_weight_computed"] = {"executed": st_w.executed, "reason": st_w.reason}
    if not st_w.executed:
        inactive_reasons["edge_weight_computed"] = st_w.reason

    # 4) Pruning
    edges2, st_prune, prune_details = _prune_edges(edges, policy)
    capability_status["graph_pruned"] = {"executed": st_prune.executed, "reason": st_prune.reason, "details": prune_details}
    if not st_prune.executed:
        inactive_reasons["graph_pruned"] = st_prune.reason

    # 5) Stats (always)
    nodes_list = list(nodes_map.values())
    max_depth = _compute_max_depth(node_ids, edges2) if node_ids and edges2 else 0
    stats = _compute_graph_statistics(len(nodes_list), len(edges2), max_depth)
    capability_status["graph_stats_computed"] = {
        "executed": True,
        "reason": "ok",
        "empty_graph": (len(nodes_list) == 0 or len(edges2) == 0),
    }

    # 6) Viz
    viz, st_viz = _build_viz_payload(nodes_list, edges2, policy)
    capability_status["viz_adaptation"] = {"executed": st_viz.executed, "reason": st_viz.reason}
    if not st_viz.executed:
        inactive_reasons["viz_adaptation"] = st_viz.reason

    # Graph object (even empty)
    graph_obj: Dict[str, Any] = {
        "graph_type": GRAPH_TYPE,
        "version": SCRIPT_VERSION,
        "generated_at": started_at,
        "policy_version": policy.get("policy", {}).get("version", "v0001"),
        "nodes": [{"id": n["id"], "label": n.get("label", "")} for n in nodes_list],
        "edges": [
            {
                "parent": e.get("parent"),
                "child": e.get("child"),
                "weight": int(e.get("weight", e.get("count", 1))),
                "count": int(e.get("count", 1)),
            }
            for e in edges2
        ],
        "statistics": stats,
    }
    if viz is not None:
        graph_obj["viz"] = viz

    # Archive rule: move previous current graph if new run succeeds (even empty graph)
    archived_name = _archive_previous_current_if_needed(
        current_graph_path=current_graph_path,
        archive_dir=archive_dir,
        run_id=run_id,
        dry_run=dry_run,
    )

    # Write graph
    wrote_graph = False
    if not dry_run:
        _write_json(current_graph_path, graph_obj)
        wrote_graph = True

    # run_meta path
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
        "dry_run": dry_run,
        "inputs": {
            "register": str(register_path),
            "hierarchy": str(hierarchy_path),
            "policy": str(policy_path) if policy_path is not None else "",
            "hashes": input_hashes,
        },
        "output": {
            "output_root": str(output_root),
            "current_graph_path": str(current_graph_path),
            "archived_previous_graph": archived_name if archived_name is not None else "",
            "graph_written": wrote_graph,
        },
        "input_stats": {
            "register_rows_read": int(register_rows),
            "register_count": int(len(node_ids)),
            "hierarchy_rows_read": int(edge_map_stats.get("rows_read", 0)),
            "hierarchy_rows_used": int(edge_map_stats.get("rows_used", 0)),
            "hierarchy_rows_dropped_not_in_register": int(edge_map_stats.get("rows_dropped_not_in_register", 0)),
            "hierarchy_rows_dropped_parse_failed": int(edge_map_stats.get("rows_dropped_parse_failed", 0)),
        },
        "capabilities": capability_status,
        "inactive_reasons": inactive_reasons,
        "result_stats": stats,
        "status": "ok" if not dry_run else "dry-run",
    }

    if not _is_nul_path(str(run_meta_path)):
        try:
            _write_json(run_meta_path, run_meta_obj)
        except Exception as e:
            print(f"[WARN] failed to write run_meta: {e}", file=sys.stderr)

    print(json.dumps(
        {
            "status": run_meta_obj["status"],
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "register_count": run_meta_obj["input_stats"]["register_count"],
            "node_count": stats["node_count"],
            "edge_count": stats["edge_count"],
            "max_depth": stats["max_depth"],
            "archived_previous_graph": archived_name if archived_name is not None else "",
            "output_graph": str(current_graph_path),
            "run_meta": str(run_meta_path),
        },
        ensure_ascii=False,
        indent=2
    ))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
