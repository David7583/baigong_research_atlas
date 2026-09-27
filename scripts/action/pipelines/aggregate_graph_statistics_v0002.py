# ============================================================
# 文件名: aggregate_graph_statistics_v0002.py
# 中文名: 图统计快照聚合脚本
# 版本号: v0002
#
# 层级: execution
# 主层级: action
# 可更新: True
#
# 制度定位:
# 本脚本在既定制度下，对 15 脚本产出的 graph_statistics 快照集合执行横向聚合，
# 生成当前唯一生效的"统计聚合对象"，并在新对象生成时对旧对象执行制度性撤销（archive）。
#
# 核心制度语义:
# - 输入是 graph_analysis/v0001/{graph_type}/ 下的快照目录（目录，不是单文件）
# - 一次运行只聚合一个 graph_type（由输入目录或参数决定）
# - 聚合失败不应中断系统，缺失字段或坏文件必须被制度性忽略并记录原因
# - 输出是制度对象，采用 current + archive 语义，且 archive 永不参与计算
#
# 输出策略（已确认）:
# 输出位于 actioning/pipelines/aggregate_graph_statistics_action/{graph_type}/ 下
#   - current/graph_statistics_aggregated.json 仅保留一个当前生效对象
#   - archive/ 保存被撤销对象，文件名追加 __revoked__{run_id}
#   - _run_meta/aggregate_run_{run_id}.json 记录审计信息
#
# 本脚本不做什么:
# - 不回读 graph
# - 不回读 15 的 run_meta
# - 不做时间序列趋势判断（增长/下降/稳定等）
# - 不解释统计含义，不给建议
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================
# alias: aggregate_graph_statistics_v0002
# family: aggregate_graph_statistics
# role: statistics_aggregate_object_manager
# version: v0002
# status: active
# entry_point: scripts/action/pipelines/aggregate_graph_statistics_v0002.py
# input:
#   - graph_statistics_snapshot_dir
# output:
#   - aggregated_statistics_object_json
#   - run_meta_json
# depends_on:
#   - python_stdlib_only
# used_by:
#   - (无法确定，需人工填写)
# notes:
#   - manages current + archive for aggregated statistics object
#   - archive never participates in aggregation


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
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_ENCODING = "utf-8"
NUL_NAMES = {"NUL", "nul"}

SCRIPT_NAME = "aggregate_graph_statistics_v0002"
SCRIPT_VERSION = "v0002"

AGGREGATED_FILENAME = "graph_statistics_aggregated.json"
RUN_META_DIRNAME = "_run_meta"


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


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"json root must be dict, got {type(obj).__name__}")
    return obj


def _parse_iso_utc(s: str) -> Optional[datetime]:
    """
    Best-effort ISO parsing. Accepts 'Z' suffix.
    Returns timezone-aware datetime in UTC if possible.
    """
    if not isinstance(s, str) or not s.strip():
        return None
    t = s.strip()
    try:
        if t.endswith("Z"):
            t2 = t[:-1] + "+00:00"
        else:
            t2 = t
        dt = datetime.fromisoformat(t2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _mean(vals: List[float]) -> float:
    if not vals:
        return 0.0
    return float(sum(vals) / len(vals))


def _infer_graph_type_from_dir(input_dir: Path) -> str:
    # expects .../graph_analysis/v0001/{graph_type}/
    name = input_dir.name.strip()
    return name if name else "unknown_graph_type"


# ------------------------------------------------------------
# 统计快照筛选与聚合
# ------------------------------------------------------------

RE_SNAPSHOT = re.compile(r"^graph_statistics__.+\.json$", re.IGNORECASE)


@dataclass(frozen=True)
class SnapshotRecord:
    path: Path
    run_id: str
    generated_at: str
    graph_type: str
    node_count: int
    edge_count: int
    density: float
    optional: Dict[str, Any]


def _extract_required_fields(obj: Dict[str, Any]) -> Tuple[Optional[SnapshotRecord], str]:
    """
    Returns (SnapshotRecord or None, skipped_reason)
    """
    gt = obj.get("graph_type")
    if not isinstance(gt, str) or not gt.strip():
        return None, "missing_graph_type"
    graph_type = gt.strip()

    run_id = obj.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None, "missing_run_id"
    run_id = run_id.strip()

    generated_at = obj.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at.strip():
        return None, "missing_generated_at"
    generated_at = generated_at.strip()

    st = obj.get("statistics")
    if not isinstance(st, dict):
        return None, "missing_statistics_object"

    # required metrics
    node_count = st.get("node_count")
    edge_count = st.get("edge_count")
    density = st.get("density")

    if not isinstance(node_count, int):
        return None, "missing_statistics.node_count"
    if not isinstance(edge_count, int):
        return None, "missing_statistics.edge_count"
    if not isinstance(density, (int, float)):
        return None, "missing_statistics.density"

    optional: Dict[str, Any] = {}
    # optional metrics (only if present)
    for k in ("max_depth", "mean_weight", "max_weight", "min_weight"):
        v = st.get(k)
        if isinstance(v, (int, float)):
            optional[k] = v

    rec = SnapshotRecord(
        path=Path(""),
        run_id=run_id,
        generated_at=generated_at,
        graph_type=graph_type,
        node_count=int(node_count),
        edge_count=int(edge_count),
        density=float(density),
        optional=optional,
    )
    return rec, ""


def _scan_snapshots(input_dir: Path, expected_graph_type: str) -> Tuple[List[SnapshotRecord], Dict[str, int], List[Dict[str, Any]]]:
    """
    Returns:
      - used snapshot records
      - skipped_reason_counts
      - skipped_file_details (for run_meta)
    """
    used: List[SnapshotRecord] = []
    skipped_counts: Dict[str, int] = {}
    skipped_details: List[Dict[str, Any]] = []

    if not input_dir.exists() or not input_dir.is_dir():
        skipped_counts["input_dir_not_found"] = 1
        return used, skipped_counts, skipped_details

    for p in sorted(input_dir.iterdir()):
        name = p.name
        if p.is_dir():
            # ignore system folders
            if name.lower() in ("_run_meta", "archive", "current", "aggregated"):
                continue
            # do not recurse in v0001
            continue

        if not p.is_file():
            continue
        if not name.lower().endswith(".json"):
            continue
        if not RE_SNAPSHOT.match(name):
            continue

        try:
            obj = _read_json(p)
        except Exception as e:
            reason = "json_parse_failed"
            skipped_counts[reason] = skipped_counts.get(reason, 0) + 1
            skipped_details.append({"file": str(p), "reason": reason, "error": str(e)})
            continue

        rec, reason = _extract_required_fields(obj)
        if rec is None:
            skipped_counts[reason] = skipped_counts.get(reason, 0) + 1
            skipped_details.append({"file": str(p), "reason": reason})
            continue

        if rec.graph_type != expected_graph_type:
            reason2 = "graph_type_mismatch"
            skipped_counts[reason2] = skipped_counts.get(reason2, 0) + 1
            skipped_details.append({"file": str(p), "reason": reason2, "graph_type": rec.graph_type})
            continue

        used.append(SnapshotRecord(
            path=p,
            run_id=rec.run_id,
            generated_at=rec.generated_at,
            graph_type=rec.graph_type,
            node_count=rec.node_count,
            edge_count=rec.edge_count,
            density=rec.density,
            optional=rec.optional,
        ))

    return used, skipped_counts, skipped_details


def _aggregate_numeric(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0}
    return {"min": float(min(values)), "max": float(max(values)), "mean": float(_mean(values))}


def _aggregate_int(values: List[int]) -> Dict[str, Any]:
    if not values:
        return {"min": 0, "max": 0, "mean": 0.0}
    return {"min": int(min(values)), "max": int(max(values)), "mean": float(_mean([float(x) for x in values]))}


def _compute_time_bounds(snaps: List[SnapshotRecord]) -> Tuple[str, str]:
    if not snaps:
        return "", ""
    parsed = []
    for s in snaps:
        dt = _parse_iso_utc(s.generated_at)
        if dt is None:
            continue
        parsed.append(dt)
    if parsed:
        first_dt = min(parsed)
        last_dt = max(parsed)
        first = first_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        last = last_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return first, last

    # fallback: lexical by generated_at string
    gats = sorted([s.generated_at for s in snaps])
    return gats[0], gats[-1]


# ------------------------------------------------------------
# 输出对象管理（current + archive）
# ------------------------------------------------------------

def _prepare_output_dirs(output_root: Path, graph_type: str) -> Dict[str, Path]:
    base = output_root / graph_type
    current_dir = base / "current"
    archive_dir = base / "archive"
    run_meta_dir = base / RUN_META_DIRNAME
    _safe_mkdir(current_dir)
    _safe_mkdir(archive_dir)
    _safe_mkdir(run_meta_dir)
    return {"base": base, "current": current_dir, "archive": archive_dir, "run_meta": run_meta_dir}


def _compute_would_be_revoked(current_dir: Path, run_id: str) -> List[str]:
    """
    Compute would-be revoked filenames without moving anything.
    Used by both dry_run (report only) and real run (before moving).
    """
    if not current_dir.exists():
        return []
    return [
        f"{p.stem}__revoked__{run_id}{p.suffix}"
        for p in sorted(current_dir.glob("*.json"))
        if p.is_file()
    ]


def _archive_existing_current(current_dir: Path, archive_dir: Path, run_id: str) -> List[str]:
    """
    Moves all json files in current_dir into archive with revoked suffix.
    Returns list of revoked filenames.
    """
    revoked: List[str] = []
    for p in sorted(current_dir.glob("*.json")):
        if not p.is_file():
            continue
        dst_name = f"{p.stem}__revoked__{run_id}{p.suffix}"
        dst = archive_dir / dst_name
        _atomic_move(p, dst)
        revoked.append(dst_name)
    return revoked


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aggregate graph_statistics snapshots into a current-only aggregated object (v0002).")
    p.add_argument("--input-dir", required=True, help="Directory containing graph_statistics__*.json snapshots (graph_analysis/v0001/{graph_type}/).")
    p.add_argument("--output-root", default="actioning/pipelines/aggregate_graph_statistics_action", help="Output root directory for aggregated graph statistics.")
    p.add_argument("--graph-type", default="", help="Optional. If empty, inferred from input-dir name.")
    p.add_argument("--run-meta", default="", help="Optional run_meta output path. If empty, write to {graph_type}/_run_meta/aggregate_run_{run_id}.json")
    p.add_argument("--dry-run", action="store_true", help="Compute but do not move/write outputs. Prints summary json.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    run_id = _now_utc_compact()
    started_at = _now_utc_iso()

    input_dir = Path(args.input_dir)
    output_root = Path(args.output_root)
    dry_run = bool(args.dry_run)

    expected_graph_type = args.graph_type.strip() if isinstance(args.graph_type, str) and args.graph_type.strip() else _infer_graph_type_from_dir(input_dir)

    # Scan snapshots
    used, skipped_counts, skipped_details = _scan_snapshots(input_dir, expected_graph_type)

    # 计数数学自洽：不做第二次目录遍历
    used_files_count = int(len(used))
    skipped_files_count = int(sum(skipped_counts.values()))
    scanned_files_count = used_files_count + skipped_files_count

    # Aggregate core metrics
    nodes = [s.node_count for s in used]
    edges = [s.edge_count for s in used]
    dens = [s.density for s in used]

    first_run_at, last_run_at = _compute_time_bounds(used)

    aggregates: Dict[str, Any] = {}
    aggregates["node_count"] = _aggregate_int(nodes)
    aggregates["edge_count"] = _aggregate_int(edges)
    aggregates["density"] = _aggregate_numeric(dens)

    # Optional fields: aggregate only those present in at least one snapshot
    optional_fields = ("max_depth", "mean_weight", "max_weight", "min_weight")
    optional_aggregates: Dict[str, Any] = {}
    optional_presence: Dict[str, int] = {k: 0 for k in optional_fields}

    for k in optional_fields:
        vals: List[float] = []
        for s in used:
            if k in s.optional and isinstance(s.optional[k], (int, float)):
                optional_presence[k] += 1
                vals.append(float(s.optional[k]))
        if vals:
            optional_aggregates[k] = _aggregate_numeric(vals)

    if optional_aggregates:
        aggregates["optional"] = {
            "presence": optional_presence,
            "aggregates": optional_aggregates,
        }
    else:
        aggregates["optional"] = {
            "presence": optional_presence,
            "aggregates": {},
        }

    # Output object (even when no snapshots)
    aggregated_obj: Dict[str, Any] = {
        "object_type": "graph_statistics_aggregated",
        "object_version": SCRIPT_VERSION,
        "graph_type": expected_graph_type,
        "generated_at": started_at,
        "run_id": run_id,
        "total_snapshots": int(used_files_count),
        "first_run_at": first_run_at,
        "last_run_at": last_run_at,
        "aggregates": aggregates if used_files_count > 0 else {},
        "note": "" if used_files_count > 0 else "no valid statistics snapshots found",
    }

    # Prepare output dirs
    dirs = _prepare_output_dirs(output_root, expected_graph_type)
    current_dir = dirs["current"]
    archive_dir = dirs["archive"]
    run_meta_dir = dirs["run_meta"]

    current_path = current_dir / AGGREGATED_FILENAME

    # dry_run: 计算 would-be revoked list 供审计，但不移动文件
    # real run: 先计算再执行移动
    would_be_revoked = _compute_would_be_revoked(current_dir, run_id)

    revoked_files: List[str] = []
    archive_triggered = False

    if not dry_run and would_be_revoked:
        revoked_files = _archive_existing_current(current_dir, archive_dir, run_id)
        archive_triggered = bool(revoked_files)

    # Input hashes (best effort)
    input_hashes: Dict[str, Any] = {
        "input_dir": str(input_dir),
        "used_snapshot_sha1": {},
    }
    for s in used:
        try:
            input_hashes["used_snapshot_sha1"][s.path.name] = _file_sha1(s.path)
        except Exception:
            input_hashes["used_snapshot_sha1"][s.path.name] = None

    # run_meta path
    if args.run_meta and not _is_nul_path(args.run_meta):
        run_meta_path = Path(args.run_meta)
    else:
        run_meta_path = run_meta_dir / f"aggregate_run_{run_id}.json"

    run_meta_obj: Dict[str, Any] = {
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "dry_run": dry_run,
        "inputs": {
            "input_dir": str(input_dir),
            "expected_graph_type": expected_graph_type,
            "hashes": input_hashes,
        },
        "scan_stats": {
            "scanned_files_count": int(scanned_files_count),
            "used_files_count": int(used_files_count),
            "skipped_files_count": int(skipped_files_count),
            "skipped_reason_counts": skipped_counts,
        },
        "skipped_files_sample": skipped_details[:200],
        "outputs": {
            "current_path": str(current_path),
            "run_meta_path": str(run_meta_path),
            "archive_triggered": bool(archive_triggered),
            "would_be_revoked": would_be_revoked,   # dry_run 下也可见
            "revoked_files": revoked_files,           # 实际执行后才有值
            "written": False,
        },
        "status": "ok" if not dry_run else "dry-run",
    }

    # Write outputs
    if not dry_run:
        _safe_mkdir(current_dir)
        _safe_mkdir(archive_dir)
        _safe_mkdir(run_meta_dir)

        run_meta_obj["outputs"]["written"] = True

        _write_json(current_path, aggregated_obj)
        _write_json(run_meta_path, run_meta_obj)

    # Print summary
    print(json.dumps(
        {
            "status": run_meta_obj["status"],
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "graph_type": expected_graph_type,
            "scanned_files_count": scanned_files_count,
            "used_files_count": used_files_count,
            "archive_triggered": archive_triggered,
            "would_be_revoked": would_be_revoked,
            "output_current": str(current_path),
            "run_meta": str(run_meta_path),
        },
        ensure_ascii=False,
        indent=2
    ))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
