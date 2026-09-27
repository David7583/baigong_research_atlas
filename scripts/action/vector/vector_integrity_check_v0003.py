# ============================================================
# 文件名: vector_integrity_check_v0003.py
# 中文名: 自包含向量完整性审计
# 版本号: v0003
#
# 主层级: action
# 层级: scripts/action/vector
# 脚本定位: 保持已验证契约的自包含版本实现
#
# 职责说明:
# - 在本文件中完整实现本脚本职责，不加载同家族旧版
# 本脚本做什么:
# - 保留输入校验、处理顺序、来源证据与失败行为
# 本脚本不做什么:
# - 不替代独立业务子脚本，不迁移原始数据
# 制度边界声明:
# - 仅向显式配置目标写入；测试写入必须隔离，失败不得伪报成功
# - 原始输入只读；模型选择沿用配置，不包含密钥
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: vector_integrity_check
# family: vector_audit
# role: integrity_checker
# version: v0003
# status: active
# entry_point: scripts/action/vector/vector_integrity_check_v0003.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
# used_by:
#   - vector_embedding_pipeline_v0004
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/action/vector/vector_integrity_check_v0001.py, scripts/action/vector/vector_integrity_check_v0002.py
# ============================================================
# 依赖
# ============================================================

from __future__ import annotations
import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import importlib.util
import os
from typing import Any, Dict, Optional

# ============================================================
# 全局常量与默认映射
# ============================================================

COLLECTION_NAME = "action_data_embeddings"


STATUS_PASS = "pass"


STATUS_FAIL = "fail"


STATUS_ERROR = "error"


SCRIPT_NAME = "vector_integrity_check_v0003.py"


SCRIPT_VERSION = "v0003"


MAIN_LAYER = "action"


VALID_TARGETS = ("all", "concept", "instance")


TARGET_ENV = "ACTION_VECTOR_INTEGRITY_TARGET"


DEFAULT_ENCODING = "utf-8"


SCRIPT_FAMILY = "vector_audit"

# ============================================================
# 数据结构、工具与核心实现
# ============================================================


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timestamp_for_path() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _pretty_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue


def _write_jsonl(path: Path, records: List[Dict[str, Any]]) -> int:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(_safe_json(rec) + "\n")
    return len(records)


def _make_index_key(object_type: str, object_id: str) -> str:
    return f"{object_type}:{object_id}"


def _connect_sql(db_path: Path) -> Optional[sqlite3.Connection]:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _connect_chromadb(chromadb_path: Path):
    try:
        import chromadb
        from chromadb.config import Settings
    except ImportError:
        raise RuntimeError("chromadb is required. Run: pip install chromadb")
    if not chromadb_path.exists():
        return (None, None)
    try:
        client = chromadb.PersistentClient(
            path=str(chromadb_path), settings=Settings(anonymized_telemetry=False)
        )
        collection = client.get_collection(name=COLLECTION_NAME)
        return (client, collection)
    except Exception:
        return (None, None)


def _get_sql_records(conn: sqlite3.Connection) -> Dict[str, str]:
    """
    获取 SQL 核心比对数据 (极简结构)
    返回: { "type:id": "content_hash" }
    """
    records: Dict[str, str] = {}
    try:
        cursor = conn.execute("SELECT instance_id, content_hash FROM instance_units")
        for row in cursor:
            records[f"instance:{row['instance_id']}"] = (
                str(row["content_hash"]).lower() if row["content_hash"] else ""
            )
    except Exception:
        pass
    try:
        cursor = conn.execute("SELECT unit_text_id, content_hash FROM concept_units")
        for row in cursor:
            records[f"concept:{row['unit_text_id']}"] = (
                str(row["content_hash"]).lower() if row["content_hash"] else ""
            )
    except Exception:
        pass
    return records


def _get_sql_counts(conn: sqlite3.Connection) -> Tuple[int, int]:
    instance_count = 0
    concept_count = 0
    try:
        instance_count = conn.execute("SELECT COUNT(*) FROM instance_units").fetchone()[0]
    except Exception:
        pass
    try:
        concept_count = conn.execute("SELECT COUNT(*) FROM concept_units").fetchone()[0]
    except Exception:
        pass
    return (instance_count, concept_count)


def _get_chromadb_records(collection) -> Dict[str, Tuple[str, str, str, str]]:
    """
    获取 ChromaDB 核心比对数据 (防覆盖与防 OOM 结构)
    以唯一的 doc_id 为键，绝不覆盖同 object_id 的多条记录。
    返回: { "signature": ("object_type", "object_id", "source_hash", "state") }
    """
    records: Dict[str, Tuple[str, str, str, str]] = {}
    try:
        total = collection.count()
        batch_size = 5000
        offset = 0
        while offset < total:
            results = collection.get(limit=batch_size, offset=offset, include=["metadatas"])
            ids = results.get("ids", [])
            metadatas = results.get("metadatas", [])
            for doc_id, meta in zip(ids, metadatas):
                obj_type = meta.get("object_type", "")
                obj_id = meta.get("object_id", "")
                if obj_type and obj_id:
                    records[doc_id] = (
                        obj_type,
                        obj_id,
                        meta.get("source_hash", "").lower(),
                        meta.get("state", "active"),
                    )
            offset += batch_size
    except Exception:
        pass
    return records


def _get_chromadb_counts(collection) -> Tuple[int, int]:
    instance_count = 0
    concept_count = 0
    try:
        total = collection.count()
        batch_size = 5000
        offset = 0
        while offset < total:
            results = collection.get(limit=batch_size, offset=offset, include=["metadatas"])
            for meta in results.get("metadatas", []):
                if meta.get("object_type") == "instance":
                    instance_count += 1
                elif meta.get("object_type") == "concept":
                    concept_count += 1
            offset += batch_size
    except Exception:
        pass
    return (instance_count, concept_count)


def _get_index_records(index_path: Path) -> Dict[str, str]:
    """
    加载状态索引
    返回: { "type:id": "embedding_signature" }
    """
    records: Dict[str, str] = {}
    for rec in _iter_jsonl(index_path):
        obj_type = rec.get("object_type", "")
        obj_id = rec.get("object_id", "")
        sig = rec.get("embedding_signature", "")
        if obj_type and obj_id and sig:
            records[f"{obj_type}:{obj_id}"] = sig
    return records


def check_count_alignment(s_inst: int, s_conc: int, c_inst: int, c_conc: int) -> Dict[str, Any]:
    inst_match = s_inst == c_inst
    conc_match = s_conc == c_conc
    return {
        "status": STATUS_PASS if inst_match and conc_match else STATUS_FAIL,
        "sql_instance_count": s_inst,
        "sql_concept_count": s_conc,
        "chromadb_instance_count": c_inst,
        "chromadb_concept_count": c_conc,
        "instance_match": inst_match,
        "concept_match": conc_match,
    }


def check_orphan_vectors(
    sql_records: Dict[str, str], chroma_records: Dict[str, Tuple[str, str, str, str]]
) -> Tuple[str, List[Dict[str, Any]]]:
    orphans = []
    for sig, (c_type, c_id, _, _) in chroma_records.items():
        if f"{c_type}:{c_id}" not in sql_records:
            orphans.append({"object_type": c_type, "object_id": c_id, "embedding_signature": sig})
    return (STATUS_PASS if not orphans else STATUS_FAIL, orphans)


def check_missing_vectors(
    sql_records: Dict[str, str], chroma_records: Dict[str, Tuple[str, str, str, str]]
) -> Tuple[str, List[Dict[str, Any]]]:
    chroma_keys = {f"{c[0]}:{c[1]}" for c in chroma_records.values()}
    missing = []
    for sql_key in sql_records:
        if sql_key not in chroma_keys:
            obj_type, obj_id = sql_key.split(":", 1)
            missing.append({"object_type": obj_type, "object_id": obj_id})
    return (STATUS_PASS if not missing else STATUS_FAIL, missing)


def check_signature_drift(
    sql_records: Dict[str, str], chroma_records: Dict[str, Tuple[str, str, str, str]]
) -> Tuple[str, List[Dict[str, Any]]]:
    drifts = []
    for sig, (c_type, c_id, c_hash, _) in chroma_records.items():
        sql_key = f"{c_type}:{c_id}"
        if sql_key in sql_records:
            s_hash = sql_records[sql_key]
            if c_hash and s_hash and (c_hash != s_hash):
                drifts.append(
                    {
                        "object_type": c_type,
                        "object_id": c_id,
                        "sql_content_hash": s_hash,
                        "chromadb_source_hash": c_hash,
                    }
                )
    return (STATUS_PASS if not drifts else STATUS_FAIL, drifts)


def check_index_consistency(
    index_records: Dict[str, str], chroma_records: Dict[str, Tuple[str, str, str, str]]
) -> Tuple[str, Dict[str, Any]]:
    chroma_active = {f"{c[0]}:{c[1]}": sig for sig, c in chroma_records.items() if c[3] == "active"}
    index_orphans = [key for key, sig in index_records.items() if chroma_active.get(key) != sig]
    index_missing = [key for key, sig in chroma_active.items() if index_records.get(key) != sig]
    status = STATUS_PASS if not index_orphans and (not index_missing) else STATUS_FAIL
    return (
        status,
        {
            "index_orphans_count": len(index_orphans),
            "index_missing_count": len(index_missing),
            "index_orphans_sample": index_orphans[:10],
            "index_missing_sample": index_missing[:10],
        },
    )


def check_multi_active(
    chroma_records: Dict[str, Tuple[str, str, str, str]]
) -> Tuple[str, List[Dict[str, Any]]]:
    active_counts: Dict[str, List[str]] = {}
    for sig, (c_type, c_id, _, c_state) in chroma_records.items():
        if c_state == "active":
            key = f"{c_type}:{c_id}"
            active_counts.setdefault(key, []).append(sig)
    violations = [
        {"object_id": key.split(":", 1)[1], "active_count": len(sigs), "signatures": sigs}
        for key, sigs in active_counts.items()
        if len(sigs) > 1
    ]
    return (STATUS_PASS if not violations else STATUS_FAIL, violations)


def _filter_sql(records: Dict[str, str], target: str) -> Dict[str, str]:
    if target == "all":
        return records
    prefix = f"{target}:"
    return {key: value for key, value in records.items() if key.startswith(prefix)}


def _filter_chroma(records: Dict[str, tuple], target: str) -> Dict[str, tuple]:
    if target == "all":
        return records
    return {signature: value for signature, value in records.items() if value[0] == target}


def _filter_index(records: Dict[str, str], target: str) -> Dict[str, str]:
    if target == "all":
        return records
    prefix = f"{target}:"
    return {key: value for key, value in records.items() if key.startswith(prefix)}


def _count_alignment(
    target: str, sql_instance: int, sql_concept: int, chroma_instance: int, chroma_concept: int
) -> Dict[str, Any]:
    details = check_count_alignment(sql_instance, sql_concept, chroma_instance, chroma_concept)
    if target == "concept":
        matched = sql_concept == chroma_concept
    elif target == "instance":
        matched = sql_instance == chroma_instance
    else:
        matched = bool(details["instance_match"] and details["concept_match"])
    details.update(
        {"status": STATUS_PASS if matched else STATUS_FAIL, "audit_target": target, "scope_match": matched}
    )
    return details


def run_integrity_check(
    sql_db_path: Path,
    chromadb_path: Path,
    state_index_path: Path,
    output_dir: Optional[Path],
    target: str = "all",
    quick_mode: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if target not in VALID_TARGETS:
        raise ValueError(f"Unsupported audit target: {target}")
    started_at = _utc_iso()
    result: Dict[str, Any] = {
        "status": "ok",
        "timestamp": started_at,
        "audit_target": target,
        "scope": "global" if target == "all" else "single_object_type",
        "summary": {},
        "checks": {},
        "details": {},
        "errors": [],
    }
    sql_conn = _connect_sql(sql_db_path)
    if sql_conn is None:
        result["errors"].append(f"Failed to connect to SQL: {sql_db_path}")
        result["status"] = "error"
    client, collection = _connect_chromadb(chromadb_path)
    if collection is None:
        result["errors"].append(f"Failed to connect to ChromaDB: {chromadb_path}")
        result["status"] = "error"
    if result["status"] == "error":
        if sql_conn is not None:
            sql_conn.close()
        return result
    try:
        sql_instance, sql_concept = _get_sql_counts(sql_conn)
        chroma_instance, chroma_concept = _get_chromadb_counts(collection)
        all_index_records = _get_index_records(state_index_path)
        result["summary"] = {
            "audit_target": target,
            "sql_instance_count": sql_instance,
            "sql_concept_count": sql_concept,
            "sql_total": sql_instance + sql_concept,
            "chromadb_instance_count": chroma_instance,
            "chromadb_concept_count": chroma_concept,
            "chromadb_total": chroma_instance + chroma_concept,
            "index_count": len(all_index_records),
        }
        count_check = _count_alignment(target, sql_instance, sql_concept, chroma_instance, chroma_concept)
        result["checks"]["count_alignment"] = count_check["status"]
        result["details"]["count_alignment"] = count_check
        if quick_mode:
            for key in (
                "orphan_vectors",
                "missing_vectors",
                "signature_drift",
                "index_consistency",
                "multi_active",
            ):
                result["checks"][key] = "skipped"
        else:
            sql_records = _filter_sql(_get_sql_records(sql_conn), target)
            chroma_records = _filter_chroma(_get_chromadb_records(collection), target)
            index_records = _filter_index(all_index_records, target)
            result["summary"]["scoped_sql_count"] = len(sql_records)
            result["summary"]["scoped_chromadb_count"] = len(chroma_records)
            result["summary"]["scoped_index_count"] = len(index_records)
            checks = (
                ("orphan_vectors", check_orphan_vectors(sql_records, chroma_records)),
                ("missing_vectors", check_missing_vectors(sql_records, chroma_records)),
                ("signature_drift", check_signature_drift(sql_records, chroma_records)),
            )
            for key, (status, details) in checks:
                result["checks"][key] = status
                result["summary"][f"{key.removesuffix('_vectors')}_count"] = len(details)
                if details:
                    result["details"][key] = details
            index_status, index_details = check_index_consistency(index_records, chroma_records)
            result["checks"]["index_consistency"] = index_status
            result["details"]["index_consistency"] = index_details
            multi_status, multi_details = check_multi_active(chroma_records)
            result["checks"]["multi_active"] = multi_status
            result["summary"]["multi_active_count"] = len(multi_details)
            if multi_details:
                result["details"]["multi_active"] = multi_details
    finally:
        sql_conn.close()
    if any((value == STATUS_FAIL for value in result["checks"].values())):
        result["status"] = "issues_found"
    result["finished_at"] = _utc_iso()
    if not dry_run and output_dir:
        try:
            _ensure_dir(output_dir)
            (output_dir / "report.json").write_text(_pretty_json(result), encoding="utf-8")
            for key in ("orphan_vectors", "missing_vectors", "signature_drift"):
                if result["details"].get(key):
                    _write_jsonl(output_dir / f"{key}.jsonl", result["details"][key])
            meta = {
                "script_name": SCRIPT_NAME,
                "script_version": SCRIPT_VERSION,
                "main_layer": MAIN_LAYER,
                "audit_target": target,
                "sql_db_path": str(sql_db_path),
                "chromadb_path": str(chromadb_path),
                "state_index_path": str(state_index_path),
                "quick_mode": quick_mode,
                "started_at": started_at,
                "finished_at": result["finished_at"],
                "status": result["status"],
            }
            (output_dir / "run_meta.json").write_text(_pretty_json(meta), encoding="utf-8")
            result["output_dir"] = str(output_dir)
        except Exception as exc:
            result["errors"].append(f"Failed to write report: {exc}")
            result["status"] = "error"
    return result


# ============================================================
# CLI / main 接口区
# ============================================================


def parse_args() -> argparse.Namespace:
    default_target = os.environ.get(TARGET_ENV, "all")
    if default_target not in VALID_TARGETS:
        default_target = "all"
    parser = argparse.ArgumentParser(
        description="Audit SQL, ChromaDB and state-index vector integrity in an explicit scope."
    )
    parser.add_argument("--sql-db", default="sql/action_data.db")
    parser.add_argument("--chromadb-path", default="chromadb/action/action_data/vectors")
    parser.add_argument("--state-index", default="vector/state/active_index.jsonl")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--target", choices=VALID_TARGETS, default=default_target)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir) if args.output_dir else None
    if output_dir is None and (not args.dry_run):
        output_dir = Path("vector/audit/integrity_check") / _timestamp_for_path()
    result = run_integrity_check(
        sql_db_path=Path(args.sql_db),
        chromadb_path=Path(args.chromadb_path),
        state_index_path=Path(args.state_index),
        output_dir=output_dir,
        target=args.target,
        quick_mode=args.quick,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get("status") == "error":
        return 1
    if result.get("status") == "issues_found":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
