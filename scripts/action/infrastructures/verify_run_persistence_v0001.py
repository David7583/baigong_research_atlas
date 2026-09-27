# ============================================================
# 文件名: verify_run_persistence_v0001.py
# 中文名: 中间产物处置前落库核验
# 版本号: v0001
#
# 主层级: action
# 层级: infrastructures / retention
# 脚本定位: 独立验证已提交运行的持久化与追溯前提
#
# 职责说明:
# - 校验提交状态、来源、SQLite、DuckDB 与向量索引一致性
# 本脚本做什么:
# - 只读核验并输出带哈希的处置准入凭证
# 本脚本不做什么:
# - 不清理文件，不写业务库，不把部分成功认作完整成功
# 制度边界声明:
# - 未提交、来源缺失或任一核验失败均拒绝处置
# 可更新: True
# ============================================================
# ALIAS_META
# alias: verify_run_persistence_v0001
# family: verify_run_persistence
# role: retention_persistence_gate
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/verify_run_persistence_v0001.py
# input:
#   - confirmed run binding and completion manifest
# output:
#   - persistence verification gate
# depends_on:
#   - intermediate_retention_contract_v0001
#   - vector_integrity_check_v0003
#   - DuckDB
# used_by:
#   - intermediate_retention_pipeline_v0001
# ============================================================
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from intermediate_retention_contract_v0001 import RetentionError, cli_result, load, now, seal, sha, verify, within, write

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "verify_run_persistence"
SCRIPT_NAME = "verify_run_persistence_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心业务区
# ============================================================
def verify_persistence(binding: dict, completion_path: Path) -> dict:
    import duckdb
    verify(binding, "binding")
    root = Path(binding["project_root"])
    completion_path = within(completion_path, root)
    run_dir = Path(binding["run_dir"])
    if completion_path.parent != run_dir or completion_path.name != "completion_manifest.json":
        raise RetentionError("Completion does not belong to this run")
    completion = load(completion_path)
    journal = load(run_dir / "transaction_journal.json")
    if completion.get("status") != "completed" or completion.get("run_id") != binding["run_id"] or journal.get("state") != "COMMITTED":
        raise RetentionError("Run is not durably completed and committed")
    source = Path(binding["source"])
    if sha(source) != binding["source_sha256"]:
        raise RetentionError("Original source changed")
    ingress = completion["canonical_ingress"]
    protected = {str(source): sha(source)}
    canonical = Path(ingress["canonical_path"])
    if sha(canonical) != ingress["canonical_sha256"]:
        raise RetentionError("Canonical provenance changed")
    for path in (canonical, Path(ingress["envelope_path"]), Path(ingress["lineage_index"]), completion_path, run_dir / "transaction_journal.json", Path(completion["action_return_manifest"]), Path(completion["action_lineage"]["record_path"])):
        protected[str(path)] = sha(path)
    lineage = load(Path(completion["action_lineage"]["record_path"]))
    if lineage.get("record_id") != completion["action_lineage"]["record_id"] or lineage.get("status") != "completed":
        raise RetentionError("Persisted action lineage does not confirm completion")
    if completion["action_lineage"].get("status") != "completed":
        raise RetentionError("Action lineage incomplete")
    for path in completion.get("child_completion_manifests", []):
        child = load(Path(path))
        if child.get("status") != "completed":
            raise RetentionError("Child pipeline incomplete")
        if child.get("pipeline") == "action_anchor_persistence_pipeline" and child.get("pipeline_version") != binding.get("anchor_pipeline_version", "v0005"):
            raise RetentionError("Anchor version differs from the authorized cleanup scope")
        protected[path] = sha(Path(path))
    paths = binding["paths"]
    counts = {}
    sql_sets = {}
    for key in ("data_db", "action_db"):
        db = within(Path(paths[key]), root)
        if not db.is_file():
            raise RetentionError("Existing business database required")
        with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as connection:
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or connection.execute("PRAGMA foreign_key_check").fetchall():
                raise RetentionError(f"SQLite integrity check failed: {key}")
            if key == "action_db":
                for table, field in (("concept_units", "unit_text_id"), ("instance_units", "instance_id")):
                    sql_sets[table] = {row[0] for row in connection.execute(f"SELECT {field} FROM {table}")}
                    counts[table] = len(sql_sets[table])
            else:
                for table in ("data_text_units", "data_text_unit_source_timestamps", "data_text_unit_source_identities"):
                    counts[table] = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                coordinates = "asset_id,path,value_index,segment_index,sentence_index,char_start,char_end"
                for side in ("data_text_unit_source_timestamps", "data_text_unit_source_identities"):
                    for left, right in (("data_text_units", side), (side, "data_text_units")):
                        if connection.execute(f"SELECT {coordinates} FROM {left} EXCEPT SELECT {coordinates} FROM {right} LIMIT 1").fetchone():
                            raise RetentionError("Source coordinate coverage mismatch")
        protected[str(db)] = sha(db)
    if not counts["data_text_units"] == counts["data_text_unit_source_timestamps"] == counts["data_text_unit_source_identities"]:
        raise RetentionError("Source side-table coverage mismatch")
    duck = within(Path(paths["duckdb_path"]), root)
    with duckdb.connect(str(duck), read_only=True) as connection:
        for table, field in (("concept_units", "unit_text_id"), ("instance_units", "instance_id")):
            found = {row[0] for row in connection.execute(f"SELECT {field} FROM {table}_latest").fetchall()}
            if found != sql_sets[table]:
                raise RetentionError(f"DuckDB identity mismatch: {table}")
    protected[str(duck)] = sha(duck)
    audit_script = root / "scripts/action/vector/vector_integrity_check_v0003.py"
    command = [sys.executable, "-B", str(audit_script), "--sql-db", paths["action_db"], "--chromadb-path", paths["chromadb_path"], "--state-index", paths["vector_state_index"], "--target", "all", "--dry-run"]
    process = subprocess.run(command, cwd=root, capture_output=True, text=True, encoding=DEFAULT_ENCODING, timeout=3600)
    if process.returncode:
        raise RetentionError(f"Vector verification failed: {process.stderr[-1500:]}")
    audit = json.loads(process.stdout)
    if audit.get("status") != "ok" or not audit.get("checks") or any(v != "pass" for v in audit["checks"].values()):
        raise RetentionError("Vector persistence incomplete")
    protected[paths["vector_state_index"]] = sha(Path(paths["vector_state_index"]))
    # Metadata consistency alone cannot establish that stored vectors are finite and complete.
    import chromadb
    import numpy as np
    import yaml
    config_path = Path(paths["embed_config"])
    dimension = int(yaml.safe_load(config_path.read_text(encoding=DEFAULT_ENCODING))["model"]["dimension"])
    collection = chromadb.PersistentClient(path=paths["chromadb_path"]).get_collection("action_data_embeddings")
    vector_count = collection.count()
    checked, active_checked = 0, 0
    for offset in range(0, vector_count, 500):
        batch = collection.get(limit=500, offset=offset, include=["embeddings", "metadatas"])
        vectors = np.asarray(batch["embeddings"])
        if vectors.ndim != 2 or vectors.shape != (len(batch["ids"]), dimension) or not np.isfinite(vectors).all():
            raise RetentionError("Stored vector dimensions or finite values failed verification")
        checked += len(batch["ids"])
        active_checked += sum(meta.get("state", "active") == "active" for meta in batch["metadatas"])
    if checked != vector_count or active_checked != counts["concept_units"] + counts["instance_units"]:
        raise RetentionError("Stored vector count does not cover all persisted objects")
    audit["stored_vectors"] = {"checked": checked, "active_checked": active_checked, "dimension": dimension, "finite": True, "batch_size": 500}
    protected[str(config_path)] = sha(config_path)
    return seal({"kind": "gate", "status": "passed", "binding_sha256": binding["contract_sha256"], "run_id": binding["run_id"], "checked_at": now(), "counts": counts, "vector_audit": audit, "protected_files": protected})


# ============================================================
# CLI / main 接口区
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binding", required=True)
    parser.add_argument("--completion", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    def run():
        result = verify_persistence(load(Path(args.binding)), Path(args.completion))
        if not args.dry_run:
            write(Path(args.output), result)
        return result
    return cli_result(run)


if __name__ == "__main__":
    raise SystemExit(main())
