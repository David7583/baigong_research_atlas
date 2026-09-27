# ============================================================
# 文件名: ingestion_receipt_v0002.py
# 中文名: 核心已提交回执核验
# 版本号: v0002
#
# 主层级: action
# 层级: tools / ingestion / ingestion_core_receipt
# 脚本定位: 核心已提交回执核验的单一职责边界
#
# 职责说明:
# - 核心已提交回执核验，输入输出与错误可检查
#
# 本脚本做什么:
# - 校验来源、状态及配置，保存本次职责的可追溯结果
#
# 本脚本不做什么:
# - 不运行未知代码，不调用收费模型，不修改原始来源
#
# 制度边界声明:
# - 运行产物只写显式受管目录，追加发布；测试写入限定 temp
# - 导入无写入副作用，失败明确返回，不覆盖既有证据
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingestion_receipt_v0002
# family: ingestion_receipt
# role: ingestion_core_receipt
# version: v0002
# status: active
# entry_point: scripts/action/tools/ingestion/ingestion_receipt_v0002.py
# input:
#   - explicit input paths and versioned contracts
# output:
#   - structured results and auditable evidence
# depends_on:
#   - ingestion_store_v0001
#   - Python stdlib
# used_by:
#   - unified_ingestion_pipeline_v0002
# ============================================================

from __future__ import annotations

import json
from pathlib import Path

from scripts.action.tools.ingestion.ingestion_store_v0001 import (
    IngestionError, Store, digest, no_links, now, sha,
)

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "ingestion_receipt"
SCRIPT_NAME = "ingestion_receipt_v0002"
SCRIPT_VERSION = "v0002"
COMPLETION_SCHEMA = "data_action_chain_completion_v0001"


# ============================================================
# 工具函数区
# ============================================================

def load_evidence(path, root):
    path = no_links(Path(path))
    if not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
        raise IngestionError("core_evidence_outside_project_or_missing")
    value = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
    if not isinstance(value, dict):
        raise IngestionError("core_evidence_must_be_object")
    return value


# ============================================================
# 核心业务组件
# ============================================================

def confirm(config, intake_run_id, completion_path, dry_run=False):
    """Observe a committed local core run, never run the core or infer success."""
    store = Store(config)
    intake = store.find(intake_run_id)
    store.verify(intake["source"])
    prepared = intake.get("downstream_receipt")
    if intake["handoff_status"] != "ready_for_handoff" or not prepared or "core_request" not in prepared:
        raise IngestionError("intake_has_no_verified_core_request")
    completion_path = no_links(Path(completion_path))
    completion = load_evidence(completion_path, config.root)
    if (completion.get("schema_version") != COMPLETION_SCHEMA
            or completion.get("pipeline") != "data_action_chain_pipeline"
            or completion.get("pipeline_version") not in {"v0018", "v0019"}
            or completion.get("status") != "completed"):
        raise IngestionError("core_completion_contract_mismatch")
    if (not isinstance(completion.get("run_id"), str)
            or not isinstance(completion.get("canonical_ingress"), dict)
            or not isinstance(completion.get("action_lineage"), dict)
            or not isinstance(completion.get("child_completion_manifests"), list)):
        raise IngestionError("core_completion_fields_missing")
    run = load_evidence(completion_path.parent / "run_manifest.json", config.root)
    journal = load_evidence(completion_path.parent / "transaction_journal.json", config.root)
    run_id = completion["run_id"]
    if (run.get("run_id") != run_id or run.get("status") != "completed"
            or journal.get("run_id") != run_id or journal.get("state") != "COMMITTED"):
        raise IngestionError("core_transaction_not_committed")
    request = prepared["core_request"]
    expected_source = (Path(request["data_root"]) / request["target"]).resolve()
    actual_source = Path(run["plan"]["target_path"]).resolve()
    if actual_source != expected_source or sha(no_links(actual_source)) != intake["source"]["source_hash"]:
        raise IngestionError("core_receipt_source_mismatch")
    envelope_path = completion["canonical_ingress"]["envelope_path"]
    envelope = load_evidence(envelope_path, config.root)
    if (envelope.get("status") != "completed"
            or envelope["source"]["sha256"] != intake["source"]["source_hash"]
            or not envelope["source"]["immutable_verified"]):
        raise IngestionError("core_envelope_source_mismatch")
    lineage = load_evidence(completion["action_lineage"]["record_path"], config.root)
    if lineage.get("status") != "completed":
        raise IngestionError("core_lineage_incomplete")
    evidence_paths = [completion_path, completion_path.parent / "run_manifest.json",
                      completion_path.parent / "transaction_journal.json", Path(envelope_path),
                      Path(completion["action_lineage"]["record_path"])]
    if completion.get("pipeline_version") == "v0019" and run.get("plan", {}).get("derivation_target") in {"neo4j", "all"}:
        coordinator_path = completion_path.parent / "graph_coordinator.json"
        graph_path = completion_path.parent / "graph_batch.json"
        coordinator = load_evidence(coordinator_path, config.root)
        graph = load_evidence(graph_path, config.root)
        if (coordinator.get("batch_id") != run_id or coordinator.get("decision") != "COMMITTED"
                or graph.get("binding", {}).get("batch_id") != run_id or graph.get("state") != "COMMITTED"):
            raise IngestionError("core_graph_transaction_not_committed")
        evidence_paths.extend([coordinator_path, graph_path])
    children = completion.get("child_completion_manifests", [])
    if not children:
        raise IngestionError("core_child_evidence_missing")
    for child_path in children:
        child = load_evidence(child_path, config.root)
        if child.get("status") != "completed":
            raise IngestionError("core_child_incomplete")
        evidence_paths.append(Path(child_path))
    result_id = "r_" + digest([intake_run_id, sha(completion_path)])[:32]
    for existing in store.catalog():
        if existing["run_id"] == result_id:
            return existing
    observed = {
        "schema_version": "ingestion_core_receipt_v0001",
        "intake_run_id": intake_run_id, "core_run_id": run_id,
        "source_hash": intake["source"]["source_hash"],
        "completion_sha256": sha(completion_path),
        "no_new_content": bool(completion.get("no_new_content")),
        "test_mode": bool(completion.get("test_mode")),
        "scope": "configured_core_stages_only",
        "vector_selection": completion.get("vector_selection"),
        "evidence": [{"path": str(p), "sha256": sha(p)} for p in evidence_paths],
    }
    result = {**intake, "run_id": result_id, "ingested_at": now(),
              "derived_from": intake_run_id, "processing_status": "core_completed",
              "body_ingestion_status": "completed", "handoff_status": "handed_off",
              "core_receipt": observed}
    if not dry_run:
        store.append(result)
        store.event(result_id, "core_completed", core_run_id=run_id)
    return result
