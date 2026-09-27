# ============================================================
# 文件名: research_ingestion_v0001.py
# 中文名: 研究成果规范接入适配
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / ingestion
# 脚本定位: 已发布研究修订到现有统一资料入口的显式适配
#
# 职责说明:
# - 保留生成来源、成果身份和证据关系并准备规范输入
#
# 本脚本做什么:
# - 将阶段成果转换为规范消息，调用独立 ingestion 进行收件和交接
#
# 本脚本不做什么:
# - 不把生成内容标为用户原文，不自动运行主数据链或 AI
#
# 制度边界声明:
# - 规范文件追加到 runtime，原件快照追加到 actioning/ingestion；已存在字节必须一致
# - 规范准备不等于核心写入或索引成功，返回下游真实状态
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_ingestion_v0001
# family: research_ingestion
# role: research_artifact_ingress_adapter
# version: v0001
# status: archived
# entry_point: staging/scripts/action/development/scripts/research_assistant/research_ingestion_v0001.py
# input:
#   - immutable published research revision
# output:
#   - canonical generated artifact package and intake receipt
# depends_on:
#   - research_ledger_v0001
#   - unified_ingestion_pipeline_v0002
#   - jsonschema
# used_by:
#   - research_assistant_v0001
# ============================================================

from __future__ import annotations

import json
from pathlib import Path

from research_ledger_v0001 import canonical, digest, require, scoped_path


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_ingestion"
SCRIPT_NAME = "research_ingestion_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def source_reference(task, value, path, source_id):
    return {"source_file_id": task["delivery_id"], "source_path": path, "source_index": None,
            "source_id": source_id, "source_value_sha256": digest(value),
            "mapping_method": "direct", "loss_status": "lossless"}


# ============================================================
# 核心业务组件
# ============================================================

def to_canonical(task):
    require(task.get("revision", 0) > 0 and task.get("archive_status") == "completed", "PUBLISHED_REVISION_REQUIRED")
    require(task["artifacts"], "NO_ARTIFACTS_TO_INGEST")
    nodes = []
    for index, artifact in enumerate(task["artifacts"]):
        require(digest(artifact["content"].encode(DEFAULT_ENCODING)) == artifact["sha256"], "STORED_ARTIFACT_CORRUPT")
        aid = task["delivery_id"] + "_" + artifact["artifact_id"]
        ref = source_reference(task, artifact["content"], f"$.artifacts[{index}].content", artifact["artifact_id"])
        time_ref = source_reference(task, artifact["generated_at"], f"$.artifacts[{index}].generated_at", artifact["artifact_id"])
        timestamp = {"event_kind": "artifact_generated", "raw_value": artifact["generated_at"], "raw_type": "iso8601",
                     "normalized_utc": artifact["generated_at"].replace("+00:00", "Z"), "semantic_status": "unverified",
                     "semantic_basis": "Executor supplied artifact creation time; not original evidence event time.", "source_ref": time_ref}
        origin = {"origin": "generated", "project_id": task["project_id"], "task_id": task["task_id"],
                  "delivery_id": task["delivery_id"], "artifact_id": artifact["artifact_id"], "artifact_sha256": artifact["sha256"],
                  "executor": task["executor"], "evidence": task["evidence"], "adapter_version": SCRIPT_VERSION}
        message = {"message_id": aid, "author": {"role": "assistant", "name": task["executor"]["name"],
                    "source_role": "generated_research_artifact", "metadata": {"executor": task["executor"]}},
                   "content": {"content_type": "text", "source_content_type": "research_artifact", "parts": [
                       {"part_id": aid + "_text", "part_index": 0, "part_type": "text", "text": artifact["content"],
                        "mime_type": "text/plain", "uri": None, "payload": None, "source_ref": ref, "extensions": origin}],
                       "metadata": {}, "extensions": {}}, "status": "finished", "recipient": None, "channel": None,
                   "event_times": [timestamp], "metadata": origin, "source_ref": ref, "extensions": {}}
        nodes.append({"node_id": aid, "parent_node_id": None, "child_node_ids": [], "node_order": index,
                      "message": message, "source_ref": ref, "extensions": {"independent_artifact": True}})
    ref = source_reference(task, task, "$", task["task_id"])
    return {"schema_version": "canonical_conversation_ingress_v0001", "dataset_id": task["delivery_id"],
            "source_envelope_id": task["delivery_id"], "canonicalized_at": task["published_at"].replace("+00:00", "Z"),
            "contract_version": "v0001", "conversations": [{"conversation_id": task["delivery_id"], "title": task["title"],
                "created_time": None, "updated_time": None, "current_node_id": nodes[-1]["node_id"], "nodes": nodes,
                "metadata": {"origin": "generated", "research_revision": task["revision"]}, "source_ref": ref,
                "extensions": {"handoff": task["handoff"], "coverage": task["coverage"]}}],
            "extensions": {"origin": "generated", "adapter": SCRIPT_NAME, "source_revision": task}}


def prepare_ingestion(root, task, dry_run=False):
    import jsonschema
    from scripts.orchestration.action.unified_ingestion_pipeline_v0002 import Pipeline
    from scripts.action.tools.ingestion.ingestion_store_v0001 import Config, publish

    root = Path(root).resolve()
    payload = to_canonical(task)
    schema = json.loads(scoped_path(root, "config/contracts/canonical_conversation_ingress_v0001.schema.json").read_text(encoding=DEFAULT_ENCODING))
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(payload)
    if dry_run:
        return {"status": "dry_run", "canonical_validated": True, "core_completed": False}
    target = scoped_path(root, "runtime/research_ingestion/" + task["delivery_id"] + "/canonical.json")
    publish(target, canonical(payload).encode(DEFAULT_ENCODING))
    config = Config(root, root / "actioning/ingestion/research", "research-assistant", 2097152, 12, 30)
    result = Pipeline(config).process(target)
    return {"status": result["processing_status"], "delivery_id": task["delivery_id"], "intake": result,
            "core_completed": False, "index_status": "pending", "origin": "generated"}
