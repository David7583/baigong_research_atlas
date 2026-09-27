# ============================================================
# 文件名: ingestion_handoff_v0001.py
# 中文名: 现有规范入口交接准备
# 版本号: v0001
#
# 主层级: action
# 层级: tools / ingestion / ingestion_handoff_gate
# 脚本定位: 现有规范入口交接准备的独立执行边界
#
# 职责说明:
# - 现有规范入口交接准备，提供明确输入输出
#
# 本脚本做什么:
# - 执行本模块声明的接入操作并返回真实状态
#
# 本脚本不做什么:
# - 不执行未知代码，不调用收费模型，不写核心数据库
#
# 制度边界声明:
# - 输入原件只读；运行产物仅写显式受管目录，追加发布，不覆盖已有证据
# - 失败明确返回；导入无写入副作用；测试仅使用 temp 下隔离数据
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingestion_handoff_v0001
# family: ingestion_handoff
# role: ingestion_handoff_gate
# version: v0001
# status: active
# entry_point: scripts/action/tools/ingestion/ingestion_handoff_v0001.py
# input:
#   - explicit paths and versioned ingestion configuration
# output:
#   - validated results with provenance and classified errors
# depends_on:
#   - ingestion_store_v0001
#   - canonical_ingress_gateway_v0003
#   - Python stdlib
# used_by:
#   - unified_ingestion_pipeline_v0001
# ============================================================

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

from scripts.action.tools.ingestion.ingestion_store_v0001 import (
    IngestionError, digest, no_links, publish, publish_json, sha,
)

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "ingestion_handoff"
SCRIPT_NAME = "ingestion_handoff_v0001"
SCRIPT_VERSION = "v0001"
GATEWAY = "scripts/adapters/ingress/canonical_ingress_gateway_v0003.py"
REGISTRY = "config/contracts/format_fingerprint_registry_v0003.json"


# ============================================================
# 工具函数区
# ============================================================

def dependencies(root):
    paths = [root / REGISTRY, root / "scripts/json_stream_v0001.py"]
    paths.extend(sorted((root / "scripts/adapters/ingress").rglob("*.py")))
    return {p.relative_to(root).as_posix(): sha(p) for p in paths}


def verify_receipt(receipt, root):
    for item in receipt["artifacts"]:
        path = no_links(Path(item["path"]))
        if not path.resolve().is_relative_to(root.resolve()) or sha(path) != item["sha256"]:
            raise IngestionError("handoff_artifact_corrupted")


# ============================================================
# 核心业务组件
# ============================================================

def prepare(config, snapshot, extraction, quality, dry_run=False):
    """Prepare existing gateway input only; never claim core ingestion."""
    waiting = {"handoff_status": "awaiting_core_contract",
               "body_ingestion_status": "not_ingested", "downstream_receipt": None}
    if quality["status"] != "passed":
        return {**waiting, "handoff_status": "blocked_quality"}
    if extraction["media_type"] != "application/json":
        return waiting
    payload = json.loads(extraction["source_text"])
    if isinstance(payload, dict) and payload.get("schema_version") != "canonical_conversation_ingress_v0001":
        return waiting
    if not isinstance(payload, (list, dict)):
        return waiting
    key = digest([snapshot["source_hash"], extraction["adapter_id"],
                  extraction["adapter_version"], quality["policy_version"], dependencies(config.root)])
    cache = config.state_root / "handoffs" / key[:20] / "receipt.json"
    if not dry_run and cache.is_file():
        receipt = json.loads(cache.read_text(encoding=DEFAULT_ENCODING))
        if receipt.get("idempotency_key") != key:
            raise IngestionError("handoff_cache_key_collision")
        verify_receipt(receipt, config.state_root)
        return {**waiting, "handoff_status": "ready_for_handoff", "downstream_receipt": receipt,
                "reused_handoff": True}
    attempt = cache.parent / ("a_" + uuid.uuid4().hex[:12])
    args = [sys.executable, str(config.root / GATEWAY), "--input", snapshot["managed_path"],
            "--output-dir", str(attempt), "--run-id", "ingestion_" + key[:24]]
    if dry_run:
        args.append("--dry-run")
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONDONTWRITEBYTECODE": "1"}
    try:
        run = subprocess.run(args, cwd=config.root, capture_output=True, text=True,
                             encoding=DEFAULT_ENCODING, timeout=config.gateway_timeout, env=env)
    except subprocess.TimeoutExpired:
        return {**waiting, "handoff_status": "failed", "error_type": "gateway_timeout"}
    try:
        reply = json.loads(run.stdout)
    except ValueError:
        return {**waiting, "handoff_status": "failed", "error_type": "invalid_gateway_reply"}
    if run.returncode != 0:
        return {**waiting, "gateway_error": reply,
                "handoff_status": "awaiting_core_contract" if reply.get("error_type") == "FormatDetectionError" else "failed"}
    if dry_run:
        return {**waiting, "handoff_status": "dry_run", "gateway_validation": reply}
    if reply.get("status") != "completed" or not reply.get("raw_unchanged"):
        raise IngestionError("gateway_receipt_not_complete")
    core_root = config.state_root / "exports"
    core_target = "data_raw/" + snapshot["asset_id"] + ".json"
    core_source = core_root / core_target
    original = no_links(Path(snapshot["managed_path"]))
    if sha(original) != snapshot["source_hash"]:
        raise IngestionError("source_changed_before_handoff")
    publish(core_source, original.read_bytes())
    if sha(core_source) != snapshot["source_hash"]:
        raise IngestionError("handoff_source_hash_mismatch")
    artifacts = []
    for key_name in ("canonical_path", "envelope_path", "validation_report", "lineage_index"):
        path = no_links(Path(reply[key_name]))
        if not path.resolve().is_relative_to(attempt):
            raise IngestionError("gateway_artifact_outside_attempt")
        artifacts.append({"path": str(path), "sha256": sha(path)})
    artifacts.append({"path": str(core_source), "sha256": snapshot["source_hash"]})
    receipt = {"stage": "canonical_preparation_only", "core_completed": False,
               "idempotency_key": key, "source_hash": snapshot["source_hash"],
               "asset_id": snapshot["asset_id"], "gateway": reply, "artifacts": artifacts,
               "core_request": {"data_root": str(core_root), "target": core_target,
                                "source_hash": snapshot["source_hash"],
                                "pipeline": "data_action_chain_pipeline_v0018",
                                "automatic_execution": False}}
    verify_receipt(receipt, config.state_root)
    try:
        publish_json(cache, receipt)
    except IngestionError:
        if not cache.is_file():
            raise
        receipt = json.loads(cache.read_text(encoding=DEFAULT_ENCODING))
        verify_receipt(receipt, config.state_root)
        if receipt.get("idempotency_key") != key:
            raise IngestionError("handoff_cache_key_collision")
    return {**waiting, "handoff_status": "ready_for_handoff", "downstream_receipt": receipt}
