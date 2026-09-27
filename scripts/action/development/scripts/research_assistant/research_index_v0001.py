# ============================================================
# 文件名: research_index_v0001.py
# 中文名: 研究成果显式索引编排
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / index
# 脚本定位: 已发布成果调用复制后的 v0019 数据链并核验回执
#
# 职责说明:
# - 分离权威成果归档与 SQL/DuckDB 索引操作
#
# 本脚本做什么:
# - 显式准备索引、记录操作状态并核验真实核心回执
#
# 本脚本不做什么:
# - 不调用模型，不生成向量，不连接 Neo4j，不无限重试
#
# 制度边界声明:
# - 仅写本工程持久运行目录及主链允许的派生目录；索引事件追加保存
# - 在途或中断状态拒绝自动重试，先人工核实；保留全部中间产物
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_index_v0001
# family: research_index
# role: research_explicit_index_orchestrator
# version: v0001
# status: active
# entry_point: scripts/action/development/scripts/research_assistant/research_index_v0001.py
# input:
#   - published task revision and explicit index request
# output:
#   - durable index events and verified ingestion receipt
# depends_on:
#   - research_ledger_v0001
#   - research_ingestion_v0001
#   - data_action_chain_pipeline_v0019
#   - ingestion_receipt_v0002
#   - ingestion_store_v0001
#   - init_action_data_sql_schema_v0001
#   - intermediate_retention_contract_v0001
# used_by:
#   - research_assistant_v0001
# ============================================================

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

import yaml

from research_ledger_v0001 import ResearchError, canonical, digest, now, require
from research_ingestion_v0001 import prepare_ingestion


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_index"
SCRIPT_NAME = "research_index_v0001"
SCRIPT_VERSION = "v0001"
INDEX_TARGET = "sqlite_duckdb"


# ============================================================
# 工具函数区
# ============================================================

def event(ledger, delivery_id, status, receipt):
    with ledger.transaction(True) as conn:
        conn.execute("INSERT INTO index_events(delivery_id,target,status,receipt,created_at) VALUES (?,?,?,?,?)",
                     (delivery_id, INDEX_TARGET, status, canonical(receipt), now()))


# ============================================================
# 核心业务组件
# ============================================================

def index_revision(root, ledger, task_id, revision=None):
    from scripts.orchestration.action import data_action_chain_pipeline_v0019 as core
    from scripts.action.tools.ingestion.ingestion_receipt_v0002 import confirm
    from scripts.action.tools.ingestion.ingestion_store_v0001 import Config

    root = Path(root).resolve()
    task = ledger.resume(task_id, revision)["task"]
    require(task.get("archive_status") == "completed", "PUBLISHED_REVISION_REQUIRED")
    delivery_id = task["delivery_id"]
    with ledger.transaction(True) as conn:
        last = conn.execute("SELECT status,receipt FROM index_events WHERE delivery_id=? ORDER BY id DESC LIMIT 1", (delivery_id,)).fetchone()
        if last and last["status"] == "succeeded":
            return {"status": "existing", "delivery_id": delivery_id, "index_receipt": json.loads(last["receipt"])}
        require(not last or last["status"] == "failed_before_execution", "INDEX_REQUIRES_STATUS_RECONCILIATION")
        conn.execute("INSERT INTO index_events(delivery_id,target,status,receipt,created_at) VALUES (?,?,?,?,?)",
                     (delivery_id, INDEX_TARGET, "processing", canonical({"phase": "preparation"}), now()))
    execution_started = False
    try:
        prepared = prepare_ingestion(root, task)
        require(prepared["status"] == "ready_for_handoff", "INGESTION_NOT_READY")
        request = prepared["intake"]["downstream_receipt"]["core_request"]
        project_key = digest(task["project_id"])[:16]
        delivery_key = digest(delivery_id)[:16]
        base = "runtime/i/" + project_key
        base_path = root / base
        base_path.mkdir(parents=True, exist_ok=True)
        writer = yaml.safe_load((root / "config/action/config/sql_writer_config_v0001.yml").read_text(encoding="utf-8-sig"))
        writer["connection"]["sqlite"]["path"] = base + "/business.sqlite3"
        writer_path = base_path / "writer.yml"
        if not writer_path.exists():
            with writer_path.open("x", encoding=DEFAULT_ENCODING) as stream:
                yaml.safe_dump(writer, stream, allow_unicode=True)
        else:
            require(yaml.safe_load(writer_path.read_text(encoding=DEFAULT_ENCODING)) == writer, "INDEX_CONFIG_CHANGED")
        if not (base_path / "business.sqlite3").exists():
            bootstrap = subprocess.run([sys.executable, "-B", str(root / "scripts/action/tools/init_action_data_sql_schema_v0001.py"),
                                        "--db-path", str(base_path / "business.sqlite3"), "--init"],
                                       cwd=root, capture_output=True, timeout=30)
            require(bootstrap.returncode == 0, "INDEX_SCHEMA_INITIALIZATION_FAILED")
        args = core.build_parser().parse_args([
            "--project-root", str(root), "--data-root", request["data_root"], "--target", request["target"],
            "--asset-version", "v0001", "--data-db", base + "/data.sqlite3", "--data-table", "data_text_units",
            "--action-db", base + "/business.sqlite3", "--sql-writer-config", base + "/writer.yml",
            "--duckdb-path", base + "/analysis.duckdb", "--vector-target", "none", "--derivation-target", "duckdb",
            "--run-id", "r" + uuid.uuid4().hex[:6], "--semantic-path", "/conversations/*/nodes/*/message/content/parts/*/text",
            "--data-intermediate-root", base + "/i", "--data-workspace-root", base + "/w",
            "--admission-target-root", "data_processed/research/" + delivery_key,
            "--registry-dir", "actioning/registry/research/" + project_key,
            "--output-root", base + "/o", "--structural-output-root", "scripts/orchestration/outputs/action",
            "--canonical-work-root", "runtime/research_canonical/" + delivery_key,
            "--structural-business-root", "actioning/pipelines/research/" + project_key,
            "--anchor-business-root", base + "/n", "--derivation-business-root", base + "/d",
            "--return-root", base + "/returns", "--vector-pipeline-root", base + "/v", "--chromadb-path", base + "/c",
            "--vector-state-index", base + "/v/index.jsonl", "--vector-replaced-archive", base + "/v/archive",
            "--confirm-execution", "--confirm-database-write", "--dry-run"])
        core.execute_with_retention(args)
        args.dry_run = False
        binding = core.retention_binding(args)
        from scripts.action.infrastructures.intermediate_retention_contract_v0001 import seal, write
        receipt_path = base_path / (args.run_id + "_keep.json")
        write(receipt_path, seal({"kind": "receipt", "binding_sha256": binding["contract_sha256"], "run_id": args.run_id,
                                 "mode": "keep", "destination": "", "confirmed_at": now(), "confirmed": True,
                                 "authorization": "explicit_research_index_operation_preserves_intermediate_artifacts"}))
        args.retention_receipt = str(receipt_path)
        execution_started = True
        event(ledger, delivery_id, "processing", {"phase": "core", "run_id": args.run_id, "base": base})
        result = core.execute_with_retention(args)
        require(result["status"] == "completed", "INDEX_CORE_NOT_COMPLETED")
        intake_config = Config(root, root / "actioning/ingestion/research", "research-assistant", 2097152, 12, 30)
        verified = confirm(intake_config, prepared["intake"]["run_id"], Path(result["completion_manifest"]))
        receipt = {"delivery_id": delivery_id, "targets": ["sqlite", "duckdb"], "vector": "not_requested", "neo4j": "not_requested",
                   "core": result, "intake": verified, "base": base}
        event(ledger, delivery_id, "succeeded", receipt)
        return {"status": "completed", "delivery_id": delivery_id, "index_receipt": receipt}
    except Exception as exc:
        status = "unknown" if execution_started else "failed_before_execution"
        code = str(exc) if isinstance(exc, ResearchError) else type(exc).__name__
        event(ledger, delivery_id, status, {"error_type": code, "automatic_retry": False})
        raise ResearchError("INDEX_" + status.upper()) from exc
