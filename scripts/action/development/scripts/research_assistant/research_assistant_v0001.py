# ============================================================
# 文件名: research_assistant_v0001.py
# 中文名: 研究助手结构化工具入口
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / interface
# 脚本定位: Agent 和人工交接共同使用的本地 CLI
#
# 职责说明:
# - 将显式操作分派到研究任务账本
#
# 本脚本做什么:
# - 创建项目任务、开始执行、提交阶段成果、精确恢复与查询状态
#
# 本脚本不做什么:
# - 不直连模型、不自动续跑、不替代研究分析
#
# 制度边界声明:
# - 配置和请求路径限于本工程；只有显式操作写运行库
# - 交付包校验后受理和归档，失败不报告完成；dry-run 不写库
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_assistant_v0001
# family: research_assistant
# role: research_tool_interface
# version: v0001
# status: active
# entry_point: scripts/action/development/scripts/research_assistant/research_assistant_v0001.py
# input:
#   - versioned configuration and operation JSON
# output:
#   - structured task, run, revision or delivery result
# depends_on:
#   - research_ledger_v0001
#   - research_ai_v0001
#   - research_retrieval_v0001
#   - research_ingestion_v0001
#   - research_index_v0001
#   - action_ai_controller_v0002
# used_by:
#   - research_ui_v0001
#   - research_continuity_v0001
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from pathlib import Path

if __package__:
    from .research_ledger_v0001 import Ledger, ResearchError, canonical, read_package, require, scoped_path, validate_executor
else:
    from research_ledger_v0001 import Ledger, ResearchError, canonical, read_package, require, scoped_path, validate_executor


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_assistant"
SCRIPT_NAME = "research_assistant_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def project_root():
    for path in Path(__file__).resolve().parents:
        if (path / "provenance/source_manifest.json").is_file():
            return path
    raise ResearchError("PROJECT_ROOT_UNRESOLVED")


def read_json(path):
    require(path.is_file() and path.stat().st_size <= 2097152, "INPUT_MISSING_OR_TOO_LARGE")
    return json.loads(path.read_text(encoding=DEFAULT_ENCODING))


# ============================================================
# 默认映射
# ============================================================

OPERATIONS = ("initialize", "describe", "create_project", "create_task", "find_projects", "find_tasks", "resume_task", "begin_run", "submit_delivery", "retry_archive", "get_delivery_status", "search_evidence", "read_evidence", "execute_strategy", "prepare_ingestion", "index_revision", "ai_models", "ai_start", "ai_pause", "generate_ai_proposal", "list_proposals")


# ============================================================
# 核心业务组件
# ============================================================

class ResearchAssistant:
    def __init__(self, root, config, *, ai_client=None):
        self.root = Path(root).resolve()
        required = {"schema_version", "ledger", "inbox", "publication_policy", "source_ids"}
        require(required <= set(config) <= required | {"query_sources", "ai"}
                and config["schema_version"] == "1.0" and config["publication_policy"] == "authority_first_explicit", "CONFIG_INVALID")
        self.ledger = Ledger(scoped_path(self.root, config["ledger"]))
        self.inbox = scoped_path(self.root, config["inbox"])
        self.source_ids = config["source_ids"]
        self.query_sources = config.get("query_sources", {})
        self.ai_config = config.get("ai")
        self.ai_client, self.ai_controller = ai_client, None
        self.ai_lock = threading.Lock()
        self.last_ai_selection = None

    def close(self):
        if self.ai_controller is not None:
            self.ai_controller.close()

    def _client(self):
        if self.ai_client is None:
            from scripts.action.infrastructures.action_ai_controller_v0002 import AIController
            spec = self.ai_config
            require(isinstance(spec, dict) and set(spec) == {"controller_config", "app_id", "token_environment", "providers"}, "AI_NOT_CONFIGURED")
            token = os.environ.get(spec["token_environment"], "")
            require(bool(token), "APPLICATION_TOKEN_MISSING")
            self.ai_controller = AIController(scoped_path(self.root, spec["controller_config"]), root=self.root)
            self.ai_client = self.ai_controller.client(spec["app_id"], token)
        return self.ai_client

    def _ai_execute(self, operation, data, dry_run):
        from scripts.action.infrastructures.action_ai_controller_v0002 import ControlError
        require(not dry_run, "AI_USE_READ_ONLY_MODEL_LIST")
        if operation == "ai_pause":
            try:
                return {**self._client().pause(), "application_running": False, "inflight_cancellation_confirmed": False}
            except ControlError as exc:
                raise ResearchError(str(exc)) from None
        require(self.ai_lock.acquire(blocking=False), "AI_OPERATION_BUSY")
        try:
            client = self._client()
            if operation == "ai_models":
                return client.models(data["provider"])
            if operation == "ai_start":
                client.select(data["provider"], data["model"], data.get("profile", "default"))
                result = client.start()
                self.last_ai_selection = dict(data)
                return {**result, "selection": data, "application_running": True, "permission_granted_by_this_operation": False}
            from research_ai_v0001 import analyze_stage, prepare_proposal
            from research_retrieval_v0001 import ResearchRetrieval
            from research_ledger_v0001 import digest
            snapshot = self.ledger.resume(data["task_id"])
            require(all(e["source_id"] in self.source_ids for e in snapshot["task"]["evidence"]), "SOURCE_REVOKED_IN_SNAPSHOT")
            task = {**snapshot["task"], "source_ids": [s for s in snapshot["task"]["source_ids"] if s in self.source_ids]}
            require(isinstance(data["object_ids"], list) and 1 <= len(data["object_ids"]) <= 20, "EVIDENCE_SELECTION_REQUIRED")
            source = data["source_id"]
            retrieval = ResearchRetrieval(self.root, self.query_sources)
            evidence, references = [], []
            for object_id in dict.fromkeys(data["object_ids"]):
                result = retrieval.query(task, source, object_id=object_id, limit=2)
                require(result["status"] == "completed" and len(result["rows"]) == 1, "EVIDENCE_UNAVAILABLE")
                row = result["rows"][0]
                text = row[self.query_sources[source]["evidence_mapping"]["text_column"]]
                require(row.get("origin") in {"original", "synthetic_original", "generated"}, "EVIDENCE_ORIGIN_REQUIRED")
                evidence.append({"source_id": source, "object_id": object_id, "text": text, "origin": row["origin"],
                                 "author": row.get("author"), "event_time": row.get("event_time"), "source_hash": digest(text.encode(DEFAULT_ENCODING))})
                references.append({"source_id": source, "object_id": object_id, "source_hash": digest(text.encode(DEFAULT_ENCODING)),
                                   "origin": row["origin"], "locator": {"author": row.get("author"), "event_time": row.get("event_time")},
                                   "claim": "Selected context for stage proposal", "relation": "context"})
            proposal = analyze_stage(client, snapshot, evidence, max_tokens=data.get("max_tokens", 4096))
            return prepare_proposal(self.ledger, self.inbox, snapshot, proposal, references)
        except ControlError as exc:
            raise ResearchError(str(exc)) from None
        finally:
            self.ai_lock.release()

    def execute(self, operation, data, dry_run=False):
        require(operation in OPERATIONS and isinstance(data, dict), "OPERATION_INVALID")
        if operation in {"ai_models", "ai_start", "ai_pause", "generate_ai_proposal"}:
            return self._ai_execute(operation, data, dry_run)
        if operation == "list_proposals":
            snapshot = self.ledger.resume(data["task_id"])
            delivery_states = {self.ledger.delivery_status(d["id"])["run_id"]: d["status"] for d in snapshot["deliveries"]}
            proposals = []
            unreadable = []
            if self.inbox.is_dir():
                folders = sorted(self.inbox.glob("proposal_*"), key=lambda p: p.name)
                for folder in folders:
                    if folder.is_symlink() or not (folder / "READY").is_file():
                        continue
                    try:
                        envelope, artifacts, raw = read_package(self.inbox, folder.name)
                    except (ResearchError, ValueError, OSError) as exc:
                        unreadable.append({"package": folder.name, "error_type": str(exc) if isinstance(exc, ResearchError) else type(exc).__name__})
                        continue
                    if envelope["task_id"] == data["task_id"]:
                        proposals.append({"package": folder.name, "base_revision": envelope["base_revision"],
                                          "status": delivery_states.get(envelope["run_id"], "awaiting_review"),
                                          "report": next((a["content"] for a in artifacts if a["path"] == "report.md"), "")})
            return {"task_id": data["task_id"], "proposals": proposals, "unreadable_packages": unreadable}
        if operation == "describe":
            return {"status": "completed", "operations": list(OPERATIONS), "source_ids": self.source_ids,
                    "switch_boundary": "stage", "executor_kinds": ["agent", "ai"],
                    "publication_policy": "authority_first_explicit", "index_targets": ["sqlite", "duckdb"],
                    "model_execution": "controller_proposal_then_explicit_archive", "retrieval_integration": "independent_executor",
                    "query_sources": sorted(self.query_sources)}
        if operation == "resume_task":
            return self.ledger.resume(**data)
        if operation == "find_tasks":
            return self.ledger.find_tasks(**data)
        if operation == "find_projects":
            return self.ledger.find_projects()
        if operation == "get_delivery_status":
            return self.ledger.delivery_status(**data)
        if operation in {"search_evidence", "read_evidence", "execute_strategy"}:
            from research_retrieval_v0001 import ResearchRetrieval
            task = self.ledger.resume(data["task_id"])["task"]
            task = {**task, "source_ids": [s for s in task["source_ids"] if s in self.source_ids]}
            retrieval = ResearchRetrieval(self.root, self.query_sources)
            if operation == "execute_strategy":
                require(not dry_run, "STRATEGY_DRY_RUN_NOT_SUPPORTED")
                return retrieval.strategy(task, data["strategy"])
            args = {key: value for key, value in data.items() if key != "task_id"}
            return retrieval.query(task, **args)
        if operation == "prepare_ingestion":
            from research_ingestion_v0001 import prepare_ingestion
            return prepare_ingestion(self.root, self.ledger.resume(**data)["task"], dry_run)
        if operation == "index_revision":
            from research_index_v0001 import index_revision
            require(not dry_run, "USE_PREPARE_INGESTION_FOR_DRY_RUN")
            return index_revision(self.root, self.ledger, **data)
        if operation == "submit_delivery":
            require(set(data) == {"package"}, "SUBMISSION_CONTRACT")
            envelope, artifacts, raw = read_package(self.inbox, data["package"])
            require(all(e["source_id"] in self.source_ids for e in envelope["evidence"]), "SOURCE_REVOKED")
            require(all(c["source_id"] in self.source_ids for c in envelope["coverage"]), "SOURCE_REVOKED")
            if self.query_sources:
                from research_retrieval_v0001 import ResearchRetrieval
                task = self.ledger.resume(envelope["task_id"])["task"]
                ResearchRetrieval(self.root, self.query_sources).verify(task, envelope["evidence"])
            if dry_run:
                current = self.ledger.resume(envelope["task_id"])
                require(current["current_revision"] == envelope["base_revision"], "REVISION_CONFLICT")
                return {"status": "dry_run", "files_validated": len(artifacts)}
            accepted = self.ledger.accept(envelope, artifacts, raw)
            return self.ledger.archive(accepted["delivery_id"])
        if operation == "begin_run":
            validate_executor(data["executor"])
        if operation == "create_task":
            require(all(s in self.source_ids for s in data["source_ids"]), "SOURCE_NOT_AUTHORIZED")
        if dry_run:
            # Read-only inspection is supported; mutation dry-run must not pretend
            # to have validated constraints only the transaction can decide.
            raise ResearchError("DRY_RUN_SUPPORTED_FOR_SUBMISSION_ONLY")
        if operation == "initialize":
            return self.ledger.initialize()
        if operation == "retry_archive":
            return self.ledger.archive(**data)
        return getattr(self.ledger, operation)(**data)


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/research_assistant_v0001.json")
    parser.add_argument("--input")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("operation", choices=OPERATIONS)
    return parser


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding=DEFAULT_ENCODING)
    args = _build_parser().parse_args()
    app = None
    try:
        root = project_root()
        sys.path.insert(0, str(root))
        app = ResearchAssistant(root, read_json(scoped_path(root, args.config)))
        data = read_json(scoped_path(root, args.input)) if args.input else {}
        result = app.execute(args.operation, data, args.dry_run)
        print(canonical(result))
        return 0
    except (ResearchError, ValueError, KeyError, TypeError, OSError) as exc:
        print(canonical({"status": "error", "error_type": str(exc) if isinstance(exc, ResearchError) else type(exc).__name__}))
        return 2
    except Exception:
        print(canonical({"status": "error", "error_type": "UNEXPECTED_ERROR"}))
        return 3
    finally:
        if app is not None:
            app.close()


if __name__ == "__main__":
    raise SystemExit(main())
