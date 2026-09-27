# ============================================================
# 文件名: retrieval_strategy_v0001.py
# 中文名: 检索策略档案与生命周期
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / strategy
# 脚本定位: 模式二执行前档案及独立执行编排
#
# 职责说明:
# - 持久化策略、追加执行事件并独立审查复用条件
#
# 本脚本做什么:
# - 先 fsync 后执行、父策略版本、拒绝/取消/超时记录、显式复用审批
#
# 本脚本不做什么:
# - 不保存隐藏思维链、不自动审批、不自动切换模式
#
# 制度边界声明:
# - 独占文件创建不覆盖；档案失败阻止查询；正文只进授权档案
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: retrieval_strategy_v0001
# family: retrieval_strategy
# role: archived_query_strategy
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/retrieval_strategy_v0001.py
# input:
#   - strategy, authorized source configuration and archive root
# output:
#   - immutable strategy events and unified evidence envelope
# depends_on:
#   - retrieval_executor_v0001
#   - experience_query_v0001
# used_by:
#   - retrieval_pipeline_v0001
# ============================================================

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

if __package__:
    from .experience_contract_v0001 import ExperienceError, bounded, canonical, digest, identifier, utcnow
    from .experience_query_v0001 import envelope
    from .experience_store_v0001 import read_connection
    from .retrieval_executor_v0001 import QueryLimits, describe_sources, execute_query
else:
    from experience_contract_v0001 import ExperienceError, bounded, canonical, digest, identifier, utcnow
    from experience_query_v0001 import envelope
    from experience_store_v0001 import read_connection
    from retrieval_executor_v0001 import QueryLimits, describe_sources, execute_query


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "retrieval_strategy"
SCRIPT_NAME = "retrieval_strategy_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def write_exclusive(path, value):
    data = canonical(value)
    with Path(path).open("x", encoding=DEFAULT_ENCODING) as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())


def policy_fingerprint(sources):
    # Includes actual paths internally; never send them to AI schema descriptions.
    states = {}
    for sid, source in sources.items():
        if source["backend"] == "sqlite":
            conn = read_connection(source["path"])
            try:
                states[sid] = [tuple(row) for row in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','view','index') ORDER BY name") if row["tbl_name"] in source["tables"]]
            finally:
                conn.close()
        elif source["backend"] == "duckdb":
            import duckdb
            conn = duckdb.connect(str(source["path"]), read_only=True, config={"enable_external_access":"false","autoload_known_extensions":"false","autoinstall_known_extensions":"false"})
            try:
                states[sid] = conn.execute("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='main' ORDER BY table_name,ordinal_position").fetchall()
            finally:
                conn.close()
        elif source["backend"] == "chroma":
            path = Path(source["path"]) / "manifest.json"
            if not path.is_file():
                raise ExperienceError("VECTOR_MANIFEST_REQUIRED")
            states[sid] = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
        else:
            states[sid] = {"live_schema_verified":False}
    return digest({"sources": sources, "schema_state": states})


# ============================================================
# 核心类
# ============================================================

class StrategyArchive:
    def __init__(self, root):
        self.root = Path(root).resolve()

    def path(self, strategy_id):
        identifier(strategy_id)
        path = self.root / strategy_id
        if path.resolve().parent != self.root or path.is_symlink():
            raise ExperienceError("ARCHIVE_PATH_DENIED")
        return path

    def read(self, strategy_id):
        with (self.path(strategy_id) / "strategy.json").open(encoding=DEFAULT_ENCODING) as stream:
            return json.load(stream)

    def event(self, strategy_id, status, **details):
        if status not in {"generated", "executing", "executed", "validated", "approved_for_reuse", "rejected", "failed", "timeout", "cancelled"}:
            raise ExperienceError("STRATEGY_STATUS_INVALID")
        event = {"event_id": uuid.uuid4().hex, "status": status, "at": utcnow(), **details}
        write_exclusive(self.path(strategy_id) / ("event_" + str(time.time_ns()) + "_" + event["event_id"] + ".json"), event)
        return event

    def events(self, strategy_id):
        return [json.loads(path.read_text(encoding=DEFAULT_ENCODING)) for path in sorted(self.path(strategy_id).glob("event_*.json"))]

    def create(self, strategy, sources, *, dry_run=False):
        bounded(strategy)
        required = {"schema_version", "strategy_id", "request_id", "task_id", "parent_strategy_id", "version", "question", "authorized_source_ids", "steps", "generator", "explanation", "applicability"}
        if not isinstance(strategy, dict) or set(strategy) != required or strategy["schema_version"] != "0.1.0":
            raise ExperienceError("STRATEGY_CONTRACT_INVALID")
        for key in ("strategy_id", "request_id", "task_id"):
            identifier(strategy[key])
        if not isinstance(strategy["question"], str) or not isinstance(strategy["explanation"], str) or len(strategy["explanation"]) > 4000:
            raise ExperienceError("STRATEGY_CONTRACT_INVALID")
        if type(strategy["version"]) is not int or not 1 <= strategy["version"] <= 8:
            raise ExperienceError("STRATEGY_VERSION_INVALID")
        parent = strategy["parent_strategy_id"]
        if parent:
            previous = self.read(parent)
            if previous["version"] + 1 != strategy["version"] or previous["request_id"] != strategy["request_id"] or previous["task_id"] != strategy["task_id"]:
                raise ExperienceError("STRATEGY_PARENT_INVALID")
        elif strategy["version"] != 1:
            raise ExperienceError("STRATEGY_PARENT_REQUIRED")
        if not isinstance(strategy["authorized_source_ids"], list) or not strategy["authorized_source_ids"]:
            raise ExperienceError("STRATEGY_SCOPE_INVALID")
        gen = strategy["generator"]
        if not isinstance(gen, dict) or set(gen) != {"kind", "model", "call_ref"} or gen["kind"] not in {"human", "model"}:
            raise ExperienceError("STRATEGY_GENERATOR_INVALID")
        if gen["kind"] == "model":
            if not isinstance(gen["call_ref"], dict) or not {"app_id", "call_id", "provider", "model", "lineage"} <= gen["call_ref"].keys() or gen["call_ref"]["lineage"] != "action" or gen["model"] != gen["call_ref"]["model"]:
                raise ExperienceError("STRATEGY_MODEL_PROVENANCE_REQUIRED")
        elif gen["model"] is not None or gen["call_ref"] is not None:
            raise ExperienceError("STRATEGY_GENERATOR_INVALID")
        if not isinstance(strategy["applicability"], dict) or set(strategy["applicability"]) != {"schema", "index", "model", "permission"}:
            raise ExperienceError("STRATEGY_APPLICABILITY_REQUIRED")
        if dry_run:
            if not isinstance(strategy["steps"], list) or not 1 <= len(strategy["steps"]) <= 8 or any(not isinstance(step,dict) or set(step) != {"source_id","query","parameters"} or step["source_id"] not in sources for step in strategy["steps"]):
                raise ExperienceError("STRATEGY_STEPS_INVALID")
            return {"status":"dry_run"}
        self.root.mkdir(parents=True, exist_ok=True)
        directory = self.path(strategy["strategy_id"])
        directory.mkdir()  # unique strategy ID: never overwrite an older plan
        saved = {**strategy, "created_at": utcnow(), "policy_fingerprint": policy_fingerprint(sources), "source_descriptions": describe_sources(sources)}
        write_exclusive(directory / "strategy.json", saved)
        self.event(strategy["strategy_id"], "generated")
        return saved

    def review(self, strategy_id, status, actor, evidence_ref, applicability, *, dry_run=False):
        identifier(actor); identifier(evidence_ref)
        if status not in {"validated", "approved_for_reuse"}:
            raise ExperienceError("INVALID_REVIEW_STATUS")
        events = self.events(strategy_id)
        required = "executed" if status == "validated" else "validated"
        if not any(e["status"] == required for e in events):
            raise ExperienceError("REVIEW_PRECONDITION_FAILED")
        if applicability != self.read(strategy_id)["applicability"]:
            raise ExperienceError("REUSE_CONDITIONS_CHANGED")
        if dry_run:
            return {"status":"dry_run"}
        return self.event(strategy_id, status, actor=actor, evidence_ref=evidence_ref, applicability=applicability)

    def check_reuse(self, strategy_id, sources, applicability):
        if any(source["backend"] == "neo4j" for source in sources.values()):
            raise ExperienceError("NEO4J_LIVE_REUSE_CONDITIONS_UNVERIFIED")
        old = self.read(strategy_id)
        if not any(e["status"] == "approved_for_reuse" for e in self.events(strategy_id)):
            raise ExperienceError("REUSE_NOT_APPROVED")
        if old["policy_fingerprint"] != policy_fingerprint(sources) or old["applicability"] != applicability:
            raise ExperienceError("REUSE_CONDITIONS_CHANGED")
        return {"status": "eligible_for_explicit_replay", "result_equivalence_guaranteed": False}


def execute_strategy(archive, strategy_id, sources, *, limits=QueryLimits(), cancel=None):
    saved = archive.read(strategy_id)
    started = time.monotonic()
    result = envelope(saved["request_id"], "strategy", {"authorized_source_ids": saved["authorized_source_ids"], "policy_fingerprint": saved["policy_fingerprint"]})
    result["strategy_id"] = strategy_id
    # Durable claim prevents duplicate concurrent execution, including recovery after unknown outcomes.
    write_exclusive(archive.path(strategy_id) / "execution_claim.json", {"at": utcnow()})
    archive.event(strategy_id, "executing")
    try:
        if saved["policy_fingerprint"] != policy_fingerprint(sources):
            raise ExperienceError("AUTHORIZATION_CHANGED")
        if any(sid not in sources for sid in saved["authorized_source_ids"]):
            raise ExperienceError("SOURCE_NOT_AUTHORIZED")
        steps = saved["steps"]
        if not isinstance(steps, list) or not 1 <= len(steps) <= 8:
            raise ExperienceError("STRATEGY_STEP_LIMIT")
        for index, step in enumerate(steps):
            if not isinstance(step, dict) or set(step) != {"source_id", "query", "parameters"} or step["source_id"] not in saved["authorized_source_ids"]:
                raise ExperienceError("SOURCE_NOT_AUTHORIZED")
            elapsed = (time.monotonic() - started) * 1000
            remaining = limits.timeout_ms - int(elapsed)
            if remaining <= 0:
                result["status"] = "timeout"; result["errors"].append({"error_type": "STRATEGY_TIMEOUT"}); break
            output = execute_query(sources[step["source_id"]], step["query"], step["parameters"], QueryLimits(remaining, limits.max_rows, limits.max_bytes), cancel)
            if output["status"] != "completed":
                result["status"] = output["status"]
                result["errors"].append({"step": index, "error_type": output["error_type"]})
                break
            for rank, row in enumerate(output["rows"]):
                hit = {"source_id": step["source_id"], "lineage": "action", "object_id": row.get("annotation_id", row.get("object_id")), "object_kind": "query_row", "query_ref": {"strategy_id": strategy_id, "step": index, "rank": rank}, "row": row, "source_refs": [], "source_verified": False, "match_reason": "generated_query", "ranking_source": "query_order", "context_owner": None}
                evidence = output.get("evidence", [])
                if rank < len(evidence) and evidence[rank]:
                    hit["source_refs"] = [evidence[rank]]
                    hit["object_id"] = evidence[rank]["source_ref"]["object_id"]
                    hit["source_verified"] = True
                    hit["object_kind"] = "source_backed_query_row"
                    hit["context_owner"] = evidence[rank].get("context_owner")
                if len(result["hits"]) >= limits.max_rows or len(canonical(result).encode(DEFAULT_ENCODING)) + len(canonical(hit).encode(DEFAULT_ENCODING)) > limits.max_bytes:
                    result["truncated"] = True
                    break
                result["hits"].append(hit)
            result["truncated"] |= output["truncated"]
            if result["truncated"]:
                break
    except ExperienceError as exc:
        result["status"] = "rejected"; result["errors"].append({"error_type": str(exc)})
    except KeyboardInterrupt:
        result["status"] = "cancelled"; result["errors"].append({"error_type": "QUERY_CANCELLED"})
    except Exception:
        result["status"] = "failed"; result["errors"].append({"error_type": "STRATEGY_EXECUTION_FAILED"})
    result["timings"]["total_ms"] = (time.monotonic()-started)*1000
    write_exclusive(archive.path(strategy_id) / "result.json", result)
    archive.event(strategy_id, "executed" if result["status"] == "completed" else result["status"], elapsed_ms=result["timings"]["total_ms"], result_ref="result.json", result_hash=digest(result))
    return result
