# ============================================================
# 文件名: experience_contract_v0001.py
# 中文名: 经验标注契约校验
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / contract
# 脚本定位: 无副作用标注与查询边界
#
# 职责说明:
# - 校验经验标注类型、来源、未知状态与时间
#
# 本脚本做什么:
# - 返回规范副本或稳定错误码，显式适配旧上下文
#
# 本脚本不做什么:
# - 不写库、不调用模型、不推断缺失事实
#
# 制度边界声明:
# - 原值保留；模型身份不构成决策权限
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: experience_contract_v0001
# family: experience_contract
# role: experience_validator
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/experience_contract_v0001.py
# input:
#   - annotation candidate
# output:
#   - validated annotation or stable error
# depends_on:
#   - Python stdlib
#   - jsonschema
# used_by:
#   - experience_store_v0001
#   - experience_query_v0001
#   - experience_episode_v0001
#   - retrieval_executor_v0001
#   - retrieval_strategy_v0001
#   - retrieval_ai_v0001
#   - experience_projection_v0001
#   - retrieval_pipeline_v0001
# ============================================================

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TypedDict

from jsonschema import Draft202012Validator


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "experience_contract"
SCRIPT_NAME = "experience_contract_v0001"
SCRIPT_VERSION = "v0001"
SCHEMA_VERSION = "0.1.0"


# ============================================================
# 异常类型
# ============================================================

class ExperienceError(ValueError):
    """Only stable codes; never embed input text or credentials."""


# ============================================================
# 数据结构
# ============================================================

class Annotation(TypedDict):
    schema_version: str
    lineage: str
    anchor_id: str
    payload: dict
    provenance: dict
    field_status: dict
    extensions: dict


# ============================================================
# 工具函数区
# ============================================================

def canonical(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        raise ExperienceError("VALIDATION_ERROR_JSON") from None


def digest(value):
    return hashlib.sha256(canonical(value).encode(DEFAULT_ENCODING)).hexdigest()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    if not isinstance(value, str):
        raise ExperienceError("VALIDATION_ERROR_TIME")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError()
        return result.astimezone(timezone.utc).isoformat()
    except ValueError:
        raise ExperienceError("VALIDATION_ERROR_TIME") from None


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value):
        raise ExperienceError("VALIDATION_ERROR_ID")
    return value


def bounded(value, max_bytes=131072, max_depth=12):
    if len(canonical(value).encode(DEFAULT_ENCODING)) > max_bytes:
        raise ExperienceError("REQUEST_TOO_LARGE")
    def walk(node, depth):
        if depth > max_depth:
            raise ExperienceError("REQUEST_TOO_DEEP")
        if isinstance(node, dict):
            for key, child in node.items():
                if re.sub(r"[^a-z]", "", key.lower()) in {"apikey", "token", "password", "authorization", "credential", "secret", "chainofthought"}:
                    raise ExperienceError("SENSITIVE_FIELD_DENIED")
                walk(child, depth + 1)
        elif isinstance(node, list):
            for child in node:
                walk(child, depth + 1)
    walk(value, 0)


@lru_cache(maxsize=1)
def _schema_validator():
    schema = json.loads(Path(__file__).with_name("experience_annotation_v0001.schema.json").read_text(encoding=DEFAULT_ENCODING))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


# ============================================================
# 默认映射
# ============================================================

PAYLOAD_FIELDS = {
    "context_5w1h": {"who", "what", "when", "where", "why", "how"},
    "state_before": {"system_version", "module_state", "known_constraints", "active_config", "relevant_files", "known_issues"},
    "state_after": {"system_version", "module_state", "known_constraints", "active_config", "relevant_files", "known_issues"},
    "action": {"type", "name", "parameters", "executor_ref", "execution_ref", "occurred_at", "source_ref_ids"},
    "result": {"status", "artifact_refs", "test_refs", "data_delta", "performance_delta", "errors", "source_ref_ids"},
    "evaluation": {"items"},
    "decision": {"value", "evaluation_refs", "decided_by", "decided_at", "rationale", "scope", "conditions", "authority_kind"},
    "transition": {"state_before_refs", "action_refs", "state_after_refs", "inputs", "executor", "parameters", "outputs", "exceptions", "side_effects", "dependencies", "source_ref_ids", "interpretation_kind"},
}
ACTIONS = {"modify_code", "add_adapter", "change_rule", "query_rewrite", "vector_search", "graph_expand", "run_test", "reject_proposal", "architecture_change"}
DECISIONS = {"ACCEPT", "REJECT", "ROLLBACK", "DEFER", "ITERATE", "REPLACE", "MERGE"}
RESULTS = {"success", "partial_success", "failure", "no_effect", "unexpected_effect"}
EVALUATIONS = {"correctness", "performance", "cost", "latency", "robustness", "traceability", "maintainability", "relevance", "coverage", "engineering_feasibility"}


# ============================================================
# 核心业务组件
# ============================================================

def validate_annotation(candidate, *, action_types=None, evaluation_types=None):
    bounded(candidate)
    c = copy.deepcopy(candidate)
    if not isinstance(c, dict) or set(c) != {"schema_version", "lineage", "anchor_id", "payload", "provenance", "field_status", "extensions"}:
        raise ExperienceError("VALIDATION_ERROR_ENVELOPE")
    if c["schema_version"] != SCHEMA_VERSION:
        raise ExperienceError("SCHEMA_UNSUPPORTED")
    if c["lineage"] != "action":
        raise ExperienceError("LINEAGE_MISMATCH")
    if next(_schema_validator().iter_errors(c), None) is not None:
        raise ExperienceError("VALIDATION_ERROR_PAYLOAD_SCHEMA")
    identifier(c["anchor_id"])
    p = c["provenance"]
    required = {"annotation_id", "annotation_type", "annotation_version", "created_at", "created_by", "actor_id", "method_version", "model_name", "confidence", "source_refs", "is_retrospective", "supersedes_annotation_id", "ai_call_refs"}
    if not isinstance(p, dict) or set(p) != required or p.get("annotation_type") not in PAYLOAD_FIELDS:
        raise ExperienceError("VALIDATION_ERROR_PROVENANCE")
    for field in ("annotation_id", "annotation_version", "actor_id", "method_version"):
        identifier(p[field])
    p["created_at"] = timestamp(p["created_at"])
    if p["created_by"] not in {"human", "model", "rule", "hybrid"} or type(p["is_retrospective"]) is not bool:
        raise ExperienceError("VALIDATION_ERROR_PROVENANCE")
    if p["confidence"] is not None and (type(p["confidence"]) not in (float, int) or not math.isfinite(p["confidence"]) or not 0 <= p["confidence"] <= 1):
        raise ExperienceError("VALIDATION_ERROR_CONFIDENCE")
    if not isinstance(p["ai_call_refs"], list):
        raise ExperienceError("VALIDATION_ERROR_AI_REFS")
    if p["created_by"] in {"model", "hybrid"}:
        if not p["model_name"] or not p["ai_call_refs"]:
            raise ExperienceError("AI_PROVENANCE_REQUIRED")
        for ref in p["ai_call_refs"]:
            if not isinstance(ref, dict) or not {"app_id", "call_id", "provider", "model", "lineage"} <= ref.keys() or ref["lineage"] != "action" or ref["model"] != p["model_name"]:
                raise ExperienceError("AI_PROVENANCE_REQUIRED")
    elif p["model_name"] is not None or p["ai_call_refs"]:
        raise ExperienceError("VALIDATION_ERROR_AI_REFS")
    if p["supersedes_annotation_id"] is not None:
        identifier(p["supersedes_annotation_id"])
        if p["supersedes_annotation_id"] == p["annotation_id"]:
            raise ExperienceError("SUPERSESSION_INVALID")
    refs = p["source_refs"]
    if not isinstance(refs, list) or not 1 <= len(refs) <= 32:
        raise ExperienceError("VALIDATION_ERROR_SOURCE_REFS")
    ref_ids = set()
    for ref in refs:
        if set(ref) != {"source_ref_id", "lineage", "object_type", "object_id", "source_hash", "locator"}:
            raise ExperienceError("VALIDATION_ERROR_SOURCE_REF")
        for key in ("source_ref_id", "object_type", "object_id"):
            identifier(ref[key])
        if ref["source_ref_id"] in ref_ids or ref["lineage"] != c["lineage"]:
            raise ExperienceError("LINEAGE_OR_SOURCE_REF_COLLISION")
        ref_ids.add(ref["source_ref_id"])
        if not isinstance(ref["source_hash"], str) or not re.fullmatch("[a-f0-9]{64}", ref["source_hash"]):
            raise ExperienceError("VALIDATION_ERROR_SOURCE_HASH")
        loc = ref["locator"]
        if not isinstance(loc, dict) or set(loc) != {"kind", "unit", "char_start", "char_end"} or loc["kind"] != "text" or loc["unit"] != "unicode_codepoint":
            raise ExperienceError("LOCATOR_UNSUPPORTED")
        if any(type(loc[k]) is not int for k in ("char_start", "char_end")) or not 0 <= loc["char_start"] < loc["char_end"]:
            raise ExperienceError("SOURCE_BOUNDS_INVALID")
    body = c["payload"]
    kind = p["annotation_type"]
    if not isinstance(body, dict) or set(body) != PAYLOAD_FIELDS[kind] or not isinstance(c["field_status"], dict) or not isinstance(c["extensions"], dict):
        raise ExperienceError("VALIDATION_ERROR_PAYLOAD")
    for pointer, status in c["field_status"].items():
        if not pointer.startswith("/payload/") or not isinstance(status, dict) or set(status) != {"status", "reason"} or status["status"] not in {"unknown", "not_recorded", "not_applicable"} or not status["reason"]:
            raise ExperienceError("VALIDATION_ERROR_FIELD_STATUS")
        node = c
        try:
            for part in pointer.split("/")[1:]:
                node = node[int(part)] if isinstance(node, list) else node[part.replace("~1", "/").replace("~0", "~")]
        except (KeyError, IndexError, ValueError, TypeError):
            raise ExperienceError("VALIDATION_ERROR_FIELD_STATUS") from None
    def missing(node, path):
        if node is None and path not in c["field_status"]:
            raise ExperienceError("MISSING_FIELD_STATUS")
        if isinstance(node, dict):
            for key, child in node.items():
                missing(child, path + "/" + key)
        if isinstance(node, list):
            for i, child in enumerate(node):
                missing(child, path + "/" + str(i))
    missing(body, "/payload")
    for field in ("source_ref_ids",):
        if body.get(field) is not None and (any(not isinstance(x, str) for x in body[field]) or not set(body[field]) <= ref_ids):
            raise ExperienceError("PAYLOAD_SOURCE_REF_UNRESOLVED")
    if kind in {"action", "decision"}:
        field = "occurred_at" if kind == "action" else "decided_at"
        if body[field] is not None:
            body[field] = timestamp(body[field])
    if kind == "action" and body["type"] is not None and body["type"] not in (action_types or ACTIONS):
        raise ExperienceError("ACTION_TYPE_UNREGISTERED")
    if kind == "result" and body["status"] is not None and body["status"] not in RESULTS:
        raise ExperienceError("VALIDATION_ERROR_RESULT")
    if kind == "decision":
        if body["value"] is not None and body["value"] not in DECISIONS:
            raise ExperienceError("VALIDATION_ERROR_DECISION")
        if body["authority_kind"] not in {None, "observed_decision", "proposed_decision"}:
            raise ExperienceError("VALIDATION_ERROR_AUTHORITY")
        if p["created_by"] == "model" and body["authority_kind"] == "observed_decision":
            raise ExperienceError("OBSERVED_DECISION_REQUIRES_NON_MODEL_ATTESTATION")
    if kind == "evaluation":
        if not isinstance(body["items"], list):
            raise ExperienceError("VALIDATION_ERROR_EVALUATION")
        for item in body["items"]:
            if set(item) != {"dimension", "value", "unit", "baseline_ref", "criterion", "verdict", "evidence_refs", "evaluator_ref", "evaluated_at"} or item["dimension"] not in (evaluation_types or EVALUATIONS):
                raise ExperienceError("VALIDATION_ERROR_EVALUATION")
            timestamp(item["evaluated_at"])
            if not isinstance(item["evidence_refs"], list) or not set(item["evidence_refs"]) <= ref_ids or item["verdict"] not in {"pass", "fail", "unknown", "qualitative"}:
                raise ExperienceError("VALIDATION_ERROR_EVALUATION")
    if kind == "context_5w1h":
        for name, fields in {"what": {"summary", "subject_refs"}, "when": {"event_time", "recorded_at", "annotated_at", "system_version_time", "retrospective_at"}, "why": {"text", "origin", "source_ref_ids"}, "how": {"method", "tool_refs", "parameters", "test_refs"}}.items():
            if body[name] is not None and (not isinstance(body[name], dict) or set(body[name]) != fields):
                raise ExperienceError("VALIDATION_ERROR_CONTEXT")
        if body["why"] and body["why"]["origin"] not in {None, "explicit", "inferred", "retrospective"}:
            raise ExperienceError("VALIDATION_ERROR_WHY")
        for name in ("who", "where"):
            if body[name] is not None and (not isinstance(body[name], list) or any(not isinstance(x, dict) for x in body[name])):
                raise ExperienceError("VALIDATION_ERROR_CONTEXT")
        if body["who"]:
            for actor in body["who"]:
                if not {"type", "actor_id", "role"} <= actor.keys():
                    raise ExperienceError("VALIDATION_ERROR_ACTOR")
        if body["when"]:
            for value in body["when"].values():
                if value is not None:
                    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                        try:
                            datetime.strptime(value, "%Y-%m-%d")
                        except ValueError:
                            raise ExperienceError("VALIDATION_ERROR_TIME") from None
                    else:
                        timestamp(value)
    return c


def adapt_legacy_context(value):
    """Lossless candidate adapter; intentionally not a persistence shortcut."""
    bounded(value)
    out = {key: None for key in PAYLOAD_FIELDS["context_5w1h"]}
    if isinstance(value.get("what"), str):
        out["what"] = {"summary": value["what"], "subject_refs": []}
    if isinstance(value.get("when"), str):
        out["when"] = {"event_time": value["when"], "recorded_at": None, "annotated_at": None, "system_version_time": None, "retrospective_at": None}
    if isinstance(value.get("why"), str):
        out["why"] = {"text": value["why"], "origin": None, "source_ref_ids": []}
    return {"candidate_context": out, "original": copy.deepcopy(value), "adapter_version": SCRIPT_VERSION, "status": "requires_review", "unresolved_fields": ["who", "where", "how", "why.origin", "decision.authority_kind"]}
