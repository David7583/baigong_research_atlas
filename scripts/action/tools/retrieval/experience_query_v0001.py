# ============================================================
# 文件名: experience_query_v0001.py
# 中文名: 受控经验结构化检索
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / controlled
# 脚本定位: 模式一筛选、原文恢复和统一证据输出
#
# 职责说明:
# - 有界查询经验标注与固定快照
#
# 本脚本做什么:
# - 结构化筛选、历史时点、分歧、正文引用和预算上下文
#
# 本脚本不做什么:
# - 不执行模式二查询、不生成最终回答、不调用模型
#
# 制度边界声明:
# - 原文作为不可信资料，超预算不伪造完整结果
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: experience_query_v0001
# family: experience_query
# role: controlled_experience_retrieval
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/experience_query_v0001.py
# input:
#   - QueryRequest and configured experience store
# output:
#   - unified evidence envelope
# depends_on:
#   - experience_contract_v0001
#   - experience_store_v0001
# used_by:
#   - retrieval_pipeline_v0001
# ============================================================

from __future__ import annotations

import json
import sqlite3
import time

if __package__:
    from .experience_contract_v0001 import ExperienceError, PAYLOAD_FIELDS, bounded, canonical, digest, identifier, timestamp
    from .experience_store_v0001 import resolve_source
else:
    from experience_contract_v0001 import ExperienceError, PAYLOAD_FIELDS, bounded, canonical, digest, identifier, timestamp
    from experience_store_v0001 import resolve_source


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "experience_query"
SCRIPT_NAME = "experience_query_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 默认映射
# ============================================================

LAYERS = {"context": ["context_5w1h"], "state": ["state_before", "state_after"], "action": ["action", "transition"], "result": ["result"], "evaluation": ["evaluation"], "decision": ["decision"], "raw_evidence": list(PAYLOAD_FIELDS), "episode": []}
QUERY_FIELDS = {"request_id", "lineage", "query_text", "semantic_layers", "annotation_types", "domain", "action_types", "result_statuses", "decisions", "event_time_range", "knowledge_as_of", "revision_mode", "purpose", "limit", "cursor", "anchor_ids", "environment"}


# ============================================================
# 核心业务组件
# ============================================================

def envelope(request_id, mode, scope):
    return {"schema_version": "0.1.0", "request_id": request_id, "mode": mode, "status": "completed", "scope": scope, "hits": [], "truncated": False, "errors": [], "timings": {}}


def restore_hits(rows, store, *, source_id="experience", max_bytes=262144):
    hits, owners, size, errors = [], {}, 0, []
    truncated = False
    for row in rows:
        body = json.loads(row["body"])
        aid = row["annotation_id"]
        refs = []
        valid = True
        for ref in body["provenance"]["source_refs"]:
            key = digest({k:v for k,v in ref.items() if k != "source_ref_id"})
            try:
                recovered = resolve_source(ref, store.sources)
                if key in owners:
                    recovered = {"source_ref": ref, "expanded_context_reference": owners[key]}
                else:
                    recovered["context_owner"] = aid
                    owners[key] = aid
                refs.append(recovered)
            except ExperienceError as exc:
                valid = False
                errors.append({"object_id": aid, "error_type": str(exc)})
        hit = {"source_id": source_id, "lineage": "action", "object_id": aid, "anchor_id": row["anchor_id"], "object_kind": "derived_annotation", "annotation": body, "ingested_at": row["ingested_at"], "source_refs": refs, "source_verified": valid, "match_reason": "structured_filter", "ranking_source": "stable_ingestion_order", "retrospective": body["provenance"]["is_retrospective"], "conflicts": [], "eligible_for_action_reference": False}
        with store.connection() as conn:
            parent = row["supersedes"]
            if parent:
                branch = conn.execute("SELECT annotation_id FROM annotations WHERE supersedes=?", (parent,)).fetchall()
                if len(branch) > 1:
                    hit["conflicts"] = [r[0] for r in branch]
        amount = len(canonical(hit).encode(DEFAULT_ENCODING))
        if size + amount > max_bytes:
            truncated = True
            break
        size += amount
        hits.append(hit)
    return hits, truncated, errors


def query_experiences(store, request, *, timeout_ms=2000, max_bytes=262144, semantic_search=None):
    bounded(request)
    if not isinstance(request, dict) or set(request) - QUERY_FIELDS or request.get("lineage", "action") != "action":
        raise ExperienceError("INVALID_QUERY")
    rid = identifier(request.get("request_id", "query"))
    limit = request.get("limit", 20)
    cursor = request.get("cursor")
    if type(limit) is not int or not 1 <= limit <= 100 or (cursor is not None and (type(cursor) is not int or cursor < 0 or cursor > 1000000)):
        raise ExperienceError("INVALID_PAGINATION")
    layers = request.get("semantic_layers", [])
    if not isinstance(layers, list) or any(layer not in LAYERS for layer in layers):
        raise ExperienceError("SEMANTIC_LAYER_UNSUPPORTED")
    purpose = request.get("purpose", "historical")
    if purpose not in {"historical", "failure_learning", "action_reference"}:
        raise ExperienceError("PURPOSE_UNSUPPORTED")
    if purpose == "action_reference" or "episode" in layers:
        return query_episodes(store, request, timeout_ms=timeout_ms, max_bytes=max_bytes)
    semantic_hits = None
    if request.get("query_text"):
        if semantic_search is None:
            raise ExperienceError("SEMANTIC_BACKEND_NOT_CONFIGURED")
        if not isinstance(request["query_text"], str) or len(request["query_text"]) > 8000:
            raise ExperienceError("SEMANTIC_QUERY_TOO_LARGE")
        semantic_hits = semantic_search(request["query_text"], limit)
        if not isinstance(semantic_hits, list) or len(semantic_hits) > 100 or any(not isinstance(h, dict) or not isinstance(h.get("annotation_id"), str) or h.get("lineage") != "action" for h in semantic_hits):
            raise ExperienceError("SEMANTIC_RESULT_INVALID")
        if not semantic_hits:
            result = envelope(rid, "controlled", request); result["status"] = "insufficient_evidence"
            return result
    if not any(request.get(k) for k in ("query_text", "annotation_types", "semantic_layers", "domain", "action_types", "result_statuses", "decisions", "anchor_ids", "event_time_range", "knowledge_as_of")) and cursor is None:
        raise ExperienceError("EMPTY_QUERY_REQUIRES_CURSOR")
    mode = request.get("revision_mode", "all")
    if mode not in {"all", "current"}:
        raise ExperienceError("REVISION_MODE_UNSUPPORTED")
    clauses, args = ["a.lineage='action'"], []
    if semantic_hits is not None:
        clauses.append("a.annotation_id IN (" + ",".join("?" for _ in semantic_hits) + ")")
        args.extend(h["annotation_id"] for h in semantic_hits)
    kinds = request.get("annotation_types", [])
    if kinds and (not isinstance(kinds, list) or any(x not in PAYLOAD_FIELDS for x in kinds)):
        raise ExperienceError("ANNOTATION_TYPE_UNSUPPORTED")
    if layers:
        layer_types = {t for layer in layers for t in LAYERS[layer]}
        kinds = [t for t in (kinds or list(PAYLOAD_FIELDS)) if t in layer_types]
        if not kinds:
            return envelope(rid, "controlled", request)
    for column, values in (("annotation_type", kinds), ("action_type", request.get("action_types")), ("result_status", request.get("result_statuses")), ("decision", request.get("decisions")), ("anchor_id", request.get("anchor_ids"))):
        if values:
            if not isinstance(values, list) or len(values) > 100 or any(not isinstance(v, str) for v in values):
                raise ExperienceError("INVALID_QUERY_FILTER")
            clauses.append("a." + column + " IN (" + ",".join("?" for _ in values) + ")")
            args.extend(values)
    if request.get("domain"):
        clauses.append("a.domain=?"); args.append(identifier(request["domain"]))
    if request.get("event_time_range"):
        bounds = request["event_time_range"]
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ExperienceError("INVALID_TIME_RANGE")
        lower, upper = map(timestamp, bounds)
        if lower > upper:
            raise ExperienceError("INVALID_TIME_RANGE")
        clauses.append("a.event_time>=? AND a.event_time<?"); args.extend([lower, upper])
    as_of = timestamp(request["knowledge_as_of"]) if request.get("knowledge_as_of") else None
    if as_of:
        clauses.append("a.created_at<=? AND a.ingested_at<=?"); args.extend([as_of, as_of])
    if mode == "current":
        clause = "NOT EXISTS(SELECT 1 FROM annotations child WHERE child.supersedes=a.annotation_id"
        if as_of:
            clause += " AND child.created_at<=? AND child.ingested_at<=?"; args.extend([as_of, as_of])
        clauses.append(clause + ")")
    if purpose == "failure_learning":
        clauses.append("(a.result_status IN ('failure','unexpected_effect') OR a.decision IN ('REJECT','ROLLBACK'))")
    started = time.monotonic()
    result = envelope(rid, "controlled", request)
    with store.connection() as conn:
        conn.set_progress_handler(lambda: int((time.monotonic() - started) * 1000 >= timeout_ms), 1000)
        try:
            rows = conn.execute("SELECT a.* FROM annotations a WHERE " + " AND ".join(clauses) + " ORDER BY a.ingested_at,a.annotation_id LIMIT ? OFFSET ?", (*args, limit + 1, cursor or 0)).fetchall()
        except sqlite3.OperationalError:
            raise ExperienceError("QUERY_TIMEOUT_OR_STORAGE_ERROR") from None
    result["hits"], byte_truncated, result["errors"] = restore_hits(rows[:limit], store, max_bytes=max_bytes)
    if semantic_hits is not None:
        by_id = {h["object_id"]: h for h in result["hits"]}
        expanded = []
        for rank, semantic in enumerate(semantic_hits):
            if semantic["annotation_id"] not in by_id:
                continue
            original = by_id[semantic["annotation_id"]]
            if semantic.get("content_hash") != digest({k:v for k,v in original["annotation"].items()}):
                result["errors"].append({"object_id": semantic["annotation_id"], "error_type": "STALE_VECTOR_PROJECTION"})
                continue
            hit = dict(original)
            hit.update(match_reason="semantic_and_structured_filter", ranking_source="vector", rank=rank, distance=semantic.get("distance"), index_version=semantic.get("index_version"), embedding_model=semantic.get("embedding_model"))
            if any(h["object_id"] == hit["object_id"] for h in expanded):
                hit["source_refs"] = [{"source_ref": r["source_ref"], "expanded_context_reference": hit["object_id"]} for r in hit["source_refs"]]
            expanded.append(hit)
        result["hits"] = []
        for hit in expanded:
            if len(canonical(result).encode(DEFAULT_ENCODING)) + len(canonical(hit).encode(DEFAULT_ENCODING)) > max_bytes:
                byte_truncated = True
                break
            result["hits"].append(hit)
    if layers == ["raw_evidence"]:
        raw_hits = []
        for hit in result["hits"]:
            if hit["source_refs"]:
                ref = hit["source_refs"][0]
                raw_hits.append({**hit, "object_id": ref["source_ref"]["object_id"], "annotation_id": hit["object_id"], "object_kind": "raw_evidence"})
        result["hits"] = raw_hits
    result["truncated"] = len(rows) > limit or byte_truncated
    result["next_cursor"] = (cursor or 0) + len(result["hits"]) if result["truncated"] else None
    if result["errors"]:
        result["status"] = "partial"
    elif not result["hits"]:
        result["status"] = "insufficient_evidence"
    result["timings"]["query_ms"] = (time.monotonic() - started) * 1000
    return result


def query_episodes(store, request, *, timeout_ms=2000, max_bytes=262144):
    """Snapshot selection preserves actual members; no latest-annotation substitution."""
    purpose = request.get("purpose", "historical")
    environment = request.get("environment")
    if purpose == "action_reference" and (not isinstance(environment, dict) or not environment):
        raise ExperienceError("ACTION_REFERENCE_ENVIRONMENT_REQUIRED")
    if request.get("query_text"):
        raise ExperienceError("SEMANTIC_BACKEND_NOT_CONFIGURED")
    if request.get("cursor") is None:
        raise ExperienceError("EPISODE_QUERY_REQUIRES_CURSOR")
    mode = request.get("revision_mode", "current" if purpose == "action_reference" else "all")
    if mode not in {"all", "current"}:
        raise ExperienceError("REVISION_MODE_UNSUPPORTED")
    as_of = timestamp(request["knowledge_as_of"]) if request.get("knowledge_as_of") else None
    started = time.monotonic()
    result = envelope(identifier(request.get("request_id", "query")), "controlled", request)
    result["excluded"] = []
    clauses, params = [], []
    if as_of:
        clauses.append("e.created_at<=?"); params.append(as_of)
    if mode == "current":
        clause = "NOT EXISTS (SELECT 1 FROM episode_revisions child WHERE child.previous_id=e.revision_id"
        if as_of:
            clause += " AND child.created_at<=?"; params.append(as_of)
        clauses.append(clause + ")")
    with store.connection() as conn:
        conn.set_progress_handler(lambda: int((time.monotonic()-started)*1000 >= timeout_ms), 500)
        rows = conn.execute("SELECT e.* FROM episode_revisions e" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY e.rowid LIMIT ? OFFSET ?", (*params, request.get("limit",20) + 1, request["cursor"])).fetchall()
        page = rows[:request.get("limit",20)]
        for row in page:
            episode = json.loads(row["body"])
            members = conn.execute("SELECT a.* FROM annotations a JOIN episode_members m USING(annotation_id) WHERE m.revision_id=? ORDER BY a.annotation_id", (row["revision_id"],)).fetchall()
            if as_of and any(m["created_at"] > as_of or m["ingested_at"] > as_of for m in members):
                continue
            filters = (("domain", "domain"), ("action_types", "action_type"), ("result_statuses", "result_status"), ("decisions", "decision"), ("annotation_types", "annotation_type"), ("anchor_ids", "anchor_id"))
            if any(request.get(key) and not any(m[column] in (request[key] if isinstance(request[key], list) else [request[key]]) for m in members) for key,column in filters):
                continue
            if request.get("event_time_range"):
                low, high = map(timestamp, request["event_time_range"])
                if not any(m["event_time"] and low <= m["event_time"] < high for m in members):
                    continue
            docs = [json.loads(m["body"]) for m in members]
            decisions = [d["payload"] for d in docs if d["provenance"]["annotation_type"] == "decision"]
            failures = any(m["result_status"] in {"failure", "unexpected_effect"} or m["decision"] in {"REJECT", "ROLLBACK"} for m in members)
            if purpose == "failure_learning" and not failures:
                continue
            reasons = []
            if any(conn.execute("SELECT 1 FROM annotations WHERE supersedes=? LIMIT 1", (m["annotation_id"],)).fetchone() for m in members):
                reasons.append("superseded_snapshot_member")
            if episode["missing_fields"]:
                reasons.append("incomplete")
            if episode["conflicts"]:
                reasons.append("snapshot_conflict")
            if len(decisions) != 1 or decisions[0].get("value") != "ACCEPT" or decisions[0].get("authority_kind") != "observed_decision" or not decisions[0].get("evaluation_refs"):
                reasons.append("observed_accept_required")
            if decisions and purpose == "action_reference":
                conditions = decisions[0].get("conditions")
                if not isinstance(conditions, dict) or not conditions or any(environment.get(key) != value for key,value in conditions.items()):
                    reasons.append("environment_mismatch_or_unspecified")
            if failures:
                reasons.append("failure_counterexample")
            if any(item.get("verdict") != "pass" or not item.get("evidence_refs") for d in docs if d["provenance"]["annotation_type"] == "evaluation" for item in (d["payload"].get("items") or [])):
                reasons.append("evaluation_not_passed")
            if not any(d["payload"].get("items") for d in docs if d["provenance"]["annotation_type"] == "evaluation"):
                reasons.append("evaluation_missing")
            recovered, truncated, errors = restore_hits(members, store, max_bytes=max_bytes)
            if errors or truncated or len(recovered) != len(members):
                reasons.append("source_unverified_or_budget")
            if any(h["conflicts"] for h in recovered):
                reasons.append("current_conflict")
            if purpose == "action_reference" and reasons:
                result["excluded"].append({"episode_revision_id": row["revision_id"], "reasons": reasons})
                continue
            hit = {"source_id": "experience", "lineage": "action", "object_id": row["episode_id"], "episode_revision_id": row["revision_id"], "object_kind": "episode", "episode": episode, "members": recovered, "source_refs": episode["source_refs"], "source_verified": not errors and not truncated, "conflicts": episode["conflicts"], "match_reason": "snapshot_filter", "ranking_source": "revision_order", "eligible_for_action_reference": not reasons, "limitations": reasons}
            if len(canonical(result).encode(DEFAULT_ENCODING)) + len(canonical(hit).encode(DEFAULT_ENCODING)) > max_bytes:
                result["truncated"] = True
                break
            result["hits"].append(hit)
    result["truncated"] |= len(rows) > len(page)
    result["next_cursor"] = request["cursor"] + len(page) if result["truncated"] else None
    if not result["hits"]:
        result["status"] = "insufficient_evidence"
    result["timings"]["query_ms"] = (time.monotonic()-started)*1000
    return result


def build_experience_context(result, token_budget):
    if type(token_budget) is not int or not 1 <= token_budget <= 32768:
        raise ExperienceError("INVALID_CONTEXT_BUDGET")
    selected, excluded, size = [], [], 0
    for hit in result["hits"]:
        amount = len(canonical(hit).encode(DEFAULT_ENCODING))
        # One UTF-8 byte per token is a conservative bound, not claimed actual model usage.
        if size + amount > token_budget or not hit.get("source_verified"):
            excluded.append({"object_id": hit["object_id"], "reason": "budget_or_unverified_source"})
        else:
            selected.append(hit); size += amount
    return {"status": "completed" if selected else "insufficient_evidence", "untrusted_evidence": selected, "excluded": excluded, "budget_method": "utf8_bytes_upper_bound", "budget_used": size, "token_budget": token_budget, "may_execute_actions": False}
