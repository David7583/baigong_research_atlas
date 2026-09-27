# ============================================================
# 文件名: experience_episode_v0001.py
# 中文名: 经验快照组装与研究导出
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / episode
# 脚本定位: 固定标注成员的不可变经验快照边界
#
# 职责说明:
# - 组装来源可追溯的经验快照
#
# 本脚本做什么:
# - 乐观锁版本、缺项和分歧、只读 Replay 导出
#
# 本脚本不做什么:
# - 不执行历史动作、不训练模型、不授予复用权限
#
# 制度边界声明:
# - 只追加快照；研究导出独占创建，不覆盖已有文件
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: experience_episode_v0001
# family: experience_episode
# role: immutable_episode_assembler
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/experience_episode_v0001.py
# input:
#   - episode identity and fixed annotation members
# output:
#   - versioned episode and research replay
# depends_on:
#   - experience_store_v0001
#   - experience_contract_v0001
# used_by:
#   - retrieval_pipeline_v0001
# ============================================================

from __future__ import annotations

import json
import math
import os
import uuid
from pathlib import Path

if __package__:
    from .experience_contract_v0001 import ExperienceError, PAYLOAD_FIELDS, bounded, canonical, digest, identifier, utcnow
    from .experience_store_v0001 import resolve_source
else:
    from experience_contract_v0001 import ExperienceError, PAYLOAD_FIELDS, bounded, canonical, digest, identifier, utcnow
    from experience_store_v0001 import resolve_source


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "experience_episode"
SCRIPT_NAME = "experience_episode_v0001"
SCRIPT_VERSION = "v0001"
REQUIRED_SLOTS = set(PAYLOAD_FIELDS) - {"transition"}


# ============================================================
# 核心业务组件
# ============================================================

def assemble_episode(store, episode_id, expected_previous_revision_id, members, *, dry_run=False):
    identifier(episode_id)
    bounded(members)
    if not isinstance(members, dict) or not members or set(members) - set(PAYLOAD_FIELDS):
        raise ExperienceError("INVALID_EPISODE_MEMBERS")
    if any(not isinstance(ids, list) or len(ids) > 32 or len(ids) != len(set(ids)) for ids in members.values()):
        raise ExperienceError("INVALID_EPISODE_MEMBERS")
    with store.connection(not dry_run) as conn:
        latest = conn.execute("SELECT revision_id FROM episode_revisions WHERE episode_id=? ORDER BY rowid DESC LIMIT 1", (episode_id,)).fetchone()
        if (latest[0] if latest else None) != expected_previous_revision_id:
            raise ExperienceError("REVISION_CONFLICT")
        documents, anchors, sources, conflicts, missing = {}, set(), [], [], []
        all_ids = {identifier(aid) for ids in members.values() for aid in ids}
        for slot, ids in members.items():
            documents[slot] = []
            for aid in ids:
                row = conn.execute("SELECT * FROM annotations WHERE annotation_id=?", (aid,)).fetchone()
                if not row or row["annotation_type"] != slot or row["lineage"] != "action":
                    raise ExperienceError("MEMBER_TYPE_OR_LINEAGE_MISMATCH")
                doc = json.loads(row["body"])
                for ref in doc["provenance"]["source_refs"]:
                    resolve_source(ref, store.sources)
                documents[slot].append(doc)
                anchors.add(row["anchor_id"])
                sources.extend(doc["provenance"]["source_refs"])
                missing.extend(aid + p for p, status in doc["field_status"].items() if status["status"] != "not_applicable")
                if row["supersedes"]:
                    branches = conn.execute("SELECT annotation_id FROM annotations WHERE supersedes=?", (row["supersedes"],)).fetchall()
                    if len(branches) > 1:
                        conflicts.append({"supersedes": row["supersedes"], "branches": [r[0] for r in branches]})
            if len(ids) > 1:
                conflicts.append({"slot": slot, "perspectives": ids})
        for doc in documents.get("decision", []):
            refs = doc["payload"]["evaluation_refs"] or []
            if not refs:
                missing.append("decision.evaluation_refs")
            for aid in refs:
                if aid not in members.get("evaluation", []):
                    raise ExperienceError("DECISION_EVALUATION_NOT_IN_SNAPSHOT")
        for doc in documents.get("transition", []):
            for field, slot in (("state_before_refs", "state_before"), ("action_refs", "action"), ("state_after_refs", "state_after")):
                if not set(doc["payload"][field] or []) <= set(members.get(slot, [])):
                    raise ExperienceError("TRANSITION_MEMBER_MISMATCH")
        missing.extend(sorted(slot for slot in REQUIRED_SLOTS if not members.get(slot)))
        revision = "revision_" + uuid.uuid4().hex
        result = {"episode_id": episode_id, "episode_revision_id": revision, "schema_version": "0.1.0", "lineage": "action", "anchor_ids": sorted(anchors), "members": members, "previous_revision_id": expected_previous_revision_id, "created_at": utcnow(), "completeness": "complete" if not missing else "incomplete", "missing_fields": missing, "conflicts": conflicts, "source_refs": sources}
        if not dry_run:
            conn.execute("INSERT INTO episode_revisions VALUES(?,?,?,?,?)", (revision, episode_id, expected_previous_revision_id, result["created_at"], canonical(result)))
            conn.executemany("INSERT INTO episode_members VALUES(?,?,?)", [(revision, slot, aid) for slot, ids in members.items() for aid in ids])
        return {"status": "dry_run" if dry_run else "persisted", "episode": result}


def export_replay(store, revisions, output, *, missing_policy="preserve"):
    if missing_policy not in {"preserve", "reject"} or not isinstance(revisions, list) or not 1 <= len(revisions) <= 100:
        raise ExperienceError("INVALID_REPLAY_REQUEST")
    records = []
    with store.connection() as conn:
        for revision in revisions:
            row = conn.execute("SELECT body FROM episode_revisions WHERE revision_id=?", (identifier(revision),)).fetchone()
            if not row:
                raise ExperienceError("REVISION_UNRESOLVED")
            episode = json.loads(row[0])
            if missing_policy == "reject" and episode["missing_fields"]:
                raise ExperienceError("REPLAY_INCOMPLETE")
            annotations = {}
            for slot, ids in episode["members"].items():
                annotations[slot] = [json.loads(conn.execute("SELECT body FROM annotations WHERE annotation_id=?", (aid,)).fetchone()[0]) for aid in ids]
            records.append({"episode": episode, "annotations": annotations, "reward": None, "purpose": "research_only"})
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    with store.connection() as conn:
        relations = [dict(row) for revision in revisions for row in conn.execute("SELECT * FROM experience_relations WHERE from_revision=? ORDER BY relation_id", (revision,))]
    # JSONL manifest line plus explicit records; missing targets are preserved as graph boundary references.
    manifest = {"record_type": "manifest", "schema_version": "0.1.0", "revision_ids": revisions, "missing_policy": missing_policy, "record_count": len(records), "content_hash": digest(records), "created_at": utcnow(), "executes_actions": False, "relations": relations}
    with path.open("x", encoding=DEFAULT_ENCODING) as stream:
        stream.write(canonical(manifest) + "\n")
        for record in records:
            stream.write(canonical({"record_type": "replay", **record}) + "\n")
        stream.flush(); os.fsync(stream.fileno())
    return {"status": "exported", "record_count": len(records), "content_hash": digest(records)}


def append_relation(store, relation, *, dry_run=False):
    bounded(relation)
    required = {"relation_id", "from_revision", "to_revision", "kind", "evidence_annotation_id", "interpretation_kind", "method_version", "confidence"}
    if set(relation) != required or relation["kind"] not in {"follows", "modifies", "based_on", "caused_by"} or relation["interpretation_kind"] not in {"observed", "hypothesis", "retrospective"}:
        raise ExperienceError("RELATION_CONTRACT_INVALID")
    if relation["from_revision"] == relation["to_revision"]:
        raise ExperienceError("RELATION_SELF_REFERENCE")
    for key in ("relation_id", "from_revision", "to_revision", "evidence_annotation_id", "method_version"):
        identifier(relation[key])
    confidence = relation["confidence"]
    if confidence is not None and (type(confidence) not in (int,float) or not math.isfinite(confidence) or not 0 <= confidence <= 1):
        raise ExperienceError("RELATION_CONFIDENCE_INVALID")
    with store.connection(not dry_run) as conn:
        for key in ("from_revision", "to_revision"):
            if not conn.execute("SELECT 1 FROM episode_revisions WHERE revision_id=?", (relation[key],)).fetchone():
                raise ExperienceError("REVISION_UNRESOLVED")
        row = conn.execute("SELECT body FROM annotations WHERE annotation_id=?", (relation["evidence_annotation_id"],)).fetchone()
        if not row:
            raise ExperienceError("RELATION_EVIDENCE_UNRESOLVED")
        for ref in json.loads(row[0])["provenance"]["source_refs"]:
            resolve_source(ref, store.sources)
        old = conn.execute("SELECT * FROM experience_relations WHERE relation_id=?", (relation["relation_id"],)).fetchone()
        if old:
            if any(old[key] != relation[key] for key in required):
                raise ExperienceError("RELATION_ID_CONFLICT")
            return {"status": "existing", "relation_id": relation["relation_id"]}
        if not dry_run:
            keys = ["relation_id", "from_revision", "to_revision", "kind", "evidence_annotation_id", "interpretation_kind", "method_version", "confidence"]
            conn.execute("INSERT INTO experience_relations VALUES(?,?,?,?,?,?,?,?,?)", [relation[key] for key in keys] + [utcnow()])
    return {"status": "dry_run" if dry_run else "persisted", "relation_id": relation["relation_id"]}


def development_memory(store, revision, *, depth=2, limit=100):
    identifier(revision)
    if type(depth) is not int or not 0 <= depth <= 4 or type(limit) is not int or not 1 <= limit <= 100:
        raise ExperienceError("GRAPH_RESOURCE_LIMIT")
    seen, frontier, edges, truncated = {revision}, [revision], {}, False
    with store.connection() as conn:
        if not conn.execute("SELECT 1 FROM episode_revisions WHERE revision_id=?", (revision,)).fetchone():
            raise ExperienceError("REVISION_UNRESOLVED")
        for _ in range(depth):
            following = []
            for node in frontier:
                for row in conn.execute("SELECT * FROM experience_relations WHERE from_revision=? OR to_revision=? ORDER BY relation_id LIMIT ?", (node,node,limit+1)):
                    if len(edges) >= limit and row["relation_id"] not in edges:
                        truncated = True; break
                    edges[row["relation_id"]] = dict(row)
                    for endpoint in (row["from_revision"], row["to_revision"]):
                        if endpoint not in seen:
                            seen.add(endpoint); following.append(endpoint)
            frontier = following
            if truncated:
                break
    return {"status": "completed", "revision_ids": sorted(seen), "relations": list(edges.values()), "boundary_revision_ids": frontier, "truncated": truncated, "inferred_edges": 0}
