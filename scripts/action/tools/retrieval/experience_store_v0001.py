# ============================================================
# 文件名: experience_store_v0001.py
# 中文名: 经验旁库追加存储
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / persistence
# 脚本定位: 来源挂接、标注事务与持久索引任务边界
#
# 职责说明:
# - 验证授权来源并追加不可变经验标注
#
# 本脚本做什么:
# - 专属新旁库、稳定 anchor、幂等凭据、修正分支与 outbox
#
# 本脚本不做什么:
# - 不迁移原库、不写开发登记库、不生成标注
#
# 制度边界声明:
# - 初始化仅接受不存在的新文件；事实表禁止 UPDATE/DELETE
# - 原文只读，事务失败回滚；不自动建正式业务库
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: experience_store_v0001
# family: experience_store
# role: append_experience_store
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/experience_store_v0001.py
# input:
#   - configured sidecar path and authorized source mappings
#   - annotation and request identity
# output:
#   - immutable annotation identity and projection state
# depends_on:
#   - experience_contract_v0001
#   - Python stdlib
# used_by:
#   - experience_episode_v0001
#   - experience_query_v0001
#   - retrieval_pipeline_v0001
# ============================================================

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from .experience_contract_v0001 import ExperienceError, canonical, digest, identifier, timestamp, utcnow, validate_annotation
else:
    from experience_contract_v0001 import ExperienceError, canonical, digest, identifier, timestamp, utcnow, validate_annotation


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "experience_store"
SCRIPT_NAME = "experience_store_v0001"
SCRIPT_VERSION = "v0001"
DB_VERSION = 1


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class Source:
    path: Path
    table: str
    id_column: str
    text_column: str
    object_type: str = "unit_text"
    lineage: str = "action"


# ============================================================
# 工具函数区
# ============================================================

def sql_name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ExperienceError("CONFIG_INVALID_IDENTIFIER")
    return '"' + value + '"'


def read_connection(path):
    path = Path(path).resolve()
    if not path.is_file() or path.name.lower() == "action.db":
        raise ExperienceError("SOURCE_UNRESOLVED")
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 4*1024*1024)
    started = time.monotonic()
    conn.set_progress_handler(lambda: int(time.monotonic()-started >= 2), 1000)
    return conn


def resolve_source(ref, sources):
    matches = [s for s in sources if s.object_type == ref["object_type"] and s.lineage == ref["lineage"]]
    if len(matches) != 1:
        raise ExperienceError("SOURCE_MAPPING_AMBIGUOUS_OR_MISSING")
    source = matches[0]
    conn = read_connection(source.path)
    try:
        rows = conn.execute(f"SELECT {sql_name(source.text_column)} FROM {sql_name(source.table)} WHERE {sql_name(source.id_column)}=? LIMIT 2", (ref["object_id"],)).fetchall()
    except sqlite3.Error:
        raise ExperienceError("SOURCE_SCHEMA_MISMATCH") from None
    finally:
        conn.close()
    if len(rows) != 1 or not isinstance(rows[0][0], str):
        raise ExperienceError("SOURCE_UNRESOLVED")
    text = rows[0][0]
    if len(text.encode(DEFAULT_ENCODING)) > 4 * 1024 * 1024:
        raise ExperienceError("SOURCE_TOO_LARGE")
    if hashlib.sha256(text.encode(DEFAULT_ENCODING)).hexdigest() != ref["source_hash"]:
        raise ExperienceError("SOURCE_HASH_MISMATCH")
    loc = ref["locator"]
    if not 0 <= loc["char_start"] < loc["char_end"] <= len(text):
        raise ExperienceError("SOURCE_BOUNDS_INVALID")
    return {"source_ref": ref, "text": text[loc["char_start"]:loc["char_end"]]}


# ============================================================
# 默认映射
# ============================================================

DDL = """
CREATE TABLE metadata(version INTEGER NOT NULL);
INSERT INTO metadata VALUES(1);
CREATE TABLE anchors(anchor_id TEXT PRIMARY KEY, lineage TEXT NOT NULL, source_identity TEXT NOT NULL UNIQUE, source_json TEXT NOT NULL);
CREATE TABLE annotations(annotation_id TEXT PRIMARY KEY, anchor_id TEXT NOT NULL REFERENCES anchors, lineage TEXT NOT NULL, annotation_type TEXT NOT NULL, created_at TEXT NOT NULL, ingested_at TEXT NOT NULL, event_time TEXT, domain TEXT, action_type TEXT, result_status TEXT, decision TEXT, authority_kind TEXT, supersedes TEXT REFERENCES annotations, content_hash TEXT NOT NULL, body TEXT NOT NULL);
CREATE INDEX annotation_anchor_type ON annotations(anchor_id,annotation_type);
CREATE INDEX annotation_time ON annotations(ingested_at,created_at,event_time);
CREATE INDEX annotation_decision ON annotations(domain,decision);
CREATE INDEX annotation_action ON annotations(action_type,result_status);
CREATE INDEX annotation_supersedes ON annotations(supersedes);
CREATE TABLE source_refs(annotation_id TEXT NOT NULL REFERENCES annotations, source_ref_id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(annotation_id,source_ref_id));
CREATE TABLE receipts(lineage TEXT NOT NULL, producer_id TEXT NOT NULL, request_id TEXT NOT NULL, content_hash TEXT NOT NULL, annotation_id TEXT NOT NULL REFERENCES annotations, PRIMARY KEY(lineage,producer_id,request_id));
CREATE TABLE episode_revisions(revision_id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, previous_id TEXT REFERENCES episode_revisions, created_at TEXT NOT NULL, body TEXT NOT NULL);
CREATE UNIQUE INDEX one_episode_revision_child ON episode_revisions(episode_id,COALESCE(previous_id,''));
CREATE TABLE episode_members(revision_id TEXT NOT NULL REFERENCES episode_revisions, slot TEXT NOT NULL, annotation_id TEXT NOT NULL REFERENCES annotations, PRIMARY KEY(revision_id,slot,annotation_id));
CREATE TABLE experience_relations(relation_id TEXT PRIMARY KEY, from_revision TEXT NOT NULL REFERENCES episode_revisions, to_revision TEXT NOT NULL REFERENCES episode_revisions, kind TEXT NOT NULL, evidence_annotation_id TEXT NOT NULL REFERENCES annotations, interpretation_kind TEXT NOT NULL, method_version TEXT NOT NULL, confidence REAL, created_at TEXT NOT NULL);
CREATE INDEX relation_from ON experience_relations(from_revision,kind);
CREATE INDEX relation_to ON experience_relations(to_revision,kind);
CREATE TABLE projection_jobs(job_id TEXT PRIMARY KEY, annotation_id TEXT NOT NULL REFERENCES annotations, target TEXT NOT NULL, index_version TEXT NOT NULL, status TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, error_code TEXT, UNIQUE(annotation_id,target,index_version));
"""


# ============================================================
# 核心类
# ============================================================

class ExperienceStore:
    def __init__(self, path, sources=(), projections=()):
        self.path = Path(path).resolve()
        if self.path.name.lower() in {"action.db", "action_data.db", "data.db", "control.sqlite3"}:
            raise ExperienceError("BUSINESS_DATABASE_MUST_BE_DEDICATED")
        self.sources = tuple(sources)
        self.projections = tuple(projections)

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.path.open("xb"):
                pass
        except FileExistsError:
            raise ExperienceError("INITIALIZATION_REQUIRES_NEW_FILE") from None
        conn = sqlite3.connect(self.path)
        try:
            conn.executescript("BEGIN IMMEDIATE;" + DDL)
            for table in ("anchors", "annotations", "source_refs", "receipts", "episode_revisions", "episode_members", "experience_relations"):
                for operation in ("UPDATE", "DELETE"):
                    conn.execute(f"CREATE TRIGGER immutable_{table}_{operation} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FACT'); END")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {"status": "initialized", "schema_version": DB_VERSION}

    @contextmanager
    def connection(self, write=False):
        conn = read_connection(self.path) if not write else sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=3)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            if conn.execute("SELECT version FROM metadata").fetchone()[0] != DB_VERSION:
                raise ExperienceError("SCHEMA_UNSUPPORTED")
            if write:
                conn.execute("BEGIN IMMEDIATE")
            yield conn
            if write:
                conn.commit()
        except BaseException:
            if write:
                conn.rollback()
            raise
        finally:
            conn.close()

    def anchor(self, source_ref, existing_anchor_id=None):
        resolve_source(source_ref, self.sources)
        identity = {k: v for k, v in source_ref.items() if k != "source_ref_id"}
        identity_hash = digest(identity)
        anchor_id = identifier(existing_anchor_id) if existing_anchor_id else "anchor_" + identity_hash
        with self.connection(True) as conn:
            row = conn.execute("SELECT * FROM anchors WHERE source_identity=? OR anchor_id=?", (identity_hash, anchor_id)).fetchone()
            if row:
                if row["source_identity"] != identity_hash or (existing_anchor_id and row["anchor_id"] != anchor_id):
                    raise ExperienceError("ANCHOR_IDENTITY_CONFLICT")
                return row["anchor_id"]
            conn.execute("INSERT INTO anchors VALUES(?,?,?,?)", (anchor_id, source_ref["lineage"], identity_hash, canonical(source_ref)))
        return anchor_id

    def append(self, request_id, producer_id, candidate, *, dry_run=False):
        identifier(request_id); identifier(producer_id)
        c = validate_annotation(candidate)
        p = c["provenance"]
        for ref in p["source_refs"]:
            resolve_source(ref, self.sources)
        semantic = json.loads(canonical(c))
        # ID is client supplied, nevertheless retry identity is semantic rather than proposed new ID.
        semantic["provenance"].pop("annotation_id")
        content_hash = digest(semantic)
        with self.connection(not dry_run) as conn:
            anchor = conn.execute("SELECT * FROM anchors WHERE anchor_id=?", (c["anchor_id"],)).fetchone()
            if not anchor or anchor["lineage"] != c["lineage"]:
                raise ExperienceError("ANCHOR_UNRESOLVED")
            if anchor["source_identity"] not in {digest({k:v for k,v in r.items() if k != "source_ref_id"}) for r in p["source_refs"]}:
                raise ExperienceError("ANCHOR_SOURCE_MISMATCH")
            old = conn.execute("SELECT * FROM receipts WHERE lineage=? AND producer_id=? AND request_id=?", (c["lineage"], producer_id, request_id)).fetchone()
            if old:
                if old["content_hash"] != content_hash:
                    raise ExperienceError("IDEMPOTENCY_CONFLICT")
                return {"status": "existing", "annotation_id": old["annotation_id"], "reused": True}
            if conn.execute("SELECT 1 FROM annotations WHERE annotation_id=?", (p["annotation_id"],)).fetchone():
                raise ExperienceError("ANNOTATION_ID_CONFLICT")
            target = p["supersedes_annotation_id"]
            if target:
                old = conn.execute("SELECT * FROM annotations WHERE annotation_id=?", (target,)).fetchone()
                if not old or any(old[k] != v for k,v in (("anchor_id", c["anchor_id"]), ("lineage", c["lineage"]), ("annotation_type", p["annotation_type"]))):
                    raise ExperienceError("SUPERSESSION_INVALID")
            relation_fields = {"evaluation_refs": "evaluation"} if p["annotation_type"] == "decision" else {"state_before_refs": "state_before", "action_refs": "action", "state_after_refs": "state_after"} if p["annotation_type"] == "transition" else {}
            for field, expected_kind in relation_fields.items():
                for aid in c["payload"].get(field) or []:
                    referenced = conn.execute("SELECT annotation_type,lineage FROM annotations WHERE annotation_id=?", (identifier(aid),)).fetchone()
                    if not referenced or referenced["annotation_type"] != expected_kind or referenced["lineage"] != c["lineage"]:
                        raise ExperienceError("ANNOTATION_MEMBER_REFERENCE_INVALID")
            # Existing immutable targets and fresh IDs make a forward-reference cycle impossible.
            if dry_run:
                return {"status": "dry_run", "annotation_id": p["annotation_id"]}
            body = c["payload"]
            kind = p["annotation_type"]
            received = utcnow()
            if p["created_at"] > received:
                raise ExperienceError("FUTURE_ANNOTATION_TIME")
            event_time = c["extensions"].get("event_time")
            if event_time is not None:
                event_time = timestamp(event_time)
            domain = c["extensions"].get("domain")
            if domain is not None:
                identifier(domain)
            conn.execute("INSERT INTO annotations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                p["annotation_id"], c["anchor_id"], c["lineage"], kind, p["created_at"], received, event_time, domain,
                body.get("type") if kind == "action" else None,
                body.get("status") if kind == "result" else None,
                body.get("value") if kind == "decision" else None,
                body.get("authority_kind") if kind == "decision" else None,
                target, content_hash, canonical(c)))
            conn.executemany("INSERT INTO source_refs VALUES(?,?,?)", [(p["annotation_id"], r["source_ref_id"], canonical(r)) for r in p["source_refs"]])
            conn.execute("INSERT INTO receipts VALUES(?,?,?,?,?)", (c["lineage"], producer_id, request_id, content_hash, p["annotation_id"]))
            for target, version in self.projections:
                identifier(target); identifier(version)
                conn.execute("INSERT INTO projection_jobs VALUES(?,?,?,?,?,0,?,NULL)", (digest([p["annotation_id"],target,version]), p["annotation_id"], target, version, "pending", received))
            branches = conn.execute("SELECT annotation_id FROM annotations WHERE supersedes=?", (p["supersedes_annotation_id"],)).fetchall() if p["supersedes_annotation_id"] else []
        return {"status": "persisted", "annotation_id": p["annotation_id"], "reused": False, "index_status": "pending" if self.projections else "disabled", "conflicts": [r[0] for r in branches] if len(branches) > 1 else []}

    def process_projection(self, target, version, writer, limit=20):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ExperienceError("INVALID_LIMIT")
        with self.connection() as conn:
            rows = conn.execute("SELECT j.*,a.body FROM projection_jobs j JOIN annotations a USING(annotation_id) WHERE target=? AND index_version=? AND status!='completed' LIMIT ?", (target, version, limit)).fetchall()
        results = []
        for row in rows:
            try:
                # Target writer must upsert this stable job/object ID; crash can repeat delivery.
                writer(row["annotation_id"], json.loads(row["body"]), row["job_id"])
                status, error = "completed", None
            except Exception:
                status, error = "failed", "PROJECTION_FAILED"
            with self.connection(True) as conn:
                conn.execute("UPDATE projection_jobs SET status=?,retry_count=retry_count+1,updated_at=?,error_code=? WHERE job_id=?", (status, utcnow(), error, row["job_id"]))
            results.append({"job_id": row["job_id"], "status": status, "error_code": error})
        return {"status": "indexing_partial" if any(r["status"] != "completed" for r in results) else "completed", "jobs": results}
