# ============================================================
# 文件名: research_ledger_v0001.py
# 中文名: 研究项目版本与交接账本
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / persistence
# 脚本定位: 项目任务、阶段执行和成果交付的事务持久层
#
# 职责说明:
# - 保存同一项目跨执行器的版本、成果和交接记录
#
# 本脚本做什么:
# - 校验就绪交付包、幂等键和基础修订，事务发布本地权威版本
# - 分离执行状态、归档状态和派生索引状态
#
# 本脚本不做什么:
# - 不执行模型或工具，不推断研究结论，不运行成果代码
#
# 制度边界声明:
# - 只写显式指定的独立运行库；历史版本和成果只追加
# - 路径限于收件根目录；归档失败保留已受理原包，不伪造索引成功
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_ledger_v0001
# family: research_ledger
# role: research_revision_ledger
# version: v0001
# status: active
# entry_point: scripts/action/development/scripts/research_assistant/research_ledger_v0001.py
# input:
#   - project, task, executor and ready delivery envelope
# output:
#   - durable revisions and separate execution/archive/index states
# depends_on:
#   - Python stdlib
# used_by:
#   - research_assistant_v0001
#   - research_ai_v0001
#   - research_ui_v0001
#   - research_retrieval_v0001
#   - research_ingestion_v0001
#   - research_index_v0001
# ============================================================

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_ledger"
SCRIPT_NAME = "research_ledger_v0001"
SCRIPT_VERSION = "v0001"
SCHEMA_VERSION = "1.0"
MAX_PACKAGE_BYTES = 2 * 1024 * 1024
MAX_ARTIFACTS = 12


# ============================================================
# 异常类型
# ============================================================

class ResearchError(RuntimeError):
    """Public errors contain a stable code, never request text or credentials."""


# ============================================================
# 数据结构
# ============================================================

class Executor(TypedDict):
    kind: str
    name: str
    version: str | None
    provider: str | None
    model: str | None
    access: str


class Delivery(TypedDict):
    schema_version: str
    project_id: str
    task_id: str
    run_id: str
    base_revision: int
    idempotency_key: str
    task_status: str
    run_status: str
    handoff: dict
    evidence: list[dict]
    artifacts: list[dict]
    coverage: list[dict]


# ============================================================
# 工具函数区
# ============================================================

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value).encode(DEFAULT_ENCODING)).hexdigest()


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", value):
        raise ResearchError("INVALID_IDENTIFIER")
    return value


def require(condition, code):
    if not condition:
        raise ResearchError(code)


def bounded(value):
    try:
        size = len(canonical(value).encode(DEFAULT_ENCODING))
    except (TypeError, ValueError, RecursionError):
        raise ResearchError("INVALID_JSON") from None
    require(size <= MAX_PACKAGE_BYTES, "PACKAGE_TOO_LARGE")


def scoped_path(root, relative):
    require(isinstance(relative, str) and relative and "\\" not in relative and ":" not in relative,
            "INVALID_RELATIVE_PATH")
    part = Path(relative)
    require(not part.is_absolute() and not any(p in {"..", "."} for p in part.parts), "PATH_ESCAPE")
    root = Path(root).resolve()
    candidate = root / part
    require(candidate.resolve().is_relative_to(root), "PATH_ESCAPE")
    for node in [candidate, *candidate.parents]:
        if node == root:
            break
        require(not node.is_symlink() and not (hasattr(node, "is_junction") and node.is_junction()), "REPARSE_POINT_DENIED")
    return candidate


def validate_executor(value):
    require(isinstance(value, dict) and set(value) == {"kind", "name", "version", "provider", "model", "access"}, "EXECUTOR_CONTRACT")
    require(value["kind"] in {"agent", "ai"} and value["access"] in {"local_tools", "manual_exchange", "controller"}, "EXECUTOR_CAPABILITY")
    require(isinstance(value["name"], str) and 0 < len(value["name"]) <= 120, "EXECUTOR_NAME")
    for key in ("version", "provider", "model"):
        require(value[key] is None or isinstance(value[key], str) and len(value[key]) <= 120, "EXECUTOR_IDENTITY")
    return value


def read_package(inbox, relative):
    folder = scoped_path(inbox, relative)
    manifest_path = scoped_path(folder, "manifest.json")
    ready_path = scoped_path(folder, "READY")
    require(manifest_path.is_file() and ready_path.is_file(), "PACKAGE_NOT_READY")
    require(manifest_path.stat().st_size <= MAX_PACKAGE_BYTES and ready_path.stat().st_size <= 128, "PACKAGE_TOO_LARGE")
    raw = manifest_path.read_bytes()
    require(ready_path.read_text(encoding=DEFAULT_ENCODING).strip() == digest(raw), "READY_HASH_MISMATCH")
    try:
        envelope = json.loads(raw)
    except (ValueError, UnicodeError):
        raise ResearchError("MANIFEST_JSON_INVALID") from None
    validate_delivery(envelope)
    artifacts, size, seen = [], len(raw), set()
    for item in envelope["artifacts"]:
        require(isinstance(item, dict) and set(item) == {"artifact_id", "path", "sha256", "size", "origin", "generated_at"}, "ARTIFACT_CONTRACT")
        identifier(item["artifact_id"])
        require(item["artifact_id"] not in seen and item["origin"] == "generated", "ARTIFACT_ID_OR_ORIGIN")
        seen.add(item["artifact_id"])
        path = scoped_path(folder, item["path"])
        require(path.suffix.lower() in {".md", ".txt", ".json", ".csv"} and path.is_file(), "ARTIFACT_TYPE_OR_MISSING")
        require(type(item["size"]) is int and 0 <= item["size"] <= MAX_PACKAGE_BYTES and path.stat().st_size == item["size"], "ARTIFACT_SIZE")
        size += item["size"]
        require(size <= MAX_PACKAGE_BYTES, "PACKAGE_TOO_LARGE")
        content = path.read_bytes()
        require(len(content) == item["size"] and digest(content) == item["sha256"], "ARTIFACT_HASH_MISMATCH")
        try:
            text = content.decode(DEFAULT_ENCODING)
            datetime.fromisoformat(item["generated_at"].replace("Z", "+00:00"))
        except (UnicodeError, ValueError, AttributeError):
            raise ResearchError("ARTIFACT_ENCODING_OR_TIME") from None
        artifacts.append({**item, "content": text})
    return envelope, artifacts, raw.decode(DEFAULT_ENCODING)


# ============================================================
# 核心类
# ============================================================

class Ledger:
    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def transaction(self, write=False):
        require(self.path.is_file(), "LEDGER_NOT_INITIALIZED")
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            require(connection.execute("PRAGMA user_version").fetchone()[0] == 1, "LEDGER_SCHEMA_UNSUPPORTED")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.path.open("xb"):
                pass
        except FileExistsError:
            with self.transaction():
                return {"status": "existing"}
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE projects (id TEXT PRIMARY KEY, body TEXT NOT NULL);
                CREATE TABLE tasks (id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), body TEXT NOT NULL, head INTEGER NOT NULL);
                CREATE TABLE runs (id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), base INTEGER NOT NULL, executor TEXT NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
                CREATE TABLE deliveries (id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), run_id TEXT NOT NULL REFERENCES runs(id), idempotency_key TEXT NOT NULL, payload_hash TEXT NOT NULL, envelope TEXT NOT NULL, original_manifest TEXT NOT NULL, artifacts TEXT NOT NULL, status TEXT NOT NULL, error_type TEXT, retry_count INTEGER NOT NULL DEFAULT 0, submitted_at TEXT NOT NULL, archived_at TEXT, UNIQUE(task_id,idempotency_key));
                CREATE TABLE revisions (task_id TEXT NOT NULL REFERENCES tasks(id), revision INTEGER NOT NULL, parent_revision INTEGER, delivery_id TEXT UNIQUE REFERENCES deliveries(id), body TEXT NOT NULL, PRIMARY KEY(task_id,revision));
                CREATE TABLE index_events (id INTEGER PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES deliveries(id), target TEXT NOT NULL, status TEXT NOT NULL, receipt TEXT NOT NULL, created_at TEXT NOT NULL);
                PRAGMA user_version=1;
                COMMIT;
            """)
        finally:
            connection.close()
        return {"status": "initialized", "schema_version": SCHEMA_VERSION}

    def create_project(self, title, project_id=None):
        require(isinstance(title, str) and 0 < len(title) <= 200, "PROJECT_TITLE")
        pid = identifier(project_id or "project_" + uuid.uuid4().hex)
        body = {"project_id": pid, "title": title, "created_at": now(), "schema_version": SCHEMA_VERSION}
        with self.transaction(True) as conn:
            require(not conn.execute("SELECT 1 FROM projects WHERE id=?", (pid,)).fetchone(), "PROJECT_EXISTS")
            conn.execute("INSERT INTO projects VALUES (?,?)", (pid, canonical(body)))
        return body

    def create_task(self, project_id, title, objective, constraints, source_ids, task_id=None):
        identifier(project_id)
        tid = identifier(task_id or "task_" + uuid.uuid4().hex)
        require(isinstance(title, str) and 0 < len(title) <= 200 and isinstance(objective, str) and 0 < len(objective) <= 10000, "TASK_TEXT")
        require(isinstance(constraints, list) and all(isinstance(x, str) for x in constraints), "CONSTRAINTS_INVALID")
        require(isinstance(source_ids, list) and all(identifier(x) for x in source_ids), "SOURCE_IDS_INVALID")
        body = {"schema_version": SCHEMA_VERSION, "project_id": project_id, "task_id": tid, "title": title,
                "objective": objective, "constraints": constraints, "source_ids": source_ids,
                "revision": 0, "parent_revision": None, "task_status": "active", "created_at": now(),
                "handoff": None, "artifacts": [], "evidence": [], "coverage": [], "index_status": "not_requested"}
        bounded(body)
        with self.transaction(True) as conn:
            require(conn.execute("SELECT 1 FROM projects WHERE id=?", (project_id,)).fetchone(), "PROJECT_NOT_FOUND")
            require(not conn.execute("SELECT 1 FROM tasks WHERE id=?", (tid,)).fetchone(), "TASK_EXISTS")
            conn.execute("INSERT INTO tasks VALUES (?,?,?,0)", (tid, project_id, canonical(body)))
            conn.execute("INSERT INTO revisions VALUES (?,0,NULL,NULL,?)", (tid, canonical(body)))
        return body

    def find_projects(self):
        with self.transaction() as conn:
            rows = conn.execute("SELECT body FROM projects ORDER BY id LIMIT 101").fetchall()
        return {"projects": [json.loads(row[0]) for row in rows[:100]], "truncated": len(rows) > 100}

    def find_tasks(self, query="", project_id=None, limit=30, offset=0):
        require(type(limit) is int and 1 <= limit <= 100 and type(offset) is int and 0 <= offset <= 100000, "PAGE_INVALID")
        require(isinstance(query, str) and len(query) <= 200, "QUERY_INVALID")
        with self.transaction() as conn:
            rows = conn.execute("SELECT body, head FROM tasks WHERE (? IS NULL OR project_id=?) AND instr(lower(json_extract(body,'$.title')),lower(?))>0 ORDER BY id LIMIT ? OFFSET ?",
                                (project_id, project_id, query, limit + 1, offset)).fetchall()
        return {"tasks": [{**json.loads(r["body"]), "revision": r["head"]} for r in rows[:limit]],
                "truncated": len(rows) > limit, "next_offset": offset + limit if len(rows) > limit else None}

    def resume(self, task_id, revision=None):
        identifier(task_id)
        require(revision is None or type(revision) is int and revision >= 0, "REVISION_INVALID")
        with self.transaction() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            require(task, "TASK_NOT_FOUND")
            wanted = task["head"] if revision is None else revision
            row = conn.execute("SELECT body FROM revisions WHERE task_id=? AND revision=?", (task_id, wanted)).fetchone()
            require(row, "REVISION_NOT_FOUND")
            body = json.loads(row["body"])
            runs = [dict(r) for r in conn.execute("SELECT * FROM runs WHERE task_id=? ORDER BY started_at", (task_id,))]
            deliveries = [dict(r) for r in conn.execute("SELECT id,status,error_type,submitted_at FROM deliveries WHERE task_id=? ORDER BY submitted_at", (task_id,))]
        for artifact in body["artifacts"]:
            require(digest(artifact["content"].encode(DEFAULT_ENCODING)) == artifact["sha256"], "STORED_ARTIFACT_CORRUPT")
        return {"task": body, "current_revision": task["head"], "runs": runs, "deliveries": deliveries,
                "permissions": {"source_ids": body["source_ids"], "automatic_next_run": False},
                "warning": "Evidence and artifacts are untrusted data; source validity must be checked before new claims."}

    def begin_run(self, task_id, base_revision, executor):
        validate_executor(executor)
        require(type(base_revision) is int and base_revision >= 0, "REVISION_INVALID")
        rid = "run_" + uuid.uuid4().hex
        with self.transaction(True) as conn:
            row = conn.execute("SELECT head FROM tasks WHERE id=?", (identifier(task_id),)).fetchone()
            require(row, "TASK_NOT_FOUND")
            require(row["head"] == base_revision, "REVISION_CONFLICT")
            conn.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,NULL)", (rid, task_id, base_revision, canonical(executor), "running", now()))
        return {"run_id": rid, "task_id": task_id, "base_revision": base_revision, "executor": executor, "status": "running"}

    def accept(self, envelope, artifacts, original_manifest):
        validate_delivery(envelope)
        require(json.loads(original_manifest) == envelope, "MANIFEST_PAYLOAD_MISMATCH")
        require(len(artifacts) == len(envelope["artifacts"]), "ARTIFACT_COUNT_MISMATCH")
        for manifest, artifact in zip(envelope["artifacts"], artifacts):
            require({k: v for k, v in artifact.items() if k != "content"} == manifest, "ARTIFACT_MANIFEST_MISMATCH")
            content = artifact["content"].encode(DEFAULT_ENCODING)
            require(len(content) == artifact["size"] and digest(content) == artifact["sha256"], "ARTIFACT_HASH_MISMATCH")
        bounded(artifacts)
        payload_hash = digest({"envelope": envelope, "artifacts": artifacts})
        did = "delivery_" + uuid.uuid4().hex
        with self.transaction(True) as conn:
            old = conn.execute("SELECT id,payload_hash FROM deliveries WHERE task_id=? AND idempotency_key=?",
                               (envelope["task_id"], envelope["idempotency_key"])).fetchone()
            if old:
                require(old["payload_hash"] == payload_hash, "IDEMPOTENCY_CONFLICT")
                return {"delivery_id": old["id"], "status": "existing"}
            task = conn.execute("SELECT * FROM tasks WHERE id=?", (envelope["task_id"],)).fetchone()
            require(task and task["project_id"] == envelope["project_id"], "PROJECT_TASK_MISMATCH")
            run = conn.execute("SELECT * FROM runs WHERE id=?", (envelope["run_id"],)).fetchone()
            require(run and run["task_id"] == envelope["task_id"] and run["base"] == envelope["base_revision"], "RUN_MISMATCH")
            require(run["status"] == "running", "RUN_ALREADY_FINISHED")
            task_body = json.loads(task["body"])
            allowed = set(task_body["source_ids"])
            require(all(e["source_id"] in allowed for e in envelope["evidence"]), "SOURCE_NOT_AUTHORIZED")
            require(all(c["source_id"] in allowed for c in envelope["coverage"]), "SOURCE_NOT_AUTHORIZED")
            conn.execute("INSERT INTO deliveries (id,task_id,run_id,idempotency_key,payload_hash,envelope,original_manifest,artifacts,status,submitted_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (did, envelope["task_id"], envelope["run_id"], envelope["idempotency_key"], payload_hash, canonical(envelope), original_manifest, canonical(artifacts), "accepted", now()))
            conn.execute("UPDATE runs SET status=?,ended_at=? WHERE id=?", (envelope["run_status"], now(), envelope["run_id"]))
        return {"delivery_id": did, "status": "accepted"}

    def archive(self, delivery_id):
        identifier(delivery_id)
        try:
            with self.transaction(True) as conn:
                row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
                require(row, "DELIVERY_NOT_FOUND")
                if row["status"] == "completed":
                    return {"status": "existing", "delivery_id": delivery_id}
                envelope = json.loads(row["envelope"])
                task = conn.execute("SELECT * FROM tasks WHERE id=?", (row["task_id"],)).fetchone()
                require(task["head"] == envelope["base_revision"], "REVISION_CONFLICT")
                previous = json.loads(conn.execute("SELECT body FROM revisions WHERE task_id=? AND revision=?", (task["id"], task["head"])).fetchone()[0])
                executor = json.loads(conn.execute("SELECT executor FROM runs WHERE id=?", (row["run_id"],)).fetchone()[0])
                artifacts = json.loads(row["artifacts"])
                require(digest({"envelope": envelope, "artifacts": artifacts}) == row["payload_hash"], "DELIVERY_CORRUPT")
                body = {**previous, **{k: envelope[k] for k in ("task_status", "handoff", "evidence", "coverage")},
                        "revision": task["head"] + 1, "parent_revision": task["head"], "delivery_id": delivery_id,
                        "run_id": row["run_id"], "executor": executor, "artifacts": artifacts,
                        "published_at": now(), "archive_status": "completed", "archive_targets": ["local_authority"],
                        "index_status": "not_requested", "publication_policy": "authority_first_explicit"}
                conn.execute("INSERT INTO revisions VALUES (?,?,?,?,?)", (task["id"], body["revision"], task["head"], delivery_id, canonical(body)))
                conn.execute("UPDATE tasks SET head=? WHERE id=?", (body["revision"], task["id"]))
                conn.execute("UPDATE deliveries SET status='completed',error_type=NULL,archived_at=? WHERE id=?", (now(), delivery_id))
            return {"status": "completed", "delivery_id": delivery_id, "revision": body["revision"],
                    "archive_targets": ["local_authority"], "index_status": "not_requested"}
        except ResearchError as exc:
            if str(exc) not in {"DELIVERY_NOT_FOUND", "LEDGER_NOT_INITIALIZED", "LEDGER_SCHEMA_UNSUPPORTED"}:
                with self.transaction(True) as conn:
                    conn.execute("UPDATE deliveries SET status='failed',error_type=?,retry_count=retry_count+1 WHERE id=? AND status!='completed'", (str(exc), delivery_id))
            raise

    def delivery_status(self, delivery_id):
        with self.transaction() as conn:
            row = conn.execute("SELECT id,task_id,run_id,status,error_type,retry_count,submitted_at,archived_at FROM deliveries WHERE id=?", (identifier(delivery_id),)).fetchone()
            require(row, "DELIVERY_NOT_FOUND")
            events = [dict(r) for r in conn.execute("SELECT target,status,receipt,created_at FROM index_events WHERE delivery_id=? ORDER BY id", (delivery_id,))]
        return {**dict(row), "archive_targets": ["local_authority"], "index_events": events,
                "index_status": events[-1]["status"] if events else "not_requested"}


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def validate_delivery(value):
    bounded(value)
    required = {"schema_version", "project_id", "task_id", "run_id", "base_revision", "idempotency_key", "task_status", "run_status", "handoff", "evidence", "artifacts", "coverage"}
    require(isinstance(value, dict) and set(value) == required and value["schema_version"] == SCHEMA_VERSION, "DELIVERY_CONTRACT")
    for key in ("project_id", "task_id", "run_id", "idempotency_key"):
        identifier(value[key])
    require(type(value["base_revision"]) is int and value["base_revision"] >= 0, "REVISION_INVALID")
    require(value["task_status"] in {"active", "paused", "completed", "abandoned"}, "TASK_STATUS_INVALID")
    require(value["run_status"] in {"succeeded", "failed", "stopped", "unknown"}, "RUN_STATUS_INVALID")
    handoff = value["handoff"]
    handoff_keys = {"completed", "failed", "decisions", "open_questions", "next_steps", "pending_confirmation", "unknown_operations"}
    require(isinstance(handoff, dict) and set(handoff) == handoff_keys, "HANDOFF_INCOMPLETE")
    require(all(isinstance(v, list) and all(isinstance(x, str) for x in v) for v in handoff.values()), "HANDOFF_LIST_INVALID")
    require(not (value["task_status"] == "completed" and (handoff["unknown_operations"] or handoff["pending_confirmation"])), "COMPLETION_HAS_PENDING_OPERATIONS")
    for key in ("evidence", "artifacts", "coverage"):
        require(isinstance(value[key], list), "DELIVERY_LIST_INVALID")
    require(len(value["artifacts"]) <= MAX_ARTIFACTS and len(value["evidence"]) <= 200 and len(value["coverage"]) <= 100, "DELIVERY_LIMIT")
    evidence_keys = {"source_id", "object_id", "source_hash", "origin", "locator", "claim", "relation"}
    for evidence in value["evidence"]:
        require(isinstance(evidence, dict) and set(evidence) == evidence_keys, "EVIDENCE_CONTRACT")
        identifier(evidence["source_id"])
        identifier(evidence["object_id"])
        require(isinstance(evidence["source_hash"], str) and re.fullmatch("[0-9a-f]{64}", evidence["source_hash"]), "EVIDENCE_HASH")
        require(evidence["origin"] in {"synthetic_original", "original", "generated"}, "EVIDENCE_ORIGIN")
        require(evidence["relation"] in {"supports", "contradicts", "context"} and isinstance(evidence["claim"], str), "EVIDENCE_RELATION")
        require(isinstance(evidence["locator"], dict), "EVIDENCE_LOCATOR")
        require(not (evidence["origin"] == "generated" and evidence["relation"] == "supports"), "GENERATED_IS_NOT_INDEPENDENT_SUPPORT")
    for coverage in value["coverage"]:
        require(isinstance(coverage, dict) and set(coverage) == {"source_id", "mode", "query", "status", "truncated", "limitations"}, "COVERAGE_CONTRACT")
        require(coverage["mode"] in {"controlled", "strategy", "manual"} and coverage["status"] in {"completed", "empty", "failed", "unavailable"}, "COVERAGE_STATE")
        require(type(coverage["truncated"]) is bool and isinstance(coverage["query"], str) and isinstance(coverage["limitations"], str), "COVERAGE_FIELDS")
