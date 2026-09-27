# ============================================================
# 文件名: retrieval_executor_v0001.py
# 中文名: 查询策略受控执行器
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / execution
# 脚本定位: 模式二独立后端执行与资源边界
#
# 职责说明:
# - 将查询限制于可信数据源和读权限
#
# 本脚本做什么:
# - SQLite 授权器、DuckDB 受限快照、向量参数与受限 Cypher
#
# 本脚本不做什么:
# - 不生成策略、不读取 AI 密钥、不回退模式一
#
# 制度边界声明:
# - 输入不能改变连接和权限；Neo4j 只执行完整语法解析重建的只读方言
# - 子进程截止终止，返回有界结果；本机可信配置不是 OS 沙箱
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: retrieval_executor_v0001
# family: retrieval_executor
# role: constrained_query_executor
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/retrieval_executor_v0001.py
# input:
#   - trusted source configuration and untrusted query
# output:
#   - bounded query rows or stable rejection
# depends_on:
#   - experience_contract_v0001
#   - experience_store_v0001
#   - DuckDB optional
#   - Chroma optional
#   - Neo4j optional
# used_by:
#   - retrieval_strategy_v0001
# ============================================================

from __future__ import annotations

import json
import hashlib
import math
import multiprocessing
import os
import re
import shutil
import sqlite3
import time
import tempfile
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from .experience_contract_v0001 import ExperienceError, bounded, canonical, digest
    from .experience_store_v0001 import read_connection, sql_name
else:
    from experience_contract_v0001 import ExperienceError, bounded, canonical, digest
    from experience_store_v0001 import read_connection, sql_name


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "retrieval_executor"
SCRIPT_NAME = "retrieval_executor_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class QueryLimits:
    timeout_ms: int = 2000
    max_rows: int = 100
    max_bytes: int = 262144


# ============================================================
# 工具函数区
# ============================================================

def validate_source(source):
    if not isinstance(source, dict) or source.get("backend") not in {"sqlite", "duckdb", "chroma", "neo4j"}:
        raise ExperienceError("SOURCE_CONFIGURATION_INVALID")
    if source["backend"] == "neo4j":
        if not source.get("labels") or source.get("dialect") != "bounded_node_lookup_v1":
            raise ExperienceError("NEO4J_RESTRICTED_DIALECT_REQUIRED")
        return source
    if source["backend"] in {"sqlite", "duckdb"}:
        if not isinstance(source.get("tables"), dict) or not source["tables"]:
            raise ExperienceError("SOURCE_TABLE_ALLOWLIST_REQUIRED")
        for table, columns in source["tables"].items():
            sql_name(table)
            if not isinstance(columns, list) or not columns:
                raise ExperienceError("SOURCE_COLUMN_ALLOWLIST_REQUIRED")
            for column in columns:
                sql_name(column)
    path = Path(source.get("path", "")).resolve()
    if path.name.lower() in {"action.db", "control.sqlite3"} or not path.exists():
        raise ExperienceError("SOURCE_PATH_DENIED_OR_MISSING")
    if source["backend"] == "chroma":
        if not isinstance(source.get("collection"), str) or type(source.get("dimension")) is not int or not 1 <= source["dimension"] <= 8192:
            raise ExperienceError("VECTOR_CONFIGURATION_INVALID")
    return source


def _rows(cursor, limits):
    names = [item[0] for item in cursor.description]
    if len(names) != len(set(names)):
        raise ExperienceError("DUPLICATE_RESULT_COLUMNS")
    rows, size, truncated = [], 0, False
    for _ in range(limits.max_rows + 1):
        row = cursor.fetchone()
        if row is None:
            break
        if len(rows) == limits.max_rows:
            truncated = True
            break
        values = {}
        for name, value in zip(names, row):
            if isinstance(value, bytes):
                raise ExperienceError("BINARY_RESULT_DENIED")
            values[name] = value
        amount = len(canonical(values).encode(DEFAULT_ENCODING))
        if amount + size > limits.max_bytes:
            truncated = True
            break
        size += amount
        rows.append(values)
    return {"rows": rows, "truncated": truncated}


def _sqlite(source, query, parameters, limits):
    conn = read_connection(source["path"])
    try:
        conn.enable_load_extension(False)
        conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, limits.max_bytes)
        conn.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 32768)
        conn.setlimit(sqlite3.SQLITE_LIMIT_EXPR_DEPTH, 40)
        conn.setlimit(sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 10)
        conn.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
        allowed = source["tables"]
        def authorize(action, one, two, database, origin):
            if action == sqlite3.SQLITE_SELECT:
                return sqlite3.SQLITE_OK
            if action == sqlite3.SQLITE_READ and one in allowed and ((database == "main" and two in allowed[one]) or (two == "" and database in {None, "main"})):
                return sqlite3.SQLITE_OK
            if action == sqlite3.SQLITE_FUNCTION and (two or one or "").lower() in PURE_FUNCTIONS:
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY
        conn.set_authorizer(authorize)
        started = time.monotonic()
        conn.set_progress_handler(lambda: int((time.monotonic()-started)*1000 >= limits.timeout_ms), 500)
        return _rows(conn.execute(query, parameters), limits)
    except sqlite3.Error as exc:
        code = "QUERY_TIMEOUT" if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_INTERRUPT else "QUERY_REJECTED"
        raise ExperienceError(code) from None
    finally:
        conn.close()


def _duckdb(source, query, parameters, limits):
    import duckdb
    # The file is a dedicated read projection. No hidden tables/columns may be accessible.
    conn = duckdb.connect(str(Path(source["path"]).resolve()), read_only=True, config={"enable_external_access": "false", "autoload_known_extensions": "false", "autoinstall_known_extensions": "false", "memory_limit": "64MB", "threads": "1", "max_temp_directory_size": "0B"})
    try:
        actual = {}
        for table, column in conn.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='main'").fetchall():
            actual.setdefault(table, []).append(column)
        if {t: sorted(cols) for t,cols in actual.items()} != {t: sorted(cols) for t,cols in source["tables"].items()}:
            raise ExperienceError("DUCKDB_REQUIRES_DEDICATED_AUTHORIZED_PROJECTION")
        if conn.execute("SELECT count(*) FROM information_schema.views WHERE table_schema='main' AND table_catalog=current_database()").fetchone()[0]:
            raise ExperienceError("DUCKDB_VIEWS_DENIED")
        conn.execute("SET lock_configuration=true")
        statements = conn.extract_statements(query)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ExperienceError("QUERY_REJECTED")
        return _rows(conn.execute(query, parameters), limits)
    finally:
        conn.close()


def _chroma(source, query, parameters, limits):
    import chromadb
    from chromadb.config import Settings
    if query is not None or not isinstance(parameters, dict) or set(parameters) - {"query_embedding", "where", "n_results"}:
        raise ExperienceError("VECTOR_PARAMETERS_INVALID")
    vector = parameters.get("query_embedding")
    count = parameters.get("n_results", 20)
    if not isinstance(vector, list) or len(vector) != source["dimension"] or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector) or type(count) is not int or not 1 <= count <= limits.max_rows:
        raise ExperienceError("VECTOR_PARAMETERS_INVALID")
    # Chroma has no filesystem read-only client. Require a dedicated sealed snapshot,
    # copy it under the configured work root, and let the client touch only that copy.
    if source.get("snapshot_sealed") is not True or not source.get("snapshot_root"):
        raise ExperienceError("CHROMA_SEALED_SNAPSHOT_REQUIRED")
    origin = Path(source["path"]).resolve()
    work_root = Path(source["snapshot_root"]).resolve()
    if work_root == origin or work_root.is_relative_to(origin):
        raise ExperienceError("CHROMA_SNAPSHOT_ROOT_INVALID")
    files = list(origin.rglob("*"))
    if len(files) > 256 or any(p.is_symlink() or not p.resolve().is_relative_to(origin) for p in files) or sum(p.stat().st_size for p in files if p.is_file()) > 128*1024*1024:
        raise ExperienceError("CHROMA_SNAPSHOT_RESOURCE_LIMIT")
    work_root.mkdir(parents=True, exist_ok=True)
    snapshot = Path(tempfile.mkdtemp(prefix="chroma_read_", dir=work_root))
    shutil.copytree(origin, snapshot, dirs_exist_ok=True)
    # Copies are retained as explicit runtime artifacts: no recursive deletion on failure.
    client = chromadb.PersistentClient(path=str(snapshot), settings=Settings(anonymized_telemetry=False))
    collection = client.get_collection(source["collection"], embedding_function=None)
    result = collection.query(query_embeddings=[vector], n_results=count, where=parameters.get("where"), include=["metadatas", "distances"])
    rows = [{"object_id": object_id, "metadata": result["metadatas"][0][i], "distance": result["distances"][0][i]} for i,object_id in enumerate(result["ids"][0])]
    size, kept = 0, []
    for row in rows:
        size += len(canonical(row).encode(DEFAULT_ENCODING))
        if size > limits.max_bytes:
            break
        kept.append(row)
    return {"rows": kept, "truncated": len(kept) != len(rows), "evidence_status": "requires_source_verification"}


def compile_cypher(source, query, parameters, max_rows):
    """Parse a complete small read dialect and rebuild it; never send arbitrary Cypher."""
    name = r"[A-Za-z_][A-Za-z0-9_]*"
    grammar = rf"MATCH \(n:({name})\)(?: WHERE n\.({name}) = \$({name}))? RETURN (n\.{name}(?: AS {name})?(?:, n\.{name}(?: AS {name})?)*) LIMIT ([0-9]+)"
    match = re.fullmatch(grammar, query)
    if not match or not isinstance(parameters, dict):
        raise ExperienceError("CYPHER_DIALECT_REJECTED")
    label, field, parameter, returns, limit = match.groups()
    allowed = source["labels"].get(label)
    if not allowed or int(limit) < 1 or int(limit) > max_rows or set(parameters) != ({parameter} if parameter else set()):
        raise ExperienceError("CYPHER_SCOPE_REJECTED")
    if field and field not in allowed:
        raise ExperienceError("CYPHER_SCOPE_REJECTED")
    columns = []
    aliases = set()
    for item in returns.split(", "):
        parts = item.split(" AS ")
        column = parts[0][2:]
        alias = parts[1] if len(parts) == 2 else column
        if column not in allowed or alias in aliases:
            raise ExperienceError("CYPHER_SCOPE_REJECTED")
        aliases.add(alias)
        columns.append(f"n.`{column}` AS `{alias}`")
    compiled = f"MATCH (n:`{label}`)"
    if field:
        if not isinstance(parameters[parameter], (str, int, float, bool)):
            raise ExperienceError("CYPHER_PARAMETER_INVALID")
        compiled += f" WHERE n.`{field}` = ${parameter}"
    return compiled + " RETURN " + ", ".join(columns) + f" LIMIT {int(limit)}"


def _neo4j(source, query, parameters, limits):
    from neo4j import GraphDatabase, Query, READ_ACCESS
    compiled = compile_cypher(source, query, parameters, limits.max_rows)
    # Connections are trusted deployment configuration; query never selects URI or credentials.
    uri = os.environ.get(source["uri_env"], "")
    user = os.environ.get(source["user_env"], "")
    password = os.environ.get(source["password_env"], "")
    if not uri or not user or not password:
        raise ExperienceError("NEO4J_CONNECTION_NOT_CONFIGURED")
    with GraphDatabase.driver(uri, auth=(user, password), connection_timeout=limits.timeout_ms/1000, max_connection_pool_size=1) as driver:
        with driver.session(database=source["database"], default_access_mode=READ_ACCESS, fetch_size=min(limits.max_rows, 100)) as session:
            result = session.run(Query(compiled, timeout=limits.timeout_ms/1000), parameters)
            rows, size, truncated = [], 0, False
            for record in result:
                value = record.data()
                amount = len(canonical(value).encode(DEFAULT_ENCODING))
                if len(rows) >= limits.max_rows or size + amount > limits.max_bytes:
                    truncated = True; break
                rows.append(value); size += amount
            return {"rows": rows, "truncated": truncated, "dialect": "bounded_node_lookup_v1"}


def _worker(pipe, source, query, parameters, limits):
    try:
        if source["backend"] == "sqlite":
            result = _sqlite(source, query, parameters, limits)
        elif source["backend"] == "duckdb":
            result = _duckdb(source, query, parameters, limits)
        elif source["backend"] == "chroma":
            result = _chroma(source, query, parameters, limits)
        elif source["backend"] == "neo4j":
            result = _neo4j(source, query, parameters, limits)
        else:
            raise ExperienceError("BACKEND_NOT_AVAILABLE")
        mapping = source.get("evidence_mapping")
        if mapping and source["backend"] == "sqlite":
            table, id_col, text_col = mapping["table"], mapping["id_column"], mapping["text_column"]
            if table not in source["tables"] or not {id_col, text_col} <= set(source["tables"][table]):
                raise ExperienceError("EVIDENCE_MAPPING_NOT_AUTHORIZED")
            evidence, owners = [], {}
            for index, row in enumerate(result["rows"]):
                object_id = row.get(mapping["result_id_column"])
                if not isinstance(object_id, str):
                    evidence.append(None); continue
                restored = _sqlite(source, f"SELECT {sql_name(text_col)} AS raw_text FROM {sql_name(table)} WHERE {sql_name(id_col)}=?", [object_id], limits)
                if len(restored["rows"]) != 1 or not isinstance(restored["rows"][0]["raw_text"], str):
                    evidence.append(None); continue
                raw_text = restored["rows"][0]["raw_text"]
                ref = {"lineage": "action", "object_type": mapping["object_type"], "object_id": object_id, "source_hash": hashlib.sha256(raw_text.encode(DEFAULT_ENCODING)).hexdigest(), "locator": {"kind": "text", "unit": "unicode_codepoint", "char_start": 0, "char_end": len(raw_text)}}
                item = {"source_ref": ref}
                if object_id in owners:
                    item["expanded_context_reference"] = owners[object_id]
                else:
                    item.update(context_owner=index, text=raw_text)
                    owners[object_id] = index
                evidence.append(item)
            result["evidence"] = evidence
            # Bound combined rows plus recovered text, not each part independently.
            while len(canonical(result).encode(DEFAULT_ENCODING)) > limits.max_bytes and result["rows"]:
                result["rows"].pop(); result["evidence"].pop(); result["truncated"] = True
        pipe.send({"status": "completed", **result})
    except ExperienceError as exc:
        pipe.send({"status": "rejected", "error_type": str(exc)})
    except Exception:
        pipe.send({"status": "failed", "error_type": "BACKEND_EXECUTION_FAILED"})
    finally:
        pipe.close()


# ============================================================
# 默认映射
# ============================================================

PURE_FUNCTIONS = {"count", "sum", "min", "max", "avg", "coalesce", "ifnull", "nullif", "lower", "upper", "length", "substr", "substring", "trim", "abs", "round", "like", "glob", "json_extract", "json_type"}


# ============================================================
# 核心业务组件
# ============================================================

def execute_query(source, query, parameters, limits=QueryLimits(), cancel=None):
    validate_source(source)
    bounded({"query": query, "parameters": parameters}, max_bytes=65536)
    if type(limits.timeout_ms) is not int or not 1 <= limits.timeout_ms <= 30000 or not 1 <= limits.max_rows <= 100 or not 1024 <= limits.max_bytes <= 1048576:
        raise ExperienceError("QUERY_LIMITS_INVALID")
    if source["backend"] != "chroma" and (not isinstance(query, str) or len(query) > 32768 or not isinstance(parameters, (dict, list))):
        raise ExperienceError("QUERY_PARAMETERS_INVALID")
    if cancel and cancel():
        return {"status": "cancelled", "error_type": "QUERY_CANCELLED"}
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_worker, args=(sender, source, query, parameters, limits), daemon=True)
    started = time.monotonic()
    process.start(); sender.close()
    try:
        while True:
            if cancel and cancel():
                return {"status": "cancelled", "error_type": "QUERY_CANCELLED"}
            if receiver.poll(0.01):
                try:
                    return receiver.recv()
                except EOFError:
                    return {"status": "failed", "error_type": "WORKER_INTERRUPTED"}
            if (time.monotonic()-started)*1000 >= limits.timeout_ms:
                return {"status": "timeout", "error_type": "QUERY_TIMEOUT"}
            if not process.is_alive():
                return {"status": "failed", "error_type": "WORKER_INTERRUPTED"}
    finally:
        if process.is_alive():
            process.terminate()
        process.join(2)
        if process.is_alive():
            process.kill(); process.join(2)
        receiver.close()


def describe_sources(sources):
    return {sid: {"backend": source["backend"], "tables": source.get("tables"), "labels": source.get("labels"), "dialect": source.get("dialect"), "collection": source.get("collection"), "dimension": source.get("dimension"), "semantics": source.get("semantics", {}), "permission_fingerprint": digest({k:v for k,v in source.items() if k != "path"})} for sid,source in sources.items()}
