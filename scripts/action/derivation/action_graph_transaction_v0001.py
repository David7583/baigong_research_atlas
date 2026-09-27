# ============================================================
# 文件名: action_graph_transaction_v0001.py
# 中文名: Action 图批次事务与补偿
# 版本号: v0001
#
# 主层级: action
# 层级: derivation / transaction
# 脚本定位: 管理图批次提交、验证、前镜像补偿与重启恢复
#
# 职责说明:
# - 每次图投影及其补偿日志在同一 Neo4j 事务内提交
#
# 本脚本做什么:
# - 对受管命名空间加锁，保存前镜像，核验写后状态并保护外部修改
#
# 本脚本不做什么:
# - 不删除原始数据，不清库，不自动建 schema，不宣称跨库 ACID
#
# 制度边界声明:
# - 显式数据库身份绑定；只写所选命名空间，保留持久恢复证据
# - 锁只约束使用本协议的 writer；外部写入冲突阻止补偿
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: action_graph_transaction_v0001
# family: action_graph_transaction
# role: graph_batch_compensation
# version: v0001
# status: active
# entry_point: scripts/action/derivation/action_graph_transaction_v0001.py
# input:
#   - explicit graph connection and deterministic SQL projection
# output:
#   - applied committed compensated or conflict batch state
# depends_on:
#   - neo4j
#   - action_sql_graph_projection_v0001
# used_by:
#   - sync_sql_to_graph_v0001
# ============================================================

from __future__ import annotations

import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

import yaml

from action_sql_graph_projection_v0001 import canonical, digest


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "action_graph_transaction"
SCRIPT_NAME = "action_graph_transaction_v0001"
SCRIPT_VERSION = "v0001"
MAX_JOURNAL_BYTES = 64 * 1024 * 1024
CONSTRAINTS = {
    "action_sql_object_key": ("ActionSqlObject", "object_key"),
    "action_sql_batch_key": ("ActionSqlBatch", "batch_key"),
    "action_sql_control_key": ("ActionSqlControl", "source_id"),
}


# ============================================================
# 异常类型
# ============================================================

class GraphError(RuntimeError):
    pass


# ============================================================
# 工具函数区
# ============================================================

def load_config(path: Path):
    raw = yaml.safe_load(path.read_text(encoding=DEFAULT_ENCODING))
    if not isinstance(raw, dict) or raw.get("schema_version") != "action_sql_graph_connection_v0001":
        raise GraphError("graph_connection_contract_required")
    cfg = raw.get("connection", {})
    for name in ("uri", "database", "database_id", "user", "password_env", "source_id"):
        if not isinstance(cfg.get(name), str) or not cfg[name].strip():
            raise GraphError("missing_connection_field:" + name)
    if "password" in cfg or "password" in raw:
        raise GraphError("plaintext_password_not_accepted")
    uri = urlsplit(cfg["uri"])
    if uri.scheme not in {"bolt", "bolt+s", "neo4j", "neo4j+s"} or not uri.hostname or uri.username or uri.password:
        raise GraphError("invalid_graph_uri")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", cfg["source_id"]):
        raise GraphError("invalid_source_id")
    return {name: cfg[name] for name in ("uri", "database", "database_id", "user", "password_env", "source_id")}


def object_properties(source_id, object_key, node):
    return {**{k: v for k, v in node["properties"].items() if v is not None},
            "object_key": canonical([source_id, object_key]), "source_id": source_id,
            "object_kind": node["kind"], "native_id": node["native_id"], "source_table": node["table"],
            "source_row_json": canonical(node["properties"])}


def desired_records(source_id, projection):
    records = {}
    for object_key, node in projection["nodes"].items():
        records["n:" + object_key] = object_properties(source_id, object_key, node)
    for link_key, link in projection["links"].items():
        records["r:" + link_key] = {**link, "link_key": canonical([source_id, link_key]), "source_id": source_id}
    return records


def check_records(store, source_id, records):
    for name, expected in records.items():
        if store.get_record(source_id, name) != expected:
            raise GraphError("graph_postflight_or_concurrent_change:" + name[:2])


# ============================================================
# 核心业务组件
# ============================================================

def apply_batch(store, source_id, batch_id, projection, *, fail_after=None):
    """Caller must execute this entire function inside one database transaction."""
    store.lock(source_id, batch_id)
    batch = store.get_batch(source_id, batch_id)
    after = desired_records(source_id, projection)
    identity = digest({"after": after, "source": projection["source"]})
    if batch:
        if batch["identity"] != identity:
            raise GraphError("batch_id_reused_with_different_input")
        if batch["state"] not in {"APPLIED", "COMMITTED"}:
            raise GraphError("batch_already_compensated_use_new_batch")
        check_records(store, source_id, after)
        if batch["state"] == "COMMITTED":
            store.unlock(source_id, batch_id)
        return {"status": batch["state"].lower(), "duplicate": True, "batch_id": batch_id}
    before = {name: store.get_record(source_id, name) for name in after}
    for name, props in after.items():
        if name.startswith("r:"):
            for endpoint in (props["source"], props["target"]):
                if "n:" + endpoint not in after:
                    raise GraphError("projection_endpoint_missing")
    journal = {"state": "APPLIED", "identity": identity, "before": before, "after": after,
               "source": projection["source"], "projection_hash": projection["projection_hash"]}
    if len(canonical(journal).encode(DEFAULT_ENCODING)) > MAX_JOURNAL_BYTES:
        raise GraphError("graph_journal_size_limit_exceeded")
    for index, (name, props) in enumerate(after.items(), 1):
        store.put_record(source_id, name, props)
        if fail_after == index:
            raise GraphError("injected_graph_apply_failure")
    check_records(store, source_id, after)
    store.put_batch(source_id, batch_id, journal)
    return {"status": "applied", "batch_id": batch_id, "duplicate": False,
            "objects": len(projection["nodes"]), "links": len(projection["links"])}


def finish_batch(store, source_id, batch_id, decision):
    if decision not in {"commit", "compensate"}:
        raise GraphError("invalid_batch_decision")
    store.lock(source_id, batch_id)
    batch = store.get_batch(source_id, batch_id)
    if not batch:
        store.unlock(source_id, batch_id)
        return {"status": "not_applied", "batch_id": batch_id}
    terminal = "COMMITTED" if decision == "commit" else "COMPENSATED"
    if batch["state"] == terminal:
        store.unlock(source_id, batch_id)
        return {"status": terminal.lower(), "batch_id": batch_id, "duplicate": True}
    if batch["state"] != "APPLIED":
        raise GraphError("conflicting_terminal_decision")
    check_records(store, source_id, batch["after"])
    if decision == "compensate":
        # Relationships first, then nodes; no DETACH DELETE, foreign edges block undo.
        for name in sorted(batch["before"], key=lambda n: 0 if n.startswith("r:") else 1):
            previous = batch["before"][name]
            if previous is None:
                store.delete_record(source_id, name)
            else:
                store.put_record(source_id, name, previous)
        check_records(store, source_id, batch["before"])
    batch["state"] = terminal
    store.put_batch(source_id, batch_id, batch)
    store.unlock(source_id, batch_id)
    return {"status": terminal.lower(), "batch_id": batch_id}


class Neo4jStore:
    """Parameterized Cypher repository bound to one explicit driver transaction."""

    def __init__(self, tx):
        self.tx = tx

    def lock(self, source_id, batch_id):
        rows = list(self.tx.run(
            "MERGE (c:ActionSqlControl {source_id:$source}) "
            "ON CREATE SET c.owner='' SET c.fence=coalesce(c.fence,0)+1 RETURN c.owner AS owner",
            source=source_id))
        if len(rows) != 1 or rows[0]["owner"] not in {"", batch_id}:
            raise GraphError("graph_namespace_has_pending_batch")
        self.tx.run("MATCH (c:ActionSqlControl {source_id:$source}) SET c.owner=$batch",
                    source=source_id, batch=batch_id).consume()

    def unlock(self, source_id, batch_id):
        self.tx.run("MATCH (c:ActionSqlControl {source_id:$source,owner:$batch}) SET c.owner=''",
                    source=source_id, batch=batch_id).consume()

    def get_batch(self, source_id, batch_id):
        rows = list(self.tx.run("MATCH (b:ActionSqlBatch {batch_key:$key}) RETURN b.payload AS payload",
                                key=canonical([source_id, batch_id])))
        if len(rows) > 1:
            raise GraphError("ambiguous_batch_identity")
        return json.loads(rows[0]["payload"]) if rows else None

    def put_batch(self, source_id, batch_id, value):
        self.tx.run("MERGE (b:ActionSqlBatch {batch_key:$key}) SET b.payload=$payload, b.state=$state",
                    key=canonical([source_id, batch_id]), payload=canonical(value), state=value["state"]).consume()

    def get_record(self, source_id, name):
        if name.startswith("n:"):
            query = "MATCH (n:ActionSqlObject {object_key:$key}) RETURN properties(n) AS properties"
        else:
            query = ("MATCH (a)-[n:ACTION_SQL_LINK {link_key:$key}]->(b) "
                     "RETURN properties(n) AS properties,a.object_key AS source,b.object_key AS target")
        rows = list(self.tx.run(query, key=canonical([source_id, name[2:]])))
        if len(rows) > 1:
            raise GraphError("ambiguous_graph_identity")
        if rows and name.startswith("r:"):
            props = rows[0]["properties"]
            if any(rows[0][side] != canonical([source_id, props[side]]) for side in ("source", "target")):
                raise GraphError("graph_relationship_endpoint_changed")
        return dict(rows[0]["properties"]) if rows else None

    def put_record(self, source_id, name, properties):
        if name.startswith("n:"):
            query = "MERGE (n:ActionSqlObject {object_key:$key}) SET n=$properties RETURN count(n) AS count"
            params = {"key": canonical([source_id, name[2:]]), "properties": properties}
        else:
            query = ("MATCH (a:ActionSqlObject {object_key:$source}), (b:ActionSqlObject {object_key:$target}) "
                     "MERGE (a)-[n:ACTION_SQL_LINK {link_key:$key}]->(b) SET n=$properties RETURN count(n) AS count")
            params = {"source": canonical([source_id, properties["source"]]),
                      "target": canonical([source_id, properties["target"]]),
                      "key": canonical([source_id, name[2:]]), "properties": properties}
        row = self.tx.run(query, **params).single()
        if row is None or row["count"] != 1:
            raise GraphError("graph_write_cardinality_mismatch")

    def delete_record(self, source_id, name):
        if name.startswith("n:"):
            query = "MATCH (n:ActionSqlObject {object_key:$key}) DELETE n"
        else:
            query = "MATCH ()-[n:ACTION_SQL_LINK {link_key:$key}]->() DELETE n"
        self.tx.run(query, key=canonical([source_id, name[2:]])).consume()


class GraphClient:
    """No automatic DDL, no default database, no credential text in exceptions."""

    def __init__(self, config):
        self.config = config

    def execute(self, operation, *, batch_id, projection=None, fail_after=None):
        if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", batch_id):
            raise GraphError("invalid_graph_batch_id")
        password = os.environ.get(self.config["password_env"])
        if not password:
            raise GraphError("graph_password_environment_missing")
        from neo4j import GraphDatabase
        try:
            with GraphDatabase.driver(self.config["uri"], auth=(self.config["user"], password),
                                      connection_timeout=10) as driver:
                with driver.session(database=self.config["database"]) as session:
                    identity = session.run("CALL db.info() YIELD id, name RETURN id, name").single()
                    if identity is None or identity["id"] != self.config["database_id"] or identity["name"] != self.config["database"]:
                        raise GraphError("graph_database_identity_mismatch")
                    constraints = list(session.run("SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties, type RETURN name, labelsOrTypes, properties, type"))
                    for name, (label, prop) in CONSTRAINTS.items():
                        if not any(r["name"] == name and r["labelsOrTypes"] == [label]
                                   and r["properties"] == [prop] and "UNIQUENESS" in r["type"] for r in constraints):
                            raise GraphError("required_graph_constraint_missing:" + name)
                    if operation == "preflight":
                        return {"status": "ready", "database": self.config["database"]}
                    def work(tx):
                        store = Neo4jStore(tx)
                        if operation == "apply":
                            return apply_batch(store, self.config["source_id"], batch_id, projection, fail_after=fail_after)
                        if operation == "verify":
                            batch = store.get_batch(self.config["source_id"], batch_id)
                            if not batch or batch["state"] not in {"APPLIED", "COMMITTED"}:
                                raise GraphError("verifiable_graph_batch_missing")
                            check_records(store, self.config["source_id"], batch["after"])
                            return {"status": "verified", "source": batch["source"],
                                    "projection_hash": batch["projection_hash"], "batch_state": batch["state"]}
                        return finish_batch(store, self.config["source_id"], batch_id, operation)
                    return session.execute_read(work) if operation == "verify" else session.execute_write(work)
        except GraphError:
            raise
        except Exception as exc:
            raise GraphError("neo4j_operation_failed:" + type(exc).__name__) from None
