# ============================================================
# 文件名: action_sql_graph_projection_v0001.py
# 中文名: Action 业务库图投影构造器
# 版本号: v0001
#
# 主层级: action
# 层级: derivation / projection
# 脚本定位: 从业务 SQLite 的本批声明构造可回查图投影
#
# 职责说明:
# - 以业务库为事实源，复用原始对象身份和来源坐标
#
# 本脚本做什么:
# - 在只读事务内读取本批对象及外键依赖，验证来源并生成确定性投影
#
# 本脚本不做什么:
# - 不写数据库，不从文本猜测实体关系，不读取开发登记库
#
# 制度边界声明:
# - 已完成隔离 Neo4j 验收；正式运行须显式绑定业务库和图数据库身份
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: action_sql_graph_projection_v0001
# family: action_sql_graph_projection
# role: sqlite_graph_projection_builder
# version: v0001
# status: active
# entry_point: scripts/action/derivation/action_sql_graph_projection_v0001.py
# input:
#   - Action anchor completion manifest and business SQLite
# output:
#   - deterministic graph projection with source evidence
# depends_on:
#   - Python stdlib
# used_by:
#   - sync_sql_to_graph_v0001
# ============================================================

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3

import yaml


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "action_sql_graph_projection"
SCRIPT_NAME = "action_sql_graph_projection_v0001"
SCRIPT_VERSION = "v0001"
TABLES = {
    "concept": ("concept_units", "unit_text_id"),
    "instance": ("instance_units", "instance_id"),
    "attribute": ("unit_attributes", "attr_id"),
}
REQUIRED = {
    "concept": {"unit_text_id", "unit_text", "content_hash", "schema_version", "run_id"},
    "instance": {"instance_id", "unit_text_id", "asset_id", "path", "char_start", "char_end", "content", "content_hash", "schema_version", "run_id"},
    "attribute": {"attr_id", "object_type", "object_id", "evidence_ref", "attr_key", "attr_value", "run_id"},
}


# ============================================================
# 异常类型
# ============================================================

class ProjectionError(RuntimeError):
    pass


# ============================================================
# 工具函数区
# ============================================================

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode(DEFAULT_ENCODING)).hexdigest()


def resolve(root, value):
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def key(kind, identity):
    if kind not in TABLES or not isinstance(identity, str) or not identity:
        raise ProjectionError("invalid_object_identity")
    return canonical([kind, identity])


def read_json(path):
    return json.loads(path.read_text(encoding=DEFAULT_ENCODING))


# ============================================================
# 核心业务组件
# ============================================================

def build_projection(anchor_path: Path, project_root: Path, *, max_objects: int = 100000):
    """Read only selected persisted objects, including already-existing declarations."""
    if max_objects < 1:
        raise ProjectionError("invalid_object_limit")
    anchor_path = anchor_path.resolve()
    root = project_root.resolve()
    manifest = read_json(anchor_path)
    if (manifest.get("pipeline") not in {"action_anchor_persistence_pipeline", "action_sql_graph_selection"}
            or manifest.get("status") != "completed"):
        raise ProjectionError("completed_action_anchor_required")
    source = resolve(root, manifest["sql_write"]["action_db"])
    if source == (root / "sql/action.db").resolve() or source.name.lower() == "action.db":
        raise ProjectionError("development_registry_forbidden")
    if not source.is_file():
        raise ProjectionError("business_database_missing")
    writer_path = resolve(root, manifest["sql_write"]["writer_config"])
    writer = yaml.safe_load(writer_path.read_text(encoding=DEFAULT_ENCODING))
    if resolve(root, writer["connection"]["sqlite"]["path"]) != source:
        raise ProjectionError("writer_database_mismatch")
    if any(writer["tables"].get(kind) != table for kind, (table, _) in TABLES.items()):
        raise ProjectionError("unsupported_writer_table_mapping")
    # Anchor write receipts provide actual persisted IDs, including duplicate/existing rows.
    receipt_path = resolve(root, manifest["sql_write"]["write_results"])
    raw = receipt_path.read_bytes()
    selected = {kind: set() for kind in TABLES}
    receipt_counts = {kind: 0 for kind in TABLES}
    for line in raw.decode(DEFAULT_ENCODING).splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ProjectionError("receipt_row_must_be_object")
        kind = row.get("kind")
        if kind == "run_log":
            continue
        if kind not in TABLES or row.get("object") != kind or row.get("status") not in {"inserted", "existing", "duplicate"}:
            raise ProjectionError("invalid_anchor_write_receipt")
        identity = row.get("object_id")
        key(kind, identity)
        selected[kind].add(identity)
        receipt_counts[kind] += 1
        if sum(map(len, selected.values())) > max_objects:
            raise ProjectionError("projection_object_limit_exceeded")
    if any(receipt_counts[kind] != manifest["declarations"][kind]["rows"] for kind in TABLES):
        raise ProjectionError("anchor_receipt_count_mismatch")
    conn = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    nodes = {}
    links = {}
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        for kind, (table, _) in TABLES.items():
            columns = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if not REQUIRED[kind].issubset(columns):
                raise ProjectionError("business_schema_mismatch:" + kind)

        def fetch(kind, identity):
            object_key = key(kind, identity)
            if object_key in nodes:
                return object_key
            if len(nodes) >= max_objects:
                raise ProjectionError("projection_object_limit_exceeded")
            table, pk = TABLES[kind]
            rows = conn.execute(f"SELECT * FROM {table} WHERE {pk}=?", (identity,)).fetchall()
            if len(rows) != 1:
                raise ProjectionError("missing_or_ambiguous_sql_object:" + kind)
            properties = dict(rows[0])
            if any(v is not None and not isinstance(v, (str, int, float)) for v in properties.values()):
                raise ProjectionError("unsupported_sql_value")
            nodes[object_key] = {"kind": kind, "native_id": identity, "table": table, "properties": properties}
            if kind == "instance":
                parent = fetch("concept", properties["unit_text_id"])
                link(object_key, parent, "INSTANCE_OF")
                start, end = properties["char_start"], properties["char_end"]
                if not isinstance(start, int) or not isinstance(end, int) or not 0 <= start <= end:
                    raise ProjectionError("invalid_source_coordinates")
            elif kind == "attribute":
                owner_kind = properties["object_type"]
                if owner_kind not in {"concept", "instance"}:
                    raise ProjectionError("unsupported_attribute_owner")
                owner = fetch(owner_kind, properties["object_id"])
                link(object_key, owner, "ATTRIBUTE_OF")
            return object_key

        def link(source_key, target_key, relation):
            link_key = canonical([source_key, relation, target_key])
            links[link_key] = {"source": source_key, "target": target_key, "kind": relation,
                               "method": "sqlite_foreign_key_projection_v0001"}

        for kind in TABLES:
            for identity in sorted(selected[kind]):
                fetch(kind, identity)
    finally:
        conn.close()
    payload = {"schema_version": "action_sql_graph_projection_v0001", "nodes": nodes, "links": links}
    return {**payload, "projection_hash": digest(payload), "source": {
        "database": str(source), "writer_config": str(writer_path),
        "anchor_manifest": str(anchor_path), "anchor_sha256": hashlib.sha256(anchor_path.read_bytes()).hexdigest(),
        "anchor_run_id": manifest["run_id"], "write_results": str(receipt_path),
        "write_results_sha256": hashlib.sha256(raw).hexdigest(),
        "selected_counts": {k: len(v) for k, v in selected.items()},
        "read_transaction": "sqlite_readonly_snapshot",
        "writer_config_sha256": hashlib.sha256(writer_path.read_bytes()).hexdigest(),
    }}


def verify_source(projection, project_root):
    """Repeat the same bounded read; fail if the persisted source selection changed."""
    current = build_projection(Path(projection["source"]["anchor_manifest"]), project_root)
    if current != projection:
        raise ProjectionError("source_changed_since_projection")
    return {"status": "verified", "objects": len(current["nodes"]), "links": len(current["links"])}


def select_existing_assets(database, writer_config, semantic_input, output_dir, project_root, run_id):
    """Bounded replay for no-new-content: SQL facts for the actual input asset IDs."""
    database, semantic_input, output_dir = Path(database).resolve(), Path(semantic_input).resolve(), Path(output_dir).resolve()
    if database.name.lower() == "action.db":
        raise ProjectionError("development_registry_forbidden")
    if output_dir.exists():
        raise ProjectionError("existing_selection_output_forbidden")
    assets = set()
    for line in semantic_input.read_text(encoding=DEFAULT_ENCODING).splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or not isinstance(row.get("asset_id"), str) or not row["asset_id"]:
            raise ProjectionError("semantic_source_asset_missing")
        assets.add(row["asset_id"])
    if not assets or len(assets) > 100000:
        raise ProjectionError("invalid_existing_asset_selection")
    conn = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    selected = {kind: set() for kind in TABLES}
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        for asset_id in sorted(assets):
            found = False
            for instance_id, concept_id in conn.execute("SELECT instance_id,unit_text_id FROM instance_units WHERE asset_id=?", (asset_id,)):
                found = True
                selected["instance"].add(instance_id)
                selected["concept"].add(concept_id)
                if sum(map(len, selected.values())) > 100000:
                    raise ProjectionError("projection_object_limit_exceeded")
            if not found:
                raise ProjectionError("no_persisted_instances_for_existing_input_asset")
        for kind in ("concept", "instance"):
            for identity in sorted(selected[kind]):
                for row in conn.execute("SELECT attr_id FROM unit_attributes WHERE object_type=? AND object_id=?", (kind, identity)):
                    selected["attribute"].add(row[0])
                    if sum(map(len, selected.values())) > 100000:
                        raise ProjectionError("projection_object_limit_exceeded")
    finally:
        conn.close()
    output_dir.mkdir(parents=True)
    receipts = output_dir / "selected_sql_objects.jsonl"
    receipts.write_text("\n".join(canonical({"kind": k, "object": k, "object_id": identity, "status": "existing"})
                                  for k in TABLES for identity in sorted(selected[k])), encoding=DEFAULT_ENCODING)
    selection = {"pipeline": "action_sql_graph_selection", "status": "completed", "run_id": run_id,
                 "selection_type": "existing_asset_replay", "read_only_source": True,
                 "semantic_source": str(semantic_input), "semantic_sha256": hashlib.sha256(semantic_input.read_bytes()).hexdigest(),
                 "sql_write": {"action_db": str(database), "writer_config": str(writer_config), "write_results": str(receipts)},
                 "declarations": {k: {"rows": len(v)} for k, v in selected.items()}}
    path = output_dir / "selection_manifest.json"
    path.write_text(canonical(selection), encoding=DEFAULT_ENCODING)
    return path
