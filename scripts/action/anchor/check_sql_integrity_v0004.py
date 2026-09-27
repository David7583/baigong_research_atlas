# ============================================================
# 文件名: check_sql_integrity_v0004.py
# 中文名: 完整坐标 SQL 一致性校验脚本
# 版本号: v0004
#
# 主层级: action
# 子层级: persistence / postflight
# 脚本定位: 在写入行动端 SQL 后，验证 concept / instance / attribute 三表关系一致性与数据锚完整性
# 可更新: True
#
# 职责说明:
# - 连接行动端 SQLite 数据库，执行结构性一致性检查
# - 可选连接数据层 SQLite 数据库，执行 instance.observed_at 的现实锚点存在性复核
# - 输出结构化 sql_integrity_report.json，供审计与下游流程闸门使用
#
# 本脚本做什么:
# - 不写入 SQL，不修改任何表
# - 不推断，不做语义裁决
#
# 本脚本不做什么:
# - 不替代 validate_ingest_payload_v0003.py 的入库前载荷校验
# - 不替代 replay_ingest_run_v0004.py 的可重放性校验
#
# 制度边界声明:
# - PASS 表示数据库满足关键结构不变量
# - FAIL 表示出现断裂引用、重复身份或缺失现实锚点，需阻断后续流程
# - orphan_concepts（无实例概念）默认仅 warning
#
# 统计守恒:
# - checked_invariants = passed + failed + warned
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: check_sql_integrity
# family: check_sql_integrity
# role: sql_integrity_checker
# version: v0004
# status: active
# entry_point: scripts/action/anchor/check_sql_integrity_v0004.py
# input:
#   - action layer SQLite db (--action-db)
#   - optional data layer SQLite db (--data-db / --data-table)
# output:
#   - sql_integrity_report (json)
# depends_on:
#   - sql_writer_v0004
# used_by:
#   - (无法确定，需人工填写；header 仅述"供审计与下游流程闸门使用")
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


SCRIPT_NAME = "check_sql_integrity_v0004.py"
SCRIPT_VERSION = "v0004"
ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_ENCODING = "utf-8"


# ============================================================
# 工具函数
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _sha256_hex_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _archive_existing(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    archive_dir = path.parent / ARCHIVE_DIRNAME
    _ensure_dir(archive_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived = archive_dir / f"{path.stem}__{ts}{path.suffix}"
    shutil.move(str(path), str(archived))
    return archived


def _atomic_write_json(path: Path, obj: Any) -> Tuple[int, str]:
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = _safe_json_dumps(obj).encode(DEFAULT_ENCODING)
    with tmp.open("wb") as f:
        f.write(data)
        f.write(b"\n")
    shutil.move(str(tmp), str(path))
    return len(data), _sha256_hex_bytes(data)


def _new_issue(*, level: str, code: str, where: Dict[str, Any], detail: str, hint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {
        "level": level,   # ERROR | WARNING
        "code": code,
        "where": where,
        "detail": detail,
    }
    if hint is not None:
        out["hint"] = hint
    return out


def _connect_sqlite(db_path: Path, timeout: int) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)).fetchone()
    return row is not None


# ============================================================
# 核心校验
# ============================================================

def _fetch_samples(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...], sample_limit: int) -> List[Dict[str, Any]]:
    rows = conn.execute(sql, params).fetchmany(sample_limit)
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        out.append(d)
    return out


def check_integrity(
    *,
    action_db: Path,
    concept_table: str,
    instance_table: str,
    attribute_table: str,
    # fields
    concept_id_field: str,
    instance_id_field: str,
    instance_concept_id_field: str,
    attr_object_type_field: str,
    attr_object_id_field: str,
    attr_key_field: str,
    attr_value_field: str,
    # observed_at fields in instance table
    inst_asset_id_field: str,
    inst_path_field: str,
    inst_value_index_field: str,
    inst_segment_index_field: str,
    inst_sentence_index_field: str,
    inst_char_start_field: str,
    inst_char_end_field: str,
    # optional data layer anchor check
    data_db: Optional[Path],
    data_table: Optional[str],
    data_asset_id_field: str,
    data_path_field: str,
    data_value_index_field: str,
    data_segment_index_field: str,
    data_sentence_index_field: str,
    data_char_start_field: str,
    data_char_end_field: str,
    sqlite_timeout: int,
    sample_limit: int,
    warn_orphan_concepts: bool,
) -> Dict[str, Any]:
    started_at = _utc_now_iso()
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    if not action_db.exists():
        return {
            "status": "FAIL",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "started_at": started_at,
            "finished_at": _utc_now_iso(),
            "summary": {"errors": 1, "warnings": 0},
            "errors": [
                _new_issue(
                    level="ERROR",
                    code="action_db_missing",
                    where={"file": str(action_db)},
                    detail="action sqlite db not found",
                )
            ],
            "warnings": [],
        }

    uconn: Optional[sqlite3.Connection] = None
    dconn: Optional[sqlite3.Connection] = None

    broken_instances = 0
    broken_attributes = 0
    duplicate_concepts = 0
    duplicate_instances = 0
    duplicate_instance_by_anchor = 0
    duplicate_instance_by_anchor_rows = 0
    duplicate_instance_by_anchor_excess_rows = 0
    orphan_concepts = 0
    missing_anchor = 0

    try:
        uconn = _connect_sqlite(action_db, timeout=sqlite_timeout)
        uconn.execute("PRAGMA foreign_keys = ON;")

        # table existence checks
        for tname, code in [
            (concept_table, "missing_concept_table"),
            (instance_table, "missing_instance_table"),
            (attribute_table, "missing_attribute_table"),
        ]:
            if not _table_exists(uconn, tname):
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code=code,
                        where={"db": str(action_db), "table": tname},
                        detail="required table not found",
                    )
                )

        if errors:
            return {
                "status": "FAIL",
                "script": SCRIPT_NAME,
                "version": SCRIPT_VERSION,
                "started_at": started_at,
                "finished_at": _utc_now_iso(),
                "inputs": {"action_db": str(action_db)},
                "summary": {"errors": len(errors), "warnings": len(warnings)},
                "errors": errors,
                "warnings": warnings,
            }

        # 1) instance -> concept broken references
        sql_broken_inst = (
            f"SELECT i.{instance_id_field} AS instance_id, i.{instance_concept_id_field} AS concept_id "
            f"FROM {instance_table} i "
            f"LEFT JOIN {concept_table} c ON c.{concept_id_field} = i.{instance_concept_id_field} "
            f"WHERE c.{concept_id_field} IS NULL "
            f"LIMIT {sample_limit}"
        )
        samples = _fetch_samples(uconn, sql_broken_inst, (), sample_limit)
        broken_instances = len(samples)
        if broken_instances > 0:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="broken_instance_concept_reference",
                    where={"db": str(action_db), "table": instance_table},
                    detail="one or more instances reference missing concept_id",
                    hint={"sample": samples},
                )
            )

        # 2) attribute -> object broken references
        # concept attributes missing
        sql_attr_missing_concept = (
            f"SELECT a.{attr_object_type_field} AS object_type, a.{attr_object_id_field} AS object_id, "
            f"a.{attr_key_field} AS attr_key "
            f"FROM {attribute_table} a "
            f"LEFT JOIN {concept_table} c ON c.{concept_id_field} = a.{attr_object_id_field} "
            f"WHERE a.{attr_object_type_field} = 'concept' AND c.{concept_id_field} IS NULL "
            f"LIMIT {sample_limit}"
        )
        samp1 = _fetch_samples(uconn, sql_attr_missing_concept, (), sample_limit)

        # instance attributes missing
        sql_attr_missing_instance = (
            f"SELECT a.{attr_object_type_field} AS object_type, a.{attr_object_id_field} AS object_id, "
            f"a.{attr_key_field} AS attr_key "
            f"FROM {attribute_table} a "
            f"LEFT JOIN {instance_table} i ON i.{instance_id_field} = a.{attr_object_id_field} "
            f"WHERE a.{attr_object_type_field} = 'instance' AND i.{instance_id_field} IS NULL "
            f"LIMIT {sample_limit}"
        )
        samp2 = _fetch_samples(uconn, sql_attr_missing_instance, (), sample_limit)

        broken_attributes = len(samp1) + len(samp2)
        if broken_attributes > 0:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="broken_attribute_object_reference",
                    where={"db": str(action_db), "table": attribute_table},
                    detail="one or more attributes reference missing concept/instance",
                    hint={"missing_concept_sample": samp1, "missing_instance_sample": samp2},
                )
            )

        # 3) duplicate identities: concept_id and instance_id duplicates
        sql_dup_concept = (
            f"SELECT {concept_id_field} AS concept_id, COUNT(1) AS cnt "
            f"FROM {concept_table} "
            f"GROUP BY {concept_id_field} "
            f"HAVING cnt > 1 "
            f"LIMIT {sample_limit}"
        )
        dup_c = _fetch_samples(uconn, sql_dup_concept, (), sample_limit)
        duplicate_concepts = len(dup_c)
        if duplicate_concepts > 0:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="duplicate_concept_id",
                    where={"db": str(action_db), "table": concept_table},
                    detail="duplicate concept_id detected in concept table",
                    hint={"sample": dup_c},
                )
            )

        sql_dup_instance = (
            f"SELECT {instance_id_field} AS instance_id, COUNT(1) AS cnt "
            f"FROM {instance_table} "
            f"GROUP BY {instance_id_field} "
            f"HAVING cnt > 1 "
            f"LIMIT {sample_limit}"
        )
        dup_i = _fetch_samples(uconn, sql_dup_instance, (), sample_limit)
        duplicate_instances = len(dup_i)
        if duplicate_instances > 0:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="duplicate_instance_id",
                    where={"db": str(action_db), "table": instance_table},
                    detail="duplicate instance_id detected in instance table",
                    hint={"sample": dup_i},
                )
            )

        # 4) orphan concepts (warning by default)
        sql_orphan = (
            f"SELECT c.{concept_id_field} AS concept_id "
            f"FROM {concept_table} c "
            f"LEFT JOIN {instance_table} i ON i.{instance_concept_id_field} = c.{concept_id_field} "
            f"WHERE i.{instance_id_field} IS NULL "
            f"LIMIT {sample_limit}"
        )
        orphan = _fetch_samples(uconn, sql_orphan, (), sample_limit)
        orphan_concepts = len(orphan)
        if orphan_concepts > 0:
            warnings.append(
                _new_issue(
                    level="WARNING" if warn_orphan_concepts else "ERROR",
                    code="orphan_concepts",
                    where={"db": str(action_db), "table": concept_table},
                    detail="concepts without any instances detected",
                    hint={"sample": orphan, "count_estimate": orphan_concepts},
                )
            )
            if not warn_orphan_concepts:
                errors.append(warnings.pop())

        # 5) duplicate instances by anchor (same concept_id + same observed_at)
        anchor_group_fields = (
            f"i.{instance_concept_id_field}, i.{inst_asset_id_field}, i.{inst_path_field}, "
            f"i.{inst_value_index_field}, i.{inst_segment_index_field}, i.{inst_sentence_index_field}, "
            f"i.{inst_char_start_field}, i.{inst_char_end_field}"
        )
        sql_dup_by_anchor_core = (
            f"SELECT i.{instance_concept_id_field} AS concept_id, "
            f"i.{inst_asset_id_field} AS asset_id, i.{inst_path_field} AS path, "
            f"i.{inst_value_index_field} AS value_index, "
            f"i.{inst_segment_index_field} AS segment_index, "
            f"i.{inst_sentence_index_field} AS sentence_index, "
            f"i.{inst_char_start_field} AS char_start, i.{inst_char_end_field} AS char_end, "
            f"COUNT(1) AS cnt "
            f"FROM {instance_table} i "
            f"GROUP BY {anchor_group_fields} "
            f"HAVING COUNT(1) > 1"
        )
        total_row = uconn.execute(
            f"SELECT COUNT(1) AS group_count, COALESCE(SUM(cnt), 0) AS row_count, "
            f"COALESCE(SUM(cnt - 1), 0) AS excess_count FROM ({sql_dup_by_anchor_core})"
        ).fetchone()
        duplicate_instance_by_anchor = int(total_row["group_count"])
        duplicate_instance_by_anchor_rows = int(total_row["row_count"])
        duplicate_instance_by_anchor_excess_rows = int(total_row["excess_count"])
        dup_anchor = _fetch_samples(uconn, f"{sql_dup_by_anchor_core} LIMIT {sample_limit}", (), sample_limit)
        if duplicate_instance_by_anchor > 0:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="duplicate_instance_by_anchor",
                    where={"db": str(action_db), "table": instance_table},
                    detail="multiple instances share the same complete (concept_id + observed_at) anchor",
                    hint={
                        "group_count": duplicate_instance_by_anchor,
                        "row_count": duplicate_instance_by_anchor_rows,
                        "excess_row_count": duplicate_instance_by_anchor_excess_rows,
                        "sample_limit": sample_limit,
                        "sample": dup_anchor,
                    },
                )
            )

        # 6) cross-world anchor check against data layer (optional)
        data_anchor_checked = False
        if data_db is not None and data_table is not None:
            data_anchor_checked = True
            if not data_db.exists():
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code="data_db_missing",
                        where={"file": str(data_db)},
                        detail="data layer db not found for anchor check",
                    )
                )
            else:
                dconn = _connect_sqlite(data_db, timeout=sqlite_timeout)
                dconn.execute("PRAGMA foreign_keys = ON;")
                if not _table_exists(dconn, data_table):
                    errors.append(
                        _new_issue(
                            level="ERROR",
                            code="data_table_missing",
                            where={"db": str(data_db), "table": str(data_table)},
                            detail="data layer anchor table not found",
                        )
                    )
                else:
                    # ATTACH data.db to action connection for cross-db JOIN
                    uconn.execute(f"ATTACH DATABASE ? AS data_layer", (str(data_db),))
                    qualified_data_table = f"data_layer.{data_table}"

                    # find missing anchors via LEFT JOIN
                    # Note: NULL-safe equality for char_start/end is implemented with (a=b OR (a IS NULL AND b IS NULL))
                    sql_missing_anchor = (
                        f"SELECT i.{instance_id_field} AS instance_id, "
                        f"i.{inst_asset_id_field} AS asset_id, i.{inst_path_field} AS path, "
                        f"i.{inst_value_index_field} AS value_index, "
                        f"i.{inst_segment_index_field} AS segment_index, "
                        f"i.{inst_sentence_index_field} AS sentence_index, "
                        f"i.{inst_char_start_field} AS char_start, i.{inst_char_end_field} AS char_end "
                        f"FROM {instance_table} i "
                        f"LEFT JOIN {qualified_data_table} d ON "
                        f"d.{data_asset_id_field} = i.{inst_asset_id_field} AND "
                        f"d.{data_path_field} = i.{inst_path_field} AND "
                        f"(d.{data_value_index_field} = i.{inst_value_index_field} OR (d.{data_value_index_field} IS NULL AND i.{inst_value_index_field} IS NULL)) AND "
                        f"d.{data_segment_index_field} = i.{inst_segment_index_field} AND "
                        f"(d.{data_sentence_index_field} = i.{inst_sentence_index_field} OR (d.{data_sentence_index_field} IS NULL AND i.{inst_sentence_index_field} IS NULL)) AND "
                        f"(d.{data_char_start_field} = i.{inst_char_start_field} OR (d.{data_char_start_field} IS NULL AND i.{inst_char_start_field} IS NULL)) AND "
                        f"(d.{data_char_end_field} = i.{inst_char_end_field} OR (d.{data_char_end_field} IS NULL AND i.{inst_char_end_field} IS NULL)) "
                        f"WHERE d.{data_asset_id_field} IS NULL "
                        f"LIMIT {sample_limit}"
                    )
                    miss = _fetch_samples(uconn, sql_missing_anchor, (), sample_limit)
                    missing_anchor = len(miss)
                    if missing_anchor > 0:
                        errors.append(
                            _new_issue(
                                level="ERROR",
                                code="missing_data_anchor",
                                where={"action_db": str(action_db), "data_db": str(data_db)},
                                detail="one or more instances have observed_at that cannot be found in data layer",
                                hint={"sample": miss},
                            )
                        )

        status = "PASS" if len(errors) == 0 else "FAIL"
        finished_at = _utc_now_iso()

        return {
            "status": status,
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "started_at": started_at,
            "finished_at": finished_at,
            "inputs": {
                "action_db": str(action_db),
                "data_db": str(data_db) if data_db is not None else None,
                "tables": {
                    "concept": concept_table,
                    "instance": instance_table,
                    "attribute": attribute_table,
                    "data_anchor": data_table,
                },
                "sha256": {
                    "action_db": _file_sha256(action_db),
                    "data_db": _file_sha256(data_db) if data_db is not None and data_db.exists() else None,
                },
            },
            "summary": {
                "broken_instances": broken_instances,
                "broken_attributes": broken_attributes,
                "duplicate_concepts": duplicate_concepts,
                "duplicate_instances": duplicate_instances,
                "duplicate_instance_by_anchor": duplicate_instance_by_anchor,
                "duplicate_instance_by_anchor_rows": duplicate_instance_by_anchor_rows,
                "duplicate_instance_by_anchor_excess_rows": duplicate_instance_by_anchor_excess_rows,
                "orphan_concepts": orphan_concepts,
                "missing_anchor": missing_anchor if data_anchor_checked else None,
                "errors": len(errors),
                "warnings": len(warnings),
                "data_anchor_checked": data_anchor_checked,
            },
            "errors": errors,
            "warnings": warnings,
        }

    finally:
        for c in (uconn, dconn):
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass


# ============================================================
# CLI
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Check integrity of action-layer SQLite DB and optional data-anchor integrity.")
    p.add_argument("--action-db", required=True, help="Path to action-layer SQLite DB")
    p.add_argument("--report", required=True, help="Output report JSON path")
    p.add_argument("--sqlite-timeout", type=int, default=30, help="SQLite timeout seconds")
    p.add_argument("--sample-limit", type=int, default=50, help="Max sample rows per invariant")

    # table names
    p.add_argument("--concept-table", default="concepts", help="Concept table name")
    p.add_argument("--instance-table", default="instances", help="Instance table name")
    p.add_argument("--attribute-table", default="attributes", help="Attribute table name")

    # field names
    p.add_argument("--concept-id-field", default="concept_id", help="Concept id field")
    p.add_argument("--instance-id-field", default="instance_id", help="Instance id field")
    p.add_argument("--instance-concept-id-field", default="concept_id", help="Instance concept_id field")

    p.add_argument("--attr-object-type-field", default="object_type", help="Attribute object_type field")
    p.add_argument("--attr-object-id-field", default="object_id", help="Attribute object_id field")
    p.add_argument("--attr-key-field", default="attr_key", help="Attribute key field")
    p.add_argument("--attr-value-field", default="attr_value", help="Attribute value field")

    # instance observed_at fields
    p.add_argument("--inst-asset-id-field", default="asset_id", help="Instance observed_at asset_id field")
    p.add_argument("--inst-path-field", default="path", help="Instance observed_at path field")
    p.add_argument("--inst-value-index-field", default="value_index", help="Instance observed_at value_index field")
    p.add_argument("--inst-segment-index-field", default="segment_index", help="Instance observed_at segment_index field")
    p.add_argument("--inst-sentence-index-field", default="sentence_index", help="Instance observed_at sentence_index field")
    p.add_argument("--inst-char-start-field", default="char_start", help="Instance observed_at char_start field")
    p.add_argument("--inst-char-end-field", default="char_end", help="Instance observed_at char_end field")

    # optional data layer anchor check
    p.add_argument("--data-db", default=None, help="Optional data-layer SQLite DB path for anchor verification")
    p.add_argument("--data-table", default=None, help="Optional data-layer table name for anchor verification")
    p.add_argument("--data-asset-id-field", default="asset_id", help="Data table asset_id field")
    p.add_argument("--data-path-field", default="path", help="Data table path field")
    p.add_argument("--data-value-index-field", default="value_index", help="Data table value_index field")
    p.add_argument("--data-segment-index-field", default="segment_index", help="Data table segment_index field")
    p.add_argument("--data-sentence-index-field", default="sentence_index", help="Data table sentence_index field")
    p.add_argument("--data-char-start-field", default="char_start", help="Data table char_start field")
    p.add_argument("--data-char-end-field", default="char_end", help="Data table char_end field")

    p.add_argument("--warn-orphan-concepts", action="store_true", help="Treat orphan concepts as WARNING (default)")
    p.add_argument("--dry-run", action="store_true", help="Do not write report, only print preview")

    return p


def main() -> int:
    args = _build_parser().parse_args()

    action_db = Path(args.action_db)
    report_path = Path(args.report)

    data_db = Path(args.data_db) if args.data_db else None
    data_table = str(args.data_table) if args.data_table else None

    _ensure_dir(report_path.parent)

    report = check_integrity(
        action_db=action_db,
        concept_table=str(args.concept_table),
        instance_table=str(args.instance_table),
        attribute_table=str(args.attribute_table),
        concept_id_field=str(args.concept_id_field),
        instance_id_field=str(args.instance_id_field),
        instance_concept_id_field=str(args.instance_concept_id_field),
        attr_object_type_field=str(args.attr_object_type_field),
        attr_object_id_field=str(args.attr_object_id_field),
        attr_key_field=str(args.attr_key_field),
        attr_value_field=str(args.attr_value_field),
        inst_asset_id_field=str(args.inst_asset_id_field),
        inst_path_field=str(args.inst_path_field),
        inst_value_index_field=str(args.inst_value_index_field),
        inst_segment_index_field=str(args.inst_segment_index_field),
        inst_sentence_index_field=str(args.inst_sentence_index_field),
        inst_char_start_field=str(args.inst_char_start_field),
        inst_char_end_field=str(args.inst_char_end_field),
        data_db=data_db,
        data_table=data_table,
        data_asset_id_field=str(args.data_asset_id_field),
        data_path_field=str(args.data_path_field),
        data_value_index_field=str(args.data_value_index_field),
        data_segment_index_field=str(args.data_segment_index_field),
        data_sentence_index_field=str(args.data_sentence_index_field),
        data_char_start_field=str(args.data_char_start_field),
        data_char_end_field=str(args.data_char_end_field),
        sqlite_timeout=int(args.sqlite_timeout),
        sample_limit=int(args.sample_limit),
        warn_orphan_concepts=True if args.warn_orphan_concepts else True,  # default is warning
    )

    if args.dry_run:
        preview = {
            "status": report.get("status"),
            "summary": report.get("summary"),
            "first_error": report.get("errors", [])[:1],
            "first_warning": report.get("warnings", [])[:1],
        }
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0

    _archive_existing(report_path)
    size_bytes, sha = _atomic_write_json(report_path, report)

    print(
        json.dumps(
            {
                "status": "ok",
                "result": report.get("status"),
                "script": SCRIPT_NAME,
                "version": SCRIPT_VERSION,
                "report": str(report_path),
                "report_size_bytes": size_bytes,
                "report_sha256": sha,
                "errors": int(report.get("summary", {}).get("errors", 0)),
                "warnings": int(report.get("summary", {}).get("warnings", 0)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    return 0 if report.get("status") == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
