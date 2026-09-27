# ============================================================
# 文件名: sql_writer_v0004.py
# 中文名: SQL 写入网关脚本
# 版本号: v0004
#
# 主层级: action
# 层级: persistence / sql
# 脚本定位: action 端的 SQL 事实写入边界脚本
#
# 职责说明:
# - 提供 Concept, Instance, Attribute 三类事实对象的写入函数
# - 提供 run_id 审计事件写入函数
# - 通过配置文件解耦数据库连接参数与表字段映射
# - 统一事务边界与错误归类
#
# 本脚本做什么:
# - 读取 sql_writer_config_v0001.yml 获取 driver, connection, tables, fields
# - 使用 sqlite 驱动执行插入与查询
# - 启动时进行 schema 自检与 PRAGMA 自检，严格对齐 init_action_data_sql_schema_v0001.py 与配置
# - 任何不对齐、缺字段、缺表、约束冲突都应抛出异常，不做静默降级
#
# 本脚本不做什么:
# - 不执行制度裁决，不读取 decision 字段
# - 不生成 unit_text_id, instance_id（上游负责）
# - 不创建或迁移数据库 schema（由 init_action_data_sql_schema_v0001.py 负责）
#
# 制度边界声明:
# - 追加式写入，禁止覆盖与删除
# - 写入的最小一致性保护仅限于外键存在性与内容哈希一致性检查
# - 事务边界为单次调用级别，失败必须回滚并可审计
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: sql_writer_v0004
# family: sql_writer
# role: sql_write_gateway
# version: v0004
# status: active
# entry_point: scripts/action/anchor/sql_writer_v0004.py
# input:
#   - config/action/config/sql_writer_config_v0001.yml
# output:
#   - SQL: concept / instance / attribute / run_log tables
# depends_on:
#   - Python stdlib: sqlite3, hashlib, json, argparse, pathlib, datetime, typing, dataclasses, re, platform
#   - PyYAML: yaml
# used_by:
#   - ingest_concept_units_v0002.py
#   - ingest_instance_units_v0002.py
#   - ingest_attribute_units_v0003.py
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


DEFAULT_ENCODING = "utf-8"

SCRIPT_FAMILY = "sql_writer"
SCRIPT_NAME = "sql_writer_v0004"
SCRIPT_VERSION = "v0004"

MAX_ROOT_SEARCH_DEPTH = 10

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ============================================================
# 异常类型
# ============================================================

class SqlWriterError(RuntimeError):
    """Base exception for strict SQL writer failures."""


class ConfigError(SqlWriterError):
    """Raised when config is missing, malformed, or incomplete."""


class SchemaError(SqlWriterError):
    """Raised when database schema does not match expectations."""


class PragmaError(SqlWriterError):
    """Raised when required PRAGMAs are not applied or verified."""


class DataError(SqlWriterError):
    """Raised when inputs are invalid or violate writer-level invariants."""


class IntegrityWriteError(SqlWriterError):
    """Raised when sqlite integrity constraints fail."""


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class WriteResult:
    status: str  # inserted | existing | duplicate
    object: str  # concept | instance | attribute | run_log
    object_id: str
    detail: Optional[str] = None


@dataclass(frozen=True)
class SqlWriterConfig:
    driver: str
    sqlite_path: Path
    sqlite_timeout: int
    sqlite_pragmas: Dict[str, Any]
    tables: Dict[str, str]
    fields: Dict[str, Dict[str, str]]
    runtime: Dict[str, Any]


# ============================================================
# 工具函数区
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256_hex(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode(DEFAULT_ENCODING))
    return h.hexdigest()


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _require_non_empty_str(name: str, v: Any) -> str:
    if not isinstance(v, str) or v.strip() == "":
        raise DataError(f"{name} must be a non-empty string")
    return v


def _coerce_int_or_none(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, bool):
        raise DataError("bool is not allowed for int fields")
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s == "":
            return None
        return int(s)
    raise DataError(f"unsupported type for int field: {type(x)}")


def _classify_sqlite_integrity(e: sqlite3.IntegrityError) -> str:
    msg = (str(e) or "").lower()
    if "foreign key" in msg:
        return "foreign_key_missing"
    if "unique" in msg or "constraint" in msg:
        return "unique_conflict"
    if "not null" in msg:
        return "not_null_violation"
    return "integrity_error"


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


def _validate_identifier(name: str, context: str) -> None:
    if not _SAFE_IDENTIFIER.match(name):
        raise ConfigError(
            f"Unsafe SQL identifier from config ({context}): '{name}'. "
            f"Only letters, digits, and underscores are allowed."
        )


def _validate_all_identifiers(tables: Dict[str, str], fields: Dict[str, Dict[str, str]]) -> None:
    for k, t in tables.items():
        _validate_identifier(t, f"tables.{k}")
    for g, mp in fields.items():
        if not isinstance(mp, dict):
            raise ConfigError(f"fields.{g} must be a mapping")
        for fk, fn in mp.items():
            _validate_identifier(fn, f"fields.{g}.{fk}")


def _merge_tables(defaults: Dict[str, str], overrides: Dict[str, Any]) -> Dict[str, str]:
    out = dict(defaults)
    if overrides:
        for k, v in overrides.items():
            if v is None:
                continue
            out[str(k)] = str(v)
    return out


def _merge_fields(defaults: Dict[str, Dict[str, str]], overrides: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {k: dict(v) for k, v in defaults.items()}
    if overrides:
        for g, mp in overrides.items():
            if mp is None:
                continue
            if g not in out:
                out[str(g)] = {}
            if not isinstance(mp, dict):
                raise ConfigError(f"fields.{g} must be a mapping")
            for fk, fn in mp.items():
                if fn is None:
                    continue
                out[str(g)][str(fk)] = str(fn)
    return out


def _env_fingerprint() -> str:
    payload = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
    }
    return _safe_json_dumps(payload)


def _stable_attr_id(
    *,
    object_type: str,
    object_id: str,
    attr_scope: str,
    attr_key: str,
    attr_value: Optional[str],
    attr_type: Optional[str],
    attr_state: str,
    evidence_ref: Optional[str],
    created_by: str,
) -> str:
    parts = [
        object_type,
        object_id,
        attr_scope or "",
        attr_key,
        attr_value or "",
        attr_type or "",
        attr_state,
        evidence_ref or "",
        created_by,
    ]
    return _sha256_hex("|".join(parts))


# ============================================================
# 默认映射
# ============================================================

DEFAULT_TABLES = {
    "concept": "concept_units",
    "instance": "instance_units",
    "attribute": "unit_attributes",
    "run_log": "ingestion_run_log",
}

DEFAULT_FIELDS = {
    "concept": {
        "id": "unit_text_id",
        "text": "unit_text",
        "hash": "content_hash",
        "first_seen_instance": "first_seen_instance_id",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "instance": {
        "id": "instance_id",
        "concept_id": "unit_text_id",
        "asset_id": "asset_id",
        "path": "path",
        "value_index": "value_index",
        "segment_index": "segment_index",
        "sentence_index": "sentence_index",
        "char_start": "char_start",
        "char_end": "char_end",
        "content": "content",
        "content_hash": "content_hash",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "attribute": {
        "id": "attr_id",
        "object_type": "object_type",
        "object_id": "object_id",
        "attr_scope": "attr_scope",
        "attr_key": "attr_key",
        "attr_value": "attr_value",
        "attr_type": "attr_type",
        "attr_state": "attr_state",
        "evidence_ref": "evidence_ref",
        "created_at": "created_at",
        "created_by": "created_by",
        "run_id": "run_id",
    },
    "run_log": {
        "run_id": "run_id",
        "script_name": "script_name",
        "script_version": "script_version",
        "started_at": "started_at",
        "finished_at": "finished_at",
        "input_hash": "input_hash",
        "output_hash": "output_hash",
        "error_summary": "error_summary",
        "environment_fingerprint": "environment_fingerprint",
    },
}

DEFAULT_REQUIRED_PRAGMAS = {
    "journal_mode": "WAL",
    "foreign_keys": True,
    "synchronous": "NORMAL",
}


# ============================================================
# 核心类
# ============================================================

class SqlWriter:
    def __init__(self, config_path: str | Path, *, strict: bool = True, verify_schema_on_connect: bool = True):
        self._config_path = Path(config_path)
        self._strict = bool(strict)
        self._verify_schema_on_connect = bool(verify_schema_on_connect)
        self._project_root = _find_project_root(self._config_path.resolve())
        self._cfg = self._load_config(self._config_path, self._project_root)

    @property
    def config(self) -> SqlWriterConfig:
        return self._cfg

    @property
    def project_root(self) -> Path:
        return self._project_root

    # -------------------------
    # 连接与自检
    # -------------------------

    def connect(self) -> sqlite3.Connection:
        if self._cfg.driver != "sqlite":
            raise ConfigError(f"unsupported driver: {self._cfg.driver}")

        db_path = self._cfg.sqlite_path
        if not db_path.is_absolute():
            db_path = (self._project_root / db_path).resolve()

        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(db_path), timeout=self._cfg.sqlite_timeout)
        conn.row_factory = sqlite3.Row

        self._apply_and_verify_pragmas(conn)

        if self._verify_schema_on_connect:
            self._verify_schema(conn)

        return conn

    def _apply_and_verify_pragmas(self, conn: sqlite3.Connection) -> None:
        pragmas = dict(DEFAULT_REQUIRED_PRAGMAS)
        # config can override required pragmas, but strict will verify
        cfg_pragmas = self._cfg.sqlite_pragmas or {}
        for k, v in cfg_pragmas.items():
            pragmas[k] = v

        # foreign_keys
        if pragmas.get("foreign_keys") is True:
            conn.execute("PRAGMA foreign_keys = ON;")
            row = conn.execute("PRAGMA foreign_keys;").fetchone()
            actual = str(row[0]) if row else "unknown"
            if actual != "1":
                raise PragmaError(f"PRAGMA foreign_keys expected 1, got {actual}")
        elif pragmas.get("foreign_keys") is False:
            conn.execute("PRAGMA foreign_keys = OFF;")
            row = conn.execute("PRAGMA foreign_keys;").fetchone()
            actual = str(row[0]) if row else "unknown"
            if actual != "0":
                raise PragmaError(f"PRAGMA foreign_keys expected 0, got {actual}")
        else:
            raise PragmaError("PRAGMA foreign_keys must be true or false")

        # journal_mode
        jm = pragmas.get("journal_mode", "WAL")
        row = conn.execute(f"PRAGMA journal_mode = {jm};").fetchone()
        actual_jm = str(row[0]).upper() if row else "unknown"
        if str(jm).upper() != actual_jm:
            raise PragmaError(f"PRAGMA journal_mode expected {jm}, got {actual_jm}")

        # synchronous
        sync = pragmas.get("synchronous", "NORMAL")
        conn.execute(f"PRAGMA synchronous = {sync};")
        # SQLite does not consistently return textual value, so we verify by reading numeric or text
        row = conn.execute("PRAGMA synchronous;").fetchone()
        if row is None:
            raise PragmaError("PRAGMA synchronous readback failed")

    def _verify_schema(self, conn: sqlite3.Connection) -> None:
        """Strict schema self-check. Ensures tables and required columns exist."""
        tables = self._cfg.tables
        fields = self._cfg.fields

        # tables exist
        cur = conn.cursor()
        for _, tname in tables.items():
            row = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (tname,),
            ).fetchone()
            if row is None:
                raise SchemaError(f"missing table: {tname}. Run init_action_data_sql_schema_v0001.py --init first.")

        # required columns exist (minimum strict set)
        expected_cols = _build_expected_columns(tables, fields)
        for tname, specs in expected_cols.items():
            rows = cur.execute(f"PRAGMA table_info({tname});").fetchall()
            actual = {r[1]: (str(r[2]).upper() if r[2] else "", bool(r[3]), bool(r[5])) for r in rows}
            for (col, col_type, notnull, pk) in specs:
                if col not in actual:
                    raise SchemaError(f"table {tname} missing column: {col}")
                act_type, act_notnull, act_pk = actual[col]
                # Type affinity: accept common variants
                if not _type_compatible(col_type, act_type):
                    raise SchemaError(f"table {tname}.{col} type expected {col_type}, got {act_type}")
                # PK check
                if pk and not act_pk:
                    raise SchemaError(f"table {tname}.{col} expected PRIMARY KEY but not marked pk")
                # NOT NULL check (skip pk implicit notnull ambiguity)
                if notnull and (not act_notnull) and (not pk):
                    raise SchemaError(f"table {tname}.{col} expected NOT NULL but got nullable")

        # foreign key definition exists for instance -> concept
        ti = tables["instance"]
        tc = tables["concept"]
        fi = fields["instance"]
        fc = fields["concept"]
        fk_rows = cur.execute(f"PRAGMA foreign_key_list({ti});").fetchall()
        expected_fk = (fi["concept_id"], tc, fc["id"])
        ok = False
        for r in fk_rows:
            # r columns: id, seq, table, from, to, on_update, on_delete, match
            if (r[3], r[2], r[4]) == expected_fk:
                ok = True
                break
        if not ok:
            raise SchemaError(f"missing foreign key on {ti}({fi['concept_id']}) -> {tc}({fc['id']})")

        # indexes exist by expected names (strict)
        expected_indexes = _build_expected_index_names(tables, fields)
        idx_rows = cur.execute("SELECT name FROM sqlite_master WHERE type='index';").fetchall()
        actual_idx = {r[0] for r in idx_rows}
        missing = [n for n in expected_indexes if n not in actual_idx]
        if missing:
            raise SchemaError(f"missing indexes: {missing}")

        # index column order check
        for idx_name, idx_cols in _build_expected_index_columns(tables, fields).items():
            cols = cur.execute(f"PRAGMA index_info({idx_name});").fetchall()
            actual_cols = [r[2] for r in sorted(cols, key=lambda x: x[0])]
            if actual_cols != idx_cols:
                raise SchemaError(f"index {idx_name} columns expected {idx_cols}, got {actual_cols}")

    # -------------------------
    # 写入接口（严格，失败抛异常）
    # -------------------------

    def write_concept(
        self,
        *,
        unit_text_id: str,
        unit_text: str,
        content_hash: str,
        schema_version: str,
        first_seen_instance_id: Optional[str],
        run_id: str,
        created_at: Optional[str] = None,
        dry_run: bool = False,
    ) -> WriteResult:
        _require_non_empty_str("unit_text_id", unit_text_id)
        _require_non_empty_str("unit_text", unit_text)
        _require_non_empty_str("content_hash", content_hash)
        _require_non_empty_str("schema_version", schema_version)
        _require_non_empty_str("run_id", run_id)

        created_at = created_at or _utc_now_iso()

        computed = _sha256_hex(unit_text)
        if computed != content_hash:
            raise DataError("content_hash mismatch for concept")

        tbl = self._cfg.tables["concept"]
        f = self._cfg.fields["concept"]

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.connect()
            cur = conn.cursor()

            existing_id = self._select_concept_id_by_hash(cur, tbl, f, content_hash)
            if existing_id is not None:
                return WriteResult(status="existing", object="concept", object_id=str(existing_id), detail="content_hash already exists")

            if dry_run:
                return WriteResult(status="inserted", object="concept", object_id=unit_text_id, detail="dry-run (validated, not inserted)")

            cols = [f["id"], f["text"], f["hash"], f["first_seen_instance"], f["created_at"], f["schema_version"], f["run_id"]]
            vals = [unit_text_id, unit_text, content_hash, first_seen_instance_id, created_at, schema_version, run_id]

            sql = f"INSERT INTO {tbl} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
            cur.execute(sql, vals)

            conn.commit()
            return WriteResult(status="inserted", object="concept", object_id=unit_text_id)

        except sqlite3.IntegrityError as e:
            if conn is not None:
                conn.rollback()
            raise IntegrityWriteError(f"concept insert integrity error: {_classify_sqlite_integrity(e)}: {e}") from e
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    def write_instance(
        self,
        *,
        instance_id: str,
        unit_text_id: str,
        asset_id: str,
        path: str,
        value_index: Optional[int] = None,
        segment_index: int,
        sentence_index: Optional[int] = None,
        char_start: Optional[int],
        char_end: Optional[int],
        content: str,
        content_hash: str,
        schema_version: str,
        run_id: str,
        created_at: Optional[str] = None,
        dry_run: bool = False,
    ) -> WriteResult:
        _require_non_empty_str("instance_id", instance_id)
        _require_non_empty_str("unit_text_id", unit_text_id)
        _require_non_empty_str("asset_id", asset_id)
        _require_non_empty_str("path", path)
        _require_non_empty_str("content", content)
        _require_non_empty_str("content_hash", content_hash)
        _require_non_empty_str("schema_version", schema_version)
        _require_non_empty_str("run_id", run_id)
        if not isinstance(segment_index, int):
            raise DataError("segment_index must be int")

        created_at = created_at or _utc_now_iso()

        computed = _sha256_hex(content)
        if computed != content_hash:
            raise DataError("content_hash mismatch for instance")

        tbl_c = self._cfg.tables["concept"]
        tbl_i = self._cfg.tables["instance"]
        f_c = self._cfg.fields["concept"]
        f_i = self._cfg.fields["instance"]

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.connect()
            cur = conn.cursor()

            if not self._concept_exists_by_id(cur, tbl_c, f_c, unit_text_id):
                raise IntegrityWriteError("concept not found for unit_text_id (foreign key missing)")

            if self._instance_exists_by_id(cur, tbl_i, f_i, instance_id):
                return WriteResult(status="duplicate", object="instance", object_id=instance_id, detail="instance_id already exists")

            if dry_run:
                return WriteResult(status="inserted", object="instance", object_id=instance_id, detail="dry-run (validated, not inserted)")

            cols = [
                f_i["id"], f_i["concept_id"], f_i["asset_id"], f_i["path"],
                f_i["value_index"], f_i["segment_index"], f_i["sentence_index"],
                f_i["char_start"], f_i["char_end"],
                f_i["content"], f_i["content_hash"],
                f_i["created_at"], f_i["schema_version"], f_i["run_id"],
            ]
            vals = [
                instance_id, unit_text_id, asset_id, path,
                value_index, segment_index, sentence_index,
                char_start, char_end,
                content, content_hash,
                created_at, schema_version, run_id,
            ]

            sql = f"INSERT INTO {tbl_i} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
            cur.execute(sql, vals)

            conn.commit()
            return WriteResult(status="inserted", object="instance", object_id=instance_id)

        except sqlite3.IntegrityError as e:
            if conn is not None:
                conn.rollback()
            raise IntegrityWriteError(f"instance insert integrity error: {_classify_sqlite_integrity(e)}: {e}") from e
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    def write_attribute(
        self,
        *,
        object_type: str,
        object_id: str,
        attr_key: str,
        attr_value: Optional[str],
        attr_type: Optional[str],
        attr_scope: Optional[str],
        attr_state: str,
        evidence_ref: Optional[str],
        run_id: str,
        created_by: Optional[str] = None,
        created_at: Optional[str] = None,
        dry_run: bool = False,
    ) -> WriteResult:
        _require_non_empty_str("object_type", object_type)
        _require_non_empty_str("object_id", object_id)
        _require_non_empty_str("attr_key", attr_key)
        _require_non_empty_str("attr_state", attr_state)
        _require_non_empty_str("run_id", run_id)

        if object_type not in {"concept", "instance"}:
            raise DataError("object_type must be concept or instance")

        created_at = created_at or _utc_now_iso()
        created_by = created_by or f"{SCRIPT_NAME}:{SCRIPT_VERSION}"

        # attr_id stable and required by schema
        attr_id = _stable_attr_id(
            object_type=object_type,
            object_id=object_id,
            attr_scope=attr_scope,
            attr_key=attr_key,
            attr_value=attr_value,
            attr_type=attr_type,
            attr_state=attr_state,
            evidence_ref=evidence_ref,
            created_by=created_by,
        )

        tbl = self._cfg.tables["attribute"]
        f = self._cfg.fields["attribute"]

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.connect()
            cur = conn.cursor()

            if dry_run:
                return WriteResult(status="inserted", object="attribute", object_id=attr_id, detail="dry-run (validated, not inserted)")

            cols = [
                f["id"],
                f["object_type"], f["object_id"],
                f["attr_scope"], f["attr_key"], f["attr_value"],
                f["attr_type"], f["attr_state"],
                f["evidence_ref"],
                f["created_at"], f["created_by"], f["run_id"],
            ]
            vals = [
                attr_id,
                object_type, object_id,
                attr_scope, attr_key, attr_value,
                attr_type, attr_state,
                evidence_ref,
                created_at, created_by, run_id,
            ]

            sql = f"INSERT INTO {tbl} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
            cur.execute(sql, vals)

            conn.commit()
            return WriteResult(status="inserted", object="attribute", object_id=attr_id)

        except sqlite3.IntegrityError as e:
            if conn is not None:
                conn.rollback()
            raise IntegrityWriteError(f"attribute insert integrity error: {_classify_sqlite_integrity(e)}: {e}") from e
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    # -------------------------
    # run_log：开始与结束
    # -------------------------

    def record_run_event(
        self,
        *,
        run_id: str,
        script_name: str,
        script_version: str,
        status: str,
        error_summary: Optional[str],
        input_hash: Optional[str] = None,
        output_hash: Optional[str] = None,
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
        environment_fingerprint: Optional[str] = None,
        dry_run: bool = False,
    ) -> WriteResult:
        """Backward-compatible CLI/API. status must be 'started' or 'finished'.

        - started: INSERT a new run_log row with started_at required
        - finished: UPDATE existing row with finished_at required, plus optional error_summary/hashes/env
        """
        _require_non_empty_str("run_id", run_id)
        _require_non_empty_str("script_name", script_name)
        _require_non_empty_str("script_version", script_version)
        _require_non_empty_str("status", status)

        status_norm = status.strip().lower()
        if status_norm not in {"started", "finished"}:
            raise DataError("status must be 'started' or 'finished'")

        tbl = self._cfg.tables["run_log"]
        f = self._cfg.fields["run_log"]

        if status_norm == "started":
            started_at = started_at or _utc_now_iso()
            if finished_at is not None:
                raise DataError("finished_at must be None for status=started")
        else:
            finished_at = finished_at or _utc_now_iso()
            if started_at is not None:
                # We do not accept overriding started_at in finish path
                raise DataError("started_at must be None for status=finished")

        environment_fingerprint = environment_fingerprint or _env_fingerprint()

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.connect()
            cur = conn.cursor()

            if dry_run:
                return WriteResult(status="inserted", object="run_log", object_id=run_id, detail="dry-run (validated, not inserted)")

            if status_norm == "started":
                cols = [
                    f["run_id"], f["script_name"], f["script_version"],
                    f["started_at"], f["finished_at"],
                    f["input_hash"], f["output_hash"], f["error_summary"],
                    f["environment_fingerprint"],
                ]
                vals = [
                    run_id, script_name, script_version,
                    started_at, None,
                    input_hash, output_hash, error_summary,
                    environment_fingerprint,
                ]
                sql = f"INSERT INTO {tbl} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
                cur.execute(sql, vals)
                conn.commit()
                return WriteResult(status="inserted", object="run_log", object_id=run_id, detail="started")

            # finished: update
            cols_set = [
                f"{f['finished_at']} = ?",
                f"{f['error_summary']} = ?",
                f"{f['input_hash']} = ?",
                f"{f['output_hash']} = ?",
                f"{f['environment_fingerprint']} = ?",
            ]
            vals = [finished_at, error_summary, input_hash, output_hash, environment_fingerprint, run_id]
            sql = f"UPDATE {tbl} SET {', '.join(cols_set)} WHERE {f['run_id']} = ?"
            cur.execute(sql, vals)
            if cur.rowcount == 0:
                raise IntegrityWriteError("run_log finish failed: run_id not found. Start run_log first.")
            conn.commit()
            return WriteResult(status="inserted", object="run_log", object_id=run_id, detail="finished")

        except sqlite3.IntegrityError as e:
            if conn is not None:
                conn.rollback()
            raise IntegrityWriteError(f"run_log write integrity error: {_classify_sqlite_integrity(e)}: {e}") from e
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    # -------------------------
    # 内部查询方法
    # -------------------------

    @staticmethod
    def _select_concept_id_by_hash(cur: sqlite3.Cursor, tbl: str, f: Dict[str, str], content_hash: str) -> Optional[str]:
        sql = f"SELECT {f['id']} FROM {tbl} WHERE {f['hash']} = ? LIMIT 1"
        row = cur.execute(sql, (content_hash,)).fetchone()
        if row is None:
            return None
        return str(row[0])

    @staticmethod
    def _concept_exists_by_id(cur: sqlite3.Cursor, tbl: str, f: Dict[str, str], unit_text_id: str) -> bool:
        sql = f"SELECT 1 FROM {tbl} WHERE {f['id']} = ? LIMIT 1"
        row = cur.execute(sql, (unit_text_id,)).fetchone()
        return row is not None

    @staticmethod
    def _instance_exists_by_id(cur: sqlite3.Cursor, tbl: str, f: Dict[str, str], instance_id: str) -> bool:
        sql = f"SELECT 1 FROM {tbl} WHERE {f['id']} = ? LIMIT 1"
        row = cur.execute(sql, (instance_id,)).fetchone()
        return row is not None

    # -------------------------
    # 配置加载（严格）
    # -------------------------

    @staticmethod
    def _load_config(config_path: Path, project_root: Path) -> SqlWriterConfig:
        if not config_path.exists():
            raise ConfigError(f"config not found: {config_path}")

        raw = yaml.safe_load(config_path.read_text(encoding=DEFAULT_ENCODING))
        if not isinstance(raw, dict):
            raise ConfigError("invalid config format, expected mapping")

        driver = str(raw.get("driver", "sqlite"))
        version = str(raw.get("version", "v0001"))
        if version != "v0001":
            raise ConfigError("config version mismatch")

        conn = raw.get("connection", {}) or {}
        sqlite_cfg = conn.get("sqlite", {}) or {}
        sqlite_path = sqlite_cfg.get("path")
        if not sqlite_path:
            raise ConfigError("missing connection.sqlite.path in config")
        sqlite_timeout = int(sqlite_cfg.get("timeout", 30))
        sqlite_pragmas = sqlite_cfg.get("pragmas", {}) or {}

        tables_raw = raw.get("tables", {}) or {}
        fields_raw = raw.get("fields", {}) or {}
        runtime = raw.get("runtime", {}) or {}

        tables = _merge_tables(DEFAULT_TABLES, tables_raw)
        fields = _merge_fields(DEFAULT_FIELDS, fields_raw)

        required_tables = {"concept", "instance", "attribute", "run_log"}
        missing_tables = sorted([k for k in required_tables if k not in tables])
        if missing_tables:
            raise ConfigError(f"missing tables mapping: {missing_tables}")

        required_field_groups = {"concept", "instance", "attribute", "run_log"}
        missing_groups = sorted([k for k in required_field_groups if k not in fields])
        if missing_groups:
            raise ConfigError(f"missing fields mapping groups: {missing_groups}")

        # Ensure required keys exist for each group (strict alignment with init_action_data_sql_schema_v0001.py)
        required_keys = {
            "concept": {"id", "text", "hash", "first_seen_instance", "created_at", "schema_version", "run_id"},
            "instance": {"id", "concept_id", "asset_id", "path", "value_index", "segment_index", "sentence_index",
                         "char_start", "char_end", "content", "content_hash", "created_at", "schema_version", "run_id"},
            "attribute": {"id", "object_type", "object_id", "attr_scope", "attr_key", "attr_value", "attr_type",
                          "attr_state", "evidence_ref", "created_at", "created_by", "run_id"},
            "run_log": {"run_id", "script_name", "script_version", "started_at", "finished_at",
                        "input_hash", "output_hash", "error_summary", "environment_fingerprint"},
        }
        missing_detail: Dict[str, List[str]] = {}
        for g, req in required_keys.items():
            got = set(fields.get(g, {}).keys())
            miss = sorted(list(req - got))
            if miss:
                missing_detail[g] = miss
        if missing_detail:
            raise ConfigError(f"fields mapping missing keys: {missing_detail}")

        _validate_all_identifiers(tables, fields)

        db_path = (project_root / str(sqlite_path)).resolve()

        return SqlWriterConfig(
            driver=driver,
            sqlite_path=db_path,
            sqlite_timeout=sqlite_timeout,
            sqlite_pragmas=sqlite_pragmas,
            tables=tables,
            fields=fields,
            runtime=runtime,
        )


# ============================================================
# Schema expectation helpers (aligned with init_action_data_sql_schema_v0001.py)
# ============================================================

def _type_compatible(expected: str, actual: str) -> bool:
    e = (expected or "").upper()
    a = (actual or "").upper()
    if e == a:
        return True
    # SQLite affinity compatibility
    text_like = {"TEXT", "VARCHAR", "CHAR", "CLOB"}
    int_like = {"INTEGER", "INT", "BIGINT", "SMALLINT"}
    if e in text_like and a in text_like:
        return True
    if e in int_like and a in int_like:
        return True
    return False


def _build_expected_columns(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, List[Tuple[str, str, bool, bool]]]:
    fc = fields["concept"]
    fi = fields["instance"]
    fa = fields["attribute"]
    fr = fields["run_log"]

    specs: Dict[str, List[Tuple[str, str, bool, bool]]] = {}

    specs[tables["concept"]] = [
        (fc["id"], "TEXT", True, True),
        (fc["text"], "TEXT", True, False),
        (fc["hash"], "TEXT", True, False),
        (fc["first_seen_instance"], "TEXT", False, False),
        (fc["created_at"], "TEXT", True, False),
        (fc["schema_version"], "TEXT", True, False),
        (fc["run_id"], "TEXT", True, False),
    ]

    specs[tables["instance"]] = [
        (fi["id"], "TEXT", True, True),
        (fi["concept_id"], "TEXT", True, False),
        (fi["asset_id"], "TEXT", True, False),
        (fi["path"], "TEXT", True, False),
        (fi["value_index"], "INTEGER", False, False),
        (fi["segment_index"], "INTEGER", True, False),
        (fi["sentence_index"], "INTEGER", False, False),
        (fi["char_start"], "INTEGER", False, False),
        (fi["char_end"], "INTEGER", False, False),
        (fi["content"], "TEXT", True, False),
        (fi["content_hash"], "TEXT", True, False),
        (fi["created_at"], "TEXT", True, False),
        (fi["schema_version"], "TEXT", True, False),
        (fi["run_id"], "TEXT", True, False),
    ]

    specs[tables["attribute"]] = [
        (fa["id"], "TEXT", True, True),
        (fa["object_type"], "TEXT", True, False),
        (fa["object_id"], "TEXT", True, False),
        (fa["attr_scope"], "TEXT", False, False),
        (fa["attr_key"], "TEXT", True, False),
        (fa["attr_value"], "TEXT", False, False),
        (fa["attr_type"], "TEXT", False, False),
        (fa["attr_state"], "TEXT", True, False),
        (fa["evidence_ref"], "TEXT", False, False),
        (fa["created_at"], "TEXT", True, False),
        (fa["created_by"], "TEXT", True, False),
        (fa["run_id"], "TEXT", True, False),
    ]

    specs[tables["run_log"]] = [
        (fr["run_id"], "TEXT", True, True),
        (fr["script_name"], "TEXT", True, False),
        (fr["script_version"], "TEXT", True, False),
        (fr["started_at"], "TEXT", True, False),
        (fr["finished_at"], "TEXT", False, False),
        (fr["input_hash"], "TEXT", False, False),
        (fr["output_hash"], "TEXT", False, False),
        (fr["error_summary"], "TEXT", False, False),
        (fr["environment_fingerprint"], "TEXT", False, False),
    ]

    return specs


def _build_expected_index_names(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[str]:
    tc = tables["concept"]
    ti = tables["instance"]
    ta = tables["attribute"]
    tr = tables["run_log"]

    fc = fields["concept"]
    fi = fields["instance"]
    fa = fields["attribute"]
    fr = fields["run_log"]

    return [
        f"idx_{tc}_{fc['hash']}",
        f"idx_{ti}_{fi['concept_id']}",
        f"idx_{ti}_asset_path_seg",
        f"idx_{ti}_{fi['content_hash']}",
        f"idx_{ta}_obj_key",
        f"idx_{ta}_{fa['run_id']}",
        f"idx_{tr}_{fr['started_at']}",
    ]


def _build_expected_index_columns(
    tables: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> Dict[str, List[str]]:
    tc = tables["concept"]
    ti = tables["instance"]
    ta = tables["attribute"]
    tr = tables["run_log"]

    fc = fields["concept"]
    fi = fields["instance"]
    fa = fields["attribute"]
    fr = fields["run_log"]

    return {
        f"idx_{tc}_{fc['hash']}": [fc["hash"]],
        f"idx_{ti}_{fi['concept_id']}": [fi["concept_id"]],
        f"idx_{ti}_asset_path_seg": [fi["asset_id"], fi["path"], fi["segment_index"]],
        f"idx_{ti}_{fi['content_hash']}": [fi["content_hash"]],
        f"idx_{ta}_obj_key": [fa["object_type"], fa["object_id"], fa["attr_key"]],
        f"idx_{ta}_{fa['run_id']}": [fa["run_id"]],
        f"idx_{tr}_{fr['started_at']}": [fr["started_at"]],
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="SQL write gateway for My_Local_AI (append-only, strict aligned)."
    )
    p.add_argument("--config", default="config/action/config/sql_writer_config_v0001.yml", help="Path to sql_writer_config_v0001.yml")
    p.add_argument("--dry-run", action="store_true", help="Validate inputs and simulate operation without INSERT.")
    p.add_argument("--no-verify-schema", action="store_true", help="Skip schema verification on connect (not recommended).")

    sub = p.add_subparsers(dest="cmd", required=True)

    c1 = sub.add_parser("write-concept", help="Insert a concept if not exists by content_hash")
    c1.add_argument("--unit-text-id", required=True)
    c1.add_argument("--unit-text", required=True)
    c1.add_argument("--content-hash", required=True)
    c1.add_argument("--schema-version", required=True)
    c1.add_argument("--first-seen-instance-id", default=None)
    c1.add_argument("--run-id", required=True)

    c2 = sub.add_parser("write-instance", help="Insert an instance (requires concept exists)")
    c2.add_argument("--instance-id", required=True)
    c2.add_argument("--unit-text-id", required=True)
    c2.add_argument("--asset-id", required=True)
    c2.add_argument("--path", required=True)
    c2.add_argument("--value-index", default=None)
    c2.add_argument("--segment-index", type=int, required=True)
    c2.add_argument("--sentence-index", default=None)
    c2.add_argument("--char-start", default=None)
    c2.add_argument("--char-end", default=None)
    c2.add_argument("--content", required=True)
    c2.add_argument("--content-hash", required=True)
    c2.add_argument("--schema-version", required=True)
    c2.add_argument("--run-id", required=True)

    c3 = sub.add_parser("write-attribute", help="Insert an attribute (append-only)")
    c3.add_argument("--object-type", required=True, choices=["concept", "instance"])
    c3.add_argument("--object-id", required=True)
    c3.add_argument("--attr-key", required=True)
    c3.add_argument("--attr-value", default=None)
    c3.add_argument("--attr-type", default=None)
    c3.add_argument("--attr-scope", default=None)
    c3.add_argument("--attr-state", required=True)
    c3.add_argument("--evidence-ref", default=None)
    c3.add_argument("--created-by", default=None)
    c3.add_argument("--run-id", required=True)

    c4 = sub.add_parser("record-run", help="Record a run event (status=started|finished)")
    c4.add_argument("--run-id", required=True)
    c4.add_argument("--script-name", required=True)
    c4.add_argument("--script-version", required=True)
    c4.add_argument("--status", required=True, choices=["started", "finished"])
    c4.add_argument("--error-summary", default=None)
    c4.add_argument("--input-hash", default=None)
    c4.add_argument("--output-hash", default=None)

    return p


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    writer = SqlWriter(
        args.config,
        strict=True,
        verify_schema_on_connect=not bool(args.no_verify_schema),
    )
    dry_run = bool(args.dry_run)

    try:
        if args.cmd == "write-concept":
            res = writer.write_concept(
                unit_text_id=args.unit_text_id,
                unit_text=args.unit_text,
                content_hash=args.content_hash,
                schema_version=args.schema_version,
                first_seen_instance_id=args.first_seen_instance_id,
                run_id=args.run_id,
                dry_run=dry_run,
            )
            print(_safe_json_dumps(res.__dict__))
            return 0

        if args.cmd == "write-instance":
            res = writer.write_instance(
                instance_id=args.instance_id,
                unit_text_id=args.unit_text_id,
                asset_id=args.asset_id,
                path=args.path,
                value_index=_coerce_int_or_none(args.value_index),
                segment_index=int(args.segment_index),
                sentence_index=_coerce_int_or_none(args.sentence_index),
                char_start=_coerce_int_or_none(args.char_start),
                char_end=_coerce_int_or_none(args.char_end),
                content=args.content,
                content_hash=args.content_hash,
                schema_version=args.schema_version,
                run_id=args.run_id,
                dry_run=dry_run,
            )
            print(_safe_json_dumps(res.__dict__))
            return 0

        if args.cmd == "write-attribute":
            res = writer.write_attribute(
                object_type=args.object_type,
                object_id=args.object_id,
                attr_key=args.attr_key,
                attr_value=args.attr_value,
                attr_type=args.attr_type,
                attr_scope=args.attr_scope,
                attr_state=args.attr_state,
                evidence_ref=args.evidence_ref,
                created_by=args.created_by,
                run_id=args.run_id,
                dry_run=dry_run,
            )
            print(_safe_json_dumps(res.__dict__))
            return 0

        if args.cmd == "record-run":
            res = writer.record_run_event(
                run_id=args.run_id,
                script_name=args.script_name,
                script_version=args.script_version,
                status=args.status,
                error_summary=args.error_summary,
                input_hash=args.input_hash,
                output_hash=args.output_hash,
                dry_run=dry_run,
            )
            print(_safe_json_dumps(res.__dict__))
            return 0

        raise SqlWriterError("unknown command")

    except SqlWriterError as e:
        payload = {
            "status": "error",
            "error_type": e.__class__.__name__,
            "detail": str(e),
        }
        print(_safe_json_dumps(payload))
        return 2
    except Exception as e:
        payload = {
            "status": "error",
            "error_type": e.__class__.__name__,
            "detail": str(e),
        }
        print(_safe_json_dumps(payload))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
