# ============================================================
# 文件名: annotation_persistence_v0001.py
# 中文名: 补充标注追加持久化脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / persistence
# 脚本定位: Framework 将运行证据与通用 Annotation Envelope 写入独立 SQLite 的边界
#
# 职责说明:
# - 初始化并严格验证 annotation_runs 与 supplemental_annotations 两张表
# - 以 annotation_id 幂等追加标注并记录 Framework 运行状态
#
# 本脚本做什么:
# - 使用 Python 标准库 sqlite3 建立独立的补充标注存储
# - 在事务内写入标注，区分 created 与 skipped duplicate
#
# 本脚本不做什么:
# - 不读取或修改 canonical data，不识别时间，不加载插件
# - 不迁移或重建 data.db、action_data.db、action.db
#
# 制度边界声明:
# - Annotation 记录只追加，重复 annotation_id 不覆盖历史结果
# - 运行状态属于可更新审计状态；失败必须标记 side_effects_may_exist
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: annotation_persistence_v0001
# family: annotation_persistence
# role: supplemental_annotation_append_store
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/framework/annotation_persistence_v0001.py
# input:
#   - validated annotation envelopes
#   - framework run metadata
# output:
#   - SQLite annotation_runs and supplemental_annotations tables
# depends_on:
#   - Python stdlib: json, sqlite3, pathlib, typing
# used_by:
#   - supplemental_annotation_runner_v0001
# ============================================================

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "annotation_persistence"
SCRIPT_NAME = "annotation_persistence_v0001"
SCRIPT_VERSION = "v0001"
SCHEMA_VERSION = "supplemental_annotation_sqlite_v0001"


# ============================================================
# 异常类型
# ============================================================

class AnnotationPersistenceError(RuntimeError):
    """Raised when annotation storage cannot preserve its declared invariants."""


class AnnotationSchemaError(AnnotationPersistenceError):
    """Raised when an existing annotation database does not match v0001."""


# ============================================================
# 数据结构
# ============================================================

RUN_TABLE = "annotation_runs"
ANNOTATION_TABLE = "supplemental_annotations"


# ============================================================
# 工具函数区
# ============================================================

def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _table_columns(connection: sqlite3.Connection, table: str) -> Tuple[str, ...]:
    return tuple(str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})"))


# ============================================================
# 默认映射
# ============================================================

EXPECTED_COLUMNS = {
    RUN_TABLE: (
        "run_id",
        "framework_version",
        "schema_version",
        "input_source",
        "input_hash",
        "config_hash",
        "enabled_plugins_json",
        "started_at",
        "finished_at",
        "status",
        "records_scanned",
        "annotations_created",
        "annotations_skipped",
        "annotations_invalid",
        "side_effects_may_exist",
        "error_summary",
    ),
    ANNOTATION_TABLE: (
        "annotation_id",
        "annotation_type",
        "annotation_subtype",
        "text_unit_id",
        "asset_id",
        "path",
        "value_index",
        "segment_index",
        "sentence_index",
        "char_start",
        "char_end",
        "span_start",
        "span_end",
        "source_kind",
        "annotator",
        "annotator_version",
        "rule_version",
        "confidence",
        "resolution_status",
        "payload_json",
        "run_id",
        "created_at",
    ),
}


# ============================================================
# 核心类
# ============================================================

class AnnotationStore:
    def __init__(self, database_path: Path):
        self.database_path = database_path.resolve()

    def connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.database_path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    def initialize(self) -> None:
        connection = self.connect()
        try:
            connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS {RUN_TABLE} (
                    run_id TEXT PRIMARY KEY,
                    framework_version TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    input_source TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    enabled_plugins_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    records_scanned INTEGER NOT NULL DEFAULT 0,
                    annotations_created INTEGER NOT NULL DEFAULT 0,
                    annotations_skipped INTEGER NOT NULL DEFAULT 0,
                    annotations_invalid INTEGER NOT NULL DEFAULT 0,
                    side_effects_may_exist INTEGER NOT NULL DEFAULT 0,
                    error_summary TEXT
                );

                CREATE TABLE IF NOT EXISTS {ANNOTATION_TABLE} (
                    annotation_id TEXT PRIMARY KEY,
                    annotation_type TEXT NOT NULL,
                    annotation_subtype TEXT NOT NULL,
                    text_unit_id TEXT,
                    asset_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    value_index INTEGER NOT NULL,
                    segment_index INTEGER NOT NULL,
                    sentence_index INTEGER NOT NULL,
                    char_start INTEGER NOT NULL,
                    char_end INTEGER NOT NULL,
                    span_start INTEGER NOT NULL,
                    span_end INTEGER NOT NULL,
                    source_kind TEXT NOT NULL,
                    annotator TEXT NOT NULL,
                    annotator_version TEXT NOT NULL,
                    rule_version TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    resolution_status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES {RUN_TABLE}(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_supplemental_annotations_identity
                    ON {ANNOTATION_TABLE} (
                        asset_id, path, value_index, segment_index,
                        sentence_index, char_start, char_end
                    );

                CREATE INDEX IF NOT EXISTS idx_supplemental_annotations_type
                    ON {ANNOTATION_TABLE} (annotation_type, annotation_subtype);

                CREATE INDEX IF NOT EXISTS idx_supplemental_annotations_run
                    ON {ANNOTATION_TABLE} (run_id);
                """
            )
            connection.commit()
            self._verify_schema(connection)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _verify_schema(self, connection: sqlite3.Connection) -> None:
        for table, expected in EXPECTED_COLUMNS.items():
            actual = _table_columns(connection, table)
            if actual != expected:
                raise AnnotationSchemaError(
                    f"annotation schema mismatch for {table}: expected {expected}, got {actual}"
                )
        foreign_keys = connection.execute(
            f"PRAGMA foreign_key_list({ANNOTATION_TABLE})"
        ).fetchall()
        if not any(
            row[2] == RUN_TABLE and row[3] == "run_id" and row[4] == "run_id"
            for row in foreign_keys
        ):
            raise AnnotationSchemaError("annotation run foreign key is missing")

    def start_run(self, metadata: Mapping[str, Any]) -> None:
        connection = self.connect()
        try:
            connection.execute(
                f"""
                INSERT INTO {RUN_TABLE} (
                    run_id, framework_version, schema_version,
                    input_source, input_hash, config_hash,
                    enabled_plugins_json, started_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metadata["run_id"],
                    metadata["framework_version"],
                    SCHEMA_VERSION,
                    metadata["input_source"],
                    metadata["input_hash"],
                    metadata["config_hash"],
                    _json_text(metadata["enabled_plugins"]),
                    metadata["started_at"],
                    "running",
                ),
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            raise AnnotationPersistenceError(f"run_id already exists: {metadata['run_id']}") from exc
        finally:
            connection.close()

    def append_annotations(
        self,
        annotations: Sequence[Mapping[str, Any]],
        *,
        run_id: str,
        created_at: str,
    ) -> Tuple[int, int]:
        if not annotations:
            return 0, 0
        connection = self.connect()
        created = 0
        skipped = 0
        try:
            for annotation in annotations:
                cursor = connection.execute(
                    f"""
                    INSERT OR IGNORE INTO {ANNOTATION_TABLE} (
                        annotation_id, annotation_type, annotation_subtype,
                        text_unit_id, asset_id, path, value_index, segment_index,
                        sentence_index, char_start, char_end, span_start, span_end,
                        source_kind, annotator, annotator_version, rule_version,
                        confidence, resolution_status, payload_json, run_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        annotation["annotation_id"],
                        annotation["annotation_type"],
                        annotation["annotation_subtype"],
                        annotation.get("text_unit_id"),
                        annotation["asset_id"],
                        annotation["path"],
                        annotation["value_index"],
                        annotation["segment_index"],
                        annotation["sentence_index"],
                        annotation["char_start"],
                        annotation["char_end"],
                        annotation["span_start"],
                        annotation["span_end"],
                        annotation["source_kind"],
                        annotation["annotator"],
                        annotation["annotator_version"],
                        annotation["rule_version"],
                        annotation["confidence"],
                        annotation["resolution_status"],
                        _json_text(annotation["payload"]),
                        run_id,
                        created_at,
                    ),
                )
                if cursor.rowcount == 1:
                    created += 1
                else:
                    skipped += 1
            connection.commit()
            return created, skipped
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def finish_run(self, run_id: str, summary: Mapping[str, Any]) -> None:
        connection = self.connect()
        try:
            cursor = connection.execute(
                f"""
                UPDATE {RUN_TABLE}
                SET finished_at = ?, status = ?, records_scanned = ?,
                    annotations_created = ?, annotations_skipped = ?,
                    annotations_invalid = ?, side_effects_may_exist = ?,
                    error_summary = ?
                WHERE run_id = ?
                """,
                (
                    summary["finished_at"],
                    summary["status"],
                    summary.get("records_scanned", 0),
                    summary.get("annotations_created", 0),
                    summary.get("annotations_skipped", 0),
                    summary.get("annotations_invalid", 0),
                    1 if summary.get("side_effects_may_exist") else 0,
                    summary.get("error_summary"),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise AnnotationPersistenceError(f"run_id not found during finish: {run_id}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def schema_summary() -> Dict[str, Any]:
    return {
        "status": "completed",
        "schema_version": SCHEMA_VERSION,
        "tables": [RUN_TABLE, ANNOTATION_TABLE],
        "annotation_write_policy": "append_only_insert_or_ignore",
    }
