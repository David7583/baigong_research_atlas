# ============================================================
# 文件名: content_delta_filter_v0001.py
# 中文名: 内容增量过滤器脚本
# 版本号: v0001
#
# 主层级: action
# 层级: pipelines / content filtering
# 脚本定位: 结构化语义文本单元进入 Action 锚点持久化前的只读内容过滤节点
#
# 职责说明:
# - 复用活动概念规范化规则计算内容哈希
# - 只读比较 Action 内容表并仅输出系统中不存在的内容记录
#
# 本脚本做什么:
# - 逐行读取 JSONL，批量查询 SQLite 内容哈希索引并生成 NEW-only JSONL
# - 生成守恒统计、问题记录和机器可读运行清单
#
# 本脚本不做什么:
# - 不修改输入、SQLite、原始数据或任何下游数据库
# - 不管理来源版本、Schema 漂移、摄取状态、事务回滚或近似重复
#
# 制度边界声明:
# - SQLite 连接强制使用只读模式，内容哈希是唯一放行依据
# - 正式清单最后写入；失败不得返回伪完成状态
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: content_delta_filter_v0001
# family: content_delta_filter
# role: content_novelty_filter
# version: v0001
# status: experimental
# entry_point: scripts/action/pipelines/content_delta_filter_v0001.py
# input:
#   - semantic_text_units JSONL
#   - configured Action SQLite content hash index
#   - content_delta_filter_config_v0001.yml
# output:
#   - NEW-only semantic_text_units JSONL
#   - content delta filter manifest JSON
#   - invalid record issues JSONL
# depends_on:
#   - scripts/action/anchor/ingest_concept_units_v0002.py
#   - config/action/config/ingest_concept_units_config_v0001.yml
# used_by:
#   - data_action_chain_pipeline_v0010.py
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, Optional, Sequence, TextIO
from urllib.parse import quote

import yaml


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "content_delta_filter"
SCRIPT_NAME = "content_delta_filter_v0001"
SCRIPT_VERSION = "v0001"
MANIFEST_SCHEMA_VERSION = "content_delta_filter_manifest_v0001"
MAX_ROOT_SEARCH_DEPTH = 12
MAX_SQLITE_BATCH_SIZE = 900

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ============================================================
# 异常类型
# ============================================================

class ContentDeltaFilterError(RuntimeError):
    """Base exception for classified content filter failures."""


class ConfigError(ContentDeltaFilterError):
    """Raised when a required configuration is missing or incompatible."""


class InputError(ContentDeltaFilterError):
    """Raised when input or output paths violate the public contract."""


class ContractError(ContentDeltaFilterError):
    """Raised when canonicalization or record contracts are incompatible."""


class DatabaseError(ContentDeltaFilterError):
    """Raised when the authoritative content index cannot be queried safely."""


class OutputError(ContentDeltaFilterError):
    """Raised when atomic output production cannot be completed."""


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class FilterConfig:
    text_field: str
    required_fields: tuple[str, ...]
    canonicalizer_entry_point: Path
    canonicalization_rules: Path
    table: str
    hash_field: str
    batch_size: int
    require_index: bool


@dataclass
class FilterStats:
    input_records: int = 0
    valid_records: int = 0
    new_records: int = 0
    existing_records: int = 0
    existing_database_records: int = 0
    existing_within_run_records: int = 0
    invalid_records: int = 0
    hash_compute_ms: int = 0
    database_lookup_ms: int = 0
    output_write_ms: int = 0


@dataclass(frozen=True)
class StagedRecord:
    line_number: int
    record: dict[str, Any]
    canonical_text: str
    content_hash: str


@dataclass(frozen=True)
class AtomicTextTarget:
    final_path: Path
    temporary_path: Path
    handle: TextIO


# ============================================================
# 工具函数区
# ============================================================

def _safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    current = start.resolve()
    fallback: Optional[Path] = None
    for _ in range(max_up):
        if (current / "scripts").is_dir() and (current / "config").is_dir():
            if fallback is None:
                fallback = current
            if (current / "AGENTS.md").is_file():
                return current
        if current.parent == current:
            break
        current = current.parent
    if fallback is not None:
        return fallback
    raise ConfigError("unable to locate project root containing scripts and config")


def _resolve_from_root(value: Any, project_root: Path, label: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label} must be a non-empty path string")
    candidate = Path(value)
    return candidate.resolve() if candidate.is_absolute() else (project_root / candidate).resolve()


def _require_safe_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
        raise ConfigError(f"{label} must contain only letters, digits, and underscores")
    return value


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{label} must be a mapping")
    return value


def _require_string_list(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{label} must be a non-empty list")
    result: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item:
            raise ConfigError(f"{label}[{index}] must be a non-empty string")
        if item not in result:
            result.append(item)
    return tuple(result)


def _iter_jsonl_lines(path: Path) -> Iterable[tuple[int, str]]:
    with path.open("r", encoding=DEFAULT_ENCODING) as handle:
        for line_number, line in enumerate(handle, 1):
            stripped = line.strip()
            if stripped:
                yield line_number, stripped


def _open_atomic_text(path: Path) -> AtomicTextTarget:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    handle = os.fdopen(descriptor, "w", encoding=DEFAULT_ENCODING, newline="\n")
    return AtomicTextTarget(final_path=path, temporary_path=Path(temporary_name), handle=handle)


def _finish_atomic_text(target: AtomicTextTarget) -> None:
    target.handle.flush()
    os.fsync(target.handle.fileno())
    target.handle.close()
    target.temporary_path.replace(target.final_path)


def _discard_atomic_text(target: Optional[AtomicTextTarget]) -> None:
    if target is None:
        return
    try:
        if not target.handle.closed:
            target.handle.close()
    finally:
        target.temporary_path.unlink(missing_ok=True)


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    target: Optional[AtomicTextTarget] = None
    try:
        target = _open_atomic_text(path)
        target.handle.write(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n")
        _finish_atomic_text(target)
    except Exception as exc:
        _discard_atomic_text(target)
        raise OutputError(f"unable to write JSON atomically: {path}: {exc}") from exc


def _sqlite_read_only_uri(path: Path) -> str:
    encoded = quote(path.resolve().as_posix(), safe="/:")
    return f"file:{encoded}?mode=ro"


def _load_python_module(path: Path) -> ModuleType:
    if not path.is_file():
        raise ConfigError(f"canonicalizer entry point is missing: {path}")
    module_name = f"content_delta_filter_canonicalizer_{_file_sha256(path)[:16]}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"unable to load canonicalizer module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise ConfigError(f"canonicalizer import failed: {type(exc).__name__}: {exc}") from exc
    return module


# ============================================================
# 默认映射与安全默认值
# ============================================================

DEFAULT_CONFIG = Path("config") / "action" / "config" / "content_delta_filter_config_v0001.yml"


# ============================================================
# 核心类
# ============================================================

class ContentDeltaFilter:
    """Read-only exact-content filter backed by the Action concept hash index."""

    def __init__(self, config: FilterConfig):
        self.config = config
        self._canonicalizer_module = _load_python_module(config.canonicalizer_entry_point)
        canonicalize = getattr(self._canonicalizer_module, "canonicalize_text", None)
        hash_text = getattr(self._canonicalizer_module, "sha256_hex_text", None)
        if not callable(canonicalize) or not callable(hash_text):
            raise ContractError("canonicalizer must expose canonicalize_text and sha256_hex_text")
        self._canonicalize = canonicalize
        self._hash_text = hash_text
        self._canonicalization_payload = self._load_canonicalization_payload()
        rules = self._canonicalization_payload.get("canonicalization")
        if not isinstance(rules, dict):
            raise ConfigError("canonicalization rules file must contain canonicalization mapping")
        self._canonicalization_rules = rules

    def _load_canonicalization_payload(self) -> dict[str, Any]:
        path = self.config.canonicalization_rules
        if not path.is_file():
            raise ConfigError(f"canonicalization rules file is missing: {path}")
        try:
            payload = yaml.safe_load(path.read_text(encoding=DEFAULT_ENCODING))
        except Exception as exc:
            raise ConfigError(f"unable to read canonicalization rules: {exc}") from exc
        if not isinstance(payload, dict):
            raise ConfigError("canonicalization rules root must be a mapping")
        return payload

    @property
    def canonicalization_version(self) -> str:
        value = self._canonicalization_payload.get("version")
        if not isinstance(value, str) or not value:
            raise ConfigError("canonicalization rules version must be a non-empty string")
        return value

    def canonicalize_and_hash(self, unit_text: str) -> tuple[str, str]:
        try:
            canonical_text = self._canonicalize(unit_text, self._canonicalization_rules)
        except Exception as exc:
            raise ContractError(f"canonicalization failed: {exc}") from exc
        if not isinstance(canonical_text, str) or not canonical_text:
            raise ContractError("canonicalization produced empty text")
        content_hash = self._hash_text(canonical_text)
        if not isinstance(content_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", content_hash):
            raise ContractError("canonicalizer produced an invalid SHA-256 hex digest")
        return canonical_text, content_hash

    def connect_read_only(self, database_path: Path) -> sqlite3.Connection:
        if not database_path.is_file():
            raise DatabaseError(f"database is not a file: {database_path}")
        connection: Optional[sqlite3.Connection] = None
        try:
            connection = sqlite3.connect(
                _sqlite_read_only_uri(database_path),
                uri=True,
                timeout=30,
            )
            connection.execute("PRAGMA query_only = ON")
            self._validate_database_contract(connection)
            return connection
        except ContentDeltaFilterError:
            if connection is not None:
                connection.close()
            raise
        except sqlite3.Error as exc:
            if connection is not None:
                connection.close()
            raise DatabaseError(f"unable to open authoritative database read-only: {exc}") from exc

    def _validate_database_contract(self, connection: sqlite3.Connection) -> None:
        table = self.config.table
        hash_field = self.config.hash_field
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None:
            raise DatabaseError(f"missing content table: {table}")
        columns = {str(item[1]) for item in connection.execute(f"PRAGMA table_info({table})")}
        if hash_field not in columns:
            raise DatabaseError(f"table {table} is missing hash field: {hash_field}")
        if self.config.require_index and not self._hash_field_is_indexed(connection):
            raise DatabaseError(f"content hash field is not indexed: {table}.{hash_field}")

    def _hash_field_is_indexed(self, connection: sqlite3.Connection) -> bool:
        for index_row in connection.execute(f"PRAGMA index_list({self.config.table})"):
            index_name = str(index_row[1])
            fields = [str(item[2]) for item in connection.execute(f"PRAGMA index_info({index_name})")]
            if fields and fields[0] == self.config.hash_field:
                return True
        return False

    def lookup_existing_hashes(
        self,
        connection: sqlite3.Connection,
        content_hashes: Sequence[str],
    ) -> set[str]:
        if not content_hashes:
            return set()
        placeholders = ",".join("?" for _ in content_hashes)
        query = (
            f"SELECT {self.config.hash_field} FROM {self.config.table} "
            f"WHERE {self.config.hash_field} IN ({placeholders})"
        )
        try:
            return {str(row[0]) for row in connection.execute(query, tuple(content_hashes))}
        except sqlite3.Error as exc:
            raise DatabaseError(f"content hash batch query failed: {exc}") from exc


# ============================================================
# Schema、契约和兼容性辅助函数
# ============================================================

def load_filter_config(config_path: Path, project_root: Path) -> FilterConfig:
    if not config_path.is_file():
        raise ConfigError(f"filter config is missing: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding=DEFAULT_ENCODING))
    except Exception as exc:
        raise ConfigError(f"unable to load filter config: {exc}") from exc
    root = _require_mapping(payload, "config")
    if root.get("version") != SCRIPT_VERSION:
        raise ConfigError(f"config version must be {SCRIPT_VERSION}")

    input_config = _require_mapping(root.get("input"), "config.input")
    text_field = _require_safe_identifier(input_config.get("text_field"), "config.input.text_field")
    required_fields = _require_string_list(input_config.get("required_fields"), "config.input.required_fields")
    if text_field not in required_fields:
        raise ConfigError("text field must be included in required fields")

    canonicalizer = _require_mapping(root.get("canonicalizer"), "config.canonicalizer")
    entry_point = _resolve_from_root(canonicalizer.get("entry_point"), project_root, "canonicalizer.entry_point")
    rules_source = _resolve_from_root(canonicalizer.get("rules_source"), project_root, "canonicalizer.rules_source")

    comparison = _require_mapping(root.get("comparison"), "config.comparison")
    table = _require_safe_identifier(comparison.get("table"), "config.comparison.table")
    hash_field = _require_safe_identifier(comparison.get("hash_field"), "config.comparison.hash_field")
    batch_size = comparison.get("batch_size")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise ConfigError("config.comparison.batch_size must be an integer")
    if batch_size < 1 or batch_size > MAX_SQLITE_BATCH_SIZE:
        raise ConfigError(f"config.comparison.batch_size must be between 1 and {MAX_SQLITE_BATCH_SIZE}")
    require_index = comparison.get("require_index")
    if not isinstance(require_index, bool):
        raise ConfigError("config.comparison.require_index must be boolean")

    return FilterConfig(
        text_field=text_field,
        required_fields=required_fields,
        canonicalizer_entry_point=entry_point,
        canonicalization_rules=rules_source,
        table=table,
        hash_field=hash_field,
        batch_size=batch_size,
        require_index=require_index,
    )


def _validate_paths(
    *,
    input_path: Path,
    database_path: Path,
    output_path: Path,
    manifest_path: Path,
    issues_path: Path,
) -> None:
    if not input_path.is_file():
        raise InputError(f"input is not a file: {input_path}")
    if not database_path.is_file():
        raise InputError(f"database is not a file: {database_path}")
    targets = [output_path, manifest_path, issues_path]
    if len(set(targets)) != len(targets):
        raise InputError("output, manifest, and issues paths must differ")
    for target in targets:
        if target in {input_path, database_path}:
            raise InputError("outputs must not overwrite the input or database")
        if target.exists():
            raise InputError(f"output target already exists: {target}")


def _invalid_issue(
    *,
    line_number: int,
    issue_type: str,
    detail: str,
    record: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    hint: dict[str, Any] = {}
    if record is not None:
        for field in ("unit_id", "asset_id", "path", "value_index", "segment_index", "sentence_index", "message_id"):
            if field in record:
                hint[field] = record.get(field)
    return {
        "type": issue_type,
        "line_number": line_number,
        "detail": detail,
        "record_hint": hint,
    }


def _write_jsonl_record(handle: Optional[TextIO], payload: Mapping[str, Any]) -> None:
    if handle is not None:
        handle.write(_safe_json_dumps(dict(payload)) + "\n")


def _process_batch(
    *,
    staged: list[StagedRecord],
    engine: ContentDeltaFilter,
    connection: sqlite3.Connection,
    output_handle: Optional[TextIO],
    stats: FilterStats,
) -> None:
    if not staged:
        return
    lookup_started = time.perf_counter()
    existing = engine.lookup_existing_hashes(connection, [item.content_hash for item in staged])
    stats.database_lookup_ms += int((time.perf_counter() - lookup_started) * 1000)
    write_started = time.perf_counter()
    for item in staged:
        if item.content_hash in existing:
            stats.existing_records += 1
            stats.existing_database_records += 1
            continue
        enriched = dict(item.record)
        enriched["content_hash"] = item.content_hash
        enriched["content_filter_version"] = SCRIPT_VERSION
        enriched["canonicalization_version"] = engine.canonicalization_version
        _write_jsonl_record(output_handle, enriched)
        stats.new_records += 1
    stats.output_write_ms += int((time.perf_counter() - write_started) * 1000)
    staged.clear()


def execute_filter(
    *,
    engine: ContentDeltaFilter,
    input_path: Path,
    database_path: Path,
    output_path: Path,
    manifest_path: Path,
    issues_path: Path,
    dry_run: bool,
) -> dict[str, Any]:
    input_path = input_path.resolve()
    database_path = database_path.resolve()
    output_path = output_path.resolve()
    manifest_path = manifest_path.resolve()
    issues_path = issues_path.resolve()
    _validate_paths(
        input_path=input_path,
        database_path=database_path,
        output_path=output_path,
        manifest_path=manifest_path,
        issues_path=issues_path,
    )

    started = time.perf_counter()
    stats = FilterStats()
    staged: list[StagedRecord] = []
    seen_hashes: set[str] = set()
    output_target: Optional[AtomicTextTarget] = None
    issues_target: Optional[AtomicTextTarget] = None
    connection: Optional[sqlite3.Connection] = None

    try:
        connection = engine.connect_read_only(database_path)
        if not dry_run:
            output_target = _open_atomic_text(output_path)
            issues_target = _open_atomic_text(issues_path)
        output_handle = output_target.handle if output_target is not None else None
        issues_handle = issues_target.handle if issues_target is not None else None

        for line_number, raw_line in _iter_jsonl_lines(input_path):
            stats.input_records += 1
            try:
                parsed = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                stats.invalid_records += 1
                _write_jsonl_record(
                    issues_handle,
                    _invalid_issue(
                        line_number=line_number,
                        issue_type="json_parse_error",
                        detail=str(exc),
                    ),
                )
                continue
            if not isinstance(parsed, dict):
                stats.invalid_records += 1
                _write_jsonl_record(
                    issues_handle,
                    _invalid_issue(
                        line_number=line_number,
                        issue_type="record_type_error",
                        detail="record must be a JSON object",
                    ),
                )
                continue
            missing = [field for field in engine.config.required_fields if field not in parsed]
            if missing:
                stats.invalid_records += 1
                _write_jsonl_record(
                    issues_handle,
                    _invalid_issue(
                        line_number=line_number,
                        issue_type="missing_required_fields",
                        detail=f"missing required fields: {missing}",
                        record=parsed,
                    ),
                )
                continue
            unit_text = parsed.get(engine.config.text_field)
            if not isinstance(unit_text, str) or not unit_text.strip():
                stats.invalid_records += 1
                _write_jsonl_record(
                    issues_handle,
                    _invalid_issue(
                        line_number=line_number,
                        issue_type="invalid_unit_text",
                        detail="unit_text must be a non-empty string",
                        record=parsed,
                    ),
                )
                continue
            hash_started = time.perf_counter()
            try:
                canonical_text, content_hash = engine.canonicalize_and_hash(unit_text)
            except ContractError as exc:
                stats.hash_compute_ms += int((time.perf_counter() - hash_started) * 1000)
                stats.invalid_records += 1
                _write_jsonl_record(
                    issues_handle,
                    _invalid_issue(
                        line_number=line_number,
                        issue_type="canonicalization_error",
                        detail=str(exc),
                        record=parsed,
                    ),
                )
                continue
            stats.hash_compute_ms += int((time.perf_counter() - hash_started) * 1000)
            supplied_hash = parsed.get("content_hash")
            if supplied_hash is not None and supplied_hash != content_hash:
                stats.invalid_records += 1
                _write_jsonl_record(
                    issues_handle,
                    _invalid_issue(
                        line_number=line_number,
                        issue_type="content_hash_conflict",
                        detail="input content_hash does not match canonicalized unit_text",
                        record=parsed,
                    ),
                )
                continue

            stats.valid_records += 1
            if content_hash in seen_hashes:
                stats.existing_records += 1
                stats.existing_within_run_records += 1
                continue
            seen_hashes.add(content_hash)
            staged.append(
                StagedRecord(
                    line_number=line_number,
                    record=parsed,
                    canonical_text=canonical_text,
                    content_hash=content_hash,
                )
            )
            if len(staged) >= engine.config.batch_size:
                _process_batch(
                    staged=staged,
                    engine=engine,
                    connection=connection,
                    output_handle=output_handle,
                    stats=stats,
                )

        _process_batch(
            staged=staged,
            engine=engine,
            connection=connection,
            output_handle=output_handle,
            stats=stats,
        )
        if stats.input_records != stats.new_records + stats.existing_records + stats.invalid_records:
            raise ContractError("count conservation failed")
        if stats.valid_records != stats.new_records + stats.existing_records:
            raise ContractError("valid record conservation failed")

        if not dry_run:
            if output_target is None or issues_target is None:
                raise OutputError("atomic output targets were not initialized")
            _finish_atomic_text(output_target)
            _finish_atomic_text(issues_target)

        total_ms = int((time.perf_counter() - started) * 1000)
        result: dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "status": "dry_run" if dry_run else "completed",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "input": {
                "path": str(input_path),
                "sha256": _file_sha256(input_path),
            },
            "comparison": {
                "database": str(database_path),
                "table": engine.config.table,
                "field": engine.config.hash_field,
                "read_only": True,
                "batch_size": engine.config.batch_size,
            },
            "canonicalization": {
                "entry_point": str(engine.config.canonicalizer_entry_point),
                "entry_point_sha256": _file_sha256(engine.config.canonicalizer_entry_point),
                "rules_source": str(engine.config.canonicalization_rules),
                "rules_source_sha256": _file_sha256(engine.config.canonicalization_rules),
                "version": engine.canonicalization_version,
                "hash_algorithm": "sha256",
            },
            "counts": asdict(stats),
            "metrics": {
                "total_ms": total_ms,
                "new_ratio": (stats.new_records / stats.valid_records) if stats.valid_records else 0.0,
            },
            "output": {
                "new_content_units": str(output_path),
                "issues": str(issues_path),
                "manifest": str(manifest_path),
            },
            "ready_for_downstream": stats.new_records > 0 and stats.invalid_records == 0,
            "database_modified": False,
        }
        if not dry_run:
            result["output"]["new_content_units_sha256"] = _file_sha256(output_path)
            result["output"]["issues_sha256"] = _file_sha256(issues_path)
            _write_json_atomic(manifest_path, result)
        return result
    except ContentDeltaFilterError:
        _discard_atomic_text(output_target)
        _discard_atomic_text(issues_target)
        raise
    except Exception as exc:
        _discard_atomic_text(output_target)
        _discard_atomic_text(issues_target)
        raise ContentDeltaFilterError(f"unexpected filter failure: {type(exc).__name__}: {exc}") from exc
    finally:
        if connection is not None:
            connection.close()


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter normalized semantic JSONL to exact content hashes absent from Action SQLite."
    )
    parser.add_argument("--input", required=True, help="Input semantic text units JSONL.")
    parser.add_argument("--database", required=True, help="Existing Action SQLite database queried read-only.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Versioned content filter YAML config.")
    parser.add_argument("--output", required=True, help="NEW-only semantic text units JSONL.")
    parser.add_argument("--manifest", required=True, help="Completed filter manifest JSON.")
    parser.add_argument("--issues", required=True, help="Invalid record issues JSONL.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and compare without writing outputs.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        script_path = Path(__file__).resolve()
        project_root = _find_project_root(script_path.parent)
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = (project_root / config_path).resolve()
        config = load_filter_config(config_path, project_root)
        engine = ContentDeltaFilter(config)
        result = execute_filter(
            engine=engine,
            input_path=Path(args.input),
            database_path=Path(args.database),
            output_path=Path(args.output),
            manifest_path=Path(args.manifest),
            issues_path=Path(args.issues),
            dry_run=bool(args.dry_run),
        )
        print(_safe_json_dumps(result))
        return 0
    except ContentDeltaFilterError as exc:
        print(_safe_json_dumps({
            "status": "error",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }))
        return 2
    except Exception as exc:
        print(_safe_json_dumps({
            "status": "error",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
