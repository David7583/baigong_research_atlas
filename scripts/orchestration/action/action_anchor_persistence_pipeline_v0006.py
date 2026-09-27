# ============================================================
# 文件名: action_anchor_persistence_pipeline_v0006.py
# 中文名: 自包含语义锚定与持久化编排
# 版本号: v0006
#
# 主层级: action
# 层级: scripts/orchestration/action
# 脚本定位: 保持已验证契约的自包含版本实现
#
# 职责说明:
# - 在本文件中完整实现本脚本职责，不加载同家族旧版
# 本脚本做什么:
# - 保留输入校验、处理顺序、来源证据与失败行为
# 本脚本不做什么:
# - 不替代独立业务子脚本，不迁移原始数据
# 制度边界声明:
# - 仅向显式配置目标写入；测试写入必须隔离，失败不得伪报成功
# - 原始输入只读；模型选择沿用配置，不包含密钥
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: action_anchor_persistence_pipeline
# family: action_anchor_persistence_pipeline
# role: action_anchor_persistence_orchestrator
# version: v0006
# status: active
# entry_point: scripts/orchestration/action/action_anchor_persistence_pipeline_v0006.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - check_sql_integrity_v0005.py
#   - ingest_attribute_units_v0003.py
#   - ingest_concept_units_v0002.py
#   - ingest_instance_units_v0003.py
#   - replay_ingest_run_v0004.py
#   - sql_writer_v0004.py
#   - validate_ingest_payload_v0003.py
#   - verify_data_anchor_v0004.py
# used_by:
#   - data_action_chain_pipeline_v0017
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/orchestration/action/action_anchor_persistence_pipeline_v0001.py, scripts/orchestration/action/action_anchor_persistence_pipeline_v0004.py
# ============================================================
# 依赖
# ============================================================

from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from typing import Any, Optional, Sequence

# ============================================================
# 全局常量与默认映射
# ============================================================

SCRIPT_FAMILY = "action_anchor_persistence_pipeline"


UPSTREAM_FAMILY = "structural_unit_governance_graph_pipeline"


UPSTREAM_VERSION = "v0003"


DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "action"


DEFAULT_BUSINESS_ROOT = Path("actioning") / "anchor" / "orchestration"


DEFAULT_SQL_WRITER_CONFIG = Path("config") / "action" / "config" / "sql_writer_config_v0001.yml"


ANCHOR_DIR = Path("scripts") / "action" / "anchor"


RUN_ID_PATTERN = re.compile("^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")


SQL_IDENTIFIER_PATTERN = re.compile("^[A-Za-z_][A-Za-z0-9_]*$")


WINDOWS_SAFE_PATH_LENGTH = 240


PINNED_SCRIPTS = {
    "concept": "ingest_concept_units_v0002.py",
    "instance": "ingest_instance_units_v0003.py",
    "attribute": "ingest_attribute_units_v0003.py",
    "validate_payload": "validate_ingest_payload_v0003.py",
    "verify_anchor": "verify_data_anchor_v0004.py",
    "replay_compare": "replay_ingest_run_v0004.py",
    "sql_writer": "sql_writer_v0004.py",
    "check_integrity": "check_sql_integrity_v0005.py",
}


PINNED_CONFIGS = {
    "concept": Path("config") / "action" / "config" / "ingest_concept_units_config_v0001.yml",
    "instance": Path("config") / "action" / "config" / "ingest_instance_units_config_v0001.yml",
    "attribute": Path("config") / "action" / "config" / "ingest_attribute_units_config_v0001.yml",
}


STEP_NAMES = [
    "baseline_concept_declarations",
    "baseline_instance_declarations",
    "baseline_attribute_declarations",
    "replay_concept_declarations",
    "replay_instance_declarations",
    "replay_attribute_declarations",
    "compare_replay_outputs",
    "validate_ingest_payload",
    "verify_data_anchor",
    "write_action_sql_payload",
    "check_sql_integrity",
]


SCRIPT_NAME = "action_anchor_persistence_pipeline_v0006.py"


SCRIPT_VERSION = "v0006"


DEFAULT_INSTANCE_CONFIG = (
    Path("config") / "action" / "config" / "ingest_instance_units_semantic_config_v0001.yml"
)


DEFAULT_ENCODING = "utf-8"

# ============================================================
# 异常类型
# ============================================================


class PipelineError(RuntimeError):
    """Raised when the Anchor pipeline cannot continue safely."""


# ============================================================
# 数据结构、工具与核心实现
# ============================================================


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"r_{stamp}_{uuid.uuid4().hex[:6]}"


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def load_json(path: Path, label: str) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PipelineError(f"{label} is not valid UTF-8 JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PipelineError(f"{label} must contain a JSON object: {path}")
    return value


def read_jsonl(path: Path, label: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                text = line.strip()
                if not text:
                    continue
                value = json.loads(text)
                if not isinstance(value, dict):
                    raise ValueError("record is not an object")
                rows.append(value)
    except Exception as exc:
        raise PipelineError(f"{label} has invalid JSONL at {path}: {exc}") from exc
    return rows


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def combined_sha256(paths: Sequence[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.resolve()).encode("utf-8"))
        digest.update(b"\x00")
        digest.update(bytes.fromhex(file_sha256(path)))
    return digest.hexdigest()


def resolve_project_root(script_path: Path, override: Optional[str]) -> Path:
    if override:
        root = Path(override).expanduser().resolve()
    else:
        root = script_path.resolve().parent
        for candidate in [root, *root.parents]:
            if (candidate / "AGENTS.md").is_file() and (candidate / "scripts").is_dir():
                root = candidate
                break
        else:
            raise PipelineError("cannot locate project root from script path")
    if not (root / "scripts").is_dir() or not (root / "config").is_dir():
        raise PipelineError(f"invalid project root: {root}")
    return root


def resolve_path(value: str | Path, project_root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def require_within(path: Path, parent: Path, label: str) -> None:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError as exc:
        raise PipelineError(f"{label} must stay under {parent}: {path}") from exc


def require_safe_identifier(value: str, label: str) -> str:
    if not SQL_IDENTIFIER_PATTERN.fullmatch(value):
        raise PipelineError(f"unsafe SQL identifier for {label}: {value!r}")
    return value


def require_file(path: Path, label: str) -> Path:
    if not path.is_file():
        raise PipelineError(f"{label} is missing: {path}")
    return path


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def resolve_pinned_dependencies(project_root: Path) -> Tuple[Dict[str, Path], Dict[str, Path]]:
    scripts: Dict[str, Path] = {}
    for key, filename in PINNED_SCRIPTS.items():
        path = (project_root / ANCHOR_DIR / filename).resolve()
        require_file(path, f"pinned script {key}")
        scripts[key] = path
    configs: Dict[str, Path] = {}
    for key, relative_path in PINNED_CONFIGS.items():
        path = (project_root / relative_path).resolve()
        require_file(path, f"pinned config {key}")
        configs[key] = path
    return (scripts, configs)


def validate_planned_path_lengths(run: "PipelineRun") -> int:
    planned_paths = [
        run.run_dir / "captured_stdout" / "11_check_sql_integrity.txt",
        run.run_dir / "completion_manifest.json",
        run.business_run_dir / "02_replay" / "attribute" / "attribute_declarations.jsonl",
        run.business_run_dir / "03_reports" / "sql_integrity_report.json",
    ]
    maximum = max((len(str(path)) for path in planned_paths))
    if os.name == "nt" and maximum > WINDOWS_SAFE_PATH_LENGTH:
        raise PipelineError(
            f"planned Anchor pipeline path is too long for reliable child-script execution ({maximum} > {WINDOWS_SAFE_PATH_LENGTH}); use shorter roots or run-id"
        )
    return maximum


def validate_upstream_manifest(path: Path, project_root: Path) -> Dict[str, Any]:
    require_file(path, "upstream completion manifest")
    completion = load_json(path, "upstream completion manifest")
    if completion.get("pipeline") != UPSTREAM_FAMILY:
        raise PipelineError(
            f"upstream pipeline must be {UPSTREAM_FAMILY}, got {completion.get('pipeline')!r}"
        )
    if completion.get("pipeline_version") != UPSTREAM_VERSION:
        raise PipelineError(
            f"upstream pipeline version must be {UPSTREAM_VERSION}, got {completion.get('pipeline_version')!r}"
        )
    if completion.get("status") != "completed":
        raise PipelineError("upstream structural pipeline status is not completed")
    upstream_run_id = completion.get("run_id")
    if not isinstance(upstream_run_id, str) or not upstream_run_id:
        raise PipelineError("upstream completion manifest is missing run_id")
    business_value = completion.get("business_run_dir")
    if not isinstance(business_value, str) or not business_value:
        raise PipelineError("upstream completion manifest is missing business_run_dir")
    business_run_dir = resolve_path(business_value, project_root)
    require_within(business_run_dir, project_root, "upstream business_run_dir")
    if not business_run_dir.is_dir():
        raise PipelineError(f"upstream business_run_dir is missing: {business_run_dir}")
    prominence = business_run_dir / "10_prominence" / "unit_prominence_decisions.jsonl"
    require_file(prominence, "upstream prominence decisions")
    source_inputs = completion.get("source_inputs")
    if not isinstance(source_inputs, dict):
        raise PipelineError("upstream completion manifest is missing source_inputs")
    source_paths: Dict[str, str] = {}
    for key in ("working_copy", "structure_report"):
        value = source_inputs.get(key)
        if not isinstance(value, str) or not value:
            raise PipelineError(f"upstream source_inputs is missing {key}")
        source_path = resolve_path(value, project_root)
        require_within(source_path, project_root, f"upstream {key}")
        require_file(source_path, f"upstream {key}")
        source_paths[key] = str(source_path)
    return {
        "completion": completion,
        "completion_manifest": str(path),
        "run_id": upstream_run_id,
        "business_run_dir": str(business_run_dir),
        "prominence_decisions": str(prominence),
        "source_inputs": source_paths,
    }


def load_sql_writer_module(path: Path) -> Any:
    module_name = f"_anchor_sql_writer_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PipelineError(f"cannot load sql_writer module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise PipelineError(f"failed to import sql_writer module: {exc}") from exc
    return module


def resolve_writer_database(writer: Any) -> Path:
    configured = Path(writer.config.sqlite_path)
    if not configured.is_absolute():
        configured = writer.project_root / configured
    return configured.resolve()


def validate_database_contracts(
    *,
    project_root: Path,
    data_db: Path,
    action_db: Path,
    sql_writer_config: Path,
    sql_writer_script: Path,
    data_table: str,
    data_fields: Mapping[str, str],
    test_mode: bool,
    validate_action_schema: bool,
) -> Tuple[Any, Any]:
    require_file(data_db, "data-layer SQLite database")
    require_file(action_db, "Action data SQLite database")
    require_file(sql_writer_config, "sql_writer configuration")
    require_safe_identifier(data_table, "data table")
    protected_action_db = (project_root / "sql" / "action.db").resolve()
    if action_db.resolve() == protected_action_db:
        raise PipelineError("sql/action.db is the Action development registry and cannot be used here")
    if test_mode:
        temp_root = (project_root / "temp").resolve()
        require_within(data_db, temp_root, "test data database")
        require_within(action_db, temp_root, "test Action database")
        require_within(sql_writer_config, temp_root, "test sql_writer config")
    module = load_sql_writer_module(sql_writer_script)
    try:
        writer = module.SqlWriter(sql_writer_config, strict=True, verify_schema_on_connect=True)
    except Exception as exc:
        raise PipelineError(f"cannot load sql_writer configuration: {exc}") from exc
    configured_db = resolve_writer_database(writer)
    if configured_db != action_db.resolve():
        raise PipelineError(
            f"sql_writer configuration database does not match --action-db: {configured_db} != {action_db}"
        )
    try:
        conn = sqlite3.connect(f"file:{data_db.as_posix()}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (data_table,)
        ).fetchone()
        columns = {str(item[1]) for item in conn.execute(f"PRAGMA table_info({data_table})").fetchall()}
        conn.close()
    except Exception as exc:
        raise PipelineError(f"cannot inspect data-layer SQLite database: {exc}") from exc
    if row is None:
        raise PipelineError(f"data anchor table is missing: {data_table}")
    missing_columns = sorted(set(data_fields.values()) - columns)
    if missing_columns:
        raise PipelineError(f"data anchor table is missing required columns: {missing_columns}")
    if validate_action_schema:
        try:
            conn = writer.connect()
            conn.close()
        except Exception as exc:
            raise PipelineError(f"Action data SQLite schema validation failed: {exc}") from exc
    return (module, writer)


@dataclass
class PipelineRun:
    project_root: Path
    output_root: Path
    business_root: Path
    run_id: str
    test_mode: bool

    def __post_init__(self) -> None:
        self.run_dir = self.output_root / SCRIPT_FAMILY / SCRIPT_VERSION / self.run_id
        self.business_run_dir = self.business_root / SCRIPT_FAMILY / SCRIPT_VERSION / self.run_id
        self.stdout_dir = self.run_dir / "captured_stdout"
        self.stderr_dir = self.run_dir / "captured_stderr"
        self.step_status_path = self.run_dir / "step_status.jsonl"
        self.run_manifest_path = self.run_dir / "run_manifest.json"
        self.failure_path = self.run_dir / "failure.json"
        self.completion_path = self.run_dir / "completion_manifest.json"
        self.sql_results_path = self.run_dir / "sql_write_results.jsonl"
        self.steps: List[Dict[str, Any]] = []
        self.started_at = utc_now_z()
        self.db_write_started = False
        self.writer: Any = None

    def initialize(self, manifest: Mapping[str, Any]) -> None:
        if self.run_dir.exists():
            raise PipelineError(f"run directory already exists: {self.run_dir}")
        if self.business_run_dir.exists():
            raise PipelineError(f"business run directory already exists: {self.business_run_dir}")
        self.stdout_dir.mkdir(parents=True, exist_ok=False)
        self.stderr_dir.mkdir(parents=True, exist_ok=False)
        self.business_run_dir.mkdir(parents=True, exist_ok=False)
        write_json_atomic(self.run_manifest_path, dict(manifest))

    def update_manifest(self, **values: Any) -> None:
        manifest = load_json(self.run_manifest_path, "run manifest")
        manifest.update(values)
        manifest["steps"] = self.steps
        write_json_atomic(self.run_manifest_path, manifest)

    def add_step(self, step: Mapping[str, Any]) -> None:
        payload = dict(step)
        self.steps.append(payload)
        append_jsonl(self.step_status_path, payload)
        self.update_manifest(status="running")


def run_child_step(
    run: PipelineRun, *, index: int, name: str, script: Path, arguments: Sequence[str], timeout_seconds: int
) -> None:
    command = [sys.executable, str(script), *[str(value) for value in arguments]]
    started_at = utc_now_z()
    started = time.monotonic()
    timed_out = False
    try:
        completed = subprocess.run(
            command,
            cwd=str(run.project_root),
            shell=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
        returncode = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        returncode = None
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        stderr = f"{stderr}\nTimeout after {timeout_seconds} seconds".strip()
    duration_ms = int((time.monotonic() - started) * 1000)
    stdout_path = run.stdout_dir / f"{index:02d}_{name}.txt"
    stderr_path = run.stderr_dir / f"{index:02d}_{name}.txt"
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    status = "success" if returncode == 0 and (not timed_out) else "failed"
    step = {
        "index": index,
        "name": name,
        "script": script.name,
        "command": command,
        "shell": False,
        "started_at": started_at,
        "finished_at": utc_now_z(),
        "duration_ms": duration_ms,
        "returncode": returncode,
        "timed_out": timed_out,
        "status": status,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }
    run.add_step(step)
    if status != "success":
        raise PipelineError(f"step {index:02d} {name} failed; see {stderr_path}")


def require_report_pass(path: Path, label: str) -> Dict[str, Any]:
    require_file(path, label)
    report = load_json(path, label)
    if report.get("status") != "PASS":
        raise PipelineError(f"{label} did not PASS: {path}")
    return report


def validate_declarations(
    concept_path: Path, instance_path: Path, attribute_path: Path
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    concepts = read_jsonl(concept_path, "concept declarations")
    instances = read_jsonl(instance_path, "instance declarations")
    attributes = read_jsonl(attribute_path, "attribute declarations")
    concept_ids = {row.get("concept_id") for row in concepts}
    if None in concept_ids or len(concept_ids) != len(concepts):
        raise PipelineError("concept declarations contain missing or duplicate concept_id")
    instance_ids = {row.get("instance_id") for row in instances}
    if None in instance_ids or len(instance_ids) != len(instances):
        raise PipelineError("instance declarations contain missing or duplicate instance_id")
    if any((row.get("concept_id") not in concept_ids for row in instances)):
        raise PipelineError("instance declarations contain a concept reference outside the payload")
    for row in attributes:
        object_type = row.get("object_type")
        object_id = row.get("object_id")
        if object_type == "concept" and object_id not in concept_ids:
            raise PipelineError("attribute declaration references a concept outside the payload")
        if object_type == "instance" and object_id not in instance_ids:
            raise PipelineError("attribute declaration references an instance outside the payload")
    return (concepts, instances, attributes)


def write_replay_comparison_view(source: Path, target: Path) -> None:
    """Preserve raw declarations and remove only non-identity generation time in a derived view."""
    rows = read_jsonl(source, f"replay comparison source {source.name}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{target.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            stable = dict(row)
            stable.pop("generated_at", None)
            handle.write(json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(target)


def find_exact_attribute_id(
    writer: Any,
    *,
    object_type: str,
    object_id: str,
    attr_key: str,
    attr_value: Optional[str],
    attr_state: str,
    evidence_ref: Optional[str],
    created_by: Optional[str],
) -> Optional[str]:
    """Read-only compatibility check for sql_writer v0004 duplicate attributes."""
    table = writer.config.tables["attribute"]
    fields = writer.config.fields["attribute"]
    conn = writer.connect()
    try:
        row = conn.execute(
            f"SELECT {fields['id']} FROM {table} WHERE {fields['object_type']} = ? AND {fields['object_id']} = ? AND {fields['attr_scope']} IS ? AND {fields['attr_key']} = ? AND {fields['attr_value']} IS ? AND {fields['attr_type']} IS ? AND {fields['attr_state']} = ? AND {fields['evidence_ref']} IS ? AND {fields['created_by']} IS ?",
            (object_type, object_id, None, attr_key, attr_value, None, attr_state, evidence_ref, created_by),
        ).fetchone()
        return None if row is None else str(row[0])
    finally:
        conn.close()


def write_sql_payload(
    run: PipelineRun,
    *,
    writer: Any,
    concepts: Sequence[Mapping[str, Any]],
    instances: Sequence[Mapping[str, Any]],
    attributes: Sequence[Mapping[str, Any]],
    input_hash: str,
) -> Dict[str, Any]:
    index = 10
    name = "write_action_sql_payload"
    started_at = utc_now_z()
    started = time.monotonic()
    counts = {
        "concept_inserted": 0,
        "concept_existing": 0,
        "instance_inserted": 0,
        "instance_duplicate": 0,
        "attribute_inserted": 0,
        "attribute_existing": 0,
    }
    run.writer = writer
    try:
        result = writer.record_run_event(
            run_id=run.run_id,
            script_name=SCRIPT_FAMILY,
            script_version=SCRIPT_VERSION,
            status="started",
            error_summary=None,
            input_hash=input_hash,
        )
        run.db_write_started = True
        append_jsonl(run.sql_results_path, {"kind": "run_log", **result.__dict__})
        first_instance_by_concept: Dict[str, str] = {}
        for row in instances:
            first_instance_by_concept.setdefault(str(row["concept_id"]), str(row["instance_id"]))
        for row in concepts:
            canonical_text = str(row.get("canonical_text") or "")
            result = writer.write_concept(
                unit_text_id=str(row["concept_id"]),
                unit_text=canonical_text,
                content_hash=str(row["content_hash"]),
                schema_version=str(row["schema_version"]),
                first_seen_instance_id=first_instance_by_concept.get(str(row["concept_id"])),
                run_id=run.run_id,
            )
            append_jsonl(run.sql_results_path, {"kind": "concept", **result.__dict__})
            if result.status == "inserted":
                counts["concept_inserted"] += 1
            elif result.status == "existing":
                counts["concept_existing"] += 1
            else:
                raise PipelineError(f"unexpected concept writer status: {result.status}")
        for row in instances:
            observed = row.get("observed_at")
            if not isinstance(observed, dict):
                raise PipelineError("instance declaration observed_at is not an object")
            canonical_text = str(row.get("canonical_text") or "")
            result = writer.write_instance(
                instance_id=str(row["instance_id"]),
                unit_text_id=str(row["concept_id"]),
                asset_id=str(observed["asset_id"]),
                path=str(observed["path"]),
                value_index=observed["value_index"],
                segment_index=int(observed["segment_index"]),
                sentence_index=observed["sentence_index"],
                char_start=observed.get("char_start"),
                char_end=observed.get("char_end"),
                content=canonical_text,
                content_hash=str(row["content_hash"]),
                schema_version=str(row["schema_version"]),
                run_id=run.run_id,
            )
            append_jsonl(run.sql_results_path, {"kind": "instance", **result.__dict__})
            if result.status == "inserted":
                counts["instance_inserted"] += 1
            elif result.status == "duplicate":
                counts["instance_duplicate"] += 1
            else:
                raise PipelineError(f"unexpected instance writer status: {result.status}")
        for row in attributes:
            provenance = row.get("provenance")
            created_by = None
            evidence_ref = None
            if isinstance(provenance, dict):
                provenance_script = provenance.get("script", provenance.get("script_name", "unknown"))
                provenance_version = provenance.get("version", provenance.get("script_version", "unknown"))
                created_by = f"{provenance_script}:{provenance_version}"
                evidence_ref = json.dumps(
                    provenance, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            object_type = str(row["object_type"])
            object_id = str(row["object_id"])
            attr_key = str(row["attr_key"])
            attr_value = None if row.get("attr_value") is None else str(row.get("attr_value"))
            attr_state = str(row["attr_state"])
            try:
                result = writer.write_attribute(
                    object_type=object_type,
                    object_id=object_id,
                    attr_key=attr_key,
                    attr_value=attr_value,
                    attr_type=None,
                    attr_scope=None,
                    attr_state=attr_state,
                    evidence_ref=evidence_ref,
                    run_id=run.run_id,
                    created_by=created_by,
                )
                append_jsonl(run.sql_results_path, {"kind": "attribute", **result.__dict__})
                if result.status != "inserted":
                    raise PipelineError(f"unexpected attribute writer status: {result.status}")
                counts["attribute_inserted"] += 1
            except Exception as exc:
                if type(exc).__name__ != "IntegrityWriteError":
                    raise
                existing_id = find_exact_attribute_id(
                    writer,
                    object_type=object_type,
                    object_id=object_id,
                    attr_key=attr_key,
                    attr_value=attr_value,
                    attr_state=attr_state,
                    evidence_ref=evidence_ref,
                    created_by=created_by,
                )
                if existing_id is None:
                    raise
                append_jsonl(
                    run.sql_results_path,
                    {
                        "kind": "attribute",
                        "status": "existing",
                        "object": "attribute",
                        "object_id": existing_id,
                        "detail": "exact stable attribute already exists; verified after writer duplicate error",
                    },
                )
                counts["attribute_existing"] += 1
        output_hash = file_sha256(run.sql_results_path)
        finish = writer.record_run_event(
            run_id=run.run_id,
            script_name=SCRIPT_FAMILY,
            script_version=SCRIPT_VERSION,
            status="finished",
            error_summary=None,
            input_hash=input_hash,
            output_hash=output_hash,
        )
        append_jsonl(run.sql_results_path, {"kind": "run_log", **finish.__dict__})
        status = "success"
        error = None
    except Exception as exc:
        status = "failed"
        error = str(exc)
        if run.db_write_started:
            try:
                finish = writer.record_run_event(
                    run_id=run.run_id,
                    script_name=SCRIPT_FAMILY,
                    script_version=SCRIPT_VERSION,
                    status="finished",
                    error_summary=error[:500],
                    input_hash=input_hash,
                )
                append_jsonl(run.sql_results_path, {"kind": "run_log", **finish.__dict__})
            except Exception as finish_exc:
                error = f"{error}; failed to close run_log: {finish_exc}"
    duration_ms = int((time.monotonic() - started) * 1000)
    stdout_path = run.stdout_dir / f"{index:02d}_{name}.txt"
    stderr_path = run.stderr_dir / f"{index:02d}_{name}.txt"
    stdout_path.write_text(json.dumps(counts, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    stderr_path.write_text((error or "") + ("\n" if error else ""), encoding="utf-8")
    step = {
        "index": index,
        "name": name,
        "script": PINNED_SCRIPTS["sql_writer"],
        "call_mode": "public_python_api",
        "shell": False,
        "started_at": started_at,
        "finished_at": utc_now_z(),
        "duration_ms": duration_ms,
        "status": status,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "counts": counts,
    }
    run.add_step(step)
    if status != "success":
        raise PipelineError(f"SQL payload write failed: {error}")
    return counts


def query_action_counts(writer: Any, run_id: str) -> Dict[str, Any]:
    tables = writer.config.tables
    fields = writer.config.fields
    conn = writer.connect()
    try:
        return {
            "total": {
                "concept_rows": int(conn.execute(f"SELECT COUNT(*) FROM {tables['concept']}").fetchone()[0]),
                "instance_rows": int(
                    conn.execute(f"SELECT COUNT(*) FROM {tables['instance']}").fetchone()[0]
                ),
                "attribute_rows": int(
                    conn.execute(f"SELECT COUNT(*) FROM {tables['attribute']}").fetchone()[0]
                ),
                "run_log_rows": int(conn.execute(f"SELECT COUNT(*) FROM {tables['run_log']}").fetchone()[0]),
            },
            "current_run": {
                "concept_rows": int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {tables['concept']} WHERE {fields['concept']['run_id']} = ?",
                        (run_id,),
                    ).fetchone()[0]
                ),
                "instance_rows": int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {tables['instance']} WHERE {fields['instance']['run_id']} = ?",
                        (run_id,),
                    ).fetchone()[0]
                ),
                "attribute_rows": int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {tables['attribute']} WHERE {fields['attribute']['run_id']} = ?",
                        (run_id,),
                    ).fetchone()[0]
                ),
                "run_log_rows": int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {tables['run_log']} WHERE {fields['run_log']['run_id']} = ?",
                        (run_id,),
                    ).fetchone()[0]
                ),
            },
        }
    finally:
        conn.close()


def _build_parser_core() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reliably orchestrate Action Anchor identity declarations, preflight gates, controlled SQLite writes, and postflight integrity checks."
    )
    parser.add_argument("--project-root", help="Optional project root override.")
    parser.add_argument(
        "--completion-manifest",
        required=True,
        help="Exact completed structural_unit_governance_graph_pipeline completion manifest.",
    )
    parser.add_argument("--data-db", required=True, help="Exact data-layer SQLite database.")
    parser.add_argument("--data-table", required=True, help="Exact data-layer anchor table.")
    parser.add_argument("--data-asset-id-field", default="asset_id")
    parser.add_argument("--data-path-field", default="path")
    parser.add_argument("--data-segment-index-field", default="segment_index")
    parser.add_argument("--data-char-start-field", default="char_start")
    parser.add_argument("--data-char-end-field", default="char_end")
    parser.add_argument("--action-db", required=True, help="Exact Action data SQLite database.")
    parser.add_argument(
        "--sql-writer-config",
        default=str(DEFAULT_SQL_WRITER_CONFIG),
        help="sql_writer config whose database path must match --action-db.",
    )
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--business-root", default=str(DEFAULT_BUSINESS_ROOT))
    parser.add_argument("--run-id", help="Optional safe, unique run id.")
    parser.add_argument("--step-timeout", type=positive_integer, default=1800)
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--confirm-database-write", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def execute(args: argparse.Namespace) -> Dict[str, Any]:
    script_path = Path(__file__).resolve()
    project_root = resolve_project_root(script_path, args.project_root)
    completion_manifest = resolve_path(args.completion_manifest, project_root)
    data_db = resolve_path(args.data_db, project_root)
    action_db = resolve_path(args.action_db, project_root)
    sql_writer_config = resolve_path(args.sql_writer_config, project_root)
    output_root = resolve_path(args.output_root, project_root)
    business_root = resolve_path(args.business_root, project_root)
    data_table = require_safe_identifier(args.data_table, "data table")
    data_fields = {
        "value_index": require_safe_identifier(args.data_value_index_field, "value_index"),
        "sentence_index": require_safe_identifier(args.data_sentence_index_field, "sentence_index"),
        "asset_id": require_safe_identifier(args.data_asset_id_field, "data asset_id field"),
        "path": require_safe_identifier(args.data_path_field, "data path field"),
        "segment_index": require_safe_identifier(args.data_segment_index_field, "data segment_index field"),
        "char_start": require_safe_identifier(args.data_char_start_field, "data char_start field"),
        "char_end": require_safe_identifier(args.data_char_end_field, "data char_end field"),
    }
    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError("run_id contains unsupported characters")
    scripts, configs = resolve_pinned_dependencies(project_root)
    upstream = validate_upstream_manifest(completion_manifest, project_root)
    identity_input = resolve_path(args.identity_input, project_root)
    require_file(identity_input, "semantic identity input")
    configs["instance"] = resolve_path(args.semantic_instance_config, project_root)
    require_file(configs["instance"], "semantic instance config")
    if args.test_mode:
        require_within(identity_input, project_root / "temp", "test identity input")
    upstream["structural_prominence_decisions"] = upstream["prominence_decisions"]
    upstream["prominence_decisions"] = str(identity_input)
    upstream["semantic_identity_input"] = str(identity_input)
    if args.test_mode:
        temp_root = (project_root / "temp").resolve()
        require_within(output_root, temp_root, "test output root")
        require_within(business_root, temp_root, "test business root")
    if not args.dry_run and (not args.confirm_database_write):
        raise PipelineError("actual execution requires --confirm-database-write")
    module, writer = validate_database_contracts(
        project_root=project_root,
        data_db=data_db,
        action_db=action_db,
        sql_writer_config=sql_writer_config,
        sql_writer_script=scripts["sql_writer"],
        data_table=data_table,
        data_fields=data_fields,
        test_mode=bool(args.test_mode),
        validate_action_schema=not bool(args.dry_run),
    )
    del module
    run = PipelineRun(
        project_root=project_root,
        output_root=output_root,
        business_root=business_root,
        run_id=run_id,
        test_mode=bool(args.test_mode),
    )
    maximum_path_length = validate_planned_path_lengths(run)
    plan = {
        "pipeline": SCRIPT_FAMILY,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "status": "dry-run" if args.dry_run else "planned",
        "test_mode": bool(args.test_mode),
        "upstream": upstream,
        "prominence_decisions": upstream["prominence_decisions"],
        "data_db": str(data_db),
        "data_table": data_table,
        "data_fields": data_fields,
        "action_db": str(action_db),
        "sql_writer_config": str(sql_writer_config),
        "output_root": str(output_root),
        "business_root": str(business_root),
        "run_dir": str(run.run_dir),
        "business_run_dir": str(run.business_run_dir),
        "maximum_planned_path_length": maximum_path_length,
        "pinned_scripts": {key: path.name for key, path in scripts.items()},
        "pinned_configs": {key: str(path) for key, path in configs.items()},
        "steps": STEP_NAMES,
        "replay_command_policy": "fixed argv with shell=False; replay comparator receives no --cmd",
    }
    if args.dry_run:
        return plan
    manifest = {
        "schema_version": "action_anchor_persistence_run_v0001",
        "pipeline": {"script": SCRIPT_NAME, "family": SCRIPT_FAMILY, "version": SCRIPT_VERSION},
        "run_id": run_id,
        "status": "running",
        "started_at": run.started_at,
        "finished_at": None,
        "test_mode": bool(args.test_mode),
        "project_root": str(project_root),
        "output_root": str(output_root),
        "business_root": str(business_root),
        "run_dir": str(run.run_dir),
        "business_run_dir": str(run.business_run_dir),
        "upstream": upstream,
        "data_db": str(data_db),
        "data_table": data_table,
        "data_fields": data_fields,
        "action_db": str(action_db),
        "sql_writer_config": str(sql_writer_config),
        "pinned_scripts": {key: path.name for key, path in scripts.items()},
        "pinned_configs": {key: str(path) for key, path in configs.items()},
        "replay_command_policy": plan["replay_command_policy"],
        "steps": [],
        "error": None,
        "completion_manifest": None,
    }
    run.initialize(manifest)
    baseline = run.business_run_dir / "01_baseline"
    replay = run.business_run_dir / "02_replay"
    reports = run.business_run_dir / "03_reports"
    orchestration_meta = run.business_run_dir / "orchestration_run_meta.json"
    write_json_atomic(
        orchestration_meta,
        {
            "run_id": run_id,
            "pipeline": SCRIPT_FAMILY,
            "version": SCRIPT_VERSION,
            "upstream_run_id": upstream["run_id"],
            "created_at": utc_now_z(),
        },
    )
    prominence = Path(upstream["prominence_decisions"])
    base_concept = baseline / "concept" / "concept_declarations.jsonl"
    base_instance = baseline / "instance" / "instance_declarations.jsonl"
    base_attribute = baseline / "attribute" / "attribute_declarations.jsonl"
    replay_concept = replay / "concept" / "concept_declarations.jsonl"
    replay_instance = replay / "instance" / "instance_declarations.jsonl"
    replay_attribute = replay / "attribute" / "attribute_declarations.jsonl"
    generator_specs = [
        (
            1,
            "baseline_concept_declarations",
            "concept",
            base_concept,
            baseline / "concept" / "run_meta.json",
            baseline / "concept" / "issues.jsonl",
        ),
        (
            2,
            "baseline_instance_declarations",
            "instance",
            base_instance,
            baseline / "instance" / "run_meta.json",
            baseline / "instance" / "issues.jsonl",
        ),
        (
            3,
            "baseline_attribute_declarations",
            "attribute",
            base_attribute,
            baseline / "attribute" / "run_meta.json",
            baseline / "attribute" / "issues.jsonl",
        ),
        (
            4,
            "replay_concept_declarations",
            "concept",
            replay_concept,
            replay / "concept" / "run_meta.json",
            replay / "concept" / "issues.jsonl",
        ),
        (
            5,
            "replay_instance_declarations",
            "instance",
            replay_instance,
            replay / "instance" / "run_meta.json",
            replay / "instance" / "issues.jsonl",
        ),
        (
            6,
            "replay_attribute_declarations",
            "attribute",
            replay_attribute,
            replay / "attribute" / "run_meta.json",
            replay / "attribute" / "issues.jsonl",
        ),
    ]
    try:
        for index, name, kind, output, run_meta, issues in generator_specs:
            arguments = [
                "--inputs",
                str(prominence),
                "--config",
                str(configs[kind]),
                "--output",
                str(output),
                "--run-meta",
                str(run_meta),
            ]
            if kind in {"concept", "instance"}:
                arguments.extend(["--issues", str(issues), "--fail-on-invalid"])
            elif kind == "attribute":
                arguments.extend(["--issues", str(issues)])
            run_child_step(
                run,
                index=index,
                name=name,
                script=scripts[kind],
                arguments=arguments,
                timeout_seconds=args.step_timeout,
            )
            require_file(output, f"{name} output")
            require_file(run_meta, f"{name} run_meta")
        comparison_root = reports / "replay_comparison_views"
        base_compare_concept = comparison_root / "baseline" / "concept.jsonl"
        base_compare_instance = comparison_root / "baseline" / "instance.jsonl"
        base_compare_attribute = comparison_root / "baseline" / "attribute.jsonl"
        replay_compare_concept = comparison_root / "replay" / "concept.jsonl"
        replay_compare_instance = comparison_root / "replay" / "instance.jsonl"
        replay_compare_attribute = comparison_root / "replay" / "attribute.jsonl"
        for source, target in (
            (base_concept, base_compare_concept),
            (base_instance, base_compare_instance),
            (base_attribute, base_compare_attribute),
            (replay_concept, replay_compare_concept),
            (replay_instance, replay_compare_instance),
            (replay_attribute, replay_compare_attribute),
        ):
            write_replay_comparison_view(source, target)
        replay_report = reports / "replay_report.json"
        run_child_step(
            run,
            index=7,
            name="compare_replay_outputs",
            script=scripts["replay_compare"],
            arguments=[
                "--base-concept",
                str(base_compare_concept),
                "--base-instance",
                str(base_compare_instance),
                "--base-attribute",
                str(base_compare_attribute),
                "--replay-concept",
                str(replay_compare_concept),
                "--replay-instance",
                str(replay_compare_instance),
                "--replay-attribute",
                str(replay_compare_attribute),
                "--workdir",
                str(replay),
                "--report",
                str(replay_report),
            ],
            timeout_seconds=args.step_timeout,
        )
        replay_payload = require_report_pass(replay_report, "replay report")
        payload_report = reports / "validation_report.json"
        run_child_step(
            run,
            index=8,
            name="validate_ingest_payload",
            script=scripts["validate_payload"],
            arguments=[
                "--concept",
                str(base_concept),
                "--instance",
                str(base_instance),
                "--attribute",
                str(base_attribute),
                "--run-meta",
                str(orchestration_meta),
                "--strict-run-id",
                "--report",
                str(payload_report),
            ],
            timeout_seconds=args.step_timeout,
        )
        validation_payload = require_report_pass(payload_report, "payload validation report")
        anchor_report = reports / "anchor_report.json"
        run_child_step(
            run,
            index=9,
            name="verify_data_anchor",
            script=scripts["verify_anchor"],
            arguments=[
                "--instance",
                str(base_instance),
                "--data-db",
                str(data_db),
                "--table",
                data_table,
                "--field-asset-id",
                data_fields["asset_id"],
                "--field-path",
                data_fields["path"],
                "--field-value-index",
                data_fields["value_index"],
                "--field-sentence-index",
                data_fields["sentence_index"],
                "--field-segment-index",
                data_fields["segment_index"],
                "--field-char-start",
                data_fields["char_start"],
                "--field-char-end",
                data_fields["char_end"],
                "--report",
                str(anchor_report),
            ],
            timeout_seconds=args.step_timeout,
        )
        anchor_payload = require_report_pass(anchor_report, "data anchor report")
        concepts, instances, attributes = validate_declarations(base_concept, base_instance, base_attribute)
        input_hash = combined_sha256([base_concept, base_instance, base_attribute])
        sql_counts = write_sql_payload(
            run,
            writer=writer,
            concepts=concepts,
            instances=instances,
            attributes=attributes,
            input_hash=input_hash,
        )
        integrity_report = reports / "sql_integrity_report.json"
        writer_tables = writer.config.tables
        writer_fields = writer.config.fields
        run_child_step(
            run,
            index=11,
            name="check_sql_integrity",
            script=scripts["check_integrity"],
            arguments=[
                "--action-db",
                str(action_db),
                "--report",
                str(integrity_report),
                "--concept-table",
                writer_tables["concept"],
                "--instance-table",
                writer_tables["instance"],
                "--attribute-table",
                writer_tables["attribute"],
                "--concept-id-field",
                writer_fields["concept"]["id"],
                "--instance-id-field",
                writer_fields["instance"]["id"],
                "--instance-concept-id-field",
                writer_fields["instance"]["concept_id"],
                "--attr-object-type-field",
                writer_fields["attribute"]["object_type"],
                "--attr-object-id-field",
                writer_fields["attribute"]["object_id"],
                "--attr-key-field",
                writer_fields["attribute"]["attr_key"],
                "--attr-value-field",
                writer_fields["attribute"]["attr_value"],
                "--inst-asset-id-field",
                writer_fields["instance"]["asset_id"],
                "--inst-path-field",
                writer_fields["instance"]["path"],
                "--inst-segment-index-field",
                writer_fields["instance"]["segment_index"],
                "--inst-char-start-field",
                writer_fields["instance"]["char_start"],
                "--inst-char-end-field",
                writer_fields["instance"]["char_end"],
                "--data-db",
                str(data_db),
                "--data-table",
                data_table,
                "--data-asset-id-field",
                data_fields["asset_id"],
                "--data-path-field",
                data_fields["path"],
                "--data-segment-index-field",
                data_fields["segment_index"],
                "--data-char-start-field",
                data_fields["char_start"],
                "--data-char-end-field",
                data_fields["char_end"],
                "--warn-orphan-concepts",
                "--inst-value-index-field",
                writer.config.fields["instance"]["value_index"],
                "--inst-sentence-index-field",
                writer.config.fields["instance"]["sentence_index"],
                "--data-value-index-field",
                data_fields["value_index"],
                "--data-sentence-index-field",
                data_fields["sentence_index"],
            ],
            timeout_seconds=args.step_timeout,
        )
        integrity_payload = require_report_pass(integrity_report, "SQL integrity report")
        database_counts = query_action_counts(writer, run_id)
        expected_current_run = {
            "concept_rows": sql_counts["concept_inserted"],
            "instance_rows": sql_counts["instance_inserted"],
            "attribute_rows": sql_counts["attribute_inserted"],
            "run_log_rows": 1,
        }
        if database_counts["current_run"] != expected_current_run:
            raise PipelineError(
                f"current-run SQL counts do not match writer evidence: {database_counts['current_run']} != {expected_current_run}"
            )
        completion = {
            "schema_version": "action_anchor_persistence_completion_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "completed_at": utc_now_z(),
            "test_mode": bool(args.test_mode),
            "prerequisite": {
                "pipeline": UPSTREAM_FAMILY,
                "version": UPSTREAM_VERSION,
                "run_id": upstream["run_id"],
                "completion_manifest": upstream["completion_manifest"],
                "state_transition": "action_structural_pipeline.completed -> action_anchor_persistence.completed",
            },
            "input": {
                "prominence_decisions": str(prominence),
                "sha256": file_sha256(prominence),
                "source_inputs": upstream["source_inputs"],
            },
            "declarations": {
                "concept": {"path": str(base_concept), "rows": len(concepts)},
                "instance": {"path": str(base_instance), "rows": len(instances)},
                "attribute": {"path": str(base_attribute), "rows": len(attributes)},
            },
            "gates": {
                "replay": {
                    "status": replay_payload.get("status"),
                    "report": str(replay_report),
                    "comparison_views": str(comparison_root),
                    "ignored_volatile_top_level_fields": ["generated_at"],
                    "raw_declarations_preserved": True,
                },
                "payload": {"status": validation_payload.get("status"), "report": str(payload_report)},
                "data_anchor": {"status": anchor_payload.get("status"), "report": str(anchor_report)},
                "sql_integrity": {"status": integrity_payload.get("status"), "report": str(integrity_report)},
            },
            "sql_write": {
                "action_db": str(action_db),
                "writer_config": str(sql_writer_config),
                "per_call_transaction_boundary": True,
                "whole_batch_atomic_rollback": False,
                "write_results": str(run.sql_results_path),
                "run_counts": sql_counts,
                "database_counts_after": database_counts,
                "expected_current_run_counts": expected_current_run,
            },
            "business_run_dir": str(run.business_run_dir),
            "excluded": {
                "action_development_registry": "sql/action.db",
                "other_databases": ["Neo4j", "DuckDB", "Chroma"],
                "automatic_retry": True,
                "shell_commands_for_replay": True,
            },
        }
        write_json_atomic(run.completion_path, completion)
        run.update_manifest(
            status="completed",
            finished_at=completion["completed_at"],
            error=None,
            completion_manifest=str(run.completion_path),
        )
        _rewrite_completion(completion, args, project_root)
        return completion
    except Exception as exc:
        failure = {
            "schema_version": "action_anchor_persistence_failure_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "failed",
            "failed_at": utc_now_z(),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "database_write_started": run.db_write_started,
            "completed_steps": len([step for step in run.steps if step.get("status") == "success"]),
            "business_run_dir": str(run.business_run_dir),
        }
        write_json_atomic(run.failure_path, failure)
        run.update_manifest(
            status="failed",
            finished_at=failure["failed_at"],
            error={"type": type(exc).__name__, "message": str(exc)},
            completion_manifest=None,
        )
        raise


def _rewrite_completion(result: dict[str, Any], args: Any, project_root: Path) -> None:
    identity_input = resolve_path(args.identity_input, project_root)
    previous_input = result.get("input") if isinstance(result.get("input"), dict) else {}
    result["input"] = {
        "identity_source": "selected_normalized_semantic_text_units",
        "semantic_text_units": str(identity_input),
        "sha256": file_sha256(identity_input),
        "source_inputs": previous_input.get("source_inputs", []),
    }
    result["identity_source_contract"] = {
        "decision_gate_required": False,
        "stable_coordinates_required": True,
        "structural_prominence_is_not_used_as_semantic_eligibility": True,
    }
    output_root = resolve_path(args.output_root, project_root)
    completion_path = (
        output_root / SCRIPT_FAMILY / SCRIPT_VERSION / result["run_id"] / "completion_manifest.json"
    )
    write_json_atomic(completion_path, result)


# ============================================================
# CLI / main 接口区
# ============================================================


def build_parser() -> Any:
    parser = _build_parser_core()
    parser.description = "Persist selected semantic text units through the validated Action Anchor chain."
    parser.add_argument("--identity-input", required=True)
    parser.add_argument("--semantic-instance-config", default=str(DEFAULT_INSTANCE_CONFIG))
    parser.add_argument("--data-value-index-field", default="value_index")
    parser.add_argument("--data-sentence-index-field", default="sentence_index")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        result = execute(args)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
