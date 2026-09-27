# ============================================================
# 文件名: vector_embedding_pipeline_v0006.py
# 中文名: 自包含审计接口向量编排
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
# alias: vector_embedding_pipeline
# family: vector_embedding_pipeline
# role: action_vector_orchestrator
# version: v0006
# status: active
# entry_point: scripts/orchestration/action/vector_embedding_pipeline_v0006.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - embedding_contract_validator_v0002.py
#   - embedding_generator_v0005.py
#   - embedding_lifecycle_guard_v0001.py
#   - embedding_writer_v0001.py
#   - vector_integrity_check_v0003.py
# used_by:
#   - data_action_chain_pipeline_v0015
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/orchestration/action/vector_embedding_pipeline_v0003.py
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
import shutil
import sqlite3
import subprocess
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# ============================================================
# 全局常量与默认映射
# ============================================================

SCRIPT_NAME = "vector_embedding_pipeline_v0006.py"


SCRIPT_FAMILY = "vector_embedding_pipeline"


SCRIPT_VERSION = "v0006"


MAX_ROOT_SEARCH_DEPTH = 10


DEFAULT_SQL_DB = Path("sql") / "action_data.db"


DEFAULT_SQL_CONFIG = Path("config") / "action" / "config" / "sql_writer_config_v0001.yml"


DEFAULT_EMBED_CONFIG = Path("config") / "action" / "config" / "embedding_generator_config_v0002.yml"


DEFAULT_PIPELINE_ROOT = Path("vector") / "pipeline"


DEFAULT_CHROMADB_PATH = Path("chromadb") / "action" / "action_data" / "vectors"


DEFAULT_STATE_INDEX = Path("vector") / "state" / "active_index.jsonl"


DEFAULT_REPLACED_ARCHIVE = Path("chromadb") / "action" / "action_data" / "replaced"


DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "action"


RUN_ID_PATTERN = re.compile("^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")


WINDOWS_SAFE_PATH_LENGTH = 240


COLLECTION_NAME = "action_data_embeddings"


VECTOR_DIR = Path("scripts") / "action" / "vector"


PINNED_SCRIPTS = {
    "generator": VECTOR_DIR / "embedding_generator_v0005.py",
    "validator": VECTOR_DIR / "embedding_contract_validator_v0002.py",
    "lifecycle": VECTOR_DIR / "embedding_lifecycle_guard_v0001.py",
    "writer": VECTOR_DIR / "embedding_writer_v0001.py",
    "integrity": VECTOR_DIR / "vector_integrity_check_v0003.py",
}


INTEGRITY_TARGET_ENV = "ACTION_VECTOR_INTEGRITY_TARGET"


REQUIRED_SQL_COLUMNS = {
    "concept_units": {"unit_text_id", "unit_text", "content_hash"},
    "instance_units": {"instance_id", "asset_id", "content", "content_hash"},
}


DEFAULT_ENCODING = "utf-8"

# ============================================================
# 异常类型
# ============================================================


class PipelineError(RuntimeError):
    """Raised when the vector pipeline cannot continue safely."""


# ============================================================
# 数据结构、工具与核心实现
# ============================================================


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"r_{stamp}_{uuid.uuid4().hex[:6]}"


def find_project_root(start: Path) -> Path:
    current = start.resolve()
    for _ in range(MAX_ROOT_SEARCH_DEPTH):
        if (current / "scripts").is_dir() and (current / "config").is_dir():
            return current
        if current.parent == current:
            break
        current = current.parent
    raise PipelineError(f"project root not found from {start}")


def resolve_path(raw: str | Path, project_root: Path) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def require_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise PipelineError(f"{label} not found: {path}")


def require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise PipelineError(f"{label} escapes required root: {path} not within {root}") from exc


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PipelineError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
            if not isinstance(payload, dict):
                raise PipelineError(f"JSONL record is not an object at {path}:{line_number}")
            yield payload


def directory_file_manifest(directory: Path) -> Dict[str, str]:
    if not directory.is_dir():
        raise PipelineError(f"stage directory not found: {directory}")
    return {
        path.relative_to(directory).as_posix(): file_sha256(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def parse_child_json(stdout: str, stage: str) -> Dict[str, Any]:
    text = stdout.strip()
    if not text:
        raise PipelineError(f"{stage} produced no JSON stdout")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PipelineError(f"{stage} stdout is not one JSON object: {exc}") from exc
    if not isinstance(payload, dict):
        raise PipelineError(f"{stage} stdout JSON is not an object")
    return payload


def load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise PipelineError("PyYAML is required in the selected Python environment") from exc
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PipelineError(f"invalid YAML config {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise PipelineError(f"YAML config must contain a mapping: {path}")
    return payload


def validate_dependencies(project_root: Path, python_executable: Path) -> Dict[str, Any]:
    require_file(python_executable, "Python executable")
    scripts: Dict[str, Any] = {}
    for key, relative in PINNED_SCRIPTS.items():
        path = (project_root / relative).resolve()
        require_file(path, f"pinned {key} script")
        scripts[key] = {"path": str(path), "sha256": file_sha256(path)}
    packages = {
        "yaml": importlib.util.find_spec("yaml") is not None,
        "chromadb": importlib.util.find_spec("chromadb") is not None,
    }
    missing = [name for name, present in packages.items() if not present]
    if missing:
        raise PipelineError(f"missing Python dependencies: {missing}")
    return {"scripts": scripts, "packages": packages}


def database_preflight(db_path: Path, target: str) -> Dict[str, Any]:
    require_file(db_path, "Action SQL source database")
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        tables: Dict[str, List[str]] = {}
        missing: Dict[str, List[str]] = {}
        for table, required in REQUIRED_SQL_COLUMNS.items():
            columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
            tables[table] = sorted(columns)
            absent = sorted(required - columns)
            if absent:
                missing[table] = absent
        if missing:
            raise PipelineError(f"Action SQL vector source schema is incomplete: {missing}")
        counts = {
            "concept": int(conn.execute("SELECT COUNT(*) FROM concept_units").fetchone()[0]),
            "instance": int(conn.execute("SELECT COUNT(*) FROM instance_units").fetchone()[0]),
        }
        if counts[target] < 1:
            raise PipelineError(f"no {target} source records are available in {db_path}")
        return {"sha256": file_sha256(db_path), "counts": counts, "tables": tables}
    finally:
        conn.close()


def validate_embed_config(path: Path) -> Dict[str, Any]:
    payload = load_yaml(path)
    model = payload.get("model") or {}
    dimension = int(model.get("dimension", 0))
    if dimension < 1:
        raise PipelineError("embedding config model.dimension must be positive")
    return {
        "sha256": file_sha256(path),
        "backend": (payload.get("backend") or {}).get("type"),
        "model": model.get("name"),
        "dimension": dimension,
        "policy_version": model.get("policy_version"),
    }


def make_runtime_sql_config(base_path: Path, db_path: Path, destination: Path) -> None:
    payload = load_yaml(base_path)
    connection = payload.setdefault("connection", {})
    if not isinstance(connection, dict):
        raise PipelineError("sql config connection must be a mapping")
    sqlite_config = connection.setdefault("sqlite", {})
    if not isinstance(sqlite_config, dict):
        raise PipelineError("sql config connection.sqlite must be a mapping")
    sqlite_config["path"] = str(db_path)
    try:
        import yaml

        rendered = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
    except Exception as exc:
        raise PipelineError(f"failed to render runtime SQL config: {exc}") from exc
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(rendered, encoding="utf-8")


class RunRecorder:

    def __init__(self, run_dir: Path, manifest: Dict[str, Any]):
        self.run_dir = run_dir
        self.manifest = manifest
        self.manifest_path = run_dir / "run_manifest.json"
        self.step_status_path = run_dir / "step_status.jsonl"
        self.stdout_dir = run_dir / "captured_stdout"
        self.stderr_dir = run_dir / "captured_stderr"

    def initialize(self) -> None:
        self.stdout_dir.mkdir(parents=True, exist_ok=False)
        self.stderr_dir.mkdir(parents=True, exist_ok=False)
        write_json_atomic(self.manifest_path, self.manifest)

    def update(self, **values: Any) -> None:
        self.manifest.update(values)
        write_json_atomic(self.manifest_path, self.manifest)

    def event(self, payload: Mapping[str, Any]) -> None:
        append_jsonl(self.step_status_path, payload)


def run_child(
    recorder: RunRecorder,
    step_number: int,
    stage: str,
    command: Sequence[str],
    cwd: Path,
    timeout_seconds: int,
) -> Dict[str, Any]:
    started = utc_now_z()
    recorder.event(
        {
            "step": step_number,
            "stage": stage,
            "event": "started",
            "timestamp": started,
            "command": list(command),
            "cwd": str(cwd),
            "shell": False,
        }
    )
    start_clock = time.monotonic()
    try:
        result = subprocess.run(
            list(command),
            cwd=str(cwd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            shell=False,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed = round(time.monotonic() - start_clock, 3)
        recorder.event(
            {
                "step": step_number,
                "stage": stage,
                "event": "failed",
                "timestamp": utc_now_z(),
                "elapsed_seconds": elapsed,
                "error": f"timeout after {timeout_seconds} seconds",
            }
        )
        raise PipelineError(f"{stage} timed out after {timeout_seconds} seconds") from exc
    elapsed = round(time.monotonic() - start_clock, 3)
    stdout_path = recorder.stdout_dir / f"{step_number:02d}_{stage}.txt"
    stderr_path = recorder.stderr_dir / f"{step_number:02d}_{stage}.txt"
    stdout_path.write_text(result.stdout or "", encoding="utf-8")
    stderr_path.write_text(result.stderr or "", encoding="utf-8")
    if result.returncode != 0:
        recorder.event(
            {
                "step": step_number,
                "stage": stage,
                "event": "failed",
                "timestamp": utc_now_z(),
                "elapsed_seconds": elapsed,
                "exit_code": result.returncode,
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
            }
        )
        raise PipelineError(f"{stage} failed with exit code {result.returncode}")
    payload = parse_child_json(result.stdout or "", stage)
    recorder.event(
        {
            "step": step_number,
            "stage": stage,
            "event": "succeeded",
            "timestamp": utc_now_z(),
            "elapsed_seconds": elapsed,
            "exit_code": result.returncode,
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
        }
    )
    return payload


def promote_validated(source: Path, target: Path) -> Dict[str, Any]:
    if target.exists():
        raise PipelineError(f"validated target already exists: {target}")
    before = directory_file_manifest(source)
    if not any((name.endswith(".jsonl") for name in before)):
        raise PipelineError("generator stage contains no JSONL records")
    shutil.copytree(source, target)
    after = directory_file_manifest(target)
    if before != after:
        raise PipelineError("validated handoff copy hash mismatch")
    return {
        "status": "PASS",
        "mode": "copy",
        "source": str(source),
        "target": str(target),
        "files": len(after),
        "jsonl_files": sum((1 for name in after if name.endswith(".jsonl"))),
        "file_sha256": after,
    }


def chroma_snapshot(chromadb_path: Path) -> Dict[str, Any]:
    import chromadb
    from chromadb.config import Settings

    client = chromadb.PersistentClient(path=str(chromadb_path), settings=Settings(anonymized_telemetry=False))
    collection = client.get_collection(name=COLLECTION_NAME)
    return {
        "collection": COLLECTION_NAME,
        "count": int(collection.count()),
        "metadata": collection.metadata or {},
    }


def state_index_snapshot(path: Path) -> Dict[str, Any]:
    records = list(iter_jsonl(path))
    keys = [f"{item.get('object_type')}:{item.get('object_id')}" for item in records]
    if len(keys) != len(set(keys)):
        raise PipelineError("state index contains duplicate object identity keys")
    return {"path": str(path), "records": len(records), "sha256": file_sha256(path)}


def cli() -> int:
    try:
        return main()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "pipeline": SCRIPT_FAMILY,
                    "pipeline_version": SCRIPT_VERSION,
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1


# ============================================================
# CLI / main 接口区
# ============================================================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reliably generate, validate, lifecycle-check, write and audit Action embeddings."
    )
    parser.add_argument("--ingest-run-id", default=None)
    parser.add_argument("--target", required=True, choices=("concept", "instance"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--sql-db", default=str(DEFAULT_SQL_DB))
    parser.add_argument("--sql-config", default=str(DEFAULT_SQL_CONFIG))
    parser.add_argument("--embed-config", default=str(DEFAULT_EMBED_CONFIG))
    parser.add_argument("--pipeline-root", default=str(DEFAULT_PIPELINE_ROOT))
    parser.add_argument("--chromadb-path", default=str(DEFAULT_CHROMADB_PATH))
    parser.add_argument("--state-index", default=str(DEFAULT_STATE_INDEX))
    parser.add_argument("--replaced-archive", default=str(DEFAULT_REPLACED_ARCHIVE))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--asset-id", default=None)
    parser.add_argument("--all-assets", action="store_true")
    parser.add_argument("--limit-items", type=int, default=None)
    parser.add_argument("--offset-items", type=int, default=0)
    parser.add_argument("--mock-api", action="store_true")
    parser.add_argument(
        "--confirm-real-model-in-test-mode",
        action="store_true",
        help="Explicitly allow a real configured model while test paths remain confined to temp.",
    )
    parser.add_argument("--integrity-target", choices=("all", "concept", "instance"), default=None)
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--confirm-vector-write", action="store_true")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def validate_incremental_writer_counts(
    generated: int, lifecycle: Mapping[str, Any], writer: Mapping[str, Any]
) -> None:
    """Require conservation across generated, duplicate and persisted vectors."""
    lifecycle_stats = lifecycle.get("stats") or {}
    writer_stats = writer.get("stats") or {}
    counters = [
        generated,
        lifecycle_stats.get("total_processed"),
        lifecycle_stats.get("marked_active"),
        lifecycle_stats.get("marked_duplicate"),
        writer_stats.get("total_processed"),
        writer_stats.get("written_to_chromadb"),
    ]
    if any(type(value) is not int or value < 0 for value in counters):
        raise PipelineError("embedding incremental counters must be nonnegative integers")
    total, processed, active, duplicate, writer_processed, written = counters
    if (
        lifecycle.get("status") != "success"
        or writer.get("status") != "success"
        or processed != total
        or active + duplicate != total
        or writer_processed != active
        or written != active
    ):
        raise PipelineError("embedding incremental count conservation failed")


def main() -> int:
    args = build_parser().parse_args()
    if args.confirm_real_model_in_test_mode and (not args.test_mode):
        raise PipelineError("--confirm-real-model-in-test-mode requires --test-mode")
    if args.confirm_real_model_in_test_mode and args.mock_api:
        raise PipelineError("real-model test confirmation cannot be combined with --mock-api")
    if args.target == "instance" and (not args.asset_id) and (not args.all_assets):
        args.all_assets = True
    integrity_target = args.integrity_target or args.target
    os.environ[INTEGRITY_TARGET_ENV] = integrity_target
    project_root = find_project_root(Path(__file__).resolve())
    temp_root = (project_root / "temp").resolve()
    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError("run_id contains unsupported characters or is too long")
    if args.timeout_seconds < 1:
        raise PipelineError("timeout_seconds must be positive")
    if args.limit_items is not None and args.limit_items < 1:
        raise PipelineError("limit_items must be positive")
    if args.offset_items < 0:
        raise PipelineError("offset_items cannot be negative")
    if args.target == "instance" and bool(args.asset_id) == bool(args.all_assets):
        raise PipelineError("instance target requires exactly one of --asset-id or --all-assets")
    if args.target == "concept" and (args.asset_id or args.all_assets):
        raise PipelineError("concept target does not accept instance asset selection")
    sql_db = resolve_path(args.sql_db, project_root)
    sql_config = resolve_path(args.sql_config, project_root)
    embed_config = resolve_path(args.embed_config, project_root)
    pipeline_root = resolve_path(args.pipeline_root, project_root)
    chromadb_path = resolve_path(args.chromadb_path, project_root)
    state_index = resolve_path(args.state_index, project_root)
    replaced_archive = resolve_path(args.replaced_archive, project_root)
    output_root = resolve_path(args.output_root, project_root)
    python_executable = resolve_path(args.python_executable, project_root)
    require_file(sql_config, "SQL mapping config")
    require_file(embed_config, "embedding config")
    dependencies = validate_dependencies(project_root, python_executable)
    source_preflight = database_preflight(sql_db, args.target)
    embedding_preflight = validate_embed_config(embed_config)
    evidence_run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
    generated_dir = pipeline_root / "1_generated" / run_id
    validated_dir = pipeline_root / "2_validated" / run_id
    lifecycle_dir = pipeline_root / "3_lifecycle_checked" / run_id
    writer_dir = pipeline_root / "4_written" / run_id
    integrity_dir = pipeline_root / "5_integrity" / run_id
    if args.test_mode:
        if not args.mock_api and (not args.confirm_real_model_in_test_mode):
            raise PipelineError(
                "real model execution in test mode requires --confirm-real-model-in-test-mode"
            )
        for path, label in (
            (sql_db, "test SQL database"),
            (pipeline_root, "test pipeline root"),
            (chromadb_path, "test Chroma path"),
            (state_index, "test state index"),
            (replaced_archive, "test replaced archive"),
            (output_root, "test orchestration output root"),
        ):
            require_within(path, temp_root, label)
    planned_paths = {
        "evidence": str(evidence_run_dir),
        "generated": str(generated_dir),
        "validated": str(validated_dir),
        "lifecycle": str(lifecycle_dir),
        "writer": str(writer_dir),
        "integrity": str(integrity_dir),
        "chromadb": str(chromadb_path),
        "state_index": str(state_index),
        "replaced_archive": str(replaced_archive),
    }
    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "pipeline": SCRIPT_FAMILY,
                    "pipeline_version": SCRIPT_VERSION,
                    "run_id": run_id,
                    "target": args.target,
                    "would_write": False,
                    "would_create_directories": False,
                    "execution_nodes": 6,
                    "business_scripts": 5,
                    "source_preflight": source_preflight,
                    "embedding_preflight": embedding_preflight,
                    "paths": planned_paths,
                    "dependencies": dependencies,
                    "safety": {
                        "shell": False,
                        "mock_api": bool(args.mock_api),
                        "real_model_test_confirmed": bool(args.confirm_real_model_in_test_mode),
                        "cleanup": False,
                        "collection_reset": False,
                        "automatic_retry": False,
                        "whole_pipeline_atomic": False,
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if not args.confirm_vector_write:
        raise PipelineError("actual vector write requires --confirm-vector-write")
    for path, label in (
        (evidence_run_dir, "orchestration run directory"),
        (generated_dir, "generated stage directory"),
        (validated_dir, "validated stage directory"),
        (lifecycle_dir, "lifecycle stage directory"),
        (writer_dir, "writer stage directory"),
        (integrity_dir, "integrity stage directory"),
    ):
        if len(str(path)) > WINDOWS_SAFE_PATH_LENGTH:
            raise PipelineError(f"{label} exceeds safe Windows path length: {path}")
        if path.exists():
            raise PipelineError(f"{label} already exists; runs are append-only: {path}")
    manifest = {
        "schema_version": "vector_embedding_pipeline_run_v0001",
        "pipeline": SCRIPT_FAMILY,
        "pipeline_version": SCRIPT_VERSION,
        "run_id": run_id,
        "status": "planned",
        "started_at": utc_now_z(),
        "target": args.target,
        "test_mode": bool(args.test_mode),
        "mock_api": bool(args.mock_api),
        "real_model_test_confirmed": bool(args.confirm_real_model_in_test_mode),
        "project_root": str(project_root),
        "sql_db": str(sql_db),
        "sql_config_source": str(sql_config),
        "embed_config": str(embed_config),
        "paths": planned_paths,
        "source_preflight": source_preflight,
        "embedding_preflight": embedding_preflight,
        "dependencies": dependencies,
        "safety": {
            "subprocess_shell": False,
            "cleanup": False,
            "collection_reset": False,
            "automatic_retry": False,
            "whole_pipeline_atomic": False,
            "explicit_write_confirmation": True,
        },
    }
    recorder = RunRecorder(evidence_run_dir, manifest)
    recorder.initialize()
    recorder.update(status="running")
    runtime_sql_config = evidence_run_dir / "runtime_config" / "sql_writer_config.yml"
    results: Dict[str, Any] = {}
    try:
        make_runtime_sql_config(sql_config, sql_db, runtime_sql_config)
        generator_command = [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["generator"]),
            "--run-id",
            run_id,
            "--target",
            args.target,
            "--sql-config",
            str(runtime_sql_config),
            "--embed-config",
            str(embed_config),
            "--output-base-dir",
            str(pipeline_root / "1_generated"),
        ]
        if args.ingest_run_id:
            generator_command.extend(["--ingest-run-id", args.ingest_run_id])
        if args.target == "concept":
            if args.limit_items is not None:
                generator_command.extend(["--limit-concepts", str(args.limit_items)])
            if args.offset_items:
                generator_command.extend(["--offset-concepts", str(args.offset_items)])
        else:
            if args.asset_id:
                generator_command.extend(["--asset-id", args.asset_id])
            else:
                generator_command.append("--all-assets")
            if args.limit_items is not None:
                generator_command.extend(["--limit-dialogs", str(args.limit_items)])
            if args.offset_items:
                generator_command.extend(["--offset-dialogs", str(args.offset_items)])
        if args.mock_api:
            generator_command.append("--mock-api")
        results["generator"] = run_child(
            recorder, 1, "embedding_generator", generator_command, project_root, args.timeout_seconds
        )
        if results["generator"].get("status") != "success":
            raise PipelineError("embedding generator did not report success")
        generated = int(results["generator"].get("total_generated", 0))
        if generated < 1:
            raise PipelineError("embedding generator produced no records")
        results["validator"] = run_child(
            recorder,
            2,
            "embedding_contract_validator",
            [
                str(python_executable),
                str(project_root / PINNED_SCRIPTS["validator"]),
                "--config",
                str(runtime_sql_config),
                "--input-dir",
                str(generated_dir),
                "--no-audit",
            ],
            project_root,
            args.timeout_seconds,
        )
        if (
            results["validator"].get("status") != "ok"
            or int(results["validator"].get("records_failed", -1)) != 0
            or int(results["validator"].get("records_processed", 0)) != generated
        ):
            raise PipelineError("embedding contract validation did not pass exactly")
        handoff_started = time.monotonic()
        recorder.event(
            {
                "step": 3,
                "stage": "validated_handoff",
                "event": "started",
                "timestamp": utc_now_z(),
                "mode": "copy",
                "shell": False,
            }
        )
        results["validated_handoff"] = promote_validated(generated_dir, validated_dir)
        write_json_atomic(evidence_run_dir / "validated_handoff_manifest.json", results["validated_handoff"])
        recorder.event(
            {
                "step": 3,
                "stage": "validated_handoff",
                "event": "succeeded",
                "timestamp": utc_now_z(),
                "elapsed_seconds": round(time.monotonic() - handoff_started, 3),
                "mode": "copy",
            }
        )
        results["lifecycle"] = run_child(
            recorder,
            4,
            "embedding_lifecycle_guard",
            [
                str(python_executable),
                str(project_root / PINNED_SCRIPTS["lifecycle"]),
                "--input-dir",
                str(validated_dir),
                "--output-dir",
                str(lifecycle_dir),
                "--state-index",
                str(state_index),
            ],
            project_root,
            args.timeout_seconds,
        )
        if (
            results["lifecycle"].get("status") != "success"
            or int((results["lifecycle"].get("stats") or {}).get("total_processed", 0)) != generated
        ):
            raise PipelineError("embedding lifecycle stage did not process all generated records")
        results["writer"] = run_child(
            recorder,
            5,
            "embedding_writer",
            [
                str(python_executable),
                str(project_root / PINNED_SCRIPTS["writer"]),
                "--input-dir",
                str(lifecycle_dir),
                "--chromadb-path",
                str(chromadb_path),
                "--state-index",
                str(state_index),
                "--replaced-archive",
                str(replaced_archive),
                "--output-dir",
                str(writer_dir),
            ],
            project_root,
            args.timeout_seconds,
        )
        validate_incremental_writer_counts(generated, results["lifecycle"], results["writer"])
        results["integrity"] = run_child(
            recorder,
            6,
            "vector_integrity_check",
            [
                str(python_executable),
                str(project_root / PINNED_SCRIPTS["integrity"]),
                "--sql-db",
                str(sql_db),
                "--chromadb-path",
                str(chromadb_path),
                "--state-index",
                str(state_index),
                "--output-dir",
                str(integrity_dir),
            ],
            project_root,
            args.timeout_seconds,
        )
        failed_checks = {
            key: value for key, value in (results["integrity"].get("checks") or {}).items() if value != "pass"
        }
        if results["integrity"].get("status") != "ok" or failed_checks:
            raise PipelineError(f"vector integrity postflight failed: {failed_checks}")
        postflight = {
            "status": "PASS",
            "source_database": database_preflight(sql_db, args.target),
            "generated_records": generated,
            "generated_manifest": directory_file_manifest(generated_dir),
            "validated_manifest": directory_file_manifest(validated_dir),
            "lifecycle_manifest": directory_file_manifest(lifecycle_dir),
            "writer_manifest": directory_file_manifest(writer_dir),
            "chroma": chroma_snapshot(chromadb_path),
            "state_index": state_index_snapshot(state_index),
            "integrity_checks": results["integrity"].get("checks"),
        }
        write_json_atomic(evidence_run_dir / "postflight_report.json", postflight)
        completion = {
            "schema_version": "vector_embedding_pipeline_completion_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "completed_at": utc_now_z(),
            "target": args.target,
            "test_mode": bool(args.test_mode),
            "mock_api": bool(args.mock_api),
            "real_model_test_confirmed": bool(args.confirm_real_model_in_test_mode),
            "generated_records": generated,
            "execution": {
                "business_scripts": 5,
                "execution_nodes": 6,
                "all_steps_success": True,
                "shell": False,
                "cleanup": False,
                "whole_pipeline_atomic": False,
                "automatic_retry": False,
            },
            "paths": planned_paths,
            "results": results,
            "postflight": str(evidence_run_dir / "postflight_report.json"),
            "run_manifest": str(recorder.manifest_path),
        }
        write_json_atomic(evidence_run_dir / "completion_manifest.json", completion)
        recorder.update(status="completed", completed_at=completion["completed_at"])
        print(json.dumps(completion, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        failed_at = utc_now_z()
        failure = {
            "schema_version": "vector_embedding_pipeline_failure_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "failed",
            "failed_at": failed_at,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "results": results,
            "paths": planned_paths,
            "automatic_retry": False,
        }
        write_json_atomic(evidence_run_dir / "failure_manifest.json", failure)
        recorder.update(status="failed", failed_at=failed_at, error=str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
