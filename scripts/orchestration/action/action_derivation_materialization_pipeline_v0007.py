# ============================================================
# 文件名: action_derivation_materialization_pipeline_v0007.py
# 中文名: 自包含派生物化编排
# 版本号: v0007
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
# alias: action_derivation_materialization_pipeline
# family: action_derivation_materialization_pipeline
# role: action_derivation_materialization_orchestrator
# version: v0007
# status: active
# entry_point: scripts/orchestration/action/action_derivation_materialization_pipeline_v0007.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - enrich_retrieval_with_analytics_v0001.py
#   - init_action_data_duckdb_schema_v0001.py
#   - init_graph_schema_v0002.py
#   - query_duckdb_direct_v0001.py
#   - sync_hierarchy_to_graph_v0002.py
#   - sync_sql_to_duckdb_v0006.py
#   - sync_to_graph_v0002.py
# used_by:
#   - data_action_chain_pipeline_v0018
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/orchestration/action/action_derivation_materialization_pipeline_v0001.py, scripts/orchestration/action/action_derivation_materialization_pipeline_v0004.py
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

SCRIPT_NAME = "action_derivation_materialization_pipeline_v0007.py"


SCRIPT_FAMILY = "action_derivation_materialization_pipeline"


SCRIPT_VERSION = "v0007"


ANCHOR_FAMILY = "action_anchor_persistence_pipeline"


ANCHOR_VERSION = "v0007"


STRUCTURAL_FAMILY = "structural_unit_governance_graph_pipeline"


STRUCTURAL_VERSION = "v0003"


DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "action"


DEFAULT_BUSINESS_ROOT = Path("actioning") / "derivation" / "orchestration"


DEFAULT_DUCKDB_CONFIG = (
    Path("config") / "action" / "init_schema" / "action_data_duckdb_schema_config_v0001.yml"
)


DEFAULT_NEO4J_CONNECTION_CONFIG = (
    Path("config") / "action" / "config" / "action_data_neo4j_connection_config_v0001.yml"
)


DEFAULT_NEO4J_SCHEMA_CONFIG = (
    Path("config") / "action" / "config" / "action_data_neo4j_schema_config_v0001.yml"
)


DERIVATION_DIR = Path("scripts") / "action" / "derivation"


TOOLS_DIR = Path("scripts") / "action" / "tools"


RUN_ID_PATTERN = re.compile("^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")


WINDOWS_SAFE_PATH_LENGTH = 240


PINNED_SCRIPTS = {
    "duckdb_schema": TOOLS_DIR / "init_action_data_duckdb_schema_v0001.py",
    "duckdb_sync": DERIVATION_DIR / "sync_sql_to_duckdb_v0006.py",
    "duckdb_query": DERIVATION_DIR / "query_duckdb_direct_v0001.py",
    "duckdb_enrich": DERIVATION_DIR / "enrich_retrieval_with_analytics_v0001.py",
    "neo4j_schema": DERIVATION_DIR / "init_graph_schema_v0002.py",
    "neo4j_sync": DERIVATION_DIR / "sync_to_graph_v0002.py",
    "neo4j_hierarchy": DERIVATION_DIR / "sync_hierarchy_to_graph_v0002.py",
}


DUCKDB_STEPS = [
    "initialize_duckdb_schema",
    "sync_sqlite_instance_to_duckdb",
    "sync_sqlite_concept_to_duckdb",
    "sync_sqlite_attribute_to_duckdb",
    "validate_duckdb_schema",
    "query_duckdb_probe",
    "enrich_retrieval_probe",
]


NEO4J_STEPS = ["initialize_neo4j_schema", "sync_structural_graph", "sync_message_hierarchy"]


DEFAULT_ENCODING = "utf-8"

# ============================================================
# 异常类型
# ============================================================


class PipelineError(RuntimeError):
    """Raised when the derivation pipeline cannot continue safely."""


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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_path(raw: str | Path, project_root: Path) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def require_within(path: Path, root: Path, label: str) -> None:
    if not is_within(path, root):
        raise PipelineError(f"{label} escapes required root: {path} not within {root}")


def require_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise PipelineError(f"{label} not found: {path}")


def require_directory(path: Path, label: str) -> None:
    if not path.is_dir():
        raise PipelineError(f"{label} not found: {path}")


def jsonl_count(path: Path, label: str) -> int:
    count = 0
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                text = line.strip()
                if not text:
                    continue
                value = json.loads(text)
                if not isinstance(value, dict):
                    raise PipelineError(f"{label} line {line_no} is not an object: {path}")
                count += 1
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(f"cannot read {label}: {path}: {exc}") from exc
    return count


def parse_json_stdout(stdout: str, label: str) -> Dict[str, Any]:
    text = stdout.strip()
    if not text:
        raise PipelineError(f"{label} returned empty stdout")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise PipelineError(f"{label} did not return a JSON object")
        try:
            payload = json.loads(text[start : end + 1])
        except json.JSONDecodeError as exc:
            raise PipelineError(f"{label} stdout is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PipelineError(f"{label} stdout JSON must be an object")
    return payload


def redact_command(command: Sequence[str]) -> List[str]:
    redacted: List[str] = []
    hide_next = False
    for item in command:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
        else:
            redacted.append(str(item))
            if item == "--password":
                hide_next = True
    return redacted


def find_project_root(start: Path) -> Path:
    current = start.resolve()
    for _ in range(12):
        if (current / "AGENTS.md").is_file() and (current / "scripts").is_dir():
            return current
        if current.parent == current:
            break
        current = current.parent
    raise PipelineError("cannot locate project root from script location")


def resolve_manifest_reference(raw: Any, manifest_path: Path, project_root: Path) -> Optional[Path]:
    if not isinstance(raw, str) or not raw.strip():
        return None
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate.resolve()
    project_candidate = (project_root / candidate).resolve()
    if project_candidate.exists():
        return project_candidate
    return (manifest_path.parent / candidate).resolve()


def validate_anchor_manifest(path: Path, project_root: Path) -> Dict[str, Any]:
    require_file(path, "Anchor completion manifest")
    payload = load_json(path, "Anchor completion manifest")
    if payload.get("pipeline") != ANCHOR_FAMILY:
        raise PipelineError(f"Anchor manifest pipeline must be {ANCHOR_FAMILY}")
    if payload.get("pipeline_version") != ANCHOR_VERSION:
        raise PipelineError(f"Anchor manifest version must be {ANCHOR_VERSION}")
    if payload.get("status") != "completed":
        raise PipelineError("Anchor manifest status must be completed")
    action_db_raw = payload.get("sql_write", {}).get("action_db")
    if not isinstance(action_db_raw, str) or not action_db_raw:
        raise PipelineError("Anchor manifest does not declare sql_write.action_db")
    action_db = resolve_path(action_db_raw, project_root)
    require_file(action_db, "Anchor Action data SQLite database")
    protected = (project_root / "sql" / "action.db").resolve()
    if action_db == protected:
        raise PipelineError("sql/action.db is the Action development registry and is forbidden")
    prerequisite = payload.get("prerequisite", {})
    structural_ref = resolve_manifest_reference(prerequisite.get("completion_manifest"), path, project_root)
    return {
        "manifest": payload,
        "path": path,
        "run_id": str(payload.get("run_id", "")),
        "action_db": action_db,
        "structural_manifest_reference": structural_ref,
        "test_mode": bool(payload.get("test_mode", False)),
    }


def validate_structural_manifest(path: Path, project_root: Path) -> Dict[str, Any]:
    require_file(path, "Structural completion manifest")
    payload = load_json(path, "Structural completion manifest")
    if payload.get("pipeline") != STRUCTURAL_FAMILY:
        raise PipelineError(f"Structural manifest pipeline must be {STRUCTURAL_FAMILY}")
    if payload.get("pipeline_version") != STRUCTURAL_VERSION:
        raise PipelineError(f"Structural manifest version must be {STRUCTURAL_VERSION}")
    if payload.get("status") != "completed":
        raise PipelineError("Structural manifest status must be completed")
    business_raw = payload.get("business_run_dir")
    if not isinstance(business_raw, str) or not business_raw:
        raise PipelineError("Structural manifest does not declare business_run_dir")
    business_dir = resolve_path(business_raw, project_root)
    require_directory(business_dir, "Structural business run directory")
    governance = payload.get("governance", {})
    registry_raw = governance.get("registry")
    registry = (
        resolve_path(registry_raw, project_root)
        if isinstance(registry_raw, str) and registry_raw
        else business_dir / "11_registry" / "registered_structural_units_v0002.jsonl"
    )
    hierarchy = business_dir / "14_hierarchy_sidecar" / "structural_hierarchy.jsonl"
    adjacency = business_dir / "16_adjacency_graph" / "current" / "unit_adjacency_edges.jsonl"
    cooccurrence = business_dir / "17_cooccurrence_graph" / "current" / "unit_cooccurrence_edges.jsonl"
    for item, label in (
        (registry, "Structural registry"),
        (hierarchy, "Structural hierarchy sidecar"),
        (adjacency, "Adjacency edge stream"),
        (cooccurrence, "Cooccurrence edge stream"),
    ):
        require_file(item, label)
        require_within(item, business_dir, label)
    return {
        "manifest": payload,
        "path": path,
        "run_id": str(payload.get("run_id", "")),
        "business_dir": business_dir,
        "registry": registry.resolve(),
        "hierarchy": hierarchy.resolve(),
        "adjacency": adjacency.resolve(),
        "cooccurrence": cooccurrence.resolve(),
        "counts": {
            "registry": jsonl_count(registry, "Structural registry"),
            "hierarchy": jsonl_count(hierarchy, "Structural hierarchy sidecar"),
            "adjacency": jsonl_count(adjacency, "Adjacency edge stream"),
            "cooccurrence": jsonl_count(cooccurrence, "Cooccurrence edge stream"),
        },
        "test_mode": bool(payload.get("test_mode", False)),
    }


def validate_lineage(anchor: Dict[str, Any], structural: Dict[str, Any]) -> None:
    reference = anchor.get("structural_manifest_reference")
    if reference is not None and reference != structural["path"].resolve():
        raise PipelineError(
            f"Anchor manifest points to a different Structural completion manifest: {reference} != {structural['path']}"
        )
    prerequisite = anchor["manifest"].get("prerequisite", {})
    declared_run = prerequisite.get("run_id")
    if declared_run and str(declared_run) != structural["run_id"]:
        raise PipelineError(
            f"Anchor prerequisite run_id does not match Structural manifest run_id: {declared_run} != {structural['run_id']}"
        )


def validate_dependencies(project_root: Path, target: str) -> Dict[str, Any]:
    scripts: Dict[str, Dict[str, Any]] = {}
    needed_keys = (
        ["duckdb_schema", "duckdb_sync", "duckdb_query", "duckdb_enrich"]
        if target == "duckdb"
        else ["neo4j_schema", "neo4j_sync", "neo4j_hierarchy"] if target == "neo4j" else list(PINNED_SCRIPTS)
    )
    for key in needed_keys:
        path = (project_root / PINNED_SCRIPTS[key]).resolve()
        require_file(path, f"pinned script {key}")
        scripts[key] = {"path": str(path), "sha256": file_sha256(path)}
    packages = {}
    if target in ("duckdb", "all"):
        packages["duckdb"] = importlib.util.find_spec("duckdb") is not None
    if target in ("neo4j", "all"):
        packages["neo4j"] = importlib.util.find_spec("neo4j") is not None
    packages["yaml"] = importlib.util.find_spec("yaml") is not None
    missing = [name for name, present in packages.items() if not present]
    if missing:
        raise PipelineError(f"missing Python dependencies in current interpreter: {', '.join(missing)}")
    return {"scripts": scripts, "packages": packages}


class RunRecorder:

    def __init__(self, run_dir: Path, business_dir: Path, manifest: Dict[str, Any]):
        self.run_dir = run_dir
        self.business_dir = business_dir
        self.manifest_path = run_dir / "run_manifest.json"
        self.step_status_path = run_dir / "step_status.jsonl"
        self.completion_path = run_dir / "completion_manifest.json"
        self.failure_path = run_dir / "failure.json"
        self.manifest = manifest
        self.steps: List[Dict[str, Any]] = []

    def initialize(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=False)
        self.business_dir.mkdir(parents=True, exist_ok=False)
        (self.run_dir / "captured_stdout").mkdir()
        (self.run_dir / "captured_stderr").mkdir()
        write_json_atomic(self.manifest_path, self.manifest)

    def update(self, **values: Any) -> None:
        self.manifest.update(values)
        self.manifest["steps"] = self.steps
        write_json_atomic(self.manifest_path, self.manifest)

    def record_step(self, record: Dict[str, Any]) -> None:
        self.steps.append(record)
        append_jsonl(self.step_status_path, record)
        self.update(status="running")


def run_child(
    recorder: RunRecorder,
    step_number: int,
    step_name: str,
    command: Sequence[str],
    project_root: Path,
    timeout_seconds: int,
) -> Tuple[Dict[str, Any], str]:
    stdout_path = recorder.run_dir / "captured_stdout" / f"{step_number:02d}_{step_name}.txt"
    stderr_path = recorder.run_dir / "captured_stderr" / f"{step_number:02d}_{step_name}.txt"
    started_at = utc_now_z()
    start = time.perf_counter()
    timed_out = False
    try:
        result = subprocess.run(
            list(command),
            cwd=str(project_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            shell=False,
            check=False,
        )
        returncode = result.returncode
        stdout = result.stdout
        stderr = result.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        returncode = -1
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stderr += f"\nTimed out after {timeout_seconds} seconds."
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    status = "success" if returncode == 0 and (not timed_out) else "failed"
    record = {
        "step": step_number,
        "name": step_name,
        "status": status,
        "started_at": started_at,
        "finished_at": utc_now_z(),
        "duration_ms": round((time.perf_counter() - start) * 1000, 3),
        "returncode": returncode,
        "timed_out": timed_out,
        "shell": False,
        "command": redact_command(command),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
    }
    recorder.record_step(record)
    if status != "success":
        tail = stderr.strip()[-800:] or stdout.strip()[-800:]
        raise PipelineError(f"step {step_number} {step_name} failed: {tail}")
    return (record, stdout)


def require_status(payload: Dict[str, Any], allowed: Iterable[str], label: str) -> None:
    allowed_set = set(allowed)
    if payload.get("status") not in allowed_set:
        raise PipelineError(f"{label} status is not one of {sorted(allowed_set)}: {payload.get('status')}")


def inspect_duckdb_semantics(
    duckdb_path: Path, sync_payloads: Mapping[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Read-only postflight for the sync-log/latest-view interface contract."""
    try:
        import duckdb
    except ImportError as exc:
        raise PipelineError("duckdb package disappeared before semantic postflight") from exc
    table_map = {
        "instance": ("instance_units_sync", "instance_units_latest"),
        "concept": ("concept_units_sync", "concept_units_latest"),
        "attribute": ("attribute_units_sync", "attribute_units_latest"),
    }
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        sync_log_rows = int(conn.execute("SELECT COUNT(*) FROM _sync_log").fetchone()[0])
        current_sync_ids = [str(sync_payloads[target].get("sync_id", "")) for target in table_map]
        placeholders = ", ".join(["?"] * len(current_sync_ids))
        current_sync_log_rows = int(
            conn.execute(
                f"SELECT COUNT(*) FROM _sync_log WHERE sync_id IN ({placeholders})", current_sync_ids
            ).fetchone()[0]
        )
        counts: Dict[str, Any] = {}
        for target, (base_table, latest_view) in table_map.items():
            base_count = int(conn.execute(f"SELECT COUNT(*) FROM {base_table}").fetchone()[0])
            latest_count = int(conn.execute(f"SELECT COUNT(*) FROM {latest_view}").fetchone()[0])
            expected = int(sync_payloads[target].get("synced_count", -1))
            counts[target] = {
                "base_rows": base_count,
                "latest_rows": latest_count,
                "expected_current_sync_rows": expected,
            }
            if latest_count != expected:
                raise PipelineError(f"{latest_view} semantic count mismatch: {latest_count} != {expected}")
        if current_sync_log_rows != 3:
            raise PipelineError(f"_sync_log must contain all 3 current-run sync ids: {current_sync_log_rows}")
        return {
            "status": "PASS",
            "sync_log_rows_total": sync_log_rows,
            "sync_log_rows_current_run": current_sync_log_rows,
            "current_sync_ids": current_sync_ids,
            "counts": counts,
        }
    finally:
        conn.close()


def execute_duckdb_branch(
    recorder: RunRecorder,
    project_root: Path,
    python_executable: Path,
    anchor: Dict[str, Any],
    duckdb_path: Path,
    duckdb_config: Path,
    retrieval_input: Optional[Path],
    timeout_seconds: int,
    start_step: int,
) -> Tuple[Dict[str, Any], int]:
    duckdb_dir = recorder.business_dir / "duckdb"
    reports_dir = recorder.business_dir / "reports"
    duckdb_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    step = start_step
    _, stdout = run_child(
        recorder,
        step,
        DUCKDB_STEPS[0],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["duckdb_schema"]),
            "--config",
            str(duckdb_config),
            "--db-path",
            str(duckdb_path),
            "--init",
        ],
        project_root,
        timeout_seconds,
    )
    init_payload = parse_json_stdout(stdout, DUCKDB_STEPS[0])
    require_status(init_payload, ("ok",), DUCKDB_STEPS[0])
    step += 1
    source_hash_before = file_sha256(anchor["action_db"])
    sync_payloads: Dict[str, Dict[str, Any]] = {}
    sync_meta_paths: Dict[str, str] = {}
    for target, step_name in zip(("instance", "concept", "attribute"), DUCKDB_STEPS[1:4]):
        sync_meta = reports_dir / f"duckdb_{target}_sync_meta.json"
        _, stdout = run_child(
            recorder,
            step,
            step_name,
            [
                str(python_executable),
                str(project_root / PINNED_SCRIPTS["duckdb_sync"]),
                "--target",
                target,
                "--source-db",
                str(anchor["action_db"]),
                "--target-db",
                str(duckdb_path),
                "--run-meta",
                str(sync_meta),
            ],
            project_root,
            timeout_seconds,
        )
        sync_payload = parse_json_stdout(stdout, step_name)
        require_status(sync_payload, ("ok",), step_name)
        if sync_payload.get("source_count") != sync_payload.get("synced_count"):
            raise PipelineError(
                f"{step_name} source/synced count mismatch: {sync_payload.get('source_count')} != {sync_payload.get('synced_count')}"
            )
        sync_payloads[target] = sync_payload
        sync_meta_paths[target] = str(sync_meta)
        step += 1
    source_hash_after = file_sha256(anchor["action_db"])
    if source_hash_before != source_hash_after:
        raise PipelineError("source Action SQLite hash changed during DuckDB synchronization")
    _, stdout = run_child(
        recorder,
        step,
        DUCKDB_STEPS[4],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["duckdb_schema"]),
            "--config",
            str(duckdb_config),
            "--db-path",
            str(duckdb_path),
            "--validate",
        ],
        project_root,
        timeout_seconds,
    )
    validate_payload = parse_json_stdout(stdout, DUCKDB_STEPS[4])
    require_status(validate_payload, ("ok",), DUCKDB_STEPS[4])
    semantic_postflight = inspect_duckdb_semantics(duckdb_path, sync_payloads)
    step += 1
    probe_sql = "SELECT unit_text_id AS object_id, 'concept' AS object_type, unit_text AS fragment FROM concept_units_latest ORDER BY unit_text_id LIMIT 20"
    _, stdout = run_child(
        recorder,
        step,
        DUCKDB_STEPS[5],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["duckdb_query"]),
            "--sql",
            probe_sql,
            "--db-path",
            str(duckdb_path),
            "--limit",
            "20",
        ],
        project_root,
        timeout_seconds,
    )
    query_payload = parse_json_stdout(stdout, DUCKDB_STEPS[5])
    require_status(query_payload, ("ok",), DUCKDB_STEPS[5])
    expected_probe_rows = min(20, int(sync_payloads["concept"].get("synced_count", 0)))
    if query_payload.get("row_count") != expected_probe_rows:
        raise PipelineError(
            f"DuckDB probe row count mismatch: {query_payload.get('row_count')} != {expected_probe_rows}"
        )
    query_report = reports_dir / "duckdb_query_probe.json"
    write_json_atomic(query_report, query_payload)
    step += 1
    if retrieval_input is None:
        rows = query_payload.get("rows", [])
        if not isinstance(rows, list):
            raise PipelineError("DuckDB probe rows must be a list")
        retrieval_input = reports_dir / "retrieval_probe_input.json"
        write_json_atomic(
            retrieval_input,
            {
                "retrieval_id": f"{recorder.manifest['run_id']}_probe",
                "query": "orchestration derivation probe",
                "retrieval_mode": "controlled_duckdb_probe",
                "results": rows,
            },
        )
    _, stdout = run_child(
        recorder,
        step,
        DUCKDB_STEPS[6],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["duckdb_enrich"]),
            "--input",
            str(retrieval_input),
            "--db-path",
            str(duckdb_path),
            "--no-log",
        ],
        project_root,
        timeout_seconds,
    )
    enrich_payload = parse_json_stdout(stdout, DUCKDB_STEPS[6])
    require_status(enrich_payload, ("ok",), DUCKDB_STEPS[6])
    if enrich_payload.get("input_count") != expected_probe_rows:
        raise PipelineError(
            f"enrichment input count mismatch: {enrich_payload.get('input_count')} != {expected_probe_rows}"
        )
    enrich_report = reports_dir / "retrieval_enrichment_probe.json"
    write_json_atomic(enrich_report, enrich_payload)
    step += 1
    return (
        {
            "status": "completed",
            "database": str(duckdb_path),
            "database_sha256": file_sha256(duckdb_path),
            "source_sqlite": str(anchor["action_db"]),
            "source_sqlite_sha256_before": source_hash_before,
            "source_sqlite_sha256_after": source_hash_after,
            "source_sqlite_unchanged": True,
            "sync_meta": sync_meta_paths,
            "sync_results": sync_payloads,
            "schema_validation": validate_payload,
            "semantic_postflight": semantic_postflight,
            "query_probe": str(query_report),
            "query_rows": query_payload.get("row_count", 0),
            "retrieval_input": str(retrieval_input),
            "enrichment_probe": str(enrich_report),
            "enriched_count": enrich_payload.get("enriched_count", 0),
        },
        step,
    )


def execute_neo4j_branch(
    recorder: RunRecorder,
    project_root: Path,
    python_executable: Path,
    structural: Dict[str, Any],
    connection_config: Path,
    schema_config: Path,
    password: str,
    timeout_seconds: int,
    start_step: int,
) -> Tuple[Dict[str, Any], int]:
    graph_dir = recorder.business_dir / "neo4j"
    graph_dir.mkdir(parents=True, exist_ok=True)
    step = start_step
    schema_meta = graph_dir / "schema_run_meta.json"
    _, stdout = run_child(
        recorder,
        step,
        NEO4J_STEPS[0],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["neo4j_schema"]),
            "--connection-config",
            str(connection_config),
            "--schema-config",
            str(schema_config),
            "--password",
            password,
            "--run-meta",
            str(schema_meta),
        ],
        project_root,
        timeout_seconds,
    )
    schema_payload = parse_json_stdout(stdout, NEO4J_STEPS[0])
    require_status(schema_payload, ("ok", "completed"), NEO4J_STEPS[0])
    step += 1
    graph_meta = graph_dir / "structural_graph_run_meta.json"
    _, stdout = run_child(
        recorder,
        step,
        NEO4J_STEPS[1],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["neo4j_sync"]),
            "--register",
            str(structural["registry"]),
            "--cooccurrence-edges",
            str(structural["cooccurrence"]),
            "--adjacency-edges",
            str(structural["adjacency"]),
            "--connection-config",
            str(connection_config),
            "--password",
            password,
            "--run-meta",
            str(graph_meta),
        ],
        project_root,
        timeout_seconds,
    )
    graph_payload = parse_json_stdout(stdout, NEO4J_STEPS[1])
    step += 1
    hierarchy_input_dir = structural["hierarchy"].parent
    _, stdout = run_child(
        recorder,
        step,
        NEO4J_STEPS[2],
        [
            str(python_executable),
            str(project_root / PINNED_SCRIPTS["neo4j_hierarchy"]),
            "--input-dir",
            str(hierarchy_input_dir),
            "--register",
            str(structural["registry"]),
            "--connection-config",
            str(connection_config),
            "--password",
            password,
            "--output-dir",
            str(graph_dir),
        ],
        project_root,
        timeout_seconds,
    )
    hierarchy_payload = parse_json_stdout(stdout, NEO4J_STEPS[2])
    step += 1
    return (
        {
            "status": "completed",
            "schema_meta": str(schema_meta),
            "graph_meta": str(graph_meta),
            "hierarchy_output_dir": str(graph_dir),
            "schema_result": schema_payload,
            "graph_result": graph_payload,
            "hierarchy_result": hierarchy_payload,
            "destructive_flags_passed": [],
        },
        step,
    )


# ============================================================
# CLI / main 接口区
# ============================================================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reliably materialize Action SQLite facts to DuckDB and structural outputs to Neo4j."
    )
    parser.add_argument("--anchor-completion-manifest", required=True)
    parser.add_argument("--structural-completion-manifest", required=True)
    parser.add_argument("--target", choices=("duckdb", "neo4j", "all"), default="all")
    parser.add_argument("--duckdb-path", default=None)
    parser.add_argument("--duckdb-config", default=str(DEFAULT_DUCKDB_CONFIG))
    parser.add_argument("--retrieval-input", default=None)
    parser.add_argument("--neo4j-connection-config", default=str(DEFAULT_NEO4J_CONNECTION_CONFIG))
    parser.add_argument("--neo4j-schema-config", default=str(DEFAULT_NEO4J_SCHEMA_CONFIG))
    parser.add_argument("--neo4j-password", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--business-output-root", default=str(DEFAULT_BUSINESS_ROOT))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--confirm-duckdb-write", action="store_true")
    parser.add_argument("--confirm-neo4j-write", action="store_true")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    project_root = find_project_root(Path(__file__).resolve())
    temp_root = (project_root / "temp").resolve()
    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError("run_id contains unsupported characters or is too long")
    if args.timeout_seconds < 1:
        raise PipelineError("timeout_seconds must be positive")
    anchor_path = resolve_path(args.anchor_completion_manifest, project_root)
    structural_path = resolve_path(args.structural_completion_manifest, project_root)
    output_root = resolve_path(args.output_root, project_root)
    business_root = resolve_path(args.business_output_root, project_root)
    python_executable = resolve_path(args.python_executable, project_root)
    duckdb_config = resolve_path(args.duckdb_config, project_root)
    connection_config = resolve_path(args.neo4j_connection_config, project_root)
    schema_config = resolve_path(args.neo4j_schema_config, project_root)
    retrieval_input = resolve_path(args.retrieval_input, project_root) if args.retrieval_input else None
    duckdb_path = (
        resolve_path(args.duckdb_path, project_root)
        if args.duckdb_path
        else (project_root / "derivation" / "action" / "duckdb" / "action_data_analysis.duckdb").resolve()
    )
    run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
    business_dir = business_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
    require_file(python_executable, "Python executable")
    anchor = validate_anchor_manifest(anchor_path, project_root)
    structural = validate_structural_manifest(structural_path, project_root)
    validate_lineage(anchor, structural)
    dependencies = validate_dependencies(project_root, args.target)
    if args.target in ("duckdb", "all"):
        require_file(duckdb_config, "DuckDB schema config")
    if args.target in ("neo4j", "all"):
        require_file(connection_config, "Neo4j connection config")
        require_file(schema_config, "Neo4j schema config")
    if retrieval_input is not None:
        require_file(retrieval_input, "retrieval input")
    if args.test_mode:
        for path, label in (
            (anchor_path, "test Anchor manifest"),
            (structural_path, "test Structural manifest"),
            (anchor["action_db"], "test Action SQLite"),
            (output_root, "test output root"),
            (business_root, "test business root"),
        ):
            require_within(path, temp_root, label)
        if args.target in ("duckdb", "all"):
            require_within(duckdb_path, temp_root, "test DuckDB path")
        if retrieval_input is not None:
            require_within(retrieval_input, temp_root, "test retrieval input")
        if not args.dry_run and args.target in ("neo4j", "all"):
            require_within(connection_config, temp_root, "test Neo4j connection config")
            require_within(schema_config, temp_root, "test Neo4j schema config")
    if args.dry_run:
        plan = {
            "status": "dry-run",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "target": args.target,
            "would_write": False,
            "would_create_directories": False,
            "anchor_manifest": str(anchor_path),
            "structural_manifest": str(structural_path),
            "action_sqlite": str(anchor["action_db"]),
            "duckdb_path": str(duckdb_path) if args.target in ("duckdb", "all") else None,
            "structural_inputs": {
                key: str(structural[key]) for key in ("registry", "adjacency", "cooccurrence", "hierarchy")
            },
            "structural_counts": structural["counts"],
            "steps": (
                DUCKDB_STEPS
                if args.target == "duckdb"
                else NEO4J_STEPS if args.target == "neo4j" else DUCKDB_STEPS + NEO4J_STEPS
            ),
            "safety": {"shell": False, "forbidden_child_flags": ["--clear", "--clear-hierarchy"]},
            "dependencies": dependencies,
        }
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return 0
    if args.target in ("duckdb", "all") and (not args.confirm_duckdb_write):
        raise PipelineError("DuckDB execution requires --confirm-duckdb-write")
    password = args.neo4j_password or os.environ.get("NEO4J_PASSWORD")
    if args.target in ("neo4j", "all"):
        if not args.confirm_neo4j_write:
            raise PipelineError("Neo4j execution requires --confirm-neo4j-write")
        if not password:
            raise PipelineError("Neo4j execution requires --neo4j-password or NEO4J_PASSWORD")
    for path, label in ((run_dir, "orchestration run directory"), (business_dir, "business run directory")):
        if len(str(path)) > WINDOWS_SAFE_PATH_LENGTH:
            raise PipelineError(f"{label} exceeds safe Windows path length: {path}")
        if path.exists():
            raise PipelineError(f"{label} already exists; runs are append-only: {path}")
    manifest = {
        "schema_version": "action_derivation_materialization_run_v0001",
        "pipeline": SCRIPT_FAMILY,
        "pipeline_version": SCRIPT_VERSION,
        "run_id": run_id,
        "status": "planned",
        "started_at": utc_now_z(),
        "target": args.target,
        "test_mode": bool(args.test_mode),
        "project_root": str(project_root),
        "output_run_dir": str(run_dir),
        "business_run_dir": str(business_dir),
        "prerequisites": {
            "anchor_manifest": str(anchor_path),
            "anchor_run_id": anchor["run_id"],
            "structural_manifest": str(structural_path),
            "structural_run_id": structural["run_id"],
        },
        "dependencies": dependencies,
        "safety": {
            "subprocess_shell": False,
            "destructive_flags_passed": [],
            "automatic_retry": False,
            "action_development_registry": "excluded: sql/action.db",
        },
        "steps": [],
    }
    recorder = RunRecorder(run_dir, business_dir, manifest)
    recorder.initialize()
    recorder.update(status="running")
    branches: Dict[str, Any] = {}
    step = 1
    try:
        if args.target in ("duckdb", "all"):
            branches["duckdb"], step = execute_duckdb_branch(
                recorder,
                project_root,
                python_executable,
                anchor,
                duckdb_path,
                duckdb_config,
                retrieval_input,
                args.timeout_seconds,
                step,
            )
        else:
            branches["duckdb"] = {"status": "not_selected"}
        if args.target in ("neo4j", "all"):
            branches["neo4j"], step = execute_neo4j_branch(
                recorder,
                project_root,
                python_executable,
                structural,
                connection_config,
                schema_config,
                str(password),
                args.timeout_seconds,
                step,
            )
        else:
            branches["neo4j"] = {"status": "not_selected"}
        completion = {
            "schema_version": "action_derivation_materialization_completion_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "completed_at": utc_now_z(),
            "target": args.target,
            "test_mode": bool(args.test_mode),
            "prerequisites": manifest["prerequisites"],
            "source": {
                "action_sqlite": str(anchor["action_db"]),
                "action_sqlite_sha256": file_sha256(anchor["action_db"]),
                "structural_business_dir": str(structural["business_dir"]),
                "structural_inputs": {
                    key: str(structural[key])
                    for key in ("registry", "adjacency", "cooccurrence", "hierarchy")
                },
                "structural_counts": structural["counts"],
            },
            "branches": branches,
            "execution": {
                "distinct_scripts": len(dependencies["scripts"]),
                "execution_nodes": len(recorder.steps),
                "all_steps_success": all((item.get("status") == "success" for item in recorder.steps)),
                "shell": False,
                "destructive_flags_passed": [],
            },
            "business_run_dir": str(business_dir),
            "run_manifest": str(recorder.manifest_path),
        }
        write_json_atomic(recorder.completion_path, completion)
        recorder.update(
            status="completed",
            completed_at=completion["completed_at"],
            branches=branches,
            completion_manifest=str(recorder.completion_path),
        )
        print(json.dumps(completion, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        failure = {
            "schema_version": "action_derivation_materialization_failure_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "failed",
            "failed_at": utc_now_z(),
            "error": str(exc),
            "traceback": traceback.format_exc()[-3000:],
            "completed_steps": len([item for item in recorder.steps if item.get("status") == "success"]),
            "branches": branches,
            "completion_manifest_created": False,
        }
        write_json_atomic(recorder.failure_path, failure)
        recorder.update(status="failed", failed_at=failure["failed_at"], failure=str(recorder.failure_path))
        print(json.dumps(failure, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
