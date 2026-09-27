# ============================================================
# 文件名: data_discovery_parse_preparation_pipeline_v0004.py
# 中文名: 自包含规范数据发现与解析编排
# 版本号: v0004
#
# 主层级: data
# 层级: scripts/orchestration/data
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
# alias: data_discovery_parse_preparation_pipeline_v0004
# family: data_discovery_parse_preparation_pipeline
# role: canonical_data_discovery_preparation_orchestrator
# version: v0004
# status: active
# entry_point: scripts/orchestration/data/data_discovery_parse_preparation_pipeline_v0004.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - analyze_json_structure_v0003.py
#   - coarse_slice_conversations_v0003.py
#   - data_admission_lineage_pipeline_v0001.py
#   - discover_data_v0001.py
#   - identify_file_type_v0002.py
#   - inventory_data_snapshot_v0001.py
#   - prepare_json_parse_task_v0001.py
#   - snapshot_path_resolver_v0001.py
# used_by:
#   - data_action_chain_pipeline_v0015
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/orchestration/data/data_discovery_parse_preparation_pipeline_v0001.py, scripts/orchestration/data/data_discovery_parse_preparation_pipeline_v0003.py
# ============================================================
# 依赖
# ============================================================

from __future__ import annotations
import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
import importlib.util
from typing import Any, Mapping, Optional, Sequence

# ============================================================
# 全局常量与默认映射
# ============================================================

DEFAULT_DATA_ROOT = Path("data")


DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "data"


DEFAULT_INTERMEDIATE_ROOT = Path("reports") / "orchestration"


DEFAULT_WORKSPACE_ROOT = Path("parse_workspace") / "orchestration"


DEFAULT_STEP_TIMEOUT_SECONDS = 600


DEFAULT_MAX_LOAD_BYTES = 64 * 1024 * 1024


DEFAULT_MAX_NODES = 250000


DEFAULT_MAX_DEPTH = 64


DEFAULT_MAX_LIST_SAMPLE = 10000


DEFAULT_MAX_KEYS_SAMPLE = 10000


DEFAULT_MAX_KEY_SAMPLE_STORE = 10000


DEFAULT_MAX_CONVERSATIONS = 10000


DOWNSTREAM_ORCHESTRATOR_RELATIVE = (
    Path("scripts") / "orchestration" / "data" / "data_admission_lineage_pipeline_v0001.py"
)


PINNED_SCRIPTS: Mapping[str, str] = {
    "discover": "discover_data_v0001.py",
    "inventory": "inventory_data_snapshot_v0001.py",
    "identify": "identify_file_type_v0002.py",
    "prepare": "prepare_json_parse_task_v0001.py",
    "resolve": "snapshot_path_resolver_v0001.py",
    "analyze": "analyze_json_structure_v0003.py",
    "slice": "coarse_slice_conversations_v0003.py",
}


RUN_ID_PATTERN = re.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


DEFAULT_ENCODING = "utf-8"


SCRIPT_FAMILY = "data_discovery_parse_preparation_pipeline"


SCRIPT_NAME = "data_discovery_parse_preparation_pipeline_v0004.py"


SCRIPT_VERSION = "v0004"


CANONICAL_SCHEMA_VERSION = "canonical_conversation_ingress_v0001"


CANONICAL_CONVERSATION_FIELDS = {
    "conversation_id",
    "title",
    "created_time",
    "updated_time",
    "current_node_id",
    "nodes",
    "metadata",
    "source_ref",
    "extensions",
}


CANONICAL_TEXT_PATH = "/conversations/*/nodes/*/message/content/parts/*/text"

# ============================================================
# 异常类型
# ============================================================


class PipelineError(RuntimeError):
    """Raised when a reliable handoff condition is not satisfied."""


class CanonicalDiscoveryV2Error(RuntimeError):
    """Raised when the v0001 discovery base cannot be configured for canonical ingress."""


# ============================================================
# 数据结构、工具与核心实现
# ============================================================


@dataclass(frozen=True)
class StepResult:
    name: str
    returncode: int
    stdout: str
    stderr: str
    duration_ms: int


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{stamp}_{uuid.uuid4().hex[:8]}"


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(text, encoding="utf-8")
    temp_path.replace(path)


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
        stream.flush()


def normalize_relative_path(value: str, *, label: str) -> Path:
    raw = value.strip()
    if not raw:
        raise PipelineError(f"{label} must not be empty")
    candidate = Path(raw.replace("/", os.sep))
    if candidate.is_absolute() or candidate.drive or candidate.anchor:
        raise PipelineError(f"{label} must be relative: {value}")
    if any((part in {"", ".", ".."} for part in candidate.parts)):
        raise PipelineError(f"{label} contains an unsafe path component: {value}")
    return candidate


def resolve_under(root: Path, relative: Path, *, label: str) -> Path:
    root_resolved = root.resolve()
    candidate = (root_resolved / relative).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise PipelineError(f"{label} escapes its allowed root: {relative}") from exc
    return candidate


def resolve_from_project(project_root: Path, value: str, *, label: str) -> Path:
    raw = Path(value)
    candidate = raw if raw.is_absolute() else project_root / raw
    resolved = candidate.resolve()
    if resolved.exists() and (not resolved.is_dir()):
        raise PipelineError(f"{label} is not a directory: {resolved}")
    return resolved


def path_relative_to(path: Path, root: Path, *, label: str) -> Path:
    try:
        return path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise PipelineError(f"{label} must be inside {root.resolve()}: {path.resolve()}") from exc


def normalize_key(value: str) -> str:
    return value.replace("\\", "/").lstrip("./")


def find_project_root(explicit: str) -> Path:
    if explicit:
        candidates = [Path(explicit).resolve()]
    else:
        candidates = []
        for starting_point in (Path(__file__).resolve().parent, Path.cwd().resolve()):
            candidates.extend([starting_point, *starting_point.parents])
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if (
            (candidate / "AGENTS.md").is_file()
            and (candidate / "scripts").is_dir()
            and (candidate / "data").is_dir()
        ):
            return candidate
    if explicit:
        raise PipelineError(f"invalid project root: {Path(explicit).resolve()}")
    raise PipelineError("unable to locate project root from script path or current directory")


def validate_pinned_scripts(project_root: Path) -> Dict[str, Path]:
    scripts_dir = project_root / "scripts"
    resolved: Dict[str, Path] = {}
    for step_name, filename in PINNED_SCRIPTS.items():
        script_path = scripts_dir / filename
        if not script_path.is_file():
            raise PipelineError(f"required pinned script is missing: {script_path}")
        resolved[step_name] = script_path.resolve()
    return resolved


def parse_json_text(text: str, *, label: str) -> Any:
    stripped = text.strip()
    if not stripped:
        raise PipelineError(f"{label} is empty")
    try:
        return json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise PipelineError(f"{label} is not valid JSON: {exc}") from exc


def load_json_file(path: Path, *, label: str) -> Any:
    if not path.is_file():
        raise PipelineError(f"{label} does not exist: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"unable to read {label}: {path}: {exc}") from exc


def load_jsonl_file(path: Path, *, label: str) -> List[Dict[str, Any]]:
    if not path.is_file():
        raise PipelineError(f"{label} does not exist: {path}")
    records: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                value = json.loads(stripped)
                if not isinstance(value, dict):
                    raise PipelineError(f"{label} line {line_number} must contain a JSON object")
                records.append(value)
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"unable to read {label}: {path}: {exc}") from exc
    return records


def ensure_single_file(directory: Path, pattern: str, *, label: str) -> Path:
    matches = sorted((path for path in directory.glob(pattern) if path.is_file()))
    if len(matches) != 1:
        raise PipelineError(f"{label} expected exactly one file matching {pattern}, found {len(matches)}")
    return matches[0]


def validate_identify_stdout(stdout: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for line_number, line in enumerate(stdout.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise PipelineError(f"identify stdout line {line_number} is not valid JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise PipelineError(f"identify stdout line {line_number} is not a JSON object")
        for required_field in ("relative_path", "size_bytes", "file_type"):
            if required_field not in value:
                raise PipelineError(f"identify record on line {line_number} is missing {required_field}")
        records.append(value)
    if not records:
        raise PipelineError("identify produced no records")
    return records


def find_record(
    records: Iterable[Mapping[str, Any]], *, path_field: str, target_key: str, label: str
) -> Dict[str, Any]:
    matches = [
        dict(record) for record in records if normalize_key(str(record.get(path_field, ""))) == target_key
    ]
    if len(matches) != 1:
        raise PipelineError(f"{label} expected one record for {target_key}, found {len(matches)}")
    return matches[0]


class PipelineRun:

    def __init__(
        self,
        *,
        project_root: Path,
        data_root: Path,
        output_root: Path,
        business_run_dir: Path,
        run_id: str,
        target_relative: Path,
        scripts: Mapping[str, Path],
        step_timeout_seconds: int,
    ) -> None:
        self.project_root = project_root
        self.data_root = data_root
        self.output_root = output_root
        self.business_run_dir = business_run_dir
        self.run_id = run_id
        self.target_relative = target_relative
        self.scripts = dict(scripts)
        self.step_timeout_seconds = step_timeout_seconds
        self.run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
        self.stdout_dir = self.run_dir / "captured_stdout"
        self.stderr_dir = self.run_dir / "captured_stderr"
        self.step_status_path = self.run_dir / "step_status.jsonl"
        self.manifest_path = self.run_dir / "run_manifest.json"
        self.steps: List[Dict[str, Any]] = []
        self.manifest: Dict[str, Any] = {
            "schema_version": "reliable_pipeline_run_v0001",
            "pipeline": {"script": SCRIPT_NAME, "family": SCRIPT_FAMILY, "version": SCRIPT_VERSION},
            "run_id": run_id,
            "status": "running",
            "started_at": utc_now_z(),
            "finished_at": None,
            "project_root": str(project_root),
            "data_root": str(data_root),
            "output_root": str(output_root),
            "run_dir": str(self.run_dir),
            "business_run_dir": str(business_run_dir),
            "target_relative": target_relative.as_posix(),
            "pinned_scripts": dict(PINNED_SCRIPTS),
            "steps": self.steps,
            "error": None,
        }

    def initialize(self) -> None:
        if self.run_dir.exists():
            raise PipelineError(f"run directory already exists: {self.run_dir}")
        if self.business_run_dir.exists():
            raise PipelineError(f"business intermediate directory already exists: {self.business_run_dir}")
        self.stdout_dir.mkdir(parents=True, exist_ok=False)
        self.stderr_dir.mkdir(parents=True, exist_ok=False)
        self.business_run_dir.mkdir(parents=True, exist_ok=False)
        write_json_atomic(self.manifest_path, self.manifest)

    def persist_manifest(self) -> None:
        write_json_atomic(self.manifest_path, self.manifest)

    def run_step(self, name: str, script_key: str, args: Sequence[str]) -> StepResult:
        script_path = self.scripts[script_key]
        command = [sys.executable, str(script_path), *[str(item) for item in args]]
        step_number = len(self.steps) + 1
        stdout_path = self.stdout_dir / f"{step_number:02d}_{name}.txt"
        stderr_path = self.stderr_dir / f"{step_number:02d}_{name}.txt"
        started_at = utc_now_z()
        started_clock = time.monotonic()
        environment = os.environ.copy()
        environment["PYTHONUTF8"] = "1"
        try:
            completed = subprocess.run(
                command,
                cwd=self.project_root,
                env=environment,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.step_timeout_seconds,
                check=False,
                shell=False,
            )
            stdout = completed.stdout or ""
            stderr = completed.stderr or ""
            returncode = int(completed.returncode)
            timed_out = False
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout if isinstance(exc.stdout, str) else ""
            stderr = exc.stderr if isinstance(exc.stderr, str) else ""
            returncode = -1
            timed_out = True
        duration_ms = int((time.monotonic() - started_clock) * 1000)
        write_text_atomic(stdout_path, stdout)
        write_text_atomic(stderr_path, stderr)
        step_record: Dict[str, Any] = {
            "index": step_number,
            "name": name,
            "script": script_path.name,
            "command": command,
            "started_at": started_at,
            "finished_at": utc_now_z(),
            "duration_ms": duration_ms,
            "returncode": returncode,
            "timed_out": timed_out,
            "status": "success" if returncode == 0 and (not timed_out) else "failure",
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }
        self.steps.append(step_record)
        append_jsonl(self.step_status_path, step_record)
        self.persist_manifest()
        if timed_out:
            raise PipelineError(f"step {name} exceeded timeout of {self.step_timeout_seconds} seconds")
        if returncode != 0:
            detail = stderr.strip() or stdout.strip() or "no process output"
            raise PipelineError(f"step {name} failed with exit code {returncode}: {detail}")
        return StepResult(
            name=name, returncode=returncode, stdout=stdout, stderr=stderr, duration_ms=duration_ms
        )

    def finish_success(self, handoff_path: Path, *, final_status: str) -> None:
        self.manifest["status"] = final_status
        self.manifest["finished_at"] = utc_now_z()
        self.manifest["handoff_manifest"] = str(handoff_path)
        self.persist_manifest()

    def finish_failure(self, error: Exception) -> None:
        self.manifest["status"] = "failed"
        self.manifest["finished_at"] = utc_now_z()
        self.manifest["error"] = {"type": type(error).__name__, "message": str(error)}
        self.persist_manifest()
        write_json_atomic(
            self.run_dir / "failure.json",
            {
                "run_id": self.run_id,
                "status": "failed",
                "failed_at": utc_now_z(),
                "error": self.manifest["error"],
            },
        )


def execute_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = find_project_root(args.project_root)
    scripts = validate_pinned_scripts(project_root)
    data_root = resolve_from_project(project_root, args.data_root, label="data root")
    if not data_root.is_dir():
        raise PipelineError(f"data root does not exist: {data_root}")
    path_relative_to(data_root, project_root, label="data root")
    target_relative = normalize_relative_path(args.target, label="target")
    if not target_relative.parts or target_relative.parts[0] != "data_raw":
        raise PipelineError("target must be located under data_raw")
    target_path = resolve_under(data_root, target_relative, label="target")
    if not target_path.is_file():
        raise PipelineError(f"target file does not exist: {target_path}")
    output_root = resolve_from_project(project_root, args.output_root, label="output root")
    intermediate_root = normalize_relative_path(args.intermediate_root, label="intermediate root")
    workspace_root = normalize_relative_path(args.workspace_root, label="workspace root")
    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError(
            "run_id must start with an alphanumeric character and contain only letters, numbers, dot, underscore, or hyphen"
        )
    downstream_script: Optional[Path] = None
    downstream_run_id = args.admission_run_id or f"{run_id}_admission"
    admission_output_root_value = args.admission_output_root or args.output_root
    if args.continue_to_admission:
        if not args.confirm_admission and (not args.dry_run):
            raise PipelineError("automatic continuation requires --confirm-admission")
        if not args.admission_target_root:
            raise PipelineError("automatic continuation requires --admission-target-root")
        if not RUN_ID_PATTERN.fullmatch(downstream_run_id):
            raise PipelineError("admission run_id contains unsupported characters")
        downstream_script = (project_root / DOWNSTREAM_ORCHESTRATOR_RELATIVE).resolve()
        if not downstream_script.is_file():
            raise PipelineError(f"fixed downstream orchestrator is missing: {downstream_script}")
        scripts["admission"] = downstream_script
    business_run_relative = intermediate_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
    business_run_dir = resolve_under(
        data_root, business_run_relative, label="business intermediate directory"
    )
    plan = {
        "pipeline": SCRIPT_FAMILY,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "project_root": str(project_root),
        "data_root": str(data_root),
        "target": target_relative.as_posix(),
        "output_root": str(output_root),
        "run_dir": str(output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id),
        "business_run_dir": str(business_run_dir),
        "pinned_scripts": dict(PINNED_SCRIPTS),
        "continue_to_admission": bool(args.continue_to_admission),
        "downstream_orchestrator": (
            str(DOWNSTREAM_ORCHESTRATOR_RELATIVE) if args.continue_to_admission else None
        ),
        "downstream_run_id": downstream_run_id if args.continue_to_admission else None,
        "dry_run": bool(args.dry_run),
    }
    if args.dry_run:
        return plan
    run = PipelineRun(
        project_root=project_root,
        data_root=data_root,
        output_root=output_root,
        business_run_dir=business_run_dir,
        run_id=run_id,
        target_relative=target_relative,
        scripts=scripts,
        step_timeout_seconds=args.step_timeout,
    )
    run.initialize()
    try:
        discover_report_relative = business_run_relative / "discover"
        run.run_step(
            "discover_internal",
            "discover",
            [
                "--data-root",
                str(data_root),
                "--reports-rel",
                str(discover_report_relative),
                "--mode",
                "internal",
            ],
        )
        discover_report_path = ensure_single_file(
            business_run_dir / "discover", "*__internal.json", label="discover report"
        )
        discover_report = load_json_file(discover_report_path, label="discover report")
        if not isinstance(discover_report, dict):
            raise PipelineError("discover report must contain a JSON object")
        inventory_dir = run.run_dir / "inventory"
        run.run_step(
            "inventory_snapshot",
            "inventory",
            ["--data-dir", str(data_root), "--output-dir", str(inventory_dir), "--no-hash"],
        )
        inventory_path = ensure_single_file(inventory_dir, "inventory_*.jsonl", label="inventory snapshot")
        inventory_records = load_jsonl_file(inventory_path, label="inventory snapshot")
        identify_result = run.run_step(
            "identify_file_types", "identify", ["--data-root", str(data_root), "--jsonl"]
        )
        identify_records = validate_identify_stdout(identify_result.stdout)
        identify_dir = business_run_dir / "identify"
        identify_snapshot_path = identify_dir / "identified_files.jsonl"
        identify_snapshot_text = "".join(
            (json.dumps(record, ensure_ascii=False) + "\n" for record in identify_records)
        )
        write_text_atomic(identify_snapshot_path, identify_snapshot_text)
        target_key = normalize_key(target_relative.as_posix())
        discover_record = find_record(
            discover_report.get("found") or [],
            path_field="locator",
            target_key=target_key,
            label="discover report",
        )
        inventory_record = find_record(
            inventory_records, path_field="path_relative", target_key=target_key, label="inventory snapshot"
        )
        identify_record = find_record(
            identify_records, path_field="relative_path", target_key=target_key, label="identify snapshot"
        )
        if identify_record.get("file_type") != "json":
            raise PipelineError(f"target is not identified as json: {identify_record.get('file_type')}")
        observed_sizes = {
            "discover": discover_record.get("size_bytes"),
            "inventory": inventory_record.get("size_bytes"),
            "identify": identify_record.get("size_bytes"),
            "filesystem": target_path.stat().st_size,
        }
        if len(set(observed_sizes.values())) != 1:
            raise PipelineError(f"target size evidence is inconsistent: {observed_sizes}")
        reconciliation = {
            "schema_version": "reliable_handoff_check_v0001",
            "target_relative": target_key,
            "decision": "ready_for_parse",
            "checks": {
                "discover_record_present": True,
                "inventory_record_present": True,
                "identify_record_present": True,
                "identified_file_type": identify_record.get("file_type"),
                "size_evidence_consistent": True,
            },
            "size_evidence": observed_sizes,
            "evidence": {
                "discover_report": str(discover_report_path),
                "inventory_snapshot": str(inventory_path),
                "identify_snapshot": str(identify_snapshot_path),
            },
        }
        reconciliation_path = run.run_dir / "reconciliation.json"
        write_json_atomic(reconciliation_path, reconciliation)
        task_id = f"task_{run_id}"
        task_workspace_relative = workspace_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
        prepare_result = run.run_step(
            "prepare_json_task",
            "prepare",
            [
                "--data-root",
                str(data_root),
                "--snapshot",
                str(identify_snapshot_path),
                "--select",
                target_key,
                "--workspace-root",
                str(task_workspace_relative),
                "--task-id",
                task_id,
                "--expected-type",
                "json",
            ],
        )
        prepare_summary = parse_json_text(prepare_result.stdout, label="prepare step stdout")
        if not isinstance(prepare_summary, dict) or prepare_summary.get("count") != 1:
            raise PipelineError("prepare step did not report exactly one working copy")
        task_dir_relative = normalize_relative_path(
            str(prepare_summary.get("task_dir", "")), label="prepared task directory"
        )
        task_dir = resolve_under(data_root, task_dir_relative, label="prepared task")
        task_manifest_path = task_dir / "manifest.json"
        task_manifest = load_json_file(task_manifest_path, label="parse task manifest")
        if not isinstance(task_manifest, dict):
            raise PipelineError("parse task manifest must contain a JSON object")
        working_copies = task_manifest.get("working_copies")
        if not isinstance(working_copies, list) or len(working_copies) != 1:
            raise PipelineError("parse task manifest must contain exactly one working copy")
        working_copy_relative = normalize_relative_path(str(working_copies[0]), label="working copy")
        working_copy_path = resolve_under(data_root, working_copy_relative, label="working copy")
        if not working_copy_path.is_file():
            raise PipelineError(f"working copy does not exist: {working_copy_path}")
        logical_working_copy = path_relative_to(working_copy_path, project_root, label="working copy")
        resolver_result = run.run_step(
            "resolve_working_copy",
            "resolve",
            [logical_working_copy.as_posix(), "--root", str(project_root), "--check-exists"],
        )
        resolved_by_script = Path(resolver_result.stdout.strip()).resolve()
        if resolved_by_script != working_copy_path.resolve():
            raise PipelineError(
                f"path resolver returned an unexpected path: {resolved_by_script} != {working_copy_path.resolve()}"
            )
        run.run_step(
            "analyze_json_structure",
            "analyze",
            [
                "--json-file",
                str(working_copy_path),
                "--max-depth",
                str(args.max_depth),
                "--max-nodes",
                str(args.max_nodes),
                "--max-list-sample",
                str(args.max_list_sample),
                "--max-keys-sample",
                str(args.max_keys_sample),
                "--max-key-sample-store",
                str(args.max_key_sample_store),
                "--max-load-bytes",
                str(args.max_load_bytes),
            ],
        )
        structure_report_path = working_copy_path.parent / "_analysis" / "structure_report.json"
        structure_report = load_json_file(structure_report_path, label="structure report")
        if not isinstance(structure_report, dict):
            raise PipelineError("structure report must contain a JSON object")
        expected_conversations = validate_conversation_structure(
            structure_report, max_conversations=args.max_conversations
        )
        input_relative_to_task = path_relative_to(working_copy_path, task_dir, label="slice input")
        run.run_step(
            "coarse_slice_conversations",
            "slice",
            [
                "--task-dir",
                str(task_dir),
                "--input-file",
                str(input_relative_to_task),
                "--out-subdir",
                "slices",
                "--mode",
                "files",
                "--limit",
                str(args.max_conversations),
            ],
        )
        slices_dir = task_dir / "slices"
        slice_manifest_path = slices_dir / "slice_manifest.json"
        slice_manifest = load_json_file(slice_manifest_path, label="slice manifest")
        if not isinstance(slice_manifest, dict):
            raise PipelineError("slice manifest must contain a JSON object")
        slice_files = sorted(slices_dir.glob("conv_*.json"))
        if slice_manifest.get("mode") != "files":
            raise PipelineError("slice manifest mode must be files")
        if slice_manifest.get("slice_count") != expected_conversations:
            raise PipelineError("slice manifest count does not match analyzed conversation count")
        if len(slice_files) != expected_conversations:
            raise PipelineError(f"expected {expected_conversations} slice files, found {len(slice_files)}")
        for slice_path in slice_files:
            slice_record = load_json_file(slice_path, label=f"slice file {slice_path.name}")
            if not isinstance(slice_record, dict):
                raise PipelineError(f"slice file must contain an object: {slice_path}")
            if not isinstance(slice_record.get("_meta"), dict):
                raise PipelineError(f"slice file is missing _meta: {slice_path}")
            if not isinstance(slice_record.get("conversation"), dict):
                raise PipelineError(f"slice file is missing conversation: {slice_path}")
        handoff = {
            "schema_version": "parse_preparation_handoff_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "ready_for_admission",
            "created_at": utc_now_z(),
            "target": {
                "relative_path": target_key,
                "absolute_path": str(target_path),
                "file_type": "json",
                "size_bytes": target_path.stat().st_size,
            },
            "evidence": {
                "discover_report": str(discover_report_path),
                "inventory_snapshot": str(inventory_path),
                "identify_snapshot": str(identify_snapshot_path),
                "reconciliation": str(reconciliation_path),
                "task_manifest": str(task_manifest_path),
                "working_copy": str(working_copy_path),
                "structure_report": str(structure_report_path),
                "slice_manifest": str(slice_manifest_path),
            },
            "prepared_output": {
                "task_dir": str(task_dir),
                "slices_dir": str(slices_dir),
                "slice_count": len(slice_files),
                "slice_files": [str(path) for path in slice_files],
            },
            "next_step": "formal_admission_requires_separate_authorization",
        }
        handoff_path = run.run_dir / "handoff_manifest.json"
        write_json_atomic(handoff_path, handoff)
        downstream_summary: Optional[Dict[str, Any]] = None
        final_status = "ready_for_admission"
        if args.continue_to_admission:
            downstream_args = [
                "--project-root",
                str(project_root),
                "--handoff-manifest",
                str(handoff_path),
                "--target-root",
                args.admission_target_root,
                "--registry-dir",
                args.admission_registry_dir,
                "--asset-version",
                args.admission_asset_version,
                "--output-root",
                admission_output_root_value,
                "--run-id",
                downstream_run_id,
                "--step-timeout",
                str(args.step_timeout),
                "--confirm-admission",
            ]
            if args.admission_test_mode:
                downstream_args.append("--test-mode")
            downstream_result = run.run_step(
                "continue_to_data_admission_lineage", "admission", downstream_args
            )
            downstream_summary = parse_json_text(
                downstream_result.stdout, label="downstream admission stdout"
            )
            if not isinstance(downstream_summary, dict) or downstream_summary.get("status") != "completed":
                raise PipelineError("downstream admission did not complete")
            run.manifest["downstream"] = downstream_summary
            final_status = "completed"
        run.finish_success(handoff_path, final_status=final_status)
        result = {
            "pipeline": SCRIPT_FAMILY,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": final_status,
            "run_dir": str(run.run_dir),
            "handoff_manifest": str(handoff_path),
            "task_dir": str(task_dir),
            "slice_count": len(slice_files),
        }
        if downstream_summary is not None:
            result["downstream"] = downstream_summary
        return result
    except Exception as exc:
        run.finish_failure(exc)
        raise


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def _build_parser_core() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reliably chain the fixed-version data discovery and parse-preparation scripts. The pipeline stops at ready_for_admission."
    )
    parser.add_argument(
        "--project-root",
        default="",
        help="Optional project root. By default it is discovered from the script path or cwd.",
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root, relative to project root unless absolute.",
    )
    parser.add_argument(
        "--target", required=True, help="One target file relative to data root; it must be under data_raw."
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Orchestration evidence root, relative to project root unless absolute. Default: scripts/orchestration/outputs/data",
    )
    parser.add_argument(
        "--intermediate-root",
        default=str(DEFAULT_INTERMEDIATE_ROOT),
        help="Business intermediate root relative to data root.",
    )
    parser.add_argument(
        "--workspace-root",
        default=str(DEFAULT_WORKSPACE_ROOT),
        help="Parse workspace root relative to data root.",
    )
    parser.add_argument("--run-id", default="", help="Optional safe, unique run id.")
    parser.add_argument(
        "--step-timeout",
        type=positive_integer,
        default=DEFAULT_STEP_TIMEOUT_SECONDS,
        help="Timeout in seconds for each child script.",
    )
    parser.add_argument(
        "--max-load-bytes",
        type=positive_integer,
        default=DEFAULT_MAX_LOAD_BYTES,
        help="Maximum JSON bytes accepted by the structure analyzer.",
    )
    parser.add_argument(
        "--max-nodes",
        type=positive_integer,
        default=DEFAULT_MAX_NODES,
        help="Maximum JSON nodes inspected by the structure analyzer.",
    )
    parser.add_argument(
        "--max-depth", type=positive_integer, default=DEFAULT_MAX_DEPTH, help="Maximum JSON traversal depth."
    )
    parser.add_argument(
        "--max-list-sample",
        type=positive_integer,
        default=DEFAULT_MAX_LIST_SAMPLE,
        help="Maximum sampled elements per JSON array.",
    )
    parser.add_argument(
        "--max-keys-sample",
        type=positive_integer,
        default=DEFAULT_MAX_KEYS_SAMPLE,
        help="Maximum sampled keys per JSON object.",
    )
    parser.add_argument(
        "--max-key-sample-store",
        type=positive_integer,
        default=DEFAULT_MAX_KEY_SAMPLE_STORE,
        help="Maximum stored key samples per structure path.",
    )
    parser.add_argument(
        "--max-conversations",
        type=positive_integer,
        default=DEFAULT_MAX_CONVERSATIONS,
        help="Safety ceiling for conversation slicing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and print the resolved plan without creating output directories.",
    )
    parser.add_argument(
        "--continue-to-admission",
        action="store_true",
        help="Explicitly invoke the fixed downstream admission orchestrator after handoff.",
    )
    parser.add_argument(
        "--admission-target-root",
        default="",
        help="Explicit target root passed to the downstream admission orchestrator.",
    )
    parser.add_argument(
        "--admission-registry-dir",
        default=str(Path("understanding") / "registry"),
        help="Registry directory passed to the downstream admission orchestrator.",
    )
    parser.add_argument(
        "--admission-asset-version",
        default="v0001",
        help="Asset version passed to the downstream admission orchestrator.",
    )
    parser.add_argument(
        "--admission-output-root",
        default="",
        help="Optional downstream evidence root; defaults to this pipeline output root.",
    )
    parser.add_argument(
        "--admission-run-id",
        default="",
        help="Optional downstream run id; defaults to <upstream_run_id>_admission.",
    )
    parser.add_argument(
        "--admission-test-mode",
        action="store_true",
        help="Require downstream target and registry directories to stay under project temp.",
    )
    parser.add_argument(
        "--confirm-admission",
        action="store_true",
        help="Explicitly authorize downstream admission when automatic continuation is enabled.",
    )
    return parser


def _find_project_root() -> Path:
    for candidate in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        if (candidate / "AGENTS.md").is_file() and (candidate / "scripts").is_dir():
            return candidate
    raise CanonicalDiscoveryV2Error("cannot locate project root")


PROJECT_ROOT = _find_project_root()


def validate_conversation_structure(report: Mapping[str, Any], *, max_conversations: int) -> int:
    if report.get("parse_status") != "success":
        raise PipelineError(f"structure analysis did not succeed: {report.get('parse_status')}")
    if report.get("truncated"):
        raise PipelineError("structure analysis was truncated")
    if report.get("root_type") != "object":
        raise PipelineError("canonical ingress root must be an object")
    paths = report.get("paths")
    if not isinstance(paths, list):
        raise PipelineError("structure report paths must be a list")
    root_entry: Optional[Mapping[str, Any]] = None
    conversations_entry: Optional[Mapping[str, Any]] = None
    conversation_entry: Optional[Mapping[str, Any]] = None
    for item in paths:
        if not isinstance(item, Mapping):
            continue
        pattern = item.get("path_pattern")
        node_type = item.get("node_type")
        if pattern == "/" and node_type == "object":
            root_entry = item
        elif pattern == "/conversations" and node_type == "array":
            conversations_entry = item
        elif pattern == "/conversations/*" and node_type == "object":
            conversation_entry = item
    if root_entry is None or conversations_entry is None or conversation_entry is None:
        raise PipelineError("structure is not canonical ingress with a conversations array")
    root_keys = set(root_entry.get("key_sample") or [])
    if {"schema_version", "dataset_id", "source_envelope_id", "conversations"} - root_keys:
        raise PipelineError("canonical root is missing contract identity fields")
    conversation_keys = set(conversation_entry.get("key_sample") or [])
    missing = sorted(CANONICAL_CONVERSATION_FIELDS - conversation_keys)
    if missing:
        raise PipelineError(f"canonical conversation records are missing fields: {missing}")
    count = conversations_entry.get("array_len_max")
    if not isinstance(count, int) or count < 1:
        raise PipelineError("canonical conversations array is missing or empty")
    if count > max_conversations:
        raise PipelineError(f"conversation count {count} exceeds safety limit {max_conversations}")
    return count


def _write_content_structure_handoff(result: Mapping[str, Any]) -> Mapping[str, Any]:
    handoff_path = Path(str(result.get("handoff_manifest", ""))).resolve()
    handoff = load_json_file(handoff_path, label="canonical discovery handoff")
    evidence = handoff.get("evidence")
    if not isinstance(evidence, Mapping):
        raise PipelineError("canonical discovery handoff is missing evidence")
    source_report_path = Path(str(evidence.get("structure_report", ""))).resolve()
    report = load_json_file(source_report_path, label="complete canonical structure report")
    paths = report.get("paths")
    if not isinstance(paths, list):
        raise PipelineError("complete canonical structure report has no paths")
    selected = [
        dict(item)
        for item in paths
        if isinstance(item, Mapping)
        and item.get("path_pattern") == CANONICAL_TEXT_PATH
        and (item.get("node_type") == "string")
    ]
    if len(selected) != 1:
        raise PipelineError("canonical content text path is missing or ambiguous")
    filtered_report = {
        **report,
        "schema_version": "canonical_content_structure_report_v0001",
        "paths": selected,
        "path_scope": {
            "contract": CANONICAL_SCHEMA_VERSION,
            "included": [CANONICAL_TEXT_PATH],
            "excluded_policy": "metadata_extensions_and_source_refs_are_not_language_content",
            "complete_structure_report": str(source_report_path),
        },
    }
    filtered_path = source_report_path.with_name("canonical_content_structure_report.json")
    write_json_atomic(filtered_path, filtered_report)
    handoff["evidence"] = {
        **dict(evidence),
        "complete_structure_report": str(source_report_path),
        "structure_report": str(filtered_path),
    }
    handoff["content_path_contract"] = {
        "schema_version": "canonical_content_path_contract_v0001",
        "included_paths": [CANONICAL_TEXT_PATH],
        "status": "completed",
    }
    write_json_atomic(handoff_path, handoff)
    return {
        **dict(result),
        "content_structure_report": str(filtered_path),
        "canonical_text_paths": [CANONICAL_TEXT_PATH],
    }


def execute(args: Any) -> Mapping[str, Any]:
    result = execute_pipeline(args)
    if args.dry_run:
        return result
    return _write_content_structure_handoff(result)


def discovery_contract() -> Mapping[str, Any]:
    return {
        "input_schema": CANONICAL_SCHEMA_VERSION,
        "provider_fields_required": [],
        "analyzer": "analyze_json_structure_v0003.py",
        "slicer": "coarse_slice_conversations_v0003.py",
        "language_content_paths": [CANONICAL_TEXT_PATH],
    }


# ============================================================
# CLI / main 接口区
# ============================================================


def build_parser() -> Any:
    parser = _build_parser_core()
    parser.description = "Prepare a complete canonical conversation ingress payload for admission."
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = execute(args)
    except Exception as exc:
        print(
            json.dumps(
                {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
