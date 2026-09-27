# ============================================================
# 文件名: structural_unit_governance_graph_pipeline_v0003.py
# 中文名: 自包含结构治理与关系图编排
# 版本号: v0003
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
# alias: structural_unit_governance_graph_pipeline
# family: structural_unit_governance_graph_pipeline
# role: action_structural_governance_orchestrator
# version: v0003
# status: active
# entry_point: scripts/orchestration/action/structural_unit_governance_graph_pipeline_v0003.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - aggregate_graph_statistics_v0002.py
#   - analyze_graph_statistics_v0002.py
#   - analyze_structural_adjacency_v0004.py
#   - analyze_structural_cooccurrence_v0004.py
#   - analyze_structural_hierarchy_v0004.py
#   - analyze_structural_statistics_v0003.py
#   - build_unit_adjacency_graph_v0003.py
#   - build_unit_cooccurrence_graph_v0003.py
#   - build_unit_hierarchy_graph_v0002.py
#   - decide_parse_eligibility_v0003.py
#   - decide_unit_prominence_v0004.py
#   - extract_frequent_units_v0003.py
#   - filter_structural_noise_v0003.py
#   - language_parse_lite_v0003.py
#   - normalize_text_units_v0002.py
#   - normalize_unit_variants_v0003.py
#   - profile_string_values_v0004.py
#   - profile_unit_structure_v0003.py
#   - register_structural_units_v0003.py
#   - validate_unit_boundaries_v0003.py
# used_by:
#   - data_action_chain_pipeline_v0015
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/orchestration/action/structural_unit_governance_graph_pipeline_v0001.py, scripts/orchestration/action/structural_unit_governance_graph_pipeline_v0002.py
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import importlib.util
from typing import Any, Dict, Mapping, Optional, Sequence

# ============================================================
# 全局常量与默认映射
# ============================================================

SCRIPT_FAMILY = "structural_unit_governance_graph_pipeline"


UPSTREAM_COMPLETION_FAMILY = "data_admission_lineage_pipeline"


UPSTREAM_COMPLETION_STATUS = "completed"


UPSTREAM_HANDOFF_FAMILY = "data_discovery_parse_preparation_pipeline"


UPSTREAM_HANDOFF_STATUS = "ready_for_admission"


DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "action"


DEFAULT_BUSINESS_ROOT = Path("actioning") / "pipelines" / "orchestration"


DEFAULT_STEP_TIMEOUT_SECONDS = 600


DEFAULT_MIN_OCCURRENCE = 3


WINDOWS_SAFE_PATH_LENGTH = 240


RUN_ID_PATTERN = re.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


PINNED_SCRIPTS: Mapping[str, str] = {
    "profile_strings": "profile_string_values_v0004.py",
    "decide_parse": "decide_parse_eligibility_v0003.py",
    "language_parse": "language_parse_lite_v0003.py",
    "normalize_text": "normalize_text_units_v0002.py",
    "normalize_variants": "normalize_unit_variants_v0003.py",
    "extract_frequent": "extract_frequent_units_v0003.py",
    "filter_noise": "filter_structural_noise_v0003.py",
    "profile_units": "profile_unit_structure_v0003.py",
    "validate_boundaries": "validate_unit_boundaries_v0003.py",
    "decide_prominence": "decide_unit_prominence_v0004.py",
    "register_units": "register_structural_units_v0003.py",
    "analyze_adjacency": "analyze_structural_adjacency_v0004.py",
    "analyze_cooccurrence": "analyze_structural_cooccurrence_v0004.py",
    "analyze_hierarchy": "analyze_structural_hierarchy_v0004.py",
    "analyze_structural_statistics": "analyze_structural_statistics_v0003.py",
    "build_adjacency": "build_unit_adjacency_graph_v0003.py",
    "build_cooccurrence": "build_unit_cooccurrence_graph_v0003.py",
    "analyze_graph_statistics": "analyze_graph_statistics_v0002.py",
    "aggregate_graph_statistics": "aggregate_graph_statistics_v0002.py",
}


PINNED_POLICIES: Mapping[str, Path] = {
    "parse_eligibility": Path("config/action/config/parse_eligibility_policy_v0002.yml"),
    "language_parse": Path("config/action/config/language_parse_lite_policy_v0002.yml"),
    "normalize_variants": Path("config/action/config/normalize_unit_variants_policy_v0001.yml"),
    "filter_noise": Path("config/action/config/filter_structural_noise_v0001.yml"),
    "validate_boundaries": Path("config/action/config/validate_unit_boundaries_v0001.yml"),
    "decide_prominence": Path("config/action/config/decide_unit_prominence_policy_v0001.yml"),
}


STEP_PLAN: Sequence[str] = (
    "profile_string_values",
    "decide_parse_eligibility",
    "language_parse_lite",
    "normalize_text_units",
    "normalize_unit_variants",
    "extract_frequent_units",
    "filter_structural_noise",
    "profile_unit_structure",
    "validate_unit_boundaries",
    "decide_unit_prominence",
    "register_structural_units",
    "analyze_structural_adjacency",
    "analyze_structural_cooccurrence",
    "analyze_structural_hierarchy_sidecar",
    "analyze_structural_statistics_sidecar",
    "build_unit_adjacency_graph",
    "build_unit_cooccurrence_graph",
    "analyze_adjacency_graph_statistics",
    "analyze_cooccurrence_graph_statistics",
    "aggregate_adjacency_graph_statistics",
    "aggregate_cooccurrence_graph_statistics",
)


SCRIPT_NAME = "structural_unit_governance_graph_pipeline_v0003.py"


SCRIPT_VERSION = "v0003"


DEFAULT_ENCODING = "utf-8"

# ============================================================
# 异常类型
# ============================================================


class PipelineError(RuntimeError):
    """Raised when the Action pipeline cannot continue safely."""


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


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
        stream.flush()


def load_json_file(path: Path, *, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise PipelineError(f"{label} does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"unable to read {label}: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PipelineError(f"{label} must contain a JSON object")
    return value


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


def parse_json_text(text: str, *, label: str) -> Dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise PipelineError(f"{label} is empty")
    try:
        value = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise PipelineError(f"{label} is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise PipelineError(f"{label} must contain a JSON object")
    return value


def require_fields(records: Sequence[Mapping[str, Any]], fields: Sequence[str], *, label: str) -> None:
    for index, record in enumerate(records, start=1):
        missing = [field for field in fields if field not in record]
        if missing:
            raise PipelineError(f"{label} row {index} is missing fields: {missing}")


def resolve_from_project(project_root: Path, value: str, *, label: str) -> Path:
    raw = Path(value)
    resolved = (raw if raw.is_absolute() else project_root / raw).resolve()
    if resolved.exists() and (not resolved.is_dir()):
        raise PipelineError(f"{label} is not a directory: {resolved}")
    return resolved


def resolve_file_from_project(project_root: Path, value: str, *, label: str) -> Path:
    raw = Path(value)
    resolved = (raw if raw.is_absolute() else project_root / raw).resolve()
    if not resolved.is_file():
        raise PipelineError(f"{label} does not exist: {resolved}")
    return resolved


def require_within(path: Path, root: Path, *, label: str, allow_equal: bool = True) -> Path:
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        relative = resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise PipelineError(f"{label} must be inside {root_resolved}: {resolved}") from exc
    if not allow_equal and relative == Path("."):
        raise PipelineError(f"{label} must not equal {root_resolved}")
    return resolved


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
            and (candidate / "scripts" / "action" / "pipelines").is_dir()
            and (candidate / "actioning").is_dir()
        ):
            return candidate
    if explicit:
        raise PipelineError(f"invalid project root: {Path(explicit).resolve()}")
    raise PipelineError("unable to locate project root from script path or current directory")


def validate_pinned_assets(project_root: Path) -> Tuple[Dict[str, Path], Dict[str, Path]]:
    scripts_dir = project_root / "scripts" / "action" / "pipelines"
    scripts: Dict[str, Path] = {}
    for key, filename in PINNED_SCRIPTS.items():
        path = (scripts_dir / filename).resolve()
        if not path.is_file():
            raise PipelineError(f"required pinned script is missing: {path}")
        scripts[key] = path
    policies: Dict[str, Path] = {}
    for key, relative_path in PINNED_POLICIES.items():
        path = (project_root / relative_path).resolve()
        if not path.is_file():
            raise PipelineError(f"required pinned policy is missing: {path}")
        policies[key] = path
    return (scripts, policies)


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def validate_planned_path_lengths(run: "PipelineRun") -> int:
    planned_paths = [
        run.run_dir / "captured_stdout" / "21_aggregate_cooccurrence_graph_statistics.txt",
        run.run_dir / "completion_manifest.json",
        run.business_run_dir
        / "15_structural_statistics"
        / "v0003"
        / run.run_id
        / "structural_statistics_summary.json",
        run.business_run_dir
        / "19_aggregated_graph_statistics"
        / "unit_cooccurrence"
        / "_run_meta"
        / f"aggregate_{run.run_id}.json",
    ]
    maximum = max((len(str(path)) for path in planned_paths))
    if os.name == "nt" and maximum > WINDOWS_SAFE_PATH_LENGTH:
        raise PipelineError(
            f"planned Action pipeline path is too long for reliable child-script execution ({maximum} > {WINDOWS_SAFE_PATH_LENGTH}); use shorter --output-root, --business-root, or --run-id"
        )
    return maximum


def resolve_manifest_path(
    manifest: Mapping[str, Any], dotted_path: Sequence[str], *, project_root: Path, label: str
) -> Path:
    value: Any = manifest
    for key in dotted_path:
        if not isinstance(value, Mapping) or key not in value:
            raise PipelineError(f"manifest is missing {'.'.join(dotted_path)}")
        value = value[key]
    if not isinstance(value, str) or not value.strip():
        raise PipelineError(f"manifest field {'.'.join(dotted_path)} must be a path")
    raw = Path(value)
    resolved = (raw if raw.is_absolute() else project_root / raw).resolve()
    require_within(resolved, project_root, label=label)
    if not resolved.is_file():
        raise PipelineError(f"{label} is not a file: {resolved}")
    return resolved


class PipelineRun:

    def __init__(
        self,
        *,
        project_root: Path,
        output_root: Path,
        business_root: Path,
        run_id: str,
        completion_manifest: Path,
        test_mode: bool,
        scripts: Mapping[str, Path],
        policies: Mapping[str, Path],
        step_timeout: int,
    ) -> None:
        self.project_root = project_root
        self.output_root = output_root
        self.business_root = business_root
        self.run_id = run_id
        self.completion_manifest = completion_manifest
        self.test_mode = test_mode
        self.scripts = dict(scripts)
        self.policies = dict(policies)
        self.step_timeout = step_timeout
        self.run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
        self.business_run_dir = business_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
        self.stdout_dir = self.run_dir / "captured_stdout"
        self.stderr_dir = self.run_dir / "captured_stderr"
        self.step_status_path = self.run_dir / "step_status.jsonl"
        self.manifest_path = self.run_dir / "run_manifest.json"
        self.steps: List[Dict[str, Any]] = []
        self.manifest: Dict[str, Any] = {
            "schema_version": "structural_unit_governance_graph_run_v0001",
            "pipeline": {"script": SCRIPT_NAME, "family": SCRIPT_FAMILY, "version": SCRIPT_VERSION},
            "run_id": run_id,
            "status": "running",
            "started_at": utc_now_z(),
            "finished_at": None,
            "test_mode": test_mode,
            "project_root": str(project_root),
            "output_root": str(output_root),
            "business_root": str(business_root),
            "run_dir": str(self.run_dir),
            "business_run_dir": str(self.business_run_dir),
            "prerequisite_completion_manifest": str(completion_manifest),
            "pinned_scripts": dict(PINNED_SCRIPTS),
            "pinned_policies": {key: str(path) for key, path in self.policies.items()},
            "steps": self.steps,
            "error": None,
        }

    def initialize(self) -> None:
        if self.run_dir.exists():
            raise PipelineError(f"run directory already exists: {self.run_dir}")
        if self.business_run_dir.exists():
            raise PipelineError(f"business run directory already exists: {self.business_run_dir}")
        self.stdout_dir.mkdir(parents=True, exist_ok=False)
        self.stderr_dir.mkdir(parents=True, exist_ok=False)
        self.business_run_dir.mkdir(parents=True, exist_ok=False)
        write_json_atomic(self.manifest_path, self.manifest)

    def persist(self) -> None:
        write_json_atomic(self.manifest_path, self.manifest)

    def run_step(self, name: str, script_key: str, args: Sequence[str]) -> str:
        script_path = self.scripts[script_key]
        command = [sys.executable, str(script_path), *[str(value) for value in args]]
        index = len(self.steps) + 1
        stdout_path = self.stdout_dir / f"{index:02d}_{name}.txt"
        stderr_path = self.stderr_dir / f"{index:02d}_{name}.txt"
        started_at = utc_now_z()
        started_clock = time.monotonic()
        environment = os.environ.copy()
        environment["PYTHONUTF8"] = "1"
        try:
            result = subprocess.run(
                command,
                cwd=self.project_root,
                env=environment,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.step_timeout,
                check=False,
                shell=False,
            )
            stdout = result.stdout or ""
            stderr = result.stderr or ""
            returncode = int(result.returncode)
            timed_out = False
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout if isinstance(exc.stdout, str) else ""
            stderr = exc.stderr if isinstance(exc.stderr, str) else ""
            returncode = -1
            timed_out = True
        duration_ms = int((time.monotonic() - started_clock) * 1000)
        write_text_atomic(stdout_path, stdout)
        write_text_atomic(stderr_path, stderr)
        step_record = {
            "index": index,
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
        self.persist()
        if timed_out:
            raise PipelineError(f"step {name} exceeded timeout of {self.step_timeout} seconds")
        if returncode != 0:
            detail = stderr.strip() or stdout.strip() or "no process output"
            raise PipelineError(f"step {name} failed with exit code {returncode}: {detail}")
        return stdout

    def finish_success(self, completion_path: Path) -> None:
        self.manifest["status"] = "completed"
        self.manifest["finished_at"] = utc_now_z()
        self.manifest["completion_manifest"] = str(completion_path)
        self.persist()

    def finish_failure(self, error: Exception) -> None:
        self.manifest["status"] = "failed"
        self.manifest["finished_at"] = utc_now_z()
        self.manifest["error"] = {"type": type(error).__name__, "message": str(error)}
        self.persist()
        write_json_atomic(
            self.run_dir / "failure.json",
            {
                "run_id": self.run_id,
                "status": "failed",
                "failed_at": utc_now_z(),
                "error": self.manifest["error"],
                "completed_steps": len([step for step in self.steps if step.get("status") == "success"]),
                "side_effects_may_exist": len(self.steps) > 0,
            },
        )


def validate_status_stdout(stdout: str, *, label: str) -> Dict[str, Any]:
    summary = parse_json_text(stdout, label=f"{label} stdout")
    if summary.get("status") not in ("ok", "completed"):
        raise PipelineError(f"{label} returned unexpected status: {summary.get('status')}")
    return summary


def validate_graph(path: Path, *, expected_type: str) -> Dict[str, Any]:
    graph = load_json_file(path, label=f"{expected_type} graph")
    if graph.get("graph_type") != expected_type:
        raise PipelineError(f"graph type mismatch: expected {expected_type}, got {graph.get('graph_type')}")
    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise PipelineError(f"{expected_type} graph must contain node and edge lists")
    node_ids: set[str] = set()
    for index, node in enumerate(nodes, start=1):
        if not isinstance(node, Mapping):
            raise PipelineError(f"{expected_type} graph node {index} must be an object")
        node_id = node.get("id")
        if not isinstance(node_id, str) or not node_id:
            raise PipelineError(f"{expected_type} graph node {index} is missing id")
        node_ids.add(node_id)
    missing_endpoints: set[str] = set()
    for index, edge in enumerate(edges, start=1):
        if not isinstance(edge, Mapping):
            raise PipelineError(f"{expected_type} graph edge {index} must be an object")
        u = edge.get("u")
        v = edge.get("v")
        if not isinstance(u, str) or not isinstance(v, str) or (not u) or (not v):
            raise PipelineError(f"{expected_type} graph edge {index} is missing endpoints")
        if u not in node_ids:
            missing_endpoints.add(u)
        if v not in node_ids:
            missing_endpoints.add(v)
    if missing_endpoints:
        sample = sorted(missing_endpoints)[:10]
        raise PipelineError(f"{expected_type} graph has edge endpoints absent from node ids: {sample}")
    return {
        "path": str(path),
        "graph_type": expected_type,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "referential_integrity": True,
    }


def validate_statistics(path: Path, *, expected_type: str) -> Dict[str, Any]:
    record = load_json_file(path, label=f"{expected_type} graph statistics")
    if record.get("graph_type") != expected_type:
        raise PipelineError(f"statistics graph type mismatch for {expected_type}")
    statistics = record.get("statistics")
    if not isinstance(statistics, Mapping):
        raise PipelineError(f"{expected_type} graph statistics are missing statistics")
    return record


def validate_aggregate(path: Path, *, expected_type: str) -> Dict[str, Any]:
    record = load_json_file(path, label=f"{expected_type} aggregated statistics")
    if record.get("graph_type") != expected_type:
        raise PipelineError(f"aggregated graph type mismatch for {expected_type}")
    if record.get("total_snapshots") != 1:
        raise PipelineError(f"{expected_type} aggregate must use exactly one snapshot in this run")
    return record


def execute_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = find_project_root(args.project_root)
    scripts, policies = validate_pinned_assets(project_root)
    completion_path = resolve_file_from_project(
        project_root, args.completion_manifest, label="prerequisite completion manifest"
    )
    require_within(completion_path, project_root, label="prerequisite completion manifest")
    upstream = validate_completed_upstream(completion_path, project_root=project_root)
    output_root = resolve_from_project(project_root, args.output_root, label="output root")
    business_root = resolve_from_project(project_root, args.business_root, label="business root")
    require_within(output_root, project_root, label="output root")
    require_within(business_root, project_root, label="business root")
    if args.test_mode:
        test_root = (project_root / "temp").resolve()
        require_within(output_root, test_root, label="test output root", allow_equal=False)
        require_within(business_root, test_root, label="test business root", allow_equal=False)
    else:
        require_within(
            output_root,
            project_root / "scripts" / "orchestration" / "outputs" / "action",
            label="formal output root",
        )
        require_within(business_root, project_root / "actioning" / "pipelines", label="formal business root")
    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError("run_id contains unsupported characters")
    run = PipelineRun(
        project_root=project_root,
        output_root=output_root,
        business_root=business_root,
        run_id=run_id,
        completion_manifest=completion_path,
        test_mode=bool(args.test_mode),
        scripts=scripts,
        policies=policies,
        step_timeout=args.step_timeout,
    )
    maximum_planned_path_length = validate_planned_path_lengths(run)
    plan = {
        "pipeline": SCRIPT_FAMILY,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "status": "dry-run" if args.dry_run else "planned",
        "test_mode": bool(args.test_mode),
        "prerequisite": {
            "completion_manifest": str(completion_path),
            "completion_run_id": upstream["completion_run_id"],
            "handoff_manifest": str(upstream["handoff_manifest"]),
            "handoff_run_id": upstream["handoff_run_id"],
            "state_transition": "data_admission_lineage.completed -> action_structural_pipeline.allowed",
        },
        "inputs": {
            "working_copy": str(upstream["working_copy"]),
            "structure_report": str(upstream["structure_report"]),
        },
        "output_root": str(output_root),
        "business_root": str(business_root),
        "run_dir": str(run.run_dir),
        "business_run_dir": str(run.business_run_dir),
        "maximum_planned_path_length": maximum_planned_path_length,
        "steps": list(STEP_PLAN),
        "excluded": [
            "build_unit_hierarchy_graph_v0002.py",
            "database synchronization",
            "total orchestration state persistence",
        ],
    }
    if args.dry_run:
        return plan
    if not args.confirm_execution:
        raise PipelineError("actual Action pipeline execution requires --confirm-execution")
    run.initialize()
    paths = {
        "observations": run.business_run_dir / "01_observations" / "observations.jsonl",
        "eligibility": run.business_run_dir / "02_eligibility" / "eligibility_decisions.jsonl",
        "language_dir": run.business_run_dir / "03_language_parse",
        "language_logs": run.business_run_dir / "03_language_parse" / "logs",
        "normalized": run.business_run_dir / "04_normalized" / "normalized_text_units.jsonl",
        "normalized_meta": run.business_run_dir / "04_normalized" / "run_meta.json",
        "variants": run.business_run_dir / "05_variants" / "unit_variants_normalized.jsonl",
        "variants_meta": run.business_run_dir / "05_variants" / "run_meta.json",
        "frequent": run.business_run_dir / "06_frequent" / "frequent_units.jsonl",
        "frequent_meta": run.business_run_dir / "06_frequent" / "run_meta.json",
        "noise": run.business_run_dir / "07_noise" / "unit_structural_noise_decisions.jsonl",
        "noise_meta": run.business_run_dir / "07_noise" / "run_meta.json",
        "profiles": run.business_run_dir / "08_profiles" / "unit_structure_profiles.jsonl",
        "profiles_meta": run.business_run_dir / "08_profiles" / "run_meta.json",
        "boundaries": run.business_run_dir / "09_boundaries" / "unit_boundary_validation.jsonl",
        "boundaries_meta": run.business_run_dir / "09_boundaries" / "run_meta.json",
        "prominence": run.business_run_dir / "10_prominence" / "unit_prominence_decisions.jsonl",
        "prominence_meta": run.business_run_dir / "10_prominence" / "run_meta.json",
        "registry_dir": run.business_run_dir / "11_registry",
        "registry": run.business_run_dir / "11_registry" / "registered_structural_units_v0002.jsonl",
        "adjacency": run.business_run_dir / "12_adjacency_facts" / "structural_adjacency.jsonl",
        "adjacency_meta": run.business_run_dir / "12_adjacency_facts" / "run_meta.json",
        "cooccurrence": run.business_run_dir / "13_cooccurrence_facts" / "structural_cooccurrence.jsonl",
        "cooccurrence_meta": run.business_run_dir / "13_cooccurrence_facts" / "run_meta.json",
        "hierarchy": run.business_run_dir / "14_hierarchy_sidecar" / "structural_hierarchy.jsonl",
        "hierarchy_meta": run.business_run_dir / "14_hierarchy_sidecar" / "run_meta.json",
        "structural_stats_root": run.business_run_dir / "15_structural_statistics",
        "adjacency_graph_root": run.business_run_dir / "16_adjacency_graph",
        "cooccurrence_graph_root": run.business_run_dir / "17_cooccurrence_graph",
        "graph_stats_root": run.business_run_dir / "18_graph_statistics",
        "aggregates_root": run.business_run_dir / "19_aggregated_graph_statistics",
    }
    try:
        stdout = run.run_step(
            "profile_string_values",
            "profile_strings",
            [
                "--structure-report",
                str(upstream["structure_report"]),
                "--data",
                str(upstream["working_copy"]),
                "--output",
                str(paths["observations"]),
            ],
        )
        validate_status_stdout(stdout, label="profile_string_values")
        observations = load_jsonl_file(paths["observations"], label="string observations")
        require_fields(observations, ("asset_id", "path", "observations"), label="string observations")
        if not observations:
            raise PipelineError("string observation output must not be empty")
        stdout = run.run_step(
            "decide_parse_eligibility",
            "decide_parse",
            [
                "--observations",
                str(paths["observations"]),
                "--policy",
                str(policies["parse_eligibility"]),
                "--output",
                str(paths["eligibility"]),
            ],
        )
        validate_status_stdout(stdout, label="decide_parse_eligibility")
        eligibility = load_jsonl_file(paths["eligibility"], label="parse eligibility decisions")
        require_fields(eligibility, ("asset_id", "path", "decision"), label="parse eligibility decisions")
        if len(eligibility) != len(observations):
            raise PipelineError("eligibility decision count does not match observations")
        invalid_decisions = sorted(
            {
                str(record.get("decision"))
                for record in eligibility
                if record.get("decision") not in ("ALLOW", "DELAY", "FREEZE")
            }
        )
        if invalid_decisions:
            raise PipelineError(f"invalid parse eligibility decisions: {invalid_decisions}")
        stdout = run.run_step(
            "language_parse_lite",
            "language_parse",
            [
                "--decisions",
                str(paths["eligibility"]),
                "--policy",
                str(policies["language_parse"]),
                "--data-input",
                str(upstream["working_copy"]),
                "--output-dir",
                str(paths["language_dir"]),
                "--log-dir",
                str(paths["language_logs"]),
            ],
        )
        language_summary = validate_status_stdout(stdout, label="language_parse_lite")
        language_output_value = language_summary.get("output")
        if not isinstance(language_output_value, str) or not language_output_value:
            raise PipelineError("language_parse_lite stdout is missing output path")
        language_output = Path(language_output_value).resolve()
        require_within(language_output, paths["language_dir"], label="language parse output")
        language_rows = load_jsonl_file(language_output, label="language text units")
        run.run_step(
            "normalize_text_units",
            "normalize_text",
            [
                "--input",
                str(language_output),
                "--output",
                str(paths["normalized"]),
                "--run-meta",
                str(paths["normalized_meta"]),
            ],
        )
        normalized = load_jsonl_file(paths["normalized"], label="normalized text units")
        require_fields(
            normalized,
            ("unit_id", "unit_text", "asset_id", "path", "char_start", "char_end"),
            label="normalized text units",
        )
        if len(normalized) > len(language_rows):
            raise PipelineError("normalized unit count exceeds language parser output count")
        normalized_ids = [str(record["unit_id"]) for record in normalized]
        if len(normalized_ids) != len(set(normalized_ids)):
            raise PipelineError("normalized text units contain duplicate unit_id values")
        stdout = run.run_step(
            "normalize_unit_variants",
            "normalize_variants",
            [
                "--input",
                str(paths["normalized"]),
                "--output",
                str(paths["variants"]),
                "--rules",
                str(policies["normalize_variants"]),
                "--run-meta",
                str(paths["variants_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="normalize_unit_variants")
        variants = load_jsonl_file(paths["variants"], label="normalized unit variants")
        require_fields(
            variants,
            ("unit_id", "unit_text", "normalized_unit_text", "variant_group_id"),
            label="normalized unit variants",
        )
        if len(variants) != len(normalized):
            raise PipelineError("variant normalization must preserve normalized row count")
        stdout = run.run_step(
            "extract_frequent_units",
            "extract_frequent",
            [
                "--input",
                str(paths["normalized"]),
                "--output",
                str(paths["frequent"]),
                "--run-meta",
                str(paths["frequent_meta"]),
                "--min-occurrence",
                str(args.min_occurrence),
            ],
        )
        validate_status_stdout(stdout, label="extract_frequent_units")
        frequent = load_jsonl_file(paths["frequent"], label="frequent unit candidates")
        require_fields(frequent, ("unit_text", "occurrence_count"), label="frequent unit candidates")
        stdout = run.run_step(
            "filter_structural_noise",
            "filter_noise",
            [
                "--input",
                str(paths["frequent"]),
                "--output",
                str(paths["noise"]),
                "--rules",
                str(policies["filter_noise"]),
                "--run-meta",
                str(paths["noise_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="filter_structural_noise")
        noise = load_jsonl_file(paths["noise"], label="structural noise decisions")
        require_fields(noise, ("unit_text", "noise_decision"), label="structural noise decisions")
        if len(noise) != len(frequent):
            raise PipelineError("noise decision count does not match frequent candidates")
        stdout = run.run_step(
            "profile_unit_structure",
            "profile_units",
            [
                "--frequent",
                str(paths["frequent"]),
                "--normalized",
                str(paths["normalized"]),
                "--output",
                str(paths["profiles"]),
                "--run-meta",
                str(paths["profiles_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="profile_unit_structure")
        profiles = load_jsonl_file(paths["profiles"], label="unit structure profiles")
        require_fields(profiles, ("unit_text",), label="unit structure profiles")
        if len(profiles) != len(frequent):
            raise PipelineError("unit profile count does not match frequent candidates")
        stdout = run.run_step(
            "validate_unit_boundaries",
            "validate_boundaries",
            [
                "--noise-decisions",
                str(paths["noise"]),
                "--profiles",
                str(paths["profiles"]),
                "--instance-dir",
                str(paths["variants"].parent),
                "--output",
                str(paths["boundaries"]),
                "--rules",
                str(policies["validate_boundaries"]),
                "--run-meta",
                str(paths["boundaries_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="validate_unit_boundaries")
        boundaries = load_jsonl_file(paths["boundaries"], label="unit boundary validation")
        require_fields(boundaries, ("unit_text", "boundary_status"), label="unit boundary validation")
        if len(boundaries) > len(profiles):
            raise PipelineError("boundary validation count exceeds profile count")
        stdout = run.run_step(
            "decide_unit_prominence",
            "decide_prominence",
            [
                "--profiles",
                str(paths["profiles"]),
                "--boundaries",
                str(paths["boundaries"]),
                "--policy",
                str(policies["decide_prominence"]),
                "--output",
                str(paths["prominence"]),
                "--run-meta",
                str(paths["prominence_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="decide_unit_prominence")
        prominence = load_jsonl_file(paths["prominence"], label="unit prominence decisions")
        require_fields(prominence, ("unit_text", "decision"), label="unit prominence decisions")
        if len(prominence) != len(profiles):
            raise PipelineError("prominence decision count does not match profile count")
        stdout = run.run_step(
            "register_structural_units",
            "register_units",
            ["--input", str(paths["prominence"]), "--output-dir", str(paths["registry_dir"])],
        )
        registry_summary = validate_status_stdout(stdout, label="register_structural_units")
        registry = load_jsonl_file(paths["registry"], label="registered structural units")
        require_fields(
            registry, ("unit_id", "structural_unit_id", "unit_text"), label="registered structural units"
        )
        allowed_texts = {
            str(record["unit_text"]).strip()
            for record in prominence
            if record.get("decision") == "ALLOW" and isinstance(record.get("unit_text"), str)
        }
        registry_texts = {str(record["unit_text"]).strip() for record in registry}
        if not registry_texts.issubset(allowed_texts):
            raise PipelineError("registry contains units not authorized by ALLOW decisions")
        stdout = run.run_step(
            "analyze_structural_adjacency",
            "analyze_adjacency",
            [
                "--input",
                str(paths["normalized"]),
                "--output",
                str(paths["adjacency"]),
                "--run-meta",
                str(paths["adjacency_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="analyze_structural_adjacency")
        adjacency = load_jsonl_file(paths["adjacency"], label="structural adjacency facts")
        require_fields(
            adjacency,
            ("unit_id_from", "unit_id_to", "unit_text_from", "unit_text_to"),
            label="structural adjacency facts",
        )
        stdout = run.run_step(
            "analyze_structural_cooccurrence",
            "analyze_cooccurrence",
            [
                "--input",
                str(paths["normalized"]),
                "--output",
                str(paths["cooccurrence"]),
                "--run-meta",
                str(paths["cooccurrence_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="analyze_structural_cooccurrence")
        cooccurrence = load_jsonl_file(paths["cooccurrence"], label="structural cooccurrence facts")
        require_fields(
            cooccurrence,
            ("unit_id_a", "unit_id_b", "unit_text_a", "unit_text_b"),
            label="structural cooccurrence facts",
        )
        stdout = run.run_step(
            "analyze_structural_hierarchy_sidecar",
            "analyze_hierarchy",
            [
                "--input",
                str(paths["normalized"]),
                "--output",
                str(paths["hierarchy"]),
                "--run-meta",
                str(paths["hierarchy_meta"]),
            ],
        )
        validate_status_stdout(stdout, label="analyze_structural_hierarchy")
        hierarchy = load_jsonl_file(paths["hierarchy"], label="message hierarchy sidecar")
        if len(hierarchy) != len(normalized):
            raise PipelineError("message hierarchy sidecar must preserve normalized row count")
        run.run_step(
            "analyze_structural_statistics_sidecar",
            "analyze_structural_statistics",
            [
                "--input",
                str(paths["normalized"]),
                "--output-root",
                str(paths["structural_stats_root"]),
                "--version",
                "v0003",
                "--run-id",
                run_id,
            ],
        )
        structural_stats_dir = paths["structural_stats_root"] / "v0003" / run_id
        structural_summary_path = structural_stats_dir / "structural_statistics_summary.json"
        structural_summary = load_json_file(structural_summary_path, label="structural statistics summary")
        if structural_summary.get("total_units") != len(normalized):
            raise PipelineError("structural statistics total_units does not match normalized units")
        stdout = run.run_step(
            "build_unit_adjacency_graph",
            "build_adjacency",
            [
                "--register",
                str(paths["registry"]),
                "--adjacency",
                str(paths["adjacency"]),
                "--output-root",
                str(paths["adjacency_graph_root"]),
                "--run-meta",
                str(paths["adjacency_graph_root"] / "_run_meta" / f"run_{run_id}.json"),
            ],
        )
        adjacency_graph_summary = validate_status_stdout(stdout, label="build_unit_adjacency_graph")
        adjacency_graph_path = Path(str(adjacency_graph_summary.get("output_graph", ""))).resolve()
        require_within(adjacency_graph_path, paths["adjacency_graph_root"], label="adjacency graph")
        adjacency_graph = validate_graph(adjacency_graph_path, expected_type="unit_adjacency")
        stdout = run.run_step(
            "build_unit_cooccurrence_graph",
            "build_cooccurrence",
            [
                "--register",
                str(paths["registry"]),
                "--cooccurrence",
                str(paths["cooccurrence"]),
                "--output-root",
                str(paths["cooccurrence_graph_root"]),
                "--run-meta",
                str(paths["cooccurrence_graph_root"] / "_run_meta" / f"run_{run_id}.json"),
            ],
        )
        cooccurrence_graph_summary = validate_status_stdout(stdout, label="build_unit_cooccurrence_graph")
        cooccurrence_graph_path = Path(str(cooccurrence_graph_summary.get("output_graph", ""))).resolve()
        require_within(cooccurrence_graph_path, paths["cooccurrence_graph_root"], label="cooccurrence graph")
        cooccurrence_graph = validate_graph(cooccurrence_graph_path, expected_type="unit_cooccurrence")
        stdout = run.run_step(
            "analyze_adjacency_graph_statistics",
            "analyze_graph_statistics",
            [
                "--graph",
                str(adjacency_graph_path),
                "--output-root",
                str(paths["graph_stats_root"]),
                "--run-meta",
                str(paths["graph_stats_root"] / "unit_adjacency" / "_run_meta" / f"run_{run_id}.json"),
            ],
        )
        adjacency_stats_summary = validate_status_stdout(stdout, label="analyze adjacency graph statistics")
        adjacency_stats_path = Path(str(adjacency_stats_summary.get("statistics", ""))).resolve()
        validate_statistics(adjacency_stats_path, expected_type="unit_adjacency")
        stdout = run.run_step(
            "analyze_cooccurrence_graph_statistics",
            "analyze_graph_statistics",
            [
                "--graph",
                str(cooccurrence_graph_path),
                "--output-root",
                str(paths["graph_stats_root"]),
                "--run-meta",
                str(paths["graph_stats_root"] / "unit_cooccurrence" / "_run_meta" / f"run_{run_id}.json"),
            ],
        )
        cooccurrence_stats_summary = validate_status_stdout(
            stdout, label="analyze cooccurrence graph statistics"
        )
        cooccurrence_stats_path = Path(str(cooccurrence_stats_summary.get("statistics", ""))).resolve()
        validate_statistics(cooccurrence_stats_path, expected_type="unit_cooccurrence")
        stdout = run.run_step(
            "aggregate_adjacency_graph_statistics",
            "aggregate_graph_statistics",
            [
                "--input-dir",
                str(adjacency_stats_path.parent),
                "--output-root",
                str(paths["aggregates_root"]),
                "--graph-type",
                "unit_adjacency",
                "--run-meta",
                str(paths["aggregates_root"] / "unit_adjacency" / "_run_meta" / f"aggregate_{run_id}.json"),
            ],
        )
        adjacency_aggregate_summary = validate_status_stdout(
            stdout, label="aggregate adjacency graph statistics"
        )
        adjacency_aggregate_path = Path(str(adjacency_aggregate_summary.get("output_current", ""))).resolve()
        adjacency_aggregate = validate_aggregate(adjacency_aggregate_path, expected_type="unit_adjacency")
        stdout = run.run_step(
            "aggregate_cooccurrence_graph_statistics",
            "aggregate_graph_statistics",
            [
                "--input-dir",
                str(cooccurrence_stats_path.parent),
                "--output-root",
                str(paths["aggregates_root"]),
                "--graph-type",
                "unit_cooccurrence",
                "--run-meta",
                str(
                    paths["aggregates_root"] / "unit_cooccurrence" / "_run_meta" / f"aggregate_{run_id}.json"
                ),
            ],
        )
        cooccurrence_aggregate_summary = validate_status_stdout(
            stdout, label="aggregate cooccurrence graph statistics"
        )
        cooccurrence_aggregate_path = Path(
            str(cooccurrence_aggregate_summary.get("output_current", ""))
        ).resolve()
        cooccurrence_aggregate = validate_aggregate(
            cooccurrence_aggregate_path, expected_type="unit_cooccurrence"
        )
        completion = {
            "schema_version": "structural_unit_governance_graph_completion_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "completed_at": utc_now_z(),
            "test_mode": bool(args.test_mode),
            "prerequisite": {
                "completion_pipeline": UPSTREAM_COMPLETION_FAMILY,
                "completion_run_id": upstream["completion_run_id"],
                "completion_manifest": str(completion_path),
                "handoff_run_id": upstream["handoff_run_id"],
                "handoff_manifest": str(upstream["handoff_manifest"]),
                "state_transition": "data_admission_lineage.completed -> action_structural_pipeline.completed",
            },
            "source_inputs": {
                "working_copy": str(upstream["working_copy"]),
                "structure_report": str(upstream["structure_report"]),
                "selection_reason": "the structure report directly describes this working copy",
            },
            "counts": {
                "observations": len(observations),
                "eligibility_decisions": len(eligibility),
                "language_rows": len(language_rows),
                "normalized_units": len(normalized),
                "variant_rows": len(variants),
                "frequent_candidates": len(frequent),
                "noise_decisions": len(noise),
                "unit_profiles": len(profiles),
                "boundary_validations": len(boundaries),
                "prominence_decisions": len(prominence),
                "registered_units": len(registry),
                "adjacency_facts": len(adjacency),
                "cooccurrence_facts": len(cooccurrence),
                "hierarchy_rows": len(hierarchy),
            },
            "governance": {
                "registry": str(paths["registry"]),
                "registry_status": registry_summary.get("status"),
                "registry_subset_of_allow": True,
            },
            "sidecars": {
                "structural_statistics": str(structural_summary_path),
                "message_hierarchy": str(paths["hierarchy"]),
                "message_hierarchy_usage": "not_used_for_structural_unit_hierarchy_graph",
            },
            "graphs": {
                "adjacency": adjacency_graph,
                "cooccurrence": cooccurrence_graph,
                "known_interface_guard": "non-empty graphs fail if edge endpoints are absent from graph node ids",
            },
            "graph_statistics": {
                "adjacency_snapshot": str(adjacency_stats_path),
                "cooccurrence_snapshot": str(cooccurrence_stats_path),
                "adjacency_aggregate": str(adjacency_aggregate_path),
                "cooccurrence_aggregate": str(cooccurrence_aggregate_path),
                "adjacency_total_snapshots": adjacency_aggregate.get("total_snapshots"),
                "cooccurrence_total_snapshots": cooccurrence_aggregate.get("total_snapshots"),
            },
            "business_run_dir": str(run.business_run_dir),
            "excluded": {
                "structural_unit_hierarchy_graph": "message-node and registered-unit identities are not aligned",
                "database_sync": True,
                "action_db_registration": True,
            },
        }
        completion_output = run.run_dir / "completion_manifest.json"
        write_json_atomic(completion_output, completion)
        run.finish_success(completion_output)
        return {
            "pipeline": SCRIPT_FAMILY,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "test_mode": bool(args.test_mode),
            "run_dir": str(run.run_dir),
            "business_run_dir": str(run.business_run_dir),
            "completion_manifest": str(completion_output),
            "registered_units": len(registry),
            "adjacency_graph": adjacency_graph,
            "cooccurrence_graph": cooccurrence_graph,
        }
    except Exception as exc:
        run.finish_failure(exc)
        raise


def _build_parser_core() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reliably orchestrate Action structural-unit governance, relation graphs, and graph statistics after the data admission stage is completed."
    )
    parser.add_argument("--project-root", default="", help="Optional project root override.")
    parser.add_argument(
        "--completion-manifest",
        required=True,
        help="Exact completed data_admission_lineage_pipeline completion_manifest.json.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Orchestration evidence root, relative to project root unless absolute.",
    )
    parser.add_argument(
        "--business-root",
        default=str(DEFAULT_BUSINESS_ROOT),
        help="Action business artifact root, relative to project root unless absolute.",
    )
    parser.add_argument("--run-id", default="", help="Optional safe, unique run id.")
    parser.add_argument(
        "--step-timeout",
        type=positive_integer,
        default=DEFAULT_STEP_TIMEOUT_SECONDS,
        help="Timeout in seconds for each child script.",
    )
    parser.add_argument(
        "--min-occurrence",
        type=positive_integer,
        default=DEFAULT_MIN_OCCURRENCE,
        help="Minimum exact unit_text occurrence count for candidate extraction.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Require orchestration evidence and business roots to stay under project temp.",
    )
    parser.add_argument(
        "--confirm-execution",
        action="store_true",
        help="Explicitly authorize actual Action pipeline business artifact generation.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the prerequisite and print a plan without creating outputs.",
    )
    return parser


def validate_completed_upstream(completion_path: Path, *, project_root: Path) -> Dict[str, Any]:
    completion = load_json_file(completion_path, label="upstream completion manifest")
    if completion.get("pipeline") != UPSTREAM_COMPLETION_FAMILY:
        raise PipelineError(f"unexpected prerequisite pipeline: {completion.get('pipeline')}")
    if completion.get("status") != "completed":
        raise PipelineError("prerequisite orchestration status must be completed")
    if completion.get("readiness") != "ready_for_action":
        raise PipelineError("prerequisite readiness must be ready_for_action")
    completion_run_id = completion.get("run_id")
    if not isinstance(completion_run_id, str) or not completion_run_id:
        raise PipelineError("upstream completion manifest is missing run_id")
    upstream_record = completion.get("upstream")
    if not isinstance(upstream_record, Mapping):
        raise PipelineError("upstream completion manifest is missing upstream record")
    handoff_path = resolve_manifest_path(
        completion,
        ("upstream", "handoff_manifest"),
        project_root=project_root,
        label="data preparation handoff manifest",
    )
    handoff = load_json_file(handoff_path, label="data preparation handoff manifest")
    if handoff.get("pipeline") != UPSTREAM_HANDOFF_FAMILY:
        raise PipelineError(f"unexpected handoff pipeline: {handoff.get('pipeline')}")
    if handoff.get("status") != UPSTREAM_HANDOFF_STATUS:
        raise PipelineError(f"handoff status must be {UPSTREAM_HANDOFF_STATUS}")
    handoff_run_id = handoff.get("run_id")
    if not isinstance(handoff_run_id, str) or not handoff_run_id:
        raise PipelineError("data preparation handoff is missing run_id")
    if upstream_record.get("run_id") != handoff_run_id:
        raise PipelineError("completion manifest and handoff run_id do not match")
    working_copy = resolve_manifest_path(
        handoff, ("evidence", "working_copy"), project_root=project_root, label="upstream working copy"
    )
    structure_report = resolve_manifest_path(
        handoff,
        ("evidence", "structure_report"),
        project_root=project_root,
        label="upstream structure report",
    )
    asset_manifest = resolve_manifest_path(
        completion,
        ("admission", "asset_manifest"),
        project_root=project_root,
        label="admitted asset manifest",
    )
    lineage_record = resolve_manifest_path(
        completion, ("lineage", "record_path"), project_root=project_root, label="Action lineage record"
    )
    inventory_snapshot = resolve_manifest_path(
        completion,
        ("inventory", "snapshot"),
        project_root=project_root,
        label="post-admission inventory snapshot",
    )
    structure = load_json_file(structure_report, label="upstream structure report")
    if structure.get("parse_status") != "success" or structure.get("truncated") is True:
        raise PipelineError("upstream structure report is not a complete successful parse")
    lineage = load_json_file(lineage_record, label="Action lineage record")
    if lineage.get("schema_version") != "action_lineage_record_v0002":
        raise PipelineError("Action lineage record schema must be v0002")
    if lineage.get("status") != "ready_for_action":
        raise PipelineError("Action lineage record must be ready_for_action")
    lineage_summary = completion.get("lineage")
    if (
        not isinstance(lineage_summary, Mapping)
        or lineage_summary.get("type") != "action"
        or lineage_summary.get("status") != "ready_for_action"
    ):
        raise PipelineError("completion manifest does not declare ready Action lineage")
    if completion.get("inventory", {}).get("required_paths_observed") is not True:
        raise PipelineError("prerequisite inventory did not observe all required paths")
    slice_count = upstream_record.get("slice_count")
    admitted_count = completion.get("admission", {}).get("admitted_count")
    if not isinstance(slice_count, int) or slice_count < 1:
        raise PipelineError("completion manifest slice_count must be positive")
    if admitted_count != slice_count:
        raise PipelineError("admitted_count does not match upstream slice_count")
    return {
        "completion": completion,
        "completion_run_id": completion_run_id,
        "completion_manifest": completion_path,
        "handoff": handoff,
        "handoff_run_id": handoff_run_id,
        "handoff_manifest": handoff_path,
        "working_copy": working_copy,
        "structure_report": structure_report,
        "asset_manifest": asset_manifest,
        "lineage_record": lineage_record,
        "inventory_snapshot": inventory_snapshot,
        "slice_count": slice_count,
        "admitted_count": admitted_count,
    }


# ============================================================
# CLI / main 接口区
# ============================================================


def build_parser() -> Any:
    parser = _build_parser_core()
    parser.description = (
        "Reliably run Action structural governance from a ready_for_action admission contract."
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = execute_pipeline(args)
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
