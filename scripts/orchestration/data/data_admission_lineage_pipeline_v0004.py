# ============================================================
# 文件名: data_admission_lineage_pipeline_v0004.py
# 中文名: 自包含规范数据准入与血缘编排
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
# alias: data_admission_lineage_pipeline_v0004
# family: data_admission_lineage_pipeline
# role: canonical_ingress_admission_orchestrator
# version: v0004
# status: active
# entry_point: scripts/orchestration/data/data_admission_lineage_pipeline_v0004.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - import_conversation_slices_to_processed_v0002.py
#   - inventory_data_snapshot_v0001.py
#   - register_action_lineage_v0002.py
# used_by:
#   - data_action_chain_pipeline_v0015
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/orchestration/data/data_admission_lineage_pipeline_v0002.py, scripts/orchestration/data/data_admission_lineage_pipeline_v0003.py
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
from typing import Any, Dict, List, Mapping, Optional, Sequence
import importlib.util
from types import ModuleType
from typing import Any, Dict, Mapping, Optional, Sequence

# ============================================================
# 全局常量与默认映射
# ============================================================

SCRIPT_FAMILY = "data_admission_lineage_pipeline"


UPSTREAM_FAMILY = "data_discovery_parse_preparation_pipeline"


UPSTREAM_STATUS = "ready_for_admission"


DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "data"


DEFAULT_REGISTRY_DIR = Path("actioning") / "registry"


DEFAULT_STEP_TIMEOUT_SECONDS = 600


DEFAULT_ASSET_VERSION = "v0001"


PINNED_SCRIPTS: Mapping[str, str] = {
    "import": "scripts/import_conversation_slices_to_processed_v0002.py",
    "lineage": "scripts/action/infrastructures/register_action_lineage_v0002.py",
    "inventory": "scripts/inventory_data_snapshot_v0001.py",
}


RUN_ID_PATTERN = re.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


VERSION_PATTERN = re.compile("^v\\d{4}$")


SCRIPT_NAME = "data_admission_lineage_pipeline_v0004.py"


SCRIPT_VERSION = "v0004"


ENVELOPE_SCHEMA = "canonical_ingress_envelope_v0001"


DEFAULT_ENCODING = "utf-8"

# ============================================================
# 异常类型
# ============================================================


class PipelineError(RuntimeError):
    """Raised when data admission cannot safely reach ready_for_action."""


# ============================================================
# 数据结构、工具与核心实现
# ============================================================


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{stamp}_{uuid.uuid4().hex[:8]}"


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(dict(payload), ensure_ascii=False) + "\n")


def load_json_file(path: Path, *, label: str) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PipelineError(f"{label} not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PipelineError(f"{label} is invalid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise PipelineError(f"{label} must be a JSON object: {path}")
    return payload


def load_jsonl_file(path: Path, *, label: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as exc:
        raise PipelineError(f"{label} not found: {path}") from exc
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PipelineError(f"{label} line {number} is invalid JSON") from exc
        if not isinstance(payload, dict):
            raise PipelineError(f"{label} line {number} must be an object")
        records.append(payload)
    return records


def parse_json_text(text: str, *, label: str) -> Dict[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PipelineError(f"{label} is not a JSON object: {exc}") from exc
    if not isinstance(payload, dict):
        raise PipelineError(f"{label} must be a JSON object")
    return payload


def find_project_root(raw: str) -> Path:
    if raw:
        candidate = Path(raw).resolve()
        if not (candidate / "AGENTS.md").is_file():
            raise PipelineError(f"project root does not contain AGENTS.md: {candidate}")
        return candidate
    for candidate in [Path.cwd(), *Path(__file__).resolve().parents]:
        if (candidate / "AGENTS.md").is_file():
            return candidate.resolve()
    raise PipelineError("unable to locate project root; provide --project-root")


def resolve_from_project(project_root: Path, raw: str, *, label: str) -> Path:
    path = Path(raw)
    resolved = (path if path.is_absolute() else project_root / path).resolve()
    require_within(resolved, project_root, label=label)
    return resolved


def require_within(path: Path, root: Path, *, label: str, allow_equal: bool = True) -> None:
    root = root.resolve()
    try:
        relative = path.resolve().relative_to(root)
    except ValueError as exc:
        raise PipelineError(f"{label} escapes allowed root {root}: {path}") from exc
    if not allow_equal and relative == Path("."):
        raise PipelineError(f"{label} must be below, not equal to, {root}")


def validate_pinned_scripts(project_root: Path) -> Dict[str, Path]:
    scripts: Dict[str, Path] = {}
    for key, relative in PINNED_SCRIPTS.items():
        path = (project_root / relative).resolve()
        require_within(path, project_root, label=f"pinned script {key}")
        if not path.is_file():
            raise PipelineError(f"pinned script is missing: {path}")
        scripts[key] = path
    return scripts


def validate_version(value: str) -> str:
    if not VERSION_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError("version must use vNNNN format")
    return value


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def resolve_manifest_path(
    manifest: Mapping[str, Any],
    dotted_path: Sequence[str],
    *,
    project_root: Path,
    label: str,
    expect_directory: bool,
) -> Path:
    value: Any = manifest
    for key in dotted_path:
        if not isinstance(value, Mapping) or key not in value:
            raise PipelineError(f"upstream handoff is missing {'.'.join(dotted_path)}")
        value = value[key]
    if not isinstance(value, str) or not value.strip():
        raise PipelineError(f"upstream handoff {'.'.join(dotted_path)} must be a path")
    resolved = resolve_from_project(project_root, value, label=label)
    if expect_directory and (not resolved.is_dir()):
        raise PipelineError(f"{label} is not a directory: {resolved}")
    if not expect_directory and (not resolved.is_file()):
        raise PipelineError(f"{label} is not a file: {resolved}")
    return resolved


def validate_upstream_handoff(handoff_path: Path, *, project_root: Path) -> Dict[str, Any]:
    handoff = load_json_file(handoff_path, label="upstream handoff manifest")
    if handoff.get("pipeline") != UPSTREAM_FAMILY:
        raise PipelineError(f"unexpected upstream pipeline: {handoff.get('pipeline')}")
    if handoff.get("status") != UPSTREAM_STATUS:
        raise PipelineError(f"upstream status must be {UPSTREAM_STATUS}")
    run_id = handoff.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise PipelineError("upstream handoff is missing run_id")
    slice_count = handoff.get("prepared_output", {}).get("slice_count")
    if not isinstance(slice_count, int) or slice_count < 1:
        raise PipelineError("upstream handoff slice_count must be positive")
    slices_dir = resolve_manifest_path(
        handoff,
        ("prepared_output", "slices_dir"),
        project_root=project_root,
        label="slices directory",
        expect_directory=True,
    )
    task_manifest = resolve_manifest_path(
        handoff,
        ("evidence", "task_manifest"),
        project_root=project_root,
        label="task manifest",
        expect_directory=False,
    )
    reconciliation = resolve_manifest_path(
        handoff,
        ("evidence", "reconciliation"),
        project_root=project_root,
        label="reconciliation",
        expect_directory=False,
    )
    structure_report = resolve_manifest_path(
        handoff,
        ("evidence", "structure_report"),
        project_root=project_root,
        label="structure report",
        expect_directory=False,
    )
    slice_manifest = resolve_manifest_path(
        handoff,
        ("evidence", "slice_manifest"),
        project_root=project_root,
        label="slice manifest",
        expect_directory=False,
    )
    task_record = load_json_file(task_manifest, label="task manifest")
    task_id = task_record.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise PipelineError("task manifest is missing task_id")
    slice_record = load_json_file(slice_manifest, label="slice manifest")
    if slice_record.get("mode") != "files" or slice_record.get("slice_count") != slice_count:
        raise PipelineError("slice manifest does not match handoff")
    slice_files = sorted((path for path in slices_dir.glob("conv_*.json") if path.is_file()))
    if len(slice_files) != slice_count:
        raise PipelineError(f"handoff expects {slice_count} slices, found {len(slice_files)}")
    return {
        "handoff": handoff,
        "upstream_run_id": run_id,
        "slice_count": slice_count,
        "slices_dir": slices_dir,
        "task_manifest": task_manifest,
        "task_id": task_id,
        "reconciliation": reconciliation,
        "structure_report": structure_report,
        "slice_manifest": slice_manifest,
        "slice_files": slice_files,
    }


class PipelineRun:

    def __init__(
        self,
        *,
        project_root: Path,
        output_root: Path,
        run_id: str,
        test_mode: bool,
        scripts: Mapping[str, Path],
        step_timeout: int,
        manifest_fields: Mapping[str, Any],
    ) -> None:
        self.project_root = project_root
        self.run_id = run_id
        self.scripts = dict(scripts)
        self.step_timeout = step_timeout
        self.run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
        self.stdout_dir = self.run_dir / "captured_stdout"
        self.stderr_dir = self.run_dir / "captured_stderr"
        self.step_status_path = self.run_dir / "step_status.jsonl"
        self.manifest_path = self.run_dir / "run_manifest.json"
        self.steps: List[Dict[str, Any]] = []
        self.manifest: Dict[str, Any] = {
            "schema_version": "data_admission_lineage_run_v0003",
            "pipeline": {"script": SCRIPT_NAME, "family": SCRIPT_FAMILY, "version": SCRIPT_VERSION},
            "run_id": run_id,
            "status": "running",
            "started_at": utc_now_z(),
            "finished_at": None,
            "test_mode": test_mode,
            "project_root": str(project_root),
            "run_dir": str(self.run_dir),
            "pinned_scripts": dict(PINNED_SCRIPTS),
            "steps": self.steps,
            "error": None,
            **dict(manifest_fields),
        }

    def initialize(self) -> None:
        if self.run_dir.exists():
            raise PipelineError(f"run directory already exists: {self.run_dir}")
        self.stdout_dir.mkdir(parents=True, exist_ok=False)
        self.stderr_dir.mkdir(parents=True, exist_ok=False)
        write_json_atomic(self.manifest_path, self.manifest)

    def persist(self) -> None:
        write_json_atomic(self.manifest_path, self.manifest)

    def run_step(self, name: str, script_key: str, arguments: Sequence[str]) -> str:
        effective = list(arguments)
        if script_key == "import":
            effective.extend(["--ingress-envelope", self.manifest["ingress_envelope"]])
        elif script_key == "lineage":
            effective.extend(["--evidence", "ingress_envelope:" + self.manifest["ingress_envelope"]])
        command = [sys.executable, str(self.scripts[script_key]), *map(str, effective)]
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
            stdout, stderr, returncode, timed_out = (
                result.stdout or "",
                result.stderr or "",
                int(result.returncode),
                False,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout if isinstance(exc.stdout, str) else ""
            stderr = exc.stderr if isinstance(exc.stderr, str) else ""
            returncode, timed_out = (-1, True)
        write_text_atomic(stdout_path, stdout)
        write_text_atomic(stderr_path, stderr)
        step = {
            "index": index,
            "name": name,
            "script": self.scripts[script_key].name,
            "command": command,
            "started_at": started_at,
            "finished_at": utc_now_z(),
            "duration_ms": int((time.monotonic() - started_clock) * 1000),
            "returncode": returncode,
            "timed_out": timed_out,
            "status": "success" if returncode == 0 and (not timed_out) else "failure",
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }
        self.steps.append(step)
        append_jsonl(self.step_status_path, step)
        self.persist()
        if timed_out:
            raise PipelineError(f"step {name} exceeded {self.step_timeout} seconds")
        if returncode != 0:
            raise PipelineError(f"step {name} failed: {stderr.strip() or stdout.strip() or 'no output'}")
        return stdout

    def finish_success(self, completion_path: Path) -> None:
        self.manifest.update(
            {"status": "completed", "finished_at": utc_now_z(), "completion_manifest": str(completion_path)}
        )
        self.persist()

    def finish_failure(self, error: Exception) -> None:
        self.manifest.update(
            {
                "status": "failed",
                "finished_at": utc_now_z(),
                "error": {"type": type(error).__name__, "message": str(error)},
            }
        )
        self.persist()
        write_json_atomic(
            self.run_dir / "failure.json",
            {
                "run_id": self.run_id,
                "status": "failed",
                "failed_at": utc_now_z(),
                "error": self.manifest["error"],
                "side_effects_may_exist": bool(self.steps),
            },
        )


def _execute_admission_core(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = find_project_root(args.project_root)
    scripts = validate_pinned_scripts(project_root)
    handoff_path = resolve_from_project(project_root, args.handoff_manifest, label="handoff manifest")
    if not handoff_path.is_file():
        raise PipelineError(f"handoff manifest is not a file: {handoff_path}")
    upstream = validate_upstream_handoff(handoff_path, project_root=project_root)
    target_root = resolve_from_project(project_root, args.target_root, label="target root")
    registry_dir = resolve_from_project(project_root, args.registry_dir, label="registry directory")
    output_root = resolve_from_project(project_root, args.output_root, label="output root")
    if args.test_mode:
        test_root = (project_root / "temp").resolve()
        for path, label in (
            (target_root, "test target root"),
            (registry_dir, "test registry directory"),
            (output_root, "test output root"),
        ):
            require_within(path, test_root, label=label, allow_equal=False)
    else:
        require_within(target_root, project_root / "data_processed", label="formal target root")
        require_within(
            registry_dir, project_root / "actioning" / "registry", label="formal Action registry directory"
        )
    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError("run_id contains unsupported characters")
    target_version_dir = target_root / args.asset_version
    if target_version_dir.exists():
        raise PipelineError(f"target asset version already exists: {target_version_dir}")
    plan = {
        "pipeline": SCRIPT_FAMILY,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "upstream_handoff": str(handoff_path),
        "target_version_dir": str(target_version_dir),
        "registry_dir": str(registry_dir),
        "output_root": str(output_root),
        "readiness_target": "ready_for_action",
        "test_mode": bool(args.test_mode),
        "dry_run": bool(args.dry_run),
        "pinned_scripts": dict(PINNED_SCRIPTS),
        "ingress_envelope": args.ingress_envelope,
    }
    if args.dry_run:
        return plan
    if not args.confirm_admission:
        raise PipelineError("actual admission requires --confirm-admission")
    run = PipelineRun(
        project_root=project_root,
        output_root=output_root,
        run_id=run_id,
        test_mode=bool(args.test_mode),
        scripts=scripts,
        step_timeout=args.step_timeout,
        manifest_fields=plan,
    )
    run.initialize()
    try:
        import_result = parse_json_text(
            run.run_step(
                "import_conversation_slices",
                "import",
                [
                    "--source-task",
                    upstream["task_id"],
                    "--source-dir",
                    str(upstream["slices_dir"]),
                    "--target-root",
                    str(target_root),
                    "--version",
                    args.asset_version,
                ],
            ),
            label="import stdout",
        )
        if (
            import_result.get("status") != "ok"
            or import_result.get("imported_count") != upstream["slice_count"]
        ):
            raise PipelineError("import result does not match upstream handoff")
        asset_manifest_path = target_version_dir / "manifest.json"
        asset_manifest = load_json_file(asset_manifest_path, label="asset manifest")
        if asset_manifest.get("version") != args.asset_version:
            raise PipelineError("asset manifest version mismatch")
        admitted_files = sorted((target_version_dir / "data").glob("conv_*.json"))
        if len(admitted_files) != upstream["slice_count"]:
            raise PipelineError("admitted file count mismatch")
        evidence = [
            f"upstream_handoff:{handoff_path}",
            f"reconciliation:{upstream['reconciliation']}",
            f"task_manifest:{upstream['task_manifest']}",
            f"structure_report:{upstream['structure_report']}",
            f"slice_manifest:{upstream['slice_manifest']}",
        ]
        opened = parse_json_text(
            run.run_step(
                "open_action_lineage",
                "lineage",
                [
                    "--project-root",
                    str(project_root),
                    "open",
                    "--asset-manifest",
                    str(asset_manifest_path),
                    "--evidence",
                    *evidence,
                    "--registry-dir",
                    str(registry_dir),
                ],
            ),
            label="Action lineage open stdout",
        )
        if opened.get("status") != "ready_for_action":
            raise PipelineError("Action lineage did not reach ready_for_action")
        record_id = opened.get("record_id")
        if not isinstance(record_id, str) or not record_id:
            raise PipelineError("Action lineage did not return record_id")
        record_path = registry_dir / f"{record_id}.json"
        if not record_path.is_file():
            raise PipelineError("Action lineage record was not written")
        inventory_dir = run.run_dir / "inventory"
        run.run_step(
            "post_admission_inventory",
            "inventory",
            ["--data-dir", str(target_version_dir), "--output-dir", str(inventory_dir), "--no-hash"],
        )
        inventory_files = sorted(inventory_dir.glob("inventory_*.jsonl"))
        if len(inventory_files) != 1:
            raise PipelineError("post-admission inventory must contain one snapshot")
        inventory_keys = {
            str(record.get("path_relative", "")).replace("\\", "/")
            for record in load_jsonl_file(inventory_files[0], label="inventory")
        }
        expected = {"manifest.json", *[f"data/{path.name}" for path in admitted_files]}
        missing = sorted(expected - inventory_keys)
        if missing:
            raise PipelineError("inventory is missing: " + ", ".join(missing))
        completion = {
            "schema_version": "data_admission_lineage_completion_v0002",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "readiness": "ready_for_action",
            "completed_at": utc_now_z(),
            "test_mode": bool(args.test_mode),
            "upstream": {
                "pipeline": UPSTREAM_FAMILY,
                "run_id": upstream["upstream_run_id"],
                "handoff_manifest": str(handoff_path),
                "slice_count": upstream["slice_count"],
            },
            "admission": {
                "target_root": str(target_root),
                "asset_version": args.asset_version,
                "asset_dir": str(target_version_dir),
                "asset_manifest": str(asset_manifest_path),
                "admitted_count": len(admitted_files),
                "admitted_files": [str(path) for path in admitted_files],
            },
            "lineage": {
                "type": "action",
                "registry_dir": str(registry_dir),
                "record_id": record_id,
                "record_path": str(record_path),
                "status": "ready_for_action",
            },
            "inventory": {"snapshot": str(inventory_files[0]), "required_paths_observed": True},
        }
        completion_path = run.run_dir / "completion_manifest.json"
        write_json_atomic(completion_path, completion)
        run.finish_success(completion_path)
        return {
            "pipeline": SCRIPT_FAMILY,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "readiness": "ready_for_action",
            "run_dir": str(run.run_dir),
            "completion_manifest": str(completion_path),
            "asset_manifest": str(asset_manifest_path),
            "action_lineage_record": str(record_path),
            "admitted_count": len(admitted_files),
        }
    except Exception as exc:
        run.finish_failure(exc)
        raise


def _build_parser_core() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reliably admit prepared slices and open Action lineage at ready_for_action."
    )
    parser.add_argument("--project-root", default="")
    parser.add_argument("--handoff-manifest", required=True)
    parser.add_argument("--target-root", required=True)
    parser.add_argument("--registry-dir", default=str(DEFAULT_REGISTRY_DIR))
    parser.add_argument("--asset-version", type=validate_version, default=DEFAULT_ASSET_VERSION)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--step-timeout", type=positive_integer, default=DEFAULT_STEP_TIMEOUT_SECONDS)
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--confirm-admission", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _find_project_root() -> Path:
    for candidate in [Path.cwd(), *Path(__file__).resolve().parents]:
        if (candidate / "AGENTS.md").is_file():
            return candidate.resolve()
    raise RuntimeError("unable to locate project root")


def _load_envelope(project_root: Path, raw_path: str) -> tuple[Path, Dict[str, Any]]:
    path = resolve_from_project(project_root, raw_path, label="ingress envelope")
    envelope = load_json_file(path, label="ingress envelope")
    if envelope.get("schema_version") != ENVELOPE_SCHEMA:
        raise PipelineError(f"ingress envelope schema_version must be {ENVELOPE_SCHEMA}")
    if envelope.get("status") != "completed":
        raise PipelineError("ingress envelope status must be completed")
    canonical = envelope.get("canonical_output")
    if not isinstance(canonical, Mapping) or canonical.get("contract_valid") is not True:
        raise PipelineError("ingress envelope canonical contract is not valid")
    source = envelope.get("source")
    if not isinstance(source, Mapping) or source.get("immutable_verified") is not True:
        raise PipelineError("ingress envelope immutable source verification failed")
    return (path, envelope)


def execute_pipeline(args: Any) -> Dict[str, Any]:
    project_root = _find_project_root()
    effective_root = find_project_root(args.project_root)
    ingress_path, envelope = _load_envelope(effective_root, args.ingress_envelope)
    args.ingress_envelope = str(ingress_path)
    result = _execute_admission_core(args)
    if args.dry_run:
        result["ingress_envelope"] = str(ingress_path)
        result["source_envelope_id"] = envelope.get("envelope_id")
        return result
    completion_path = Path(str(result["completion_manifest"]))
    completion = load_json_file(completion_path, label="completion manifest")
    completion["schema_version"] = "data_admission_lineage_completion_v0003"
    completion["ingress"] = {
        "envelope_path": str(ingress_path),
        "envelope_id": envelope.get("envelope_id"),
        "format_family": envelope.get("detection", {}).get("format_family"),
        "fingerprint_version": envelope.get("detection", {}).get("fingerprint_version"),
        "adapter_family": envelope.get("adapter", {}).get("family"),
        "adapter_version": envelope.get("adapter", {}).get("version"),
    }
    write_json_atomic(completion_path, completion)
    result["ingress_envelope"] = str(ingress_path)
    result["source_envelope_id"] = envelope.get("envelope_id")
    return result


# ============================================================
# CLI / main 接口区
# ============================================================


def build_parser() -> Any:
    parser = _build_parser_core()
    parser.description = (
        "Admit canonical conversation slices with an immutable complete-contract ingress envelope."
    )
    parser.add_argument("--ingress-envelope", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = execute_pipeline(args)
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
