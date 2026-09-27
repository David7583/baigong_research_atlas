#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: data_admission_lineage_pipeline_v0001.py
# 中文名: 数据正式接纳与理解来源登记后半链编排脚本
# Version: v0001
# Layer: orchestration
# Main Layer: Data
# Updatable: True
# 功能说明: 接收上游解析准备handoff，可靠串联对话切片接纳、理解来源登记和接纳后库存核验。
# Scope: 从 ready_for_admission 开始，完成显式目标目录中的资产接纳，最终状态为 completed。
#
# 职责说明:
# 1. 校验上游handoff、切片、任务manifest和结构证据。
# 2. 调用固定版本脚本完成切片接纳、lineage open/finalize和库存核验。
# 3. 捕获每一步stdout、stderr、退出码和运行证据。
# 4. 任一步骤失败时停止并保留已发生事实，不执行自动删除或伪回退。
# 5. 成功时生成 completion_manifest.json，状态为 completed。
#
# 明确不做的事情:
# 1. 不修改或删除上游原始数据、工作副本和切片。
# 2. 不自动选择最新handoff、脚本版本或资产版本。
# 3. 不调用外部服务，不生成语义内容，不写数据库。
# 4. 不在缺少显式确认时执行真实接纳。
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: data_admission_lineage_pipeline
# family: data_admission_lineage_pipeline
# role: data_admission_lineage_orchestrator
# version: v0001
# status: active
# entry_point: scripts/orchestration/data/data_admission_lineage_pipeline_v0001.py
# input:
#   - upstream parse preparation handoff manifest
#   - explicit admission target root
#   - explicit understanding registry directory
# output:
#   - admitted conversation asset and manifest
#   - finalized understanding lineage record
#   - post-admission inventory and completion manifest
# depends_on:
#   - scripts/import_conversation_slices_to_processed_v0001.py
#   - scripts/register_understanding_lineage_v0001.py
#   - scripts/inventory_data_snapshot_v0001.py
# used_by:
#   - data_discovery_parse_preparation_pipeline
# governance:
#   level: middle
#   principle: compliance_only
# ============================================================

from __future__ import annotations

# ============================================================
# Imports 区
# ============================================================

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


# ============================================================
# 常量、固定版本与安全边界区
# ============================================================

SCRIPT_NAME = "data_admission_lineage_pipeline_v0001.py"
SCRIPT_FAMILY = "data_admission_lineage_pipeline"
SCRIPT_VERSION = "v0001"

UPSTREAM_FAMILY = "data_discovery_parse_preparation_pipeline"
UPSTREAM_STATUS = "ready_for_admission"

DEFAULT_OUTPUT_ROOT = Path("scripts") / "orchestration" / "outputs" / "data"
DEFAULT_REGISTRY_DIR = Path("understanding") / "registry"
DEFAULT_STEP_TIMEOUT_SECONDS = 600
DEFAULT_ASSET_VERSION = "v0001"

PINNED_SCRIPTS: Mapping[str, str] = {
    "import": "import_conversation_slices_to_processed_v0001.py",
    "lineage": "register_understanding_lineage_v0001.py",
    "inventory": "inventory_data_snapshot_v0001.py",
}

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
VERSION_PATTERN = re.compile(r"^v\d{4}$")


# ============================================================
# 异常与通用工具函数区
# ============================================================

class PipelineError(RuntimeError):
    """Raised when a downstream admission handoff is unsafe or incomplete."""


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{stamp}_{uuid.uuid4().hex[:8]}"


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
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
                    raise PipelineError(
                        f"{label} line {line_number} must contain a JSON object"
                    )
                records.append(value)
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"unable to read {label}: {path}: {exc}") from exc
    return records


def resolve_from_project(project_root: Path, value: str, *, label: str) -> Path:
    raw = Path(value)
    resolved = (raw if raw.is_absolute() else project_root / raw).resolve()
    if resolved.exists() and not resolved.is_dir():
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
    for key, filename in PINNED_SCRIPTS.items():
        path = (scripts_dir / filename).resolve()
        if not path.is_file():
            raise PipelineError(f"required pinned script is missing: {path}")
        resolved[key] = path
    return resolved


def validate_version(value: str) -> str:
    if not VERSION_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError("version must use vNNNN format")
    return value


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


# ============================================================
# 上游handoff校验区
# ============================================================

def resolve_handoff_path(
    handoff: Mapping[str, Any],
    dotted_path: Sequence[str],
    *,
    project_root: Path,
    label: str,
    expect_directory: bool,
) -> Path:
    value: Any = handoff
    for key in dotted_path:
        if not isinstance(value, Mapping) or key not in value:
            raise PipelineError(f"upstream handoff is missing {'.'.join(dotted_path)}")
        value = value[key]
    if not isinstance(value, str) or not value.strip():
        raise PipelineError(f"upstream handoff {'.'.join(dotted_path)} must be a path")

    raw = Path(value)
    resolved = (raw if raw.is_absolute() else project_root / raw).resolve()
    require_within(resolved, project_root, label=label)
    if expect_directory and not resolved.is_dir():
        raise PipelineError(f"{label} is not a directory: {resolved}")
    if not expect_directory and not resolved.is_file():
        raise PipelineError(f"{label} is not a file: {resolved}")
    return resolved


def validate_upstream_handoff(
    handoff_path: Path,
    *,
    project_root: Path,
) -> Dict[str, Any]:
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
        raise PipelineError("upstream handoff slice_count must be a positive integer")

    slices_dir = resolve_handoff_path(
        handoff,
        ("prepared_output", "slices_dir"),
        project_root=project_root,
        label="upstream slices directory",
        expect_directory=True,
    )
    task_dir = resolve_handoff_path(
        handoff,
        ("prepared_output", "task_dir"),
        project_root=project_root,
        label="upstream task directory",
        expect_directory=True,
    )
    task_manifest = resolve_handoff_path(
        handoff,
        ("evidence", "task_manifest"),
        project_root=project_root,
        label="upstream task manifest",
        expect_directory=False,
    )
    reconciliation = resolve_handoff_path(
        handoff,
        ("evidence", "reconciliation"),
        project_root=project_root,
        label="upstream reconciliation",
        expect_directory=False,
    )
    structure_report = resolve_handoff_path(
        handoff,
        ("evidence", "structure_report"),
        project_root=project_root,
        label="upstream structure report",
        expect_directory=False,
    )
    slice_manifest = resolve_handoff_path(
        handoff,
        ("evidence", "slice_manifest"),
        project_root=project_root,
        label="upstream slice manifest",
        expect_directory=False,
    )

    task_record = load_json_file(task_manifest, label="upstream task manifest")
    task_id = task_record.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise PipelineError("upstream task manifest is missing task_id")

    slice_record = load_json_file(slice_manifest, label="upstream slice manifest")
    if slice_record.get("mode") != "files":
        raise PipelineError("upstream slice mode must be files")
    if slice_record.get("slice_count") != slice_count:
        raise PipelineError("upstream slice manifest count does not match handoff")

    slice_files = sorted(
        path for path in slices_dir.glob("conv_*.json") if path.is_file()
    )
    if len(slice_files) != slice_count:
        raise PipelineError(
            f"upstream handoff expects {slice_count} slices, found {len(slice_files)}"
        )

    return {
        "handoff": handoff,
        "upstream_run_id": run_id,
        "slice_count": slice_count,
        "slices_dir": slices_dir,
        "task_dir": task_dir,
        "task_manifest": task_manifest,
        "task_id": task_id,
        "reconciliation": reconciliation,
        "structure_report": structure_report,
        "slice_manifest": slice_manifest,
        "slice_files": slice_files,
    }


# ============================================================
# 子脚本执行与运行证据管理区
# ============================================================

class PipelineRun:
    def __init__(
        self,
        *,
        project_root: Path,
        output_root: Path,
        run_id: str,
        upstream_handoff: Path,
        target_root: Path,
        registry_dir: Path,
        asset_version: str,
        test_mode: bool,
        scripts: Mapping[str, Path],
        step_timeout: int,
    ) -> None:
        self.project_root = project_root
        self.output_root = output_root
        self.run_id = run_id
        self.upstream_handoff = upstream_handoff
        self.target_root = target_root
        self.registry_dir = registry_dir
        self.asset_version = asset_version
        self.test_mode = test_mode
        self.scripts = dict(scripts)
        self.step_timeout = step_timeout
        self.run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
        self.stdout_dir = self.run_dir / "captured_stdout"
        self.stderr_dir = self.run_dir / "captured_stderr"
        self.step_status_path = self.run_dir / "step_status.jsonl"
        self.manifest_path = self.run_dir / "run_manifest.json"
        self.steps: List[Dict[str, Any]] = []
        self.manifest: Dict[str, Any] = {
            "schema_version": "data_admission_lineage_run_v0001",
            "pipeline": {
                "script": SCRIPT_NAME,
                "family": SCRIPT_FAMILY,
                "version": SCRIPT_VERSION,
            },
            "run_id": run_id,
            "status": "running",
            "started_at": utc_now_z(),
            "finished_at": None,
            "test_mode": test_mode,
            "project_root": str(project_root),
            "output_root": str(output_root),
            "run_dir": str(self.run_dir),
            "upstream_handoff": str(upstream_handoff),
            "target_root": str(target_root),
            "registry_dir": str(registry_dir),
            "asset_version": asset_version,
            "pinned_scripts": dict(PINNED_SCRIPTS),
            "steps": self.steps,
            "error": None,
        }

    def initialize(self) -> None:
        if self.run_dir.exists():
            raise PipelineError(f"run directory already exists: {self.run_dir}")
        self.stdout_dir.mkdir(parents=True, exist_ok=False)
        self.stderr_dir.mkdir(parents=True, exist_ok=False)
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
            "status": "success" if returncode == 0 and not timed_out else "failure",
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
        self.manifest["error"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
        self.persist()
        write_json_atomic(
            self.run_dir / "failure.json",
            {
                "run_id": self.run_id,
                "status": "failed",
                "failed_at": utc_now_z(),
                "error": self.manifest["error"],
                "side_effects_may_exist": len(self.steps) > 0,
            },
        )


# ============================================================
# 正式接纳与lineage主流程区
# ============================================================

def execute_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = find_project_root(args.project_root)
    scripts = validate_pinned_scripts(project_root)
    handoff_path = resolve_file_from_project(
        project_root,
        args.handoff_manifest,
        label="upstream handoff manifest",
    )
    require_within(handoff_path, project_root, label="upstream handoff manifest")
    upstream = validate_upstream_handoff(handoff_path, project_root=project_root)

    target_root = resolve_from_project(project_root, args.target_root, label="target root")
    registry_dir = resolve_from_project(project_root, args.registry_dir, label="registry directory")
    output_root = resolve_from_project(project_root, args.output_root, label="output root")

    if args.test_mode:
        test_root = (project_root / "temp").resolve()
        require_within(target_root, test_root, label="test target root", allow_equal=False)
        require_within(registry_dir, test_root, label="test registry directory", allow_equal=False)
    else:
        require_within(
            target_root,
            project_root / "data_processed",
            label="formal target root",
        )
        require_within(
            registry_dir,
            project_root / "understanding" / "registry",
            label="formal registry directory",
        )

    run_id = args.run_id or new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise PipelineError("run_id contains unsupported characters")

    asset_version = args.asset_version
    target_version_dir = target_root / asset_version
    if target_version_dir.exists():
        raise PipelineError(f"target asset version already exists: {target_version_dir}")

    run_dir = output_root / SCRIPT_FAMILY / SCRIPT_VERSION / run_id
    plan = {
        "pipeline": SCRIPT_FAMILY,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "upstream_handoff": str(handoff_path),
        "upstream_run_id": upstream["upstream_run_id"],
        "slice_count": upstream["slice_count"],
        "target_root": str(target_root),
        "target_version_dir": str(target_version_dir),
        "registry_dir": str(registry_dir),
        "output_root": str(output_root),
        "run_dir": str(run_dir),
        "test_mode": bool(args.test_mode),
        "dry_run": bool(args.dry_run),
        "pinned_scripts": dict(PINNED_SCRIPTS),
    }
    if args.dry_run:
        return plan
    if not args.confirm_admission:
        raise PipelineError("actual admission requires --confirm-admission")

    run = PipelineRun(
        project_root=project_root,
        output_root=output_root,
        run_id=run_id,
        upstream_handoff=handoff_path,
        target_root=target_root,
        registry_dir=registry_dir,
        asset_version=asset_version,
        test_mode=bool(args.test_mode),
        scripts=scripts,
        step_timeout=args.step_timeout,
    )
    run.initialize()

    try:
        import_stdout = run.run_step(
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
                asset_version,
            ],
        )
        import_result = parse_json_text(import_stdout, label="import stdout")
        if import_result.get("status") != "ok":
            raise PipelineError("import step did not return status ok")
        if import_result.get("imported_count") != upstream["slice_count"]:
            raise PipelineError("imported count does not match upstream slice count")
        if Path(str(import_result.get("target_dir", ""))).resolve() != target_version_dir.resolve():
            raise PipelineError("import step returned an unexpected target directory")

        asset_manifest_path = target_version_dir / "manifest.json"
        asset_manifest = load_json_file(asset_manifest_path, label="asset manifest")
        if asset_manifest.get("version") != asset_version:
            raise PipelineError("asset manifest version does not match requested version")
        admitted_files = sorted((target_version_dir / "data").glob("conv_*.json"))
        if len(admitted_files) != upstream["slice_count"]:
            raise PipelineError("admitted file count does not match upstream slice count")

        evidence_items = [
            f"upstream_handoff:{handoff_path}",
            f"reconciliation:{upstream['reconciliation']}",
            f"task_manifest:{upstream['task_manifest']}",
            f"structure_report:{upstream['structure_report']}",
            f"slice_manifest:{upstream['slice_manifest']}",
        ]
        open_stdout = run.run_step(
            "open_understanding_lineage",
            "lineage",
            [
                "open",
                "--asset-manifest",
                str(asset_manifest_path),
                "--evidence",
                *evidence_items,
                "--registry-dir",
                str(registry_dir),
            ],
        )
        opened_record = parse_json_text(open_stdout, label="lineage open stdout")
        if opened_record.get("status") != "opened":
            raise PipelineError("lineage open did not return opened status")
        record_id = opened_record.get("record_id")
        if not isinstance(record_id, str) or not record_id:
            raise PipelineError("lineage open did not return record_id")
        registry_record_path = registry_dir / f"{record_id}.json"
        if not registry_record_path.is_file():
            raise PipelineError("lineage registry record was not written")

        finalize_stdout = run.run_step(
            "finalize_understanding_lineage",
            "lineage",
            [
                "finalize",
                "--record-id",
                record_id,
                "--analysis-evidence-root",
                str(upstream["structure_report"].parent),
                "--registry-dir",
                str(registry_dir),
            ],
        )
        finalized_record = parse_json_text(
            finalize_stdout,
            label="lineage finalize stdout",
        )
        if finalized_record.get("status") != "completed":
            raise PipelineError(
                f"lineage finalize status is not completed: {finalized_record.get('status')}"
            )
        if finalized_record.get("record_id") != record_id:
            raise PipelineError("lineage finalize returned a different record_id")

        inventory_dir = run.run_dir / "inventory"
        run.run_step(
            "post_admission_inventory",
            "inventory",
            [
                "--data-dir",
                str(target_version_dir),
                "--output-dir",
                str(inventory_dir),
                "--no-hash",
            ],
        )
        inventory_files = sorted(inventory_dir.glob("inventory_*.jsonl"))
        if len(inventory_files) != 1:
            raise PipelineError("post-admission inventory must produce exactly one snapshot")
        inventory_records = load_jsonl_file(
            inventory_files[0],
            label="post-admission inventory",
        )
        inventory_keys = {
            str(record.get("path_relative", "")).replace("\\", "/")
            for record in inventory_records
        }
        expected_inventory_keys = {"manifest.json"} | {
            f"data/{path.name}" for path in admitted_files
        }
        missing_inventory = sorted(expected_inventory_keys - inventory_keys)
        if missing_inventory:
            raise PipelineError(
                "post-admission inventory is missing: " + ", ".join(missing_inventory)
            )

        completion = {
            "schema_version": "data_admission_lineage_completion_v0001",
            "pipeline": SCRIPT_FAMILY,
            "pipeline_version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
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
                "asset_version": asset_version,
                "asset_dir": str(target_version_dir),
                "asset_manifest": str(asset_manifest_path),
                "admitted_count": len(admitted_files),
                "admitted_files": [str(path) for path in admitted_files],
            },
            "lineage": {
                "registry_dir": str(registry_dir),
                "record_id": record_id,
                "record_path": str(registry_record_path),
                "status": finalized_record.get("status"),
            },
            "inventory": {
                "snapshot": str(inventory_files[0]),
                "required_paths_observed": True,
            },
        }
        completion_path = run.run_dir / "completion_manifest.json"
        write_json_atomic(completion_path, completion)
        run.finish_success(completion_path)
        return {
            "pipeline": SCRIPT_FAMILY,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "status": "completed",
            "test_mode": bool(args.test_mode),
            "run_dir": str(run.run_dir),
            "completion_manifest": str(completion_path),
            "asset_manifest": str(asset_manifest_path),
            "lineage_record": str(registry_record_path),
            "admitted_count": len(admitted_files),
        }
    except Exception as exc:
        run.finish_failure(exc)
        raise


# ============================================================
# CLI 参数与入口区
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reliably admit prepared conversation slices, finalize understanding lineage, "
            "and verify the admitted asset."
        )
    )
    parser.add_argument("--project-root", default="", help="Optional project root override.")
    parser.add_argument(
        "--handoff-manifest",
        required=True,
        help="Exact handoff_manifest.json produced by the upstream preparation pipeline.",
    )
    parser.add_argument(
        "--target-root",
        required=True,
        help="Explicit asset target root. A version directory is created underneath it.",
    )
    parser.add_argument(
        "--registry-dir",
        default=str(DEFAULT_REGISTRY_DIR),
        help="Understanding registry directory, relative to project root unless absolute.",
    )
    parser.add_argument(
        "--asset-version",
        type=validate_version,
        default=DEFAULT_ASSET_VERSION,
        help="Explicit asset version using vNNNN format.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Orchestration evidence root, relative to project root unless absolute.",
    )
    parser.add_argument("--run-id", default="", help="Optional safe, unique run id.")
    parser.add_argument(
        "--step-timeout",
        type=positive_integer,
        default=DEFAULT_STEP_TIMEOUT_SECONDS,
        help="Timeout in seconds for each child script.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Require target and registry directories to stay under project temp.",
    )
    parser.add_argument(
        "--confirm-admission",
        action="store_true",
        help="Explicitly authorize the actual admission steps.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print a plan without creating outputs or invoking child scripts.",
    )
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
