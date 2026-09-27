#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: register_action_lineage_v0002.py
# 中文名: 行动端执行来源登记脚本
# Version: v0002
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose
# 对一次真实 Action 执行建立、结束或失败登记，保存资产锚点、执行证据和状态事实。
#
# What it does
# 1) 接纳完成后创建 ready_for_action 登记记录；
# 2) Action 子编排全部完成后，验证完成清单并冻结为 completed；
# 3) Action 执行失败时，保存显式失败证据并冻结为 failed。
#
# What it does NOT do
# 1) 不规划、选择或执行 Action 流程；
# 2) 不推断未提供的证据；
# 3) 不修改数据资产、数据库或 Action 业务产物；
# 4) 不覆盖已经结束的登记记录。
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: register_action_lineage
# family: action_registry
# role: action_lineage_recorder
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/register_action_lineage_v0002.py
# input:
#   - admitted asset manifest
#   - explicit evidence pointers
#   - completed Action orchestration manifests or failure manifest
# output:
#   - Action lineage registry record (json)
# depends_on: []
# used_by:
#   - data_admission_lineage_pipeline
#   - data_action_chain_pipeline
# governance:
#   level: high
#   principle: evidence_only
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


SCRIPT_NAME = "register_action_lineage_v0002.py"
SCRIPT_VERSION = "v0002"
RECORD_SCHEMA = "action_lineage_record_v0002"
DEFAULT_REGISTRY_DIR = Path("actioning") / "registry"
CURRENT_PIPELINE_FILE = ".current_pipeline.json"
FINAL_STATUSES = {"completed", "failed"}


class LineageError(RuntimeError):
    """Raised when an Action lineage transition is invalid or unsafe."""


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def dumps_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def load_json_object(path: Path, *, label: str) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LineageError(f"{label} not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise LineageError(f"{label} is not valid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LineageError(f"{label} must contain a JSON object: {path}")
    return payload


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(dumps_json(payload) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def resolve_path(raw: str, *, project_root: Path) -> Path:
    path = Path(raw)
    return (path if path.is_absolute() else project_root / path).resolve()


def require_within(path: Path, root: Path, *, label: str) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise LineageError(f"{label} escapes project root: {path}") from exc


def find_project_root(raw: str) -> Path:
    if raw:
        candidate = Path(raw).resolve()
        if not (candidate / "AGENTS.md").is_file():
            raise LineageError(f"project root does not contain AGENTS.md: {candidate}")
        return candidate
    candidates = [Path.cwd(), *Path(__file__).resolve().parents]
    for candidate in candidates:
        if (candidate / "AGENTS.md").is_file():
            return candidate.resolve()
    raise LineageError("unable to locate project root; provide --project-root")


def generate_record_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"AREG_{stamp}_{uuid.uuid4().hex[:8]}"


def environment_snapshot() -> Dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "executable": sys.executable,
    }


def parse_kind_path(item: str, *, project_root: Path) -> Tuple[str, Path]:
    if ":" not in item:
        raise LineageError("evidence must use kind:path format")
    kind, raw_path = item.split(":", 1)
    kind = kind.strip()
    raw_path = raw_path.strip()
    if not kind or not raw_path:
        raise LineageError("evidence kind and path must both be non-empty")
    path = resolve_path(raw_path, project_root=project_root)
    require_within(path, project_root, label="evidence path")
    if not path.exists():
        raise LineageError(f"evidence path does not exist: {path}")
    return kind, path


def validate_record(record: Mapping[str, Any], *, record_id: str) -> None:
    if record.get("schema_version") != RECORD_SCHEMA:
        raise LineageError("registry record schema is not supported by v0002")
    if record.get("record_id") != record_id:
        raise LineageError("registry record_id does not match requested record")


def write_current_pointer(registry_dir: Path, record: Mapping[str, Any]) -> None:
    write_json_atomic(
        registry_dir / CURRENT_PIPELINE_FILE,
        {
            "record_id": record["record_id"],
            "asset_dir": record["asset_anchor"]["asset_dir"],
            "status": record["status"],
            "updated_at_utc": utc_now_z(),
        },
    )


def open_registration(
    *,
    asset_manifest_path: Path,
    evidence_items: Sequence[Tuple[str, Path]],
    registry_dir: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    asset_manifest = load_json_object(asset_manifest_path, label="asset manifest")
    version = asset_manifest.get("version")
    if not isinstance(version, str) or not version:
        raise LineageError("asset manifest is missing a non-empty version")

    record_id = generate_record_id()
    lineage_facts = [{"kind": "asset_manifest", "path": str(asset_manifest_path)}]
    lineage_facts.extend({"kind": kind, "path": str(path)} for kind, path in evidence_items)
    record: Dict[str, Any] = {
        "schema_version": RECORD_SCHEMA,
        "record_id": record_id,
        "created_at_utc": utc_now_z(),
        "finalized_at_utc": None,
        "failed_at_utc": None,
        "status": "ready_for_action",
        "registrar": {
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "environment": environment_snapshot(),
        },
        "asset_anchor": {
            "asset_type": asset_manifest.get("asset_type"),
            "source": asset_manifest.get("source"),
            "version": version,
            "asset_dir": str(asset_manifest_path.parent),
            "manifest_path": str(asset_manifest_path),
        },
        "lineage_facts": lineage_facts,
        "action_chain": [],
        "artifacts": [],
        "failure": None,
    }
    if dry_run:
        return record

    registry_dir.mkdir(parents=True, exist_ok=True)
    record_path = registry_dir / f"{record_id}.json"
    if record_path.exists():
        raise LineageError(f"registry record already exists: {record_path}")
    write_json_atomic(record_path, record)
    write_current_pointer(registry_dir, record)
    return record


def load_active_record(record_id: str, registry_dir: Path) -> Tuple[Path, Dict[str, Any]]:
    record_path = registry_dir / f"{record_id}.json"
    record = load_json_object(record_path, label="Action lineage record")
    validate_record(record, record_id=record_id)
    if record.get("status") in FINAL_STATUSES:
        raise LineageError(f"Action lineage is already final: {record.get('status')}")
    if record.get("status") != "ready_for_action":
        raise LineageError(f"unsupported Action lineage state: {record.get('status')}")
    return record_path, record


def finalize_registration(
    *,
    record_id: str,
    registry_dir: Path,
    completion_manifests: Sequence[Path],
    return_manifest: Optional[Path],
    dry_run: bool,
) -> Dict[str, Any]:
    record_path, record = load_active_record(record_id, registry_dir)
    if not completion_manifests:
        raise LineageError("at least one Action completion manifest is required")

    action_chain: List[Dict[str, Any]] = []
    for index, path in enumerate(completion_manifests, start=1):
        completion = load_json_object(path, label="Action completion manifest")
        if completion.get("status") != "completed":
            raise LineageError(f"Action completion is not completed: {path}")
        pipeline = completion.get("pipeline")
        run_id = completion.get("run_id")
        if not isinstance(pipeline, str) or not pipeline:
            raise LineageError(f"Action completion is missing pipeline: {path}")
        if not isinstance(run_id, str) or not run_id:
            raise LineageError(f"Action completion is missing run_id: {path}")
        action_chain.append(
            {
                "step_index": index,
                "pipeline": pipeline,
                "pipeline_version": completion.get("pipeline_version"),
                "run_id": run_id,
                "status": "completed",
                "completion_manifest": str(path),
            }
        )

    artifacts: List[Dict[str, str]] = []
    if return_manifest is not None:
        return_record = load_json_object(return_manifest, label="Action return manifest")
        if return_record.get("status") != "ready_for_data_discovery":
            raise LineageError("Action return manifest is not ready_for_data_discovery")
        artifacts.append({"kind": "action_return_manifest", "path": str(return_manifest)})

    record["action_chain"] = action_chain
    record["artifacts"] = artifacts
    record["status"] = "completed"
    record["finalized_at_utc"] = utc_now_z()
    if dry_run:
        return record
    write_json_atomic(record_path, record)
    write_current_pointer(registry_dir, record)
    return record


def fail_registration(
    *,
    record_id: str,
    registry_dir: Path,
    reason: str,
    failure_manifest: Optional[Path],
    dry_run: bool,
) -> Dict[str, Any]:
    record_path, record = load_active_record(record_id, registry_dir)
    if not reason.strip():
        raise LineageError("failure reason must be non-empty")
    failure: Dict[str, Any] = {"reason": reason.strip(), "manifest": None}
    if failure_manifest is not None:
        load_json_object(failure_manifest, label="failure manifest")
        failure["manifest"] = str(failure_manifest)
    record["status"] = "failed"
    record["failed_at_utc"] = utc_now_z()
    record["failure"] = failure
    if dry_run:
        return record
    write_json_atomic(record_path, record)
    write_current_pointer(registry_dir, record)
    return record


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Register an Action lineage lifecycle.")
    parser.add_argument("--project-root", default="", help="Optional project root override.")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    open_parser = subparsers.add_parser("open", help="Create a ready_for_action record.")
    open_parser.add_argument("--asset-manifest", required=True)
    open_parser.add_argument("--evidence", nargs="*", default=[])
    open_parser.add_argument("--registry-dir", default=str(DEFAULT_REGISTRY_DIR))
    open_parser.add_argument("--dry-run", action="store_true")

    finalize_parser = subparsers.add_parser("finalize", help="Complete an Action record.")
    finalize_parser.add_argument("--record-id", required=True)
    finalize_parser.add_argument("--completion-manifest", action="append", default=[])
    finalize_parser.add_argument("--return-manifest", default="")
    finalize_parser.add_argument("--registry-dir", default=str(DEFAULT_REGISTRY_DIR))
    finalize_parser.add_argument("--dry-run", action="store_true")

    fail_parser = subparsers.add_parser("fail", help="Freeze an Action record as failed.")
    fail_parser.add_argument("--record-id", required=True)
    fail_parser.add_argument("--reason", required=True)
    fail_parser.add_argument("--failure-manifest", default="")
    fail_parser.add_argument("--registry-dir", default=str(DEFAULT_REGISTRY_DIR))
    fail_parser.add_argument("--dry-run", action="store_true")
    return parser


def execute(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = find_project_root(args.project_root)
    registry_dir = resolve_path(args.registry_dir, project_root=project_root)
    require_within(registry_dir, project_root, label="registry directory")

    if args.mode == "open":
        asset_manifest = resolve_path(args.asset_manifest, project_root=project_root)
        require_within(asset_manifest, project_root, label="asset manifest")
        evidence = [
            parse_kind_path(item, project_root=project_root)
            for item in args.evidence
        ]
        return open_registration(
            asset_manifest_path=asset_manifest,
            evidence_items=evidence,
            registry_dir=registry_dir,
            dry_run=bool(args.dry_run),
        )

    if args.mode == "finalize":
        completions = [resolve_path(raw, project_root=project_root) for raw in args.completion_manifest]
        for path in completions:
            require_within(path, project_root, label="completion manifest")
        return_manifest = resolve_path(args.return_manifest, project_root=project_root) if args.return_manifest else None
        if return_manifest is not None:
            require_within(return_manifest, project_root, label="return manifest")
        return finalize_registration(
            record_id=args.record_id,
            registry_dir=registry_dir,
            completion_manifests=completions,
            return_manifest=return_manifest,
            dry_run=bool(args.dry_run),
        )

    failure_manifest = resolve_path(args.failure_manifest, project_root=project_root) if args.failure_manifest else None
    if failure_manifest is not None:
        require_within(failure_manifest, project_root, label="failure manifest")
    return fail_registration(
        record_id=args.record_id,
        registry_dir=registry_dir,
        reason=args.reason,
        failure_manifest=failure_manifest,
        dry_run=bool(args.dry_run),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = execute(args)
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(dumps_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
