#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: source_access_gateway_v0001.py
# 中文名: 补充标注已解析来源访问门禁脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / framework
# 脚本定位: 结构解析完成后为需要回看来源的补充标注插件生成最小授权范围
#
# 职责说明:
# - 校验 structural completion、入口信封、working copy 和 language units 属于同一处理链
# - 仅从已完成结构解析的 language units 生成来源坐标允许集
#
# 本脚本做什么:
# - 使用 Python 标准库产生 source access manifest 和 allowed coordinates JSONL
# - 将结构解析完成证明变成所有来源回看插件可复用的公共接口
#
# 本脚本不做什么:
# - 不提取时间或其他标注，不执行插件，不写数据库
# - 不允许请求者越过 working copy 直接改指最初 data_raw 或其他批次
#
# 制度边界声明:
# - status=authorized 只能在全部门禁通过且允许坐标非空时生成
# - dry-run 完成全部校验和坐标构建，但不写任何授权产物
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: source_access_gateway_v0001
# family: source_access_gateway
# role: supplemental_annotation_source_access_gateway
# version: v0001
# status: experimental
# entry_point: scripts/action/native_plugin/supplemental_annotation/framework/source_access_gateway_v0001.py
# input:
#   - canonical ingress envelope
#   - structural completion manifest and its exact working copy
#   - structural language text units JSONL
# output:
#   - supplemental annotation source access manifest and allowed coordinates JSONL
# depends_on:
#   - Python standard library
# used_by:
#   - supplemental_annotation_runner_v0002
#   - data_action_chain_pipeline_v0008
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "source_access_gateway"
SCRIPT_NAME = "source_access_gateway_v0001.py"
SCRIPT_VERSION = "v0001"
ACCESS_SCHEMA_VERSION = "supplemental_annotation_source_access_v0001"
ENVELOPE_SCHEMA_VERSION = "canonical_ingress_envelope_v0001"
STRUCTURAL_PIPELINE = "structural_unit_governance_graph_pipeline"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
STABLE_COORDINATE_FIELDS = (
    "asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end",
)
SOURCE_ALLOW_FIELDS = ("asset_id", "path", "value_index", "source_value_sha1")
ALLOWED_PURPOSES = frozenset({"source_fact_annotation", "source_relation_annotation", "source_evidence_validation"})


# ============================================================
# 异常类型
# ============================================================

class SourceAccessGatewayError(RuntimeError):
    """Base error for supplemental annotation source authorization."""


class SourceAccessContractError(SourceAccessGatewayError):
    """Raised when completion, envelope, source, or language units do not reconcile."""


# ============================================================
# 数据结构
# ============================================================

# Public source access artifacts are JSON/JSONL so every plugin language can
# enforce the same authorization without importing this Python module.


# ============================================================
# 工具函数区
# ============================================================

def _utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode(DEFAULT_ENCODING)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _find_project_root(start: Path) -> Path:
    for candidate in (start.resolve(), *start.resolve().parents):
        if (candidate / "AGENTS.md").is_file() and (candidate / "scripts").is_dir() and (candidate / "config").is_dir():
            return candidate
    raise SourceAccessContractError(f"cannot locate project root from: {start}")


def _resolve_from_root(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise SourceAccessContractError(f"{label} must remain within project root") from exc


def _relative(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError as exc:
        raise SourceAccessContractError(f"path must remain within project root: {path}") from exc


def _load_object(path: Path, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise SourceAccessContractError(f"{label} is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceAccessContractError(f"{label} is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SourceAccessContractError(f"{label} root must be an object")
    return payload


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding=DEFAULT_ENCODING, newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary_name).replace(path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _atomic_write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    _atomic_write_text(path, "".join(_canonical_json(dict(row)) + "\n" for row in rows))


def _require_non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SourceAccessContractError(f"{label} must be a non-empty string")
    return value


def _require_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SourceAccessContractError(f"{label} must be an integer")
    return value


def _iter_language_coordinates(path: Path) -> Tuple[List[Dict[str, Any]], int, int]:
    rows: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str, int, str]] = set()
    assets: Set[str] = set()
    paths: Set[str] = set()
    with path.open("r", encoding=DEFAULT_ENCODING) as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SourceAccessContractError(f"language units line {line_number} is invalid JSON: {exc}") from exc
            if not isinstance(record, dict):
                raise SourceAccessContractError(f"language units line {line_number} must be an object")
            for field in STABLE_COORDINATE_FIELDS:
                if field not in record:
                    raise SourceAccessContractError(f"language units line {line_number} is missing stable coordinate: {field}")
            asset_id = _require_non_empty_string(record["asset_id"], f"line {line_number} asset_id")
            source_path = _require_non_empty_string(record["path"], f"line {line_number} path")
            value_index = _require_integer(record["value_index"], f"line {line_number} value_index")
            if value_index < 0:
                raise SourceAccessContractError(f"language units line {line_number} value_index must be non-negative")
            for field in ("segment_index", "sentence_index", "char_start", "char_end"):
                _require_integer(record[field], f"line {line_number} {field}")
            source_value_sha1 = _require_non_empty_string(record.get("source_value_sha1"), f"line {line_number} source_value_sha1")
            coordinate = (asset_id, source_path, value_index, source_value_sha1)
            if coordinate not in seen:
                seen.add(coordinate)
                rows.append({
                    "asset_id": asset_id,
                    "path": source_path,
                    "value_index": value_index,
                    "source_value_sha1": source_value_sha1,
                })
            assets.add(asset_id)
            paths.add(source_path)
    if not rows:
        raise SourceAccessContractError("language units produced no source coordinates")
    rows.sort(key=lambda row: (row["asset_id"], row["path"], row["value_index"], row["source_value_sha1"]))
    return rows, len(assets), len(paths)


# ============================================================
# 默认映射
# ============================================================

# No provider or source-format defaults belong in this gateway.


# ============================================================
# 核心门禁组件
# ============================================================

def authorize(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = _find_project_root(Path(__file__).resolve().parent)
    completion_path = _resolve_from_root(project_root, args.structural_completion)
    envelope_path = _resolve_from_root(project_root, args.ingress_envelope)
    requested_source = _resolve_from_root(project_root, args.source)
    language_units = _resolve_from_root(project_root, args.language_units)
    manifest_path = _resolve_from_root(project_root, args.manifest)
    allowed_path = _resolve_from_root(project_root, args.allowed_coordinates)
    for path, label in (
        (completion_path, "structural completion"),
        (envelope_path, "ingress envelope"),
        (requested_source, "requested source"),
        (language_units, "language units"),
        (manifest_path, "source access manifest"),
        (allowed_path, "allowed coordinates"),
    ):
        _require_within(path, project_root, label)
    if len({manifest_path, allowed_path, completion_path, envelope_path, requested_source, language_units}) != 6:
        raise SourceAccessContractError("source access inputs and outputs must be distinct files")
    if args.purpose not in ALLOWED_PURPOSES:
        raise SourceAccessContractError(f"unsupported source access purpose: {args.purpose}")
    requester_name = _require_non_empty_string(args.requester_name, "requester name")
    requester_version = _require_non_empty_string(args.requester_version, "requester version")
    completion = _load_object(completion_path, "structural completion")
    envelope = _load_object(envelope_path, "ingress envelope")
    if completion.get("pipeline") != STRUCTURAL_PIPELINE:
        raise SourceAccessContractError("completion pipeline is not structural_unit_governance_graph_pipeline")
    if completion.get("status") != "completed":
        raise SourceAccessContractError("structural completion status must be completed")
    pipeline_version = _require_non_empty_string(completion.get("pipeline_version"), "structural pipeline_version")
    structural_run_id = _require_non_empty_string(completion.get("run_id"), "structural run_id")
    source_inputs = completion.get("source_inputs")
    if not isinstance(source_inputs, Mapping):
        raise SourceAccessContractError("structural completion is missing source_inputs")
    working_copy_value = _require_non_empty_string(source_inputs.get("working_copy"), "structural working_copy")
    working_copy = Path(working_copy_value).resolve()
    _require_within(working_copy, project_root, "structural working copy")
    if working_copy != requested_source:
        raise SourceAccessContractError("requested source must be the exact structural working copy")
    if not requested_source.is_file():
        raise SourceAccessContractError(f"structural working copy is missing: {requested_source}")
    if envelope.get("schema_version") != ENVELOPE_SCHEMA_VERSION or envelope.get("status") != "completed":
        raise SourceAccessContractError("ingress envelope must be a completed canonical_ingress_envelope_v0001")
    canonical_output = envelope.get("canonical_output")
    if not isinstance(canonical_output, Mapping) or canonical_output.get("contract_valid") is not True:
        raise SourceAccessContractError("ingress envelope canonical output is not contract-valid")
    expected_canonical_hash = _require_non_empty_string(canonical_output.get("sha256"), "envelope canonical sha256")
    actual_source_hash = _sha256_file(requested_source)
    if actual_source_hash != expected_canonical_hash:
        raise SourceAccessContractError("structural working copy hash does not match ingress envelope canonical hash")
    business_dir_value = _require_non_empty_string(completion.get("business_run_dir"), "structural business_run_dir")
    business_dir = Path(business_dir_value).resolve()
    expected_language_root = (business_dir / "03_language_parse").resolve()
    _require_within(expected_language_root, project_root, "structural language root")
    try:
        language_units.relative_to(expected_language_root)
    except ValueError as exc:
        raise SourceAccessContractError("language units do not belong to the structural run") from exc
    if not language_units.is_file():
        raise SourceAccessContractError(f"language units are missing: {language_units}")
    coordinates, asset_count, path_count = _iter_language_coordinates(language_units)
    access_id = "sha256:" + _sha256_text(_canonical_json({
        "completion_sha256": _sha256_file(completion_path),
        "canonical_sha256": actual_source_hash,
        "language_sha256": _sha256_file(language_units),
        "requester": requester_name,
        "requester_version": requester_version,
        "purpose": args.purpose,
    }))
    if args.dry_run:
        return {
            "status": "dry_run",
            "access_id": access_id,
            "structural_run_id": structural_run_id,
            "requester": requester_name,
            "purpose": args.purpose,
            "allowed_coordinate_count": len(coordinates),
            "asset_count": asset_count,
            "path_count": path_count,
            "authorized": True,
        }
    if manifest_path.exists() or allowed_path.exists():
        raise SourceAccessContractError("source access outputs are append-only and must not already exist")
    _atomic_write_jsonl(allowed_path, coordinates)
    allowed_hash = _sha256_file(allowed_path)
    manifest = {
        "schema_version": ACCESS_SCHEMA_VERSION,
        "access_id": access_id,
        "created_at": _utc_now_z(),
        "status": "authorized",
        "requester": {"name": requester_name, "version": requester_version},
        "purpose": args.purpose,
        "canonical_source": {
            "project_relative_path": _relative(requested_source, project_root),
            "sha256": actual_source_hash,
        },
        "ingress_envelope": {
            "project_relative_path": _relative(envelope_path, project_root),
            "sha256": _sha256_file(envelope_path),
        },
        "structural_completion": {
            "project_relative_path": _relative(completion_path, project_root),
            "sha256": _sha256_file(completion_path),
            "pipeline": STRUCTURAL_PIPELINE,
            "pipeline_version": pipeline_version,
            "run_id": structural_run_id,
            "completion_status": "completed",
        },
        "language_units": {
            "project_relative_path": _relative(language_units, project_root),
            "sha256": _sha256_file(language_units),
            "record_count": len(coordinates),
            "asset_count": asset_count,
            "path_count": path_count,
        },
        "authorization": {
            "structural_completed": True,
            "working_copy_matches_envelope": True,
            "requested_source_matches_working_copy": True,
            "canonical_hash_matches": True,
            "language_units_belong_to_structural_run": True,
            "stable_coordinates_complete": True,
            "authorized": True,
        },
        "allowed_coordinates": {
            "project_relative_path": _relative(allowed_path, project_root),
            "sha256": allowed_hash,
            "record_count": len(coordinates),
            "coordinate_fields": list(SOURCE_ALLOW_FIELDS),
        },
    }
    _atomic_write_json(manifest_path, manifest)
    return {
        "status": "authorized",
        "access_id": access_id,
        "structural_run_id": structural_run_id,
        "requester": requester_name,
        "purpose": args.purpose,
        "source_access_manifest": str(manifest_path),
        "allowed_coordinates": str(allowed_path),
        "allowed_coordinates_sha256": allowed_hash,
        "allowed_coordinate_count": len(coordinates),
        "asset_count": asset_count,
        "path_count": path_count,
    }


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def source_access_contract() -> Dict[str, Any]:
    return {
        "schema_version": ACCESS_SCHEMA_VERSION,
        "structural_status_required": "completed",
        "requested_source_policy": "exact_structural_working_copy",
        "coordinate_fields": list(SOURCE_ALLOW_FIELDS),
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Authorize source access only for a completed structural parse and its parsed coordinates.")
    parser.add_argument("--structural-completion", required=True)
    parser.add_argument("--ingress-envelope", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--language-units", required=True)
    parser.add_argument("--requester-name", required=True)
    parser.add_argument("--requester-version", required=True)
    parser.add_argument("--purpose", required=True, choices=sorted(ALLOWED_PURPOSES))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allowed-coordinates", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = authorize(args)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except SourceAccessGatewayError as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
