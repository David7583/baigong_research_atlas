#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: extract_source_message_timestamps_v0003.py
# 中文名: 已授权规范来源消息时间提取脚本
# 版本号: v0003
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_timestamp
# 脚本定位: 仅从 Supplemental Annotation 来源门禁授权的规范工作副本恢复消息时间事实
#
# 职责说明:
# - 消费 source_access_manifest 和已解析坐标允许集，不接受任意来源路径
# - 将规范消息的 message_created 时间事实挂接到已解析文本来源坐标
#
# 本脚本做什么:
# - 保留原始时间值、原字段路径、语义状态、消息身份和文本 hash
# - 原子写出确定性标注 JSONL、issues 和运行 manifest
#
# 本脚本不做什么:
# - 不自行打开 data_raw，不绕过结构解析完成门禁，不提取未授权坐标
# - 不修改 text units，不写数据库，不从正文推断时间
#
# 制度边界声明:
# - source access manifest 、允许集、规范来源 hash 任一不一致都必须停止
# - 只有 semantic_status=verified 且具有 normalized_utc 的 message_created 才返回 resolved
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: extract_source_message_timestamps_v0003
# family: extract_source_message_timestamps
# role: authorized_source_message_timestamp_extractor
# version: v0003
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_timestamp_annotation/extract_source_message_timestamps_v0003.py
# input:
#   - authorized supplemental annotation source access manifest
#   - canonical conversation working copy and allowed coordinates referenced by that manifest
# output:
#   - source message timestamp annotations, issues, and extraction manifest
# depends_on:
#   - source_access_gateway_v0001
#   - Python standard library
# used_by:
#   - data_action_chain_pipeline_v0015
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
SCRIPT_FAMILY = "extract_source_message_timestamps"
SCRIPT_NAME = "extract_source_message_timestamps_v0003.py"
SCRIPT_VERSION = "v0003"
ACCESS_SCHEMA_VERSION = "supplemental_annotation_source_access_v0001"
CANONICAL_SCHEMA_VERSION = "canonical_conversation_ingress_v0001"
OUTPUT_SCHEMA_VERSION = "source_message_timestamp_annotation_v0002"
EVENT_TIME_KIND = "message_created"
CANONICAL_TEXT_PATH = "/conversations/*/nodes/*/message/content/parts/*/text"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


# ============================================================
# 异常类型
# ============================================================


class AuthorizedTimestampExtractionError(RuntimeError):
    """Raised when authorized source timestamp extraction cannot proceed safely."""


# ============================================================
# 数据结构
# ============================================================

# Coordinates and annotations are represented by versioned JSON mappings to
# remain compatible with the existing attachment and persistence scripts.


# ============================================================
# 工具函数区
# ============================================================


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode(DEFAULT_ENCODING)).hexdigest()


def _find_project_root(start: Path) -> Path:
    for candidate in (start.resolve(), *start.resolve().parents):
        if (
            (candidate / "AGENTS.md").is_file()
            and (candidate / "scripts").is_dir()
            and (candidate / "config").is_dir()
        ):
            return candidate
    raise AuthorizedTimestampExtractionError(f"cannot locate project root from: {start}")


def _resolve_from_root(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise AuthorizedTimestampExtractionError(f"{label} must remain within project root") from exc


def _load_object(path: Path, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise AuthorizedTimestampExtractionError(f"{label} is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuthorizedTimestampExtractionError(f"{label} is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise AuthorizedTimestampExtractionError(f"{label} root must be an object")
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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    _atomic_write_text(path, "".join(_canonical_json(dict(row)) + "\n" for row in rows))


def _load_allowed(path: Path) -> Tuple[Set[Tuple[str, str, int, str]], str]:
    allowed: Set[Tuple[str, str, int, str]] = set()
    with path.open("r", encoding=DEFAULT_ENCODING) as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise AuthorizedTimestampExtractionError(
                    f"allowed coordinates line {line_number} is invalid JSON: {exc}"
                ) from exc
            if not isinstance(row, dict):
                raise AuthorizedTimestampExtractionError(
                    f"allowed coordinates line {line_number} must be an object"
                )
            asset_id = row.get("asset_id")
            path_value = row.get("path")
            value_index = row.get("value_index")
            source_sha1 = row.get("source_value_sha1")
            if (
                not isinstance(asset_id, str)
                or not asset_id
                or not isinstance(path_value, str)
                or not path_value
            ):
                raise AuthorizedTimestampExtractionError(
                    f"allowed coordinates line {line_number} has invalid identity"
                )
            if isinstance(value_index, bool) or not isinstance(value_index, int) or value_index < 0:
                raise AuthorizedTimestampExtractionError(
                    f"allowed coordinates line {line_number} has invalid value_index"
                )
            if not isinstance(source_sha1, str) or not source_sha1:
                raise AuthorizedTimestampExtractionError(
                    f"allowed coordinates line {line_number} has invalid source_value_sha1"
                )
            coordinate = (asset_id, path_value, value_index, source_sha1)
            if coordinate in allowed:
                raise AuthorizedTimestampExtractionError(
                    f"allowed coordinates contain duplicate at line {line_number}"
                )
            allowed.add(coordinate)
    if not allowed:
        raise AuthorizedTimestampExtractionError("allowed coordinates must not be empty")
    asset_ids = sorted({item[0] for item in allowed})
    if len(asset_ids) != 1:
        raise AuthorizedTimestampExtractionError(
            "source timestamp extraction currently requires exactly one authorized asset"
        )
    return allowed, asset_ids[0]


def _select_created_fact(
    message: Mapping[str, Any]
) -> Tuple[Optional[Mapping[str, Any]], str, Optional[str]]:
    facts = [
        fact
        for fact in message.get("event_times", [])
        if isinstance(fact, Mapping) and fact.get("event_kind") == EVENT_TIME_KIND
    ]
    verified = [
        fact
        for fact in facts
        if fact.get("semantic_status") == "verified" and isinstance(fact.get("normalized_utc"), str)
    ]
    if len(verified) == 1:
        return verified[0], "resolved", None
    if len(verified) > 1:
        unique = {
            (str(fact.get("normalized_utc")), _canonical_json(fact.get("source_ref"))) for fact in verified
        }
        if len(unique) == 1:
            return verified[0], "resolved", None
        return None, "ambiguous", "multiple verified message_created facts disagree"
    if not facts:
        return None, "missing", None
    statuses = {str(fact.get("semantic_status")) for fact in facts}
    if "invalid" in statuses:
        return facts[0], "invalid", "message_created source value is invalid"
    return facts[0], "ambiguous", "message_created fact is not semantically verified"


# ============================================================
# 默认映射
# ============================================================

# The canonical text path and message_created event kind are contract fields,
# not provider-specific source paths.


# ============================================================
# 核心提取组件
# ============================================================


def extract_authorized(access_manifest_path: Path, project_root: Path) -> Dict[str, Any]:
    access = _load_object(access_manifest_path, "source access manifest")
    if access.get("schema_version") != ACCESS_SCHEMA_VERSION or access.get("status") != "authorized":
        raise AuthorizedTimestampExtractionError("source access manifest must be authorized v0001")
    authorization = access.get("authorization")
    if not isinstance(authorization, Mapping) or authorization.get("authorized") is not True:
        raise AuthorizedTimestampExtractionError("source access manifest authorization is not true")
    canonical_ref = access.get("canonical_source")
    allowed_ref = access.get("allowed_coordinates")
    if not isinstance(canonical_ref, Mapping) or not isinstance(allowed_ref, Mapping):
        raise AuthorizedTimestampExtractionError(
            "source access manifest is missing canonical source or allowed coordinates"
        )
    source_path = _resolve_from_root(project_root, str(canonical_ref.get("project_relative_path", "")))
    allowed_path = _resolve_from_root(project_root, str(allowed_ref.get("project_relative_path", "")))
    _require_within(source_path, project_root, "authorized canonical source")
    _require_within(allowed_path, project_root, "authorized coordinates")
    if _sha256_file(source_path) != canonical_ref.get("sha256"):
        raise AuthorizedTimestampExtractionError("authorized canonical source hash mismatch")
    if _sha256_file(allowed_path) != allowed_ref.get("sha256"):
        raise AuthorizedTimestampExtractionError("allowed coordinates hash mismatch")
    allowed, asset_id = _load_allowed(allowed_path)
    source = _load_object(source_path, "authorized canonical source")
    if source.get("schema_version") != CANONICAL_SCHEMA_VERSION:
        raise AuthorizedTimestampExtractionError(
            "authorized source is not canonical_conversation_ingress_v0001"
        )
    annotations: List[Dict[str, Any]] = []
    issues: List[Dict[str, Any]] = []
    status_counts = {"resolved": 0, "missing": 0, "invalid": 0, "ambiguous": 0}
    value_index = 0
    matching_allowed = {coordinate for coordinate in allowed if coordinate[1] == CANONICAL_TEXT_PATH}
    for conversation_index, conversation in enumerate(source.get("conversations", [])):
        if not isinstance(conversation, Mapping):
            continue
        for node_index, node in enumerate(conversation.get("nodes", [])):
            if not isinstance(node, Mapping):
                continue
            message = node.get("message")
            if not isinstance(message, Mapping):
                continue
            fact, status, detail = _select_created_fact(message)
            content = message.get("content")
            parts = content.get("parts") if isinstance(content, Mapping) else []
            for part_index, part in enumerate(parts if isinstance(parts, list) else []):
                if not isinstance(part, Mapping) or not isinstance(part.get("text"), str):
                    continue
                # Match parser coordinates for all string text fields, including code.
                text = str(part["text"])
                source_sha1 = _sha1_text(text)
                coordinate = (asset_id, CANONICAL_TEXT_PATH, value_index, source_sha1)
                if coordinate in matching_allowed:
                    source_ref = fact.get("source_ref") if isinstance(fact, Mapping) else None
                    source_ref = source_ref if isinstance(source_ref, Mapping) else {}
                    raw_time = fact.get("raw_value") if isinstance(fact, Mapping) else None
                    event_time = (
                        fact.get("normalized_utc")
                        if status == "resolved" and isinstance(fact, Mapping)
                        else None
                    )
                    identity_payload = {
                        "access_id": access.get("access_id"),
                        "asset_id": asset_id,
                        "target_path": CANONICAL_TEXT_PATH,
                        "value_index": value_index,
                        "node_id": node.get("node_id"),
                        "message_id": message.get("message_id"),
                        "event_time_raw": raw_time,
                        "event_time_kind": EVENT_TIME_KIND,
                        "event_time_source_path": source_ref.get("source_path"),
                    }
                    annotation = {
                        "schema_version": OUTPUT_SCHEMA_VERSION,
                        "annotation_id": "sha256:"
                        + _sha256_bytes(_canonical_json(identity_payload).encode(DEFAULT_ENCODING)),
                        "asset_id": asset_id,
                        "target_path": CANONICAL_TEXT_PATH,
                        "value_index": value_index,
                        "source_value_sha1": source_sha1,
                        "mapping_node_id": str(node.get("node_id")),
                        "message_id": str(message.get("message_id")),
                        "event_time_raw": raw_time,
                        "event_time": event_time,
                        "event_time_kind": EVENT_TIME_KIND,
                        "event_time_source_path": source_ref.get("source_path"),
                        "event_time_status": status,
                        "event_time_rule_version": "canonical_event_time_v0001",
                        "source_text_path": (
                            part.get("source_ref", {}).get("source_path")
                            if isinstance(part.get("source_ref"), Mapping)
                            else None
                        ),
                        "source_access_id": access.get("access_id"),
                        "canonical_text_path": f"/conversations/{conversation_index}/nodes/{node_index}/message/content/parts/{part_index}/text",
                    }
                    annotations.append(annotation)
                    status_counts[status] += 1
                    if detail:
                        issues.append(
                            {
                                "annotation_id": annotation["annotation_id"],
                                "status": status,
                                "detail": detail,
                                "source_path": annotation["event_time_source_path"],
                            }
                        )
                value_index += 1
    if not annotations or len(annotations) != len(matching_allowed):
        raise AuthorizedTimestampExtractionError("authorized text coordinate coverage mismatch")
    annotations.sort(
        key=lambda row: (row["asset_id"], row["target_path"], row["value_index"], row["annotation_id"])
    )
    issues.sort(key=lambda row: row["annotation_id"])
    return {
        "source_path": source_path,
        "allowed_path": allowed_path,
        "asset_id": asset_id,
        "annotations": annotations,
        "issues": issues,
        "status_counts": status_counts,
        "authorized_coordinate_count": len(allowed),
        "matching_text_coordinate_count": len(matching_allowed),
        "unused_matching_coordinates": len(matching_allowed) - len(annotations),
        "source_access_id": access.get("access_id"),
    }


# ============================================================
# Schema / 契约辅助函数
# ============================================================


def extraction_contract() -> Dict[str, Any]:
    return {
        "output_schema": OUTPUT_SCHEMA_VERSION,
        "access_schema": ACCESS_SCHEMA_VERSION,
        "canonical_schema": CANONICAL_SCHEMA_VERSION,
        "event_time_kind": EVENT_TIME_KIND,
        "target_path": CANONICAL_TEXT_PATH,
    }


# ============================================================
# CLI / main 接口区
# ============================================================


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract message timestamps only from a source authorized after structural parsing."
    )
    parser.add_argument("--source-access-manifest", required=True)
    parser.add_argument("--annotations", required=True)
    parser.add_argument("--issues", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if not RUN_ID_PATTERN.fullmatch(args.run_id or ""):
            raise AuthorizedTimestampExtractionError("run_id contains unsupported characters")
        project_root = _find_project_root(Path(__file__).resolve().parent)
        access_path = _resolve_from_root(project_root, args.source_access_manifest)
        annotations_path = _resolve_from_root(project_root, args.annotations)
        issues_path = _resolve_from_root(project_root, args.issues)
        manifest_path = _resolve_from_root(project_root, args.manifest)
        for path, label in (
            (access_path, "source access manifest"),
            (annotations_path, "annotations output"),
            (issues_path, "issues output"),
            (manifest_path, "extraction manifest"),
        ):
            _require_within(path, project_root, label)
        if len({access_path, annotations_path, issues_path, manifest_path}) != 4:
            raise AuthorizedTimestampExtractionError("source access input and outputs must be distinct")
        result = extract_authorized(access_path, project_root)
        if args.dry_run:
            output = {
                "status": "dry_run",
                "run_id": args.run_id,
                "source_access_id": result["source_access_id"],
                "records_created": len(result["annotations"]),
                "status_counts": result["status_counts"],
                "authorized_coordinate_count": result["authorized_coordinate_count"],
                "matching_text_coordinate_count": result["matching_text_coordinate_count"],
            }
            print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
            return 0
        if annotations_path.exists() or issues_path.exists() or manifest_path.exists():
            raise AuthorizedTimestampExtractionError(
                "timestamp extraction outputs are append-only and must not already exist"
            )
        _write_jsonl(annotations_path, result["annotations"])
        _write_jsonl(issues_path, result["issues"])
        manifest = {
            "schema_version": "source_timestamp_extraction_manifest_v0002",
            "run_id": args.run_id,
            "status": "completed",
            "completed_at": _utc_now_z(),
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "source_access_manifest": str(access_path),
            "source_access_manifest_sha256": _sha256_file(access_path),
            "source_access_id": result["source_access_id"],
            "authorized_source": str(result["source_path"]),
            "authorized_source_sha256": _sha256_file(result["source_path"]),
            "allowed_coordinates": str(result["allowed_path"]),
            "allowed_coordinates_sha256": _sha256_file(result["allowed_path"]),
            "authorized_coordinate_count": result["authorized_coordinate_count"],
            "matching_text_coordinate_count": result["matching_text_coordinate_count"],
            "records_created": len(result["annotations"]),
            "status_counts": result["status_counts"],
            "unused_matching_coordinates": result["unused_matching_coordinates"],
            "annotations": str(annotations_path),
            "annotations_sha256": _sha256_file(annotations_path),
            "issues": str(issues_path),
            "issues_sha256": _sha256_file(issues_path),
        }
        _write_json(manifest_path, manifest)
        output = {
            "status": "completed",
            "run_id": args.run_id,
            "source_access_id": result["source_access_id"],
            "source_sha256": manifest["authorized_source_sha256"],
            "annotations": str(annotations_path),
            "annotations_sha256": manifest["annotations_sha256"],
            "records_created": len(result["annotations"]),
            "status_counts": result["status_counts"],
            "authorized_coordinate_count": result["authorized_coordinate_count"],
        }
        print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except AuthorizedTimestampExtractionError as exc:
        print(
            json.dumps(
                {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
