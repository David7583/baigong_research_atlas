#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: canonical_ingress_contract_validator_v0002.py
# 中文名: 完整入口规范数据契约校验脚本
# 版本号: v0002
#
# 主层级: system
# 层级: adapters / ingress / contract
# 脚本定位: 系统入口规范对话载荷的严格纯校验边界
#
# 职责说明:
# - 完整校验 canonical_conversation_ingress_v0001 字段、类型、身份和图引用
# - 汇总对话、节点、消息、内容部件和时间事实数量
# - 对文件入口逐对话校验，避免完整规范载荷常驻内存
#
# 本脚本做什么:
# - 使用 Python 标准库执行不依赖 Provider 字段名的严格契约校验
# - 可选原子写出机器可读校验报告
#
# 本脚本不做什么:
# - 不识别外部格式，不转换数据，不补造字段
# - 不修改输入，不写数据库，不执行补充标注
#
# 制度边界声明:
# - 任一必需字段、稳定身份或图引用失败都必须准确拒绝
# - dry-run 完成全部校验但不写报告
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: canonical_ingress_contract_validator_v0002
# family: canonical_ingress_contract_validator
# role: canonical_ingress_contract_validator
# version: v0002
# status: experimental
# entry_point: scripts/adapters/ingress/canonical_ingress_contract_validator_v0002.py
# input:
#   - canonical_conversation_ingress_v0001 JSON
# output:
#   - canonical ingress validation report JSON
# depends_on:
#   - Python standard library
#   - scripts/json_stream_v0001.py
# used_by:
#   - canonical_ingress_gateway_v0003
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "canonical_ingress_contract_validator"
SCRIPT_NAME = "canonical_ingress_contract_validator_v0002.py"
SCRIPT_VERSION = "v0002"
CONTRACT_SCHEMA_VERSION = "canonical_conversation_ingress_v0001"
CONTRACT_VERSION = "v0001"
SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
ALLOWED_ROLES = frozenset({"user", "assistant", "system", "tool", "developer", "unknown"})
ALLOWED_PART_TYPES = frozenset({
    "text", "image", "audio", "file", "tool_call", "tool_result", "code", "structured", "unknown",
})
ALLOWED_MAPPING_METHODS = frozenset({
    "direct", "renamed", "normalized", "derived_from_sequence", "synthesized_stable_id", "preserved_extension",
})
ALLOWED_LOSS_STATUSES = frozenset({"lossless", "represented_in_extension", "degraded", "unmapped"})
ALLOWED_RAW_TYPES = frozenset({"unix_seconds", "iso8601", "string", "integer", "decimal", "unknown"})
ALLOWED_SEMANTIC_STATUSES = frozenset({"verified", "unverified", "ambiguous", "invalid", "missing"})


# ============================================================
# 异常类型
# ============================================================

class CanonicalContractError(RuntimeError):
    """Raised when canonical ingress data violates the complete contract."""


class CanonicalContractInputError(CanonicalContractError):
    """Raised when the input file cannot be read as JSON."""


# ============================================================
# 数据结构
# ============================================================

@dataclass
class ContractCounts:
    conversations: int = 0
    nodes: int = 0
    messages: int = 0
    content_parts: int = 0
    event_times: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "conversations": self.conversations,
            "nodes": self.nodes,
            "messages": self.messages,
            "content_parts": self.content_parts,
            "event_times": self.event_times,
        }


# ============================================================
# 工具函数区
# ============================================================

def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding=DEFAULT_ENCODING, newline="\n") as handle:
            handle.write(json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary_name).replace(path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def _load_json(path: Path) -> Any:
    if not path.is_file():
        raise CanonicalContractInputError(f"canonical input is not a file: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanonicalContractInputError(f"canonical input is invalid JSON: {exc}") from exc


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CanonicalContractError(f"{label} must be an object")
    return value


def _require_exact_keys(value: Mapping[str, Any], required: Iterable[str], label: str) -> None:
    required_set = set(required)
    actual = set(value)
    missing = sorted(required_set - actual)
    extra = sorted(actual - required_set)
    if missing:
        raise CanonicalContractError(f"{label} is missing fields: {missing}")
    if extra:
        raise CanonicalContractError(f"{label} contains unsupported fields: {extra}")


def _require_string(value: Any, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise CanonicalContractError(f"{label} must be {'a string' if allow_empty else 'a non-empty string'}")
    return value


def _require_nullable_string(value: Any, label: str) -> Optional[str]:
    if value is not None and not isinstance(value, str):
        raise CanonicalContractError(f"{label} must be a string or null")
    return value


def _require_nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CanonicalContractError(f"{label} must be a non-negative integer")
    return value


def _require_list(value: Any, label: str, *, non_empty: bool = False) -> List[Any]:
    if not isinstance(value, list) or (non_empty and not value):
        raise CanonicalContractError(f"{label} must be {'a non-empty list' if non_empty else 'a list'}")
    return value


def _require_utc(value: Any, label: str) -> str:
    text = _require_string(value, label)
    if not text.endswith("Z"):
        raise CanonicalContractError(f"{label} must use UTC Z notation")
    try:
        datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise CanonicalContractError(f"{label} is not a valid ISO-8601 UTC timestamp") from exc
    return text


def _require_sha256(value: Any, label: str) -> str:
    text = _require_string(value, label)
    if SHA256_PATTERN.fullmatch(text) is None:
        raise CanonicalContractError(f"{label} must be a lowercase SHA-256 hex string")
    return text


# ============================================================
# 默认映射
# ============================================================

ROOT_FIELDS = (
    "schema_version", "dataset_id", "source_envelope_id", "canonicalized_at",
    "contract_version", "conversations", "extensions",
)
CONVERSATION_FIELDS = (
    "conversation_id", "title", "created_time", "updated_time", "current_node_id",
    "nodes", "metadata", "source_ref", "extensions",
)
NODE_FIELDS = (
    "node_id", "parent_node_id", "child_node_ids", "node_order", "message", "source_ref", "extensions",
)
MESSAGE_FIELDS = (
    "message_id", "author", "content", "status", "recipient", "channel", "event_times",
    "metadata", "source_ref", "extensions",
)
AUTHOR_FIELDS = ("role", "name", "source_role", "metadata")
CONTENT_FIELDS = ("content_type", "source_content_type", "parts", "metadata", "extensions")
PART_FIELDS = (
    "part_id", "part_index", "part_type", "text", "mime_type", "uri", "payload", "source_ref", "extensions",
)
EVENT_TIME_FIELDS = (
    "event_kind", "raw_value", "raw_type", "normalized_utc", "semantic_status", "semantic_basis", "source_ref",
)
SOURCE_REF_FIELDS = (
    "source_file_id", "source_path", "source_index", "source_id", "source_value_sha256",
    "mapping_method", "loss_status",
)


# ============================================================
# 核心契约
# ============================================================

def _validate_source_ref(value: Any, label: str, source_file_ids: Set[str]) -> Dict[str, Any]:
    row = _require_mapping(value, label)
    _require_exact_keys(row, SOURCE_REF_FIELDS, label)
    source_file_id = _require_string(row["source_file_id"], f"{label}.source_file_id")
    source_file_ids.add(source_file_id)
    _require_string(row["source_path"], f"{label}.source_path", allow_empty=True)
    if row["source_index"] is not None:
        _require_nonnegative_integer(row["source_index"], f"{label}.source_index")
    _require_nullable_string(row["source_id"], f"{label}.source_id")
    _require_sha256(row["source_value_sha256"], f"{label}.source_value_sha256")
    if row["mapping_method"] not in ALLOWED_MAPPING_METHODS:
        raise CanonicalContractError(f"{label}.mapping_method is invalid")
    if row["loss_status"] not in ALLOWED_LOSS_STATUSES:
        raise CanonicalContractError(f"{label}.loss_status is invalid")
    return dict(row)


def _validate_event_time(value: Any, label: str, source_file_ids: Set[str]) -> None:
    row = _require_mapping(value, label)
    _require_exact_keys(row, EVENT_TIME_FIELDS, label)
    _require_string(row["event_kind"], f"{label}.event_kind")
    raw = row["raw_value"]
    if isinstance(raw, bool) or raw is not None and not isinstance(raw, (str, int, float)):
        raise CanonicalContractError(f"{label}.raw_value has unsupported type")
    if row["raw_type"] not in ALLOWED_RAW_TYPES:
        raise CanonicalContractError(f"{label}.raw_type is invalid")
    if row["normalized_utc"] is not None:
        _require_utc(row["normalized_utc"], f"{label}.normalized_utc")
    if row["semantic_status"] not in ALLOWED_SEMANTIC_STATUSES:
        raise CanonicalContractError(f"{label}.semantic_status is invalid")
    _require_string(row["semantic_basis"], f"{label}.semantic_basis", allow_empty=True)
    _validate_source_ref(row["source_ref"], f"{label}.source_ref", source_file_ids)


def _validate_message(
    value: Any,
    label: str,
    source_file_ids: Set[str],
    message_ids: Set[str],
    counts: ContractCounts,
) -> None:
    row = _require_mapping(value, label)
    _require_exact_keys(row, MESSAGE_FIELDS, label)
    message_id = _require_string(row["message_id"], f"{label}.message_id")
    if message_id in message_ids:
        raise CanonicalContractError(f"duplicate message_id in dataset: {message_id}")
    message_ids.add(message_id)
    author = _require_mapping(row["author"], f"{label}.author")
    _require_exact_keys(author, AUTHOR_FIELDS, f"{label}.author")
    if author["role"] not in ALLOWED_ROLES:
        raise CanonicalContractError(f"{label}.author.role is invalid")
    _require_nullable_string(author["name"], f"{label}.author.name")
    _require_string(author["source_role"], f"{label}.author.source_role")
    _require_mapping(author["metadata"], f"{label}.author.metadata")
    content = _require_mapping(row["content"], f"{label}.content")
    _require_exact_keys(content, CONTENT_FIELDS, f"{label}.content")
    _require_string(content["content_type"], f"{label}.content.content_type")
    _require_nullable_string(content["source_content_type"], f"{label}.content.source_content_type")
    parts = _require_list(content["parts"], f"{label}.content.parts")
    part_ids: Set[str] = set()
    for index, item in enumerate(parts):
        part_label = f"{label}.content.parts[{index}]"
        part = _require_mapping(item, part_label)
        _require_exact_keys(part, PART_FIELDS, part_label)
        part_id = _require_string(part["part_id"], f"{part_label}.part_id")
        if part_id in part_ids:
            raise CanonicalContractError(f"duplicate part_id in message {message_id}: {part_id}")
        part_ids.add(part_id)
        if _require_nonnegative_integer(part["part_index"], f"{part_label}.part_index") != index:
            raise CanonicalContractError(f"{part_label}.part_index must equal its list position")
        if part["part_type"] not in ALLOWED_PART_TYPES:
            raise CanonicalContractError(f"{part_label}.part_type is invalid")
        _require_nullable_string(part["text"], f"{part_label}.text")
        if part["part_type"] == "text" and part["text"] is None:
            raise CanonicalContractError(f"{part_label}.text must not be null for text parts")
        _require_nullable_string(part["mime_type"], f"{part_label}.mime_type")
        _require_nullable_string(part["uri"], f"{part_label}.uri")
        _validate_source_ref(part["source_ref"], f"{part_label}.source_ref", source_file_ids)
        _require_mapping(part["extensions"], f"{part_label}.extensions")
        counts.content_parts += 1
    _require_mapping(content["metadata"], f"{label}.content.metadata")
    _require_mapping(content["extensions"], f"{label}.content.extensions")
    _require_nullable_string(row["status"], f"{label}.status")
    _require_nullable_string(row["recipient"], f"{label}.recipient")
    _require_nullable_string(row["channel"], f"{label}.channel")
    event_times = _require_list(row["event_times"], f"{label}.event_times")
    event_keys: Set[Tuple[str, str]] = set()
    for index, item in enumerate(event_times):
        event_label = f"{label}.event_times[{index}]"
        _validate_event_time(item, event_label, source_file_ids)
        event = _require_mapping(item, event_label)
        key = (str(event["event_kind"]), _canonical_json(event["source_ref"]))
        if key in event_keys:
            raise CanonicalContractError(f"duplicate event-time fact in {label}: {key[0]}")
        event_keys.add(key)
        counts.event_times += 1
    _require_mapping(row["metadata"], f"{label}.metadata")
    _validate_source_ref(row["source_ref"], f"{label}.source_ref", source_file_ids)
    _require_mapping(row["extensions"], f"{label}.extensions")
    counts.messages += 1


def _validate_conversation(
    value: Any,
    index: int,
    conversation_ids: Set[str],
    message_ids: Set[str],
    source_file_ids: Set[str],
    counts: ContractCounts,
) -> None:
    label = f"conversations[{index}]"
    row = _require_mapping(value, label)
    _require_exact_keys(row, CONVERSATION_FIELDS, label)
    conversation_id = _require_string(row["conversation_id"], f"{label}.conversation_id")
    if conversation_id in conversation_ids:
        raise CanonicalContractError(f"duplicate conversation_id: {conversation_id}")
    conversation_ids.add(conversation_id)
    _require_nullable_string(row["title"], f"{label}.title")
    for field in ("created_time", "updated_time"):
        if row[field] is not None:
            _validate_event_time(row[field], f"{label}.{field}", source_file_ids)
            counts.event_times += 1
    current_node_id = _require_nullable_string(row["current_node_id"], f"{label}.current_node_id")
    nodes = _require_list(row["nodes"], f"{label}.nodes")
    node_ids: Set[str] = set()
    node_rows: Dict[str, Mapping[str, Any]] = {}
    node_orders: Set[int] = set()
    for node_index, item in enumerate(nodes):
        node_label = f"{label}.nodes[{node_index}]"
        node = _require_mapping(item, node_label)
        _require_exact_keys(node, NODE_FIELDS, node_label)
        node_id = _require_string(node["node_id"], f"{node_label}.node_id")
        if node_id in node_ids:
            raise CanonicalContractError(f"duplicate node_id in conversation {conversation_id}: {node_id}")
        node_ids.add(node_id)
        node_rows[node_id] = node
        order = _require_nonnegative_integer(node["node_order"], f"{node_label}.node_order")
        if order in node_orders or order != node_index:
            raise CanonicalContractError(f"{node_label}.node_order must be unique and equal its list position")
        node_orders.add(order)
        _require_nullable_string(node["parent_node_id"], f"{node_label}.parent_node_id")
        children = _require_list(node["child_node_ids"], f"{node_label}.child_node_ids")
        child_ids = [_require_string(child, f"{node_label}.child_node_ids item") for child in children]
        if len(child_ids) != len(set(child_ids)):
            raise CanonicalContractError(f"{node_label}.child_node_ids contains duplicates")
        _validate_source_ref(node["source_ref"], f"{node_label}.source_ref", source_file_ids)
        _require_mapping(node["extensions"], f"{node_label}.extensions")
        if node["message"] is not None:
            _validate_message(node["message"], f"{node_label}.message", source_file_ids, message_ids, counts)
        counts.nodes += 1
    if not nodes and current_node_id is not None:
        raise CanonicalContractError(f"{label}.current_node_id must be null when nodes is empty")
    if current_node_id is not None and current_node_id not in node_ids:
        raise CanonicalContractError(f"{label}.current_node_id does not reference an existing node")
    roots = 0
    for node_id, node in node_rows.items():
        parent = node["parent_node_id"]
        if parent is None:
            roots += 1
        else:
            if parent not in node_ids:
                raise CanonicalContractError(f"node {node_id} parent does not exist: {parent}")
            if node_id not in node_rows[parent]["child_node_ids"]:
                raise CanonicalContractError(f"node {node_id} parent/child relationship is not symmetric")
        if node_id in node["child_node_ids"]:
            raise CanonicalContractError(f"node {node_id} must not reference itself")
        for child in node["child_node_ids"]:
            if child not in node_ids:
                raise CanonicalContractError(f"node {node_id} child does not exist: {child}")
            if node_rows[child]["parent_node_id"] != node_id:
                raise CanonicalContractError(f"node {node_id} child/parent relationship is not symmetric: {child}")
    if nodes and roots < 1:
        raise CanonicalContractError(f"conversation {conversation_id} has no root node")
    _require_mapping(row["metadata"], f"{label}.metadata")
    _validate_source_ref(row["source_ref"], f"{label}.source_ref", source_file_ids)
    _require_mapping(row["extensions"], f"{label}.extensions")
    counts.conversations += 1


def validate_canonical_payload(payload: Any) -> Dict[str, Any]:
    root = _require_mapping(payload, "canonical root")
    _require_exact_keys(root, ROOT_FIELDS, "canonical root")
    if root["schema_version"] != CONTRACT_SCHEMA_VERSION:
        raise CanonicalContractError(f"schema_version must be {CONTRACT_SCHEMA_VERSION}")
    _require_string(root["dataset_id"], "dataset_id")
    _require_string(root["source_envelope_id"], "source_envelope_id")
    _require_utc(root["canonicalized_at"], "canonicalized_at")
    if root["contract_version"] != CONTRACT_VERSION:
        raise CanonicalContractError(f"contract_version must be {CONTRACT_VERSION}")
    conversations = _require_list(root["conversations"], "conversations", non_empty=True)
    _require_mapping(root["extensions"], "extensions")
    counts = ContractCounts()
    conversation_ids: Set[str] = set()
    message_ids: Set[str] = set()
    source_file_ids: Set[str] = set()
    for index, conversation in enumerate(conversations):
        _validate_conversation(
            conversation, index, conversation_ids, message_ids, source_file_ids, counts,
        )
    return {
        "status": "passed",
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "counts": counts.as_dict(),
        "conversation_ids_unique": True,
        "message_ids_unique": True,
        "graph_integrity": True,
        "source_file_ids": sorted(source_file_ids),
    }


def _load_stream_module() -> Any:
    for parent in Path(__file__).resolve().parents:
        candidates = [parent / "scripts" / "json_stream_v0001.py"]
        if parent.name == "scripts":
            candidates.append(parent / "json_stream_v0001.py")
        for candidate in candidates:
            if not candidate.is_file():
                continue
            spec = importlib.util.spec_from_file_location("json_stream_validator_runtime_v0001", candidate)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            return module
    raise CanonicalContractInputError("json_stream_v0001.py cannot be located")


def validate_file_streaming(input_path: Path) -> Dict[str, Any]:
    stream_module = _load_stream_module()
    inspection = stream_module.inspect_object_array(input_path, "conversations")
    metadata = inspection["metadata"]
    root_keys = set(metadata) | {"conversations"}
    if root_keys != set(ROOT_FIELDS):
        missing = sorted(set(ROOT_FIELDS) - root_keys)
        extra = sorted(root_keys - set(ROOT_FIELDS))
        raise CanonicalContractError(f"canonical root key mismatch: missing={missing}, extra={extra}")
    if metadata["schema_version"] != CONTRACT_SCHEMA_VERSION:
        raise CanonicalContractError(f"schema_version must be {CONTRACT_SCHEMA_VERSION}")
    _require_string(metadata["dataset_id"], "dataset_id")
    _require_string(metadata["source_envelope_id"], "source_envelope_id")
    _require_utc(metadata["canonicalized_at"], "canonicalized_at")
    if metadata["contract_version"] != CONTRACT_VERSION:
        raise CanonicalContractError(f"contract_version must be {CONTRACT_VERSION}")
    _require_mapping(metadata["extensions"], "extensions")
    if int(inspection["count"]) < 1:
        raise CanonicalContractError("conversations must be a non-empty list")

    counts = ContractCounts()
    conversation_ids: Set[str] = set()
    message_ids: Set[str] = set()
    source_file_ids: Set[str] = set()
    observed = 0
    for index, conversation in stream_module.iter_object_array(input_path, "conversations"):
        _validate_conversation(
            conversation,
            index,
            conversation_ids,
            message_ids,
            source_file_ids,
            counts,
        )
        observed += 1
    if observed != int(inspection["count"]):
        raise CanonicalContractError(
            f"streaming validation count mismatch: inspected={inspection['count']} validated={observed}"
        )
    return {
        "status": "passed",
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "counts": counts.as_dict(),
        "conversation_ids_unique": True,
        "message_ids_unique": True,
        "graph_integrity": True,
        "source_file_ids": sorted(source_file_ids),
        "streaming": {
            "enabled": True,
            "passes": 2,
            "maximum_resident_scope": "single_conversation_plus_decoder_buffer",
        },
    }


def validate_file(input_path: Path) -> Dict[str, Any]:
    result = validate_file_streaming(input_path)
    result["input_sha256"] = file_sha256(input_path)
    result["input_size_bytes"] = input_path.stat().st_size
    return result


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def contract_summary() -> Dict[str, Any]:
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "strict_additional_properties": True,
        "provider_independent": True,
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the complete canonical conversation ingress contract.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--report", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        input_path = Path(args.input).resolve()
        result = validate_file(input_path)
        result["status"] = "dry_run" if args.dry_run else "passed"
        if args.report and not args.dry_run:
            report_path = Path(args.report).resolve()
            if report_path == input_path:
                raise CanonicalContractInputError("report must not overwrite canonical input")
            _atomic_write_json(report_path, result)
            result["report"] = str(report_path)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except CanonicalContractError as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
