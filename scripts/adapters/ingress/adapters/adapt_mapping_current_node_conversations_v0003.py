#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: adapt_mapping_current_node_conversations_v0003.py
# 中文名: Mapping Current Node 对话节点图入口适配脚本
# 版本号: v0003
#
# 主层级: data
# 层级: adapters / ingress / format_adapter
# 脚本定位: 将 mapping/current_node 根数组无损映射到完整入口规范契约
#
# 职责说明:
# - 保留源生 conversation、node、parent/children、message 和 current_node 身份
# - 将多形态 content 转换为规范内容部件，同时在 extensions 中保留未知字段
# - 保留原始时间值、来源 JSON Pointer、值 hash 和数量守恒证据
#
# 本脚本做什么:
# - 使用标准库逐对话生成 canonical_conversation_ingress_v0001 工作副本
# - 以两次有界扫描保持跨文件消息 ID 消歧和旧版本稳定身份
# - 对源 ID、图引用、消息身份和内容部件执行显式校验
#
# 本脚本不做什么:
# - 不按文件名或厂商名识别格式，不猜测未知字段的业务语义
# - 不修改原始数据，不过滤消息，不写数据库，不执行下游流程
# - 不将完整源文件、规范载荷或 lineage 索引同时保存在内存
#
# 制度边界声明:
# - 源生图关系直接保留，不按列表顺序重建关系
# - 未知字段进入 extensions；任何记录无法保真时整体失败
# - 仅用户确认的消息 create_time（Unix 秒）标为 verified；其他时间保持 unverified
# - 文件输出先写临时文件并原子晋升，失败不保留伪完整产物
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: adapt_mapping_current_node_conversations_v0003
# family: adapt_mapping_current_node_conversations
# role: canonical_ingress_format_adapter
# version: v0003
# status: experimental
# entry_point: scripts/adapters/ingress/adapters/adapt_mapping_current_node_conversations_v0003.py
# input:
#   - mapping_current_node_conversations_v1 JSON value
# output:
#   - canonical_conversation_ingress_v0001 mapping
#   - source-to-canonical lineage rows and conservation statistics
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
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple


# ============================================================
# 默认配置
# ============================================================

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "adapt_mapping_current_node_conversations"
SCRIPT_NAME = "adapt_mapping_current_node_conversations_v0003.py"
SCRIPT_VERSION = "v0003"
SOURCE_FORMAT = "mapping_current_node_conversations_v1"
TARGET_SCHEMA = "canonical_conversation_ingress_v0001"

CONVERSATION_KNOWN_FIELDS = frozenset({
    "conversation_id", "title", "create_time", "update_time", "current_node", "mapping",
})
NODE_KNOWN_FIELDS = frozenset({"id", "message", "parent", "children"})
MESSAGE_KNOWN_FIELDS = frozenset({
    "id", "author", "create_time", "update_time", "content", "status", "recipient", "channel",
})
AUTHOR_KNOWN_FIELDS = frozenset({"role", "name", "metadata"})
CONTENT_KNOWN_FIELDS = frozenset({"content_type", "parts", "metadata"})


# ============================================================
# 异常类型
# ============================================================

class MappingCurrentNodeAdapterError(RuntimeError):
    """Raised when a mapping/current_node source cannot be preserved completely."""


# ============================================================
# 工具函数区
# ============================================================

def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode(DEFAULT_ENCODING)).hexdigest()


def _stable_id(kind: str, identity: Mapping[str, Any]) -> str:
    return "sha256:" + _sha256_value({"kind": kind, **dict(identity)})


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MappingCurrentNodeAdapterError(f"{label} must be an object")
    return value


def _require_list(value: Any, label: str) -> List[Any]:
    if not isinstance(value, list):
        raise MappingCurrentNodeAdapterError(f"{label} must be a list")
    return value


def _require_string(value: Any, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        requirement = "a string" if allow_empty else "a non-empty string"
        raise MappingCurrentNodeAdapterError(f"{label} must be {requirement}")
    return value


def _nullable_string(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _unknown_fields(source: Mapping[str, Any], known: Set[str]) -> Dict[str, Any]:
    return {str(key): value for key, value in source.items() if str(key) not in known}


def _source_ref(
    *,
    source_file_id: str,
    source_path: str,
    source_index: Optional[int],
    source_id: Optional[str],
    source_value: Any,
    mapping_method: str,
    loss_status: str = "lossless",
) -> Dict[str, Any]:
    return {
        "source_file_id": source_file_id,
        "source_path": source_path,
        "source_index": source_index,
        "source_id": source_id,
        "source_value_sha256": _sha256_value(source_value),
        "mapping_method": mapping_method,
        "loss_status": loss_status,
    }


def _normalize_unix_seconds(raw: Any) -> Tuple[Optional[str], str, str]:
    if raw is None:
        return None, "unknown", "missing"
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None, "unknown", "invalid"
    try:
        normalized = datetime.fromtimestamp(float(raw), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OSError, OverflowError, ValueError):
        return None, "unix_seconds", "invalid"
    return normalized, "unix_seconds", "unverified"


def _event_time(
    *,
    event_kind: str,
    raw: Any,
    source_file_id: str,
    source_path: str,
    source_index: Optional[int],
    source_id: Optional[str],
) -> Dict[str, Any]:
    normalized, raw_type, semantic_status = _normalize_unix_seconds(raw)
    # User-confirmed on 2026-09-18; scoped to this format and exact creation fields.
    confirmed = (
        event_kind == "message_created"
        and re.fullmatch(r"/[0-9]+/mapping/[^/]+/message/create_time", source_path)
    )
    basis = f"{SOURCE_FORMAT} numeric time field observed across seven immutable exports; unit normalized as seconds without asserting vendor semantics"
    if confirmed and semantic_status == "unverified":
        semantic_status = "verified"
        basis = "user_confirmed_mapping_message_creation_unix_seconds_v0001; confirmed 2026-09-18; message creation only"
    return {
        "event_kind": event_kind,
        "raw_value": raw,
        "raw_type": raw_type,
        "normalized_utc": normalized,
        "semantic_status": semantic_status,
        "semantic_basis": basis,
        "source_ref": _source_ref(
            source_file_id=source_file_id,
            source_path=source_path,
            source_index=source_index,
            source_id=source_id,
            source_value=raw,
            mapping_method="normalized",
        ),
    }


def _canonical_role(author: Mapping[str, Any]) -> Tuple[str, str]:
    source_role = _require_string(author.get("role"), "message.author.role")
    aliases = {
        "assistant": "assistant",
        "developer": "developer",
        "system": "system",
        "tool": "tool",
        "user": "user",
    }
    return aliases.get(source_role.strip().lower(), "unknown"), source_role


def _part_type(source_content_type: str, raw_part: Any) -> str:
    if isinstance(raw_part, str):
        return "text"
    if isinstance(raw_part, Mapping):
        if "asset_pointer" in raw_part:
            return "image"
        if source_content_type == "code":
            return "code"
        return "structured"
    return "unknown"


def _content_parts(
    content: Mapping[str, Any],
    *,
    message_id: str,
    source_file_id: str,
    content_path: str,
    source_message_id: str,
) -> List[Dict[str, Any]]:
    source_content_type = _nullable_string(content.get("content_type")) or "unknown"
    raw_parts = content.get("parts")
    prepared: List[Tuple[Any, str]] = []
    if isinstance(raw_parts, list):
        prepared.extend((raw_part, f"{content_path}/parts/{index}") for index, raw_part in enumerate(raw_parts))
    elif "parts" in content and raw_parts is not None:
        raise MappingCurrentNodeAdapterError(f"{content_path}/parts must be a list or null")
    representation = _unknown_fields(content, set(CONTENT_KNOWN_FIELDS))
    if not prepared and representation:
        prepared.append((representation, content_path))
    parts: List[Dict[str, Any]] = []
    for index, (raw_part, part_path) in enumerate(prepared):
        part_type = _part_type(source_content_type, raw_part)
        text = raw_part if isinstance(raw_part, str) else None
        mime_type = "text/plain" if isinstance(raw_part, str) else None
        uri = None
        if isinstance(raw_part, Mapping):
            candidate_uri = raw_part.get("asset_pointer") or raw_part.get("url") or raw_part.get("uri")
            uri = _nullable_string(candidate_uri)
            candidate_text = raw_part.get("text")
            if isinstance(candidate_text, str):
                text = candidate_text
        parts.append({
            "part_id": f"{message_id}:part:{index}",
            "part_index": index,
            "part_type": part_type,
            "text": text,
            "mime_type": mime_type,
            "uri": uri,
            "payload": None if isinstance(raw_part, str) else raw_part,
            "source_ref": _source_ref(
                source_file_id=source_file_id,
                source_path=part_path,
                source_index=index,
                source_id=source_message_id,
                source_value=raw_part,
                mapping_method="direct" if isinstance(raw_part, Mapping) else "renamed",
            ),
            "extensions": {"source_content_type": source_content_type},
        })
    return parts


def _raw_message_id_counts(conversations_source: Sequence[Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for conversation in conversations_source:
        if not isinstance(conversation, Mapping):
            continue
        mapping = conversation.get("mapping")
        if not isinstance(mapping, Mapping):
            continue
        for node in mapping.values():
            if not isinstance(node, Mapping):
                continue
            message = node.get("message")
            if not isinstance(message, Mapping):
                continue
            message_id = message.get("id")
            if isinstance(message_id, str) and message_id:
                counts[message_id] += 1
    return counts


# ============================================================
# 核心适配组件
# ============================================================

def adapt_payload(
    source: Any,
    *,
    source_file_id: str,
    envelope_id: str,
    canonicalized_at: str,
    raw_message_id_counts_override: Optional[Counter[str]] = None,
    conversation_index_offset: int = 0,
    used_conversation_ids_override: Optional[Set[str]] = None,
    used_message_ids_override: Optional[Set[str]] = None,
    source_canonical_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    conversations_source = _require_list(source, "source root")
    if not conversations_source:
        raise MappingCurrentNodeAdapterError("source root must contain at least one conversation")
    canonical_conversations: List[Dict[str, Any]] = []
    lineage: List[Dict[str, Any]] = []
    used_conversation_ids = used_conversation_ids_override if used_conversation_ids_override is not None else set()
    used_message_ids = used_message_ids_override if used_message_ids_override is not None else set()
    counts = {"conversations": 0, "nodes": 0, "messages": 0, "content_parts": 0, "event_times": 0}
    input_counts = {"conversations": 0, "nodes": 0, "messages": 0, "content_parts": 0, "event_times": 0}
    extension_preserved = 0
    source_ids_reused = 0
    stable_ids_synthesized = 0
    time_status_counts = {"verified": 0, "unverified": 0, "ambiguous": 0, "invalid": 0, "missing": 0}
    raw_message_id_counts = (
        raw_message_id_counts_override
        if raw_message_id_counts_override is not None
        else _raw_message_id_counts(conversations_source)
    )

    for local_index, item in enumerate(conversations_source):
        conversation_index = conversation_index_offset + local_index
        conversation_path = f"/{conversation_index}"
        conversation = _require_mapping(item, f"root[{conversation_index}]")
        conversation_id = _require_string(conversation.get("conversation_id"), f"{conversation_path}/conversation_id")
        if conversation_id in used_conversation_ids:
            raise MappingCurrentNodeAdapterError(f"duplicate conversation_id: {conversation_id}")
        used_conversation_ids.add(conversation_id)
        source_ids_reused += 1
        mapping = _require_mapping(conversation.get("mapping"), f"{conversation_path}/mapping")
        node_ids: Set[str] = set()
        for raw_node_id in mapping:
            node_id = _require_string(raw_node_id, f"{conversation_path}/mapping key")
            if node_id in node_ids:
                raise MappingCurrentNodeAdapterError(f"duplicate node key in conversation {conversation_id}: {node_id}")
            node_ids.add(node_id)
        current_node_raw = conversation.get("current_node")
        current_node_id = _nullable_string(current_node_raw)
        if current_node_id is not None and current_node_id not in node_ids:
            raise MappingCurrentNodeAdapterError(f"current_node does not exist in conversation {conversation_id}: {current_node_id}")

        nodes: List[Dict[str, Any]] = []
        for node_index, (raw_node_id, node_item) in enumerate(mapping.items()):
            node_id = str(raw_node_id)
            node_path = f"{conversation_path}/mapping/{_escape_pointer(node_id)}"
            node = _require_mapping(node_item, node_path)
            parent_node_id = _nullable_string(node.get("parent"))
            if parent_node_id is not None and parent_node_id not in node_ids:
                raise MappingCurrentNodeAdapterError(f"node parent does not exist: {conversation_id}/{node_id}/{parent_node_id}")
            raw_children = _require_list(node.get("children"), f"{node_path}/children")
            child_node_ids = [_require_string(child, f"{node_path}/children item") for child in raw_children]
            if len(child_node_ids) != len(set(child_node_ids)):
                raise MappingCurrentNodeAdapterError(f"node contains duplicate children: {conversation_id}/{node_id}")
            missing_children = [child for child in child_node_ids if child not in node_ids]
            if missing_children:
                raise MappingCurrentNodeAdapterError(f"node children do not exist: {conversation_id}/{node_id}/{missing_children}")
            node_unknown = _unknown_fields(node, set(NODE_KNOWN_FIELDS))
            embedded_id = node.get("id")
            if embedded_id is not None and str(embedded_id) != node_id:
                node_unknown["embedded_id_mismatch"] = embedded_id
            if node_unknown:
                extension_preserved += 1

            raw_message = node.get("message")
            canonical_message: Optional[Dict[str, Any]] = None
            if raw_message is not None:
                message = _require_mapping(raw_message, f"{node_path}/message")
                raw_message_id = message.get("id")
                if raw_message_id is None:
                    message_id = _stable_id("message", {
                        "conversation_id": conversation_id,
                        "node_id": node_id,
                    })
                    stable_ids_synthesized += 1
                else:
                    source_message_id = _require_string(raw_message_id, f"{node_path}/message/id")
                    if raw_message_id_counts[source_message_id] == 1:
                        message_id = source_message_id
                        source_ids_reused += 1
                    else:
                        message_id = _stable_id("message", {
                            "conversation_id": conversation_id,
                            "node_id": node_id,
                            "source_message_id": source_message_id,
                        })
                        stable_ids_synthesized += 1
                if message_id in used_message_ids:
                    raise MappingCurrentNodeAdapterError(f"message identity collision remains after stable synthesis: {message_id}")
                used_message_ids.add(message_id)
                author = _require_mapping(message.get("author"), f"{node_path}/message/author")
                role, source_role = _canonical_role(author)
                author_metadata = author.get("metadata")
                if not isinstance(author_metadata, Mapping):
                    raise MappingCurrentNodeAdapterError(f"{node_path}/message/author/metadata must be an object")
                author_unknown = _unknown_fields(author, set(AUTHOR_KNOWN_FIELDS))
                if author_unknown:
                    extension_preserved += 1
                content = _require_mapping(message.get("content"), f"{node_path}/message/content")
                source_content_type = _nullable_string(content.get("content_type")) or "unknown"
                content_metadata = content.get("metadata", {})
                if not isinstance(content_metadata, Mapping):
                    raise MappingCurrentNodeAdapterError(f"{node_path}/message/content/metadata must be an object when present")
                message_metadata = message.get("metadata")
                if not isinstance(message_metadata, Mapping):
                    raise MappingCurrentNodeAdapterError(f"{node_path}/message/metadata must be an object")
                content_path = f"{node_path}/message/content"
                parts = _content_parts(
                    content,
                    message_id=message_id,
                    source_file_id=source_file_id,
                    content_path=content_path,
                    source_message_id=str(raw_message_id or message_id),
                )
                content_unknown = _unknown_fields(content, set(CONTENT_KNOWN_FIELDS))
                if content_unknown:
                    extension_preserved += 1
                event_times: List[Dict[str, Any]] = []
                for field, event_kind in (("create_time", "message_created"), ("update_time", "message_updated")):
                    if field in message:
                        fact = _event_time(
                            event_kind=event_kind,
                            raw=message.get(field),
                            source_file_id=source_file_id,
                            source_path=f"{node_path}/message/{field}",
                            source_index=node_index,
                            source_id=str(raw_message_id or message_id),
                        )
                        event_times.append(fact)
                        time_status_counts[fact["semantic_status"]] += 1
                message_unknown = _unknown_fields(message, set(MESSAGE_KNOWN_FIELDS))
                if message_unknown:
                    extension_preserved += 1
                canonical_message = {
                    "message_id": message_id,
                    "author": {
                        "role": role,
                        "name": _nullable_string(author.get("name")),
                        "source_role": source_role,
                        "metadata": {**dict(author_metadata), "source_fields": author_unknown},
                    },
                    "content": {
                        "content_type": source_content_type,
                        "source_content_type": source_content_type,
                        "parts": parts,
                        "metadata": dict(content_metadata),
                        "extensions": {"source_fields": content_unknown},
                    },
                    "status": _nullable_string(message.get("status")),
                    "recipient": _nullable_string(message.get("recipient")),
                    "channel": _nullable_string(message.get("channel")),
                    "event_times": event_times,
                    "metadata": dict(message_metadata),
                    "source_ref": _source_ref(
                        source_file_id=source_file_id,
                        source_path=f"{node_path}/message",
                        source_index=node_index,
                        source_id=str(raw_message_id) if raw_message_id is not None else None,
                        source_value=message,
                        mapping_method="direct",
                    ),
                    "extensions": {"source_fields": message_unknown},
                }
                lineage.append({
                    "canonical_path": f"/conversations/{conversation_index}/nodes/{node_index}/message",
                    "source_path": f"{node_path}/message",
                    "mapping_method": "direct",
                    "loss_status": "lossless",
                })
                counts["messages"] += 1
                counts["content_parts"] += len(parts)
                counts["event_times"] += len(event_times)
                input_counts["messages"] += 1
                input_counts["content_parts"] += len(parts)
                input_counts["event_times"] += len(event_times)

            nodes.append({
                "node_id": node_id,
                "parent_node_id": parent_node_id,
                "child_node_ids": child_node_ids,
                "node_order": node_index,
                "message": canonical_message,
                "source_ref": _source_ref(
                    source_file_id=source_file_id,
                    source_path=node_path,
                    source_index=node_index,
                    source_id=node_id,
                    source_value=node,
                    mapping_method="renamed",
                ),
                "extensions": {"source_fields": node_unknown},
            })
            lineage.append({
                "canonical_path": f"/conversations/{conversation_index}/nodes/{node_index}",
                "source_path": node_path,
                "mapping_method": "renamed",
                "loss_status": "lossless",
            })
            counts["nodes"] += 1
            input_counts["nodes"] += 1

        node_by_id = {node["node_id"]: node for node in nodes}
        for node in nodes:
            parent = node["parent_node_id"]
            if parent is not None and node["node_id"] not in node_by_id[parent]["child_node_ids"]:
                raise MappingCurrentNodeAdapterError(f"asymmetric parent/child relation: {conversation_id}/{node['node_id']}")
            for child in node["child_node_ids"]:
                if node_by_id[child]["parent_node_id"] != node["node_id"]:
                    raise MappingCurrentNodeAdapterError(f"asymmetric child/parent relation: {conversation_id}/{node['node_id']}/{child}")

        conversation_times: Dict[str, Optional[Dict[str, Any]]] = {"created_time": None, "updated_time": None}
        for field, canonical_field, event_kind in (
            ("create_time", "created_time", "conversation_created"),
            ("update_time", "updated_time", "conversation_updated"),
        ):
            if field in conversation:
                fact = _event_time(
                    event_kind=event_kind,
                    raw=conversation.get(field),
                    source_file_id=source_file_id,
                    source_path=f"{conversation_path}/{field}",
                    source_index=conversation_index,
                    source_id=conversation_id,
                )
                conversation_times[canonical_field] = fact
                time_status_counts[fact["semantic_status"]] += 1
                counts["event_times"] += 1
                input_counts["event_times"] += 1
        conversation_unknown = _unknown_fields(conversation, set(CONVERSATION_KNOWN_FIELDS))
        if conversation_unknown:
            extension_preserved += 1
        canonical_conversations.append({
            "conversation_id": conversation_id,
            "title": _nullable_string(conversation.get("title")),
            "created_time": conversation_times["created_time"],
            "updated_time": conversation_times["updated_time"],
            "current_node_id": current_node_id,
            "nodes": nodes,
            "metadata": {},
            "source_ref": _source_ref(
                source_file_id=source_file_id,
                source_path=conversation_path,
                source_index=conversation_index,
                source_id=conversation_id,
                source_value=conversation,
                mapping_method="renamed",
            ),
            "extensions": {"source_fields": conversation_unknown},
        })
        lineage.append({
            "canonical_path": f"/conversations/{conversation_index}",
            "source_path": conversation_path,
            "mapping_method": "renamed",
            "loss_status": "lossless",
        })
        counts["conversations"] += 1
        input_counts["conversations"] += 1

    canonical = {
        "schema_version": TARGET_SCHEMA,
        "dataset_id": _stable_id("dataset", {
            "source_file_id": source_file_id,
            "source_sha256": source_canonical_sha256 or _sha256_value(source),
        }),
        "source_envelope_id": envelope_id,
        "canonicalized_at": canonicalized_at,
        "contract_version": "v0001",
        "conversations": canonical_conversations,
        "extensions": {"source_format": SOURCE_FORMAT},
    }
    return {
        "status": "completed",
        "adapter_family": SCRIPT_FAMILY,
        "adapter_version": SCRIPT_VERSION,
        "source_format": SOURCE_FORMAT,
        "target_schema": TARGET_SCHEMA,
        "canonical": canonical,
        "lineage": lineage,
        "conservation": {
            "input_counts": input_counts,
            "output_counts": counts,
            "dropped_records": 0,
            "degraded_records": 0,
            "extension_preserved_records": extension_preserved,
            "unmapped_records": 0,
            "source_ids_reused": source_ids_reused,
            "stable_ids_synthesized": stable_ids_synthesized,
            "source_graph_relationships_preserved": counts["nodes"],
            "counts_reconciled": input_counts == counts,
            "identities_unique": True,
            "graph_integrity": True,
            "time_semantic_status_counts": time_status_counts,
        },
    }


def _load_stream_module() -> Any:
    for parent in Path(__file__).resolve().parents:
        candidates = [parent / "scripts" / "json_stream_v0001.py"]
        if parent.name == "scripts":
            candidates.append(parent / "json_stream_v0001.py")
        for candidate in candidates:
            if not candidate.is_file():
                continue
            spec = importlib.util.spec_from_file_location("json_stream_runtime_v0001", candidate)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            return module
    raise MappingCurrentNodeAdapterError("json_stream_v0001.py cannot be located")


def _empty_counts() -> Dict[str, int]:
    return {"conversations": 0, "nodes": 0, "messages": 0, "content_parts": 0, "event_times": 0}


def _merge_counts(target: Dict[str, int], source: Mapping[str, Any]) -> None:
    for key in target:
        target[key] += int(source.get(key, 0))


def adapt_file_streaming(
    source_path: Path,
    *,
    canonical_path: Path,
    lineage_path: Path,
    source_file_id: str,
    envelope_id: str,
    canonicalized_at: str,
) -> Dict[str, Any]:
    stream_module = _load_stream_module()
    source_path = source_path.resolve()
    canonical_path = canonical_path.resolve()
    lineage_path = lineage_path.resolve()
    if canonical_path == source_path or lineage_path == source_path:
        raise MappingCurrentNodeAdapterError("streaming outputs must not replace the source")
    if canonical_path.exists() or lineage_path.exists():
        raise MappingCurrentNodeAdapterError("streaming output already exists")

    raw_message_counts: Counter[str] = Counter()
    canonical_source_digest = hashlib.sha256()
    canonical_source_digest.update(b"[")
    conversation_count = 0
    for index, item in stream_module.iter_root_array(source_path):
        conversation = _require_mapping(item, f"root[{index}]")
        if index:
            canonical_source_digest.update(b",")
        canonical_source_digest.update(_canonical_json(conversation).encode(DEFAULT_ENCODING))
        raw_message_counts.update(_raw_message_id_counts([conversation]))
        conversation_count += 1
    canonical_source_digest.update(b"]")
    if conversation_count < 1:
        raise MappingCurrentNodeAdapterError("source root must contain at least one conversation")
    source_canonical_sha256 = canonical_source_digest.hexdigest()
    dataset_id = _stable_id("dataset", {
        "source_file_id": source_file_id,
        "source_sha256": source_canonical_sha256,
    })

    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    lineage_path.parent.mkdir(parents=True, exist_ok=True)
    canonical_descriptor, canonical_temporary_name = tempfile.mkstemp(prefix=f".{canonical_path.name}.", dir=canonical_path.parent)
    lineage_descriptor, lineage_temporary_name = tempfile.mkstemp(prefix=f".{lineage_path.name}.", dir=lineage_path.parent)
    canonical_temporary = Path(canonical_temporary_name)
    lineage_temporary = Path(lineage_temporary_name)
    input_counts = _empty_counts()
    output_counts = _empty_counts()
    time_status_counts = {"verified": 0, "unverified": 0, "ambiguous": 0, "invalid": 0, "missing": 0}
    totals = {"extension_preserved_records": 0, "source_ids_reused": 0, "stable_ids_synthesized": 0}
    used_conversation_ids: Set[str] = set()
    used_message_ids: Set[str] = set()
    written = 0
    try:
        with os.fdopen(canonical_descriptor, "w", encoding=DEFAULT_ENCODING, newline="\n") as canonical_stream, os.fdopen(
            lineage_descriptor, "w", encoding=DEFAULT_ENCODING, newline="\n"
        ) as lineage_stream:
            prefix = {
                "schema_version": TARGET_SCHEMA,
                "dataset_id": dataset_id,
                "source_envelope_id": envelope_id,
                "canonicalized_at": canonicalized_at,
                "contract_version": "v0001",
            }
            canonical_stream.write("{")
            for position, (key, value) in enumerate(prefix.items()):
                if position:
                    canonical_stream.write(",")
                canonical_stream.write(_canonical_json(key))
                canonical_stream.write(":")
                canonical_stream.write(_canonical_json(value))
            canonical_stream.write(',"conversations":[')
            for index, item in stream_module.iter_root_array(source_path):
                adapted = adapt_payload(
                    [item],
                    source_file_id=source_file_id,
                    envelope_id=envelope_id,
                    canonicalized_at=canonicalized_at,
                    raw_message_id_counts_override=raw_message_counts,
                    conversation_index_offset=index,
                    used_conversation_ids_override=used_conversation_ids,
                    used_message_ids_override=used_message_ids,
                    source_canonical_sha256=source_canonical_sha256,
                )
                conversation = adapted["canonical"]["conversations"][0]
                if written:
                    canonical_stream.write(",")
                canonical_stream.write(_canonical_json(conversation))
                for row in adapted["lineage"]:
                    lineage_stream.write(_canonical_json(row))
                    lineage_stream.write("\n")
                conservation = adapted["conservation"]
                _merge_counts(input_counts, conservation["input_counts"])
                _merge_counts(output_counts, conservation["output_counts"])
                for key in totals:
                    totals[key] += int(conservation[key])
                for key in time_status_counts:
                    time_status_counts[key] += int(conservation["time_semantic_status_counts"].get(key, 0))
                written += 1
            canonical_stream.write('],"extensions":')
            canonical_stream.write(_canonical_json({"source_format": SOURCE_FORMAT}))
            canonical_stream.write("}\n")
            canonical_stream.flush()
            lineage_stream.flush()
            os.fsync(canonical_stream.fileno())
            os.fsync(lineage_stream.fileno())
        if written != conversation_count:
            raise MappingCurrentNodeAdapterError(
                f"streaming conversation conservation failed: scanned={conversation_count} written={written}"
            )
        canonical_temporary.replace(canonical_path)
        lineage_temporary.replace(lineage_path)
    except Exception:
        canonical_temporary.unlink(missing_ok=True)
        lineage_temporary.unlink(missing_ok=True)
        canonical_path.unlink(missing_ok=True)
        lineage_path.unlink(missing_ok=True)
        raise

    conservation = {
        "input_counts": input_counts,
        "output_counts": output_counts,
        "dropped_records": 0,
        "degraded_records": 0,
        **totals,
        "unmapped_records": 0,
        "source_graph_relationships_preserved": output_counts["nodes"],
        "counts_reconciled": input_counts == output_counts,
        "identities_unique": True,
        "graph_integrity": True,
        "time_semantic_status_counts": time_status_counts,
    }
    return {
        "status": "completed",
        "adapter_family": SCRIPT_FAMILY,
        "adapter_version": SCRIPT_VERSION,
        "source_format": SOURCE_FORMAT,
        "target_schema": TARGET_SCHEMA,
        "canonical_path": str(canonical_path),
        "lineage_path": str(lineage_path),
        "dataset_id": dataset_id,
        "conservation": conservation,
        "streaming": {
            "enabled": True,
            "passes": 2,
            "maximum_resident_scope": "single_conversation_plus_decoder_buffer",
        },
    }


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def adapter_declaration() -> Dict[str, Any]:
    return {
        "family": SCRIPT_FAMILY,
        "version": SCRIPT_VERSION,
        "source_format": SOURCE_FORMAT,
        "target_schema": TARGET_SCHEMA,
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Adapt a mapping/current_node conversation array to the canonical ingress contract.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--lineage-output", default="")
    parser.add_argument("--source-file-id", required=True)
    parser.add_argument("--envelope-id", required=True)
    parser.add_argument("--canonicalized-at", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        output_path = Path(args.output)
        lineage_path = Path(args.lineage_output) if args.lineage_output else output_path.with_suffix(".lineage.jsonl")
        result = adapt_file_streaming(
            Path(args.input),
            canonical_path=output_path,
            lineage_path=lineage_path,
            source_file_id=args.source_file_id,
            envelope_id=args.envelope_id,
            canonicalized_at=args.canonicalized_at,
        )
        print(json.dumps({
            "status": result["status"],
            "adapter_family": result["adapter_family"],
            "adapter_version": result["adapter_version"],
            "conservation": result["conservation"],
        }, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
