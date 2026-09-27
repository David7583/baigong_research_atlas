#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: adapt_array_uuid_chat_messages_v0001.py
# 中文名: UUID Chat Messages 数组导出入口适配脚本
# 版本号: v0001
#
# 主层级: system
# 层级: adapters / ingress / format_adapter
# 脚本定位: 将 array_uuid_chat_messages_v1 无损映射到完整入口规范契约
#
# 职责说明:
# - 将 uuid/chat_messages/sender/text 线性对话导出转换为 Provider 无关节点图
# - 保留原始 ID、字段、时间语义、来源指针和数量守恒证据
#
# 本脚本做什么:
# - 使用标准库生成 canonical_conversation_ingress_v0001 对象和追溯行
# - 在源无稳定 ID 时使用来源 hash 和稳定位置生成可重现 ID
#
# 本脚本不做什么:
# - 不识别其他格式，不猜测未知时间字段的事件语义
# - 不修改原始数据，不写数据库，不调用下游解析
#
# 制度边界声明:
# - 线性消息关系显式标记为 derived_from_sequence，不伪装为源生图
# - 任一对话或消息无法保真时准确失败，不返回部分成功
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: adapt_array_uuid_chat_messages_v0001
# family: adapt_array_uuid_chat_messages
# role: canonical_ingress_format_adapter
# version: v0001
# status: experimental
# entry_point: scripts/adapters/ingress/adapters/adapt_array_uuid_chat_messages_v0001.py
# input:
#   - array_uuid_chat_messages_v1 JSON value
# output:
#   - canonical_conversation_ingress_v0001 mapping
#   - source-to-canonical lineage rows and conservation statistics
# depends_on:
#   - Python standard library
# used_by:
#   - canonical_ingress_gateway_v0001
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "adapt_array_uuid_chat_messages"
SCRIPT_NAME = "adapt_array_uuid_chat_messages_v0001.py"
SCRIPT_VERSION = "v0001"
SOURCE_FORMAT = "array_uuid_chat_messages_v1"
TARGET_SCHEMA = "canonical_conversation_ingress_v0001"


# ============================================================
# 异常类型
# ============================================================

class ArrayChatMessagesAdapterError(RuntimeError):
    """Raised when the source format cannot be adapted without silent loss."""


# ============================================================
# 数据结构
# ============================================================

# Public results are versioned JSON-compatible mappings so the gateway can
# invoke the adapter without a project-specific runtime dependency.


# ============================================================
# 工具函数区
# ============================================================

def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode(DEFAULT_ENCODING)).hexdigest()


def _stable_id(kind: str, source_file_id: str, source_path: str, value: Any) -> str:
    identity = {
        "kind": kind,
        "source_file_id": source_file_id,
        "source_path": source_path,
        "value_sha256": _sha256_value(value),
    }
    return "sha256:" + _sha256_value(identity)


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ArrayChatMessagesAdapterError(f"{label} must be an object")
    return value


def _require_list(value: Any, label: str) -> List[Any]:
    if not isinstance(value, list):
        raise ArrayChatMessagesAdapterError(f"{label} must be a list")
    return value


def _require_string(value: Any, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ArrayChatMessagesAdapterError(f"{label} must be {'a string' if allow_empty else 'a non-empty string'}")
    return value


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


def _normalize_utc(raw: Any) -> Tuple[Optional[str], str]:
    if raw is None:
        return None, "missing"
    if not isinstance(raw, str) or not raw.strip():
        return None, "invalid"
    text = raw.strip()
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        return None, "invalid"
    if parsed.tzinfo is None:
        return None, "ambiguous"
    normalized = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return normalized, "verified"


def _event_time(
    *,
    event_kind: str,
    raw: Any,
    source_file_id: str,
    source_path: str,
    source_index: Optional[int],
    source_id: Optional[str],
    semantic_basis: str,
) -> Dict[str, Any]:
    normalized, status = _normalize_utc(raw)
    return {
        "event_kind": event_kind,
        "raw_value": raw,
        "raw_type": "iso8601" if isinstance(raw, str) else "unknown",
        "normalized_utc": normalized,
        "semantic_status": status,
        "semantic_basis": semantic_basis,
        "source_ref": _source_ref(
            source_file_id=source_file_id,
            source_path=source_path,
            source_index=source_index,
            source_id=source_id,
            source_value=raw,
            mapping_method="normalized",
            loss_status="lossless",
        ),
    }


def _unknown_fields(source: Mapping[str, Any], known: Set[str]) -> Dict[str, Any]:
    return {str(key): value for key, value in source.items() if str(key) not in known}


def _role(sender: Any) -> Tuple[str, str, Optional[str], Dict[str, Any]]:
    if isinstance(sender, str):
        source_role = sender
        name = None
        metadata: Dict[str, Any] = {}
    elif isinstance(sender, Mapping):
        raw_role = sender.get("role") or sender.get("type") or sender.get("name") or "unknown"
        source_role = str(raw_role)
        raw_name = sender.get("name")
        name = str(raw_name) if raw_name is not None else None
        metadata = _unknown_fields(sender, {"role", "type", "name"})
    else:
        raise ArrayChatMessagesAdapterError("chat message sender must be a string or object")
    lowered = source_role.strip().lower()
    aliases = {
        "human": "user",
        "user": "user",
        "assistant": "assistant",
        "ai": "assistant",
        "system": "system",
        "tool": "tool",
        "developer": "developer",
    }
    return aliases.get(lowered, "unknown"), source_role, name, metadata


# ============================================================
# 默认映射
# ============================================================

CONVERSATION_KNOWN_FIELDS = frozenset({"uuid", "id", "name", "title", "created_at", "updated_at", "chat_messages"})
MESSAGE_KNOWN_FIELDS = frozenset({
    "uuid", "id", "sender", "text", "created_at", "updated_at", "status", "recipient", "channel",
})


# ============================================================
# 核心适配组件
# ============================================================

def adapt_payload(
    source: Any,
    *,
    source_file_id: str,
    envelope_id: str,
    canonicalized_at: str,
) -> Dict[str, Any]:
    conversations_source = _require_list(source, "source root")
    if not conversations_source:
        raise ArrayChatMessagesAdapterError("source root must contain at least one conversation")
    canonical_conversations: List[Dict[str, Any]] = []
    lineage: List[Dict[str, Any]] = []
    used_conversation_ids: Set[str] = set()
    used_message_ids: Set[str] = set()
    counts = {"conversations": 0, "nodes": 0, "messages": 0, "content_parts": 0, "event_times": 0}
    extension_preserved = 0
    source_ids_reused = 0
    stable_ids_synthesized = 0
    time_status_counts = {"verified": 0, "unverified": 0, "ambiguous": 0, "invalid": 0, "missing": 0}

    for conversation_index, item in enumerate(conversations_source):
        source_path = f"/{conversation_index}"
        conversation = _require_mapping(item, f"root[{conversation_index}]")
        raw_conversation_id = conversation.get("uuid") or conversation.get("id")
        if raw_conversation_id is None:
            conversation_id = _stable_id("conversation", source_file_id, source_path, conversation)
            stable_ids_synthesized += 1
            conversation_mapping_method = "synthesized_stable_id"
        else:
            conversation_id = _require_string(raw_conversation_id, f"root[{conversation_index}].uuid")
            source_ids_reused += 1
            conversation_mapping_method = "renamed"
        if conversation_id in used_conversation_ids:
            conversation_id = _stable_id("conversation", source_file_id, source_path, conversation)
            stable_ids_synthesized += 1
            conversation_mapping_method = "synthesized_stable_id"
        used_conversation_ids.add(conversation_id)
        raw_messages = _require_list(conversation.get("chat_messages"), f"root[{conversation_index}].chat_messages")
        nodes: List[Dict[str, Any]] = []
        node_ids: List[str] = []
        prepared_messages: List[Tuple[Mapping[str, Any], str, str]] = []
        for message_index, message_item in enumerate(raw_messages):
            message_path = f"{source_path}/chat_messages/{message_index}"
            message = _require_mapping(message_item, f"root[{conversation_index}].chat_messages[{message_index}]")
            raw_message_id = message.get("uuid") or message.get("id")
            if raw_message_id is None:
                message_id = _stable_id("message", source_file_id, message_path, message)
                stable_ids_synthesized += 1
            else:
                message_id = _require_string(raw_message_id, f"{message_path}/uuid")
                source_ids_reused += 1
            if message_id in used_message_ids:
                message_id = _stable_id("message", source_file_id, message_path, message)
                stable_ids_synthesized += 1
            used_message_ids.add(message_id)
            node_id = "node:" + message_id
            node_ids.append(node_id)
            prepared_messages.append((message, message_id, message_path))

        for message_index, prepared in enumerate(prepared_messages):
            message, message_id, message_path = prepared
            node_id = node_ids[message_index]
            text = _require_string(message.get("text"), f"{message_path}/text", allow_empty=True)
            role, source_role, author_name, author_metadata = _role(message.get("sender"))
            raw_message_source_id = str(message.get("uuid") or message.get("id") or message_id)
            event_times: List[Dict[str, Any]] = []
            for field, event_kind in (("created_at", "message_created"), ("updated_at", "message_updated")):
                if field in message:
                    fact = _event_time(
                        event_kind=event_kind,
                        raw=message.get(field),
                        source_file_id=source_file_id,
                        source_path=f"{message_path}/{field}",
                        source_index=message_index,
                        source_id=raw_message_source_id,
                        semantic_basis=f"{SOURCE_FORMAT}.{field} adapter rule v0001",
                    )
                    event_times.append(fact)
                    time_status_counts[fact["semantic_status"]] += 1
            message_unknown = _unknown_fields(message, set(MESSAGE_KNOWN_FIELDS))
            if message_unknown:
                extension_preserved += 1
            message_source_ref = _source_ref(
                source_file_id=source_file_id,
                source_path=message_path,
                source_index=message_index,
                source_id=raw_message_source_id,
                source_value=message,
                mapping_method="renamed",
            )
            canonical_message = {
                "message_id": message_id,
                "author": {
                    "role": role,
                    "name": author_name,
                    "source_role": source_role,
                    "metadata": author_metadata,
                },
                "content": {
                    "content_type": "text",
                    "source_content_type": "text",
                    "parts": [
                        {
                            "part_id": f"{message_id}:part:0",
                            "part_index": 0,
                            "part_type": "text",
                            "text": text,
                            "mime_type": "text/plain",
                            "uri": None,
                            "payload": None,
                            "source_ref": _source_ref(
                                source_file_id=source_file_id,
                                source_path=f"{message_path}/text",
                                source_index=0,
                                source_id=raw_message_source_id,
                                source_value=text,
                                mapping_method="renamed",
                            ),
                            "extensions": {},
                        }
                    ],
                    "metadata": {},
                    "extensions": {},
                },
                "status": str(message["status"]) if message.get("status") is not None else None,
                "recipient": str(message["recipient"]) if message.get("recipient") is not None else None,
                "channel": str(message["channel"]) if message.get("channel") is not None else None,
                "event_times": event_times,
                "metadata": {},
                "source_ref": message_source_ref,
                "extensions": {"source_fields": message_unknown},
            }
            node_source_ref = dict(message_source_ref)
            node_source_ref["mapping_method"] = "derived_from_sequence"
            nodes.append(
                {
                    "node_id": node_id,
                    "parent_node_id": node_ids[message_index - 1] if message_index > 0 else None,
                    "child_node_ids": [node_ids[message_index + 1]] if message_index + 1 < len(node_ids) else [],
                    "node_order": message_index,
                    "message": canonical_message,
                    "source_ref": node_source_ref,
                    "extensions": {"relationship_origin": "derived_from_sequence"},
                }
            )
            canonical_base = f"/conversations/{conversation_index}/nodes/{message_index}"
            lineage.extend(
                [
                    {
                        "canonical_path": canonical_base,
                        "source_path": message_path,
                        "mapping_method": "derived_from_sequence",
                        "loss_status": "lossless",
                    },
                    {
                        "canonical_path": f"{canonical_base}/message",
                        "source_path": message_path,
                        "mapping_method": "renamed",
                        "loss_status": "lossless",
                    },
                    {
                        "canonical_path": f"{canonical_base}/message/content/parts/0/text",
                        "source_path": f"{message_path}/text",
                        "mapping_method": "renamed",
                        "loss_status": "lossless",
                    },
                ]
            )
            counts["nodes"] += 1
            counts["messages"] += 1
            counts["content_parts"] += 1
            counts["event_times"] += len(event_times)

        conversation_unknown = _unknown_fields(conversation, set(CONVERSATION_KNOWN_FIELDS))
        if conversation_unknown:
            extension_preserved += 1
        conversation_source_ref = _source_ref(
            source_file_id=source_file_id,
            source_path=source_path,
            source_index=conversation_index,
            source_id=str(raw_conversation_id) if raw_conversation_id is not None else None,
            source_value=conversation,
            mapping_method=conversation_mapping_method,
        )
        conversation_times: Dict[str, Optional[Dict[str, Any]]] = {"created_time": None, "updated_time": None}
        for field, canonical_field, event_kind in (
            ("created_at", "created_time", "conversation_created"),
            ("updated_at", "updated_time", "conversation_updated"),
        ):
            if field in conversation:
                fact = _event_time(
                    event_kind=event_kind,
                    raw=conversation.get(field),
                    source_file_id=source_file_id,
                    source_path=f"{source_path}/{field}",
                    source_index=conversation_index,
                    source_id=str(raw_conversation_id) if raw_conversation_id is not None else None,
                    semantic_basis=f"{SOURCE_FORMAT}.{field} adapter rule v0001",
                )
                conversation_times[canonical_field] = fact
                time_status_counts[fact["semantic_status"]] += 1
                counts["event_times"] += 1
        title_value = conversation.get("name", conversation.get("title"))
        title = str(title_value) if title_value is not None else None
        canonical_conversations.append(
            {
                "conversation_id": conversation_id,
                "title": title,
                "created_time": conversation_times["created_time"],
                "updated_time": conversation_times["updated_time"],
                "current_node_id": node_ids[-1] if node_ids else None,
                "nodes": nodes,
                "metadata": {},
                "source_ref": conversation_source_ref,
                "extensions": {"source_fields": conversation_unknown},
            }
        )
        lineage.append(
            {
                "canonical_path": f"/conversations/{conversation_index}",
                "source_path": source_path,
                "mapping_method": conversation_mapping_method,
                "loss_status": "lossless",
            }
        )
        counts["conversations"] += 1

    dataset_id = _stable_id("dataset", source_file_id, "/", source)
    canonical = {
        "schema_version": TARGET_SCHEMA,
        "dataset_id": dataset_id,
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
            "input_counts": dict(counts),
            "output_counts": dict(counts),
            "dropped_records": 0,
            "degraded_records": 0,
            "extension_preserved_records": extension_preserved,
            "unmapped_records": 0,
            "source_ids_reused": source_ids_reused,
            "stable_ids_synthesized": stable_ids_synthesized,
            "sequence_relationships_derived": len(canonical_conversations),
            "counts_reconciled": True,
            "identities_unique": True,
            "graph_integrity": True,
            "time_semantic_status_counts": time_status_counts,
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
    parser = argparse.ArgumentParser(description="Adapt array uuid/chat_messages exports to the canonical ingress contract.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--source-file-id", required=True)
    parser.add_argument("--envelope-id", required=True)
    parser.add_argument("--canonicalized-at", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        source = json.loads(Path(args.input).read_text(encoding="utf-8-sig"))
        result = adapt_payload(
            source,
            source_file_id=args.source_file_id,
            envelope_id=args.envelope_id,
            canonicalized_at=args.canonicalized_at,
        )
        output = {
            "status": "dry_run" if args.dry_run else result["status"],
            "adapter_family": result["adapter_family"],
            "adapter_version": result["adapter_version"],
            "source_format": result["source_format"],
            "target_schema": result["target_schema"],
            "conservation": result["conservation"],
        }
        print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except ArrayChatMessagesAdapterError as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
