#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: canonical_ingress_gateway_v0003.py
# 中文名: 统一入口格式识别与适配网关脚本
# 版本号: v0003
#
# 主层级: data
# 层级: adapters / ingress / gateway
# 脚本定位: 外部数据进入内部规范契约前的唯一格式识别与适配调度边界
#
# 职责说明:
# - 按版本化指纹登记表唯一识别输入格式并调用固定适配器
# - 在产生任何正式输出前完成规范契约、数量守恒和原始不变校验
# - 对大型根数组和规范 conversations 数组采用逐对话有界读取
#
# 本脚本做什么:
# - 使用 Python 标准库生成规范工作副本、追溯索引、校验报告和统一信封
# - 对未知格式和多指纹歧义准确拒绝，不选择“最像的”适配器
#
# 本脚本不做什么:
# - 不修改 data_raw，不将 Provider 名当作格式指纹，不执行下游解析
# - 不动态扫描未登记脚本，不下载插件，不写数据库
#
# 制度边界声明:
# - 原始输入只读；信封最后写入，部分产物不得被宣告为完整成功
# - dry-run 完成识别、适配和完整契约校验，但不写任何产物
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: canonical_ingress_gateway_v0003
# family: canonical_ingress_gateway
# role: canonical_ingress_adapter_gateway
# version: v0003
# status: experimental
# entry_point: scripts/adapters/ingress/canonical_ingress_gateway_v0003.py
# input:
#   - immutable external or canonical conversation JSON
#   - config/contracts/format_fingerprint_registry_v0003.json
# output:
#   - canonical conversation working copy, lineage index, validation report, and ingress envelope
# depends_on:
#   - canonical_ingress_contract_validator_v0002
#   - scripts/json_stream_v0001.py
#   - registered ingress format adapter
#   - Python standard library
# used_by:
#   - data_action_chain_pipeline_v0012
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "canonical_ingress_gateway"
SCRIPT_NAME = "canonical_ingress_gateway_v0003.py"
SCRIPT_VERSION = "v0003"
REGISTRY_SCHEMA_VERSION = "format_fingerprint_registry_v0003"
REGISTRY_VERSION = "v0003"
CANONICAL_SCHEMA_VERSION = "canonical_conversation_ingress_v0001"
ENVELOPE_SCHEMA_VERSION = "canonical_ingress_envelope_v0001"
DEFAULT_REGISTRY = Path("config") / "contracts" / "format_fingerprint_registry_v0003.json"
VALIDATOR_RELATIVE = Path("scripts") / "adapters" / "ingress" / "canonical_ingress_contract_validator_v0002.py"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
MAX_NON_STREAMING_ADAPTER_BYTES = 64 * 1024 * 1024


# ============================================================
# 异常类型
# ============================================================

class CanonicalIngressGatewayError(RuntimeError):
    """Base error for format detection, adapter dispatch, and envelope generation."""


class IngressConfigError(CanonicalIngressGatewayError):
    """Raised when the registry or registered script cannot be used safely."""


class FormatDetectionError(CanonicalIngressGatewayError):
    """Raised when the source format is unknown or ambiguous."""


class AdapterExecutionError(CanonicalIngressGatewayError):
    """Raised when a registered adapter cannot produce a valid canonical payload."""


# ============================================================
# 数据结构
# ============================================================

# The public boundary is JSON so adapters remain independently versioned and
# can later be implemented in another language without changing the envelope.


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
    raise IngressConfigError(f"cannot locate project root from: {start}")


def _resolve_from_root(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise IngressConfigError(f"{label} must remain within project root") from exc


def _relative(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError as exc:
        raise IngressConfigError(f"path must remain within project root: {path}") from exc


def _load_json(path: Path, label: str) -> Any:
    if not path.is_file():
        raise IngressConfigError(f"{label} is not a file: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IngressConfigError(f"{label} is invalid JSON: {exc}") from exc


def _load_module(name: str, path: Path) -> ModuleType:
    if not path.is_file():
        raise IngressConfigError(f"registered module is missing: {path}")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise IngressConfigError(f"cannot load registered module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


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


def _atomic_write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    _atomic_write_text(path, "".join(_canonical_json(dict(row)) + "\n" for row in rows))


def _require_non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise IngressConfigError(f"{label} must be a non-empty string")
    return value


# ============================================================
# 默认映射
# ============================================================

SUPPORTED_DETECTORS = frozenset({
    "canonical_conversation_ingress_v0001",
    "array_uuid_chat_messages_v1",
    "mapping_current_node_conversations_v1",
})


# ============================================================
# 核心网关组件
# ============================================================

def _validate_registry(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        raise IngressConfigError("format registry root must be an object")
    if payload.get("schema_version") != REGISTRY_SCHEMA_VERSION or payload.get("registry_version") != REGISTRY_VERSION:
        raise IngressConfigError("format registry version mismatch")
    if payload.get("canonical_contract") != CANONICAL_SCHEMA_VERSION:
        raise IngressConfigError("format registry canonical contract mismatch")
    formats = payload.get("formats")
    if not isinstance(formats, list) or not formats:
        raise IngressConfigError("format registry must contain formats")
    identities: set[Tuple[str, str]] = set()
    validated: List[Dict[str, Any]] = []
    for index, item in enumerate(formats):
        if not isinstance(item, Mapping):
            raise IngressConfigError(f"format registry item {index} must be an object")
        family = _require_non_empty_string(item.get("format_family"), f"format {index} family")
        version = _require_non_empty_string(item.get("fingerprint_version"), f"format {index} fingerprint_version")
        identity = (family, version)
        if identity in identities:
            raise IngressConfigError(f"duplicate format fingerprint: {identity}")
        identities.add(identity)
        detector = _require_non_empty_string(item.get("detector"), f"format {index} detector")
        if detector not in SUPPORTED_DETECTORS:
            raise IngressConfigError(f"unsupported registered detector: {detector}")
        if item.get("root_type") not in {"object", "array"}:
            raise IngressConfigError(f"format {index} root_type is invalid")
        for field in ("required_paths", "forbidden_paths"):
            values = item.get(field)
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                raise IngressConfigError(f"format {index} {field} must be a string list")
        adapter = item.get("adapter")
        if not isinstance(adapter, Mapping):
            raise IngressConfigError(f"format {index} adapter must be an object")
        _require_non_empty_string(adapter.get("family"), f"format {index} adapter family")
        _require_non_empty_string(adapter.get("version"), f"format {index} adapter version")
        entry = Path(_require_non_empty_string(adapter.get("entry_point"), f"format {index} adapter entry_point"))
        if entry.is_absolute() or ".." in entry.parts:
            raise IngressConfigError(f"format {index} adapter entry_point must be a safe relative path")
        if not isinstance(adapter.get("native_passthrough"), bool):
            raise IngressConfigError(f"format {index} native_passthrough must be boolean")
        validated.append(dict(item))
    return validated


def _detect_canonical(source: Any) -> Tuple[bool, List[str], List[str]]:
    evidence: List[str] = []
    exclusions: List[str] = []
    if not isinstance(source, Mapping):
        return False, evidence, ["root is not an object"]
    if source.get("schema_version") == CANONICAL_SCHEMA_VERSION:
        evidence.append("/schema_version matches canonical contract")
    else:
        exclusions.append("/schema_version does not match canonical contract")
    if isinstance(source.get("conversations"), list) and source.get("conversations"):
        evidence.append("/conversations is a non-empty array")
    else:
        exclusions.append("/conversations is missing or empty")
    for field in ("dataset_id", "source_envelope_id"):
        if isinstance(source.get(field), str) and source.get(field):
            evidence.append(f"/{field} is present")
        else:
            exclusions.append(f"/{field} is missing")
    return not exclusions, evidence, exclusions


def _detect_array_chat_messages(source: Any) -> Tuple[bool, List[str], List[str]]:
    evidence: List[str] = []
    exclusions: List[str] = []
    if not isinstance(source, list) or not source:
        return False, evidence, ["root is not a non-empty array"]
    conversation_count = 0
    message_count = 0
    for conversation_index, conversation in enumerate(source):
        if not isinstance(conversation, Mapping):
            exclusions.append(f"root[{conversation_index}] is not an object")
            continue
        if not isinstance(conversation.get("uuid"), str) or not conversation.get("uuid"):
            exclusions.append(f"root[{conversation_index}].uuid is missing")
        messages = conversation.get("chat_messages")
        if not isinstance(messages, list):
            exclusions.append(f"root[{conversation_index}].chat_messages is missing or not a list")
            continue
        conversation_count += 1
        for message_index, message in enumerate(messages):
            if not isinstance(message, Mapping):
                exclusions.append(f"root[{conversation_index}].chat_messages[{message_index}] is not an object")
                continue
            if "sender" not in message:
                exclusions.append(f"root[{conversation_index}].chat_messages[{message_index}].sender is missing")
            if not isinstance(message.get("text"), str):
                exclusions.append(f"root[{conversation_index}].chat_messages[{message_index}].text is not a string")
            message_count += 1
    if message_count < 1:
        exclusions.append("source contains no chat messages")
    if not exclusions:
        evidence.extend([
            f"{conversation_count} conversations contain uuid and chat_messages",
            f"{message_count} messages contain sender and text",
        ])
    return not exclusions, evidence, exclusions


def _detect_mapping_current_node(source: Any) -> Tuple[bool, List[str], List[str]]:
    evidence: List[str] = []
    exclusions: List[str] = []
    if not isinstance(source, list) or not source:
        return False, evidence, ["root is not a non-empty array"]
    conversation_count = 0
    node_count = 0
    message_count = 0
    for conversation_index, conversation in enumerate(source):
        if not isinstance(conversation, Mapping):
            exclusions.append(f"root[{conversation_index}] is not an object")
            continue
        conversation_id = conversation.get("conversation_id")
        if not isinstance(conversation_id, str) or not conversation_id:
            exclusions.append(f"root[{conversation_index}].conversation_id is missing")
        mapping = conversation.get("mapping")
        if not isinstance(mapping, Mapping) or not mapping:
            exclusions.append(f"root[{conversation_index}].mapping is missing or empty")
            continue
        current_node = conversation.get("current_node")
        if not isinstance(current_node, str) or not current_node:
            exclusions.append(f"root[{conversation_index}].current_node is missing")
        elif current_node not in mapping:
            exclusions.append(f"root[{conversation_index}].current_node does not exist in mapping")
        conversation_count += 1
        for node_id, node in mapping.items():
            if not isinstance(node_id, str) or not node_id:
                exclusions.append(f"root[{conversation_index}].mapping contains a non-string node key")
                continue
            if not isinstance(node, Mapping):
                exclusions.append(f"root[{conversation_index}].mapping[{node_id}] is not an object")
                continue
            if not isinstance(node.get("children"), list):
                exclusions.append(f"root[{conversation_index}].mapping[{node_id}].children is not a list")
            message = node.get("message")
            if message is not None:
                if not isinstance(message, Mapping):
                    exclusions.append(f"root[{conversation_index}].mapping[{node_id}].message is not an object or null")
                    continue
                author = message.get("author")
                content = message.get("content")
                if not isinstance(message.get("id"), str) or not message.get("id"):
                    exclusions.append(f"root[{conversation_index}].mapping[{node_id}].message.id is missing")
                if not isinstance(author, Mapping) or not isinstance(author.get("role"), str) or not author.get("role"):
                    exclusions.append(f"root[{conversation_index}].mapping[{node_id}].message.author.role is missing")
                if not isinstance(content, Mapping) or not isinstance(content.get("content_type"), str) or not content.get("content_type"):
                    exclusions.append(f"root[{conversation_index}].mapping[{node_id}].message.content.content_type is missing")
                message_count += 1
            node_count += 1
    if message_count < 1:
        exclusions.append("source contains no non-null messages")
    if not exclusions:
        evidence.extend([
            f"{conversation_count} conversations contain conversation_id, mapping, and current_node",
            f"{node_count} mapping nodes contain children and nullable message",
            f"{message_count} messages contain id, author.role, and content.content_type",
        ])
    return not exclusions, evidence, exclusions


def detect_format(source: Any, formats: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    matches: List[Mapping[str, Any]] = []
    for definition in formats:
        detector = str(definition["detector"])
        if detector == "canonical_conversation_ingress_v0001":
            matched, evidence, exclusions = _detect_canonical(source)
        elif detector == "array_uuid_chat_messages_v1":
            matched, evidence, exclusions = _detect_array_chat_messages(source)
        elif detector == "mapping_current_node_conversations_v1":
            matched, evidence, exclusions = _detect_mapping_current_node(source)
        else:
            raise IngressConfigError(f"unsupported detector at runtime: {detector}")
        candidates.append({
            "format_family": definition["format_family"],
            "fingerprint_version": definition["fingerprint_version"],
            "matched": matched,
            "evidence": evidence,
            "exclusions": exclusions,
        })
        if matched:
            matches.append(definition)
    if len(matches) == 0:
        return {
            "status": "rejected",
            "format_family": None,
            "fingerprint_version": None,
            "registry_version": REGISTRY_VERSION,
            "candidates": candidates,
            "stop_reason": "no registered format fingerprint matched",
            "definition": None,
        }
    if len(matches) > 1:
        return {
            "status": "ambiguous",
            "format_family": None,
            "fingerprint_version": None,
            "registry_version": REGISTRY_VERSION,
            "candidates": candidates,
            "stop_reason": "multiple registered format fingerprints matched",
            "definition": None,
        }
    selected = matches[0]
    return {
        "status": "exact",
        "format_family": selected["format_family"],
        "fingerprint_version": selected["fingerprint_version"],
        "registry_version": REGISTRY_VERSION,
        "candidates": candidates,
        "stop_reason": None,
        "definition": dict(selected),
    }


def _load_validator(adapter_root: Path) -> ModuleType:
    return _load_module(
        "canonical_ingress_contract_validator_runtime_v0001",
        (adapter_root / VALIDATOR_RELATIVE).resolve(),
    )


def _load_stream_module(project_root: Path) -> ModuleType:
    return _load_module(
        "json_stream_gateway_runtime_v0001",
        (project_root / "scripts" / "json_stream_v0001.py").resolve(),
    )


def _stream_detection_probe(source_path: Path, stream_module: ModuleType) -> Tuple[Any, str, int]:
    with source_path.open("r", encoding="utf-8-sig") as handle:
        first = ""
        while True:
            character = handle.read(1)
            if character == "":
                break
            if not character.isspace():
                first = character
                break
    if first == "[":
        samples: List[Any] = []
        count = 0
        for index, item in stream_module.iter_root_array(source_path):
            if index < 32:
                samples.append(item)
            count += 1
        return samples, "json_array", count
    if first == "{":
        inspection = stream_module.inspect_object_array(source_path, "conversations")
        sample_conversations: List[Any] = []
        for index, item in stream_module.iter_object_array(source_path, "conversations"):
            if index < 2:
                sample_conversations.append(item)
            else:
                break
        source_probe = dict(inspection["metadata"])
        source_probe["conversations"] = sample_conversations
        return source_probe, "json_object", int(inspection["count"])
    raise FormatDetectionError("source root must be a JSON array or object")


def _adapter_path(adapter_root: Path, definition: Mapping[str, Any]) -> Path:
    adapter = definition["adapter"]
    relative = Path(str(adapter["entry_point"]))
    path = (adapter_root / relative).resolve()
    _require_within(path, adapter_root, "registered adapter")
    return path


def _native_conservation(payload: Mapping[str, Any], validation: Mapping[str, Any]) -> Dict[str, Any]:
    counts = dict(validation["counts"])
    statuses = {"verified": 0, "unverified": 0, "ambiguous": 0, "invalid": 0, "missing": 0}
    for conversation in payload["conversations"]:
        for field in ("created_time", "updated_time"):
            fact = conversation.get(field)
            if isinstance(fact, Mapping):
                statuses[str(fact["semantic_status"])] += 1
        for node in conversation["nodes"]:
            message = node.get("message")
            if isinstance(message, Mapping):
                for fact in message["event_times"]:
                    statuses[str(fact["semantic_status"])] += 1
    return {
        "input_counts": counts,
        "output_counts": counts,
        "dropped_records": 0,
        "degraded_records": 0,
        "extension_preserved_records": 0,
        "unmapped_records": 0,
        "source_ids_reused": counts["conversations"] + counts["messages"],
        "stable_ids_synthesized": 0,
        "sequence_relationships_derived": 0,
        "counts_reconciled": True,
        "identities_unique": True,
        "graph_integrity": True,
        "time_semantic_status_counts": statuses,
    }


def execute_legacy(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = _find_project_root(Path(__file__).resolve().parent)
    adapter_root = _resolve_from_root(project_root, args.adapter_root) if args.adapter_root else project_root
    source_path = _resolve_from_root(project_root, args.input)
    output_dir = _resolve_from_root(project_root, args.output_dir)
    registry_path = _resolve_from_root(project_root, args.registry or DEFAULT_REGISTRY)
    for path, label in (
        (adapter_root, "adapter root"),
        (source_path, "source input"),
        (output_dir, "output directory"),
        (registry_path, "format registry"),
    ):
        _require_within(path, project_root, label)
    if not source_path.is_file():
        raise CanonicalIngressGatewayError(f"source input is not a file: {source_path}")
    if output_dir == source_path or source_path in output_dir.parents:
        raise CanonicalIngressGatewayError("output directory must not replace or contain the source file")
    if not RUN_ID_PATTERN.fullmatch(args.run_id or ""):
        raise CanonicalIngressGatewayError("run_id contains unsupported characters")
    source_hash_before = _sha256_file(source_path)
    source = _load_json(source_path, "source input")
    registry_payload = _load_json(registry_path, "format registry")
    formats = _validate_registry(registry_payload)
    detection = detect_format(source, formats)
    if detection["status"] != "exact":
        raise FormatDetectionError(str(detection["stop_reason"]))
    definition = detection.pop("definition")
    if not isinstance(definition, Mapping):
        raise FormatDetectionError("exact detection did not select a registered format")
    source_file_id = "sha256:" + source_hash_before
    created_at = _utc_now_z()
    adapter_definition = definition["adapter"]
    native = bool(adapter_definition["native_passthrough"])
    if native:
        envelope_id = str(source.get("source_envelope_id"))
    else:
        envelope_id = "sha256:" + _sha256_text(_canonical_json({
            "source_sha256": source_hash_before,
            "format_family": definition["format_family"],
            "fingerprint_version": definition["fingerprint_version"],
            "contract": CANONICAL_SCHEMA_VERSION,
        }))
    validator = _load_validator(adapter_root)
    if native:
        canonical = dict(source)
        validation = validator.validate_canonical_payload(canonical)
        lineage = [{
            "canonical_path": "/",
            "source_path": "/",
            "mapping_method": "direct",
            "loss_status": "lossless",
        }]
        conservation = _native_conservation(canonical, validation)
        adapter_script = Path(__file__).resolve()
    else:
        adapter_script = _adapter_path(adapter_root, definition)
        adapter_module = _load_module(
            f"canonical_ingress_adapter_{definition['format_family']}_{definition['fingerprint_version']}",
            adapter_script,
        )
        try:
            adapted = adapter_module.adapt_payload(
                source,
                source_file_id=source_file_id,
                envelope_id=envelope_id,
                canonicalized_at=created_at,
            )
        except Exception as exc:
            raise AdapterExecutionError(f"registered adapter failed: {type(exc).__name__}: {exc}") from exc
        if not isinstance(adapted, Mapping) or adapted.get("status") != "completed":
            raise AdapterExecutionError("registered adapter did not return status=completed")
        canonical = adapted.get("canonical")
        lineage = adapted.get("lineage")
        conservation = adapted.get("conservation")
        if not isinstance(lineage, list) or not isinstance(conservation, Mapping):
            raise AdapterExecutionError("registered adapter result is missing lineage or conservation")
        validation = validator.validate_canonical_payload(canonical)
    if conservation.get("dropped_records") != 0 or conservation.get("unmapped_records") != 0:
        raise AdapterExecutionError("adapter conservation contains dropped or unmapped records")
    if conservation.get("counts_reconciled") is not True or conservation.get("identities_unique") is not True or conservation.get("graph_integrity") is not True:
        raise AdapterExecutionError("adapter conservation or identity checks failed")
    if dict(conservation.get("output_counts", {})) != dict(validation.get("counts", {})):
        raise AdapterExecutionError("adapter output counts do not match contract validator counts")
    source_hash_after_validation = _sha256_file(source_path)
    if source_hash_after_validation != source_hash_before:
        raise CanonicalIngressGatewayError("source input changed during adaptation")
    if args.dry_run:
        return {
            "status": "dry_run",
            "run_id": args.run_id,
            "format_family": definition["format_family"],
            "fingerprint_version": definition["fingerprint_version"],
            "adapter_family": adapter_definition["family"],
            "adapter_version": adapter_definition["version"],
            "canonical_schema": CANONICAL_SCHEMA_VERSION,
            "validation": validation,
            "conservation": dict(conservation),
            "raw_unchanged": True,
        }
    if output_dir.exists() and any(output_dir.iterdir()):
        raise CanonicalIngressGatewayError(f"output directory must be absent or empty: {output_dir}")
    canonical_path = output_dir / "canonical_conversations_v0001.json"
    lineage_path = output_dir / "source_to_canonical_lineage_v0001.jsonl"
    validation_path = output_dir / "canonical_contract_validation_v0001.json"
    envelope_path = output_dir / "canonical_ingress_envelope_v0001.json"
    _atomic_write_json(canonical_path, canonical)
    _atomic_write_jsonl(lineage_path, [dict(row) for row in lineage])
    validation_report = {
        **dict(validation),
        "canonical_path": _relative(canonical_path, project_root),
        "canonical_sha256": _sha256_file(canonical_path),
        "registry_path": _relative(registry_path, project_root),
        "registry_sha256": _sha256_file(registry_path),
    }
    _atomic_write_json(validation_path, validation_report)
    canonical_hash = _sha256_file(canonical_path)
    lineage_hash = _sha256_file(lineage_path)
    validation_hash = _sha256_file(validation_path)
    source_hash_after = _sha256_file(source_path)
    envelope = {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "envelope_id": envelope_id,
        "contract_version": "v0001",
        "created_at": created_at,
        "run_id": args.run_id,
        "status": "completed",
        "source": {
            "source_file_id": source_file_id,
            "project_relative_path": _relative(source_path, project_root),
            "sha256": source_hash_before,
            "size_bytes": source_path.stat().st_size,
            "container_type": "json_object" if isinstance(source, Mapping) else "json_array",
            "encoding": "utf-8-sig",
            "immutable_verified": source_hash_after == source_hash_before,
        },
        "detection": detection,
        "adapter": {
            "family": adapter_definition["family"],
            "version": adapter_definition["version"],
            "entry_point": str(adapter_definition["entry_point"]),
            "script_sha256": _sha256_file(adapter_script),
            "native_passthrough": native,
            "status": "completed",
        },
        "canonical_output": {
            "schema_version": CANONICAL_SCHEMA_VERSION,
            "contract_version": "v0001",
            "project_relative_path": _relative(canonical_path, project_root),
            "sha256": canonical_hash,
            "validation_report_path": _relative(validation_path, project_root),
            "validation_report_sha256": validation_hash,
            "lineage_index_path": _relative(lineage_path, project_root),
            "lineage_index_sha256": lineage_hash,
            "contract_valid": True,
        },
        "conservation": dict(conservation),
        "lineage": {
            "source_to_canonical_traceable": bool(lineage),
            "raw_unchanged": source_hash_after == source_hash_before,
            "warnings": [],
            "issues": [],
        },
    }
    _atomic_write_json(envelope_path, envelope)
    return {
        "status": "completed",
        "run_id": args.run_id,
        "format_family": definition["format_family"],
        "fingerprint_version": definition["fingerprint_version"],
        "adapter_family": adapter_definition["family"],
        "adapter_version": adapter_definition["version"],
        "canonical_path": str(canonical_path),
        "canonical_sha256": canonical_hash,
        "envelope_path": str(envelope_path),
        "envelope_id": envelope_id,
        "validation_report": str(validation_path),
        "lineage_index": str(lineage_path),
        "conservation": dict(conservation),
        "raw_unchanged": source_hash_after == source_hash_before,
    }


def _atomic_copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        shutil.copyfile(source, temporary)
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _native_conservation_streaming(
    source_path: Path,
    validation: Mapping[str, Any],
    stream_module: ModuleType,
) -> Dict[str, Any]:
    counts = dict(validation["counts"])
    statuses = {"verified": 0, "unverified": 0, "ambiguous": 0, "invalid": 0, "missing": 0}
    for _, conversation in stream_module.iter_object_array(source_path, "conversations"):
        if not isinstance(conversation, Mapping):
            continue
        for field in ("created_time", "updated_time"):
            fact = conversation.get(field)
            if isinstance(fact, Mapping) and str(fact.get("semantic_status")) in statuses:
                statuses[str(fact["semantic_status"])] += 1
        for node in conversation.get("nodes", []):
            if not isinstance(node, Mapping) or not isinstance(node.get("message"), Mapping):
                continue
            for fact in node["message"].get("event_times", []):
                if isinstance(fact, Mapping) and str(fact.get("semantic_status")) in statuses:
                    statuses[str(fact["semantic_status"])] += 1
    return {
        "input_counts": counts,
        "output_counts": counts,
        "dropped_records": 0,
        "degraded_records": 0,
        "extension_preserved_records": 0,
        "unmapped_records": 0,
        "source_ids_reused": counts["conversations"] + counts["messages"],
        "stable_ids_synthesized": 0,
        "sequence_relationships_derived": 0,
        "counts_reconciled": True,
        "identities_unique": True,
        "graph_integrity": True,
        "time_semantic_status_counts": statuses,
    }


def execute(args: argparse.Namespace) -> Dict[str, Any]:
    project_root = _find_project_root(Path(__file__).resolve().parent)
    adapter_root = _resolve_from_root(project_root, args.adapter_root) if args.adapter_root else project_root
    source_path = _resolve_from_root(project_root, args.input)
    output_dir = _resolve_from_root(project_root, args.output_dir)
    registry_path = _resolve_from_root(project_root, args.registry or DEFAULT_REGISTRY)
    for path, label in (
        (adapter_root, "adapter root"),
        (source_path, "source input"),
        (output_dir, "output directory"),
        (registry_path, "format registry"),
    ):
        _require_within(path, project_root, label)
    if not source_path.is_file():
        raise CanonicalIngressGatewayError(f"source input is not a file: {source_path}")
    if output_dir == source_path or source_path in output_dir.parents:
        raise CanonicalIngressGatewayError("output directory must not replace or contain the source file")
    if not RUN_ID_PATTERN.fullmatch(args.run_id or ""):
        raise CanonicalIngressGatewayError("run_id contains unsupported characters")
    if not args.dry_run and output_dir.exists() and any(output_dir.iterdir()):
        raise CanonicalIngressGatewayError(f"output directory must be absent or empty: {output_dir}")

    source_hash_before = _sha256_file(source_path)
    registry_payload = _load_json(registry_path, "format registry")
    formats = _validate_registry(registry_payload)
    stream_module = _load_stream_module(adapter_root)
    source_probe, container_type, source_item_count = _stream_detection_probe(source_path, stream_module)
    detection = detect_format(source_probe, formats)
    if detection["status"] != "exact":
        raise FormatDetectionError(str(detection["stop_reason"]))
    definition = detection.pop("definition")
    if not isinstance(definition, Mapping):
        raise FormatDetectionError("exact detection did not select a registered format")

    source_file_id = "sha256:" + source_hash_before
    created_at = _utc_now_z()
    adapter_definition = definition["adapter"]
    native = bool(adapter_definition["native_passthrough"])
    envelope_id = (
        str(source_probe.get("source_envelope_id"))
        if native
        else "sha256:" + _sha256_text(_canonical_json({
            "source_sha256": source_hash_before,
            "format_family": definition["format_family"],
            "fingerprint_version": definition["fingerprint_version"],
            "contract": CANONICAL_SCHEMA_VERSION,
        }))
    )
    validator = _load_validator(adapter_root)
    adapter_script = Path(__file__).resolve() if native else _adapter_path(adapter_root, definition)

    temporary_context: Any = tempfile.TemporaryDirectory(prefix="canonical_ingress_dry_run_") if args.dry_run else None
    working_dir = Path(temporary_context.name) if temporary_context is not None else output_dir
    canonical_path = working_dir / "canonical_conversations_v0001.json"
    lineage_path = working_dir / "source_to_canonical_lineage_v0001.jsonl"
    validation_path = output_dir / "canonical_contract_validation_v0002.json"
    envelope_path = output_dir / "canonical_ingress_envelope_v0001.json"
    try:
        if native:
            validation = validator.validate_file_streaming(source_path)
            conservation = _native_conservation_streaming(source_path, validation, stream_module)
            _atomic_copy_file(source_path, canonical_path)
            _atomic_write_jsonl(lineage_path, [{
                "canonical_path": "/",
                "source_path": "/",
                "mapping_method": "direct",
                "loss_status": "lossless",
            }])
            adapter_streaming = True
        else:
            adapter_module = _load_module(
                f"canonical_ingress_adapter_{definition['format_family']}_{definition['fingerprint_version']}",
                adapter_script,
            )
            if hasattr(adapter_module, "adapt_file_streaming"):
                adapted = adapter_module.adapt_file_streaming(
                    source_path,
                    canonical_path=canonical_path,
                    lineage_path=lineage_path,
                    source_file_id=source_file_id,
                    envelope_id=envelope_id,
                    canonicalized_at=created_at,
                )
                adapter_streaming = True
            else:
                if source_path.stat().st_size > MAX_NON_STREAMING_ADAPTER_BYTES:
                    raise AdapterExecutionError(
                        "registered adapter has no streaming file interface and input exceeds the 64 MiB safety limit"
                    )
                source = _load_json(source_path, "source input")
                adapted = adapter_module.adapt_payload(
                    source,
                    source_file_id=source_file_id,
                    envelope_id=envelope_id,
                    canonicalized_at=created_at,
                )
                _atomic_write_json(canonical_path, adapted["canonical"])
                _atomic_write_jsonl(lineage_path, [dict(row) for row in adapted["lineage"]])
                adapter_streaming = False
            if not isinstance(adapted, Mapping) or adapted.get("status") != "completed":
                raise AdapterExecutionError("registered adapter did not return status=completed")
            conservation = adapted.get("conservation")
            if not isinstance(conservation, Mapping):
                raise AdapterExecutionError("registered adapter result is missing conservation")
            validation = validator.validate_file_streaming(canonical_path)

        if conservation.get("dropped_records") != 0 or conservation.get("unmapped_records") != 0:
            raise AdapterExecutionError("adapter conservation contains dropped or unmapped records")
        if conservation.get("counts_reconciled") is not True or conservation.get("identities_unique") is not True or conservation.get("graph_integrity") is not True:
            raise AdapterExecutionError("adapter conservation or identity checks failed")
        if dict(conservation.get("output_counts", {})) != dict(validation.get("counts", {})):
            raise AdapterExecutionError("adapter output counts do not match contract validator counts")
        source_hash_after_validation = _sha256_file(source_path)
        if source_hash_after_validation != source_hash_before:
            raise CanonicalIngressGatewayError("source input changed during adaptation")
        if args.dry_run:
            return {
                "status": "dry_run",
                "run_id": args.run_id,
                "format_family": definition["format_family"],
                "fingerprint_version": definition["fingerprint_version"],
                "adapter_family": adapter_definition["family"],
                "adapter_version": adapter_definition["version"],
                "canonical_schema": CANONICAL_SCHEMA_VERSION,
                "validation": validation,
                "conservation": dict(conservation),
                "raw_unchanged": True,
                "streaming": {"gateway": True, "adapter": adapter_streaming, "source_items": source_item_count},
            }

        canonical_hash = _sha256_file(canonical_path)
        lineage_hash = _sha256_file(lineage_path)
        validation_report = {
            **dict(validation),
            "canonical_path": _relative(canonical_path, project_root),
            "canonical_sha256": canonical_hash,
            "registry_path": _relative(registry_path, project_root),
            "registry_sha256": _sha256_file(registry_path),
        }
        _atomic_write_json(validation_path, validation_report)
        validation_hash = _sha256_file(validation_path)
        source_hash_after = _sha256_file(source_path)
        envelope = {
            "schema_version": ENVELOPE_SCHEMA_VERSION,
            "envelope_id": envelope_id,
            "contract_version": "v0001",
            "created_at": created_at,
            "run_id": args.run_id,
            "status": "completed",
            "source": {
                "source_file_id": source_file_id,
                "project_relative_path": _relative(source_path, project_root),
                "sha256": source_hash_before,
                "size_bytes": source_path.stat().st_size,
                "container_type": container_type,
                "encoding": "utf-8-sig",
                "immutable_verified": source_hash_after == source_hash_before,
            },
            "detection": detection,
            "adapter": {
                "family": adapter_definition["family"],
                "version": adapter_definition["version"],
                "entry_point": str(adapter_definition["entry_point"]),
                "script_sha256": _sha256_file(adapter_script),
                "native_passthrough": native,
                "status": "completed",
                "streaming": adapter_streaming,
            },
            "canonical_output": {
                "schema_version": CANONICAL_SCHEMA_VERSION,
                "contract_version": "v0001",
                "project_relative_path": _relative(canonical_path, project_root),
                "sha256": canonical_hash,
                "validation_report_path": _relative(validation_path, project_root),
                "validation_report_sha256": validation_hash,
                "lineage_index_path": _relative(lineage_path, project_root),
                "lineage_index_sha256": lineage_hash,
                "contract_valid": True,
            },
            "conservation": dict(conservation),
            "lineage": {
                "source_to_canonical_traceable": lineage_path.stat().st_size > 0,
                "raw_unchanged": source_hash_after == source_hash_before,
                "warnings": [],
                "issues": [],
            },
            "streaming": {
                "gateway": True,
                "adapter": adapter_streaming,
                "validator": bool((validation.get("streaming") or {}).get("enabled")),
                "source_items": source_item_count,
            },
        }
        _atomic_write_json(envelope_path, envelope)
        return {
            "status": "completed",
            "run_id": args.run_id,
            "format_family": definition["format_family"],
            "fingerprint_version": definition["fingerprint_version"],
            "adapter_family": adapter_definition["family"],
            "adapter_version": adapter_definition["version"],
            "canonical_path": str(canonical_path),
            "canonical_sha256": canonical_hash,
            "envelope_path": str(envelope_path),
            "envelope_id": envelope_id,
            "validation_report": str(validation_path),
            "lineage_index": str(lineage_path),
            "conservation": dict(conservation),
            "raw_unchanged": source_hash_after == source_hash_before,
            "streaming": envelope["streaming"],
        }
    except Exception:
        if not args.dry_run:
            canonical_path.unlink(missing_ok=True)
            lineage_path.unlink(missing_ok=True)
            validation_path.unlink(missing_ok=True)
            envelope_path.unlink(missing_ok=True)
        raise
    finally:
        if temporary_context is not None:
            temporary_context.cleanup()


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def gateway_contract() -> Dict[str, Any]:
    return {
        "registry_schema": REGISTRY_SCHEMA_VERSION,
        "canonical_schema": CANONICAL_SCHEMA_VERSION,
        "envelope_schema": ENVELOPE_SCHEMA_VERSION,
        "unknown_format_policy": "reject",
        "ambiguous_format_policy": "reject",
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detect and adapt one immutable source into the complete canonical ingress contract.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--registry", default="")
    parser.add_argument("--adapter-root", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = execute(args)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except CanonicalIngressGatewayError as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, separators=(",", ":")))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
