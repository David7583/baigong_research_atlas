# ============================================================
# 文件名: annotation_contract_v0001.py
# 中文名: 补充标注插件契约校验脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / contract
# 脚本定位: Supplemental Annotation Framework 的纯契约与稳定身份校验边界
#
# 职责说明:
# - 定义 Framework 配置、Plugin Registry、Plugin Result 与 Annotation Envelope 契约
# - 将 canonical text unit 绑定到现有七字段稳定复合坐标
#
# 本脚本做什么:
# - 使用 Python 标准库校验 JSON 映射和插件运行结果
# - 返回规范化且语义稳定的配置、身份和标注记录
#
# 本脚本不做什么:
# - 不加载或执行插件，不写数据库，不识别时间
# - 不修改 canonical text，不制造缺失的身份坐标
#
# 制度边界声明:
# - 所有校验先于持久化副作用，非法契约必须明确失败
# - 原始事实与派生标注分离，annotation 只能引用 canonical identity
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: annotation_contract_v0001
# family: annotation_contract
# role: supplemental_annotation_contract_validator
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/framework/annotation_contract_v0001.py
# input:
#   - supplemental annotation config JSON
#   - native plugin registry JSON
#   - canonical text unit mappings
#   - plugin result mappings
# output:
#   - validated framework configuration, stable identities, and annotation envelopes
# depends_on:
#   - Python stdlib: dataclasses, typing
# used_by:
#   - annotation_registry_v0001
#   - supplemental_annotation_runner_v0001
# ============================================================

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Sequence, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "annotation_contract"
SCRIPT_NAME = "annotation_contract_v0001"
SCRIPT_VERSION = "v0001"
CONTRACT_VERSION = "supplemental_annotation_contract_v0001"
CONFIG_SCHEMA_VERSION = "supplemental_annotation_config_v0001"
REGISTRY_SCHEMA_VERSION = "native_plugin_registry_v0001"

STABLE_COORDINATE_FIELDS: Tuple[str, ...] = (
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
)

ANNOTATION_REQUIRED_FIELDS: Tuple[str, ...] = (
    "annotation_id",
    "annotation_type",
    "annotation_subtype",
    "text_unit_id",
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
    "span_start",
    "span_end",
    "source_kind",
    "annotator",
    "annotator_version",
    "rule_version",
    "confidence",
    "resolution_status",
    "payload",
)


# ============================================================
# 异常类型
# ============================================================

class AnnotationContractError(RuntimeError):
    """Raised when framework, plugin, identity, or annotation contracts are invalid."""


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class PluginDeclaration:
    name: str
    version: str
    capability: str
    entry_point: str


@dataclass(frozen=True)
class PluginExecutionConfig:
    name: str
    enabled: bool
    required: bool
    version: str
    timeout_seconds: int
    config: Dict[str, Any]


@dataclass(frozen=True)
class FrameworkConfig:
    schema_version: str
    framework_version: str
    batch_size: int
    execution_order: Tuple[str, ...]
    plugins: Dict[str, PluginExecutionConfig]


# ============================================================
# 工具函数区
# ============================================================

def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AnnotationContractError(f"{label} must be a mapping")
    return value


def _require_non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AnnotationContractError(f"{label} must be a non-empty string")
    return value.strip()


def _require_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AnnotationContractError(f"{label} must be an integer")
    return value


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise AnnotationContractError(f"{label} must be a boolean")
    return value


def _require_string_sequence(value: Any, label: str) -> Tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise AnnotationContractError(f"{label} must be a string list")
    result = tuple(_require_non_empty_string(item, f"{label} item") for item in value)
    if len(result) != len(set(result)):
        raise AnnotationContractError(f"{label} contains duplicate values")
    return result


# ============================================================
# 默认映射
# ============================================================

ALLOWED_PLUGIN_STATUSES = frozenset({"completed", "skipped"})
ALLOWED_RESOLUTION_STATUSES = frozenset({"resolved", "unresolved", "ambiguous"})


# ============================================================
# 核心契约
# ============================================================

def validate_registry(payload: Any) -> Dict[str, PluginDeclaration]:
    root = _require_mapping(payload, "plugin registry")
    if root.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        raise AnnotationContractError("plugin registry schema_version mismatch")
    if root.get("contract_version") != CONTRACT_VERSION:
        raise AnnotationContractError("plugin registry contract_version mismatch")
    raw_plugins = root.get("plugins")
    if not isinstance(raw_plugins, list):
        raise AnnotationContractError("plugin registry plugins must be a list")
    declarations: Dict[str, PluginDeclaration] = {}
    for index, item in enumerate(raw_plugins):
        row = _require_mapping(item, f"plugin registry item {index}")
        declaration = PluginDeclaration(
            name=_require_non_empty_string(row.get("name"), f"plugin {index} name"),
            version=_require_non_empty_string(row.get("version"), f"plugin {index} version"),
            capability=_require_non_empty_string(row.get("capability"), f"plugin {index} capability"),
            entry_point=_require_non_empty_string(row.get("entry_point"), f"plugin {index} entry_point"),
        )
        if declaration.name in declarations:
            raise AnnotationContractError(f"duplicate plugin declaration: {declaration.name}")
        if declaration.capability != "supplemental_annotation":
            raise AnnotationContractError(f"unsupported plugin capability: {declaration.capability}")
        declarations[declaration.name] = declaration
    return declarations


def validate_framework_config(payload: Any) -> FrameworkConfig:
    root = _require_mapping(payload, "framework config")
    if root.get("schema_version") != CONFIG_SCHEMA_VERSION:
        raise AnnotationContractError("framework config schema_version mismatch")
    if root.get("framework_version") != SCRIPT_VERSION:
        raise AnnotationContractError("framework config framework_version mismatch")
    batch_size = _require_integer(root.get("batch_size"), "framework config batch_size")
    if batch_size < 1 or batch_size > 10000:
        raise AnnotationContractError("framework config batch_size must be between 1 and 10000")
    execution_order = _require_string_sequence(root.get("execution_order"), "execution_order")
    raw_plugins = _require_mapping(root.get("plugins"), "framework config plugins")
    plugins: Dict[str, PluginExecutionConfig] = {}
    for name, value in raw_plugins.items():
        plugin_name = _require_non_empty_string(name, "plugin config name")
        row = _require_mapping(value, f"plugin config {plugin_name}")
        config_value = row.get("config", {})
        config_mapping = _require_mapping(config_value, f"plugin config payload {plugin_name}")
        plugins[plugin_name] = PluginExecutionConfig(
            name=plugin_name,
            enabled=_require_bool(row.get("enabled"), f"plugin {plugin_name} enabled"),
            required=_require_bool(row.get("required"), f"plugin {plugin_name} required"),
            version=_require_non_empty_string(row.get("version"), f"plugin {plugin_name} version"),
            timeout_seconds=_require_integer(
                row.get("timeout_seconds", 60), f"plugin {plugin_name} timeout_seconds"
            ),
            config=dict(config_mapping),
        )
        if not 1 <= plugins[plugin_name].timeout_seconds <= 3600:
            raise AnnotationContractError(
                f"plugin {plugin_name} timeout_seconds must be between 1 and 3600"
            )
    if set(execution_order) != set(plugins):
        raise AnnotationContractError("execution_order must list every configured plugin exactly once")
    return FrameworkConfig(
        schema_version=CONFIG_SCHEMA_VERSION,
        framework_version=SCRIPT_VERSION,
        batch_size=batch_size,
        execution_order=execution_order,
        plugins=plugins,
    )


def bind_stable_identity(record: Any, line_number: int) -> Dict[str, Any]:
    row = _require_mapping(record, f"canonical record at line {line_number}")
    identity: Dict[str, Any] = {}
    for field in STABLE_COORDINATE_FIELDS:
        if field not in row:
            raise AnnotationContractError(
                f"canonical record at line {line_number} is missing stable coordinate: {field}"
            )
        value = row[field]
        if field in {"asset_id", "path"}:
            identity[field] = _require_non_empty_string(value, f"line {line_number} {field}")
        else:
            identity[field] = _require_integer(value, f"line {line_number} {field}")
    if identity["char_start"] < 0 or identity["char_end"] < identity["char_start"]:
        raise AnnotationContractError(f"canonical record at line {line_number} has invalid char coordinates")
    text_unit_id = row.get("unit_id") or row.get("text_unit_id")
    if text_unit_id is not None:
        text_unit_id = _require_non_empty_string(text_unit_id, f"line {line_number} text_unit_id")
    identity["text_unit_id"] = text_unit_id
    return identity


def validate_plugin_meta(meta: Any, declaration: PluginDeclaration) -> Dict[str, Any]:
    row = _require_mapping(meta, f"plugin metadata for {declaration.name}")
    name = _require_non_empty_string(row.get("name"), "plugin metadata name")
    version = _require_non_empty_string(row.get("version"), "plugin metadata version")
    contract_version = _require_non_empty_string(
        row.get("contract_version"), "plugin metadata contract_version"
    )
    if name != declaration.name or version != declaration.version:
        raise AnnotationContractError(f"plugin metadata does not match registry: {declaration.name}")
    if contract_version != CONTRACT_VERSION:
        raise AnnotationContractError(f"plugin contract mismatch: {declaration.name}")
    return dict(row)


def validate_plugin_result(
    result: Any,
    declaration: PluginDeclaration,
    expected_records: int,
) -> Dict[str, Any]:
    row = _require_mapping(result, f"plugin result for {declaration.name}")
    status = _require_non_empty_string(row.get("status"), "plugin result status")
    if status not in ALLOWED_PLUGIN_STATUSES:
        raise AnnotationContractError(f"plugin returned invalid status: {status}")
    if row.get("plugin_name") != declaration.name or row.get("plugin_version") != declaration.version:
        raise AnnotationContractError(f"plugin result identity mismatch: {declaration.name}")
    records_scanned = _require_integer(row.get("records_scanned"), "plugin records_scanned")
    if records_scanned != expected_records:
        raise AnnotationContractError(
            f"plugin records_scanned mismatch: expected {expected_records}, got {records_scanned}"
        )
    annotations = row.get("annotations")
    if not isinstance(annotations, list):
        raise AnnotationContractError("plugin annotations must be a list")
    validated = [validate_annotation(item) for item in annotations]
    normalized = dict(row)
    normalized["annotations"] = validated
    return normalized


def validate_annotation(annotation: Any) -> Dict[str, Any]:
    row = _require_mapping(annotation, "annotation")
    missing = [field for field in ANNOTATION_REQUIRED_FIELDS if field not in row]
    if missing:
        raise AnnotationContractError(f"annotation is missing fields: {missing}")
    normalized = dict(row)
    for field in (
        "annotation_id",
        "annotation_type",
        "annotation_subtype",
        "asset_id",
        "path",
        "source_kind",
        "annotator",
        "annotator_version",
        "rule_version",
        "resolution_status",
    ):
        normalized[field] = _require_non_empty_string(normalized[field], f"annotation {field}")
    if normalized["resolution_status"] not in ALLOWED_RESOLUTION_STATUSES:
        raise AnnotationContractError("annotation resolution_status is invalid")
    for field in (
        "value_index",
        "segment_index",
        "sentence_index",
        "char_start",
        "char_end",
        "span_start",
        "span_end",
    ):
        normalized[field] = _require_integer(normalized[field], f"annotation {field}")
    if normalized["span_start"] < 0 or normalized["span_end"] <= normalized["span_start"]:
        raise AnnotationContractError("annotation span is invalid")
    confidence = normalized["confidence"]
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise AnnotationContractError("annotation confidence must be numeric")
    if not 0.0 <= float(confidence) <= 1.0:
        raise AnnotationContractError("annotation confidence must be between 0 and 1")
    normalized["confidence"] = float(confidence)
    normalized["payload"] = dict(_require_mapping(normalized["payload"], "annotation payload"))
    text_unit_id = normalized.get("text_unit_id")
    if text_unit_id is not None:
        normalized["text_unit_id"] = _require_non_empty_string(text_unit_id, "annotation text_unit_id")
    return normalized


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def contract_summary() -> Dict[str, Any]:
    return {
        "status": "completed",
        "contract_version": CONTRACT_VERSION,
        "stable_coordinate_fields": list(STABLE_COORDINATE_FIELDS),
        "annotation_required_fields": list(ANNOTATION_REQUIRED_FIELDS),
    }
