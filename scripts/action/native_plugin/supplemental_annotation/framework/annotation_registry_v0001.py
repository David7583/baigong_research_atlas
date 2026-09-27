# ============================================================
# 文件名: annotation_registry_v0001.py
# 中文名: 补充标注插件注册加载脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / registry
# 脚本定位: Framework 根据受控注册表加载固定版本插件的唯一入口
#
# 职责说明:
# - 读取显式 Plugin Registry 并解析固定相对入口
# - 解析 registered, enabled, compatible 插件的固定入口供隔离 runtime 调用
#
# 本脚本做什么:
# - 使用 Python 标准库解析明确登记的单个插件入口
# - 拒绝绝对入口、目录越界和版本声明不匹配
#
# 本脚本不做什么:
# - 不扫描目录，不自动选择最新版本，不执行插件业务
# - 不写配置、数据库或 canonical data
#
# 制度边界声明:
# - 插件入口必须相对 Supplemental Annotation 子系统根目录且不得越界
# - 未登记、未启用或不兼容插件必须准确失败
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: annotation_registry_v0001
# family: annotation_registry
# role: supplemental_annotation_plugin_loader
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/framework/annotation_registry_v0001.py
# input:
#   - config/plugin_registry_v0001.json
#   - validated PluginExecutionConfig
# output:
#   - validated fixed plugin entry declaration
# depends_on:
#   - annotation_contract_v0001
#   - Python stdlib: dataclasses, json, pathlib, typing
# used_by:
#   - supplemental_annotation_runner_v0001
# ============================================================

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Dict


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "annotation_registry"
SCRIPT_NAME = "annotation_registry_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 异常类型
# ============================================================

class AnnotationRegistryError(RuntimeError):
    """Raised when a registered plugin cannot be loaded safely."""


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class LoadedPlugin:
    declaration: Any
    entry_point: Path


# ============================================================
# 工具函数区
# ============================================================

def _load_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise AnnotationRegistryError(f"plugin registry is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
    except (OSError, json.JSONDecodeError) as exc:
        raise AnnotationRegistryError(f"cannot read plugin registry: {exc}") from exc
    if not isinstance(payload, dict):
        raise AnnotationRegistryError("plugin registry root must be an object")
    return payload


def _resolve_registered_entry(subsystem_root: Path, relative: str) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute():
        raise AnnotationRegistryError("plugin entry_point must be relative")
    resolved = (subsystem_root / candidate).resolve()
    try:
        resolved.relative_to(subsystem_root.resolve())
    except ValueError as exc:
        raise AnnotationRegistryError("plugin entry_point escapes subsystem root") from exc
    if not resolved.is_file():
        raise AnnotationRegistryError(f"registered plugin entry is missing: {resolved}")
    return resolved


# ============================================================
# 默认映射
# ============================================================

REQUIRED_PLUGIN_CALLABLE = "annotate_batch"


# ============================================================
# 核心类
# ============================================================

class PluginRegistry:
    def __init__(self, subsystem_root: Path, registry_path: Path, contract: ModuleType):
        self._subsystem_root = subsystem_root.resolve()
        self._registry_path = registry_path.resolve()
        self._contract = contract
        payload = _load_json(self._registry_path)
        self._declarations = contract.validate_registry(payload)

    @property
    def declarations(self) -> Dict[str, Any]:
        return dict(self._declarations)

    def load(self, execution_config: Any) -> LoadedPlugin:
        declaration = self._declarations.get(execution_config.name)
        if declaration is None:
            raise AnnotationRegistryError(f"plugin is not registered: {execution_config.name}")
        if declaration.version != execution_config.version:
            raise AnnotationRegistryError(
                f"registered plugin version mismatch for {execution_config.name}: "
                f"expected {execution_config.version}, got {declaration.version}"
            )
        entry_point = _resolve_registered_entry(self._subsystem_root, declaration.entry_point)
        return LoadedPlugin(
            declaration=declaration,
            entry_point=entry_point,
        )


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def registry_summary(registry: PluginRegistry) -> Dict[str, Any]:
    return {
        "status": "completed",
        "registered_plugins": sorted(registry.declarations),
        "auto_discovery": False,
    }
