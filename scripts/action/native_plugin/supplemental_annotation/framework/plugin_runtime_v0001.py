# ============================================================
# 文件名: plugin_runtime_v0001.py
# 中文名: 原生标注插件隔离运行脚本
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / runtime
# 脚本定位: Framework 对单个已登记插件执行导入和批处理调用的超时隔离子进程
#
# 职责说明:
# - 从 stdin 接收规范批次并加载 CLI 指定的固定插件文件
# - 输出 plugin metadata 与原始 plugin result 的机器可读 envelope
#
# 本脚本做什么:
# - 使用 Python 标准库 importlib 在独立进程内导入并调用 annotate_batch
# - 校验预期插件名、版本和调用接口，准确返回错误退出码
#
# 本脚本不做什么:
# - 不选择插件、不读取 Registry、不写数据库或 manifest
# - 不实现任何 Annotation 业务规则，不修改输入数据
#
# 制度边界声明:
# - 超时终止由父 Framework 控制；本进程不吞并插件异常
# - stdout 只输出单行 JSON，错误写入 stderr 且返回非零退出码
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: plugin_runtime_v0001
# family: plugin_runtime
# role: native_plugin_isolated_batch_runtime
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/framework/plugin_runtime_v0001.py
# input:
#   - stdin JSON batch envelope
#   - fixed plugin file, expected name, and expected version
# output:
#   - plugin_meta and plugin result JSON envelope
# depends_on:
#   - Python stdlib: argparse, importlib, json, pathlib, sys, typing
# used_by:
#   - supplemental_annotation_runner_v0001
# ============================================================

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Mapping, Optional, Sequence


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "plugin_runtime"
SCRIPT_NAME = "plugin_runtime_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 异常类型
# ============================================================

class PluginRuntimeError(RuntimeError):
    """Raised when a plugin cannot be imported or invoked under the runtime contract."""


# ============================================================
# 数据结构
# ============================================================

# The stdin and stdout envelopes remain versioned JSON mappings.


# ============================================================
# 工具函数区
# ============================================================

def _safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _load_plugin(path: Path) -> ModuleType:
    if not path.is_file():
        raise PluginRuntimeError(f"plugin file is missing: {path}")
    module_name = f"isolated_native_plugin_{abs(hash(str(path.resolve())))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PluginRuntimeError(f"cannot create plugin import spec: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_stdin() -> Dict[str, Any]:
    try:
        payload = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        raise PluginRuntimeError(f"stdin is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PluginRuntimeError("stdin envelope must be an object")
    return payload


# ============================================================
# 默认映射
# ============================================================

PLUGIN_CALLBACK = "annotate_batch"


# ============================================================
# 核心业务组件
# ============================================================

def execute(args: argparse.Namespace) -> Dict[str, Any]:
    module = _load_plugin(Path(args.plugin).resolve())
    metadata = getattr(module, "PLUGIN_META", None)
    if not isinstance(metadata, Mapping):
        raise PluginRuntimeError("plugin does not expose PLUGIN_META mapping")
    if metadata.get("name") != args.expected_name:
        raise PluginRuntimeError("plugin name does not match expected registry identity")
    if metadata.get("version") != args.expected_version:
        raise PluginRuntimeError("plugin version does not match expected registry identity")
    callback = getattr(module, PLUGIN_CALLBACK, None)
    if not callable(callback):
        raise PluginRuntimeError(f"plugin does not expose callable {PLUGIN_CALLBACK}")
    payload = _load_stdin()
    records = payload.get("records")
    run_context = payload.get("run_context")
    plugin_config = payload.get("plugin_config")
    if not isinstance(records, list):
        raise PluginRuntimeError("stdin records must be a list")
    if not isinstance(run_context, Mapping) or not isinstance(plugin_config, Mapping):
        raise PluginRuntimeError("stdin run_context and plugin_config must be mappings")
    result = callback(records, run_context, plugin_config)
    if not isinstance(result, Mapping):
        raise PluginRuntimeError("plugin result must be a mapping")
    return {"plugin_meta": dict(metadata), "result": dict(result)}


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def runtime_summary() -> Dict[str, Any]:
    return {
        "status": "completed",
        "runtime": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "isolation": "subprocess",
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Invoke one fixed Native Plugin batch.")
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--expected-name", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--dry-run", action="store_true", help="Validate runtime arguments; stdin callback still remains read-only.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = execute(args)
    except PluginRuntimeError as exc:
        print(
            _safe_json(
                {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        print(
            _safe_json(
                {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
            ),
            file=sys.stderr,
        )
        return 3
    print(_safe_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
