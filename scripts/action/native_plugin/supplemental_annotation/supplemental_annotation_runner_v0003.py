# ============================================================
# 文件名: supplemental_annotation_runner_v0003.py
# 中文名: 自包含来源授权补充标注执行器
# 版本号: v0003
#
# 主层级: action
# 层级: scripts/action/native_plugin/supplemental_annotation
# 脚本定位: 保持已验证契约的自包含版本实现
#
# 职责说明:
# - 在本文件中完整实现本脚本职责，不加载同家族旧版
# 本脚本做什么:
# - 保留输入校验、处理顺序、来源证据与失败行为
# 本脚本不做什么:
# - 不替代独立业务子脚本，不迁移原始数据
# 制度边界声明:
# - 仅向显式配置目标写入；测试写入必须隔离，失败不得伪报成功
# - 原始输入只读；模型选择沿用配置，不包含密钥
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: supplemental_annotation_runner_v0003
# family: supplemental_annotation_runner
# role: supplemental_annotation_framework_runner
# version: v0003
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/supplemental_annotation_runner_v0003.py
# input:
#   - explicit CLI inputs and versioned configuration
# output:
#   - structured results and provenance manifests
# depends_on:
#   - Python stdlib
#   - annotation_contract_v0001.py
#   - annotation_persistence_v0001.py
#   - annotation_registry_v0001.py
#   - plugin_runtime_v0001.py
# used_by:
#   - data_action_chain_pipeline_v0015
# ============================================================

# 实现来源（仅溯源，不在运行时加载）: scripts/action/native_plugin/supplemental_annotation/supplemental_annotation_runner_v0001.py, scripts/action/native_plugin/supplemental_annotation/supplemental_annotation_runner_v0002.py
# ============================================================
# 依赖
# ============================================================

from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from typing import Any, Dict, Mapping, Optional, Sequence

# ============================================================
# 全局常量与默认映射
# ============================================================

MAX_ROOT_SEARCH_DEPTH = 12


DEFAULT_CONFIG = Path("config") / "supplemental_annotation_config_v0001.json"


DEFAULT_REGISTRY = Path("config") / "plugin_registry_v0001.json"


CONTRACT_MODULE = Path("framework") / "annotation_contract_v0001.py"


REGISTRY_MODULE = Path("framework") / "annotation_registry_v0001.py"


PERSISTENCE_MODULE = Path("framework") / "annotation_persistence_v0001.py"


PLUGIN_RUNTIME = Path("framework") / "plugin_runtime_v0001.py"


FRAMEWORK_STATUS_COMPLETED = "completed"


FRAMEWORK_STATUS_FAILED = "failed"


DEFAULT_ENCODING = "utf-8"


SCRIPT_FAMILY = "supplemental_annotation_runner"


SCRIPT_NAME = "supplemental_annotation_runner_v0003.py"


SCRIPT_VERSION = "v0003"


DEFAULT_REGISTRY_V2 = (
    Path("scripts")
    / "action"
    / "native_plugin"
    / "supplemental_annotation"
    / "config"
    / "plugin_registry_v0002.json"
)


SOURCE_ACCESS_SCHEMA_VERSION = "supplemental_annotation_source_access_v0001"


ALLOWED_SOURCE_ACCESS_DECLARATIONS = frozenset({"required", "not_required"})

# ============================================================
# 异常类型
# ============================================================


class SupplementalAnnotationError(RuntimeError):
    """Base exception for framework validation and orchestration failures."""


class FrameworkConfigError(SupplementalAnnotationError):
    """Raised when framework configuration cannot be used safely."""


class CanonicalInputError(SupplementalAnnotationError):
    """Raised when canonical input is missing, malformed, or unstable."""


class RequiredPluginError(SupplementalAnnotationError):
    """Raised when a required plugin fails or violates the contract."""


class SupplementalAnnotationRunnerV2Error(RuntimeError):
    """Raised when source access declarations or authorization are invalid."""


# ============================================================
# 数据结构、工具与核心实现
# ============================================================

SUBSYSTEM_ROOT = Path(__file__).resolve().parent


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_json(value: Any, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _ipc_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode(DEFAULT_ENCODING)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _find_project_root(start: Path) -> Path:
    current = start.resolve()
    for _ in range(MAX_ROOT_SEARCH_DEPTH):
        if (current / "AGENTS.md").is_file() and (current / "scripts").is_dir():
            return current
        if current.parent == current:
            break
        current = current.parent
    raise FrameworkConfigError(f"cannot locate project root from: {start}")


def _resolve_from_root(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _relative_display(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _load_json(path: Path, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise FrameworkConfigError(f"{label} is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
    except (OSError, json.JSONDecodeError) as exc:
        raise FrameworkConfigError(f"cannot read {label}: {exc}") from exc
    if not isinstance(payload, dict):
        raise FrameworkConfigError(f"{label} root must be an object")
    return payload


def _load_module(name: str, path: Path) -> ModuleType:
    if not path.is_file():
        raise FrameworkConfigError(f"framework module is missing: {path}")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise FrameworkConfigError(f"cannot load framework module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


def _write_text_atomic(path: Path, text: str) -> None:
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


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    _write_text_atomic(path, _safe_json(dict(payload), pretty=True) + "\n")


def _iter_bound_batches(
    input_path: Path, batch_size: int, contract: ModuleType
) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    with input_path.open("r", encoding=DEFAULT_ENCODING) as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                raise CanonicalInputError(f"invalid JSON at line {line_number}: {exc}") from exc
            if not isinstance(record, dict):
                raise CanonicalInputError(f"canonical record at line {line_number} must be an object")
            identity = contract.bind_stable_identity(record, line_number)
            batch.append({"record": record, "stable_identity": identity})
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _empty_plugin_summary(name: str, version: str, required: bool, started_at: str) -> Dict[str, Any]:
    return {
        "plugin_name": name,
        "plugin_version": version,
        "required": required,
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "records_scanned": 0,
        "annotations_created": 0,
        "annotations_skipped": 0,
        "annotations_invalid": 0,
        "stats": {},
        "error": None,
    }


def _merge_numeric_stats(target: Dict[str, int], source: Mapping[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, int) and (not isinstance(value, bool)):
            target[key] = target.get(key, 0) + value


def _invoke_plugin(
    loaded: Any, batch: Sequence[Mapping[str, Any]], run_context: Mapping[str, Any], plugin_config: Any
) -> Dict[str, Any]:
    runtime_path = (SUBSYSTEM_ROOT / PLUGIN_RUNTIME).resolve()
    if not runtime_path.is_file():
        raise FrameworkConfigError(f"plugin runtime is missing: {runtime_path}")
    payload = {
        "records": list(batch),
        "run_context": dict(run_context),
        "plugin_config": dict(plugin_config.config),
    }
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(runtime_path),
                "--plugin",
                str(loaded.entry_point),
                "--expected-name",
                loaded.declaration.name,
                "--expected-version",
                loaded.declaration.version,
            ],
            input=_ipc_json(payload),
            capture_output=True,
            text=True,
            encoding=DEFAULT_ENCODING,
            errors="replace",
            timeout=plugin_config.timeout_seconds,
            check=False,
            shell=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RequiredPluginError(
            f"plugin {loaded.declaration.name} exceeded {plugin_config.timeout_seconds} seconds"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no runtime output"
        raise RequiredPluginError(f"plugin runtime failed for {loaded.declaration.name}: {detail}")
    try:
        wrapper = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RequiredPluginError(
            f"plugin runtime returned invalid JSON for {loaded.declaration.name}"
        ) from exc
    if not isinstance(wrapper, dict) or not isinstance(wrapper.get("result"), dict):
        raise RequiredPluginError(f"plugin runtime returned invalid envelope for {loaded.declaration.name}")
    return wrapper


def _execute_core(args: argparse.Namespace, invoke_plugin) -> Dict[str, Any]:
    project_root = _find_project_root(Path(__file__).resolve().parent)
    input_path = _resolve_from_root(project_root, args.input)
    output_db = _resolve_from_root(project_root, args.output_db)
    manifest_path = _resolve_from_root(project_root, args.manifest)
    config_path = (
        _resolve_from_root(project_root, args.config)
        if args.config
        else (SUBSYSTEM_ROOT / DEFAULT_CONFIG).resolve()
    )
    registry_path = (
        _resolve_from_root(project_root, args.registry)
        if args.registry
        else (SUBSYSTEM_ROOT / DEFAULT_REGISTRY).resolve()
    )
    for path, label in (
        (input_path, "canonical input"),
        (output_db, "annotation database"),
        (manifest_path, "framework manifest"),
        (config_path, "framework config"),
        (registry_path, "plugin registry"),
    ):
        _require_within(path, project_root, label)
    if not input_path.is_file():
        raise CanonicalInputError(f"canonical input is not a file: {input_path}")
    if output_db == input_path or manifest_path == input_path:
        raise FrameworkConfigError("framework outputs must not overwrite canonical input")
    if output_db == manifest_path:
        raise FrameworkConfigError("annotation database and manifest paths must differ")
    contract = _load_module(
        "supplemental_annotation_contract_runtime_v0001", (SUBSYSTEM_ROOT / CONTRACT_MODULE).resolve()
    )
    registry_module = _load_module(
        "supplemental_annotation_registry_runtime_v0001", (SUBSYSTEM_ROOT / REGISTRY_MODULE).resolve()
    )
    persistence = _load_module(
        "supplemental_annotation_persistence_runtime_v0001", (SUBSYSTEM_ROOT / PERSISTENCE_MODULE).resolve()
    )
    config_payload = _load_json(config_path, "framework config")
    framework_config = contract.validate_framework_config(config_payload)
    registry = registry_module.PluginRegistry(SUBSYSTEM_ROOT, registry_path, contract)
    enabled_names = [
        name for name in framework_config.execution_order if framework_config.plugins[name].enabled
    ]
    run_id = args.run_id
    if not isinstance(run_id, str) or not run_id.strip():
        raise FrameworkConfigError("run_id must be a non-empty string")
    input_hash_before = _file_sha256(input_path)
    config_hash = _sha256_text(_safe_json(config_payload))
    started_at = _utc_now_iso()
    manifest: Dict[str, Any] = {
        "status": "dry-run" if args.dry_run else "running",
        "run_id": run_id,
        "framework": SCRIPT_NAME,
        "framework_version": SCRIPT_VERSION,
        "contract_version": contract.CONTRACT_VERSION,
        "started_at": started_at,
        "finished_at": None,
        "test_mode": bool(args.test_mode),
        "dry_run": bool(args.dry_run),
        "input_source": _relative_display(input_path, project_root),
        "input_hash_before": input_hash_before,
        "input_hash_after": None,
        "canonical_immutable": None,
        "config": _relative_display(config_path, project_root),
        "config_hash": config_hash,
        "registry": _relative_display(registry_path, project_root),
        "output_database": _relative_display(output_db, project_root),
        "manifest": _relative_display(manifest_path, project_root),
        "execution_order": list(framework_config.execution_order),
        "enabled_plugins": enabled_names,
        "records_scanned": 0,
        "plugins_completed": 0,
        "plugins_failed": 0,
        "annotations_created": 0,
        "annotations_skipped": 0,
        "annotations_invalid": 0,
        "side_effects_may_exist": False,
        "plugins": [],
        "error": None,
    }
    store: Optional[Any] = None
    run_started = False
    if not args.dry_run:
        store = persistence.AnnotationStore(output_db)
        store.initialize()
        store.start_run(
            {
                "run_id": run_id,
                "framework_version": SCRIPT_VERSION,
                "input_source": manifest["input_source"],
                "input_hash": input_hash_before,
                "config_hash": config_hash,
                "enabled_plugins": enabled_names,
                "started_at": started_at,
            }
        )
        run_started = True
    canonical_count: Optional[int] = None
    try:
        if not enabled_names:
            canonical_count = sum(
                (
                    len(batch)
                    for batch in _iter_bound_batches(input_path, framework_config.batch_size, contract)
                )
            )
        for name in framework_config.execution_order:
            plugin_config = framework_config.plugins[name]
            if not plugin_config.enabled:
                manifest["plugins"].append(
                    {
                        "plugin_name": name,
                        "plugin_version": plugin_config.version,
                        "required": plugin_config.required,
                        "status": "skipped",
                        "reason": "disabled_by_config",
                    }
                )
                continue
            plugin_started = _utc_now_iso()
            plugin_summary = _empty_plugin_summary(
                name, plugin_config.version, plugin_config.required, plugin_started
            )
            numeric_stats: Dict[str, int] = {}
            metadata: Dict[str, Any] = {}
            try:
                loaded = registry.load(plugin_config)
                for batch in _iter_bound_batches(input_path, framework_config.batch_size, contract):
                    wrapper = invoke_plugin(
                        loaded,
                        batch,
                        {
                            "run_id": run_id,
                            "framework_version": SCRIPT_VERSION,
                            "contract_version": contract.CONTRACT_VERSION,
                            "input_source": manifest["input_source"],
                        },
                        plugin_config,
                    )
                    metadata = contract.validate_plugin_meta(wrapper.get("plugin_meta"), loaded.declaration)
                    result = contract.validate_plugin_result(
                        wrapper["result"], loaded.declaration, len(batch)
                    )
                    annotations = result["annotations"]
                    created = len(annotations)
                    skipped = 0
                    if store is not None:
                        created, skipped = store.append_annotations(
                            annotations, run_id=run_id, created_at=_utc_now_iso()
                        )
                    plugin_summary["records_scanned"] += len(batch)
                    plugin_summary["annotations_created"] += created
                    plugin_summary["annotations_skipped"] += skipped
                    _merge_numeric_stats(numeric_stats, result.get("stats", {}))
                if canonical_count is None:
                    canonical_count = plugin_summary["records_scanned"]
                elif plugin_summary["records_scanned"] != canonical_count:
                    raise RequiredPluginError(f"plugin {name} did not scan the canonical record count")
                plugin_summary.update(
                    {
                        "status": "completed",
                        "finished_at": _utc_now_iso(),
                        "entry_point": _relative_display(loaded.entry_point, project_root),
                        "rule_version": metadata.get("rule_version")
                        or plugin_config.config.get("rule_version"),
                        "stats": numeric_stats,
                    }
                )
                manifest["plugins_completed"] += 1
                manifest["annotations_created"] += plugin_summary["annotations_created"]
                manifest["annotations_skipped"] += plugin_summary["annotations_skipped"]
                if not args.dry_run:
                    plugin_manifest_path = manifest_path.parent / "plugins" / f"{name}_run_manifest.json"
                    _write_json_atomic(plugin_manifest_path, plugin_summary)
                    plugin_summary["run_manifest"] = _relative_display(plugin_manifest_path, project_root)
            except Exception as exc:
                plugin_summary.update(
                    {
                        "status": "failed",
                        "finished_at": _utc_now_iso(),
                        "error": {"type": type(exc).__name__, "message": str(exc)},
                        "stats": numeric_stats,
                    }
                )
                manifest["plugins_failed"] += 1
                if plugin_config.required:
                    manifest["plugins"].append(plugin_summary)
                    raise RequiredPluginError(f"required plugin failed: {name}: {exc}") from exc
            manifest["plugins"].append(plugin_summary)
        manifest["records_scanned"] = canonical_count or 0
        input_hash_after = _file_sha256(input_path)
        manifest["input_hash_after"] = input_hash_after
        manifest["canonical_immutable"] = input_hash_before == input_hash_after
        if not manifest["canonical_immutable"]:
            raise CanonicalInputError("canonical input hash changed during annotation")
        manifest["status"] = "dry-run" if args.dry_run else FRAMEWORK_STATUS_COMPLETED
        manifest["finished_at"] = _utc_now_iso()
        if store is not None:
            store.finish_run(
                run_id,
                {
                    "finished_at": manifest["finished_at"],
                    "status": FRAMEWORK_STATUS_COMPLETED,
                    "records_scanned": manifest["records_scanned"],
                    "annotations_created": manifest["annotations_created"],
                    "annotations_skipped": manifest["annotations_skipped"],
                    "annotations_invalid": manifest["annotations_invalid"],
                    "side_effects_may_exist": False,
                    "error_summary": None,
                },
            )
        if not args.dry_run:
            _write_json_atomic(manifest_path, manifest)
        return manifest
    except Exception as exc:
        manifest["status"] = FRAMEWORK_STATUS_FAILED
        manifest["finished_at"] = _utc_now_iso()
        manifest["records_scanned"] = canonical_count or manifest["records_scanned"]
        manifest["side_effects_may_exist"] = manifest["annotations_created"] > 0
        manifest["error"] = {"type": type(exc).__name__, "message": str(exc)}
        try:
            input_hash_after = _file_sha256(input_path)
            manifest["input_hash_after"] = input_hash_after
            manifest["canonical_immutable"] = input_hash_before == input_hash_after
        except OSError:
            pass
        if store is not None and run_started:
            store.finish_run(
                run_id,
                {
                    "finished_at": manifest["finished_at"],
                    "status": FRAMEWORK_STATUS_FAILED,
                    "records_scanned": manifest["records_scanned"],
                    "annotations_created": manifest["annotations_created"],
                    "annotations_skipped": manifest["annotations_skipped"],
                    "annotations_invalid": manifest["annotations_invalid"],
                    "side_effects_may_exist": manifest["side_effects_may_exist"],
                    "error_summary": str(exc),
                },
            )
        if not args.dry_run:
            _write_json_atomic(manifest_path, manifest)
        raise


def _build_parser_core() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run registered Supplemental Annotation plugins against canonical text units."
    )
    parser.add_argument("--input", required=True, help="Canonical language text-units JSONL.")
    parser.add_argument("--output-db", required=True, help="Independent annotation SQLite output.")
    parser.add_argument("--manifest", required=True, help="Framework run manifest JSON.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--config", default=None, help="Project-relative framework config override.")
    parser.add_argument("--registry", default=None, help="Project-relative plugin registry override.")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _resolve(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise SupplementalAnnotationRunnerV2Error(f"{label} must remain within project root") from exc


def _load_object(path: Path, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise SupplementalAnnotationRunnerV2Error(f"{label} is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SupplementalAnnotationRunnerV2Error(f"{label} is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SupplementalAnnotationRunnerV2Error(f"{label} root must be an object")
    return payload


PROJECT_ROOT = _find_project_root(Path(__file__).resolve().parent)


def _validate_registry_source_access(
    registry: Mapping[str, Any], enabled_names: Sequence[str]
) -> Dict[str, str]:
    plugins = registry.get("plugins")
    if not isinstance(plugins, list):
        raise SupplementalAnnotationRunnerV2Error("plugin registry plugins must be a list")
    declarations: Dict[str, str] = {}
    for index, item in enumerate(plugins):
        if not isinstance(item, Mapping):
            raise SupplementalAnnotationRunnerV2Error(f"plugin registry item {index} must be an object")
        name = item.get("name")
        declaration = item.get("source_access")
        if not isinstance(name, str) or not name:
            raise SupplementalAnnotationRunnerV2Error(f"plugin registry item {index} has invalid name")
        if declaration not in ALLOWED_SOURCE_ACCESS_DECLARATIONS:
            raise SupplementalAnnotationRunnerV2Error(
                f"plugin {name} must declare source_access=required|not_required"
            )
        declarations[name] = str(declaration)
    missing = sorted(set(enabled_names) - set(declarations))
    if missing:
        raise SupplementalAnnotationRunnerV2Error(
            f"enabled plugins are missing source access declarations: {missing}"
        )
    return declarations


def _validate_access_manifest(access_path: Path, input_path: Path) -> Dict[str, Any]:
    access = _load_object(access_path, "source access manifest")
    if access.get("schema_version") != SOURCE_ACCESS_SCHEMA_VERSION or access.get("status") != "authorized":
        raise SupplementalAnnotationRunnerV2Error("source access manifest must be authorized v0001")
    authorization = access.get("authorization")
    if not isinstance(authorization, Mapping) or authorization.get("authorized") is not True:
        raise SupplementalAnnotationRunnerV2Error("source access authorization must be true")
    language = access.get("language_units")
    if not isinstance(language, Mapping):
        raise SupplementalAnnotationRunnerV2Error("source access manifest is missing language_units")
    authorized_input = _resolve(PROJECT_ROOT, str(language.get("project_relative_path", "")))
    if authorized_input != input_path:
        raise SupplementalAnnotationRunnerV2Error(
            "runner input must be the exact language units authorized by source access"
        )
    if _file_sha256(input_path) != language.get("sha256"):
        raise SupplementalAnnotationRunnerV2Error("runner input hash does not match source access")
    return access


def execute(args: argparse.Namespace) -> Dict[str, Any]:
    input_path = _resolve(PROJECT_ROOT, args.input)
    registry_path = _resolve(PROJECT_ROOT, args.registry or DEFAULT_REGISTRY_V2)
    _require_within(input_path, PROJECT_ROOT, "canonical input")
    _require_within(registry_path, PROJECT_ROOT, "plugin registry")
    config_path = (
        _resolve(PROJECT_ROOT, args.config) if args.config else (SUBSYSTEM_ROOT / DEFAULT_CONFIG).resolve()
    )
    config_payload = _load_object(config_path, "framework config")
    configured_plugins = config_payload.get("plugins")
    if not isinstance(configured_plugins, Mapping):
        raise SupplementalAnnotationRunnerV2Error("framework config plugins must be an object")
    enabled_names = [
        str(name)
        for name, value in configured_plugins.items()
        if isinstance(value, Mapping) and value.get("enabled") is True
    ]
    registry_payload = _load_object(registry_path, "plugin registry")
    declarations = _validate_registry_source_access(registry_payload, enabled_names)
    required_names = sorted((name for name in enabled_names if declarations[name] == "required"))
    access: Optional[Dict[str, Any]] = None
    access_path: Optional[Path] = None
    if args.source_access_manifest:
        access_path = _resolve(PROJECT_ROOT, args.source_access_manifest)
        _require_within(access_path, PROJECT_ROOT, "source access manifest")
        access = _validate_access_manifest(access_path, input_path)
    if required_names and access is None:
        raise SupplementalAnnotationRunnerV2Error(
            f"source access is required by enabled plugins: {required_names}"
        )
    original_invoke = _invoke_plugin

    def invoke_with_source_access(
        loaded: Any, batch: Any, run_context: Mapping[str, Any], plugin_config: Any
    ) -> Dict[str, Any]:
        context = dict(run_context)
        declaration = declarations.get(loaded.declaration.name)
        context["source_access"] = {
            "requirement": declaration,
            "manifest": str(access_path) if access_path is not None else None,
            "access_id": access.get("access_id") if access is not None else None,
            "status": access.get("status") if access is not None else "not_provided",
        }
        return original_invoke(loaded, batch, context, plugin_config)

    result = _execute_core(args, invoke_with_source_access)
    result["framework"] = SCRIPT_NAME
    result["framework_version"] = SCRIPT_VERSION
    result["source_access_contract_version"] = SOURCE_ACCESS_SCHEMA_VERSION
    result["source_access"] = {
        "provided": access is not None,
        "manifest": _relative_display(access_path, PROJECT_ROOT) if access_path is not None else None,
        "access_id": access.get("access_id") if access is not None else None,
        "status": access.get("status") if access is not None else "not_provided",
        "required_plugins": required_names,
        "declarations": declarations,
    }
    if not args.dry_run:
        _write_json_atomic(_resolve(PROJECT_ROOT, args.manifest), result)
    return result


def framework_summary() -> Dict[str, Any]:
    return {
        "framework": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "source_access_contract": SOURCE_ACCESS_SCHEMA_VERSION,
        "source_access_declaration_required": True,
    }


# ============================================================
# CLI / main 接口区
# ============================================================


def _build_parser() -> argparse.ArgumentParser:
    parser = _build_parser_core()
    parser.description = (
        "Run Supplemental Annotation plugins with explicit parsed-source access declarations."
    )
    parser.set_defaults(registry=str(DEFAULT_REGISTRY_V2))
    parser.add_argument("--source-access-manifest", default="")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = execute(args)
        print(_safe_json(result))
        return 0
    except (
        SupplementalAnnotationRunnerV2Error,
        SupplementalAnnotationError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        print(
            _safe_json(
                {
                    "status": "error",
                    "framework": SCRIPT_NAME,
                    "version": SCRIPT_VERSION,
                    "error_type": type(exc).__name__,
                    "detail": str(exc),
                }
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        print(
            _safe_json(
                {
                    "status": "error",
                    "framework": SCRIPT_NAME,
                    "version": SCRIPT_VERSION,
                    "error_type": type(exc).__name__,
                    "detail": str(exc),
                }
            ),
            file=sys.stderr,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
