# ============================================================
# 文件名: backend_pipeline_v0002.py
# 中文名: 后端流程总编排
# 版本号: v0002
#
# 主层级: action
# 层级: orchestration / action / backend
# 脚本定位: 组合独立摄取流水线与数据处理流水线的后端入口
#
# 职责说明:
# - 调用摄取编排、数据处理编排及完成回执接口
#
# 本脚本做什么:
# - 传递经校验的交接参数，汇总各阶段真实状态
#
# 本脚本不做什么:
# - 不实现格式解析、质量判断或数据库业务，不提供 UI
#
# 制度边界声明:
# - 摄取产物追加写入配置目录；核心写入沿用显式参数与授权
# - 不自动重试核心写入；失败保留阶段证据，dry-run 不持久化
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: backend_pipeline_v0002
# family: backend_pipeline
# role: backend_orchestrator
# version: v0002
# status: active
# entry_point: scripts/orchestration/action/backend_pipeline_v0002.py
# input:
#   - ingestion config, source or intake run, explicit core CLI arguments JSON
# output:
#   - backend_result_v0001 with separate intake, core and receipt states
# depends_on:
#   - unified_ingestion_pipeline_v0002
#   - data_action_chain_pipeline_v0019
# used_by:
#   - explicit CLI
# ============================================================

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "backend_pipeline"
SCRIPT_NAME = "backend_pipeline_v0002"
SCRIPT_VERSION = "v0002"
PROJECT_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "AGENTS.md").is_file())
ENTRY_ROOT = PROJECT_ROOT / "scripts/orchestration/action"
DEFAULT_CONFIG = PROJECT_ROOT / "config/action/config/unified_ingestion_config_v0001.json"
RESERVED = {"--data-root", "--target", "--dry-run", "--recover-transaction", "--confirm-interrupted-run"}


# ============================================================
# 异常类型
# ============================================================

class BackendError(RuntimeError):
    pass


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class Request:
    config: Path
    source: Path | None = None
    intake_run: str | None = None
    core_args: Path | None = None
    completion: Path | None = None
    dry_run: bool = False


# ============================================================
# 工具函数区
# ============================================================

def load_entry(name):
    path = ENTRY_ROOT / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def read_core_args(path):
    value = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
    if not isinstance(value, list) or not value or any(not isinstance(x, str) for x in value):
        raise BackendError("core_args_must_be_nonempty_string_array")
    core = load_entry("data_action_chain_pipeline_v0019")
    parser = core.build_parser()
    parser.allow_abbrev = False
    options = parser._option_string_actions
    for token in value:
        if token.startswith("-"):
            key = token.split("=", 1)[0]
            if key in RESERVED or key not in options or key in {"-h", "--help"}:
                raise BackendError("core_option_not_allowed:" + key)
    # Parse exact options before intake has any side effects; source is bound later.
    try:
        args = parser.parse_args([*value, "--data-root", str(PROJECT_ROOT), "--target", "placeholder.json"])
    except SystemExit as exc:
        raise BackendError("invalid_core_arguments") from exc
    if not args.run_id or not args.retention_receipt:
        raise BackendError("explicit_core_run_and_retention_receipt_required")
    if not args.confirm_execution or not args.confirm_database_write:
        raise BackendError("explicit_core_execution_and_database_confirmation_required")
    return value


def call_core(arguments):
    # Child owns its step timeouts and transaction recovery; no outer kill/retry.
    process = subprocess.run(
        [sys.executable, "-B", str(ENTRY_ROOT / "data_action_chain_pipeline_v0019.py"), *arguments],
        cwd=PROJECT_ROOT, stdin=subprocess.DEVNULL, capture_output=True,
        text=True, encoding=DEFAULT_ENCODING, errors="replace",
    )
    try:
        result = json.loads(process.stdout.strip() or process.stderr.strip())
        if not isinstance(result, dict):
            raise ValueError("core_result_not_object")
    except ValueError:
        result = {"status": "error", "error_type": "CoreOutputError",
                  "detail": "core_did_not_return_structured_result"}
    return {"exit_code": process.returncode, "result": result}


# ============================================================
# 核心编排
# ============================================================

def execute(request):
    if bool(request.source) == bool(request.intake_run):
        raise BackendError("exactly_one_source_or_intake_run_required")
    if request.completion and (not request.intake_run or request.core_args):
        raise BackendError("completion_requires_existing_intake_without_core_execution")
    arguments = read_core_args(request.core_args) if request.core_args else None
    intake = load_entry("unified_ingestion_pipeline_v0002")
    pipeline = intake.Pipeline(intake.Config.load(request.config, PROJECT_ROOT))
    row = (pipeline.store.find(request.intake_run) if request.intake_run else
           pipeline.process(request.source, dry_run=request.dry_run))
    intake.validate_result(row)
    if request.intake_run:
        pipeline.store.verify(row["source"])
    outcome = {"schema_version": "backend_result_v0001", "status": "prepared",
               "backend_run_id": "backend_" + uuid.uuid4().hex,
               "intake": row, "core": None, "receipt": None}
    if request.dry_run and request.source:
        outcome.update(status="dry_run", core_validation="requires_preserved_intake_run" if arguments else "not_requested")
        return outcome
    try:
        if request.completion:
            outcome["receipt"] = intake.confirm(pipeline.config, row["run_id"], request.completion, request.dry_run)
            intake.validate_result(outcome["receipt"])
            outcome["status"] = "dry_run" if request.dry_run else "completed"
        elif row["processing_status"] == "failed":
            outcome["status"] = "failed"
        elif row["handoff_status"] != "ready_for_handoff":
            outcome["status"] = "not_ready"
        elif arguments is not None:
            handoff = row["downstream_receipt"]["core_request"]
            bound = [*arguments, "--data-root", handoff["data_root"], "--target", handoff["target"]]
            preflight = call_core([*bound, "--dry-run"])
            outcome["core"] = preflight
            if preflight["exit_code"] or preflight["result"].get("status") != "dry-run":
                raise BackendError("core_preflight_failed")
            if request.dry_run:
                outcome["status"] = "dry_run"
                return outcome
            pipeline.store.event(outcome["backend_run_id"], "core_started", intake_run_id=row["run_id"])
            core = call_core(bound)
            outcome["core"] = core
            status = core["result"].get("status")
            if status in {"completed", "completed_with_retention_error"}:
                outcome["receipt"] = intake.confirm(
                    pipeline.config, row["run_id"], Path(core["result"]["completion_manifest"]))
                intake.validate_result(outcome["receipt"])
                outcome["status"] = "completed" if status == "completed" and core["exit_code"] == 0 else "partial"
            else:
                raise BackendError("core_execution_failed_no_automatic_retry")
        elif request.dry_run:
            outcome["status"] = "dry_run"
    except Exception as exc:
        outcome.update(status="failed", error_type=type(exc).__name__, detail=str(exc))
    if not request.dry_run:
        try:
            pipeline.store.event(outcome["backend_run_id"], "backend_result", result=outcome)
        except Exception as exc:
            outcome.update(status="failed", audit_error=type(exc).__name__)
    return outcome


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser():
    parser = argparse.ArgumentParser(description="Backend coordinator: intake -> core -> verified receipt.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input", type=Path)
    group.add_argument("--intake-run")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--core-args", type=Path, help="JSON array of explicit v0018 CLI options; source bound by backend")
    mode.add_argument("--completion", type=Path, help="Verify an existing completion without rerunning core")
    return parser


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding=DEFAULT_ENCODING)
    args = _build_parser().parse_args()
    try:
        result = execute(Request(args.config, args.input, args.intake_run, args.core_args, args.completion, args.dry_run))
        print(json.dumps(result, ensure_ascii=False))
        return 2 if result["status"] in {"failed", "partial"} else 0
    except (BackendError, OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
