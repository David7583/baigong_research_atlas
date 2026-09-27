# ============================================================
# 文件名: intermediate_retention_pipeline_v0002.py
# 中文名: 运行后中间产物处置编排
# 版本号: v0002
#
# 主层级: action
# 层级: orchestration / retention
# 脚本定位: 顺序编排落库核验、文件清单和处置执行
#
# 职责说明:
# - 用独立业务脚本和中间凭证执行已确认的处置策略
# 本脚本做什么:
# - 与总编排共享互斥锁，保留阶段证据与独立清理状态
# 本脚本不做什么:
# - 不实现业务落库，不将清理失败误标为入库回滚
# 制度边界声明:
# - 无当前运行确认不得执行；失败保留凭证和未处理文件
# 可更新: True
# ============================================================
# ALIAS_META
# alias: intermediate_retention_pipeline_v0002
# family: intermediate_retention_pipeline
# role: verified_retention_orchestrator
# version: v0002
# status: active
# entry_point: scripts/orchestration/action/intermediate_retention_pipeline_v0002.py
# input:
#   - per-run binding, confirmation receipt and committed completion
# output:
#   - verification gate, exact plan and disposition journal
# depends_on:
#   - verify_run_persistence_v0001
#   - plan_intermediate_retention_v0002
#   - apply_intermediate_retention_v0002
# used_by:
#   - data_action_chain_pipeline_v0018
# ============================================================
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "action/infrastructures"))
from intermediate_retention_contract_v0001 import RetentionError, cli_result, load, no_links, now, verify, write

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "intermediate_retention_pipeline"
SCRIPT_NAME = "intermediate_retention_pipeline_v0002"
SCRIPT_VERSION = "v0002"


# ============================================================
# 核心编排区
# ============================================================
def execute(binding_path: Path, receipt_path: Path, completion: Path, dry_run: bool = False) -> dict:
    binding_path, receipt_path, completion = (no_links(path) for path in (binding_path, receipt_path, completion))
    binding = verify(load(binding_path), "binding")
    receipt = verify(load(receipt_path), "receipt")
    if receipt.get("binding_sha256") != binding["contract_sha256"] or not receipt.get("confirmed"):
        raise RetentionError("Missing current-run confirmation")
    root = Path(binding["project_root"])
    evidence = Path(binding["run_dir"]) / "retention"
    infra = Path(__file__).resolve().parents[2] / "action/infrastructures"
    lock = no_links(Path(binding["paths"]["output_root"]) / ".data_action_chain_transaction.lock")
    if dry_run:
        from verify_run_persistence_v0001 import verify_persistence
        from plan_intermediate_retention_v0002 import plan
        gate = verify_persistence(binding, completion)
        inventory = plan(binding, receipt, gate)
        return {"status": "dry_run", "mode": receipt["mode"], "bytes": inventory["total_bytes"], "files": len(inventory["files"])}
    descriptor = None
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(descriptor, json.dumps({"kind": "retention", "pid": os.getpid(), "run_id": binding["run_id"]}).encode())
        os.close(descriptor)
        descriptor = None
    except FileExistsError as exc:
        raise RetentionError("Another ingestion/retention operation holds the store lock") from exc
    except OSError:
        if descriptor is not None:
            try:
                os.close(descriptor)
            finally:
                lock.unlink(missing_ok=True)
        raise
    steps = []
    try:
        evidence.mkdir(parents=True, exist_ok=True)
        if (evidence / "steps.json").is_file():
            steps = list(load(evidence / "steps.json")["steps"])
        gate = evidence / "persistence_gate.json"
        inventory = evidence / "disposition_plan.json"
        journal = evidence / "disposition_journal.json"
        if journal.is_file():
            work = [("apply_intermediate_retention_v0002.py", ["--binding", binding_path, "--receipt", receipt_path, "--gate", gate, "--plan", inventory, "--journal", journal])]
        else:
            work = [
                ("verify_run_persistence_v0001.py", ["--binding", binding_path, "--completion", completion, "--output", gate]),
                ("plan_intermediate_retention_v0002.py", ["--binding", binding_path, "--receipt", receipt_path, "--gate", gate, "--output", inventory]),
                ("apply_intermediate_retention_v0002.py", ["--binding", binding_path, "--receipt", receipt_path, "--gate", gate, "--plan", inventory, "--journal", journal]),
            ]
        for name, arguments in work:
            command = [sys.executable, "-B", str(infra / name), *map(str, arguments)]
            process = subprocess.run(command, cwd=root, capture_output=True, text=True, encoding=DEFAULT_ENCODING, timeout=7200)
            label = f"{len(steps) + 1:03d}_{name}"
            stdout_path, stderr_path = evidence / (label + ".stdout.txt"), evidence / (label + ".stderr.txt")
            stdout_path.write_text(process.stdout, encoding=DEFAULT_ENCODING)
            stderr_path.write_text(process.stderr, encoding=DEFAULT_ENCODING)
            steps.append({"script": name, "command": command, "returncode": process.returncode, "at": now(), "stdout": str(stdout_path), "stderr": str(stderr_path)})
            write(evidence / "steps.json", {"steps": steps})
            if process.returncode:
                raise RetentionError(f"Retention step failed: {name}; see {evidence}")
        result = load(journal)
        summary = {"status": result["status"], "mode": receipt["mode"], "completed_at": now(), "removed_bytes": result.get("removed_bytes", 0), "removed_files": result.get("removed_files", 0), "journal": str(journal), "plan": str(inventory), "archive": result.get("archive")}
        write(evidence / "completion_manifest.json", summary)
        return summary
    except Exception as exc:
        write(evidence / "failure.json", {"status": "retention_failed", "ingestion_status": "completed" if (evidence / "persistence_gate.json").is_file() else "unverified", "error_type": type(exc).__name__, "detail": str(exc), "at": now()})
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)
        lock.unlink(missing_ok=True)


# ============================================================
# CLI / main 接口区
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binding", required=True)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--completion", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return cli_result(lambda: execute(Path(args.binding), Path(args.receipt), Path(args.completion), args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
