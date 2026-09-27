# ============================================================
# 文件名: apply_intermediate_retention_v0002.py
# 中文名: 已核验中间产物处置执行
# 版本号: v0002
#
# 主层级: action
# 层级: infrastructures / retention
# 脚本定位: 将明确文件清单按用户策略处置并保留逐文件日志
#
# 职责说明:
# - 执行删除、压缩后删除、转移后删除或保留
# 本脚本做什么:
# - 先完整验证清单与归档内容，再逐文件处置并支持中断恢复
# 本脚本不做什么:
# - 不扫描其他运行，不递归删除目录，不清理原始来源或持久化库
# 制度边界声明:
# - 任何修改、链接、归档错误或确认不匹配均拒绝删除
# - 删除不可逆；压缩和转移保留经 SHA-256 校验的恢复副本
# 可更新: True
# ============================================================
# ALIAS_META
# alias: apply_intermediate_retention_v0002
# family: apply_intermediate_retention
# role: verified_payload_disposition
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/apply_intermediate_retention_v0002.py
# input:
#   - binding, receipt, gate and exact disposition plan
# output:
#   - durable per-file journal and optional verified archive
# depends_on:
#   - intermediate_retention_contract_v0001
#   - plan_intermediate_retention_v0002
# used_by:
#   - intermediate_retention_pipeline_v0002
# ============================================================
from __future__ import annotations

import argparse
import hashlib
import shutil
import uuid
import zipfile
from pathlib import Path

from intermediate_retention_contract_v0001 import RetentionError, check_file, cli_result, eligible, load, no_links, now, seal, sha, verify, write
from plan_intermediate_retention_v0002 import allowed_roots

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "apply_intermediate_retention"
SCRIPT_NAME = "apply_intermediate_retention_v0002"
SCRIPT_VERSION = "v0002"


# ============================================================
# 工具函数区
# ============================================================
def verify_archive(plan: dict, target: Path, mode: str) -> None:
    if mode == "compress":
        no_links(target)
        with zipfile.ZipFile(target) as archive:
            if set(archive.namelist()) != {r["relative_path"] for r in plan["files"]}:
                raise RetentionError("Archive inventory mismatch")
            for row in plan["files"]:
                with archive.open(row["relative_path"]) as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != row["sha256"]:
                        raise RetentionError("Archive payload hash mismatch")
    else:
        if load(target / "transfer_manifest.json") != plan:
            raise RetentionError("Transfer provenance manifest mismatch")
        for index, row in enumerate(plan["files"]):
            target_file = no_links(target / (f"{index:06d}_" + row["sha256"][:16] + ".jsonl"))
            if not target_file.is_relative_to(target.resolve()) or sha(target_file) != row["sha256"]:
                raise RetentionError("Transferred payload hash mismatch")


def archive_payloads(plan: dict, root: Path, target: Path, mode: str) -> None:
    no_links(target)
    if target.exists():
        verify_archive(plan, target, mode)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(target.parent).free < plan["total_bytes"] + 16 * 1024 * 1024:
        raise RetentionError("Insufficient destination free space; source files retained")
    if mode == "compress":
        temporary = target.with_name(target.name + "." + uuid.uuid4().hex[:8] + ".partial")
        with zipfile.ZipFile(temporary, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as archive:
            for row in plan["files"]:
                archive.write(check_file(row, root), row["relative_path"])
        verify_archive(plan, temporary, mode)
        temporary.rename(target)
    else:
        temporary = target.with_name(target.name + "." + uuid.uuid4().hex[:8] + ".partial")
        temporary.mkdir()
        for index, row in enumerate(plan["files"]):
            source = check_file(row, root)
            destination = no_links(temporary / (f"{index:06d}_" + row["sha256"][:16] + ".jsonl"))
            if not destination.is_relative_to(temporary.resolve()):
                raise RetentionError("Archive path traversal")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        write(temporary / "transfer_manifest.json", plan)
        verify_archive(plan, temporary, mode)
        temporary.rename(target)


# ============================================================
# 核心业务区
# ============================================================
def apply(binding: dict, receipt: dict, gate: dict, plan: dict, journal_path: Path, dry_run: bool = False) -> dict:
    for value, kind in ((binding, "binding"), (receipt, "receipt"), (gate, "gate"), (plan, "plan")):
        verify(value, kind)
    if not receipt.get("confirmed") or gate.get("status") != "passed" or receipt.get("mode") != plan.get("mode") or plan.get("mode") not in {"delete", "compress", "transfer", "keep"}:
        raise RetentionError("Missing confirmation or invalid disposition")
    if any(x.get("binding_sha256") != binding["contract_sha256"] for x in (receipt, gate, plan)) or plan["receipt_sha256"] != receipt["contract_sha256"] or plan["gate_sha256"] != gate["contract_sha256"]:
        raise RetentionError("Evidence is bound to a different run")
    root = no_links(Path(binding["project_root"]))
    scopes = [no_links(p) for p in allowed_roots(binding)]
    seen = set()
    protected_paths = {Path(path).resolve() for path in gate["protected_files"]}
    for row in plan["files"]:
        file = eligible(Path(row["path"]), root)
        if file in protected_paths or not any(file.is_relative_to(p) for p in scopes) or file in seen:
            raise RetentionError("File is outside the exact authorized run scopes")
        if row["relative_path"] != file.relative_to(root).as_posix():
            raise RetentionError("Archive relative path mismatch")
        seen.add(file)
    journal_path = no_links(journal_path)
    if not journal_path.is_relative_to(Path(binding["run_dir"]).resolve()) or journal_path in seen:
        raise RetentionError("Journal must be retained in this run")
    journal = load(journal_path) if journal_path.exists() else {"kind": "disposition_journal", "plan_sha256": plan["contract_sha256"], "status": "planned", "files": {}, "started_at": now()}
    if journal.get("plan_sha256") != plan["contract_sha256"]:
        raise RetentionError("Journal belongs to another plan")
    if journal.get("status") == "completed":
        return {**journal, "idempotent": True}
    for path, expected in gate["protected_files"].items():
        if sha(Path(path)) != expected:
            raise RetentionError(f"Persistence/provenance changed since verification: {path}")
    for row in plan["files"]:
        state = journal["files"].get(row["path"])
        if state == "deleted" or (state == "deleting" and not Path(row["path"]).exists()):
            continue
        check_file(row, root)
    if dry_run:
        return {"status": "dry_run", "files": len(plan["files"]), "bytes": plan["total_bytes"], "mode": plan["mode"]}
    mode = plan["mode"]
    journal["mode"] = mode
    write(journal_path, journal)
    try:
        if mode in {"compress", "transfer"}:
            if receipt["destination"] != plan["destination"]:
                raise RetentionError("Destination changed after confirmation")
            destination = no_links(Path(plan["destination"]))
            if any(destination == scope or destination.is_relative_to(scope) for scope in scopes):
                raise RetentionError("Destination overlaps source payload tree")
            name = binding["run_id"][:16] + "_" + plan["contract_sha256"][:12]
            target = destination / (name + ".zip" if mode == "compress" else name)
            archive_payloads(plan, root, target, mode)
            journal["archive"] = {"path": str(target), "verified": True, "mode": mode}
            write(journal_path, journal)
        if mode != "keep":
            for row in plan["files"]:
                if journal["files"].get(row["path"]) == "deleted":
                    continue
                if journal["files"].get(row["path"]) == "deleting" and not Path(row["path"]).exists():
                    journal["files"][row["path"]] = "deleted"
                    write(journal_path, journal)
                    continue
                file = check_file(row, root)
                journal["files"][row["path"]] = "deleting"
                write(journal_path, journal)
                file.unlink()
                journal["files"][row["path"]] = "deleted"
                write(journal_path, journal)
        journal.update(status="completed", completed_at=now(), removed_bytes=plan["total_bytes"] if mode != "keep" else 0, removed_files=len(plan["files"]) if mode != "keep" else 0)
        write(journal_path, journal)
        return journal
    except Exception as exc:
        journal.update(status="failed", error_type=type(exc).__name__, detail=str(exc))
        write(journal_path, journal)
        raise


# ============================================================
# CLI / main 接口区
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser()
    for name in ("binding", "receipt", "gate", "plan", "journal"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return cli_result(lambda: apply(*(load(Path(getattr(args, name))) for name in ("binding", "receipt", "gate", "plan")), Path(args.journal), args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
