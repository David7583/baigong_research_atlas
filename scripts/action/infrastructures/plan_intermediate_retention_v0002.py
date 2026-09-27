# ============================================================
# 文件名: plan_intermediate_retention_v0002.py
# 中文名: 中间产物处置清单生成
# 版本号: v0002
#
# 主层级: action
# 层级: infrastructures / retention
# 脚本定位: 按本次运行白名单枚举可重建载荷
#
# 职责说明:
# - 将通过落库核验的运行映射为逐文件处置清单
# 本脚本做什么:
# - 保留来源映射、结构图、报告和持久化状态，记录待处置 JSONL 哈希
# 本脚本不做什么:
# - 不递归删除目录，不按整个 temp 文件夹清理
# 制度边界声明:
# - 清单只涵盖本次运行已可重建的文本和向量载荷
# 可更新: True
# ============================================================
# ALIAS_META
# alias: plan_intermediate_retention_v0002
# family: plan_intermediate_retention
# role: retention_inventory
# version: v0002
# status: active
# entry_point: scripts/action/infrastructures/plan_intermediate_retention_v0002.py
# input:
#   - run binding, confirmation receipt and persistence gate
# output:
#   - exact file inventory with SHA-256 and byte counts
# depends_on:
#   - intermediate_retention_contract_v0001
# used_by:
#   - intermediate_retention_pipeline_v0002
# ============================================================
from __future__ import annotations

import argparse
from pathlib import Path

from intermediate_retention_contract_v0001 import RetentionError, cli_result, eligible, load, now, seal, sha, verify, within, write

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "plan_intermediate_retention"
SCRIPT_NAME = "plan_intermediate_retention_v0002"
SCRIPT_VERSION = "v0002"


# ============================================================
# 核心业务区
# ============================================================
def allowed_roots(binding: dict) -> list[Path]:
    run = Path(binding["run_dir"])
    prefix = binding["run_id"][:48]
    roots = [run / name for name in ("semantic_source", "content_delta_filter", "source_timestamp_annotation", "source_identity_annotation")]
    anchor_version = binding.get("anchor_pipeline_version", "v0005")
    if anchor_version not in {"v0005", "v0006", "v0007"}:
        raise RetentionError("Unsupported anchor pipeline scope")
    roots.append(Path(binding["paths"]["anchor_business_root"]) / "action_anchor_persistence_pipeline" / anchor_version / (prefix + "_p"))
    for suffix in ("_vc", "_vi"):
        roots.extend(Path(binding["paths"]["vector_pipeline_root"]) / stage / (prefix + suffix) for stage in ("1_generated", "2_validated", "3_lifecycle_checked", "4_written"))
    return roots


def plan(binding: dict, receipt: dict, gate: dict) -> dict:
    verify(binding, "binding")
    verify(receipt, "receipt")
    verify(gate, "gate")
    if not receipt.get("confirmed") or gate.get("status") != "passed" or any(x.get("binding_sha256") != binding["contract_sha256"] for x in (receipt, gate)):
        raise RetentionError("Confirmation or persistence evidence belongs to another run")
    root = Path(binding["project_root"])
    rows = []
    seen = set()
    protected_paths = {Path(path).resolve() for path in gate["protected_files"]}
    directories = allowed_roots(binding)
    for folder in directories:
        within(folder, root)
        if not folder.exists():
            continue
        for file in sorted(folder.rglob("*.jsonl")):
            file = eligible(file, root)
            if file in protected_paths or file in seen:
                raise RetentionError("Protected or overlapping inventory")
            seen.add(file)
            rows.append({"path": str(file), "relative_path": file.relative_to(root).as_posix(), "size": file.stat().st_size, "sha256": sha(file), "category": "reproducible_run_payload"})
    return seal({"kind": "plan", "run_id": binding["run_id"], "binding_sha256": binding["contract_sha256"], "receipt_sha256": receipt["contract_sha256"], "gate_sha256": gate["contract_sha256"], "mode": receipt["mode"], "destination": receipt["destination"], "created_at": now(), "allowed_roots": [str(p) for p in directories], "files": rows, "total_bytes": sum(row["size"] for row in rows), "retained": ["original sources", "canonical sources and source-to-canonical mappings", "structural facts and graphs", "databases and active vector indices", "completion, lineage, reports and logs"]})


# ============================================================
# CLI / main 接口区
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser()
    for name in ("binding", "receipt", "gate", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    def run():
        result = plan(load(Path(args.binding)), load(Path(args.receipt)), load(Path(args.gate)))
        if not args.dry_run:
            write(Path(args.output), result)
        return {"status": "dry_run" if args.dry_run else "planned", "files": len(result["files"]), "bytes": result["total_bytes"]}
    return cli_result(run)


if __name__ == "__main__":
    raise SystemExit(main())
