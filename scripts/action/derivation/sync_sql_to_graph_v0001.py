# ============================================================
# 文件名: sync_sql_to_graph_v0001.py
# 中文名: Action 业务 SQLite 图同步入口
# 版本号: v0001
#
# 主层级: action
# 层级: derivation / sync
# 脚本定位: 图投影、补偿与恢复的结构化 CLI
#
# 职责说明:
# - 将本批业务库事实交给图批次事务，不直接消费旧结构图 JSON
#
# 本脚本做什么:
# - 绑定数据库目标与批次日志，执行 preflight/apply/commit/compensate
#
# 本脚本不做什么:
# - 不自动建库建约束，不修改源库，不自动提交上层总链
#
# 制度边界声明:
# - apply 后需上层显式 commit 或 compensate
# - 恢复复用原配置指纹；不保存密码，不打印驱动原始异常
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: sync_sql_to_graph_v0001
# family: sync_sql_to_graph
# role: sql_graph_sync_entry
# version: v0001
# status: active
# entry_point: scripts/action/derivation/sync_sql_to_graph_v0001.py
# input:
#   - completed Action anchor manifest
#   - graph connection contract and transaction journal
# output:
#   - graph batch state and source verification
# depends_on:
#   - action_sql_graph_projection_v0001
#   - action_graph_transaction_v0001
# used_by:
#   - action_derivation_materialization_pipeline_v0008
#   - data_action_chain_pipeline_v0019
# ============================================================

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from action_graph_transaction_v0001 import GraphClient, GraphError, load_config
from action_sql_graph_projection_v0001 import build_projection, canonical, digest, verify_source


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "sync_sql_to_graph"
SCRIPT_NAME = "sync_sql_to_graph_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding=DEFAULT_ENCODING, newline="\n") as handle:
        handle.write(canonical(value) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


# ============================================================
# 核心业务组件
# ============================================================

def execute(args):
    config = load_config(args.config)
    client = GraphClient(config)
    binding = {"config_fingerprint": digest(config), "batch_id": args.batch_id,
               "source_id": config["source_id"], "database": config["database"]}
    if args.operation == "preflight":
        if args.dry_run:
            return {"status": "dry-run", "connection_checked": False, **binding}
        return client.execute("preflight", batch_id=args.batch_id)
    if args.operation == "verify":
        result = client.execute("verify", batch_id=args.batch_id)
        projection = build_projection(Path(result["source"]["anchor_manifest"]), args.project_root)
        if projection["projection_hash"] != result["projection_hash"] or projection["source"] != result["source"]:
            raise GraphError("graph_sql_evidence_roundtrip_mismatch")
        return {"status": "verified", "batch_state": result["batch_state"],
                "objects": len(projection["nodes"]), "links": len(projection["links"]), **binding}
    if args.operation == "apply":
        if not args.anchor_manifest:
            raise GraphError("anchor_manifest_required")
        projection = build_projection(args.anchor_manifest, args.project_root, max_objects=args.max_objects)
        if args.dry_run:
            return {"status": "dry-run", "source": projection["source"],
                    "objects": len(projection["nodes"]), "links": len(projection["links"]),
                    "projection_hash": projection["projection_hash"], "connection_checked": False, **binding}
    elif args.dry_run:
        raise GraphError("recovery_dry_run_use_preflight_instead")
    if not args.confirm_write or args.journal is None:
        raise GraphError("explicit_write_confirmation_and_journal_required")
    if args.journal.exists():
        journal = json.loads(args.journal.read_text(encoding=DEFAULT_ENCODING))
        if journal["binding"] != binding:
            raise GraphError("graph_journal_target_or_batch_changed")
    elif args.operation == "apply":
        journal = {"schema_version": "action_graph_local_journal_v0001", "binding": binding,
                   "state": "PREPARED", "projection_hash": projection["projection_hash"]}
        atomic_json(args.journal, journal)
    else:
        raise GraphError("graph_recovery_journal_missing")
    if args.operation == "apply":
        if journal["projection_hash"] != projection["projection_hash"]:
            raise GraphError("graph_projection_changed_on_retry")
        journal["state"] = "APPLY_UNCERTAIN"
        atomic_json(args.journal, journal)
        result = client.execute("apply", batch_id=args.batch_id, projection=projection)
        # A source mismatch fails the branch; coordinator compensates the durable graph batch.
        result["source_verification"] = verify_source(projection, args.project_root)
    else:
        result = client.execute(args.operation, batch_id=args.batch_id)
    journal["state"] = result["status"].upper()
    journal["result"] = result
    atomic_json(args.journal, journal)
    return {**result, "journal": str(args.journal), "binding": binding}


# ============================================================
# CLI / main 接口区
# ============================================================

def build_parser():
    parser = argparse.ArgumentParser(description="Action SQLite graph projection and batch recovery")
    parser.add_argument("operation", choices=("preflight", "apply", "commit", "compensate", "verify"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--anchor-manifest", type=Path)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--max-objects", type=int, default=100000)
    parser.add_argument("--confirm-write", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main():
    try:
        result = execute(build_parser().parse_args())
        print(canonical(result))
        return 0
    except Exception as exc:
        # Unknown exception text may contain source content or driver authentication detail.
        detail = str(exc) if isinstance(exc, GraphError) else "graph_sync_failed"
        print(canonical({"status": "error", "error_type": type(exc).__name__, "detail": detail}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
