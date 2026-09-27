# ============================================================
# 文件名: action_graph_chain_bridge_v0001.py
# 中文名: Action 总链图补偿协调器
# 版本号: v0001
#
# 主层级: action
# 层级: derivation / coordination
# 脚本定位: 协调本地事务屏障与图批次的最终决定
#
# 职责说明:
# - 持久化提交决定，使跨进程中断后可继续提交或补偿
#
# 本脚本做什么:
# - 冻结配置指纹，提交决定前允许补偿，决定后只允许完成提交
#
# 本脚本不做什么:
# - 不冒充分布式 ACID，不绕过本地数据库备份与恢复
#
# 制度边界声明:
# - 恢复须显式确认原进程已停止
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: action_graph_chain_bridge_v0001
# family: action_graph_chain_bridge
# role: graph_local_store_coordinator
# version: v0001
# status: active
# entry_point: scripts/action/derivation/action_graph_chain_bridge_v0001.py
# input:
#   - local transaction barrier and explicit graph configuration
# output:
#   - durable commit or compensation decision
# depends_on:
#   - action_graph_transaction_v0001
#   - sync_sql_to_graph_v0001
# used_by:
#   - data_action_chain_pipeline_v0019
# ============================================================

from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timezone
import json
from pathlib import Path

from action_graph_transaction_v0001 import GraphClient, GraphError, load_config
from action_sql_graph_projection_v0001 import digest, select_existing_assets
from sync_sql_to_graph_v0001 import atomic_json, execute


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "action_graph_chain_bridge"
SCRIPT_NAME = "action_graph_chain_bridge_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心业务组件
# ============================================================

class GraphChainCoordinator:
    def __init__(self, project_root, config_path, batch_id, run_dir):
        self.root = Path(project_root).resolve()
        self.config_path = Path(config_path).resolve()
        self.batch_id = batch_id
        self.run_dir = Path(run_dir).resolve()
        if not self.run_dir.is_relative_to(self.root):
            raise GraphError("coordinator_outside_project")
        self.path = self.run_dir / "graph_coordinator.json"
        self.graph_journal = self.run_dir / "graph_batch.json"
        self.config = load_config(self.config_path)
        self.record = {"schema_version": "action_graph_coordinator_v0001", "decision": "UNDECIDED",
                       "batch_id": batch_id, "config_path": str(self.config_path),
                       "config_fingerprint": digest(self.config)}
        if self.path.exists():
            old = json.loads(self.path.read_text(encoding=DEFAULT_ENCODING))
            if any(old.get(k) != self.record[k] for k in ("schema_version", "batch_id", "config_path", "config_fingerprint")):
                raise GraphError("coordinator_binding_changed")
            self.record = old

    def prepare(self):
        if self.path.exists():
            raise GraphError("coordinator_already_exists_use_recovery")
        GraphClient(self.config).execute("preflight", batch_id=self.batch_id)
        atomic_json(self.path, self.record)

    def finish_graph(self, decision):
        if not self.graph_journal.exists():
            return {"status": "not_applied"}
        return execute(Namespace(operation=decision, config=self.config_path, project_root=self.root,
                                 batch_id=self.batch_id, journal=self.graph_journal, dry_run=False,
                                 confirm_write=True))

    def bind_publications(self, completion_path, return_path):
        paths = {"completion": str(Path(completion_path).resolve()), "return": str(Path(return_path).resolve()),
                 "prepared_return": str(self.run_dir / "prepared_return_manifest.json")}
        if any(not Path(p).is_relative_to(self.root) for p in paths.values()):
            raise GraphError("publication_outside_project")
        self.record["publications"] = paths
        atomic_json(self.path, self.record)

    def publish(self):
        paths = self.record.get("publications")
        if not paths:
            return
        if any(not Path(p).resolve().is_relative_to(self.root) for p in paths.values()):
            raise GraphError("publication_outside_project")
        prepared = Path(paths["prepared_return"])
        value = json.loads(prepared.read_text(encoding=DEFAULT_ENCODING))
        value["status"] = "ready_for_data_discovery"
        atomic_json(Path(paths["return"]), value)
        completion = Path(paths["completion"])
        value = json.loads(completion.read_text(encoding=DEFAULT_ENCODING))
        value["status"] = "completed"
        atomic_json(completion, value)

    def apply_existing(self, database, writer_config, semantic_input):
        selection = select_existing_assets(database, writer_config, semantic_input,
                                           self.run_dir / "existing_graph_selection", self.root, self.batch_id)
        return execute(Namespace(operation="apply", config=self.config_path, project_root=self.root,
                                 batch_id=self.batch_id, journal=self.graph_journal, dry_run=False,
                                 confirm_write=True, anchor_manifest=selection, max_objects=100000))

    def commit(self, barrier):
        if self.record["decision"] not in {"UNDECIDED", "COMMIT_DECIDED", "COMMITTED"}:
            raise GraphError("coordinator_cannot_commit")
        self.record["decision"] = "COMMIT_DECIDED"
        atomic_json(self.path, self.record)
        self.record["graph"] = self.finish_graph("commit")
        if barrier.state != "COMMITTED":
            barrier.commit()
        self.publish()
        self.record["decision"] = "COMMITTED"
        atomic_json(self.path, self.record)

    def rollback(self, barrier, cause):
        if self.record["decision"] in {"COMMIT_DECIDED", "COMMITTED"}:
            return {"status": "commit_recovery_required", "coordinator": str(self.path)}
        try:
            self.record["graph"] = self.finish_graph("compensate")
        except Exception as exc:
            self.record["recovery_error_type"] = type(exc).__name__
            atomic_json(self.path, self.record)
            # Preserve both stores and the local lock until graph outcome is known.
            return {"status": "graph_recovery_required", "coordinator": str(self.path)}
        result = barrier.rollback(cause)
        self.record["decision"] = "COMPENSATED" if result["status"] in {"rolled_back", "not_required"} else "ROLLBACK_REQUIRED"
        atomic_json(self.path, self.record)
        return result


def recover_graph_chain(journal_path, project_root, barrier_class):
    """Called only after the top-level CLI has confirmed interrupted-run recovery."""
    root = Path(project_root).resolve()
    path = Path(journal_path).resolve()
    if not path.is_relative_to(root):
        raise GraphError("recovery_journal_outside_project")
    local = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
    info = json.loads((path.parent / "graph_coordinator.json").read_text(encoding=DEFAULT_ENCODING))
    if local.get("run_id") != info.get("batch_id") or local.get("schema_version") != "data_action_chain_transaction_v0001":
        raise GraphError("recovery_journal_binding_mismatch")
    run_path = path.parent / "run_manifest.json"
    if run_path.exists() and json.loads(run_path.read_text(encoding=DEFAULT_ENCODING)).get("run_id") != local["run_id"]:
        raise GraphError("recovery_run_manifest_binding_mismatch")
    coordinator = GraphChainCoordinator(root, info["config_path"], info["batch_id"], path.parent)
    lock_path = Path(local["lock_path"]).resolve()
    if not lock_path.is_relative_to(root):
        raise GraphError("recovery_lock_outside_project")
    if lock_path.exists():
        lock = json.loads(lock_path.read_text(encoding=DEFAULT_ENCODING))
        if lock.get("run_id") != local["run_id"]:
            raise GraphError("recovery_lock_owned_by_other_run")
    commit = info["decision"] in {"COMMIT_DECIDED", "COMMITTED"}
    if local["state"] not in {"PREPARING", "PENDING", "ROLLING_BACK", "ROLLBACK_FAILED", "COMMITTED", "ROLLED_BACK"}:
        raise GraphError("local_state_not_recoverable")
    if (local["state"] == "COMMITTED" and not commit) or (local["state"] == "ROLLED_BACK" and commit):
        raise GraphError("local_and_graph_decision_conflict")
    for item in local["records"]:
        target = Path(item["target"]).resolve()
        if not target.is_relative_to(root):
            raise GraphError("recovery_target_outside_project")
        if item.get("existed") and local["state"] not in {"COMMITTED", "ROLLED_BACK"}:
            backup = Path(item["backup"]).resolve()
            if not backup.is_relative_to(path.parent) or not backup.exists():
                raise GraphError("recovery_backup_missing_or_outside_run")
    barrier = barrier_class(project_root=root, run_dir=path.parent, lock_root=lock_path.parent, run_id=local["run_id"])
    barrier.records = local["records"]
    barrier.state = local["state"] if local["state"] in {"COMMITTED", "ROLLED_BACK"} else "PENDING"
    if commit:
        coordinator.commit(barrier)
        result = {"status": "commit_recovered"}
    else:
        result = coordinator.rollback(barrier, GraphError("operator_confirmed_recovery"))
        if result["status"] not in {"rolled_back", "not_required"}:
            raise GraphError("recovery_incomplete")
    # Invalidate stale success publication on compensation; commit recovery marks its origin.
    for kind, value in info.get("publications", {}).items():
        if commit and kind == "prepared_return":
            continue
        publication = Path(value).resolve()
        if not publication.is_relative_to(root):
            raise GraphError("recovery_publication_outside_project")
        if publication.exists():
            payload = json.loads(publication.read_text(encoding=DEFAULT_ENCODING))
            payload["status"] = ("completed" if kind == "completion" else "ready_for_data_discovery") if commit else "compensated"
            payload["transaction_recovery"] = result
            atomic_json(publication, payload)
    recovery = {**result, "run_id": local["run_id"], "graph_decision": coordinator.record["decision"]}
    run_path = path.parent / "run_manifest.json"
    if run_path.exists():
        run = json.loads(run_path.read_text(encoding=DEFAULT_ENCODING))
        if run.get("run_id") != local["run_id"]:
            raise GraphError("recovery_run_manifest_binding_mismatch")
        run["transaction_recovery"] = {**recovery, "previous_status": run.get("status"),
                                       "previous_error": run.get("error")}
        run["status"] = "completed" if commit else "failed"
        run["finished_at"] = datetime.now(timezone.utc).isoformat()
        run.setdefault("transaction", {}).update(state=barrier.state, journal=str(path))
        if commit:
            run["error"] = None
            if info.get("publications", {}).get("completion"):
                run["completion_manifest"] = info["publications"]["completion"]
        atomic_json(run_path, run)
    return recovery
