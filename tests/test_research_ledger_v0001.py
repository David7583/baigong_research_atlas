# ============================================================
# 文件名: test_research_ledger_v0001.py
# 中文名: 研究交接与隔离收件机制测试
# 版本号: v0001
#
# 主层级: action
# 层级: tests / research_assistant
# 脚本定位: 合成资料上的版本、往返切换和失败路径测试
#
# 职责说明:
# - 检查同项目跨执行器接续及原件接入边界
#
# 本脚本做什么:
# - 在 test-output 中创建全新合成包和隔离 SQLite
#
# 本脚本不做什么:
# - 不读取真实业务资料，不调用模型，不宣称真实客户端验收
#
# 制度边界声明:
# - 测试目录每次唯一；保留失败证据，不修改主项目数据
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: test_research_ledger_v0001
# family: test_research_ledger
# role: synthetic_continuity_test
# version: v0001
# status: experimental
# entry_point: tests/test_research_ledger_v0001.py
# input:
#   - synthetic evidence authored inside this test
# output:
#   - unittest results and isolated test-output artifacts
# depends_on:
#   - research_ledger_v0001
#   - research_assistant_v0001
#   - unified_ingestion_pipeline_v0002
# used_by: []
# ============================================================

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "test_research_ledger"
SCRIPT_NAME = "test_research_ledger_v0001"
SCRIPT_VERSION = "v0001"
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts/action/development/scripts/research_assistant"))

from research_ledger_v0001 import Ledger, ResearchError, canonical, digest, now, read_package
from research_assistant_v0001 import ResearchAssistant


# ============================================================
# 工具函数区
# ============================================================

def executor(kind, name):
    return {"kind": kind, "name": name, "version": None, "provider": None, "model": None,
            "access": "local_tools" if kind == "agent" else "manual_exchange"}


# ============================================================
# 核心测试组件
# ============================================================

class LedgerTests(unittest.TestCase):
    def setUp(self):
        output = ROOT / "test-output"
        output.mkdir(exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="ledger_", dir=output))
        self.config = {"schema_version": "1.0", "ledger": "ledger.sqlite3", "inbox": "inbox",
                       "publication_policy": "authority_first_explicit", "source_ids": ["synthetic_research"]}
        self.app = ResearchAssistant(self.root, self.config)
        self.ledger = self.app.ledger
        self.ledger.initialize()
        self.project = self.ledger.create_project("合成：保温材料比较")
        self.task = self.ledger.create_task(self.project["project_id"], "查验样本差异", "区分证据与推断",
                                            ["仅合成资料", "不外发", "反例必须保留"], ["synthetic_research"])

    def package(self, base=0, actor=None, key=None):
        run = self.ledger.begin_run(self.task["task_id"], base, actor or executor("agent", "Synthetic Agent A"))
        name = "package_" + run["run_id"]
        folder = self.root / "inbox" / name
        folder.mkdir(parents=True)
        content = "# 合成阶段报告\n样本 A 在 20°C 时损耗 8%；样本 B 为 12%。\n反例：40°C 条件不同，不能直接外推。\n"
        raw = content.encode(DEFAULT_ENCODING)
        (folder / "report.md").write_bytes(raw)
        payload = {"schema_version": "1.0", "project_id": self.project["project_id"], "task_id": self.task["task_id"],
                   "run_id": run["run_id"], "base_revision": base, "idempotency_key": key or run["run_id"],
                   "task_status": "paused", "run_status": "succeeded",
                   "handoff": {"completed": ["比较20°C样本"], "failed": [], "decisions": ["不同温度不能直接比较"],
                               "open_questions": ["40°C是否相同"], "next_steps": ["核对40°C反例"],
                               "pending_confirmation": [], "unknown_operations": []},
                   "artifacts": [{"artifact_id": "report_1", "path": "report.md", "sha256": digest(raw), "size": len(raw),
                                  "origin": "generated", "generated_at": now()}],
                   "evidence": [{"source_id": "synthetic_research", "object_id": "sample_20c", "source_hash": digest(b"synthetic 20C: A=8 B=12"),
                                 "origin": "synthetic_original", "locator": {"author": "synthetic_user", "event_time": "2026-01-01T00:00:00Z", "char_start": 0, "char_end": 24},
                                 "claim": "20C comparison", "relation": "supports"}],
                   "coverage": [{"source_id": "synthetic_research", "mode": "manual", "query": "20C", "status": "completed", "truncated": False, "limitations": "合成样本，未验证40C"}]}
        self.write_manifest(folder, payload)
        return name, folder, payload

    def write_manifest(self, folder, payload):
        raw = canonical(payload).encode(DEFAULT_ENCODING)
        (folder / "manifest.json").write_bytes(raw)
        (folder / "READY").write_text(digest(raw), encoding=DEFAULT_ENCODING)

    def test_roundtrip_agent_ai_ai_agent(self):
        actors = [executor("agent", "A"), executor("ai", "B"), executor("ai", "C"), executor("agent", "A"), executor("agent", "D"), executor("agent", "A")]
        for revision, actor in enumerate(actors):
            name, _, _ = self.package(revision, actor)
            result = self.app.execute("submit_delivery", {"package": name})
            self.assertEqual(result["revision"], revision + 1)
            resumed = Ledger(self.ledger.path).resume(self.task["task_id"])["task"]
            self.assertEqual(resumed["project_id"], self.project["project_id"])
            self.assertEqual(resumed["executor"], actor)
            self.assertEqual(resumed["constraints"], self.task["constraints"])
            self.assertEqual(resumed["handoff"]["next_steps"], ["核对40°C反例"])
        self.assertEqual(self.ledger.resume(self.task["task_id"], 1)["task"]["executor"]["name"], "A")

    def test_duplicate_is_idempotent(self):
        name, _, _ = self.package()
        self.app.execute("submit_delivery", {"package": name})
        self.assertEqual(self.app.execute("submit_delivery", {"package": name})["status"], "existing")
        self.assertEqual(self.ledger.resume(self.task["task_id"])["current_revision"], 1)

    def test_idempotency_payload_conflict(self):
        name, folder, payload = self.package()
        self.app.execute("submit_delivery", {"package": name})
        payload["handoff"]["next_steps"] = ["不同载荷"]
        self.write_manifest(folder, payload)
        with self.assertRaisesRegex(ResearchError, "IDEMPOTENCY_CONFLICT"):
            self.app.execute("submit_delivery", {"package": name})

    def test_concurrent_publication_conflict_preserves_failed_work(self):
        first, _, _ = self.package()
        second, _, _ = self.package(actor=executor("ai", "B"))
        def submit(name):
            try:
                return self.app.execute("submit_delivery", {"package": name})["status"]
            except ResearchError as exc:
                return str(exc)
        with ThreadPoolExecutor(max_workers=2) as pool:
            states = list(pool.map(submit, [first, second]))
        self.assertCountEqual(states, ["completed", "REVISION_CONFLICT"])
        resumed = self.ledger.resume(self.task["task_id"])
        self.assertEqual(len(resumed["deliveries"]), 2)
        self.assertEqual({r["status"] for r in resumed["runs"]}, {"succeeded"})
        self.assertEqual({d["status"] for d in resumed["deliveries"]}, {"completed", "failed"})

    def test_half_written_rejected(self):
        name, folder, _ = self.package()
        (folder / "READY").write_text("", encoding=DEFAULT_ENCODING)
        with self.assertRaisesRegex(ResearchError, "READY_HASH_MISMATCH"):
            self.app.execute("submit_delivery", {"package": name})
        self.assertEqual(self.ledger.resume(self.task["task_id"])["deliveries"], [])

    def test_artifact_tamper_rejected(self):
        name, folder, _ = self.package()
        (folder / "report.md").write_text("tampered", encoding=DEFAULT_ENCODING)
        with self.assertRaisesRegex(ResearchError, "ARTIFACT_SIZE"):
            self.app.execute("submit_delivery", {"package": name})

    def test_path_escape_rejected(self):
        name, folder, payload = self.package()
        payload["artifacts"][0]["path"] = "../report.md"
        self.write_manifest(folder, payload)
        with self.assertRaisesRegex(ResearchError, "PATH_ESCAPE"):
            self.app.execute("submit_delivery", {"package": name})

    def test_generated_not_independent_evidence(self):
        name, folder, payload = self.package()
        payload["evidence"][0]["origin"] = "generated"
        self.write_manifest(folder, payload)
        with self.assertRaisesRegex(ResearchError, "GENERATED_IS_NOT_INDEPENDENT_SUPPORT"):
            self.app.execute("submit_delivery", {"package": name})

    def test_unauthorized_source_rejected(self):
        name, folder, payload = self.package()
        payload["evidence"][0]["source_id"] = "private_data"
        self.write_manifest(folder, payload)
        with self.assertRaisesRegex(ResearchError, "SOURCE_REVOKED"):
            self.app.execute("submit_delivery", {"package": name})

    def test_dry_run_no_ledger_writes(self):
        name, _, _ = self.package()
        before = self.ledger.path.read_bytes()
        self.assertEqual(self.app.execute("submit_delivery", {"package": name}, True)["status"], "dry_run")
        self.assertEqual(before, self.ledger.path.read_bytes())

    def test_accept_then_restart_archive(self):
        name, _, _ = self.package()
        envelope, artifacts, raw = read_package(self.app.inbox, name)
        accepted = self.ledger.accept(envelope, artifacts, raw)
        self.assertEqual(self.ledger.delivery_status(accepted["delivery_id"])["status"], "accepted")
        other = Ledger(self.ledger.path)
        self.assertEqual(other.archive(accepted["delivery_id"])["status"], "completed")
        self.assertEqual(other.delivery_status(accepted["delivery_id"])["index_status"], "not_requested")

    def test_stopped_run_keeps_questions(self):
        name, folder, payload = self.package()
        payload["run_status"] = "stopped"
        payload["handoff"]["unknown_operations"] = ["外部操作结果未知，禁止自动重试"]
        self.write_manifest(folder, payload)
        self.app.execute("submit_delivery", {"package": name})
        self.assertEqual(self.ledger.resume(self.task["task_id"])["runs"][0]["status"], "stopped")

    def test_find_empty_not_missing_evidence_claim(self):
        self.assertEqual(self.ledger.find_tasks("没有这个标题")["tasks"], [])

    def test_ingestion_markdown_preservation_is_not_core_completion(self):
        from scripts.orchestration.action.unified_ingestion_pipeline_v0002 import Pipeline
        from scripts.action.tools.ingestion.ingestion_store_v0001 import Config
        config = Config(ROOT, ROOT / "temp" / self.root.name, "synthetic-test", 2097152, 12, 30)
        source = self.root / "synthetic_report.md"
        source.write_text("# 合成资料\n此处只记录人造样本，不含真实业务数据。\n", encoding=DEFAULT_ENCODING)
        before = digest(source.read_bytes())
        result = Pipeline(config).process(source)
        self.assertEqual(result["quality_status"], "passed")
        self.assertEqual(result["handoff_status"], "awaiting_core_contract")
        self.assertEqual(result["body_ingestion_status"], "not_ingested")
        self.assertEqual(digest(source.read_bytes()), before)
        self.assertTrue(Path(result["source"]["managed_path"]).resolve().is_relative_to(ROOT))

    def test_generated_report_reaches_canonical_handoff(self):
        from research_ingestion_v0001 import prepare_ingestion
        name, _, _ = self.package()
        self.app.execute("submit_delivery", {"package": name})
        task = self.ledger.resume(self.task["task_id"])["task"]
        prepared = prepare_ingestion(ROOT, task)
        self.assertEqual(prepared["status"], "ready_for_handoff", prepared)
        self.assertFalse(prepared["core_completed"])
        self.assertEqual(prepared["intake"]["downstream_receipt"]["core_request"]["pipeline"], "data_action_chain_pipeline_v0019")
        self.assertEqual(prepared["origin"], "generated")

    def test_main_chain_dependency_preflight(self):
        import yaml
        from scripts.orchestration.action import data_action_chain_pipeline_v0019 as core
        from research_ingestion_v0001 import prepare_ingestion
        name, _, _ = self.package()
        self.app.execute("submit_delivery", {"package": name})
        prepared = prepare_ingestion(ROOT, self.ledger.resume(self.task["task_id"])["task"])
        request = prepared["intake"]["downstream_receipt"]["core_request"]
        base = "temp/c" + uuid.uuid4().hex[:6]
        writer = yaml.safe_load((ROOT / "config/action/config/sql_writer_config_v0001.yml").read_text(encoding="utf-8-sig"))
        writer["connection"]["sqlite"]["path"] = base + "/business.sqlite3"
        writer_path = ROOT / base / "writer.yml"
        writer_path.parent.mkdir(parents=True)
        writer_path.write_text(yaml.safe_dump(writer, allow_unicode=True), encoding=DEFAULT_ENCODING)
        args = core.build_parser().parse_args([
            "--project-root", str(ROOT), "--data-root", request["data_root"], "--target", request["target"],
            "--asset-version", "v0001", "--data-db", base + "/data.sqlite3", "--data-table", "data_text_units",
            "--action-db", base + "/business.sqlite3", "--sql-writer-config", base + "/writer.yml",
            "--duckdb-path", base + "/analysis.duckdb", "--vector-target", "none", "--derivation-target", "duckdb",
            "--run-id", "r" + uuid.uuid4().hex[:4], "--semantic-path", "/conversations/*/nodes/*/message/content/parts/*/text",
            "--data-intermediate-root", base + "/intermediate", "--data-workspace-root", base + "/workspace",
            "--admission-target-root", "data_processed/research_tests/" + self.root.name,
            "--registry-dir", "actioning/registry/research_tests/" + self.root.name,
            "--output-root", base + "/o", "--structural-output-root", "scripts/orchestration/outputs/action",
            "--canonical-work-root", base + "/canonical",
            "--structural-business-root", "actioning/pipelines/research_tests/" + self.root.name,
            "--anchor-business-root", base + "/anchor", "--derivation-business-root", base + "/derivation",
            "--return-root", base + "/returns", "--vector-pipeline-root", base + "/vector",
            "--chromadb-path", base + "/chroma", "--vector-state-index", base + "/vector/index.jsonl",
            "--vector-replaced-archive", base + "/vector/archive",
            "--confirm-execution", "--confirm-database-write", "--dry-run"])
        expected_workspace = args.canonical_work_root
        args.canonical_work_root = "data_raw"
        with self.assertRaisesRegex(core.PipelineError, "canonical work root"):
            core.validate_args_and_plan(args)
        args.canonical_work_root = expected_workspace
        result = core.execute_with_retention(args)
        self.assertEqual(result["status"], "dry-run")
        bootstrap = subprocess.run([sys.executable, "-B", str(ROOT / "scripts/action/tools/init_action_data_sql_schema_v0001.py"),
                                    "--db-path", str(ROOT / base / "business.sqlite3"), "--init"],
                                   cwd=ROOT, capture_output=True, text=True, encoding=DEFAULT_ENCODING, timeout=30)
        self.assertEqual(bootstrap.returncode, 0, bootstrap.stdout + bootstrap.stderr)
        args.dry_run = False
        binding = core.retention_binding(args)
        from intermediate_retention_contract_v0001 import seal, write
        receipt = seal({"kind": "receipt", "binding_sha256": binding["contract_sha256"], "run_id": args.run_id,
                        "mode": "keep", "destination": "", "confirmed_at": now(), "confirmed": True,
                        "authorization": "user_requested_synthetic_isolated_test_no_deletion"})
        receipt_path = ROOT / base / "retention_receipt.json"
        write(receipt_path, receipt)
        args.retention_receipt = str(receipt_path)
        result = core.execute_with_retention(args)
        (ROOT / base / "research_integration_result.json").write_text(canonical(result), encoding=DEFAULT_ENCODING)
        self.assertEqual(result["status"], "completed", result)
        from research_index_v0001 import index_revision
        indexed = index_revision(ROOT, self.ledger, self.task["task_id"])
        self.assertEqual(indexed["status"], "completed")
        self.assertTrue(indexed["index_receipt"]["base"].startswith("runtime/i/"))
        self.assertEqual(index_revision(ROOT, self.ledger, self.task["task_id"])["status"], "existing")
        baseline = sqlite3.connect(ROOT / base / "business.sqlite3")
        candidate = sqlite3.connect(ROOT / indexed["index_receipt"]["base"] / "business.sqlite3")
        try:
            self.assertEqual(baseline.execute("SELECT instance_id,content FROM instance_units ORDER BY instance_id").fetchall(),
                             candidate.execute("SELECT instance_id,content FROM instance_units ORDER BY instance_id").fetchall())
        finally:
            baseline.close()
            candidate.close()
        # A second stage must not reuse the first delivery's asset-version path.
        second, _, _ = self.package(base=1)
        self.app.execute("submit_delivery", {"package": second})
        continued = index_revision(ROOT, self.ledger, self.task["task_id"])
        self.assertEqual(continued["status"], "completed")
        self.assertEqual(continued["index_receipt"]["base"], indexed["index_receipt"]["base"])
        self.assertNotEqual(continued["delivery_id"], indexed["delivery_id"])
        self.assertEqual(index_revision(ROOT, self.ledger, self.task["task_id"], revision=1)["status"], "existing")

    def test_independent_retrieval_evidence_identity_empty_and_write_denial(self):
        from research_retrieval_v0001 import ResearchRetrieval
        from scripts.action.tools.retrieval.retrieval_executor_v0001 import execute_query
        path = self.root / "synthetic.sqlite3"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE evidence(object_id TEXT PRIMARY KEY, text TEXT, origin TEXT, author TEXT, event_time TEXT)")
        conn.executemany("INSERT INTO evidence VALUES(?,?,?,?,?)", [
            ("sample_20c", "synthetic 20C: A=8 B=12", "synthetic_original", "synthetic_user", "2026-01-01T00:00:00Z"),
            ("sample_40c", "synthetic 40C: A=18 B=11", "synthetic_original", "synthetic_user", "2026-02-01T00:00:00Z"),
            ("hypothesis", "20C cannot establish 40C", "generated", "synthetic_assistant", "2026-03-01T00:00:00Z")])
        conn.commit()
        conn.close()
        source = {"backend": "sqlite", "path": "synthetic.sqlite3",
                  "tables": {"evidence": ["object_id", "text", "origin", "author", "event_time"]},
                  "semantics": {"content": "fully synthetic test evidence"},
                  "evidence_mapping": {"table": "evidence", "id_column": "object_id", "text_column": "text", "result_id_column": "object_id", "object_type": "synthetic_observation"}}
        retrieval = ResearchRetrieval(self.root, {"synthetic_research": source})
        result = retrieval.query(self.task, "synthetic_research", text="20C")
        self.assertEqual(result["status"], "completed", result)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["evidence"][1]["source_ref"]["object_id"], "sample_20c")
        self.assertEqual(retrieval.query(self.task, "synthetic_research", text="不存在")["status"], "empty")
        self.assertTrue(retrieval.query(self.task, "synthetic_research", text="synthetic", limit=1)["truncated"])
        denied = execute_query(retrieval.sources["synthetic_research"], "DELETE FROM evidence", [])
        self.assertEqual(denied["status"], "rejected")
        name, _, payload = self.package()
        self.assertEqual(retrieval.verify(self.task, payload["evidence"])[0]["status"], "verified")
        payload["evidence"][0]["source_hash"] = "0" * 64
        with self.assertRaisesRegex(ResearchError, "EVIDENCE_CHANGED"):
            retrieval.verify(self.task, payload["evidence"])


# ============================================================
# CLI / main 接口区
# ============================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
