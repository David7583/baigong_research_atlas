# ============================================================
# 文件名: test_research_integration_v0001.py
# 中文名: 研究助手 AI 与界面集成回归
# 版本号: v0001
#
# 主层级: action
# 层级: tests / research_assistant / integration
# 脚本定位: 隔离总控许可、生成提案、归档与界面错误测试
#
# 职责说明:
# - 验证真实总控逻辑与应用/HTTP入口之间的契约
#
# 本脚本做什么:
# - 使用全新合成SQLite与明确模拟传输，不产生真实模型费用
#
# 本脚本不做什么:
# - 不将模拟传输当作Provider或客户端实测
#
# 制度边界声明:
# - 测试产物限test-output；测试密钥为无效合成值
# - 权限拒绝必须在模拟传输之前生效，失败不推进正式修订
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: test_research_integration_v0001
# family: test_research_integration
# role: controller_ui_integration_test
# version: v0001
# status: experimental
# entry_point: tests/test_research_integration_v0001.py
# input:
#   - synthetic source and simulated model output
# output:
#   - offline integration assertions
# depends_on:
#   - research_assistant_v0001
#   - research_ui_v0001
#   - research_harness_bridge_v0001
# used_by: []
# ============================================================

from __future__ import annotations

import http.client
import json
import sqlite3
import sys
import threading
import unittest
import uuid
from pathlib import Path

import yaml


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "test_research_integration"
SCRIPT_NAME = "test_research_integration_v0001"
SCRIPT_VERSION = "v0001"
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts/action/development/scripts/research_assistant"))

from research_assistant_v0001 import ResearchAssistant
from research_ledger_v0001 import ResearchError
from research_harness_bridge_v0001 import HarnessBridge
from research_ui_v0001 import Workbench, serve
from scripts.action.infrastructures.action_ai_controller_v0002 import AIController, ControlError


# ============================================================
# 隔离传输与测试
# ============================================================

class FakeTransport:
    def __init__(self):
        self.calls, self.output, self.error = 0, {}, None

    def execute(self, plan):
        self.calls += 1
        self.last_payload = plan.payload
        if self.error:
            raise ControlError(self.error)
        return {"choices": [{"message": {"role": "assistant", "content": json.dumps(self.output)}, "finish_reason": "stop"}],
                "usage": {"total_tokens": 10}}

    def close(self):
        pass


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.root = ROOT / "test-output" / ("integration_" + uuid.uuid4().hex[:8])
        self.root.mkdir(parents=True)
        control_config = yaml.safe_load((ROOT / "config/action/ai/action_ai_controller_config_v0002.yml").read_text(encoding="utf-8-sig"))
        control_config["state_path"] = "control.sqlite3"
        control_config["sources"] = {"keys": "keys.yml", "catalog": "catalog.yml", "ollama": "ollama.yml"}
        keys = {"schema_version": "2.0", "credentials": {"fixture": {"provider": "fixture", "scope": "inference", "source": "inline", "value": "synthetic-invalid-credential"}}}
        catalog = {"providers": {"fixture": {"authentication": {"scheme": "Bearer", "header": "Authorization"},
            "profiles": {"default": {"credential": "fixture", "protocol": "openai", "base_url": "https://example.invalid/v1"}}}},
            "models": [{"provider": "fixture", "model_id": "model-a", "capabilities": ["text_generation"]}, {"provider": "fixture", "model_id": "model-b", "capabilities": ["chat"]}]}
        for name, value in (("controller.yml", control_config), ("keys.yml", keys), ("catalog.yml", catalog)):
            (self.root / name).write_text(yaml.safe_dump(value), encoding=DEFAULT_ENCODING)
        self.transport = FakeTransport()
        self.control = AIController(self.root / "controller.yml", root=self.root, transport=self.transport)
        self.control.initialize()
        token = self.control.register("research_test")["token"]
        self.client = self.control.client("research_test", token)
        with sqlite3.connect(self.root / "evidence.sqlite3") as conn:
            conn.execute("CREATE TABLE evidence(object_id TEXT PRIMARY KEY, text TEXT, origin TEXT, author TEXT, event_time TEXT)")
            conn.execute("INSERT INTO evidence VALUES('obs','Synthetic observation only','synthetic_original','fixture-author','2026-01-01T00:00:00Z')")
        source = {"backend": "sqlite", "path": "evidence.sqlite3", "tables": {"evidence": ["object_id", "text", "origin", "author", "event_time"]},
                  "semantics": {}, "evidence_mapping": {"table": "evidence", "id_column": "object_id", "text_column": "text", "result_id_column": "object_id", "object_type": "observation"}}
        self.app = ResearchAssistant(self.root, {"schema_version": "1.0", "ledger": "ledger.sqlite3", "inbox": "inbox",
            "publication_policy": "authority_first_explicit", "source_ids": ["synthetic"], "query_sources": {"synthetic": source}}, ai_client=self.client)
        self.app.execute("initialize", {})
        project = self.app.execute("create_project", {"title": "Synthetic UI integration"})
        self.task = self.app.execute("create_task", {"project_id": project["project_id"], "title": "<script>alert(1)</script>", "objective": "Evidence test", "constraints": ["synthetic only"], "source_ids": ["synthetic"]})
        self.transport.output = {"project_id": self.task["project_id"], "task_id": self.task["task_id"], "report": "合成提案", "completed": ["核对"], "open_questions": ["缺少重复"], "next_steps": ["补充样本"], "evidence_ids": ["obs"]}
        self.request = {"task_id": self.task["task_id"], "source_id": "synthetic", "object_ids": ["obs"]}
        self.app.execute("ai_start", {"provider": "fixture", "model": "model-a"})

    def tearDown(self):
        self.control.close()

    def permit(self):
        self.control.set_global(True)
        self.control.set_permission("research_test", True)

    def test_permission_pause_and_other_application_isolation(self):
        for expected in ("global_ai_disabled", "application_ai_denied"):
            with self.assertRaisesRegex(ResearchError, expected):
                self.app.execute("generate_ai_proposal", self.request)
            self.control.set_global(True)
        self.assertEqual(self.transport.calls, 0)
        self.permit()
        token = self.control.register("other")["token"]
        other = self.control.client("other", token)
        other.select("fixture", "model-b")
        other.start()
        before = self.control.status()
        self.app.execute("ai_pause", {})
        with self.assertRaisesRegex(ResearchError, "application_ai_paused"):
            self.app.execute("generate_ai_proposal", self.request)
        after = self.control.status()
        self.assertEqual([x for x in before["applications"] if x["app_id"] == "other"], [x for x in after["applications"] if x["app_id"] == "other"])
        self.assertEqual(self.transport.calls, 0)

    def test_proposal_explicit_archive_and_duplicate_without_extra_model_call(self):
        self.permit()
        proposal = self.app.execute("generate_ai_proposal", self.request)
        self.assertEqual(proposal["status"], "proposal_ready", proposal)
        prompt = self.transport.last_payload["messages"][0]["content"]
        self.assertIn("fixture-author", prompt)
        self.assertIn("2026-01-01T00:00:00Z", prompt)
        self.assertEqual(self.app.execute("list_proposals", {"task_id": self.task["task_id"]})["proposals"][0]["status"], "awaiting_review")
        self.assertEqual(self.app.ledger.resume(self.task["task_id"])["current_revision"], 0)
        first = self.app.execute("submit_delivery", {"package": proposal["package"]})
        again = self.app.execute("submit_delivery", {"package": proposal["package"]})
        self.assertEqual(first["delivery_id"], again["delivery_id"])
        self.assertEqual(again["status"], "existing")
        self.assertEqual(self.app.ledger.resume(self.task["task_id"])["current_revision"], 1)
        self.assertEqual(self.transport.calls, 1)
        self.assertEqual(self.app.execute("list_proposals", {"task_id": self.task["task_id"]})["proposals"][0]["status"], "completed")
        saved = self.app.ledger.resume(self.task["task_id"])["task"]
        self.assertTrue(any(a["path"] == "call_provenance.json" for a in saved["artifacts"]))

    def test_invalid_identity_is_preserved_but_not_ready(self):
        self.permit()
        self.transport.output["project_id"] = "wrong"
        result = self.app.execute("generate_ai_proposal", self.request)
        self.assertEqual(result["status"], "proposal_rejected")
        self.assertTrue((self.app.inbox / result["package"] / "model_output.json").is_file())
        self.assertFalse((self.app.inbox / result["package"] / "READY").exists())
        self.assertEqual(self.app.ledger.resume(self.task["task_id"])["current_revision"], 0)

    def test_provider_failure_has_no_automatic_retry(self):
        self.permit()
        self.transport.error = "provider_http_error_503"
        with self.assertRaisesRegex(ResearchError, "provider_http_error_503"):
            self.app.execute("generate_ai_proposal", self.request)
        self.assertEqual(self.transport.calls, 1)
        self.assertEqual(self.app.ledger.resume(self.task["task_id"])["current_revision"], 0)

    def test_unknown_evidence_and_model_rejected_before_call(self):
        self.permit()
        with self.assertRaisesRegex(ResearchError, "EVIDENCE_UNAVAILABLE"):
            self.app.execute("generate_ai_proposal", {**self.request, "object_ids": ["missing"]})
        with self.assertRaisesRegex(ResearchError, "model_not_in_catalog"):
            self.app.execute("ai_start", {"provider": "fixture", "model": "missing"})
        self.assertEqual(self.transport.calls, 0)

    def test_bridge_model_budget_and_sse(self):
        self.permit()
        bridge = HarnessBridge(self.client, "model-a", request_limit=1)
        body = {"model": "model-a", "messages": [{"role": "user", "content": "synthetic"}], "stream": True}
        with self.assertRaisesRegex(ValueError, "MODEL_SELECTION_MISMATCH"):
            bridge.invoke({**body, "model": "wrong"})
        output = bridge.invoke(body)
        self.assertIn("data: [DONE]", output)
        self.assertEqual(bridge.audit[0]["transport"], "buffered_sync_response_as_sse")
        with self.assertRaisesRegex(ValueError, "REQUEST_BUDGET_EXCEEDED"):
            bridge.invoke(body)

    def test_ui_escaping_csrf_host_and_post_redirect(self):
        self.app.ai_config = {"providers": ["fixture"]}
        self.app.execute("ai_start", {"provider": "fixture", "model": "model-b"})
        workbench = Workbench(self.app)
        page = workbench.page(self.task["task_id"])
        self.assertIn("fixture / model-a", page)
        self.assertIn("fixture / model-b", page)
        self.assertIn('selected>fixture / model-b</option>', page)
        self.assertEqual(page.count('<form '), page.count('<form method="post" action="/">'))
        self.assertNotIn("<script>alert", page)
        self.assertIn("&lt;script&gt;", page)
        with self.assertRaisesRegex(ResearchError, "CSRF_REJECTED"):
            workbench.submit({"csrf": "bad", "action": "initialize"})
        server = serve(self.app, 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = http.client.HTTPConnection("127.0.0.1", server.server_port)
            client.request("GET", "/", headers={"Host": "evil.invalid"})
            response = client.getresponse()
            self.assertEqual(response.status, 403)
            response.read()
            client.request("GET", "/")
            response = client.getresponse()
            html = response.read().decode(DEFAULT_ENCODING)
            csrf = html.split('name="csrf" value="')[1].split('"')[0]
            client.request("POST", "/", body=f"csrf={csrf}&action=create_project&title=synthetic", headers={"Content-Type": "application/x-www-form-urlencoded"})
            response = client.getresponse()
            self.assertEqual(response.status, 303)
            location = response.getheader("Location")
            response.read()
            for _ in range(2):
                client.request("GET", location)
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                response.read()
            self.assertEqual(len(self.app.ledger.find_projects()["projects"]), 2)
            client.close()
        finally:
            server.shutdown()
            server.server_close()

    def test_restart_recovers_proposal_and_retains_pending_operations(self):
        from research_ai_v0001 import prepare_proposal
        snapshot = self.app.ledger.resume(self.task["task_id"])
        snapshot["task"]["handoff"] = {"pending_confirmation": ["confirm external side effect"], "unknown_operations": ["previous tool outcome unknown"]}
        output = {"visible_output": json.dumps(self.transport.output), "provenance": {"provider": "fixture", "model": "model-a", "call_id": "synthetic_call"}}
        self.transport.output["evidence_ids"] = []
        output["visible_output"] = json.dumps(self.transport.output)
        prepared = prepare_proposal(self.app.ledger, self.app.inbox, snapshot, output, [])
        self.assertEqual(prepared["status"], "proposal_ready", prepared)
        manifest = json.loads((self.app.inbox / prepared["package"] / "manifest.json").read_text(encoding=DEFAULT_ENCODING))
        self.assertEqual(manifest["handoff"]["unknown_operations"], ["previous tool outcome unknown"])
        self.assertEqual(manifest["handoff"]["pending_confirmation"], ["confirm external side effect"])
        restored = ResearchAssistant(self.root, {"schema_version": "1.0", "ledger": "ledger.sqlite3", "inbox": "inbox", "publication_policy": "authority_first_explicit", "source_ids": ["synthetic"]})
        self.assertEqual(restored.execute("list_proposals", {"task_id": self.task["task_id"]})["proposals"][0]["package"], prepared["package"])

    def test_corrupt_proposal_does_not_hide_other_tasks(self):
        self.permit()
        proposal = self.app.execute("generate_ai_proposal", self.request)
        (self.app.inbox / proposal["package"] / "report.md").write_text("tampered", encoding=DEFAULT_ENCODING)
        result = self.app.execute("list_proposals", {"task_id": self.task["task_id"]})
        self.assertEqual(result["proposals"], [])
        self.assertEqual(len(result["unreadable_packages"]), 1)

    def test_pause_during_inflight_does_not_claim_cancellation(self):
        self.permit()
        entered, release = threading.Event(), threading.Event()
        original = self.transport.execute
        def wait_execute(plan):
            entered.set()
            release.wait(3)
            return original(plan)
        self.transport.execute = wait_execute
        results = []
        worker = threading.Thread(target=lambda: results.append(self.app.execute("generate_ai_proposal", self.request)))
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            paused = self.app.execute("ai_pause", {})
            self.assertFalse(paused["application_running"])
            self.assertFalse(paused["inflight_cancellation_confirmed"])
        finally:
            release.set()
            worker.join(4)
        self.assertEqual(results[0]["status"], "proposal_ready")
        with self.assertRaisesRegex(ResearchError, "application_ai_paused"):
            self.app.execute("generate_ai_proposal", self.request)
        self.assertEqual(self.transport.calls, 1)


if __name__ == "__main__":
    unittest.main()
