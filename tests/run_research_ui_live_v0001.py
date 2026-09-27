# ============================================================
# 文件名: run_research_ui_live_v0001.py
# 中文名: 研究界面真实模型验收启动器
# 版本号: v0001
#
# 主层级: action
# 层级: tests / research_assistant / ui
# 脚本定位: 显式管理员引导的限次浏览器验收环境
#
# 职责说明:
# - 提供隔离合成来源、独立总控身份与真实界面
#
# 本脚本做什么:
# - 在runtime创建测试配置，最多放行两次模型请求并保存总控审计
#
# 本脚本不做什么:
# - 不自动提交界面操作、不读取真实研究数据、不修改原总控许可
#
# 制度边界声明:
# - 凭据仅在进程内；结束或超时关闭测试许可
# - STOP文件结束服务器，保留全部运行证据
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: run_research_ui_live_v0001
# family: run_research_ui_live
# role: bounded_live_ui_acceptance
# version: v0001
# status: experimental
# entry_point: tests/run_research_ui_live_v0001.py
# input:
#   - explicit administrator source configuration and live authorization
# output:
#   - isolated browser server and actual controller receipts
# depends_on:
#   - run_research_live_v0001
#   - research_ui_v0001
# used_by: []
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

import yaml

from run_research_live_v0001 import ROOT, AIController, prepare_controller, require, write_json
from research_assistant_v0001 import ResearchAssistant
from research_ui_v0001 import serve
from research_ledger_v0001 import scoped_path


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "run_research_ui_live"
SCRIPT_NAME = "run_research_ui_live_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 限次句柄与验收环境
# ============================================================

class LimitedClient:
    def __init__(self, client):
        self.client, self.attempts = client, 0

    def models(self, *args, **kwargs):
        return self.client.models(*args, **kwargs)

    def select(self, *args, **kwargs):
        return self.client.select(*args, **kwargs)

    def start(self):
        return self.client.start()

    def pause(self):
        return self.client.pause()

    def invoke(self, *args, **kwargs):
        require(self.attempts < 2, "LIVE_TEST_REQUEST_LIMIT")
        try:
            result = self.client.invoke(*args, **kwargs)
            self.attempts += 1
            return result
        except Exception as exc:
            if type(exc).__name__ != "PermissionDenied":
                self.attempts += 1
            raise


def run(args):
    require(args.confirm_live, "EXPLICIT_LIVE_AUTHORIZATION_REQUIRED")
    folder = ROOT / "runtime/ui_live" / uuid.uuid4().hex[:8]
    folder.mkdir(parents=True)
    config_path, env_names = prepare_controller(args.source_root, folder, [("deepseek", "deepseek-v4-flash"), ("kimi", "kimi-k3")])
    # Isolated test budget only: keep the original administrator configuration unchanged.
    controller_config = yaml.safe_load(config_path.read_text(encoding=DEFAULT_ENCODING))
    controller_config["read_timeout_seconds"] = 240
    controller_config["total_timeout_seconds"] = 300
    config_path.write_text(yaml.safe_dump(controller_config), encoding=DEFAULT_ENCODING)
    base = folder.relative_to(ROOT).as_posix()
    with sqlite3.connect(folder / "evidence.sqlite3") as conn:
        conn.execute("CREATE TABLE evidence(object_id TEXT PRIMARY KEY,text TEXT,origin TEXT,author TEXT,event_time TEXT)")
        conn.executemany("INSERT INTO evidence VALUES(?,?,?,?,?)", [
            ("obs20", "20°C: A loss 8%, B loss 12%", "synthetic_original", "synthetic_user", "2026-01-01T00:00:00Z"),
            ("obs40", "40°C: A loss 18%, B loss 11%", "synthetic_original", "synthetic_user", "2026-02-01T00:00:00Z"),
            ("hypothesis", "AI unverified hypothesis: A always better", "generated", "synthetic_assistant", "2026-03-01T00:00:00Z")])
    source = {"backend": "sqlite", "path": base + "/evidence.sqlite3", "tables": {"evidence": ["object_id", "text", "origin", "author", "event_time"]},
              "semantics": {"description": "new synthetic test records"},
              "evidence_mapping": {"table": "evidence", "id_column": "object_id", "text_column": "text", "result_id_column": "object_id", "object_type": "synthetic_observation"}}
    config = {"schema_version": "1.0", "ledger": base + "/research.sqlite3", "inbox": base + "/inbox", "publication_policy": "authority_first_explicit",
              "source_ids": ["synthetic_research"], "query_sources": {"synthetic_research": source},
              "ai": {"controller_config": base + "/controller.yml", "app_id": "research_ui_test", "token_environment": "RA_UI_TEST_TOKEN", "providers": ["deepseek", "kimi"]}}
    if args.resume_config:
        require(args.task_id and args.resume_config.startswith("runtime/ui_live/"), "SYNTHETIC_UI_RESUME_REQUIRED")
        previous = json.loads(scoped_path(ROOT, args.resume_config).read_text(encoding=DEFAULT_ENCODING))
        config = {**previous, "ai": config["ai"]}
    write_json(folder / "application.json", config)
    with AIController(config_path, root=ROOT) as control:
        control.initialize()
        token = control.register("research_ui_test")["token"]
        client = LimitedClient(control.client("research_ui_test", token))
        app = ResearchAssistant(ROOT, config, ai_client=client)
        if args.resume_config:
            task = app.execute("resume_task", {"task_id": args.task_id})["task"]
            project = {"project_id": task["project_id"]}
        else:
            app.execute("initialize", {})
            project = app.execute("create_project", {"title": "界面真实模型验收 · 合成项目"})
            task = app.execute("create_task", {"project_id": project["project_id"], "title": "验证温度反例与AI接续", "objective": "比较20°C和40°C，拒绝无依据的跨温度推广", "constraints": ["仅合成资料", "生成假设不是原始观察"], "source_ids": ["synthetic_research"]})
        control.set_global(True)
        control.set_permission("research_ui_test", True)
        try:
            with serve(app, args.port) as server:
                server.timeout = 1
                url = f"http://127.0.0.1:{server.server_port}/?task={task['task_id']}"
                write_json(folder / "session.json", {"url": url, "task_id": task["task_id"], "project_id": project["project_id"]})
                print(json.dumps({"status": "listening", "url": url, "run": base, "stop_file": base + "/STOP"}), flush=True)
                end = time.monotonic() + 1200
                while time.monotonic() < end and not (folder / "STOP").exists():
                    server.handle_request()
        finally:
            control.set_global(False)
            control.set_permission("research_ui_test", False)
            write_json(folder / "summary.json", {"status": "stopped", "controller_calls": control.calls(), "resume": app.ledger.resume(task["task_id"])})
            for name in env_names:
                os.environ.pop(name, None)
    return 0


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8794)
    parser.add_argument("--confirm-live", action="store_true")
    parser.add_argument("--resume-config")
    parser.add_argument("--task-id")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
