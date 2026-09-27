# ============================================================
# 文件名: run_research_harness_v0001.py
# 中文名: Harness 真实工具接续验收
# 版本号: v0001
#
# 主层级: action
# 层级: tests / research_assistant / client
# 脚本定位: 使用已安装客户端和独立总控运行合成资料验收
#
# 职责说明:
# - 验证外部 Harness 读取持久交接、执行文件工具并交付下一阶段
#
# 本脚本做什么:
# - 创建隔离客户端配置和测试账本，显式管理测试许可，保存真实回执
#
# 本脚本不做什么:
# - 不安装依赖、不复用客户端私有历史、不自动晋升、不冒充客户端直接写库
#
# 制度边界声明:
# - Provider 凭据由显式管理员引导，仅在父进程环境传递；客户端仅有本机令牌
# - 最多八次请求；无自动重试；只读写合成工作区；退出关闭测试许可
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: run_research_harness_v0001
# family: run_research_harness
# role: isolated_client_acceptance
# version: v0001
# status: experimental
# entry_point: tests/run_research_harness_v0001.py
# input:
#   - installed dsh bin, explicit source configuration and live authorization
# output:
#   - client artifacts, controller audit and accepted research revision
# depends_on:
#   - run_research_live_v0001
#   - research_harness_bridge_v0001
# used_by: []
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import subprocess
import threading
import uuid
from pathlib import Path

import yaml

from run_research_live_v0001 import ROOT, AIController, Ledger, deliver, digest, prepare_controller, require, write_json
from research_harness_bridge_v0001 import HarnessBridge


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "run_research_harness"
SCRIPT_NAME = "run_research_harness_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心验收
# ============================================================

def run(args):
    require(args.confirm_live, "EXPLICIT_LIVE_AUTHORIZATION_REQUIRED")
    bin_path = args.dsh_bin.resolve()
    require(bin_path.is_file(), "INSTALLED_DSH_REQUIRED")
    live = ROOT / "runtime/harness" / uuid.uuid4().hex[:10]
    workspace = live / "workspace"
    workspace.mkdir(parents=True)
    config_path, environment_names = prepare_controller(args.source_root, live, [(args.provider, args.model)])
    ledger = Ledger(live / "research.sqlite3")
    ledger.initialize()
    project = ledger.create_project("Harness真实工具接续：合成资料")
    task = ledger.create_task(project["project_id"], "温度条件与反例", "判断20°C结论能否推广至40°C", ["仅合成资料", "生成假设不是原始观察"], ["synthetic_research"])
    evidence = [{"object_id": "obs20", "text": "20°C A loss 8%, B loss 12%", "origin": "synthetic_original", "author": "synthetic_user", "event_time": "2026-01-01T00:00:00Z"},
                {"object_id": "obs40", "text": "40°C A loss 18%, B loss 11%", "origin": "synthetic_original", "author": "synthetic_user", "event_time": "2026-02-01T00:00:00Z"},
                {"object_id": "hypothesis", "text": "AI hypothesis: A always better. Unverified.", "origin": "generated", "author": "synthetic_assistant", "event_time": "2026-03-01T00:00:00Z"}]
    actor = {"kind": "agent", "name": "Codex authored initial fixture", "version": None, "provider": None, "model": None, "access": "local_tools"}
    deliver(ledger, live, task, actor, "20°C观察中A损耗较低，尚未核对40°C；跨温度推广待核查。", ["核对20°C"], ["40°C是否有反例"], ["读取证据核查40°C"], evidence[:1])
    # File tools truncate long lines: readable transfer files must preserve
    # complete identity fields without relying on a minified single line.
    for filename, value in (("task.json", ledger.resume(task["task_id"])), ("evidence.json", evidence)):
        (workspace / filename).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding=DEFAULT_ENCODING)
    input_hashes = {name: digest((workspace / name).read_bytes()) for name in ("task.json", "evidence.json")}
    write_json(live / "input_hashes.json", input_hashes)
    home = live / "dsh_home"
    profile = home / "profiles/headless"
    profile.mkdir(parents=True)
    write_json(profile / "package.json", {"private": True, "dsh": {"profile": {"bundles": ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-headless"], "patchReload": "startup"}}})
    summary = {"status": "running", "project_id": task["project_id"], "task_id": task["task_id"], "runtime": str(bin_path),
               "scope": "Harness file tools; local host validates and archives stage.json", "request_limit": 8}
    server = None
    try:
        with AIController(config_path, root=ROOT) as control:
            control.initialize()
            registration = control.register("research_harness_test")
            control.set_global(True)
            control.set_permission("research_harness_test", True)
            client = control.client("research_harness_test", registration["token"])
            client.select(args.provider, args.model)
            client.start()
            bridge = HarnessBridge(client, args.model)
            server = bridge.server()
            threading.Thread(target=server.serve_forever, daemon=True).start()
            patch = [{"id": "agent-default-model", "config": {"provider": "deepseek-official", "model": args.model}},
                     {"id": "llm-deepseek", "config": {"apiKeyEnv": "RA_HARNESS_TOKEN", "baseURL": f"http://127.0.0.1:{server.server_port}/v1", "thinking": "disabled", "reasoningEffort": "off", "maxTokens": 4096}}]
            disabled = ["session-title-llm", "session-log-deepseek", "plugin-package-inventory-deepseek", "session-telemetry-otel",
                        "tool-subagent", "tool-subagent-control", "tool-subagent-list-agents", "tool-subagent-fork", "tool-workflow",
                        "tool-web", "web-search-deepseek", "web-fetch-http", "tool-bash", "tool-pwsh", "tool-jobs", "tool-skill",
                        "agent-instructions", "llm-retry", "tool-ralph", "tool-goal", "tool-todo"]
            patch += [{"id": row, "disabled": True} for row in disabled]
            (profile / "cordis.patch.yml").write_text(yaml.safe_dump(patch, allow_unicode=True), encoding=DEFAULT_ENCODING)
            env = {k: v for k, v in os.environ.items() if k in {"PATH", "Path", "SystemRoot", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "COMSPEC", "PATHEXT"}}
            env.update(DSH_HOME=str(home), DSH_TELEMETRY_DISABLED="1", DSH_PERMISSION_MODE="workspace-write", RA_HARNESS_TOKEN=bridge.token, NO_PROXY="127.0.0.1,localhost")
            prompt = (
                "This is an authorized synthetic research continuity test. Use your file tools to read task.json and evidence.json from this workspace. "
                "Continue the SAME project/task without private previous chat history. Verify the 40°C counterexample and distinguish generated hypotheses from observations. "
                "Use file tools to write stage.json as UTF-8 JSON with project_id, task_id, report (Chinese Markdown), completed/open_questions/next_steps/evidence_ids (arrays of strings). "
                "Preserve constraints and unresolved questions. Do not create code, install packages, call other agents, use network tools or modify input files. "
                "Do not claim to write the ledger: the local host will validate/archive stage.json. Finish with a short factual handoff."
            )
            execution = subprocess.run(["node", str(bin_path), "--profile", "headless", prompt], cwd=workspace, env=env, capture_output=True, text=True, encoding=DEFAULT_ENCODING, timeout=300)
            (live / "client_stdout.txt").write_text(execution.stdout, encoding=DEFAULT_ENCODING)
            (live / "client_stderr.txt").write_text(execution.stderr, encoding=DEFAULT_ENCODING)
            summary.update(client_exit=execution.returncode, bridge_audit=bridge.audit, controller_calls=control.calls())
            write_json(live / "summary.json", summary)
            require(execution.returncode == 0, "HARNESS_CLIENT_FAILED")
            result_path = workspace / "stage.json"
            require(result_path.is_file() and result_path.stat().st_size <= 65536, "CLIENT_DELIVERY_MISSING")
            parsed = json.loads(result_path.read_text(encoding=DEFAULT_ENCODING))
            require(parsed["project_id"] == task["project_id"] and parsed["task_id"] == task["task_id"], "CLIENT_IDENTITY_CHANGED")
            require(set(parsed["evidence_ids"]) <= {e["object_id"] for e in evidence}, "CLIENT_EVIDENCE_INVENTED")
            require(all(digest((workspace / name).read_bytes()) == value for name, value in input_hashes.items()), "CLIENT_INPUT_CHANGED")
            package = json.loads((bin_path.parents[1] / "package.json").read_text(encoding=DEFAULT_ENCODING))
            actor = {"kind": "agent", "name": "DeepSeek Harness headless (file tools; host archive)", "version": package["version"], "provider": args.provider, "model": args.model, "access": "local_tools"}
            archived = deliver(ledger, live, task, actor, parsed["report"], parsed["completed"], parsed["open_questions"], parsed["next_steps"], evidence)
            summary.update(status="completed", revision=archived["revision"], client_version=package["version"])
            write_json(live / "resume.json", ledger.resume(task["task_id"]))
    except Exception as exc:
        summary.update(status="failed", error_type=type(exc).__name__, error_code=str(exc) if type(exc).__name__ in {"ResearchError", "ControlError", "ConfigError", "PermissionDenied"} else None)
    finally:
        if server:
            server.shutdown()
            server.server_close()
        with AIController(config_path, root=ROOT) as control:
            summary["controller_calls"] = control.calls()
            control.set_global(False)
            control.set_permission("research_harness_test", False)
        for name in environment_names:
            os.environ.pop(name, None)
        write_json(live / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "report": str((live / "summary.json").relative_to(ROOT)), "error_code": summary.get("error_code")}))
    return 0 if summary["status"] == "completed" else 2


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--dsh-bin", type=Path, required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--confirm-live", action="store_true")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
