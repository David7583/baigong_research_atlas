# ============================================================
# 文件名: run_research_live_v0001.py
# 中文名: 授权真实模型接续验收
# 版本号: v0001
#
# 主层级: action
# 层级: tests / research_assistant / live
# 脚本定位: 显式管理员配置注入与合成资料真实模型验收
#
# 职责说明:
# - 在独立总控运行库中验证已授权的模型接续
#
# 本脚本做什么:
# - 管理员按显式来源读取现有 API 配置，只在进程环境传递必要密钥
# - 创建自身应用身份，保存可见输出和调用回执，不复制原总控状态
#
# 本脚本不做什么:
# - 不读取客户端登录凭证或真实研究数据，不安装依赖，不自动重试
#
# 制度边界声明:
# - 真实收费调用仅在 --confirm-live 授权参数下执行，最多两次
# - 密钥及新应用 Token 只在内存；文件仅含环境变量引用及去敏回执
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: run_research_live_v0001
# family: run_research_live
# role: authorized_live_acceptance
# version: v0001
# status: experimental
# entry_point: tests/run_research_live_v0001.py
# input:
#   - explicit administrator configuration source and live authorization
# output:
#   - isolated runtime artifacts and real controller call provenance
# depends_on:
#   - research_ai_v0001
#   - action_ai_controller_v0002
#   - PyYAML
# used_by: []
# ============================================================

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import uuid
from pathlib import Path

import yaml


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "run_research_live"
SCRIPT_NAME = "run_research_live_v0001"
SCRIPT_VERSION = "v0001"
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts/action/development/scripts/research_assistant"))

from research_ai_v0001 import analyze_stage
from research_ledger_v0001 import Ledger, canonical, digest, now, read_package, require
from scripts.action.infrastructures.action_ai_controller_v0002 import AIController, PermissionDenied


# ============================================================
# 工具函数区
# ============================================================

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical(data), encoding=DEFAULT_ENCODING)


def deliver(ledger, folder, task, actor, report, completed, questions, steps, evidence):
    revision = ledger.resume(task["task_id"])["current_revision"]
    run = ledger.begin_run(task["task_id"], revision, actor)
    package = folder / run["run_id"]
    package.mkdir()
    raw = report.encode(DEFAULT_ENCODING)
    (package / "report.md").write_bytes(raw)
    envelope = {"schema_version": "1.0", "project_id": task["project_id"], "task_id": task["task_id"], "run_id": run["run_id"],
                "base_revision": revision, "idempotency_key": run["run_id"], "task_status": "paused", "run_status": "succeeded",
                "handoff": {"completed": completed, "failed": [], "decisions": ["按温度分层比较"], "open_questions": questions,
                            "next_steps": steps, "pending_confirmation": [], "unknown_operations": []},
                "artifacts": [{"artifact_id": "report", "path": "report.md", "size": len(raw), "sha256": digest(raw), "origin": "generated", "generated_at": now()}],
                "evidence": [{"source_id": "synthetic_research", "object_id": e["object_id"], "source_hash": digest(e["text"].encode(DEFAULT_ENCODING)),
                              "origin": e["origin"], "locator": {"event_time": e["event_time"], "author": e["author"]}, "claim": "temperature-specific comparison", "relation": "context"} for e in evidence],
                "coverage": [{"source_id": "synthetic_research", "mode": "manual", "query": "20C and 40C synthetic comparison", "status": "completed", "truncated": False, "limitations": "synthetic records only; generated hypotheses are not observations"}]}
    write_json(package / "manifest.json", envelope)
    (package / "READY").write_text(digest((package / "manifest.json").read_bytes()), encoding=DEFAULT_ENCODING)
    envelope, artifacts, original = read_package(folder, run["run_id"])
    accepted = ledger.accept(envelope, artifacts, original)
    return ledger.archive(accepted["delivery_id"])


# ============================================================
# 核心验收
# ============================================================

def prepare_controller(source_root, live_root, selections):
    source_root = Path(source_root).resolve()
    source_config = yaml.safe_load((source_root / "config/action/ai/action_ai_controller_config_v0002.yml").read_text(encoding="utf-8-sig"))
    source_keys = yaml.safe_load((source_root / source_config["sources"]["keys"]).read_text(encoding="utf-8-sig"))
    source_catalog = yaml.safe_load((source_root / source_config["sources"]["catalog"]).read_text(encoding="utf-8-sig"))
    config = copy.deepcopy(source_config)
    relative = live_root.relative_to(ROOT).as_posix()
    config["state_path"] = relative + "/controller.sqlite3"
    config["sources"] = {name: relative + "/" + name + ".yml" for name in ("keys", "catalog", "ollama", "local_embeddings")}
    catalog = {"schema_version": source_catalog.get("schema_version"), "providers": {}, "models": []}
    keys = {"schema_version": "2.0", "credentials": {}, "provider_bindings": {}}
    environment_names = []
    for provider, model in selections:
        descriptor = copy.deepcopy(source_catalog["providers"][provider])
        profile = descriptor["profiles"]["default"]
        binding = source_keys.get("provider_bindings", {}).get(provider, {})
        require(binding.get("profile", "default") == "default", "REVIEW_NONDEFAULT_PROVIDER_BINDING")
        credential_name = binding.get("credential", profile["credential"])
        original = source_keys["credentials"][credential_name]
        secret = original.get("value", "") if original.get("source") == "inline" else os.environ.get(original.get("environment_variable", ""), "")
        require(isinstance(secret, str) and secret.strip(), "LIVE_CREDENTIAL_UNAVAILABLE")
        environment_name = "RA_LIVE_" + uuid.uuid4().hex.upper()
        os.environ[environment_name] = secret
        environment_names.append(environment_name)
        descriptor["profiles"] = {"default": profile}
        catalog["providers"][provider] = descriptor
        catalog["models"] += [m for m in source_catalog["models"] if m["provider"] == provider and m["model_id"] == model]
        keys["credentials"][profile["credential"]] = {"provider": provider, "scope": "inference", "source": "environment", "environment_variable": environment_name}
    source_keys = None
    secret = None
    values = {"keys": keys, "catalog": catalog, "ollama": {"schema_version": "1.0", "provider": "ollama", "model_inventory": []},
              "local_embeddings": {"schema_version": "1.0", "models": {}}}
    for name, value in values.items():
        (ROOT / config["sources"][name]).write_text(yaml.safe_dump(value, allow_unicode=True), encoding=DEFAULT_ENCODING)
    config_path = live_root / "controller.yml"
    config_path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding=DEFAULT_ENCODING)
    return config_path, environment_names


def run(source_root):
    live_root = ROOT / "runtime/live" / uuid.uuid4().hex[:12]
    live_root.mkdir(parents=True)
    selections = [("deepseek", "deepseek-v4-flash"), ("kimi", "kimi-k3")]
    config_path, environment_names = prepare_controller(source_root, live_root, selections)
    ledger = Ledger(live_root / "research.sqlite3")
    ledger.initialize()
    project = ledger.create_project("真实模型接续 · 仅合成资料")
    task = ledger.create_task(project["project_id"], "保温样本条件比较", "核查20°C结论能否推广到40°C", ["仅合成资料", "区分用户观察与AI假设", "保留反例"], ["synthetic_research"])
    evidence = [{"object_id": "obs20", "text": "20°C: material A loss=8%, B loss=12%.", "origin": "synthetic_original", "author": "synthetic_user", "event_time": "2026-01-01T00:00:00Z"},
                {"object_id": "obs40", "text": "40°C: material A loss=18%, B loss=11%.", "origin": "synthetic_original", "author": "synthetic_user", "event_time": "2026-02-01T00:00:00Z"},
                {"object_id": "hypothesis", "text": "Assistant hypothesis: A always performs better at every temperature. Not verified.", "origin": "generated", "author": "synthetic_assistant", "event_time": "2026-03-01T00:00:00Z"}]
    write_json(live_root / "synthetic_evidence.json", evidence)
    actor = {"kind": "agent", "name": "Codex visible authored stage", "version": None, "provider": None, "model": None, "access": "local_tools"}
    deliver(ledger, live_root, task, actor, "# 阶段一\n合成20°C观察中A损耗8%，B为12%，仅在此条件下A更低。尚未核对40°C，不接受‘所有温度均优于B’的AI假设。", ["比较20°C"], ["40°C是否出现反例？"], ["核对40°C观察并修订结论"], evidence[:1])
    summary = {"status": "running", "project_id": task["project_id"], "task_id": task["task_id"], "calls": [], "client_scope": "Codex authored stage -> actual model API stages; not Harness client acceptance"}
    try:
        with AIController(config_path, root=ROOT) as control:
            control.initialize()
            registration = control.register("research_assistant_live")
            client = control.client("research_assistant_live", registration["token"])
            for index, (provider, model) in enumerate(selections):
                client.select(provider, model)
                client.start()
                if index == 0:
                    try:
                        client.invoke("chat", {"messages": [{"role": "user", "content": "must be denied"}]})
                        raise RuntimeError("PERMISSION_GATE_FAILED")
                    except PermissionDenied:
                        summary["global_off_denied"] = True
                    control.set_global(True)
                    control.set_permission("research_assistant_live", True)
                proposal = analyze_stage(client, ledger.resume(task["task_id"]), evidence)
                write_json(live_root / f"model_{index + 1}_visible.json", proposal)
                text = proposal["visible_output"].strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                parsed = json.loads(text)
                require(parsed["project_id"] == task["project_id"] and parsed["task_id"] == task["task_id"], "MODEL_CHANGED_PROJECT_IDENTITY")
                require(set(parsed["evidence_ids"]) <= {e["object_id"] for e in evidence}, "MODEL_INVENTED_EVIDENCE_ID")
                actor = {"kind": "ai", "name": provider + " API via Action controller", "version": None, "provider": provider, "model": model, "access": "controller"}
                result = deliver(ledger, live_root, task, actor, parsed["report"], parsed["completed"], parsed["open_questions"], parsed["next_steps"], evidence)
                summary["calls"].append({"provenance": proposal["provenance"], "revision": result["revision"], "identity_preserved": True})
                write_json(live_root / "summary.json", summary)
            summary["status"] = "completed"
            summary["controller_calls"] = control.calls()
            control.set_global(False)
        write_json(live_root / "resume.json", ledger.resume(task["task_id"]))
    except Exception as exc:
        summary["status"] = "failed"
        summary["error_type"] = type(exc).__name__
        # Public controller errors are safe stable codes; never include request headers.
        summary["error_code"] = str(exc) if type(exc).__name__ in {"ConfigError", "DataError", "PermissionDenied", "ControlError", "ResearchError"} else None
    finally:
        # Close the isolated administrative test permission even after a failed call.
        with AIController(config_path, root=ROOT) as cleanup:
            summary["controller_calls"] = cleanup.calls()
            cleanup.set_global(False)
            cleanup.set_permission("research_assistant_live", False)
        for name in environment_names:
            os.environ.pop(name, None)
    write_json(live_root / "summary.json", summary)
    print(canonical({"status": summary["status"], "report": (live_root / "summary.json").relative_to(ROOT).as_posix(),
                     "calls_completed": len(summary["calls"]), "error_type": summary.get("error_type"), "error_code": summary.get("error_code")}))
    return 0 if summary["status"] == "completed" else 2


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--confirm-live", action="store_true")
    args = parser.parse_args()
    require(args.confirm_live, "EXPLICIT_LIVE_AUTHORIZATION_REQUIRED")
    return run(args.source_root)


if __name__ == "__main__":
    raise SystemExit(main())
