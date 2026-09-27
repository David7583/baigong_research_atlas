# ============================================================
# 文件名: research_ui_v0001.py
# 中文名: 研究接续工作台
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / ui
# 脚本定位: 本机研究任务和交接状态的浏览器交互入口
#
# 职责说明:
# - 展示项目、阶段、证据、成果、归档与索引状态
#
# 本脚本做什么:
# - 将用户表单转换为研究助手工具请求，展示真实返回结果
#
# 本脚本不做什么:
# - 不实现检索、模型、权限或归档业务，不自动初始化或启用 AI
#
# 制度边界声明:
# - 只监听 127.0.0.1；POST 校验 Host、Origin 和会话 CSRF
# - 所有资料按文本转义显示；不渲染成果中的程序，不输出内部异常正文
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_ui_v0001
# family: research_ui
# role: local_research_workbench
# version: v0001
# status: archived
# entry_point: staging/scripts/action/development/scripts/research_assistant/research_ui_v0001.py
# input:
#   - browser forms and research application configuration
# output:
#   - escaped responsive HTML and explicit operation results
# depends_on:
#   - research_assistant_v0001
#   - Python stdlib
# used_by: []
# ============================================================

from __future__ import annotations

import argparse
import html
import json
import secrets
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

from research_assistant_v0001 import ResearchAssistant, project_root, read_json
from research_ledger_v0001 import ResearchError, require, scoped_path


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_ui"
SCRIPT_NAME = "research_ui_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def esc(value):
    return html.escape(str(value), quote=True)


def pretty(value):
    return esc(json.dumps(value, ensure_ascii=False, indent=2))


def field(label, name, value="", kind="text"):
    return f'<label>{esc(label)}<input name="{esc(name)}" type="{kind}" value="{esc(value)}" required></label>'


# ============================================================
# 核心界面组件
# ============================================================

class Workbench:
    def __init__(self, app):
        self.app = app
        self.csrf = secrets.token_urlsafe(32)
        self.receipts = {}

    def form(self, action, fields, button):
        return f'<form method="post" action="/"><input type="hidden" name="csrf" value="{self.csrf}"><input type="hidden" name="action" value="{esc(action)}">{fields}<button>{esc(button)}</button></form>'

    def submit(self, form):
        require(secrets.compare_digest(form.get("csrf", ""), self.csrf), "CSRF_REJECTED")
        action = form["action"]
        if action == "initialize":
            return self.app.execute("initialize", {})
        if action == "create_project":
            return self.app.execute(action, {"title": form["title"]})
        if action == "create_task":
            return self.app.execute(action, {"project_id": form["project_id"], "title": form["title"], "objective": form["objective"],
                                             "constraints": [s for s in form["constraints"].splitlines() if s.strip()], "source_ids": self.app.source_ids})
        if action == "begin_run":
            kind = form["kind"]
            return self.app.execute(action, {"task_id": form["task_id"], "base_revision": int(form["base_revision"]),
                "executor": {"kind": kind, "name": form["name"], "version": form.get("version") or None,
                             "provider": form.get("provider") or None, "model": form.get("model") or None,
                             "access": "manual_exchange" if kind == "ai" else "local_tools"}})
        if action in {"prepare_ingestion", "index_revision"}:
            return self.app.execute(action, {"task_id": form["task_id"]})
        if action == "submit_delivery":
            return self.app.execute(action, {"package": form["package"]})
        if action == "search_evidence":
            return self.app.execute(action, {"task_id": form["task_id"], "source_id": form["source_id"], "text": form["text"]})
        if action == "ai_start":
            return self.app.execute(action, json.loads(form["selection"]))
        if action == "ai_pause":
            return self.app.execute(action, {})
        if action == "generate_ai_proposal":
            return self.app.execute(action, {"task_id": form["task_id"], "source_id": form["source_id"],
                                            "object_ids": [s.strip() for s in form["object_ids"].splitlines() if s.strip()]})
        raise ResearchError("FORM_ACTION_UNSUPPORTED")

    def page(self, task_id=None, result=None, error=None):
        try:
            projects = self.app.execute("find_projects", {})["projects"]
            tasks = self.app.execute("find_tasks", {})["tasks"]
            initialized = True
        except ResearchError as exc:
            if str(exc) != "LEDGER_NOT_INITIALIZED":
                raise
            projects, tasks, initialized = [], [], False
        selected = self.app.execute("resume_task", {"task_id": task_id}) if task_id else None
        task = selected["task"] if selected else None
        delivery_state = self.app.execute("get_delivery_status", {"delivery_id": task["delivery_id"]}) if task and task.get("delivery_id") else None
        links = ''.join(f'<a class="task-card {"selected" if task_id == t["task_id"] else ""}" href="/?{urlencode({"task": t["task_id"]})}"><strong>{esc(t["title"])}</strong><span>修订 {t["revision"]} · {esc(t["task_id"][-8:])}</span></a>' for t in tasks)
        banner = f'<div class="notice error" role="alert">{esc(error)}</div>' if error else ''
        if result is not None:
            banner += f'<details class="notice" open><summary>本次操作回执</summary><pre>{pretty(result)}</pre></details>'
        if not initialized:
            content = '<section class="card"><h2>开始一个属于你的研究项目</h2><p>先创建本工程的独立任务账本。此操作不启用模型，不连接外部数据库。</p>' + self.form("initialize", "", "初始化本地账本") + '</section>'
        elif not task:
            options = ''.join(f'<option value="{esc(p["project_id"])}">{esc(p["title"])}</option>' for p in projects)
            content = '<div class="grid"><section class="card"><span class="eyebrow">01 / PROJECT</span><h2>创建研究项目</h2><p>项目保持连续，执行器可以更换。</p>'
            content += self.form("create_project", field("项目名称", "title"), "创建项目") + '</section><section class="card"><span class="eyebrow">02 / TASK</span><h2>定义研究任务</h2>'
            if projects:
                fields = f'<label>所属项目<select name="project_id">{options}</select></label>' + field("任务标题", "title")
                fields += '<label>研究目标<textarea name="objective" required placeholder="希望查清什么？怎样判断完成？"></textarea></label><label>约束（每行一条）<textarea name="constraints">仅使用合成资料\n保留反例与不确定性</textarea></label>'
                content += self.form("create_task", fields, "创建任务")
            else:
                content += '<p class="muted">创建项目后，在这里开始第一个任务。</p>'
            content += '</section></div>'
        else:
            identity = f'<input type="hidden" name="task_id" value="{esc(task_id)}">'
            index_status = delivery_state["index_status"] if delivery_state else "not_requested"
            content = f'<section class="card hero"><span class="eyebrow">RESEARCH IN PROGRESS</span><h2>{esc(task["title"])}</h2><p>{esc(task["objective"])}</p><div class="pills"><span>修订 {task["revision"]}</span><span>{esc(task["task_status"])}</span><span>权威归档：{esc(task.get("archive_status", "尚无交付"))}</span><span>索引：{esc(index_status)}</span></div><p class="id">{esc(task_id)}</p></section>'
            content += '<div class="grid"><section class="card"><h3>切换并开始下一阶段</h3><p class="muted">保留同一任务。普通 AI 采用人工交接，不代表它已执行工具。</p>'
            fields = identity + f'<input type="hidden" name="base_revision" value="{task["revision"]}">'
            fields += '<label>执行器类型<select name="kind"><option value="agent">Agent · 可调用本地工具</option><option value="ai">AI · 人工交接</option></select></label>'
            fields += field("执行器名称", "name") + '<div class="grid compact"><label>版本（未知留空）<input name="version"></label><label>Provider（未知留空）<input name="provider"></label></div><label>模型（未知留空）<input name="model"></label>'
            content += self.form("begin_run", fields, "以当前修订开始") + '</section><section class="card"><h3>阶段成果交接</h3><p class="muted">提交完整交接包。READY 必须与 manifest 哈希一致。</p>'
            proposal_path = result.get("package", "") if isinstance(result, dict) and result.get("status") == "proposal_ready" else ""
            content += self.form("submit_delivery", identity + field("收件目录内包路径", "package", proposal_path), "验证并归档")
            content += '<div class="divider"></div><h3>显式处理成果</h3><p class="muted">归档与索引独立。索引仅执行 SQLite / DuckDB，保留中间产物。</p>'
            content += self.form("prepare_ingestion", identity, "准备规范交接") + self.form("index_revision", identity, "运行 SQL / DuckDB 索引") + '</section></div>'
            sources = ''.join(f'<option>{esc(s)}</option>' for s in task["source_ids"] if s in self.app.query_sources)
            content += '<section class="card"><h3>通过总控接续到 AI</h3>'
            if self.app.ai_config:
                try:
                    model_options = []
                    for provider in self.app.ai_config["providers"]:
                        catalog = self.app.execute("ai_models", {"provider": provider})
                        for model in catalog["models"]:
                            if {"chat", "text_generation"}.intersection(model["capabilities"]):
                                choice = json.dumps({"provider": provider, "model": model["model"]})
                                selected_model = self.app.last_ai_selection or {}
                                selected_attr = ' selected' if selected_model.get("provider") == provider and selected_model.get("model") == model["model"] else ''
                                model_options.append(f'<option value="{esc(choice)}"{selected_attr}>{esc(provider)} / {esc(model["model"])}</option>')
                    content += self.form("ai_start", identity + '<label>总控模型<select name="selection">' + ''.join(model_options) + '</select></label>', "选择并启动本应用 AI")
                    content += self.form("ai_pause", identity, "暂停本应用 AI")
                    content += '<p class="muted">启动仍受总控许可约束。暂停阻止后续调用，已发送的请求可能继续完成。</p>'
                    if sources:
                        fields = identity + f'<label>发送的证据来源<select name="source_id">{sources}</select></label><label>证据 ID（每行一条，最多20条）<textarea name="object_ids" required></textarea></label>'
                        content += self.form("generate_ai_proposal", fields, "发送选定证据并生成提案")
                        content += '<p class="muted">提案保留真实调用来源。审阅操作回执后，再点击“验证并归档”；失败不会自动重复调用模型。</p>'
                    else:
                        content += '<p class="muted">配置可查询证据来源后可生成阶段提案。</p>'
                except ResearchError as exc:
                    content += f'<p class="notice error">AI暂不可用：{esc(exc)}</p>'
            else:
                content += '<p class="muted">尚未配置本应用总控身份，当前可使用人工交接。配置后可选择真实模型并生成阶段提案。</p>'
            content += '</section>'
            proposal_listing = self.app.execute("list_proposals", {"task_id": task_id})
            proposals = proposal_listing["proposals"]
            if proposal_listing["unreadable_packages"]:
                content += '<details class="notice error"><summary>有交接包未通过完整性检查，原文件已保留</summary><pre>' + pretty(proposal_listing["unreadable_packages"]) + '</pre></details>'
            pending = [p for p in proposals if p["status"] != "completed"]
            if pending:
                content += '<section class="card"><h3>待审阅提案</h3><p class="muted">即使页面中断，已经生成的提案仍保存在本地。审阅后归档，无需再次调用模型。</p>'
                for proposal in pending:
                    content += f'<details><summary>基础修订 {proposal["base_revision"]} · {esc(proposal["status"])}</summary><pre>{esc(proposal["report"])}</pre></details>'
                    content += self.form("submit_delivery", identity + f'<input type="hidden" name="package" value="{esc(proposal["package"])}">', "归档此提案")
                content += '</section>'
            content += '<section class="card"><h3>证据检索</h3>'
            if sources:
                content += self.form("search_evidence", identity + f'<label>来源<select name="source_id">{sources}</select></label>' + field("检索词", "text"), "受控词项查询")
            else:
                content += '<p class="muted">此部署尚未配置可查询来源。不会连接原项目数据库。</p>'
            content += '</section><div class="grid"><section class="card"><h3>交接与证据</h3><pre>' + pretty({"constraints": task["constraints"], "handoff": task["handoff"], "coverage": task["coverage"], "evidence": task["evidence"]}) + '</pre></section><section class="card"><h3>成果与执行历史</h3>'
            for artifact in task["artifacts"]:
                content += f'<details><summary>{esc(artifact["path"])} · 生成成果</summary><pre>{esc(artifact["content"])}</pre></details>'
            content += '<pre>' + pretty({"runs": selected["runs"], "deliveries": selected["deliveries"]}) + '</pre></section></div>'
            content += '<section class="card"><h3>可携带的恢复记录</h3><p class="muted">供无工具 AI 人工交接；不包含任何客户端私有会话。</p><details><summary>展开精确恢复 JSON</summary><pre>' + pretty(selected) + '</pre></details></section>'
        return f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>研究接续 · Research Atlas</title><link rel="stylesheet" href="/style.css"></head><body><aside><a class="brand" href="/">◈ <b>研究接续</b><small>RESEARCH ATLAS</small></a><div class="sidebar-label">你的研究任务</div><nav>{links or "<p class=muted>尚无研究任务</p>"}</nav><a class="new-project" href="/">＋ 创建项目 / 任务</a><footer>本地成果 · 明确证据<br>阶段接续 · 执行器可换</footer></aside><main><header><span>WORKSPACE / LOCAL</span><span class="local-dot">本机隔离工程</span></header><div class="intro"><h1>让研究继续，<br><em>让证据留下。</em></h1><p>Agent 与 AI 可以往返切换。项目、来源与成果持续保存在这里。</p></div>{banner}{content}<p class="footnote">当前为隔离候选。机制测试与真实客户端验收分别记录；没有运行中无缝迁移承诺。</p></main></body></html>'


def serve(app, port=8793):
    workbench = Workbench(app)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, status, body, mime="text/html; charset=utf-8"):
            data = body.encode(DEFAULT_ENCODING)
            self.send_response(status)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers()
            self.wfile.write(data)

        def allowed_host(self):
            return self.headers.get("Host") == f"127.0.0.1:{self.server.server_port}"

        def redirect_receipt(self, task_id, *, result=None, error=None):
            receipt_id = secrets.token_urlsafe(16)
            workbench.receipts[receipt_id] = {"result": result, "error": error}
            if len(workbench.receipts) > 100:
                workbench.receipts.pop(next(iter(workbench.receipts)))
            query = {"receipt": receipt_id}
            if task_id:
                query["task"] = task_id
            self.send_response(303)
            self.send_header("Location", "/?" + urlencode(query))
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            if not self.allowed_host():
                return self.send(403, "HOST_REJECTED")
            route = urlsplit(self.path)
            if route.path == "/style.css":
                css = scoped_path(app.root, "ui/research_style_v0001.css").read_text(encoding=DEFAULT_ENCODING)
                return self.send(200, css, "text/css; charset=utf-8")
            if route.path != "/":
                return self.send(404, "NOT_FOUND")
            try:
                query = parse_qs(route.query)
                task_id = query.get("task", [None])[0]
                receipt = workbench.receipts.get(query.get("receipt", [None])[0], {})
                self.send(200, workbench.page(task_id, receipt.get("result"), receipt.get("error")))
            except Exception as exc:
                self.send(400, workbench.page(error=str(exc) if isinstance(exc, ResearchError) else type(exc).__name__))

        def do_POST(self):
            origin = self.headers.get("Origin")
            if not self.allowed_host() or origin not in {None, f"http://127.0.0.1:{self.server.server_port}"} or self.path != "/":
                return self.send(403, "ORIGIN_REJECTED")
            form = {}
            try:
                length = int(self.headers.get("Content-Length", "0"))
                require(0 < length <= 65536, "FORM_SIZE_LIMIT")
                values = parse_qs(self.rfile.read(length).decode(DEFAULT_ENCODING), keep_blank_values=True, max_num_fields=20)
                require(all(len(v) == 1 for v in values.values()), "DUPLICATE_FORM_FIELD")
                form = {k: v[0] for k, v in values.items()}
                result = workbench.submit(form)
                task_id = result.get("task_id", form.get("task_id"))
                # GET after mutation: refreshing a result never repeats a paid call.
                self.redirect_receipt(task_id, result=result)
            except Exception as exc:
                error = str(exc) if isinstance(exc, ResearchError) else type(exc).__name__
                self.redirect_receipt(form.get("task_id"), error=error)

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="staging/config/research_assistant_v0001.json")
    parser.add_argument("--port", type=int, default=8793)
    args = parser.parse_args()
    root = project_root()
    sys.path.insert(0, str(root))
    try:
        app = ResearchAssistant(root, read_json(scoped_path(root, args.config)))
        with serve(app, args.port) as server:
            print(json.dumps({"status": "listening", "url": f"http://127.0.0.1:{server.server_port}"}), flush=True)
            server.serve_forever()
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__}))
        return 2
    finally:
        if "app" in locals():
            app.close()


if __name__ == "__main__":
    raise SystemExit(main())
