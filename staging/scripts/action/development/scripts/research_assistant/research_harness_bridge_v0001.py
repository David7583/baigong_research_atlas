# ============================================================
# 文件名: research_harness_bridge_v0001.py
# 中文名: Harness 本机总控协议适配
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / adapter
# 脚本定位: Harness SSE 客户端到既有同步总控句柄的适配器
#
# 职责说明:
# - 将获准的本机请求交给自身应用句柄，不实现模型连接或工具执行
#
# 本脚本做什么:
# - 检查本机身份、选择和请求上限，将完整响应转为 SSE 帧
#
# 本脚本不做什么:
# - 不读取 Provider 密钥、不提权、不重试、不声称实时流式推理
#
# 制度边界声明:
# - 仅绑定回环地址；审计保留调用来源，不记录认证头或隐藏推理
# - 同步请求在完成后发送正文和工具调用；中断不声称已取消上游
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_harness_bridge_v0001
# family: research_harness_bridge
# role: buffered_controller_protocol_adapter
# version: v0001
# status: archived
# entry_point: staging/scripts/action/development/scripts/research_assistant/research_harness_bridge_v0001.py
# input:
#   - own application client, expected model, local token and request budget
# output:
#   - buffered SSE response and safe call provenance
# depends_on:
#   - action_ai_controller_v0002
#   - Python stdlib
# used_by:
#   - run_research_harness_v0001
# ============================================================

from __future__ import annotations

import json
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_harness_bridge"
SCRIPT_NAME = "research_harness_bridge_v0001"
SCRIPT_VERSION = "v0001"
ALLOWED_FIELDS = {"messages", "tools", "tool_choice", "parallel_tool_calls", "thinking", "reasoning_effort",
                  "max_tokens", "temperature", "top_p", "stop", "response_format", "presence_penalty", "frequency_penalty"}


# ============================================================
# 核心业务组件
# ============================================================

class HarnessBridge:
    def __init__(self, client, model, *, request_limit=8, max_tokens=4096):
        self.client, self.model = client, model
        self.request_limit, self.max_tokens = request_limit, max_tokens
        self.token = secrets.token_urlsafe(32)
        self.audit = []
        self.attempts = 0
        self.lock = threading.Lock()

    def invoke(self, body):
        if not isinstance(body, dict) or body.get("model") != self.model:
            raise ValueError("MODEL_SELECTION_MISMATCH")
        if set(body) - ALLOWED_FIELDS - {"model", "stream", "stream_options"}:
            raise ValueError("REQUEST_FIELD_UNSUPPORTED")
        payload = {k: v for k, v in body.items() if k in ALLOWED_FIELDS}
        budget = payload.get("max_tokens", self.max_tokens)
        if type(budget) is not int or not 1 <= budget <= self.max_tokens:
            raise ValueError("OUTPUT_BUDGET_EXCEEDED")
        payload["max_tokens"] = budget
        # Bound external-agent loops independently of upstream retry policy.
        with self.lock:
            if self.attempts >= self.request_limit:
                raise ValueError("REQUEST_BUDGET_EXCEEDED")
            self.attempts += 1
        result = self.client.invoke("chat", payload)
        data = result["data"]
        choices = data.get("choices", [])
        if result.get("status") != "completed" or len(choices) != 1:
            raise ValueError("UPSTREAM_RESPONSE_INVALID")
        choice = choices[0]
        message = choice["message"]
        delta = {k: message[k] for k in ("role", "content") if message.get(k) is not None}
        calls = message.get("tool_calls", [])
        if calls:
            delta["tool_calls"] = [{"index": i, **call} for i, call in enumerate(calls)]
        self.audit.append({"call_id": result.get("call_id"), "app_id": result.get("app_id"),
                           "provider": result.get("provider"), "model": result.get("model"),
                           "usage": result.get("usage"), "finish_reason": choice.get("finish_reason"),
                           "tool_names": [c.get("function", {}).get("name") for c in calls],
                           "transport": "buffered_sync_response_as_sse"})
        common = {"id": data.get("id", result["call_id"]), "object": "chat.completion.chunk",
                  "created": data.get("created", 0), "model": result["model"]}
        frames = [{**common, "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                  {**common, "choices": [{"index": 0, "delta": {}, "finish_reason": choice.get("finish_reason")}],
                   "usage": result.get("usage", {})}]
        return "".join("data: " + json.dumps(f, ensure_ascii=False) + "\n\n" for f in frames) + "data: [DONE]\n\n"

    def server(self):
        bridge = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                authorized = secrets.compare_digest(self.headers.get("Authorization", ""), "Bearer " + bridge.token)
                if not authorized or self.headers.get("Host") != f"127.0.0.1:{self.server.server_port}" or self.path != "/v1/chat/completions":
                    self.send_error(403)
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= 1048576:
                        raise ValueError("REQUEST_SIZE_LIMIT")
                    body = json.loads(self.rfile.read(length))
                    response = bridge.invoke(body).encode(DEFAULT_ENCODING)
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                except Exception as exc:
                    # Only stable codes; provider responses and authentication stay private.
                    code = str(exc) if type(exc).__name__ in {"ValueError", "ControlError", "PermissionDenied", "DataError", "ConfigError", "BusyError"} else type(exc).__name__
                    bridge.audit.append({"status": "failed", "error_type": type(exc).__name__, "code": code})
                    response = json.dumps({"error": {"message": code, "type": "controller_bridge_error"}}).encode(DEFAULT_ENCODING)
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                try:
                    self.wfile.write(response)
                except (BrokenPipeError, ConnectionResetError):
                    bridge.audit.append({"status": "client_disconnected", "upstream_cancellation_confirmed": False})

        return ThreadingHTTPServer(("127.0.0.1", 0), Handler)
