# ============================================================
# 文件名: action_ai_transport_v0001.py
# 中文名: 行动端 AI HTTP 执行器
# 版本号: v0001
#
# 主层级: action
# 层级: infrastructures / ai_transport
# 脚本定位: AI 总控内部的有界 HTTP JSON 传输组件
#
# 职责说明:
# - 执行已经过总控授权的请求，返回真实响应及分类错误
#
# 本脚本做什么:
# - 限制响应大小、连接数、超时，禁用重定向和自动重试
# - 继承 action_llm_caller_v0001.py 的执行职责，独立家族从 v0001 开始
#
# 本脚本不做什么:
# - 不读取密钥配置、不选择模型、不判断应用权限、不执行模型返回的工具
#
# 制度边界声明:
# - Provider、模型、Prompt、认证头由总控传入；不记录正文和密钥
# - 无持久化写入；传输失败不泄露外部错误正文，不伪装成功
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: action_ai_transport_v0001
# family: action_ai_transport
# role: ai_http_executor
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/action_ai_transport_v0001.py
# input:
#   - authorized HTTP request plan
# output:
#   - provider JSON response or classified transport error
# depends_on:
#   - Python stdlib
#   - httpx
# used_by:
#   - action_ai_controller_v0001
# ============================================================

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import httpx


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "action_ai_transport"
SCRIPT_NAME = "action_ai_transport_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 异常类型
# ============================================================

class TransportError(RuntimeError):
    def __init__(self, code: str, http_status: int | None = None):
        super().__init__(code)
        self.code = code
        self.http_status = http_status


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class RequestPlan:
    url: str
    headers: dict[str, str] = field(repr=False)
    payload: dict[str, Any] = field(repr=False)
    connect_timeout: float
    read_timeout: float
    total_timeout: float
    max_response_bytes: int
    method: str = "POST"


# ============================================================
# 核心类
# ============================================================

class JsonTransport:
    """Thread-safe pooled client; controller owns lifetime and admission limits."""

    def __init__(self, max_connections: int):
        self._client = httpx.Client(
            trust_env=False, follow_redirects=False,
            limits=httpx.Limits(max_connections=max_connections,
                               max_keepalive_connections=max_connections),
        )

    def close(self) -> None:
        self._client.close()

    def execute(self, plan: RequestPlan) -> dict[str, Any]:
        started = time.monotonic()
        timeout = httpx.Timeout(connect=plan.connect_timeout, read=plan.read_timeout,
                                write=plan.connect_timeout, pool=plan.connect_timeout)
        try:
            with self._client.stream(plan.method, plan.url, headers=plan.headers,
                                     json=plan.payload if plan.method == "POST" else None,
                                     timeout=timeout) as response:
                if not 200 <= response.status_code < 300:
                    raise TransportError("provider_http_error", response.status_code)
                chunks = bytearray()
                for chunk in response.iter_bytes(chunk_size=4096):
                    if time.monotonic() - started > plan.total_timeout:
                        raise TransportError("request_deadline_exceeded")
                    chunks.extend(chunk)
                    if len(chunks) > plan.max_response_bytes:
                        raise TransportError("response_too_large")
                try:
                    result = json.loads(chunks)
                except (ValueError, UnicodeError):
                    raise TransportError("invalid_provider_json") from None
                if not isinstance(result, dict) or "error" in result:
                    raise TransportError("invalid_provider_result")
                return result
        except TransportError:
            raise
        except httpx.TimeoutException:
            raise TransportError("provider_timeout") from None
        except httpx.HTTPError:
            raise TransportError("provider_connection_error") from None


# ============================================================
# CLI / main 接口区
# ============================================================

def main() -> int:
    print(json.dumps({"status": "error", "error_type": "InternalComponent",
                      "detail": "Use the AI controller; no standalone execution."}))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
