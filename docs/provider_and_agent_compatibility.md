# Provider 与 Agent 兼容边界

2026-09-27，基于本地当前代码核对，不是各家服务的通用兼容承诺。

| 层 | 当前行为 | 更换方式 |
|---|---|---|
| 项目/任务账本 | 身份独立于执行器；名称、版本、Provider、模型保存为来源字段 | 保留同一 project_id/task_id，按当前修订开始新 run |
| Agent | 无 Codex/Harness 名称白名单 | 调用 CLI 或编写标准交接包；专用客户端协议需单独适配 |
| 普通聊天 AI | 支持人工传递恢复记录和交接包 | 使用 manual_exchange，宿主校验归档，不冒充自动工具调用 |
| 自动 AI 提案 | 经自身 Action 总控身份调用 `chat`，发送 messages/max_tokens，要求可见 JSON 文本 | 总控 catalog 配置 Provider/profile/model，应用 providers 配置控制界面列表 |
| OpenAI 兼容 chat | 业务没有 DeepSeek/Kimi 分支；总控处理地址和认证 | 配置已支持的协议并做小范围验收，不能只凭“兼容”标签保证所有参数支持 |
| Ollama chat | 总控有原生 chat 路由，业务可读取 message.content | 配置本地服务与模型，尚未进行本应用真实服务验收 |
| Anthropic/Gemini/Responses 等原生协议 | 总控可能支持对应操作，但研究提案当前固定调用 chat | 需要将业务输入映射到 messages/generate_content/responses 等操作并统一可见输出；当前不能仅改名字就使用 |
| Harness 桥接 | 独立可选的本机 HTTP 适配器，实测文件工具 | 其他客户端只有满足请求字段、认证及工具协议契约时才可复用，否则新增薄适配 |

已通过的无网络机制证据：`test_research_integration_v0001.py` 使用自定义 `fixture` Provider、`model-a/model-b` 和模拟传输，经过真实总控许可逻辑生成并归档；`test_research_ledger_v0001.py` 的往返序列使用任意 A/B/C/D 名称。它们证明名称与项目身份解耦，不证明某个第三方服务的可用性。

所有应用发起的真实模型请求继续经过 Action 总控。拒绝后不直连、不静默换 Provider，不改变其他应用许可。专用客户端自行发起的推理不能自动宣称受本总控审计。

费用控制：复用本机已保存的真实调用证据，普通回归使用模拟传输；真实验收脚本必须显式 `--confirm-live`。提案生成和归档分离，归档/刷新不重新调用模型，失败不自动重试。当前没有完整的跨任务费用预算系统；输出上限不是费用上限，历史输入和厂商实际 usage 仍需计入。
