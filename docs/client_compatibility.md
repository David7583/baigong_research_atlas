# 执行器兼容核对（进行中）

日期：2026-09-27。用户确认当前 OpenAI.Codex 应用包中的 ChatGPT.exe 就是本次所指客户端。

| 对象 | 实测或来源 | 尚未验证 |
|---|---|---|
| DeepSeek Harness | 本机 @deepseek-ai/dsh 0.1.5-rc.3；headless 新会话经本机适配层和独立总控实际完成read/write工具交接；当前Agent恢复归档至修订3 | 未测试web界面的同等路径、完整shell/网络工具或新建独立Codex会话 |
| Harness 接口 | 本机随包 README 与帮助列出 headless、SDK JSON-RPC、ACP stdio；工作目录默认为 workspace | 不据此声称所有接口已经接入本研究助手 |
| 用户所指 ChatGPT | 可读进程路径归属 OpenAI.Codex_26.917.9434.0；用户已确认目标 | exe 的 153.0.8010.53 是文件产品版本，不能冒充独立 ChatGPT 业务版本；尚未完成客户端交接 |
| 普通 AI | 已通过独立 Action 总控真实调用 deepseek-v4-flash、kimi-k3，接续同一项目至修订3；当前 Codex Agent 读取结果后归档修订4 | 仅验收两条合成观察及一条生成假设，不代表通用研究质量或 Harness 客户端验收 |

本次真实客户端路径是 Harness headless 文件工具及当前 Codex Agent 本地恢复；浏览器自动化用于研究助手自己的UI。现有模型 API、CLI或当前会话不能自行证明所有客户端和全新会话都已验收。

官方参考：[DeepSeek Harness 官方仓库](https://github.com/deepseek-ai/deepseek-harness)；[OpenAI 插件连接与测试](https://developers.openai.com/plugins/deploy/connect-chatgpt)。官方支持某协议与本机版本实际验收分开记录。

## 人工参与验收流程

1. 在独立工程中创建合成研究任务，保存任务 ID，使用已测试工具生成交接包。
2. 在 Harness 新会话中只提供自有 Skill、独立工程位置和任务 ID，不提供其他客户端历史。允许其使用工程 CLI；先核对模型调用范围及费用授权。
3. 要求查询 20°C 的 A/B 样本，再查 40°C 反例。正确结果必须保留条件差异，不得把 assistant 假设当用户证据。
4. 提交阶段成果，核查 revision、run executor 和实际工具回执。
5. 在用户确认的 ChatGPT/Codex 客户端开启独立会话，提供精确恢复输出和合成资料。若没有可用工具，就按 manual_exchange 传递；人工执行返回的结构化交付，不能声称客户端调用了工具。
6. 依次执行 A→B→A、Agent→AI→Agent、AI→AI→AI；每次保留同一 project_id/task_id，增加 revision，比较约束、证据、下一步和反例。
7. 保存两端真实版本、可获取的模型名、原始工具回执、人工操作清单和逐项判定。未知信息留空。

只完成离线机制测试时，上述验收状态保持未通过。没有复制登录凭证、真实聊天记录或客户端私有会话。

## 2026-09-27 真实模型补测

用户明确允许真实模型调用后，使用源总控配置中的 Provider 与凭据进行显式管理员测试引导：凭据仅传入当前进程环境，独立工程保留环境变量引用；应用身份和总控运行库重新创建，未复用原总控运行状态。

成功运行目录为 `runtime/live/6026aff1a0b9/`（Git 忽略），实际顺序为当前 Agent 撰写阶段1 → DeepSeek API 阶段2 → Kimi API 阶段3 → 当前 Agent 阅读本地结果并归档阶段4。项目、任务身份和研究约束不变。成功两次请求合计6237 Token；实际耗时见总控回执（约6.58秒、47.48秒）。输出均保留40°C反例，明确生成假设不能替代原始观察。

失败记录保留：最初网络受限；1200 Token 输出预算时 DeepSeek 返回无可见正文；提高至4096后成功；配置里的 kimi-latest 返回HTTP404，改用配置已有的 kimi-k3 后成功。这些失败不算接续通过。早期有实际Token用量的失败/重测也可能计费，不包含在上述成功轮次6237 Token中。

测试结束关闭独立总控总许可与应用许可。普通应用代码不负责注册或提权。当时17项回归通过（33.619秒）。后续客户端/UI集成见 `integration_test_report_20260927.md`；模型API测试、客户端Agent测试和正式晋升仍分别记录。
