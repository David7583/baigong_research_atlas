# 研究接续 · Research Atlas

在同一个项目和任务上切换 Agent 或普通 AI，保留证据、阶段成果、未决问题和可校验的恢复记录。项目身份独立于客户端、Provider 和模型。

当前研究助手入口已通过本地 **Active 晋升验收**，尚未对外发布。本机已实测 Harness 文件工具接续、当前 Codex Agent 恢复，以及经 Action 总控调用 DeepSeek/Kimi；不表示只支持这些产品，也不表示所有客户端均已验收。

## 本地启动

使用自己的 Python 环境，不复制别人的 `.venv`。当前实测 Python 为 3.12.7；已安装环境无需重复安装依赖。依赖和可选能力见 [部署说明](docs/deployment.md)。

从本工程根目录执行：

```text
python -B scripts/action/skill/research_continuity/research_continuity_v0001.py initialize
python -B scripts/action/skill/research_continuity/research_continuity_v0001.py ui --port 8793
```

打开 `http://127.0.0.1:8793`。默认配置只提供本地项目/任务与交接，不配置或开启 AI，不包含真实资料源。首次初始化会创建新账本；已有账本不会被清空。界面仅监听本机回环。

离线回归（不调用模型）：

```text
python -B -m unittest discover -s tests -p "test_research*.py"
```

## 接入其他 Agent 与 API

- Agent 可以调用 CLI、读取 `resume_task`、提交标准交接包；客户端名称没有白名单限制。
- 无工具 AI 使用人工交接。模型生成的文本不代表它已经写文件或执行工具。
- API 的 Provider、模型、地址和认证由总控配置；当前自动提案使用 `chat` 操作。兼容协议可配置接入，其他原生协议需要适配和独立验收。
- Harness 是可选客户端适配器，不是研究账本、UI 或其他 Agent 的必需运行组件。

详情：[兼容边界](docs/provider_and_agent_compatibility.md)、[Skill](skill/action/research-continuity/SKILL.md)、[集成验收](docs/integration_test_report_20260927.md)、[晋升报告](docs/promotion_acceptance_20260927.md)。

## 来源与发布

复用本机 Data–Action–Data 实现；关联项目为 [baigong](https://github.com/David7583/baigong)。复制文件和哈希见 [来源清单](provenance/source_manifest.json)。本工程尚未发布，许可和关联形式待发布评审确认；不能把上游声明自动视作全部新增代码的授权。见 [发布边界](docs/publication_scope.md)。
