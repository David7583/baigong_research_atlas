<p align="center"><img src="assets/baigong-research-atlas.png" alt="Baigong research_atlas" width="280"></p>

# 百工·research_atlas

[English](Baigong·research_atlas.md) · [baigong](https://github.com/David7583/baigong)

在同一个项目和任务上切换 Agent 或普通 AI，保留证据、阶段成果、未决问题和可校验的恢复记录。项目身份独立于客户端、Provider 和模型。支持阶段边界的 Agent↔Agent、Agent↔AI、AI↔AI 接续。

## 安装与启动

下载本仓库源码并解压，或使用 Git 克隆，然后在仓库根目录打开终端。使用自己的 Python 环境；当前实测 Windows / Python 3.12.7，独立目录验收复用了已有解释器，尚未完成全新机器安装验收。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -B scripts/action/skill/research_continuity/research_continuity_v0001.py initialize
.\.venv\Scripts\python.exe -B scripts/action/skill/research_continuity/research_continuity_v0001.py ui --port 8793
```

打开 http://127.0.0.1:8793 。initialize 创建本地账本，已有账本不会被清空。已安装依赖无需重复安装。默认不连接真实资料、不启用 AI，也不需要 Harness。

## 如何使用

1. 在本地界面创建项目与任务，记录目标、约束和允许使用的资料来源。
2. 开始执行前读取任务的当前修订和交接信息；保持同一 project_id / task_id。
3. Agent 使用下方 Skill 和 CLI 执行；没有工具的普通 AI 使用人工交接，不能把其文本当作已执行操作。
4. 阶段结束后提交含成果、证据、失败项、未决问题和下一步的交接包。归档形成新修订；更换执行器后恢复同一任务。
5. 需要进入检索数据层时，显式执行 prepare_ingestion 和 index_revision。归档成功与索引成功分别记录，目前索引验收范围为 SQLite / DuckDB。

Agent 接入说明见 [research-continuity Skill](skill/action/research-continuity/SKILL.md)。交接包包含 manifest.json、UTF-8 成果文件以及最后写入的 READY 校验文件。不要覆盖历史修订；遇到冲突先读取新修订，未知执行结果先核对状态，不盲目重试。

CLI 示例（request.json 保存于 runtime 内，内容为真实任务 ID）：

```json
{"task_id": "replace-with-existing-task-id"}
```

```text
python -B scripts/action/skill/research_continuity/research_continuity_v0001.py describe
python -B scripts/action/skill/research_continuity/research_continuity_v0001.py --input runtime/request.json resume_task
```

## 配置 AI 和资料源

本地 API 请求统一经过 Action AI Controller。管理员配置 Provider 地址、模型和凭据引用，注册独立应用身份，并授予总许可与应用许可。Provider Key 与应用 Token 分离；界面启动不会自动授权。按 [部署说明](docs/deployment.md) 合并 ai 配置，参考 config/examples 中的无密钥模板，使用环境变量提供凭据。模板中的 example.invalid 不能用于真实调用。

Provider 和 Agent 名称没有限定为 DeepSeek、Kimi、Codex 或 Harness。当前自动提案使用 chat 接口；兼容协议可配置，其他原生协议需要适配和验收。Harness 是可选桥接器。已实测范围及限制见 [兼容说明](docs/provider_and_agent_compatibility.md)。AI 提案需显式归档，不会自动成为可信证据；不自动重试付费请求。

资料检索默认无连接。配置 query_sources 的路径、允许表列、语义及 evidence_mapping，并在任务中授权 source_ids。当前受控检索为词项/精确 ID 查询，不能宣称已经实现所有语义检索能力。

## 备份、测试与回退

停止相关进程后备份运行目录、总控状态和本地配置；它们被 Git 忽略，但不是临时缓存。运行中 SQLite 使用备份接口以保留 WAL 一致性。详细路径见 [部署与备份](docs/deployment.md)。回退恢复匹配版本代码与配置，保留账本、原件和失败记录。

离线测试（不调用模型）：

```text
python -B -m unittest discover -s tests -p "test_research*.py"
```

[晋升验收](docs/promotion_acceptance_20260927.md)记录了已有测试；不将机制测试等同于所有客户端的实测。

## DeepSeek Harness 插件

原生 DeepSeek Harness 插件已提供：见 [插件安装与操作契约](plugins/dsh-research-atlas/README.md) 和 [验收记录](docs/harness_plugin_acceptance_20260927.md)。默认只读，显式配置后才开放写入；兼容性限定为已验收的 Harness `0.1.5-rc.3`。它通过 Cordis 工具调用现有 Python 核心，区别于已有模型 HTTP 桥接。

## 来源、许可与引用

本项目关联 [baigong](https://github.com/David7583/baigong)，复用其本机 Data–Action–Data 源码。逐文件来源与哈希见 [source_manifest.json](provenance/source_manifest.json)，不自动等同于上游某个发行版本。

使用 [Apache-2.0](LICENSE)；归属与修改说明见 [NOTICE](NOTICE) 和 [来源说明](docs/source_and_dependency_notice.md)。引用元数据见 [CITATION.cff](CITATION.cff)。Zenodo 归档步骤与实际状态见 [发布记录](docs/publication_scope.md)；仅在真实 DOI 生成后填入，不使用占位 DOI。
