# 部署与运行数据

业务入口在 `scripts/action/development/scripts/research_assistant/`，薄入口在 `scripts/action/skill/research_continuity/research_continuity_v0001.py`，默认配置在 `config/research_assistant_v0001.json`。staging 副本已标记 archived，仅供回退对照，正式代码不导入它们。命令中的 Python 应替换为部署环境实际解释器。

## 环境

本机验证：Windows、Python 3.12.7。账本和界面主要使用标准库；总控需要 PyYAML/httpx，规范摄取需要 jsonschema，SQL/DuckDB 索引需要 DuckDB。已测环境版本分别为 6.0.3、0.28.1、4.26.0、1.5.5。复制的外围能力还包含可选依赖；有文件不代表该功能已经完成部署。

不自动安装依赖，不复制源工程 `.venv`、模型权重、登录凭据或运行库。新环境可自行创建虚拟环境后执行 `python -m pip install -r requirements.txt`；本轮没有执行安装。Chroma、Neo4j、sentence-transformers 不属于当前已验收的研究助手索引路径；不应为启动研究账本而默认安装它们。独立路径测试复用已有解释器，不等于干净机器安装验收。

## 非 AI 首次启动

按 README 的显式 initialize 和 UI 命令运行。`describe` 可在未初始化账本时查看接口。CLI 的 `--input` 是本工程内 JSON 请求文件；所有相对路径按工程根解析，不依赖终端当前目录。不要将 `source_ids` 中的名称当作已经存在的数据库连接。

配置真实资料时，为每个来源填写 `query_sources` 的路径、允许表/列、语义说明和 evidence_mapping；创建任务时仅授权所需 source_ids。默认样例没有资料连接，检索不可用时应准确报错。

## 接入 AI

管理员为部署准备总控 catalog、Provider 凭据引用与独立应用身份，再单独授予总许可和应用许可。应用普通启动不自动注册或授权。Provider Key 与应用身份 Token 必须分离，均不能进入共享配置、Prompt、报告或 Git。

应用配置可增加：

```json
{
  "ai": {
    "controller_config": "config/action/ai/action_ai_controller_config_v0002.yml",
    "app_id": "research_assistant",
    "token_environment": "RESEARCH_ASSISTANT_TOKEN",
    "providers": ["your_provider_id"]
  }
}
```

将该 ai 对象合并到完整应用配置，不替换 schema_version、ledger、inbox 等必需字段。Provider 名称应与管理员维护的总控目录一致。以上不是可直接运行的凭据配置。当前工程不携带实际 Provider 目录和密钥，不能承诺下载即联网可用。

无凭据模板位于 `config/examples/ai_catalog.example.yml` 和 `credential_refs.example.yml`。管理员按实际服务填写 catalog 的地址与模型，凭据文件只保留 environment/environment_variable 引用，并配置总控 sources 指向这些本地文件。样例域名 example.invalid 故意不可用，不会自动访问真实厂商。

AI 选择/启动与暂停只控制本应用。暂停阻止下一请求，不取消已发送请求。关闭界面服务后，管理员仍应根据部署策略处理长期许可；测试运行器在 finally 中关闭自己的隔离许可。

## 持久化、备份和升级

- `runtime/research/`：默认任务账本及收件交接包。
- `runtime/research_ingestion/`：规范输入；`runtime/research_canonical/`：每份交付的短路径规范工作区。
- `actioning/ingestion/research/`：原件快照和接入记录。
- `runtime/i/`：按项目隔离的 SQLite/DuckDB、写入配置、主链回执和保留记录。
- `data_processed/research/`：每份交付独立的准入目录，避免连续修订复用 v0001 路径。
- `actioning/registry/research/`、`actioning/pipelines/research/`、`scripts/orchestration/outputs/action/`：主链要求的持久记录和派生产物；也应纳入备份。
- 总控状态目录以 controller config 的 state_path 为准；默认位于 `data_runtime/action/ai_controller/`。

这些目录被 Git 忽略，但不是可随意清理的缓存。停止相关进程后备份整个运行目录、总控状态以及本地配置；运行中备份 SQLite 应使用其备份接口，不能只复制主文件而遗漏 WAL。

正式路径已按各子链契约对齐，不再使用 test-mode。复制的 v0019 增加两个可选工作区参数，以避免 Windows 长路径，并保留结构子链自己的正式输出边界；未改动主项目源码、目录保护或旧参数默认行为。变更哈希见 `provenance/local_changes.json`。历史 temp 验收数据原地保留，不自动搬迁或重建。

回退时保留现有账本、交接包、原件与失败记录；恢复匹配版本的代码和配置，不清空数据库。新路径与旧测试结果的对应关系保存在测试回执中。

## 登记与发布

本地晋升与独立工程开发登记已完成，详见晋升报告。普通 UI 启动不扫描或登记。发布源码不携带本机 sql/action.db；维护者需要登记时，先核对新环境中该库是否已存在，再按开发登记规范操作，不能盲目运行会归档既有库的 init_action_db。

用户已确认公开仓库 baigong_research_atlas、Apache-2.0 与 README/NOTICE 上游关联；发布操作与 Zenodo 状态见 publication_scope.md 和 publication_result.md。完整真实资料质量、所有厂商原生协议及全客户端兼容不属于现有验收结论。
