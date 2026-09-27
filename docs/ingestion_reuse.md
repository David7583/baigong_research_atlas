# Ingestion 复用核对

2026-09-27，根据用户要求检查主项目 `scripts/action/tools/ingestion` 并复制全部 7 个 Python 文件。源码逐字节复制，来源 SHA-256 记录在 `provenance/source_manifest.json`。未复制业务数据、运行状态、凭证或数据库。

| 文件 | 研究助手中的用途 |
|---|---|
| ingestion_store_v0001 | 原件快照、来源哈希、追加登记和运行事件 |
| ingestion_extract_v0001 | 有界 UTF-8 文本及 JSON 提取；不支持的格式明确标记 |
| ingestion_quality_v0001 | 正文质量与来源覆盖检查 |
| ingestion_handoff_v0002 | 将合规输入交给规范入口，声明主链 v0019 |
| ingestion_receipt_v0002 | 核验主链真实提交回执，不以受理冒充入库完成 |
| ingestion_handoff_v0001 / ingestion_receipt_v0001 | 随既有 v0001 编排保留的兼容依赖；新研究助手目标链使用 v0002 |

同时复制统一接入结果 Schema 和四份规范入口资源；隔离接入配置使用 `temp/ingestion_runtime`，不沿用原项目运行目录。

以上记录初始隔离验收布局。晋升后的研究助手使用 `actioning/ingestion/research`、`runtime` 及主链各自允许的正式派生目录；复制的旧配置不是研究助手真实入口的运行配置。当前路径和连续修订验收见 `deployment.md` 与 `promotion_acceptance_20260927.md`。

边界：Markdown 可保存及提取，但 handoff 返回 awaiting_core_contract。需要显式转换为 canonical_conversation_ingress_v0001 才能进入现有规范入口；转换必须保留生成来源、原报告和证据引用。原件登记、正文准备、主链交接、核心写入和索引就绪分别报告。

已完成研究成果适配：`research_ingestion_v0001` 将生成报告与来源转换为规范输入，`research_index_v0001` 显式调用真实 v0019 主链，并用 `ingestion_receipt_v0002` 核对回执。隔离测试已通过 Markdown 原件保存、规范交接、SQLite/DuckDB实际写入、重复调用及直接主链/适配链内容对比。PDF、DOCX 等仍返回 awaiting_adapter_validation，不能把它们列为已验收解析能力。Chroma/Neo4j 未在本次索引路径启用。
