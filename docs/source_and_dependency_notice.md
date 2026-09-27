# 来源与依赖说明（发布前核对稿）

研究助手的新业务、界面和Skill由本项目开发；复用的Data–Action–Data模块来自用户本机源码。`provenance/source_manifest.json`逐文件记录来源和复制时哈希，不能将它们自动等同于某个远端发布版本。

关联上游为README所链接的baigong。此前核对其公开仓库声明为Apache-2.0，但本机快照与新增代码的最终许可证、版权标注和必要NOTICE仍须在发布前确认。本文是溯源记录，不是自行授予许可证，也不替代正式LICENSE。

直接依赖由requirements.txt列出：PyYAML、httpx、jsonschema、DuckDB。源码包不包含这些第三方包的安装文件、模型权重或运行数据库；安装者通过自己的环境获取它们。若将来分发包含依赖的二进制安装包，应另行收集对应版本及其传递依赖的许可证文本。

本地对复制主链的唯一行为扩展记录在 `provenance/local_changes.json`；原始副本的SHA-256仍保留，主项目原文件未修改。历史候选和正式实现的对应关系记录在 `promotion_equivalence.json`。
