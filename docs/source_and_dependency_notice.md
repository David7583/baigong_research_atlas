# 来源与依赖说明

本项目按用户选择使用 Apache-2.0，全文见根目录 LICENSE；保留上游归属的 NOTICE 同时分发。

研究助手业务、界面和 Skill 为本项目新增；Data–Action–Data 模块来自用户本机 baigong 源码快照。provenance/source_manifest.json 保存原始来源和哈希，不能自动等同于远端发布版本。关联上游：https://github.com/David7583/baigong 。上游 NOTICE 为 Baigong (百工)，Copyright 2026 Ruixin Yang；完整内容保留在本项目 NOTICE。

复制主链的工作区参数扩展见 provenance/local_changes.json，修改文件内保留显著说明；原始源码未覆盖。复制配置的变更与哈希在来源清单中记录。历史候选与正式实现的对应关系见 provenance/promotion_equivalence.json。

requirements.txt 列出 PyYAML、httpx、jsonschema、DuckDB。发布源码不捆绑第三方安装文件、模型权重或运行数据库；这些依赖仍受其各自许可证约束。如分发二进制安装包，需要另外收集对应版本及传递依赖的授权文本。

README 图片由仓库所有者提供，原字节复制，哈希见 provenance/branding.json。
