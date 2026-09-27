# 发布边界与来源

2026-09-27：用户确定公开仓库名 baigong_research_atlas，采用 Apache-2.0，关联 baigong，并授权 GitHub 与 Zenodo 发布。

发布对象仅为本独立 Git 工程的受审查源码。仓库目标：https://github.com/David7583/baigong_research_atlas 。实际远端发布结果记录在 publication_result.md，不能将准备完成当成已上线。

关联方式为 README 上游链接、NOTICE 归属及 provenance/source_manifest.json 逐文件溯源。复制的是本机快照，不声称等同于远端发行版本；不修改 baigong 上游仓库。

不发布 runtime、真实资料、数据库、模型输出、凭据、权限 Token、虚拟环境、模型权重和私有会话。

## Zenodo

用户随后明确暂缓 Zenodo，本次不连接、不创建归档或 DOI。下列仅为未来可选流程。

采用 GitHub Release 归档流程：登录 Zenodo，在 GitHub 集成中启用本仓库，然后创建发行版本，核对归档文件和引用元数据，再将真实 DOI 回填 CITATION.cff 与 README。若首次授权涉及新增访问权限，由用户完成授权。未取得 DOI 时不填造假的占位 DOI，也不复用 baigong 的 DOI。

引用作者沿用上游公开 CITATION.cff 的 Ruixin Yang（David7583）。CITATION.cff 为软件元数据来源，不另外维护一份可能冲突的 .zenodo.json。

官方说明：https://help.zenodo.org/docs/github/
