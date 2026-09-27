# Harness 插件发布与生态接入记录

2026-09-27，Harness 阶段已完成。WeKnora 与 Zenodo 均未开展。

## 已发布产物

- [公开源码插件](https://github.com/David7583/baigong_research_atlas/tree/main/plugins/dsh-research-atlas)
- [GitHub Release：dsh-plugin-v0.1.0](https://github.com/David7583/baigong_research_atlas/releases/tag/dsh-plugin-v0.1.0)
- [可安装 tarball](https://github.com/David7583/baigong_research_atlas/releases/download/dsh-plugin-v0.1.0/dsh-research-atlas-0.1.0.tgz)
- 源码提交：`025dab67d2ccccbd35ce51bafb5abdca2fdd2f2d`
- 包 SHA-256：`0661d0cef55e6e3aa0aca24ae905bfafc3bb511d92d9de29d65d3173d4cc8d32`

公开 GitHub API 已核验 release 非草稿、附件存在、服务端摘要与本地 tarball 完全一致。包内只有插件源码、package.json、Cordis patch、README、LICENSE 和 NOTICE 六个文件，无 node_modules、测试探针、账本、运行日志或凭据。未发布 npm 包。

源码先在 staging 验收，再晋升正式插件目录。Python 核心、原工程、真实数据、已有许可证和来源追溯文件未改写。新包是可直接执行的 JavaScript/Cordis 薄插件，省去 TypeScript 编译依赖与安装钩子。

## 实际生态关联

- 仓库已设置 `dsh-plugin`、`deepseek-harness`、`research`、`agent-memory`、`baigong` topics。
- GitHub 搜索 `repo:David7583/baigong_research_atlas topic:dsh-plugin` 实际返回 **1** 个仓库。
- [社区插件 topic 页面（按最近更新排序）](https://github.com/topics/dsh-plugin?o=desc&s=updated) 的公开 HTML 已实际包含 `David7583/baigong_research_atlas`，不只是完成 topic 写入请求。
- 官方仓库 **Show Your Plugins!** 分类已发布 [Discussion #8046](https://github.com/deepseek-ai/deepseek-harness/discussions/8046)，公开访问返回 HTTP 200。

DeepSeek 官网的 Community plugins 入口直接链接上述 GitHub topic 目录，因此此次完成的是**进入官网所链接的社区发现目录并在官方仓库分享**。这不是官方背书、核心仓库合并、审核认证或独立插件商店收录。该机制无需另等维护者批准；本轮没有等待审核的 PR。目录排序和搜索结果会随新项目更新而变化。

## 本地部署与验收

已复用本机 Harness `0.1.5-rc.3` / Cordis `4.0.2` / Schemastery `3.18.2`，未安装新的第三方依赖。正式源码链接安装在工程自己的 `runtime/dsh-plugin-deployment` 下，profile 为 `research-atlas`，包含现有 host 自带的 Web surface；默认已启用查询工具，`allowWrites: false`。没有修改用户原有 DSH_HOME 或模型配置，也没有自动初始化研究账本。

本机启动与移除说明保存在 Git 忽略的 `runtime/dsh-plugin-deployment/START_HERE.md`，机器路径单独保存在 `deployment.local.json`。没有常驻验收进程；正常启动不要使用 `probe.yml`，它会在验收后退出进程。分发安装方式见[插件 README](../plugins/dsh-research-atlas/README.md)。

真实 CLI 验证了本地链接安装、tarball 安装、配置层识别、加载后调用和卸载；tarball profile 为 `research-atlas-packed`。pnpm 在不单独安装 peers 时显示 peer 提醒，但完整 host 已按自身解析规则提供匹配版本，真实启动与工具调用成功。测试包从本地文件导入，无远程依赖下载。

验收共包括 27 项既有 Python 回归、真实 Cordis/ToolRuntime 集成、四阶段接续与幂等、错误/权限/取消/卸载路径。新测试的模型调用数为 **0**。详见[验收报告](harness_plugin_acceptance_20260927.md)。未重新验证真实模型推理或 Web 页面视觉效果；其他 Harness 版本和操作系统尚未验收。

Git 推送最初遇到网络重置；只对当次命令采用已存在的系统代理后成功，没有改全局 Git 代理或 safe.directory。

## 回退

停用 profile 中的 `baigong-research-atlas` 行，或在相同 DSH_HOME 下执行 `dsh plugin --profile research-atlas remove dsh-research-atlas`。已验证卸载不删除账本或研究产物。源码回退使用 Git 对插件交付提交作可审查的 revert；发布前基线为 `b671cdb9aee763b6ed13154108ce72249a09d698`。若写入途中终止，先核对任务/交付状态，不自动重试，也不把终止进程当作业务回滚。
