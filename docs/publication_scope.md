# 发布边界与来源

发布来源为当前独立 Git 工程。尚未设置 remote、推送、创建公开仓库或 Release。

已核实 [David7583/baigong](https://github.com/David7583/baigong) 是公开的本地 Data–Action–Data 项目，仓库声明 Apache-2.0。研究助手复制的是本机当前源码，不把它自动等同于远端 v0009 发布快照。复制来源及哈希见 provenance/source_manifest.json，修改过的配置另记复制后哈希。

建议关联方式：独立研究助手仓库的 README 链接 baigong，并列出复用代码来源和版本。是否修改 baigong 索引、是否作为其子项目以及新仓库名称、公开性、许可，均在可审查包完成后由用户确认。

不得直接将上游许可证声明扩展为用户尚未选择的新代码许可证。当前不放置臆定版权人或未获确认的新项目授权声明；正式发布前补齐 LICENSE/NOTICE 与第三方依赖归属。

发布清单需排除运行目录、真实数据、SQLite/DuckDB、密钥、权限 Token、.venv、npm 缓存、模型权重、私有会话及大体积中间产物。源文件完整性和真实执行验收是两个不同门槛。
