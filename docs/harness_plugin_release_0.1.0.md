# DeepSeek Harness plugin 0.1.0

Baigong research_atlas now provides native Cordis tools for continuing the same research project across executors. The adapter calls the existing Python core and preserves task IDs, evidence, revisions and explicit stage deliveries.

- Read-only by default; writes require explicit deployment configuration.
- Verified against Harness `0.1.5-rc.3`, Cordis `4.0.2`, Node `24.18.0` on Windows.
- 27 existing Python tests passed; real Cordis/ToolRuntime and full CLI load passed; synthetic four-stage roundtrip reached revision 4.
- Local package and tarball installation, tool unload/reload and package removal were checked. No paid model calls or new dependency installation were required.
- Apache-2.0. Attribution to Ruixin Yang / David7583 and original baigong provenance are retained.

Download `dsh-research-atlas-0.1.0.tgz` from this release and install it into a prepared Harness web profile:

```text
dsh --profile research-atlas --from-default-profile web --help
dsh plugin --profile research-atlas add ./dsh-research-atlas-0.1.0.tgz
```

The bundle starts disabled. Follow [the configuration instructions](https://github.com/David7583/baigong_research_atlas/tree/main/plugins/dsh-research-atlas) to point it at a prepared Python checkout and enable it. The archive contains the adapter, not the Python application or dependencies. Existing host peers are required. No npm registry publication is claimed.

SHA-256: `0661d0cef55e6e3aa0aca24ae905bfafc3bb511d92d9de29d65d3173d4cc8d32`.

This is a community plugin, not an official DeepSeek package. Tested compatibility does not extend automatically to future developer-preview versions. The new acceptance uses real runtime code with synthetic executor labels, not new live model inference. Read [the acceptance report](https://github.com/David7583/baigong_research_atlas/blob/main/docs/harness_plugin_acceptance_20260927.md).

Rollback: disable the profile row or run `dsh plugin --profile research-atlas remove dsh-research-atlas`. Research data stays intact. Reconcile any interrupted write before retrying.

百工研究接续原生插件：默认只读，显式开启写入；沿用现有 Python 核心。完整安装、权限边界、测试范围与回退方法见仓库文档。本次仅交付 Harness 阶段，不涉及 WeKnora 或 Zenodo。
