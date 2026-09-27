# Baigong Research Atlas — DeepSeek Harness plugin

[中文项目说明](../../百工·research_atlas.md) · [English project overview](../../Baigong·research_atlas.md) · [Source](https://github.com/David7583/baigong_research_atlas)

Native Cordis tools for persistent research tasks, evidence and stage handoffs across executors. This is a community plugin, not a DeepSeek first-party package. It calls the existing Python CLI; it does not duplicate the research core or use the model HTTP bridge.

Validated host: `@deepseek-ai/dsh` / `dsh-tools` **0.1.5-rc.3**, Cordis **4.0.2**, Schemastery **3.18.2**, Node **24.18.0**, Windows. Other Harness versions and operating systems are unverified. Harness is a developer preview: pin the tested host and re-run the tests before upgrading.

## Install

Prerequisites: a trusted checkout of baigong_research_atlas with its Python environment prepared according to the project instructions, and the tested Harness host already installed. The plugin contains ready-to-run JavaScript, with no build or installation scripts. It uses the host's declared peers; no separate model SDK is needed.

From the repository root:

```text
dsh --profile research-atlas --from-default-profile web --help
dsh plugin --profile research-atlas add ./plugins/dsh-research-atlas
dsh --profile research-atlas --dump-config
```

Alternatively, install the release tarball with `dsh plugin --profile research-atlas add ./dsh-research-atlas-0.1.0.tgz`. A release tarball contains the adapter, not the Python checkout. Do not install the repository root as an npm package.

The bundle is **disabled on installation**. In `$DSH_HOME/profiles/research-atlas/cordis.patch.yml`, configure and enable the row:

```yaml
- id: baigong-research-atlas
  disabled: false
  config:
    projectRoot: '/absolute/path/to/baigong_research_atlas'
    python: '/absolute/path/to/its/python'
    configPath: config/research_assistant_v0001.json
    allowWrites: false
    timeoutMs: 60000
    maxOutputBytes: 8388608
```

On Windows use quoted forward-slash paths, e.g. `C:/your-checkout/.venv/Scripts/python.exe`. Paths are deployment settings, never model arguments. The project must contain `provenance/source_manifest.json` and the existing skill CLI. Initialize a **new** research ledger explicitly using that CLI's `initialize` operation before using task operations. The plugin never initializes or migrates a database automatically.

The first command creates a fresh profile from the shipped web template without starting a conversation. Run `dsh --profile research-atlas --host 127.0.0.1 --no-open` to start its local web surface. Alternatively, install the bundle into an existing web profile and configure that profile's patch. A base-only profile created by `plugin add` has tools but no interactive surface. The profile selects the host model; this plugin does not change it. Normal Harness model use may incur the host Provider's fees.

Set `allowWrites: true` only when you intend to let this profile create tasks and publish explicit stage deliveries. This deployment authorization exposes the write tool. Harness policies can still deny calls; this plugin does not grant or bypass them. Core `source_ids`, revision checks, package validation and scoped paths remain enforced. This is trusted local process integration, not an OS sandbox.

## Tools and requests

Both tools accept `operation`, `data_json` (a JSON-encoded object), and optional `dry_run`. They return the unchanged successful Python JSON object. Python and transport failures become Harness tool errors, not successful values. `describe` lists all **core** operations; the plugin only exposes the allowlists below.

| Tool | Operations |
|---|---|
| `research_atlas_read` | `describe`, `find_projects`, `find_tasks`, `resume_task`, `get_delivery_status`, `search_evidence`, `read_evidence`, `list_proposals` |
| `research_atlas_write` (opt-in) | `create_project`, `create_task`, `begin_run`, `submit_delivery`, `retry_archive`, `prepare_ingestion`, `index_revision` |

Examples:

```json
{"operation":"find_tasks","data_json":"{\"query\":\"counterexample\"}"}
{"operation":"resume_task","data_json":"{\"task_id\":\"task_...\"}"}
{"operation":"submit_delivery","data_json":"{\"package\":\"package_...\"}","dry_run":true}
```

Read [the research-continuity skill](../../skill/action/research-continuity/SKILL.md) before creating delivery packages. Call `resume_task`, then `begin_run` with the exact current revision and honest executor identity. A stage package in the configured inbox contains artifacts, a hash-checked manifest and `READY` written last. Prepare it using trusted local file tools or manual exchange, then explicitly submit it. The adapter never invents evidence or writes a package on behalf of the caller. Preserve project/task IDs when switching agents or models.

Only `submit_delivery` and `prepare_ingestion` support mutation dry-run. Publishing locally does not imply indexing completed; query delivery status. `index_revision` is an explicit SQLite/DuckDB operation. No AI, vector or Neo4j invocation is added by this plugin. AI permission/start/generation and unrestricted strategy execution are intentionally outside this adapter's operation allowlist. Existing project AI capabilities still use Action AI Controller. External Harness inference is owned/audited by Harness, not automatically by that controller.

Temporary request files are created under the checkout's `runtime/dsh-request-*` and removed after child exit; research outputs follow the configured core paths. Source text returned to Harness may enter its model context and session logs: authorize sources accordingly. Python inherits a minimal OS environment without Provider keys or application tokens. Timeouts, cancellation or child failure during writes can leave an **unknown outcome**; inspect the ledger/delivery status before retrying. The adapter never retries automatically. Trusted local concurrent filesystem modification is outside its isolation guarantee.

## Verify and roll back

The repository test uses the real installed Cordis and ToolRuntime, plus the real Python CLI, with synthetic data only:

```powershell
$env:ATLAS_TEST_ROOT = (Get-Location).Path
$env:ATLAS_TEST_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
node plugins/dsh-research-atlas/test_plugin_v0001.js
```

Standalone Node tests require the declared peers and `@deepseek-ai/dsh-system-prompt` from the same tested host to be resolvable. Tests do not install them. Results remain under `test-output/dsh-plugin-*`. This tests plugin execution and synthetic executor transitions; it does not claim fresh live model inference. See [the acceptance report](../../docs/harness_plugin_acceptance_20260927.md).

Disable the row or remove the bundle:

```text
dsh plugin --profile research-atlas remove dsh-research-atlas
```

Unload removes registered tools and terminates/waits for owned child processes. Removal does not erase research data. Code rollback uses the prior Git commit/release; reconcile interrupted writes separately. This external plugin is registered in Cordis, not by running the local Action development-registration writers. No `action.db` or original baigong engineering data is changed during installation.

## Provenance and community

Apache-2.0, Ruixin Yang / David7583. Existing baigong provenance remains in the parent repository; see its `NOTICE`, `CITATION.cff` and `provenance/` directory.

Primary interface references: [tool authoring](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/user/develop/basic/tool.md), [packaging](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/user/develop/basic/publish.md), [contribution policy](https://github.com/deepseek-ai/deepseek-harness/blob/master/CONTRIBUTING.md).

DeepSeek's [Community plugins link](https://www.deepseek.com/harness/en/) leads to the GitHub [`dsh-plugin` topic](https://github.com/topics/dsh-plugin). Topic association provides community discovery, not first-party endorsement or incorporation into the core repository. No npm registry publication is claimed by a GitHub release.
