# DeepSeek Harness plugin acceptance — 2026-09-27

Scope: native research-continuity tool adapter, local deployment, public release and community discovery. No WeKnora work, Zenodo setup, paid model calls, new dependency installation, core Python rewrite or original engineering data changes.

## Implementation and maturity

The candidate was developed under `staging/plugins/dsh-research-atlas/`, then promoted to `plugins/dsh-research-atlas/` after targeted tests. The release is an Active adapter for the explicitly tested host version; the upstream host remains a developer preview. Staging copies are delivery evidence, not runtime dependencies.

The adapter exports Cordis `apply`, `inject = ['tools']` and a Schemastery configuration. Two native tools forward allowlisted operations to the existing `research_continuity_v0001.py` CLI. Ready-to-run JavaScript avoids TypeScript compilation/install hooks and additional build dependencies. The Python core and existing model bridge were not changed.

Read-only deployment is the default. Writes require `allowWrites: true`. The model cannot choose the interpreter, project root, configuration, environment or shell command. Runtime invokes a fixed Python entry with an argument array and `shell: false`. Config/input paths stay within the checkout, original operation results retain their semantics, and failure remains a Harness tool error. Host policy rejection is honored. This is a trusted local adapter, not OS-level isolation from other same-user processes.

No automatic Action registration runs: the new files are an external Cordis package outside `scripts/action`; the existing registered Python entry is reused. No new Action script, function, task or function block was written to `action.db`. Original baigong provenance, licensing and author metadata remain unchanged.

## Version evidence

| Component | Verified value |
|---|---|
| `@deepseek-ai/dsh`, tools and system-prompt | `0.1.5-rc.3` |
| Cordis | `4.0.2` |
| Schemastery | `3.18.2` |
| Node | `24.18.0` |
| Platform | Windows; Python from the existing project environment |
| Upstream master checked during work | `477b4f420553e8a52c2fbccc464d7561b239c443` |
| npm latest checked during work | `0.1.5-rc.3` |

The tested npm host version is the compatibility target. Reading upstream master documentation does not imply testing every source commit. Other versions/platforms require validation.

## Tests and results

- Existing Python baseline: `python -m unittest discover -s tests -p 'test_research*.py'` — **27 passed**, 39.925 seconds. Its nine appended diagnostic log records were preserved under ignored `test-output/baseline-test-*.jsonl`; tracked logs were restored to their verified pre-test prefix. No evidence was discarded.
- Node syntax/import and real Cordis/ToolRuntime integration: **passed**. No mock tool registry was used. Calls reached the actual Python CLI.
- `describe` result equals the direct CLI result.
- Four synthetic stages crossed direct CLI → native Harness tools → manual-AI executor label → direct CLI. Revision reached **4**, preserving task/project identity, constraints, original evidence hash, next steps and explicit `index_status: not_requested`.
- Repeated identical submissions returned `existing`; stale revision, unsupported operation, AI-generation operation, package traversal, malformed/non-object JSON and out-of-root configuration were rejected.
- A real ToolRuntime guard denied the plugin call without dispatching it.
- Pre-start cancellation, child timeout, output limit, invalid JSON and disposal during a running child were checked. Child completion is awaited; uncertain write outcomes require reconciliation, never automatic retry.
- Unload removed registered tools; reload in read-only mode exposed no write tool and preserved the ledger.
- Actual `dsh plugin add` linked the local bundle offline; `--dump-config` showed its configuration layer. The shipped bundle starts disabled until configured.
- A complete `dsh` CLI boot loaded the configured bundle and an isolated probe successfully called `research_atlas_read/describe` through the host registry, with **zero model calls**.
- Actual `dsh plugin remove` removed both dependency and bundle registration. (`pnpm remove` does not accept the add-only `--offline/--ignore-scripts` flags; the normal remove command passed.)

Local detailed evidence remains in ignored `test-output/dsh-plugin-qHpbUW/summary.json` and `runtime/dsh-plugin-deployment/`. The test source ships in Git, while the install tarball excludes test probes. The final deployment and release-pack checks are recorded in the publication record.

The new tests verify the real adapter/runtime with synthetic inputs and executor labels. They do **not** claim a new live LLM conversation, every retrieval backend, web UI visual acceptance, or automatic cross-client private-history migration. Earlier live Harness/model acceptance remains separately described in `integration_test_report_20260927.md`.

## Rollback and limitations

Disable the profile row or `dsh plugin --profile research-atlas remove dsh-research-atlas`. Removal and tool unload preserve the ledger and research outputs. Revert the adapter release commit to roll back source without changing data. Interrupted mutations must be reconciled using task/delivery status; process termination is not a database rollback guarantee.

The plugin only bridges existing core operations. Initialization, AI permission/start/generation and unrestricted strategy execution are not exposed. Local publication and explicit SQLite/DuckDB indexing remain distinct. Harness-owned inference is not represented as Action Controller-audited inference. Any approved source returned to the host can enter its prompt/session log.

## Official ecosystem mechanism

Verified [DeepSeek landing page](https://www.deepseek.com/harness/en/): “Community plugins” points directly to [GitHub `dsh-plugin`](https://github.com/topics/dsh-plugin). GitHub operates the topic directory; DeepSeek links it and recommends topic association in its [contribution policy](https://github.com/deepseek-ai/deepseek-harness/blob/master/CONTRIBUTING.md). The core repository currently does not accept external PRs.

The official repository has a **Show Your Plugins!** Discussions category. Topic association and a plugin announcement are community participation, not a core merge, npm publication, security certification or DeepSeek endorsement. Publication status must be recorded from actual API responses and visible URLs, not inferred from submission intent.
