<p align="center"><img src="assets/baigong-research-atlas.png" alt="Baigong research_atlas" width="280"></p>

# Baigong·research_atlas

[中文](百工·research_atlas.md) · [baigong](https://github.com/David7583/baigong)

Continue the same project and task across agents and ordinary AI clients. Preserve evidence, stage outputs, open questions and verifiable handoffs. Project identity is independent of the client, provider and model. Switching takes place at stage boundaries: Agent↔Agent, Agent↔AI and AI↔AI.

## Install and start

Download and extract this repository, or clone it with Git. Open a terminal in its root. Use your own Python environment. Windows with Python 3.12.7 was tested; isolated-directory acceptance reused an existing interpreter and is not a clean-machine installation test.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -B scripts/action/skill/research_continuity/research_continuity_v0001.py initialize
.\.venv\Scripts\python.exe -B scripts/action/skill/research_continuity/research_continuity_v0001.py ui --port 8793
```

Open http://127.0.0.1:8793 . Initialization creates the local ledger without clearing an existing ledger. Skip dependency installation if already installed. The default setup has no real evidence sources and does not enable AI. Harness is optional.

## Use the project

1. Create a project and task in the local UI, recording the goal, constraints and authorized evidence sources.
2. Read the current revision and handoff before starting. Keep the same project_id and task_id when switching executors.
3. Tool-capable agents use the Skill and CLI below. For AI clients without tools, exchange inputs and outputs manually; generated text is not proof that an operation ran.
4. Submit a stage delivery with artifacts, evidence, failures, open questions and next steps. Explicit archiving creates a new revision. The next executor resumes that same task.
5. Run prepare_ingestion and index_revision explicitly when indexing is needed. Archive and index status are separate. The accepted index path currently covers SQLite and DuckDB.

See the [research-continuity Skill](skill/action/research-continuity/SKILL.md) for the CLI and package contract. A delivery includes manifest.json, UTF-8 artifacts and a READY checksum written last. Preserve history; reconcile revision conflicts and operations with unknown outcomes before retrying.

CLI example: save this JSON as runtime/request.json using a real existing task ID:

```json
{"task_id": "replace-with-existing-task-id"}
```

```text
python -B scripts/action/skill/research_continuity/research_continuity_v0001.py describe
python -B scripts/action/skill/research_continuity/research_continuity_v0001.py --input runtime/request.json resume_task
```

## Configure AI and evidence

Local API calls go through Action AI Controller. An administrator configures the provider endpoint, model and credential reference, registers a separate application identity, and grants both global and application permissions. Keep the provider key separate from the application token. Starting the UI does not grant permission.

Merge this object into config/research_assistant_v0001.json, retaining its existing fields:

```json
{"ai":{"controller_config":"config/action/ai/action_ai_controller_config_v0002.yml","app_id":"research_assistant","token_environment":"RESEARCH_ASSISTANT_TOKEN","providers":["your_provider_id"]}}
```

Use the credential-free templates in config/examples, set the controller sources to your local catalog and credential-reference files, and supply secrets through environment variables. The example.invalid endpoint is deliberately nonfunctional. The application token environment variable is RESEARCH_ASSISTANT_TOKEN; the example provider credential uses RESEARCH_PROVIDER_KEY. Never commit either value.

Agent and provider names are not restricted to DeepSeek, Kimi, Codex or Harness. Automatic proposals currently use the chat operation. Compatible protocols can be configured; other native protocols require adapters and acceptance tests. Harness is an optional bridge. Proposals require explicit archiving and do not become independent evidence. Paid requests are not automatically retried. Pausing blocks the next request and does not cancel a request already sent.

Evidence retrieval is disconnected by default. Configure query_sources with paths, allowed tables/columns, semantics and evidence_mapping; authorize source_ids per task. Current controlled retrieval uses lexical/exact-ID queries. Do not assume optional vector or graph capabilities are deployed merely because source files exist.

## Backup, tests and rollback

Stop processes before backing up runtime/, actioning/, data_processed/research/, data_runtime/action/ai_controller/, scripts/orchestration/outputs/action/ and local configurations. Check controller state_path if customized. These ignored directories contain persistent records. Use SQLite's backup API when backing up a running database so WAL data is not lost. Roll back matching code/configuration while preserving ledgers, original artifacts and failure records.

Offline tests (no model calls):

```text
python -B -m unittest discover -s tests -p "test_research*.py"
```

The accepted deployment includes actual DeepSeek/Kimi API and Harness handoff tests, offline integration tests and independent-directory checks. This does not certify every provider/client or operating system. See the [deployment details](docs/deployment.md) and [acceptance evidence](docs/promotion_acceptance_20260927.md) (Chinese).

## Attribution, license and citation

This project is associated with [baigong](https://github.com/David7583/baigong) and reuses a local snapshot of its Data–Action–Data implementation. [Source hashes](provenance/source_manifest.json) identify the copied files; the snapshot is not asserted to match a particular upstream release.

Licensed under [Apache-2.0](LICENSE). See [NOTICE](NOTICE), [source attribution](docs/source_and_dependency_notice.md) and [CITATION.cff](CITATION.cff). Zenodo publication status is tracked in [publication scope](docs/publication_scope.md). A DOI is added only after it actually exists.
