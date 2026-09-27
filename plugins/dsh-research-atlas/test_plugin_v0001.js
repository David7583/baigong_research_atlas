// ============================================================
// 文件名: test_plugin_v0001.js
// 中文名: Harness 插件隔离集成验收
// 版本号: v0001
//
// 主层级: action
// 层级: tests / harness_plugin
// 脚本定位: 真实 Cordis 与工具运行时到 Python CLI 的合成数据验收
//
// 职责说明:
// - 验证注册、调用、交接、权限拒绝、取消和卸载
//
// 本脚本做什么:
// - 在 test-output 唯一目录保存合成账本和报告
//
// 本脚本不做什么:
// - 不调用模型、不安装依赖、不读取真实业务库
//
// 制度边界声明:
// - 仅测试配置和合成产物写入；测试失败保留证据
//
// 可更新: True
// ============================================================

// ============================================================
// ALIAS_META
// ============================================================
// alias: test_plugin_v0001
// family: test_plugin
// role: harness_plugin_acceptance
// version: v0001
// status: experimental
// entry_point: plugins/dsh-research-atlas/test_plugin_v0001.js
// input:
//   - ATLAS_TEST_ROOT and ATLAS_TEST_PYTHON environment paths
// output:
//   - isolated test-output JSON report and synthetic ledger
// depends_on:
//   - Node.js stdlib
//   - @deepseek-ai/cordis
//   - @deepseek-ai/dsh-tools
//   - @deepseek-ai/dsh-system-prompt
//   - research_atlas_plugin_v0001
// used_by: []
// ============================================================

import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, writeFile, readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { Context } from '@deepseek-ai/cordis';
import { ToolRuntime } from '@deepseek-ai/dsh-tools';
import { SystemPrompt } from '@deepseek-ai/dsh-system-prompt';
import * as plugin from './research_atlas_plugin_v0001.js';

const DEFAULT_ENCODING = 'utf8';
const SCRIPT_FAMILY = 'test_plugin';
const SCRIPT_NAME = 'test_plugin_v0001';
const SCRIPT_VERSION = 'v0001';

// ============================================================
// 工具函数区
// ============================================================

const sha = value => createHash('sha256').update(value).digest('hex');
const json = value => JSON.stringify(value);
async function waitUntil(predicate) {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error('PLUGIN_LIFECYCLE_TIMEOUT');
}

// ============================================================
// 核心测试组件
// ============================================================

async function main() {
  const root = path.resolve(process.env.ATLAS_TEST_ROOT);
  const python = path.resolve(process.env.ATLAS_TEST_PYTHON);
  const entry = path.join(root, 'scripts/action/skill/research_continuity/research_continuity_v0001.py');
  await mkdir(path.join(root, 'test-output'), { recursive: true });
  const output = await mkdtemp(path.join(root, 'test-output/dsh-plugin-'));
  const relative = path.relative(root, output).split(path.sep).join('/');
  const configPath = `${relative}/config.json`;
  await writeFile(path.join(root, configPath), json({ schema_version: '1.0', ledger: `${relative}/ledger.sqlite3`, inbox: `${relative}/inbox`, publication_policy: 'authority_first_explicit', source_ids: ['synthetic'] }));
  const cli = (operation, data = {}) => {
    const request = path.join(output, 'cli-input.json');
    execFileSync(python, ['-c', 'import sys; assert sys.version_info >= (3, 10)'], { windowsHide: true });
    return writeFile(request, json(data)).then(() => JSON.parse(execFileSync(python, [entry, '--config', configPath, '--input', path.relative(root, request).split(path.sep).join('/'), operation], { cwd: root, encoding: DEFAULT_ENCODING, windowsHide: true })));
  };
  await cli('initialize');
  const config = plugin.Config({ projectRoot: root, python, configPath, allowWrites: true });
  const ctx = new Context();
  ctx.plugin(SystemPrompt, { includeHarnessIdentity: false });
  ctx.plugin(ToolRuntime, { mode: 'native' });
  await waitUntil(() => ctx.tools);
  let fork = ctx.plugin(plugin, config);
  await waitUntil(() => ctx.tools.schemas().some(t => t.name === 'research_atlas_write'));
  const checks = [];
  const invoke = async (operation, data = {}, writing = false, signal = new AbortController().signal) => ctx.tools.execute({ id: randomUUID(), name: writing ? 'research_atlas_write' : 'research_atlas_read', arguments: { operation, data_json: json(data) }, signal });
  const value = async (...args) => { const result = await invoke(...args); assert.equal(result.isError, false, json(result)); return result.value; };
  assert.deepEqual(await value('describe'), await cli('describe'));
  checks.push('CLI reference equivalence through real ToolRuntime');
  const project = await value('create_project', { title: 'Synthetic Harness continuity' }, true);
  const task = await value('create_task', { project_id: project.project_id, title: 'Counterexample', objective: 'Preserve evidence across executors', constraints: ['synthetic only', 'retain counterexample'], source_ids: ['synthetic'] }, true);
  const actors = ['Synthetic Agent A', 'Synthetic Harness tool caller', 'Synthetic AI via manual exchange', 'Synthetic Agent A return'];
  let lastDelivery;
  for (const [revision, actor] of actors.entries()) {
    const executor = { kind: revision === 2 ? 'ai' : 'agent', name: actor, version: null, provider: null, model: null, access: revision === 2 ? 'manual_exchange' : 'local_tools' };
    // First and final executors use the existing CLI; middle phases use native Harness tools.
    const run = revision === 0 || revision === 3 ? await cli('begin_run', { task_id: task.task_id, base_revision: revision, executor }) : await value('begin_run', { task_id: task.task_id, base_revision: revision, executor }, true);
    const folder = path.join(output, 'inbox', run.run_id);
    await mkdir(folder, { recursive: true });
    const report = '# Synthetic stage\n20°C A=8 B=12; 40°C is an unresolved counterexample.\n';
    await writeFile(path.join(folder, 'report.md'), report);
    const envelope = { schema_version: '1.0', project_id: project.project_id, task_id: task.task_id, run_id: run.run_id, base_revision: revision, idempotency_key: run.run_id, task_status: 'paused', run_status: 'succeeded', handoff: { completed: ['Read 20°C'], failed: [], decisions: [], open_questions: ['40°C?'], next_steps: ['Check counterexample'], pending_confirmation: [], unknown_operations: [] }, artifacts: [{ artifact_id: 'report', path: 'report.md', sha256: sha(report), size: Buffer.byteLength(report), origin: 'generated', generated_at: new Date().toISOString() }], evidence: [{ source_id: 'synthetic', object_id: 'obs20', source_hash: sha('A8 B12'), origin: 'synthetic_original', locator: { author: 'synthetic', event_time: '2026-01-01T00:00:00Z' }, claim: '20°C only', relation: 'supports' }], coverage: [{ source_id: 'synthetic', mode: 'manual', query: '20°C', status: 'completed', truncated: false, limitations: 'Synthetic only' }] };
    await writeFile(path.join(folder, 'manifest.json'), json(envelope));
    await writeFile(path.join(folder, 'READY'), sha(json(envelope)));
    const published = revision === 0 || revision === 3 ? await cli('submit_delivery', { package: run.run_id }) : await value('submit_delivery', { package: run.run_id }, true);
    lastDelivery = published.delivery_id;
    assert.equal(published.revision, revision + 1);
    assert.equal((await value('submit_delivery', { package: run.run_id }, true)).status, 'existing');
    const resumed = await value('resume_task', { task_id: task.task_id });
    assert.equal(resumed.task.project_id, project.project_id);
    assert.deepEqual(resumed.task.constraints, task.constraints);
    assert.equal(resumed.task.evidence[0].source_hash, sha('A8 B12'));
    assert.deepEqual(resumed.task.handoff.next_steps, ['Check counterexample']);
    assert.equal(resumed.current_revision, revision + 1);
  }
  assert.equal((await value('get_delivery_status', { delivery_id: lastDelivery })).index_status, 'not_requested');
  checks.push('4-stage CLI/Harness/manual-executor-label/CLI roundtrip, stable identity/evidence, idempotency, explicit index status');
  assert.equal((await invoke('begin_run', { task_id: task.task_id, base_revision: 0, executor: { kind: 'agent', name: 'stale', version: null, provider: null, model: null, access: 'local_tools' } }, true)).isError, true);
  assert.equal((await invoke('initialize', {}, true)).isError, true);
  assert.equal((await invoke('generate_ai_proposal', {}, true)).isError, true);
  assert.equal((await invoke('submit_delivery', { package: '../../outside' }, true)).isError, true);
  const denied = ctx.tools.guard(exec => exec.name.startsWith('research_atlas_') ? 'TEST_POLICY_DENIAL' : undefined);
  assert.equal((await invoke('describe')).isError, true);
  denied();
  checks.push('revision conflict, unsupported/AI operations, traversal and Harness policy denial');
  const bridge = new plugin.AtlasBridge(config);
  await assert.rejects(bridge.invoke('describe', '[]', false), /INPUT_OBJECT_REQUIRED/);
  await assert.rejects(bridge.invoke('describe', '{bad', false), /INPUT_JSON_INVALID/);
  await assert.rejects(new plugin.AtlasBridge({ ...config, configPath: '../config/research_assistant_v0001.json' }).validate(), /PATH_OUTSIDE_PROJECT/);
  await assert.rejects(bridge.invoke('describe', '{}', false, AbortSignal.abort()), /CANCELLED/);
  await bridge.validate();
  bridge.config = { ...config, timeoutMs: 25 };
  await assert.rejects(bridge.run(['-c', 'import time; time.sleep(5)'], new AbortController().signal, true), /TIMEOUT_OUTCOME_UNKNOWN/);
  bridge.config = { ...config, maxOutputBytes: 1024 };
  await assert.rejects(bridge.run(['-c', 'print("x" * 4096)'], new AbortController().signal, false), /OUTPUT_LIMIT/);
  bridge.config = config;
  await assert.rejects(bridge.run(['-c', 'print("invalid json")'], new AbortController().signal, false), /CLI_INVALID_JSON/);
  const pending = bridge.run(['-c', 'import time; time.sleep(5)'], new AbortController().signal, true);
  const pendingAssertion = assert.rejects(pending, /PLUGIN_UNLOADED_OUTCOME_UNKNOWN/);
  await bridge.dispose();
  await pendingAssertion;
  assert.equal(bridge.active.size, 0);
  checks.push('input/config validation, cancellation, timeout, output bound, invalid JSON, quiescent disposal');
  await fork.dispose();
  assert.equal(ctx.tools.schemas().some(t => t.name.startsWith('research_atlas_')), false);
  fork = ctx.plugin(plugin, { ...config, allowWrites: false });
  await waitUntil(() => ctx.tools.schemas().some(t => t.name === 'research_atlas_read'));
  assert.equal(ctx.tools.schemas().some(t => t.name === 'research_atlas_write'), false);
  assert.equal((await invoke('begin_run', {}, true)).isError, true);
  assert.equal((await value('resume_task', { task_id: task.task_id })).current_revision, 4);
  await fork.dispose();
  checks.push('unload removes tools; read-only reload rejects mutation and preserves ledger');
  const requests = (await readdir(path.join(root, 'runtime'))).filter(p => p.startsWith('dsh-request-'));
  assert.deepEqual(requests, []);
  const summary = { status: 'passed', checks, dsh: '0.1.5-rc.3', cordis: '4.0.2', node: process.version, model_calls: 0, output, final_revision: 4, limitation: 'Real plugin runtime and Python core; synthetic executor labels, no live model inference.' };
  await writeFile(path.join(output, 'summary.json'), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

// ============================================================
// 显式验收入口
// ============================================================

main().catch(error => { console.error(error); process.exitCode = 1; });
