// ============================================================
// 文件名: test_host_load_v0001.js
// 中文名: Harness 实际启动装载探针
// 版本号: v0001
//
// 主层级: action
// 层级: tests / harness_plugin / loader
// 脚本定位: 在隔离 DSH_HOME 内验证真实 CLI 装载与只读工具调用
//
// 职责说明:
// - 等待实际工具注册，调用 describe 后退出隔离验收进程
//
// 本脚本做什么:
// - 输出一次验收 JSON 与明确退出码
//
// 本脚本不做什么:
// - 不启动模型、不写研究数据、不用于用户交互会话
//
// 制度边界声明:
// - 仅挂载于测试 overlay；十秒内完成或失败并退出
//
// 可更新: True
// ============================================================

// ============================================================
// ALIAS_META
// ============================================================
// alias: test_host_load_v0001
// family: test_host_load
// role: isolated_host_load_probe
// version: v0001
// status: experimental
// entry_point: plugins/dsh-research-atlas/test_host_load_v0001.js
// input:
//   - isolated Harness test overlay
// output:
//   - structured loader acceptance and process exit
// depends_on:
//   - Node.js stdlib
//   - Harness tools service
// used_by: []
// ============================================================

const DEFAULT_ENCODING = 'utf8';
const SCRIPT_FAMILY = 'test_host_load';
const SCRIPT_NAME = 'test_host_load_v0001';
const SCRIPT_VERSION = 'v0001';
export const name = 'baigong-isolated-load-test';
export const inject = ['tools'];

// ============================================================
// 隔离验收入口
// ============================================================

export async function apply(ctx) {
  try {
    for (let i = 0; i < 100; i++) {
      if (ctx.tools.schemas().some(tool => tool.name === 'research_atlas_read')) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const result = await ctx.tools.execute({ id: 'atlas-host-load', name: 'research_atlas_read', arguments: { operation: 'describe', data_json: '{}' }, signal: AbortSignal.timeout(10000) });
    if (result.isError || result.value.switch_boundary !== 'stage') throw new Error('HOST_PLUGIN_CALL_FAILED');
    process.stdout.write(JSON.stringify({ status: 'passed', check: 'actual dsh CLI profile bundle load and tool invocation', model_calls: 0, value: result.value }) + '\n', DEFAULT_ENCODING);
    process.exit(0);
  } catch {
    process.stdout.write(JSON.stringify({ status: 'error', error_type: 'HOST_PLUGIN_LOAD_FAILED' }) + '\n', DEFAULT_ENCODING);
    process.exit(2);
  }
}
