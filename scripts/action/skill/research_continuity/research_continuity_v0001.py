# ============================================================
# 文件名: research_continuity_v0001.py
# 中文名: 研究接续 Skill 薄入口
# 版本号: v0001
#
# 主层级: action
# 层级: skill / research_continuity
# 脚本定位: 将 CLI 或 UI 参数转交研究助手业务入口
#
# 职责说明:
# - 为不同 Agent 提供相同的本地启动位置
#
# 本脚本做什么:
# - 转交 CLI 参数；首参数 ui 时转交界面入口
#
# 本脚本不做什么:
# - 不复制业务逻辑，不安装依赖，不执行开发登记
#
# 制度边界声明:
# - 写入和错误契约沿用业务入口；导入不启动服务
# - AI 仍经应用自身总控句柄，入口不读取 Provider 凭据或开启许可
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_continuity_v0001
# family: research_continuity
# role: skill_entrypoint
# version: v0001
# status: active
# entry_point: scripts/action/skill/research_continuity/research_continuity_v0001.py
# input:
#   - CLI operation arguments or ui arguments
# output:
#   - forwarded structured result or local UI service
# depends_on:
#   - research_assistant_v0001
#   - research_ui_v0001
# used_by: []
# ============================================================

from __future__ import annotations

import sys
from pathlib import Path


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_continuity"
SCRIPT_NAME = "research_continuity_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    business = Path(__file__).resolve().parents[2] / "development/scripts/research_assistant"
    sys.path.insert(0, str(business))
    if sys.argv[1:2] == ["ui"]:
        del sys.argv[1]
        from research_ui_v0001 import main as run
    else:
        from research_assistant_v0001 import main as run
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
