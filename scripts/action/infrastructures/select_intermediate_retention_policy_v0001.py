# ============================================================
# 文件名: select_intermediate_retention_policy_v0001.py
# 中文名: 运行前中间产物处置选择
# 版本号: v0001
#
# 主层级: action
# 层级: infrastructures / retention
# 脚本定位: 在业务运行前获取本次运行的明确处置选择
#
# 职责说明:
# - 默认选择删除，但未获得本次明确确认不得开始运行
# 本脚本做什么:
# - 提供删除、压缩、转移、保留选择并生成绑定运行的凭证
# 本脚本不做什么:
# - 不运行业务链，不删除产物，不复用旧运行的确认
# 制度边界声明:
# - EOF、取消和不明确的确认均拒绝启动；提示写 stderr
# 可更新: True
# ============================================================
# ALIAS_META
# alias: select_intermediate_retention_policy_v0001
# family: select_intermediate_retention_policy
# role: per_run_retention_selection
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/select_intermediate_retention_policy_v0001.py
# input:
#   - run binding and current user input
# output:
#   - confirmed retention receipt
# depends_on:
#   - intermediate_retention_contract_v0001
# used_by:
#   - data_action_chain_pipeline_v0017
# ============================================================
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from intermediate_retention_contract_v0001 import RetentionError, cli_result, load, no_links, now, seal, sha, verify, write

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "select_intermediate_retention_policy"
SCRIPT_NAME = "select_intermediate_retention_policy_v0001"
SCRIPT_VERSION = "v0001"
MODES = ("delete", "compress", "transfer", "keep")


# ============================================================
# 核心业务区
# ============================================================
def select(binding: dict, mode: str = "delete", destination: str = "", reader=None) -> dict:
    verify(binding, "binding")
    reader = reader or input
    print(f"运行 {binding['run_id']}，来源 {binding['source']}\n落库校验后处理中间产物：delete 删除（默认）/ compress 压缩 / transfer 转移 / keep 保留。\n原始来源、数据库、来源映射和审计凭证保留。请输入选择：", file=sys.stderr)
    try:
        chosen = reader().strip().lower() or mode
        if chosen not in MODES:
            raise RetentionError("Unknown retention choice; run not started")
        if chosen in {"compress", "transfer"}:
            if not destination:
                print("请输入归档目标目录：", file=sys.stderr)
                destination = reader().strip()
            if not destination:
                raise RetentionError("Archive destination required")
            destination = str(no_links(Path(destination)))
        else:
            destination = ""
        print(f"本次选择 {chosen}，目标 {destination or '本次运行中间产物'}。确认并开始请输入 yes；其他输入取消：", file=sys.stderr)
        if reader().strip().lower() != "yes":
            raise RetentionError("User did not confirm; run not started")
    except EOFError as exc:
        raise RetentionError("Interactive confirmation required for this run; run not started") from exc
    return seal({"kind": "receipt", "binding_sha256": binding["contract_sha256"], "run_id": binding["run_id"], "mode": chosen, "destination": destination, "confirmed_at": now(), "confirmed": True})


# ============================================================
# CLI / main 接口区
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binding", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=MODES, default="delete")
    parser.add_argument("--destination", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    def run():
        binding = verify(load(Path(args.binding)), "binding")
        if args.dry_run:
            return {"status": "dry_run", "default_mode": args.mode, "confirmation_required": True}
        receipt = select(binding, args.mode, args.destination)
        output = Path(args.output)
        if output.exists():
            raise RetentionError("Confirmation receipt must not be overwritten")
        write(output, receipt)
        return {"status": "confirmed", "receipt": str(output), "mode": receipt["mode"]}
    return cli_result(run)


if __name__ == "__main__":
    raise SystemExit(main())
