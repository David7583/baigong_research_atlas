# ============================================================
# 文件名: validate_source_identity_attachment_v0001.py
# 中文名: 消息身份挂接守恒校验
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 消息身份挂接守恒校验
#
# 职责说明:
# - 消息身份挂接守恒校验
#
# 本脚本做什么:
# - 按版本化消息身份契约处理显式输入并输出校验证据
#
# 本脚本不做什么:
# - 不推断用户实名或实际后台模型，不修改正文或稳定身份
#
# 制度边界声明:
# - 写入仅限显式输出；既有标注冲突拒绝，失败不伪报成功
# - 不调用模型和外部服务；来源缺失明确保留，不回填猜测
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: validate_source_identity_attachment_v0001
# family: validate_source_identity_attachment
# role: validate_source_identity_attachment_v0001
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/validate_source_identity_attachment_v0001.py
# input:
#   - original and identity enriched text units
# output:
#   - field conservation and identity contract report
# depends_on:
#   - Python standard library
#   - source_message_identity_contract_v0001
# used_by:
#   - data_action_chain_pipeline_v0013
# ============================================================

from __future__ import annotations

import argparse
from pathlib import Path
from source_message_identity_contract_v0001 import FIELDS, IdentityError, cli_result, read_jsonl, sha, validate_payload, write_json

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "validate_source_identity_attachment"
SCRIPT_NAME = "validate_source_identity_attachment_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心校验
# ============================================================

def validate_attachment(original, enriched):
    before, after = read_jsonl(original), read_jsonl(enriched)
    errors = []
    if len(before) != len(after):
        errors.append({"kind":"row_count_mismatch"})
    for i,(source,row) in enumerate(zip(before,after),1):
        if any(k not in row or row[k] != v for k,v in source.items()):
            errors.append({"line":i,"kind":"original_field_changed"})
        if set(row)-set(source) != set(FIELDS):
            errors.append({"line":i,"kind":"unexpected_or_missing_fields"})
        try:
            validate_payload(row)
        except IdentityError as exc:
            errors.append({"line":i,"kind":"invalid_payload","detail":str(exc)})
    return {"status":"failed" if errors else "passed","original_records":len(before),"enriched_records":len(after),
            "error_count":len(errors),"error_samples":errors[:100],"original_sha256":sha(original),"enriched_sha256":sha(enriched)}


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    p=argparse.ArgumentParser()
    for name in ("original","enriched","report","run-id"):
        p.add_argument("--"+name,required=True)
    p.add_argument("--dry-run",action="store_true")
    a=p.parse_args()
    def run():
        if len({Path(x).resolve() for x in (a.original,a.enriched,a.report)}) != 3:
            raise IdentityError("input/report overlap")
        result=validate_attachment(a.original,a.enriched)
        result["run_id"]=a.run_id
        if not a.dry_run: write_json(a.report,result)
        return result
    return cli_result(run)


if __name__ == "__main__":
    raise SystemExit(main())
