# ============================================================
# 文件名: attach_source_identities_to_text_units_v0001.py
# 中文名: 消息身份坐标挂接
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 消息身份坐标挂接
#
# 职责说明:
# - 消息身份坐标挂接
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
# alias: attach_source_identities_to_text_units_v0001
# family: attach_source_identities_to_text_units
# role: attach_source_identities_to_text_units_v0001
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/attach_source_identities_to_text_units_v0001.py
# input:
#   - text units and authorized identity annotations
# output:
#   - same ordered text units enriched with identity fields
# depends_on:
#   - Python standard library
#   - source_message_identity_contract_v0001
#   - extract_source_message_identities_v0001
# used_by:
#   - data_action_chain_pipeline_v0013
# ============================================================

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path
from source_message_identity_contract_v0001 import (
    FIELDS, IdentityError, canonical, cli_result, read_jsonl, sha, validate_payload, write_json, write_output,
)

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "attach_source_identities_to_text_units"
SCRIPT_NAME = "attach_source_identities_to_text_units_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def coordinate(row, annotation=False):
    asset, path, index = row.get("asset_id"), row.get("target_path" if annotation else "path"), row.get("value_index")
    if not isinstance(asset, str) or not asset or not isinstance(path, str) or not path or type(index) is not int or index < 0:
        raise IdentityError("invalid attachment coordinate")
    return asset, path, index


def attach_identities(units_path, annotations_path):
    index = defaultdict(list)
    for row in read_jsonl(annotations_path):
        validate_payload(row)
        index[coordinate(row, True)].append(row)
    output, counts = [], Counter()
    for source in read_jsonl(units_path):
        if set(source).intersection(FIELDS):
            raise IdentityError("input already has identity fields")
        candidates = index.get(coordinate(source), [])
        payload = {f: None for f in FIELDS}
        payload["message_identity_status"] = "not_applicable"
        if candidates:
            identities = {canonical({f:r[f] for f in FIELDS}) for r in candidates}
            if len(identities) != 1:
                raise IdentityError("conflicting identity annotations")
            selected = candidates[0]
            if source.get("source_value_sha1") is not None and source["source_value_sha1"] != selected["source_value_sha1"]:
                raise IdentityError("source text hash mismatch")
            payload = {f:selected[f] for f in FIELDS}
        enriched = {**source, **payload}
        validate_payload(enriched)
        output.append(enriched)
        counts[payload["message_identity_status"]] += 1
    return output, dict(counts)


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    for name in ("units", "annotations", "output", "manifest", "run-id"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    def run():
        paths = [Path(x).resolve() for x in (args.units,args.annotations,args.output,args.manifest)]
        if len(set(paths)) != 4 or any(p.exists() for p in paths[2:]):
            raise IdentityError("input/output paths overlap or output exists")
        rows, counts = attach_identities(paths[0], paths[1])
        report = {"status":"dry_run" if args.dry_run else "completed", "run_id":args.run_id,
                  "input_records":len(rows),"output_records":len(rows),"status_counts":counts,
                  "units_input_sha256":sha(paths[0]),"annotations_input_sha256":sha(paths[1])}
        if not args.dry_run:
            write_output(paths[2], "".join(canonical(r)+"\n" for r in rows))
            report["output_sha256"] = sha(paths[2])
            write_json(paths[3],report)
        return report
    return cli_result(run)


if __name__ == "__main__":
    raise SystemExit(main())
