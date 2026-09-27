# ============================================================
# 文件名: validate_source_identity_data_db_v0001.py
# 中文名: 消息身份旁表只读对账
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 消息身份旁表只读对账
#
# 职责说明:
# - 消息身份旁表只读对账
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
# alias: validate_source_identity_data_db_v0001
# family: validate_source_identity_data_db
# role: validate_source_identity_data_db_v0001
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/validate_source_identity_data_db_v0001.py
# input:
#   - identity enriched units and data.db
# output:
#   - read-only identity completeness and equality report
# depends_on:
#   - Python standard library
#   - source_message_identity_contract_v0001
#   - write_source_identity_annotations_v0001
# used_by:
#   - data_action_chain_pipeline_v0013
# ============================================================

from __future__ import annotations

import argparse
from pathlib import Path
from source_message_identity_contract_v0001 import KEY_FIELDS, DB_FIELDS, SIDE_TABLE, IdentityError, cli_result, connect, key, payload, read_jsonl, sha, write_json

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "validate_source_identity_data_db"
SCRIPT_NAME = "validate_source_identity_data_db_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心校验
# ============================================================

def validate_database(units,db):
    rows=read_jsonl(units)
    conn=connect(db)
    errors,seen=[],set()
    where=" AND ".join(f"{f}=?" for f in KEY_FIELDS)
    try:
        for number,row in enumerate(rows,1):
            k,expected=key(row),payload(row)
            if k in seen: errors.append({"line":number,"kind":"duplicate_key"})
            seen.add(k)
            canonical=conn.execute("SELECT text FROM data_text_units WHERE "+where,k).fetchone()
            actual=conn.execute(f"SELECT {','.join(DB_FIELDS)} FROM {SIDE_TABLE} WHERE "+where,k).fetchone()
            if actual != expected or canonical is None or canonical[0] != row.get("text",row.get("unit_text")):
                errors.append({"line":number,"kind":"source_or_payload_mismatch"})
        total=conn.execute(f"SELECT count(*) FROM {SIDE_TABLE}").fetchone()[0]
    finally:
        conn.close()
    return {"status":"failed" if errors else "passed","input_records":len(rows),"side_table_records":total,
            "validated_side_records":len(rows)-len(errors),"error_count":len(errors),"error_samples":errors[:100],"input_sha256":sha(units)}


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    p=argparse.ArgumentParser()
    for name in ("units","db","report","run-id"): p.add_argument("--"+name,required=True)
    p.add_argument("--dry-run",action="store_true")
    a=p.parse_args()
    def run():
        if Path(a.report).resolve() in {Path(a.units).resolve(),Path(a.db).resolve()}:
            raise IdentityError("report overlaps input")
        result=validate_database(a.units,a.db)
        if not a.dry_run: write_json(a.report,result)
        return result
    return cli_result(run)


if __name__=="__main__":
    raise SystemExit(main())
