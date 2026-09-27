# ============================================================
# 文件名: write_source_identity_annotations_v0001.py
# 中文名: 消息身份旁表事务写入
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 消息身份旁表事务写入
#
# 职责说明:
# - 消息身份旁表事务写入
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
# alias: write_source_identity_annotations_v0001
# family: write_source_identity_annotations
# role: write_source_identity_annotations_v0001
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/write_source_identity_annotations_v0001.py
# input:
#   - identity enriched units and existing data.db
# output:
#   - append-only identity rows and write manifest
# depends_on:
#   - Python standard library
#   - source_message_identity_contract_v0001
#   - init_source_identity_annotation_schema_v0001
# used_by:
#   - data_action_chain_pipeline_v0013
# ============================================================

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from source_message_identity_contract_v0001 import KEY_FIELDS, DB_FIELDS, SIDE_TABLE, IdentityError, cli_result, connect, key, payload, read_jsonl, sha, write_json

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "write_source_identity_annotations"
SCRIPT_NAME = "write_source_identity_annotations_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心事务写入
# ============================================================

def write_annotations(units,db,run_id,*,dry_run=False):
    rows=read_jsonl(units)
    records=[(key(r),payload(r),r) for r in rows]
    if len({k for k,_,_ in records}) != len(records):
        raise IdentityError("duplicate text coordinates")
    if not isinstance(run_id,str) or not run_id.strip():
        raise IdentityError("run_id required")
    conn=connect(db,readonly=dry_run)
    inserted=duplicates=0
    where=" AND ".join(f"{f}=?" for f in KEY_FIELDS)
    try:
        if not dry_run: conn.execute("BEGIN IMMEDIATE")
        for k,values,row in records:
            original=conn.execute("SELECT text FROM data_text_units WHERE "+where,k).fetchone()
            if original is None or original[0] != row.get("text",row.get("unit_text")):
                raise IdentityError("canonical text missing or differs")
            existing=conn.execute(f"SELECT {','.join(DB_FIELDS)} FROM {SIDE_TABLE} WHERE "+where,k).fetchone()
            if existing is not None:
                if existing != values: raise IdentityError("conflicting existing identity annotation")
                duplicates+=1
            elif not dry_run:
                cols=KEY_FIELDS+DB_FIELDS+("annotation_run_id","annotated_at")
                conn.execute(f"INSERT INTO {SIDE_TABLE} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
                             k+values+(run_id,datetime.now(timezone.utc).isoformat()))
                inserted+=1
        if not dry_run: conn.commit()
        return {"status":"dry_run" if dry_run else "completed","input_records":len(rows),"inserted_records":inserted,
                "unchanged_duplicates":duplicates,"table":SIDE_TABLE,"input_sha256":sha(units)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    p=argparse.ArgumentParser()
    for name in ("units","db","manifest","run-id"): p.add_argument("--"+name,required=True)
    p.add_argument("--dry-run",action="store_true")
    a=p.parse_args()
    def run():
        if Path(a.manifest).exists() or Path(a.manifest).resolve() in {Path(a.units).resolve(),Path(a.db).resolve()}:
            raise IdentityError("manifest exists or overlaps input")
        result=write_annotations(a.units,a.db,a.run_id,dry_run=a.dry_run)
        if not a.dry_run: write_json(a.manifest,result)
        return result
    return cli_result(run)


if __name__=="__main__":
    raise SystemExit(main())
