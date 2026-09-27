# ============================================================
# 文件名: init_source_identity_annotation_schema_v0001.py
# 中文名: 消息身份旁表初始化
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 消息身份旁表初始化
#
# 职责说明:
# - 消息身份旁表初始化
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
# alias: init_source_identity_annotation_schema_v0001
# family: init_source_identity_annotation_schema
# role: init_source_identity_annotation_schema_v0001
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/init_source_identity_annotation_schema_v0001.py
# input:
#   - existing data.db canonical text schema
# output:
#   - additive source identity side table and indexes
# depends_on:
#   - Python standard library
#   - source_message_identity_contract_v0001
# used_by:
#   - data_action_chain_pipeline_v0013
# ============================================================

from __future__ import annotations

import argparse
from source_message_identity_contract_v0001 import KEY_FIELDS, DB_FIELDS, SIDE_TABLE, IdentityError, cli_result, connect

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "init_source_identity_annotation_schema"
SCRIPT_NAME = "init_source_identity_annotation_schema_v0001"
SCRIPT_VERSION = "v0001"
EXPECTED_COLUMNS = KEY_FIELDS + DB_FIELDS + ("annotation_run_id","annotated_at")


# ============================================================
# 核心数据库契约
# ============================================================

def initialize_schema(db, *, dry_run=False):
    conn=connect(db,readonly=dry_run)
    try:
        cols={r[1] for r in conn.execute("PRAGMA table_info(data_text_units)")}
        if not set(KEY_FIELDS).issubset(cols):
            raise IdentityError("canonical table missing or incompatible")
        exists=conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(SIDE_TABLE,)).fetchone()
        if exists:
            actual=tuple(r[1] for r in conn.execute(f"PRAGMA table_info({SIDE_TABLE})"))
            if actual != EXPECTED_COLUMNS:
                raise IdentityError("existing identity schema incompatible")
        if dry_run:
            return {"status":"dry_run","already_exists":bool(exists)}
        conn.execute("BEGIN")
        definitions=[f"{f} {'TEXT' if i<2 else 'INTEGER'} NOT NULL" for i,f in enumerate(KEY_FIELDS)]
        definitions += [f"{f} TEXT" + (" NOT NULL" if f=="message_identity_status" else "") for f in DB_FIELDS]
        definitions += ["annotation_run_id TEXT NOT NULL","annotated_at TEXT NOT NULL",
                        "PRIMARY KEY ("+",".join(KEY_FIELDS)+")",
                        "FOREIGN KEY ("+",".join(KEY_FIELDS)+") REFERENCES data_text_units ("+",".join(KEY_FIELDS)+")"]
        conn.execute(f"CREATE TABLE IF NOT EXISTS {SIDE_TABLE} ("+",".join(definitions)+")")
        for field in ("message_role","message_model_slug","identity_message_id"):
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{SIDE_TABLE}_{field} ON {SIDE_TABLE} ({field})")
        conn.commit()
        return {"status":"completed","already_exists":bool(exists),"table":SIDE_TABLE}
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
    p.add_argument("--db",required=True)
    p.add_argument("--dry-run",action="store_true")
    a=p.parse_args()
    return cli_result(lambda:initialize_schema(a.db,dry_run=a.dry_run))


if __name__=="__main__":
    raise SystemExit(main())
