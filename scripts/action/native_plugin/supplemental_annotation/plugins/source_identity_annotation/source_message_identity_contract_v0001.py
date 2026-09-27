# ============================================================
# 文件名: source_message_identity_contract_v0001.py
# 中文名: 消息身份字段契约与纯校验组件
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 消息身份字段契约与纯校验组件
#
# 职责说明:
# - 消息身份字段契约与纯校验组件
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
# alias: source_message_identity_contract_v0001
# family: source_message_identity_contract
# role: source_message_identity_contract_v0001
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/source_message_identity_contract_v0001.py
# input:
#   - identity row and canonical locator
# output:
#   - validated payload, deterministic JSON and atomic output helpers
# depends_on:
#   - Python standard library
# used_by:
#   - data_action_chain_pipeline_v0013
# ============================================================

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "source_message_identity_contract"
SCRIPT_NAME = "source_message_identity_contract_v0001"
SCRIPT_VERSION = "v0001"
KEY_FIELDS = ("asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end")
FIELDS = ("message_role", "message_author_name", "message_model_slug", "message_default_model_slug",
          "message_identity_evidence", "message_identity_status", "message_identity_rule_version",
          "identity_message_id", "identity_mapping_node_id", "source_identity_annotation_id")
DB_FIELDS = tuple("message_identity_evidence_json" if x == "message_identity_evidence" else x for x in FIELDS)
SIDE_TABLE = "data_text_unit_source_identities"
ROLES = {"user", "assistant", "system", "tool", "developer"}
STATUSES = {"resolved", "missing", "invalid", "ambiguous", "not_applicable"}
SCHEMA_VERSION = "source_message_identity_annotation_v0001"


class IdentityError(RuntimeError):
    pass


# ============================================================
# 工具函数区
# ============================================================

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_jsonl(path):
    rows = []
    with Path(path).open(encoding="utf-8-sig") as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise IdentityError(f"line {number} is not an object")
                rows.append(row)
    if not rows:
        raise IdentityError("empty input")
    return rows


def write_output(path, text):
    path = Path(path).resolve()
    if path.exists():
        raise IdentityError(f"refuse to overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temp.write_text(text, encoding=DEFAULT_ENCODING, newline="\n")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def write_json(path, value):
    write_output(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def key(row):
    values = tuple(row.get(f) for f in KEY_FIELDS)
    if any(not isinstance(v, str) or not v for v in values[:2]):
        raise IdentityError("invalid asset/path")
    if any(type(v) is not int or v < 0 for v in values[2:]) or values[-1] < values[-2]:
        raise IdentityError("invalid text coordinates")
    return values


def connect(db, *, readonly=True):
    db = Path(db).resolve()
    if not db.is_file():
        raise IdentityError(f"database missing: {db}")
    conn = sqlite3.connect(db.as_uri() + ("?mode=ro" if readonly else "?mode=rw"), uri=True)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def validate_payload(row):
    if any(field not in row for field in FIELDS):
        raise IdentityError("missing identity fields")
    status = row["message_identity_status"]
    if status not in STATUSES:
        raise IdentityError("invalid identity status")
    if status == "not_applicable":
        if any(row[f] is not None for f in FIELDS if f != "message_identity_status"):
            raise IdentityError("not_applicable contains identity facts")
        return
    if status == "resolved" and row["message_role"] not in ROLES:
        raise IdentityError("resolved identity needs a known role")
    for field in FIELDS:
        if field not in {"message_identity_evidence"} and row[field] is not None and not isinstance(row[field], str):
            raise IdentityError(f"invalid field type: {field}")
    evidence = row["message_identity_evidence"]
    if status == "resolved":
        if not row["identity_message_id"] or not row["source_identity_annotation_id"] or not row["message_identity_rule_version"]:
            raise IdentityError("resolved identity lacks traceability")
        if not isinstance(evidence, dict) or evidence.get("semantics") != "source_recorded_identity":
            raise IdentityError("resolved identity lacks source evidence")
        if not isinstance(evidence.get("source_ref"), dict) or not evidence["source_ref"].get("source_path"):
            raise IdentityError("resolved identity lacks source path")
        fields = evidence.get("fields", {})
        for name, output in (("role", "message_role"), ("author_name", "message_author_name"),
                             ("model_slug", "message_model_slug"), ("default_model_slug", "message_default_model_slug")):
            fact = fields.get(name)
            if not isinstance(fact, dict) or fact.get("status") not in {"recorded", "missing", "invalid"}:
                raise IdentityError(f"invalid fact status: {name}")
            value = row[output]
            if fact["status"] == "recorded" and (not isinstance(value, str) or not value or fact.get("value") != value):
                raise IdentityError(f"recorded fact mismatch: {name}")
            if fact["status"] != "recorded" and value is not None:
                raise IdentityError(f"unverified fact promoted: {name}")


def payload(row):
    validate_payload(row)
    return tuple(canonical(row[f]) if f == "message_identity_evidence" and row[f] is not None else row[f] for f in FIELDS)


def cli_result(operation):
    try:
        result = operation()
        print(canonical(result))
        return 0 if result.get("status") != "failed" else 2
    except IdentityError as exc:
        print(canonical({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2
    except Exception as exc:
        print(canonical({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 3
