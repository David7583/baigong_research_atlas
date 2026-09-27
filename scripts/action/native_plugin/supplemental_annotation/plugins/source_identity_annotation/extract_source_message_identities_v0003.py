# ============================================================
# 文件名: extract_source_message_identities_v0003.py
# 中文名: 获授权消息身份提取
# 版本号: v0003
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / source_identity
# 脚本定位: 获授权消息身份提取
#
# 职责说明:
# - 获授权消息身份提取
#
# 本脚本做什么:
# - 按解析器全部字符串 text 字段的坐标顺序恢复消息身份并输出完整覆盖证据
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
# alias: extract_source_message_identities_v0003
# family: extract_source_message_identities
# role: authorized_source_message_identity_extractor
# version: v0003
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/source_identity_annotation/extract_source_message_identities_v0003.py
# input:
#   - authorized source access manifest and canonical conversations
# output:
#   - identity annotations, unavailable field issues and manifest
# depends_on:
#   - Python standard library
#   - source_access_gateway_v0001
#   - source_message_identity_contract_v0001
# used_by:
#   - data_action_chain_pipeline_v0015
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from source_message_identity_contract_v0001 import (
    FIELDS,
    ROLES,
    SCHEMA_VERSION,
    IdentityError,
    canonical,
    cli_result,
    read_jsonl,
    sha,
    write_json,
    write_output,
)

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "extract_source_message_identities"
SCRIPT_NAME = "extract_source_message_identities_v0003"
SCRIPT_VERSION = "v0003"
TEXT_PATH = "/conversations/*/nodes/*/message/content/parts/*/text"


# ============================================================
# 工具函数区
# ============================================================


def root_from(start):
    for parent in (start, *start.parents):
        if (
            (parent / "AGENTS.md").is_file()
            and (parent / "config").is_dir()
            and (parent / "scripts").is_dir()
        ):
            return parent
    raise IdentityError("project root not found")


def authorized_path(root, reference):
    path = (root / reference["project_relative_path"]).resolve()
    if not path.is_relative_to(root.resolve()) or sha(path) != reference["sha256"]:
        raise IdentityError("source boundary/hash mismatch")
    return path


def fact(value, pointer):
    status = (
        "missing" if value is None or value == "" else "recorded" if isinstance(value, str) else "invalid"
    )
    return {
        "raw_value": value,
        "value": value if status == "recorded" else None,
        "status": status,
        "canonical_path": pointer,
    }


def message_identity(message, node, canonical_message_path):
    author = message.get("author") or {}
    metadata = message.get("metadata") or {}
    if not isinstance(author, dict) or not isinstance(metadata, dict):
        raise IdentityError("author/metadata must be objects")
    role = author.get("role")
    fields = {
        "role": fact(role if role in ROLES else None, canonical_message_path + "/author/role"),
        "author_name": fact(author.get("name"), canonical_message_path + "/author/name"),
        "model_slug": fact(metadata.get("model_slug"), canonical_message_path + "/metadata/model_slug"),
        "default_model_slug": fact(
            metadata.get("default_model_slug"), canonical_message_path + "/metadata/default_model_slug"
        ),
    }
    fields["role"]["raw_value"] = author.get("source_role", role)
    return {
        "message_role": fields["role"]["value"],
        "message_author_name": fields["author_name"]["value"],
        "message_model_slug": fields["model_slug"]["value"],
        "message_default_model_slug": fields["default_model_slug"]["value"],
        "message_identity_status": "resolved" if role in ROLES else "missing" if role is None else "invalid",
        "message_identity_rule_version": "canonical_message_identity_v0001",
        "identity_message_id": message.get("message_id"),
        "identity_mapping_node_id": node.get("node_id"),
        "message_identity_evidence": {
            "semantics": "source_recorded_identity",
            "source_ref": message.get("source_ref"),
            "fields": fields,
        },
    }


def extract_authorized(access_path, root):
    access = json.loads(Path(access_path).read_text(encoding=DEFAULT_ENCODING))
    if (
        access.get("schema_version") != "supplemental_annotation_source_access_v0001"
        or access.get("status") != "authorized"
        or (access.get("authorization") or {}).get("authorized") is not True
    ):
        raise IdentityError("source access not authorized")
    source_path = authorized_path(root, access["canonical_source"])
    allowed_path = authorized_path(root, access["allowed_coordinates"])
    source = json.loads(source_path.read_text(encoding=DEFAULT_ENCODING))
    source_sha256 = sha(source_path)
    if source.get("schema_version") != "canonical_conversation_ingress_v0001":
        raise IdentityError("unsupported canonical schema")
    coordinates = read_jsonl(allowed_path)
    allowed = set()
    for row in coordinates:
        if type(row.get("value_index")) is not int or row["value_index"] < 0:
            raise IdentityError("invalid allowed coordinate")
        coord = (row["asset_id"], row["path"], row["value_index"], row["source_value_sha1"])
        if coord in allowed:
            raise IdentityError("duplicate allowed coordinate")
        allowed.add(coord)
    assets = {r[0] for r in allowed}
    if len(assets) != 1:
        raise IdentityError("exactly one authorized asset required")
    asset = next(iter(assets))
    rows, issues, index = [], [], 0
    for ci, conversation in enumerate(source.get("conversations", [])):
        for ni, node in enumerate(conversation.get("nodes", [])):
            message = node.get("message")
            if not isinstance(message, dict):
                continue
            pointer = f"/conversations/{ci}/nodes/{ni}/message"
            identity = message_identity(message, node, pointer)
            for pi, part in enumerate((message.get("content") or {}).get("parts", [])):
                if not isinstance(part, dict) or not isinstance(part.get("text"), str):
                    continue
                # value_index follows every string /parts/*/text, including code.
                text_hash = hashlib.sha1(part["text"].encode(DEFAULT_ENCODING)).hexdigest()
                if (asset, TEXT_PATH, index, text_hash) in allowed:
                    evidence = {
                        **identity["message_identity_evidence"],
                        "source_access_id": access.get("access_id"),
                        "canonical_source_sha256": source_sha256,
                    }
                    row = {
                        **identity,
                        "message_identity_evidence": evidence,
                        "schema_version": SCHEMA_VERSION,
                        "asset_id": asset,
                        "target_path": TEXT_PATH,
                        "value_index": index,
                        "source_value_sha1": text_hash,
                        "canonical_text_path": pointer + f"/content/parts/{pi}/text",
                        "source_text_path": (part.get("source_ref") or {}).get("source_path"),
                    }
                    annotation_id = (
                        "sha256:" + hashlib.sha256(canonical(row).encode(DEFAULT_ENCODING)).hexdigest()
                    )
                    row["annotation_id"] = annotation_id
                    row["source_identity_annotation_id"] = annotation_id
                    rows.append(row)
                    missing = [k for k, v in evidence["fields"].items() if v["status"] != "recorded"]
                    if missing:
                        issues.append({"annotation_id": annotation_id, "unavailable_fields": missing})
                index += 1
    matching = sum(c[1] == TEXT_PATH for c in allowed)
    if not rows or len(rows) != matching:
        raise IdentityError("authorized text coordinate coverage mismatch")
    return {
        "rows": rows,
        "issues": issues,
        "source_sha256": source_sha256,
        "source_access_id": access.get("access_id"),
    }


# ============================================================
# CLI / main 接口区
# ============================================================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-access-manifest", required=True)
    parser.add_argument("--annotations", required=True)
    parser.add_argument("--issues", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    def run():
        root = root_from(Path(__file__).resolve().parent)
        outputs = [Path(p).resolve() for p in (args.annotations, args.issues, args.manifest)]
        if len(set(outputs)) != 3 or any(not p.is_relative_to(root) or p.exists() for p in outputs):
            raise IdentityError("output must be new, distinct and within project")
        result = extract_authorized(Path(args.source_access_manifest).resolve(), root)
        report = {
            "status": "dry_run" if args.dry_run else "completed",
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "records_created": len(result["rows"]),
            "source_sha256": result["source_sha256"],
            "source_access_id": result["source_access_id"],
        }
        if not args.dry_run:
            write_output(outputs[0], "".join(canonical(r) + "\n" for r in result["rows"]))
            write_output(outputs[1], "".join(canonical(r) + "\n" for r in result["issues"]))
            report["annotations_sha256"] = sha(outputs[0])
            write_json(outputs[2], report)
        return report

    return cli_result(run)


if __name__ == "__main__":
    raise SystemExit(main())
