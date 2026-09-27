#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# 文件名: import_conversation_slices_to_processed_v0002.py
# 中文名: 规范对话切片数据导入与承认脚本
# 版本号: v0002
#
# 主层级: data
# 层级: scripts / data_admission
# 脚本定位: 将完成入口契约解析的对话切片导入 data_processed
#
# 职责说明:
# - 校验入口信封与每个规范切片的来源身份
# - 原样复制切片并生成不绑定 Provider 的准入清单
#
# 本脚本做什么:
# - 使用标准库验证信封、切片身份和逐文件哈希
# - 在新版本目录中写入连续编号副本与 manifest.json
#
# 本脚本不做什么:
# - 不识别原始格式，不调用适配器，不修改对话字段
# - 不覆盖既有资产版本，不删除任何输入
#
# 制度边界声明:
# - 只有 status=completed 且 contract_valid=true 的入口信封可准入
# - 切片 source_envelope_id 必须全部与入口信封一致
# - dry-run 不创建目录或文件
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: import_conversation_slices_to_processed_v0002
# family: data_import
# role: canonical_conversation_asset_importer
# version: v0002
# status: experimental
# entry_point: scripts/import_conversation_slices_to_processed_v0002.py
# input:
#   - canonical conversation slices
#   - canonical ingress envelope
# output:
#   - admitted conversation dataset and source-neutral manifest
# depends_on:
#   - Python standard library
# used_by:
#   - data_admission_lineage_pipeline_v0003
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


SCRIPT_NAME = "import_conversation_slices_to_processed_v0002.py"
SCRIPT_VERSION = "v0002"
ENVELOPE_SCHEMA = "canonical_ingress_envelope_v0001"
SLICE_SCHEMA = "canonical_conversation_slice_v0001"
DEFAULT_NAMING_PATTERN = "conv_{index:06d}.json"
EXCLUDE_FILENAMES = {"slice_manifest.json", "manifest.json"}


class CanonicalImportError(RuntimeError):
    """Raised when canonical slices cannot be admitted safely."""


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_object(path: Path, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise CanonicalImportError(f"{label} is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanonicalImportError(f"{label} is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise CanonicalImportError(f"{label} must be a JSON object")
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _list_slice_files(source_dir: Path) -> List[Path]:
    return sorted(
        path for path in source_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".json" and path.name not in EXCLUDE_FILENAMES
    )


def _validate_envelope(path: Path) -> Dict[str, Any]:
    envelope = _load_object(path, "ingress envelope")
    if envelope.get("schema_version") != ENVELOPE_SCHEMA:
        raise CanonicalImportError(f"ingress envelope schema_version must be {ENVELOPE_SCHEMA}")
    if envelope.get("status") != "completed":
        raise CanonicalImportError("ingress envelope status must be completed")
    canonical = envelope.get("canonical_output")
    if not isinstance(canonical, Mapping) or canonical.get("contract_valid") is not True:
        raise CanonicalImportError("ingress envelope canonical contract is not valid")
    source = envelope.get("source")
    if not isinstance(source, Mapping) or source.get("immutable_verified") is not True:
        raise CanonicalImportError("ingress envelope did not verify immutable source")
    return envelope


def _validate_slice(path: Path, envelope_id: str) -> Dict[str, Any]:
    record = _load_object(path, "conversation slice")
    meta = record.get("_meta")
    conversation = record.get("conversation")
    if not isinstance(meta, Mapping) or meta.get("schema_version") != SLICE_SCHEMA:
        raise CanonicalImportError(f"slice has invalid _meta schema: {path}")
    if meta.get("source_envelope_id") != envelope_id:
        raise CanonicalImportError(f"slice source_envelope_id mismatch: {path}")
    if not isinstance(conversation, Mapping):
        raise CanonicalImportError(f"slice conversation must be an object: {path}")
    if conversation.get("conversation_id") != meta.get("conversation_id"):
        raise CanonicalImportError(f"slice conversation identity mismatch: {path}")
    return dict(meta)


def run_import(
    *,
    source_task_id: str,
    source_dir: Path,
    ingress_envelope: Path,
    target_root: Path,
    version: str,
    dry_run: bool,
) -> Dict[str, Any]:
    source_dir = source_dir.resolve()
    target_root = target_root.resolve()
    if not source_dir.is_dir():
        raise CanonicalImportError(f"source slices directory not found: {source_dir}")
    envelope = _validate_envelope(ingress_envelope.resolve())
    envelope_id = envelope.get("envelope_id")
    if not isinstance(envelope_id, str) or not envelope_id:
        raise CanonicalImportError("ingress envelope is missing envelope_id")
    slice_files = _list_slice_files(source_dir)
    if not slice_files:
        raise CanonicalImportError("no conversation slice JSON files found")
    source_hashes: List[Dict[str, Any]] = []
    for source_file in slice_files:
        meta = _validate_slice(source_file, envelope_id)
        source_hashes.append({
            "source_name": source_file.name,
            "conversation_id": meta["conversation_id"],
            "sha256": _sha256_file(source_file),
        })
    target_version_dir = target_root / version
    target_data_dir = target_version_dir / "data"
    if target_version_dir.exists():
        raise CanonicalImportError(f"target version already exists: {target_version_dir}")
    admitted: List[Dict[str, Any]] = []
    for index, (source_file, source_record) in enumerate(zip(slice_files, source_hashes), start=1):
        destination_name = DEFAULT_NAMING_PATTERN.format(index=index)
        destination_path = target_data_dir / destination_name
        if not dry_run:
            target_data_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, destination_path)
            destination_hash = _sha256_file(destination_path)
            if destination_hash != source_record["sha256"]:
                raise CanonicalImportError(f"copied slice hash mismatch: {destination_path}")
        else:
            destination_hash = source_record["sha256"]
        admitted.append({
            **source_record,
            "admitted_name": destination_name,
            "admitted_sha256": destination_hash,
        })
    detection = envelope.get("detection", {})
    adapter = envelope.get("adapter", {})
    manifest = {
        "schema_version": "canonical_conversation_asset_manifest_v0001",
        "asset_type": "conversation_collection",
        "version": version,
        "imported_at": _utc_now_z(),
        "imported_by": SCRIPT_NAME,
        "source_envelope": {
            "envelope_id": envelope_id,
            "path": str(ingress_envelope.resolve()),
            "schema_version": envelope["schema_version"],
            "source_file_id": envelope["source"]["source_file_id"],
            "format_family": detection.get("format_family"),
            "fingerprint_version": detection.get("fingerprint_version"),
            "adapter_family": adapter.get("family"),
            "adapter_version": adapter.get("version"),
        },
        "source_task": {
            "task_id": source_task_id,
            "source_dir": str(source_dir),
            "slice_count": len(slice_files),
        },
        "storage": {"format": "json-per-conversation", "naming": DEFAULT_NAMING_PATTERN},
        "integrity": {
            "hash_algorithm": "sha256",
            "per_file_hash": True,
            "source_and_admitted_hashes_equal": True,
            "files": admitted,
        },
        "notes": "Provider-neutral canonical conversation dataset admitted after complete ingress contract validation.",
    }
    if not dry_run:
        (target_version_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return {
        "status": "dry_run" if dry_run else "ok",
        "version": version,
        "imported_count": len(slice_files),
        "target_dir": str(target_version_dir),
        "source_envelope_id": envelope_id,
        "dry_run": dry_run,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Admit canonical conversation slices with an exact ingress envelope.")
    parser.add_argument("--source-task", required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--ingress-envelope", required=True)
    parser.add_argument("--target-root", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = run_import(
            source_task_id=args.source_task,
            source_dir=Path(args.source_dir),
            ingress_envelope=Path(args.ingress_envelope),
            target_root=Path(args.target_root),
            version=args.version,
            dry_run=bool(args.dry_run),
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
