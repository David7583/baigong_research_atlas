#!/usr/bin/env python3
# ============================================================
# filename: register_structural_units_v0003.py
# 中文名: 结构单元制度登记脚本
# version: v0003
# layer: registration
# main_layer: action
# 可更新: True
#
# 职责说明:
# - 将已通过制度裁决（decision == ALLOW）的全局候选结构单元
#   登记为系统级制度对象（structural unit）
# - 生成两套 ID:
#   1. unit_id: SHA1(unit_text)，对齐 analyze v0003 / builder v0002 的边数据
#   2. structural_unit_id: "SU_v0002_" + SHA256(unit_text)[:12]，register 内部制度身份
# - 生成行动层 v0002 唯一有效的 registry
#
# 本脚本做什么:
# - 读取 decide_unit_prominence v0003 的全局单文件输出
# - 对 decision == ALLOW 的结构单元进行登记
# - 对结构单元进行类型级去重（按 unit_text）
# - 覆盖式生成 registry，并自动归档旧版本
#
# 本脚本不做什么:
# - 不进行结构分析、统计计算或语义判断
# - 不修改任何结构单元内容
# - 不存储 occurrence、位置、关系或 embedding
# - 不参与图构建
#
# v0002 相对 v0001 的变更:
# - 输入从 per-conversation 改为全局单文件
# - 新增 unit_id 字段（SHA1），对齐 analyze v0003 边数据的 unit_id 体系
# - structural_unit_id 保留为 register 内部制度身份（SHA256 截断）
# - decision_source 更新为 decide_unit_prominence_v0004
# - source_pipeline 更新为 action_v0002
# - utc_now_iso 改用 datetime.now(timezone.utc)
# - 新增 --verbose 进度输出
# - 流式写入 + 原子替换
# - v0001 保留不动，SQL 入库管线仍可使用
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: register_structural_units
# family: structural_unit_registry
# role: registry
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/register_structural_units_v0003.py
# input:
#   - decide_unit_prominence v0003 全局输出（单文件）
# output:
#   - registered_structural_units_v0002.jsonl（全局单文件）
# depends_on:
#   - decide_unit_prominence_v0004.py
# used_by:
#   - build_unit_cooccurrence_graph_v0003.py
#   - build_unit_adjacency_graph_v0003.py
#   - sync_to_graph_v0002.py
# ============================================================


# ============================================================
# Imports
# ============================================================
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


# ============================================================
# 常量
# ============================================================
SCRIPT_NAME = "register_structural_units_v0003.py"
SCRIPT_VERSION = "v0003"

STRUCTURAL_UNIT_ID_PREFIX = "SU_v0002_"

REGISTRY_FILENAME = "registered_structural_units_v0002.jsonl"
ARCHIVE_DIRNAME = "archive"

PROGRESS_INTERVAL = 20000


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha1_hex(text: str) -> str:
    """SHA1 hash，对齐 analyze v0003 / builder v0002 的 unit_id 体系"""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def sha256_hex(text: str) -> str:
    """SHA256 hash，用于 register 内部 structural_unit_id"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_jsonl_stream(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# ============================================================
# 归档逻辑
# ============================================================
def archive_existing_registry(registry_path: Path) -> Optional[Path]:
    if not registry_path.exists():
        return None

    archive_dir = registry_path.parent / ARCHIVE_DIRNAME
    ensure_dir(archive_dir)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived_path = archive_dir / f"{registry_path.stem}__{ts}{registry_path.suffix}"

    shutil.move(str(registry_path), str(archived_path))
    return archived_path


# ============================================================
# 核心逻辑
# ============================================================
def run(
    input_path: Path,
    output_dir: Path,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:

    started_at = utc_now_iso()

    registry_path = output_dir / REGISTRY_FILENAME
    tmp_path = registry_path.with_suffix(".jsonl.tmp")
    run_meta_path = output_dir / "run_meta.json"

    counters = {
        "rows_read": 0,
        "rows_allow": 0,
        "rows_delay": 0,
        "rows_freeze": 0,
        "rows_skipped": 0,
        "duplicates_skipped": 0,
        "registered": 0,
        "rows_written": 0,
    }

    # 用 set 做去重，dict 存 registry 条目
    seen_texts: set = set()
    registry_rows: List[Dict[str, Any]] = []

    if verbose:
        print(f"[REGISTER] Reading decisions: {input_path}", file=sys.stderr)

    for record in read_jsonl_stream(input_path):
        counters["rows_read"] += 1

        decision = record.get("decision")
        if decision != "ALLOW":
            if decision == "DELAY":
                counters["rows_delay"] += 1
            elif decision == "FREEZE":
                counters["rows_freeze"] += 1
            else:
                counters["rows_skipped"] += 1
            continue

        counters["rows_allow"] += 1

        unit_text = record.get("unit_text")
        if not isinstance(unit_text, str) or not unit_text.strip():
            counters["rows_skipped"] += 1
            continue

        ut = unit_text.strip()

        # 类型级去重
        if ut in seen_texts:
            counters["duplicates_skipped"] += 1
            continue
        seen_texts.add(ut)

        # 双 ID 体系
        unit_id = sha1_hex(ut)  # 对齐 analyze v0003 / builder v0002
        fingerprint = sha256_hex(ut)
        structural_unit_id = STRUCTURAL_UNIT_ID_PREFIX + fingerprint[:12]

        entry = {
            "unit_id": unit_id,
            "structural_unit_id": structural_unit_id,
            "unit_text": ut,
            "unit_canonical_form": ut,
            "fingerprint": fingerprint,
            "structure_signature": None,  # v0002 占位字段
            "source_pipeline": "action_v0002",
            "registered_at": utc_now_iso(),
            "decision_source": "decide_unit_prominence_v0004",
            "trace": {
                "decision_policy_version": record.get("decision_policy_version"),
                "audit_confidence": record.get("audit_confidence"),
                "structural_role": record.get("structural_role"),
                "boundary_status": record.get("decision_basis", {}).get("boundary_status"),
            },
        }

        registry_rows.append(entry)
        counters["registered"] += 1

        if verbose and counters["rows_read"] % PROGRESS_INTERVAL == 0:
            print(
                f"[REGISTER] Progress: {counters['rows_read']} rows read, "
                f"{counters['registered']} registered",
                file=sys.stderr,
            )

    if verbose:
        print(
            f"[REGISTER] Scan done: {counters['rows_read']} rows, "
            f"{counters['rows_allow']} ALLOW, "
            f"{counters['registered']} registered "
            f"({counters['duplicates_skipped']} duplicates skipped)",
            file=sys.stderr,
        )

    if dry_run:
        meta = {
            "status": "ok",
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "started_at": started_at,
            "generated_at": utc_now_iso(),
            "dry_run": True,
            "input": str(input_path),
            "counters": counters,
        }
        return meta

    # 写入
    ensure_dir(output_dir)
    with tmp_path.open("w", encoding="utf-8", newline="\n") as f:
        for entry in registry_rows:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            counters["rows_written"] += 1

    # 归档旧版本
    archived_path = archive_existing_registry(registry_path)

    # 原子替换
    os.replace(str(tmp_path), str(registry_path))

    if verbose:
        print(
            f"[REGISTER] Written: {counters['rows_written']} entries to {registry_path}",
            file=sys.stderr,
        )
        if archived_path:
            print(f"[REGISTER] Previous registry archived to: {archived_path}", file=sys.stderr)

    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": utc_now_iso(),
        "input": str(input_path),
        "output": {
            "registry": str(registry_path),
            "run_meta": str(run_meta_path),
        },
        "counters": counters,
        "archived_registry": str(archived_path) if archived_path else None,
        "id_scheme": {
            "unit_id": "SHA1(unit_text) — aligned with analyze v0003 / builder v0002",
            "structural_unit_id": "SU_v0002_ + SHA256(unit_text)[:12] — register internal identity",
        },
        "known_limitations": [
            "v0002 reads decide_unit_prominence v0003 global output.",
            "v0002 only registers decision=ALLOW candidates.",
            "v0002 deduplicates by unit_text (exact match).",
            "v0002 unit_id uses SHA1 to align with analyze v0003 edge data.",
            "v0002 structure_signature is a placeholder field.",
        ],
    }

    with run_meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# ============================================================
# CLI
# ============================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Register structural units from global prominence decisions. "
                    "v0002: global single-file input, dual ID scheme (SHA1 + SHA256)."
    )
    p.add_argument(
        "--input", required=True, type=Path,
        help="Input JSONL: decide_unit_prominence v0003 全局输出（单文件）",
    )
    p.add_argument(
        "--output-dir", type=Path,
        default="actioning/pipelines/register_structural_units_action",
        help="Directory to store registry JSONL and run_meta",
    )
    p.add_argument("--dry-run", action="store_true", help="Dry run: 不写文件")
    p.add_argument("--verbose", action="store_true", help="输出处理进度到 stderr")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.input.exists():
        print(f"[ERROR] Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    meta = run(
        input_path=args.input,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    print(json.dumps(meta, ensure_ascii=False, indent=2))


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    main()
