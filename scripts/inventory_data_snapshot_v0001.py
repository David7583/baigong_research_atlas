#!/usr/bin/env python3
# ============================================================
# File: inventory_data_snapshot_v0001.py
# 中文名: 数据层盘点脚本
# Version: v0001
# Layer: execution
# Main Layer: Data
# Updatable: True
#
# 职责说明:
# 本脚本用于在某一确定时间点，对 data 目录进行一次
# 无判断、无筛选、无副作用的完整盘点扫描，
# 并生成可审计、不可变的事实快照。
#
# 本脚本做什么:
# - 遍历 data 目录下所有可访问文件
# - 记录文件系统层面的客观属性
# - 冻结为一次性盘点结果文件
#
# 本脚本不做什么:
# - 不调用任何寻找脚本（内寻 / 外寻）
# - 不比较文件之间的关系
# - 不判断文件是否重复、是否有用
# - 不修改、移动、删除任何对象
# - 不进行登记、清洗或业务解释
#
# 制度属性说明:
# - 本脚本属于事实层生成脚本
# - 不进行制度判断
# - 不产生业务语义
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: inventory_data_snapshot
# family: inventory_data_snapshot
# role: data_inventory_snapshot
# version: v0001
# status: active
# entry_point: inventory_data_snapshot_v0001.py
# input: data directory
# output: inventory snapshot file (jsonl)
# depends_on: []
# used_by: [registration, audit, analysis]
# ============================================================

# ============================================================
# 制度与职责边界声明
# ============================================================
# - 本脚本仅生成“系统看到的事实”
# - 输出结果不具备制度承认效力
# - 后续任何登记、清洗、分析行为
#   均不得反向修改本脚本输出
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

HASH_CHUNK_SIZE = 8192
DEFAULT_DATA_DIR = "data"
DEFAULT_OUTPUT_DIR = "data/reports/inventory"
RUN_LOG_PATH = "scripts/_logs/inventory_data_snapshot_runs.jsonl"
PROGRESS_INTERVAL = 1000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_sha256(path: Path) -> Optional[str]:
    try:
        sha256 = hashlib.sha256()
        with path.open("rb") as f:
            while True:
                chunk = f.read(HASH_CHUNK_SIZE)
                if not chunk:
                    break
                sha256.update(chunk)
        return f"sha256:{sha256.hexdigest()}"
    except Exception:
        return None


def iter_files(base_dir: Path) -> Iterator[Path]:
    for root, _, files in os.walk(base_dir):
        for name in files:
            yield Path(root) / name


def write_run_log(entry: dict) -> None:
    log_path = Path(RUN_LOG_PATH)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()


def run_inventory(
    *,
    run_id: str,
    data_dir: Path,
    output_path: Path,
    compute_hash: bool,
    max_hash_bytes: Optional[int],
) -> None:
    observed_at = utc_now_iso()
    scanned = 0
    tmp_path = output_path.with_suffix(".jsonl.tmp")

    with tmp_path.open("w", encoding="utf-8") as out:
        for file_path in iter_files(data_dir):
            scanned += 1
            if scanned % PROGRESS_INTERVAL == 0:
                print(f"[INFO] scanned {scanned} files...", file=sys.stderr)

            record = {
                "inventory_run_id": run_id,
                "observed_at_utc": observed_at,
                "path_absolute": str(file_path.resolve()),
                "path_relative": str(file_path.relative_to(data_dir)),
                "object_type": "file",
                "size_bytes": None,
                "mtime_utc": None,
                "ctime_utc": None,
                "content_hash": None,
                "hash_status": "disabled" if not compute_hash else None,
                "read_status": "ok",
            }

            try:
                stat = file_path.stat()
                record["size_bytes"] = stat.st_size
                record["mtime_utc"] = datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                record["ctime_utc"] = datetime.fromtimestamp(
                    stat.st_ctime, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

                if compute_hash:
                    if max_hash_bytes is not None and stat.st_size > max_hash_bytes:
                        record["hash_status"] = "skipped_size"
                    else:
                        record["content_hash"] = compute_sha256(file_path)
                        record["hash_status"] = (
                            "computed" if record["content_hash"] else "error"
                        )

            except Exception as e:
                record["read_status"] = f"error:{type(e).__name__}"

            out.write(json.dumps(record, ensure_ascii=False) + "\n")

    tmp_path.replace(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a factual inventory snapshot of the data directory."
    )
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--no-hash", action="store_true")
    parser.add_argument("--max-hash-bytes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    start_time = utc_now_iso()
    run_id = str(uuid.uuid4())
    args = parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists() or not data_dir.is_dir():
        print(f"[ERROR] data directory not found: {data_dir}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = output_dir / f"inventory_{timestamp}.jsonl"

    status = "success"
    try:
        run_inventory(
            run_id=run_id,
            data_dir=data_dir,
            output_path=output_file,
            compute_hash=not args.no_hash,
            max_hash_bytes=args.max_hash_bytes,
        )
    except Exception:
        status = "failure"
        raise
    finally:
        end_time = utc_now_iso()
        write_run_log(
            {
                "run_id": run_id,
                "script": "inventory_data_snapshot_v0001.py",
                "start_time": start_time,
                "end_time": end_time,
                "data_dir": str(data_dir),
                "output_file": str(output_file),
                "params": {
                    "no_hash": args.no_hash,
                    "max_hash_bytes": args.max_hash_bytes,
                    "output_dir": str(output_dir),
                },
                "status": status,
            }
        )

    print(f"[OK] Inventory snapshot created: {output_file}")
