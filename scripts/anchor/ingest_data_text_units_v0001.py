#!/usr/bin/env python3
# ============================================================
# 文件名: ingest_data_text_units_v0001.py
# 中文名: 数据层文本单元入库脚本
# 版本号: v0001
#
# 主层级: data
# 子层级: ingestion
# 脚本定位: 将 language_parse_lite 产出的 text_units JSONL 写入数据层 SQLite
# 可更新: True
#
# 职责说明:
# - 读取 text_units JSONL（language_parse_lite 的产出）
# - 校验必需字段
# - 写入 data.db 的 data_text_units 表
# - 写入 data_run_log 审计记录
#
# 本脚本不做什么:
# - 不做 canonicalize、不生成 id、不做裁决
# - 不操作理解层或行动层的数据库
# - 不修改输入数据的任何字段
# - 不做语义判断
#
# 统计守恒:
# input_records = inserted_records + skipped_duplicate + invalid_records
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingest_data_text_units
# family: data_ingestion
# role: data_layer_writer
# version: v0001
# status: active
# entry_point: ingest_data_text_units_v0001.py
# input:
#   - text_units JSONL (from language_parse_lite)
# output:
#   - rows in data.db data_text_units table
#   - run_meta JSON
# depends_on:
#   - init_data_schema_v0001.py (table must exist)
# used_by:
#   - verify_data_anchor_v0001.py (queries the table)
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCRIPT_NAME = "ingest_data_text_units_v0001.py"
SCRIPT_VERSION = "v0001"

TABLE_NAME = "data_text_units"
RUN_LOG_TABLE = "data_run_log"

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

REQUIRED_FIELDS = (
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
    "text",
)

INSERT_COLUMNS = (
    "asset_id",
    "path",
    "value_index",
    "segment_index",
    "sentence_index",
    "char_start",
    "char_end",
    "text",
    "source_value_sha1",
    "parse_version",
    "unit_type",
    "ingested_at",
)


# ============================================================
# 工具函数区（无副作用）
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_inputs_fingerprint(paths: List[Path]) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    combined = hashlib.sha256()
    for p in paths:
        sha = file_sha256(p)
        st = p.stat()
        items.append({"path": str(p), "size_bytes": st.st_size, "sha256": sha})
        combined.update(p.as_posix().encode("utf-8"))
        combined.update(b"\n")
        combined.update(sha.encode("utf-8"))
        combined.update(b"\n")
    return {"count": len(paths), "combined_sha256": combined.hexdigest(), "files": items}


def read_jsonl(path: Path) -> Iterable[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield i, obj
                else:
                    yield i, {"__parse_error__": True}
            except Exception:
                yield i, {"__parse_error__": True}


# ============================================================
# 核心业务逻辑区
# ============================================================

def validate_record(rec: Dict[str, Any]) -> Optional[str]:
    """校验必需字段，返回错误原因或 None"""
    for f in REQUIRED_FIELDS:
        v = rec.get(f)
        if v is None:
            return f"missing_{f}"
        if isinstance(v, str) and not v.strip():
            return f"empty_{f}"

    # 数值字段校验
    for f in ("value_index", "segment_index", "sentence_index", "char_start", "char_end"):
        if safe_int(rec.get(f)) is None:
            return f"invalid_int_{f}"

    return None


def build_row(rec: Dict[str, Any], ingested_at: str) -> Tuple:
    return (
        str(rec["asset_id"]),
        str(rec["path"]),
        int(rec["value_index"]),
        int(rec["segment_index"]),
        int(rec["sentence_index"]),
        int(rec["char_start"]),
        int(rec["char_end"]),
        str(rec["text"]),
        rec.get("source_value_sha1"),
        rec.get("parse_version"),
        rec.get("unit_type"),
        ingested_at,
    )


def ingest(
    input_paths: List[Path],
    db_path: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:

    ingested_at = utc_now_iso()
    started_at = ingested_at
    run_id = f"data_ingest_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    input_records = 0
    inserted_records = 0
    skipped_duplicate = 0
    invalid_records = 0
    issues: List[Dict[str, Any]] = []

    rows: List[Tuple] = []

    for path in input_paths:
        for line_no, rec in read_jsonl(path):
            input_records += 1

            if rec.get("__parse_error__"):
                invalid_records += 1
                issues.append({"type": "json_parse_error", "file": str(path), "line": line_no})
                continue

            error = validate_record(rec)
            if error:
                invalid_records += 1
                issues.append({"type": error, "file": str(path), "line": line_no})
                continue

            rows.append(build_row(rec, ingested_at))

    if dry_run:
        return {
            "status": "dry-run",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "generated_at": utc_now_iso(),
            "stats": {
                "input_records": input_records,
                "valid_records": len(rows),
                "invalid_records": invalid_records,
            },
            "preview_first_3": [
                dict(zip(INSERT_COLUMNS, r)) for r in rows[:3]
            ],
            "issues_count": len(issues),
        }

    # 写入 SQLite
    if not db_path.exists():
        return {
            "status": "error",
            "error": f"database not found: {db_path}",
            "hint": "run init_data_schema_v0001.py --init first",
        }

    conn = sqlite3.connect(str(db_path), timeout=30)

    try:
        conn.execute("PRAGMA foreign_keys = OFF;")
        conn.execute("PRAGMA journal_mode = WAL;")

        placeholders = ", ".join(["?"] * len(INSERT_COLUMNS))
        cols = ", ".join(INSERT_COLUMNS)
        sql = f"INSERT OR IGNORE INTO {TABLE_NAME} ({cols}) VALUES ({placeholders})"

        cur = conn.cursor()
        for row in rows:
            cur.execute(sql, row)
            if cur.rowcount > 0:
                inserted_records += 1
            else:
                skipped_duplicate += 1

        # 审计记录
        run_log_sql = (
            f"INSERT INTO {RUN_LOG_TABLE} "
            f"(run_id, script_name, script_version, started_at, finished_at, input_hash, record_count) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        finished_at = utc_now_iso()
        inputs_fp = compute_inputs_fingerprint(input_paths)
        cur.execute(run_log_sql, (
            run_id,
            SCRIPT_NAME,
            SCRIPT_VERSION,
            started_at,
            finished_at,
            inputs_fp["combined_sha256"],
            inserted_records,
        ))

        conn.commit()

        return {
            "status": "ok",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "run_id": run_id,
            "generated_at": finished_at,
            "db_path": str(db_path),
            "stats": {
                "input_records": input_records,
                "inserted_records": inserted_records,
                "skipped_duplicate": skipped_duplicate,
                "invalid_records": invalid_records,
            },
            "inputs": inputs_fp,
            "issues_count": len(issues),
        }

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            "status": "error",
            "script": SCRIPT_NAME,
            "error": str(e)[:500],
        }

    finally:
        conn.close()


# ============================================================
# CLI / main 接口区
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest text_units JSONL into data layer SQLite (data_text_units table)."
    )
    in_group = p.add_mutually_exclusive_group(required=True)
    in_group.add_argument("--inputs", nargs="+", help="Input JSONL files.")
    in_group.add_argument("--input-dir", help="Directory containing input JSONL files.")

    p.add_argument("--db", required=True, help="Path to data layer SQLite database.")
    p.add_argument("--run-meta", required=True, help="Output run meta JSON path.")
    p.add_argument("--dry-run", action="store_true", help="Dry run without writing to database.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # resolve inputs
    if args.input_dir:
        in_dir = Path(args.input_dir)
        if not in_dir.exists():
            print(safe_json({"status": "error", "error": f"input-dir not found: {args.input_dir}"}))
            return 2
        input_paths = sorted(in_dir.glob("*.jsonl"))
        if not input_paths:
            print(safe_json({"status": "error", "error": f"no jsonl files in input-dir: {args.input_dir}"}))
            return 2
    else:
        input_paths = [Path(p) for p in args.inputs]
        missing = [str(p) for p in input_paths if not p.exists()]
        if missing:
            print(safe_json({"status": "error", "error": "missing input files", "missing": missing}))
            return 2
        input_paths = sorted(input_paths)

    db_path = Path(args.db)
    run_meta_path = Path(args.run_meta)
    ensure_dir(run_meta_path.parent)

    result = ingest(
        input_paths=input_paths,
        db_path=db_path,
        dry_run=bool(args.dry_run),
    )

    # write run_meta
    if not args.dry_run and result.get("status") == "ok":
        with run_meta_path.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    print(json.dumps(result, ensure_ascii=False, indent=2))

    return 0 if result.get("status") in ("ok", "dry-run") else 1


# ============================================================
# 入口
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
