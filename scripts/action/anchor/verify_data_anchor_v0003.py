# ============================================================
# 文件名: verify_data_anchor_v0003.py
# 中文名: 完整坐标数据层锚点存在性校验脚本
# 版本号: v0003
#
# 主层级: action
# 子层级: persistence / preflight
# 脚本定位: 在写入 action SQL 之前，验证 instance.observed_at 是否在数据层 SQL 中真实存在
# 可更新: True
#
# 职责说明:
# - 读取 instance_identity_declarations JSONL（行动端实例声明）
# - 从每条 instance 记录提取完整 observed_at 八字段坐标
# - 连接数据层 SQLite 数据库，按坐标查询锚点是否存在
# - 输出结构化 anchor_report.json，用于阻断“悬浮解释”写入
#
# 本脚本做什么:
# - 不写入 SQL
# - 不修改任何表
# - 不推断，不裁决，不调用向量库
#
# 本脚本不做什么:
# - 不验证 action 层内部引用拓扑（由 validate_ingest_payload_v0003.py 负责）
# - 不负责跨库同步或迁移
#
# 制度边界声明:
# - PASS 表示所有 instance 坐标均可在数据层找到至少一条匹配记录
# - FAIL 表示存在缺失锚点或查询失败，禁止继续写入行动库
# - 本脚本的“存在性”定义由 CLI 参数指定的表与字段映射决定
#
# 统计守恒:
# - instance_input_lines = instance_valid + instance_invalid
# - anchor_checks = anchors_found + anchors_missing + anchors_error
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: verify_data_anchor
# family: verify_data_anchor
# role: data_anchor_verifier
# version: v0003
# status: active
# entry_point: scripts/action/anchor/verify_data_anchor_v0003.py
# input:
#   - instance_identity_declarations (jsonl)
#   - data layer SQLite db (--data-db / --table)
# output:
#   - anchor_report (json)
# depends_on:
#   - ingest_instance_units_v0003
#   - ingest_data_text_units_v0001
# used_by:
#   - sql_writer_v0004
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCRIPT_NAME = "verify_data_anchor_v0003.py"
SCRIPT_VERSION = "v0003"
ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_ENCODING = "utf-8"


# ============================================================
# 工具函数
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _sha256_hex_text(s: str) -> str:
    h = hashlib.sha256()
    h.update(s.encode(DEFAULT_ENCODING))
    return h.hexdigest()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _archive_existing(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    archive_dir = path.parent / ARCHIVE_DIRNAME
    _ensure_dir(archive_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived = archive_dir / f"{path.stem}__{ts}{path.suffix}"
    shutil.move(str(path), str(archived))
    return archived


def _atomic_write_json(path: Path, obj: Any) -> Tuple[int, str]:
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = _safe_json_dumps(obj).encode(DEFAULT_ENCODING)
    with tmp.open("wb") as f:
        f.write(data)
        f.write(b"\n")
    shutil.move(str(tmp), str(path))
    return len(data), _sha256_hex_text(data.decode(DEFAULT_ENCODING))


def _read_jsonl(path: Path) -> Iterable[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        for i, line in enumerate(f, 1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                yield i, {"__parse_error__": True, "__raw__": s}
                continue
            if not isinstance(obj, dict):
                yield i, {"__parse_error__": True, "__raw__": s}
                continue
            yield i, obj


def _require_non_empty_str(field: str, v: Any) -> str:
    if not isinstance(v, str) or v.strip() == "":
        raise ValueError(f"{field} must be a non-empty string")
    return v


def _coerce_int_or_none(field: str, v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError(f"{field} must be int or null, bool is not allowed")
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        return int(s)
    raise ValueError(f"{field} must be int or null")


def _new_issue(*, level: str, code: str, where: Dict[str, Any], detail: str, hint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {
        "level": level,   # ERROR | WARNING
        "code": code,
        "where": where,
        "detail": detail,
    }
    if hint is not None:
        out["hint"] = hint
    return out


# ============================================================
# 统计结构
# ============================================================

@dataclass
class Counts:
    input_lines: int = 0
    valid: int = 0
    invalid: int = 0


@dataclass
class AnchorStats:
    checks: int = 0
    found: int = 0
    missing: int = 0
    errors: int = 0


# ============================================================
# 核心校验逻辑
# ============================================================

def _extract_anchor(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    从 instance 记录中提取 observed_at 锚点坐标。
    约束:
    - observed_at 必须是对象
    - asset_id/path 必须非空字符串
    - value_index/sentence_index/char_start/char_end 允许 null 或 int
    - segment_index 必须 int
    """
    instance_id = _require_non_empty_str("instance_id", rec.get("instance_id"))

    observed_at = rec.get("observed_at")
    if not isinstance(observed_at, dict):
        raise ValueError("observed_at must be object")

    asset_id = _require_non_empty_str("observed_at.asset_id", observed_at.get("asset_id"))
    path = _require_non_empty_str("observed_at.path", observed_at.get("path"))

    value_index = _coerce_int_or_none("observed_at.value_index", observed_at.get("value_index"))

    seg = observed_at.get("segment_index")
    if not isinstance(seg, int) or isinstance(seg, bool):
        raise ValueError("observed_at.segment_index must be int")
    segment_index = int(seg)

    sentence_index = _coerce_int_or_none("observed_at.sentence_index", observed_at.get("sentence_index"))

    char_start = _coerce_int_or_none("observed_at.char_start", observed_at.get("char_start"))
    char_end = _coerce_int_or_none("observed_at.char_end", observed_at.get("char_end"))

    if char_start is not None and char_end is not None and char_end < char_start:
        raise ValueError("observed_at.char_end is less than char_start")

    return {
        "instance_id": instance_id,
        "asset_id": asset_id,
        "path": path,
        "value_index": value_index,
        "segment_index": segment_index,
        "sentence_index": sentence_index,
        "char_start": char_start,
        "char_end": char_end,
    }


def _connect_sqlite(db_path: Path, timeout: int) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    return conn


def _build_exists_sql(
    table: str,
    f_asset: str,
    f_path: str,
    f_value: str,
    f_seg: str,
    f_sentence: str,
    f_start: str,
    f_end: str,
) -> str:
    """
    生成存在性查询 SQL。
    说明:
    - 对于 char_start/char_end，使用 (col = ? OR (col IS NULL AND ? IS NULL)) 以支持 NULL 比较。
    """
    return (
        f"SELECT 1 FROM {table} "
        f"WHERE {f_asset} = ? AND {f_path} = ? "
        f"AND ({f_value} = ? OR ({f_value} IS NULL AND ? IS NULL)) "
        f"AND {f_seg} = ? "
        f"AND ({f_sentence} = ? OR ({f_sentence} IS NULL AND ? IS NULL)) "
        f"AND ({f_start} = ? OR ({f_start} IS NULL AND ? IS NULL)) "
        f"AND ({f_end} = ? OR ({f_end} IS NULL AND ? IS NULL)) "
        f"LIMIT 1"
    )


def verify_data_anchors(
    *,
    instance_jsonl: Path,
    data_db: Path,
    table: str,
    field_asset_id: str,
    field_path: str,
    field_value_index: str,
    field_segment_index: str,
    field_sentence_index: str,
    field_char_start: str,
    field_char_end: str,
    sqlite_timeout: int,
    sample_limit: int,
) -> Dict[str, Any]:
    started_at = _utc_now_iso()

    counts = Counts()
    stats = AnchorStats()
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    if not data_db.exists():
        return {
            "status": "FAIL",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "started_at": started_at,
            "finished_at": _utc_now_iso(),
            "summary": {"errors": 1, "warnings": 0},
            "errors": [
                _new_issue(
                    level="ERROR",
                    code="data_db_missing",
                    where={"file": str(data_db)},
                    detail="data layer sqlite db not found",
                )
            ],
            "warnings": [],
        }

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = _connect_sqlite(data_db, timeout=sqlite_timeout)
        cur = conn.cursor()

        # 可选：打开外键约束（不改变数据，只影响查询语义的一致性）
        try:
            cur.execute("PRAGMA foreign_keys = ON;")
        except Exception:
            pass

        sql_exists = _build_exists_sql(
            table=table,
            f_asset=field_asset_id,
            f_path=field_path,
            f_value=field_value_index,
            f_seg=field_segment_index,
            f_sentence=field_sentence_index,
            f_start=field_char_start,
            f_end=field_char_end,
        )

        missing_samples: List[Dict[str, Any]] = []
        error_samples: List[Dict[str, Any]] = []

        for line_no, rec in _read_jsonl(instance_jsonl):
            counts.input_lines += 1

            if rec.get("__parse_error__"):
                counts.invalid += 1
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code="jsonl_parse_error",
                        where={"file": str(instance_jsonl), "line": line_no, "object_type": "instance"},
                        detail="invalid json line",
                        hint={"raw_preview": str(rec.get("__raw__", ""))[:200]},
                    )
                )
                continue

            try:
                anchor = _extract_anchor(rec)
            except Exception as e:
                counts.invalid += 1
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code="instance_anchor_invalid",
                        where={"file": str(instance_jsonl), "line": line_no, "object_type": "instance"},
                        detail=str(e),
                        hint={"record_hint": {k: rec.get(k) for k in ("instance_id", "concept_id")}},
                    )
                )
                continue

            counts.valid += 1
            stats.checks += 1

            try:
                row = cur.execute(
                    sql_exists,
                    (
                        anchor["asset_id"],
                        anchor["path"],
                        anchor["value_index"],
                        anchor["value_index"],
                        anchor["segment_index"],
                        anchor["sentence_index"],
                        anchor["sentence_index"],
                        anchor["char_start"],
                        anchor["char_start"],
                        anchor["char_end"],
                        anchor["char_end"],
                    ),
                ).fetchone()

                if row is None:
                    stats.missing += 1
                    if len(missing_samples) < sample_limit:
                        missing_samples.append(
                            {
                                "instance_id": anchor["instance_id"],
                                "asset_id": anchor["asset_id"],
                                "path": anchor["path"],
                                "value_index": anchor["value_index"],
                                "segment_index": anchor["segment_index"],
                                "sentence_index": anchor["sentence_index"],
                                "char_start": anchor["char_start"],
                                "char_end": anchor["char_end"],
                                "line": line_no,
                            }
                        )
                else:
                    stats.found += 1

            except Exception as e:
                stats.errors += 1
                if len(error_samples) < sample_limit:
                    error_samples.append(
                        {
                            "instance_id": anchor["instance_id"],
                            "error": str(e),
                            "line": line_no,
                        }
                    )
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code="anchor_query_error",
                        where={"file": str(instance_jsonl), "line": line_no, "object_type": "instance", "object_id": anchor["instance_id"]},
                        detail="failed to query anchor existence in data db",
                        hint={"error": str(e)},
                    )
                )

        # missing anchors are errors (hard gate)
        if stats.missing > 0:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="missing_data_anchor",
                    where={"file": str(instance_jsonl), "line": None},
                    detail="one or more instance anchors cannot be found in data db",
                    hint={"missing_count": stats.missing, "sample": missing_samples},
                )
            )

        status = "PASS" if len(errors) == 0 else "FAIL"
        finished_at = _utc_now_iso()

        return {
            "status": status,
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "started_at": started_at,
            "finished_at": finished_at,
            "inputs": {
                "instance_jsonl": str(instance_jsonl),
                "data_db": str(data_db),
                "table": table,
                "fields": {
                    "asset_id": field_asset_id,
                    "path": field_path,
                    "value_index": field_value_index,
                    "segment_index": field_segment_index,
                    "sentence_index": field_sentence_index,
                    "char_start": field_char_start,
                    "char_end": field_char_end,
                },
                "sha256": {
                    "instance_jsonl": _file_sha256(instance_jsonl) if instance_jsonl.exists() else None,
                    "data_db": _file_sha256(data_db) if data_db.exists() else None,
                },
            },
            "counts": {
                "instance": counts.__dict__,
                "anchor": stats.__dict__,
            },
            "summary": {
                "instance_valid": counts.valid,
                "anchor_checks": stats.checks,
                "anchors_found": stats.found,
                "anchors_missing": stats.missing,
                "anchors_error": stats.errors,
                "errors": len(errors),
                "warnings": len(warnings),
            },
            "errors": errors,
            "warnings": warnings,
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# CLI
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Verify that instance.observed_at anchors exist in the data-layer SQLite DB."
    )
    p.add_argument("--instance", required=True, help="Path to instance identity declarations JSONL")
    p.add_argument("--data-db", required=True, help="Path to data-layer SQLite database file")
    p.add_argument("--table", required=True, help="Table name in data DB for anchor existence check")
    p.add_argument("--field-asset-id", default="asset_id", help="Field name for asset_id")
    p.add_argument("--field-path", default="path", help="Field name for path")
    p.add_argument("--field-value-index", default="value_index", help="Field name for value_index")
    p.add_argument("--field-segment-index", default="segment_index", help="Field name for segment_index")
    p.add_argument("--field-sentence-index", default="sentence_index", help="Field name for sentence_index")
    p.add_argument("--field-char-start", default="char_start", help="Field name for char_start")
    p.add_argument("--field-char-end", default="char_end", help="Field name for char_end")
    p.add_argument("--sqlite-timeout", type=int, default=30, help="SQLite timeout seconds")
    p.add_argument("--report", required=True, help="Output report JSON path")
    p.add_argument("--sample-limit", type=int, default=50, help="Max samples to include in report for missing/query errors")
    p.add_argument("--dry-run", action="store_true", help="Do not write report, only print preview")
    return p


def main() -> int:
    args = _build_parser().parse_args()

    instance_path = Path(args.instance)
    data_db = Path(args.data_db)
    report_path = Path(args.report)

    if not instance_path.exists():
        print(_safe_json_dumps({"status": "error", "error": "missing instance file", "path": str(instance_path)}))
        return 2

    _ensure_dir(report_path.parent)

    report = verify_data_anchors(
        instance_jsonl=instance_path,
        data_db=data_db,
        table=str(args.table),
        field_asset_id=str(args.field_asset_id),
        field_path=str(args.field_path),
        field_value_index=str(args.field_value_index),
        field_segment_index=str(args.field_segment_index),
        field_sentence_index=str(args.field_sentence_index),
        field_char_start=str(args.field_char_start),
        field_char_end=str(args.field_char_end),
        sqlite_timeout=int(args.sqlite_timeout),
        sample_limit=int(args.sample_limit),
    )

    if args.dry_run:
        preview = {
            "status": report.get("status"),
            "summary": report.get("summary"),
            "first_error": report.get("errors", [])[:1],
        }
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0

    _archive_existing(report_path)
    size_bytes, sha = _atomic_write_json(report_path, report)

    print(
        json.dumps(
            {
                "status": "ok",
                "result": report.get("status"),
                "script": SCRIPT_NAME,
                "version": SCRIPT_VERSION,
                "report": str(report_path),
                "report_size_bytes": size_bytes,
                "report_sha256": sha,
                "errors": int(report.get("summary", {}).get("errors", 0)),
                "warnings": int(report.get("summary", {}).get("warnings", 0)),
                "anchors_missing": int(report.get("summary", {}).get("anchors_missing", 0)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    return 0 if report.get("status") == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
