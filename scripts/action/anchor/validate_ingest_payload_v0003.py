# ============================================================
# 文件名: validate_ingest_payload_v0003.py
# 中文名: 写入载荷校验脚本
# 版本号: v0003
#
# 主层级: action
# 子层级: persistence / preflight
# 脚本定位: SQL 写入前的载荷一致性与可回指性校验器
# 可更新: True
#
# 职责说明:
# - 校验 concept / instance / attribute 三类 JSONL 载荷的结构完整性与身份稳定性
# - 校验载荷内部引用拓扑，确保 instance 引用 concept，attribute 引用 concept 或 instance
# - 校验 run_id 一致性，防止跨批污染
# - 识别 attribute 冲突集合并输出 warning 级报告
# - 输出结构化 validation_report.json 供 sql_writer 前置闸门使用
#
# 本脚本做什么:
# - 不写入 SQL
# - 不修改任何历史数据
# - 不推断，不做语义裁决
# - 不访问向量库
#
# 本脚本不做什么:
# - 不替代 sql_writer 的最小一致性保护
# - 不创建或迁移数据库 schema
#
# 制度边界声明:
# - PASS 仅表示载荷可被写入，并不表示“世界无矛盾”
# - attribute 冲突在 v0001 视为可记录事实，按 warning 输出，不阻断写入
# - FAIL 表示载荷存在结构性错误或断裂引用，禁止写入
#
# 统计守恒:
# - concept_input_lines = concept_valid + concept_invalid
# - instance_input_lines = instance_valid + instance_invalid
# - attribute_input_lines = attribute_valid + attribute_invalid
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: validate_ingest_payload
# family: validate_ingest_payload
# role: payload_validator
# version: v0003
# status: active
# entry_point: scripts/action/anchor/validate_ingest_payload_v0003.py
# input:
#   - concept_identity_declarations (jsonl)
#   - instance_identity_declarations (jsonl)
#   - attribute_declarations (jsonl)
#   - run_meta (json, optional, for run_id resolution)
# output:
#   - validation_report (json)
# depends_on:
#   - ingest_concept_units_v0002
#   - ingest_instance_units_v0002
#   - ingest_attribute_units_v0003
# used_by:
#   - sql_writer_v0004
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


SCRIPT_NAME = "validate_ingest_payload_v0003.py"
SCRIPT_VERSION = "v0003"
ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_ENCODING = "utf-8"


# ============================================================
# 工具函数区
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


def _read_json(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding=DEFAULT_ENCODING)
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("run_meta must be a JSON object")
    return obj


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


# ============================================================
# 报告结构
# ============================================================

@dataclass
class Counts:
    input_lines: int = 0
    valid: int = 0
    invalid: int = 0


def _new_issue(*, level: str, code: str, where: Dict[str, Any], detail: str, hint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {
        "level": level,            # ERROR | WARNING
        "code": code,              # stable error code
        "where": where,            # {file, line, object_type, object_id}
        "detail": detail,
    }
    if hint is not None:
        out["hint"] = hint
    return out


# ============================================================
# 校验核心
# ============================================================

def _extract_run_id(run_metas: List[Path]) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    issues: List[Dict[str, Any]] = []
    run_ids: List[str] = []

    for p in run_metas:
        try:
            obj = _read_json(p)
        except Exception as e:
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="run_meta_parse_error",
                    where={"file": str(p), "line": None},
                    detail=str(e),
                )
            )
            continue

        rid = obj.get("run_id")
        if isinstance(rid, str) and rid.strip():
            run_ids.append(rid.strip())
        else:
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="run_meta_missing_run_id",
                    where={"file": str(p), "line": None},
                    detail="run_meta.run_id missing or invalid",
                )
            )

    uniq = sorted(set(run_ids))
    if not uniq:
        return None, issues

    if len(uniq) > 1:
        issues.append(
            _new_issue(
                level="ERROR",
                code="cross_run_contamination",
                where={"files": [str(x) for x in run_metas], "line": None},
                detail="multiple run_id values detected in run_meta inputs",
                hint={"run_ids": uniq},
            )
        )
        return None, issues

    return uniq[0], issues


def _load_concepts(concept_path: Path) -> Tuple[Dict[str, Dict[str, Any]], Counts, List[Dict[str, Any]]]:
    issues: List[Dict[str, Any]] = []
    counts = Counts()

    concepts: Dict[str, Dict[str, Any]] = {}
    seen: Set[str] = set()

    for line_no, rec in _read_jsonl(concept_path):
        counts.input_lines += 1

        if rec.get("__parse_error__"):
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="jsonl_parse_error",
                    where={"file": str(concept_path), "line": line_no, "object_type": "concept"},
                    detail="invalid json line",
                    hint={"raw_preview": str(rec.get("__raw__", ""))[:200]},
                )
            )
            continue

        try:
            concept_id = _require_non_empty_str("concept_id", rec.get("concept_id"))
            canonical_text = _require_non_empty_str("canonical_text", rec.get("canonical_text"))
            content_hash = _require_non_empty_str("content_hash", rec.get("content_hash"))
        except Exception as e:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="concept_missing_required_fields",
                    where={"file": str(concept_path), "line": line_no, "object_type": "concept"},
                    detail=str(e),
                    hint={"record_hint": {k: rec.get(k) for k in ("concept_id", "content_hash")}},
                )
            )
            continue

        if concept_id in seen:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="duplicate_concept_id",
                    where={"file": str(concept_path), "line": line_no, "object_type": "concept", "object_id": concept_id},
                    detail="duplicate concept_id in payload",
                )
            )
            continue

        computed = _sha256_hex_text(canonical_text)
        if computed != content_hash:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="identity_unstable_concept_hash_mismatch",
                    where={"file": str(concept_path), "line": line_no, "object_type": "concept", "object_id": concept_id},
                    detail="content_hash does not match sha256(canonical_text)",
                    hint={"computed": computed, "declared": content_hash},
                )
            )
            continue

        seen.add(concept_id)
        counts.valid += 1
        concepts[concept_id] = rec

    return concepts, counts, issues


def _load_instances(instance_path: Path) -> Tuple[Dict[str, Dict[str, Any]], Counts, List[Dict[str, Any]]]:
    issues: List[Dict[str, Any]] = []
    counts = Counts()

    instances: Dict[str, Dict[str, Any]] = {}
    seen: Set[str] = set()

    for line_no, rec in _read_jsonl(instance_path):
        counts.input_lines += 1

        if rec.get("__parse_error__"):
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="jsonl_parse_error",
                    where={"file": str(instance_path), "line": line_no, "object_type": "instance"},
                    detail="invalid json line",
                    hint={"raw_preview": str(rec.get("__raw__", ""))[:200]},
                )
            )
            continue

        try:
            instance_id = _require_non_empty_str("instance_id", rec.get("instance_id"))
            concept_id = _require_non_empty_str("concept_id", rec.get("concept_id"))
            canonical_text = _require_non_empty_str("canonical_text", rec.get("canonical_text"))
            content_hash = _require_non_empty_str("content_hash", rec.get("content_hash"))
            observed_at = rec.get("observed_at")
            if not isinstance(observed_at, dict):
                raise ValueError("observed_at must be object")
            asset_id = _require_non_empty_str("asset_id", observed_at.get("asset_id"))
            path = _require_non_empty_str("path", observed_at.get("path"))
            segment_index = observed_at.get("segment_index")
            if not isinstance(segment_index, int) or isinstance(segment_index, bool):
                raise ValueError("observed_at.segment_index must be int")
            char_start = _coerce_int_or_none("observed_at.char_start", observed_at.get("char_start"))
            char_end = _coerce_int_or_none("observed_at.char_end", observed_at.get("char_end"))
        except Exception as e:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="instance_missing_required_fields",
                    where={"file": str(instance_path), "line": line_no, "object_type": "instance"},
                    detail=str(e),
                    hint={"record_hint": {k: rec.get(k) for k in ("instance_id", "concept_id")}},
                )
            )
            continue

        if instance_id in seen:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="duplicate_instance_id",
                    where={"file": str(instance_path), "line": line_no, "object_type": "instance", "object_id": instance_id},
                    detail="duplicate instance_id in payload",
                )
            )
            continue

        computed = _sha256_hex_text(canonical_text)
        if computed != content_hash:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="identity_unstable_instance_hash_mismatch",
                    where={"file": str(instance_path), "line": line_no, "object_type": "instance", "object_id": instance_id},
                    detail="content_hash does not match sha256(canonical_text)",
                    hint={"computed": computed, "declared": content_hash},
                )
            )
            continue

        # observed_at sanity checks
        if char_start is not None and char_end is not None and char_end < char_start:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="invalid_observed_at_range",
                    where={"file": str(instance_path), "line": line_no, "object_type": "instance", "object_id": instance_id},
                    detail="observed_at.char_end is less than char_start",
                    hint={"char_start": char_start, "char_end": char_end, "asset_id": asset_id, "path": path, "segment_index": segment_index},
                )
            )
            continue

        seen.add(instance_id)
        counts.valid += 1
        instances[instance_id] = rec

    return instances, counts, issues


def _load_attributes(attribute_path: Path) -> Tuple[List[Dict[str, Any]], Counts, List[Dict[str, Any]]]:
    issues: List[Dict[str, Any]] = []
    counts = Counts()

    attrs: List[Dict[str, Any]] = []

    for line_no, rec in _read_jsonl(attribute_path):
        counts.input_lines += 1

        if rec.get("__parse_error__"):
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="jsonl_parse_error",
                    where={"file": str(attribute_path), "line": line_no, "object_type": "attribute"},
                    detail="invalid json line",
                    hint={"raw_preview": str(rec.get("__raw__", ""))[:200]},
                )
            )
            continue

        try:
            object_type = _require_non_empty_str("object_type", rec.get("object_type"))
            object_id = _require_non_empty_str("object_id", rec.get("object_id"))
            attr_key = _require_non_empty_str("attr_key", rec.get("attr_key"))
            attr_value = _require_non_empty_str("attr_value", rec.get("attr_value"))
        except Exception as e:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="attribute_missing_required_fields",
                    where={"file": str(attribute_path), "line": line_no, "object_type": "attribute"},
                    detail=str(e),
                    hint={"record_hint": {k: rec.get(k) for k in ("object_type", "object_id", "attr_key")}},
                )
            )
            continue

        if object_type not in {"concept", "instance"}:
            counts.invalid += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="invalid_attribute_object_type",
                    where={"file": str(attribute_path), "line": line_no, "object_type": "attribute", "object_id": object_id},
                    detail="attribute.object_type must be concept or instance",
                    hint={"object_type": object_type},
                )
            )
            continue

        counts.valid += 1
        attrs.append(rec)

    return attrs, counts, issues


def _validate_topology(
    concepts: Dict[str, Dict[str, Any]],
    instances: Dict[str, Dict[str, Any]],
    attrs: List[Dict[str, Any]],
    concept_path: Path,
    instance_path: Path,
    attribute_path: Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    # instance -> concept
    for inst_id, inst in instances.items():
        c_id = inst.get("concept_id")
        if c_id not in concepts:
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="foreign_reference_missing_concept",
                    where={"file": str(instance_path), "line": None, "object_type": "instance", "object_id": inst_id},
                    detail="instance.concept_id not found in concept payload",
                    hint={"concept_id": c_id},
                )
            )

    # attribute -> object
    for idx, a in enumerate(attrs):
        obj_type = a.get("object_type")
        obj_id = a.get("object_id")
        where = {"file": str(attribute_path), "line": None, "object_type": "attribute", "object_id": obj_id, "attr_key": a.get("attr_key")}
        if obj_type == "concept":
            if obj_id not in concepts:
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code="foreign_reference_missing_concept_for_attribute",
                        where=where,
                        detail="attribute references missing concept_id in payload",
                        hint={"object_id": obj_id},
                    )
                )
        elif obj_type == "instance":
            if obj_id not in instances:
                errors.append(
                    _new_issue(
                        level="ERROR",
                        code="foreign_reference_missing_instance_for_attribute",
                        where=where,
                        detail="attribute references missing instance_id in payload",
                        hint={"object_id": obj_id},
                    )
                )
        else:
            # should already be validated
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="invalid_attribute_object_type_internal",
                    where=where,
                    detail="attribute.object_type invalid after parsing",
                    hint={"object_type": obj_type},
                )
            )

    # attribute conflict sets
    # B strategy
    bucket: Dict[Tuple[str, str, str], Set[str]] = {}
    for a in attrs:
        k = (str(a.get("object_type")), str(a.get("object_id")), str(a.get("attr_key")))
        v = str(a.get("attr_value"))
        if k not in bucket:
            bucket[k] = set()
        bucket[k].add(v)

    conflicts: List[Dict[str, Any]] = []
    for (obj_type, obj_id, attr_key), values in bucket.items():
        if len(values) > 1:
            conflicts.append(
                {
                    "object_type": obj_type,
                    "object_id": obj_id,
                    "attr_key": attr_key,
                    "values": sorted(values),
                }
            )
            warnings.append(
                _new_issue(
                    level="WARNING",
                    code="attribute_conflict_set",
                    where={"object_type": obj_type, "object_id": obj_id, "attr_key": attr_key},
                    detail="multiple values detected for same (object_type, object_id, attr_key) within the payload",
                    hint={"values": sorted(values)},
                )
            )

    return errors, warnings


def validate_payload(
    *,
    concept_jsonl: Path,
    instance_jsonl: Path,
    attribute_jsonl: Path,
    run_meta_paths: List[Path],
    strict_run_id: bool,
) -> Dict[str, Any]:
    started_at = _utc_now_iso()

    run_id, run_meta_issues = _extract_run_id(run_meta_paths) if run_meta_paths else (None, [])
    if strict_run_id and not run_id:
        run_meta_issues.append(
            _new_issue(
                level="ERROR",
                code="missing_run_id",
                where={"files": [str(p) for p in run_meta_paths], "line": None},
                detail="run_id cannot be resolved, strict_run_id is enabled",
            )
        )

    concepts, c_counts, c_issues = _load_concepts(concept_jsonl)
    instances, i_counts, i_issues = _load_instances(instance_jsonl)
    attrs, a_counts, a_issues = _load_attributes(attribute_jsonl)

    topo_errors, topo_warnings = _validate_topology(
        concepts, instances, attrs, concept_jsonl, instance_jsonl, attribute_jsonl
    )

    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    # split run_meta issues by level
    for it in run_meta_issues:
        if it.get("level") == "WARNING":
            warnings.append(it)
        else:
            errors.append(it)

    for it in c_issues + i_issues + a_issues + topo_errors:
        errors.append(it)
    for it in topo_warnings:
        warnings.append(it)

    status = "PASS" if len(errors) == 0 else "FAIL"

    finished_at = _utc_now_iso()

    report = {
        "status": status,
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "inputs": {
            "concept_jsonl": str(concept_jsonl),
            "instance_jsonl": str(instance_jsonl),
            "attribute_jsonl": str(attribute_jsonl),
            "run_meta": [str(p) for p in run_meta_paths],
            "sha256": {
                "concept_jsonl": _file_sha256(concept_jsonl) if concept_jsonl.exists() else None,
                "instance_jsonl": _file_sha256(instance_jsonl) if instance_jsonl.exists() else None,
                "attribute_jsonl": _file_sha256(attribute_jsonl) if attribute_jsonl.exists() else None,
                "run_meta": [{"path": str(p), "sha256": _file_sha256(p)} for p in run_meta_paths if p.exists()],
            },
        },
        "counts": {
            "concept": c_counts.__dict__,
            "instance": i_counts.__dict__,
            "attribute": a_counts.__dict__,
        },
        "summary": {
            "concept_count": len(concepts),
            "instance_count": len(instances),
            "attribute_count": len(attrs),
            "errors": len(errors),
            "warnings": len(warnings),
        },
        "errors": errors,
        "warnings": warnings,
    }
    return report


# ============================================================
# CLI
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Validate SQL ingest payload (concept/instance/attribute JSONL) before writing."
    )
    p.add_argument("--concept", required=True, help="Path to concept identity declarations JSONL")
    p.add_argument("--instance", required=True, help="Path to instance identity declarations JSONL")
    p.add_argument("--attribute", required=True, help="Path to attribute declarations JSONL")
    p.add_argument("--run-meta", nargs="*", default=[], help="One or more run_meta.json paths for run_id resolution")
    p.add_argument("--report", required=True, help="Output report JSON path")
    p.add_argument("--strict-run-id", action="store_true", help="Fail if run_id cannot be resolved from run_meta")
    p.add_argument("--dry-run", action="store_true", help="Do not write report, only print preview")
    return p


def main() -> int:
    args = _build_parser().parse_args()

    concept_path = Path(args.concept)
    instance_path = Path(args.instance)
    attribute_path = Path(args.attribute)
    report_path = Path(args.report)
    run_meta_paths = [Path(x) for x in (args.run_meta or [])]

    missing = [str(p) for p in [concept_path, instance_path, attribute_path] if not p.exists()]
    if missing:
        print(_safe_json_dumps({"status": "error", "error": "missing input files", "missing": missing}))
        return 2

    # report output directory prepare
    _ensure_dir(report_path.parent)

    report = validate_payload(
        concept_jsonl=concept_path,
        instance_jsonl=instance_path,
        attribute_jsonl=attribute_path,
        run_meta_paths=[p for p in run_meta_paths if p.exists()],
        strict_run_id=bool(args.strict_run_id),
    )

    if args.dry_run:
        preview = {
            "status": report.get("status"),
            "run_id": report.get("run_id"),
            "summary": report.get("summary"),
            "first_error": report.get("errors", [])[:1],
            "first_warning": report.get("warnings", [])[:1],
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
                "run_id": report.get("run_id"),
                "report": str(report_path),
                "report_size_bytes": size_bytes,
                "report_sha256": sha,
                "errors": int(report.get("summary", {}).get("errors", 0)),
                "warnings": int(report.get("summary", {}).get("warnings", 0)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    return 0 if report.get("status") == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
