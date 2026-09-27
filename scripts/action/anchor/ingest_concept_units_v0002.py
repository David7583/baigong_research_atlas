# ============================================================
# 文件名: ingest_concept_units_v0002.py
# 中文名: 概念单元身份生成脚本
# 版本号: v0002
#
# 主层级: action
# 子层级: anchor
# 脚本定位: 行动端产物进入事实层之前的概念身份生成器
# 可更新: True
#
# 职责说明:
# - 读取行动端已 normalize 的结构化 JSONL 产物
# - 基于配置文件冻结的 canonicalization 规则生成 canonical_text
# - 计算 content_hash 并生成 concept_id
# - 对同一运行内的 concept 进行去重
# - 输出 concept 身份声明 JSONL 供后续写入 SQL 层使用
#
# 本脚本做什么:
# - 生成稳定的 concept_id
# - 输出可回指的 first_observed_in 追踪信息
# - 生成可审计 run_meta
#
# 本脚本不做什么:
# - 不写入 SQL
# - 不检查 SQL 中是否已存在该 concept
# - 不生成 instance
# - 不写入 attribute
# - 不做语义推断与关键词提取
#
# 制度边界声明:
# - 本脚本不进行制度裁决
# - 若输入包含 decision 字段, 本脚本仅执行闸门过滤 decision == ALLOW
# - 本脚本只生成身份声明, 不固化事实
#
# 批次原子性语义声明:
# - fail_on_invalid = True: 任何无效记录导致整个批次拒绝（全有或全无）
# - fail_on_invalid = False: 跳过无效记录，处理所有有效记录（记录到 issues）
# - 理由: 批次来自同一 asset，一条无效记录意味着该 asset 历史不完整
# - 制度目标: 避免生成"带漏洞的事实快照"，确保对象存在可能性的完整性
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingest_concept_units
# family: anchor
# role: concept_identity_builder
# version: v0002
# status: active
# entry_point: scripts/action/anchor/ingest_concept_units_v0002.py
# input:
#   - normalized_unit_records (jsonl)
#   - optional decision gate field
#   - config (yml)
# output:
#   - concept_identity_declarations (jsonl)
#   - run_meta (json)
# depends_on: []
# used_by:
#   - anchor write layer (sql_writer)
#   - downstream instance ingest

# ============================================================
# 制度与职责说明注释区
# ============================================================
# - 本脚本只做身份生成与声明输出
# - 本脚本不修改任何既有系统对象
# - 本脚本输出允许被丢弃, 重算, 否定
# - dry-run 下不得写入任何文件
# - 不允许隐式扫描未知目录, 输入必须显式提供
#
# 统计守恒定律:
# - input_records = accepted_records + skipped_by_gate + invalid_records
# - 此守恒关系用于验证重放一致性，不可违背
#
# 错误归因层级稳定性:
# - 字段缺失 → 在 ingest 层拒绝，记录到 issues
# - 内容格式错误 → 在 ingest 层拒绝，记录到 issues
# - 外键不存在 → 在 sql_writer 层拒绝（由 sql_writer 负责）

from __future__ import annotations

# ============================================================
# Imports 区
# ============================================================
import argparse
import hashlib
import json
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    yaml = None
    _YAML_IMPORT_ERROR = e

import unicodedata

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 不依赖其他 family 内部实现
# - 所有输入路径必须显式传入
# - 仅以配置文件冻结 identity 规则
# - 输出采用原子写入, 并对既有输出做 archive 移动
# - 发生输入解析错误时不静默, 计入 invalid_records

# ============================================================
# 常量与全局配置区
# ============================================================
SCRIPT_NAME = "ingest_concept_units_v0002.py"
SCRIPT_VERSION = "v0002"

ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

DEFAULT_CANONICAL_UNICODE = "NFC"
VALID_UNICODE_FORMS = {"NFC", "NFD", "NFKC", "NFKD"}

# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def read_text_lines(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield line


def sha256_hex_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def sha256_hex_text(s: str) -> str:
    return sha256_hex_bytes(s.encode("utf-8"))


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_inputs_fingerprint(paths: List[Path]) -> Dict[str, Any]:
    """
    为审计目的生成输入指纹
    - per_file_sha256 可能较慢, 但可审计且可回放
    """
    items: List[Dict[str, Any]] = []
    combined = hashlib.sha256()
    for p in paths:
        sha = file_sha256(p)
        st = p.stat()
        item = {
            "path": str(p),
            "size_bytes": st.st_size,
            "sha256": sha,
        }
        items.append(item)
        combined.update(p.as_posix().encode("utf-8"))
        combined.update(b"\n")
        combined.update(sha.encode("utf-8"))
        combined.update(b"\n")
    return {
        "count": len(paths),
        "combined_sha256": combined.hexdigest(),
        "files": items,
    }


def load_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError(f"PyYAML not available: {_YAML_IMPORT_ERROR}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("config must be a YAML mapping at top level")
    return data


def coerce_bool(x: Any, field: str) -> bool:
    if isinstance(x, bool):
        return x
    raise ValueError(f"{field} must be boolean")


def normalize_line_endings(s: str) -> str:
    # CRLF and CR to LF
    return s.replace("\r\n", "\n").replace("\r", "\n")


_ws_re = re.compile(r"\s+", flags=re.UNICODE)


def collapse_internal_whitespace(s: str) -> str:
    return _ws_re.sub(" ", s)


def canonicalize_text(unit_text: str, rules: Dict[str, Any]) -> str:
    s = unit_text

    # normalize line endings first to stabilize whitespace behavior
    if rules.get("normalize_line_endings", False):
        s = normalize_line_endings(s)

    # unicode normalization
    form = rules.get("unicode_normalization", DEFAULT_CANONICAL_UNICODE)
    if form is None:
        form = DEFAULT_CANONICAL_UNICODE
    if not isinstance(form, str) or form not in VALID_UNICODE_FORMS:
        raise ValueError(f"unicode_normalization must be one of {sorted(VALID_UNICODE_FORMS)}")
    s = unicodedata.normalize(form, s)

    # trim
    if rules.get("trim_whitespace", False):
        s = s.strip()

    # collapse internal whitespace
    if rules.get("collapse_internal_whitespace", False):
        s = collapse_internal_whitespace(s)

    # case folding
    if rules.get("case_folding", False):
        s = s.casefold()

    # fullwidth_to_halfwidth
    # v0001 默认 false
    if rules.get("fullwidth_to_halfwidth", False):
        # NFKC may change semantics, but if enabled, user takes responsibility
        s = unicodedata.normalize("NFKC", s)

    # punctuation normalization placeholder
    if rules.get("punctuation_normalization", False):
        # v0001 不默认提供具体策略, 避免隐性语义变化
        raise ValueError("punctuation_normalization is not supported in v0001")

    return s


def archive_existing_output(output_path: Path) -> Optional[Path]:
    if not output_path.exists():
        return None
    archive_dir = output_path.parent / ARCHIVE_DIRNAME
    ensure_dir(archive_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived_path = archive_dir / f"{output_path.stem}__{ts}{output_path.suffix}"
    shutil.move(str(output_path), str(archived_path))
    return archived_path


def atomic_write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> Tuple[int, str]:
    tmp = path.with_suffix(path.suffix + ".tmp")
    count = 0
    h = hashlib.sha256()
    with tmp.open("w", encoding="utf-8") as f:
        for r in rows:
            line = safe_json_dumps(r) + "\n"
            f.write(line)
            h.update(line.encode("utf-8"))
            count += 1
    shutil.move(str(tmp), str(path))
    return count, h.hexdigest()


# ============================================================
# 核心业务逻辑区
# ============================================================
@dataclass
class BuildStats:
    input_records: int
    accepted_records: int
    skipped_by_gate: int
    invalid_records: int
    unique_concepts: int


def iter_records(paths: List[Path]) -> Iterable[Tuple[Path, Dict[str, Any]]]:
    for p in paths:
        for line in read_text_lines(p):
            try:
                obj = json.loads(line)
            except Exception:
                yield p, {"__parse_error__": True, "__raw__": line}
                continue
            if not isinstance(obj, dict):
                yield p, {"__parse_error__": True, "__raw__": line}
                continue
            yield p, obj


def build_concept_declarations(
    input_paths: List[Path],
    config: Dict[str, Any],
    fail_on_invalid: bool,
) -> Tuple[List[Dict[str, Any]], BuildStats, List[Dict[str, Any]]]:
    canonical_rules = config.get("canonicalization", {})
    if not isinstance(canonical_rules, dict):
        raise ValueError("config.canonicalization must be a mapping")

    identity_hash = config.get("identity_hash", {})
    if not isinstance(identity_hash, dict):
        raise ValueError("config.identity_hash must be a mapping")

    algo = identity_hash.get("algorithm", "sha256")
    if algo != "sha256":
        raise ValueError("v0001 only supports sha256 identity_hash.algorithm")

    schema_cfg = config.get("schema", {})
    if not isinstance(schema_cfg, dict):
        raise ValueError("config.schema must be a mapping")
    schema_version_value = schema_cfg.get("schema_version_value", "v0001")
    if not isinstance(schema_version_value, str) or not schema_version_value:
        raise ValueError("config.schema.schema_version_value must be non-empty string")

    seen: Dict[str, Dict[str, Any]] = {}
    issues: List[Dict[str, Any]] = []

    input_records = 0
    accepted_records = 0
    skipped_by_gate = 0
    invalid_records = 0

    for src_path, rec in iter_records(input_paths):
        input_records += 1

        if rec.get("__parse_error__"):
            invalid_records += 1
            issues.append(
                {
                    "type": "json_parse_error",
                    "source_file": str(src_path),
                    "raw": rec.get("__raw__"),
                }
            )
            continue

        # optional decision gate
        if "decision" in rec and rec.get("decision") != "ALLOW":
            skipped_by_gate += 1
            continue

        unit_text = rec.get("unit_text")
        if not isinstance(unit_text, str) or not unit_text.strip():
            invalid_records += 1
            issues.append(
                {
                    "type": "missing_unit_text",
                    "source_file": str(src_path),
                    "record_hint": {k: rec.get(k) for k in ("asset_id", "path", "segment_index")},
                }
            )
            continue

        # ============================================================
        # 修正区：兼容嵌套在 example_unit 中的必需字段
        # ============================================================
        required_fields = ["asset_id", "path", "segment_index"]
        missing_fields = []
        
        # 建立临时映射：优先取顶层，若为 None 则取 example_unit 内部
        ex = rec.get("example_unit", {})
        f_vals = {
            "asset_id": rec.get("asset_id") if rec.get("asset_id") is not None else ex.get("asset_id"),
            "path": rec.get("path") if rec.get("path") is not None else ex.get("path"),
            "segment_index": rec.get("segment_index") if rec.get("segment_index") is not None else ex.get("segment_index"),
            "char_start": rec.get("char_start") if rec.get("char_start") is not None else ex.get("char_start"),
            "char_end": rec.get("char_end") if rec.get("char_end") is not None else ex.get("char_end"),
        }

        for f in required_fields:
            v = f_vals[f]
            if v is None:
                missing_fields.append(f)
            elif isinstance(v, str) and not v.strip():
                missing_fields.append(f)
        
        if missing_fields:
            invalid_records += 1
            issues.append(
                {
                    "type": "missing_required_fields",
                    "source_file": str(src_path),
                    "missing_fields": missing_fields,
                    "record_hint": {
                        "asset_id": f_vals["asset_id"],
                        "path": f_vals["path"],
                        "segment_index": f_vals["segment_index"],
                        "unit_text_preview": unit_text[:100] if unit_text else None,
                    },
                }
            )
            continue

        try:
            canonical_text = canonicalize_text(unit_text, canonical_rules)
        except Exception as e:
            invalid_records += 1
            issues.append(
                {
                    "type": "canonicalization_error",
                    "source_file": str(src_path),
                    "error": str(e),
                    "unit_text": unit_text[:200],
                }
            )
            continue

        # 只有通过所有验证后才递增 accepted_records，确保统计守恒
        accepted_records += 1

        content_hash = sha256_hex_text(canonical_text)
        concept_id = content_hash  # direct mapping in v0001

        if concept_id in seen:
            continue

        # 使用修正后的 f_vals 填充追踪信息
        first_observed_in = {
            "asset_id": f_vals["asset_id"],
            "path": f_vals["path"],
            "segment_index": f_vals["segment_index"],
            "char_start": f_vals["char_start"],
            "char_end": f_vals["char_end"],
        }

        seen[concept_id] = {
            "concept_id": concept_id,
            "unit_text": unit_text,
            "canonical_text": canonical_text,
            "content_hash": content_hash,
            "schema_version": schema_version_value,
            "first_observed_in": first_observed_in,
        }

    if fail_on_invalid and invalid_records > 0:
        raise RuntimeError(f"invalid_records > 0: {invalid_records}")

    decls = sorted(seen.values(), key=lambda x: x["concept_id"])
    stats = BuildStats(
        input_records=input_records,
        accepted_records=accepted_records,
        skipped_by_gate=skipped_by_gate,
        invalid_records=invalid_records,
        unique_concepts=len(decls),
    )
    return decls, stats, issues


# ============================================================
# CLI / main 接口区
# ============================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build concept identity declarations from normalized unit records (no SQL write)."
    )

    in_group = p.add_mutually_exclusive_group(required=True)
    in_group.add_argument(
        "--inputs",
        nargs="+",
        help="Input JSONL files (normalized unit records).",
    )
    in_group.add_argument(
        "--input-dir",
        help="Directory containing input JSONL files.",
    )

    p.add_argument(
        "--config",
        default="config/action/config/ingest_concept_units_config_v0001.yml",
        help="YAML config file freezing canonicalization and identity rules.",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Output JSONL path for concept identity declarations.",
    )
    p.add_argument(
        "--run-meta",
        required=True,
        help="Run meta JSON path.",
    )
    p.add_argument(
        "--issues",
        default=None,
        help="Optional issues JSONL path (invalid records and warnings).",
    )
    p.add_argument(
        "--fail-on-invalid",
        action="store_true",
        help="Fail the run if any invalid records are found.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run without writing any files.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # resolve inputs
    if args.input_dir:
        in_dir = Path(args.input_dir)
        if not in_dir.exists():
            print(safe_json_dumps({"status": "error", "error": f"input-dir not found: {args.input_dir}"}))
            return 2
        input_paths = sorted(in_dir.glob("*.jsonl"))
        if not input_paths:
            print(safe_json_dumps({"status": "error", "error": f"no jsonl files in input-dir: {args.input_dir}"}))
            return 2
    else:
        input_paths = [Path(p) for p in args.inputs]
        missing = [str(p) for p in input_paths if not p.exists()]
        if missing:
            print(safe_json_dumps({"status": "error", "error": "missing input files", "missing": missing}))
            return 2
        input_paths = sorted(input_paths)

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        print(safe_json_dumps({"status": "error", "error": f"config not found: {args.config}"}))
        return 2

    out_path = Path(args.output)
    run_meta_path = Path(args.run_meta)
    issues_path = Path(args.issues) if args.issues else None

    ensure_dir(out_path.parent)
    ensure_dir(run_meta_path.parent)
    if issues_path:
        ensure_dir(issues_path.parent)

    try:
        config = load_yaml(cfg_path)
    except Exception as e:
        print(safe_json_dumps({"status": "error", "error": f"failed to load config: {str(e)}"}))
        return 2

    cfg_sha = file_sha256(cfg_path)
    inputs_fp = compute_inputs_fingerprint(input_paths)

    try:
        decls, stats, issues = build_concept_declarations(
            input_paths=input_paths,
            config=config,
            fail_on_invalid=bool(args.fail_on_invalid),
        )
    except Exception as e:
        print(
            safe_json_dumps(
                {
                    "status": "error",
                    "script": SCRIPT_NAME,
                    "version": SCRIPT_VERSION,
                    "error": str(e),
                }
            )
        )
        return 1

    if args.dry_run:
        preview_summary = [
            {
                "concept_id": d["concept_id"],
                "content_hash": d["content_hash"],
                "schema_version": d["schema_version"],
                "unit_text_length": len(d["unit_text"]),
                "canonical_text_length": len(d["canonical_text"]),
                "first_observed_in": d["first_observed_in"],
            }
            for d in decls[:3]
        ]
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "script": SCRIPT_NAME,
                    "version": SCRIPT_VERSION,
                    "generated_at": utc_now_iso(),
                    "config_sha256": cfg_sha,
                    "inputs": inputs_fp,
                    "stats": stats.__dict__,
                    "preview_summary_first_3": preview_summary,
                    "issues_count": len(issues),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    archived_output = archive_existing_output(out_path)
    archived_issues = archive_existing_output(issues_path) if (issues_path and issues_path.exists()) else None

    rows_written, output_hash = atomic_write_jsonl(out_path, decls)

    issues_written = 0
    issues_hash = None
    if issues_path:
        issues_written, issues_hash = atomic_write_jsonl(issues_path, issues)

    run_meta: Dict[str, Any] = {
        "status": "ok",
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "generated_at": utc_now_iso(),
        "config": {
            "path": str(cfg_path),
            "sha256": cfg_sha,
            "version": config.get("version"),
        },
        "inputs": inputs_fp,
        "stats": stats.__dict__,
        "output": {
            "path": str(out_path),
            "rows_written": rows_written,
            "sha256": output_hash,
            "archived_previous": str(archived_output) if archived_output else None,
        },
        "issues": {
            "enabled": bool(issues_path),
            "path": str(issues_path) if issues_path else None,
            "rows_written": issues_written if issues_path else 0,
            "sha256": issues_hash if issues_path else None,
            "archived_previous": str(archived_issues) if archived_issues else None,
        },
    }

    with run_meta_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(run_meta, ensure_ascii=False, indent=2))

    print(json.dumps(run_meta, ensure_ascii=False, indent=2))
    return 0


# ============================================================
# if __name__ == "__main__" 入口
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
