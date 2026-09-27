# ============================================================
# 文件名: ingest_instance_units_v0003.py
# 中文名: 实例观测事件身份生成脚本
# 版本号: v0003
#
# 主层级: action
# 子层级: anchor
# 脚本定位: 行动端产物进入事实层之前的实例观测事件身份声明生成器
# 可更新: True
#
# 职责说明:
# - 读取行动端已 normalize 的结构化 JSONL 产物
# - 基于配置文件冻结的 canonicalization 规则生成 canonical_text
# - 计算 content_hash 并生成 concept_id
# - 基于观测坐标系生成 instance_id
# - 同一运行内按配置进行去重
# - 输出 instance 身份声明 JSONL 供后续写入 SQL 层使用
#
# 本脚本做什么:
# - 为每条合法观测生成稳定 instance_id
# - 生成可审计 run_meta
# - 输出 issues 用于错误归因稳定
#
# 本脚本不做什么:
# - 不写入 SQL
# - 不检查 SQL 中 concept 是否存在
# - 不生成 concept
# - 不写入 attribute
# - 不做语义推断与关键词提取
#
# 制度边界声明:
# - 若输入包含 decision 字段且启用闸门, 仅 decision == ALLOW 的记录参与计算
# - 本脚本只生成身份声明, 不固化事实
#
# 统计守恒定律:
# - input_records = accepted_records + skipped_by_gate + invalid_records
#
# 批次原子性语义声明:
# - fail_on_invalid = True: 任何无效记录导致整个批次拒绝（全有或全无）
# - fail_on_invalid = False: 跳过无效记录，处理所有有效记录（记录到 issues）
# - 理由: 批次来自同一 asset，一条无效记录意味着该 asset 观测历史不完整
# - 制度目标: 避免生成"带漏洞的观测快照"，确保实例观测的完整性
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingest_instance_units
# family: anchor
# role: instance_identity_builder
# version: v0003
# status: active
# entry_point: scripts/action/anchor/ingest_instance_units_v0003.py
# input:
#   - normalized_unit_records (jsonl)
#   - config (yml)
# output:
#   - instance_identity_declarations (jsonl)
#   - run_meta (json)
#   - optional issues (jsonl)
# depends_on: []
# used_by:
#   - anchor write layer (sql_writer)
#   - downstream attribute ingest

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
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


SCRIPT_NAME = "ingest_instance_units_v0003.py"
SCRIPT_VERSION = "v0003"

ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

DEFAULT_CANONICAL_UNICODE = "NFC"
VALID_UNICODE_FORMS = {"NFC", "NFD", "NFKC", "NFKD"}

_ws_re = re.compile(r"\s+", flags=re.UNICODE)


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


def load_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError(f"PyYAML not available: {_YAML_IMPORT_ERROR}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("config must be a YAML mapping at top level")
    return data


def normalize_line_endings(s: str) -> str:
    return s.replace("\r\n", "\n").replace("\r", "\n")


def collapse_internal_whitespace(s: str) -> str:
    return _ws_re.sub(" ", s)


def canonicalize_text(unit_text: str, rules: Dict[str, Any]) -> str:
    s = unit_text

    if rules.get("normalize_line_endings", False):
        s = normalize_line_endings(s)

    form = rules.get("unicode_normalization", DEFAULT_CANONICAL_UNICODE)
    if form is None:
        form = DEFAULT_CANONICAL_UNICODE
    if not isinstance(form, str) or form not in VALID_UNICODE_FORMS:
        raise ValueError(f"unicode_normalization must be one of {sorted(VALID_UNICODE_FORMS)}")
    s = unicodedata.normalize(form, s)

    if rules.get("trim_whitespace", False):
        s = s.strip()

    if rules.get("collapse_internal_whitespace", False):
        s = collapse_internal_whitespace(s)

    if rules.get("case_folding", False):
        s = s.casefold()

    if rules.get("fullwidth_to_halfwidth", False):
        s = unicodedata.normalize("NFKC", s)

    if rules.get("punctuation_normalization", False):
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


def _is_int_like(x: Any) -> bool:
    if isinstance(x, bool):
        return False
    return isinstance(x, int)


def _coerce_int_or_none(x: Any, allow_none: bool) -> Optional[int]:
    """
    修复点 1: 修正逻辑错误
    - 当 x 为 None 且 allow_none=False 时，应该抛出异常而非返回 None
    - 明确拒绝 bool 类型（防止静默容错）
    - 严格类型检查（输入应该已由上游 normalize）
    """
    if x is None:
        if not allow_none:
            raise ValueError("field does not allow None")
        return None
    if isinstance(x, bool):
        raise ValueError("bool is not allowed for int field")
    if isinstance(x, int):
        return x
    raise ValueError(f"int field must be int or null, got {type(x).__name__}")


def _require_non_empty_str(x: Any, field: str) -> str:
    if not isinstance(x, str) or not x.strip():
        raise ValueError(f"{field} must be non-empty string")
    return x


def resolve_path(obj: Dict[str, Any], path: str) -> Any:
    """
    支持从记录中解析 nested path。
    示例:
    - asset_id
    - example_unit.asset_id
    """
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _join_components_for_hash(components: List[Any], null_rep: str) -> str:
    out: List[str] = []
    for c in components:
        if c is None:
            out.append(null_rep)
        else:
            out.append(str(c))
    return "|".join(out)


@dataclass
class BuildStats:
    input_records: int
    accepted_records: int
    skipped_by_gate: int
    invalid_records: int
    unique_instances: int


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


def build_instance_declarations(
    input_paths: List[Path],
    config: Dict[str, Any],
    fail_on_invalid: bool,
) -> Tuple[List[Dict[str, Any]], BuildStats, List[Dict[str, Any]]]:
    
    # ============================================================
    # 修复点 2: 配置预验证（错误归因稳定性）
    # 配置错误应在处理任何记录前发现，而非运行时计入 invalid_records
    # ============================================================
    
    canonical_rules = config.get("canonicalization", {})
    if not isinstance(canonical_rules, dict):
        raise ValueError("config.canonicalization must be a mapping")

    schema_cfg = config.get("schema", {})
    if not isinstance(schema_cfg, dict):
        raise ValueError("config.schema must be a mapping")
    schema_version_value = schema_cfg.get("schema_version_value", "v0001")
    if not isinstance(schema_version_value, str) or not schema_version_value:
        raise ValueError("config.schema.schema_version_value must be non-empty string")
    instance_type = schema_cfg.get("instance_type", "observation_event")

    gate_cfg = config.get("decision_gate", {})
    if not isinstance(gate_cfg, dict):
        raise ValueError("config.decision_gate must be a mapping")
    gate_enabled = bool(gate_cfg.get("enabled", True))
    gate_allow_value = gate_cfg.get("allow_value", "ALLOW")

    val_cfg = config.get("validation", {})
    if not isinstance(val_cfg, dict):
        raise ValueError("config.validation must be a mapping")
    required_fields = val_cfg.get("required_fields", ["unit_text", "asset_id", "path", "segment_index"])
    if not isinstance(required_fields, list) or not all(isinstance(x, str) for x in required_fields):
        raise ValueError("config.validation.required_fields must be a list of strings")
    allow_null_position = bool(val_cfg.get("allow_null_position", True))

    anchor_cfg = config.get("identity_anchor", {})
    if not isinstance(anchor_cfg, dict):
        raise ValueError("config.identity_anchor must be a mapping")
    algo = anchor_cfg.get("algorithm", "sha256")
    if algo != "sha256":
        raise ValueError("v0001 only supports sha256 identity_anchor.algorithm")
    components_order = anchor_cfg.get(
        "components",
        ["concept_id", "asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end"],
    )
    if not isinstance(components_order, list) or not all(isinstance(x, str) for x in components_order):
        raise ValueError("config.identity_anchor.components must be a list of strings")
    
    # 修复点 2（续）: 验证 components 是否在允许的字段集合中
    valid_components = {"concept_id", "asset_id", "path", "value_index", "segment_index", "sentence_index", "char_start", "char_end"}

    def _is_valid_component(name: str) -> bool:
        if name in valid_components:
            return True
        if "." in name:
            return True
        return False

    invalid_comps = [c for c in components_order if not _is_valid_component(c)]
    if invalid_comps:
        raise ValueError(f"invalid components in config.identity_anchor.components: {invalid_comps}")
    
    null_rep = anchor_cfg.get("null_representation", "NULL")
    if not isinstance(null_rep, str) or not null_rep:
        raise ValueError("config.identity_anchor.null_representation must be non-empty string")

    runtime_cfg = config.get("runtime", {})
    if not isinstance(runtime_cfg, dict):
        raise ValueError("config.runtime must be a mapping")
    dedup_within_run = bool(runtime_cfg.get("deduplicate_within_run", True))

    # ============================================================
    # 配置验证完成，开始处理记录
    # ============================================================

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
            issues.append({"type": "json_parse_error", "source_file": str(src_path), "raw": rec.get("__raw__")})
            continue

        if gate_enabled and "decision" in rec and rec.get("decision") != gate_allow_value:
            skipped_by_gate += 1
            continue

        try:
            unit_text = _require_non_empty_str(rec.get("unit_text"), "unit_text")
        except Exception as e:
            invalid_records += 1
            issues.append({"type": "missing_or_invalid_unit_text", "source_file": str(src_path), "error": str(e)})
            continue

        # 注意: 字符串字段拒绝 None 和空字符串; 数值字段仅拒绝 None（0 是合法值）
        missing = []
        for f in required_fields:
            v = resolve_path(rec, f)
            if v is None:
                missing.append(f)
            elif isinstance(v, str) and not v.strip():
                missing.append(f)
        if missing:
            invalid_records += 1
            issues.append(
                {
                    "type": "missing_required_fields",
                    "source_file": str(src_path),
                    "missing_fields": missing,
                    "record_hint": {
                        "asset_id": resolve_path(rec, "asset_id") or resolve_path(rec, "example_unit.asset_id"),
                        "path": resolve_path(rec, "path") or resolve_path(rec, "example_unit.path"),
                        "segment_index": resolve_path(rec, "segment_index") or resolve_path(rec, "example_unit.segment_index"),
                        "unit_text_preview": unit_text[:100] if unit_text else None,
                    },
                }
            )
            continue

        try:
            asset_id = _require_non_empty_str(
                resolve_path(rec, "asset_id") if resolve_path(rec, "asset_id") is not None else resolve_path(rec, "example_unit.asset_id"),
                "asset_id",
            )
            path = _require_non_empty_str(
                resolve_path(rec, "path") if resolve_path(rec, "path") is not None else resolve_path(rec, "example_unit.path"),
                "path",
            )
            segment_index_raw = (
                resolve_path(rec, "segment_index")
                if resolve_path(rec, "segment_index") is not None
                else resolve_path(rec, "example_unit.segment_index")
            )
            if not _is_int_like(segment_index_raw):
                raise ValueError("segment_index must be int")
            segment_index = int(segment_index_raw)

            value_index_raw = (
                resolve_path(rec, "value_index")
                if resolve_path(rec, "value_index") is not None
                else resolve_path(rec, "example_unit.value_index")
            )
            sentence_index_raw = (
                resolve_path(rec, "sentence_index")
                if resolve_path(rec, "sentence_index") is not None
                else resolve_path(rec, "example_unit.sentence_index")
            )
            value_index = _coerce_int_or_none(value_index_raw, allow_none=allow_null_position)
            sentence_index = _coerce_int_or_none(sentence_index_raw, allow_none=allow_null_position)

            char_start_raw = (
                resolve_path(rec, "char_start")
                if resolve_path(rec, "char_start") is not None
                else resolve_path(rec, "example_unit.char_start")
            )
            char_end_raw = (
                resolve_path(rec, "char_end")
                if resolve_path(rec, "char_end") is not None
                else resolve_path(rec, "example_unit.char_end")
            )

            char_start = _coerce_int_or_none(char_start_raw, allow_none=allow_null_position)
            char_end = _coerce_int_or_none(char_end_raw, allow_none=allow_null_position)
        except Exception as e:
            invalid_records += 1
            issues.append(
                {
                    "type": "field_validation_error",
                    "source_file": str(src_path),
                    "error": str(e),
                    "record_hint": {
                        "asset_id": resolve_path(rec, "asset_id") or resolve_path(rec, "example_unit.asset_id"),
                        "path": resolve_path(rec, "path") or resolve_path(rec, "example_unit.path"),
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
                    "unit_text_preview": unit_text[:200],
                }
            )
            continue

        # 只有通过所有验证后才递增 accepted_records（统计守恒）
        accepted_records += 1

        content_hash = sha256_hex_text(canonical_text)
        concept_id = content_hash

        ctx: Dict[str, Any] = {
            "concept_id": concept_id,
            "asset_id": asset_id,
            "path": path,
            "value_index": value_index,
            "segment_index": segment_index,
            "sentence_index": sentence_index,
            "char_start": char_start,
            "char_end": char_end,
        }

        # 修复点 2（续）: 支持 nested component path
        comps: List[Any] = []
        for name in components_order:
            if name == "concept_id":
                comps.append(concept_id)
            elif "." in name:
                comps.append(resolve_path(rec, name))
            else:
                comps.append(ctx.get(name))

        joined = _join_components_for_hash(comps, null_rep=null_rep)
        instance_id = sha256_hex_text(joined)

        if dedup_within_run and instance_id in seen:
            continue

        seen[instance_id] = {
            "instance_id": instance_id,
            "concept_id": concept_id,
            "schema_version": schema_version_value,
            "instance_type": instance_type,
            "observed_at": {
                "asset_id": asset_id,
                "path": path,
                "value_index": value_index,
                "segment_index": segment_index,
                "sentence_index": sentence_index,
                "char_start": char_start,
                "char_end": char_end,
            },
            "unit_text": unit_text,
            "canonical_text": canonical_text,
            "content_hash": content_hash,
        }

    if fail_on_invalid and invalid_records > 0:
        raise RuntimeError(f"invalid_records > 0: {invalid_records}")

    decls = list(seen.values())
    decls.sort(key=lambda x: x["instance_id"])

    stats = BuildStats(
        input_records=input_records,
        accepted_records=accepted_records,
        skipped_by_gate=skipped_by_gate,
        invalid_records=invalid_records,
        unique_instances=len(decls),
    )
    return decls, stats, issues


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build instance identity declarations from normalized unit records (no SQL write)."
    )

    in_group = p.add_mutually_exclusive_group(required=True)
    in_group.add_argument("--inputs", nargs="+", help="Input JSONL files (normalized unit records).")
    in_group.add_argument("--input-dir", help="Directory containing input JSONL files.")

    p.add_argument("--config", default="config/action/config/ingest_instance_units_config_v0001.yml", help="YAML config file freezing canonicalization and anchor rules.")
    p.add_argument("--output", required=True, help="Output JSONL path for instance identity declarations.")
    p.add_argument("--run-meta", required=True, help="Run meta JSON path.")
    p.add_argument("--issues", default=None, help="Optional issues JSONL path.")
    p.add_argument("--fail-on-invalid", action="store_true", help="Fail the run if any invalid records are found.")
    p.add_argument("--dry-run", action="store_true", help="Dry run without writing any files.")

    return p.parse_args()


def main() -> int:
    args = parse_args()

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

    cfg_runtime = config.get("runtime", {}) if isinstance(config.get("runtime", {}), dict) else {}
    config_fail_on_invalid = bool(cfg_runtime.get("fail_on_invalid", False))
    effective_fail_on_invalid = bool(args.fail_on_invalid) or config_fail_on_invalid

    try:
        decls, stats, issues = build_instance_declarations(
            input_paths=input_paths,
            config=config,
            fail_on_invalid=effective_fail_on_invalid,
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
                "instance_id": d["instance_id"],
                "concept_id": d["concept_id"],
                "schema_version": d["schema_version"],
                "instance_type": d.get("instance_type"),
                "unit_text_length": len(d.get("unit_text", "")),
                "canonical_text_length": len(d.get("canonical_text", "")),
                "observed_at": d.get("observed_at"),
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
                    "effective_fail_on_invalid": effective_fail_on_invalid,
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
        "config": {"path": str(cfg_path), "sha256": cfg_sha, "version": config.get("version")},
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
        "effective_fail_on_invalid": effective_fail_on_invalid,
    }

    with run_meta_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(run_meta, ensure_ascii=False, indent=2))

    print(json.dumps(run_meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
