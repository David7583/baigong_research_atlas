# ============================================================
# 文件名: ingest_attribute_units_v0003.py
# 中文名: 属性声明生成脚本
# 版本号: v0003
#
# 主层级: action
# 子层级: anchor
# 脚本定位: 行动端裁决结果进入事实层之前的属性声明生成器
# 可更新: True
#
# 职责说明:
# - 读取行动端裁决结果 JSONL（prominence decisions）
# - 从 unit_text 生成 concept_id（与 ingest_concept_units 规则一致）
# - 按 config 中的 attribute_mappings 展开为 attribute 声明
# - 校验展开后的 attr_value 是否属于制度允许空间
# - 同一运行内按规则去重
# - 输出供 SQL 写入层使用
#
# 本脚本不做什么:
# - 不写入 SQL
# - 不修改历史
# - 不生成 superseded
# - 不推断
#
# 统计守恒:
# input_records = accepted_records + skipped_by_gate + invalid_records
#
# 批次原子性语义声明:
# - fail_on_invalid = True: 任何无效记录导致整个批次拒绝
# - fail_on_invalid = False: 跳过无效记录，处理所有有效记录
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: ingest_attribute_units
# family: anchor
# role: attribute_declaration_builder
# version: v0003
# status: active
# entry_point: scripts/action/anchor/ingest_attribute_units_v0003.py
# input:
#   - prominence_decisions (jsonl)
#   - config (yml)
# output:
#   - attribute_declarations (jsonl)
#   - run_meta (json)
#   - optional issues (jsonl)
# depends_on:
#   - decide_unit_prominence_v0004
# used_by:
#   - sql_writer_v0004
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml
except Exception as e:
    yaml = None
    _YAML_IMPORT_ERROR = e


SCRIPT_NAME = "ingest_attribute_units_v0003.py"
SCRIPT_VERSION = "v0003"
ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

DEFAULT_CANONICAL_UNICODE = "NFC"
VALID_UNICODE_FORMS = {"NFC", "NFD", "NFKC", "NFKD"}

# ============================================================
# 工具函数
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def sha256_hex_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


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


def load_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError(f"PyYAML missing: {_YAML_IMPORT_ERROR}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("config must be mapping")
    return data


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
            line = safe_json(r) + "\n"
            f.write(line)
            h.update(line.encode("utf-8"))
            count += 1
    shutil.move(str(tmp), str(path))
    return count, h.hexdigest()


# ============================================================
# 文本规范化
# ============================================================

_ws_re = re.compile(r"\s+", flags=re.UNICODE)


def collapse_internal_whitespace(s: str) -> str:
    return _ws_re.sub(" ", s)


def canonicalize_text(unit_text: str, rules: Dict[str, Any]) -> str:
    s = unit_text

    if rules.get("normalize_line_endings", False):
        s = s.replace("\r\n", "\n").replace("\r", "\n")

    form = rules.get("unicode_normalization", DEFAULT_CANONICAL_UNICODE)
    if form not in VALID_UNICODE_FORMS:
        raise ValueError("invalid unicode_normalization")

    s = unicodedata.normalize(form, s)

    if rules.get("trim_whitespace", False):
        s = s.strip()

    if rules.get("collapse_internal_whitespace", False):
        s = collapse_internal_whitespace(s)

    if rules.get("case_folding", False):
        s = s.casefold()

    if rules.get("fullwidth_to_halfwidth", False):
        s = unicodedata.normalize("NFKC", s)

    return s


# ============================================================
# Attribute 展开
# ============================================================

def resolve_source_field(rec: Dict[str, Any], source_field: str) -> Any:
    parts = source_field.split(".")
    cur: Any = rec
    for p in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def expand_attributes(
    rec: Dict[str, Any],
    concept_id: str,
    object_type: str,
    mappings: Dict[str, Dict[str, Any]],
    default_state: str,
    provenance: Dict[str, Any],
) -> List[Dict[str, Any]]:

    results = []

    for attr_key, mapping in mappings.items():

        raw_value = resolve_source_field(rec, mapping.get("source_field", ""))

        if raw_value is None and mapping.get("skip_if_null", False):
            continue

        if raw_value is None:
            continue

        results.append({
            "object_type": object_type,
            "object_id": concept_id,
            "attr_key": attr_key,
            "attr_value": str(raw_value),
            "attr_state": default_state,
            "provenance": provenance,
            "generated_at": utc_now_iso(),
        })

    return results


# ============================================================
# 核心逻辑
# ============================================================

@dataclass
class Stats:
    input_records: int
    accepted_records: int
    skipped_by_gate: int
    invalid_records: int
    unique_records: int


def build_attributes(
    input_paths: List[Path],
    config: Dict[str, Any],
    fail_on_invalid: bool,
) -> Tuple[List[Dict[str, Any]], Stats, List[Dict[str, Any]]]:

    canonical_rules = config.get("canonicalization", {})
    mappings = config.get("attribute_mappings")

    obj_binding = config.get("object_binding", {})
    default_object_type = obj_binding.get("default_object_type", "concept")

    attr_schema = config.get("attribute_schema")
    namespaces = attr_schema.get("namespaces")
    reject_unknown_attr = attr_schema.get("reject_unknown_attribute", True)

    state_model = config.get("state_model")
    default_state = state_model.get("default_state")

    conflict_policy = config.get("conflict_policy")
    dup_resolution = conflict_policy.get("duplicate_resolution")

    gate_cfg = config.get("decision_gate", {})
    gate_enabled = bool(gate_cfg.get("enabled", False))
    gate_allow_value = gate_cfg.get("allow_value", "ALLOW")

    provenance = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "config_version": config.get("version", "unknown"),
    }

    seen: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    issues: List[Dict[str, Any]] = []

    input_records = 0
    accepted_records = 0
    skipped_by_gate = 0
    invalid_records = 0

    for path in input_paths:
        for line_no, rec in read_jsonl(path):

            input_records += 1

            if rec.get("__parse_error__"):
                invalid_records += 1
                continue

            if gate_enabled and rec.get("decision") != gate_allow_value:
                skipped_by_gate += 1
                continue

            unit_text = rec.get("unit_text")

            if not isinstance(unit_text, str):
                invalid_records += 1
                continue

            canonical_text = canonicalize_text(unit_text, canonical_rules)
            concept_id = sha256_hex_text(canonical_text)

            expanded = expand_attributes(
                rec,
                concept_id,
                default_object_type,
                mappings,
                default_state,
                provenance,
            )

            if not expanded:
                invalid_records += 1
                continue

            all_valid = True

            for attr_rec in expanded:

                ak = attr_rec["attr_key"]
                av = attr_rec["attr_value"]

                if ak not in namespaces:

                    if reject_unknown_attr:
                        all_valid = False
                        issues.append({
                            "type": "unknown_attr_key",
                            "key": ak,
                            "source_file": str(path),
                            "line": line_no
                        })
                        break

                else:

                    ns = namespaces.get(ak, {})
                    allowed = ns.get("allowed_values")

                    if isinstance(allowed, list) and av not in allowed:
                        all_valid = False
                        issues.append({
                            "type": "invalid_attr_value",
                            "key": ak,
                            "value": av,
                            "source_file": str(path),
                            "line": line_no
                        })
                        break

            if not all_valid:
                invalid_records += 1
                continue

            accepted_records += 1

            for attr_rec in expanded:

                key = (
                    attr_rec["object_type"],
                    attr_rec["object_id"],
                    attr_rec["attr_key"],
                )

                if dup_resolution == "keep_last":
                    seen[key] = attr_rec
                elif key not in seen:
                    seen[key] = attr_rec

    results = list(seen.values())

    stats = Stats(
        input_records,
        accepted_records,
        skipped_by_gate,
        invalid_records,
        len(results),
    )

    return results, stats, issues


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:

    p = argparse.ArgumentParser()

    p.add_argument("--inputs", nargs="+")
    p.add_argument("--config", default="config/action/config/ingest_attribute_units_config_v0001.yml")
    p.add_argument("--output", required=True)
    p.add_argument("--run-meta", required=True)
    p.add_argument("--issues")

    return p.parse_args()


def main() -> int:

    args = parse_args()

    input_paths = [Path(p) for p in args.inputs]

    cfg = load_yaml(Path(args.config))

    results, stats, issues = build_attributes(
        input_paths,
        cfg,
        fail_on_invalid=False,
    )

    out_path = Path(args.output)

    ensure_dir(out_path.parent)

    rows_written, output_hash = atomic_write_jsonl(out_path, results)

    run_meta = {
        "status": "ok",
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "generated_at": utc_now_iso(),
        "stats": stats.__dict__,
        "output": {
            "path": str(out_path),
            "rows_written": rows_written,
            "sha256": output_hash,
        },
    }

    with Path(args.run_meta).open("w", encoding="utf-8") as f:
        json.dump(run_meta, f, indent=2, ensure_ascii=False)

    print(json.dumps(run_meta, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
