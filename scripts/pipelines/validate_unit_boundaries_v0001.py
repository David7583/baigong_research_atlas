# filename: validate_unit_boundaries_v0001.py
# 中文名: 结构单元边界稳定性校验脚本
# version: v0001
# layer: execution
# main_layer: understand
# 可更新: True
#
# 职责说明
# 本脚本用于在理解层中对候选结构单元执行“边界稳定性”校验。
# 它只基于结构与形式层事实生成边界状态标注，不进行语义判断，不进行重要性裁决。
#
# 本脚本做什么
# - 读取 filter_structural_noise 的结果（实例层，带 noise_decision）
# - 读取 profile_unit_structure 的候选画像（候选层）
# - 以候选 unit_text 为主键，汇总实例层边界相关证据
# - 按 policy 输出 boundary_status 与 boundary_issues 及 evidence
#
# 本脚本不做什么
# - 不删除或合并实例数据
# - 不进行语义推断，不使用词表，不使用 embedding
# - 不输出 KEEP/DROP 价值判断，不替 decide_unit_prominence 做决策
#
# 输出对象说明
# 输出为“候选层”校验结果，每个候选 unit_text 输出一行。
# 该输出可作为后续 decide_unit_prominence 的可信度信号输入。


# ============================================================
# ALIAS_META
# ============================================================
# alias: validate_unit_boundaries
# family: validate_unit_boundaries
# role: execution
# version: v0001
# status: active
# entry_point: validate_unit_boundaries_v0001.py
# input:
#   - unit_structural_noise_decisions.jsonl
#   - unit_structure_profiles.jsonl
# output:
#   - unit_boundary_validation.jsonl
# depends_on:
#   - filter_structural_noise_v0001.py
#   - profile_unit_structure_v0001.py
# used_by:
#   - decide_unit_prominence_v0001.py


# ============================================================
# 制度与职责边界声明
# ============================================================
# - 本脚本仅输出边界校验状态 OK / UNSTABLE / ANOMALOUS / SKIP
# - 所有阈值与判定尺度必须来自 policy
# - 本脚本不产生淘汰决策，不做语义判断
# - 本脚本不修改输入记录，仅在候选层汇总并新增校验字段


# ============================================================
# Imports
# ============================================================
from __future__ import annotations

import argparse
import json
import os
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


# ============================================================
# 常量与全局配置
# ============================================================
STATUS_OK = "OK"
STATUS_UNSTABLE = "UNSTABLE"
STATUS_ANOMALOUS = "ANOMALOUS"
STATUS_SKIP = "SKIP"


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def is_windows_nul(path_str: str) -> bool:
    return path_str.strip().upper() == "NUL"


def read_jsonl(path: Path, encoding: str) -> Iterable[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding=encoding) as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    yield line_no, obj
                else:
                    yield line_no, {"__raw__": obj}
            except Exception as e:
                yield line_no, {"__parse_error__": str(e), "__raw_line__": s}


def write_jsonl(path_str: str, rows: Iterable[Dict[str, Any]], encoding: str, dry_run: bool) -> int:
    if dry_run or is_windows_nul(path_str):
        return sum(1 for _ in rows)

    path = Path(path_str)
    ensure_parent_dir(path)
    tmp = path.with_suffix(path.suffix + ".tmp")

    written = 0
    with tmp.open("w", encoding=encoding, newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            written += 1

    os.replace(str(tmp), str(path))
    return written


def load_policy(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load policy YAML.")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Policy YAML root must be a mapping object.")
    return data


def get_policy_body(policy: Dict[str, Any]) -> Dict[str, Any]:
    # 支持 policy 顶层带 meta: 的结构
    # 执行语义只从同级字段读取
    return policy


def variance_safe(values: List[int]) -> float:
    if len(values) < 2:
        return 0.0
    try:
        return float(statistics.pvariance(values))
    except Exception:
        return 0.0


def cross_ratio_from_counts(counts: Counter) -> float:
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    if len(counts) <= 1:
        return 0.0
    dominant = max(counts.values())
    return max(0.0, 1.0 - (dominant / total))


def classify_by_thresholds(value: float, thresholds: Dict[str, Any]) -> str:
    ok_max = float(thresholds.get("ok_max", 0.0))
    unstable_max = float(thresholds.get("unstable_max", ok_max))
    if value <= ok_max:
        return STATUS_OK
    if value <= unstable_max:
        return STATUS_UNSTABLE
    return STATUS_ANOMALOUS


# ============================================================
# 核心业务逻辑区
# ============================================================
def build_instance_evidence(
    noise_decisions_path: Path,
    encoding: str,
    warnings_sample: List[Dict[str, Any]],
    counters: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    从实例层噪声判定文件汇总候选 unit_text 的边界证据。
    仅对 noise_decision=KEEP 的实例计入统计。
    """
    acc: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "occurrences": 0,
        "lengths": [],
        "segment_counts": Counter(),
        "sentence_counts": Counter(),
        "dropped_occurrences": 0,
        "drop_reasons_sample": [],
    })

    for line_no, rec in read_jsonl(noise_decisions_path, encoding):
        counters["noise_rows_read"] += 1

        if "__parse_error__" in rec:
            counters["warnings_count"] += 1
            if len(warnings_sample) < 10:
                warnings_sample.append({"kind": "json_parse_error", "line_no": line_no, "detail": rec.get("__parse_error__")})
            continue

        unit_text = rec.get("unit_text")
        if not isinstance(unit_text, str) or not unit_text:
            counters["noise_rows_skipped"] += 1
            continue

        noise_decision = rec.get("noise_decision")
        if noise_decision == "DROP":
            acc[unit_text]["dropped_occurrences"] += 1
            rs = rec.get("noise_reasons", [])
            if isinstance(rs, list) and rs and len(acc[unit_text]["drop_reasons_sample"]) < 5:
                acc[unit_text]["drop_reasons_sample"].append(rs[:])
            continue

        if noise_decision != "KEEP":
            # 未知或缺失，保守跳过
            counters["noise_rows_skipped"] += 1
            continue

        text_for_length = rec.get("normalized_unit_text")
        if not isinstance(text_for_length, str) or text_for_length == "":
            text_for_length = unit_text

        seg = rec.get("segment_index")
        sent = rec.get("sentence_index")

        acc[unit_text]["occurrences"] += 1
        acc[unit_text]["lengths"].append(len(text_for_length))

        if isinstance(seg, int):
            acc[unit_text]["segment_counts"][seg] += 1
        else:
            acc[unit_text]["segment_counts"]["__unknown__"] += 1

        # 句级统计使用 (segment_index, sentence_index) 组合以避免跨段混淆
        if isinstance(seg, int) and isinstance(sent, int):
            acc[unit_text]["sentence_counts"][(seg, sent)] += 1
        else:
            acc[unit_text]["sentence_counts"][("__unknown__", "__unknown__")] += 1

    return acc


def validate_candidate(
    profile_row: Dict[str, Any],
    evidence: Dict[str, Any],
    policy_body: Dict[str, Any],
) -> Dict[str, Any]:
    """
    对单个候选 unit_text 生成边界校验输出（候选层）。
    """
    unit_text = profile_row.get("unit_text")

    out = dict(profile_row)

    occ = int(evidence.get("occurrences", 0))
    lengths: List[int] = evidence.get("lengths", []) or []
    seg_counts: Counter = evidence.get("segment_counts", Counter())
    sent_counts: Counter = evidence.get("sentence_counts", Counter())

    min_occ = int(policy_body.get("min_occurrence_count", 1))

    # 默认
    issues: List[str] = []
    status = STATUS_OK

    # 样本不足，保守不判异常
    if occ < min_occ:
        out["boundary_status"] = STATUS_OK
        out["boundary_issues"] = ["insufficient_sample"]
        out["evidence"] = {
            "occurrence_count_effective": occ,
            "min_occurrence_count": min_occ,
            "note": "below_min_occurrence_count",
        }
        return out

    # 长度方差
    length_var = variance_safe(lengths)
    length_flag = STATUS_OK
    if bool(policy_body.get("enable_length_variance_check", True)):
        length_flag = classify_by_thresholds(length_var, policy_body.get("length_variance_thresholds", {}))
        if length_flag == STATUS_UNSTABLE:
            issues.append("length_variance_high")
        elif length_flag == STATUS_ANOMALOUS:
            issues.append("length_variance_severe")

    # segment 跨越比例
    seg_ratio = 0.0
    seg_flag = STATUS_OK
    if bool(policy_body.get("enable_segment_cross_check", True)):
        seg_ratio = cross_ratio_from_counts(seg_counts)
        seg_flag = classify_by_thresholds(seg_ratio, policy_body.get("max_segment_cross_ratio", {}))
        if seg_flag == STATUS_UNSTABLE:
            issues.append("segment_crossing_frequent")
        elif seg_flag == STATUS_ANOMALOUS:
            issues.append("segment_crossing_severe")

    # sentence 跨越比例
    sent_ratio = 0.0
    sent_flag = STATUS_OK
    if bool(policy_body.get("enable_sentence_level_check", True)) and bool(policy_body.get("enable_sentence_cross_check", True)):
        sent_ratio = cross_ratio_from_counts(sent_counts)
        sent_flag = classify_by_thresholds(sent_ratio, policy_body.get("max_sentence_cross_ratio", {}))
        if sent_flag == STATUS_UNSTABLE:
            issues.append("sentence_crossing_frequent")
        elif sent_flag == STATUS_ANOMALOUS:
            issues.append("sentence_crossing_severe")

    # 综合状态
    if any(x in issues for x in ["length_variance_severe", "segment_crossing_severe", "sentence_crossing_severe"]):
        status = STATUS_ANOMALOUS
    elif issues:
        status = STATUS_UNSTABLE
    else:
        status = STATUS_OK

    out["boundary_status"] = status
    out["boundary_issues"] = issues
    out["evidence"] = {
        "occurrence_count_effective": occ,
        "length_variance": length_var,
        "length_min": min(lengths) if lengths else None,
        "length_max": max(lengths) if lengths else None,
        "segment_cross_ratio": seg_ratio,
        "sentence_cross_ratio": sent_ratio,
        "segments_observed": len([k for k in seg_counts.keys() if k != "__unknown__"]),
        "sentences_observed": len([k for k in sent_counts.keys() if k != ("__unknown__", "__unknown__")]),
    }

    return out


def generate_validation_rows(
    profiles_path: Path,
    instance_evidence: Dict[str, Dict[str, Any]],
    encoding: str,
    policy_body: Dict[str, Any],
    warnings_sample: List[Dict[str, Any]],
    counters: Dict[str, Any],
) -> Iterable[Dict[str, Any]]:
    for line_no, row in read_jsonl(profiles_path, encoding):
        counters["profiles_rows_read"] += 1

        if "__parse_error__" in row:
            counters["warnings_count"] += 1
            if len(warnings_sample) < 10:
                warnings_sample.append({"kind": "profiles_json_parse_error", "line_no": line_no, "detail": row.get("__parse_error__")})
            continue

        unit_text = row.get("unit_text")
        if not isinstance(unit_text, str) or not unit_text:
            counters["profiles_rows_skipped"] += 1
            continue

        ev = instance_evidence.get(unit_text, {
            "occurrences": 0,
            "lengths": [],
            "segment_counts": Counter(),
            "sentence_counts": Counter(),
            "dropped_occurrences": 0,
            "drop_reasons_sample": [],
        })

        out = validate_candidate(row, ev, policy_body)

        st = out.get("boundary_status")
        if st == STATUS_OK:
            counters["status_ok"] += 1
        elif st == STATUS_UNSTABLE:
            counters["status_unstable"] += 1
        elif st == STATUS_ANOMALOUS:
            counters["status_anomalous"] += 1
        elif st == STATUS_SKIP:
            counters["status_skip"] += 1

        counters["candidates_written"] += 1
        yield out


# ============================================================
# CLI / main 接口区
# ============================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate structural unit boundary stability (candidate-level).")
    p.add_argument("--noise-decisions", required=True, help="Input JSONL from filter_structural_noise (instance-level, with noise_decision).")
    p.add_argument("--profiles", required=True, help="Input JSONL from profile_unit_structure (candidate-level).")
    p.add_argument("--output", required=True, help="Output JSONL path for boundary validation (candidate-level) or NUL on Windows.")
    p.add_argument("--rules", required=True, help="Policy YAML path: validate_unit_boundaries_v0001.yml")
    p.add_argument("--run-meta", required=True, help="Run meta JSON output path.")
    p.add_argument("--encoding", default="utf-8", help="Text encoding for IO. Default utf-8.")
    p.add_argument("--dry-run", action="store_true", help="Dry run. Output JSONL will not be written.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    noise_path = Path(args.noise_decisions)
    profiles_path = Path(args.profiles)
    rules_path = Path(args.rules)
    run_meta_path = Path(args.run_meta)

    policy = load_policy(rules_path)
    policy_body = get_policy_body(policy)

    warnings_sample: List[Dict[str, Any]] = []
    counters: Dict[str, Any] = {
        "noise_rows_read": 0,
        "noise_rows_skipped": 0,
        "profiles_rows_read": 0,
        "profiles_rows_skipped": 0,
        "candidates_written": 0,
        "status_ok": 0,
        "status_unstable": 0,
        "status_anomalous": 0,
        "status_skip": 0,
        "warnings_count": 0,
    }

    instance_evidence = build_instance_evidence(
        noise_decisions_path=noise_path,
        encoding=args.encoding,
        warnings_sample=warnings_sample,
        counters=counters,
    )

    rows_iter = generate_validation_rows(
        profiles_path=profiles_path,
        instance_evidence=instance_evidence,
        encoding=args.encoding,
        policy_body=policy_body,
        warnings_sample=warnings_sample,
        counters=counters,
    )

    rows_written = write_jsonl(args.output, rows_iter, args.encoding, args.dry_run)

    run_meta = {
        "status": "ok",
        "script_name": "validate_unit_boundaries_v0001.py",
        "script_version": "v0001",
        "generated_at": utc_now_iso(),
        "policy_version": policy_body.get("policy_version", None),
        "input_paths": {
            "noise_decisions": str(noise_path),
            "profiles": str(profiles_path),
            "rules": str(rules_path),
        },
        "output_paths": {
            "output": args.output,
            "run_meta": str(run_meta_path),
        },
        "counters": {
            **counters,
            "rows_written": rows_written,
            "units_with_instance_evidence": len(instance_evidence),
        },
        "warnings_sample": warnings_sample,
        "known_limitations": [
            "v0001 boundary signals are computed from instance-level distribution proxies; no semantic inference is performed.",
            "v0001 uses candidate unit_text as the join key between profiles and instance evidence.",
            "v0001 does not drop candidates; it only marks boundary_status for downstream decisions.",
        ],
    }

    ensure_parent_dir(run_meta_path)
    with run_meta_path.open("w", encoding="utf-8") as f:
        json.dump(run_meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(run_meta, ensure_ascii=False, indent=2))
    return 0


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
