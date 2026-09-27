#!/usr/bin/env python3
# filename: validate_unit_boundaries_v0003.py
# 中文名: 结构单元边界稳定性校验脚本
# version: v0003
# layer: execution
# main_layer: action
# 可更新: True
#
# 职责说明:
# 本脚本用于在行动层中对全局候选结构单元执行"边界稳定性"校验。
# 它只基于结构与形式层事实生成边界状态标注，不进行语义判断，不进行重要性裁决。
#
# 本脚本做什么:
# - 读取 filter_structural_noise v0003 的全局输出（带 noise_decision）
# - 读取 profile_unit_structure v0003 的全局输出（候选层画像）
# - 扫描 normalize_unit_variants v0003 的目录（实例层），按 unit_text 汇总边界证据
# - 按 policy 输出 boundary_status 与 boundary_issues 及 evidence
#
# 本脚本不做什么:
# - 不删除或合并实例数据
# - 不进行语义推断，不使用词表，不使用 embedding
# - 不输出 KEEP/DROP 价值判断，不替 decide_unit_prominence 做决策
# - 不修改输入记录，仅在候选层汇总并新增校验字段
#
# v0002 相对 v0001 的变更:
# - 输入从 per-conversation 单文件改为全局模式
# - noise_decisions 和 profiles 都是全局单文件
# - 新增 --instance-dir 参数：扫描 normalize_variants v0002 目录汇总实例层证据
# - 流式写入，原子替换
# - 新增 --verbose 进度输出
# - run_meta 格式对齐 v0002 管线规范
# - v0001 保留不动，SQL 入库管线仍可使用


# ============================================================
# ALIAS_META
# ============================================================
# alias: validate_unit_boundaries
# family: validate_unit_boundaries
# role: execution
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/validate_unit_boundaries_v0003.py
# input:
#   - filter_structural_noise v0003 全局输出（单文件）
#   - profile_unit_structure v0003 全局输出（候选层画像）
#   - normalize_unit_variants v0003 目录（实例层，用于边界证据汇总）
# output:
#   - unit_boundary_validation.jsonl（全局单文件）
# depends_on:
#   - filter_structural_noise_v0003.py
#   - profile_unit_structure_v0003.py
#   - normalize_unit_variants_v0003.py (instance data)
# used_by:
#   - decide_unit_prominence_v0004.py


# ============================================================
# 制度与职责边界声明
# ============================================================
# - 本脚本仅输出边界校验状态 OK / UNSTABLE / ANOMALOUS
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
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None


# ============================================================
# 常量与全局配置
# ============================================================
SCRIPT_NAME = "validate_unit_boundaries_v0003.py"
SCRIPT_VERSION = "v0003"

STATUS_OK = "OK"
STATUS_UNSTABLE = "UNSTABLE"
STATUS_ANOMALOUS = "ANOMALOUS"

PROGRESS_INTERVAL = 100  # files
PROGRESS_INTERVAL_ROWS = 500000  # rows for instance scan


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_policy(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load policy YAML. Install: pip install pyyaml")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Policy YAML root must be a mapping object.")
    return data


def read_jsonl_stream(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def glob_jsonl_files(dir_path: Path) -> List[Path]:
    return sorted(dir_path.glob("*.jsonl"))


def variance_safe(values: List[int]) -> float:
    if len(values) < 2:
        return 0.0
    try:
        return float(statistics.pvariance(values))
    except Exception:
        return 0.0


def cross_ratio_from_counts(counts: Counter) -> float:
    """计算分布的跨越比：1 - (dominant_count / total)"""
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
# 实例层证据汇总
# ============================================================
def build_instance_evidence(
    instance_dir: Path,
    candidate_set: set,
    verbose: bool = False,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    """
    扫描 normalize_variants v0002 目录中的所有 JSONL 文件，
    按 unit_text 汇总实例层边界证据。
    只对 candidate_set 中的 unit_text 汇总，其余跳过。
    """
    acc: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "occurrences": 0,
        "lengths": [],
        "segment_counts": Counter(),
        "sentence_counts": Counter(),
    })

    counters = {
        "files_scanned": 0,
        "rows_read": 0,
        "rows_skipped": 0,
        "candidate_hits": 0,
    }

    files = glob_jsonl_files(instance_dir)
    if not files:
        if verbose:
            print(f"[BOUNDARY] WARNING: No JSONL files found in {instance_dir}", file=sys.stderr)
        return dict(acc), counters

    if verbose:
        print(f"[BOUNDARY] Instance scan: {len(files)} files in {instance_dir}", file=sys.stderr)

    for file_idx, fpath in enumerate(files):
        for record in read_jsonl_stream(fpath):
            counters["rows_read"] += 1

            unit_text = record.get("unit_text")
            if not isinstance(unit_text, str) or not unit_text:
                counters["rows_skipped"] += 1
                continue

            if unit_text not in candidate_set:
                continue

            counters["candidate_hits"] += 1

            # 长度证据：优先用 normalized_unit_text，fallback 到 unit_text
            text_for_length = record.get("normalized_unit_text")
            if not isinstance(text_for_length, str) or text_for_length == "":
                text_for_length = unit_text

            seg = record.get("segment_index")
            sent = record.get("sentence_index")

            acc[unit_text]["occurrences"] += 1
            acc[unit_text]["lengths"].append(len(text_for_length))

            if isinstance(seg, int):
                acc[unit_text]["segment_counts"][seg] += 1
            else:
                acc[unit_text]["segment_counts"]["__unknown__"] += 1

            if isinstance(seg, int) and isinstance(sent, int):
                acc[unit_text]["sentence_counts"][(seg, sent)] += 1
            else:
                acc[unit_text]["sentence_counts"][("__unknown__", "__unknown__")] += 1

        counters["files_scanned"] += 1

        if verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            print(
                f"[BOUNDARY] Instance scan progress: {file_idx + 1}/{len(files)} files, "
                f"{counters['rows_read']} rows, {counters['candidate_hits']} hits",
                file=sys.stderr,
            )

    if verbose:
        print(
            f"[BOUNDARY] Instance scan done: {counters['files_scanned']} files, "
            f"{counters['rows_read']} rows, {counters['candidate_hits']} candidate hits",
            file=sys.stderr,
        )

    return dict(acc), counters


# ============================================================
# 候选层边界校验
# ============================================================
def validate_candidate(
    profile_row: Dict[str, Any],
    evidence: Dict[str, Any],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    """
    对单个候选 unit_text 生成边界校验输出（候选层）。
    """
    out = dict(profile_row)

    occ = int(evidence.get("occurrences", 0))
    lengths: List[int] = evidence.get("lengths", []) or []
    seg_counts: Counter = evidence.get("segment_counts", Counter())
    sent_counts: Counter = evidence.get("sentence_counts", Counter())

    min_occ = int(policy.get("min_occurrence_count", 1))

    issues: List[str] = []
    status = STATUS_OK

    # 样本不足，保守不判异常
    if occ < min_occ:
        out["boundary_status"] = STATUS_OK
        out["boundary_issues"] = ["insufficient_sample"]
        out["boundary_evidence"] = {
            "occurrence_count_effective": occ,
            "min_occurrence_count": min_occ,
            "note": "below_min_occurrence_count",
        }
        return out

    # 长度方差
    length_var = variance_safe(lengths)
    if bool(policy.get("enable_length_variance_check", True)):
        length_flag = classify_by_thresholds(
            length_var, policy.get("length_variance_thresholds", {})
        )
        if length_flag == STATUS_UNSTABLE:
            issues.append("length_variance_high")
        elif length_flag == STATUS_ANOMALOUS:
            issues.append("length_variance_severe")

    # segment 跨越比例
    seg_ratio = 0.0
    if bool(policy.get("enable_segment_cross_check", True)):
        seg_ratio = cross_ratio_from_counts(seg_counts)
        seg_flag = classify_by_thresholds(
            seg_ratio, policy.get("max_segment_cross_ratio", {})
        )
        if seg_flag == STATUS_UNSTABLE:
            issues.append("segment_crossing_frequent")
        elif seg_flag == STATUS_ANOMALOUS:
            issues.append("segment_crossing_severe")

    # sentence 跨越比例
    sent_ratio = 0.0
    if bool(policy.get("enable_sentence_level_check", True)) and bool(
        policy.get("enable_sentence_cross_check", True)
    ):
        sent_ratio = cross_ratio_from_counts(sent_counts)
        sent_flag = classify_by_thresholds(
            sent_ratio, policy.get("max_sentence_cross_ratio", {})
        )
        if sent_flag == STATUS_UNSTABLE:
            issues.append("sentence_crossing_frequent")
        elif sent_flag == STATUS_ANOMALOUS:
            issues.append("sentence_crossing_severe")

    # 综合状态
    severe_issues = {
        "length_variance_severe",
        "segment_crossing_severe",
        "sentence_crossing_severe",
    }
    if any(x in severe_issues for x in issues):
        status = STATUS_ANOMALOUS
    elif issues:
        status = STATUS_UNSTABLE
    else:
        status = STATUS_OK

    out["boundary_status"] = status
    out["boundary_issues"] = issues
    out["boundary_evidence"] = {
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


# ============================================================
# 主流程
# ============================================================
def run(
    noise_decisions_path: Path,
    profiles_path: Path,
    instance_dir: Path,
    policy_path: Path,
    output_path: Path,
    run_meta_path: Path,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:

    started_at = utc_now_iso()
    policy = load_policy(policy_path)
    policy_version = policy.get("policy_version", "unknown")

    # ── Step 1: 读取 noise_decisions，构建候选 unit_text 集合 ──
    # 只取 noise_decision == KEEP 的候选
    candidate_set: set = set()
    noise_counters = {"rows_read": 0, "rows_keep": 0, "rows_drop": 0, "rows_skipped": 0}

    if verbose:
        print(f"[BOUNDARY] Loading noise decisions: {noise_decisions_path}", file=sys.stderr)

    for record in read_jsonl_stream(noise_decisions_path):
        noise_counters["rows_read"] += 1
        ut = record.get("unit_text")
        if not isinstance(ut, str) or not ut:
            noise_counters["rows_skipped"] += 1
            continue
        decision = record.get("noise_decision")
        if decision == "KEEP":
            candidate_set.add(ut)
            noise_counters["rows_keep"] += 1
        else:
            noise_counters["rows_drop"] += 1

    if verbose:
        print(
            f"[BOUNDARY] Noise decisions loaded: {noise_counters['rows_read']} rows, "
            f"{noise_counters['rows_keep']} KEEP, {noise_counters['rows_drop']} DROP → "
            f"{len(candidate_set)} candidate unit_texts",
            file=sys.stderr,
        )

    # ── Step 2: 扫描实例层目录，汇总边界证据 ──
    instance_evidence, instance_counters = build_instance_evidence(
        instance_dir=instance_dir,
        candidate_set=candidate_set,
        verbose=verbose,
    )

    if verbose:
        print(
            f"[BOUNDARY] Instance evidence built for {len(instance_evidence)} unit_texts",
            file=sys.stderr,
        )

    # ── Step 3: 读取 profiles，逐行做边界校验 ──
    profile_counters = {
        "rows_read": 0,
        "rows_skipped": 0,
        "candidates_validated": 0,
        "not_in_candidate_set": 0,
        "status_ok": 0,
        "status_unstable": 0,
        "status_anomalous": 0,
        "rows_written": 0,
    }

    output_f = None
    tmp_path = None
    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        output_f = tmp_path.open("w", encoding="utf-8", newline="\n")

    if verbose:
        print(f"[BOUNDARY] Validating profiles: {profiles_path}", file=sys.stderr)

    try:
        for record in read_jsonl_stream(profiles_path):
            profile_counters["rows_read"] += 1

            ut = record.get("unit_text")
            if not isinstance(ut, str) or not ut:
                profile_counters["rows_skipped"] += 1
                continue

            # 只校验在 candidate_set（KEEP）中的 unit
            if ut not in candidate_set:
                profile_counters["not_in_candidate_set"] += 1
                continue

            ev = instance_evidence.get(ut, {
                "occurrences": 0,
                "lengths": [],
                "segment_counts": Counter(),
                "sentence_counts": Counter(),
            })

            out_record = validate_candidate(record, ev, policy)

            st = out_record.get("boundary_status")
            if st == STATUS_OK:
                profile_counters["status_ok"] += 1
            elif st == STATUS_UNSTABLE:
                profile_counters["status_unstable"] += 1
            elif st == STATUS_ANOMALOUS:
                profile_counters["status_anomalous"] += 1

            profile_counters["candidates_validated"] += 1

            if not dry_run and output_f is not None:
                output_f.write(json.dumps(out_record, ensure_ascii=False) + "\n")
                profile_counters["rows_written"] += 1

            if verbose and profile_counters["rows_read"] % 50000 == 0:
                print(
                    f"[BOUNDARY] Profile progress: {profile_counters['rows_read']} rows, "
                    f"{profile_counters['candidates_validated']} validated",
                    file=sys.stderr,
                )

    finally:
        if output_f is not None:
            output_f.close()

    # 原子替换
    if not dry_run and tmp_path is not None:
        os.replace(str(tmp_path), str(output_path))

    if verbose:
        print(
            f"[BOUNDARY] Done: {profile_counters['candidates_validated']} validated "
            f"({profile_counters['status_ok']} OK / "
            f"{profile_counters['status_unstable']} UNSTABLE / "
            f"{profile_counters['status_anomalous']} ANOMALOUS)",
            file=sys.stderr,
        )

    # ── Run meta ──
    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": utc_now_iso(),
        "policy_version": policy_version,
        "inputs": {
            "noise_decisions": str(noise_decisions_path),
            "profiles": str(profiles_path),
            "instance_dir": str(instance_dir),
            "policy": str(policy_path),
        },
        "outputs": {
            "output": str(output_path),
            "run_meta": str(run_meta_path),
        },
        "noise_counters": noise_counters,
        "instance_counters": instance_counters,
        "profile_counters": profile_counters,
        "known_limitations": [
            "v0003 operates on global candidate set; noise_decisions and profiles are global single files.",
            "v0003 scans instance-dir (normalize_variants v0003 output) for boundary evidence.",
            "v0003 only validates candidates with noise_decision=KEEP; DROP candidates are excluded.",
            "v0003 uses unit_text as join key between profiles and instance evidence.",
            "v0003 does not drop candidates; it only marks boundary_status for downstream decisions.",
        ],
    }

    if not dry_run:
        run_meta_path.parent.mkdir(parents=True, exist_ok=True)
        with run_meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# ============================================================
# CLI
# ============================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validate structural unit boundary stability (global candidate-level). "
                    "v0002: reads global noise_decisions + profiles + instance-dir."
    )
    p.add_argument(
        "--noise-decisions", required=True, type=Path,
        help="Input JSONL: filter_structural_noise v0003 全局输出（带 noise_decision）",
    )
    p.add_argument(
        "--profiles", required=True, type=Path,
        help="Input JSONL: profile_unit_structure v0003 全局输出",
    )
    p.add_argument(
        "--instance-dir", required=True, type=Path,
        help="Directory: normalize_unit_variants v0003 输出目录（实例层 JSONL 文件）",
    )
    p.add_argument(
        "--output", type=Path,
        default="actioning/pipelines/validate_unit_boundaries_action/unit_boundary_validation.jsonl",
        help="Output JSONL: 全局边界校验结果（单文件）",
    )
    p.add_argument(
        "--rules", default="config/action/config/validate_unit_boundaries_v0001.yml", type=Path,
        help="Policy YAML file",
    )
    p.add_argument(
        "--run-meta", type=Path,
        default="actioning/pipelines/validate_unit_boundaries_action/run_meta.json",
        help="Run meta JSON output path",
    )
    p.add_argument("--dry-run", action="store_true", help="Dry run: 不写 output 文件")
    p.add_argument("--verbose", action="store_true", help="输出处理进度到 stderr")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.noise_decisions.exists():
        print(f"[ERROR] Noise decisions file not found: {args.noise_decisions}", file=sys.stderr)
        sys.exit(1)
    if not args.profiles.exists():
        print(f"[ERROR] Profiles file not found: {args.profiles}", file=sys.stderr)
        sys.exit(1)
    if not args.instance_dir.exists():
        print(f"[ERROR] Instance dir not found: {args.instance_dir}", file=sys.stderr)
        sys.exit(1)
    if not args.rules.exists():
        print(f"[ERROR] Policy file not found: {args.rules}", file=sys.stderr)
        sys.exit(1)

    meta = run(
        noise_decisions_path=args.noise_decisions,
        profiles_path=args.profiles,
        instance_dir=args.instance_dir,
        policy_path=args.rules,
        output_path=args.output,
        run_meta_path=args.run_meta,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    print(json.dumps(meta, ensure_ascii=False, indent=2))


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    main()
