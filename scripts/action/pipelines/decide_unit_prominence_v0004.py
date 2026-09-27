#!/usr/bin/env python3
# filename: decide_unit_prominence_v0004.py
# 中文名: 结构单元制度显著性裁决脚本
# version: v0004
# layer: governance
# main_layer: action
# 可更新: True
#
# 职责说明:
# 本脚本位于行动层分析管线阶段四（制度裁决与登记之前），
# 其职责是在不生成任何新结构事实、不修改任何既有字段的前提下，
# 基于全局候选层结构画像与全局候选层边界验证结果，
# 按外部 policy 执行制度裁决（ALLOW / DELAY / FREEZE），
# 并仅向候选记录追加制度裁决字段，供下游登记脚本使用。
#
# 本脚本做什么:
# - 读取候选层 unit 结构画像（profiles, 全局单文件）
# - 读取候选层边界验证（boundaries, 全局单文件）
# - 使用 unit_text 作为 Join Key 关联候选层结果
# - 依据外部 policy 执行制度裁决
# - 仅追加制度裁决字段输出到 JSONL，并输出 run_meta
#
# 本脚本不做什么:
# - 不读取实例层数据，不依赖 noise_decisions 实例文件
# - 不做语义判断，不用 embedding，不调用模型推理
# - 不修改任何输入记录的既有字段
# - 不执行登记或身份赋值
#
# v0003 相对 v0002 的变更:
# - 职责说明从"单一 conv"改为"全局候选层"
# - ALIAS_META depends_on 更新到 v0002 上游
# - import yaml 改为 graceful fallback (try/except)
# - now_utc_iso 改用 datetime.now(timezone.utc)，避免 deprecated utcnow()
# - 新增 --verbose 进度输出
# - 新增原子写入（.tmp + os.replace）
# - run_meta 新增 started_at
# - v0002 保留不动，SQL 入库管线仍可使用


# ============================================================
# ALIAS_META
# ============================================================
# alias: decide_unit_prominence
# family: decide_unit_prominence
# role: governance_decision
# version: v0004
# status: active
# entry_point: scripts/action/pipelines/decide_unit_prominence_v0004.py
# input:
#   - unit_structure_profiles.jsonl (全局候选层, key = unit_text)
#   - unit_boundary_validation.jsonl (全局候选层, key = unit_text)
#   - decide_unit_prominence_policy_v0001.yml
# output:
#   - unit_prominence_decisions.jsonl
# depends_on:
#   - profile_unit_structure_v0003.py
#   - validate_unit_boundaries_v0003.py
# used_by:
#   - register_structural_units_v0003.py


from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

SCRIPT_NAME = "decide_unit_prominence_v0004.py"
SCRIPT_VERSION = "v0004"
ALLOWED_DECISIONS = {"ALLOW", "DELAY", "FREEZE"}
PROGRESS_INTERVAL = 50000


# =========================
# IO helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def load_policy(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load policy YAML. Install: pip install pyyaml")
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # 兼容带 meta 头的 policy 文件
    if isinstance(raw, dict) and "meta" in raw:
        raw = {k: v for k, v in raw.items() if k != "meta"}

    if not isinstance(raw, dict):
        raise ValueError("policy file is not a mapping")

    # 基本校验
    pv = raw.get("policy_version")
    if not pv:
        raise ValueError("policy_version missing in policy")
    if "hard_constraints" not in raw or "noise_constraints" not in raw or "structural_role_rules" not in raw:
        raise ValueError("policy missing required sections: hard_constraints/noise_constraints/structural_role_rules")

    allowed = raw.get("allowed_decisions")
    if allowed:
        if not isinstance(allowed, list) or not set(allowed).issubset(ALLOWED_DECISIONS):
            raise ValueError("allowed_decisions invalid in policy")

    return raw


# =========================
# Decision context (candidate level)
# =========================
@dataclass(frozen=True)
class DecisionContext:
    unit_text: str
    boundary_status: Optional[str]
    noise_risk: Optional[str]
    structural_role: Optional[str]


def extract_structural_role(profile: Dict[str, Any]) -> Optional[str]:
    for k in ("structural_role", "role", "unit_role"):
        v = profile.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def extract_boundary_status(boundary: Dict[str, Any]) -> Optional[str]:
    for k in ("boundary_status", "status", "unit_boundary_status"):
        v = boundary.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def extract_noise_risk(boundary: Dict[str, Any]) -> Optional[str]:
    for k in ("noise_risk", "noise_level", "structural_noise_risk"):
        v = boundary.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


# =========================
# Policy application (no side effects)
# =========================
def apply_policy(ctx: DecisionContext, policy: Dict[str, Any]) -> Tuple[str, Dict[str, Any], float]:
    """
    仅基于候选层上下文执行制度裁决。
    不生成结构事实，不读取实例数据。
    返回: (decision, basis, audit_confidence)
    """
    boundary_status = ctx.boundary_status
    noise_risk = ctx.noise_risk
    structural_role = ctx.structural_role

    decision: Optional[str] = None

    STABLE_BOUNDARY_VALUES = {"stable", "ok"}

    bs_norm = boundary_status.strip().lower() if isinstance(boundary_status, str) else None

    # 1) Hard constraints
    if bool(policy["hard_constraints"].get("require_boundary_stable", False)):
        if bs_norm not in STABLE_BOUNDARY_VALUES:
            decision = policy["hard_constraints"].get("boundary_unstable_action", "DELAY")

    # 2) Noise constraints
    if decision is None and noise_risk:
        nr = noise_risk.lower().strip()
        if nr == "high":
            decision = policy["noise_constraints"].get("high_noise_action", "FREEZE")
        elif nr == "medium":
            decision = policy["noise_constraints"].get("medium_noise_action", "DELAY")
        elif nr == "low":
            decision = policy["noise_constraints"].get("low_noise_action", "ALLOW")

    # 3) Structural role rules
    if decision is None and structural_role:
        sr = structural_role.lower().strip()
        if sr == "stable":
            decision = policy["structural_role_rules"].get("stable_role_action", "ALLOW")
        elif sr == "diffuse":
            decision = policy["structural_role_rules"].get("diffuse_role_action", "DELAY")
        elif sr == "incidental":
            decision = policy["structural_role_rules"].get("incidental_role_action", "FREEZE")

    # 4) Fallback
    if decision not in ALLOWED_DECISIONS:
        if bs_norm in STABLE_BOUNDARY_VALUES:
            decision = "ALLOW"
        else:
            decision = "DELAY"

    basis: Dict[str, Any] = {
        "boundary_status": boundary_status,
        "noise_risk": noise_risk,
        "structural_role": structural_role,
    }

    # 审计置信度：仅用于说明依据充分程度，不是重要性评分
    evidence_count = 0
    if boundary_status is not None:
        evidence_count += 1
    if noise_risk is not None:
        evidence_count += 1
    if structural_role is not None:
        evidence_count += 1

    if decision == "ALLOW":
        audit_conf = 1.0 if evidence_count >= 2 else 0.75
    elif decision == "FREEZE":
        audit_conf = 0.75 if evidence_count >= 2 else 0.5
    else:
        audit_conf = 0.5 if evidence_count >= 1 else 0.25

    return decision, basis, audit_conf


# =========================
# Core run
# =========================
def build_boundary_map(
    boundaries_path: Path,
    verbose: bool = False,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    """
    候选层 Join Map: key = unit_text
    """
    m: Dict[str, Dict[str, Any]] = {}
    counters = {"rows_read": 0, "rows_skipped": 0, "rows_indexed": 0}
    for r in read_jsonl(boundaries_path):
        counters["rows_read"] += 1
        ut = r.get("unit_text")
        if not isinstance(ut, str) or not ut.strip():
            counters["rows_skipped"] += 1
            continue
        ut = ut.strip()
        m[ut] = r
        counters["rows_indexed"] += 1

    if verbose:
        print(
            f"[PROMINENCE] Boundary map loaded: {counters['rows_indexed']} indexed "
            f"({counters['rows_skipped']} skipped)",
            file=sys.stderr,
        )

    return m, counters


def run(
    profiles_path: Path,
    boundaries_path: Path,
    policy_path: Path,
    output_path: Path,
    run_meta_path: Path,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:

    started_at = utc_now_iso()

    policy = load_policy(policy_path)
    policy_version = policy.get("policy_version", "unknown")

    if verbose:
        print(f"[PROMINENCE] Loading boundaries: {boundaries_path}", file=sys.stderr)

    boundary_map, boundary_counters = build_boundary_map(boundaries_path, verbose=verbose)

    counters = {
        "profiles_rows_read": 0,
        "profiles_rows_skipped": 0,
        "joined_rows": 0,
        "join_miss": 0,
        "dec_allow": 0,
        "dec_delay": 0,
        "dec_freeze": 0,
        "rows_written": 0,
    }

    output_f = None
    tmp_path = None
    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        output_f = tmp_path.open("w", encoding="utf-8", newline="\n")

    if verbose:
        print(f"[PROMINENCE] Processing profiles: {profiles_path}", file=sys.stderr)

    try:
        for profile in read_jsonl(profiles_path):
            counters["profiles_rows_read"] += 1

            ut = profile.get("unit_text")
            if not isinstance(ut, str) or not ut.strip():
                counters["profiles_rows_skipped"] += 1
                continue
            unit_text = ut.strip()

            boundary = boundary_map.get(unit_text)
            if boundary is None:
                counters["join_miss"] += 1
                boundary = {}
                join_status = "miss"
            else:
                counters["joined_rows"] += 1
                join_status = "hit"

            ctx = DecisionContext(
                unit_text=unit_text,
                boundary_status=extract_boundary_status(boundary),
                noise_risk=extract_noise_risk(boundary),
                structural_role=extract_structural_role(profile),
            )

            decision, basis, audit_conf = apply_policy(ctx, policy)

            if decision == "ALLOW":
                counters["dec_allow"] += 1
            elif decision == "FREEZE":
                counters["dec_freeze"] += 1
            else:
                counters["dec_delay"] += 1

            basis["join_status"] = join_status

            # 只追加字段，不修改任何既有字段
            out_record = dict(profile)
            appended_fields = {
                "decision": decision,
                "decision_basis": basis,
                "audit_confidence": audit_conf,
                "decision_policy_version": policy_version,
                "decision_timestamp": utc_now_iso(),
            }

            # Field conflict guard
            conflicts = [k for k in appended_fields if k in out_record]
            if conflicts:
                raise RuntimeError(
                    f"[{SCRIPT_NAME}] Field conflict: upstream profile already contains "
                    f"fields that this script would overwrite: {conflicts}. "
                    f"unit_text={unit_text!r}"
                )

            out_record.update(appended_fields)

            if not dry_run and output_f is not None:
                output_f.write(json.dumps(out_record, ensure_ascii=False) + "\n")
                counters["rows_written"] += 1

            if verbose and counters["profiles_rows_read"] % PROGRESS_INTERVAL == 0:
                print(
                    f"[PROMINENCE] Progress: {counters['profiles_rows_read']} rows, "
                    f"{counters['dec_allow']} ALLOW / {counters['dec_delay']} DELAY / "
                    f"{counters['dec_freeze']} FREEZE",
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
            f"[PROMINENCE] Done: {counters['profiles_rows_read']} profiles → "
            f"{counters['dec_allow']} ALLOW / {counters['dec_delay']} DELAY / "
            f"{counters['dec_freeze']} FREEZE "
            f"(join_miss={counters['join_miss']})",
            file=sys.stderr,
        )

    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": utc_now_iso(),
        "policy_version": policy_version,
        "inputs": {
            "profiles": str(profiles_path),
            "boundaries": str(boundaries_path),
            "policy": str(policy_path),
        },
        "outputs": {
            "output": str(output_path),
            "run_meta": str(run_meta_path),
        },
        "counters": {
            **counters,
            "boundaries_rows_read": boundary_counters["rows_read"],
            "boundaries_rows_skipped": boundary_counters["rows_skipped"],
            "boundaries_rows_indexed": boundary_counters["rows_indexed"],
        },
        "known_limitations": [
            "v0004 operates on global candidate-level data and joins by unit_text.",
            "v0004 does not read any instance-level noise decisions.",
            "v0004 relies on boundary output to provide explicit noise_risk when available; otherwise noise constraints may not apply.",
            "v0004 only appends decision fields and never modifies upstream fields.",
        ],
    }

    if not dry_run:
        run_meta_path.parent.mkdir(parents=True, exist_ok=True)
        with run_meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Global candidate-level unit prominence decision (policy-driven). "
                    "v0004: reads global profiles + boundaries, adds --verbose, atomic write."
    )
    p.add_argument("--profiles", required=True, type=Path,
                    help="全局候选层 unit profiles JSONL（must include unit_text）")
    p.add_argument("--boundaries", required=True, type=Path,
                    help="全局候选层 unit boundary validation JSONL（must include unit_text）")
    p.add_argument("--policy", default="config/action/config/decide_unit_prominence_policy_v0001.yml", type=Path,
                    help="Decision policy YAML")
    p.add_argument("--output", type=Path,
                    default="actioning/pipelines/decide_unit_prominence_action/unit_prominence_decisions.jsonl",
                    help="Output JSONL with appended decision fields")
    p.add_argument("--run-meta", type=Path,
                    default="actioning/pipelines/decide_unit_prominence_action/run_meta.json",
                    help="Run meta JSON output path")
    p.add_argument("--dry-run", action="store_true",
                    help="Dry run: 不写 output/run_meta，print meta to stdout")
    p.add_argument("--verbose", action="store_true",
                    help="输出处理进度到 stderr")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.profiles.exists():
        print(f"[ERROR] Profiles file not found: {args.profiles}", file=sys.stderr)
        sys.exit(1)
    if not args.boundaries.exists():
        print(f"[ERROR] Boundaries file not found: {args.boundaries}", file=sys.stderr)
        sys.exit(1)
    if not args.policy.exists():
        print(f"[ERROR] Policy file not found: {args.policy}", file=sys.stderr)
        sys.exit(1)

    meta = run(
        profiles_path=args.profiles,
        boundaries_path=args.boundaries,
        policy_path=args.policy,
        output_path=args.output,
        run_meta_path=args.run_meta,
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
    )

    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
