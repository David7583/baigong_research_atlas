# filename: decide_parse_eligibility_v0003.py
# version: v0003
# layer: execution
# main_layer: action
#
# v0002 changes:
# - match_mode: any (default) / all (all conditions must hit)
# - allow_override: ALLOW rules override DELAY but never FREEZE
# - _composite rule type for dynamic observation traversal
# - Output dir auto-creation

# ALIAS_META
# alias: decide_parse_eligibility_v0003
# family: decide_parse_eligibility
# role: parse_entry_decision
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/decide_parse_eligibility_v0003.py
# input: observations_jsonl, parse_eligibility_policy_v0002.yml
# output: eligibility_decisions_jsonl
# depends_on: profile_string_values_v0004
# used_by: language_parse_lite_v0003

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml  # type: ignore

ALLOWED_DECISIONS = {"ALLOW", "DELAY", "FREEZE"}

DECISION_PRIORITY = {
    "FREEZE": 3,
    "DELAY": 2,
    "ALLOW": 1,
}

DEFAULT_ENCODING = "utf-8"


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_policy(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        return yaml.safe_load(f)


def get_metric_value(observations: Dict[str, Any], metric_path: str) -> Any:
    parts = metric_path.split(".")
    cur: Any = observations
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def evaluate_condition(value: Any, condition: Dict[str, Any]) -> bool:
    if value is None:
        return False
    if "gt" in condition:
        return value > condition["gt"]
    if "lt" in condition:
        return value < condition["lt"]
    if "ge" in condition:
        return value >= condition["ge"]
    if "le" in condition:
        return value <= condition["le"]
    return False


def resolve_thresholds(obj: Any, thresholds: Dict[str, Any]) -> Any:
    if isinstance(obj, dict):
        return {k: resolve_thresholds(v, thresholds) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_thresholds(v, thresholds) for v in obj]
    if isinstance(obj, str) and obj.startswith("$" + "{thresholds."):
        key = obj.replace("$" + "{thresholds.", "").replace("}", "")
        parts = key.split(".")
        cur: Any = thresholds
        for p in parts:
            cur = cur[p]
        return cur
    return obj


# v0002: composite rule evaluators

def evaluate_composite_dominant_block_low_entropy(
    observations: Dict[str, Any],
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    block_entropy_max = params.get("block_entropy_max", 1.5)
    block_dominance_min = params.get("block_dominance_min", 0.5)
    unicode_blocks = observations.get("unicode_blocks", {})
    entropy_by_block = observations.get("entropy_by_block", {})

    for block_label, ratio in unicode_blocks.items():
        if ratio < block_dominance_min:
            continue
        block_entropy = entropy_by_block.get(block_label)
        if block_entropy is None:
            continue
        if block_entropy <= block_entropy_max:
            return {
                "block": block_label,
                "block_ratio": ratio,
                "block_entropy": block_entropy,
                "block_entropy_max": block_entropy_max,
                "block_dominance_min": block_dominance_min,
            }
    return None


COMPOSITE_EVALUATORS = {
    "dominant_block_low_entropy": evaluate_composite_dominant_block_low_entropy,
}


# v0002: rule evaluation with match_mode

def evaluate_rule(
    rule: Dict[str, Any],
    observations: Dict[str, Any],
    thresholds: Dict[str, Any],
) -> tuple:
    """
    Returns (hits, has_missing).
    hits: list of evidence dicts (empty = not triggered)
    has_missing: True if any standard condition referenced a metric
                 not present in observations
    """
    rule_id = rule.get("id")
    when = resolve_thresholds(rule.get("when", {}), thresholds)
    match_mode = rule.get("match_mode", "any")

    composite_type = when.get("_composite")
    if composite_type is not None:
        evaluator = COMPOSITE_EVALUATORS.get(composite_type)
        if evaluator is None:
            return [], False
        params = resolve_thresholds(when.get("_params", {}), thresholds)
        evidence = evaluator(observations, params)
        if evidence is not None:
            return [{
                "rule_id": rule_id,
                "metric": "_composite:" + composite_type,
                "value": evidence,
                "condition": params,
            }], False
        return [], False

    standard_conditions = {
        k: v for k, v in when.items()
        if not k.startswith("_")
    }

    if not standard_conditions:
        return [], False

    hits: List[Dict[str, Any]] = []
    has_missing = False
    for metric_path, condition in standard_conditions.items():
        value = get_metric_value(observations, metric_path)
        if value is None:
            has_missing = True
            continue
        if evaluate_condition(value, condition):
            hits.append({
                "rule_id": rule_id,
                "metric": metric_path,
                "value": value,
                "condition": condition,
            })

    if match_mode == "all":
        if len(hits) == len(standard_conditions):
            return hits, False
        return [], has_missing
    else:
        return hits, has_missing


# v0002: decide_for_row with allow_override

def decide_for_row(
    row: Dict[str, Any],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    observations = row.get("observations", {})
    thresholds = policy.get("thresholds", {})
    rules = policy.get("rules", [])
    defaults = policy.get("defaults", {})

    all_triggered: List[Dict[str, Any]] = []
    final_decision = defaults.get("decision", "ALLOW")
    allow_override_active = False
    any_metric_missing = False

    for rule in rules:
        decision = rule.get("decision")
        if decision not in ALLOWED_DECISIONS:
            continue

        hits, has_missing = evaluate_rule(rule, observations, thresholds)
        if has_missing:
            any_metric_missing = True
        if not hits:
            continue

        all_triggered.extend(hits)

        if rule.get("allow_override", False) and decision == "ALLOW":
            allow_override_active = True
            continue

        if DECISION_PRIORITY[decision] > DECISION_PRIORITY[final_decision]:
            final_decision = decision

    # allow_override: may override DELAY, must NOT override FREEZE
    if allow_override_active and final_decision == "DELAY":
        final_decision = "ALLOW"

    # on_missing_metric: if any rule had missing observations and
    # current decision has not escalated above the missing_metric fallback,
    # escalate to on_missing_metric (default DELAY).
    # Does not override FREEZE. Does not override allow_override result
    # unless on_missing_metric priority is higher.
    if any_metric_missing:
        missing_action = defaults.get("on_missing_metric", "DELAY")
        if DECISION_PRIORITY.get(missing_action, 0) > DECISION_PRIORITY.get(final_decision, 0):
            final_decision = missing_action

    if final_decision not in ALLOWED_DECISIONS:
        final_decision = defaults.get("on_missing_metric", "DELAY")

    return {
        "asset_id": row.get("asset_id"),
        "path": row.get("path"),
        "decision": final_decision,
        "policy_version": policy.get("policy", {}).get("version"),
        "triggered_rules": all_triggered,
    }


# CLI / main

def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decide parse eligibility (v0003)."
    )
    parser.add_argument("--observations", required=True)
    parser.add_argument("--policy", default="config/action/config/parse_eligibility_policy_v0002.yml")
    parser.add_argument("--output", default="actioning/pipelines/decide_parse_eligibility_action/eligibility_decisions.jsonl")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    observations_path = Path(args.observations)
    policy_path = Path(args.policy)
    output_path = Path(args.output)

    if not observations_path.exists():
        print(f"[ERROR] Observations file not found: {observations_path}", file=sys.stderr)
        return 1
    if not policy_path.exists():
        print(f"[ERROR] Policy file not found: {policy_path}", file=sys.stderr)
        return 1

    observations = load_jsonl(observations_path)
    policy = load_policy(policy_path)

    results: List[Dict[str, Any]] = []
    for row in observations:
        result = decide_for_row(row, policy)
        results.append(result)

    if args.dry_run:
        print(json.dumps({"status": "dry-run", "rows_evaluated": len(results)}, ensure_ascii=False, indent=2))
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=DEFAULT_ENCODING) as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False))
            f.write("\n")

    print(json.dumps(
        {"status": "ok", "rows_written": len(results), "output": str(output_path), "version": "v0003"},
        ensure_ascii=False, indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
