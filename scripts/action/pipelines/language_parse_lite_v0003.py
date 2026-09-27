# filename: language_parse_lite_v0003.py
# 中文名: 轻量语言结构化解析脚本
# version: v0003
# layer: execution
# main_layer: action
# 可更新: True
#
# 职责说明
# 本脚本在第二步裁决结果的约束下，仅对 decision 为 ALLOW 的 (asset_id, path) 执行轻量结构化解析。
# 本脚本输出可回指的结构化文本单元，包含段句索引与字符坐标，并输出低阶统计特征。
#
# v0002 变更说明 (Holistic Alignment & Critical Bugfix)
# - 修复了 char_start/char_end 的坐标漂移问题，使用严格的段落绝对偏移与局部相对游标进行锚点定位。
# - 移除了脆性的正则 YAML 解析器 (LitePolicyLoader)，与管线其他脚本对齐，统一使用标准的 pyyaml 库。

# ============================================================
# ALIAS_META
# ============================================================
# alias: language_parse_lite_v0003
# family: language_parse_lite
# role: structural_parser_lite
# version: v0003
# status: active
# entry_point: scripts/action/pipelines/language_parse_lite_v0003.py
# input:
#   - parse_eligibility_decisions_jsonl
#   - language_parse_lite_policy_yaml
#   - json_assets (referenced by asset_id)
# output:
#   - text_units_jsonl
# depends_on:
#   - pyyaml  <--- 依赖已更新
# used_by:
#   - understanding_analysis_chain
# notes:
#   - Pure structural parsing. No NLP model used.

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List

import yaml  # <--- 引入管线标准依赖

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def calculate_sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    data = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return data

def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            count += 1
    return count

def _iter_json_files(root: Path) -> Generator[Path, None, None]:
    if not root.exists():
        return
    if root.is_file():
        if root.suffix.lower() == ".json":
            yield root
        return
    for p in root.rglob("*.json"):
        if p.is_file():
            yield p

def _append_run_log(log_dir: Path, record: Dict[str, Any]) -> None:
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "language_parse_lite_runs.jsonl"
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

# ============================================================
# 策略对象模型 (对齐 standard dict)
# ============================================================

class ParsePolicy:
    def __init__(self, policy_dict: Dict[str, Any]):
        p = policy_dict.get("policy", {})
        self.policy_version = p.get("version", "v0002")
        
        ic = policy_dict.get("input_constraints", {})
        self.input_accepted_decision = ic.get("accepted_decision", "ALLOW")
        
        seg = policy_dict.get("segmentation", {})
        para = seg.get("paragraph", {})
        self.seg_para_enabled = para.get("enabled", True)
        self.seg_para_split_on_newline = para.get("split_on_newline", True)
        self.seg_para_keep_empty = para.get("keep_empty", False)
        
        sent = seg.get("sentence", {})
        self.seg_sent_enabled = sent.get("enabled", True)
        self.seg_sent_punct = sent.get("split_on_punctuation", ".?!。！？")
        
        stats = policy_dict.get("statistics", {})
        self.stats_punct = stats.get("punct_definition", {}).get("characters", ".,;:!?。！？")
        
        self.output_unit_type = policy_dict.get("output", {}).get("unit_type", "text_unit")

# ============================================================
# 核心业务逻辑区
# ============================================================

@dataclass
class Target:
    asset_id: str
    path: str

def iter_allowed_targets(decisions_path: Path, policy: ParsePolicy) -> Generator[Target, None, None]:
    for row in load_jsonl(decisions_path):
        if row.get("decision") == policy.input_accepted_decision:
            yield Target(
                asset_id=row["asset_id"],
                path=row["path"]
            )

def extract_value_from_asset(asset: Dict[str, Any], path: str) -> List[str]:
    segments = [s for s in path.split("/") if s]
    current_level = [asset]
    
    for seg in segments:
        next_level = []
        for node in current_level:
            if isinstance(node, dict):
                if seg in node:
                    val = node[seg]
                    if isinstance(val, list):
                        next_level.append(val)
                    else:
                        next_level.append(val)
                elif seg == "*":
                    next_level.extend(node.values())
            elif isinstance(node, list):
                if seg == "*":
                    next_level.extend(node)
                else:
                    try:
                        idx = int(seg)
                        if 0 <= idx < len(node):
                            next_level.append(node[idx])
                    except ValueError:
                        pass
        current_level = next_level
    
    results = []
    def collect_strings(obj):
        if isinstance(obj, str):
            results.append(obj)
        elif isinstance(obj, list):
            for item in obj:
                collect_strings(item)
        
    for item in current_level:
        collect_strings(item)
        
    return results

def split_into_sentences(text: str, punct_chars: str) -> List[str]:
    escaped_punct = re.escape(punct_chars)
    pattern = f"([{escaped_punct}]+)"
    parts = re.split(pattern, text)
    sentences = []
    current = ""
    for part in parts:
        if re.match(pattern, part):
            current += part
            sentences.append(current)
            current = ""
        else:
            current += part
    if current:
        sentences.append(current)
    return [s.strip() for s in sentences if s.strip()]

def compute_lite_stats(text: str, policy: ParsePolicy) -> Dict[str, Any]:
    return {
        "char_len": len(text),
        "punct_count": sum(1 for c in text if c in policy.stats_punct)
    }

def generate_text_units_for_target(
    asset_id: str, 
    path: str, 
    asset_obj: Dict[str, Any], 
    policy: ParsePolicy
) -> Generator[Dict[str, Any], None, None]:
    
    values = extract_value_from_asset(asset_obj, path)

    for val_idx, val_str in enumerate(values):
        val_sha1 = calculate_sha1(val_str)
        
        # 精准坐标修复算法
        paragraphs_with_offset = []
        if policy.seg_para_enabled and policy.seg_para_split_on_newline:
            cursor = 0
            for p in val_str.split("\n"):
                paragraphs_with_offset.append((cursor, p))
                cursor += len(p) + 1
        else:
            paragraphs_with_offset = [(0, val_str)]
            
        seg_idx_counter = 0
        
        for para_offset, para in paragraphs_with_offset:
            if not para.strip() and not policy.seg_para_keep_empty:
                continue
                
            if policy.seg_sent_enabled:
                sentences = split_into_sentences(para, policy.seg_sent_punct)
            else:
                sentences = [para]
                
            sent_idx_counter = 0
            para_cursor = 0
            
            for sent in sentences:
                sent_len = len(sent)
                local_start = para.find(sent, para_cursor)
                if local_start == -1:
                    local_start = para_cursor 
                
                local_end = local_start + sent_len
                abs_start = para_offset + local_start
                abs_end = para_offset + local_end
                
                yield {
                    "asset_id": asset_id,
                    "path": path,
                    "value_index": val_idx,
                    "source_value_sha1": val_sha1,
                    "segment_index": seg_idx_counter,
                    "sentence_index": sent_idx_counter,
                    "char_start": abs_start,
                    "char_end": abs_end,
                    "text": sent,
                    "stats": compute_lite_stats(sent, policy),
                    "parse_version": policy.policy_version,
                    "unit_type": policy.output_unit_type
                }
                
                sent_idx_counter += 1
                para_cursor = local_end
            
            seg_idx_counter += 1

def build_asset_index(data_input: Path) -> Dict[str, Path]:
    files = _iter_json_files(data_input)
    idx: Dict[str, Path] = {}
    for fp in files:
        idx[fp.stem] = fp
    return idx

# ============================================================
# CLI / main 接口区
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Lightweight language structural parser (v0003).")
    parser.add_argument("--decisions", required=True)
    parser.add_argument("--policy", default="config/action/config/language_parse_lite_policy_v0002.yml")
    parser.add_argument("--data-input", required=True)
    parser.add_argument("--output-dir", default="actioning/pipelines/language_parse_lite_action")
    parser.add_argument("--log-dir", default="_logs")
    args = parser.parse_args()
    
    decisions_path = Path(args.decisions)
    policy_path = Path(args.policy)
    data_input = Path(args.data_input)
    output_dir = Path(args.output_dir)
    log_dir = Path(args.log_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with policy_path.open("r", encoding="utf-8") as f:
            raw_policy_dict = yaml.safe_load(f)
        policy = ParsePolicy(raw_policy_dict)
    except Exception as e:
        print(f"[ERROR] Failed to load policy: {e}", file=sys.stderr)
        return 1

    asset_index = build_asset_index(data_input)
    
    input_stem = decisions_path.stem
    base_name = input_stem[:-10] if input_stem.endswith("_decisions") else input_stem
    out_name = f"{base_name}_text_units.jsonl"
    out_path = output_dir / out_name
    
    run_record = {
        "script": "language_parse_lite_v0003",
        "timestamp": _now_utc_iso(),
        "decisions_input": str(decisions_path),
        "output_file": str(out_path),
        "status": "running"
    }
    
    def rows_iter():
        targets = list(iter_allowed_targets(decisions_path, policy))
        from itertools import groupby
        targets.sort(key=lambda x: x.asset_id)
        
        for asset_id, group in groupby(targets, key=lambda x: x.asset_id):
            group_list = list(group)
            fp = asset_index.get(asset_id)
            if not fp:
                yield {"asset_id": asset_id, "path": group_list[0].path, "unit_type": "error", "reason": "asset_file_not_found"}
                continue
                
            try:
                with fp.open("r", encoding="utf-8") as f:
                    asset_obj = json.load(f)
            except Exception as e:
                 yield {"asset_id": asset_id, "path": group_list[0].path, "unit_type": "error", "reason": f"asset_load_failed:{e}"}
                 continue

            for t in group_list:
                yield from generate_text_units_for_target(asset_id, t.path, asset_obj, policy)

    try:
        wrote = write_jsonl(out_path, rows_iter())
    except Exception as e:
        _append_run_log(log_dir, {**run_record, "status": "error", "error": str(e)})
        print(f"[ERROR] failed to write output: {e}", file=sys.stderr)
        return 1

    _append_run_log(log_dir, {**run_record, "status": "ok", "rows_written": int(wrote)})

    print(json.dumps({
        "status": "ok",
        "version": "v0003",
        "policy_version": policy.policy_version,
        "output": str(out_path),
        "rows_written": wrote
    }, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())