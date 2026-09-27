#!/usr/bin/env python3
# ============================================================
# File: embedding_lifecycle_guard_v0001.py
# 中文名: 向量生命周期守卫脚本
# Version: v0001
# Layer: derivation
# Main Layer: action
# Updatable: True
#
# Purpose:
# 在 embedding 写入向量库之前，检查是否存在旧版本，决定新旧版本的状态标记。
#
# What it does:
# 1. 读取 2_validated 目录下已校验的 embedding JSONL 文件
# 2. 读取状态索引文件获取当前 active 的 embedding 记录
# 3. 对比新记录与历史记录，判定状态（active/replaced/duplicate）
# 4. 输出带状态标记的 JSONL 到 3_lifecycle_checked 目录
# 5. 输出 lifecycle_actions.jsonl 记录本次状态变更
# 6. 生成 run_meta.json 保证批次可追溯（包括异常情况）
#
# What it does NOT do:
# 1. 不写入向量库
# 2. 不删除向量库记录
# 3. 不修改输入文件
# 4. 不修改 SQL 数据
# 5. 不更新状态索引（由 writer 负责）
# ============================================================

# ============================================================
# ALIAS_META
# alias: embedding_lifecycle_guard
# family: embedding_lifecycle
# role: lifecycle_guard
# version: v0001
# status: active
# entry_point: scripts/action/vector/embedding_lifecycle_guard_v0001.py
# input: Validated JSONL from 2_validated, state index file
# output: Lifecycle-checked JSONL to 3_lifecycle_checked, actions log
# depends_on: Python stdlib only (json, argparse, pathlib, datetime, typing, hashlib)
# used_by: embedding pipeline, manual trigger
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 是否进行合规判断: 否（输入已通过 contract_validator 校验）
# - 是否进行制度判断: 是（判定 active/replaced/duplicate 状态）
# - 是否修改系统对象: 否（只读输入，只写新输出）
# - 是否解析其他头部结构: 否
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# ============================================================
# 边界声明与强约束说明
#
# 1. 内存边界约束: 加载状态索引时只保留必要字段（embedding_signature），
#    绝对禁止将完整 embedding_vector 载入内存，防止 OOM。
# 2. 输出边界约束: CLI 标准输出仅允许打印最终的结构化 Summary。
# 3. 异常兜底约束: 无论成功或失败，必须写入 run_meta.json 保证审计链完整。
# 4. 幂等性约束: 相同输入多次运行产生相同输出。
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================

SCRIPT_NAME = "embedding_lifecycle_guard"
SCRIPT_VERSION = "v0001"
MAIN_LAYER = "action"

# 状态常量
STATE_ACTIVE = "active"
STATE_REPLACED = "replaced"
STATE_DUPLICATE = "duplicate"

# 动作常量
ACTION_MARK_ACTIVE = "mark_active"
ACTION_MARK_REPLACED = "mark_replaced"
ACTION_SKIP_DUPLICATE = "skip_duplicate"

# 状态索引中只保留这些字段（防止 OOM）
INDEX_KEEP_FIELDS = ["object_type", "object_id", "embedding_signature", "source_hash"]

# ============================================================
# 工具函数区（无副作用）
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _pretty_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    """流式读取 JSONL 文件"""
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    yield obj
            except Exception as e:
                raise ValueError(f"Invalid JSONL at {path} line {ln}: {e}")


def _write_jsonl(path: Path, records: List[Dict[str, Any]]) -> int:
    """写入 JSONL 文件"""
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(_safe_json(rec) + "\n")
    return len(records)


def _append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    """追加单条记录到 JSONL 文件"""
    with path.open("a", encoding="utf-8") as f:
        f.write(_safe_json(record) + "\n")


def _make_index_key(object_type: str, object_id: str) -> str:
    """生成索引键：object_type:object_id"""
    return f"{object_type}:{object_id}"


# ============================================================
# 状态索引管理
# ============================================================

def load_state_index(index_path: Path) -> Dict[str, Dict[str, Any]]:
    """
    加载状态索引文件。
    
    索引结构（只保留必要字段，防止 OOM）：
    {
        "concept:abc123": {
            "object_type": "concept",
            "object_id": "abc123",
            "embedding_signature": "xxx",
            "source_hash": "yyy"
        },
        ...
    }
    
    如果文件不存在，返回空字典（冷启动场景）。
    
    注意：绝对不能将 embedding_vector 加载到内存！
    """
    if not index_path.exists():
        return {}
    
    index: Dict[str, Dict[str, Any]] = {}
    
    for rec in _iter_jsonl(index_path):
        obj_type = rec.get("object_type", "")
        obj_id = rec.get("object_id", "")
        state = rec.get("state", "")
        
        # 只索引 active 状态的记录
        if state == STATE_ACTIVE and obj_type and obj_id:
            key = _make_index_key(obj_type, obj_id)
            # 只保留必要字段，防止 OOM
            index[key] = {k: rec.get(k) for k in INDEX_KEEP_FIELDS}
    
    return index


# ============================================================
# 生命周期判定逻辑
# ============================================================

def determine_lifecycle_state(
    record: Dict[str, Any],
    active_index: Dict[str, Dict[str, Any]],
) -> Tuple[str, str, Optional[Dict[str, Any]]]:
    """
    判定单条记录的生命周期状态。
    
    返回：(new_state, reason, old_record_or_none)
    
    判定逻辑：
    1. 无历史记录 → active, "new_record", None
    2. 有历史记录，signature 相同 → duplicate, "signature_identical", old_record
    3. 有历史记录，signature 不同 → active, "source_hash_changed", old_record
    """
    obj_type = record.get("object_type", "")
    obj_id = record.get("object_id", "")
    new_signature = record.get("embedding_signature", "")
    
    key = _make_index_key(obj_type, obj_id)
    
    # 情况 1：无历史记录
    if key not in active_index:
        return STATE_ACTIVE, "new_record", None
    
    old_record = active_index[key]
    old_signature = old_record.get("embedding_signature", "")
    
    # 情况 2：signature 完全相同（重复）
    if new_signature == old_signature:
        return STATE_DUPLICATE, "signature_identical", old_record
    
    # 情况 3：signature 不同（需要替换）
    return STATE_ACTIVE, "source_hash_changed", old_record


# ============================================================
# 核心业务逻辑
# ============================================================

def process_lifecycle(
    input_dir: Path,
    output_dir: Path,
    state_index_path: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    处理生命周期检查的主逻辑。
    """
    started_at = _utc_iso()
    
    # 统计
    stats = {
        "total_processed": 0,
        "marked_active": 0,
        "marked_duplicate": 0,
        "need_replace": 0,
    }
    
    # 需要标记为 replaced 的旧记录
    records_to_replace: List[Dict[str, Any]] = []
    
    # 动作日志
    actions: List[Dict[str, Any]] = []
    
    # 错误信息
    error_msg: Optional[str] = None
    
    # 加载状态索引
    active_index: Dict[str, Dict[str, Any]] = {}
    index_loaded = 0
    
    try:
        active_index = load_state_index(state_index_path)
        index_loaded = len(active_index)
    except Exception as e:
        error_msg = f"Failed to load state index: {e}"
    
    # 检查输入目录
    if error_msg is None and not input_dir.exists():
        error_msg = f"Input directory not found: {input_dir}"
    
    # 创建输出目录结构
    if error_msg is None and not dry_run:
        try:
            _ensure_dir(output_dir)
        except Exception as e:
            error_msg = f"Failed to create output directory: {e}"
    
    # 主处理逻辑
    if error_msg is None:
        try:
            # 遍历输入目录中的子目录和文件
            subdirs = ["concept", "instance"]
            
            for subdir_name in subdirs:
                subdir_input = input_dir / subdir_name
                if not subdir_input.exists():
                    continue
                
                subdir_output = output_dir / subdir_name
                if not dry_run:
                    _ensure_dir(subdir_output)
                
                # 处理该子目录下的所有 JSONL 文件
                for jsonl_file in sorted(subdir_input.glob("*.jsonl")):
                    output_records: List[Dict[str, Any]] = []
                    
                    for record in _iter_jsonl(jsonl_file):
                        stats["total_processed"] += 1
                        
                        # 判定状态
                        new_state, reason, old_record = determine_lifecycle_state(
                            record, active_index
                        )
                        
                        # 构建动作记录
                        action_record = {
                            "timestamp": _utc_iso(),
                            "object_type": record.get("object_type"),
                            "object_id": record.get("object_id"),
                            "new_signature": record.get("embedding_signature"),
                            "reason": reason,
                        }
                        
                        if new_state == STATE_ACTIVE:
                            # 标记为 active
                            record["state"] = STATE_ACTIVE
                            output_records.append(record)
                            stats["marked_active"] += 1
                            
                            action_record["action"] = ACTION_MARK_ACTIVE
                            
                            # 如果有旧记录需要替换
                            if old_record is not None:
                                action_record["old_signature"] = old_record.get("embedding_signature")
                                stats["need_replace"] += 1
                                
                                # 记录需要标记为 replaced 的旧记录
                                old_replace_record = {
                                    "action": ACTION_MARK_REPLACED,
                                    "timestamp": _utc_iso(),
                                    "object_type": old_record.get("object_type"),
                                    "object_id": old_record.get("object_id"),
                                    "old_signature": old_record.get("embedding_signature"),
                                    "new_signature": record.get("embedding_signature"),
                                    "reason": reason,
                                }
                                records_to_replace.append(old_replace_record)
                        
                        elif new_state == STATE_DUPLICATE:
                            # 跳过重复记录，不输出到结果文件
                            stats["marked_duplicate"] += 1
                            
                            action_record["action"] = ACTION_SKIP_DUPLICATE
                            action_record["existing_signature"] = old_record.get("embedding_signature") if old_record else None
                        
                        actions.append(action_record)
                    
                    # 写入输出文件
                    if not dry_run and output_records:
                        output_file = subdir_output / jsonl_file.name
                        _write_jsonl(output_file, output_records)
            
            # 写入动作日志和需要替换的记录
            if not dry_run:
                # lifecycle_actions.jsonl - 所有动作
                actions_file = output_dir / "lifecycle_actions.jsonl"
                _write_jsonl(actions_file, actions)
                
                # replaced_records.jsonl - 需要在向量库中标记为 replaced 的旧记录
                if records_to_replace:
                    replace_file = output_dir / "replaced_records.jsonl"
                    _write_jsonl(replace_file, records_to_replace)
        
        except Exception as e:
            error_msg = f"Processing failed: {e}"
    
    # 写入 run_meta.json（无论成功或失败都要写）
    finished_at = _utc_iso()
    status = "success" if error_msg is None else "failed"
    
    meta = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "main_layer": MAIN_LAYER,
        "run_id": input_dir.name,
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "state_index_path": str(state_index_path),
        "index_records_loaded": index_loaded,
        "started_at": started_at,
        "finished_at": finished_at,
        "dry_run": dry_run,
        "status": status,
        "error": error_msg,
        "stats": stats,
    }
    
    if not dry_run:
        try:
            _ensure_dir(output_dir)
            meta_file = output_dir / "run_meta.json"
            meta_file.write_text(_pretty_json(meta), encoding="utf-8")
        except Exception:
            pass  # 尽力写入，但不因此抛出异常
    
    return {
        "status": status,
        "dry_run": dry_run,
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "index_records_loaded": index_loaded,
        "stats": stats,
        "started_at": started_at,
        "finished_at": finished_at,
        "error": error_msg,
    }


# ============================================================
# CLI / main 接口区
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Embedding lifecycle guard: determine active/replaced/duplicate states before writing to vector store."
    )
    ap.add_argument(
        "--input-dir",
        required=True,
        help="Input directory containing validated JSONL files (e.g., vector/pipeline/2_validated/run_xxx)",
    )
    ap.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for lifecycle-checked JSONL files (e.g., vector/pipeline/3_lifecycle_checked/run_xxx)",
    )
    ap.add_argument(
        "--state-index",
        default="vector/state/active_index.jsonl",
        help="Path to state index file. Default: vector/state/active_index.jsonl",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode. No files will be written.",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    state_index_path = Path(args.state_index)
    
    result = process_lifecycle(
        input_dir=input_dir,
        output_dir=output_dir,
        state_index_path=state_index_path,
        dry_run=args.dry_run,
    )
    
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
    if result.get("status") == "failed":
        return 1
    return 0


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())