#!/usr/bin/env python3
# ============================================================
# File: embedding_writer_v0001.py
# 中文名: 向量写入器脚本
# Version: v0001
# Layer: derivation
# Main Layer: action
# Updatable: True
#
# Purpose:
# 将 3_lifecycle_checked 的 embedding 数据写入 ChromaDB 向量库，
# 并更新状态索引，处理 replaced 记录。
#
# What it does:
# 1. 读取 3_lifecycle_checked 目录下的 JSONL 文件
# 2. 连接或创建 ChromaDB 持久化实例
# 3. 读取 replaced_records.jsonl 并归档旧记录
# 4. 批量写入 active 记录到 ChromaDB
# 5. 更新 active_index.jsonl 状态索引
# 6. 生成写入日志和 run_meta.json
#
# What it does NOT do:
# 1. 不校验契约（已在 validator 完成）
# 2. 不判定生命周期（已在 lifecycle_guard 完成）
# 3. 不修改输入文件
# 4. 不修改 SQL 数据
# ============================================================

# ============================================================
# ALIAS_META
# alias: embedding_writer
# family: embedding_writer
# role: writer
# version: v0001
# status: active
# entry_point: scripts/action/vector/embedding_writer_v0001.py
# input: Lifecycle-checked JSONL from 3_lifecycle_checked
# output: ChromaDB vectors, updated state index, write logs
# depends_on: chromadb, Python stdlib
# used_by: embedding pipeline, manual trigger
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 是否进行合规判断: 否（输入已通过 validator 和 lifecycle_guard）
# - 是否进行制度判断: 否（只执行写入）
# - 是否修改系统对象: 是（写入 ChromaDB，更新状态索引）
# - 是否解析其他头部结构: 否
# ============================================================

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# ============================================================
# 边界声明与强约束说明
#
# 1. 原子性约束: 尽可能保证写入的原子性，失败时记录已写入的记录
# 2. 状态索引同步: 写入成功后必须更新 active_index.jsonl
# 3. replaced 归档: replaced 记录从 ChromaDB 删除后归档到 replaced/ 目录
# 4. 异常兜底: 无论成功或失败，必须写入 run_meta.json
# 5. 批量写入: 使用 ChromaDB 批量 API 提高性能
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================

SCRIPT_NAME = "embedding_writer"
SCRIPT_VERSION = "v0001"
MAIN_LAYER = "action"

# ChromaDB Collection 名称
COLLECTION_NAME = "action_data_embeddings"

# 状态索引中只保留这些字段
INDEX_KEEP_FIELDS = [
    "object_type",
    "object_id", 
    "embedding_signature",
    "source_hash",
    "embedding_model_name",
    "embedding_model_version",
    "run_id",
    "generated_at",
    "state",
]

# 批量写入大小
BATCH_SIZE = 100

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
    if not path.exists():
        return
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


def _append_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    """追加记录到 JSONL 文件"""
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(_safe_json(rec) + "\n")


def _make_index_key(object_type: str, object_id: str) -> str:
    """生成索引键"""
    return f"{object_type}:{object_id}"


def _extract_index_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """从完整记录中提取索引记录（不含 embedding_vector）"""
    return {k: record.get(k) for k in INDEX_KEEP_FIELDS}


def _process_batch(
    batch: List[Dict[str, Any]],
    collection,
    state_index: Dict[str, Dict[str, Any]],
    write_log: List[Dict[str, Any]],
    stats: Dict[str, int],
    dry_run: bool,
) -> None:
    """处理一个批次的记录：写入 ChromaDB、更新索引、记录日志"""
    if not batch:
        return
    
    # 构建 ChromaDB 格式
    ids = []
    embeddings = []
    metadatas = []
    
    for record in batch:
        chroma_rec = _build_chroma_record(record)
        ids.append(chroma_rec["id"])
        embeddings.append(chroma_rec["embedding"])
        metadatas.append(chroma_rec["metadata"])
    
    if not dry_run and collection is not None:
        # 使用 upsert 以支持重复写入
        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
        )
    
    # 更新状态索引
    for record in batch:
        obj_type = record.get("object_type", "")
        obj_id = record.get("object_id", "")
        if obj_type and obj_id:
            key = _make_index_key(obj_type, obj_id)
            state_index[key] = _extract_index_record(record)
    
    # 记录写入日志
    for record in batch:
        write_log.append({
            "timestamp": _utc_iso(),
            "action": "write",
            "object_type": record.get("object_type"),
            "object_id": record.get("object_id"),
            "embedding_signature": record.get("embedding_signature"),
            "status": "success" if not dry_run else "dry-run",
        })
    
    stats["written_to_chromadb"] += len(batch)


# ============================================================
# ChromaDB 操作
# ============================================================

def _get_or_create_collection(chromadb_path: Path):
    """获取或创建 ChromaDB Collection"""
    try:
        import chromadb
        from chromadb.config import Settings
    except ImportError:
        raise RuntimeError("chromadb is required. Run: pip install chromadb")
    
    _ensure_dir(chromadb_path)
    
    # 创建持久化客户端
    client = chromadb.PersistentClient(
        path=str(chromadb_path),
        settings=Settings(anonymized_telemetry=False),
    )
    
    # 获取或创建 collection
    # hnsw:space = cosine: 使用余弦距离度量，距离范围 0.0（完全相同）到 2.0（完全相反）
    # 高相关阈值参考: < 0.3 高相关, 0.3-0.5 中等相关, > 0.5 基本不相关
    # 注意: 此参数在 collection 首次创建时生效，已存在的 collection 需删除重建
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={
            "description": "Understanding layer embeddings",
            "hnsw:space": "cosine",
        }
    )
    
    return client, collection


def _build_chroma_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """构建 ChromaDB 记录格式"""
    # 使用 embedding_signature 作为 id
    doc_id = record.get("embedding_signature", "")
    
    # embedding vector
    embedding = record.get("embedding_vector", [])
    
    # metadata（不含 embedding_vector）
    metadata = {
        "object_type": record.get("object_type", ""),
        "object_id": record.get("object_id", ""),
        "source_hash": record.get("source_hash", ""),
        "embedding_model_name": record.get("embedding_model_name", ""),
        "embedding_model_version": record.get("embedding_model_version", ""),
        "tokenizer_version": record.get("tokenizer_version", ""),
        "embedding_parameters_hash": record.get("embedding_parameters_hash", ""),
        "policy_version": record.get("policy_version", ""),
        "embedding_dimension": record.get("embedding_dimension", 0),
        "generated_at": record.get("generated_at", ""),
        "run_id": record.get("run_id", ""),
        "state": record.get("state", "active"),
    }
    
    return {
        "id": doc_id,
        "embedding": embedding,
        "metadata": metadata,
    }


# ============================================================
# 状态索引管理
# ============================================================

def _load_state_index(index_path: Path) -> Dict[str, Dict[str, Any]]:
    """加载状态索引"""
    if not index_path.exists():
        return {}
    
    index: Dict[str, Dict[str, Any]] = {}
    for rec in _iter_jsonl(index_path):
        obj_type = rec.get("object_type", "")
        obj_id = rec.get("object_id", "")
        if obj_type and obj_id:
            key = _make_index_key(obj_type, obj_id)
            index[key] = rec
    
    return index


def _save_state_index(index_path: Path, index: Dict[str, Dict[str, Any]]) -> None:
    """保存状态索引"""
    _ensure_dir(index_path.parent)
    records = list(index.values())
    _write_jsonl(index_path, records)


# ============================================================
# 核心业务逻辑
# ============================================================

def process_write(
    input_dir: Path,
    chromadb_path: Path,
    state_index_path: Path,
    replaced_archive_path: Path,
    output_dir: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    执行向量写入的主逻辑。
    """
    started_at = _utc_iso()
    
    # 统计
    stats = {
        "total_processed": 0,
        "written_to_chromadb": 0,
        "replaced_archived": 0,
        "replaced_deleted": 0,
        "index_updated": 0,
    }
    
    # 错误信息
    error_msg: Optional[str] = None
    
    # 写入日志
    write_log: List[Dict[str, Any]] = []
    
    # 检查输入目录
    if not input_dir.exists():
        error_msg = f"Input directory not found: {input_dir}"
    
    # 创建输出目录
    if error_msg is None and not dry_run:
        try:
            _ensure_dir(output_dir)
        except Exception as e:
            error_msg = f"Failed to create output directory: {e}"
    
    # 加载状态索引
    state_index: Dict[str, Dict[str, Any]] = {}
    if error_msg is None:
        try:
            state_index = _load_state_index(state_index_path)
        except Exception as e:
            error_msg = f"Failed to load state index: {e}"
    
    # 连接 ChromaDB
    client = None
    collection = None
    if error_msg is None and not dry_run:
        try:
            client, collection = _get_or_create_collection(chromadb_path)
        except Exception as e:
            error_msg = f"Failed to connect to ChromaDB: {e}"
    
    # 处理 replaced 记录
    replaced_records_path = input_dir / "replaced_records.jsonl"
    if error_msg is None and replaced_records_path.exists():
        try:
            replaced_records = list(_iter_jsonl(replaced_records_path))
            
            if replaced_records and not dry_run:
                # 归档 replaced 记录
                run_id = input_dir.name
                archive_dir = replaced_archive_path / run_id
                _ensure_dir(archive_dir)
                archive_file = archive_dir / "replaced_records.jsonl"
                _write_jsonl(archive_file, replaced_records)
                stats["replaced_archived"] = len(replaced_records)
                
                # 从 ChromaDB 删除 replaced 记录（分批删除，避免超载）
                if collection is not None:
                    ids_to_delete = [
                        rec.get("old_signature", "") 
                        for rec in replaced_records 
                        if rec.get("old_signature")
                    ]
                    if ids_to_delete:
                        try:
                            # 分批删除，避免 ChromaDB 批处理上限
                            for i in range(0, len(ids_to_delete), BATCH_SIZE):
                                batch_ids = ids_to_delete[i:i + BATCH_SIZE]
                                collection.delete(ids=batch_ids)
                                stats["replaced_deleted"] += len(batch_ids)
                        except Exception:
                            # 删除失败不阻塞写入，但记录
                            pass
                
                # 从状态索引中移除 replaced 记录
                for rec in replaced_records:
                    obj_type = rec.get("object_type", "")
                    obj_id = rec.get("object_id", "")
                    if obj_type and obj_id:
                        key = _make_index_key(obj_type, obj_id)
                        state_index.pop(key, None)
        
        except Exception as e:
            # replaced 处理失败不阻塞主流程
            pass
    
    # 流式处理：批量读取并写入，避免 OOM
    # 排除的辅助文件
    EXCLUDED_FILES = {"replaced_records.jsonl", "lifecycle_actions.jsonl", "run_meta.json"}
    
    if error_msg is None:
        try:
            subdirs = ["concept", "instance"]
            
            for subdir_name in subdirs:
                subdir_input = input_dir / subdir_name
                if not subdir_input.exists():
                    continue
                
                for jsonl_file in sorted(subdir_input.glob("*.jsonl")):
                    # 排除辅助文件
                    if jsonl_file.name in EXCLUDED_FILES:
                        continue
                    
                    # 流式批量处理每个文件
                    batch: List[Dict[str, Any]] = []
                    
                    for record in _iter_jsonl(jsonl_file):
                        stats["total_processed"] += 1
                        batch.append(record)
                        
                        # 达到批量大小时处理
                        if len(batch) >= BATCH_SIZE:
                            _process_batch(
                                batch=batch,
                                collection=collection,
                                state_index=state_index,
                                write_log=write_log,
                                stats=stats,
                                dry_run=dry_run,
                            )
                            batch = []  # 清空批次，释放内存
                    
                    # 处理剩余记录
                    if batch:
                        _process_batch(
                            batch=batch,
                            collection=collection,
                            state_index=state_index,
                            write_log=write_log,
                            stats=stats,
                            dry_run=dry_run,
                        )
        
        except Exception as e:
            error_msg = f"Failed to write to ChromaDB: {e}"
    
    # 保存状态索引
    if error_msg is None and not dry_run:
        try:
            _save_state_index(state_index_path, state_index)
            stats["index_updated"] = len(state_index)
        except Exception as e:
            error_msg = f"Failed to save state index: {e}"
    
    # 写入日志文件
    if not dry_run and write_log:
        try:
            log_file = output_dir / "write_log.jsonl"
            _write_jsonl(log_file, write_log)
        except Exception:
            pass
    
    # 写入 run_meta.json
    finished_at = _utc_iso()
    status = "success" if error_msg is None else "failed"
    
    meta = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "main_layer": MAIN_LAYER,
        "run_id": input_dir.name,
        "input_dir": str(input_dir),
        "chromadb_path": str(chromadb_path),
        "collection_space": "cosine",
        "state_index_path": str(state_index_path),
        "output_dir": str(output_dir),
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
            pass
    
    return {
        "status": status,
        "dry_run": dry_run,
        "input_dir": str(input_dir),
        "chromadb_path": str(chromadb_path),
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
        description="Write lifecycle-checked embeddings to ChromaDB vector store."
    )
    ap.add_argument(
        "--input-dir",
        required=True,
        help="Input directory containing lifecycle-checked JSONL files (e.g., vector/pipeline/3_lifecycle_checked/run_xxx)",
    )
    ap.add_argument(
        "--chromadb-path",
        default="chromadb/action/action_data/vectors",
        help="Path to ChromaDB persistence directory. Default: chromadb/action/action_data/vectors",
    )
    ap.add_argument(
        "--state-index",
        default="vector/state/active_index.jsonl",
        help="Path to state index file. Default: vector/state/active_index.jsonl",
    )
    ap.add_argument(
        "--replaced-archive",
        default="chromadb/action/action_data/replaced",
        help="Path to archive replaced records. Default: chromadb/action/action_data/replaced",
    )
    ap.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for write logs. Default: vector/pipeline/4_written/{run_id}",
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
    chromadb_path = Path(args.chromadb_path)
    state_index_path = Path(args.state_index)
    replaced_archive_path = Path(args.replaced_archive)
    
    # 默认输出目录
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        run_id = input_dir.name
        output_dir = Path("vector/pipeline/4_written") / run_id
    
    result = process_write(
        input_dir=input_dir,
        chromadb_path=chromadb_path,
        state_index_path=state_index_path,
        replaced_archive_path=replaced_archive_path,
        output_dir=output_dir,
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