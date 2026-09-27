#!/usr/bin/env python3
# ============================================================
# File: embedding_generator_v0004.py
# 中文名: 向量生成器脚本
# Version: v0004
# Layer: infrastructure
# Main Layer: action
# Updatable: True
#
# Purpose:
# 负责从 SQL 提取文本真源，调用大模型生成向量，并输出符合契约的 JSONL 文件。
#
# What it does:
# 1. 读取 sql_writer_config 获取数据库路径与字段映射 (严格防注入)。
# 2. 读取 embedding_generator_config 获取 API 与模型参数。
# 3. 从 instance 或 concept 表中分批抽取待向量化的文本。
# 4. 调用大语言模型 API (HTTP POST) 或本地模型或 Mock API 生成 Embedding 向量。
# 5. 按 asset_id 或全局隔离输出写入可配置输出根目录下的 run_xxx 目录。
# 6. 生成目录级的 run_meta.json，保证批次可追溯 (无论成功或异常)。
# 7. 严格按照契约组装 14 个 REQUIRED_FIELDS 并计算 embedding_signature。
# 8. 强校验 API 返回的向量维度，防止非法张量入库。
#
# What it does NOT do:
# 1. 不写入任何向量数据库 (如 Chroma/FAISS)。
# 2. 不回写 SQL 数据库，不修改任何源数据状态。
# 3. 不判断旧向量的生命周期 (交由 lifecycle_guard 负责)。
# ============================================================

# ============================================================
# ALIAS_META
# alias: embedding_generator
# family: embedding_generator
# role: generator
# version: v0004
# status: active
# entry_point: scripts/action/vector/embedding_generator_v0004.py
# input: SQL text records (via sqlite3), YAML Configs, optional output base directory
# output: Standardized JSONL embedding records and run_meta.json
# depends_on: sqlite3, json, argparse, pathlib, datetime, typing, urllib, hashlib, random, re, sentence_transformers (optional)
# used_by: execution_flow, manual_trigger
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 是否进行合规判断: 否 (仅生产数据，校验交由 contract_validator 负责)
# - 是否进行制度判断: 否 (不执行业务分发与状态裁决)
# - 是否修改系统对象: 否 (绝对只读 SQL 真源，仅生成本地独立文件)
# - 是否解析其他头部结构: 否 (仅读取 YAML 结构化数据)
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ============================================================
# 边界声明与强约束说明
#
# 1. 输出隔离约束: 必须按照 run_id 建立独立文件夹 (1_generated/run_XXX/)，并附带 run_meta.json。
# 2. 实例隔离约束: Instance 类型的向量，必须按 asset_id 拆分生成独立文件。
# 3. API 解耦约束: 绝对禁止硬编码模型逻辑，必须依赖 HTTP 标准或 Mock，通过 config 解耦。
# 4. 内存边界约束: 查询与推理必须分页批处理 (Batching)，严禁将全表数据载入内存。
# 5. 终端克制约束: 绝对禁止打印过程日志，仅允许在退出前打印一次结构化 JSON Summary。
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================

SCRIPT_NAME = "embedding_generator"
SCRIPT_VERSION = "v0004"
MAIN_LAYER = "action"

MAX_ROOT_SEARCH_DEPTH = 10

DEFAULT_SQL_CONFIG_PATH = Path("config") / "action" / "config" / "sql_writer_config_v0001.yml"
DEFAULT_EMBED_CONFIG_PATH = Path("config") / "action" / "config" / "embedding_generator_config_v0002.yml"

OUTPUT_BASE_DIR = Path("vector") / "pipeline" / "1_generated"

ALLOWED_TARGETS = {"instance", "concept"}

SIGNATURE_FIELDS = [
    "object_type",
    "object_id",
    "embedding_model_name",
    "embedding_model_version",
    "tokenizer_version",
    "embedding_parameters_hash",
    "policy_version",
    "source_hash",
]

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)

# ============================================================
# 本地模型全局缓存（避免重复加载）
# ============================================================
_LOCAL_MODEL_CACHE: Dict[str, Any] = {}

# ============================================================
# 工具函数区 (无副作用)
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()

def _validate_identifier(name: str, context: str) -> None:
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe SQL identifier from config ({context}): '{name}'. "
            f"Only letters, digits, and underscores are allowed."
        )

def _validate_all_identifiers(tables: Dict[str, str], fields: Dict[str, Dict[str, str]]) -> None:
    for key, tbl_name in tables.items():
        _validate_identifier(str(tbl_name), f"tables.{key}")
    for group_key, field_map in fields.items():
        if not isinstance(field_map, dict):
            raise ValueError(f"Invalid fields.{group_key} (expected mapping)")
        for fkey, fname in field_map.items():
            _validate_identifier(str(fname), f"fields.{group_key}.{fkey}")

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest().lower()

def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _normalize_null(v: Any) -> str:
    if v is None or str(v) == "":
        return "NULL"
    return str(v)

def _recompute_signature(record: Dict[str, Any]) -> str:
    items = [(k, _normalize_null(record.get(k))) for k in SIGNATURE_FIELDS]
    items.sort(key=lambda x: x[0])
    return _sha256_hex("|".join([f"{k}={v}" for k, v in items]))

def _hash_parameters(model: str, dim: int, policy: str) -> str:
    param_str = f"model={model}|dim={dim}|policy={policy}"
    return _sha256_hex(param_str)

# ============================================================
# 核心业务逻辑区
# ============================================================

def _load_yaml(yaml_path: Path) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError:
        raise RuntimeError("PyYAML is required. Run: pip install pyyaml")
    
    if not yaml_path.exists():
        raise FileNotFoundError(f"Config file not found: {yaml_path}")
    
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid YAML content in {yaml_path}")
    return raw

def _get_sql_mappings(project_root: Path, config_path: Path) -> Tuple[Path, Dict[str, str], Dict[str, Dict[str, str]]]:
    raw = _load_yaml(config_path)
    conn = raw.get("connection", {}) or {}
    sqlite_cfg = conn.get("sqlite", {}) or {}
    db_path_str = sqlite_cfg.get("path", "sql/data.db")
    db_path = (project_root / str(db_path_str)).resolve()
    
    tables = raw.get("tables", {}) or {}
    fields = raw.get("fields", {}) or {}
    
    _validate_all_identifiers(tables, fields)
    
    return db_path, tables, fields

def _fetch_unique_assets(conn: sqlite3.Connection, tables: Dict[str, str], fields: Dict[str, Dict[str, str]]) -> List[str]:
    tbl_i = tables["instance"]
    f_asset = fields["instance"]["asset_id"]
    sql = f"SELECT DISTINCT {f_asset} FROM {tbl_i} WHERE {f_asset} IS NOT NULL"
    rows = conn.execute(sql).fetchall()
    return [str(r[0]) for r in rows]

def _fetch_instance_batch(
    conn: sqlite3.Connection, tables: Dict[str, str], fields: Dict[str, Dict[str, str]], 
    asset_id: str, offset: int, limit: int
) -> List[sqlite3.Row]:
    tbl_i = tables["instance"]
    fi = fields["instance"]
    sql = f"""
        SELECT {fi['id']} as id, {fi['content']} as content, {fi['content_hash']} as hash
        FROM {tbl_i}
        WHERE {fi['asset_id']} = ?
        ORDER BY {fi['id']}
        LIMIT ? OFFSET ?
    """
    return conn.execute(sql, (asset_id, limit, offset)).fetchall()

def _fetch_concept_batch(
    conn: sqlite3.Connection, tables: Dict[str, str], fields: Dict[str, Dict[str, str]], 
    offset: int, limit: int
) -> List[sqlite3.Row]:
    tbl_c = tables["concept"]
    fc = fields["concept"]
    sql = f"""
        SELECT {fc['id']} as id, {fc['text']} as content, {fc['hash']} as hash
        FROM {tbl_c}
        ORDER BY {fc['id']}
        LIMIT ? OFFSET ?
    """
    return conn.execute(sql, (limit, offset)).fetchall()

def _mock_api_call(texts: List[str], dim: int) -> List[List[float]]:
    """Mock API generator avoiding network calls and token costs for testing."""
    vectors = []
    for _ in texts:
        vectors.append([random.uniform(-1.0, 1.0) for _ in range(dim)])
    return vectors

def _local_model_call(texts: List[str], embed_config: Dict[str, Any]) -> List[List[float]]:
    """Load local sentence-transformers model and generate embeddings."""
    global _LOCAL_MODEL_CACHE
    
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise RuntimeError("sentence-transformers is required for local mode. Run: pip install sentence-transformers")
    
    backend_cfg = embed_config.get("backend", {})
    model_name = backend_cfg.get("model_name", "BAAI/bge-m3")
    device = backend_cfg.get("device", "cpu")
    
    # 使用缓存避免重复加载模型
    cache_key = f"{model_name}:{device}"
    if cache_key not in _LOCAL_MODEL_CACHE:
        _LOCAL_MODEL_CACHE[cache_key] = SentenceTransformer(model_name, device=device)
    
    model = _LOCAL_MODEL_CACHE[cache_key]
    
    # 生成 embedding（分批 forward pass，防止 GPU OOM）
    encode_batch_size = int(backend_cfg.get("encode_batch_size", 4))
    embeddings = model.encode(
        texts,
        batch_size=encode_batch_size,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    
    # 转换为 List[List[float]]
    return [emb.tolist() for emb in embeddings]

def _http_api_call(texts: List[str], embed_config: Dict[str, Any]) -> List[List[float]]:
    """Standard OpenAI RESTful POST request compatible with vLLM, Infinity, etc."""
    api_cfg = embed_config.get("api", {})
    endpoint = api_cfg.get("endpoint")
    if not endpoint:
        raise ValueError("API endpoint is not configured in YAML.")
        
    token_env = api_cfg.get("auth_token_env", "")
    timeout = int(api_cfg.get("timeout", 120))
    model_name = embed_config.get("model", {}).get("name", "BAAI/bge-m3")

    headers = {"Content-Type": "application/json"}
    if token_env and token_env in os.environ:
        headers["Authorization"] = f"Bearer {os.environ[token_env]}"

    payload = json.dumps({"model": model_name, "input": texts}).encode("utf-8")
    req = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            res_body = json.loads(resp.read().decode("utf-8"))
            data_objs = sorted(res_body.get("data", []), key=lambda x: x.get("index", 0))
            return [obj["embedding"] for obj in data_objs]
    except urllib.error.URLError as e:
        raise RuntimeError(f"Embedding API request failed: {e}")

def _generate_and_write_batch(
    rows: List[sqlite3.Row],
    target: str,
    embed_config: Dict[str, Any],
    run_id: str,
    out_file: Path,
    mock: bool
) -> int:
    if not rows:
        return 0

    texts = []
    for r in rows:
        src_hash = str(r["hash"]).lower()
        if not _HEX_64_RE.match(src_hash):
            raise ValueError(f"Invalid source_hash format (not hex64) for id: {r['id']}")
        texts.append(str(r["content"]))

    model_cfg = embed_config.get("model", {})
    dim = int(model_cfg.get("dimension", 1024))
    
    # 1. Get Vectors
    if mock:
        vectors = _mock_api_call(texts, dim)
    else:
        backend_cfg = embed_config.get("backend", {})
        backend_type = backend_cfg.get("type", "local")
        
        if backend_type == "local":
            # 本地模型调用
            vectors = _local_model_call(texts, embed_config)
        elif backend_type == "http":
            # HTTP API 调用（带重试）
            max_retries = int(embed_config.get("runtime", {}).get("max_retries", 3))
            delay = int(embed_config.get("runtime", {}).get("retry_delay", 5))
            vectors = []
            for attempt in range(max_retries):
                try:
                    vectors = _http_api_call(texts, embed_config)
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(delay)
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")

    if len(vectors) != len(rows):
        raise RuntimeError(f"Model returned {len(vectors)} vectors, expected {len(rows)}")

    # 2. Strict Dimension Validation
    for vec in vectors:
        if len(vec) != dim:
            raise RuntimeError(f"Embedding dimension mismatch: model returned {len(vec)}, expected config {dim}")

    # 3. Build Contract JSON
    model_name = model_cfg.get("name", "unknown")
    model_ver = model_cfg.get("version", "v1.0")
    tok_ver = model_cfg.get("tokenizer_version", "unknown")
    pol_ver = model_cfg.get("policy_version", "v0001")
    param_hash = _hash_parameters(model_name, dim, pol_ver)
    
    records = []
    generated_at = _utc_iso()

    for row, vec in zip(rows, vectors):
        rec = {
            "object_type": target,
            "object_id": str(row["id"]),
            "embedding_vector": vec,
            "embedding_dimension": dim,
            "embedding_model_name": model_name,
            "embedding_model_version": model_ver,
            "tokenizer_version": tok_ver,
            "embedding_parameters_hash": param_hash,
            "policy_version": pol_ver,
            "source_hash": str(row["hash"]).lower(),
            "generated_at": generated_at,
            "run_id": run_id,
            "state": "active",
        }
        rec["embedding_signature"] = _recompute_signature(rec)
        records.append(rec)

    # 4. Append to output file
    with out_file.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(_safe_json(rec) + "\n")

    return len(records)

def _write_run_meta(run_dir: Path, meta_data: Dict[str, Any]) -> None:
    meta_path = run_dir / "run_meta.json"
    meta_path.write_text(_safe_json(meta_data), encoding="utf-8")

# ============================================================
# CLI / main 接口区
# ============================================================

# --- 1. 修改参数解析区 (约第 360 行) ---
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate Embedding JSONL Contract Files.")
    ap.add_argument("--run-id", required=True, help="Global execution batch ID.")
    ap.add_argument("--target", required=True, choices=sorted(ALLOWED_TARGETS), help="Type of data to vectorize.")
    ap.add_argument("--asset-id", default=None, help="Specific asset (conv_xxx) to process. Required if target=instance.")
    ap.add_argument("--all-assets", action="store_true", help="Process ALL instances by discovering distinct asset_ids.")
    
    # --- 新增：分页控制参数 ---
    ap.add_argument("--limit-dialogs", type=int, default=None, help="Limit number of dialogs to process in this run.")
    ap.add_argument("--offset-dialogs", type=int, default=0, help="Skip N dialogs from the start of the discovered list.")
    ap.add_argument("--limit-concepts", type=int, default=None, help="Limit number of concept records to process in this run.")
    ap.add_argument("--offset-concepts", type=int, default=0, help="Skip N concept records from the start of the table.")
    # -----------------------

    ap.add_argument("--mock-api", action="store_true", help="Use random floats instead of calling real API.")
    ap.add_argument("--sql-config", default=None, help="Override path to sql_writer_config_v0001.yml.")
    ap.add_argument("--embed-config", default=None, help="Override path to embedding_generator_config_v0002.yml.")
    ap.add_argument(
        "--output-base-dir",
        default=None,
        help="Override generated-run parent directory. Default: vector/pipeline/1_generated.",
    )
    return ap.parse_args()

def main() -> int:
    started_at = _utc_iso()
    args = parse_args()
    
    summary: Dict[str, Any] = {
        "action": "generate_embeddings",
        "status": "started",
        "run_id": args.run_id,
        "target": args.target,
        "total_generated": 0,
        "assets_processed": [],
        "output_dir": None,
        "error": None
    }

    run_dir = None
    embed_config = {}
    total_processed = 0
    assets_processed = []
    
    try:
        if args.target == "instance" and not args.asset_id and not args.all_assets:
            raise ValueError("Must specify either --asset-id or --all-assets when target is instance.")

        here = Path(__file__).resolve() if "__file__" in globals() else Path.cwd()
        project_root = _find_project_root(here)

        sql_cfg_path = Path(args.sql_config).resolve() if args.sql_config else project_root / DEFAULT_SQL_CONFIG_PATH
        emb_cfg_path = Path(args.embed_config).resolve() if args.embed_config else project_root / DEFAULT_EMBED_CONFIG_PATH

        db_path, tables, fields = _get_sql_mappings(project_root, sql_cfg_path)
        embed_config = _load_yaml(emb_cfg_path)
        
        batch_size = int(embed_config.get("runtime", {}).get("batch_size", 100))
        output_base_dir = (
            Path(args.output_base_dir).resolve()
            if args.output_base_dir
            else project_root / OUTPUT_BASE_DIR
        )
        run_dir = output_base_dir / args.run_id
        _ensure_dir(run_dir)
        summary["output_dir"] = str(run_dir)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        if args.target == "concept":
            concept_dir = run_dir / "concept"
            _ensure_dir(concept_dir)
            out_file = concept_dir / "all_concepts.jsonl"
            # v0002: concept 分片支持
            sql_offset = args.offset_concepts
            records_remaining = args.limit_concepts  # None means unlimited
            while True:
                rows = _fetch_concept_batch(conn, tables, fields, sql_offset, batch_size)
                if not rows:
                    break
                # 如果设置了 limit，裁剪最后一个 batch
                if records_remaining is not None:
                    if records_remaining <= 0:
                        break
                    rows = rows[:records_remaining]
                count = _generate_and_write_batch(rows, "concept", embed_config, args.run_id, out_file, args.mock_api)
                total_processed += count
                sql_offset += batch_size
                if records_remaining is not None:
                    records_remaining -= count
            
            if total_processed == 0 and args.offset_concepts > 0:
                print(json.dumps({"status": "info", "message": "No matching records found in specified range."}, ensure_ascii=False))
                conn.close()
                return 0
                
# --- 2. 修改业务逻辑区 (约第 430 行) ---
        elif args.target == "instance":
            instance_dir = run_dir / "instance"
            _ensure_dir(instance_dir)
            
            # 获取所有待处理的 asset_id
            all_assets = [args.asset_id] if args.asset_id else _fetch_unique_assets(conn, tables, fields)
            
            # --- 关键修改：应用分页切片 ---
            start = args.offset_dialogs
            # 如果设置了 limit，则计算结束索引，否则处理剩余所有
            end = start + args.limit_dialogs if args.limit_dialogs else len(all_assets)
            assets = all_assets[start:end]
            
            if not assets:
                print(json.dumps({"status": "info", "message": "No matching records found in specified range."}, ensure_ascii=False))
                return 0
            # ----------------------------

            for asset in assets:
                out_file = instance_dir / f"{asset}.jsonl"
                offset = 0
                while True:
                    rows = _fetch_instance_batch(conn, tables, fields, asset, offset, batch_size)
                    if not rows:
                        break
                    count = _generate_and_write_batch(rows, "instance", embed_config, args.run_id, out_file, args.mock_api)
                    total_processed += count
                    offset += batch_size
                assets_processed.append(asset)
        
        conn.close()

        summary["status"] = "success"
        summary["total_generated"] = total_processed
        summary["assets_processed"] = assets_processed

    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = str(e)
    
    finally:
        # 无论成功失败，只要 run_dir 被创建，就必定写入 run_meta.json 闭环审计
        if run_dir and run_dir.exists():
            model_cfg = embed_config.get("model", {})
            backend_cfg = embed_config.get("backend", {})
            meta_data = {
                "run_id": args.run_id,
                "target": args.target,
                "backend_type": backend_cfg.get("type", "unknown"),
                "model_name": model_cfg.get("name", "unknown"),
                "dimension": int(model_cfg.get("dimension", 1024)) if model_cfg else 0,
                "policy_version": model_cfg.get("policy_version", "unknown"),
                "status": summary["status"],
                "error": summary["error"],
                "started_at": started_at,
                "completed_at": _utc_iso(),
                "total_generated": total_processed,
                "assets": assets_processed if args.target == "instance" else ["global_concepts"],
                "mock_mode": args.mock_api
            }
            _write_run_meta(run_dir, meta_data)

    # 退出前仅执行一次结构化打印
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    
    return 0 if summary["status"] == "success" else 1

# ============================================================
# if __name__ == "__main__" 入口
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
