# ============================================================
# 文件名: experience_projection_v0001.py
# 中文名: 经验可重建投影
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / projection
# 脚本定位: 从不可变经验旁库生成独立版本投影
#
# 职责说明:
# - 保留标注身份和来源版本的分析与向量投影
#
# 本脚本做什么:
# - 新 DuckDB 快照、显式向量写入和召回适配
#
# 本脚本不做什么:
# - 不重建既有库、不隐式产生嵌入、不写 Neo4j
#
# 制度边界声明:
# - 向量必须来自调用方的获准总控；测试向量明确为合成
# - 新目标独占创建，失败留存状态，不冒充已封存快照
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: experience_projection_v0001
# family: experience_projection
# role: rebuildable_experience_projection
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/experience_projection_v0001.py
# input:
#   - experience store and explicitly supplied embeddings
# output:
#   - versioned DuckDB or Chroma projection
# depends_on:
#   - experience_contract_v0001
#   - retrieval_executor_v0001
#   - retrieval_ai_v0001
#   - DuckDB
#   - Chroma
# used_by:
#   - retrieval_pipeline_v0001
# ============================================================

from __future__ import annotations

import json
import math
from pathlib import Path

if __package__:
    from .experience_contract_v0001 import ExperienceError, canonical, digest, identifier, utcnow
    from .retrieval_ai_v0001 import embed_texts
    from .retrieval_executor_v0001 import QueryLimits, execute_query
    from .retrieval_strategy_v0001 import write_exclusive
else:
    from experience_contract_v0001 import ExperienceError, canonical, digest, identifier, utcnow
    from retrieval_ai_v0001 import embed_texts
    from retrieval_executor_v0001 import QueryLimits, execute_query
    from retrieval_strategy_v0001 import write_exclusive


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "experience_projection"
SCRIPT_NAME = "experience_projection_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心业务组件
# ============================================================

def export_duckdb(store, output, *, batch_id, index_version, limit=100):
    import duckdb
    identifier(batch_id); identifier(index_version)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ExperienceError("PROJECTION_LIMIT_INVALID")
    path = Path(output)
    if path.exists():
        raise ExperienceError("PROJECTION_NEW_TARGET_REQUIRED")
    path.parent.mkdir(parents=True, exist_ok=True)
    claim = path.with_suffix(path.suffix + ".claim.json")
    write_exclusive(claim, {"batch_id": batch_id, "index_version": index_version, "status": "building", "at": utcnow()})
    with store.connection() as conn:
        rows = conn.execute("SELECT * FROM annotations ORDER BY annotation_id LIMIT ?", (limit+1,)).fetchall()
    if len(rows) > limit:
        raise ExperienceError("PROJECTION_REQUIRES_EXPLICIT_LARGER_BATCH_IMPLEMENTATION")
    con = duckdb.connect(str(path))
    try:
        con.execute("BEGIN")
        con.execute("CREATE TABLE experience_annotations(annotation_id VARCHAR,anchor_id VARCHAR,lineage VARCHAR,annotation_type VARCHAR,event_time VARCHAR,created_at VARCHAR,ingested_at VARCHAR,domain VARCHAR,result_status VARCHAR,decision VARCHAR,content_hash VARCHAR,schema_version VARCHAR,index_version VARCHAR,ingest_batch_id VARCHAR)")
        for row in rows:
            con.execute("INSERT INTO experience_annotations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [row[k] for k in ("annotation_id","anchor_id","lineage","annotation_type","event_time","created_at","ingested_at","domain","result_status","decision","content_hash")] + ["0.1.0", index_version, batch_id])
        con.execute("COMMIT")
    finally:
        con.close()
    write_exclusive(path.with_suffix(path.suffix + ".manifest.json"), {"status": "sealed", "batch_id": batch_id, "index_version": index_version, "count": len(rows), "source_ids": [r["annotation_id"] for r in rows], "created_at": utcnow()})
    return {"status": "completed", "count": len(rows), "index_version": index_version}


def project_vectors(store, output, collection, embeddings, *, model, index_version, batch_id, call_ref=None, synthetic=False):
    import chromadb
    from chromadb.config import Settings
    identifier(collection); identifier(index_version); identifier(batch_id)
    if not synthetic and (not isinstance(call_ref, dict) or call_ref.get("lineage") != "action" or call_ref.get("model") != model or not call_ref.get("call_id")):
        raise ExperienceError("EMBEDDING_CONTROLLER_PROVENANCE_REQUIRED")
    if not isinstance(embeddings, dict) or not 1 <= len(embeddings) <= 100:
        raise ExperienceError("VECTOR_BATCH_INVALID")
    dimensions = {len(v) for v in embeddings.values() if isinstance(v, list)}
    if len(dimensions) != 1 or not 1 <= next(iter(dimensions)) <= 8192 or any(not isinstance(v,list) or any(type(x) not in (int,float) or not math.isfinite(x) for x in v) for v in embeddings.values()):
        raise ExperienceError("VECTOR_DIMENSION_INVALID")
    records = []
    with store.connection() as conn:
        for aid in embeddings:
            row = conn.execute("SELECT body FROM annotations WHERE annotation_id=?", (identifier(aid),)).fetchone()
            if not row:
                raise ExperienceError("VECTOR_OBJECT_UNRESOLVED")
            body = json.loads(row[0])
            records.append((aid, body))
    path = Path(output)
    path.mkdir(parents=True)  # versioned new projection only
    write_exclusive(path / "building.json", {"batch_id": batch_id, "index_version": index_version, "synthetic": synthetic, "call_ref": call_ref})
    client = chromadb.PersistentClient(path=str(path), settings=Settings(anonymized_telemetry=False))
    target = client.create_collection(collection, embedding_function=None, metadata={"embedding_model": model, "index_version": index_version, "synthetic": synthetic})
    target.add(ids=[aid for aid,_ in records], embeddings=[embeddings[aid] for aid,_ in records], metadatas=[{"annotation_id": aid, "lineage": "action", "content_hash": digest(body), "schema_version": "0.1.0", "index_version": index_version, "embedding_model": model, "ingest_batch_id": batch_id} for aid,body in records])
    write_exclusive(path / "manifest.json", {"status": "sealed", "count": len(records), "dimension": next(iter(dimensions)), "embedding_model": model, "index_version": index_version, "created_at": utcnow(), "synthetic": synthetic})
    return {"status": "completed", "count": len(records), "synthetic": synthetic}


def semantic_search(client, source, query, limit):
    embedded = embed_texts(client, [query])
    if embedded["call_ref"]["model"] != source["embedding_model"]:
        raise ExperienceError("EMBEDDING_MODEL_MISMATCH")
    result = execute_query(source, None, {"query_embedding": embedded["vectors"][0], "n_results": limit}, QueryLimits(30000, limit))
    if result["status"] != "completed":
        raise ExperienceError(result["error_type"])
    hits = []
    for row in result["rows"]:
        meta = row["metadata"]
        if meta.get("embedding_model") != source["embedding_model"] or meta.get("index_version") != source["index_version"] or meta.get("lineage") != "action":
            raise ExperienceError("VECTOR_PROJECTION_MISMATCH")
        hits.append({**meta, "distance": row["distance"], "embedding_call_ref": embedded["call_ref"]})
    return hits
