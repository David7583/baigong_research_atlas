# ============================================================
# 文件名: split_conversation_source_v0001.py
# 中文名: 对话来源流式分批
# 版本号: v0001
# 主层级: action
# 层级: infrastructures / source_batching
# 脚本定位: 将完整顶层数组按连续会话分为有界批次
# 职责说明:
# - 不裁剪会话，不丢弃字段，记录全部索引与内容哈希
# 本脚本做什么:
# - 流式读取原始数组，输出批次与完整覆盖凭证
# 本脚本不做什么:
# - 不写数据库，不修改原始文件，不分割单个会话
# 制度边界声明:
# - 已存在输出拒绝覆盖；单个超大会话独占一批并如实记录
# 可更新: True
# ============================================================
# ALIAS_META
# alias: split_conversation_source_v0001
# family: split_conversation_source
# role: bounded_source_batching
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/split_conversation_source_v0001.py
# input:
#   - immutable conversation array and batch byte budget
# output:
#   - immutable batch sources and exhaustive provenance manifest
# depends_on:
#   - json_stream_v0001
#   - intermediate_retention_contract_v0001
# used_by:
#   - full source ingestion caller
# ============================================================
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from intermediate_retention_contract_v0001 import RetentionError, cli_result, no_links, sha, write

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "split_conversation_source"
SCRIPT_NAME = "split_conversation_source_v0001"
SCRIPT_VERSION = "v0001"

# ============================================================
# 核心业务区
# ============================================================
def split(source: Path, output: Path, byte_budget: int, scripts_root: Path) -> dict:
    if byte_budget < 1024:
        raise RetentionError("Batch byte budget must be at least 1024")
    sys.path.insert(0, str(scripts_root))
    from json_stream_v0001 import iter_root_array
    source, output = no_links(source), no_links(output)
    if output.exists():
        raise RetentionError("Batch source output already exists; refusing overwrite")
    original_hash = sha(source)
    output.mkdir(parents=True)
    batches, pending, indices, digest = [], [], [], hashlib.sha256()
    pending_bytes = 2
    count = 0

    def flush():
        nonlocal pending, indices, pending_bytes
        if not pending:
            return
        path = output / ("b%03d.json" % len(batches))
        with path.open("xb") as stream:
            stream.write(b"[" + b",".join(pending) + b"]")
        batches.append({"path": str(path), "sha256": sha(path), "bytes": path.stat().st_size, "start_index": indices[0], "end_index_exclusive": indices[-1] + 1, "conversations": len(pending)})
        pending, indices, pending_bytes = [], [], 2

    for index, conversation in iter_root_array(source):
        if index != count:
            raise RetentionError("Non-contiguous source indices")
        encoded = json.dumps(conversation, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(DEFAULT_ENCODING)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        if pending and pending_bytes + len(encoded) + 1 > byte_budget:
            flush()
        pending.append(encoded)
        indices.append(index)
        pending_bytes += len(encoded) + 1
        count += 1
    flush()
    if not count or sha(source) != original_hash:
        raise RetentionError("Empty or changed source")
    # Independently stream every output to prove content/order equivalence.
    verify_digest, verified = hashlib.sha256(), 0
    for batch in batches:
        for _, item in iter_root_array(Path(batch["path"])):
            encoded = json.dumps(item, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(DEFAULT_ENCODING)
            verify_digest.update(len(encoded).to_bytes(8, "big"))
            verify_digest.update(encoded)
            verified += 1
    if verified != count or verify_digest.digest() != digest.digest():
        raise RetentionError("Batch reconstruction differs from complete source")
    result = {"status": "completed", "source": str(source), "source_sha256": original_hash, "source_bytes": source.stat().st_size, "conversations": count, "content_sequence_sha256": digest.hexdigest(), "coverage_verified": True, "byte_budget": byte_budget, "batches": batches}
    write(output / "manifest.json", result)
    return result

# ============================================================
# CLI / main 接口区
# ============================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--byte-budget", type=int, default=8 * 1024 * 1024)
    p.add_argument("--scripts-root", type=Path, default=Path(__file__).resolve().parents[2])
    a = p.parse_args()
    return cli_result(lambda: split(Path(a.source), Path(a.output), a.byte_budget, a.scripts_root))

if __name__ == "__main__":
    raise SystemExit(main())
