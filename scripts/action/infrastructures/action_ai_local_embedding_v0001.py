# ============================================================
# 文件名: action_ai_local_embedding_v0001.py
# 中文名: 行动端受控本地向量执行器
# 版本号: v0001
#
# 主层级: action
# 层级: infrastructures / ai_local_embedding
# 脚本定位: Action 总控授权后的本地 SentenceTransformer 推理组件
#
# 职责说明:
# - 从中央配置指定的现存本地目录加载模型，生成查询向量
#
# 本脚本做什么:
# - 继承独立 Chat 原 ChromaAdapter 的归一化 BGE 查询向量行为
# - 有界缓存一个模型并串行保护本机模型加载和推理
#
# 本脚本不做什么:
# - 不下载模型、不访问 Provider API、不查询或写入向量库
#
# 制度边界声明:
# - 模型路径由总控校验，输入文本由应用提供；不读取密钥、不记录正文
# - 仅返回向量，依赖或模型缺失明确失败，不静默更换模型
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: action_ai_local_embedding_v0001
# family: action_ai_local_embedding
# role: controlled_local_embedding_executor
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/action_ai_local_embedding_v0001.py
# input:
#   - validated local model descriptor and text list
# output:
#   - normalized embedding vectors
# depends_on:
#   - Python stdlib
#   - sentence_transformers
# used_by:
#   - action_ai_controller_v0002
# ============================================================

from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "action_ai_local_embedding"
SCRIPT_NAME = "action_ai_local_embedding_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 异常类型
# ============================================================

class LocalEmbeddingError(RuntimeError):
    pass


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class EmbeddingModel:
    path: Path
    device: str
    dimension: int
    max_texts: int
    max_text_chars: int


# ============================================================
# 核心类
# ============================================================

class LocalEmbeddingExecutor:
    _lock = threading.RLock()
    _model = None
    _identity = None

    def execute(self, descriptor: EmbeddingModel, payload: dict) -> dict:
        if set(payload) != {"input"}:
            raise LocalEmbeddingError("embedding_input_only")
        texts = payload["input"]
        texts = [texts] if isinstance(texts, str) else texts
        if not isinstance(texts, list) or not 1 <= len(texts) <= descriptor.max_texts:
            raise LocalEmbeddingError("embedding_batch_out_of_bounds")
        if not all(isinstance(x, str) and 0 < len(x) <= descriptor.max_text_chars for x in texts):
            raise LocalEmbeddingError("embedding_text_out_of_bounds")
        if not descriptor.path.is_dir():
            raise LocalEmbeddingError("local_embedding_model_missing")
        identity = (str(descriptor.path.resolve()), descriptor.device)
        cls = type(self)
        with cls._lock:
            if cls._identity != identity:
                try:
                    from sentence_transformers import SentenceTransformer
                    model = SentenceTransformer(str(descriptor.path), device=descriptor.device, local_files_only=True)
                except Exception:
                    raise LocalEmbeddingError("local_embedding_load_failed") from None
                cls._model, cls._identity = model, identity
            try:
                vectors = cls._model.encode(texts, normalize_embeddings=True, show_progress_bar=False).tolist()
            except Exception:
                raise LocalEmbeddingError("local_embedding_inference_failed") from None
        if len(vectors) != len(texts) or any(len(v) != descriptor.dimension or not all(math.isfinite(x) for x in v) for v in vectors):
            raise LocalEmbeddingError("local_embedding_dimension_or_value_mismatch")
        return {"embeddings": vectors, "dimension": descriptor.dimension, "normalized": True}


# ============================================================
# CLI / main 接口区
# ============================================================

def main():
    print(json.dumps({"status": "error", "error_type": "InternalComponent", "detail": "Use Action AI controller."}))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
