# ============================================================
# 文件名: research_retrieval_v0001.py
# 中文名: 研究证据独立检索接入
# 版本号: v0001
#
# 主层级: action
# 层级: development / research_assistant / retrieval
# 脚本定位: 授权研究任务到既有独立检索接口的薄适配
#
# 职责说明:
# - 绑定任务来源范围并复用现有只读查询执行器
#
# 本脚本做什么:
# - 提供参数化词项和精确 ID 查询、来源描述、证据哈希复核
# - 显式策略模式复用现有策略先存档后执行路径
#
# 本脚本不做什么:
# - 不实现向量引擎，不做 AI 调用，不改变数据库权限
#
# 制度边界声明:
# - 数据源路径限于独立工程；SQL 只交给既有受限执行器
# - 查询失败、空结果及截断分开返回，不声称全量研究覆盖
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: research_retrieval_v0001
# family: research_retrieval
# role: research_evidence_adapter
# version: v0001
# status: archived
# entry_point: staging/scripts/action/development/scripts/research_assistant/research_retrieval_v0001.py
# input:
#   - authorized task and trusted source mapping
# output:
#   - bounded evidence results and coverage information
# depends_on:
#   - research_ledger_v0001
#   - retrieval_executor_v0001
#   - retrieval_strategy_v0001
# used_by:
#   - research_assistant_v0001
# ============================================================

from __future__ import annotations

import time
from pathlib import Path

from research_ledger_v0001 import ResearchError, digest, identifier, require, scoped_path
from scripts.action.tools.retrieval.retrieval_executor_v0001 import QueryLimits, describe_sources, execute_query
from scripts.action.tools.retrieval.retrieval_strategy_v0001 import StrategyArchive, execute_strategy
from scripts.action.tools.retrieval.experience_store_v0001 import sql_name


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "research_retrieval"
SCRIPT_NAME = "research_retrieval_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 核心业务组件
# ============================================================

class ResearchRetrieval:
    def __init__(self, root, sources):
        self.root = Path(root).resolve()
        self.sources = {}
        for source_id, spec in sources.items():
            identifier(source_id)
            source = dict(spec)
            for field in ("path", "snapshot_root"):
                if field in source:
                    source[field] = str(scoped_path(self.root, source[field]))
            self.sources[source_id] = source

    def describe(self, task):
        sources = {key: value for key, value in self.sources.items() if key in task["source_ids"]}
        return {"sources": describe_sources(sources), "unavailable_source_ids": sorted(set(task["source_ids"]) - set(sources)),
                "modes": ["controlled", "strategy"], "automatic_fallback": False}

    def query(self, task, source_id, *, text=None, object_id=None, limit=30, offset=0):
        started = time.perf_counter()
        require(source_id in task["source_ids"] and source_id in self.sources, "SOURCE_NOT_AUTHORIZED")
        require((text is None) != (object_id is None), "ONE_QUERY_SELECTOR_REQUIRED")
        require(type(limit) is int and 1 <= limit <= 100 and type(offset) is int and 0 <= offset <= 10000, "QUERY_LIMIT")
        source = self.sources[source_id]
        require(source["backend"] == "sqlite" and "evidence_mapping" in source, "CONTROLLED_MAPPING_UNAVAILABLE")
        mapping = source["evidence_mapping"]
        table, id_column, text_column = (sql_name(mapping[k]) for k in ("table", "id_column", "text_column"))
        columns = source["tables"][mapping["table"]]
        selected = ",".join(sql_name(c) for c in columns)
        if object_id is not None:
            identifier(object_id)
            predicate, args = f"{id_column}=?", [object_id]
        else:
            require(isinstance(text, str) and 0 < len(text) <= 2000, "QUERY_TEXT_INVALID")
            predicate, args = f"instr({text_column},?)>0", [text]
        # instr is not in the existing executor's function allowlist: use escaped
        # LIKE, whose allowed semantics are already covered by the shared executor.
        if text is not None:
            predicate = f"{text_column} LIKE ? ESCAPE '\\'"
            args = ["%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"]
        result = execute_query(source, f"SELECT {selected} FROM {table} WHERE {predicate} ORDER BY {id_column} LIMIT ? OFFSET ?",
                               [*args, limit + 1, offset], QueryLimits(5000, limit, 262144))
        result.update(mode="controlled", source_id=source_id,
                      coverage={"source_ids": [source_id], "method": "exact_id" if object_id else "lexical_substring",
                                "semantic_recall": False, "exhaustive_research": False, "offset": offset},
                      elapsed_ms=round((time.perf_counter() - started) * 1000, 3))
        if result["status"] == "completed" and not result["rows"]:
            result["status"] = "empty"
        result["next_offset"] = offset + limit if result.get("truncated") else None
        return result

    def verify(self, task, references):
        checks = []
        for reference in references:
            result = self.query(task, reference["source_id"], object_id=reference["object_id"], limit=2)
            require(result["status"] == "completed" and len(result["rows"]) == 1, "EVIDENCE_UNAVAILABLE")
            spec = self.sources[reference["source_id"]]
            row = result["rows"][0]
            content = row[spec["evidence_mapping"]["text_column"]]
            require(digest(content.encode(DEFAULT_ENCODING)) == reference["source_hash"], "EVIDENCE_CHANGED")
            if "origin" in row:
                require(row["origin"] == reference["origin"], "EVIDENCE_ORIGIN_MISMATCH")
            checks.append({"source_id": reference["source_id"], "object_id": reference["object_id"], "status": "verified"})
        return checks

    def strategy(self, task, strategy):
        require(strategy.get("task_id") == task["task_id"], "STRATEGY_TASK_MISMATCH")
        require(set(strategy.get("authorized_source_ids", [])) <= set(task["source_ids"]) & set(self.sources), "SOURCE_NOT_AUTHORIZED")
        sources = {key: self.sources[key] for key in strategy["authorized_source_ids"]}
        archive = StrategyArchive(self.root / "runtime/research/strategies")
        saved = archive.create(strategy, sources)
        return execute_strategy(archive, saved["strategy_id"], sources, limits=QueryLimits(5000, 100, 262144))
