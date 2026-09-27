# ============================================================
# 文件名: retrieval_pipeline_v0001.py
# 中文名: 独立经验与双模式检索入口
# 版本号: v0001
#
# 主层级: action
# 层级: tools / retrieval / pipeline
# 脚本定位: 经验和双模式查询的独立 CLI 编排
#
# 职责说明:
# - 显式编排存储、查询、策略生成与执行
#
# 本脚本做什么:
# - 加载本应用配置、分发显式操作、输出结构化状态
#
# 本脚本不做什么:
# - 不接入主链、不启动开发登记、不授予 AI 权限
#
# 制度边界声明:
# - dry-run 不写入或调用 AI；初始化必须显式指定操作
# - 测试必须使用隔离配置；策略先存档后执行，不自动换模式
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: retrieval_pipeline_v0001
# family: retrieval_pipeline
# role: independent_experience_pipeline
# version: v0001
# status: active
# entry_point: scripts/action/tools/retrieval/retrieval_pipeline_v0001.py
# input:
#   - versioned configuration and operation JSON
# output:
#   - structured operation result
# depends_on:
#   - experience_store_v0001
#   - experience_episode_v0001
#   - experience_query_v0001
#   - retrieval_strategy_v0001
#   - retrieval_ai_v0001
# used_by: []
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

if __package__:
    from .experience_contract_v0001 import ExperienceError, bounded, canonical, digest, identifier, validate_annotation
    from .experience_store_v0001 import ExperienceStore, Source, resolve_source
    from .experience_episode_v0001 import append_relation, assemble_episode, development_memory, export_replay
    from .experience_query_v0001 import build_experience_context, query_experiences
    from .retrieval_ai_v0001 import connect_application, generate_queries
    from .retrieval_executor_v0001 import QueryLimits, describe_sources, validate_source
    from .retrieval_strategy_v0001 import StrategyArchive, execute_strategy, write_exclusive
    from .experience_projection_v0001 import semantic_search
else:
    from experience_contract_v0001 import ExperienceError, bounded, canonical, digest, identifier, validate_annotation
    from experience_store_v0001 import ExperienceStore, Source, resolve_source
    from experience_episode_v0001 import append_relation, assemble_episode, development_memory, export_replay
    from experience_query_v0001 import build_experience_context, query_experiences
    from retrieval_ai_v0001 import connect_application, generate_queries
    from retrieval_executor_v0001 import QueryLimits, describe_sources, validate_source
    from retrieval_strategy_v0001 import StrategyArchive, execute_strategy, write_exclusive
    from experience_projection_v0001 import semantic_search


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "retrieval_pipeline"
SCRIPT_NAME = "retrieval_pipeline_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 工具函数区
# ============================================================

def project_root():
    for path in Path(__file__).resolve().parents:
        if (path / "AGENTS.md").is_file() and (path / "development_agent.md").is_file():
            return path
    raise ExperienceError("PROJECT_ROOT_UNRESOLVED")


def within(root, value):
    if not isinstance(value, str) or Path(value).is_absolute():
        raise ExperienceError("CONFIG_REQUIRES_RELATIVE_PATH")
    path = (root / value).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ExperienceError("CONFIG_PATH_ESCAPE")
    return path


def load_config(path, root):
    if Path(path).stat().st_size > 131072:
        raise ExperienceError("CONFIG_TOO_LARGE")
    config = json.loads(Path(path).read_text(encoding=DEFAULT_ENCODING))
    required = {"schema_version", "enabled", "lineage", "store_path", "archive_root", "replay_path", "source_mappings", "query_sources", "limits", "ai"}
    if set(config) != required or config["schema_version"] != "0.1.0" or config["lineage"] != "action" or type(config["enabled"]) is not bool:
        raise ExperienceError("CONFIG_INVALID")
    mappings = []
    for source in config["source_mappings"]:
        mappings.append(Source(within(root, source["path"]), source["table"], source["id_column"], source["text_column"], source["object_type"]))
    store = ExperienceStore(within(root, config["store_path"]), mappings)
    sources = {}
    for sid, source in config["query_sources"].items():
        identifier(sid)
        sources[sid] = dict(source)
        for field in ("path", "snapshot_root"):
            if field in source:
                sources[sid][field] = str(within(root, source[field]))
    limits = config["limits"]
    if set(limits) != {"timeout_ms", "max_rows", "max_bytes", "context_token_budget"}:
        raise ExperienceError("CONFIG_LIMITS_REQUIRED")
    if any(type(v) is not int or v < 1 for v in limits.values()) or limits["max_rows"] > 100 or limits["timeout_ms"] > 30000 or not 1024 <= limits["max_bytes"] <= 1048576 or limits["context_token_budget"] > 32768:
        raise ExperienceError("CONFIG_LIMITS_INVALID")
    return config, store, sources, StrategyArchive(within(root, config["archive_root"]))


# ============================================================
# 核心业务组件
# ============================================================

def run(config_path, operation, data, *, root=None, dry_run=False):
    root = Path(root) if root else project_root()
    config, store, sources, archive = load_config(config_path, root)
    bounded(data)
    if not config["enabled"]:
        raise ExperienceError("EXPERIENCE_DISABLED")
    limits = QueryLimits(**{k:config["limits"][k] for k in ("timeout_ms", "max_rows", "max_bytes")})
    if operation == "describe":
        return {"status": "completed", "sources": describe_sources(sources), "mode_switch": "explicit_only"}
    if operation == "initialize":
        if dry_run:
            if store.path.exists():
                raise ExperienceError("INITIALIZATION_REQUIRES_NEW_FILE")
            return {"status": "dry_run"}
        return store.initialize()
    if operation == "anchor":
        if dry_run:
            resolve_source(data["source_ref"], store.sources)
            return {"status": "dry_run"}
        return {"status": "persisted", "anchor_id": store.anchor(data["source_ref"], data.get("existing_anchor_id"))}
    if operation == "append":
        return store.append(data["request_id"], data["producer_id"], data["candidate"], dry_run=dry_run)
    if operation == "assemble":
        return assemble_episode(store, data["episode_id"], data.get("expected_previous_revision_id"), data["members"], dry_run=dry_run)
    if operation == "relation":
        return append_relation(store, data, dry_run=dry_run)
    if operation == "memory":
        return development_memory(store, **data)
    if operation == "query":
        if data.get("mode") != "controlled":
            raise ExperienceError("EXPLICIT_CONTROLLED_MODE_REQUIRED")
        control, search = None, None
        if data["request"].get("query_text"):
            source_id = data.get("semantic_source_id")
            if source_id not in sources or sources[source_id]["backend"] != "chroma":
                raise ExperienceError("SEMANTIC_BACKEND_NOT_CONFIGURED")
            identity = config["ai"]
            if not identity["app_id"]:
                raise ExperienceError("AI_APP_NOT_CONFIGURED")
            if dry_run:
                validate_source(sources[source_id])
                return {"status": "dry_run", "model_called": False}
            control, client = connect_application(root, identity["app_id"], os.environ.get(identity["token_env"], ""))
            search = lambda question, count: semantic_search(client, sources[source_id], question, count)
        try:
            result = query_experiences(store, data["request"], timeout_ms=limits.timeout_ms, max_bytes=limits.max_bytes, semantic_search=search)
        finally:
            if control:
                control.close()
        result["context"] = build_experience_context(result, config["limits"]["context_token_budget"])
        return result
    if operation == "strategy":
        if data.get("mode") != "strategy":
            raise ExperienceError("EXPLICIT_STRATEGY_MODE_REQUIRED")
        if dry_run:
            archive.create(data["strategy"], sources, dry_run=True)
            for step in data["strategy"]["steps"]:
                if step["source_id"] not in sources:
                    raise ExperienceError("SOURCE_NOT_AUTHORIZED")
                validate_source(sources[step["source_id"]])
            return {"status": "dry_run", "query_executed": False}
        saved = archive.create(data["strategy"], sources)
        return execute_strategy(archive, saved["strategy_id"], sources, limits=limits)
    if operation == "generate":
        if dry_run:
            if not config["ai"]["app_id"]:
                raise ExperienceError("AI_APP_NOT_CONFIGURED")
            return {"status": "dry_run", "model_called": False}
        identity = config["ai"]
        if not identity["app_id"]:
            raise ExperienceError("AI_APP_NOT_CONFIGURED")
        control, client = connect_application(root, identity["app_id"], os.environ.get(identity["token_env"], ""))
        try:
            # Selection and application state are explicit caller choices; never change global permission.
            if data.get("selection"):
                client.select(**data["selection"])
            if data.get("start") is True:
                client.start()
            generated = generate_queries(client, data["question"], describe_sources(sources), operation=identity["operation"], previous_evidence=data.get("previous_evidence"))
        finally:
            control.close()
        generation_id = "generation_" + uuid.uuid4().hex
        directory = archive.root / generation_id
        directory.mkdir(parents=True)
        # Visible output is a sensitive artifact, never printed to stdout. No hidden reasoning fields.
        write_exclusive(directory / "generation.json", generated)
        try:
            parsed = json.loads(generated["visible_output"])
            bounded(parsed)
            if set(parsed) != {"steps", "explanation"}:
                raise ExperienceError("AI_QUERY_CONTRACT_INVALID")
            strategy = {"schema_version": "0.1.0", "strategy_id": "strategy_" + uuid.uuid4().hex, "request_id": data["request_id"], "task_id": data["task_id"], "parent_strategy_id": data.get("parent_strategy_id"), "version": data.get("version", 1), "question": data["question"], "authorized_source_ids": list(sources), "steps": parsed["steps"], "generator": generated["generator"], "explanation": parsed["explanation"], "applicability": data["applicability"]}
            saved = archive.create(strategy, sources)
            write_exclusive(directory / "status.json", {"status": "generated", "strategy_id": saved["strategy_id"]})
        except Exception:
            write_exclusive(directory / "status.json", {"status": "rejected", "error_type": "AI_QUERY_CONTRACT_INVALID"})
            raise ExperienceError("AI_QUERY_CONTRACT_INVALID") from None
        return {"status": "generated", "strategy_id": saved["strategy_id"], "generation_id": generation_id, "executed": False}
    if operation == "execute":
        if dry_run:
            archive.read(data["strategy_id"])
            return {"status": "dry_run"}
        return execute_strategy(archive, data["strategy_id"], sources, limits=limits)
    if operation == "review":
        return archive.review(**data, dry_run=dry_run)
    if operation == "export":
        if dry_run:
            return {"status": "dry_run"}
        return export_replay(store, data["revisions"], within(root, config["replay_path"]), missing_policy=data.get("missing_policy", "preserve"))
    raise ExperienceError("OPERATION_UNSUPPORTED")


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--input")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("operation", choices=["describe", "initialize", "anchor", "append", "assemble", "relation", "memory", "query", "strategy", "generate", "execute", "review", "export"])
    return parser


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding=DEFAULT_ENCODING)
    args = _build_parser().parse_args()
    try:
        if args.input and Path(args.input).stat().st_size > 131072:
            raise ExperienceError("REQUEST_TOO_LARGE")
        data = json.loads(Path(args.input).read_text(encoding=DEFAULT_ENCODING)) if args.input else {}
        result = run(args.config, args.operation, data, dry_run=args.dry_run)
        print(canonical(result))
        return 0 if result.get("status") in {"completed", "persisted", "existing", "initialized", "generated", "validated", "approved_for_reuse", "dry_run", "exported", "insufficient_evidence"} else 2
    except (ExperienceError, KeyError, ValueError, FileNotFoundError, FileExistsError) as exc:
        code = str(exc) if isinstance(exc, ExperienceError) else type(exc).__name__
        print(canonical({"status": "error", "error_type": code, "detail": None}))
        return 2
    except Exception:
        print(canonical({"status": "error", "error_type": "UNEXPECTED_ERROR", "detail": None}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
