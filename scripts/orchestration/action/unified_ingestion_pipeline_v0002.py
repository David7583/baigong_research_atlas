# ============================================================
# 文件名: unified_ingestion_pipeline_v0002.py
# 中文名: 统一资料接入有限编排
# 版本号: v0002
#
# 主层级: action
# 层级: tools / ingestion / unified_ingestion_orchestrator
# 脚本定位: 统一资料接入有限编排的独立执行边界
#
# 职责说明:
# - 统一资料接入有限编排，提供明确输入输出
#
# 本脚本做什么:
# - 执行本模块声明的接入操作并返回真实状态
#
# 本脚本不做什么:
# - 不执行未知代码，不调用收费模型，不写核心数据库
#
# 制度边界声明:
# - 输入原件只读；运行产物仅写显式受管目录，追加发布，不覆盖已有证据
# - 失败明确返回；导入无写入副作用；测试仅使用 temp 下隔离数据
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: unified_ingestion_pipeline_v0002
# family: unified_ingestion_pipeline
# role: unified_ingestion_orchestrator
# version: v0002
# status: active
# entry_point: scripts/orchestration/action/unified_ingestion_pipeline_v0002.py
# input:
#   - explicit paths and versioned ingestion configuration
# output:
#   - validated results with provenance and classified errors
# depends_on:
#   - ingestion_store_v0001
#   - ingestion_extract_v0001
#   - ingestion_quality_v0001
#   - ingestion_handoff_v0002
#   - ingestion_receipt_v0002
#   - jsonschema (existing environment)
# used_by:
#   - explicit CLI
# ============================================================

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[3]
PROJECT_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "AGENTS.md").is_file())
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(CODE_ROOT))

from scripts.action.tools.ingestion.ingestion_store_v0001 import (
    Config, IngestionError, Store, digest, now, publish_json, sha,
)
from scripts.action.tools.ingestion.ingestion_extract_v0001 import extract
from scripts.action.tools.ingestion.ingestion_quality_v0001 import assess
from scripts.action.tools.ingestion.ingestion_handoff_v0002 import prepare
from scripts.action.tools.ingestion.ingestion_receipt_v0002 import confirm

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "unified_ingestion_pipeline"
SCRIPT_NAME = "unified_ingestion_pipeline_v0002"
SCRIPT_VERSION = "v0002"
DEFAULT_CONFIG = CODE_ROOT / "config/action/config/unified_ingestion_config_v0001.json"


# ============================================================
# 工具函数区
# ============================================================

def validate_result(record):
    import jsonschema
    schema = json.loads((CODE_ROOT / "config/action/config/unified_ingestion_result_v0001.schema.json")
                        .read_text(encoding=DEFAULT_ENCODING))
    jsonschema.Draft202012Validator(schema).validate(record)
    if record["body_ingestion_status"] == "completed" and not record.get("core_receipt"):
        raise IngestionError("core_completion_requires_verified_receipt")
    if record["quality_status"] != "passed" and record["units"]:
        raise IngestionError("untrusted_units_in_body_channel")


# ============================================================
# 核心编排
# ============================================================

class Pipeline:
    def __init__(self, config):
        import jsonschema
        schema = json.loads((CODE_ROOT / "config/action/config/unified_ingestion_result_v0001.schema.json")
                            .read_text(encoding=DEFAULT_ENCODING))
        jsonschema.Draft202012Validator.check_schema(schema)
        self.config = config
        self.store = Store(config)

    def process(self, source=None, *, dry_run=False, resume=None, batch_id=None,
                original_name=None, browser_upload=False):
        run_id = "r_" + uuid.uuid4().hex
        batch_id = batch_id or "b_" + uuid.uuid4().hex
        if resume:
            old = self.store.find(resume)
            snapshot = dict(old["source"])
            path = self.store.verify(snapshot)
        else:
            snapshot = self.store.preserve(source, dry_run, original_name, browser_upload)
            path = Path(source) if dry_run else self.store.verify(snapshot)
        record = {
            "schema_version": "unified_ingestion_result_v0001", "run_id": run_id,
            "ingest_batch_id": batch_id, "ingested_at": now(), "source": snapshot,
            "processing_status": "processing", "registration_status": "local_catalog",
            "parse_status": "not_attempted", "quality_status": None, "quality": None,
            "body_ingestion_status": "not_ingested", "handoff_status": "not_attempted",
            "downstream_receipt": None, "units": [], "warnings": [],
            "reprocess_status": "available", "derived_from": resume,
            "registration": {"content_kind": "asset_registration",
                             "text": snapshot["original_name"] + "：已登记原件；正文尚未摄取。",
                             "answer_scope": "existence_location_status_only"},
        }
        if not dry_run:
            self.store.event(run_id, "preserved", source=snapshot)
            publish_json(self.config.state_root / "registrations" / (run_id + ".json"), record)
        extraction = extract(path, snapshot["original_name"], self.config.max_file_bytes)
        quality = assess(path, extraction)
        record.update(parse_status=extraction["parse_status"], quality_status=quality["status"],
                      quality=quality, media_type=extraction["media_type"],
                      adapter_id=extraction["adapter_id"], adapter_version=extraction["adapter_version"],
                      method=extraction["method"], warnings=extraction["warnings"])
        trusted = quality["status"] == "passed"
        key = digest([snapshot["source_hash"], extraction["adapter_id"],
                      extraction["adapter_version"], quality["policy_version"]])
        if trusted:
            record["units"] = [
                {**unit, "unit_id": "iu_" + digest([key, i, unit])}
                for i, unit in enumerate(extraction["units"])
            ]
        if not dry_run:
            # Re-check bytes before publication, including restored snapshots.
            self.store.verify(snapshot)
            diagnostics = self.config.state_root / "derivations" / key / "extraction.json"
            publish_json(diagnostics, {"extraction": extraction, "quality": quality})
            record["derivation"] = {"path": str(diagnostics), "sha256": sha(diagnostics),
                                    "idempotency_key": key}
            self.store.event(run_id, "quality_checked", quality_status=quality["status"])
        if dry_run:
            record.update(processing_status="dry_run", registration_status="dry_run",
                          handoff_status="dry_run" if trusted else "blocked_quality")
        else:
            record.update(prepare(self.config, snapshot, extraction, quality))
            record["processing_status"] = (
                "ready_for_handoff" if record["handoff_status"] == "ready_for_handoff"
                else "failed" if record["handoff_status"] == "failed"
                else "body_prepared" if trusted else "registration_only")
            record["registration"]["text"] = (
                snapshot["original_name"] + "：原件已保存；"
                + ("已生成通过检查的正文，尚未入核心库。" if trusted else "正文未可靠解析，仅可查询登记。"))
        validate_result(record)
        if not dry_run:
            self.store.append(record)
            self.store.event(run_id, record["processing_status"])
        return record

    def batch(self, paths, dry_run=False):
        paths = list(paths)
        if not paths or len(paths) > self.config.max_batch_files:
            raise IngestionError("invalid_batch_size")
        batch_id = "b_" + uuid.uuid4().hex
        results = []
        for path in paths:
            try:
                results.append(self.process(path, dry_run=dry_run, batch_id=batch_id))
            except Exception as exc:
                error = {"processing_status": "failed", "error_type": type(exc).__name__,
                         "detail": str(exc), "input": str(path)}
                results.append(error)
                if not dry_run:
                    try:
                        self.store.event(batch_id, "failed", error=error)
                    except Exception as log_exc:
                        error["audit_error"] = {"error_type": type(log_exc).__name__,
                                                "detail": str(log_exc)}
        failed = sum(r["processing_status"] == "failed" for r in results)
        return {"status": "failed" if failed == len(results) else "partial" if failed else
                "dry_run" if dry_run else "completed",
                "scope": "intake_only_not_core_ingestion", "ingest_batch_id": batch_id,
                "failed": failed, "results": results}


# ============================================================
# CLI / main 接口区
# ============================================================

def _build_parser():
    parser = argparse.ArgumentParser(description="Preserve and quality-gate local materials.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input", type=Path, nargs="+")
    group.add_argument("--query")
    group.add_argument("--resume", help="Reprocess a recorded run from its verified snapshot")
    group.add_argument("--confirm-core", help="Intake run_id whose core completion is to be verified")
    parser.add_argument("--completion", type=Path)
    return parser


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = _build_parser().parse_args()
    try:
        pipeline = Pipeline(Config.load(args.config, PROJECT_ROOT))
        if args.confirm_core:
            if args.completion is None:
                raise IngestionError("completion_manifest_required")
            value = confirm(pipeline.config, args.confirm_core, args.completion, args.dry_run)
            validate_result(value)
        elif args.query is not None:
            value = {"status": "completed", "scope": "asset_registration",
                     "results": pipeline.store.catalog(args.query)}
        elif args.resume:
            value = pipeline.process(resume=args.resume, dry_run=args.dry_run)
        else:
            value = pipeline.batch(args.input, args.dry_run)
        print(json.dumps(value, ensure_ascii=False))
        return 2 if value.get("status") in {"failed", "partial"} or value.get("processing_status") == "failed" else 0
    except (IngestionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
