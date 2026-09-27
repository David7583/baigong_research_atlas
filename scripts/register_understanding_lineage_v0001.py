#!/usr/bin/env python3
# ============================================================
# File: register_understanding_lineage_v0001.py
# 中文名: 理解端分析链路登记脚本
# Version: v0001
# Layer: registry
# Main Layer: understanding
# Updatable: True
#
# Purpose
# 本脚本用于对一条已经真实发生的理解 / 分析链路
# 进行制度级、不可逆、可审计的事实登记。
#
# What it does
# 1) 在理解开始阶段，对已承认的数据资产建立理解登记记录（第一次登记）
# 2) 在理解完成阶段，汇编实际发生的分析链路证据并冻结登记记录（第二次登记）
#
# What it does NOT do
# 1) 不规划分析流程
# 2) 不授权或阻断任何脚本执行
# 3) 不分析或判断数据内容
# 4) 不推断未显式提供的证据语义
# 5) 不自动发现最新任务或目录
#
# Non-v0001 targets (deferred, still recorded here)
# 1) 自动解析 step manifest 以填充 analysis_chain 的脚本信息字段
# 2) 引入 step run event 统一事件文件并据此重建链路
# 3) 自动探测 runtime_deps 与 import 语句级依赖
# 4) 细粒度的模型 usage 指标与跨 provider 的统一归一
# 5) 多资产合并理解登记与跨链路对比登记
#
# Notes
# - 本脚本是理解端的登记器，不是调度器
# - 所有登记均基于既存事实证据
# - v0001 采用证据引用优先策略
# ============================================================

# ===========================
# ALIAS_META (comment block)
# ===========================
# alias: register_understanding_lineage
# family: understanding_registry
# role: understanding_lineage_recorder
# version: v0001
# status: active
# entry_point: register_understanding_lineage_v0001.py
# input:
#   - asset_manifest_path
#   - evidence_items (optional, kind:path)
#   - analysis_evidence_root (finalize phase)
#   - declared_deps_summary (optional)
#   - model_usage_summary (optional)
# output:
#   - understanding registry record (json)
# depends_on: []
# used_by:
#   - understanding flows
#   - audit / replay tools

# =====================================
# 制度与职责说明注释区
# =====================================
# - 本脚本只登记事实，不进行制度裁决
# - 本脚本不修改 data_processed 中的数据资产
# - 本脚本不解析任何分析结果语义
# - 本脚本不自动发现最新任务或目录
# - 本脚本所有输入必须显式指定
# - v0001 允许 evidence 引用汇编，但不允许语义推断

from __future__ import annotations

# =====================================
# Imports 区
# =====================================
import argparse
import json
import platform
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# =====================================
# 边界声明与强约束说明
# =====================================
# - 仅使用标准库
# - 不执行任何隐式扫描行为
# - 不覆盖既有登记记录
# - 所有路径必须显式传入
# - dry-run 下不得写入任何文件

# =====================================
# 常量与全局配置区
# =====================================
SCRIPT_NAME = "register_understanding_lineage_v0001.py"
SCRIPT_VERSION = "v0001"

DEFAULT_REGISTRY_DIR = Path("understanding/registry")
CURRENT_PIPELINE_FILE = ".current_pipeline.json"

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

# =====================================
# 工具函数区（无副作用）
# =====================================
def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dumps_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dumps_json(data), encoding="utf-8")


def generate_record_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:6]
    return f"UREG_{ts}_{suffix}"


def environment_snapshot() -> Dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "executable": sys.executable,
    }


def parse_kind_path(item: str) -> Tuple[str, Path]:
    """
    Parse evidence item formatted as 'kind:path'.

    This function does not infer kind.
    It only parses the explicit declaration provided by caller.
    """
    if ":" not in item:
        raise ValueError("evidence must be formatted as kind:path")
    kind, raw_path = item.split(":", 1)
    kind = kind.strip()
    raw_path = raw_path.strip()

    if not kind:
        raise ValueError("evidence kind cannot be empty")
    if not raw_path:
        raise ValueError("evidence path cannot be empty")

    return kind, Path(raw_path)


def build_step_placeholder(step_index: int, evidence_file: Path) -> Dict[str, Any]:
    """
    v0001 uses evidence reference mode.
    It freezes a stable step schema, but does not parse evidence contents.
    """
    return {
        "step_index": step_index,
        "evidence_file": str(evidence_file),
        "script_alias": None,
        "script_family": None,
        "script_version": None,
        "inputs": None,
        "outputs": None,
        "started_at": None,
        "ended_at": None,
        "result_status": None,
    }


# =====================================
# 核心业务逻辑区
# =====================================
def open_registration(
    *,
    asset_manifest_path: Path,
    evidence_items: List[Tuple[str, Path]],
    registry_dir: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    """
    Open an understanding registry record.

    This phase is invoked after the data asset has been admitted into data_processed.
    It freezes asset anchor facts and evidence pointers without inferring semantics.
    """
    if not asset_manifest_path.exists():
        raise FileNotFoundError(f"asset manifest not found: {asset_manifest_path}")

    record_id = generate_record_id()
    asset_manifest = load_json(asset_manifest_path)

    # Always include the asset manifest as a first-class evidence fact.
    lineage_facts: List[Dict[str, str]] = [{
        "kind": "import_manifest",
        "path": str(asset_manifest_path),
    }]

    for kind, p in evidence_items:
        lineage_facts.append({
            "kind": kind,
            "path": str(p),
        })

    record: Dict[str, Any] = {
        "record_id": record_id,
        "created_at_utc": utc_now(),
        "finalized_at_utc": None,
        "status": "opened",
        "registrar": {
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "environment": environment_snapshot(),
        },
        "asset_anchor": {
            "asset_type": asset_manifest.get("asset_type"),
            "source": asset_manifest.get("source"),
            "version": asset_manifest.get("version"),
            "asset_dir": str(asset_manifest_path.parent),
            "manifest_path": str(asset_manifest_path),
        },
        "lineage_facts": lineage_facts,
        "analysis_chain": [],
        "model_usage": [],
        "third_party_usage": {
            "declared_deps": [],
            "runtime_deps": None,
        },
        "artifacts": [],
        "replay_hint": {},
    }

    if dry_run:
        # Dry-run must not write files.
        return record

    record_path = registry_dir / f"{record_id}.json"
    if record_path.exists():
        raise FileExistsError(f"registry record already exists: {record_path}")

    write_json(record_path, record)

    current_pipeline = {
        "record_id": record_id,
        "asset_dir": str(asset_manifest_path.parent),
        "status": "opened",
    }
    write_json(registry_dir / CURRENT_PIPELINE_FILE, current_pipeline)

    return record


def finalize_registration(
    *,
    record_id: str,
    registry_dir: Path,
    analysis_evidence_root: Path,
    model_usage_summary: Optional[Path],
    declared_deps_summary: Optional[Path],
    dry_run: bool,
) -> Dict[str, Any]:
    """
    Finalize an understanding registry record.

    v0001 finalization:
    - Assembles analysis evidence references with a frozen step schema
    - Optionally injects model usage summary and declared deps summary
    - Freezes status without interpreting evidence semantics
    """
    record_path = registry_dir / f"{record_id}.json"
    if not record_path.exists():
        raise FileNotFoundError(f"registry record not found: {record_path}")

    record = load_json(record_path)

    # Assemble chain steps in evidence reference mode.
    chain_steps: List[Dict[str, Any]] = []
    if not analysis_evidence_root.exists():
        # Evidence root missing is not a hard failure of the registry itself.
        # But it means chain evidence is incomplete.
        chain_steps = []
    else:
        json_files = [
            p for p in analysis_evidence_root.iterdir()
            if p.is_file() and p.suffix.lower() == ".json"
        ]
        for idx, p in enumerate(sorted(json_files), start=1):
            chain_steps.append(build_step_placeholder(idx, p))

    record["analysis_chain"] = chain_steps

    if model_usage_summary:
        if model_usage_summary.exists():
            record["model_usage"] = load_json(model_usage_summary)
        else:
            # Keep the field stable; do not infer.
            record["model_usage"] = []

    if declared_deps_summary:
        if declared_deps_summary.exists():
            deps = load_json(declared_deps_summary)
            if not isinstance(deps, list):
                raise ValueError("declared_deps_summary must be a JSON list")
            record["third_party_usage"]["declared_deps"] = deps
        else:
            record["third_party_usage"]["declared_deps"] = []

    record["finalized_at_utc"] = utc_now()
    record["status"] = "completed" if len(chain_steps) > 0 else "partial"

    if dry_run:
        # Dry-run must not write files, including current pipeline pointer.
        return record

    write_json(record_path, record)

    current_pipeline_path = registry_dir / CURRENT_PIPELINE_FILE
    if current_pipeline_path.exists():
        current = load_json(current_pipeline_path)
        current["status"] = record["status"]
        write_json(current_pipeline_path, current)

    return record


# =====================================
# CLI / main 接口区
# =====================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register an understanding lineage (open or finalize)."
    )

    subparsers = parser.add_subparsers(dest="mode", required=True)

    open_p = subparsers.add_parser("open", help="Open an understanding registration")
    open_p.add_argument("--asset-manifest", required=True, help="Path to asset manifest.json")
    open_p.add_argument(
        "--evidence",
        nargs="*",
        default=[],
        help="Evidence pointers formatted as kind:path",
    )
    open_p.add_argument(
        "--registry-dir",
        default=str(DEFAULT_REGISTRY_DIR),
        help="Understanding registry directory",
    )
    open_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run without writing files",
    )

    fin_p = subparsers.add_parser("finalize", help="Finalize an understanding registration")
    fin_p.add_argument("--record-id", required=True, help="Understanding record_id")
    fin_p.add_argument(
        "--analysis-evidence-root",
        required=True,
        help="Directory containing analysis evidence json files",
    )
    fin_p.add_argument(
        "--model-usage-summary",
        help="Optional model usage summary json",
    )
    fin_p.add_argument(
        "--declared-deps-summary",
        help="Optional declared dependencies summary json (list)",
    )
    fin_p.add_argument(
        "--registry-dir",
        default=str(DEFAULT_REGISTRY_DIR),
        help="Understanding registry directory",
    )
    fin_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run without writing files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry_dir = Path(args.registry_dir)

    if args.mode == "open":
        evidence_items: List[Tuple[str, Path]] = []
        for item in args.evidence:
            kind, p = parse_kind_path(item)
            evidence_items.append((kind, p))

        record = open_registration(
            asset_manifest_path=Path(args.asset_manifest),
            evidence_items=evidence_items,
            registry_dir=registry_dir,
            dry_run=bool(args.dry_run),
        )
        print(dumps_json(record))
        return 0

    if args.mode == "finalize":
        record = finalize_registration(
            record_id=args.record_id,
            registry_dir=registry_dir,
            analysis_evidence_root=Path(args.analysis_evidence_root),
            model_usage_summary=Path(args.model_usage_summary) if args.model_usage_summary else None,
            declared_deps_summary=Path(args.declared_deps_summary) if args.declared_deps_summary else None,
            dry_run=bool(args.dry_run),
        )
        print(dumps_json(record))
        return 0

    return 1


# =====================================
# Entry Point
# =====================================
if __name__ == "__main__":
    raise SystemExit(main())
