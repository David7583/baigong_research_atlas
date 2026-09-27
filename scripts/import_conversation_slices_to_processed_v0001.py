#!/usr/bin/env python3
# ============================================================
# File: import_conversation_slices_to_processed_v0001.py
# 中文名: 对话切片数据导入与承认脚本
# Version: v0001
# Layer: execution
# Main Layer: Data
# Updatable: True
#
# 职责说明:
# 本脚本用于将 parse_workspace 中某一明确 task 的
# conversation 切片数据复制并导入 data_processed，
# 使其获得系统承认的数据资产身份。
#
# 本脚本做什么:
# - 接收明确指定的 source task 与 slices 目录
# - 校验切片任务的基本完整性
# - 将 conversation 切片复制至 data_processed 的标准目录
# - 生成一份 import manifest 作为制度承认凭证
#
# 本脚本不做什么:
# - 不解析 message 内容
# - 不修改任何 conversation 字段
# - 不做清洗、聚合、重排或语义理解
# - 不生成索引、向量或数据库
# - 不删除或移动 parse_workspace 中的任何文件
#
# 设计说明补充:
# - 目标目录中的文件名采用连续编号形式，
#   作为稳定定位符而非语义承载体。
# - 原始 conversation_index 等追溯信息
#   由数据内容内部的 meta 字段负责保存。
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: import_conversation_slices_to_processed
# family: data_import
# role: conversation_asset_importer
# version: v0001
# status: active
# entry_point: import_conversation_slices_to_processed_v0001.py
# input:
#   - parse_workspace task slices directory
# output:
#   - data_processed conversation dataset
#   - import manifest.json
# depends_on: []
# used_by:
#   - downstream analysis scripts
#   - future indexing / database loaders
# ============================================================

# ============================================================
# 制度与职责说明注释区
# ============================================================
# 1. 本脚本属于执行型数据层脚本，仅在人工触发下运行
# 2. 本脚本不进行任何制度判断或业务裁决
# 3. 本脚本允许写入 data_processed 指定目录
# 4. 本脚本不修改、不删除任何源数据
# 5. 本脚本生成的 manifest 作为系统承认凭证，不得随意修改
# ============================================================

# ============================================================
# Imports 区
# ============================================================
import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 必须显式指定 source task 与 slices 目录
# - 不允许自动发现或推断最新 task
# - 不允许覆盖既有 version 目录
# - 默认支持 dry-run 模式
# - 禁止在 import 阶段执行任何 I/O 之外的逻辑
# ============================================================

# ============================================================
# 常量与全局配置区
# ============================================================
SCRIPT_NAME = "import_conversation_slices_to_processed_v0001.py"
SCRIPT_VERSION = "v0001"

DEFAULT_NAMING_PATTERN = "conv_{index:06d}.json"
EXCLUDE_FILENAMES = {
    "slice_manifest.json",
    "manifest.json",
}

# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    path.mkdir(parents=True, exist_ok=True)


def list_slice_files(slices_dir: Path) -> List[Path]:
    return sorted(
        p for p in slices_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".json"
        and p.name not in EXCLUDE_FILENAMES
    )


# ============================================================
# 核心业务逻辑区
# ============================================================
def run_import(
    *,
    source_task_id: str,
    source_dir: Path,
    target_root: Path,
    version: str,
    dry_run: bool,
) -> dict:
    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError(f"source slices directory not found: {source_dir}")

    slice_files = list_slice_files(source_dir)
    if not slice_files:
        raise ValueError("no conversation slice json files found in source directory")

    target_version_dir = target_root / version
    target_data_dir = target_version_dir / "data"
    manifest_path = target_version_dir / "manifest.json"

    if target_version_dir.exists():
        raise FileExistsError(f"target version already exists: {target_version_dir}")

    ensure_dir(target_data_dir, dry_run=dry_run)

    for idx, src_file in enumerate(slice_files, start=1):
        dst_name = DEFAULT_NAMING_PATTERN.format(index=idx)
        dst_path = target_data_dir / dst_name
        if dry_run:
            print(f"[DRY-RUN] would copy {src_file.name} -> {dst_path}")
        else:
            shutil.copy2(src_file, dst_path)

    manifest = {
        "asset_type": "conversation_collection",
        "source": "chatgpt",
        "version": version,
        "imported_at": utc_now_z(),
        "imported_by": SCRIPT_NAME,
        "source_task": {
            "task_id": source_task_id,
            "source_dir": str(source_dir),
            "slice_count": len(slice_files),
        },
        "storage": {
            "format": "json-per-conversation",
            "naming": DEFAULT_NAMING_PATTERN,
        },
        "integrity": {
            "hash_algorithm": None,
            "per_file_hash": False,
        },
        "notes": "First officially admitted ChatGPT conversation dataset.",
    }

    if dry_run:
        print("[DRY-RUN] would write manifest.json")
    else:
        ensure_dir(target_version_dir, dry_run=False)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return {
        "status": "ok",
        "version": version,
        "imported_count": len(slice_files),
        "target_dir": str(target_version_dir),
        "dry_run": dry_run,
    }


# ============================================================
# CLI / main 接口区
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import conversation slices from parse_workspace into data_processed with formal admission."
    )
    parser.add_argument("--source-task", required=True, help="Source task_id (e.g. task_20260122T113724Z)")
    parser.add_argument("--source-dir", required=True, help="Slices directory of the source task")
    parser.add_argument("--target-root", required=True, help="Target root under data_processed")
    parser.add_argument("--version", required=True, help="Version label (e.g. v0001)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without writing any files")

    args = parser.parse_args()

    result = run_import(
        source_task_id=args.source_task,
        source_dir=Path(args.source_dir),
        target_root=Path(args.target_root),
        version=args.version,
        dry_run=args.dry_run,
    )

    # 无论 dry-run 与否，统一输出结构化结果
    print(json.dumps(result, ensure_ascii=False, indent=2))

    return 0


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
