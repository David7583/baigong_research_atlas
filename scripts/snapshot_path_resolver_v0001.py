#!/usr/bin/env python3
# ============================================================
# 文件名: snapshot_path_resolver_v0001.py
# 中文名: 快照路径解析与还原脚本
# 版本号: v0001
# 层级: execution
# 主层级: Bottom
# 可更新: True
#
# 职责说明:
# - 将 snapshot 中的逻辑路径（POSIX 相对路径字符串）
#   解析并还原为当前操作系统下的真实 pathlib.Path 对象
#
# 不做什么:
# - 不扫描目录
# - 不修改任何文件
# - 不进行归档、删除或复制操作
# - 不进行制度或合规判断
# ============================================================

# -----------------------------
# ALIAS_META（系统级制度声明）
# -----------------------------
# ALIAS_META:
#   alias: snapshot_path_resolver
#   family: snapshot_path_resolver
#   role: path_resolver
#   version: v0001
#   status: active
#   entry_point: snapshot_path_resolver_v0001.py
#   input:
#     - project_root (Path)
#     - logical_path (str)
#   output:
#     - resolved_path (Path)
#   depends_on: []

# ------------------------------------------------------------
# 制度与职责边界说明
# ------------------------------------------------------------
# 本脚本仅负责“路径还原”这一基础动作。
# 所有文件系统行动脚本，若需将 snapshot.path
# 转换为真实可执行路径，必须通过本脚本完成。
#
# 若路径语义不成立，应立即失败。
# CLI 层允许生成诊断性报告，但不得改变失败语义。
# ------------------------------------------------------------

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# -----------------------------
# 边界声明与强约束说明
# -----------------------------
# - logical_path 必须是相对路径
# - 不允许包含 .. 进行路径逃逸
# - 解析结果必须位于 project_root 内

# -----------------------------
# 常量与全局配置区
# -----------------------------
FORBIDDEN_PARTS = {".."}

# -----------------------------
# 工具函数区（无副作用）
# -----------------------------
def _validate_logical_path(logical_path: str) -> None:
    p = Path(logical_path)
    if p.is_absolute():
        raise ValueError(f"Absolute path is not allowed: {logical_path}")
    if any(part in FORBIDDEN_PARTS for part in p.parts):
        raise ValueError(f"Path traversal is not allowed: {logical_path}")

# -----------------------------
# 核心业务逻辑区
# -----------------------------
def resolve_snapshot_path(
    project_root: Path,
    logical_path: str,
    check_exists: bool = False,
) -> Path:
    """
    将 snapshot 中的逻辑路径解析为真实路径。
    """
    _validate_logical_path(logical_path)

    project_root = project_root.resolve()
    resolved = (project_root / logical_path).resolve()

    try:
        resolved.relative_to(project_root)
    except ValueError:
        raise ValueError(
            f"Security Violation: Resolved path escapes project root: {resolved}"
        )

    if check_exists and not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {resolved}")

    return resolved

# -----------------------------
# CLI / main 接口区
# -----------------------------
def _write_cli_diagnostic_report(
    project_root: Path,
    logical_path: str,
    error: Exception,
) -> None:
    """
    CLI 诊断报告，仅用于人工调试与审计，不参与执行语义。
    """
    reports_dir = project_root / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = reports_dir / f"snapshot_path_resolver_cli_error_{timestamp}.json"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "script": "snapshot_path_resolver_v0001.py",
        "logical_path": logical_path,
        "project_root": str(project_root),
        "error_type": type(error).__name__,
        "error_message": str(error),
    }

    report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[CLI-DIAG] diagnostic report written: {report_path}", file=sys.stderr)

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve snapshot logical path into real filesystem path."
    )
    parser.add_argument(
        "logical_path",
        help="Logical path from snapshot (POSIX relative path)",
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Project root directory",
    )
    parser.add_argument(
        "--check-exists",
        action="store_true",
        help="Check whether resolved path exists",
    )
    args = parser.parse_args()

    project_root = Path(args.root).resolve()

    try:
        resolved = resolve_snapshot_path(
            project_root=project_root,
            logical_path=args.logical_path,
            check_exists=args.check_exists,
        )
        print(resolved)
        return 0
    except Exception as e:
        _write_cli_diagnostic_report(
            project_root=project_root,
            logical_path=args.logical_path,
            error=e,
        )
        raise

# -----------------------------
# 入口
# -----------------------------
if __name__ == "__main__":
    raise SystemExit(main())
