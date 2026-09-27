#!/usr/bin/env python3
# ============================================================
# File: identify_file_type_v0002.py
# 中文名: 文件类型识别脚本
# Version: v0002
# Layer: execution
# Main Layer: Data
# Updatable: True
#
# Purpose
# 本脚本用于递归扫描当前项目 data/data_raw 目录中的文件，
# 并对发现的文件进行“类型识别”，使用相对 data 目录的路径输出结果。
#
# What it does
# 1) 递归扫描 data/data_raw 及其子目录中的所有文件
# 2) 基于扩展名与少量文件头签名识别文件类型
# 3) 使用相对 data 目录的路径输出 JSON / JSONL 结果
#
# What it does NOT do
# 1) 不判断文件是否“可用”或“合规”
# 2) 不阻断任何数据流程
# 3) 不进行内容解析、不做质量评估
#
# Notes
# - 识别结果是“标签”，不是治理结论
# - 若无法识别，则返回 unknown
# ============================================================

# ===========================
# ALIAS_META (comment block)
# ===========================
# alias: identify_file_type
# family: identify_file_type
# role: data_identify
# version: v0002
# status: active
# entry_point: identify_file_type_v0002.py
# input: data_root (dir)
# output: json / jsonl (stdout)
# depends_on: []
# used_by: []

# =====================================
# 制度与职责说明注释区
# =====================================
# - 本脚本仅进行“类型识别”，不进行制度判断或合规判断
# - 本脚本不修改系统对象，不移动文件
# - 本脚本输出使用相对路径，避免环境绑定

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any

# =====================================
# 边界声明与强约束说明
# =====================================
# - 只读取文件头部少量字节用于签名识别
# - 不读取完整内容，不做结构解析
# - 识别策略以“稳定、简单”为优先

READ_HEAD_BYTES_DEFAULT = 64


@dataclass
class IdentifyResult:
    relative_path: str
    size_bytes: int
    ext: str
    file_type: str
    method: str
    signals: Dict[str, Any]


def _safe_ext(p: Path) -> str:
    suf = p.suffix.lower()
    if suf.startswith(".") and len(suf) > 1:
        return suf[1:]
    return ""


def _read_head(p: Path, n: int) -> bytes:
    with p.open("rb") as f:
        return f.read(n)


def _match_magic(head: bytes) -> Optional[Tuple[str, Dict[str, Any]]]:
    if head.startswith(b"%PDF"):
        return "pdf", {"magic": "%PDF"}
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png", {"magic": "png_signature"}
    if head.startswith(b"\xFF\xD8\xFF"):
        return "jpg", {"magic": "jpeg_signature"}
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return "gif", {"magic": "gif_signature"}
    if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06") or head.startswith(b"PK\x07\x08"):
        return "zip", {"magic": "zip_signature"}
    if head.startswith(b"\x1F\x8B"):
        return "gz", {"magic": "gzip_signature"}
    return None


def identify_file(p: Path, root: Path, read_head_bytes: int) -> IdentifyResult:
    ext = _safe_ext(p)
    signals: Dict[str, Any] = {"ext": ext}
    size = p.stat().st_size

    try:
        head = _read_head(p, max(8, min(read_head_bytes, 4096)))
        magic_hit = _match_magic(head)
    except Exception as e:
        magic_hit = None
        signals["read_error"] = repr(e)

    if magic_hit:
        ft, sig = magic_hit
        signals.update(sig)
        if ft == "zip" and ext in {"docx", "xlsx", "pptx"}:
            return IdentifyResult(
                relative_path=str(p.relative_to(root)),
                size_bytes=size,
                ext=ext,
                file_type=ext,
                method="magic+ext",
                signals=signals | {"refined_by_ext": ext},
            )
        return IdentifyResult(
            relative_path=str(p.relative_to(root)),
            size_bytes=size,
            ext=ext,
            file_type=ft,
            method="magic",
            signals=signals,
        )

    if ext:
        return IdentifyResult(
            relative_path=str(p.relative_to(root)),
            size_bytes=size,
            ext=ext,
            file_type=ext,
            method="ext",
            signals=signals,
        )

    return IdentifyResult(
        relative_path=str(p.relative_to(root)),
        size_bytes=size,
        ext=ext,
        file_type="unknown",
        method="unknown",
        signals=signals,
    )


def collect_target_files(data_root: Path) -> List[Path]:
    files: List[Path] = []

    # data/data_raw 目录（递归）
    data_raw = data_root / "data_raw"
    if data_raw.exists() and data_raw.is_dir():
        for p in data_raw.rglob("*"):
            if p.is_file():
                files.append(p)

    return files


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Identify file types recursively in the current project's data/data_raw directory."
    )
    ap.add_argument(
        "--data-root",
        default=Path(__file__).resolve().parent.parent / "data",
        help="Data root directory containing data_raw (default: project_root/data)",
    )
    ap.add_argument("--jsonl", action="store_true", help="Output JSON Lines")
    ap.add_argument("--read-head-bytes", type=int, default=READ_HEAD_BYTES_DEFAULT)
    args = ap.parse_args()

    root = Path(args.data_root).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Invalid data root: {root}")

    results: List[IdentifyResult] = []
    for p in collect_target_files(root):
        results.append(identify_file(p, root, args.read_head_bytes))

    if args.jsonl:
        for r in results:
            print(json.dumps(asdict(r), ensure_ascii=False))
    else:
        print(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
