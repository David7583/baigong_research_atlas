#!/usr/bin/env python3
# ============================================================
# File: scan_alias_meta_v0003.py
# 中文名: ALIAS_META 扫描器
# Version: v0003
# Layer: infrastructures
# Main Layer: action
# Updatable: True
#
# Purpose:
# 扫描项目中多种编程语言的脚本文件，解析其 ALIAS_META 块，
# 产出一份包含全部脚本元数据的 JSONL 文件。
#
# What it does:
# 1. 读取 config，获取扫描范围、排除规则、解析模式
# 2. 递归遍历 scan_root 下所有匹配后缀或文件名的文件
# 3. 按文件类型归一化注释格式并解析 ALIAS_META 结构化字段
# 4. 可选提取 comment header 原文（功能说明 + 制度声明）
# 5. 产出 JSONL 到 staging 待选区
# 6. 提供 --list-dirs 模式供 flow 脚本获取文件夹列表
#
# What it does NOT do:
# 1. 不入库
# 2. 不建表
# 3. 不做交互（交互由 flow_scan_alias_meta 负责）
# 4. 不判断字段内容是否合理
# 5. 不修改任何被扫描的脚本文件
# 6. 不做增量判断（v0003 仍为全量扫描）
#
# Non-v0003 targets (deferred):
# 1. 增量扫描（基于 last_scan_at 时间戳）
# 2. 并行扫描加速
# 3. 字段校验与缺失字段报告
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: scan_alias_meta
# family: scan_alias_meta
# role: scanner
# version: v0003
# status: active
# entry_point: scripts/action/infrastructures/scan_alias_meta_v0003.py
# input:
#   - config file path
#   - exclude dirs list (optional, from flow)
# output:
#   - actioning/scan_alias_meta/scan_alias_meta_output.jsonl
# depends_on: []
# used_by:
#   - register_script_v0002
#   - flow_scan_alias_meta_v0002
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 本脚本是只读扫描器，不修改任何系统对象
# - 本脚本不进行制度判断
# - 本脚本不做交互，交互由 flow 脚本负责
# - 本脚本唯一的写操作是产出 JSONL 和更新 config 时间戳
# ============================================================

from __future__ import annotations

# ============================================================
# Imports
# ============================================================
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ============================================================
# 边界声明与强约束说明
# ============================================================
# - 第三方依赖: PyYAML (已在系统第三方注册表中登记)
# - dry-run 下不得写入任何文件
# - 不修改被扫描的脚本文件

# ============================================================
# 常量与全局配置区
# ============================================================
SCRIPT_NAME = "scan_alias_meta_v0003.py"
SCRIPT_VERSION = "v0003"
MAIN_LAYER = "action"

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"


# ============================================================
# 工具函数区（无副作用）
# ============================================================
def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def dumps_jsonl_line(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False)


def load_config(path: Path) -> Dict[str, Any]:
    """
    使用 PyYAML 加载 config 文件。
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def update_yaml_last_scan(config_path: Path, timestamp: str) -> None:
    """
    更新 config 文件中的 last_scan_at 字段。
    使用 PyYAML 读写以保持结构完整性。
    注意：PyYAML dump 会丢失注释，因此采用行替换策略保留注释。
    如果 config 中不存在 last_scan_at 行，则在末尾追加。
    """
    lines = config_path.read_text(encoding="utf-8").splitlines()
    new_lines = []
    found = False
    for line in lines:
        if line.strip().startswith("last_scan_at:"):
            new_lines.append(f'last_scan_at: "{timestamp}"')
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f'last_scan_at: "{timestamp}"')
    config_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


# ============================================================
# 文件发现
# ============================================================
def discover_files(
    scan_root: Path,
    extensions: List[str],
    exclude_dirs: List[str],
    always_exclude_dirs: List[str],
    always_exclude_files: List[str],
    filenames: Optional[List[str]] = None,
) -> List[Path]:
    """
    递归遍历 scan_root，收集所有匹配后缀的文件。
    跳过排除目录和排除文件。
    """
    all_exclude_dirs = set(exclude_dirs) | set(always_exclude_dirs)
    all_exclude_files = set(always_exclude_files)
    ext_set = {ext.lower() for ext in extensions}
    filename_set = set(filenames or [])
    found: List[Path] = []

    for dirpath, dirnames, filenames in os.walk(scan_root):
        # 原地修改 dirnames 以跳过排除目录
        dirnames[:] = [
            d for d in dirnames
            if d not in all_exclude_dirs
            and os.path.join(dirpath, d) not in all_exclude_dirs
        ]

        for fname in filenames:
            if fname in all_exclude_files:
                continue
            fpath = Path(dirpath) / fname
            if fpath.suffix.lower() in ext_set or fname in filename_set:
                found.append(fpath)

    return sorted(found)


def build_format_maps(config: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    将配置中的语言注释格式整理为后缀映射和精确文件名映射。

    兼容旧配置：如果没有 scan_formats，则 scan_extensions 全部按 # 行注释处理。
    """
    extension_map: Dict[str, Dict[str, Any]] = {}
    filename_map: Dict[str, Dict[str, Any]] = {}
    formats = config.get("scan_formats", [])

    if not formats:
        legacy_profile = {
            "name": "hash_line_comment",
            "line_prefixes": ["#"],
            "case_insensitive": False,
        }
        for ext in config.get("scan_extensions", [".py"]):
            extension_map[str(ext).lower()] = legacy_profile
        return extension_map, filename_map

    for item in formats:
        profile = {
            "name": item.get("name", "unnamed"),
            "line_prefixes": list(item.get("line_prefixes", [])),
            "block_start": item.get("block_start"),
            "block_end": item.get("block_end"),
            "case_insensitive": bool(item.get("case_insensitive", False)),
        }
        if not profile["line_prefixes"] and not profile["block_start"]:
            raise ValueError(f"scan format has no comment marker: {profile['name']}")
        for ext in item.get("extensions", []):
            normalized_ext = str(ext).lower()
            if not normalized_ext.startswith("."):
                raise ValueError(f"scan extension must start with '.': {ext}")
            if normalized_ext in extension_map:
                raise ValueError(f"duplicate scan extension: {normalized_ext}")
            extension_map[normalized_ext] = profile
        for filename in item.get("filenames", []):
            normalized_name = str(filename)
            if normalized_name in filename_map:
                raise ValueError(f"duplicate scan filename: {normalized_name}")
            filename_map[normalized_name] = profile

    return extension_map, filename_map


def resolve_format_profile(
    file_path: Path,
    extension_map: Dict[str, Dict[str, Any]],
    filename_map: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """按精确文件名优先、后缀其次的顺序确定注释格式。"""
    if file_path.name in filename_map:
        return filename_map[file_path.name]
    return extension_map.get(file_path.suffix.lower())


def _strip_line_comment(line: str, prefixes: List[str], case_insensitive: bool) -> Optional[str]:
    """如果一行以某个注释标记开头，返回去掉标记后的正文。"""
    stripped = line.lstrip()
    candidate = stripped.lower() if case_insensitive else stripped
    for prefix in sorted(prefixes, key=len, reverse=True):
        marker = prefix.lower() if case_insensitive else prefix
        if not candidate.startswith(marker):
            continue
        if marker.lower() == "rem" and len(stripped) > len(prefix) and not stripped[len(prefix)].isspace():
            continue
        return stripped[len(prefix):]
    return None


def normalize_comment_lines(text: str, profile: Dict[str, Any]) -> List[str]:
    """
    将不同语言的注释行按行归一化为 # 注释，供既有解析器复用。

    归一化保持行数不变，因此解析位置仍可映射回源文件原始行。
    """
    source_lines = text.splitlines()
    prefixes = list(profile.get("line_prefixes", []))
    case_insensitive = bool(profile.get("case_insensitive", False))
    block_start = profile.get("block_start")
    block_end = profile.get("block_end")
    in_block = False
    normalized: List[str] = []

    for line in source_lines:
        if block_start:
            stripped = line.strip()
            block_text: Optional[str] = None
            if in_block:
                block_text = stripped
            elif block_start in stripped:
                before, block_text = stripped.split(block_start, 1)
                if before.strip():
                    normalized.append(line)
                    continue
                in_block = True

            if block_text is not None:
                if block_end and block_end in block_text:
                    block_text, after = block_text.split(block_end, 1)
                    in_block = False
                    if after.strip():
                        normalized.append(line)
                        continue
                normalized.append("#" + block_text.rstrip())
                continue

        comment_text = _strip_line_comment(line, prefixes, case_insensitive)
        if comment_text is not None:
            normalized.append("#" + comment_text.rstrip())
        else:
            normalized.append(line)

    return normalized


def list_subdirs(scan_root: Path, always_exclude_dirs: List[str]) -> List[str]:
    """
    列出 scan_root 下的一级子文件夹名称。
    排除 always_exclude_dirs 中的目录。
    供 flow 脚本交互使用。
    """
    if not scan_root.is_dir():
        return []
    exclude_set = set(always_exclude_dirs)
    dirs = []
    for item in sorted(scan_root.iterdir()):
        if item.is_dir() and item.name not in exclude_set:
            dirs.append(item.name)
    return dirs


# ============================================================
# ALIAS_META 解析
# ============================================================
def parse_alias_meta(
    lines: List[str],
    keyword: str,
    end_conditions: List[Dict[str, str]],
    field_pattern: str,
    list_item_pattern: str,
    compiled: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], int, int]:
    """
    从文件的行列表中定位并解析 ALIAS_META 块。

    返回:
        (parsed_fields, start_line_index, end_line_index)
        如果未找到 ALIAS_META 则返回 (None, -1, -1)

    compiled: 可选的预编译正则对象字典，避免重复编译。
    """
    if compiled:
        field_re = compiled["field_re"]
        list_item_re = compiled["list_item_re"]
        end_res = compiled["end_res"]
    else:
        field_re = re.compile(field_pattern)
        list_item_re = re.compile(list_item_pattern)
        end_res = [(ec["type"], re.compile(ec["pattern"])) for ec in end_conditions]

    # 第一步：找到 ALIAS_META 块的起始行
    # 要求：去掉 # 和等号装饰后，核心内容必须是 ALIAS_META（允许括号备注）
    # 匹配: "# ALIAS_META" / "# ALIAS_META (comment block)" / "# ====\n# ALIAS_META\n# ===="
    # 不匹配: "# 中文名: ALIAS_META 扫描器" / "# 解析 ALIAS_META 块"
    start_idx = -1
    alias_meta_line_re = re.compile(
        r"^#\s*ALIAS_META\s*(\(.*\))?\s*$"
    )
    for i, line in enumerate(lines):
        if alias_meta_line_re.match(line.strip()):
            start_idx = i
            break

    if start_idx == -1:
        return None, -1, -1

    # 跳过起始行本身和紧跟其后的分隔线
    parse_begin = start_idx + 1
    for i in range(parse_begin, len(lines)):
        stripped = lines[i].strip()
        # 跳过分隔线和包含 keyword 的装饰行
        if re.match(r"^#\s*={3,}\s*$", stripped):
            parse_begin = i + 1
            continue
        break

    # 第二步：逐行解析字段，直到命中结束条件
    fields: Dict[str, Any] = {}
    current_key: Optional[str] = None
    end_idx = len(lines)

    for i in range(parse_begin, len(lines)):
        line = lines[i]
        raw = line.rstrip()

        # 检查结束条件
        hit_end = False
        for etype, ere in end_res:
            if ere.match(raw):
                # 分隔线类型：如果紧跟在起始后面则跳过，否则结束
                if etype == "separator" and i == parse_begin:
                    continue
                hit_end = True
                break
        if hit_end:
            end_idx = i
            break

        # 尝试匹配字段行 (# key: value)
        fm = field_re.match(raw)
        if fm:
            key = fm.group(1)
            val = fm.group(2).strip()
            if val == "" or val is None:
                # 可能后面跟着列表项
                fields[key] = []
                current_key = key
            elif val == "[]":
                fields[key] = []
                current_key = None
            else:
                # 去掉可能的尾部注释
                fields[key] = val
                current_key = key if val == "" else None
            continue

        # 尝试匹配列表项 (#   - item)
        lm = list_item_re.match(raw)
        if lm:
            item = lm.group(1).strip()
            if current_key and isinstance(fields.get(current_key), list):
                fields[current_key].append(item)
            continue

        # 既不是字段也不是列表项
        # 如果是注释行但不属于 ALIAS_META 的结构，视为块结束
        # 这防止制度声明等后续注释块被错误地吃进最后一个列表字段
        if raw.strip().startswith("#"):
            end_idx = i
            break

    return fields, start_idx, end_idx


# ============================================================
# Comment Header 原文提取
# ============================================================
def extract_header_raw(
    lines: List[str],
    alias_meta_start: int,
    alias_meta_end: int,
) -> Tuple[str, str]:
    """
    提取 ALIAS_META 块之外的 comment header 原文。

    ALIAS_META 之前的注释块 → header_purpose_raw（功能说明）
    ALIAS_META 之后的注释块 → header_constraint_raw（制度声明）

    只提取连续的注释行块，遇到代码行停止。
    """
    # 提取 ALIAS_META 之前的注释块（从文件开头到 ALIAS_META 起始之前）
    purpose_lines: List[str] = []
    if alias_meta_start > 0:
        for i in range(0, alias_meta_start):
            line = lines[i].rstrip()
            if line.strip().startswith("#") or line.strip() == "":
                purpose_lines.append(line)
            elif line.strip().startswith("#!/"):
                # shebang 行，保留
                purpose_lines.append(line)
            else:
                # 遇到代码行，停止
                break

    # 提取 ALIAS_META 之后的注释块
    constraint_lines: List[str] = []
    if alias_meta_end >= 0 and alias_meta_end < len(lines):
        # 从 ALIAS_META 结束行的下一行开始
        search_start = alias_meta_end + 1
        for i in range(search_start, len(lines)):
            line = lines[i].rstrip()
            if line.strip().startswith("#") or line.strip() == "":
                constraint_lines.append(line)
            else:
                # 遇到代码行（非注释、非空），停止
                break

    purpose_raw = "\n".join(purpose_lines).strip()
    constraint_raw = "\n".join(constraint_lines).strip()

    return purpose_raw, constraint_raw


def extract_header_raw_from_source(
    source_lines: List[str],
    normalized_lines: List[str],
    alias_meta_start: int,
    alias_meta_end: int,
) -> Tuple[str, str]:
    """使用归一化行判定注释范围，同时保留源文件中的原始注释符号。"""
    purpose_lines: List[str] = []
    if alias_meta_start > 0:
        for i in range(0, alias_meta_start):
            normalized = normalized_lines[i].rstrip()
            if normalized.strip().startswith("#") or normalized.strip() == "":
                purpose_lines.append(source_lines[i].rstrip())
            else:
                break

    constraint_lines: List[str] = []
    if alias_meta_end >= 0 and alias_meta_end < len(normalized_lines):
        search_start = alias_meta_end + 1
        for i in range(search_start, len(normalized_lines)):
            normalized = normalized_lines[i].rstrip()
            if normalized.strip().startswith("#") or normalized.strip() == "":
                constraint_lines.append(source_lines[i].rstrip())
            else:
                break

    return (
        "\n".join(purpose_lines).strip(),
        "\n".join(constraint_lines).strip(),
    )


# ============================================================
# 单文件处理
# ============================================================
def process_file(
    file_path: Path,
    scan_root: Path,
    parsing_config: Dict[str, Any],
    do_extract_raw: bool,
    format_profile: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    解析单个文件，返回一条元数据记录。
    如果文件不包含 ALIAS_META 则返回 None。
    """
    try:
        text = file_path.read_text(encoding="utf-8-sig")
    except Exception as e:
        return {
            "_scan_error": f"read_failed: {type(e).__name__}: {e}",
            "file_path": str(file_path.relative_to(scan_root)),
            "scanned_at": utc_now(),
        }

    source_lines = text.splitlines()
    profile = format_profile or {
        "name": "hash_line_comment",
        "line_prefixes": ["#"],
        "case_insensitive": False,
    }
    lines = normalize_comment_lines(text, profile)

    keyword = parsing_config.get("keyword", "ALIAS_META")
    end_conditions = parsing_config.get("end_conditions", [])
    field_pattern = parsing_config.get("field_pattern", r"^#\s*([a-z_]+)\s*:\s*(.*)$")
    list_item_pattern = parsing_config.get("list_item_pattern", r"^#\s*-\s+(.+)$")
    compiled = parsing_config.get("_compiled", None)

    fields, start_idx, end_idx = parse_alias_meta(
        lines=lines,
        keyword=keyword,
        end_conditions=end_conditions,
        field_pattern=field_pattern,
        list_item_pattern=list_item_pattern,
        compiled=compiled,
    )

    if fields is None or not fields or "alias" not in fields:
        return None

    record: Dict[str, Any] = {}
    record.update(fields)
    record["file_path"] = str(file_path.relative_to(scan_root))
    record["scanned_at"] = utc_now()

    if do_extract_raw:
        purpose_raw, constraint_raw = extract_header_raw_from_source(
            source_lines,
            lines,
            start_idx,
            end_idx,
        )
        record["header_purpose_raw"] = purpose_raw
        record["header_constraint_raw"] = constraint_raw

    return record


# ============================================================
# 核心业务逻辑：全量扫描
# ============================================================
def run_scan(
    config_path: Path,
    exclude_dirs_override: Optional[List[str]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    执行全量扫描。

    返回扫描摘要。
    """
    cfg = load_config(config_path)

    scan_root = Path(cfg.get("scan_root", "scripts"))
    extension_map, filename_map = build_format_maps(cfg)
    extensions = sorted(extension_map.keys())
    filenames = sorted(filename_map.keys())
    exclude_dirs = exclude_dirs_override if exclude_dirs_override is not None else cfg.get("exclude_dirs", [])
    always_exclude_dirs = cfg.get("always_exclude_dirs", [])
    always_exclude_files = cfg.get("always_exclude_files", [])
    parsing_config = cfg.get("alias_meta_parsing", {})
    output_path = Path(cfg.get("output_jsonl_path", "actioning/scan_alias_meta/scan_alias_meta_output.jsonl"))
    do_extract_raw = cfg.get("extract_header_raw", True)

    # 预编译正则表达式，避免每个文件重复编译
    field_pattern_str = parsing_config.get("field_pattern", r"^#\s*([a-z_]+)\s*:\s*(.*)$")
    list_item_pattern_str = parsing_config.get("list_item_pattern", r"^#\s*-\s+(.+)$")
    compiled_patterns = {
        "field_re": re.compile(field_pattern_str),
        "list_item_re": re.compile(list_item_pattern_str),
        "end_res": [
            (ec["type"], re.compile(ec["pattern"]))
            for ec in parsing_config.get("end_conditions", [])
        ],
    }
    parsing_config["_compiled"] = compiled_patterns

    # 确保 scan_root 存在
    if not scan_root.is_dir():
        return {
            "status": "error",
            "error": f"scan_root not found: {scan_root}",
        }

    # 发现文件
    files = discover_files(
        scan_root=scan_root,
        extensions=extensions,
        exclude_dirs=exclude_dirs,
        always_exclude_dirs=always_exclude_dirs,
        always_exclude_files=always_exclude_files,
        filenames=filenames,
    )

    # 解析每个文件
    records: List[Dict[str, Any]] = []
    skipped: List[str] = []
    errors: List[str] = []

    for fpath in files:
        format_profile = resolve_format_profile(fpath, extension_map, filename_map)
        if format_profile is None:
            skipped.append(str(fpath.relative_to(scan_root)))
            continue
        result = process_file(
            file_path=fpath,
            scan_root=scan_root,
            parsing_config=parsing_config,
            do_extract_raw=do_extract_raw,
            format_profile=format_profile,
        )
        if result is None:
            skipped.append(str(fpath.relative_to(scan_root)))
        elif "_scan_error" in result:
            errors.append(result["_scan_error"])
            records.append(result)
        else:
            records.append(result)

    # 收集所有出现过的字段名（字段并集）
    all_fields: set = set()
    for rec in records:
        if "_scan_error" not in rec:
            all_fields.update(rec.keys())

    scan_timestamp = utc_now()

    # 写出 JSONL
    if not dry_run and records:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(dumps_jsonl_line(rec) + "\n")

        # 更新 config 时间戳
        update_yaml_last_scan(config_path, scan_timestamp)

    # 扫描摘要
    summary = {
        "status": "ok",
        "scan_timestamp": scan_timestamp,
        "files_discovered": len(files),
        "files_parsed": len(records) - len(errors),
        "files_skipped_no_alias_meta": len(skipped),
        "files_skipped_list": sorted(skipped),
        "files_with_errors": len(errors),
        "total_fields_discovered": sorted(all_fields),
        "output_path": str(output_path),
        "dry_run": dry_run,
    }

    return summary


# ============================================================
# CLI 模式：列出子目录
# ============================================================
def run_list_dirs(config_path: Path) -> Dict[str, Any]:
    """
    列出 scan_root 下的一级子文件夹，供 flow 脚本交互使用。
    """
    cfg = load_config(config_path)
    scan_root = Path(cfg.get("scan_root", "scripts"))
    always_exclude = cfg.get("always_exclude_dirs", [])

    dirs = list_subdirs(scan_root, always_exclude)

    return {
        "status": "ok",
        "scan_root": str(scan_root),
        "subdirs": dirs,
        "count": len(dirs),
    }


# ============================================================
# CLI / main 接口区
# ============================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan all scripts for ALIAS_META headers and produce JSONL."
    )
    parser.add_argument(
        "--config",
        default="config/action/config/scan_alias_meta_config_v0002.yml",
        help="Path to scan_alias_meta_config_v0002.yml",
    )
    parser.add_argument(
        "--exclude-dirs",
        default=None,
        help="Comma-separated list of directories to exclude (overrides config)",
    )
    parser.add_argument(
        "--list-dirs",
        action="store_true",
        help="List subdirectories under scan_root and exit (for flow interaction)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and report without writing files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)

    if not config_path.exists():
        print(dumps_json({"status": "error", "error": f"config not found: {config_path}"}))
        return 1

    # 列出子目录模式
    if args.list_dirs:
        result = run_list_dirs(config_path)
        print(dumps_json(result))
        return 0

    # 正式扫描模式
    exclude_override = None
    if args.exclude_dirs is not None:
        exclude_override = [d.strip() for d in args.exclude_dirs.split(",") if d.strip()]

    summary = run_scan(
        config_path=config_path,
        exclude_dirs_override=exclude_override,
        dry_run=bool(args.dry_run),
    )

    print(dumps_json(summary))

    if summary.get("status") == "error":
        return 1

    return 0


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    raise SystemExit(main())
