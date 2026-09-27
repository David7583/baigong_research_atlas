#!/usr/bin/env python3
# ============================================================
# filename: analyze_structural_hierarchy_v0004.py
# 中文名: 结构层级树提取脚本
# version: v0004
# layer: execution
# main_layer: action
# 可更新: True
#
# 职责说明:
# - 从 normalize 的 JSONL 输出中提取每个对话的消息树层级结构
# - 在每一行原有字段基础上追加 hierarchy 相关字段
# - 1:1 透传，只加不减不改定义
#
# 异常行处理策略:
# - JSON 解析失败的行采用"保留占位 + 原文存储"策略
# - 输出为 {__hierarchy_parse_failed__: true, raw_text_content: <原文>}
# - hierarchy 字段不追加到解析失败行（因无法确定其 path 归属）
# - 解析失败行计入 rows_written，保证输出行数与输入行数一致
#
# 本脚本做什么:
# 1. 逐文件处理 normalize 输出（支持单文件和目录模式）
# 2. 第一遍扫描：提取 conversation_id、title、mapping node 的
#    parent/children/role 信息，重建消息树，计算 tree_depth
# 3. 第二遍扫描：逐行读入，原样保留所有字段，追加 hierarchy 字段后写出
# 4. 每个输入文件产出一个输出文件，行数完全一致
#
# 本脚本不做什么:
# 1. 不删除或修改输入行的任何已有字段
# 2. 不进行过滤、筛选、去重或语义判断
# 3. 不聚合跨文件数据
# 4. 不写入任何数据库
# 5. 不调用其他脚本
#
# 追加字段说明:
# - hierarchy_conversation_id: 该行所属对话的 conversation_id
# - hierarchy_title: 该对话的 title（非 mapping 行和 mapping 行均追加）
# - hierarchy_node_id: 该行所属的 mapping node id（非 mapping 行为 null）
# - hierarchy_parent_node_id: 该 node 的父节点 id（root 和非 mapping 行为 null）
# - hierarchy_role: 该 node 的 message author role（无 message 的为 null）
# - hierarchy_tree_depth: 该 node 在消息树中的深度（root=0，非 mapping 行为 null）
# - hierarchy_has_message: 该 node 是否有 message（非 mapping 行为 null）
# - hierarchy_is_leaf: 该 node 是否为叶子节点（非 mapping 行为 null）
#
# v0003 说明:
# - 版本号对齐 analyze_structural_cooccurrence_v0004 /
#   analyze_structural_adjacency_v0004
# - 输入为 normalize_text_units_v0002 的原始输出
# - 输出格式遵循"只加不减"原则，与 normalize_unit_variants_v0003
#   的透传模式一致
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: analyze_structural_hierarchy
# family: analyze_structural_hierarchy
# role: execution
# version: v0004
# status: active
# entry_point: scripts/action/pipelines/analyze_structural_hierarchy_v0004.py
# input:
#   - normalize_text_units_v0002 输出目录（per-conversation JSONL）
# output:
#   - per-conversation hierarchy JSONL（1:1 透传 + 追加字段）
#   - run_meta.json（单文件模式）或 per-file run_meta（目录模式）
# depends_on:
#   - normalize_text_units_v0002.py
# used_by:
#   - sync_hierarchy_to_graph_v0002.py
# ============================================================


# ============================================================
# Imports
# ============================================================
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from collections import defaultdict, deque, Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# ============================================================
# 常量
# ============================================================
SCRIPT_NAME = "analyze_structural_hierarchy_v0004.py"
SCRIPT_VERSION = "v0004"
DEFAULT_ENCODING = "utf-8"
PROGRESS_INTERVAL = 500  # 每处理 N 个文件打印一次进度
ARCHIVE_DIRNAME = "archive"

# path 匹配模式：提取 mapping 下的 node_id
# 匹配: /conversation/mapping/{node_id}/...
_MAPPING_PREFIX = "/conversation/mapping/"

# UUID 模式（ChatGPT mapping node id）+ client-created-root
_NODE_ID_PATTERN = re.compile(
    r"^/conversation/mapping/([^/]+)/"
)
_NODE_ID_PATTERN_EXACT = re.compile(
    r"^/conversation/mapping/([^/]+)$"
)


# ============================================================
# 工具函数（无副作用）
# ============================================================
def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _glob_jsonl_files(dir_path: Path) -> List[Path]:
    return sorted(dir_path.glob("*.jsonl"))


def _read_jsonl_stream(path: Path, encoding: str = DEFAULT_ENCODING) -> Iterable[str]:
    """逐行 yield 原始字符串（不解析 JSON），减少内存压力。"""
    with path.open("r", encoding=encoding) as f:
        for line in f:
            stripped = line.rstrip("\n\r")
            if stripped:
                yield stripped


def _parse_json_line(raw: str) -> Optional[Dict[str, Any]]:
    """安全解析单行 JSON，失败返回 None。"""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return None


def _extract_node_id_from_path(path: str) -> Optional[str]:
    """从 path 中提取 mapping node_id。

    匹配:
      /conversation/mapping/{node_id}/...
      /conversation/mapping/{node_id}   (极少见但合法)

    非 mapping 路径返回 None。
    """
    if not path.startswith(_MAPPING_PREFIX):
        return None

    m = _NODE_ID_PATTERN.match(path)
    if m:
        return m.group(1)

    # 极端情况: path == /conversation/mapping/{node_id}（无尾部斜杠）
    m2 = _NODE_ID_PATTERN_EXACT.match(path)
    if m2:
        return m2.group(1)

    return None


def _extract_path_suffix(path: str, node_id: str) -> str:
    """提取 mapping node_id 之后的路径后缀。

    例: /conversation/mapping/xxx-yyy/message/author/role -> message/author/role
    """
    prefix = f"/conversation/mapping/{node_id}/"
    if path.startswith(prefix):
        return path[len(prefix):]
    # path == /conversation/mapping/{node_id}
    return ""


def _archive_if_exists(
    file_path: Path, archive_dir: Path, run_id: str
) -> Optional[str]:
    if not file_path.exists():
        return None
    _safe_mkdir(archive_dir)
    archived_name = f"{file_path.stem}__revoked__{run_id}{file_path.suffix}"
    dst = archive_dir / archived_name
    shutil.move(str(file_path), str(dst))
    return str(dst)


# ============================================================
# Pass 1: 骨架提取
# ============================================================
class ConversationSkeleton:
    """从一个 normalize JSONL 文件中提取的对话树骨架。

    内存开销说明:
    - 只存储 node_id -> 属性映射，不存储 unit 内容
    - 一个典型对话约 30-700 个 mapping node，每个 node 几百字节
    - 即使极端大对话（数千 node），骨架也只占数 MB
    - unit 行（占 80%+ 的行数）在 pass 1 中只提取 node_id，不存储内容
    """

    __slots__ = (
        "conversation_id",
        "title",
        "parent_map",        # node_id -> parent_node_id
        "children_map",      # node_id -> [child_node_id, ...]
        "role_map",          # node_id -> role string
        "has_message_set",   # set of node_ids that have message
        "all_node_ids",      # set of all observed node_ids
        "tree_depth_map",    # node_id -> depth (computed after scan)
        "leaf_set",          # set of leaf node_ids
        "orphan_node_ids",   # list of orphan node_ids (computed after scan)
        "dangling_parent_count",  # nodes whose parent points to non-existent node
    )

    def __init__(self) -> None:
        self.conversation_id: Optional[str] = None
        self.title: Optional[str] = None
        self.parent_map: Dict[str, str] = {}
        self.children_map: Dict[str, List[str]] = defaultdict(list)
        self.role_map: Dict[str, str] = {}
        self.has_message_set: Set[str] = set()
        self.all_node_ids: Set[str] = set()
        self.tree_depth_map: Dict[str, int] = {}
        self.leaf_set: Set[str] = set()
        self.orphan_node_ids: List[str] = []
        self.dangling_parent_count: int = 0

    def ingest_line(self, obj: Dict[str, Any]) -> None:
        """从一行 parsed JSON 中提取骨架信息。"""
        path = obj.get("path", "")
        unit_text = obj.get("unit_text", "")

        # 非 mapping 行：提取 conversation 级元数据
        if not path.startswith(_MAPPING_PREFIX):
            if path == "/_meta/conversation_id" and self.conversation_id is None:
                self.conversation_id = unit_text
            elif path == "/conversation/conversation_id" and self.conversation_id is None:
                self.conversation_id = unit_text
            elif path == "/conversation/title":
                self.title = unit_text
            return

        # mapping 行：提取 node 属性
        node_id = _extract_node_id_from_path(path)
        if node_id is None:
            return

        self.all_node_ids.add(node_id)
        suffix = _extract_path_suffix(path, node_id)

        if suffix == "parent":
            # 防御性检查：空串或纯空白不作为有效 parent
            if unit_text and unit_text.strip():
                self.parent_map[node_id] = unit_text.strip()
        elif suffix.startswith("children/") or suffix == "children":
            # 兼容 children/* 和 children/0, children/1 等变体
            if unit_text and unit_text.strip():
                self.children_map[node_id].append(unit_text.strip())
        elif suffix == "message/author/role":
            self.role_map[node_id] = unit_text
            self.has_message_set.add(node_id)
        elif suffix.startswith("message/"):
            # 任何 message/ 下的字段都说明该 node 有 message
            self.has_message_set.add(node_id)

    def compute_tree(self) -> None:
        """根据 parent_map 计算 tree_depth 和 leaf_set。

        使用迭代式 BFS + deque，不使用递归，避免深树栈溢出。
        deque.popleft() 为 O(1)，避免 list.pop(0) 的 O(n) 性能退化。
        """
        # 找 root: 在 all_node_ids 中但不在 parent_map 中的
        # parent_map 只包含有效 parent（空值已在 ingest_line 中过滤）
        roots = self.all_node_ids - set(self.parent_map.keys())

        # 检测 dangling parent: parent 指向的 node_id 不在 all_node_ids 中
        self.dangling_parent_count = 0
        for node_id, parent_id in self.parent_map.items():
            if parent_id not in self.all_node_ids:
                self.dangling_parent_count += 1

        # BFS 计算深度
        queue: deque = deque((r, 0) for r in sorted(roots))
        visited: Set[str] = set()

        while queue:
            node_id, depth = queue.popleft()
            if node_id in visited:
                continue
            visited.add(node_id)
            self.tree_depth_map[node_id] = depth

            children = self.children_map.get(node_id, [])
            for child_id in children:
                if child_id not in visited:
                    queue.append((child_id, depth + 1))

        # 未被 BFS 覆盖的孤立节点（理论上不应存在，防御性处理）
        # 记录孤立节点 id 供 run_meta 审计
        self.orphan_node_ids = []
        for node_id in self.all_node_ids:
            if node_id not in self.tree_depth_map:
                self.tree_depth_map[node_id] = -1
                self.orphan_node_ids.append(node_id)

        # leaf set: 没有 children 或 children 为空的节点
        for node_id in self.all_node_ids:
            if not self.children_map.get(node_id):
                self.leaf_set.add(node_id)

    def get_hierarchy_fields(self, node_id: Optional[str]) -> Dict[str, Any]:
        """为给定 node_id 生成追加字段。非 mapping 行传 None。"""
        base = {
            "hierarchy_conversation_id": self.conversation_id,
            "hierarchy_title": self.title,
        }

        if node_id is None:
            base["hierarchy_node_id"] = None
            base["hierarchy_parent_node_id"] = None
            base["hierarchy_role"] = None
            base["hierarchy_tree_depth"] = None
            base["hierarchy_has_message"] = None
            base["hierarchy_is_leaf"] = None
            return base

        base["hierarchy_node_id"] = node_id
        base["hierarchy_parent_node_id"] = self.parent_map.get(node_id)
        base["hierarchy_role"] = self.role_map.get(node_id)
        base["hierarchy_tree_depth"] = self.tree_depth_map.get(node_id)
        base["hierarchy_has_message"] = node_id in self.has_message_set
        base["hierarchy_is_leaf"] = node_id in self.leaf_set

        return base


# ============================================================
# Pass 1 执行
# ============================================================
def _build_skeleton(
    input_path: Path, encoding: str = DEFAULT_ENCODING
) -> ConversationSkeleton:
    """第一遍扫描：提取骨架信息。

    只解析需要的字段（path, unit_text），不存储完整行。
    """
    skeleton = ConversationSkeleton()

    for raw_line in _read_jsonl_stream(input_path, encoding):
        obj = _parse_json_line(raw_line)
        if obj is None:
            continue
        skeleton.ingest_line(obj)

    skeleton.compute_tree()
    return skeleton


# ============================================================
# Pass 2: 逐行追加写出
# ============================================================
def _process_single_file(
    input_path: Path,
    output_path: Path,
    encoding: str = DEFAULT_ENCODING,
) -> Dict[str, Any]:
    """处理单个 JSONL 文件：两遍扫描 + 追加写出。

    返回 counters 字典。
    """
    counters = {
        "rows_read": 0,
        "rows_written": 0,
        "rows_parse_failed": 0,
        "rows_mapping": 0,
        "rows_non_mapping": 0,
        "node_count": 0,
        "tree_depth_max": 0,
        "has_orphan_nodes": False,
        "orphan_node_count": 0,
        "dangling_parent_count": 0,
        "has_message_without_role_count": 0,
    }

    # Pass 1: 骨架
    skeleton = _build_skeleton(input_path, encoding)

    counters["node_count"] = len(skeleton.all_node_ids)
    if skeleton.tree_depth_map:
        counters["tree_depth_max"] = max(skeleton.tree_depth_map.values())
    counters["orphan_node_count"] = len(skeleton.orphan_node_ids)
    counters["has_orphan_nodes"] = counters["orphan_node_count"] > 0
    counters["dangling_parent_count"] = skeleton.dangling_parent_count

    # 统计 has_message 但无 role 的不对称状态
    for nid in skeleton.has_message_set:
        if nid not in skeleton.role_map:
            counters["has_message_without_role_count"] += 1

    # Pass 2: 逐行追加
    _safe_mkdir(output_path.parent)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")

    with tmp_path.open("w", encoding=encoding, newline="\n") as out_f:
        for raw_line in _read_jsonl_stream(input_path, encoding):
            counters["rows_read"] += 1

            obj = _parse_json_line(raw_line)
            if obj is None:
                counters["rows_parse_failed"] += 1
                # 将无法解析的原始文本包装成合法 JSON，保留现场供溯源
                error_record = {
                    "__hierarchy_parse_failed__": True,
                    "raw_text_content": raw_line,
                }
                out_f.write(json.dumps(error_record, ensure_ascii=False) + "\n")
                counters["rows_written"] += 1
                continue

            path = obj.get("path", "")
            node_id = _extract_node_id_from_path(path)

            if node_id is not None:
                counters["rows_mapping"] += 1
            else:
                counters["rows_non_mapping"] += 1

            # 追加 hierarchy 字段
            hierarchy_fields = skeleton.get_hierarchy_fields(node_id)
            obj.update(hierarchy_fields)

            out_f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            counters["rows_written"] += 1

    # 原子替换
    os.replace(str(tmp_path), str(output_path))

    return counters


# ============================================================
# CLI
# ============================================================
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Extract message tree hierarchy from normalize JSONL output. "
            "v0003: 1:1 pass-through with appended hierarchy fields."
        )
    )

    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input", default=None,
        help="Single input JSONL file path.",
    )
    input_group.add_argument(
        "--input-dir", default=None,
        help="Directory containing normalize JSONL files.",
    )

    output_group = p.add_mutually_exclusive_group()
    output_group.add_argument(
        "--output",
        default="actioning/pipelines/analyze_structural_hierarchy_action/structural_hierarchy.jsonl",
        help="Output JSONL path (single-file mode).",
    )
    output_group.add_argument(
        "--output-dir",
        default="actioning/pipelines/analyze_structural_hierarchy_action",
        help="Output directory (directory mode).",
    )

    p.add_argument(
        "--run-meta",
        default="actioning/pipelines/analyze_structural_hierarchy_action/run_meta.json",
        help="Run meta JSON output path (single-file mode).",
    )
    p.add_argument(
        "--encoding", default=DEFAULT_ENCODING,
        help=f"File encoding (default: {DEFAULT_ENCODING}).",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Dry run: scan and report without writing output files.",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Print progress to stderr.",
    )

    return p


def _run_single_file_mode(args: argparse.Namespace) -> None:
    """单文件模式。"""
    input_path = Path(args.input)
    if not input_path.exists():
        print(
            json.dumps({"status": "error", "error": f"Input not found: {input_path}"},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        sys.exit(1)

    if not args.output:
        print(
            json.dumps({"status": "error", "error": "Single-file mode requires --output."},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        sys.exit(1)

    output_path = Path(args.output)
    started_at = _utc_now_iso()

    if args.verbose:
        print(f"[HIERARCHY] Single file: {input_path.name}", file=sys.stderr)

    if args.dry_run:
        skeleton = _build_skeleton(input_path, args.encoding)
        meta = {
            "status": "ok",
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "started_at": started_at,
            "generated_at": _utc_now_iso(),
            "dry_run": True,
            "input": str(input_path),
            "skeleton": {
                "conversation_id": skeleton.conversation_id,
                "title": skeleton.title,
                "node_count": len(skeleton.all_node_ids),
                "tree_depth_max": max(skeleton.tree_depth_map.values())
                if skeleton.tree_depth_map
                else 0,
                "roles": dict(Counter(skeleton.role_map.values())),
                "orphan_node_count": len(skeleton.orphan_node_ids),
            },
        }
        print(json.dumps(meta, ensure_ascii=False, indent=2))
        return

    counters = _process_single_file(input_path, output_path, args.encoding)

    meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": _utc_now_iso(),
        "input": str(input_path),
        "output": str(output_path),
        "counters": counters,
    }

    run_meta_path = (
        Path(args.run_meta) if args.run_meta
        else output_path.parent / "run_meta.json"
    )
    _safe_mkdir(run_meta_path.parent)
    with run_meta_path.open("w", encoding=args.encoding) as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if args.verbose:
        print(
            f"[HIERARCHY] Done: {counters['rows_written']} rows, "
            f"{counters['node_count']} nodes, "
            f"depth={counters['tree_depth_max']}",
            file=sys.stderr,
        )

    print(json.dumps({
        "status": "ok",
        "output": str(output_path),
        "run_meta": str(run_meta_path),
        "counters": counters,
    }, ensure_ascii=False, indent=2))


def _run_directory_mode(args: argparse.Namespace) -> None:
    """目录模式：批量处理。"""
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        print(
            json.dumps({"status": "error", "error": f"Input dir not found: {input_dir}"},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        sys.exit(1)

    if not args.output_dir:
        print(
            json.dumps({"status": "error", "error": "Directory mode requires --output-dir."},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        sys.exit(1)

    output_dir = Path(args.output_dir)
    _safe_mkdir(output_dir)

    run_meta_dir = output_dir / "run_meta"
    _safe_mkdir(run_meta_dir)

    archive_dir = output_dir / ARCHIVE_DIRNAME
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    input_files = _glob_jsonl_files(input_dir)
    if not input_files:
        print(
            json.dumps({"status": "error", "error": f"No .jsonl files in: {input_dir}"},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        sys.exit(1)

    started_at = _utc_now_iso()

    if args.verbose:
        print(
            f"[HIERARCHY] Directory mode: {len(input_files)} files in {input_dir}",
            file=sys.stderr,
        )

    # 汇总统计
    total_counters = {
        "files_processed": 0,
        "files_skipped": 0,
        "files_error": 0,
        "total_rows_read": 0,
        "total_rows_written": 0,
        "total_nodes": 0,
        "max_tree_depth": 0,
    }

    file_results: List[Dict[str, Any]] = []

    for file_idx, fpath in enumerate(input_files):
        stem = fpath.stem
        out_path = output_dir / f"{stem}_hierarchy.jsonl"
        meta_path = run_meta_dir / f"{stem}_hierarchy_run_meta.json"

        if args.dry_run:
            try:
                skeleton = _build_skeleton(fpath, args.encoding)
                result = {
                    "file": fpath.name,
                    "status": "ok",
                    "dry_run": True,
                    "conversation_id": skeleton.conversation_id,
                    "node_count": len(skeleton.all_node_ids),
                    "tree_depth_max": max(skeleton.tree_depth_map.values())
                    if skeleton.tree_depth_map
                    else 0,
                }
                total_counters["files_processed"] += 1
                total_counters["total_nodes"] += len(skeleton.all_node_ids)
            except Exception as e:
                result = {"file": fpath.name, "status": "error", "error": str(e)}
                total_counters["files_error"] += 1
            file_results.append(result)
            continue

        try:
            # 归档已有输出
            _archive_if_exists(out_path, archive_dir, run_id)

            counters = _process_single_file(fpath, out_path, args.encoding)

            # per-file meta
            file_meta = {
                "status": "ok",
                "script_name": SCRIPT_NAME,
                "script_version": SCRIPT_VERSION,
                "input": str(fpath),
                "output": str(out_path),
                "counters": counters,
            }
            with meta_path.open("w", encoding=args.encoding) as mf:
                json.dump(file_meta, mf, ensure_ascii=False, indent=2)

            total_counters["files_processed"] += 1
            total_counters["total_rows_read"] += counters["rows_read"]
            total_counters["total_rows_written"] += counters["rows_written"]
            total_counters["total_nodes"] += counters["node_count"]
            total_counters["max_tree_depth"] = max(
                total_counters["max_tree_depth"],
                counters["tree_depth_max"],
            )

            file_results.append({
                "file": fpath.name,
                "status": "ok",
                "rows": counters["rows_written"],
                "nodes": counters["node_count"],
                "depth": counters["tree_depth_max"],
            })

        except Exception as e:
            total_counters["files_error"] += 1
            file_results.append({
                "file": fpath.name,
                "status": "error",
                "error": str(e),
            })

        if args.verbose and (file_idx + 1) % PROGRESS_INTERVAL == 0:
            print(
                f"[HIERARCHY] Progress: {file_idx + 1}/{len(input_files)} files",
                file=sys.stderr,
            )

    # 全局 run_meta
    global_meta = {
        "status": "ok",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "generated_at": _utc_now_iso(),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "dry_run": args.dry_run,
        "total_files": len(input_files),
        "counters": total_counters,
        "file_results": file_results,
        "known_limitations": [
            "v0004 reads normalize_text_units_v0002 output.",
            "v0004 does not carry timestamp fields (pending timestamp annotation script).",
            "v0004 appends hierarchy fields only; does not modify existing fields.",
            "v0004 tree_depth = -1 indicates orphan node (defensive, should not occur).",
            "v0004 parse-failed rows are written as {__hierarchy_parse_failed__: true, raw_text_content: <original>}; hierarchy fields are NOT appended to these rows.",
        ],
    }

    global_meta_path = output_dir / "run_meta_global.json"
    with global_meta_path.open("w", encoding=args.encoding) as f:
        json.dump(global_meta, f, ensure_ascii=False, indent=2)

    if args.verbose:
        print(
            f"[HIERARCHY] Complete: {total_counters['files_processed']} files, "
            f"{total_counters['total_rows_written']} rows, "
            f"{total_counters['total_nodes']} nodes, "
            f"max_depth={total_counters['max_tree_depth']}, "
            f"errors={total_counters['files_error']}",
            file=sys.stderr,
        )

    # stdout 输出摘要
    print(json.dumps({
        "status": "ok",
        "total_files": len(input_files),
        "counters": total_counters,
        "run_meta": str(global_meta_path),
    }, ensure_ascii=False, indent=2))


# ============================================================
# Main
# ============================================================
def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.input:
        _run_single_file_mode(args)
    else:
        _run_directory_mode(args)


if __name__ == "__main__":
    main()
