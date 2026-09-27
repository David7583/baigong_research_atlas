# ============================================================
# 文件名: json_stream_v0001.py
# 中文名: 大型 JSON 增量读取工具
# 版本号: v0001
#
# 主层级: data
# 层级: scripts / common / streaming
# 脚本定位: 为入口适配、规范校验、结构分析和对话切片提供标准库流式 JSON 读取能力
#
# 职责说明:
# - 逐项读取顶层数组，不将完整文件载入内存
# - 逐项读取对象中指定的数组字段，并保留其余顶层元数据
#
# 本脚本做什么:
# - 使用 JSONDecoder.raw_decode 和有界字符缓冲区解析 UTF-8 JSON
# - 提供数组计数、顶层元数据读取和规范化数组哈希
#
# 本脚本不做什么:
# - 不修改输入，不解释业务字段，不写正式数据库
# - 不容忍尾随垃圾、缺失分隔符或不完整 JSON
#
# 制度边界声明:
# - 内存上界由单个数组元素大小和读取块大小决定，不由完整文件大小决定
# - 解析失败必须停止并报告字符位置，不返回部分成功
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: json_stream_v0001
# family: json_stream
# role: bounded_json_stream_reader
# version: v0001
# status: experimental
# entry_point: scripts/json_stream_v0001.py
# input:
#   - UTF-8 or UTF-8-BOM JSON file
# output:
#   - incrementally decoded JSON values and aggregate metadata
# depends_on:
#   - Python standard library
# used_by:
#   - canonical_ingress_gateway_v0003.py
#   - canonical_ingress_contract_validator_v0002.py
#   - adapt_mapping_current_node_conversations_v0002.py
#   - analyze_json_structure_v0003.py
#   - coarse_slice_conversations_v0003.py
# ============================================================

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "json_stream"
SCRIPT_NAME = "json_stream_v0001.py"
SCRIPT_VERSION = "v0001"
DEFAULT_CHUNK_CHARS = 1024 * 1024
COMPACT_THRESHOLD_CHARS = 2 * 1024 * 1024


# ============================================================
# 异常类型
# ============================================================

class JsonStreamError(RuntimeError):
    """Raised when an incrementally read JSON document is invalid."""


# ============================================================
# 数据结构
# ============================================================

class JsonStreamReader:
    """Incremental character reader backed by JSONDecoder.raw_decode."""

    def __init__(self, path: Path, *, chunk_chars: int = DEFAULT_CHUNK_CHARS) -> None:
        if chunk_chars < 4096:
            raise JsonStreamError("chunk_chars must be at least 4096")
        self.path = path.resolve()
        self.chunk_chars = chunk_chars
        self.decoder = json.JSONDecoder()
        self.stream: Any = None
        self.buffer = ""
        self.position = 0
        self.absolute_position = 0
        self.eof = False

    def __enter__(self) -> "JsonStreamReader":
        if not self.path.is_file():
            raise JsonStreamError(f"JSON input is not a file: {self.path}")
        self.stream = self.path.open("r", encoding="utf-8-sig", newline="")
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.stream is not None:
            self.stream.close()

    def _compact(self) -> None:
        if self.position >= COMPACT_THRESHOLD_CHARS:
            self.absolute_position += self.position
            self.buffer = self.buffer[self.position:]
            self.position = 0

    def _fill(self) -> bool:
        if self.eof:
            return False
        chunk = self.stream.read(self.chunk_chars)
        if chunk == "":
            self.eof = True
            return False
        self.buffer += chunk
        return True

    def skip_whitespace(self) -> None:
        while True:
            while self.position < len(self.buffer) and self.buffer[self.position].isspace():
                self.position += 1
            self._compact()
            if self.position < len(self.buffer) or not self._fill():
                return

    def peek(self) -> Optional[str]:
        self.skip_whitespace()
        if self.position >= len(self.buffer) and not self._fill():
            return None
        self.skip_whitespace()
        return self.buffer[self.position] if self.position < len(self.buffer) else None

    def expect(self, expected: str) -> None:
        actual = self.peek()
        if actual != expected:
            raise JsonStreamError(
                f"expected {expected!r} at character {self.absolute_position + self.position}, found {actual!r}"
            )
        self.position += 1
        self._compact()

    def decode_value(self) -> Any:
        self.skip_whitespace()
        start_absolute = self.absolute_position + self.position
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.position)
                self.position = end
                self._compact()
                return value
            except json.JSONDecodeError as exc:
                if self._fill():
                    continue
                raise JsonStreamError(
                    f"invalid or incomplete JSON value near character {start_absolute}: {exc.msg}"
                ) from exc

    def require_end(self) -> None:
        self.skip_whitespace()
        if self.peek() is not None:
            raise JsonStreamError(
                f"unexpected trailing JSON content at character {self.absolute_position + self.position}"
            )


# ============================================================
# 工具函数区
# ============================================================

def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def iter_root_array(path: Path) -> Iterator[Tuple[int, Any]]:
    with JsonStreamReader(path) as reader:
        reader.expect("[")
        index = 0
        if reader.peek() == "]":
            reader.expect("]")
            reader.require_end()
            return
        while True:
            yield index, reader.decode_value()
            index += 1
            separator = reader.peek()
            if separator == ",":
                reader.expect(",")
                continue
            if separator == "]":
                reader.expect("]")
                reader.require_end()
                return
            raise JsonStreamError(f"root array item {index - 1} is not followed by ',' or ']'")


def inspect_root_array(path: Path) -> Dict[str, Any]:
    count = 0
    first: Any = None
    for index, value in iter_root_array(path):
        if index == 0:
            first = value
        count += 1
    return {"container_type": "json_array", "count": count, "first_item": first}


def canonical_root_array_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    count = 0
    for _, value in iter_root_array(path):
        if count:
            digest.update(b",")
        digest.update(_canonical_json(value).encode(DEFAULT_ENCODING))
        count += 1
    digest.update(b"]")
    if count < 1:
        raise JsonStreamError("root array must not be empty")
    return digest.hexdigest()


def _walk_object_array(
    path: Path,
    array_field: str,
    *,
    yield_items: bool,
) -> Iterator[Tuple[str, Any]]:
    with JsonStreamReader(path) as reader:
        reader.expect("{")
        found = False
        if reader.peek() == "}":
            raise JsonStreamError("root object must not be empty")
        while True:
            key = reader.decode_value()
            if not isinstance(key, str):
                raise JsonStreamError("root object key must be a string")
            reader.expect(":")
            if key != array_field:
                yield "metadata", (key, reader.decode_value())
            else:
                if found:
                    raise JsonStreamError(f"duplicate root field: {array_field}")
                found = True
                reader.expect("[")
                index = 0
                if reader.peek() != "]":
                    while True:
                        value = reader.decode_value()
                        if yield_items:
                            yield "item", (index, value)
                        else:
                            yield "count", index
                        index += 1
                        separator = reader.peek()
                        if separator == ",":
                            reader.expect(",")
                            continue
                        if separator == "]":
                            break
                        raise JsonStreamError(
                            f"{array_field} item {index - 1} is not followed by ',' or ']'"
                        )
                reader.expect("]")
                yield "array_end", index
            separator = reader.peek()
            if separator == ",":
                reader.expect(",")
                continue
            if separator == "}":
                reader.expect("}")
                reader.require_end()
                break
            raise JsonStreamError("root object field is not followed by ',' or '}'")
        if not found:
            raise JsonStreamError(f"root object is missing array field: {array_field}")


def inspect_object_array(path: Path, array_field: str) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    count = 0
    for kind, value in _walk_object_array(path, array_field, yield_items=False):
        if kind == "metadata":
            key, item = value
            metadata[key] = item
        elif kind == "array_end":
            count = int(value)
    return {
        "container_type": "json_object",
        "array_field": array_field,
        "count": count,
        "metadata": metadata,
    }


def iter_object_array(path: Path, array_field: str) -> Iterator[Tuple[int, Any]]:
    for kind, value in _walk_object_array(path, array_field, yield_items=True):
        if kind == "item":
            yield value


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def stream_contract() -> Dict[str, Any]:
    return {
        "root_array": True,
        "object_array_field": True,
        "maximum_resident_scope": "single_array_item_plus_buffer",
        "trailing_content_rejected": True,
    }

