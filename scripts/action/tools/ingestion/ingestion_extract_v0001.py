# ============================================================
# 文件名: ingestion_extract_v0001.py
# 中文名: 有界格式识别与无损文本提取
# 版本号: v0001
#
# 主层级: action
# 层级: tools / ingestion / bounded_ingestion_extractor
# 脚本定位: 有界格式识别与无损文本提取的独立执行边界
#
# 职责说明:
# - 有界格式识别与无损文本提取，提供明确输入输出
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
# alias: ingestion_extract_v0001
# family: ingestion_extract
# role: bounded_ingestion_extractor
# version: v0001
# status: active
# entry_point: scripts/action/tools/ingestion/ingestion_extract_v0001.py
# input:
#   - explicit paths and versioned ingestion configuration
# output:
#   - validated results with provenance and classified errors
# depends_on:
#   - Python stdlib
# used_by:
#   - unified_ingestion_pipeline_v0001
# ============================================================

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import TypedDict

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "ingestion_extract"
SCRIPT_NAME = "ingestion_extract_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 数据结构
# ============================================================

class Extraction(TypedDict):
    media_type: str
    adapter_id: str | None
    adapter_version: str | None
    parse_status: str
    units: list
    warnings: list
    source_text: str | None
    method: str | None


# ============================================================
# 工具函数区
# ============================================================

def result(media, status, adapter=None, units=None, warnings=None, source_text=None):
    return Extraction(media_type=media, adapter_id=adapter,
                      adapter_version="v0001" if adapter else None,
                      parse_status=status, units=units or [], warnings=warnings or [],
                      source_text=source_text, method="strict_utf8_decode" if adapter else None)


# ============================================================
# 核心业务组件
# ============================================================

def extract(path: Path, original_name: str, max_bytes: int) -> Extraction:
    """Only bounded, deterministic, validated text variants are enabled."""
    if path.stat().st_size > max_bytes:
        return result("application/octet-stream", "resource_limit")
    data = path.read_bytes()
    if len(data) > max_bytes:
        return result("application/octet-stream", "resource_limit")
    suffix = Path(original_name).suffix.lower()
    if data.startswith(b"%PDF-"):
        return result("application/pdf", "awaiting_adapter_validation",
                      warnings=["PDF extraction and quality policy are not validated in this release"])
    if data.startswith(b"PK"):
        try:
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                if "word/document.xml" in names:
                    return result("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                  "awaiting_adapter_validation")
                return result("application/zip", "unsupported_format")
        except (zipfile.BadZipFile, OSError):
            return result("application/zip", "damaged")
    for signature, media in ((b"\x89PNG", "image/png"), (b"\xff\xd8\xff", "image/jpeg"),
                             (b"RIFF", "audio_or_video/riff"), (b"ID3", "audio/mpeg"),
                             (b"\xd0\xcf\x11\xe0", "application/x-ole-storage")):
        if data.startswith(signature):
            return result(media, "awaiting_adapter_validation")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return result("application/octet-stream", "unsupported_encoding_or_binary")
    stripped = text.lstrip()
    is_json = suffix == ".json" or stripped.startswith(("{", "["))
    if is_json:
        try:
            parsed = json.loads(text)
        except (ValueError, RecursionError):
            return result("application/json", "damaged", warnings=["invalid_json"])
        # Preserve all fields verbatim; do not flatten source roles, messages or times.
        media = "application/json"
        adapter = "utf8_json_verbatim"
    elif suffix in {".txt", ".md", ".csv", ".tsv", ".log"}:
        media, adapter = "text/plain", "utf8_text_verbatim"
    else:
        return result("application/octet-stream", "unsupported_format",
                      warnings=["unknown extension and no validated structural fingerprint"])
    units = [{"content_kind": "extracted_text", "text": text,
              "source_locator": {"kind": "decoded_character_range", "char_start": 0,
                                 "char_end": len(text), "encoding": "utf-8-sig"}}]
    return result(media, "extracted", adapter, units, source_text=text)

