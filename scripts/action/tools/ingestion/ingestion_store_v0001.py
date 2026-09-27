# ============================================================
# 文件名: ingestion_store_v0001.py
# 中文名: 原件快照与登记存储
# 版本号: v0001
#
# 主层级: action
# 层级: tools / ingestion / immutable_ingestion_store
# 脚本定位: 原件快照与登记存储的独立执行边界
#
# 职责说明:
# - 原件快照与登记存储，提供明确输入输出
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
# alias: ingestion_store_v0001
# family: ingestion_store
# role: immutable_ingestion_store
# version: v0001
# status: active
# entry_point: scripts/action/tools/ingestion/ingestion_store_v0001.py
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

import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "ingestion_store"
SCRIPT_NAME = "ingestion_store_v0001"
SCRIPT_VERSION = "v0001"


# ============================================================
# 异常类型
# ============================================================

class IngestionError(RuntimeError):
    """Classified input, integrity or configuration failure."""


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class Config:
    root: Path
    state_root: Path
    device_id: str
    max_file_bytes: int
    max_batch_files: int
    gateway_timeout: int

    def __post_init__(self):
        root = self.root.resolve()
        target = no_links(self.state_root).resolve()
        if not any(target.is_relative_to(p) for p in (root / "actioning" / "ingestion", root / "temp")):
            raise IngestionError("state_root_must_be_ingestion_or_temp")
        if not self.device_id.strip():
            raise IngestionError("device_id_required")
        if any(type(v) is not int or v <= 0 for v in
               (self.max_file_bytes, self.max_batch_files, self.gateway_timeout)):
            raise IngestionError("invalid_resource_limit")

    @classmethod
    def load(cls, path: Path, root: Path):
        data = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
        expected = {"schema_version", "state_root", "device_id", "max_file_bytes",
                    "max_batch_files", "gateway_timeout"}
        if set(data) != expected or data["schema_version"] != "unified_ingestion_config_v0001":
            raise IngestionError("unsupported_config")
        root = root.resolve()
        target = (root / data["state_root"]).resolve()
        allowed = [root / "actioning" / "ingestion", root / "temp"]
        if not any(target.is_relative_to(p) and target != p.parent for p in allowed):
            raise IngestionError("state_root_must_be_ingestion_or_temp")
        no_links(root / data["state_root"])
        if not isinstance(data["device_id"], str) or not data["device_id"].strip():
            raise IngestionError("device_id_required")
        for key in ("max_file_bytes", "max_batch_files", "gateway_timeout"):
            if type(data[key]) is not int or data[key] <= 0:
                raise IngestionError("invalid_limit:" + key)
        return cls(root, target, data["device_id"], data["max_file_bytes"],
                   data["max_batch_files"], data["gateway_timeout"])


# ============================================================
# 工具函数区
# ============================================================

def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode(DEFAULT_ENCODING)).hexdigest()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def no_links(path):
    path = Path(path).absolute()
    for p in (path, *path.parents):
        if p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()):
            raise IngestionError("linked_path_rejected")
    return path


def publish(path, data):
    """Atomic no-replace publication; conflicting content fails closed."""
    path = no_links(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != data:
            raise IngestionError("immutable_output_conflict")
        return
    fd, name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise IngestionError("concurrent_output_conflict")
    finally:
        tmp.unlink(missing_ok=True)


def publish_json(path, value):
    publish(path, (json.dumps(value, ensure_ascii=False, indent=2,
                             sort_keys=True) + "\n").encode(DEFAULT_ENCODING))


# ============================================================
# 核心类
# ============================================================

class Store:
    def __init__(self, config):
        self.config = config
        self.root = config.state_root

    def preserve(self, source, dry_run=False, original_name=None, browser_upload=False):
        source = no_links(Path(source))
        if not source.is_file():
            raise IngestionError("source_not_regular_file")
        if source.resolve().is_relative_to(self.root):
            raise IngestionError("managed_output_cannot_be_reimported_use_reprocess")
        before = source.stat()
        if before.st_size > self.config.max_file_bytes:
            raise IngestionError("file_size_limit")
        content_hash = sha(source)
        after = source.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise IngestionError("source_changed_during_read")
        asset_id = "a_" + content_hash[:20]
        managed = self.root / "originals" / content_hash / "payload.bin"
        location = None if browser_upload else str(source.resolve())
        occurrence_id = "o_" + digest([self.config.device_id,
                                      location if location is not None else str(uuid.uuid4())])
        record = {
            "asset_id": asset_id, "source_hash": content_hash,
            "snapshot_id": "sha256:" + content_hash, "size_bytes": before.st_size,
            "original_name": original_name or source.name, "managed_path": str(managed),
            "original_path": location, "original_path_status": "unknown" if browser_upload else "verified",
            "device_id": self.config.device_id, "occurrence_id": occurrence_id,
            "event_time": None, "ingested_at": now(), "last_verified_at": now(),
            "preservation_status": "dry_run" if dry_run else "preserved",
            "bundle_completeness": "unknown",
        }
        if dry_run:
            return record
        no_links(managed)
        managed.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".pending-", dir=managed.parent)
        tmp = Path(name)
        try:
            count = 0
            with source.open("rb") as inp, os.fdopen(fd, "wb") as out:
                for part in iter(lambda: inp.read(1024 * 1024), b""):
                    count += len(part)
                    if count > self.config.max_file_bytes:
                        raise IngestionError("source_grew_over_limit")
                    out.write(part)
                out.flush()
                os.fsync(out.fileno())
            final = source.stat()
            if (final.st_size, final.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                raise IngestionError("source_changed_during_copy")
            if sha(tmp) != content_hash or sha(source) != content_hash:
                raise IngestionError("snapshot_hash_mismatch")
            try:
                os.link(tmp, managed)
            except FileExistsError:
                if sha(managed) != content_hash:
                    raise IngestionError("existing_snapshot_corrupted")
        finally:
            tmp.unlink(missing_ok=True)
        return record

    def verify(self, snapshot):
        target = no_links(Path(snapshot["managed_path"]))
        if not target.resolve().is_relative_to(self.root / "originals"):
            raise IngestionError("snapshot_outside_store")
        if not target.is_file() or sha(target) != snapshot["source_hash"]:
            raise IngestionError("snapshot_missing_or_corrupt")
        if (snapshot["asset_id"] != "a_" + snapshot["source_hash"][:20]
                or snapshot["snapshot_id"] != "sha256:" + snapshot["source_hash"]
                or target.stat().st_size != snapshot["size_bytes"]):
            raise IngestionError("snapshot_identity_mismatch")
        return target

    def append(self, record):
        run_id = record["run_id"]
        if not run_id.startswith("r_") or not all(c.isalnum() or c == "_" for c in run_id):
            raise IngestionError("invalid_run_id")
        publish_json(self.root / "catalog" / (run_id + ".json"), record)

    def catalog(self, query=""):
        rows = []
        completed = {p.name: p for p in (self.root / "catalog").glob("r_*.json")}
        pending = {p.name: p for p in (self.root / "registrations").glob("r_*.json")}
        for path in sorted({**pending, **completed}.values()):
            no_links(path)
            record = json.loads(path.read_text(encoding=DEFAULT_ENCODING))
            if query.casefold() in json.dumps(record.get("source", {}), ensure_ascii=False).casefold():
                rows.append(record)
        return sorted(rows, key=lambda r: r["ingested_at"])

    def event(self, run_id, status, **details):
        publish_json(self.root / "events" / (str(uuid.uuid4()) + ".json"),
                     {"run_id": run_id, "time": now(), "status": status, **details})

    def find(self, run_id):
        matches = [r for r in self.catalog() if r["run_id"] == run_id]
        if len(matches) != 1:
            raise IngestionError("run_not_found")
        return matches[0]
