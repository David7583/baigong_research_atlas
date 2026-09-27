#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: discover_data_v0001.py
# 中文名: 数据层寻找脚本（外寻与内寻）
# Version: v0001
# Layer: execution
# Main Layer: Bottom
# Updatable: True
#
# 功能概述
# 本脚本属于 My_Local_AI 数据层底层脚本之一，负责“寻找”这一单一职责。
# 它支持两种模式。
# 1 外寻 external 由行动端业务提交请求文件，脚本按请求规则尝试发现目标对象，并可将实体下载或复制到 data/incoming 暂存区。
# 2 内寻 internal 对整个 data 目录进行递归盘点，发现“存在于 data 空间但尚未登记”的候选对象，并生成发现报告交由登记脚本处理。
#
# 职责边界
# 本脚本做什么
# 1 发现对象的存在性变化
# 2 外寻成功时获得实体并暂存到 data/incoming
# 3 生成发现报告供登记脚本读取
# 4 外寻结束后自动触发一次内寻，以保证新实体进入登记视野
#
# 本脚本不做什么
# 1 不写登记册，不生成系统身份
# 2 不将任何对象写入 data/data_raw
# 3 不做类型判定，不做来源判定
# 4 不做解析，不做清洗，不做结构化，不入库
# 5 不做价值判断，不直接把结果交给行动端使用
#
# 可更新条款
# 本脚本允许以保持向后兼容的方式扩展 source_profile 适配器、报告字段与安全策略。
# 在未发布新版本前，不应改变既有字段语义与不可跳跃链路。
# ============================================================

# ============================================================
# ALIAS_META (comment block, non-executable)
# alias: discover_data
# family: discover_data
# role: data_layer_discover
# version: v0001
# status: active
# entry_point: scripts/discover_data_v0001.py
# input:
#   - external request json file
#   - internal scan root path
# output:
#   - discovery report json
#   - run log jsonl
# depends_on:
#   - (none, stdlib only)
# used_by:
#   - register_data (future)
#   - ingest_data (future)
# ============================================================

# ============================================================
# 制度与职责说明
# 1 本脚本只负责寻找与盘点，不进行制度判断，不进行合规判断。
# 2 本脚本允许写入 data/incoming 与 data/reports/discover，除此之外不写入 data 空间的其他位置。
# 3 本脚本读取登记索引文件仅用于比对，登记索引的生产与更新由登记脚本负责。
# 4 本脚本不得在 import 阶段执行任何 I/O 或业务逻辑。
# ============================================================

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request
import ipaddress
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


# ============================================================
# 边界声明与强约束说明
# ============================================================

DEFAULT_SCHEMA_VERSION = "discover_report_v0001"

DEFAULT_DATA_ROOT = Path("data")
DEFAULT_INCOMING_REL = Path("incoming")
DEFAULT_REPORTS_REL = Path("reports") / "discover"

DEFAULT_LOG_DIR = Path("scripts") / "_logs"
DEFAULT_RUN_LOG_NAME = "discover_data_runs.jsonl"

DEFAULT_EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    ".index",
    "reports",  # 避免把系统自生成报告当作候选对象
}

DEFAULT_EXCLUDE_SUFFIXES = {
    ".tmp",
    ".part",
}

DEFAULT_MAX_FILES = 30
DEFAULT_MAX_BYTES = 250 * 1024 * 1024  # 250MB
DEFAULT_TIMEOUT_SECS = 30

SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._\-]+")

SHA256_HEX_LEN = 64
MIN_HASH_HEX_LEN = 32  # 兼容截断或旧数据，但不用于裁决

# ============================================================
# 常量与全局配置区
# ============================================================

@dataclass(frozen=True)
class ExternalRequest:
    request_id: str
    business_id: str
    business_run_id: Optional[str]
    purpose_summary: Optional[str]
    source_profile: Dict
    search_rule: Dict
    download_policy: Dict
    dry_run: bool


@dataclass(frozen=True)
class Candidate:
    kind: str  # "url" | "path"
    value: str
    source_hint: str


# ============================================================
# 工具函数区（无副作用）
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_discover_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    return f"DISC-{stamp}-{suffix}"


def _safe_relpath(path: Path, root: Path) -> str:
    try:
        rel = path.resolve().relative_to(root.resolve())
    except Exception:
        rel = path
    return rel.as_posix()


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _sanitize_filename(name: str, fallback: str = "downloaded") -> str:
    name = name.strip().replace("\x00", "")
    if not name:
        return fallback
    name = SAFE_FILENAME_PATTERN.sub("_", name)
    # 防止极端长度
    if len(name) > 180:
        stem, dot, suf = name.rpartition(".")
        if dot:
            stem = stem[:160]
            suf = suf[:16]
            name = f"{stem}.{suf}"
        else:
            name = name[:180]
    return name or fallback


def _sha256_of_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _read_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Dict) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _append_jsonl(path: Path, obj: Dict) -> None:
    _ensure_dir(path.parent)
    line = json.dumps(obj, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _iter_files_recursive(root: Path, exclude_dirs: Set[str], exclude_suffixes: Set[str]) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        # 原地过滤目录
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        for fn in filenames:
            p = Path(dirpath) / fn
            if p.suffix.lower() in exclude_suffixes:
                continue
            yield p


def _normalize_domain(url: str) -> str:
    try:
        u = urllib.parse.urlparse(url)
        return (u.hostname or "").lower()
    except Exception:
        return ""


def _is_blocked_host(hostname: str) -> Tuple[bool, str]:
    hn = (hostname or '').strip().lower()
    if not hn:
        return True, 'missing_hostname'
    if hn == 'localhost' or hn.endswith('.localhost'):
        return True, 'localhost_blocked'
    try:
        ip = ipaddress.ip_address(hn)
        if ip.is_loopback:
            return True, 'loopback_ip_blocked'
        if ip.is_private:
            return True, 'private_ip_blocked'
        if ip.is_link_local:
            return True, 'link_local_ip_blocked'
        if ip.is_reserved or ip.is_multicast:
            return True, 'reserved_or_multicast_ip_blocked'
    except ValueError:
        pass
    return False, ''


def _validate_url_for_download(url: str, allow_domains: Optional[Set[str]]) -> Tuple[bool, Dict]:
    meta: Dict = {}
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception as e:
        meta['status'] = 'blocked_invalid_url'
        meta['error'] = f"{type(e).__name__}: {e}"
        return False, meta

    scheme = (parsed.scheme or '').lower()
    if scheme not in {'http', 'https'}:
        meta['status'] = 'blocked_scheme'
        meta['blocked_scheme'] = scheme or 'missing'
        return False, meta

    hostname = (parsed.hostname or '').strip()
    blocked, reason = _is_blocked_host(hostname)
    if blocked:
        meta['status'] = 'blocked_host'
        meta['blocked_host'] = hostname
        meta['blocked_reason'] = reason
        return False, meta

    if not allow_domains:
        meta['status'] = 'blocked_no_allowlist'
        meta['message'] = 'allow_domains is empty; external download is disabled by default'
        return False, meta

    dom = hostname.lower()
    if dom not in allow_domains:
        meta['status'] = 'blocked_domain'
        meta['blocked_domain'] = dom
        return False, meta

    return True, meta
def _load_registry_index(path: Optional[Path]) -> Tuple[Set[str], Set[str]]:
    """
    返回两个集合
    1 registered_locators 相对路径或外部 locator
    2 registered_hashes sha256 集合
    支持 json 与 jsonl 两种。
    """
    if not path:
        return set(), set()
    if not path.exists():
        return set(), set()

    locators: Set[str] = set()
    hashes: Set[str] = set()

    try:
        if path.suffix.lower() == ".jsonl":
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    locator = rec.get("locator") or rec.get("path") or rec.get("object_locator")
                    if isinstance(locator, str) and locator.strip():
                        locators.add(locator.strip())
                    h = rec.get("sha256") or rec.get("hash")
                    if isinstance(h, str) and len(h) >= MIN_HASH_HEX_LEN:
                        hashes.add(h.strip())
        else:
            data = _read_json(path)
            if isinstance(data, dict) and "records" in data:
                data = data["records"]
            if isinstance(data, list):
                for rec in data:
                    if not isinstance(rec, dict):
                        continue
                    locator = rec.get("locator") or rec.get("path") or rec.get("object_locator")
                    if isinstance(locator, str) and locator.strip():
                        locators.add(locator.strip())
                    h = rec.get("sha256") or rec.get("hash")
                    if isinstance(h, str) and len(h) >= MIN_HASH_HEX_LEN:
                        hashes.add(h.strip())
    except Exception:
        # 索引不可读时返回空集合，由上层记录错误
        return set(), set()

    return locators, hashes


# ============================================================
# 核心业务逻辑区
# ============================================================

def load_external_request(path: Path) -> ExternalRequest:
    data = _read_json(path)

    request_id = str(data.get("request_id") or "").strip()
    business_id = str(data.get("business_id") or "").strip()
    if not request_id or not business_id:
        raise ValueError("external request must include request_id and business_id")

    return ExternalRequest(
        request_id=request_id,
        business_id=business_id,
        business_run_id=(str(data.get("business_run_id")).strip() if data.get("business_run_id") else None),
        purpose_summary=(str(data.get("purpose_summary")).strip() if data.get("purpose_summary") else None),
        source_profile=(data.get("source_profile") or {}),
        search_rule=(data.get("search_rule") or {}),
        download_policy=(data.get("download_policy") or {}),
        dry_run=bool(data.get("dry_run", False)),
    )


def build_candidates(req: ExternalRequest, data_root: Path) -> List[Candidate]:
    """
    只生成候选，不进行下载。
    """
    sp = req.source_profile or {}
    sr = req.search_rule or {}
    stype = str(sp.get("type") or sp.get("source_type") or "").strip().lower()

    cands: List[Candidate] = []

    if stype in {"direct_urls", "direct_url", "urls"}:
        urls = sp.get("urls") or sr.get("urls") or []
        if isinstance(urls, str):
            urls = [urls]
        for u in urls:
            if isinstance(u, str) and u.strip():
                cands.append(Candidate(kind="url", value=u.strip(), source_hint="direct_urls"))

    elif stype in {"manifest_file", "manifest"}:
        mpath = sp.get("manifest_path") or sr.get("manifest_path")
        if not mpath:
            return cands
        mp = Path(mpath)
        try:
            if not mp.is_absolute():
                mp = (data_root / mp).resolve()
            else:
                mp = mp.resolve()
        except Exception:
            return cands
        if not _is_within_root(mp, data_root):
            return cands
        if mp.exists() and mp.is_file():
            with mp.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    cands.append(Candidate(kind="url", value=line, source_hint="manifest_file"))

    elif stype in {"local_mount", "local"}:
        base = sp.get("base_path") or sr.get("base_path") or sp.get("path") or sr.get("path")
        if not base:
            return cands
        basep = Path(base)
        if not basep.is_absolute():
            basep = (Path.cwd() / basep).resolve()
        patterns = sr.get("patterns") or sr.get("pattern") or ["*"]
        if isinstance(patterns, str):
            patterns = [patterns]
        recursive = bool(sr.get("recursive", True))
        for pat in patterns:
            if not isinstance(pat, str) or not pat.strip():
                continue
            if recursive:
                for p in basep.rglob(pat):
                    if p.is_file():
                        cands.append(Candidate(kind="path", value=str(p), source_hint="local_mount"))
            else:
                for p in basep.glob(pat):
                    if p.is_file():
                        cands.append(Candidate(kind="path", value=str(p), source_hint="local_mount"))

    elif stype in {"simple_listing", "listing"}:
        url = sp.get("url") or sr.get("url")
        if not url or not isinstance(url, str):
            return cands
        pattern = sr.get("href_regex") or sr.get("pattern") or r'href=["\']([^"\']+)["\']'
        suffix_allow = sr.get("suffix_allow")  # e.g. [".pdf"]
        if isinstance(suffix_allow, str):
            suffix_allow = [suffix_allow]

        raw_allow = req.download_policy.get("allow_domains") or sr.get("allow_domains") or sp.get("allow_domains")
        allow_set: Optional[Set[str]] = None
        if isinstance(raw_allow, str):
            allow_set = {x.strip().lower() for x in raw_allow.split(",") if x.strip()}
        elif isinstance(raw_allow, list):
            allow_set = {str(x).strip().lower() for x in raw_allow if str(x).strip()}
        ok, _vmeta = _validate_url_for_download(url, allow_domains=allow_set)
        if not ok:
            return cands

        text = fetch_text(url, timeout=int(req.download_policy.get("timeout_secs") or DEFAULT_TIMEOUT_SECS))
        links = extract_links(text, pattern=pattern)
        base = urllib.parse.urlparse(url)
        for link in links:
            abs_url = urllib.parse.urljoin(url, link)
            if suffix_allow:
                if not any(abs_url.lower().endswith(s.lower()) for s in suffix_allow if isinstance(s, str)):
                    continue
            cands.append(Candidate(kind="url", value=abs_url, source_hint="simple_listing"))

    else:
        # 未知类型，返回空列表
        return cands

    # 最小去重，按 value
    seen: Set[str] = set()
    uniq: List[Candidate] = []
    for c in cands:
        if c.value in seen:
            continue
        seen.add(c.value)
        uniq.append(c)
    return uniq


def fetch_text(url: str, timeout: int) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "My_Local_AI/DiscoverData"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        # 尝试 utf-8，失败则 latin-1
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return raw.decode("latin-1", errors="replace")


def extract_links(text: str, pattern: str) -> List[str]:
    try:
        rx = re.compile(pattern, flags=re.IGNORECASE)
    except Exception:
        rx = re.compile(r'href=["\']([^"\']+)["\']', flags=re.IGNORECASE)
    out: List[str] = []
    for m in rx.finditer(text):
        if m.lastindex:
            out.append(m.group(1))
    return out


def build_data_index_for_dedupe(
    data_root: Path,
    exclude_dirs: Set[str],
    exclude_suffixes: Set[str],
    compute_hash: bool,
    max_hash_files: int,
    max_hash_bytes: int,
) -> Dict[str, Dict]:
    """
    扫描整个 data 目录以支持去重。
    返回索引 dict
    - by_basename_lower: basename -> list of locators
    - by_sha256: sha256 -> locator
    """
    by_basename: Dict[str, List[str]] = {}
    by_sha: Dict[str, str] = {}

    hashed_files = 0
    hashed_bytes = 0

    for fp in _iter_files_recursive(data_root, exclude_dirs=exclude_dirs, exclude_suffixes=exclude_suffixes):
        if not fp.is_file():
            continue
        locator = _safe_relpath(fp, data_root)
        b = fp.name.lower()
        by_basename.setdefault(b, []).append(locator)

        if not compute_hash:
            continue
        if hashed_files >= max_hash_files:
            continue
        try:
            size = fp.stat().st_size
        except Exception:
            continue
        if size > max_hash_bytes:
            continue
        if hashed_bytes + size > max_hash_bytes:
            continue

        try:
            h = _sha256_of_file(fp)
            by_sha[h] = locator
            hashed_files += 1
            hashed_bytes += size
        except Exception:
            continue

    return {
        "by_basename_lower": by_basename,
        "by_sha256": by_sha,
        "hash_stats": {"hashed_files": hashed_files, "hashed_bytes": hashed_bytes},
    }


def download_or_copy_candidate(
    cand: Candidate,
    dest_dir: Path,
    name_tag: str,
    timeout: int,
    max_bytes: int,
    allow_domains: Optional[Set[str]],
    dry_run: bool,
) -> Tuple[Optional[Path], Dict]:
    """
    成功返回 (dest_path, meta)
    失败返回 (None, meta)
    """
    meta: Dict = {"candidate": {"kind": cand.kind, "value": cand.value, "source_hint": cand.source_hint}}

    if dry_run:
        meta["status"] = "dry_run_skipped"
        return None, meta

    if cand.kind == "url":
        url = cand.value
        ok, vmeta = _validate_url_for_download(url, allow_domains=allow_domains)
        if not ok:
            meta.update(vmeta)
            return None, meta

        parsed = urllib.parse.urlparse(url)
        filename = Path(parsed.path).name or "downloaded"
        filename = _sanitize_filename(filename, fallback="downloaded")

        # 为避免同名覆盖，默认加入业务或运行后缀
        tag = (name_tag or "").strip()
        if tag:
            p = Path(filename)
            stem = p.stem or "downloaded"
            suf = p.suffix
            filename = f"{stem}__{tag}{suf}"

        _ensure_dir(dest_dir)

        def _resolve_conflict(fn: str) -> str:
            fp = dest_dir / fn
            if not fp.exists():
                return fn
            p2 = Path(fn)
            stem2 = p2.stem or "downloaded"
            suf2 = p2.suffix
            short = uuid.uuid4().hex[:8]
            return f"{stem2}__dup_{short}{suf2}"

        filename = _resolve_conflict(filename)
        tmp_path = dest_dir / (filename + ".part")
        final_path = dest_dir / filename

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "My_Local_AI/DiscoverData"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                total = 0
                with tmp_path.open("wb") as f:
                    while True:
                        chunk = resp.read(1024 * 128)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_bytes:
                            raise ValueError(f"download too large, exceeds max_bytes={max_bytes}")
                        f.write(chunk)
            tmp_path.replace(final_path)
            meta["status"] = "downloaded"
            meta["download_path"] = str(final_path)
            meta["stored_filename"] = filename
            return final_path, meta
        except Exception as e:
            meta["status"] = "download_failed"
            meta["error"] = f"{type(e).__name__}: {e}"
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            return None, meta

    if cand.kind == "path":
        src = Path(cand.value)
        if not src.exists() or not src.is_file():
            meta["status"] = "source_missing"
            return None, meta

        filename = _sanitize_filename(src.name, fallback="copied")
        tag = (name_tag or "").strip()
        if tag:
            p = Path(filename)
            stem = p.stem or "copied"
            suf = p.suffix
            filename = f"{stem}__{tag}{suf}"
        _ensure_dir(dest_dir)
        # 同目录下不允许静默覆盖
        final_path = dest_dir / filename
        if final_path.exists():
            p2 = Path(filename)
            stem2 = p2.stem or "copied"
            suf2 = p2.suffix
            short = uuid.uuid4().hex[:8]
            filename = f"{stem2}__dup_{short}{suf2}"
            final_path = dest_dir / filename
        tmp_path = dest_dir / (filename + ".part")

        try:
            size = src.stat().st_size
            if size > max_bytes:
                raise ValueError(f"file too large, exceeds max_bytes={max_bytes}")
            shutil.copy2(src, tmp_path)
            tmp_path.replace(final_path)
            meta["status"] = "copied"
            meta["stored_filename"] = filename
            meta["download_path"] = str(final_path)
            return final_path, meta
        except Exception as e:
            meta["status"] = "copy_failed"
            meta["error"] = f"{type(e).__name__}: {e}"
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            return None, meta

    meta["status"] = "unknown_candidate_kind"
    return None, meta


def run_external(
    req: ExternalRequest,
    *,
    data_root: Path,
    incoming_dir: Path,
    reports_dir: Path,
    registry_index_path: Optional[Path],
    exclude_dirs: Set[str],
    exclude_suffixes: Set[str],
    dedupe_hash: bool,
    max_hash_files: int,
    hash_budget_bytes: int,
    max_files: int,
    max_bytes: int,
    timeout_secs: int,
    allow_domains: Optional[Set[str]],
    auto_internal_after: bool,
) -> Tuple[Dict, Optional[Dict]]:
    discover_id = _new_discover_id()
    started = _utc_now_iso()

    # 外寻会话目录
    session_dir = incoming_dir / f"{discover_id}__{_sanitize_filename(req.business_id)}__{_sanitize_filename(req.request_id)}"
    _ensure_dir(session_dir)

    report: Dict = {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "discover_id": discover_id,
        "mode": "external",
        "timestamp": started,
        "request_id": req.request_id,
        "business_id": req.business_id,
        "business_run_id": req.business_run_id,
        "purpose_summary": req.purpose_summary,
        "dedupe_scope": "data_root_full",
        "found": [],
        "skipped": [],
        "not_found": [],
        "errors": [],
        "hints": [],
        "session_dir": _safe_relpath(session_dir, data_root) if _is_within_root(session_dir, data_root) else str(session_dir),
        "policy": {
            "dry_run": bool(req.dry_run),
            "max_files": max_files,
            "max_bytes": max_bytes,
            "timeout_secs": timeout_secs,
            "dedupe_hash": dedupe_hash,
            "exclude_dirs": sorted(list(exclude_dirs)),
        },
        "source_profile": req.source_profile,
        "search_rule": req.search_rule,
    }

    report["handoff"] = {"session_dir": report.get("session_dir"), "delivered_files": []}

    # 构建候选
    try:
        candidates = build_candidates(req, data_root=data_root)
    except Exception as e:
        report["errors"].append({"stage": "build_candidates", "message": f"{type(e).__name__}: {e}"})
        candidates = []

    if not candidates:
        report["not_found"].append({
            "target": "candidates",
            "rule_snapshot": {"source_profile": req.source_profile, "search_rule": req.search_rule},
            "reason": "no candidates generated",
        })
        # 即便没有候选也可输出报告并触发内寻
        report_path = write_report(report, reports_dir=reports_dir, filename_hint=f"{discover_id}__external")
        report["report_path"] = report_path.as_posix()

        internal_report = None
        if auto_internal_after:
            internal_report, _ = run_internal(
                data_root=data_root,
                reports_dir=reports_dir,
                registry_index_path=registry_index_path,
                exclude_dirs=exclude_dirs,
                exclude_suffixes=exclude_suffixes,
                compute_hash=dedupe_hash,
                max_hash_files=max_hash_files,
                hash_budget_bytes=hash_budget_bytes,
                hint={
                    "triggered_by": "external_no_candidates",
                    "discover_id": discover_id,
                    "request_id": req.request_id,
                    "business_id": req.business_id,
                },
            )
        return report, internal_report

    # 外寻去重索引
    index = build_data_index_for_dedupe(
        data_root=data_root,
        exclude_dirs=exclude_dirs,
        exclude_suffixes=exclude_suffixes,
        compute_hash=dedupe_hash,
        max_hash_files=max_hash_files,
        max_hash_bytes=hash_budget_bytes,
    )
    by_basename = index.get("by_basename_lower", {})
    by_sha = index.get("by_sha256", {})

    downloaded_count = 0
    downloaded_bytes = 0

    for cand in candidates:
        if downloaded_count >= max_files:
            report["skipped"].append({"candidate": {"kind": cand.kind, "value": cand.value}, "reason": "max_files_reached"})
            continue

        # basename 仅用于提示，不用于裁决
        basename = ""
        if cand.kind == "url":
            try:
                basename = Path(urllib.parse.urlparse(cand.value).path).name.lower()
            except Exception:
                basename = ""
        elif cand.kind == "path":
            basename = Path(cand.value).name.lower()

        if basename and basename in by_basename:
            report["hints"].append({
                "candidate": {"kind": cand.kind, "value": cand.value, "source_hint": cand.source_hint},
                "hint": "basename_exists_in_data_root",
                "basename": basename,
                "existing_locators": by_basename.get(basename, [])[:20],
            })

        name_tag = f"biz_{_sanitize_filename(req.business_id)}"

        dest_path, meta = download_or_copy_candidate(
            cand,
            dest_dir=session_dir,
            name_tag=name_tag,
            timeout=timeout_secs,
            max_bytes=max_bytes,
            allow_domains=allow_domains,
            dry_run=bool(req.dry_run),
        )


        if not dest_path:
            status = meta.get("status")
            if status in {"dry_run_skipped"}:
                report["skipped"].append(meta)
            else:
                report["errors"].append({"stage": "download_or_copy", **meta})
            continue

        # 下载后 hash 去重
        try:
            size = dest_path.stat().st_size
        except Exception:
            size = None

        file_hash = None
        if dedupe_hash:
            try:
                file_hash = _sha256_of_file(dest_path)
            except Exception as e:
                report["errors"].append({"stage": "hash_after_download", "path": str(dest_path), "message": f"{type(e).__name__}: {e}"})

        if file_hash and file_hash in by_sha:
            report["hints"].append({
                "candidate": {"kind": cand.kind, "value": cand.value, "source_hint": cand.source_hint},
                "hint": "sha256_exists_in_data_root",
                "existing_locator": by_sha[file_hash],
                "downloaded_locator": _safe_relpath(dest_path, data_root),
                "sha256": file_hash,
            })
        downloaded_count += 1
        if size is not None:
            downloaded_bytes += int(size)

        record = {
            "locator": _safe_relpath(dest_path, data_root) if _is_within_root(dest_path, data_root) else str(dest_path),
            "evidence": meta.get("status", "downloaded"),
            "source_request": req.request_id,
            "business_id": req.business_id,
            "download_meta": meta,
        }
        if size is not None:
            record["size_bytes"] = int(size)
        if file_hash:
            record["sha256"] = file_hash
        try:
            record["mtime"] = int(dest_path.stat().st_mtime)
        except Exception:
            pass

        report["found"].append(record)
        report["handoff"]["delivered_files"].append({"locator": record.get("locator"), "sha256": record.get("sha256"), "stored_filename": meta.get("stored_filename"), "source_hint": cand.source_hint})

    report["stats"] = {
        "candidates": len(candidates),
        "downloaded_files": downloaded_count,
        "downloaded_bytes": downloaded_bytes,
        "dedupe_hash_stats": index.get("hash_stats"),
    }

    report_path = write_report(report, reports_dir=reports_dir, filename_hint=f"{discover_id}__external")
    report["report_path"] = report_path.as_posix()

    internal_report = None
    if auto_internal_after:
        internal_report, _ = run_internal(
            data_root=data_root,
            reports_dir=reports_dir,
            registry_index_path=registry_index_path,
            exclude_dirs=exclude_dirs,
            exclude_suffixes=exclude_suffixes,
            compute_hash=dedupe_hash,
            max_hash_files=max_hash_files,
            hash_budget_bytes=hash_budget_bytes,
            hint={
                "triggered_by": "external_completed",
                "discover_id": discover_id,
                "request_id": req.request_id,
                "business_id": req.business_id,
                "external_report_path": report_path.as_posix(),
            },
        )

    return report, internal_report


def run_internal(
    *,
    data_root: Path,
    reports_dir: Path,
    registry_index_path: Optional[Path],
    exclude_dirs: Set[str],
    exclude_suffixes: Set[str],
    compute_hash: bool,
    max_hash_files: int,
    hash_budget_bytes: int,
    hint: Optional[Dict],
) -> Tuple[Dict, Path]:
    discover_id = _new_discover_id()
    started = _utc_now_iso()

    report: Dict = {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "discover_id": discover_id,
        "mode": "internal",
        "timestamp": started,
        "scan_scope": {"root": data_root.as_posix(), "exclude_dirs": sorted(list(exclude_dirs))},
        "dedupe_scope": "data_root_full",
        "registry_index_path": registry_index_path.as_posix() if registry_index_path else None,
        "hint": hint or {},
        "found": [],
        "errors": [],
        "stats": {},
    }

    registered_locators, registered_hashes = _load_registry_index(registry_index_path)
    if registry_index_path and registry_index_path.exists() and not (registered_locators or registered_hashes):
        report["errors"].append({
            "stage": "load_registry_index",
            "message": "registry index loaded but empty or unreadable",
            "path": registry_index_path.as_posix(),
        })

    # 扫描整个 data
    candidates: List[Dict] = []
    hashed_files = 0
    hashed_bytes = 0

    for fp in _iter_files_recursive(data_root, exclude_dirs=exclude_dirs, exclude_suffixes=exclude_suffixes):
        if not fp.is_file():
            continue
        # 不要越界
        if not _is_within_root(fp, data_root):
            continue

        locator = _safe_relpath(fp, data_root)
        try:
            st = fp.stat()
            size = int(st.st_size)
            mtime = int(st.st_mtime)
        except Exception:
            size = None
            mtime = None

        rec: Dict = {"locator": locator, "evidence": "file_exists"}
        if size is not None:
            rec["size_bytes"] = size
        if mtime is not None:
            rec["mtime"] = mtime

        file_hash = None
        if compute_hash and hashed_files < max_hash_files and size is not None:
            if size <= hash_budget_bytes and (hashed_bytes + size) <= hash_budget_bytes:
                try:
                    file_hash = _sha256_of_file(fp)
                    rec["sha256"] = file_hash
                    hashed_files += 1
                    hashed_bytes += size
                except Exception as e:
                    report["errors"].append({"stage": "hash_scan", "locator": locator, "message": f"{type(e).__name__}: {e}"})

        # 判定是否已登记
        is_registered = False
        if registered_locators and locator in registered_locators:
            is_registered = True
        elif file_hash and registered_hashes and file_hash in registered_hashes:
            is_registered = True

        if not is_registered:
            candidates.append(rec)

    report["found"] = candidates
    report["stats"] = {
        "unregistered_candidates": len(candidates),
        "hash_stats": {"hashed_files": hashed_files, "hashed_bytes": hashed_bytes},
        "registry_loaded_locators": len(registered_locators),
        "registry_loaded_hashes": len(registered_hashes),
    }

    report_path = write_report(report, reports_dir=reports_dir, filename_hint=f"{discover_id}__internal")
    report["report_path"] = report_path.as_posix()
    return report, report_path


def write_report(report: Dict, *, reports_dir: Path, filename_hint: str) -> Path:
    _ensure_dir(reports_dir)
    name = _sanitize_filename(filename_hint, fallback="discover")
    path = reports_dir / f"{name}.json"
    _write_json(path, report)
    return path


# ============================================================
# CLI / main 接口区
# ============================================================

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="discover_data_v0001.py",
        description="My_Local_AI Data Layer Discover Script (external + internal).",
    )

    p.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT), help="Root directory of data space, default: data")
    p.add_argument("--incoming-rel", default=str(DEFAULT_INCOMING_REL), help="Incoming relative dir under data-root")
    p.add_argument("--reports-rel", default=str(DEFAULT_REPORTS_REL), help="Reports relative dir under data-root")
    p.add_argument("--registry-index", default="", help="Optional registry index (json or jsonl), read-only")

    p.add_argument("--mode", choices=["external", "internal"], required=True, help="Run mode")

    p.add_argument("--request", default="", help="External request json file path, required for external mode")

    p.add_argument("--dry-run", action="store_true", help="Dry run for external mode, overrides request.dry_run")

    p.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES, help="Max files to download or copy in external mode")
    p.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES, help="Max bytes per file for download or copy")
    p.add_argument("--timeout-secs", type=int, default=DEFAULT_TIMEOUT_SECS, help="Timeout seconds for network operations")

    p.add_argument("--allow-domains", default="", help="Comma-separated allowlist domains for url download, empty means allow all")

    p.add_argument("--dedupe-hash", action="store_true", help="Compute sha256 for partial data scan to improve dedupe")
    p.add_argument("--max-hash-files", type=int, default=500, help="Max files to hash during scan (performance guard)")
    p.add_argument("--hash-budget-bytes", type=int, default=512 * 1024 * 1024, help="Total hash budget bytes")

    p.add_argument("--no-internal-after-external", action="store_true", help="Do not auto-run internal after external")

    p.add_argument("--exclude-dirs", default="", help="Comma-separated directory names to exclude from scans")
    p.add_argument("--exclude-suffixes", default="", help="Comma-separated suffixes to exclude from scans, e.g. .tmp,.part")

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    data_root = Path(args.data_root)
    incoming_dir = data_root / Path(args.incoming_rel)
    reports_dir = data_root / Path(args.reports_rel)

    # 目录安全性检查
    if not _is_within_root(incoming_dir, data_root):
        print("ERROR incoming directory must be within data-root", file=sys.stderr)
        return 2
    if not _is_within_root(reports_dir, data_root):
        print("ERROR reports directory must be within data-root", file=sys.stderr)
        return 2

    exclude_dirs = set(DEFAULT_EXCLUDE_DIRS)
    if args.exclude_dirs.strip():
        exclude_dirs |= {x.strip() for x in args.exclude_dirs.split(",") if x.strip()}

    exclude_suffixes = set(DEFAULT_EXCLUDE_SUFFIXES)
    if args.exclude_suffixes.strip():
        exclude_suffixes |= {x.strip() for x in args.exclude_suffixes.split(",") if x.strip()}

    allow_domains = None
    if args.allow_domains.strip():
        allow_domains = {x.strip().lower() for x in args.allow_domains.split(",") if x.strip()}

    registry_index_path = Path(args.registry_index) if args.registry_index.strip() else None

    run_id = f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    run_log_path = DEFAULT_LOG_DIR / DEFAULT_RUN_LOG_NAME

    run_record: Dict = {
        "run_id": run_id,
        "script": "discover_data_v0001.py",
        "version": "v0001",
        "mode": args.mode,
        "timestamp": _utc_now_iso(),
        "data_root": data_root.as_posix(),
        "registry_index": registry_index_path.as_posix() if registry_index_path else None,
        "ok": False,
        "reports": [],
        "errors": [],
    }

    try:
        if args.mode == "external":
            if not args.request:
                raise ValueError("--request is required for external mode")
            req_path = Path(args.request)
            req = load_external_request(req_path)
            if args.dry_run:
                req = ExternalRequest(
                    request_id=req.request_id,
                    business_id=req.business_id,
                    business_run_id=req.business_run_id,
                    purpose_summary=req.purpose_summary,
                    source_profile=req.source_profile,
                    search_rule=req.search_rule,
                    download_policy=req.download_policy,
                    dry_run=True,
                )

            ext_report, int_report = run_external(
                req,
                data_root=data_root,
                incoming_dir=incoming_dir,
                reports_dir=reports_dir,
                registry_index_path=registry_index_path,
                exclude_dirs=exclude_dirs,
                exclude_suffixes=exclude_suffixes,
                dedupe_hash=bool(args.dedupe_hash),
                max_hash_files=int(args.max_hash_files),
                hash_budget_bytes=int(args.hash_budget_bytes),
                max_files=int(args.max_files),
                max_bytes=int(args.max_bytes),
                timeout_secs=int(args.timeout_secs),
                allow_domains=allow_domains,
                auto_internal_after=not bool(args.no_internal_after_external),
            )
            if "report_path" in ext_report:
                run_record["reports"].append(ext_report["report_path"])
            if int_report and "report_path" in int_report:
                run_record["reports"].append(int_report["report_path"])

        else:
            int_report, _ = run_internal(
                data_root=data_root,
                reports_dir=reports_dir,
                registry_index_path=registry_index_path,
                exclude_dirs=exclude_dirs,
                exclude_suffixes=exclude_suffixes,
                compute_hash=bool(args.dedupe_hash),
                max_hash_files=int(args.max_hash_files),
                hash_budget_bytes=int(args.hash_budget_bytes),
                hint={"triggered_by": "cli_internal"},
            )
            if "report_path" in int_report:
                run_record["reports"].append(int_report["report_path"])

        run_record["ok"] = True
        _append_jsonl(run_log_path, run_record)
        return 0

    except Exception as e:
        run_record["errors"].append(f"{type(e).__name__}: {e}")
        try:
            _append_jsonl(run_log_path, run_record)
        except Exception:
            pass
        print(f"ERROR {type(e).__name__}: {e}", file=sys.stderr)
        return 1


# ============================================================
# __main__ 入口
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
