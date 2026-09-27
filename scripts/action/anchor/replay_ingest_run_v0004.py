# ============================================================
# 文件名: replay_ingest_run_v0004.py
# 中文名: 写入运行重放与一致性校验脚本
# 版本号: v0004
#
# 主层级: action
# 子层级: persistence / preflight
# 脚本定位: 对 ingest 产物进行“可重放性”验证。通过重跑生成并与基准产物做规范化对比
# 可更新: True
#
# 职责说明:
# - 在隔离工作目录中重放一次 ingest 生成过程（通过外部命令执行）
# - 读取基准 concept/instance/attribute JSONL，并读取重放生成的新 JSONL
# - 对两份 JSONL 做规范化排序与内容哈希对比，识别非确定性与内容漂移
# - 输出结构化 replay_report.json，供审计与调试使用
#
# 本脚本做什么:
# - 不写入 SQL
# - 不修改任何输入文件
# - 不推断，不做语义裁决
#
# 本脚本不做什么:
# - 不替代 validate_ingest_payload_v0003.py 的合法性校验
# - 不替代 verify_data_anchor_v0002.py 的现实锚点存在性校验
#
# 制度边界声明:
# - PASS 表示在同一命令与同一输入下，重放产物与基准产物在“规范化视角”完全一致
# - FAIL 表示存在内容漂移或重放失败，必须阻断落库与下游构建
# - 本脚本默认忽略 JSONL 行顺序差异。顺序差异会作为 warning 提示
#
# 统计守恒:
# - compared_files = matched + mismatched + missing
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: replay_ingest_run
# family: replay_ingest_run
# role: replay_consistency_checker
# version: v0004
# status: active
# entry_point: scripts/action/anchor/replay_ingest_run_v0004.py
# input:
#   - baseline concept/instance/attribute declarations (jsonl)
#   - replay output concept/instance/attribute paths (jsonl)
#   - external replay commands (--cmd, repeatable)
# output:
#   - replay_report (json)
# depends_on:
#   - ingest_concept_units_v0002
#   - ingest_instance_units_v0002
#   - ingest_attribute_units_v0003
# used_by:
#   - (无法确定，需人工填写；header 仅述"必须阻断落库与下游构建")
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCRIPT_NAME = "replay_ingest_run_v0004.py"
SCRIPT_VERSION = "v0004"
ARCHIVE_DIRNAME = "archive"
UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_ENCODING = "utf-8"


# ============================================================
# 工具函数
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _sha256_hex_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _archive_existing(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    archive_dir = path.parent / ARCHIVE_DIRNAME
    _ensure_dir(archive_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived = archive_dir / f"{path.stem}__{ts}{path.suffix}"
    shutil.move(str(path), str(archived))
    return archived


def _atomic_write_json(path: Path, obj: Any) -> Tuple[int, str]:
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = _safe_json_dumps(obj).encode(DEFAULT_ENCODING)
    with tmp.open("wb") as f:
        f.write(data)
        f.write(b"\n")
    shutil.move(str(tmp), str(path))
    return len(data), _sha256_hex_bytes(data)


def _read_jsonl(path: Path) -> Iterable[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding=DEFAULT_ENCODING) as f:
        for i, line in enumerate(f, 1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                yield i, {"__parse_error__": True, "__raw__": s}
                continue
            if not isinstance(obj, dict):
                yield i, {"__parse_error__": True, "__raw__": s}
                continue
            yield i, obj


def _norm_json(obj: Dict[str, Any]) -> bytes:
    # 规范化 JSON 用于比特级一致性比较。确保 key 排序与紧凑输出
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(DEFAULT_ENCODING)


def _run_cmd(cmd: str, cwd: Path, timeout_sec: int) -> Dict[str, Any]:
    # 使用 shell 执行以兼容 PowerShell 传入的整行命令
    # 结果中不返回过长 stdout/stderr，避免报告膨胀
    started = _utc_now_iso()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        finished = _utc_now_iso()
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        return {
            "status": "ok" if proc.returncode == 0 else "error",
            "returncode": proc.returncode,
            "started_at": started,
            "finished_at": finished,
            "cmd": cmd,
            "stdout_preview": out[:4000],
            "stderr_preview": err[:4000],
        }
    except subprocess.TimeoutExpired as e:
        finished = _utc_now_iso()
        return {
            "status": "error",
            "returncode": None,
            "started_at": started,
            "finished_at": finished,
            "cmd": cmd,
            "stdout_preview": (e.stdout or "")[:4000] if hasattr(e, "stdout") else "",
            "stderr_preview": (e.stderr or "")[:4000] if hasattr(e, "stderr") else "",
            "error": f"timeout after {timeout_sec}s",
        }
    except Exception as e:
        finished = _utc_now_iso()
        return {
            "status": "error",
            "returncode": None,
            "started_at": started,
            "finished_at": finished,
            "cmd": cmd,
            "stdout_preview": "",
            "stderr_preview": "",
            "error": str(e),
        }


def _new_issue(*, level: str, code: str, where: Dict[str, Any], detail: str, hint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {
        "level": level,
        "code": code,
        "where": where,
        "detail": detail,
    }
    if hint is not None:
        out["hint"] = hint
    return out


# ============================================================
# 规范化对比
# ============================================================

@dataclass
class NormDigest:
    raw_sha256: str
    norm_sha256: str
    record_count: int
    parse_errors: int
    order_sensitive_equal: Optional[bool] = None


def _sort_key_for_record(kind: str, rec: Dict[str, Any]) -> Tuple:
    if kind == "concept":
        return (str(rec.get("concept_id", "")), str(rec.get("content_hash", "")))
    if kind == "instance":
        return (str(rec.get("instance_id", "")), str(rec.get("concept_id", "")))
    # attribute
    return (
        str(rec.get("object_type", "")),
        str(rec.get("object_id", "")),
        str(rec.get("attr_key", "")),
        str(rec.get("attr_value", "")),
    )


def _normalize_jsonl(path: Path, kind: str) -> Tuple[NormDigest, List[Tuple[Tuple, bytes]], List[Dict[str, Any]]]:
    issues: List[Dict[str, Any]] = []
    records: List[Tuple[Tuple, bytes]] = []
    parse_errors = 0

    for line_no, rec in _read_jsonl(path):
        if rec.get("__parse_error__"):
            parse_errors += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="jsonl_parse_error",
                    where={"file": str(path), "line": line_no, "kind": kind},
                    detail="invalid json line",
                    hint={"raw_preview": str(rec.get("__raw__", ""))[:200]},
                )
            )
            continue

        try:
            k = _sort_key_for_record(kind, rec)
            b = _norm_json(rec)
            records.append((k, b))
        except Exception as e:
            parse_errors += 1
            issues.append(
                _new_issue(
                    level="ERROR",
                    code="normalize_record_error",
                    where={"file": str(path), "line": line_no, "kind": kind},
                    detail=str(e),
                )
            )

    records_sorted = sorted(records, key=lambda x: x[0])
    norm_stream = b"\n".join([b for _, b in records_sorted]) + (b"\n" if records_sorted else b"")

    raw_sha = _file_sha256(path)
    norm_sha = _sha256_hex_bytes(norm_stream)
    digest = NormDigest(
        raw_sha256=raw_sha,
        norm_sha256=norm_sha,
        record_count=len(records_sorted),
        parse_errors=parse_errors,
        order_sensitive_equal=None,
    )

    # 顺序敏感差异提示
    try:
        raw_bytes = path.read_bytes()
        # 尝试直接做 raw 对比的依据是上层调用会比较两份文件
        # 这里仅计算自身 raw 与 norm 的一致性提示
        digest.order_sensitive_equal = (raw_sha == norm_sha)
    except Exception:
        digest.order_sensitive_equal = None

    return digest, records_sorted, issues


def _compare_norm_records(
    base: List[Tuple[Tuple, bytes]],
    replay: List[Tuple[Tuple, bytes]],
    sample_limit: int,
) -> Tuple[bool, List[Dict[str, Any]]]:
    if len(base) != len(replay):
        mismatch = [{
            "type": "count_mismatch",
            "base_count": len(base),
            "replay_count": len(replay),
        }]
        return False, mismatch[:sample_limit]

    mismatches: List[Dict[str, Any]] = []
    ok = True
    for i, (b, r) in enumerate(zip(base, replay)):
        bk, bb = b
        rk, rb = r
        if bk != rk or bb != rb:
            ok = False
            if len(mismatches) < sample_limit:
                mismatches.append(
                    {
                        "index": i,
                        "base_key": bk,
                        "replay_key": rk,
                        "base_line_sha256": _sha256_hex_bytes(bb),
                        "replay_line_sha256": _sha256_hex_bytes(rb),
                    }
                )
            else:
                break
    return ok, mismatches


# ============================================================
# 主逻辑
# ============================================================

def replay_and_compare(
    *,
    base_concept: Path,
    base_instance: Path,
    base_attribute: Path,
    replay_concept: Path,
    replay_instance: Path,
    replay_attribute: Path,
    workdir: Path,
    cmds: List[str],
    timeout_sec: int,
    sample_limit: int,
) -> Dict[str, Any]:
    started_at = _utc_now_iso()
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    cmd_results: List[Dict[str, Any]] = []

    _ensure_dir(workdir)

    # 1. 运行重放命令
    for cmd in cmds:
        if not cmd.strip():
            continue
        res = _run_cmd(cmd, cwd=workdir, timeout_sec=timeout_sec)
        cmd_results.append(res)
        if res.get("status") != "ok":
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="replay_command_failed",
                    where={"cmd": cmd, "cwd": str(workdir)},
                    detail="replay command returned non-zero or failed",
                    hint={"cmd_result": res},
                )
            )
            # 命令失败时不继续盲目比较
            break

    # 2. 检查重放输出文件是否存在
    expected = [
        ("concept", replay_concept),
        ("instance", replay_instance),
        ("attribute", replay_attribute),
    ]
    for kind, p in expected:
        if not p.exists():
            errors.append(
                _new_issue(
                    level="ERROR",
                    code="replay_output_missing",
                    where={"file": str(p), "kind": kind, "cwd": str(workdir)},
                    detail="replay output file not found",
                )
            )

    # 如果已有错误，仍生成报告但不做进一步比较
    if errors:
        finished_at = _utc_now_iso()
        return {
            "status": "FAIL",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "started_at": started_at,
            "finished_at": finished_at,
            "workdir": str(workdir),
            "commands": cmd_results,
            "summary": {"errors": len(errors), "warnings": len(warnings)},
            "errors": errors,
            "warnings": warnings,
        }

    # 3. 规范化与比较
    comparisons: List[Dict[str, Any]] = []
    overall_ok = True

    for kind, base_path, replay_path in [
        ("concept", base_concept, replay_concept),
        ("instance", base_instance, replay_instance),
        ("attribute", base_attribute, replay_attribute),
    ]:
        base_digest, base_norm, base_issues = _normalize_jsonl(base_path, kind)
        replay_digest, replay_norm, replay_issues = _normalize_jsonl(replay_path, kind)

        for it in base_issues + replay_issues:
            errors.append(it)

        if base_digest.record_count == replay_digest.record_count and base_digest.norm_sha256 == replay_digest.norm_sha256:
            matched = True
        else:
            matched, mismatch_samples = _compare_norm_records(base_norm, replay_norm, sample_limit=sample_limit)
            if not matched:
                overall_ok = False
                comparisons.append(
                    {
                        "kind": kind,
                        "matched": False,
                        "base": base_digest.__dict__,
                        "replay": replay_digest.__dict__,
                        "mismatches_sample": mismatch_samples,
                    }
                )
                continue

        # 顺序差异提示
        if base_digest.raw_sha256 != replay_digest.raw_sha256 and matched:
            warnings.append(
                _new_issue(
                    level="WARNING",
                    code="order_or_format_difference",
                    where={"kind": kind},
                    detail="normalized content matches but raw file differs, likely order or whitespace differences",
                    hint={"base_raw_sha256": base_digest.raw_sha256, "replay_raw_sha256": replay_digest.raw_sha256},
                )
            )

        comparisons.append(
            {
                "kind": kind,
                "matched": True,
                "base": base_digest.__dict__,
                "replay": replay_digest.__dict__,
            }
        )

    if errors:
        overall_ok = False

    status = "PASS" if overall_ok else "FAIL"
    finished_at = _utc_now_iso()

    return {
        "status": status,
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "started_at": started_at,
        "finished_at": finished_at,
        "workdir": str(workdir),
        "commands": cmd_results,
        "inputs": {
            "base": {
                "concept": str(base_concept),
                "instance": str(base_instance),
                "attribute": str(base_attribute),
            },
            "replay": {
                "concept": str(replay_concept),
                "instance": str(replay_instance),
                "attribute": str(replay_attribute),
            },
            "sha256": {
                "base": {
                    "concept": _file_sha256(base_concept),
                    "instance": _file_sha256(base_instance),
                    "attribute": _file_sha256(base_attribute),
                },
                "replay": {
                    "concept": _file_sha256(replay_concept),
                    "instance": _file_sha256(replay_instance),
                    "attribute": _file_sha256(replay_attribute),
                },
            },
        },
        "comparisons": comparisons,
        "summary": {
            "matched_files": sum(1 for c in comparisons if c.get("matched") is True),
            "mismatched_files": sum(1 for c in comparisons if c.get("matched") is False),
            "errors": len(errors),
            "warnings": len(warnings),
        },
        "errors": errors,
        "warnings": warnings,
    }


# ============================================================
# CLI
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Replay ingest generation via commands and compare JSONL outputs to a baseline with normalized comparison."
    )

    p.add_argument("--base-concept", required=True, help="Baseline concept JSONL")
    p.add_argument("--base-instance", required=True, help="Baseline instance JSONL")
    p.add_argument("--base-attribute", required=True, help="Baseline attribute JSONL")

    p.add_argument("--replay-concept", required=True, help="Replay output concept JSONL path")
    p.add_argument("--replay-instance", required=True, help="Replay output instance JSONL path")
    p.add_argument("--replay-attribute", required=True, help="Replay output attribute JSONL path")

    p.add_argument("--workdir", required=True, help="Working directory for replay commands")

    p.add_argument(
        "--cmd",
        action="append",
        default=[],
        help="Replay command to execute. Provide multiple times to form a sequence",
    )

    p.add_argument("--timeout-sec", type=int, default=1800, help="Timeout seconds per command")
    p.add_argument("--sample-limit", type=int, default=50, help="Max mismatch samples to include")
    p.add_argument("--report", required=True, help="Output report JSON path")
    p.add_argument("--dry-run", action="store_true", help="Do not write report, only print preview")

    return p


def main() -> int:
    args = _build_parser().parse_args()

    base_concept = Path(args.base_concept)
    base_instance = Path(args.base_instance)
    base_attribute = Path(args.base_attribute)

    replay_concept = Path(args.replay_concept)
    replay_instance = Path(args.replay_instance)
    replay_attribute = Path(args.replay_attribute)

    workdir = Path(args.workdir)
    report_path = Path(args.report)

    missing = [str(p) for p in [base_concept, base_instance, base_attribute] if not p.exists()]
    if missing:
        print(_safe_json_dumps({"status": "error", "error": "missing baseline files", "missing": missing}))
        return 2

    _ensure_dir(report_path.parent)
    _ensure_dir(workdir)

    report = replay_and_compare(
        base_concept=base_concept,
        base_instance=base_instance,
        base_attribute=base_attribute,
        replay_concept=replay_concept,
        replay_instance=replay_instance,
        replay_attribute=replay_attribute,
        workdir=workdir,
        cmds=list(args.cmd or []),
        timeout_sec=int(args.timeout_sec),
        sample_limit=int(args.sample_limit),
    )

    if args.dry_run:
        preview = {
            "status": report.get("status"),
            "summary": report.get("summary"),
            "first_error": report.get("errors", [])[:1],
            "first_warning": report.get("warnings", [])[:1],
        }
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0

    _archive_existing(report_path)
    size_bytes, sha = _atomic_write_json(report_path, report)

    print(
        json.dumps(
            {
                "status": "ok",
                "result": report.get("status"),
                "script": SCRIPT_NAME,
                "version": SCRIPT_VERSION,
                "report": str(report_path),
                "report_size_bytes": size_bytes,
                "report_sha256": sha,
                "errors": int(report.get("summary", {}).get("errors", 0)),
                "warnings": int(report.get("summary", {}).get("warnings", 0)),
                "matched_files": int(report.get("summary", {}).get("matched_files", 0)),
                "mismatched_files": int(report.get("summary", {}).get("mismatched_files", 0)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    return 0 if report.get("status") == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
