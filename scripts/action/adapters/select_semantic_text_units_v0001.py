#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: select_semantic_text_units_v0001.py
# 中文名: 语义文本单位选择脚本
# Version: v0001
# Layer: action
# Sub-layer: adapters
# Updatable: True
# 功能说明: 从标准化文本单位中按显式JSON路径选择语义正文，保持身份与坐标不变。
#
# ALIAS_META
# alias: select_semantic_text_units
# family: select_semantic_text_units
# role: semantic_text_unit_selector
# version: v0001
# status: active
# entry_point: scripts/action/adapters/select_semantic_text_units_v0001.py
# input: normalized_text_units JSONL and explicit include paths
# output: selected normalized text units JSONL and run metadata
# depends_on: normalize_text_units_v0002
# used_by: data_action_chain_pipeline
# ============================================================

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCRIPT_NAME = "select_semantic_text_units_v0001.py"
SCRIPT_VERSION = "v0001"
REQUIRED_FIELDS = (
    "unit_id",
    "unit_text",
    "asset_id",
    "path",
    "segment_index",
    "char_start",
    "char_end",
)


class SelectionError(RuntimeError):
    """Raised when semantic-unit selection cannot be completed safely."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary_name).replace(path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_text_atomic(path, json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n")


def iter_jsonl(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SelectionError(f"invalid JSON at line {line_number}: {exc}") from exc
            if not isinstance(payload, dict):
                raise SelectionError(f"record at line {line_number} is not an object")
            yield line_number, payload


def select_records(
    input_path: Path,
    include_paths: Sequence[str],
    max_records: int | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    allowed_paths = frozenset(include_paths)
    selected: list[dict[str, Any]] = []
    rows_read = 0
    rows_path_excluded = 0
    duplicate_unit_ids = 0
    seen_unit_ids: set[str] = set()

    for line_number, record in iter_jsonl(input_path):
        rows_read += 1
        missing = [field for field in REQUIRED_FIELDS if field not in record]
        if missing:
            raise SelectionError(
                f"record at line {line_number} is missing required fields: {missing}"
            )
        if record["path"] not in allowed_paths:
            rows_path_excluded += 1
            continue
        unit_id = record["unit_id"]
        if not isinstance(unit_id, str) or not unit_id:
            raise SelectionError(f"record at line {line_number} has invalid unit_id")
        if unit_id in seen_unit_ids:
            duplicate_unit_ids += 1
            raise SelectionError(f"duplicate unit_id at line {line_number}: {unit_id}")
        if not isinstance(record["unit_text"], str) or not record["unit_text"].strip():
            raise SelectionError(f"record at line {line_number} has empty unit_text")
        seen_unit_ids.add(unit_id)
        selected.append(record)
        if max_records is not None and len(selected) >= max_records:
            break

    stats = {
        "rows_read": rows_read,
        "rows_selected": len(selected),
        "rows_path_excluded": rows_path_excluded,
        "duplicate_unit_ids": duplicate_unit_ids,
        "selection_limited": max_records is not None,
        "max_records": max_records,
    }
    return selected, stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Select normalized semantic text units by exact JSON path."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--include-path", action="append", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-meta", required=True)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def execute(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    run_meta_path = Path(args.run_meta).resolve()
    if not input_path.is_file():
        raise SelectionError(f"input is not a file: {input_path}")
    if output_path == input_path or run_meta_path == input_path:
        raise SelectionError("outputs must not overwrite the input")
    if output_path == run_meta_path:
        raise SelectionError("output and run-meta paths must differ")
    if args.max_records is not None:
        if args.max_records < 1:
            raise SelectionError("max-records must be at least 1")
        if not args.test_mode:
            raise SelectionError("max-records is permitted only with --test-mode")

    include_paths = list(dict.fromkeys(args.include_path))
    records, stats = select_records(input_path, include_paths, args.max_records)
    if not records:
        raise SelectionError("explicit path selection produced zero semantic text units")

    result = {
        "status": "dry-run" if args.dry_run else "completed",
        "script": SCRIPT_NAME,
        "version": SCRIPT_VERSION,
        "input": str(input_path),
        "input_sha256": file_sha256(input_path),
        "include_paths": include_paths,
        "output": str(output_path),
        "run_meta": str(run_meta_path),
        "stats": stats,
        "identity_and_coordinates_preserved": True,
    }
    if args.dry_run:
        return result

    output_text = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    )
    write_text_atomic(output_path, output_text)
    result["output_sha256"] = file_sha256(output_path)
    write_json_atomic(run_meta_path, result)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = execute(args)
    except Exception as exc:
        print(json.dumps({
            "status": "error",
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
