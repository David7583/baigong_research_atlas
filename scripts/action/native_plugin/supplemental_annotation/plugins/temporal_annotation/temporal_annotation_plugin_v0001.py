# ============================================================
# 文件名: temporal_annotation_plugin_v0001.py
# 中文名: 中文时间补充标注插件
# 版本号: v0001
#
# 主层级: action
# 层级: native_plugin / supplemental_annotation / temporal
# 脚本定位: Supplemental Annotation Framework 的首个独立规则型业务插件
#
# 职责说明:
# - 发现中文及常见 ISO 时间表达式并生成 Temporal Annotation
# - 在可靠 reference time 存在时解析相对时间，否则保留 unresolved
#
# 本脚本做什么:
# - 使用 Python 标准库正则识别 point, interval, relative, duration, recurring
# - 保留原表达式、精度、置信度、规则版本、证据和稳定身份
#
# 本脚本不做什么:
# - 不修改正文，不决定 semantic eligibility，不建立 Action identity
# - 不写数据库，不调用模型、外部 API、Chroma 或 Neo4j
#
# 制度边界声明:
# - 缺少可靠 reference time 时绝不使用机器运行时间猜测相对时间
# - 规则结果属于派生标注；低精度时间不得伪装为日级事实
#
# 可更新: True
# ============================================================

# ============================================================
# ALIAS_META
# ============================================================
# alias: temporal_annotation_plugin_v0001
# family: temporal_annotation_plugin
# role: temporal_supplemental_annotation_plugin
# version: v0001
# status: active
# entry_point: scripts/action/native_plugin/supplemental_annotation/plugins/temporal_annotation/temporal_annotation_plugin_v0001.py
# input:
#   - canonical text-unit batches with stable_identity
#   - temporal plugin configuration and run context
# output:
#   - validated-compatible temporal annotation envelopes
# depends_on:
#   - annotation_contract_v0001
#   - Python stdlib: calendar, dataclasses, datetime, hashlib, json, re, typing
# used_by:
#   - supplemental_annotation_runner_v0001 through explicit plugin registry
# ============================================================

from __future__ import annotations

import calendar
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Pattern, Sequence, Tuple


DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "temporal_annotation_plugin"
SCRIPT_NAME = "temporal_annotation_plugin_v0001"
SCRIPT_VERSION = "v0001"
CONTRACT_VERSION = "supplemental_annotation_contract_v0001"
DEFAULT_RULE_VERSION = "temporal_rules_zh_v0001"

PLUGIN_META = {
    "name": "temporal_annotation",
    "version": SCRIPT_VERSION,
    "contract_version": CONTRACT_VERSION,
    "capability": "supplemental_annotation",
    "annotation_type": "temporal",
    "implementation": "python_stdlib_rules",
}


# ============================================================
# 异常类型
# ============================================================

class TemporalAnnotationError(RuntimeError):
    """Raised when temporal input or configured rules are invalid."""


# ============================================================
# 数据结构
# ============================================================

@dataclass(frozen=True)
class TemporalCandidate:
    rule_name: str
    expression: str
    start: int
    end: int
    priority: int
    groups: Dict[str, Optional[str]]


@dataclass(frozen=True)
class TemporalResolution:
    temporal_type: str
    normalized_start: Optional[str]
    normalized_end: Optional[str]
    precision: str
    reference_time: Optional[str]
    resolution_status: str
    confidence: float
    approximation: bool
    convention: Optional[str]


@dataclass(frozen=True)
class TemporalRule:
    name: str
    priority: int
    pattern: Pattern[str]
    resolver: Callable[[TemporalCandidate, Optional[datetime]], TemporalResolution]


# ============================================================
# 工具函数区
# ============================================================

def _stable_hash(payload: Mapping[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(text.encode(DEFAULT_ENCODING)).hexdigest()


def _iso_day(value: date) -> str:
    return value.isoformat()


def _month_bounds(year: int, month: int) -> Tuple[str, str]:
    if month < 1 or month > 12:
        raise TemporalAnnotationError(f"invalid month: {month}")
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1).isoformat(), date(year, month, last_day).isoformat()


def _year_bounds(year: int) -> Tuple[str, str]:
    return date(year, 1, 1).isoformat(), date(year, 12, 31).isoformat()


def _add_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 + months
    target_year, zero_month = divmod(absolute, 12)
    target_month = zero_month + 1
    target_day = min(value.day, calendar.monthrange(target_year, target_month)[1])
    return date(target_year, target_month, target_day)


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(year=value.year + years, month=2, day=28)


def _chinese_integer(value: str) -> int:
    if value.isdigit():
        return int(value)
    digits = {"零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
              "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
    units = {"十": 10, "百": 100, "千": 1000}
    total = 0
    current = 0
    for character in value:
        if character in digits:
            current = digits[character]
        elif character in units:
            total += (current or 1) * units[character]
            current = 0
        else:
            raise TemporalAnnotationError(f"unsupported Chinese number: {value}")
    return total + current


def _mapping_value(mapping: Mapping[str, Any], dotted_field: str) -> Any:
    value: Any = mapping
    for part in dotted_field.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return None
        value = value[part]
    return value


def _parse_reference_time(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.combine(date.fromisoformat(text), datetime.min.time())
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _reference_time(record: Mapping[str, Any], fields: Sequence[str]) -> Optional[datetime]:
    for field in fields:
        parsed = _parse_reference_time(_mapping_value(record, field))
        if parsed is not None:
            return parsed
    return None


def _text_value(record: Mapping[str, Any], fields: Sequence[str]) -> str:
    for field in fields:
        value = _mapping_value(record, field)
        if isinstance(value, str) and value:
            return value
    raise TemporalAnnotationError("canonical record does not expose a configured text field")


def _resolved(
    temporal_type: str,
    start: Optional[str],
    end: Optional[str],
    precision: str,
    *,
    reference: Optional[datetime] = None,
    confidence: float,
    approximation: bool = False,
    convention: Optional[str] = None,
) -> TemporalResolution:
    return TemporalResolution(
        temporal_type=temporal_type,
        normalized_start=start,
        normalized_end=end,
        precision=precision,
        reference_time=reference.isoformat() if reference else None,
        resolution_status="resolved",
        confidence=confidence,
        approximation=approximation,
        convention=convention,
    )


def _unresolved(
    temporal_type: str,
    precision: str,
    *,
    confidence: float,
) -> TemporalResolution:
    return TemporalResolution(
        temporal_type=temporal_type,
        normalized_start=None,
        normalized_end=None,
        precision=precision,
        reference_time=None,
        resolution_status="unresolved",
        confidence=confidence,
        approximation=False,
        convention=None,
    )


# ============================================================
# 默认映射
# ============================================================

SEASON_MONTHS = {
    "春": (3, 5),
    "春天": (3, 5),
    "春季": (3, 5),
    "夏": (6, 8),
    "夏天": (6, 8),
    "夏季": (6, 8),
    "秋": (9, 11),
    "秋天": (9, 11),
    "秋季": (9, 11),
    "冬": (12, 2),
    "冬天": (12, 2),
    "冬季": (12, 2),
}

SIMPLE_RELATIVE = {
    "前天": ("day", -2),
    "昨天": ("day", -1),
    "今天": ("day", 0),
    "明天": ("day", 1),
    "后天": ("day", 2),
    "去年": ("year", -1),
    "今年": ("year", 0),
    "明年": ("year", 1),
    "上个月": ("month", -1),
    "本月": ("month", 0),
    "这个月": ("month", 0),
    "下个月": ("month", 1),
}


# ============================================================
# 时间解析规则
# ============================================================

def _resolve_cn_range(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    groups = candidate.groups
    y1 = int(groups["y1"] or "0")
    m1 = int(groups["m1"] or "0")
    d1 = int(groups["d1"] or "1")
    y2 = int(groups.get("y2") or y1)
    m2 = int(groups["m2"] or "0")
    d2_text = groups.get("d2")
    d2 = int(d2_text) if d2_text else calendar.monthrange(y2, m2)[1]
    start = date(y1, m1, d1)
    end = date(y2, m2, d2)
    if end < start:
        raise TemporalAnnotationError(f"temporal range ends before it starts: {candidate.expression}")
    precision = "day" if groups.get("d1") and groups.get("d2") else "month"
    return _resolved("interval", _iso_day(start), _iso_day(end), precision, confidence=0.99)


def _resolve_cn_date(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    value = date(
        int(candidate.groups["year"] or "0"),
        int(candidate.groups["month"] or "0"),
        int(candidate.groups["day"] or "0"),
    )
    return _resolved("point", _iso_day(value), _iso_day(value), "day", confidence=1.0)


def _resolve_iso_date(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    value = date.fromisoformat(
        f"{candidate.groups['year']}-{int(candidate.groups['month'] or '0'):02d}-"
        f"{int(candidate.groups['day'] or '0'):02d}"
    )
    return _resolved("point", _iso_day(value), _iso_day(value), "day", confidence=1.0)


def _resolve_year_month(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    start, end = _month_bounds(
        int(candidate.groups["year"] or "0"), int(candidate.groups["month"] or "0")
    )
    return _resolved("interval", start, end, "month", confidence=0.98)


def _resolve_quarter(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    year = int(candidate.groups["year"] or "0")
    quarter = _chinese_integer(candidate.groups["quarter"] or "0")
    if quarter not in {1, 2, 3, 4}:
        raise TemporalAnnotationError(f"invalid quarter: {candidate.expression}")
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    start = date(year, start_month, 1).isoformat()
    end = date(year, end_month, calendar.monthrange(year, end_month)[1]).isoformat()
    return _resolved("interval", start, end, "quarter", confidence=0.97)


def _resolve_season(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    year = int(candidate.groups["year"] or "0")
    season = candidate.groups["season"] or ""
    start_month, end_month = SEASON_MONTHS[season]
    start = date(year, start_month, 1)
    end_year = year + 1 if start_month == 12 else year
    end = date(end_year, end_month, calendar.monthrange(end_year, end_month)[1])
    return _resolved(
        "interval",
        start.isoformat(),
        end.isoformat(),
        "season",
        confidence=0.88,
        approximation=True,
        convention="meteorological_season_v0001",
    )


def _resolve_year(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    start, end = _year_bounds(int(candidate.groups["year"] or "0"))
    return _resolved("interval", start, end, "year", confidence=0.97)


def _resolve_simple_relative(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    if reference is None:
        return _unresolved("relative", "unknown", confidence=0.50)
    unit, offset = SIMPLE_RELATIVE[candidate.expression]
    base = reference.date()
    if unit == "day":
        value = base + timedelta(days=offset)
        return _resolved("relative", value.isoformat(), value.isoformat(), "day", reference=reference, confidence=0.95)
    if unit == "month":
        target = _add_months(base.replace(day=1), offset)
        start, end = _month_bounds(target.year, target.month)
        return _resolved("relative", start, end, "month", reference=reference, confidence=0.93)
    target_year = base.year + offset
    start, end = _year_bounds(target_year)
    return _resolved("relative", start, end, "year", reference=reference, confidence=0.93)


def _resolve_numeric_relative(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    unit = candidate.groups["unit"] or ""
    amount = _chinese_integer(candidate.groups["amount"] or "0")
    direction = -1 if candidate.groups["direction"] == "前" else 1
    if reference is None:
        precision = "month" if unit in {"月", "个月"} else unit
        return _unresolved("relative", precision, confidence=0.45)
    base = reference.date()
    if unit == "天":
        value = base + timedelta(days=direction * amount)
        precision = "day"
    elif unit == "周":
        value = base + timedelta(days=direction * amount * 7)
        precision = "day"
    elif unit in {"月", "个月"}:
        value = _add_months(base, direction * amount)
        precision = "day"
    else:
        value = _add_years(base, direction * amount)
        precision = "day"
    return _resolved("relative", value.isoformat(), value.isoformat(), precision, reference=reference, confidence=0.90)


def _resolve_duration(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    unit = candidate.groups["unit"] or ""
    precision = "month" if unit in {"月", "个月"} else unit
    return TemporalResolution(
        temporal_type="duration",
        normalized_start=None,
        normalized_end=None,
        precision=precision,
        reference_time=None,
        resolution_status="resolved",
        confidence=0.85,
        approximation=False,
        convention=None,
    )


def _resolve_recurring(candidate: TemporalCandidate, reference: Optional[datetime]) -> TemporalResolution:
    return TemporalResolution(
        temporal_type="recurring",
        normalized_start=None,
        normalized_end=None,
        precision=candidate.groups["unit"] or "unknown",
        reference_time=None,
        resolution_status="resolved",
        confidence=0.90,
        approximation=False,
        convention=None,
    )


RULES: Tuple[TemporalRule, ...] = (
    TemporalRule(
        "cn_date_range",
        10,
        re.compile(
            r"(?P<y1>\d{4})年(?P<m1>1[0-2]|0?[1-9])月(?:(?P<d1>3[01]|[12]\d|0?[1-9])日)?"
            r"(?:至|到|—|–|~|～)(?:(?P<y2>\d{4})年)?(?P<m2>1[0-2]|0?[1-9])月"
            r"(?:(?P<d2>3[01]|[12]\d|0?[1-9])日)?"
        ),
        _resolve_cn_range,
    ),
    TemporalRule(
        "cn_full_date",
        20,
        re.compile(r"(?P<year>\d{4})年(?P<month>1[0-2]|0?[1-9])月(?P<day>3[01]|[12]\d|0?[1-9])日"),
        _resolve_cn_date,
    ),
    TemporalRule(
        "iso_full_date",
        21,
        re.compile(r"(?<!\d)(?P<year>\d{4})[-/](?P<month>1[0-2]|0[1-9])[-/](?P<day>3[01]|[12]\d|0[1-9])(?!\d)"),
        _resolve_iso_date,
    ),
    TemporalRule(
        "quarter",
        30,
        re.compile(r"(?P<year>\d{4})年(?:第)?(?P<quarter>[一二三四1-4])季度"),
        _resolve_quarter,
    ),
    TemporalRule(
        "season",
        31,
        re.compile(r"(?P<year>\d{4})年(?P<season>春天|春季|春|夏天|夏季|夏|秋天|秋季|秋|冬天|冬季|冬)"),
        _resolve_season,
    ),
    TemporalRule(
        "year_month",
        40,
        re.compile(r"(?P<year>\d{4})年(?P<month>1[0-2]|0?[1-9])月"),
        _resolve_year_month,
    ),
    TemporalRule(
        "numeric_relative",
        50,
        re.compile(r"(?P<amount>\d+|[零〇一二两三四五六七八九十百千]+)(?P<unit>天|周|个月|月|年)(?P<direction>前|后)"),
        _resolve_numeric_relative,
    ),
    TemporalRule(
        "simple_relative",
        51,
        re.compile("|".join(sorted((re.escape(key) for key in SIMPLE_RELATIVE), key=len, reverse=True))),
        _resolve_simple_relative,
    ),
    TemporalRule(
        "recurring",
        60,
        re.compile(r"每(?P<unit>天|日|周|月|季度|年)"),
        _resolve_recurring,
    ),
    TemporalRule(
        "duration",
        70,
        re.compile(r"(?P<amount>\d+|[零〇一二两三四五六七八九十百千]+)(?P<unit>天|周|个月|月|年)"),
        _resolve_duration,
    ),
    TemporalRule(
        "year",
        65,
        re.compile(r"(?<!\d)(?P<year>\d{4})年"),
        _resolve_year,
    ),
)


# ============================================================
# 核心业务组件
# ============================================================

def _candidates(text: str) -> List[TemporalCandidate]:
    found: List[TemporalCandidate] = []
    for rule in RULES:
        for match in rule.pattern.finditer(text):
            found.append(
                TemporalCandidate(
                    rule_name=rule.name,
                    expression=match.group(0),
                    start=match.start(),
                    end=match.end(),
                    priority=rule.priority,
                    groups=match.groupdict(),
                )
            )
    found.sort(key=lambda item: (item.start, item.priority, -(item.end - item.start)))
    accepted: List[TemporalCandidate] = []
    for candidate in found:
        if any(candidate.start < existing.end and existing.start < candidate.end for existing in accepted):
            continue
        accepted.append(candidate)
    return sorted(accepted, key=lambda item: (item.start, item.end))


def _rule_for(name: str) -> TemporalRule:
    for rule in RULES:
        if rule.name == name:
            return rule
    raise TemporalAnnotationError(f"unknown temporal rule: {name}")


def _annotation(
    record: Mapping[str, Any],
    identity: Mapping[str, Any],
    candidate: TemporalCandidate,
    resolution: TemporalResolution,
    *,
    rule_version: str,
) -> Dict[str, Any]:
    id_payload = {
        "identity": dict(identity),
        "annotation_type": "temporal",
        "span_start": candidate.start,
        "span_end": candidate.end,
        "expression": candidate.expression,
        "annotator_version": SCRIPT_VERSION,
        "rule_version": rule_version,
    }
    evidence = {
        "rule_name": candidate.rule_name,
        "expression": candidate.expression,
        "local_span_start": candidate.start,
        "local_span_end": candidate.end,
        "source_span_start": int(identity["char_start"]) + candidate.start,
        "source_span_end": int(identity["char_start"]) + candidate.end,
    }
    payload = {
        "original_expression": candidate.expression,
        "temporal_type": resolution.temporal_type,
        "normalized_start": resolution.normalized_start,
        "normalized_end": resolution.normalized_end,
        "precision": resolution.precision,
        "reference_time": resolution.reference_time,
        "resolution_status": resolution.resolution_status,
        "source_kind": "derived_rule",
        "confidence": resolution.confidence,
        "evidence": evidence,
        "approximation": resolution.approximation,
        "normalization_convention": resolution.convention,
    }
    return {
        "annotation_id": _stable_hash(id_payload),
        "annotation_type": "temporal",
        "annotation_subtype": resolution.temporal_type,
        "text_unit_id": identity.get("text_unit_id"),
        "asset_id": identity["asset_id"],
        "path": identity["path"],
        "value_index": identity["value_index"],
        "segment_index": identity["segment_index"],
        "sentence_index": identity["sentence_index"],
        "char_start": identity["char_start"],
        "char_end": identity["char_end"],
        "span_start": candidate.start,
        "span_end": candidate.end,
        "source_kind": "derived_rule",
        "annotator": SCRIPT_NAME,
        "annotator_version": SCRIPT_VERSION,
        "rule_version": rule_version,
        "confidence": resolution.confidence,
        "resolution_status": resolution.resolution_status,
        "payload": payload,
    }


def annotate_batch(
    records: Sequence[Mapping[str, Any]],
    run_context: Mapping[str, Any],
    plugin_config: Mapping[str, Any],
) -> Dict[str, Any]:
    rule_version = plugin_config.get("rule_version", DEFAULT_RULE_VERSION)
    if not isinstance(rule_version, str) or not rule_version:
        raise TemporalAnnotationError("rule_version must be a non-empty string")
    text_fields = plugin_config.get("text_fields", ["text", "unit_text"])
    reference_fields = plugin_config.get(
        "reference_time_fields", ["event_time", "message_time", "created_at", "timestamp"]
    )
    if not isinstance(text_fields, list) or not all(isinstance(item, str) and item for item in text_fields):
        raise TemporalAnnotationError("text_fields must be a non-empty string list")
    if not isinstance(reference_fields, list) or not all(
        isinstance(item, str) and item for item in reference_fields
    ):
        raise TemporalAnnotationError("reference_time_fields must be a string list")

    annotations: List[Dict[str, Any]] = []
    detected = 0
    resolved = 0
    unresolved = 0
    ambiguous = 0
    for item in records:
        if not isinstance(item, Mapping):
            raise TemporalAnnotationError("batch record must be a mapping")
        record = item.get("record")
        identity = item.get("stable_identity")
        if not isinstance(record, Mapping) or not isinstance(identity, Mapping):
            raise TemporalAnnotationError("batch record is missing record or stable_identity")
        text = _text_value(record, text_fields)
        reference = _reference_time(record, reference_fields)
        for candidate in _candidates(text):
            resolution = _rule_for(candidate.rule_name).resolver(candidate, reference)
            annotations.append(
                _annotation(record, identity, candidate, resolution, rule_version=rule_version)
            )
            detected += 1
            if resolution.resolution_status == "resolved":
                resolved += 1
            elif resolution.resolution_status == "unresolved":
                unresolved += 1
            else:
                ambiguous += 1

    return {
        "status": "completed",
        "plugin_name": PLUGIN_META["name"],
        "plugin_version": SCRIPT_VERSION,
        "rule_version": rule_version,
        "records_scanned": len(records),
        "annotations": annotations,
        "stats": {
            "expressions_detected": detected,
            "resolved": resolved,
            "unresolved": unresolved,
            "ambiguous": ambiguous,
            "invalid": 0,
        },
    }


# ============================================================
# Schema / 契约辅助函数
# ============================================================

def plugin_summary() -> Dict[str, Any]:
    return {
        "status": "completed",
        "plugin": dict(PLUGIN_META),
        "rule_version": DEFAULT_RULE_VERSION,
        "supported_types": ["point", "interval", "relative", "duration", "recurring"],
        "external_dependencies": [],
    }
