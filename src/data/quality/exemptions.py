#!/usr/bin/env python3

r"""数据契约巡检的「已知违规基线」。

背景
----
通达信历史源数据里长期存在两类无法修复的瑕疵：退市/停更个股尾部的
``volume`` / ``amount`` 空值，以及 1991-1995 年少数指数的 OHLC 越界。
它们数量大且恒定，若每次巡检都计为 error，``--fail-on-error`` 门禁会
长期为红，真正的新回归反而被淹没。

本模块提供**精确到行**的豁免基线：

- 指纹为 ``(rule_id, ts_code, trade_date)``，不是按标的整体豁免；
  任何新增日期、新增规则、新增标的的违规仍会触发门禁失败。
- 每条记录带 ``reason``（登记理由）与 ``created_at``，可选 ``expires``
  （到期日，过期自动失效，防止基线永久化）。
- 违规**照常记录**在报告里，只是不计入 ``actionable_errors``；
  ``--fail-on-error`` 改用该字段判定，审计信息不丢失。

基线文件位于 ``data/local_store/meta/anomalies/quality_exemptions.csv``，
属于**不可重建资产**：文件缺失时门禁退回「全部 error 都算失败」的
fail-closed 行为，不会静默放行。

只读、无网络、无数据库访问；文件按 mtime 缓存，写入走原子替换。
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from src.data.atomic_write import write_text_atomic

#: 项目根目录（src/data/quality/ -> 上溯 3 层）
PROJECT_ROOT = Path(__file__).resolve().parents[3]

#: 默认基线文件；与异常台账清单同目录，被 .gitignore 排除
DEFAULT_EXEMPTION_PATH = (
    PROJECT_ROOT / 'data' / 'local_store' / 'meta'
    / 'anomalies' / 'quality_exemptions.csv'
)

FIELDNAMES = ('rule_id', 'ts_code', 'trade_date', 'reason', 'created_at', 'expires')

#: 无具体交易日的违规（例如整列缺失）使用的通配标记
NO_DATE = '*'


@dataclass(frozen=True)
class ExemptionRecord:
    """一条已确认的历史违规指纹。"""

    rule_id: str
    ts_code: str
    trade_date: str
    reason: str
    created_at: str
    expires: str = ''

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.rule_id, self.ts_code.upper(), self.trade_date)

    def active_on(self, today: date) -> bool:
        """是否仍在有效期内；空 expires 表示长期有效。"""
        if not self.expires:
            return True
        try:
            return today <= date.fromisoformat(self.expires)
        except ValueError:
            return False


def normalize_trade_date(value: Any) -> str:
    """把 date/datetime/str/None 归一为 ISO 日期或 :data:`NO_DATE`。"""
    if value is None or value == '':
        return NO_DATE
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10] or NO_DATE


def read_exemptions(path: Path | str = DEFAULT_EXEMPTION_PATH) -> tuple[ExemptionRecord, ...]:
    """读取基线文件；文件不存在返回空元组，坏行跳过。"""
    target = Path(path)
    if not target.is_file():
        return ()
    records: list[ExemptionRecord] = []
    with target.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not set(FIELDNAMES).issubset(reader.fieldnames):
            return ()
        for row in reader:
            rule_id = (row.get('rule_id') or '').strip()
            ts_code = (row.get('ts_code') or '').strip()
            trade_date = (row.get('trade_date') or '').strip()
            reason = (row.get('reason') or '').strip()
            if not rule_id or not ts_code or not trade_date or not reason:
                continue
            records.append(ExemptionRecord(
                rule_id=rule_id,
                ts_code=ts_code,
                trade_date=trade_date,
                reason=reason,
                created_at=(row.get('created_at') or '').strip(),
                expires=(row.get('expires') or '').strip(),
            ))
    return tuple(records)


def write_exemptions(records: Iterable[ExemptionRecord],
                     path: Path | str = DEFAULT_EXEMPTION_PATH) -> int:
    """去重排序后原子写入基线文件，返回落盘条数。"""
    target = Path(path)
    unique = {record.key: record for record in records}
    ordered = sorted(unique.values(), key=lambda item: item.key)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(FIELDNAMES))
    writer.writeheader()
    for record in ordered:
        writer.writerow({
            'rule_id': record.rule_id,
            'ts_code': record.ts_code,
            'trade_date': record.trade_date,
            'reason': record.reason,
            'created_at': record.created_at,
            'expires': record.expires,
        })
    write_text_atomic(buffer.getvalue(), target)
    return len(ordered)


def build_baseline(violations: Iterable[Any], *, reason: str,
                   expires: str = '',
                   created_at: str | None = None) -> tuple[ExemptionRecord, ...]:
    """从一次巡检的 error 级违规生成基线记录。

    参数是 duck-typed 的（``QualityViolation`` 或任何含
    ``rule_id / severity / ts_code / trade_date`` 的对象），避免与
    :mod:`src.data.quality.checker` 形成循环导入。
    """
    if not reason.strip():
        raise ValueError('登记基线必须提供非空 reason')
    stamp = created_at or datetime.now().isoformat(timespec='seconds')
    records: dict[tuple[str, str, str], ExemptionRecord] = {}
    for violation in violations:
        if getattr(violation, 'severity', None) != 'error':
            continue
        record = ExemptionRecord(
            rule_id=str(getattr(violation, 'rule_id', '')),
            ts_code=str(getattr(violation, 'ts_code', '')),
            trade_date=normalize_trade_date(getattr(violation, 'trade_date', None)),
            reason=reason.strip(),
            created_at=stamp,
            expires=expires.strip(),
        )
        if not record.rule_id or not record.ts_code:
            continue
        records[record.key] = record
    return tuple(sorted(records.values(), key=lambda item: item.key))


class ExemptionIndex:
    """基线指纹集合，按文件 mtime 自动失效。"""

    def __init__(self, path: Path | str = DEFAULT_EXEMPTION_PATH) -> None:
        self.path = Path(path)
        self._keys: frozenset[tuple[str, str, str]] = frozenset()
        self._mtime_ns: int | None = None
        self._lock = RLock()

    def _refresh(self) -> None:
        if not self.path.is_file():
            with self._lock:
                if self._mtime_ns is not None:
                    self._keys = frozenset()
                    self._mtime_ns = None
            return
        mtime = self.path.stat().st_mtime_ns
        if self._mtime_ns == mtime:
            return
        today = date.today()
        keys = frozenset(
            record.key for record in read_exemptions(self.path)
            if record.active_on(today)
        )
        with self._lock:
            self._keys = keys
            self._mtime_ns = mtime

    def is_exempt(self, rule_id: str, ts_code: str, trade_date: Any) -> bool:
        """单条违规是否已登记为已知基线。"""
        self._refresh()
        key = (rule_id, str(ts_code).upper(), normalize_trade_date(trade_date))
        return key in self._keys

    @property
    def size(self) -> int:
        self._refresh()
        return len(self._keys)


_INDEX = ExemptionIndex()


def get_exemption_index() -> ExemptionIndex:
    """返回进程级共享索引。"""
    return _INDEX


def is_exempt(rule_id: str, ts_code: str, trade_date: Any) -> bool:
    """便捷查询入口。"""
    return _INDEX.is_exempt(rule_id, ts_code, trade_date)


__all__ = [
    'DEFAULT_EXEMPTION_PATH',
    'ExemptionIndex',
    'ExemptionRecord',
    'NO_DATE',
    'build_baseline',
    'get_exemption_index',
    'is_exempt',
    'normalize_trade_date',
    'read_exemptions',
    'write_exemptions',
]
