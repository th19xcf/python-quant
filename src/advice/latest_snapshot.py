#!/usr/bin/env python3

"""``reports/advice/latest.json`` 的唯一 schema 与读写实现。

CLI（``src/advice/cli.py``）与 GUI（``src/core/usecases/get_daily_recommendations.py``）
写同一个路径，必须以同一套字段落盘：字段集合、``items`` 结构与「可下单」判据
都只在这里定义，两侧只负责传入同一个 ``AdviceResult``。

读侧兼容旧格式：旧文件缺 ``exec_status`` / ``usage_boundary`` / ``disposition``
时按可推导规则补齐并写入 ``load_warnings``，不抛错、不返回 ``None``。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.advice.config import OBSERVE_ONLY_TIERS
from src.advice.daily_store import (
    EXECUTION_BASIS,
    REF_PRICE_BASIS,
    action_label,
    is_actionable,
    next_trading_day,
    resolve_exec_status,
    usage_boundary,
)
from src.advice.models import AdviceResult
from src.data.atomic_write import write_text_atomic

#: 快照 schema 版本；字段增删必须同步升版本，读侧据此判定旧格式
SCHEMA_VERSION = 2

#: 顶层字段全集（两个写者必须全部写出，缺任何一个即视为 schema 分叉）
LATEST_KEYS: tuple[str, ...] = (
    'schema_version', 'usage_boundary', 'signal_date', 'earliest_exec_date',
    'exec_status', 'exec_date_basis', 'trade_date', 'generated_at', 'equity',
    'is_sample', 'ref_price_basis', 'execution_basis', 'notes', 'tiers', 'items',
)

#: 单条推荐字段全集（= ``models.OUTPUT_COLUMNS`` 的导出列 + 快照侧补充字段）
ITEM_KEYS: tuple[str, ...] = (
    'trade_date', 'ts_code', 'name', 'name_source', 'direction', 'disposition',
    'target_weight_pct', 'shares', 'ref_price', 'horizon', 'validity_days',
    'confidence', 'top_factors', 'risk_notes', 'industry', 'industry_source',
    'score', 'score_percentile',
    # T 日涨停标注：由 Recommendation.to_row() 产出、属单条推荐输出项，
    # 不是 tiers 汇总字段，也不在顶层
    'limit_up_risk', 'limit_up_basis',
    'tier', 'tier_label', 'rank', 'signal_date',
    'earliest_exec_date', 'exec_status', 'action', 'actionable',
    'defer_reason', 'defer_until',
)

#: 读侧补齐的告警键（只在读取时出现，不落盘）
LOAD_WARNINGS_KEY = 'load_warnings'


def build_latest_payload(result: AdviceResult, *,
                         generated_at: datetime | None = None,
                         equity: float | None = None,
                         earliest_exec_date: date | None = None,
                         exec_date_basis: str | None = None,
                         is_sample: bool = False) -> dict[str, Any]:
    """把引擎结果压成 ``latest.json`` 的唯一结构（两个入口共用）。

    执行时点、可下单判据与动作标签都复用 ``daily_store`` 的同一批函数，
    避免「归档说延后、快照说可下单」这类同日两份结论。
    """
    gen = generated_at or datetime.now()
    signal_day = result.signal_date or result.trade_date
    if earliest_exec_date is None:
        earliest_exec_date = result.earliest_exec_date
    if earliest_exec_date is None:
        earliest_exec_date, default_basis = next_trading_day(signal_day)
        basis = exec_date_basis or default_basis
    else:
        basis = exec_date_basis or result.exec_date_basis
    status = resolve_exec_status(earliest_exec_date, gen)
    expired = status == 'expired'

    tiers: dict[str, Any] = {}
    items: list[dict[str, Any]] = []
    for key, advice in result.tiers.items():
        summary = dict(advice.summary())
        summary['observe_only'] = key in OBSERVE_ONLY_TIERS
        tiers[key] = summary
        rows = sorted(advice.rows,
                      key=lambda rec: rec.score_percentile, reverse=True)
        for rank, rec in enumerate(rows, start=1):
            row = rec.to_row()
            row.update({
                'tier': key,
                'tier_label': advice.label,
                'rank': rank,
                'signal_date': signal_day.isoformat(),
                'earliest_exec_date': earliest_exec_date.isoformat(),
                'exec_status': status,
                'action': action_label(rec.direction, rec.disposition,
                                       expired=expired),
                'actionable': is_actionable(rec.direction, rec.disposition,
                                            expired=expired),
                'defer_reason': rec.defer_reason or None,
                'defer_until': (rec.defer_until.isoformat()
                                if rec.defer_until else None),
            })
            items.append(row)

    notes = [
        'ref_price 为推荐日未复权收盘价（raw_close），非复权价',
        f'signal_date={signal_day.isoformat()}；earliest_exec_date='
        f'{earliest_exec_date.isoformat()}；exec_status={status}',
        '动作在 T+1 开盘执行；涨跌停/停牌当日不可成交',
        'exec_status 为生成时点判定；隔日读取请用 earliest_exec_date 与当前日期复核',
    ]
    if expired:
        notes.append('exec_status=expired：T+1 开盘已过，BUY/SELL 不得按“可买/可卖”使用，'
                     '仅保留信号供复盘')

    return {
        'schema_version': SCHEMA_VERSION,
        'usage_boundary': usage_boundary(),
        'signal_date': signal_day.isoformat(),
        'earliest_exec_date': earliest_exec_date.isoformat(),
        'exec_status': status,
        'exec_date_basis': basis,
        'trade_date': result.trade_date.isoformat(),
        'generated_at': gen.isoformat(timespec='seconds'),
        'equity': round(float(equity if equity is not None else result.equity), 2),
        'is_sample': bool(is_sample),
        'ref_price_basis': REF_PRICE_BASIS,
        'execution_basis': EXECUTION_BASIS,
        'notes': notes,
        'tiers': tiers,
        'items': items,
    }


def write_latest_snapshot(result: AdviceResult, path: Path | str, *,
                          generated_at: datetime | None = None,
                          equity: float | None = None,
                          earliest_exec_date: date | None = None,
                          exec_date_basis: str | None = None,
                          is_sample: bool = False) -> Path:
    """原子覆盖写最新快照；返回落盘路径。"""
    target = Path(path)
    payload = build_latest_payload(
        result, generated_at=generated_at, equity=equity,
        earliest_exec_date=earliest_exec_date,
        exec_date_basis=exec_date_basis, is_sample=is_sample)
    write_text_atomic(
        json.dumps(payload, ensure_ascii=False, indent=2), target)
    return target


def load_latest_snapshot(path: Path | str) -> dict[str, Any] | None:
    """读取最新快照；文件缺失或不可解析返回 ``None``，旧格式降级读出。"""
    target = Path(path)
    if not target.is_file():
        return None
    try:
        raw = json.loads(target.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(raw, dict):
        return None
    return normalize_latest_payload(raw)


def normalize_latest_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """把任意历史版本的快照补齐成当前 schema，缺字段按规则推导而非抛错。"""
    payload = dict(raw)
    warnings: list[str] = []
    if not isinstance(payload.get('schema_version'), int):
        payload['schema_version'] = 1
        warnings.append('旧快照缺 schema_version，按 v1 处理')

    _normalize_dates(payload, warnings)
    status = _normalize_exec_status(payload, warnings)
    expired = status == 'expired'

    if not payload.get('usage_boundary'):
        payload['usage_boundary'] = usage_boundary()
        warnings.append('旧快照缺 usage_boundary，已补当前使用边界声明')
    payload.setdefault('ref_price_basis', REF_PRICE_BASIS)
    payload.setdefault('execution_basis', EXECUTION_BASIS)
    payload.setdefault('is_sample', False)
    payload.setdefault('notes', [])
    payload.setdefault('equity', 1_000_000.0)
    if not isinstance(payload.get('tiers'), dict):
        payload['tiers'] = {}

    payload['items'] = [
        _normalize_item(item, status, expired, warnings)
        for item in payload.get('items') or [] if isinstance(item, dict)
    ]
    payload[LOAD_WARNINGS_KEY] = warnings
    return payload


def _normalize_dates(payload: dict[str, Any], warnings: list[str]) -> None:
    """补齐 signal_date / trade_date / earliest_exec_date / exec_date_basis。"""
    signal = payload.get('signal_date') or payload.get('trade_date')
    if not payload.get('signal_date'):
        payload['signal_date'] = signal
        warnings.append('旧快照缺 signal_date，已用 trade_date 补齐')
    if not payload.get('trade_date') and signal:
        payload['trade_date'] = signal

    earliest = payload.get('earliest_exec_date')
    basis = payload.get('exec_date_basis')
    if not earliest and signal:
        try:
            earliest_day, default_basis = next_trading_day(
                date.fromisoformat(str(signal)[:10]))
        except ValueError:
            pass
        else:
            earliest = earliest_day.isoformat()
            basis = basis or default_basis
            warnings.append('旧快照缺 earliest_exec_date，已按下一交易日推导')
    payload['earliest_exec_date'] = earliest
    payload['exec_date_basis'] = basis or 'weekday_fallback'
    if not earliest:
        warnings.append('旧快照缺 earliest_exec_date，执行窗口无法推导，按不可下单处理')


def _normalize_exec_status(payload: dict[str, Any],
                           warnings: list[str]) -> str:
    """读已有的 exec_status；缺失时按日期推导，推不出保守判 expired。"""
    status = payload.get('exec_status')
    if status not in ('pending', 'expired'):
        status = _derive_status(payload.get('earliest_exec_date'),
                                payload.get('generated_at'))
        warnings.append(f'旧快照缺 exec_status，已推导为 {status}')
    payload['exec_status'] = status
    return status


def _normalize_item(item: dict[str, Any], status: str, expired: bool,
                    warnings: list[str]) -> dict[str, Any]:
    """补齐单条推荐的 disposition / action / actionable，规则与写入侧一致。"""
    row = dict(item)
    row.setdefault('tier', 'mid')
    row.setdefault('tier_label', '')
    if not row.get('disposition'):
        row['disposition'] = 'active'
        warnings.append(
            f"{row.get('ts_code', '?')} 旧快照缺 disposition，按 active 处理")
    if row.get('exec_status') not in ('pending', 'expired'):
        row['exec_status'] = status
    row_expired = row['exec_status'] == 'expired' or expired
    direction = str(row.get('direction') or '')
    if not row.get('action'):
        row['action'] = action_label(
            direction, str(row['disposition']), expired=row_expired)
    if not isinstance(row.get('actionable'), bool):
        row['actionable'] = is_actionable(
            direction, str(row['disposition']), expired=row_expired)
    row.setdefault('defer_reason', None)
    row.setdefault('defer_until', None)
    return row


def _derive_status(earliest: Any, generated_at: Any) -> str:
    """按 ``earliest_exec_date`` 与生成时刻推导执行状态；推不出即保守判 expired。"""
    try:
        earliest_day = date.fromisoformat(str(earliest)[:10])
    except (TypeError, ValueError):
        return 'expired'
    try:
        stamp = datetime.fromisoformat(str(generated_at))
    except (TypeError, ValueError):
        stamp = datetime.now()
    return resolve_exec_status(earliest_day, stamp)
