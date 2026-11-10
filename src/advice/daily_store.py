#!/usr/bin/env python3

"""每日推荐归档：一个交易日一份 JSON，可幂等重写。

设计约束：

- **不动 ``state/*.json``**：归档只读 ``AdviceResult``，不参与 L1/L2 学习链路；
- **一天一份**：路径 ``{daily_dir}/{YYYYMMDD}.json``，同一日重跑直接原子覆盖，
  不会产生重复记录（幂等）；
- **推荐价口径写死**：``ref_price`` 为推荐日**未复权收盘价**（人工下单看到的价格），
  成交口径为**下一交易日的开盘**（见 ``execution_basis``），两者都写进文件；
- **执行时点显式化**：``signal_date``（信号日/面板最后交易日）、
  ``earliest_exec_date``（最早可执行日 T+1）、``exec_status``
  （``pending`` / ``expired``）。生成时若 T+1 开盘已过即标 ``expired``，
  清单里的 BUY/SELL 不得再带「可买/可卖」语义；
- 归档目录默认 ``reports/advice/daily``，与 ``latest.json``（最新快照，覆盖写）
  和 ``state/``（最新持仓，覆盖写）分离。

字段：``signal_date / earliest_exec_date / exec_status / trade_date / tier /
rank / ts_code / direction / disposition / action / score / score_percentile /
target_weight_pct / shares / ref_price / ref_price_basis / validity_days /
confidence / industry``。
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable

from src.advice.models import (
    DIRECTION_AVOID,
    DIRECTION_BUY,
    DIRECTION_HOLD,
    DIRECTION_SELL,
    AdviceResult,
)

#: ``ref_price`` 的口径：推荐日收盘价（未复权）
REF_PRICE_BASIS = 'close'
#: 推荐动作对应的成交口径：信号日 T 收盘生成，T+1 开盘执行
EXECUTION_BASIS = 'next_open'

#: 回放结果归档路径（相对项目根），供使用边界动态引用
REPLAY_SUMMARY_REL = Path('reports/advice/backtest/replay_summary.json')


def usage_boundary(project_root: Path | None = None) -> dict[str, Any]:
    """使用边界声明，写入归档/最新快照/汇总的头部。

    回放数字从 ``replay_summary.json`` 动态读取，不在代码里写死；文件缺失
    或不可读时只给出来源路径，避免文案随重跑结果过期。
    """
    root = project_root or Path(__file__).resolve().parents[2]
    validation = (f'3 个月回放结果见 {REPLAY_SUMMARY_REL.as_posix()}；'
                  '没有样本外 / walk-forward 组合验证')
    try:
        payload = json.loads(
            (root / REPLAY_SUMMARY_REL).read_text(encoding='utf-8'))
        keys = [key for key in ('short', 'mid', 'long') if key in payload]
        if keys:
            start = payload[keys[0]].get('start')
            end = payload[keys[0]].get('end')
            returns = ' / '.join(
                f"{key} {float(payload[key]['net_return']) * 100:+.2f}%"
                for key in keys)
            window = f'{start}~{end}' if start and end else ''
            validation = (f'3 个月回放净收益（{window}）：{returns}；'
                          '没有样本外 / walk-forward 组合验证')
    except (OSError, ValueError, TypeError, KeyError):
        pass
    return {
        'confidence': ('0.30/0.40/0.25 因子权重合成的启发式分数，不是概率、'
                       '未经概率校准，不能解读为“赚钱概率”'),
        'stop_loss': ('系统没有止损字段，不提供止损价、也不提供目标价；'
                      '风险控制需使用者自行设定'),
        'return_claim': ('不得把推荐输出转述成“年化 X%”或任何收益承诺；'
                         '推荐清单只做候选排序，不构成收益预测'),
        'validation': validation,
    }

ACTION_LABELS = {
    DIRECTION_BUY: '买入',
    DIRECTION_SELL: '调出',
    DIRECTION_HOLD: '持有',
    DIRECTION_AVOID: 'AVOID',
}


def action_label(direction: str, disposition: str = 'active', *,
                 expired: bool = False) -> str:
    """动作标签的唯一实现：归档、``latest.json``、GUI 表格共用同一判据。

    延后优先于过期——``deferred`` 行本就不该执行，标「延后」比标「已过期」更准确。
    """
    label = ACTION_LABELS.get(direction, direction)
    if disposition == 'deferred':
        return f'{label}(延后)'
    if expired and direction in (DIRECTION_BUY, DIRECTION_SELL):
        return f'{label}(已过期)'
    return label


def is_actionable(direction: str, disposition: str = 'active', *,
                  expired: bool = False) -> bool:
    """能否按「可买/可卖」使用：非延后、未过期、且方向为 BUY/SELL。"""
    return (not expired and disposition != 'deferred'
            and direction in (DIRECTION_BUY, DIRECTION_SELL))


#: A 股开盘时间；生成时点已过该时刻且 earliest_exec_date 为当天 → expired
MARKET_OPEN = time(9, 30)


def next_trading_day(day: date,
                     known_dates: Iterable[date] | None = None) -> tuple[date, str]:
    """返回 ``(下一交易日, 来源)``。

    优先用本地已知交易日序列里晚于 ``day`` 的第一天；若信号日已是最新一天
    （本地没有未来日历），退回「下一工作日」，并把来源标为
    ``weekday_fallback``，提醒使用者在节假日前人工复核。
    """
    future = sorted({item for item in (known_dates or []) if item > day})
    if future:
        return future[0], 'calendar'
    candidate = day + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate, 'weekday_fallback'


def resolve_exec_status(earliest_exec_date: date,
                        generated_at: datetime) -> str:
    """按生成时刻判定执行窗口：``pending`` / ``expired``。

    只要 T+1 开盘（09:30）已过，该信号就不能再按「可买/可卖」使用；
    仅比较日期会让「当天生成、当天开盘价已错过」被误判为可执行。
    """
    gen_day = generated_at.date()
    if earliest_exec_date < gen_day:
        return 'expired'
    if earliest_exec_date == gen_day and generated_at.time() >= MARKET_OPEN:
        return 'expired'
    return 'pending'


def default_daily_dir(project_root: Path | None = None) -> Path:
    """默认归档目录 ``reports/advice/daily``。"""
    root = project_root or Path(__file__).resolve().parents[2]
    return root / 'reports' / 'advice' / 'daily'


def snapshot_path(daily_dir: Path | str, trade_date: date) -> Path:
    """单日归档路径 ``{daily_dir}/{YYYYMMDD}.json``。"""
    return Path(daily_dir) / f'{trade_date:%Y%m%d}.json'


def build_payload(result: AdviceResult,
                  generated_at: datetime | None = None,
                  earliest_exec_date: date | None = None,
                  exec_date_basis: str | None = None) -> dict[str, Any]:
    """把一次推荐结果压成按日归档结构（每个档位的行按分数降序带 rank）。

    头部固定写出 ``signal_date / earliest_exec_date / exec_status``，
    并带上 ``usage_boundary``，避免“次日开盘”这类相对词被事后误读。
    """
    gen = generated_at or datetime.now()
    signal_day = result.signal_date or result.trade_date
    if earliest_exec_date is None:
        earliest_exec_date = result.earliest_exec_date
    if earliest_exec_date is None:
        earliest_exec_date, basis = next_trading_day(signal_day)
    else:
        basis = exec_date_basis or result.exec_date_basis
    status = resolve_exec_status(earliest_exec_date, gen)
    expired = status == 'expired'

    payload: dict[str, Any] = {
        'usage_boundary': usage_boundary(),
        'signal_date': signal_day.isoformat(),
        'earliest_exec_date': earliest_exec_date.isoformat(),
        'exec_status': status,
        'exec_date_basis': exec_date_basis or basis,
        'trade_date': result.trade_date.isoformat(),
        'generated_at': gen.isoformat(timespec='seconds'),
        'equity': round(float(result.equity), 2),
        'ref_price_basis': REF_PRICE_BASIS,
        'execution_basis': EXECUTION_BASIS,
        'notes': [
            'ref_price 为推荐日未复权收盘价（raw_close），非复权价',
            f'signal_date={signal_day.isoformat()}；earliest_exec_date='
            f'{earliest_exec_date.isoformat()}；exec_status={status}',
            '动作在 T+1 开盘执行；涨跌停/停牌当日不可成交',
            'exec_status 为生成时点判定；隔日读取请用 earliest_exec_date 与当前日期复核',
        ],
        'tiers': [],
    }
    if expired:
        payload['notes'].append(
            'exec_status=expired：T+1 开盘已过，BUY/SELL 不得按“可买/可卖”使用，'
            '仅保留信号供复盘')

    row_merge_total = 0
    tier_merges: list[dict[str, Any]] = []
    rows_total = 0
    codes: set[str] = set()
    for key, advice in result.tiers.items():
        rows = sorted(advice.rows,
                      key=lambda rec: rec.score_percentile, reverse=True)
        rows_total += len(rows)
        codes.update(rec.ts_code for rec in rows)
        if advice.row_merge:
            row_merge_total += len(advice.row_merge)
            tier_merges.append({
                'tier': key,
                'dropped': len(advice.row_merge),
                'details': advice.row_merge[:20],
            })
        items = []
        for rank, rec in enumerate(rows, start=1):
            action = action_label(rec.direction, rec.disposition, expired=expired)
            items.append({
                'signal_date': signal_day.isoformat(),
                'earliest_exec_date': earliest_exec_date.isoformat(),
                'exec_status': status,
                'trade_date': result.trade_date.isoformat(),
                'rank': rank,
                'ts_code': rec.ts_code,
                'name': rec.name,
                'direction': rec.direction,
                'disposition': rec.disposition,
                'action': action,
                'actionable': is_actionable(
                    rec.direction, rec.disposition, expired=expired),
                'defer_reason': rec.defer_reason,
                'defer_until': (rec.defer_until.isoformat()
                                if rec.defer_until else None),
                'score': round(float(rec.score), 4),
                'score_percentile': round(float(rec.score_percentile), 4),
                'top_factors': ' | '.join(
                    f'{name} {value:+.2f}' for name, value in rec.top_factors[:3]),
                'risk_notes': '；'.join(rec.risk_notes),
                'target_weight_pct': round(float(rec.target_weight) * 100, 2),
                'shares': int(rec.shares),
                'ref_price': round(float(rec.ref_price), 3),
                'ref_price_basis': REF_PRICE_BASIS,
                'validity_days': int(rec.validity_days),
                'confidence': round(float(rec.confidence), 3),
                'industry': rec.industry,
            })
        payload['tiers'].append({
            'tier': key,
            'label': advice.label,
            'counts': _counts(rows),
            'row_merge': {
                'dropped': len(advice.row_merge),
                'details': advice.row_merge[:20],
            },
            'items': items,
        })
    payload['row_merge'] = {
        'dropped': row_merge_total,
        'unique_codes': len(codes),
        'rows_after': rows_total,
        'by_tier': tier_merges,
        'rule': '合并键=(档位, 代码)；冲突优先级 BUY > SELL > HOLD > AVOID',
    }
    return payload


def _counts(rows: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rec in rows:
        counts[rec.direction] = counts.get(rec.direction, 0) + 1
    return counts


def save_daily_snapshot(result: AdviceResult, daily_dir: Path | str,
                        generated_at: datetime | None = None,
                        earliest_exec_date: date | None = None,
                        exec_date_basis: str | None = None) -> Path:
    """原子写入当日归档；同日重跑覆盖同一文件（幂等）。"""
    path = snapshot_path(daily_dir, result.trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload(result, generated_at, earliest_exec_date,
                            exec_date_basis)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                   encoding='utf-8')
    os.replace(tmp, path)
    return path


def load_daily_snapshot(daily_dir: Path | str, trade_date: date) -> dict[str, Any] | None:
    """读取指定交易日的归档；不存在返回 ``None``。"""
    path = snapshot_path(daily_dir, trade_date)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def list_daily_snapshots(daily_dir: Path | str) -> list[dict[str, Any]]:
    """列出归档目录里已有的交易日（升序），附带各档条数。"""
    directory = Path(daily_dir)
    if not directory.is_dir():
        return []
    entries: list[dict[str, Any]] = []
    for path in sorted(directory.glob('[0-9]' * 8 + '.json')):
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            continue
        counts = {item['tier']: sum(item.get('counts', {}).values())
                  for item in payload.get('tiers', [])}
        entries.append({
            'trade_date': payload.get('trade_date', path.stem),
            'path': str(path),
            'items': counts,
        })
    return entries
