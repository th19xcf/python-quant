#!/usr/bin/env python3

"""生产侧滚动 IC 失效监控。

每日推荐收尾时，用**已加载的历史面板**复算冻结信号（``score``）对未来 h 日
收益的 RankIC，按 ``window_days`` 个交易日聚合成滚动窗口，交给
``factor_eval.ic_monitor`` 判定是否失效。

点时约束：只用 t 日及以前的特征 + 已实现的未来收益；最后 h 个交易日的标签
尚未实现（``shift(-h)`` 为 null），不参与统计，因此不会偷看未来。

输出：
- ``reports/advice/ic_monitor/{YYYYMMDD}_{tier}.json``：当日快照（窗口序列+判定）
- ``reports/advice/ic_monitor/history.jsonl``：逐日追加，便于事后追溯
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.advice.factor_eval import MIN_PAIRS, _rank_ic, ic_monitor

DEFAULT_WINDOW_DAYS = 20
DEFAULT_CONSECUTIVE = 3
DEFAULT_THRESHOLD = 0.0
# ``MIN_PAIRS``（单窗口最少有效样本对）唯一真源在 ``src.advice.factor_eval``，
# 此处只重导出：监控与评估若各写一份，同一窗口一边判「样本不足」一边判「失效」。
#: 尾部不满窗口至少要有半个窗口的天数才参与判定，避免 1~2 天的残窗误报
DEFAULT_MIN_DAYS: int | None = None

#: 各档位的标签视界（交易日），与持有期而不是有效期对齐
TIER_HORIZON: dict[str, int] = {'short': 5, 'mid': 20, 'long': 60}


def _monitor_dir() -> Path:
    path = Path(__file__).resolve().parents[2] / 'reports' / 'advice' / 'ic_monitor'
    path.mkdir(parents=True, exist_ok=True)
    return path


def rolling_ic_series(scored: pl.DataFrame, horizon: int,
                      window_days: int = DEFAULT_WINDOW_DAYS,
                      min_pairs: int = MIN_PAIRS,
                      min_days: int | None = DEFAULT_MIN_DAYS,
                      ) -> tuple[list[dict[str, Any]], list[float]]:
    """冻结分数 vs 已实现未来收益 → 滚动窗口 IC 均值序列。

    ``min_days`` 控制尾部残窗是否参与判定（默认 ``window_days // 2``）：
    最后一个窗口常因标签缺失只剩几天，若直接进判定会被单日噪声触发误报。
    残窗仍保留在明细里（便于追溯），但不计入返回的均值序列。

    Returns:
        (窗口明细, 参与判定的窗口 IC 均值列表)；明细含起止日期、天数、
        均值与最小/最大日 IC。
    """
    if scored.is_empty() or 'score' not in scored.columns:
        return [], []
    label = '__fwd_monitor'
    frame = scored.select(['date', 'ts_code', 'score', 'close']).sort(['ts_code', 'date'])
    frame = frame.with_columns(
        (pl.col('close').shift(-horizon).over('ts_code') / pl.col('close') - 1.0)
        .alias(label))
    ic = _rank_ic(frame, 'score', label)
    if ic.is_empty():
        return [], []
    # group_by 的输出顺序不保证；滚动窗口必须按交易日切块
    # 截面退化（corr 为 null/NaN）的日子不参与窗口，避免污染均值
    ic = ic.filter(pl.col('ic').is_not_null() & pl.col('ic').is_finite()) \
        .sort('date')

    windowed = ic.with_row_index('_i').with_columns(
        (pl.col('_i') // window_days).alias('_window'))
    grouped = windowed.group_by('_window').agg([
        pl.col('date').min().alias('start'),
        pl.col('date').max().alias('end'),
        pl.len().alias('days'),
        pl.col('ic').mean().alias('ic_mean'),
        pl.col('ic').min().alias('ic_min'),
        pl.col('ic').max().alias('ic_max'),
    ]).sort('_window')

    rows: list[dict[str, Any]] = [
        {
            'start': str(item['start']),
            'end': str(item['end']),
            'days': int(item['days']),
            'ic_mean': float(item['ic_mean']),
            'ic_min': float(item['ic_min']),
            'ic_max': float(item['ic_max']),
        }
        for item in grouped.to_dicts()
    ]
    floor = max(1, window_days // 2) if min_days is None else max(1, min_days)
    values: list[float] = [float(row['ic_mean']) for row in rows
                           if row['days'] >= floor]
    return rows, values


def run_ic_monitor(scored: pl.DataFrame, tier: str, *, trade_date: date,
                   horizon: int | None = None,
                   window_days: int = DEFAULT_WINDOW_DAYS,
                   threshold: float = DEFAULT_THRESHOLD,
                   consecutive: int = DEFAULT_CONSECUTIVE,
                   min_days: int | None = DEFAULT_MIN_DAYS,
                   out_dir: Path | None = None,
                   persist: bool = True) -> dict[str, Any]:
    """计算并落盘某一档位的 IC 监控快照，返回判定结果。

    告警条件（与评估脚本一致）：
    - 最近 ``consecutive`` 个窗口的 IC 均值全部 < 0；或
    - 最近 ``consecutive`` 个窗口的均值 < ``threshold``（默认 0，即前者覆盖）。

    ``min_days`` 默认取半个窗口：尾部因标签缺失只剩 1~2 天的残窗不参与判定。
    """
    label = horizon or TIER_HORIZON.get(tier, 20)
    floor = max(1, window_days // 2) if min_days is None else max(1, min_days)
    windows, values = rolling_ic_series(scored, label, window_days,
                                       min_days=floor)
    status = ic_monitor(pl.Series('ic', values) if values else pl.Series('ic', [],
                                                                       dtype=pl.Float64),
                        threshold=threshold, consecutive=consecutive)
    snapshot = {
        'trade_date': str(trade_date),
        'tier': tier,
        'horizon_days': label,
        'window_days': window_days,
        'min_window_days': floor,
        'threshold': threshold,
        'consecutive': consecutive,
        'n_windows': len(windows),
        'n_usable_windows': len(values),
        'latest_window_ic': values[-1] if values else None,
        'recent_ic': values[-consecutive:],
        'windows': windows,
        'status': status,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
    }
    message = (f'[IC监控] tier={tier} horizon={label} '
               f'窗口={window_days}日 近{consecutive}窗均值='
               f'{snapshot["recent_ic"]} 判定={status["reason"]}')
    if status['failed']:
        logger.warning(message)
    else:
        logger.info(message)

    if persist:
        directory = out_dir or _monitor_dir()
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f'{trade_date:%Y%m%d}_{tier}.json'
        path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2),
                        encoding='utf-8')
        with (directory / 'history.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps({
                'trade_date': snapshot['trade_date'], 'tier': tier,
                'horizon_days': label, 'n_windows': len(windows),
                'n_usable_windows': len(values),
                'recent_ic': snapshot['recent_ic'],
                'failed': status['failed'], 'reason': status['reason'],
                'generated_at': snapshot['generated_at'],
            }, ensure_ascii=False) + '\n')
        snapshot['path'] = str(path)
    return snapshot


def monitor_note(snapshot: dict[str, Any]) -> str:
    """把监控结果压成一条可写进推荐清单的备注。"""
    status = snapshot['status']
    recent = snapshot.get('recent_ic') or []
    tail = '、'.join(f'{value:+.3f}' for value in recent) if recent else '无'
    flag = '告警' if status['failed'] else '正常'
    return (f"IC监控({snapshot['tier']}·{snapshot['horizon_days']}日·"
            f"{snapshot['window_days']}日窗)：{flag}，近窗均值 {tail}；"
            f"{status['reason']}")


__all__ = [
    'DEFAULT_CONSECUTIVE',
    'DEFAULT_THRESHOLD',
    'DEFAULT_WINDOW_DAYS',
    'TIER_HORIZON',
    'monitor_note',
    'rolling_ic_series',
    'run_ic_monitor',
]
