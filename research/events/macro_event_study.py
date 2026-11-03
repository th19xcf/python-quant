#!/usr/bin/env python3

"""宏观/主题事件 → 板块映射的事件研究试点。

输入：
- ``data/macro_events.csv``：人工整理的可回溯宏观事件清单（每条带官方来源）；
- ``data/event_sector_map.csv``：事件类型 × 板块标签 × 行业 ETF 的映射知识表；
- ``data/sector_etf_universe.csv``：ETF 代码与本地列存类型。

输出：
- ``data/macro_event_level_car.csv``：事件 × 板块 × 窗口的事件级 CAR；
- ``data/macro_event_summary.csv``：事件类型 × 板块 × 窗口的均值/t 值/胜率/牛熊分段；
- ``data/macro_mapping_verdict.csv``：映射是否经得起检验的结论表。

口径：估计窗 [-120,-20]，事件窗 [-5,+20]，市场模型（alpha/beta OLS），
建仓价统一为事件日 0 的开盘价；事件日 0 默认取发布日期之后的下一个交易日，
两会等「开盘前已知」事件用 ``trade_day`` 显式指定。
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from event_study_core import (
    Benchmark,
    PriceSeries,
    compute_event_car,
    load_price_series,
    summarize,
    write_csv,
)
from loguru import logger

DATA_DIR = Path(__file__).resolve().parent / 'data'
HORIZONS = (0, 1, 5, 20)


def _read_str_csv(path: Path) -> pl.DataFrame:
    return pl.read_csv(path, infer_schema_length=0)


def _series_cache(ts_code: str, kind: str, cache: dict[str, PriceSeries | None]) -> PriceSeries | None:
    if ts_code not in cache:
        cache[ts_code] = load_price_series(ts_code, kind=kind)
    return cache[ts_code]


def evaluate(events: pl.DataFrame, mapping: pl.DataFrame,
             etf_universe: pl.DataFrame) -> pl.DataFrame:
    benchmark = Benchmark('000300.SH')
    kind_by_code = {row['ts_code']: row['kind'] for row in etf_universe.to_dicts()}
    cache: dict[str, PriceSeries | None] = {}
    rows: list[dict[str, Any]] = []
    for event in events.to_dicts():
        event_date = date.fromisoformat(event['event_date'])
        explicit = date.fromisoformat(event['trade_day']) if event['trade_day'] else None
        for item in mapping.filter(pl.col('event_type') == event['event_type']).to_dicts():
            ts_code = item['etf_code']
            series = _series_cache(ts_code, kind_by_code.get(ts_code, 'fund'), cache)
            if series is None:
                logger.warning('ETF 缺失: {} ({})', ts_code, item['etf_name'])
                continue
            result = compute_event_car(series, benchmark, event_date, HORIZONS, trade_day=explicit)
            if result is None:
                logger.warning('事件无法计算 CAR: {} {} {}', event['event_id'], ts_code, event_date)
                continue
            rows.append({
                'event_id': event['event_id'],
                'event_type': event['event_type'],
                'event_name': event['event_name'],
                'event_date': event['event_date'],
                'sector_tag': item['sector_tag'],
                'etf_code': ts_code,
                'etf_name': item['etf_name'],
                'expected_direction': item['market_direction'],
                'mapping_strength': item['strength'],
                'trade_day': result['trade_day'].isoformat(),
                'regime': result['regime'],
                'beta': result['beta'],
                'alpha': result['alpha'],
                'n_est': result['n_est'],
                **{f'car{h}': result[f'car{h}'] for h in HORIZONS},
                **{f'car_ma{h}': result[f'car_ma{h}'] for h in HORIZONS},
            })
    return pl.DataFrame(rows)


def add_direction_hit(frame: pl.DataFrame) -> pl.DataFrame:
    hit_expr = []
    for horizon in HORIZONS:
        positive = pl.col('expected_direction') == '+'
        col = pl.col(f'car{horizon}')
        hit_expr.append(
            pl.when(positive).then((col > 0).cast(pl.Int8))
            .otherwise((col < 0).cast(pl.Int8))
            .alias(f'hit{horizon}')
        )
    return frame.with_columns(hit_expr)


def build_verdict(summary: pl.DataFrame) -> pl.DataFrame:
    """以 +5 日窗口为主，给出映射的可用性结论。"""
    rows: list[dict[str, Any]] = []
    for item in summary.filter(pl.col('horizon') == 5).to_dicts():
        n = item['n']
        t_value = item['t'] if item['t'] is not None else float('nan')
        mean = item['mean'] if item['mean'] is not None else float('nan')
        direction = item['expected_direction']
        same_sign = (mean > 0) if direction == '+' else (mean < 0)
        if n < 10:
            verdict = '样本不足'
        elif abs(t_value) >= 1.96 and same_sign:
            verdict = '成立'
        elif abs(t_value) >= 1.96 and not same_sign:
            verdict = '显著反向'
        else:
            verdict = '不成立'
        rows.append({
            'event_type': item['event_type'],
            'sector_tag': item['sector_tag'],
            'etf_code': item['etf_code'],
            'expected_direction': direction,
            'n': n,
            'mean_car5': mean,
            't_car5': t_value,
            'win_rate': item['win_rate'],
            'verdict': verdict,
        })
    return pl.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description='宏观事件 → 板块映射事件研究')
    parser.add_argument('--events', default=str(DATA_DIR / 'macro_events.csv'))
    parser.add_argument('--mapping', default=str(DATA_DIR / 'event_sector_map.csv'))
    parser.add_argument('--universe', default=str(DATA_DIR / 'sector_etf_universe.csv'))
    args = parser.parse_args()

    events = _read_str_csv(Path(args.events))
    mapping = _read_str_csv(Path(args.mapping))
    universe = _read_str_csv(Path(args.universe))
    logger.info('宏观事件 {} 条，映射 {} 条', events.height, mapping.height)

    level = evaluate(events, mapping, universe)
    if level.height == 0:
        logger.error('没有任何事件成功计算 CAR，请检查数据覆盖')
        return
    level = add_direction_hit(level)
    summary = summarize(level, ['event_type', 'sector_tag', 'etf_code', 'expected_direction'], HORIZONS)
    verdict = build_verdict(summary)

    write_csv(level, DATA_DIR / 'macro_event_level_car.csv')
    write_csv(summary, DATA_DIR / 'macro_event_summary.csv')
    write_csv(verdict, DATA_DIR / 'macro_mapping_verdict.csv')
    logger.info('事件级 {} 行，汇总 {} 行，结论 {} 行', level.height, summary.height, verdict.height)
    logger.info('映射结论分布:\n{}', verdict['verdict'].value_counts().to_pandas().to_string(index=False))


if __name__ == '__main__':
    main()
