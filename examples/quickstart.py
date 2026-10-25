#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
端到端 Quickstart：取数 → 因子 → 选股 → 回测 → 报告

用法:
    python examples/quickstart.py                 # 合成数据，零配置，一条命令跑通
    python examples/quickstart.py --bars 1000     # 加长样本
    python examples/quickstart.py --real 600000.SH --start 2023-01-01 --end 2024-12-31
                                                  # 走真实数据源取数（需 .env + 数据库）

设计约束:
    - 默认不连数据库、不读 .env，clone 后即可运行
    - 只调用对外入口：calculate_factors_cross_sectional / run_backtest
    - 断言关键不变量，异常时返回非 0，可直接作为冒烟测试
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 先静音框架日志，保持 quickstart 输出干净；带 --verbose 时恢复
from loguru import logger

logger.remove()
if '--verbose' in sys.argv:
    logger.add(sys.stderr, level='INFO')

from src.alpha.calculator.alpha_calculator import calculate_factors_cross_sectional
from src.backtest.strategies.ma_strategy import MAStrategy
from src.core.usecases.run_backtest import run_backtest

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

UNIVERSE = ['600000.SH', '000001.SZ', '600519.SH', '000651.SZ', '601318.SH']
TOP_N = 2
INITIAL_CAPITAL = 1_000_000.0
FACTOR_TYPES = ['price_momentum']
RANK_COLUMN = 'momentum_3m'


def build_synthetic(bars: int, seed: int = 42) -> pl.DataFrame:
    """合成多标的 OHLCV（不同标的给不同漂移，便于因子区分优劣）"""
    rng = np.random.default_rng(seed)
    calendar = pl.date_range(
        pl.date(2021, 1, 4), pl.date(2021, 1, 4) + pl.duration(days=bars - 1),
        interval='1d', eager=True,
    )
    dates = calendar.filter(calendar.dt.weekday() < 5)  # 只保留工作日

    frames = []
    for k, code in enumerate(UNIVERSE):
        ret = rng.normal(0.0006 * (k - 2), 0.018, dates.len())
        close = 20.0 * np.exp(np.cumsum(ret))
        open_ = np.r_[close[0], close[:-1]] * (1 + rng.normal(0, 0.003, dates.len()))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.006, dates.len())))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.006, dates.len())))
        frames.append(pl.DataFrame({
            'ts_code': code,
            'date': dates,
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': rng.integers(500_000, 2_000_000, dates.len()),
        }))
    return pl.concat(frames, how='vertical')


def load_real(ts_code: str, start: str, end: str) -> pl.DataFrame:
    """走真实数据源（后复权），需要 .env 与数据库"""
    from src.data.managers.data_fetcher import DataFetcher
    from src.database.db_manager import DatabaseManager
    from src.utils.config import Config

    config = Config()
    db = DatabaseManager(config)
    db.connect()
    try:
        df = DataFetcher(config, db_manager=db).get_stock_data(
            ts_code, start, end, adjustment_type='hfq')
    finally:
        db.disconnect()
    return df.with_columns(pl.lit(ts_code).alias('ts_code'))


def select_universe(factors: pl.DataFrame) -> pl.DataFrame:
    """按因子最新截面排序，取前 TOP_N 只"""
    snapshot = (
        factors.sort('date')
        .group_by('ts_code')
        .agg(pl.col(RANK_COLUMN).last().alias(RANK_COLUMN))
        .sort(RANK_COLUMN, descending=True)
    )
    return snapshot.head(TOP_N)


def main() -> int:
    parser = argparse.ArgumentParser(description='量化系统端到端 Quickstart')
    parser.add_argument('--bars', type=int, default=750, help='合成数据的日历天数')
    parser.add_argument('--real', type=str, default=None, help='用真实数据源取数的股票代码')
    parser.add_argument('--start', type=str, default='2023-01-01')
    parser.add_argument('--end', type=str, default='2024-12-31')
    args = parser.parse_args()

    # ---- 1. 取数 ----
    if args.real:
        data = load_real(args.real, args.start, args.end)
        source = f'真实数据源（{args.real}，后复权）'
    else:
        data = build_synthetic(args.bars)
        source = f'合成数据（{len(UNIVERSE)} 只 × {data.height // len(UNIVERSE)} 交易日）'
    print(f'[1/4] 取数完成：{source}，共 {data.height} 行')

    # ---- 2. 因子 ----
    factors = calculate_factors_cross_sectional(data, FACTOR_TYPES)
    assert RANK_COLUMN in factors.columns, f'因子缺少 {RANK_COLUMN} 列'
    selected = select_universe(factors)
    codes = selected['ts_code'].to_list()
    print(f'[2/4] 因子计算完成：{len(factors.columns) - len(data.columns)} 个因子列；'
          f'按 {RANK_COLUMN} 选出 {codes}')

    # ---- 3. 回测 ----
    panel = {
        code: factors.filter(pl.col('ts_code') == code).drop('ts_code')
        for code in codes
    }
    strategies = {code: MAStrategy(5, 20) for code in codes}
    result = run_backtest(
        data=panel,
        strategies=strategies,
        initial_capital=INITIAL_CAPITAL,
        lot_size=100,
    )
    print(f'[3/4] 回测完成：{len(result.get("trades", []))} 笔成交')

    # ---- 4. 报告 ----
    perf = result.get('performance', {}) or {}
    report = {
        '数据来源': source,
        '参与回测标的': codes,
        '初始资金': INITIAL_CAPITAL,
        '期末权益': result.get('final_equity'),
        '总收益率(%)': result.get('total_return'),
        '成交笔数': len(result.get('trades', [])),
        '关键指标': {k: perf[k] for k in
                     ('annual_return', 'sharpe_ratio',
                      'max_drawdown', 'win_rate') if k in perf},
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    trades = result.get('trades', [])
    if trades:
        pl.DataFrame(trades).write_csv(OUTPUT_DIR / 'trades.csv')
    equity = result.get('equity_curve', [])
    if equity:
        pl.DataFrame(equity).write_csv(OUTPUT_DIR / 'equity_curve.csv')
    (OUTPUT_DIR / 'report.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    print('[4/4] 报告已写入', OUTPUT_DIR)
    for key, value in report.items():
        if key != '关键指标':
            print(f'      {key}: {value}')
    for key, value in report['关键指标'].items():
        print(f'      {key}: {value}')

    # ---- 冒烟断言：链路坏掉时返回非 0 ----
    assert result.get('final_equity') is not None, '回测未返回期末权益'
    assert equity, '回测未返回权益曲线'
    return 0


if __name__ == '__main__':
    sys.exit(main())
