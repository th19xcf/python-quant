#!/usr/bin/env python3

"""Supertrend 策略回测（单票 × 真实行情）

标的：上证指数 + 深证成指 + 创业板指 + 中国平安 + 平安银行
区间：指数尽量全、个股从 2015 开始
参数：ATR 10 / 倍数 3（TradingView 默认口径）

只读 data/local_store/quotes，不写任何正式数据。
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.supertrend_strategy import SupertrendStrategy

DATA = ROOT / 'data' / 'local_store' / 'quotes'
ANALYZER = PerformanceAnalyzer()
INITIAL_CAPITAL = 1_000_000.0
TARGETS = [
    ('index/sh/000001.SH.parquet', '上证指数'),
    ('index/sz/399001.SZ.parquet', '深证成指'),
    ('index/sz/399006.SZ.parquet', '创业板指'),
    ('stock/sh/601318.SH.parquet', '中国平安'),
    ('stock/sz/000001.SZ.parquet', '平安银行'),
]


def run_one(rel_path: str, name: str, start: date) -> dict | None:
    p = DATA / rel_path
    if not p.exists():
        print(f'  SKIP {name}: {rel_path} 不存在')
        return None
    df = pl.read_parquet(p).sort('date').filter(pl.col('date') >= start)
    if 'hfq_factor' in df.columns:
        for col in ('open', 'high', 'low', 'close'):
            df = df.with_columns((pl.col(col) * pl.col('hfq_factor')).alias(col))
    need = {'date', 'open', 'high', 'low', 'close', 'volume'}
    if not need <= set(df.columns):
        print(f'  SKIP {name}: 缺列 {need - set(df.columns)}')
        return None

    strat = SupertrendStrategy()
    engine = BacktestEngine(df)
    engine.set_strategy(strat)
    res = engine.run_backtest(initial_capital=INITIAL_CAPITAL)

    trades = res.get('trades', [])
    eq = res.get('equity_curve', [])
    perf = ANALYZER.analyze(eq, trades)

    n_buy = sum(1 for t in trades if t.get('signal') == 'buy')
    n_sell = sum(1 for t in trades if t.get('signal') == 'sell')

    # 买入持有基准（年化）
    ret_bh = (df['close'][-1] / df['close'][0]) - 1
    ann_bh = (1 + ret_bh) ** (252 / df.height) - 1 if df.height > 1 else 0

    out = {
        'name': name,
        'file': rel_path,
        'bars': df.height,
        'period': f"{df['date'][0]} → {df['date'][-1]}",
        'final_equity': round(eq[-1]['equity'], 2) if eq else 0,
        'total_return': round(perf['total_return'], 2),
        'annual_return': round(perf['annual_return'], 2),
        'max_drawdown': round(perf['max_drawdown'], 2),
        'sharpe': round(perf['sharpe_ratio'], 3),
        'win_rate': round(perf['winning_rate'], 2),
        'trades': perf['trades_count'],
        'buy_total': n_buy,
        'sell_total': n_sell,
        'annual_return_bh': round(ann_bh * 100, 2),
        'total_return_bh': round(ret_bh * 100, 2),
    }
    print(json.dumps(out, ensure_ascii=False))
    return out


def main():
    print(f'\nSupertrend 策略回测（ATR 10 / 倍数 3.0）')
    print(f'标的：{", ".join(t[1] for t in TARGETS)}')
    print(f'初始资金：{INITIAL_CAPITAL:,}  频率：日线  成本：默认保守档\n')
    results = []
    for rel, name in TARGETS:
        start = date(2005, 1, 1) if 'index' in rel else date(2015, 1, 1)
        print(f'--- {name} ---')
        r = run_one(rel, name, start)
        if r:
            results.append(r)
    # 汇总表
    print(f'\n{"="*100}')
    print(f'{"标的":<8} {"区间":<22} {"策略年化":>9} {"策略MDD":>9} {"Sharpe":>8} '
          f'{"胜率":>8} {"笔数":>6} {"持有年化":>9} {"超额pp":>8} {"最终权益":>12}')
    print('-' * 100)
    for r in results:
        exc = round(r['annual_return'] - r['annual_return_bh'], 2)
        print(f"{r['name']:<8} {r['period']:<22} {r['annual_return']:>8.2f}% "
              f"{r['max_drawdown']:>8.2f}% {r['sharpe']:>8.3f} {r['win_rate']:>7.2f}% "
              f"{r['trades']:>6} {r['annual_return_bh']:>8.2f}% {exc:>+8.2f} "
              f"{r['final_equity']:>12,.2f}")
    print(f'{"="*100}')


if __name__ == '__main__':
    main()
