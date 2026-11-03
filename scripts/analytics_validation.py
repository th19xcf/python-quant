#!/usr/bin/env python3

"""只读分析层实测脚本：Alphalens 截面因子评估 + QuantStats 绩效 tearsheet。

不写数据库、不改列存、不改撮合；只读取 ``data/local_store/quotes`` 下的
个股 parquet（后复权口径：``raw OHLC * hfq_factor``），跑一次全流程并输出：

- ``reports/analytics_validation/alphalens/*.html|json``
- ``reports/analytics_validation/quantstats/*.html``
- ``reports/analytics_validation/validation.json``（汇总数值）

用法::

    .venv\\Scripts\\python.exe scripts\\analytics_validation.py
    .venv\\Scripts\\python.exe scripts\\analytics_validation.py --n-stocks 250
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import date, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.alpha.calculator.alpha_calculator import calculate_factors_cross_sectional
from src.alpha.evaluation.alphalens_adapter import run_alphalens_evaluation
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.ma_strategy import MAStrategy
from src.quant.reports.quantstats_report import (
    build_quantstats_metrics,
    generate_quantstats_report,
)

ROOT = Path(__file__).resolve().parents[1]
STORE_DIR = ROOT / 'data' / 'local_store' / 'quotes' / 'stock'
OUT_DIR = ROOT / 'reports' / 'analytics_validation'

_PRICE_COLS = ['open', 'high', 'low', 'close']


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--n-stocks', type=int, default=250, help='选股数量（200-300）')
    parser.add_argument('--start', default='2021-01-01', help='评估起始日')
    parser.add_argument('--end', default='2025-12-31', help='评估结束日')
    parser.add_argument('--seed', type=int, default=2024, help='抽样随机种子')
    parser.add_argument('--factor', default='momentum_3m', help='因子列名')
    parser.add_argument('--backtest-code', default='600519.SH', help='QuantStats 回测标的')
    return parser.parse_args()


def _select_universe(n_stocks: int, read_start: date, end: date,
                     seed: int) -> dict[str, Path]:
    """按覆盖度筛选个股，再确定性抽样；全程只读 parquet 元数据列。"""
    candidates: dict[str, Path] = {}
    for path in STORE_DIR.rglob('*.parquet'):
        try:
            dates = pl.read_parquet(path, columns=['date'])['date']
        except (pl.exceptions.PolarsError, OSError) as exc:
            logger.warning(f"[validation] 跳过无法读取的列存文件 {path.name}: {exc}")
            continue
        if dates.len() == 0:
            continue
        if dates.min() <= read_start and dates.max() >= end:
            candidates[path.stem] = path
    logger.info(f"[validation] 覆盖度达标标的 {len(candidates)} 只，目标抽样 {n_stocks} 只")
    if len(candidates) < n_stocks:
        raise ValueError(
            f"覆盖度达标标的仅 {len(candidates)} 只，少于请求的 {n_stocks} 只")
    # 非安全上下文：仅用于可复现实测抽样
    rng = random.Random(seed)  # noqa: S311
    return {code: candidates[code]
            for code in sorted(rng.sample(sorted(candidates), n_stocks))}


def _load_panel(codes: dict[str, Path], read_start: date,
                end: date) -> pl.DataFrame:
    """读取个股列存并换算后复权价，同时保留 raw_open/raw_close 供撮合限价。"""
    frames = []
    for code, path in codes.items():
        frame = pl.read_parquet(path)
        frame = frame.filter(
            (pl.col('date') >= read_start) & (pl.col('date') <= end))
        if frame.is_empty():
            continue
        frame = frame.with_columns([
            pl.col('open').alias('raw_open'),
            pl.col('close').alias('raw_close'),
            *[(pl.col(col) * pl.col('hfq_factor')).alias(col)
              for col in _PRICE_COLS],
        ])
        frames.append(frame.select([
            'ts_code', 'date', *_PRICE_COLS, 'volume', 'hfq_factor',
            'raw_open', 'raw_close',
        ]))
    if not frames:
        raise ValueError('没有可用行情数据')
    return pl.concat(frames)


def _run_backtest(panel: pl.DataFrame, code: str, start: date,
                  end: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    data = panel.filter(pl.col('ts_code') == code)
    if data.is_empty():
        raise ValueError(f'回测标的不在抽样池中: {code}')
    data = data.filter(
        (pl.col('date') >= start) & (pl.col('date') <= end)).sort('date')
    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    result = engine.run_backtest(
        initial_capital=1_000_000.0, lot_size=100, ts_code=code)
    return result.get('equity_curve', []), result.get('trades', [])


def _compare_metrics(pa: dict[str, Any], qs: dict[str, Any]) -> dict[str, Any]:
    """把 PerformanceAnalyzer 的百分数口径换算成小数后与 QuantStats 对比。"""
    # 两个实现都基于同一条净值序列，差异只可能来自年化/标准差口径
    total_expected = qs['final_equity'] / qs['initial_equity'] - 1
    return {
        'total_return': {
            'performance_analyzer': pa.get('total_return', 0.0) / 100,
            'quantstats': qs['total_return'],
            'delta': pa.get('total_return', 0.0) / 100 - total_expected,
        },
        'annual_return': {
            'performance_analyzer': pa.get('annual_return', 0.0) / 100,
            'quantstats': qs['annual_return'],
            'delta': pa.get('annual_return', 0.0) / 100 - qs['annual_return'],
        },
        'sharpe': {
            'performance_analyzer': pa.get('sharpe_ratio', 0.0),
            'quantstats': qs['sharpe'],
            'delta': pa.get('sharpe_ratio', 0.0) - qs['sharpe'],
        },
        'max_drawdown': {
            'performance_analyzer': pa.get('max_drawdown', 0.0) / 100,
            'quantstats': qs['max_drawdown'],
            'delta': pa.get('max_drawdown', 0.0) / 100 - qs['max_drawdown'],
        },
        'volatility': {
            'performance_analyzer': pa.get('volatility', 0.0) / 100,
            'quantstats': qs['volatility'],
            'delta': pa.get('volatility', 0.0) / 100 - qs['volatility'],
        },
    }


def main() -> None:
    args = _parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    # 因子暖机：读取起始日前推 240 个自然日（约 160 个交易日）
    read_start = start - timedelta(days=240)

    started = time.perf_counter()
    codes = _select_universe(args.n_stocks, read_start, end, args.seed)
    panel = _load_panel(codes, read_start, end)
    logger.info(f"[validation] 面板 {panel.height} 行 × {len(codes)} 标的，"
                f"读取耗时 {time.perf_counter() - started:.1f}s")

    t0 = time.perf_counter()
    factor_panel = calculate_factors_cross_sectional(panel, ['price_momentum'])
    factor_panel = factor_panel.filter(pl.col('date') >= start)
    logger.info(f"[validation] 因子计算耗时 {time.perf_counter() - t0:.1f}s，"
                f"评估区间 {factor_panel.height} 行")

    alphalens_result = run_alphalens_evaluation(
        factor_panel.select(['ts_code', 'date', *_PRICE_COLS,
                             'raw_open', 'raw_close', args.factor]),
        args.factor,
        periods=(1, 5, 10, 20),
        quantiles=5,
        entry_price='open',
        output_dir=OUT_DIR / 'alphalens',
        report_name=f'alphalens_{args.factor}_{args.n_stocks}stocks',
    )

    t0 = time.perf_counter()
    backtest_code = (args.backtest_code if args.backtest_code in codes
                     else next(iter(codes)))
    if backtest_code != args.backtest_code:
        logger.info(
            f"[validation] 指定回测标的 {args.backtest_code} 未进入抽样池，"
            f"改用样本内标的 {backtest_code}")
    equity_curve, trades = _run_backtest(panel, backtest_code, start, end)
    if len(equity_curve) < 2:
        raise ValueError(f'回测权益曲线点数不足: {len(equity_curve)}')
    pa_metrics = PerformanceAnalyzer().analyze(equity_curve, trades, frequency='daily')
    qs_metrics = build_quantstats_metrics(equity_curve)
    qs_report = generate_quantstats_report(
        equity_curve,
        output_path=OUT_DIR / 'quantstats' / f'quantstats_{backtest_code}.html',
        title=f'{backtest_code} MA(5,20) 绩效 Tearsheet',
    )
    comparison = _compare_metrics(pa_metrics, qs_metrics)
    logger.info(f"[validation] 回测 + QuantStats 耗时 {time.perf_counter() - t0:.1f}s，"
                f"权益点 {len(equity_curve)}，交易 {len(trades)}")

    payload = {
        'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'versions': {
            'alphalens-reloaded': version('alphalens-reloaded'),
            'quantstats': version('quantstats'),
        },
        'alphalens': {
            'n_stocks': len(codes),
            'factor': args.factor,
            'entry_rule': alphalens_result['entry_rule'],
            'elapsed_seconds': alphalens_result['elapsed_seconds'],
            'ic_mean': alphalens_result['ic_mean'],
            'ic_std': alphalens_result['ic_std'],
            'icir': alphalens_result['icir'],
            'ic_positive_ratio': alphalens_result['ic_positive_ratio'],
            'ic_decay': alphalens_result['ic_decay'],
            'quantile_returns': alphalens_result['quantile_returns'],
            'long_short_return': alphalens_result['long_short_return'],
            'turnover': alphalens_result['turnover'],
            'tearsheet_path': alphalens_result['tearsheet_path'],
            'json_path': alphalens_result['json_path'],
        },
        'quantstats': {
            'backtest_code': backtest_code,
            'strategy': 'MA(5,20)',
            'equity_points': len(equity_curve),
            'trades': len(trades),
            'metrics': qs_metrics,
            'report_path': qs_report['report_path'],
        },
        'performance_analyzer': pa_metrics,
        'comparison': comparison,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_json = OUT_DIR / 'validation.json'
    out_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    logger.info(f"[validation] 汇总结果: {out_json}")
    logger.info(
        f"[validation] Alphalens IC(1D)={alphalens_result['ic_mean'].get('1D')}, "
        f"ICIR(1D)={alphalens_result['icir'].get('1D')}")
    logger.info(
        f"[validation] QuantStats CAGR={qs_metrics['annual_return']:.4f} "
        f"Sharpe={qs_metrics['sharpe']:.4f} MaxDD={qs_metrics['max_drawdown']:.4f}")


if __name__ == '__main__':
    main()
