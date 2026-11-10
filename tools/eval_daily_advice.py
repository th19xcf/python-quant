#!/usr/bin/env python3

"""每日推荐因子评估 CLI（可重复运行）。

用法::

    python tools/eval_daily_advice.py --start 2021-01-01 --end 2026-11-01 \
        --universe 500 --hold 20 --factor-set legacy
    python tools/eval_daily_advice.py --start 2021-01-01 --end 2026-11-01 \
        --universe 500 --hold 5 --factor-set extended

输出到 ``reports/eval_daily_advice/``：
- ``factor_metrics.csv``：全样本因子指标（仅报告，不用于选择）
- ``walk_forward.json``：每个训练/测试窗的入选与样本外表现
- ``comparison.json``：legacy vs extended 净收益对比
- ``equity_{set}_{hold}.csv``：净值曲线
- ``windows_{set}_{hold}.csv``：逐测试窗明细（含 Sharpe）

成本唯一来源：``src/backtest/costs/cost_model.py::CostModel``（回测撮合层同一套）；
``--stress`` 只把滑点×2 做压力对照。walk-forward 复用
``src/quant/learning/splits.py`` 的 purge/embargo（默认 embargo=20 交易日）。

所有参数均显式可配置；不修改数据、不写生产配置。
"""

from __future__ import annotations

import argparse
import json
import pickle
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.config import TIER_PRESETS  # noqa: E402
from src.advice.extended_factors import (  # noqa: E402
    EXTRA_COLUMNS,
    FACTOR_LIB_CANDIDATES,
    compute_extended_candidates,
)
from src.advice.factor_eval import (  # noqa: E402
    LEGACY_COLUMNS,
    EvalConfig,
    SimConfig,
    add_forward_returns,
    build_composite,
    combine_curves,
    evaluate_candidates,
    ic_monitor,
    simulate_fixed_hold,
    walk_forward,
)
from src.advice.factor_panel import compute_factor_panel  # noqa: E402
from src.advice.fundamental_factors import (  # noqa: E402
    FUNDAMENTAL_CANDIDATES,
    FUNDAMENTAL_COLUMNS,
    compute_fundamental_factors,
)
from src.advice.universe import list_local_stock_codes  # noqa: E402
from src.backtest.costs.cost_model import COST_DEFAULTS  # noqa: E402
from src.data.local_store import get_local_quote_store  # noqa: E402
from src.quant.factor_lib.compute import compute_all  # noqa: E402
from src.quant.factor_lib.data import PanelRequest, load_panel  # noqa: E402
from src.utils.config import get_config  # noqa: E402
from tools.benchmark_alignment import benchmark_window_stats  # noqa: E402


def _cache_dir() -> Path:
    path = ROOT / 'tmp' / 'eval_daily_advice'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _panel_cache_key(start: str, end: str, tag: str) -> Path:
    return _cache_dir() / f'panel_{tag}_{start}_{end}.parquet'


def _matrix_cache_key(start: str, end: str, tag: str) -> Path:
    return _cache_dir() / f'factors_{tag}_{start}_{end}.parquet'


def strat_universe(size: int, start: str, end: str, seed: int) -> dict:
    """全市场分层抽样（按流动性十分位等比例），含退市标的。

    与前几轮「近 250 日成交额 Top-N」不同：这里只要求区间内至少 60 个交易日，
    因此停牌、流动性尾部、以及样本期内退市的代码都能进入池子；
    这正是消除幸存者偏差的关键。北交所因 30% 涨跌停规则未建模而剔除。
    """
    cache = _cache_dir() / f'universe_strat_{size}_{seed}_{start}_{end}.json'
    if cache.exists():
        payload = json.loads(cache.read_text(encoding='utf-8'))
        logger.info(f'[universe] 读取分层抽样缓存 {cache}（{len(payload["codes"])} 只）')
        return payload

    started = time.perf_counter()
    store = get_local_quote_store(get_config())
    markets = ('sh', 'sz')
    all_codes = list_local_stock_codes(get_config(), markets=markets)
    lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    rows: list[tuple[str, float, date]] = []
    for code in all_codes:
        frame = store.read_file('stock', code)
        if frame is None or frame.is_empty():
            continue
        sub = frame.filter((pl.col('date') >= lo) & (pl.col('date') <= hi))
        if sub.height < 60:  # noqa: PLR2004 新股/长期停牌不足半年不参与
            continue
        amount = float(cast(float, sub['amount'].median() or 0.0))
        if amount <= 0:
            continue
        last_day = cast(date, sub['date'].max())
        rows.append((code, amount, last_day))

    rows.sort(key=lambda item: item[1])
    tail_date = hi - timedelta(days=10)
    delisted = [code for code, _amt, last in rows if last < tail_date]
    eligible = len(rows)
    target = eligible if size <= 0 else min(size, eligible)
    allocations = [target // 10] * 10
    for index in range(target - sum(allocations)):
        allocations[index] += 1

    rng = random.Random(seed)  # noqa: S311 抽样可复现，非加密用途
    codes: list[str] = []
    for decile, take in enumerate(allocations):
        begin = eligible * decile // 10
        stop = eligible * (decile + 1) // 10
        group = [code for code, _amt, _last in rows[begin:stop]]
        codes.extend(rng.sample(group, min(take, len(group))))
    codes = sorted(set(codes))

    payload = {
        'codes': codes,
        'eligible': eligible,
        'delisted_in_period': len(delisted),
        'sampled': len(codes),
        'seed': seed,
        'start': start, 'end': end,
        'markets': list(markets),
        'rule': '流动性十分位等比例随机抽样，区间内≥60个交易日，含样本期内退市标的',
        'elapsed_sec': round(time.perf_counter() - started, 1),
    }
    cache.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f'[universe] 合格 {eligible} 只（其中样本期内退市 {len(delisted)}），'
                f'抽样 {len(codes)} 只，耗时 {payload["elapsed_sec"]}s')
    return payload


def load_or_build_panel(start: str, end: str, universe: int,
                        seed: int) -> tuple[pl.DataFrame, dict]:
    """加载/缓存后复权面板（全市场分层抽样，剔除北交所）。"""
    tag = f'strat{universe}s{seed}'
    cache = _panel_cache_key(start, end, tag)
    info = strat_universe(universe, start, end, seed)
    if cache.exists():
        logger.info(f'[eval] 读取面板缓存 {cache}')
        return pl.read_parquet(cache), info
    logger.info(f'[eval] 构建面板 universe={universe} {start}~{end}')
    panel = load_panel(PanelRequest(start=start, end=end, universe_size=universe,
                                    use_cache=True), codes=info['codes'])
    panel.write_parquet(cache)
    return panel, info


def build_factor_matrix(panel: pl.DataFrame) -> pl.DataFrame:
    """163 条 factor_lib + 15 条扩展技术 + legacy 12 因子 + 7 条基本面/股本因子。"""
    started = time.perf_counter()
    result = compute_all(panel)
    base = result.frame
    market_cols = [name for name in (
        'ret', 'open', 'high', 'low', 'raw_open', 'raw_high', 'raw_low',
        'raw_close', 'amount', 'pre_close') if name in panel.columns]
    base = base.join(panel.select(['date', 'ts_code', *market_cols]),
                     on=['date', 'ts_code'], how='left')
    logger.info(f'[eval] factor_lib {len(result.names)} 条，失败 {len(result.failures)}')

    extra = compute_extended_candidates(panel)
    extra_cols = [name for name in EXTRA_COLUMNS if name in extra.columns]
    base = base.join(extra.select(['date', 'ts_code', *extra_cols]),
                     on=['date', 'ts_code'], how='left')

    fundamental = compute_fundamental_factors(panel)
    fundamental_cols = [name for name in FUNDAMENTAL_COLUMNS
                        if name in fundamental.columns]
    base = base.join(fundamental.select(['date', 'ts_code', *fundamental_cols]),
                     on=['date', 'ts_code'], how='left')
    logger.info(f'[eval] 基本面/股本因子 {len(fundamental_cols)} 条：{fundamental_cols}')

    compat = panel.with_columns([
        pl.col('amount').alias('raw_amount'),
        pl.col('volume').alias('raw_volume'),
        pl.col('pre_close').alias('raw_prev_close')
        if 'pre_close' in panel.columns else pl.col('raw_close').shift(1).over(
            'ts_code').alias('raw_prev_close'),
    ])
    legacy = compute_factor_panel(compat)
    legacy_cols = [name for name in (
        'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
        'rsi_14', 'volume_momentum_20', 'volatility_20', 'atr_pct',
        'ma20_gap', 'trend_slope_60', 'sharpe_ratio_20', 'max_drawdown_20',
        'history_rows') if name in legacy.columns]
    base = base.join(legacy.select(['date', 'ts_code', *legacy_cols]),
                     on=['date', 'ts_code'], how='left')
    logger.info(f'[eval] 因子矩阵完成 {base.height} 行 × {len(base.columns)} 列，'
                f'耗时 {time.perf_counter() - started:.1f}s')
    return base


def factor_list(matrix: pl.DataFrame) -> list[str]:
    """候选池：factor_lib 全部可算 + 扩展技术 + 基本面，排除 legacy 重名列。"""
    skip = {'date', 'ts_code', 'close', 'volume', 'next_tradable', 'history_rows',
            'open', 'high', 'low', 'raw_open', 'raw_high', 'raw_low', 'raw_close',
            'amount', 'pre_close', 'ret'}
    names = [name for name in matrix.columns
             if name not in skip and name not in LEGACY_COLUMNS]
    return [name for name in names if matrix[name].null_count() < matrix.height * 0.5]


def _legacy_weights(tier: str = 'mid') -> dict[str, float]:
    return {spec.column: spec.direction * spec.weight
            for spec in TIER_PRESETS[tier].factors}


def _print_metrics(metrics: list) -> None:
    for item in sorted(metrics, key=lambda x: abs(x.ic_ir), reverse=True)[:15]:
        logger.info(
            f'{item.factor:<22} {item.style:<10} cov={item.coverage:.0%} '
            f'IC={item.ic_mean:+.4f} IR={item.ic_ir:+.2f} '
            f'q={item.q_value:.3f} mono={item.monotonic:+.2f} '
            f'LS={item.long_short:+.3%} {item.reason}')


def _simulate_windows(matrix: pl.DataFrame, windows: list, weights_for,
                      sim: SimConfig) -> tuple[pl.DataFrame, dict, list[dict]]:
    """逐测试窗独立模拟（权重来自该窗训练结果），再复利拼接为连续净值。"""
    curves: list[pl.DataFrame] = []
    rows: list[dict] = []
    for item in windows:
        weights = weights_for(item)
        if not weights:
            rows.append({'test': f'{item.test_start}~{item.test_end}',
                         'n_factors': 0, 'note': '训练窗无入选因子'})
            continue
        chunk = matrix.filter((pl.col('date') >= item.test_start)
                              & (pl.col('date') < item.test_end))
        if chunk['date'].n_unique() < 5:  # noqa: PLR2004
            continue
        curve, stats = simulate_fixed_hold(build_composite(chunk, weights), sim)
        curves.append(curve)
        rows.append({
            'test': f'{item.test_start}~{item.test_end}',
            'n_factors': len(weights),
            'net_return': stats['net_return'],
            'annualized': stats['annualized'],
            'max_drawdown': stats['max_drawdown'],
            'sharpe': stats['sharpe'],
            'monthly_win_rate': stats['monthly_win_rate'],
            'turnover_annual': stats['turnover_annual'],
            'days': stats['days'],
        })
    combined, agg = combine_curves(curves)
    total_days = sum(row.get('days', 0) for row in rows)
    if total_days:
        agg['turnover_annual'] = sum(row.get('turnover_annual', 0.0) * row.get('days', 0)
                                     for row in rows) / total_days
    else:
        agg['turnover_annual'] = 0.0
    agg['windows'] = len(curves)
    return combined, agg, rows


def _benchmark_windows(matrix: pl.DataFrame, windows: list) -> tuple[dict, list[dict]]:
    """同窗口等权基准：零成本、每日全池等权，用后复权日收益复利。

    基准与策略同起点：剔除测试窗首日 ``d0``，从 ``d1`` 收盘起算，避免把
    ``d0→d1`` 隔夜跳空算进基准。基准不持有现金缓冲、不做涨跌停/停牌过滤，
    因此偏乐观，仅作相对刻度。
    """
    curves: list[pl.DataFrame] = []
    rows: list[dict] = []
    for item in windows:
        chunk = matrix.filter(
            (pl.col('date') >= item.test_start) & (pl.col('date') < item.test_end)
        ).select(['date', 'ts_code', 'ret']).drop_nulls()
        n_days = chunk['date'].n_unique()
        curve, stats = benchmark_window_stats(chunk, n_days=n_days, aligned=True)
        if curve.height < 5:  # noqa: PLR2004
            continue
        curves.append(curve)
        rows.append({'test': f'{item.test_start}~{item.test_end}',
                     'net_return': stats['net_return'],
                     'annualized': stats['annualized'],
                     'max_drawdown': stats['max_drawdown'],
                     'sharpe': stats['sharpe'],
                     'days': n_days})
    combined, agg = combine_curves(curves)
    agg['windows'] = len(curves)
    return agg, rows


def _attach_excess(comparisons: dict[str, dict], benchmark: dict) -> None:
    """给每个组合追加相对等权基准的超额（年化与 Calmar）。"""
    for agg in comparisons.values():
        agg['excess_annualized_vs_benchmark'] = (
            agg['annualized'] - benchmark['annualized'])
        agg['excess_calmar_vs_benchmark'] = agg['calmar'] - benchmark['calmar']


def _cost_overrides(args: argparse.Namespace) -> dict[str, float]:
    """把命令行覆盖项映射到 CostModel 键；留空表示用回测内核默认值。"""
    overrides: dict[str, float] = {}
    if args.commission is not None:
        overrides['commission_rate'] = args.commission
    if args.stamp_duty is not None:
        overrides['stamp_tax_rate'] = args.stamp_duty
    if args.slippage is not None:
        overrides['slippage'] = args.slippage
    return overrides


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='每日推荐因子评估（walk-forward）')
    parser.add_argument('--start', default='2021-01-01')
    parser.add_argument('--end', default='2026-11-01')
    parser.add_argument('--universe', type=int, default=2000,
                        help='全市场分层抽样只数；0 表示全部合格标的')
    parser.add_argument('--seed', type=int, default=20261105,
                        help='分层抽样随机种子（固定可复现）')
    parser.add_argument('--hold', type=int, nargs='+', default=[5, 20])
    parser.add_argument('--factor-set', choices=['legacy', 'extended'], default='extended',
                        help='本次评估的主因子集；--compare 时同时跑另一组')
    parser.add_argument('--compare', action='store_true',
                        help='同时评估 legacy 与 extended（默认仅主因子集）')
    parser.add_argument('--new-share', type=float, default=0.70,
                        help='extended 中新因子总权重占比，其余留给 legacy 基线')
    parser.add_argument('--horizon', type=int, default=20)
    parser.add_argument('--top-k', type=int, default=20)
    parser.add_argument('--commission', type=float, default=None,
                        help='覆盖回测内核佣金率；留空用 CostModel 默认 '
                             f'{COST_DEFAULTS["commission_rate"]}')
    parser.add_argument('--stamp-duty', type=float, default=None,
                        help='覆盖印花税率；留空按 CostModel 历史时间表')
    parser.add_argument('--slippage', type=float, default=None,
                        help='覆盖滑点；留空用 CostModel 默认 '
                             f'{COST_DEFAULTS["slippage"]}')
    parser.add_argument('--stress', action='store_true',
                        help='压力对照：在基准成本上把滑点×2')
    parser.add_argument('--embargo', type=int, default=20,
                        help='训练/测试隔离带交易日数，必须 ≥ horizon')
    parser.add_argument('--monitor-threshold', type=float, default=0.0,
                        help='失效监控：窗口 IC 均值阈值')
    parser.add_argument('--monitor-windows', type=int, default=3,
                        help='失效监控：连续 N 个窗口低于阈值/为负即告警')
    parser.add_argument('--no-cache', action='store_true')
    parser.add_argument('--out-dir', default='',
                        help='输出目录（默认 reports/eval_daily_advice）')
    args = parser.parse_args(argv)

    started = time.perf_counter()
    config = EvalConfig(start=args.start, end=args.end,
                        universe_size=args.universe, hold_days=args.hold[0],
                        horizon=args.horizon, embargo=args.embargo,
                        top_k=args.top_k)
    cost_overrides = _cost_overrides(args)

    tag = f'strat{args.universe}s{args.seed}'
    panel, universe_info = load_or_build_panel(args.start, args.end, args.universe,
                                               args.seed)
    matrix_path = _matrix_cache_key(args.start, args.end, tag)
    if matrix_path.exists() and not args.no_cache:
        matrix = pl.read_parquet(matrix_path)
        logger.info(f'[eval] 读取因子缓存 {matrix_path}')
    else:
        matrix = build_factor_matrix(panel)
        matrix.write_parquet(matrix_path)

    factors = factor_list(matrix)
    matrix = add_forward_returns(matrix, config.horizons)
    dates = sorted(matrix['date'].unique().to_list())
    sample_dates = dates[::5]

    # 1) 全样本报告（仅报告，不作为选择依据）；5/20 日两份
    out_dir = (Path(args.out_dir) if args.out_dir
               else ROOT / 'reports' / 'eval_daily_advice')
    out_dir.mkdir(parents=True, exist_ok=True)
    full_by_h: dict[int, list] = {}
    for horizon in config.horizons:
        metrics = evaluate_candidates(matrix, factors, horizon, sample_dates)
        full_by_h[horizon] = metrics
        _print_metrics(metrics)
        pl.DataFrame([item.__dict__ for item in metrics]).write_csv(
            out_dir / f'factor_metrics_{horizon}d.csv')
    full_metrics = full_by_h.get(config.horizon, next(iter(full_by_h.values())))

    # 2) walk-forward：只在训练窗选择，测试窗只评估
    legacy_weights = _legacy_weights('mid')
    cache_key = {
        'start': args.start, 'end': args.end, 'universe': args.universe,
        'seed': args.seed, 'horizon': config.horizon, 'embargo': config.embargo,
        'n_factors': len(factors), 'factors': sorted(factors), 'version': 5,
    }
    cache_path = out_dir / 'windows_cache.pkl'
    windows = None
    if cache_path.is_file() and not args.no_cache:
        cached = pickle.loads(cache_path.read_bytes())  # noqa: S301 本地自生成缓存
        if cached.get('key') == cache_key:
            windows = cached['windows']
            logger.info('[wf] 复用 walk-forward 缓存')
    if windows is None:
        windows = walk_forward(matrix, factors, config,
                               baseline_weights=legacy_weights,
                               new_share=args.new_share)
        cache_path.write_bytes(pickle.dumps({'key': cache_key, 'windows': windows}))
    payload = [
        {
            'train': f'{item.train_start}~{item.train_end}',
            'test': f'{item.test_start}~{item.test_end}',
            'n_selected': len(item.selected),
            'selected': item.selected,
            'weights': {name: round(weight, 6)
                        for name, weight in sorted(item.weights.items())},
            **item.test_metrics,
        }
        for item in windows
    ]
    (out_dir / 'walk_forward.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    for item in windows:
        logger.info(f'[wf] test {item.test_start}~{item.test_end}: '
                    f'n={len(item.selected)} '
                    f'IC={item.test_metrics.get("ic_mean", 0):+.4f} '
                    f'IR={item.test_metrics.get("ic_ir", 0):+.2f} '
                    f'L/S={item.test_metrics.get("long_short", 0):+.2%} '
                    f'失效={item.test_metrics.get("failed_factors", [])[:5]}')
    if not windows:
        logger.error('[eval] walk-forward 无有效窗口，停止')
        return 1
    monitor = ic_monitor(
        pl.Series('ic', [item.test_metrics.get('ic_mean', 0.0) for item in windows]),
        threshold=args.monitor_threshold, consecutive=args.monitor_windows)
    logger.info(f'[monitor] {monitor}')

    # 2.5) 同窗口等权基准（零成本），作为相对刻度而非目标
    benchmark, benchmark_rows = _benchmark_windows(matrix, windows)
    logger.info(f'[benchmark] 等权零成本 年化={benchmark["annualized"]:.2%} '
                f'MDD={benchmark["max_drawdown"]:.2%} Sharpe={benchmark["sharpe"]:.2f} '
                f'Calmar={benchmark["calmar"]:.2f}')
    (out_dir / 'benchmark.json').write_text(
        json.dumps({'aggregate': benchmark, 'windows': benchmark_rows},
                   ensure_ascii=False, indent=2), encoding='utf-8')
    pl.DataFrame(benchmark_rows).write_csv(out_dir / 'benchmark_windows.csv')

    # 3) 逐测试窗模拟：入选名单与权重只来自该窗训练结果
    sets = ['legacy', 'extended'] if args.compare else [args.factor_set]
    comparisons: dict[str, dict] = {}
    window_rows: dict[str, list[dict]] = {}
    for hold in args.hold:
        sim = SimConfig(top_k=args.top_k, hold_days=hold,
                        stress=args.stress,
                        cost_overrides=cost_overrides or None)
        for name in sets:
            weights_for = (lambda _item: legacy_weights) if name == 'legacy' \
                else (lambda item: item.weights)
            curve, agg, rows = _simulate_windows(matrix, windows, weights_for, sim)
            agg['hold'] = hold
            agg['factor_set'] = name
            comparisons[f'{name}_{hold}'] = agg
            window_rows[f'{name}_{hold}'] = rows
            if not curve.is_empty():
                curve.write_csv(out_dir / f'equity_{name}_{hold}.csv')
            pl.DataFrame(rows).write_csv(out_dir / f'windows_{name}_{hold}.csv')
            worst = min((row for row in rows if 'net_return' in row),
                        key=lambda r: r['net_return'], default=None)
            message = (
                f'[sim] {name} hold={hold}: 年化={agg["annualized"]:.2%} '
                f'MDD={agg["max_drawdown"]:.2%} Sharpe={agg["sharpe"]:.2f} '
                f'Calmar={agg["calmar"]:.2f} '
                f'月胜率={agg["monthly_win_rate"]:.1%} '
                f'季胜率={agg["quarterly_win_rate"]:.1%} '
                f'换手={agg["turnover_annual"]:.1f}x')
            if worst:
                message += f' 最差窗={worst["test"]}({worst["net_return"]:.2%})'
            logger.info(message)

    _attach_excess(comparisons, benchmark)
    (out_dir / 'comparison.json').write_text(
        json.dumps(comparisons, ensure_ascii=False, indent=2), encoding='utf-8')
    (out_dir / 'window_returns.json').write_text(
        json.dumps(window_rows, ensure_ascii=False, indent=2), encoding='utf-8')
    union: list[str] = []
    frequency: dict[str, int] = {}
    for item in windows:
        for name in item.selected:
            frequency[name] = frequency.get(name, 0) + 1
            if name not in union:
                union.append(name)
    summary = {
        'sample': {'start': args.start, 'end': args.end,
                   'universe': args.universe,
                   'stocks': matrix['ts_code'].n_unique(),
                   'trading_days': len(dates),
                   'eligible_all_market': universe_info['eligible'],
                   'delisted_in_period': universe_info['delisted_in_period'],
                   'sampling_seed': args.seed,
                   'sampling_rule': universe_info['rule']},
        'oos_start': str(windows[0].test_start),
        'oos_end': str(windows[-1].test_end),
        'candidates': len(factors),
        'selected_union': sorted(union),
        'selected_frequency': frequency,
        'bh_before': sum(1 for item in full_metrics if item.p_value < config.bh_alpha),
        'bh_after': sum(1 for item in full_metrics if item.q_value < config.bh_alpha),
        'windows': len(windows),
        'monitor': monitor,
        'costs': {
            'source': 'src/backtest/costs/cost_model.py::CostModel',
            'params': SimConfig(top_k=args.top_k, hold_days=args.hold[0],
                                stress=args.stress,
                                cost_overrides=cost_overrides or None
                                ).make_cost_model().params,
            'overrides': cost_overrides,
            'stress': args.stress,
            'hold': args.hold, 'top_k': args.top_k,
        },
        'embargo': config.embargo,
        'benchmark': benchmark,
        'benchmark_windows': benchmark_rows,
        'fundamental_candidates': [
            {'column': item.column, 'style': item.style,
             'direction': item.direction, 'expression': item.expression}
            for item in FUNDAMENTAL_CANDIDATES],
        'comparison': comparisons,
        'elapsed_sec': round(time.perf_counter() - started, 1),
    }
    (out_dir / 'summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f'[eval] 完成，输出 {out_dir}，耗时 {summary["elapsed_sec"]}s')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
