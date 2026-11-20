#!/usr/bin/env python3

"""
多指标共振择时策略 —— 基线回测跑批脚本（2026-11-20）

流程：
    1. 从名录按固定规则挑样本（2015-01-05 成交额前 10 的非 ST 在市个股）
    2. 逐标的用单票引擎 BacktestEngine 回测（T 日信号 → T+1 开盘成交由引擎保证）
    3. 等权合成组合净值，算全样本 / 样本内 / 样本外绩效与买入持有基准
    4. 信号整体再延迟 1 根 bar 做无未来函数对照
    5. 9 组参数敏感性

产物写到 2026-11-20/策略可行性评估/，只读 data/local_store，不写任何正式数据。
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.backtest.engine.backtest_engine import BacktestEngine  # noqa: E402
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer  # noqa: E402
from src.backtest.strategies.multi_resonance_timing import (  # noqa: E402
    MultiResonanceTimingStrategy,
)
from src.backtest.strategies.multi_resonance_timing_v2 import (  # noqa: E402
    MultiResonanceTimingV2Strategy,
)

DATA = ROOT / 'data' / 'local_store'
OUT_DIR = ROOT / '2026-11-20' / '策略可行性评估'
INDEX_CODE = '000001.SH'
SELECT_DATE = date(2015, 1, 5)
DATA_START = date(2014, 6, 1)   # 指标暖机（MACD 慢线 26 + 信号 9，60 日窗口）
SAMPLE_START = date(2015, 1, 1)
IS_END = date(2021, 12, 31)
OOS_START = date(2022, 1, 1)
END = date(2099, 1, 1)
INITIAL_CAPITAL = 1_000_000.0
ANALYZER = PerformanceAnalyzer()
VARIANTS = {
    'v1': MultiResonanceTimingStrategy,
    'v2': MultiResonanceTimingV2Strategy,
}


def make_strategy(variant: str, **kwargs: Any) -> Any:
    return VARIANTS[variant](**kwargs)


# ---------------------------------------------------------------------------
# 样本选取
# ---------------------------------------------------------------------------

def select_universe() -> List[str]:
    """2015-01-05 成交额前 10 的「2015 年前上市、至今在市、非 ST」个股。"""
    listing = pl.read_parquet(DATA / 'meta' / 'universe' / 'listing_intervals.parquet')
    st = pl.read_parquet(DATA / 'meta' / 'st_status' / 'st_status_history.parquet')
    st_latest = (
        st.sort('as_of').group_by('code').agg(pl.col('is_st').last())
    )
    last_quote = listing.filter(pl.col('data_type') == 'stock')['last_quote_date'].max()
    cand = listing.filter(
        (pl.col('data_type') == 'stock')
        & pl.col('market').is_in(['sh', 'sz'])
        & (pl.col('listing_status') == 'active')
        & (pl.col('first_trade_date') <= SELECT_DATE)
        & (pl.col('last_quote_date') >= last_quote)
    ).join(st_latest, left_on='ts_code', right_on='code', how='left').filter(
        pl.col('is_st') == False  # noqa: E712 —— 名录侧非 ST 才算合格样本
    ).select('ts_code', 'market')

    ranked = []
    for code, market in cand.iter_rows():
        path = DATA / 'quotes' / 'stock' / market / f'{code}.parquet'
        if not path.exists():
            continue
        row = pl.read_parquet(path, columns=['date', 'amount']).filter(
            pl.col('date') == SELECT_DATE)
        if row.height == 0 or row['amount'][0] is None:
            continue
        ranked.append((float(row['amount'][0]), code))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [code for _, code in ranked[:10]]


# ---------------------------------------------------------------------------
# 回测
# ---------------------------------------------------------------------------

def load_frame(code: str) -> pl.DataFrame:
    if code == INDEX_CODE:
        path = DATA / 'quotes' / 'index' / 'sh' / f'{code}.parquet'
    else:
        market = code.split('.')[1].lower()
        path = DATA / 'quotes' / 'stock' / market / f'{code}.parquet'
    return pl.read_parquet(path).filter(pl.col('date') >= DATA_START).sort('date')


def run_symbol(df: pl.DataFrame, code: str, strategy: MultiResonanceTimingStrategy) -> Dict[str, Any]:
    engine = BacktestEngine(df)
    engine.set_strategy(strategy)
    return engine.run_backtest(
        initial_capital=INITIAL_CAPITAL, ts_code=code, is_st=False)


def run_all(frames: Dict[str, pl.DataFrame], strategy: MultiResonanceTimingStrategy) -> Dict[str, Dict[str, Any]]:
    return {code: run_symbol(df, code, strategy) for code, df in frames.items()}


def variant_run(frames: Dict[str, pl.DataFrame], variant: str,
                **kwargs: Any) -> Any:
    """按变体构造策略 → 逐标的回测 → 等权合成，返回 (策略, 结果, 面板)。"""
    strategy = make_strategy(variant, **kwargs)
    results = run_all(frames, strategy)
    return strategy, results, build_panel(results, frames)


# ---------------------------------------------------------------------------
# 等权合成
# ---------------------------------------------------------------------------

def build_panel(results: Dict[str, Dict[str, Any]],
                frames: Dict[str, pl.DataFrame]) -> Dict[str, Any]:
    """等权组合：各标的日收益取均值再累乘，仓位取各标的仓位均值。"""
    navs: Dict[str, pl.DataFrame] = {}
    weights: Dict[str, pl.DataFrame] = {}
    for code, res in results.items():
        curve = res['equity_curve']
        start = curve[0]['equity']
        navs[code] = pl.DataFrame({
            'date': [c['date'] for c in curve],
            'nav': [c['equity'] / start for c in curve],
        })
        # 持仓股数从成交明细累计；收盘价取行情，缺失日按前值填充
        equity = pl.DataFrame({'date': [c['date'] for c in curve],
                               'equity': [c['equity'] for c in curve]})
        buys = res['trades']
        shares = 0
        trade_idx = 0
        share_rows = []
        for d in frames[code]['date'].to_list():
            while trade_idx < len(buys) and buys[trade_idx]['date'] <= d:
                t = buys[trade_idx]
                shares += t['shares'] if t['signal'] == 'buy' else -t['shares']
                trade_idx += 1
            share_rows.append(shares)
        held = frames[code].select('date', 'close').with_columns(
            pl.Series('shares', share_rows, dtype=pl.Int64)
        ).join(equity, on='date', how='left').with_columns(
            pl.col('equity').forward_fill(),
            (pl.col('shares') * pl.col('close') / pl.col('equity')).alias('weight'),
        ).select('date', 'weight', 'shares')
        weights[code] = held

    dates = sorted({d for df in navs.values() for d in df['date'].to_list()})
    nav_wide = pl.DataFrame({'date': dates})
    weight_wide = pl.DataFrame({'date': dates})
    for code, df in navs.items():
        nav_wide = nav_wide.join(df.rename({'nav': code}), on='date', how='left')
        weight_wide = weight_wide.join(
            weights[code].rename({'weight': f'{code}_w', 'shares': f'{code}_s'}),
            on='date', how='left')
    code_cols = list(navs)
    nav_wide = nav_wide.with_columns([pl.col(c).forward_fill() for c in code_cols])
    weight_wide = weight_wide.with_columns(
        [pl.col(c).forward_fill().fill_null(0.0)
         for c in weight_wide.columns if c != 'date'])

    combined = weight_wide.select(
        pl.col('date'),
        pl.concat_list([pl.col(f'{c}_w') for c in code_cols]).list.mean().alias('weight'),
        pl.concat_list([pl.col(f'{c}_s') for c in code_cols]).alias('shares'),
    ).join(
        nav_wide.select(
            pl.col('date'),
            pl.concat_list([pl.col(c) for c in code_cols]).list.mean().alias('nav')),
        on='date', how='left')
    holdings = [
        ';'.join(c for c, s in zip(code_cols, row, strict=False) if s > 0)
        for row in combined['shares'].to_list()
    ]
    combined = combined.with_columns(
        pl.Series('holdings', holdings, dtype=pl.Utf8)).drop('shares')
    return {'nav': combined, 'date_cols': code_cols, 'nav_wide': nav_wide}


def benchmark_nav(frames: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """买入持有基准：各标的窗口内收盘价归一后等权平均（不计成本）。"""
    dates = sorted({d for df in frames.values()
                    for d in df.filter(pl.col('date') >= SAMPLE_START)['date'].to_list()})
    wide = pl.DataFrame({'date': dates})
    for code, df in frames.items():
        part = df.filter(pl.col('date') >= SAMPLE_START)
        base = part['close'][0]
        wide = wide.join(
            part.select(pl.col('date'), (pl.col('close') / base).alias(code)),
            on='date', how='left')
    code_cols = list(frames)
    wide = wide.with_columns([pl.col(c).forward_fill() for c in code_cols])
    return wide.select(
        pl.col('date'),
        pl.concat_list([pl.col(c) for c in code_cols]).list.mean().alias('nav'),
    )


# ---------------------------------------------------------------------------
# 绩效
# ---------------------------------------------------------------------------

def window_metrics(nav_df: pl.DataFrame, trades: List[Dict[str, Any]],
                   start: Any, end: Any, frame_codes: List[str]) -> Dict[str, Any]:
    part = nav_df.filter((pl.col('date') >= start) & (pl.col('date') <= end))
    part = part.with_columns((pl.col('nav') / pl.col('nav').first()).alias('nav'))
    curve = [{'date': d, 'equity': e * INITIAL_CAPITAL}
             for d, e in zip(part['date'].to_list(), part['nav'].to_list(), strict=False)]
    part_trades = [t for t in trades if start <= t['date'] <= end]
    perf = ANALYZER.analyze(curve, part_trades)
    years = max((part['date'][-1] - part['date'][0]).days / 365.0, 1e-9)
    avg_equity = INITIAL_CAPITAL * float(part['nav'].mean())
    buy_notional = sum(t['price'] * t['shares'] for t in part_trades if t['signal'] == 'buy')
    turnover = ANALYZER.annual_turnover(
        buy_notional / max(len(frame_codes), 1), avg_equity, years)
    return {
        'annual_return': perf.get('annual_return', 0.0),
        'max_drawdown': perf.get('max_drawdown', 0.0),
        'sharpe': perf.get('sharpe_ratio', 0.0),
        'calmar': perf.get('calmar', 0.0),
        'win_rate': perf.get('winning_rate', 0.0),
        'profit_factor': perf.get('average_profit_loss', 0.0),
        'turnover_one_way': turnover['one_way'],
        'trades': len(part_trades),
    }


def merged_trades(results: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for res in results.values():
        out.extend(res['trades'])
    out.sort(key=lambda t: t['date'])
    return out


# ---------------------------------------------------------------------------
# 动作可达性 / 维度独立性
# ---------------------------------------------------------------------------

def action_counts(frames: Dict[str, pl.DataFrame], strategy: Any) -> Dict[str, int]:
    """五行动作逐个统计触发次数（全样本、全部标的合计）。"""
    total = Counter()
    for df in frames.values():
        total.update(strategy.build_signal_frame(df)['trigger_type'].to_list())
    total.pop('无', None)
    return {name: int(total.get(name, 0)) for name in
            ['买入共振', '卖出共振', '震荡低吸', '震荡高抛', '趋势持股']}


def _ratio(a: int, b: int) -> Any:
    return round(a / b, 4) if b else None


def dimension_stats(frames: Dict[str, pl.DataFrame], strategy: Any) -> Dict[str, Any]:
    """D1/D2/D3 两两同真比例（仅 v2 有诊断列；v1 返回空）。"""
    cols = ['d1_buy', 'd2_low', 'd3_buy', 'd1_sell', 'd2_high', 'd3_sell']
    totals = dict.fromkeys(cols, 0)
    pairs = dict.fromkeys(['d1_d2', 'd1_d3', 'd2_d3', 'd1_d2_d3',
                           's1_s2', 's1_s3', 's2_s3', 's1_s2_s3'], 0)
    found = False
    for df in frames.values():
        frame = strategy.build_signal_frame(df)
        if 'd1_buy' not in frame.columns:
            return {}
        found = True
        b = {c: frame[c].to_list() for c in cols}
        totals['d1_buy'] += sum(b['d1_buy'])
        totals['d2_low'] += sum(b['d2_low'])
        totals['d3_buy'] += sum(b['d3_buy'])
        totals['d1_sell'] += sum(b['d1_sell'])
        totals['d2_high'] += sum(b['d2_high'])
        totals['d3_sell'] += sum(b['d3_sell'])
        for i in range(frame.height):
            p1, p2, p3 = b['d1_buy'][i], b['d2_low'][i], b['d3_buy'][i]
            s1, s2, s3 = b['d1_sell'][i], b['d2_high'][i], b['d3_sell'][i]
            pairs['d1_d2'] += int(p1 and p2)
            pairs['d1_d3'] += int(p1 and p3)
            pairs['d2_d3'] += int(p2 and p3)
            pairs['d1_d2_d3'] += int(p1 and p2 and p3)
            pairs['s1_s2'] += int(s1 and s2)
            pairs['s1_s3'] += int(s1 and s3)
            pairs['s2_s3'] += int(s2 and s3)
            pairs['s1_s2_s3'] += int(s1 and s2 and s3)
    if not found:
        return {}
    return {
        'buy_counts': {k: totals[k] for k in ['d1_buy', 'd2_low', 'd3_buy']},
        'buy_pairs': {k: pairs[k] for k in ['d1_d2', 'd1_d3', 'd2_d3', 'd1_d2_d3']},
        'buy_conditional': {
            'P(D2|D1)': _ratio(pairs['d1_d2'], totals['d1_buy']),
            'P(D1|D2)': _ratio(pairs['d1_d2'], totals['d2_low']),
            'P(D3|D1)': _ratio(pairs['d1_d3'], totals['d1_buy']),
            'P(D1|D3)': _ratio(pairs['d1_d3'], totals['d3_buy']),
            'P(D3|D2)': _ratio(pairs['d2_d3'], totals['d2_low']),
            'P(D2|D3)': _ratio(pairs['d2_d3'], totals['d3_buy']),
            'P(D3|D1&D2)': _ratio(pairs['d1_d2_d3'], pairs['d1_d2']),
            'P(D2|D1&D3)': _ratio(pairs['d1_d2_d3'], pairs['d1_d3']),
        },
        'sell_counts': {k: totals[k] for k in ['d1_sell', 'd2_high', 'd3_sell']},
        'sell_conditional': {
            'P(D2|D1)': _ratio(pairs['s1_s2'], totals['d1_sell']),
            'P(D3|D1)': _ratio(pairs['s1_s3'], totals['d1_sell']),
            'P(D1|D3)': _ratio(pairs['s1_s3'], totals['d3_sell']),
            'P(D3|D1&D2)': _ratio(pairs['s1_s2_s3'], pairs['s1_s2']),
        },
    }


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    global OUT_DIR
    parser = argparse.ArgumentParser(description='多指标共振择时回测（--variant v1/v2）')
    parser.add_argument('--variant', choices=['v1', 'v2'], default='v2')
    args = parser.parse_args()
    variant = args.variant
    prefix = 'v2' if variant == 'v2' else ''
    OUT_DIR = ROOT / '2026-11-20' / '策略可行性评估' / ('' if variant == 'v1' else 'v2')
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    codes = select_universe()
    universe = [INDEX_CODE] + codes
    frames = {code: load_frame(code) for code in universe}

    # 本变体基线
    strategy, base_results, panel = variant_run(frames, variant)
    base_trades = merged_trades(base_results)
    bench = benchmark_nav(frames)

    # 净值曲线
    nav_out = panel['nav'].join(bench.rename({'nav': 'bench'}), on='date', how='left')
    nav_out.select(
        pl.col('date').cast(pl.Utf8),
        pl.col('nav').round(6).alias('净值'),
        pl.col('weight').round(4).alias('仓位'),
        pl.col('holdings').alias('持仓标的'),
        pl.col('bench').round(6).alias('基准净值'),
    ).write_csv(OUT_DIR / f'{prefix}净值曲线_2026-11-20.csv')

    # 交易明细（触发信号类型取成交前一交易日）
    rows = []
    triggers = {}
    for code, df in frames.items():
        frame = strategy.build_signal_frame(df)
        triggers[code] = dict(zip(
            frame['date'].to_list(), frame['trigger_type'].to_list(), strict=False))
    for res_code, res in base_results.items():
        dates = frames[res_code]['date'].to_list()
        for t in res['trades']:
            idx = dates.index(t['date']) if t['date'] in dates else 0
            prev = dates[idx - 1] if idx > 0 else None
            rows.append({
                '成交日': t['date'],
                '标的': res_code,
                '方向': '买入' if t['signal'] == 'buy' else '卖出',
                '成交价': round(t['price'], 4),
                '数量': t['shares'],
                '成交额': round(t['price'] * t['shares'], 2),
                '费用': round(t['cost'], 2),
                '触发信号类型': triggers[res_code].get(prev, ''),
            })
    pl.DataFrame(rows, schema={
        '成交日': pl.Date, '标的': pl.Utf8, '方向': pl.Utf8, '成交价': pl.Float64,
        '数量': pl.Int64, '成交额': pl.Float64, '费用': pl.Float64,
        '触发信号类型': pl.Utf8,
    }).sort(['成交日', '标的']).write_csv(OUT_DIR / f'{prefix}交易明细_2026-11-20.csv')

    # 分窗口绩效
    windows = {
        '全样本': (SAMPLE_START, END),
        '样本内': (SAMPLE_START, IS_END),
        '样本外': (OOS_START, END),
    }
    summary = {}
    for label, (start, end) in windows.items():
        summary[label] = {
            '策略': window_metrics(panel['nav'], base_trades, start, end, universe),
            '基准': window_metrics(bench, [], start, end, universe),
        }

    # 无未来函数对照：信号整体再延迟 1 根 bar
    _, lag_results, lag_panel = variant_run(frames, variant, signal_lag=1)
    lag_full = window_metrics(
        lag_panel['nav'], merged_trades(lag_results), SAMPLE_START, END, universe)

    # 参数敏感性（9 组）
    param_sets = [
        ('基线(20/50/80/60)', {}),
        ('RSI低位15', {'rsi_low_center': 15}),
        ('RSI低位25', {'rsi_low_center': 25}),
        ('RSI中位45', {'rsi_mid_center': 45}),
        ('RSI中位55', {'rsi_mid_center': 55}),
        ('RSI高位75', {'rsi_high_center': 75}),
        ('RSI高位85', {'rsi_high_center': 85}),
        ('新高低窗口48', {'high_low_window': 48}),
        ('新高低窗口72', {'high_low_window': 72}),
    ]
    sens_rows = []
    for label, kwargs in param_sets:
        s_i, r_i, p_i = variant_run(frames, variant, **kwargs)
        metrics = window_metrics(p_i['nav'], merged_trades(r_i),
                                 SAMPLE_START, END, universe)
        p = s_i.params
        sens_rows.append({
            '参数组': label,
            'RSI低位': p['rsi_low_center'], 'RSI中位': p['rsi_mid_center'],
            'RSI高位': p['rsi_high_center'], '新高低窗口': p['high_low_window'],
            **{k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()},
        })
    pl.DataFrame(sens_rows).write_csv(OUT_DIR / f'{prefix}参数敏感性_2026-11-20.csv')

    # 逐标的绩效（全样本）
    per_symbol = {}
    for code, res in base_results.items():
        nav_df = pl.DataFrame({
            'date': [c['date'] for c in res['equity_curve']],
            'nav': [c['equity'] / res['equity_curve'][0]['equity'] for c in res['equity_curve']],
        })
        per_symbol[code] = window_metrics(nav_df, res['trades'], SAMPLE_START, END, [code])

    # v1 vs v2 对照（两个变体都跑一遍）
    other = 'v1' if variant == 'v2' else 'v2'
    other_strategy, other_results, other_panel = variant_run(frames, other)
    comparison = {}
    for label, strat, res, pan in (
            (variant, strategy, base_results, panel),
            (other, other_strategy, other_results, other_panel)):
        comparison[label] = {
            'full': window_metrics(pan['nav'], merged_trades(res),
                                   SAMPLE_START, END, universe),
            'avg_position': round(float(pan['nav'].filter(
                pl.col('date') >= SAMPLE_START)['weight'].mean()), 4),
            'actions': action_counts(frames, strat),
            'dimension_stats': dimension_stats(frames, strat),
        }

    payload = {
        'variant': variant,
        'universe': universe,
        'signal_counts_total': sum(action_counts(frames, strategy).values()),
        'actions': action_counts(frames, strategy),
        'dimension_stats': dimension_stats(frames, strategy),
        'avg_position': comparison[variant]['avg_position'],
        'summary': summary,
        'lag1_full': lag_full,
        'per_symbol': per_symbol,
        'trades': len(base_trades),
        'sensitivity': sens_rows,
        'comparison': comparison,
    }
    (OUT_DIR / 'work').mkdir(exist_ok=True)
    metrics_name = 'v2_metrics.json' if variant == 'v2' else 'baseline_metrics.json'
    (OUT_DIR / 'work' / metrics_name).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    zero = [k for k, v in payload['actions'].items() if v == 0]
    print(json.dumps({  # noqa: T201 —— 跑批脚本按契约只回一行标量
        'variant': variant,
        'n_symbols': len(universe),
        'full_ann': round(summary['全样本']['策略']['annual_return'], 2),
        'full_mdd': round(summary['全样本']['策略']['max_drawdown'], 2),
        'is_ann': round(summary['样本内']['策略']['annual_return'], 2),
        'oos_ann': round(summary['样本外']['策略']['annual_return'], 2),
        'lag1_ann': round(lag_full['annual_return'], 2),
        'trades': len(base_trades),
        'zero_actions': zero,
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
