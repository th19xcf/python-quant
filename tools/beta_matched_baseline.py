"""beta 匹配基准对照：策略是「有 alpha 的低 beta 组合」还是「半仓指数」。

做法：
- 策略沿用 P0 的冻结窗与生产参数（Top-20、持有 20 日、固定名义、含成本），
  逐窗 ``simulate_fixed_hold`` 后用 ``combine_curves`` 拼接；
- 对每个被动标的构造 ``w`` 比例指数 + ``(1-w)`` 现金、每日再平衡，``w`` 取策略
  相对该指数的同期日频 beta，使对照组合与策略 beta 相同；
- 基准起算点统一复用 ``tools/benchmark_alignment.py``（剔除 d0、d1 收盘归一）；
- 只读：不写 ``src/**``、不改数据、不写生产配置。

用法::

    python tools/beta_matched_baseline.py
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.factor_eval import (  # noqa: E402
    SimConfig,
    build_composite,
    combine_curves,
    simulate_fixed_hold,
)
from tools.benchmark_alignment import (  # noqa: E402
    aligned_benchmark_curve,
    daily_excess_stats,
    same_start_excess_annualized,
)

DEFAULT_DATASETS: tuple[dict[str, Any], ...] = (
    {
        'tag': 'pool2000',
        'matrix': 'tmp/eval_daily_advice/'
                  'factors_strat2000s20261105_2021-01-01_2026-11-01.parquet',
        'windows': 'tmp/kelly_fixed_matrix/windows_frozen_alt2000.json',
    },
    {
        'tag': 'frozen500',
        'matrix': 'tmp/kelly_fixed_matrix/matrix_frozen_500.parquet',
        'windows': 'tmp/kelly_fixed_matrix/windows_frozen_500.json',
    },
)

#: 被动对照：指数/ETF 的本地日线文件（只读）；``weight`` 缺省时按同 beta 反解
PASSIVE: tuple[dict[str, Any], ...] = (
    {
        'tag': 'C1_csi300',
        'label': 'C1 沪深300 + 现金',
        'path': 'data/local_store/quotes/index/sh/000300.SH.parquet',
    },
    {
        'tag': 'C1b_csi300_w055',
        'label': 'C1b 沪深300 强制 w=0.55 + 现金',
        'path': 'data/local_store/quotes/index/sh/000300.SH.parquet',
        'weight': 0.55,
    },
    {
        'tag': 'C2_csi500',
        'label': 'C2 中证500 + 现金',
        'path': 'data/local_store/quotes/index/sh/000905.SH.parquet',
    },
    {
        'tag': 'C2b_csi1000',
        'label': 'C2b 中证1000 + 现金',
        'path': 'data/local_store/quotes/index/sh/000852.SH.parquet',
    },
    {
        'tag': 'C3_div_lowvol',
        'label': 'C3 红利低波ETF + 现金',
        'path': 'data/local_store/quotes/fund/sh/512890.SH.parquet',
    },
)


def load_prices(path: Path) -> pl.DataFrame:
    """读取本地日线，统一为 ``date``(Date) / ``close`` 两列。"""
    frame = pl.read_parquet(path, columns=['date', 'close'])
    return (frame.with_columns(pl.col('date').cast(pl.Date))
            .sort('date')
            .select('date', 'close'))


def load_windows(path: Path) -> list[dict[str, Any]]:
    """读取冻结窗口，日期转 ``date``。"""
    payload = json.loads(path.read_text(encoding='utf-8'))
    return [{
        'test_start': date.fromisoformat(str(item['test_start'])[:10]),
        'test_end': date.fromisoformat(str(item['test_end'])[:10]),
        'weights': {str(k): float(v) for k, v in item['weights'].items()},
    } for item in payload['windows']]


def required_columns(windows: list[dict[str, Any]]) -> list[str]:
    """策略撮合所需列（与 P0 审计一致）。"""
    base = ['date', 'ts_code', 'ret', 'close', 'open', 'raw_open',
            'raw_close', 'pre_close', 'volume', 'history_rows']
    extra: set[str] = set()
    for item in windows:
        extra.update(item['weights'])
    return sorted(set(base) | extra)


def index_window_curve(prices: pl.DataFrame, window: dict[str, Any],
                       code: str) -> pl.DataFrame:
    """单窗指数净值：起点口径复用 ``aligned_benchmark_curve``（d1 收盘=1.0）。"""
    frame = prices.with_columns(
        (pl.col('close') / pl.col('close').shift(1) - 1.0).alias('ret'),
        pl.lit(code).alias('ts_code'))
    chunk = frame.filter((pl.col('date') >= window['test_start'])
                         & (pl.col('date') < window['test_end']))
    chunk = chunk.drop_nulls('ret')
    return aligned_benchmark_curve(chunk)


def _aligned_returns(left: pl.DataFrame, right: pl.DataFrame
                     ) -> tuple[np.ndarray, np.ndarray]:
    """两条净值曲线同日对齐后的日收益序列。"""
    a = left.select('date', (pl.col('equity') / pl.col('equity').shift(1) - 1.0)
                    .alias('rs'))
    b = right.select('date', (pl.col('equity') / pl.col('equity').shift(1) - 1.0)
                     .alias('rb'))
    joined = a.join(b, on='date', how='inner').drop_nulls(['rs', 'rb'])
    return joined['rs'].to_numpy(), joined['rb'].to_numpy()


def matched_beta(strategy: list[pl.DataFrame], passive: list[pl.DataFrame]) -> float:
    """同期日频回归的 beta：``cov(策略, 指数)/var(指数)``，跨窗拼接。"""
    rs_parts: list[np.ndarray] = []
    rb_parts: list[np.ndarray] = []
    for left, right in zip(strategy, passive, strict=True):
        rs, rb = _aligned_returns(left, right)
        rs_parts.append(rs)
        rb_parts.append(rb)
    rs = np.concatenate(rs_parts) if rs_parts else np.array([])
    rb = np.concatenate(rb_parts) if rb_parts else np.array([])
    if rs.size < 3 or float(rb.var(ddof=1)) <= 0:
        return 0.0
    return float(np.cov(rs, rb, ddof=1)[0, 1] / float(rb.var(ddof=1)))


def weight_from_beta(beta: float) -> float:
    """同 beta 的指数权重：限定在 [0, 1]（负 beta 或 >1 无法用现金+指数复现）。"""
    if not np.isfinite(beta):
        return 0.0
    return float(min(max(beta, 0.0), 1.0))


def weighted_curve(index_curve: pl.DataFrame, weight: float) -> pl.DataFrame:
    """``weight`` 指数 + ``(1-weight)`` 现金、每日再平衡的净值曲线。"""
    if index_curve.is_empty():
        return index_curve
    ret = (index_curve['equity'] / index_curve['equity'].shift(1) - 1.0).fill_null(0.0)
    equity = (ret * weight + 1.0).cum_prod()
    return index_curve.select('date').with_columns(equity.alias('equity'))


def window_net(curve: pl.DataFrame) -> float:
    """窗内同起点净收益（首点归一）。"""
    if curve.is_empty():
        return 0.0
    return float(curve['equity'][-1] / curve['equity'][0] - 1.0)


def _agg_annualized(curves: list[pl.DataFrame]) -> float:
    """拼接后的年化（未加现金腿的原始指数收益，仅作对照）。"""
    _, agg = combine_curves(curves)
    return float(agg['annualized'])


def run_dataset(dataset: dict[str, Any], sim: SimConfig) -> dict[str, Any]:
    matrix = pl.read_parquet(ROOT / dataset['matrix'],
                             columns=required_columns(load_windows(
                                 ROOT / dataset['windows'])))
    windows = load_windows(ROOT / dataset['windows'])

    strategy_curves: list[pl.DataFrame] = []
    strategy_rows: list[dict[str, Any]] = []
    for item in windows:
        chunk = matrix.filter((pl.col('date') >= item['test_start'])
                              & (pl.col('date') < item['test_end']))
        composite = build_composite(chunk, item['weights'])
        curve, _ = simulate_fixed_hold(composite, sim)
        strategy_curves.append(curve)
        strategy_rows.append({
            'test': f"{item['test_start']}~{item['test_end']}",
            'days': chunk['date'].n_unique(),
            'strategy_net': window_net(curve),
        })
    strategy_combined, strategy_agg = combine_curves(strategy_curves)
    n_days = strategy_combined.height

    rows = [{
        'tag': 'strategy', 'label': '策略（Top-20/持有20日/固定名义）',
        'weight_vs_index': 1.0,
        'strategy_beta_vs_index': None,
        'weight_mode': 'none',
        'annualized': strategy_agg['annualized'],
        'max_drawdown': strategy_agg['max_drawdown'],
        'calmar': strategy_agg['calmar'],
        'sharpe': strategy_agg['sharpe'],
        'annual_volatility': strategy_agg['annual_volatility'],
        'excess_vs_self': 0.0,
        'excess_t': 0.0,
        'excess_std': 0.0,
        'beta_check': 1.0,
        'index_annualized': None,
    }]
    per_window: dict[str, list[dict[str, Any]]] = {'strategy': strategy_rows}
    for item in PASSIVE:
        prices = load_prices(ROOT / item['path'])
        code = Path(item['path']).stem
        index_curves = [index_window_curve(prices, win, code) for win in windows]
        beta = matched_beta(strategy_curves, index_curves)
        weight = float(item['weight']) if 'weight' in item \
            else round(weight_from_beta(beta), 3)
        weight_mode = 'fixed' if 'weight' in item else 'matched'
        combo_curves = [weighted_curve(curve, weight) for curve in index_curves]
        combo_combined, combo_agg = combine_curves(combo_curves)
        index_combined, _ = combine_curves(index_curves)
        daily = daily_excess_stats(strategy_combined, combo_combined)
        check = daily_excess_stats(combo_combined, index_combined)
        excess_list = [
            same_start_excess_annualized(
                {'net_return': srow['strategy_net']},
                {'net_return': window_net(cc)}, srow['days'])
            for srow, cc in zip(strategy_rows, combo_curves, strict=True)
        ]
        per_window[item['tag']] = [{
            'test': srow['test'],
            'days': srow['days'],
            'strategy_net': srow['strategy_net'],
            'passive_net': window_net(cc),
            'passive_index_net': window_net(ic),
            'excess_annualized': excess_list[offset],
        } for offset, (srow, cc, ic) in enumerate(
            zip(strategy_rows, combo_curves, index_curves, strict=True))]
        rows.append({
            'tag': item['tag'],
            'label': item['label'],
            'weight_vs_index': weight,
            'strategy_beta_vs_index': beta,
            'weight_mode': weight_mode,
            'annualized': combo_agg['annualized'],
            'max_drawdown': combo_agg['max_drawdown'],
            'calmar': combo_agg['calmar'],
            'sharpe': combo_agg['sharpe'],
            'annual_volatility': combo_agg['annual_volatility'],
            'excess_vs_self': same_start_excess_annualized(
                strategy_agg, combo_agg, n_days),
            'excess_t': daily.get('excess_t', 0.0),
            'excess_std': float(np.std(excess_list, ddof=1))
            if len(excess_list) > 1 else 0.0,
            'beta_check': check.get('beta', 0.0),
            'index_annualized': _agg_annualized(index_curves),
        })

    # 涨跌窗分解（仅对 C1）：策略在指数下跌窗是否跑赢、上涨窗是否跑输
    c1 = next(item for item in per_window if item.startswith('C1'))
    down = [row['excess_annualized'] for row in per_window[c1]
            if row['passive_net'] < 0]
    up = [row['excess_annualized'] for row in per_window[c1]
          if row['passive_net'] >= 0]
    return {
        'tag': dataset['tag'],
        'days': n_days,
        'n_windows': len(windows),
        'rows': rows,
        'per_window': per_window,
        'up_down_c1': {
            'down_windows': len(down),
            'down_excess_mean': float(np.mean(down)) if down else 0.0,
            'up_windows': len(up),
            'up_excess_mean': float(np.mean(up)) if up else 0.0,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='beta 匹配基准对照')
    parser.add_argument('--out-dir', default='reports/beta_matched')
    parser.add_argument('--tag', action='append', default=None)
    args = parser.parse_args(argv)

    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    sim = SimConfig(top_k=20, hold_days=20)
    selected = [item for item in DEFAULT_DATASETS
                if not args.tag or item['tag'] in args.tag]
    summaries = []
    for dataset in selected:
        summary = run_dataset(dataset, sim)
        summaries.append(summary)
        table = pl.DataFrame(summary['rows'])
        table.write_csv(out_dir / f'beta_match_{summary["tag"]}.csv')
        windows = pl.concat([
            pl.DataFrame(rows).with_columns(pl.lit(tag).alias('benchmark'))
            for tag, rows in summary['per_window'].items()
        ], how='diagonal_relaxed')
        windows.write_csv(out_dir / f'beta_windows_{summary["tag"]}.csv')
        print(f"\n[{summary['tag']}] {summary['days']} 交易日 / "
              f"{summary['n_windows']} 窗")
        print(table.select(
            'label', 'weight_vs_index', 'annualized', 'annual_volatility',
            'max_drawdown', 'calmar', 'sharpe', 'excess_vs_self', 'excess_t',
            'excess_std', 'beta_check', 'index_annualized'))
        print('涨跌窗分解(C1):', summary['up_down_c1'])

    (out_dir / 'beta_match_summary.json').write_text(
        json.dumps({'datasets': summaries}, ensure_ascii=False,
                   indent=2, default=str),
        encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
