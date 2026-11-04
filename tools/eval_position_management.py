"""只读评估：凯利仓位叠加 + 金字塔加仓 / 网格仓位的样本外检验。

不修改 ``src/**``；仅读取现有 OOS 净值、因子面板、walk-forward 窗口与本地行情，
输出到 ``reports/kelly_sizing/``。

用法：
    .venv\\Scripts\\python.exe tools/eval_position_management.py
"""

from __future__ import annotations

import json
import math
import pickle
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice import regime
from src.advice.factor_eval import (
    EvalConfig,
    SimConfig,
    _curve_stats,
    add_forward_returns,
    build_composite,
    combine_curves,
    simulate_fixed_hold,
    walk_forward,
)
from tools.eval_daily_advice import _legacy_weights, factor_list
from tools.position_management_sim import (
    StagedConfig,
    StagedResult,
    build_atr_map,
    simulate_buy_hold,
    simulate_equal,
    simulate_grid,
    simulate_staged,
)

OUT = ROOT / 'reports' / 'kelly_sizing'
MATRIX = (ROOT / 'tmp' / 'eval_daily_advice'
          / 'factors_strat500s20261105_2021-01-01_2026-11-01.parquet')
WINDOWS = ROOT / 'reports' / 'eval_daily_advice' / 'windows_cache.pkl'
EQUITY = ROOT / 'reports' / 'eval_daily_advice' / 'equity_extended_20.csv'
INITIAL = 1_000_000.0
ANNUAL = 252.0

#: 任务给定的历史基准（旧股票池；与本次可复现序列存在池子差异）
GIVEN_BASE = {
    'calmar': 0.80, 'annualized': 0.3532, 'worst_window': -0.2405,
    'turnover_annual': 8.94,
}
GIVEN_R1 = {'calmar': 1.32, 'annualized': 0.3486, 'worst_window': -0.0887,
            'turnover_annual': 20.1, 'cost_annual': 0.0089}
THRESHOLDS = {'calmar_improve': 0.15, 'annual_drop_pp': 0.01,
              'worst_improve': 0.30, 'cost_increment': 0.005}


@dataclass(frozen=True)
class Window:
    """walk-forward 测试窗。"""

    test_start: date
    test_end: date
    weights: dict[str, float]


# ----------------------------------------------------------------------
# 数据与基准
# ----------------------------------------------------------------------
def load_windows(matrix: pl.DataFrame) -> tuple[list[Window], dict[str, Any]]:
    """固定矩阵 + 固定因子清单重算 7 窗，并把快照写入本任务目录。

    共享 ``windows_cache.pkl`` 会被并行任务用其它股票池覆盖，不能作为本任务依据。
    """
    import hashlib

    digest = hashlib.sha256()
    with MATRIX.open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    matrix_sha = digest.hexdigest()
    cache_key = {
        'matrix_sha256': matrix_sha, 'start': '2021-01-01',
        'end': '2026-11-01', 'new_share': 0.70, 'version': 1,
    }
    snapshot = OUT / 'windows_snapshot.pkl'
    windows: list[Window] | None = None
    meta: dict[str, Any] = {}
    if snapshot.is_file():
        with snapshot.open('rb') as handle:
            cached = pickle.load(handle)  # noqa: S301 本任务自生成
        if cached.get('key') == cache_key:
            windows = [Window(item['test_start'], item['test_end'],
                              dict(item['weights']))
                       for item in cached['windows']]
            meta['windows_source'] = 'snapshot'
            meta['n_factors'] = cached.get('n_factors')
    if windows is None:
        factors = factor_list(matrix)
        frame = add_forward_returns(matrix, (5, 20))
        config = EvalConfig(start='2021-01-01', end='2026-11-01',
                            universe_size=500, horizon=20, embargo=20,
                            top_k=20, hold_days=20)
        results = walk_forward(frame, factors, config,
                               baseline_weights=_legacy_weights('mid'),
                               new_share=0.70)
        windows = [Window(item.test_start, item.test_end, dict(item.weights))
                   for item in results]
        payload = {'key': cache_key, 'n_factors': len(factors), 'windows': [
            {'test_start': w.test_start, 'test_end': w.test_end,
             'weights': w.weights} for w in windows]}
        OUT.mkdir(parents=True, exist_ok=True)
        snapshot.write_bytes(pickle.dumps(payload))
        meta['windows_source'] = 'recomputed'
        meta['n_factors'] = len(factors)
    meta.update({
        'matrix': str(MATRIX.relative_to(ROOT)), 'matrix_sha256': matrix_sha,
        'cache_key': cache_key, 'n_windows': len(windows),
        'test_spans': [[str(w.test_start), str(w.test_end)] for w in windows],
    })
    return windows, meta


def simulate_windows_equal(matrix: pl.DataFrame,
                            windows: list[Window]) -> list[StagedResult]:
    results = []
    for item in windows:
        chunk = matrix.filter(
            (pl.col('date') >= item.test_start) & (pl.col('date') < item.test_end))
        results.append(simulate_equal(build_composite(chunk, item.weights)))
    return results


def verify_equity(results: list[StagedResult]) -> dict[str, Any]:
    """用现有引擎逐窗重算，并与仓库现成 OOS 净值对齐。"""
    curves = [item.curve for item in results]
    combined, stats = combine_curves(curves)
    reference = pl.read_csv(EQUITY).with_columns(
        pl.col('date').cast(pl.String).str.to_date())
    joined = combined.join(reference, on='date', how='inner', suffix='_ref')
    max_diff = (joined['equity'] - joined['equity_ref']).abs().max() \
        if not joined.is_empty() else None
    return {
        'combined': _clean(stats),
        'rows': combined.height,
        'reference_rows': reference.height,
        'max_abs_equity_diff': _num(max_diff) if max_diff is not None else None,
    }


# ----------------------------------------------------------------------
# 指标与仓位
# ----------------------------------------------------------------------
def _num(value: Any, default: float = 0.0) -> float:
    """Polars 标量（宽 Union）安全转 float。"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_clean(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def daily_returns(equity: pl.DataFrame) -> np.ndarray:
    values = equity['equity'].to_numpy()
    out = np.empty(len(values), dtype=float)
    out[0] = values[0] / INITIAL - 1.0
    out[1:] = np.diff(values) / values[:-1]
    return out


def compute_position(rets: np.ndarray, *, window: int | None, c: float,
                     low: float, high: float, shrink: bool = False,
                     min_obs: int = 60) -> dict[str, np.ndarray]:
    """滚动 μ̂/σ̂² → 分数凯利；t 日收盘计算，调用方负责 shift(1)。"""
    n = len(rets)
    pos = np.ones(n, dtype=float)
    fstar = np.zeros(n, dtype=float)
    mu_arr = np.zeros(n, dtype=float)
    var_arr = np.full(n, np.nan, dtype=float)
    se_arr = np.full(n, np.nan, dtype=float)
    for i in range(n):
        start = 0 if window is None else max(0, i - window + 1)
        sample = rets[start:i + 1]
        expanding = rets[:i + 1]
        if len(sample) < min_obs:
            continue
        mu = float(np.mean(sample))
        var = float(np.var(sample, ddof=1))
        if var <= 1e-12:
            continue
        if shrink:
            mu = 0.5 * mu + 0.5 * float(np.mean(expanding))
        f = max(mu, 0.0) / var
        fstar[i] = f
        mu_arr[i] = mu
        var_arr[i] = var
        se_arr[i] = math.sqrt(var / len(sample))
        pos[i] = min(max(c * f, low), high)
    return {'position': pos, 'fstar': fstar, 'mu': mu_arr, 'var': var_arr,
            'se': se_arr}


def overlay_global(position: pl.DataFrame, base_curves: list[pl.DataFrame],
                   dates: list[date], cost_model: Any) -> tuple[
                       list[pl.DataFrame], dict[str, float]]:
    """全局 t+1 执行仓位叠加到逐窗净值（跨窗边界也不丢 shift）。"""
    frame = position.sort('date').with_columns(
        pl.col('position').shift(1).fill_null(1.0).alias('exec_position'))
    frame = frame.with_columns(
        (pl.col('exec_position') - pl.col('exec_position').shift(1)
         ).fill_null(0.0).alias('delta'))
    rates = regime.cost_rate_frame(dates, cost_model)
    merged = frame.join(rates, on='date', how='left').sort('date')
    curves: list[pl.DataFrame] = []
    total_cost = 0.0
    total_turnover = 0.0
    exposure_days = 0.0
    for base in base_curves:
        joined = base.join(merged, on='date', how='left').sort('date')
        values = joined['equity'].to_numpy()
        gross = np.empty(len(values), dtype=float)
        gross[0] = values[0] / INITIAL - 1.0
        gross[1:] = np.diff(values) / values[:-1]
        exec_pos = joined['exec_position'].fill_null(1.0).to_numpy()
        delta = joined['delta'].fill_null(0.0).to_numpy()
        buy = joined['buy_rate'].fill_null(0.0).to_numpy()
        sell = joined['sell_rate'].fill_null(0.0).to_numpy()
        cost = np.where(delta > 0, delta * buy, -delta * sell)
        net = exec_pos * gross - cost
        equity = INITIAL * np.cumprod(1.0 + net)
        curves.append(pl.DataFrame({'date': joined['date'],
                                    'equity': equity}))
        total_cost += float(cost.sum())
        total_turnover += float(np.abs(delta).sum())
        exposure_days += float(exec_pos.sum())
    diag = {
        'overlay_cost_total': total_cost,
        'overlay_turnover_total': total_turnover,
        'mean_exposure': exposure_days / max(1, sum(c.height for c in curves)),
    }
    return curves, diag


def scheme_metrics(curves: list[pl.DataFrame], base_curves: list[pl.DataFrame],
                   diag: dict[str, float]) -> dict[str, Any]:
    combined, stats = combine_curves(curves)
    windows = []
    for curve in curves:
        item = _curve_stats(curve, INITIAL, curve.height)
        windows.append({
            'net_return': item['net_return'], 'annualized': item['annualized'],
            'max_drawdown': item['max_drawdown'], 'calmar': item['calmar'],
            'sharpe': item['sharpe'],
        })
    years = max(1e-9, combined.height / ANNUAL)
    return {
        'combined': _clean(stats),
        'net_return': float(stats['net_return']),
        'annualized': float(stats['annualized']),
        'max_drawdown': float(stats['max_drawdown']),
        'calmar': float(stats['calmar']),
        'sharpe': float(stats['sharpe']),
        'worst_window': float(min(item['net_return'] for item in windows)),
        'windows': windows,
        'days': combined.height,
        'years': years,
        'overlay_cost_total': diag['overlay_cost_total'],
        'overlay_cost_annual': diag['overlay_cost_total'] / years,
        'turnover_annual': diag['overlay_turnover_total'] / years,
        'mean_exposure': diag['mean_exposure'],
        'curve': combined, 'curves': curves,
    }


def base_scheme(results: list[StagedResult]) -> dict[str, Any]:
    curves = [item.curve for item in results]
    combined, stats = combine_curves(curves)
    windows = [{'net_return': item.stats['net_return']} for item in results]
    years = max(1e-9, combined.height / ANNUAL)
    return {
        'combined': _clean(stats), 'net_return': float(stats['net_return']),
        'annualized': float(stats['annualized']),
        'max_drawdown': float(stats['max_drawdown']),
        'calmar': float(stats['calmar']), 'sharpe': float(stats['sharpe']),
        'worst_window': float(min(item['net_return'] for item in windows)),
        'windows': windows, 'days': combined.height, 'years': years,
        'curve': combined, 'curves': curves,
        'base_cost_annual': float(sum(
            item.diagnostics.get('total_cost', 0.0) for item in results)
            / years / max(_num(combined['equity'].mean()), 1.0)),
        'base_turnover_annual': float(
            np.mean([item.diagnostics.get('turnover_annual', 0.0)
                     for item in results])),
    }


def r1_position(dates: list[date]) -> pl.DataFrame:
    index_frame = regime.load_index_daily('000300.SH')
    rule = regime.ma_position(index_frame, regime.RegimeParams())
    frame = pl.DataFrame({'date': dates}).join(rule, on='date', how='left')
    frame = frame.with_columns(pl.col('position').fill_null(1.0))
    return frame


def min_position(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
    joined = a.rename({'position': 'pa'}).join(
        b.rename({'position': 'pb'}), on='date', how='inner')
    return joined.select(
        'date', pl.min_horizontal(pl.col('pa'), pl.col('pb')).alias('position'))


# ----------------------------------------------------------------------
# 估计误差
# ----------------------------------------------------------------------
def bootstrap_ci(values: np.ndarray, *, n_boot: int = 1000,
                 seed: int = 20261105) -> dict[str, float]:
    if len(values) < 10:
        return {}
    rng = np.random.default_rng(seed)
    means = np.array([values[rng.integers(0, len(values), len(values))].mean()
                      for _ in range(n_boot)])
    annual = means * ANNUAL
    return {
        'mean_annual': float(values.mean() * ANNUAL),
        'ci_low': float(np.percentile(annual, 2.5)),
        'ci_high': float(np.percentile(annual, 97.5)),
    }


def distribution(values: np.ndarray) -> dict[str, float]:
    clean = values[np.isfinite(values)]
    if not len(clean):
        return {}
    return {
        'p5': float(np.percentile(clean, 5)), 'p25': float(np.percentile(clean, 25)),
        'p50': float(np.percentile(clean, 50)), 'p75': float(np.percentile(clean, 75)),
        'p95': float(np.percentile(clean, 95)), 'min': float(clean.min()),
        'max': float(clean.max()), 'mean': float(clean.mean()),
        'std': float(clean.std(ddof=1)),
    }


# ----------------------------------------------------------------------
# 金字塔 / 网格
# ----------------------------------------------------------------------
def window_return(curve: pl.DataFrame, start: date, end: date) -> float:
    frame = curve.sort('date')
    values = frame['equity'].to_numpy()
    idx = np.where((frame['date'] >= start) & (frame['date'] < end))[0]
    if not len(idx):
        return 0.0
    first = int(idx[0])
    last = int(idx[-1])
    base = values[first - 1] if first > 0 else INITIAL
    return float(values[last] / base - 1.0)


def staged_windows(matrix: pl.DataFrame, windows: list[Window],
                   cfg: StagedConfig, *, atr: bool = False) -> list[StagedResult]:
    results = []
    for item in windows:
        chunk = matrix.filter(
            (pl.col('date') >= item.test_start) & (pl.col('date') < item.test_end))
        chunk = build_composite(chunk, item.weights)
        atr_map = {}
        if atr:
            buffer = matrix.filter(
                (pl.col('date') >= item.test_start - timedelta(days=120))
                & (pl.col('date') < item.test_end))
            atr_map = build_atr_map(buffer)
        results.append(simulate_staged(chunk, cfg, atr_map=atr_map))
    return results


def aggregate_staged(results: list[StagedResult]) -> dict[str, Any]:
    curves = [item.curve for item in results]
    combined, stats = combine_curves(curves)
    years = max(1e-9, combined.height / ANNUAL)
    entries = sum(item.diagnostics.get('entries', 0) for item in results)
    multi = sum(item.diagnostics.get('positions_with_add', 0) for item in results)
    add_events = sum(item.diagnostics.get('add_events', 0) for item in results)
    stops = {'stop_entry': 0, 'stop_trail': 0, 'time': 0}
    for item in results:
        for key, value in item.diagnostics.get('stop_counts', {}).items():
            stops[key] = stops.get(key, 0) + int(value)
    total_cost = sum(item.diagnostics.get('total_cost', 0.0) for item in results)
    trades = [trade for item in results for trade in item.trades]
    wins = [t['ret'] for t in trades if t['ret'] > 0]
    losses = [t['ret'] for t in trades if t['ret'] <= 0]
    avg_equity = _num(combined['equity'].mean())
    return {
        'combined': _clean(stats), 'net_return': float(stats['net_return']),
        'annualized': float(stats['annualized']),
        'max_drawdown': float(stats['max_drawdown']),
        'calmar': float(stats['calmar']), 'sharpe': float(stats['sharpe']),
        'worst_window': float(min(item.stats['net_return'] for item in results)),
        'days': combined.height, 'years': years, 'curve': combined,
        'windows': [{'net_return': item.stats['net_return']} for item in results],
        'entries': entries, 'positions_with_add': multi,
        'add_events': add_events,
        'add_trigger_ratio': multi / max(1, entries),
        'single_tranche_ratio': 1.0 - multi / max(1, entries),
        'stop_counts': stops, 'open_positions': sum(
            item.diagnostics.get('open_positions', 0) for item in results),
        'capped_entries': sum(item.diagnostics.get('capped_entries', 0)
                              for item in results),
        'total_cost': total_cost,
        'cost_ratio': total_cost / (INITIAL * max(1, len(results))),
        'cost_annual': total_cost / max(avg_equity, 1.0) / years,
        'turnover_annual': sum(item.costs.get('buy_notional', 0.0)
                               for item in results) / max(avg_equity, 1.0) / years,
        'win_rate': len(wins) / max(1, len(trades)),
        'profit_factor': (sum(wins) / abs(sum(losses))) if losses else None,
        'closed_trades': len(trades), 'trades': trades, 'curves': curves,
    }


def acceptance(scheme: dict[str, Any], base: dict[str, Any],
               base_cost_annual: float, given: dict[str, float] | None = None
               ) -> dict[str, Any]:
    c1_defined = base['calmar'] > 0
    c1 = (c1_defined and scheme['calmar']
          >= base['calmar'] * (1.0 + THRESHOLDS['calmar_improve']))
    c2 = scheme['annualized'] >= base['annualized'] - THRESHOLDS['annual_drop_pp']
    c3 = ((scheme['worst_window'] - base['worst_window'])
          / abs(base['worst_window']) >= THRESHOLDS['worst_improve'])
    raw_cost = scheme.get('cost_annual',
                          scheme.get('overlay_cost_annual', 0.0))
    c4 = (raw_cost - base_cost_annual <= THRESHOLDS['cost_increment'])
    nominal = None
    if given:
        nominal = {
            'calmar_improve': scheme['calmar'] / given['calmar'] - 1.0,
            'annual_drop_pp': given['annualized'] - scheme['annualized'],
            'worst_improve': (scheme['worst_window'] - given['worst_window'])
            / abs(given['worst_window']),
        }
    return {
        'calmar_improve': scheme['calmar'] / base['calmar'] - 1.0,
        'annual_drop_pp': base['annualized'] - scheme['annualized'],
        'worst_improve': (scheme['worst_window'] - base['worst_window'])
        / abs(base['worst_window']),
        'cost_increment': raw_cost - base_cost_annual,
        'pass_c1': bool(c1), 'pass_c1_defined': bool(c1_defined),
        'pass_c2': bool(c2), 'pass_c3': bool(c3),
        'pass_c4': bool(c4), 'pass_all': bool(c1_defined and c1 and c2 and c3 and c4),
        'nominal_vs_given': nominal,
    }


def _net_returns(items: list[Any]) -> list[float]:
    return [float(item['net_return']) if isinstance(item, dict) else float(item)
            for item in items]


def leave_one_out(scheme: dict[str, Any], base: dict[str, Any],
                  scheme_curves: list[pl.DataFrame],
                  base_curves: list[pl.DataFrame]) -> dict[str, Any]:
    improvements = [a - b for a, b in zip(
        _net_returns(scheme['windows']), _net_returns(base['windows']),
        strict=True)]
    key = int(np.argmax(improvements))

    def _calmar_drop(drop: int, curves: list[pl.DataFrame]) -> float:
        kept = [curve for idx, curve in enumerate(curves) if idx != drop]
        _, stats = combine_curves(kept)
        return float(stats['calmar'])

    best = int(np.argmax(_net_returns(scheme['windows'])))
    scheme_after = _calmar_drop(key, scheme_curves)
    base_after = _calmar_drop(key, base_curves)
    improve_after = scheme_after / base_after - 1.0 if base_after else 0.0
    scheme_best = _calmar_drop(best, scheme_curves)
    base_best = _calmar_drop(best, base_curves)
    rows = []
    for idx in range(len(scheme_curves)):
        sc = _calmar_drop(idx, scheme_curves)
        bc = _calmar_drop(idx, base_curves)
        rows.append({'drop_window': idx + 1, 'scheme_calmar': sc,
                     'base_calmar': bc,
                     'improve': sc / bc - 1.0 if bc else 0.0})
    return {
        'key_window_index': key,
        'key_window_improvement_pp': improvements[key],
        'per_window_improvement_pp': improvements,
        'calmar_after_drop_key': scheme_after,
        'base_calmar_after_drop_key': base_after,
        'improve_after_drop_key': improve_after,
        'single_window_dependency': bool(improve_after <= 0.0),
        'best_window_index': best,
        'calmar_after_drop_best': scheme_best,
        'base_calmar_after_drop_best': base_best,
        'drop_each': rows,
    }


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def _log(message: str) -> None:
    print(f'[stage] {message}', flush=True)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    matrix = pl.read_parquet(MATRIX).with_columns(pl.col('date').cast(pl.Date))
    _log(f'matrix loaded: {matrix.height} rows')
    windows, meta = load_windows(matrix)
    _log(f'windows ready: {len(windows)} ({meta["windows_source"]})')
    base_results = simulate_windows_equal(matrix, windows)
    _log('base equal-weight simulated')
    base = base_scheme(base_results)
    dates = base['curve']['date'].to_list()
    rets = daily_returns(base['curve'])
    cost_model = SimConfig().make_cost_model()
    r1 = r1_position(dates)

    # 与旧引擎逐窗对齐（防止模拟器实现漂移）
    engine_diffs = []
    for item, result in zip(windows, base_results, strict=True):
        chunk = matrix.filter(
            (pl.col('date') >= item.test_start) & (pl.col('date') < item.test_end))
        engine_curve, _ = simulate_fixed_hold(
            build_composite(chunk, item.weights), SimConfig())
        joined = engine_curve.join(result.curve, on='date', suffix='_new')
        engine_diffs.append(_num(
            (joined['equity'] - joined['equity_new']).abs().max()))

    kelly = evaluate_kelly(base, base_results, dates, rets, r1, cost_model)
    _log('kelly schemes done')
    pyramid = evaluate_pyramid(matrix, windows, base, base_results, r1,
                               cost_model, dates)
    _log('pyramid schemes done')
    grid = evaluate_grid(windows, dates)
    _log('grid schemes done')

    import hashlib

    equity_sha = hashlib.sha256(EQUITY.read_bytes()).hexdigest()
    payload = {
        'meta': {
            **meta,
            'reference_equity_file': {
                'path': str(EQUITY.relative_to(ROOT)),
                'sha256': equity_sha,
                'mtime': EQUITY.stat().st_mtime,
                'note': '该共享文件已被 universe=2000 的并行任务覆盖，未用作本任务基准；'
                        '本任务基准为固定矩阵 + 固定因子清单重算的 7 窗 OOS',
            },
            'max_engine_equity_diff': max(engine_diffs),
            'given_reference': GIVEN_BASE,
            'given_r1': GIVEN_R1,
            'thresholds': THRESHOLDS,
            'cost_round_trip': float(
                cost_model.params['commission_rate'] * 2
                + cost_model.params['stamp_tax_rate']
                + cost_model.params['transfer_fee_rate'] * 2
                + cost_model.params['slippage'] * 2),
        },
        'kelly': kelly,
        'pyramid_grid': {'pyramid': pyramid, 'grid': grid},
    }
    payload = _clean(payload)
    (OUT / 'kelly_result.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    write_csvs(kelly, pyramid, grid, base_results)
    write_report(payload)
    print('[done] reports/kelly_sizing/')


def evaluate_kelly(base: dict[str, Any], base_results: list[StagedResult],
                   dates: list[date], rets: np.ndarray, r1: pl.DataFrame,
                   cost_model: Any) -> dict[str, Any]:
    base_curves = [item.curve for item in base_results]
    schemes: dict[str, Any] = {'A_full': {
        'annualized': base['annualized'], 'max_drawdown': base['max_drawdown'],
        'calmar': base['calmar'], 'sharpe': base['sharpe'],
        'worst_window': base['worst_window'],
        'turnover_annual': base['base_turnover_annual'],
        'overlay_cost_annual': 0.0, 'mean_exposure': 1.0,
        'windows': base['windows'], 'curve': base['curve'],
    }}
    positions: dict[str, pl.DataFrame] = {}
    configs = {
        'C_kelly_c0.5_w250': (250, 0.5, 0.3, 1.0, False),
        'D_kelly_c0.25_w250': (250, 0.25, 0.3, 1.0, False),
        'E_kelly_c0.25_w500_shrink': (500, 0.25, 0.3, 1.0, True),
    }
    for label, (window, c, low, high, shrink) in configs.items():
        result = compute_position(rets, window=window, c=c, low=low, high=high,
                                  shrink=shrink)
        frame = pl.DataFrame({'date': dates, 'position': result['position']})
        positions[label] = frame
        curves, diag = overlay_global(frame, base_curves, dates, cost_model)
        schemes[label] = {**scheme_metrics(curves, base_curves, diag),
                          'position_stats': _position_stats(result)}
    positions['B_r1_half'] = r1
    curves, diag = overlay_global(r1, base_curves, dates, cost_model)
    schemes['B_r1_half'] = {**scheme_metrics(curves, base_curves, diag),
                            'position_stats': None}
    combo = min_position(positions['E_kelly_c0.25_w500_shrink'], r1)
    positions['F_min_kelly_r1'] = combo
    curves, diag = overlay_global(combo, base_curves, dates, cost_model)
    schemes['F_min_kelly_r1'] = {**scheme_metrics(curves, base_curves, diag),
                                 'position_stats': None}

    full = compute_position(rets, window=250, c=1.0, low=0.0, high=10.0)
    frame_full = pl.DataFrame({'date': dates, 'position': full['position']})
    curves_full, diag_full = overlay_global(frame_full, base_curves, dates,
                                            cost_model)
    full_metrics = scheme_metrics(curves_full, base_curves, diag_full)
    full_metrics['position_stats'] = _position_stats(full)

    error = estimate_error(rets, windows_dates(dates), base, positions,
                           cost_model, base_curves, dates)
    trade_kelly = evaluate_trade_kelly(base_results, dates, positions)
    sensitivity = sensitivity_grid(base, base_results, dates, rets, cost_model)
    acceptance_rows = {}
    for label, scheme in schemes.items():
        acceptance_rows[label] = acceptance(
            scheme, base, 0.0, GIVEN_BASE)
    acceptance_rows['B_r1_half']['nominal_vs_given_r1'] = acceptance(
        schemes['B_r1_half'], base, 0.0, GIVEN_R1)['nominal_vs_given']
    for label, scheme in schemes.items():
        if label == 'A_full':
            continue
        loo = leave_one_out(scheme, base, scheme['curves'], base_curves)
        acceptance_rows[label]['leave_one_out'] = loo

    return {
        'schemes': {k: _strip_curve(v) for k, v in schemes.items()},
        'full_kelly_unclipped': _strip_curve(full_metrics),
        'error': error,
        'trade_kelly': trade_kelly,
        'sensitivity': sensitivity,
        'acceptance': acceptance_rows,
        'selected_configs': {
            'C': {'window': 250, 'c': 0.5, 'bounds': [0.3, 1.0]},
            'D': {'window': 250, 'c': 0.25, 'bounds': [0.3, 1.0]},
            'E': {'window': 500, 'c': 0.25, 'bounds': [0.3, 1.0],
                  'shrink_lambda': 0.5,
                  'prior': 't 时点可得历史的扩张均值（因果，未用未来）'},
            'F': {'base': 'E', 'cap': 'R1_half'},
            'rf': 0.0,
        },
    }


def _position_stats(result: dict[str, np.ndarray]) -> dict[str, Any]:
    pos = result['position']
    return {
        'mean': float(np.mean(pos)), 'p5': float(np.percentile(pos, 5)),
        'p50': float(np.percentile(pos, 50)),
        'share_at_lower': float(np.mean(pos <= 0.3001)),
        'share_at_upper': float(np.mean(pos >= 0.9999)),
        'fstar': distribution(result['fstar']),
    }


def _strip_curve(scheme: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in scheme.items() if k not in ('curve', 'curves', 'trades')}


def windows_dates(dates: list[date]) -> list[date]:
    return dates


def estimate_error(rets: np.ndarray, _dates: list[date], base: dict[str, Any],
                   positions: dict[str, pl.DataFrame], cost_model: Any,
                   base_curves: list[pl.DataFrame], dates: list[date]) -> dict[str, Any]:
    c_result = compute_position(rets, window=250, c=0.5, low=0.3, high=1.0)
    fstar = c_result['fstar'][c_result['fstar'] > 0]
    se = c_result['se'][np.isfinite(c_result['se'])]
    pos = c_result['position']
    shifted = np.minimum(np.maximum(
        0.5 * (c_result['mu'] + c_result['se']) / c_result['var'], 0.3), 1.0)
    pos_shift = np.abs(shifted - pos)
    valid_shift = np.isfinite(pos_shift)
    shift_median = float(np.median(pos_shift[valid_shift])) if valid_shift.any() else None
    shift_p95 = float(np.percentile(pos_shift[valid_shift], 95)) if valid_shift.any() else None
    window_boot = [bootstrap_ci(daily_returns(curve)[1:])
                   for curve in base['curves']]
    boot_full = bootstrap_ci(rets[1:])
    return {
        'mu_annual_mean': float(np.mean(rets) * ANNUAL),
        'mu_bootstrap_full': boot_full,
        'mu_se_annual_median': float(np.median(se) * math.sqrt(ANNUAL)),
        'mu_se_annual_p95': float(np.percentile(se, 95) * math.sqrt(ANNUAL)),
        'fstar_distribution': distribution(fstar),
        'fstar_share_gt1': float(np.mean(fstar > 1.0)),
        'position_mu_1se_shift_median': shift_median,
        'position_mu_1se_shift_p95': shift_p95,
        'position_distribution': distribution(pos),
        'full_kelly_fstar_distribution': distribution(
            compute_position(rets, window=250, c=1.0, low=0.0, high=10.0)['fstar']),
        'bootstrap_note': '窗口级 bootstrap 见 slow path 输出',
        'window_boot': window_boot,
        'unclipped_reference': _full_kelly_reference(rets, base_curves, dates,
                                                     cost_model),
    }


def _full_kelly_reference(rets: np.ndarray, base_curves: list[pl.DataFrame],
                          dates: list[date], cost_model: Any) -> dict[str, Any]:
    result = compute_position(rets, window=250, c=1.0, low=0.0, high=10.0)
    frame = pl.DataFrame({'date': dates, 'position': result['position']})
    curves, diag = overlay_global(frame, base_curves, dates, cost_model)
    metrics = scheme_metrics(curves, base_curves, diag)
    return {k: v for k, v in metrics.items() if k not in ('curve', 'curves')}


def evaluate_trade_kelly(results: list[StagedResult], dates: list[date],
                         positions: dict[str, pl.DataFrame]) -> dict[str, Any]:
    trades = [trade for item in results for trade in item.trades]
    trades.sort(key=lambda t: t['exit_date'])
    date_index = {day: i for i, day in enumerate(dates)}
    records = []
    for trade in trades:
        exit_idx = date_index.get(trade['exit_date'])
        if exit_idx is None:
            continue
        recent = [t for t in trades
                  if t['exit_date'] <= trade['exit_date']
                  and date_index.get(t['exit_date'], -10**9) >= exit_idx - 250]
        if len(recent) < 20:
            continue
        wins = [t['ret'] for t in recent if t['ret'] > 0]
        losses = [t['ret'] for t in recent if t['ret'] <= 0]
        p = len(wins) / len(recent)
        loss_avg = abs(np.mean(losses)) if losses else 1e-9
        win_avg = float(np.mean(wins)) if wins else 0.0
        b = win_avg / max(loss_avg, 1e-9)
        f_trade = p - (1.0 - p) / b if b > 0 else p
        records.append({'date': trade['exit_date'], 'p': p, 'b': b,
                        'f_trade': f_trade, 'n_trades': len(recent)})
    series = {item['date']: item['f_trade'] for item in records}
    portfolio = np.array([series.get(day, np.nan) for day in dates])
    port_frame = positions['E_kelly_c0.25_w500_shrink'].sort('date')
    port_pos = np.array([
        float(port_frame.filter(pl.col('date') == day)['position'][0])
        if port_frame.filter(pl.col('date') == day).height else np.nan
        for day in dates])
    valid = np.isfinite(portfolio) & np.isfinite(port_pos)
    gap = portfolio[valid] - port_pos[valid]
    gap_slot = portfolio[valid] - port_pos[valid] / 20.0
    return {
        'records': [{'date': str(r['date']), 'p': float(r['p']),
                     'b': float(r['b']), 'f_trade': float(r['f_trade']),
                     'n_trades': int(r['n_trades'])} for r in records],
        'f_trade_distribution': distribution(np.array(
            [r['f_trade'] for r in records])),
        'gap_vs_portfolio_median': float(np.median(gap)) if len(gap) else None,
        'gap_vs_portfolio_mean': float(np.mean(gap)) if len(gap) else None,
        'gap_vs_per_slot_median': float(np.median(gap_slot)) if len(gap_slot) else None,
        'correlation': float(np.corrcoef(portfolio[valid], port_pos[valid])[0, 1])
        if valid.sum() > 2 else None,
        'note': 'f_trade 为单笔押注占比参考；组合同时持有 10~20 只，相关性改变最优仓位',
    }


def sensitivity_grid(base: dict[str, Any], base_results: list[StagedResult],
                     dates: list[date], rets: np.ndarray,
                     cost_model: Any) -> list[dict[str, Any]]:
    base_curves = [item.curve for item in base_results]
    rows = []
    for c in (0.1, 0.25, 0.5, 1.0):
        for window in (120, 250, 500, None):
            for low, high in ((0.3, 1.0), (0.0, 1.0)):
                result = compute_position(rets, window=window, c=c, low=low,
                                          high=high)
                frame = pl.DataFrame({'date': dates,
                                      'position': result['position']})
                curves, diag = overlay_global(frame, base_curves, dates,
                                              cost_model)
                metrics = scheme_metrics(curves, base_curves, diag)
                rows.append({
                    'c': c, 'window': window or 'expanding',
                    'bounds': f'[{low},{high}]',
                    'annualized': metrics['annualized'],
                    'max_drawdown': metrics['max_drawdown'],
                    'calmar': metrics['calmar'],
                    'worst_window': metrics['worst_window'],
                    'cost_annual': metrics['overlay_cost_annual'],
                    'mean_exposure': metrics['mean_exposure'],
                })
    _ = base
    return rows


def evaluate_pyramid(matrix: pl.DataFrame, windows: list[Window],
                     base: dict[str, Any], base_results: list[StagedResult],
                     r1: pl.DataFrame, cost_model: Any,
                     dates: list[date]) -> dict[str, Any]:
    schemes: dict[str, Any] = {}
    eq5 = aggregate_staged(base_results)
    schemes['EQ5_equal_once'] = {
        k: v for k, v in eq5.items() if k not in ('trades', 'curve', 'curves')}
    matrix10 = matrix
    eq10 = staged_windows(matrix10, windows, StagedConfig(
        name='eq10', fracs=(1.0,), target_weight=0.10, top_k=20,
        entry_stop_pct=None, trail_stop_pct=None))
    eq10_agg = aggregate_staged(eq10)
    schemes['EQ10_equal_once'] = {
        k: v for k, v in eq10_agg.items()
        if k not in ('trades', 'curve', 'curves')}
    eq_curves = [item.curve for item in base_results]
    curves, diag = overlay_global(r1, eq_curves, dates, cost_model)
    r1_overlay = scheme_metrics(curves, eq_curves, diag)
    schemes['EQ5_R1_half'] = {
        k: v for k, v in r1_overlay.items() if k not in ('curve', 'curves')}

    configs = {
        'P1_scaled10': StagedConfig(
            name='P1', fracs=(0.2, 0.2, 0.2, 0.4), target_weight=0.10,
            add_trigger_pct=0.10, entry_stop_pct=0.10, trail_stop_pct=0.10),
        'P2_scaled10_reverse': StagedConfig(
            name='P2', fracs=(0.2, 0.4, 0.2, 0.2), target_weight=0.10,
            add_trigger_pct=0.10, entry_stop_pct=0.10, trail_stop_pct=0.10),
        'P3_scaled10_atr': StagedConfig(
            name='P3', fracs=(1.0, 1.0, 1.0, 1.0), target_weight=0.10,
            atr_mode=True, risk_budget=0.02, max_tranches=4),
        'P4_scaled10_trail': StagedConfig(
            name='P4', fracs=(0.2, 0.2, 0.2, 0.4), target_weight=0.10,
            add_trigger_pct=0.10, entry_stop_pct=None, trail_stop_pct=0.10),
        'P1_original_ratio': StagedConfig(
            name='P1_orig', fracs=(0.2, 0.2, 0.2, 0.4), target_weight=0.05,
            add_trigger_pct=0.10, entry_stop_pct=0.10, trail_stop_pct=0.10),
    }
    full_aggs: dict[str, dict[str, Any]] = {}
    for label, cfg in configs.items():
        results = staged_windows(matrix, windows, cfg,
                                 atr=cfg.atr_mode)
        agg = aggregate_staged(results)
        full_aggs[label] = agg
        schemes[label] = {k: v for k, v in agg.items()
                          if k not in ('trades', 'curve', 'curves')}
        schemes[label]['trade_summary'] = _trade_summary(agg)

    sens = pyramid_sensitivity(matrix, windows, base)
    acceptance_rows = {}
    for label, scheme in schemes.items():
        if label.startswith('EQ'):
            continue
        acceptance_rows[label] = acceptance(
            scheme, eq5, eq5['cost_annual'], GIVEN_BASE)
        loo = leave_one_out(scheme, eq5, full_aggs[label]['curves'], eq_curves)
        acceptance_rows[label]['leave_one_out'] = loo
        acceptance_rows[label]['rule_effective'] = bool(
            scheme['single_tranche_ratio'] <= 0.70)
        acceptance_rows[label]['pass_all_with_rule'] = bool(
            acceptance_rows[label]['pass_all']
            and acceptance_rows[label]['rule_effective'])
    return {
        'schemes': schemes, 'sensitivity': sens,
        'acceptance': acceptance_rows,
        'scaling': '最终判断使用单票目标权重 10%（2%/2%/2%/4%）；原始比例（5% 槽位 → 1%/1%/1%/2%）仅 P1 对照',
    }


def _trade_summary(agg: dict[str, Any]) -> dict[str, Any]:
    return {
        'closed_trades': agg['closed_trades'],
        'win_rate': agg['win_rate'], 'profit_factor': agg['profit_factor'],
        'add_trigger_ratio': agg['add_trigger_ratio'],
        'single_tranche_ratio': agg['single_tranche_ratio'],
        'stop_counts': agg['stop_counts'],
        'entries': agg['entries'], 'add_events': agg['add_events'],
    }


def pyramid_sensitivity(matrix: pl.DataFrame, windows: list[Window],
                        base: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for trigger in (0.05, 0.10, 0.15):
        for stop in ('5%', '10%', '2xATR'):
            if stop == '2xATR':
                cfg = StagedConfig(
                    name='sens', fracs=(0.2, 0.2, 0.2, 0.4),
                    target_weight=0.10, add_trigger_pct=trigger,
                    atr_stop=True, atr_entry_mult=2.0, atr_trail_mult=2.0)
                atr = True
            else:
                value = 0.05 if stop == '5%' else 0.10
                cfg = StagedConfig(
                    name='sens', fracs=(0.2, 0.2, 0.2, 0.4),
                    target_weight=0.10, add_trigger_pct=trigger,
                    entry_stop_pct=value, trail_stop_pct=value)
                atr = False
            agg = aggregate_staged(staged_windows(matrix, windows, cfg, atr=atr))
            rows.append({'trigger': trigger, 'stop': stop,
                         'annualized': agg['annualized'],
                         'calmar': agg['calmar'],
                         'max_drawdown': agg['max_drawdown']})
    _ = base
    return rows


def evaluate_grid(windows: list[Window], dates: list[date]) -> dict[str, Any]:
    from src.data.local_store import get_local_quote_store
    from src.utils.config import get_config

    store = get_local_quote_store(get_config())
    raw = store.read_file('fund', '510300.SH')
    if raw is None or raw.is_empty():
        raise FileNotFoundError('本地列存缺少 510300.SH')
    daily = raw.select(
        pl.col('date').cast(pl.String).str.to_date(), pl.col('open').cast(pl.Float64),
        pl.col('close').cast(pl.Float64)).sort('date')
    start = dates[0]
    buffer = daily.filter(pl.col('date') <= start)
    daily_ext = daily.filter(pl.col('date') >= start - timedelta(days=400))
    g1 = simulate_grid(daily_ext, name='G1', start=start)
    g2 = simulate_grid(daily_ext, name='G2', trend_filter=True, start=start)
    hold = simulate_buy_hold(daily_ext.filter(pl.col('date') >= start))
    out: dict[str, Any] = {}
    for label, item in (('G1_grid', g1), ('G2_grid_ma200', g2),
                        ('buy_hold', hold)):
        curve = item.curve
        wins = [window_return(curve, w.test_start, w.test_end) for w in windows]
        years = max(1e-9, curve.height / ANNUAL)
        avg_equity = _num(curve['equity'].mean())
        out[label] = {
            'annualized': item.stats['annualized'],
            'max_drawdown': item.stats['max_drawdown'],
            'calmar': item.stats['calmar'], 'sharpe': item.stats['sharpe'],
            'worst_window': float(min(wins)), 'windows': wins,
            'turnover_annual': item.diagnostics['turnover_annual'],
            'cost_annual': item.diagnostics['cost_annual'],
            'cost_ratio': item.diagnostics['cost_ratio'],
            'final_exposure': item.diagnostics['final_exposure'],
            'final_tranches': item.diagnostics['final_tranches'],
            'win_rate': item.diagnostics['win_rate'],
            'profit_factor': item.diagnostics['profit_factor'],
            'tranche_marks': item.diagnostics['tranche_marks'],
            'years': years, 'curve': curve,
        }
        _ = buffer
        _ = avg_equity
    base = out['buy_hold']
    base_curves = slice_windows(base['curve'], windows)
    for label in ('G1_grid', 'G2_grid_ma200'):
        out[label]['acceptance'] = acceptance(
            out[label], base, base['cost_annual'], None)
        loo = leave_one_out(out[label], base,
                            slice_windows(out[label]['curve'], windows),
                            base_curves)
        out[label]['acceptance']['leave_one_out'] = loo
    out['acceptance_note'] = (
        '网格对照组为同期 510300.SH 买入持有；买入持有 Calmar 近零，'
        '第 1 条相对提升阈值无定义，按不通过处理')
    for label in ('G1_grid', 'G2_grid_ma200', 'buy_hold'):
        out[label].pop('curve', None)
    out['stock_counterexample'] = stock_counterexample()
    return out


def slice_windows(curve: pl.DataFrame,
                  windows: list[Window]) -> list[pl.DataFrame]:
    """把连续净值按测试窗切片并各自归一到 1M，供逐窗/剔除单窗统计。"""
    out: list[pl.DataFrame] = []
    for item in windows:
        part = curve.filter(
            (pl.col('date') >= item.test_start)
            & (pl.col('date') < item.test_end)).sort('date')
        if part.is_empty():
            continue
        first = float(part['equity'][0])
        out.append(part.with_columns(
            (pl.col('equity') / first * INITIAL).alias('equity')))
    return out


def stock_counterexample() -> dict[str, Any]:
    matrix = pl.read_parquet(MATRIX).with_columns(pl.col('date').cast(pl.Date))
    start = date(2023, 3, 6)
    frame = matrix.filter(pl.col('date') >= start).select(
        'date', 'ts_code', 'open', 'close').drop_nulls().sort(['ts_code', 'date'])
    frame = frame.with_columns(
        pl.col('close').cum_max().over('ts_code').alias('peak'))
    frame = frame.with_columns(
        (pl.col('close') / pl.col('peak') - 1.0).alias('dd'))
    summary = frame.group_by('ts_code').agg(
        pl.col('dd').min().alias('max_dd'),
        pl.len().alias('rows'),
        pl.col('date').min().alias('first'),
        pl.col('date').max().alias('last')).sort('max_dd')
    candidate = summary[0]
    code = str(candidate['ts_code'][0])
    row = frame.filter(pl.col('ts_code') == code).sort('date')
    peak_row = row.sort('close', descending=True).head(1)
    peak_date = peak_row['date'][0]
    series = row.filter(pl.col('date') >= peak_date).select('date', 'open', 'close')
    grid = simulate_grid(series, name='stock_grid', start=peak_date,
                         tranches=5, tranche_weight=0.10, step=0.05,
                         ts_code=code)
    decline = float(series['close'][-1] / series['close'][0] - 1.0)
    return {
        'code': code, 'max_drawdown_from_peak': float(candidate['max_dd'][0]),
        'rows': int(candidate['rows'][0]), 'peak_date': str(peak_date),
        'last_date': str(series['date'][-1]), 'decline_since_peak': decline,
        'annualized': grid.stats['annualized'],
        'net_return': grid.stats['net_return'],
        'max_drawdown': grid.stats['max_drawdown'],
        'final_exposure': grid.diagnostics['final_exposure'],
        'final_tranches': grid.diagnostics['final_tranches'],
        'total_cost': grid.diagnostics['total_cost'],
        'net_pnl': float(grid.curve['equity'][-1] - INITIAL),
        'tranche_marks': grid.diagnostics['tranche_marks'],
    }


def write_csvs(kelly: dict[str, Any], pyramid: dict[str, Any],
               grid: dict[str, Any], base_results: list[StagedResult]) -> None:
    rows = []
    for label, scheme in kelly['schemes'].items():
        rows.append({'scheme': label, 'annualized': scheme['annualized'],
                     'max_drawdown': scheme['max_drawdown'],
                     'calmar': scheme['calmar'], 'sharpe': scheme['sharpe'],
                     'worst_window': scheme['worst_window'],
                     'turnover_annual': scheme['turnover_annual'],
                     'cost_annual': scheme.get('overlay_cost_annual', 0.0),
                     'mean_exposure': scheme.get('mean_exposure', 1.0)})
    pl.DataFrame(rows).write_csv(OUT / 'kelly_schemes.csv')
    windows_rows = []
    for label, scheme in kelly['schemes'].items():
        for idx, item in enumerate(scheme['windows']):
            windows_rows.append({'scheme': label, 'window': idx + 1,
                                 'net_return': item['net_return'],
                                 'annualized': item.get('annualized'),
                                 'max_drawdown': item.get('max_drawdown'),
                                 'calmar': item.get('calmar'),
                                 'sharpe': item.get('sharpe')})
    pl.DataFrame(windows_rows).write_csv(OUT / 'kelly_windows.csv')
    pl.DataFrame(kelly['sensitivity']).write_csv(OUT / 'kelly_sensitivity.csv')
    trade_rows = [{'ts_code': t['ts_code'], 'entry_date': str(t['entry_date']),
                   'exit_date': str(t['exit_date']), 'ret': t['ret'],
                   'pnl': t['pnl'], 'held_days': t['held_days']}
                  for item in base_results for t in item.trades]
    pl.DataFrame(trade_rows).write_csv(OUT / 'trades_extended_20.csv')
    pyr_rows = []
    for label, scheme in pyramid['schemes'].items():
        pyr_rows.append({'scheme': label, 'annualized': scheme['annualized'],
                         'max_drawdown': scheme['max_drawdown'],
                         'calmar': scheme['calmar'], 'sharpe': scheme['sharpe'],
                         'worst_window': scheme['worst_window'],
                         'turnover_annual': scheme.get('turnover_annual'),
                         'cost_annual': scheme.get('cost_annual'),
                         'add_trigger_ratio': scheme.get('add_trigger_ratio'),
                         'stop_entry': scheme.get('stop_counts', {}).get(
                             'stop_entry', 0),
                         'stop_trail': scheme.get('stop_counts', {}).get(
                             'stop_trail', 0)})
    pl.DataFrame(pyr_rows).write_csv(OUT / 'pyramid_schemes.csv')
    pl.DataFrame(pyramid['sensitivity']).write_csv(OUT / 'pyramid_sensitivity.csv')
    grid_rows = []
    for label in ('G1_grid', 'G2_grid_ma200', 'buy_hold'):
        item = grid[label]
        grid_rows.append({'scheme': label, 'annualized': item['annualized'],
                          'max_drawdown': item['max_drawdown'],
                          'calmar': item['calmar'], 'sharpe': item['sharpe'],
                          'worst_window': item['worst_window'],
                          'turnover_annual': item['turnover_annual'],
                          'cost_annual': item['cost_annual'],
                          'final_exposure': item['final_exposure'],
                          'final_tranches': item['final_tranches']})
    pl.DataFrame(grid_rows).write_csv(OUT / 'grid_schemes.csv')


def write_report(payload: dict[str, Any]) -> None:
    # 详细中文结论在脚本外人工撰写；这里只落一份可追溯的实测摘要。
    meta = payload['meta']
    (OUT / 'README_measured.md').write_text(
        '# 实测摘要（自动生成）\n\n'
        '完整结论见 `仓位管理评估报告.md`。\n\n'
        f'- 引擎对齐 max|Δequity|：{meta.get("max_engine_equity_diff")}\n'
        f'- 窗口来源：{meta.get("windows_source")}，矩阵 SHA256：{meta.get("matrix_sha256")}\n'
        f'- 凯利方案：{json.dumps(payload["kelly"]["acceptance"], ensure_ascii=False, indent=2)}\n'
        f'- 金字塔接受度：{json.dumps(payload["pyramid_grid"]["pyramid"]["acceptance"], ensure_ascii=False, indent=2)}\n'
        f'- 网格接受度：{json.dumps(payload["pyramid_grid"]["grid"].get("acceptance_note"), ensure_ascii=False)}\n',
        encoding='utf-8')


if __name__ == '__main__':
    main()
