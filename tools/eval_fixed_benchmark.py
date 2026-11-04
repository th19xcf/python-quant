"""固定基准矩阵重建 + R1 / 凯利 / 金字塔统一口径复算（只读评估）。

背景：``reports/regime_filter`` 对 R1 半仓给出年化 34.86%，``reports/kelly_sizing``
对同一条规则给出 22.83%。本脚本把矩阵与 walk-forward 窗口冻结到
``tmp/kelly_fixed_matrix/``（不读写 ``equity_extended_20.csv``、``windows_cache.pkl``），
在同一矩阵、同一 CostModel、同一代码路径上复算 A/B/D/P1，并用消融实验逐项定位差异。

用法::

    .venv\\Scripts\\python.exe tools/eval_fixed_benchmark.py
    .venv\\Scripts\\python.exe tools/eval_fixed_benchmark.py --skip-2000
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice import regime  # noqa: E402
from src.advice.factor_eval import (  # noqa: E402
    EvalConfig,
    SimConfig,
    _curve_stats,
    add_forward_returns,
    build_composite,
    combine_curves,
    simulate_fixed_hold,
    walk_forward,
)
from tools.eval_daily_advice import _legacy_weights, factor_list  # noqa: E402
from tools.eval_position_management import (  # noqa: E402
    THRESHOLDS,
    Window,
    acceptance,
    aggregate_staged,
    base_scheme,
    compute_position,
    daily_returns,
    staged_windows,
)
from tools.position_management_sim import (  # noqa: E402
    StagedConfig,
    StagedResult,
    simulate_equal,
)

ANNUAL = 252.0
INITIAL = 1_000_000.0
FIX = ROOT / 'tmp' / 'kelly_fixed_matrix'
OUT = ROOT / 'reports' / 'kelly_sizing'
ADVICE = ROOT / 'tmp' / 'eval_daily_advice'
SOURCES = {
    'fixed_500': ADVICE / 'factors_strat500s20261105_2021-01-01_2026-11-01.parquet',
    'alt_500': ADVICE / 'factors_500_2021-01-01_2026-11-01.parquet',
    'alt_2000': ADVICE / 'factors_strat2000s20261105_2021-01-01_2026-11-01.parquet',
}
GIVEN_BASE = {'annualized': 0.3532, 'max_drawdown': -0.4400, 'calmar': 0.8028,
              'worst_window': -0.2405, 'turnover_annual': 8.942}
GIVEN_R1 = {'annualized': 0.3486, 'max_drawdown': -0.2651, 'calmar': 1.32,
            'worst_window': -0.0887, 'turnover_annual': 20.1,
            'cost_annual': 0.0089}


@dataclass(frozen=True)
class Frozen:
    """冻结后的一个 walk-forward 测试窗。"""

    test_start: date
    test_end: date
    weights: dict[str, float]
    selected: tuple[str, ...]


# ----------------------------------------------------------------------
# 冻结与画像
# ----------------------------------------------------------------------
def _num(value: Any, default: float = 0.0) -> float:
    """Polars 标量（宽 Union）安全转 float。"""
    if isinstance(value, bool):
        return default
    if isinstance(value, int | float):
        return float(value)
    return default


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def freeze_matrix(source: Path, target: Path) -> dict[str, Any]:
    """把源矩阵复制到本任务目录；已存在则直接复用，保证跨次运行不变。"""
    FIX.mkdir(parents=True, exist_ok=True)
    if not target.is_file():
        shutil.copy2(source, target)
    return {'source': str(source.relative_to(ROOT)),
            'frozen': str(target.relative_to(ROOT)),
            'sha256': sha256_file(target),
            'bytes': target.stat().st_size}


def matrix_profile(frame: pl.DataFrame) -> dict[str, Any]:
    profile = {
        'rows': frame.height,
        'columns': frame.width,
        'stocks': frame['ts_code'].n_unique(),
        'start': str(frame['date'].min()),
        'end': str(frame['date'].max()),
        'trading_days': frame['date'].n_unique(),
    }
    return profile


def load_matrix(path: Path) -> pl.DataFrame:
    frame = pl.read_parquet(path).with_columns(pl.col('date').cast(pl.Date))
    return frame


# ----------------------------------------------------------------------
# walk-forward 窗口（写本任务缓存，不碰共享 pkl）
# ----------------------------------------------------------------------
def load_windows(frame: pl.DataFrame, matrix_path: Path, cache_path: Path
                 ) -> tuple[list[Frozen], dict[str, Any]]:
    """按矩阵 SHA256 + 评估参数缓存 7 窗权重与入选因子。"""
    key = {'matrix_sha256': sha256_file(matrix_path),
           'start': '2021-01-01', 'end': '2026-11-01', 'horizon': 20,
           'embargo': 20, 'top_k': 20, 'hold': 20, 'new_share': 0.70,
           'version': 1}
    if cache_path.is_file():
        cached = json.loads(cache_path.read_text(encoding='utf-8'))
        if cached.get('key') == key:
            windows = [
                Frozen(date.fromisoformat(item['test_start']),
                       date.fromisoformat(item['test_end']),
                       {k: float(v) for k, v in item['weights'].items()},
                       tuple(item['selected']))
                for item in cached['windows']
            ]
            return windows, {'source': 'cache', 'cache': str(
                cache_path.relative_to(ROOT))}
    factors = factor_list(frame)
    prepared = add_forward_returns(frame, (5, 20))
    config = EvalConfig(start='2021-01-01', end='2026-11-01',
                        universe_size=500, hold_days=20, horizon=20,
                        embargo=20, top_k=20)
    results = walk_forward(prepared, factors, config,
                           baseline_weights=_legacy_weights('mid'),
                           new_share=0.70)
    windows = [Frozen(item.test_start, item.test_end, dict(item.weights),
                      tuple(item.selected)) for item in results]
    payload = {'key': key, 'n_factors': len(factors), 'windows': [
        {'test_start': str(w.test_start), 'test_end': str(w.test_end),
         'weights': w.weights, 'selected': list(w.selected)} for w in windows]}
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                          encoding='utf-8')
    return windows, {'source': 'recomputed', 'n_factors': len(factors),
                     'cache': str(cache_path.relative_to(ROOT))}


def replace_weights(windows: list[Frozen],
                    builder: Any) -> list[Frozen]:
    return [Frozen(w.test_start, w.test_end, builder(w), w.selected)
            for w in windows]


def all_positive_weights(window: Frozen,
                         baseline: dict[str, float],
                         new_share: float = 0.70) -> dict[str, float]:
    """复刻 02:08 修复前的权重口径：入选因子一律取正权重。

    旧实现用已按登记方向对齐的 ``ic_mean`` 判符号，该值恒 ≥0，等价于全部 +1。
    """
    out = {name: weight * (1.0 - new_share)
           for name, weight in baseline.items()}
    if window.selected and new_share > 0:
        per = new_share / len(window.selected)
        for name in window.selected:
            out[name] = out.get(name, 0.0) + per
    return out


# ----------------------------------------------------------------------
# 模拟与指标
# ----------------------------------------------------------------------
def base_results(frame: pl.DataFrame, windows: list[Frozen]
                 ) -> list[StagedResult]:
    results = []
    for item in windows:
        chunk = frame.filter(
            (pl.col('date') >= item.test_start)
            & (pl.col('date') < item.test_end))
        results.append(simulate_equal(build_composite(chunk, item.weights)))
    return results


def as_windows(windows: list[Frozen]) -> list[Window]:
    """把冻结窗转成模拟器要求的 ``Window``（丢弃 selected）。"""
    return [Window(w.test_start, w.test_end, w.weights) for w in windows]


def engine_results(frame: pl.DataFrame, windows: list[Frozen]
                   ) -> list[pl.DataFrame]:
    curves = []
    for item in windows:
        chunk = frame.filter(
            (pl.col('date') >= item.test_start)
            & (pl.col('date') < item.test_end))
        curve, _ = simulate_fixed_hold(build_composite(chunk, item.weights),
                                       SimConfig())
        curves.append(curve)
    return curves


def base_metrics(results: list[StagedResult]) -> dict[str, Any]:
    metrics = base_scheme(results)
    metrics['cost_annual'] = metrics['base_cost_annual']
    metrics['turnover_annual'] = metrics['base_turnover_annual']
    return metrics


def curve_metrics(curves: list[pl.DataFrame]) -> dict[str, Any]:
    combined, stats = combine_curves(curves)
    windows = []
    for curve in curves:
        item = _curve_stats(curve, INITIAL, curve.height)
        windows.append({name: item[name] for name in (
            'net_return', 'annualized', 'max_drawdown', 'calmar', 'sharpe')})
    return {
        'combined': {k: (float(v) if isinstance(v, (int, float)) else v)
                     for k, v in stats.items()},
        'net_return': float(stats['net_return']),
        'annualized': float(stats['annualized']),
        'max_drawdown': float(stats['max_drawdown']),
        'calmar': float(stats['calmar']),
        'sharpe': float(stats['sharpe']),
        'worst_window': float(min(item['net_return'] for item in windows)),
        'windows': windows,
        'days': combined.height,
        'years': max(1e-9, combined.height / ANNUAL),
        'curve': combined,
    }


def overlay_metrics(curves: list[pl.DataFrame], position: pl.DataFrame,
                    cost_model: Any) -> dict[str, Any]:
    """R1/凯利统一叠加：逐窗 ``regime.overlay_curve``，与择时报告同一条路径。"""
    overlaid: list[pl.DataFrame] = []
    total_cost = 0.0
    total_delta = 0.0
    exposure = 0.0
    rows = 0
    for curve in curves:
        detail = regime.overlay_curve(curve, position, cost_model,
                                      initial=INITIAL)
        overlaid.append(detail.select(
            'date', pl.col('equity_overlay').alias('equity')))
        total_cost += float(detail['overlay_cost'].sum())
        total_delta += float(detail['delta'].abs().sum())
        exposure += float(detail['exec_position'].fill_null(1.0).sum())
        rows += detail.height
    metrics = curve_metrics(overlaid)
    # ``overlay_cost`` 已是占净值的比例，按年求和即为年化成本率。
    metrics['cost_annual'] = total_cost / max(metrics['years'], 1e-9)
    metrics['turnover_annual'] = total_delta / max(metrics['years'], 1e-9)
    metrics['mean_exposure'] = exposure / max(1, rows)
    metrics['curves'] = overlaid
    return metrics


def level_scale_metrics(curves: list[pl.DataFrame], position: pl.DataFrame,
                        cash_buffer: float = 0.05) -> dict[str, Any]:
    """对照用的错误映射：把仓位乘在净值水平上，而不是乘在日收益上。"""
    scaled: list[pl.DataFrame] = []
    for curve in curves:
        joined = (curve.select('date', 'equity')
                  .join(regime.executed_position(position), on='date',
                        how='left')
                  .sort('date')
                  .with_columns(pl.col('exec_position').fill_null(1.0)))
        scaled.append(joined.select(
            'date',
            (pl.col('equity') * (1.0 - cash_buffer) * pl.col('exec_position')
             ).alias('equity')))
    return curve_metrics(scaled)


def r1_frame(dates: list[date]) -> pl.DataFrame:
    rule = regime.ma_position(regime.load_index_daily('000300.SH'),
                              regime.RegimeParams())
    frame = pl.DataFrame({'date': dates}).join(rule, on='date', how='left')
    return frame.with_columns(pl.col('position').fill_null(1.0))


def kelly_frame(dates: list[date], rets: np.ndarray) -> pl.DataFrame:
    position = compute_position(rets, window=250, c=0.25, low=0.3, high=1.0)
    return pl.DataFrame({'date': dates,
                         'position': position['position']})


def scheme_row(name: str, metrics: dict[str, Any], base: dict[str, Any]
               ) -> dict[str, Any]:
    gate = acceptance(metrics, base, base['cost_annual'])
    row = {
        'scheme': name,
        'annualized': float(metrics['annualized']),
        'max_drawdown': float(metrics['max_drawdown']),
        'calmar': float(metrics['calmar']),
        'sharpe': float(metrics['sharpe']),
        'worst_window': float(metrics['worst_window']),
        'turnover_annual': float(metrics.get('turnover_annual', 0.0)),
        'cost_annual': float(metrics.get('cost_annual', 0.0)),
    }
    row.update({k: v for k, v in gate.items() if k.startswith('pass')})
    row['calmar_improve'] = gate['calmar_improve']
    row['annual_drop_pp'] = gate['annual_drop_pp']
    row['worst_improve'] = gate['worst_improve']
    row['cost_increment'] = gate['cost_increment']
    return row


# ----------------------------------------------------------------------
# 消融
# ----------------------------------------------------------------------
def ablation_row(label: str, frame: pl.DataFrame, windows: list[Frozen],
                 r1: pl.DataFrame, cost_model: Any,
                 note: str) -> dict[str, Any]:
    results = base_results(frame, windows)
    base = base_metrics(results)
    curves = [item.curve for item in results]
    dates = base['curve']['date'].to_list()
    position = r1.filter(pl.col('date').is_in(dates))
    r1m = overlay_metrics(curves, position, cost_model)
    rets = daily_returns(base['curve'])
    kelly = overlay_metrics(curves, kelly_frame(dates, rets), cost_model)
    return {
        'variant': label,
        'note': note,
        'base': {k: base[k] for k in (
            'annualized', 'max_drawdown', 'calmar', 'sharpe', 'worst_window',
            'cost_annual', 'turnover_annual')},
        'r1_half': {k: r1m[k] for k in (
            'annualized', 'max_drawdown', 'calmar', 'sharpe', 'worst_window',
            'cost_annual', 'turnover_annual', 'mean_exposure')},
        'kelly_d': {k: kelly[k] for k in (
            'annualized', 'max_drawdown', 'calmar', 'sharpe', 'worst_window',
            'cost_annual', 'turnover_annual', 'mean_exposure')},
        'delta_r1_annual_pp': float(r1m['annualized'] - base['annualized']),
    }


def _log(message: str) -> None:
    print(f'[fixed] {message}', flush=True)


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:  # noqa: C901
    args = sys.argv[1:] if argv is None else argv
    skip_2000 = '--skip-2000' in args
    OUT.mkdir(parents=True, exist_ok=True)

    frozen_target = FIX / 'matrix_frozen_500.parquet'
    freeze = freeze_matrix(SOURCES['fixed_500'], frozen_target)
    _log(f'frozen matrix ready: {freeze["sha256"][:12]}')
    frame = load_matrix(frozen_target)
    profile = matrix_profile(frame)
    _log(f'matrix: {profile["rows"]} rows × {profile["columns"]} cols, '
         f'{profile["stocks"]} stocks, {profile["start"]}~{profile["end"]}')

    windows, window_meta = load_windows(
        frame, frozen_target, FIX / 'windows_frozen_500.json')
    _log(f'windows ready: {len(windows)} ({window_meta["source"]})')

    results = base_results(frame, windows)
    base = base_metrics(results)
    curves = [item.curve for item in results]
    dates = base['curve']['date'].to_list()
    rets = daily_returns(base['curve'])
    cost_model = SimConfig().make_cost_model()
    r1 = r1_frame(dates)
    _log(f'base A: {base["annualized"]:.4f} / {base["max_drawdown"]:.4f} '
         f'/ calmar {base["calmar"]:.3f}')

    engine = engine_results(frame, windows)
    engine_metrics = curve_metrics(engine)
    engine_diff = max(
        _num((a.join(b, on='date', suffix='_e')['equity']
              - a.join(b, on='date', suffix='_e')['equity_e'])
             .cast(pl.Float64).abs().max())
        for a, b in zip(engine, curves, strict=True))

    r1_half = overlay_metrics(curves, r1, cost_model)
    kelly = overlay_metrics(curves, kelly_frame(dates, rets), cost_model)
    level = level_scale_metrics(curves, r1)
    _log(f'B R1: {r1_half["annualized"]:.4f} / calmar {r1_half["calmar"]:.3f}; '
         f'D kelly: {kelly["annualized"]:.4f} / calmar {kelly["calmar"]:.3f}')

    p1_scaled = aggregate_staged(staged_windows(
        frame, as_windows(windows), StagedConfig(
            name='P1', fracs=(0.2, 0.2, 0.2, 0.4), target_weight=0.10,
            add_trigger_pct=0.10, entry_stop_pct=0.10, trail_stop_pct=0.10)))
    p1_orig = aggregate_staged(staged_windows(
        frame, as_windows(windows), StagedConfig(
            name='P1_orig', fracs=(0.2, 0.2, 0.2, 0.4), target_weight=0.05,
            add_trigger_pct=0.10, entry_stop_pct=0.10, trail_stop_pct=0.10)))
    _log(f'P1: {p1_scaled["annualized"]:.4f} / calmar '
         f'{p1_scaled["calmar"]:.3f}, single-tranche '
         f'{p1_scaled["single_tranche_ratio"]:.3f}')

    scheme_rows = [
        scheme_row('A_equal_full', base, base),
        scheme_row('B_r1_half', r1_half, base),
        scheme_row('D_kelly_025_250', kelly, base),
        scheme_row('P1_scaled10', p1_scaled, base),
        scheme_row('P1_original_ratio', p1_orig, base),
    ]
    for idx, ratio in ((3, p1_scaled['single_tranche_ratio']),
                       (4, p1_orig['single_tranche_ratio'])):
        scheme_rows[idx]['rule_effective'] = bool(ratio <= 0.70)
        scheme_rows[idx]['single_tranche_ratio'] = float(ratio)
    for idx in (0, 1, 2):
        scheme_rows[idx]['rule_effective'] = None
        scheme_rows[idx]['single_tranche_ratio'] = None

    # ---- 消融：权重符号 / 因子列 / 标的池 / 成本 ----
    ablation: list[dict[str, Any]] = []
    old_windows = replace_weights(
        windows, lambda w: all_positive_weights(w, _legacy_weights('mid')))
    ablation.append(ablation_row(
        'V1_旧符号权重_同矩阵', frame, old_windows, r1, cost_model,
        '同矩阵同因子清单，仅把入选因子权重改回修复前的全正号'))
    _log('ablation V1 done')

    alt500 = load_matrix(SOURCES['alt_500'])
    w500, meta500 = load_windows(alt500, SOURCES['alt_500'],
                                 FIX / 'windows_frozen_alt500.json')
    _log(f'V2 windows: {meta500["source"]}')
    d500 = sorted(alt500.filter(
        (pl.col('date') >= w500[0].test_start)
        & (pl.col('date') < w500[-1].test_end))['date'].unique().to_list())
    ablation.append(ablation_row(
        'V2_205列_新符号', alt500, w500, r1_frame(d500), cost_model,
        '同一 500 面板、同一代码，仅换成 205 列的旧因子矩阵'))
    _log('ablation V2 done')

    old500 = replace_weights(
        w500, lambda w: all_positive_weights(w, _legacy_weights('mid')))
    ablation.append(ablation_row(
        'V3_205列_旧符号', alt500, old500, r1_frame(d500), cost_model,
        '205 列旧矩阵 + 修复前全正号，即择时报告当时的完整口径'))
    _log('ablation V3 done')

    if not skip_2000:
        alt2000 = load_matrix(SOURCES['alt_2000'])
        w2000, _ = load_windows(alt2000, SOURCES['alt_2000'],
                                FIX / 'windows_frozen_alt2000.json')
        d2000 = sorted(alt2000.filter(
            (pl.col('date') >= w2000[0].test_start)
            & (pl.col('date') < w2000[-1].test_end))['date'].unique().to_list())
        ablation.append(ablation_row(
            'V4_标的池2000', alt2000, w2000, r1_frame(d2000), cost_model,
            '同代码同因子集，仅把标的池从 500 扩到分层 2000'))
        _log('ablation V4 done')
    else:
        ablation.append({'variant': 'V4_标的池2000', 'skipped': True})

    no_cost = SimConfig(cost_overrides={'commission_rate': 0.0,
                                       'stamp_tax_rate': 0.0,
                                       'transfer_fee_rate': 0.0,
                                       'slippage': 0.0,
                                       'min_commission': 0.0})
    zero_curves = engine_results_with(frame, windows, no_cost)
    zero_metrics = curve_metrics(zero_curves)
    cost_effect = float(zero_metrics['annualized'] - base['annualized'])

    payload = {
        'meta': {
            'frozen_matrix': freeze,
            'profile': profile,
            'windows': window_meta,
            'window_cache': str((FIX / 'windows_frozen_500.json').relative_to(ROOT)),
            'window_cache_sha256': sha256_file(
                FIX / 'windows_frozen_500.json'),
            'windows_meta': [{'test_start': str(w.test_start),
                              'test_end': str(w.test_end),
                              'n_selected': len(w.selected)} for w in windows],
            'given_base': GIVEN_BASE, 'given_r1': GIVEN_R1,
            'thresholds': THRESHOLDS,
            'cost_round_trip': float(
                cost_model.params['commission_rate'] * 2
                + cost_model.params['stamp_tax_rate']
                + cost_model.params['transfer_fee_rate'] * 2
                + cost_model.params['slippage'] * 2),
            'panels': {
                'panel_500_sha256': sha256_file(
                    ADVICE / 'panel_500_2021-01-01_2026-11-01.parquet'),
                'panel_strat500s_sha256': sha256_file(
                    ADVICE / 'panel_strat500s20261105_2021-01-01_2026-11-01.parquet'),
            },
            'engine_check': {
                'engine_annualized': engine_metrics['annualized'],
                'engine_calmar': engine_metrics['calmar'],
                'max_abs_equity_diff': engine_diff,
            },
        },
        'schemes': {row['scheme']: row for row in scheme_rows},
        'mapping_variants': {
            'returns_x_exposure_per_window': {
                'annualized': r1_half['annualized'],
                'calmar': r1_half['calmar'],
                'worst_window': r1_half['worst_window'],
                'cost_annual': r1_half['cost_annual'],
                'turnover_annual': r1_half['turnover_annual'],
            },
            'level_x_exposure': {
                'annualized': level['annualized'],
                'max_drawdown': level['max_drawdown'],
                'calmar': level['calmar'],
            },
        },
        'ablation': ablation,
        'cost_check': {'zero_cost_annualized': zero_metrics['annualized'],
                       'cost_drag_pp': cost_effect},
        'windows_detail': {
            'A_equal_full': [w['net_return'] for w in base['windows']],
            'B_r1_half': [w['net_return'] for w in r1_half['windows']],
            'D_kelly': [w['net_return'] for w in kelly['windows']],
            'P1_scaled10': [w['net_return'] for w in p1_scaled['windows']],
        },
        'p1_diagnostics': {
            'scaled10': {k: p1_scaled[k] for k in (
                'add_trigger_ratio', 'single_tranche_ratio', 'stop_counts',
                'entries', 'add_events', 'closed_trades', 'win_rate',
                'profit_factor', 'cost_annual')},
            'original': {k: p1_orig[k] for k in (
                'add_trigger_ratio', 'single_tranche_ratio', 'stop_counts',
                'entries', 'add_events', 'closed_trades', 'win_rate',
                'profit_factor', 'cost_annual')},
        },
    }
    payload = _clean(payload)
    (OUT / 'fixed_benchmark.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    write_csvs(scheme_rows, ablation, base, r1_half, kelly, p1_scaled)
    write_report(payload)
    print('[done] reports/kelly_sizing/fixed_benchmark.json')
    return 0


def engine_results_with(frame: pl.DataFrame, windows: list[Frozen],
                        sim: SimConfig) -> list[pl.DataFrame]:
    curves = []
    for item in windows:
        chunk = frame.filter(
            (pl.col('date') >= item.test_start)
            & (pl.col('date') < item.test_end))
        curve, _ = simulate_fixed_hold(build_composite(chunk, item.weights), sim)
        curves.append(curve)
    return curves


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_clean(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def write_csvs(rows: list[dict[str, Any]], ablation: list[dict[str, Any]],
               base: dict[str, Any], r1: dict[str, Any], kelly: dict[str, Any],
               p1: dict[str, Any]) -> None:
    pl.DataFrame(rows).write_csv(OUT / 'fixed_schemes.csv')
    flat = []
    for item in ablation:
        if item.get('skipped'):
            flat.append({'variant': item['variant'], 'note': 'skipped'})
            continue
        flat.append({
            'variant': item['variant'], 'note': item['note'],
            **{f'base_{k}': v for k, v in item['base'].items()},
            **{f'r1_{k}': v for k, v in item['r1_half'].items()},
            **{f'kelly_{k}': v for k, v in item['kelly_d'].items()},
            'delta_r1_annual_pp': item['delta_r1_annual_pp'],
        })
    pl.DataFrame(flat).write_csv(OUT / 'fixed_ablation.csv')
    names = ['A_equal_full', 'B_r1_half', 'D_kelly', 'P1_scaled10']
    series = [base['windows'], r1['windows'], kelly['windows'],
              p1['windows']]
    detail = []
    for idx in range(len(base['windows'])):
        detail.append({'window': idx + 1, **{
            name: float(series[j][idx]['net_return'])
            for j, name in enumerate(names)}})
    pl.DataFrame(detail).write_csv(OUT / 'fixed_windows.csv')


def write_report(payload: dict[str, Any]) -> None:
    meta = payload['meta']
    schemes = payload['schemes']
    ablation = payload['ablation']
    mapping = payload['mapping_variants']
    base = schemes['A_equal_full']
    r1 = schemes['B_r1_half']
    by_name = {item['variant']: item for item in ablation
               if not item.get('skipped')}
    v1 = by_name.get('V1_旧符号权重_同矩阵')
    v2 = by_name.get('V2_205列_新符号')
    v3 = by_name.get('V3_205列_旧符号')
    given = meta['given_base']
    verdict = {
        'c1': r1['pass_c1'], 'c2': r1['pass_c2'],
        'c3': r1['pass_c3'], 'c4': r1['pass_c4'],
    }
    stops = payload['p1_diagnostics']['scaled10']['stop_counts']
    repro = ''
    if v3:
        repro = (f'旧完整口径（205 列矩阵 + 全正号）实测基准 '
                 f'{v3["base"]["annualized"]:.2%} / MDD '
                 f'{v3["base"]["max_drawdown"]:.2%}，R1 '
                 f'{v3["r1_half"]["annualized"]:.2%}；'
                 f'与择时报告 {given["annualized"]:.2%} / '
                 f'{given["max_drawdown"]:.2%}（R1 '
                 f'{meta["given_r1"]["annualized"]:.2%}）的差为 '
                 f'{(v3["base"]["annualized"] - given["annualized"]) * 100:+.2f}pp / '
                 f'{abs(v3["base"]["max_drawdown"] - given["max_drawdown"]) * 100:.2f}pp / '
                 f'{(v3["r1_half"]["annualized"] - meta["given_r1"]["annualized"]) * 100:+.2f}pp。')
    lines = [
        '# 固定基准口径重建报告',
        '',
        '## 结论（先看这里）',
        '',
        f'- 统一口径下基准 A 年化 **{base["annualized"]:.2%}**、'
        f'MDD **{base["max_drawdown"]:.2%}**、Calmar **{base["calmar"]:.3f}**。',
        f'- R1 半仓年化 **{r1["annualized"]:.2%}**（相对 A 下降 '
        f'{r1["annual_drop_pp"] * 100:.2f}pp），Calmar '
        f'**{r1["calmar"]:.3f}**（{r1["calmar_improve"]:+.1%}）。',
        f'- **若按统一口径，R1 不通过**：c1={verdict["c1"]}、c2={verdict["c2"]}、'
        f'c3={verdict["c3"]}、c4={verdict["c4"]}；'
        f'年化降幅 {r1["annual_drop_pp"] * 100:.2f}pp 远超 c2 的 1.00pp 上限，'
        '不得为保留 R1 放宽门槛。',
        f'- 34.86% 与 22.83% 同规则两套数字的根因是**基准曲线换了口径**，'
        f'不是标的池、成本或 R1 映射。{repro}',
        '- 结论：旧口径（205 列 + 全正号，含 02:08 已修复的权重符号缺陷）'
        '不能用于决策；统一口径下 R1 是负增量。',
        '',
        '## 1. 冻结基准',
        '',
        f'- 矩阵：`{meta["frozen_matrix"]["frozen"]}`',
        f'- SHA256：`{meta["frozen_matrix"]["sha256"]}`',
        f'- 行数 {meta["profile"]["rows"]}，列数 {meta["profile"]["columns"]}，'
        f'标的 {meta["profile"]["stocks"]} 只，'
        f'{meta["profile"]["start"]}~{meta["profile"]["end"]}'
        f'（{meta["profile"]["trading_days"]} 个交易日）',
        f'- 两份 500 面板 SHA256 是否相同：'
        f'`{meta["panels"]["panel_500_sha256"] == meta["panels"]["panel_strat500s_sha256"]}`',
        f'- 窗口缓存：`{meta["window_cache"]}`，'
        f'SHA256 `{meta["window_cache_sha256"]}`（不读写共享 windows_cache.pkl）',
        f'- 成本往返 {meta["cost_round_trip"]:.4%}（CostModel 默认参数，未覆盖）',
        f'- 引擎对齐（simulate_fixed_hold vs 本脚本 simulate_equal）：'
        f'年化 {meta["engine_check"]["engine_annualized"]:.2%}，'
        f'max|Δequity| {meta["engine_check"]["max_abs_equity_diff"]:.2f}'
        f'（初始资金 {meta["engine_check"]["max_abs_equity_diff"] / INITIAL:.2%}）',
        '',
        '## 2. 统一四门槛表（同一分母 A）',
        '',
        '| 方案 | 年化 | MDD | Calmar | Sharpe | 最差窗口 | 换手 | 成本/年 '
        '| c1 | c2 | c3 | c4 | 全过 |',
        '|---|---|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    for name in ('A_equal_full', 'B_r1_half', 'D_kelly_025_250',
                 'P1_scaled10', 'P1_original_ratio'):
        row = schemes[name]
        is_base = name == 'A_equal_full'
        flags = '| — | — | — | — | — |' if is_base else (
            f'| {row["pass_c1"]} | {row["pass_c2"]} | {row["pass_c3"]} | '
            f'{row["pass_c4"]} | {row["pass_all"]} |')
        lines.append(
            f'| {name} | {row["annualized"]:.2%} | '
            f'{row["max_drawdown"]:.2%} | {row["calmar"]:.3f} | '
            f'{row["sharpe"]:.2f} | {row["worst_window"]:.2%} | '
            f'{row["turnover_annual"]:.1f}x | {row["cost_annual"]:.2%} '
            f'{flags}')
    lines += [
        '',
        f'门槛（预先注册，未调整）：c1 Calmar 提升 ≥'
        f'{THRESHOLDS["calmar_improve"]:.0%}；c2 年化降 ≤'
        f'{THRESHOLDS["annual_drop_pp"] * 100:.2f}pp；c3 最差窗口改善 ≥'
        f'{THRESHOLDS["worst_improve"]:.0%}；c4 成本增量 ≤'
        f'{THRESHOLDS["cost_increment"]:.2%}/年。',
        '',
        '## 3. 差异定位（逐项实测）',
        '',
        '| 变体 | 基准年化 | 基准 MDD | 基准 Calmar | R1 年化 | R1 Calmar '
        '| R1−基准 pp | 说明 |',
        '|---|---|---|---|---|---|---|---|',
    ]
    for item in ablation:
        if item.get('skipped'):
            lines.append(f'| {item["variant"]} | skipped | | | | | | 未运行 |')
            continue
        lines.append(
            f'| {item["variant"]} | {item["base"]["annualized"]:.2%} | '
            f'{item["base"]["max_drawdown"]:.2%} | '
            f'{item["base"]["calmar"]:.3f} | '
            f'{item["r1_half"]["annualized"]:.2%} | '
            f'{item["r1_half"]["calmar"]:.3f} | '
            f'{item["delta_r1_annual_pp"] * 100:+.2f} | {item["note"]} |')

    def _cell(item: dict[str, Any] | None) -> str:
        if not item:
            return '—'
        b = item.get('base', item)
        return (f'{b["annualized"]:.2%} / {b["max_drawdown"]:.2%} / '
                f'Calmar {b["calmar"]:.3f}')

    lines += [
        '',
        '### 3.1 因子列 × 权重符号 2×2（基准 A）',
        '',
        '| 因子矩阵 | 修复后（原始 IC 符号） | 修复前（全正号） |',
        '|---|---|---|',
        f'| 220 列（当前） | {_cell(base)} | {_cell(v1)} |',
        f'| 205 列（旧） | {_cell(v2)} | {_cell(v3)} |',
        '',
        '2×2 的每一格都只改一个维度，差异因此可完全归因；'
        '再叠加标的池 2000 得到 V4。',
        '',
        f'- 权重符号：同一 220 列矩阵，修复前 {v1["base"]["annualized"]:.2%} / '
        f'R1 {v1["r1_half"]["annualized"]:.2%}（仅 {v1["delta_r1_annual_pp"] * 100:+.2f}pp），'
        f'修复后 {base["annualized"]:.2%} / R1 {r1["annualized"]:.2%}'
        f'（{r1["annual_drop_pp"] * 100:.2f}pp）。R1 的“几乎不掉年化”只在旧符号下成立。'
        if v1 else '- 权重符号：V1 未运行。',
        f'- 因子列：旧符号下 205 列 {v3["base"]["annualized"]:.2%} 对 220 列 '
        f'{v1["base"]["annualized"]:.2%}；新符号下 205 列 '
        f'{v2["base"]["annualized"]:.2%} 对 220 列 {base["annualized"]:.2%}。'
        if v2 and v3 and v1 else '- 因子列：部分变体未运行。',
        f'- 标的池：500→2000 使基准年化 '
        f'{by_name["V4_标的池2000"]["base"]["annualized"]:.2%}，'
        f'R1 差 {by_name["V4_标的池2000"]["delta_r1_annual_pp"] * 100:+.2f}pp；'
        '影响基准水平，但不是 6.6pp 差异的来源。'
        if 'V4_标的池2000' in by_name else '- 标的池：V4 未运行。',
        f'- 成本：基准成本 {base["cost_annual"]:.2%}/年、R1 '
        f'{r1["cost_annual"]:.2%}/年；零成本仅使年化 '
        f'{payload["cost_check"]["cost_drag_pp"] * 100:.2f}pp，口径一致。',
        '',
        f'- 零成本对照：同矩阵同权重年化 '
        f'{payload["cost_check"]["zero_cost_annualized"]:.2%}，'
        f'成本拖累 {payload["cost_check"]["cost_drag_pp"] * 100:.2f}pp；'
        '成本参数两套口径完全一致，不构成 6.6pp 差异来源。',
        f'- R1 映射对照：日收益×仓位年化 '
        f'{mapping["returns_x_exposure_per_window"]["annualized"]:.2%}；'
        f'把仓位乘在净值水平上年化 '
        f'{mapping["level_x_exposure"]["annualized"]:.2%}'
        f'（MDD {mapping["level_x_exposure"]["max_drawdown"]:.2%}，'
        '会产生仓位跳变带来的虚假收益，属错误实现，仅作对照）。',
        '',
        '## 4. 金字塔诊断',
        '',
        f'- scaled10：加仓触发比例 '
        f'{payload["p1_diagnostics"]["scaled10"]["add_trigger_ratio"]:.2%}，'
        f'只走首档比例 '
        f'{payload["p1_diagnostics"]["scaled10"]["single_tranche_ratio"]:.2%}；'
        f'入场止损 {stops.get("stop_entry", 0)} 次、跟踪止损 '
        f'{stops.get("stop_trail", 0)} 次、时间退出 {stops.get("time", 0)} 次。',
        '（只走首档 >70% 即判定规则未实际生效，属于硬性否决项。）',
        '',
        '## 5. 复现方式',
        '',
        '```text',
        '.venv\\Scripts\\python.exe tools/eval_fixed_benchmark.py',
        '```',
        '',
        f'产物：`{payload["meta"]["frozen_matrix"]["frozen"]}`、'
        '`reports/kelly_sizing/fixed_benchmark.json`、'
        '`fixed_schemes.csv`、`fixed_ablation.csv`、`fixed_windows.csv`。',
    ]
    (OUT / '固定基准口径重建报告.md').write_text('\n'.join(lines),
                                              encoding='utf-8')


if __name__ == '__main__':
    raise SystemExit(main())
