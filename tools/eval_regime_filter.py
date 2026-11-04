#!/usr/bin/env python3

"""市场状态判别 + 仓位择时的研究评估入口。

三块输出：
1. Oracle 理论上界：用未来 N 日收益做完美满仓/空仓（N=5/10/20）；
2. 三条极简规则：均线状态、宽度分位、波动率目标，逐窗与汇总；
3. 叠加到现行因子集：同一套 walk-forward、同一成本，报最差窗与换手变化。

纪律：
- 规则参数全部事前给定，测试窗只应用不调参；
- 规则选择（若需要）只在训练窗完成，训练窗不触碰测试窗行情；
- 状态判定只使用 t 日及之前数据，仓位 t+1 生效；
- 只读行情/广度，不写生产配置，不 commit。

用法::

    python tools/eval_regime_filter.py --start 2021-01-01 --end 2026-11-01 \
        --universe 500 --hold 20
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.config import TIER_PRESETS  # noqa: E402
from src.advice.factor_eval import (  # noqa: E402
    LEGACY_COLUMNS,
    EvalConfig,
    SimConfig,
    _curve_stats,
    build_composite,
    combine_curves,
    simulate_fixed_hold,
    walk_forward,
)
from src.advice.regime import (  # noqa: E402
    RegimeParams,
    build_rule_positions,
    load_breadth_snapshot,
    load_index_daily,
    oracle_equity,
    overlay_curve,
    position_segments,
)

#: 模拟所需的市场列 + 历史长度列
BASE_COLUMNS = [
    'date', 'ts_code', 'close', 'open', 'volume', 'raw_open', 'raw_close',
    'history_rows',
]

#: 规则选择候选（含“不择时”基准，训练窗跑不赢满仓就不择时）
RULE_NAMES = ['none', 'R1_half', 'R1_empty', 'R2_breadth', 'R3_vol']


def _matrix_path(args: argparse.Namespace) -> Path:
    return ROOT / 'tmp' / 'eval_daily_advice' / (
        f'factors_{args.universe}_{args.start}_{args.end}.parquet')


def _windows_cache_path(args: argparse.Namespace) -> Path:
    return ROOT / 'reports' / 'eval_daily_advice' / 'windows_cache.pkl'


def _load_windows(args: argparse.Namespace, matrix: pl.DataFrame) -> list[Any]:
    """优先复用 eval_daily_advice 的 walk-forward 缓存；缺失则重算。"""
    path = _windows_cache_path(args)
    if path.is_file():
        cached = pickle.loads(path.read_bytes())  # noqa: S301 本地自生成缓存
        key = cached.get('key', {})
        expected = {
            'start': args.start, 'end': args.end, 'universe': args.universe,
            'horizon': args.horizon, 'embargo': args.embargo,
        }
        if all(key.get(name) == value for name, value in expected.items()):
            logger.info(f'[regime] 复用 walk-forward 缓存 {path}')
            return cached['windows']
        logger.warning('[regime] walk-forward 缓存 key 不匹配，将重新计算')

    factors = _factor_columns(matrix)
    baseline = {
        spec.column: spec.direction * spec.weight
        for spec in TIER_PRESETS['mid'].factors
    }
    config = EvalConfig(
        start=args.start, end=args.end, universe_size=args.universe,
        hold_days=args.hold, horizon=args.horizon, embargo=args.embargo,
        top_k=args.top_k)
    logger.info(f'[regime] 重新计算 walk-forward（{len(factors)} 个候选因子）')
    return walk_forward(matrix, factors, config, baseline_weights=baseline)


def _factor_columns(matrix: pl.DataFrame) -> list[str]:
    skip = {
        'date', 'ts_code', 'close', 'volume', 'next_tradable', 'history_rows',
        'open', 'high', 'low', 'raw_open', 'raw_high', 'raw_low', 'raw_close',
        'amount', 'pre_close',
    }
    names = [
        name for name in matrix.columns
        if name not in skip and name not in LEGACY_COLUMNS
    ]
    return [name for name in names
            if matrix[name].null_count() < matrix.height * 0.5]


def _read_matrix(args: argparse.Namespace, columns: list[str] | None) -> pl.DataFrame:
    path = _matrix_path(args)
    if not path.is_file():
        raise FileNotFoundError(
            f'缺少因子矩阵缓存 {path}；请先运行 tools/eval_daily_advice.py')
    available = set(pl.read_parquet_schema(path))
    selected = None if columns is None else [
        name for name in columns if name in available]
    return pl.read_parquet(path, columns=selected)


def _simulate_test_windows(
    matrix: pl.DataFrame,
    windows: list[Any],
    sim: SimConfig,
) -> list[tuple[Any, pl.DataFrame, dict[str, Any]]]:
    """逐测试窗满仓模拟（与 eval_daily_advice 完全同口径，含 < test_end 过滤）。"""
    output: list[tuple[Any, pl.DataFrame, dict[str, Any]]] = []
    for item in windows:
        chunk = matrix.filter(
            (pl.col('date') >= item.test_start)
            & (pl.col('date') < item.test_end))
        if chunk['date'].n_unique() < 5:  # noqa: PLR2004
            continue
        composite = build_composite(chunk, item.weights)
        curve, stats = simulate_fixed_hold(composite, sim)
        output.append((item, curve, stats))
    return output


def _simulate_train_window(
    matrix: pl.DataFrame,
    item: Any,
    sim: SimConfig,
) -> pl.DataFrame | None:
    chunk = matrix.filter(
        (pl.col('date') >= item.train_start)
        & (pl.col('date') <= item.train_end))
    if chunk['date'].n_unique() < 5:  # noqa: PLR2004
        return None
    composite = build_composite(chunk, item.weights)
    curve, _ = simulate_fixed_hold(composite, sim)
    return curve if not curve.is_empty() else None


def _clean(value: Any) -> Any:
    if isinstance(value, float):
        return value if value == value and value not in (float('inf'), float('-inf')) \
            else None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _stats(curve: pl.DataFrame) -> dict[str, Any]:
    stats = _curve_stats(curve, 1_000_000.0, curve.height)
    return {name: _clean(value) for name, value in stats.items()}


def _overlay_window(
    curve: pl.DataFrame,
    position: pl.DataFrame,
    cost_model: Any,
    *,
    label: str,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    overlay = overlay_curve(curve, position, cost_model)
    years = max(1e-9, overlay.height / 252.0)
    delta_sum = float(overlay['delta'].abs().sum())
    turnover = delta_sum / years
    equity = overlay.select('date', pl.col('equity_overlay').alias('equity'))
    stats = _stats(equity)
    base = _stats(curve)
    return equity, {
        'test': label,
        'net_return': stats['net_return'],
        'annualized': stats['annualized'],
        'max_drawdown': stats['max_drawdown'],
        'calmar': stats['calmar'],
        'sharpe': stats['sharpe'],
        'days': overlay.height,
        'overlay_turnover': turnover,
        'overlay_delta_sum': delta_sum,
        'overlay_cost_total': float(overlay['overlay_cost'].sum()),
        'delta_annualized': (stats['annualized'] or 0.0) - (base['annualized'] or 0.0),
        'delta_max_drawdown': (stats['max_drawdown'] or 0.0)
        - (base['max_drawdown'] or 0.0),
        'delta_calmar': (stats['calmar'] or 0.0) - (base['calmar'] or 0.0),
        'delta_sharpe': (stats['sharpe'] or 0.0) - (base['sharpe'] or 0.0),
    }


def _evaluate_rule(
    rule: str,
    windows: list[Any],
    curves: list[pl.DataFrame],
    positions: dict[str, pl.DataFrame],
    cost_model: Any,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    overlay_curves: list[pl.DataFrame] = []
    for item, curve in zip(windows, curves, strict=True):
        label = f'{item.test_start}~{item.test_end}'
        equity, stats = _overlay_window(curve, positions[rule], cost_model,
                                        label=label)
        rows.append(stats)
        overlay_curves.append(equity)
    combined, _ = combine_curves(overlay_curves)
    total_years = max(1e-9, sum(row['days'] for row in rows) / 252.0)
    return {
        'combined': _stats(combined),
        'windows': rows,
        'overlay_turnover_annual': float(
            sum(row['overlay_delta_sum'] for row in rows) / total_years),
        'overlay_cost_total': float(
            sum(row['overlay_cost_total'] for row in rows)),
    }


def _baseline(
    windows: list[Any],
    curves: list[pl.DataFrame],
    stats: list[dict[str, Any]],
) -> dict[str, Any]:
    combined, _ = combine_curves(curves)
    total_days = sum(item.get('days', 0) for item in stats)
    turnover = sum(
        item['turnover_annual'] * item.get('days', 0) for item in stats
    ) / max(1, total_days)
    rows: list[dict[str, Any]] = []
    for item, item_stats in zip(windows, stats, strict=True):
        if not item_stats:
            continue
        rows.append({
            'test': f'{item.test_start}~{item.test_end}',
            'net_return': item_stats['net_return'],
            'annualized': item_stats['annualized'],
            'max_drawdown': item_stats['max_drawdown'],
            'calmar': item_stats['calmar'],
            'sharpe': item_stats['sharpe'],
            'turnover_annual': item_stats['turnover_annual'],
            'days': item_stats['days'],
        })
    return {
        'combined': _stats(combined),
        'windows': rows,
        'turnover_annual': float(turnover),
        'worst_window': min(rows, key=lambda row: row['net_return'])
        if rows else None,
    }


def _oracle(base_curve: pl.DataFrame, horizons: list[int]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for horizon in horizons:
        curve = oracle_equity(base_curve, horizon)
        result = _stats(curve)
        output[f'horizon_{horizon}'] = {
            **result,
            'days': curve.height,
            'start': str(curve['date'][0]) if curve.height else None,
            'end': str(curve['date'][-1]) if curve.height else None,
        }
    return output


def _sensitivity(
    windows: list[Any],
    curves: list[pl.DataFrame],
    index_frame: pl.DataFrame,
    breadth: pl.DataFrame,
    base_params: RegimeParams,
    cost_model: Any,
) -> dict[str, Any]:
    """参数 ±10% 的诊断性敏感性；结果不用于重新选择参数。"""
    variants: dict[str, list[RegimeParams]] = {
        'R1_half': [
            replace(base_params, ma_short=18),
            replace(base_params, ma_short=22),
            replace(base_params, ma_long=54),
            replace(base_params, ma_long=66),
        ],
        'R1_empty': [
            replace(base_params, low_exposure=0.0, ma_short=18),
            replace(base_params, low_exposure=0.0, ma_short=22),
            replace(base_params, low_exposure=0.0, ma_long=54),
            replace(base_params, low_exposure=0.0, ma_long=66),
        ],
        'R2_breadth': [
            replace(base_params, breadth_low=0.27),
            replace(base_params, breadth_low=0.33),
            replace(base_params, breadth_high=0.63),
            replace(base_params, breadth_high=0.77),
        ],
        'R3_vol': [
            replace(base_params, target_vol=0.135),
            replace(base_params, target_vol=0.165),
        ],
    }
    output: dict[str, Any] = {}
    for rule, params_list in variants.items():
        items: list[dict[str, Any]] = []
        for params in params_list:
            positions = build_rule_positions(index_frame, breadth, params)
            evaluated = _evaluate_rule(rule, windows, curves, positions, cost_model)
            items.append({
                'params': {
                    'ma_short': params.ma_short,
                    'ma_long': params.ma_long,
                    'breadth_low': params.breadth_low,
                    'breadth_high': params.breadth_high,
                    'target_vol': params.target_vol,
                    'low_exposure': params.low_exposure,
                },
                'annualized': evaluated['combined']['annualized'],
                'max_drawdown': evaluated['combined']['max_drawdown'],
                'calmar': evaluated['combined']['calmar'],
            })
        output[rule] = items
    return output


def _walk_forward_rule_selection(
    matrix: pl.DataFrame,
    windows: list[Any],
    curves: list[pl.DataFrame],
    positions: dict[str, pl.DataFrame],
    sim: SimConfig,
    cost_model: Any,
) -> dict[str, Any]:
    """每个窗口只用训练期数据选规则，再应用到测试窗。"""
    rows: list[dict[str, Any]] = []
    selected_curves: list[pl.DataFrame] = []
    picks: dict[str, int] = dict.fromkeys(RULE_NAMES, 0)
    for item, test_curve in zip(windows, curves, strict=True):
        train_curve = _simulate_train_window(matrix, item, sim)
        choice = 'none'
        if train_curve is not None:
            scores: dict[str, float] = {}
            for rule in RULE_NAMES:
                _, stats = _overlay_window(
                    train_curve, positions[rule], cost_model, label='train')
                scores[rule] = float(stats['calmar'] or 0.0)
            choice = max(scores, key=lambda name: (scores[name], name))
        picks[choice] += 1
        equity, stats = _overlay_window(
            test_curve, positions[choice], cost_model,
            label=f'{item.test_start}~{item.test_end}')
        stats['selected_rule'] = choice
        rows.append(stats)
        selected_curves.append(equity)
    combined, _ = combine_curves(selected_curves)
    return {
        'combined': _stats(combined),
        'windows': rows,
        'picks': picks,
    }


def _leave_one_out(
    windows: list[Any],
    curves: list[pl.DataFrame],
    positions: dict[str, pl.DataFrame],
    cost_model: Any,
    rule_names: list[str],
) -> dict[str, Any]:
    """逐窗剔除后重算，检验改善是否依赖单一窗口。"""
    output: dict[str, Any] = {}
    for rule in rule_names:
        overlays = [
            overlay_curve(curve, positions[rule], cost_model).select(
                'date', pl.col('equity_overlay').alias('equity'))
            for curve in curves
        ]
        rows: list[dict[str, Any]] = []
        for drop_index, item in enumerate(windows):
            kept = [
                curve for index, curve in enumerate(overlays)
                if index != drop_index
            ]
            if not kept:
                continue
            combined, _ = combine_curves(kept)
            stats = _stats(combined)
            rows.append({
                'dropped_window': f'{item.test_start}~{item.test_end}',
                'annualized': stats['annualized'],
                'max_drawdown': stats['max_drawdown'],
                'calmar': stats['calmar'],
                'sharpe': stats['sharpe'],
            })
        output[rule] = rows
    return output


def _state_stats(
    index_frame: pl.DataFrame,
    positions: dict[str, pl.DataFrame],
    start: date,
    end: date,
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    oos_index = index_frame.filter(
        (pl.col('date') >= start) & (pl.col('date') <= end))
    for rule, position in positions.items():
        if rule == 'none':
            continue
        full = position.filter(
            (pl.col('date') >= start) & (pl.col('date') <= end))
        segments = position_segments(full)
        full_history = position_segments(position)
        output[rule] = {
            'oos_days': full.height,
            'oos_mean_exposure': float(full['position'].mean()),
            'oos_full_share': float((full['position'] >= 0.999).mean()),
            'oos_segments': len(segments),
            'oos_switches': int((full['position'].diff().abs() > 1e-9).sum()),
            'history_segments': len(full_history),
            'history_days': position.height,
            'oos_index_days': oos_index.height,
        }
    return output


def _sensitivity_summary(result: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for rule, items in result['sensitivity'].items():
        calmars = [item['calmar'] for item in items
                   if item['calmar'] is not None]
        base = result['rules'][rule]['combined']['calmar']
        output[rule] = {
            'base_calmar': base,
            'min_calmar': min(calmars) if calmars else None,
            'max_calmar': max(calmars) if calmars else None,
            'spread': (max(calmars) - min(calmars)) if calmars else None,
        }
    return output


def _write_csvs(out_dir: Path, result: dict[str, Any]) -> None:
    pl.DataFrame(result['baseline']['windows']).write_csv(
        out_dir / 'baseline_windows.csv')
    rule_rows = []
    for rule, payload in result['rules'].items():
        for row in payload['windows']:
            rule_rows.append({'rule': rule, **row})
    pl.DataFrame(rule_rows).write_csv(out_dir / 'rules_windows.csv')
    selection = result['walk_forward_rule_selection']['windows']
    pl.DataFrame(selection).write_csv(out_dir / 'rule_selection_windows.csv')
    loo_rows = []
    for rule, items in result['leave_one_out'].items():
        for item in items:
            loo_rows.append({'rule': rule, **item})
    pl.DataFrame(loo_rows).write_csv(out_dir / 'leave_one_out.csv')
    sensitivity_rows = []
    for rule, items in result['sensitivity'].items():
        for item in items:
            flat = {name: value for name, value in item.items() if name != 'params'}
            sensitivity_rows.append({
                'rule': rule,
                **item.get('params', {}),
                **flat,
            })
    pl.DataFrame(sensitivity_rows).write_csv(out_dir / 'sensitivity.csv')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='市场状态判别 + 仓位择时研究评估')
    parser.add_argument('--start', default='2021-01-01')
    parser.add_argument('--end', default='2026-11-01')
    parser.add_argument('--universe', type=int, default=500)
    parser.add_argument('--hold', type=int, default=20)
    parser.add_argument('--horizon', type=int, default=20)
    parser.add_argument('--top-k', type=int, default=20)
    parser.add_argument('--embargo', type=int, default=20)
    parser.add_argument('--index-code', default='000300.SH')
    parser.add_argument('--oracle-horizons', type=int, nargs='+', default=[5, 10, 20])
    parser.add_argument('--out-dir', default=str(ROOT / 'reports' / 'regime_filter'))
    args = parser.parse_args(argv)

    window_cache = _windows_cache_path(args)
    windows_columns: list[str] = []
    if window_cache.is_file():
        cached = pickle.loads(window_cache.read_bytes())  # noqa: S301
        windows_columns = sorted({
            name for item in cached.get('windows', []) for name in item.weights
        })
    matrix = _read_matrix(args, None if not windows_columns else [
        *BASE_COLUMNS, *windows_columns])
    windows = _load_windows(args, matrix)
    if not windows:
        logger.error('[regime] 无有效 walk-forward 窗口')
        return 1
    logger.info(f'[regime] 窗口数 {len(windows)}，'
                f'测试区间 {windows[0].test_start}~{windows[-1].test_end}')

    sim = SimConfig(top_k=args.top_k, hold_days=args.hold)
    cost_model = sim.make_cost_model()
    records = _simulate_test_windows(matrix, windows, sim)
    if len(records) != len(windows):
        logger.error('[regime] 部分测试窗无法模拟')
        return 1
    curves = [curve for _, curve, _ in records]
    window_stats = [stats for _, _, stats in records]
    baseline = _baseline(windows, curves, window_stats)
    logger.info(f'[regime] 基准：年化={baseline["combined"]["annualized"]:.2%} '
                f'MDD={baseline["combined"]["max_drawdown"]:.2%} '
                f'Calmar={baseline["combined"]["calmar"]:.2f} '
                f'换手={baseline["turnover_annual"]:.2f}x')
    base_curve = combine_curves(curves)[0]

    index_frame = load_index_daily(args.index_code)
    breadth = load_breadth_snapshot()
    params = RegimeParams()
    positions = build_rule_positions(index_frame, breadth, params)
    rules = {
        rule: _evaluate_rule(rule, windows, curves, positions, cost_model)
        for rule in ['R1_half', 'R1_empty', 'R2_breadth', 'R3_vol', 'none']
    }
    for rule, payload in rules.items():
        combined = payload['combined']
        logger.info(f'[regime] {rule}: 年化={combined["annualized"]:.2%} '
                    f'MDD={combined["max_drawdown"]:.2%} '
                    f'Calmar={combined["calmar"]:.2f} '
                    f'Sharpe={combined["sharpe"]:.2f} '
                    f'叠加换手={payload["overlay_turnover_annual"]:.1f}x')

    oracle = _oracle(base_curve, args.oracle_horizons)
    selection = _walk_forward_rule_selection(
        matrix, windows, curves, positions, sim, cost_model)
    leave_one_out = _leave_one_out(
        windows, curves, positions, cost_model,
        ['none', 'R1_half', 'R1_empty', 'R2_breadth', 'R3_vol'])
    sensitivity = _sensitivity(
        windows, curves, index_frame, breadth, params, cost_model)
    states = _state_stats(
        index_frame, positions, windows[0].test_start, windows[-1].test_end)
    result: dict[str, Any] = {
        'meta': {
            'start': args.start, 'end': args.end, 'universe': args.universe,
            'hold': args.hold, 'top_k': args.top_k, 'embargo': args.embargo,
            'horizon': args.horizon, 'index_code': args.index_code,
            'windows': len(windows),
            'oos_start': str(windows[0].test_start),
            'oos_end': str(windows[-1].test_end),
            'cost_params': dict(cost_model.params),
            'params': {
                'ma_short': params.ma_short, 'ma_long': params.ma_long,
                'breadth_metric': params.breadth_metric,
                'breadth_window': params.breadth_window,
                'breadth_low': params.breadth_low,
                'breadth_high': params.breadth_high,
                'target_vol': params.target_vol,
                'vol_window': params.vol_window,
                'low_exposure': params.low_exposure,
            },
        },
        'baseline': baseline,
        'oracle': oracle,
        'rules': rules,
        'walk_forward_rule_selection': selection,
        'leave_one_out': leave_one_out,
        'sensitivity': sensitivity,
        'sensitivity_summary': _sensitivity_summary({'sensitivity': sensitivity,
                                                     'rules': rules}),
        'state_stats': states,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / 'regime_result.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    _write_csvs(out_dir, result)
    logger.info(f'[regime] 完成，输出 {out_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
