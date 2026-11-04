"""P0 基准对齐审计：修正前 / 修正后对照（只读）。

输入为冻结窗口与因子矩阵缓存，按生产参数（Top-20、持有 20 日、5% 现金缓冲）
逐窗复现策略，再分别用修正前（``d0`` 起算）与修正后（``d1`` 收盘起算）基准
计算同起点超额，输出逐窗与汇总对照。

用法::

    python tools/p0_benchmark_audit.py
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

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
    benchmark_window_stats,
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


def load_windows(path: Path) -> list[dict[str, Any]]:
    """读取冻结窗口，日期转成 ``date`` 对象。"""
    payload = json.loads(path.read_text(encoding='utf-8'))
    rows = []
    for item in payload['windows']:
        rows.append({
            'test_start': date.fromisoformat(str(item['test_start'])[:10]),
            'test_end': date.fromisoformat(str(item['test_end'])[:10]),
            'weights': {str(k): float(v) for k, v in item['weights'].items()},
        })
    return rows


def required_columns(windows: list[dict[str, Any]]) -> list[str]:
    """只投影策略/基准/撮合需要的列，避免整表载入内存。"""
    base = ['date', 'ts_code', 'ret', 'close', 'open', 'raw_open',
            'raw_close', 'pre_close', 'volume', 'history_rows']
    extra: set[str] = set()
    for item in windows:
        extra.update(item['weights'])
    return sorted(set(base) | extra)


def run_dataset(dataset: dict[str, Any], sim: SimConfig) -> dict[str, Any]:
    matrix_path = ROOT / dataset['matrix']
    windows = load_windows(ROOT / dataset['windows'])
    frame = pl.read_parquet(matrix_path, columns=required_columns(windows))

    curves: list[pl.DataFrame] = []
    old_curves: list[pl.DataFrame] = []
    new_curves: list[pl.DataFrame] = []
    rows: list[dict[str, Any]] = []
    for item in windows:
        chunk = frame.filter((pl.col('date') >= item['test_start'])
                             & (pl.col('date') < item['test_end']))
        composite = build_composite(chunk, item['weights'])
        strategy_curve, strategy_stats = simulate_fixed_hold(composite, sim)
        n_days = chunk['date'].n_unique()
        # 同起点口径下策略净值也从首个净值点（d1 收盘）归一
        strategy_same = {
            'net_return': float(strategy_curve['equity'][-1]
                                / strategy_curve['equity'][0] - 1.0)}

        old_curve, old_stats = benchmark_window_stats(
            chunk, n_days=n_days, aligned=False)
        new_curve, new_stats = benchmark_window_stats(
            chunk, n_days=n_days, aligned=True)
        daily = daily_excess_stats(strategy_curve, new_curve)

        curves.append(strategy_curve)
        old_curves.append(old_curve)
        new_curves.append(new_curve)
        rows.append({
            'test': f"{item['test_start']}~{item['test_end']}",
            'days': n_days,
            'strategy_net': strategy_stats['net_return'],
            'strategy_net_same': strategy_same['net_return'],
            'strategy_annualized': strategy_stats['annualized'],
            'old_bench_net': old_stats['net_return'],
            'old_bench_annualized': old_stats['annualized'],
            'new_bench_net': new_stats['net_return'],
            'new_bench_annualized': new_stats['annualized'],
            'old_excess_same_annualized': same_start_excess_annualized(
                strategy_same, old_stats, n_days),
            'new_excess_same_annualized': same_start_excess_annualized(
                strategy_same, new_stats, n_days),
            'excess_t': daily.get('excess_t', 0.0),
            'beta': daily.get('beta', 0.0),
            'turnover_annual': strategy_stats['turnover_annual'],
        })

    strategy_combined, strategy_agg = combine_curves(curves)
    old_combined, old_agg = combine_curves(old_curves)
    new_combined, new_agg = combine_curves(new_curves)
    n_days = strategy_combined.height
    daily_all = daily_excess_stats(strategy_combined, new_combined)
    daily_old = daily_excess_stats(strategy_combined, old_combined)

    summary = {
        'tag': dataset['tag'],
        'windows': rows,
        'strategy_annualized': strategy_agg['annualized'],
        'bench_old_annualized': old_agg['annualized'],
        'bench_new_annualized': new_agg['annualized'],
        # 生产口径：两条曲线各按自己的交易日数年化
        'old_excess_annualized_reported': (strategy_agg['annualized']
                                           - old_agg['annualized']),
        'new_excess_annualized_reported': (strategy_agg['annualized']
                                           - new_agg['annualized']),
        'old_excess_same_annualized': same_start_excess_annualized(
            strategy_agg, old_agg, n_days),
        'new_excess_same_annualized': same_start_excess_annualized(
            strategy_agg, new_agg, n_days),
        'excess_t': daily_all.get('excess_t', 0.0),
        'old_excess_t': daily_old.get('excess_t', 0.0),
        'beta': daily_all.get('beta', 0.0),
        'old_beta': daily_old.get('beta', 0.0),
        'excess_std': _window_excess_std(rows, 'new_excess_same_annualized'),
        'old_excess_std': _window_excess_std(rows, 'old_excess_same_annualized'),
        'turnover_annual': float(
            sum(row['turnover_annual'] * row['days'] for row in rows)
            / max(1, sum(row['days'] for row in rows))),
    }
    return summary


def _window_excess_std(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row[key]) for row in rows]
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return float((sum((value - mean) ** 2 for value in values) / (len(values) - 1))
                 ** 0.5)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='P0 基准对齐前后对照')
    parser.add_argument('--out-dir', default='reports/benchmark_alignment')
    parser.add_argument('--tag', action='append', default=None,
                        help='只跑指定数据集标签，可重复')
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
        detail = pl.DataFrame(summary['windows'])
        detail.write_csv(out_dir / f'p0_{summary["tag"]}_windows.csv')
        print(f"[{summary['tag']}] 修正前超额 "
              f"{summary['old_excess_annualized_reported']:+.2%} → 修正后 "
              f"{summary['new_excess_same_annualized']:+.2%}，"
              f"t={summary['excess_t']:.2f}，beta={summary['beta']:.3f}")

    (out_dir / 'p0_summary.json').write_text(
        json.dumps({'datasets': summaries}, ensure_ascii=False, indent=2),
        encoding='utf-8')
    table = pl.DataFrame([{
        'dataset': item['tag'],
        'strategy_annualized': item['strategy_annualized'],
        'bench_old_annualized': item['bench_old_annualized'],
        'bench_new_annualized': item['bench_new_annualized'],
        'old_excess_reported': item['old_excess_annualized_reported'],
        'new_excess_reported': item['new_excess_annualized_reported'],
        'old_excess_same': item['old_excess_same_annualized'],
        'new_excess_same': item['new_excess_same_annualized'],
        'old_excess_t': item['old_excess_t'],
        'excess_t': item['excess_t'],
        'beta': item['beta'],
        'old_beta': item['old_beta'],
        'excess_std': item['excess_std'],
        'old_excess_std': item['old_excess_std'],
        'turnover_annual': item['turnover_annual'],
    } for item in summaries])
    table.write_csv(out_dir / 'p0_comparison.csv')
    print(table)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
