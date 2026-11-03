#!/usr/bin/env python3

"""
因子库命令行入口 —— 一条命令完成「取数 → 计算 → 分层回测 → 聚类 → 报告」。

用法（项目根目录）::

    .venv\\Scripts\\python.exe -m src.quant.factor_lib --size 80 --start 2021-01-01
    .venv\\Scripts\\python.exe -m src.quant.factor_lib --size 80 --skip-strategies
    .venv\\Scripts\\python.exe -m src.quant.factor_lib --size 30 --categories alpha158 technical

首次验证建议 50~100 只流动性最好的标的、5 年数据；确认流程无误后再扩到全市场
（``--size 500`` 或直接用 ``--universe-file`` 指定清单）。
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from loguru import logger

from src.quant.factor_lib import compute, data, evaluate, redundancy, report, strategies
from src.quant.factor_lib.factors import load_all
from src.quant.factor_lib.registry import all_specs, computable_specs


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='因子库：计算 + 分层回测 + 冗余聚类')
    parser.add_argument('--size', type=int, default=80, help='流动性筛选的标的数量')
    parser.add_argument('--start', default='2021-01-01', help='起始日期')
    parser.add_argument('--end', default='2026-10-31', help='结束日期')
    parser.add_argument('--horizon', type=int, default=20, help='持有期（交易日）')
    parser.add_argument('--groups', type=int, default=5, help='分层数量')
    parser.add_argument('--min-names', type=int, default=15, help='每日最少标的数')
    parser.add_argument('--categories', nargs='*', default=None,
                        help='只跑指定大类：alpha101 gtja191 alpha158 famafrench technical')
    parser.add_argument('--universe-file', default=None,
                        help='股票池清单（每行一个 ts_code），优先于流动性筛选')
    parser.add_argument('--skip-strategies', action='store_true', help='跳过策略对照')
    parser.add_argument('--no-cache', action='store_true', help='不复用面板缓存')
    return parser.parse_args(argv)


def _read_universe(path: str) -> list[str]:
    """从文本文件读取股票池（每行一个 ts_code）。"""
    lines = Path(path).read_text(encoding='utf-8').splitlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith('#')]


def run(args: argparse.Namespace) -> dict:
    """执行完整流程并返回汇总字典。"""
    started = time.perf_counter()
    load_all()
    codes = _read_universe(args.universe_file) if args.universe_file else None
    request = data.PanelRequest(
        start=args.start, end=args.end, universe_size=args.size,
        use_cache=not args.no_cache,
    )
    panel = data.load_panel(request, codes=codes)
    summary = data.panel_summary(panel)
    dimension = data.dimension_check(panel)
    logger.info(f'面板：{summary}；量纲自检：{dimension}')

    specs = computable_specs(args.categories)
    trimmed = compute.warmup_trim(panel, specs)
    logger.info(f'预热裁剪：{panel.height} → {trimmed.height} 行（最大 lookback '
                f'{max(s.lookback for s in specs)}）')

    values = compute.compute_all(trimmed, args.categories)
    config = evaluate.EvalConfig(
        horizon=args.horizon, n_groups=args.groups, min_names=args.min_names,
    )
    metrics = evaluate.evaluate_all(values.frame, trimmed, config, specs)

    dates = evaluate.rebalance_dates(trimmed, args.horizon)
    corr = redundancy.correlation_matrix(values.frame, values.names, dates)
    clusters = redundancy.build_clusters(corr, metrics)

    specs_all = all_specs()
    selected = report.select_recommendations(metrics, clusters, specs_all)
    headline = report.build_headline(selected)
    registered = len(specs_all)
    computable = sum(1 for spec in specs_all if spec.computable)
    registry_headline = (
        f'登记表里 {registered} 条公式全部来自公开论文与开源仓库，其中 '
        f'{computable} 条能用本地日线算出来，其余 {registered - computable} 条缺'
        '基本面/行业数据只能登记；公式多不等于有效，能不能赚钱要看分层回测。'
    )

    report.write_registry(specs_all, registry_headline)
    report.write_evaluation(metrics, headline)
    report.write_clusters(clusters, headline)
    recommend_path = report.write_recommendation(selected, headline)

    strategy_frame = None
    strategy_headline = None
    if not args.skip_strategies:
        strategy_frame = strategies.run_all(trimmed, args.horizon)
        strategy_headline = report.build_strategy_headline(strategy_frame)
        report.write_strategies(strategy_frame, strategy_headline)

    buy_rate, sell_rate = evaluate.cost_rates()
    elapsed = round(time.perf_counter() - started, 1)
    meta = {
        'request': vars(args),
        'panel': summary,
        'dimension_check': dimension,
        'cost': {'buy': f'{buy_rate:.6f}', 'sell': f'{sell_rate:.6f}'},
        'factor_counts': {
            'registered': registered,
            'computable': computable,
            'computed': len(values.names),
            'failed': values.failures,
        },
        'eval': {
            'horizon': args.horizon,
            'rebalance_dates': len(dates),
            'evaluated': metrics.height,
        },
        'clusters': {
            'clusters': clusters.clusters.height,
            'kept': len(clusters.kept),
            'dropped': len(clusters.dropped),
        },
        'elapsed_sec': elapsed,
        'report_dir': str(report.REPORT_DIR),
        'recommendation': str(recommend_path),
    }
    summary_path = report.write_summary(
        metrics, clusters, selected, meta, strategy_frame, headline,
    )
    meta['summary'] = str(summary_path)
    report.write_meta(meta)
    logger.info(f'全流程完成，耗时 {elapsed}s，报告目录 {report.REPORT_DIR}')
    return {
        'summary': summary,
        'dimension': dimension,
        'metrics': metrics,
        'clusters': clusters,
        'strategies': strategy_frame,
        'meta': meta,
    }


def main(argv: list[str] | None = None) -> int:
    """CLI 入口。"""
    args = _parse_args(argv)
    run(args)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
