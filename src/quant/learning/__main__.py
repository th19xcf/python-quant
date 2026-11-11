#!/usr/bin/env python3

"""命令行入口：在本地列存上跑 L1/L2 样本外评估并生成 Markdown 报告。"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

from src.quant.learning.config import WalkForwardConfig
from src.quant.learning.data import (
    DEFAULT_FACTOR_COLS,
    cost_input_coverage,
    load_learning_dataset,
)
from src.quant.learning.report import render_report
from src.quant.learning.walk_forward import build_learning_report

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='自学习第一阶段 walk-forward 评估')
    parser.add_argument('--root', type=Path, default=PROJECT_ROOT,
                        help='项目根目录（默认自动推断）')
    parser.add_argument('--start', type=_parse_date, default=date(2018, 1, 1),
                        help='样本起始日（含），默认 2018-01-01')
    parser.add_argument('--end', type=_parse_date, default=date(2026, 10, 31),
                        help='样本结束日（含），默认 2026-10-31')
    parser.add_argument('--limit', type=int, default=600, help='抽样标的数')
    parser.add_argument('--asset', default='stock', choices=['stock', 'fund'],
                        help='资产类型，默认 stock')
    parser.add_argument('--horizon', type=int, default=10, help='持有期（交易日）')
    parser.add_argument('--train-size', type=int, default=504, help='训练窗长度')
    parser.add_argument('--test-size', type=int, default=126, help='测试窗长度')
    parser.add_argument('--embargo', type=int, default=20, help='隔离带长度，须 ≥ 持有期')
    parser.add_argument('--warmup', type=int, default=250,
                        help='因子预热交易日数（从样本开头剔除）')
    parser.add_argument('--lightgbm', action='store_true',
                        help='启用 LightGBM 对照（需 poetry install --with learning）')
    parser.add_argument('--report', type=Path,
                        default=PROJECT_ROOT / 'docs' / '自学习第一阶段报告.md',
                        help='输出 Markdown 报告路径')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logger.remove()
    logger.add(sys.stderr, level='INFO')
    config = WalkForwardConfig(
        horizon=args.horizon,
        train_size=args.train_size,
        test_size=args.test_size,
        embargo=args.embargo,
    )
    started = time.perf_counter()

    # 向前多取一段用于因子预热，再剔除开头 warmup 个交易日
    load_start = args.start - timedelta(days=450)
    panel, scope = load_learning_dataset(
        Path(args.root), load_start, args.end, limit=args.limit,
        asset=args.asset)
    dates = panel['date'].unique().sort().to_list()
    if dates:
        warmup_cutoff = dates[min(args.warmup, len(dates) - 1)]
        cutoff = max(warmup_cutoff, args.start)
        panel = panel.filter(panel['date'] >= cutoff)
        scope['start'] = str(panel['date'].min())
        scope['rows'] = panel.height
        scope['codes'] = panel['ts_code'].n_unique()
        scope.update(cost_input_coverage(panel))

    report = build_learning_report(
        panel, DEFAULT_FACTOR_COLS, config,
        title='自学习第一阶段：L1 参数自适应 vs L2 因子权重学习',
        include_lightgbm=args.lightgbm,
        data_scope=scope,
    )
    output = Path(args.report)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(report), encoding='utf-8', newline='\n')
    logger.info(f'报告已写入 {output}，总耗时 {time.perf_counter() - started:.1f}s')
    for item in report.weight_schemes:
        logger.info(
            f'[L2] {item.name}: IC={item.ic_mean:.4f} ICIR={item.icir:.3f} '
            f'年化={item.annual_return:.2f}% 回撤={item.max_drawdown:.2f}%')
    for item in report.param_schemes:
        logger.info(
            f'[L1] {item.name}: IC={item.ic_mean:.4f} 年化={item.annual_return:.2f}% '
            f'回撤={item.max_drawdown:.2f}%')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
