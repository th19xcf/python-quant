#!/usr/bin/env python3

r"""断点续采估值分片并合并成最终表（``collect_financials.py`` 的收尾补充）。

``collect_financials.py --only valuation --force`` 会重采全市场且**不会**
自动合并（``--only valuation`` 不在 merge 白名单里）；若进程被中断，会留下
一批含最新交易日但覆盖不全的分片。本工具：

1. 从 ``progress.json`` 与全部 ``parts/valuation/*.parquet`` 汇总已完成标的；
2. 只采剩余标的（``force=True`` 传入已筛选的 codes，避免重复抓取）；
3. 调用 ``merge_parts('valuation')``，按 ``(ts_code, trade_date)`` keep='last'
   覆盖旧值，把新交易日并入最终表；
4. 打印合并后的覆盖统计。

用法::

    python tools/resume_valuation.py --commit
    python tools/resume_valuation.py            # 只报告待采数量
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl  # noqa: E402
from loguru import logger  # noqa: E402

from src.data.financials import store  # noqa: E402
from src.data.financials.collector import FinancialsCollector, RetryPolicy  # noqa: E402
from src.utils.config import Config  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='断点续采并合并日频估值')
    parser.add_argument('--commit', action='store_true',
                        help='真正采集并合并（默认只报告）')
    parser.add_argument('--workers', type=int, default=6)
    return parser.parse_args(argv)


def _target_date(root: Path):
    """以列存行情的最新交易日作为估值应追平的目标日。"""
    di = pl.read_parquet(root / 'meta' / 'daily_index.parquet',
                         columns=['data_type', 'end'])
    return di.filter(pl.col('data_type') == 'stock')['end'].max()


def _done_codes(root: Path, target) -> set[str]:
    """只统计**已含目标交易日**的标的；历史分片到 11-05 不算已续采。

    ``progress.json`` 记的是「历史上采过没有」，不能用来判断是否有最新
    交易日，否则会把全部 5,718 只误判为完成。
    """
    done: set[str] = set()
    for path in sorted(store.parts_dir('valuation', root).glob('*.parquet')):
        try:
            frame = pl.read_parquet(path, columns=['ts_code', 'trade_date'])
            done.update(str(c) for c in frame.filter(
                pl.col('trade_date') == target)['ts_code'].unique().to_list())
        except (OSError, pl.exceptions.PolarsError) as exc:
            logger.warning(f'[resume-valuation] 分片不可读 {path}: {exc}')
    return done


def _listed_codes() -> list[str]:
    from tools.collect_financials import load_universe  # noqa: PLC0415

    return load_universe()[1]


def _report(root: Path) -> dict[str, object]:
    path = store.table_path('valuation', root)
    if not path.is_file():
        return {'exists': False}
    frame = pl.read_parquet(path, columns=['ts_code', 'trade_date'])
    return {'exists': True, 'rows': frame.height,
            'codes': frame['ts_code'].n_unique(),
            'max_trade_date': str(frame['trade_date'].max())}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = PROJECT_ROOT / 'data' / 'local_store'
    target = _target_date(root)
    done = _done_codes(root, target)
    listed = _listed_codes()
    pending = [c for c in listed if c not in done]
    logger.info(f'[resume-valuation] 目标日 {target}，在市 {len(listed)} 只，'
                f'已含目标日 {len(done)} 只，待采 {len(pending)} 只')
    if not args.commit:
        logger.info(f'[resume-valuation] dry-run 结束；当前最终表 {_report(root)}')
        return 0
    if pending:
        config = Config()
        retry = RetryPolicy(
            max_retries=int(getattr(config.data, 'retry_max_retries', 3)),
            delay=float(getattr(config.data, 'retry_delay', 1.0)),
            backoff_factor=float(getattr(config.data, 'retry_backoff_factor', 2.0)),
            max_delay=float(getattr(config.data, 'retry_max_delay', 10.0)),
        )
        collector = FinancialsCollector(root, workers=args.workers, retry=retry)
        result = collector.collect_valuation(pending, force=True)
        logger.info(f'[resume-valuation] 续采完成: {result.__dict__}')
    rows = store.merge_parts('valuation', root)
    logger.info(f'[resume-valuation] 合并 {rows:,} 行；最终表 {_report(root)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
