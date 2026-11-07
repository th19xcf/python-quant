#!/usr/bin/env python3

r"""按日在市股票池（PIT universe）。

**为什么需要**：TDX 源保留了退市股直到退市日的完整行情，本地并不缺
「退市股历史 K 线」；真正的幸存者偏差来自「用当前仍在市的代码列表构造
历史回测池」，或者反过来「用当前是否停更去整段剔除历史池」（后者是
前视偏差）。本模块把每只标的的实际有行情区间固化成可点时查询的资产：

    listed_codes(date(2016, 6, 1))  -> 当日 first<=d<=last 的全部代码

区间来源 ``meta/daily_index.parquet``（实测 9,761 个文件逐个登记，
``start/end`` 与磁盘一致），停更标记来自 ``meta/universe/stale_status.parquet``。
只读业务数据；唯一写入是 ``meta/universe/listing_intervals.parquet`` 本身。

用法::

    python -m src.data.quality.listing_intervals --json        # 只读统计
    python -m src.data.quality.listing_intervals --commit      # 生成资产
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = PROJECT_ROOT / 'data' / 'local_store'
OUTPUT_RELATIVE = Path('meta/universe/listing_intervals.parquet')


def build_listing_intervals(store_root: Path | str = DEFAULT_ROOT) -> pl.DataFrame:
    """由 daily_index + stale_status 生成在市区间表。"""
    root = Path(store_root)
    index = pl.read_parquet(root / 'meta' / 'daily_index.parquet').select(
        ['ts_code', 'data_type', 'market', 'rows', 'start', 'end'])
    status_path = root / 'meta' / 'universe' / 'stale_status.parquet'
    if status_path.is_file():
        status = pl.read_parquet(status_path).select(
            ['ts_code', 'is_stale', 'action', 'last_trade_date'])
        frame = index.join(status, on='ts_code', how='left')
    else:
        frame = index.with_columns([
            pl.lit(False).alias('is_stale'),
            pl.lit('keep').alias('action'),
            pl.col('end').alias('last_trade_date'),
        ])
    return frame.with_columns([
        pl.col('start').alias('first_trade_date'),
        pl.col('end').alias('last_quote_date'),
        pl.col('is_stale').fill_null(False),
        pl.col('action').fill_null('keep'),
        pl.when(pl.col('is_stale').fill_null(False))
          .then(pl.lit('delisted_or_stale'))
          .otherwise(pl.lit('active'))
          .alias('listing_status'),
        pl.lit('daily_index.start/end；停牌日无 bar 不代表退市')
          .alias('coverage_note'),
    ]).sort(['data_type', 'ts_code'])


def write_listing_intervals(store_root: Path | str = DEFAULT_ROOT) -> Path:
    """原子写入在市区间表，返回路径。"""
    root = Path(store_root)
    target = root / OUTPUT_RELATIVE
    target.parent.mkdir(parents=True, exist_ok=True)
    frame = build_listing_intervals(root)
    tmp = target.with_suffix('.parquet.tmp')
    frame.write_parquet(tmp, compression='zstd', statistics=True)
    tmp.replace(target)
    return target


def _load(root: Path | str = DEFAULT_ROOT) -> pl.DataFrame:
    path = Path(root) / OUTPUT_RELATIVE
    if not path.is_file():
        # 资产缺失时按 daily_index 现场构建，保证查询永远可用
        return build_listing_intervals(root)
    return pl.read_parquet(path)


def listed_codes(
    as_of: date | str,
    store_root: Path | str = DEFAULT_ROOT,
    data_type: str = 'stock',
) -> set[str]:
    """返回 ``as_of`` 当日实际在市（有行情区间覆盖）的代码集合。"""
    day = date.fromisoformat(as_of) if isinstance(as_of, str) else as_of
    frame = _load(store_root).filter(
        (pl.col('data_type') == data_type)
        & (pl.col('first_trade_date') <= day)
        & (pl.col('last_quote_date') >= day))
    return set(frame['ts_code'].to_list())


def coverage_summary(store_root: Path | str = DEFAULT_ROOT) -> dict[str, Any]:
    frame = build_listing_intervals(store_root)
    summary: dict[str, Any] = {
        'rows': frame.height,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'by_type': frame.group_by('data_type').agg(
            pl.len().alias('codes'),
            pl.col('first_trade_date').min().alias('earliest'),
            pl.col('last_quote_date').max().alias('latest'),
        ).sort('data_type').to_dicts(),
        'status': frame['listing_status'].value_counts().to_dicts(),
    }
    for probe in ('2010-01-04', '2016-01-04', '2020-01-02', '2024-01-02'):
        summary.setdefault('listed_probe', {})[probe] = len(listed_codes(
            date.fromisoformat(probe), store_root, data_type='stock'))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='按日在市股票池（PIT universe）')
    parser.add_argument('--store-root', default=str(DEFAULT_ROOT))
    parser.add_argument('--json', action='store_true', help='输出统计 JSON')
    parser.add_argument('--commit', action='store_true',
                        help='写入 meta/universe/listing_intervals.parquet')
    args = parser.parse_args(argv)
    root = Path(args.store_root)
    if args.commit:
        path = write_listing_intervals(root)
        logger.info(f'已生成在市区间表: {path}')
    if args.json or not args.commit:
        sys.stdout.write(json.dumps(coverage_summary(root), ensure_ascii=False,
                                    indent=2, default=str) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
