#!/usr/bin/env python3

r"""给基金表打可交易性标记（不删行、不改既有列）。

判据（可量化、可自证）
======================
``is_tradable = False`` 当且仅当该标的的全部本地行情 bar 成交量为 0
（``traded_bars == 0``）。只要历史上出现过任意一根 ``volume > 0`` 的 bar，
就仍视为可交易，避免把成交稀疏的正常 LOF/新上市 ETF 误标。

``tradable_reason``：
- ``placeholder_only``：全段 ``close == 1.0 & volume == 0 & amount == 0``
  （占位行情，无真实净值）；
- ``nav_only_no_trade``：有价格变动但全段零成交（只有净值、无场内成交）；
- ``no_local_quote``：基金表有记录但本地无行情文件。

为什么不用"零成交占比"阈值：实测 887 只非 ETF 中，凡是有过成交的标的，
其零成交占比最高也未达到 99%，不存在"90%~100% 之间"的模糊区间；
用"是否曾有过成交"即可把 86 只从未成交的标的与 801 只正常 LOF/分级分开，
且对 1,734 只 ETF、以及对 2026 年新上市标的不产生任何误标。

落地范围
========
- 默认 dry-run，只打印判据与影响数字；
- ``--apply`` 才写回 ``meta/fund_basic.parquet``（新增两列，保留全部行），
  覆盖前自动备份到 ``_backup/fund_basic_tradability_<时间戳>/``；
- 不改变任何既有筛选默认值；调用方如需排除不可交易标的，用
  ``src.data.quality.tradable_universe.is_tradable()`` 或直接读本列。

用法::

    .venv\\Scripts\\python.exe tools\\mark_fund_tradability.py           # dry-run
    .venv\\Scripts\\python.exe tools\\mark_fund_tradability.py --apply
"""

from __future__ import annotations

import argparse
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic

ROOT = Path(__file__).resolve().parents[1]


def _scan(root: Path) -> tuple[dict[str, dict[str, Any]], dict[int, int]]:
    """扫描基金行情：逐只统计 + 年度行数汇总。"""
    stats: dict[str, dict[str, Any]] = {}
    year_total: dict[int, int] = defaultdict(int)
    for path in sorted((root / 'quotes' / 'fund').rglob('*.parquet')):
        frame = pl.read_parquet(
            path, columns=['date', 'close', 'volume', 'amount'])
        n = frame.height
        traded = int((frame['volume'] > 0).sum())
        placeholder = int(((frame['close'] == 1.0) & (frame['volume'] == 0)
                           & (frame['amount'] == 0)).sum())
        years = frame.group_by(pl.col('date').dt.year().alias('y')).len()
        for year, number in years.iter_rows():
            year_total[int(year)] += int(number)
        stats[path.stem] = {
            'rows': n, 'traded': traded, 'placeholder': placeholder,
            'years': {int(y): int(c) for y, c in years.iter_rows()},
        }
    return stats, year_total


def _classify(stats: dict[str, Any] | None) -> tuple[bool, str | None]:
    if stats is None:
        return False, 'no_local_quote'
    if stats['traded'] > 0:
        return True, None
    if stats['placeholder'] == stats['rows']:
        return False, 'placeholder_only'
    return False, 'nav_only_no_trade'


def _yearly_shares(
    untradable: dict[str, dict[str, Any]], stats: dict[str, dict[str, Any]],
    year_total: dict[int, int],
) -> list[tuple[int, int, int, int, int]]:
    """返回 (年, 不可交易只数, 当年有行情只数, 不可交易行数, 总行数)。"""
    year_codes: dict[int, int] = defaultdict(int)
    for item in stats.values():
        for year in item['years']:
            year_codes[year] += 1
    untrad_codes: dict[int, int] = defaultdict(int)
    untrad_rows: dict[int, int] = defaultdict(int)
    for item in untradable.values():
        for year, number in item['years'].items():
            untrad_codes[year] += 1
            untrad_rows[year] += number
    return [
        (year, untrad_codes[year], year_codes[year], untrad_rows[year],
         year_total[year])
        for year in sorted(year_total)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='基金可交易性标记')
    parser.add_argument('--store-root', default='data/local_store')
    parser.add_argument('--apply', action='store_true', help='写回 fund_basic.parquet')
    args = parser.parse_args(argv)
    root = Path(args.store_root)
    if not root.is_absolute():
        root = ROOT / root
    basic_path = root / 'meta' / 'fund_basic.parquet'
    basic = pl.read_parquet(basic_path)

    stats, year_total = _scan(root)
    untradable: dict[str, dict[str, Any]] = {}
    labels: dict[str, tuple[bool, str | None]] = {}
    for code in basic['ts_code'].to_list():
        item = stats.get(code)
        is_ok, reason = _classify(item)
        labels[code] = (is_ok, reason)
        if not is_ok and item is not None:
            untradable[code] = item

    etf_rows = basic.filter(pl.col('fund_type') == 'ETF')
    etf_bad = [c for c in etf_rows['ts_code'].to_list() if not labels[c][0]]
    untradable_rows = sum(item['rows'] for item in untradable.values())
    print(f'基金表: {basic.height} 只 / 行情文件: {len(stats)} 只')
    print(f'不可交易: {len(untradable)} 只 / {untradable_rows} 行 '
          f'({untradable_rows / sum(i["rows"] for i in stats.values()):.2%})')
    reasons: dict[str, int] = defaultdict(int)
    for code, (_, reason) in labels.items():
        if reason:
            reasons[reason] += 1
    print(f'原因分布: {dict(reasons)}')
    print(f'ETF 中被标不可交易: {len(etf_bad)} 只 {etf_bad[:5]}')
    print('不可交易标的在历史各年的占比（只数 / 行数）:')
    for year, uc, ac, ur, tr in _yearly_shares(
            untradable, stats, year_total):
        print(f'  {year}: {uc}/{ac} 只 ({uc / ac:.1%}), '
              f'{ur}/{tr} 行 ({ur / tr:.2%})')

    if not args.apply:
        print('dry-run：未写盘（加 --apply 执行）')
        return 0

    frame = basic.with_columns([
        pl.Series('is_tradable',
                  [labels[c][0] for c in basic['ts_code'].to_list()],
                  dtype=pl.Boolean),
        pl.Series('tradable_reason',
                  [labels[c][1] for c in basic['ts_code'].to_list()],
                  dtype=pl.Utf8),
    ])
    if {'is_tradable', 'tradable_reason'} <= set(basic.columns) and frame.equals(basic):
        logger.info('[fund_tradability] 标记无变化，跳过写盘')
        return 0
    backup_dir = ROOT / '_backup' / (
        'fund_basic_tradability_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(basic_path, backup_dir / basic_path.name)
    write_parquet_atomic(frame, basic_path)
    logger.info(f'[fund_tradability] 已写入 {frame.height} 行: {basic_path}')
    logger.info(f'[fund_tradability] 原文件备份: {backup_dir}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
