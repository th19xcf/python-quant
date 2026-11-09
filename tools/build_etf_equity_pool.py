#!/usr/bin/env python3

r"""重建 ETF 权益池清单 ``meta/pools/etf_equity_pool_<YYYYMMDD>.csv``。

背景
====
原清单由仓库外的一次性脚本（``E:\www\tmp\etf_pool_build.py``）生成，仓库内
没有入口，口径只散落在文档里。本脚本把口径固化，可按任意 ``--as-of`` 重算。

口径（与 2026-11-04 版清单一致）
================================
- 标的域：``meta/names.json`` 名称含 ``ETF`` 且不含 ``联接``，本地有基金行情，
  且上市首行不晚于 ``--as-of``；
- 分类：按名称/代码段分为 境内权益 / 跨境 / 债券 / 黄金；
- ``full``：境内权益；
- ``std3y``：``full`` 且首行不晚于 ``as-of`` 往前 3 年；
- ``pool``：``std3y`` 且近 ``--adv-window``（默认 120）行日成交额中位数
  ≥ ``--min-adv``（默认 1 亿）；
- ``median_adv_120d``：该标的自身末 120 行（截至 ``--as-of``）的成交额中位数，
  四舍五入 2 位；
- ``event_count``：``fund_dividend`` + ``fund_share_change`` 中 ``ex_date``
  ≤ ``as-of`` 的行数；
- ``fake_bar_hits``：``meta/anomalies/fund_fake_bar_bars.csv`` 中该标的、
  ``trade_date`` ≤ ``as-of`` 的行数；
- 排序：``full`` → ``pool`` → ``std3y``，各档内按 ``median_adv_120d`` 降序。

数据来源
========
``meta/names.json``、``quotes/fund/**``、``factors/fund_dividend.parquet``、
``factors/fund_share_change.parquet``、``meta/anomalies/fund_fake_bar_bars.csv``。

更新时机
========
随基金行情与分红/份额事件更新后重跑；文件名为 ``--as-of`` 日期，历史版本保留。

用法::

    .venv\\Scripts\\python.exe tools\\build_etf_equity_pool.py            # dry-run
    .venv\\Scripts\\python.exe tools\\build_etf_equity_pool.py --apply
    .venv\\Scripts\\python.exe tools\\build_etf_equity_pool.py \\
        --as-of 2026-11-04 --check
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

ROOT = Path(__file__).resolve().parents[1]

COLUMNS = [
    'tier', 'ts_code', 'name', 'list_date', 'rows', 'first_date', 'last_date',
    'median_adv_120d', 'event_count', 'fake_bar_hits', 'in_pool_reason',
]

#: 跨境 ETF 名称关键词（与 2026-11-04 版清单口径一致）
CROSS_KEYWORDS = (
    '纳指', '纳斯达克', '标普', '恒生', '港股', '中概', '美国', '日经', '德国',
    '法国', '亚太', '东南亚', '沙特', '越南', '印度', '海外', '全球', '国际',
    '日韩', '英国', '欧洲', '日本',
)

REASONS = {
    'full': '境内权益ETF（宽基/行业/主题），standard 池内可交易',
}


def _resolve_root(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def _years_before(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:  # 2 月 29 日
        return day.replace(year=day.year - years, day=28)


def classify(name: str, ts_code: str) -> str:
    """按名称/代码段分类：equity/cross/bond/gold。"""
    if '黄金股' in name:
        return 'equity'
    if ts_code.startswith('511') or '债' in name:
        return 'bond'
    if ts_code.startswith('518') or '黄金' in name or '白银' in name:
        return 'gold'
    if ts_code.startswith('513') or any(k in name for k in CROSS_KEYWORDS):
        return 'cross'
    return 'equity'


def _event_counts(root: Path, cutoff: date) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name in ('fund_dividend', 'fund_share_change'):
        path = root / 'factors' / f'{name}.parquet'
        if not path.is_file():
            continue
        frame = pl.read_parquet(path, columns=['ts_code', 'ex_date'])
        frame = frame.filter(pl.col('ex_date').is_not_null()
                             & (pl.col('ex_date') <= cutoff))
        for code, number in frame.group_by('ts_code').len().iter_rows():
            counts[code] = counts.get(code, 0) + int(number)
    return counts


def _fake_bar_counts(root: Path, cutoff: date) -> dict[str, int]:
    path = root / 'meta' / 'anomalies' / 'fund_fake_bar_bars.csv'
    if not path.is_file():
        return {}
    frame = pl.read_csv(path, infer_schema_length=0)
    frame = frame.with_columns(pl.col('trade_date').str.slice(0, 10)
                               .str.strptime(pl.Date, '%Y-%m-%d', strict=False))
    frame = frame.filter(pl.col('trade_date').is_not_null()
                         & (pl.col('trade_date') <= cutoff))
    return {code: int(number) for code, number in
            frame.group_by('ts_code').len().iter_rows()}


def _fmt_median(value: float) -> str:
    return f'{round(value, 2)}'


def build_rows(
    root: Path,
    as_of: date,
    *,
    min_adv: float = 1e8,
    adv_window: int = 120,
    min_years: int = 3,
) -> list[dict[str, str]]:
    """按口径生成三档清单行。"""
    names: dict[str, str] = json.loads(
        (root / 'meta' / 'names.json').read_text(encoding='utf-8'))
    # 输出 name 列以 fund_basic 为准（与 2026-11-04 版一致），缺失时回退 names.json
    basic_names: dict[str, str] = dict(pl.read_parquet(
        root / 'meta' / 'fund_basic.parquet',
        columns=['ts_code', 'name'],
    ).iter_rows())
    events = _event_counts(root, as_of)
    fake_bars = _fake_bar_counts(root, as_of)
    cutoff_3y = _years_before(as_of, min_years)

    records: list[dict[str, Any]] = []
    for path in sorted((root / 'quotes' / 'fund').rglob('*.parquet')):
        code = path.stem
        name = names.get(code, '')
        if 'ETF' not in name or '联接' in name:
            continue
        try:
            frame = pl.read_parquet(path, columns=['date', 'amount'])
        except (OSError, ValueError, PolarsError) as exc:
            logger.warning(f'[etf_pool] 读取失败 {path}: {exc}')
            continue
        frame = frame.filter(pl.col('date') <= as_of).sort('date')
        if frame.is_empty():
            continue
        first = frame['date'][0]
        adv = float(frame.tail(adv_window)['amount'].median() or 0.0)
        records.append({
            'ts_code': code,
            'name': basic_names.get(code) or name,
            'kind': classify(name, code),
            'first': first,
            'last': frame['date'][-1],
            'rows': frame.height,
            'adv': adv,
            'events': events.get(code, 0),
            'fake': fake_bars.get(code, 0),
        })

    equity = [r for r in records if r['kind'] == 'equity']
    std3y = [r for r in equity if r['first'] <= cutoff_3y]
    pool = [r for r in std3y if r['adv'] >= min_adv]

    rows: list[dict[str, str]] = []
    reasons = {
        'full': REASONS['full'],
        'std3y': (f'境内权益 + 上市≥{min_years}年'
                  f'（first_date≤{cutoff_3y.isoformat()}）'),
        'pool': (f'境内权益 + 上市≥{min_years}年 + 近半年({adv_window}日)'
                 f'中位日成交额≥{min_adv / 1e8:g}亿元'),
    }
    for tier, subset in (('full', equity), ('pool', pool), ('std3y', std3y)):
        for item in sorted(subset, key=lambda r: r['adv'], reverse=True):
            rows.append({
                'tier': tier,
                'ts_code': item['ts_code'],
                'name': item['name'],
                'list_date': item['first'].isoformat(),
                'rows': str(item['rows']),
                'first_date': item['first'].isoformat(),
                'last_date': item['last'].isoformat(),
                'median_adv_120d': _fmt_median(item['adv']),
                'event_count': str(item['events']),
                'fake_bar_hits': str(item['fake']),
                'in_pool_reason': reasons[tier],
            })
    return rows


def _write_csv(rows: list[dict[str, str]], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + '.tmp')
    with open(tmp, 'w', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(target)


def _compare(rows: list[dict[str, str]], target: Path) -> int:
    """逐行逐列比对；返回 0 一致，1 有差异。"""
    if not target.is_file():
        print(f'--check: 目标不存在 {target}')
        return 1
    ref = pl.read_csv(target, infer_schema_length=0)
    ref_rows = ref.to_dicts()
    if len(ref_rows) != len(rows):
        print(f'--check: 行数不一致 现有 {len(ref_rows)} / 重算 {len(rows)}')
        return 1
    diffs: dict[str, int] = {}
    samples: list[str] = []
    for index, (left, right) in enumerate(zip(ref_rows, rows, strict=True), 1):
        for column in COLUMNS:
            if str(left.get(column, '')) != right[column]:
                diffs[column] = diffs.get(column, 0) + 1
                if len(samples) < 8:
                    samples.append(
                        f'  行{index} {right["tier"]}/{right["ts_code"]} '
                        f'{column}: 现有={left.get(column)!r} 重算={right[column]!r}')
    if not diffs:
        print(f'--check: {len(rows)} 行逐列一致')
        return 0
    print(f'--check: {len(rows)} 行中 {sum(diffs.values())} 个单元格不一致 {diffs}')
    for line in samples:
        print(line)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='重建 ETF 权益池清单')
    parser.add_argument('--store-root', default='data/local_store')
    parser.add_argument('--as-of', default='',
                        help='快照日 YYYY-MM-DD（默认取 510300.SH 末根 bar）')
    parser.add_argument('--output', default='',
                        help='输出 CSV（默认 <store>/meta/pools/etf_equity_pool_<as_of>.csv）')
    parser.add_argument('--min-adv', type=float, default=1e8)
    parser.add_argument('--adv-window', type=int, default=120)
    parser.add_argument('--min-years', type=int, default=3)
    parser.add_argument('--apply', action='store_true', help='写盘；缺省只打印摘要')
    parser.add_argument('--check', action='store_true', help='与现有 CSV 逐列比对')
    args = parser.parse_args(argv)

    root = _resolve_root(args.store_root)
    as_of = _as_date(args.as_of) if args.as_of else None
    if as_of is None:
        as_of = pl.read_parquet(
            root / 'quotes' / 'fund' / 'sh' / '510300.SH.parquet',
            columns=['date'],
        )['date'].max()
    target = (Path(args.output) if args.output else
              root / 'meta' / 'pools' / f'etf_equity_pool_{as_of:%Y%m%d}.csv')

    rows = build_rows(
        root, as_of, min_adv=args.min_adv,
        adv_window=args.adv_window, min_years=args.min_years,
    )
    counts = {tier: sum(1 for r in rows if r['tier'] == tier)
              for tier in ('full', 'std3y', 'pool')}
    print(f'as_of={as_of} 输出={target}')
    print(f'full={counts["full"]} std3y={counts["std3y"]} pool={counts["pool"]}')

    if args.check:
        return _compare(rows, target)
    if not args.apply:
        print('dry-run：未写盘（加 --apply 执行）')
        return 0

    if target.is_file():
        backup = target.with_suffix(target.suffix + '.bak')
        shutil.copy2(target, backup)
        logger.info(f'[etf_pool] 原文件备份: {backup}')
    _write_csv(rows, target)
    logger.info(f'[etf_pool] 已写入 {len(rows)} 行: {target}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
