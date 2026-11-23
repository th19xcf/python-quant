#!/usr/bin/env python3

"""拉取 2014 年后新股发行价（tushare ``new_share``），供首日涨跌幅制度还原。

产物（``--apply`` 才写正式层）::

    data/local_store/meta/new_share.parquet
    data/local_store/meta/new_share.manifest.json

字段（4 列，tushare 原名）::

    ts_code(str) / ipo_date(date) / issue_date(date) / price(float)
    / price_is_zero(bool)

口径
====
- tushare 字段含义：``ipo_date`` = 上网发行日，``issue_date`` = 上市日，
  ``price`` = 发行价。首日涨跌幅还原用 ``issue_date`` 定位首日，
  ``ipo_date`` 只作发行口径对照。
- ``price`` 即发行价；``price=0`` 的行原样保留，仅用 ``price_is_zero`` 标出，
  不猜值、不改写（实测 3,389/3,390 有值，1 条为 0）。
- 一只证券一行，按 ``ts_code`` 去重（keep last），按 ``ts_code`` 排序。
- 按 3 段日期区间串行请求（2014-2018 / 2019-2022 / 2023-2026），共 3 次；
  原始响应缓存到 ``tmp/new_share_raw/seg_<start>_<end>.parquet``，重复执行
  命中缓存不联网（``--refresh`` 强制重拉，``--offline`` 只用缓存）。

与 ``stock_basic.list_date`` 对拍
================================
分别以 ``ipo_date``（发行日）与 ``issue_date``（上市日）为基准计算
``diff_days = list_date - base_date``，统计分位数/分桶与极值清单写入 sidecar；
极值原因只用本地可验证证据（行情首根 bar、退市原因、list_date_basis）描述，
不做无证据推断。

用法::

    python tools/build_new_share.py            # 重建 + 对拍，不写盘
    python tools/build_new_share.py --apply    # 原子覆盖正式层 + 登记 manifest
    python tools/build_new_share.py --offline  # 只用缓存
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.manifest_refresh import sha256_file  # noqa: E402
from src.data.dataset_contracts import write_dataset

STORE = ROOT / 'data' / 'local_store'
OUT = STORE / 'meta' / 'new_share.parquet'
SIDECAR = STORE / 'meta' / 'new_share.manifest.json'
MANIFEST = STORE / 'meta' / 'manifest.json'
RAW_DIR = ROOT / 'tmp' / 'new_share_raw'
TOKEN_FILE = ROOT / '.secrets' / 'tushare_token.txt'

FIELDS = ('ts_code', 'ipo_date', 'issue_date', 'price')
SEGMENTS = (
    ('20140101', '20181231'),
    ('20190101', '20221231'),
    ('20230101', '20261122'),
)
REQUEST_SLEEP = 0.6

DATE_COLS = ('ipo_date', 'issue_date')


class FetchError(RuntimeError):
    pass


def _pro() -> Any:
    import tushare as ts  # noqa: PLC0415

    token = TOKEN_FILE.read_text(encoding='utf-8').strip() if TOKEN_FILE.is_file() else ''
    if not token:
        raise FetchError(f'tushare token 不存在或为空: {TOKEN_FILE}')
    return ts.pro_api(token)


def _retry(fn, **kwargs: Any) -> pl.DataFrame:
    last: Exception | None = None
    for attempt in range(4):
        try:
            return pl.DataFrame(fn(**kwargs))
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(62.0 if '频率' in str(exc) else 2.0 * (attempt + 1))
    raise FetchError(f'new_share {kwargs} 连续失败: {last}')


def _date_expr(column: str) -> pl.Expr:
    return pl.coalesce([
        pl.col(column).cast(pl.String)
        .str.strptime(pl.Date, '%Y%m%d', strict=False),
        pl.col(column).cast(pl.String).str.slice(0, 10)
        .str.to_date(strict=False),
    ]).alias(column)


def _normalize(raw: pl.DataFrame) -> pl.DataFrame:
    missing = [column for column in FIELDS if column not in raw.columns]
    if missing:
        raise FetchError(f'new_share 返回缺列: {missing}；实际列 {raw.columns}')
    frame = raw.select(list(FIELDS)).with_columns(
        _date_expr('ipo_date'),
        _date_expr('issue_date'),
        pl.col('ts_code').cast(pl.String),
        pl.col('price').cast(pl.Float64),
    )
    if frame['ts_code'].null_count() or frame['ipo_date'].null_count():
        raise FetchError('ts_code / ipo_date 存在空值，不落盘')
    return frame


def _segment_path(start: str, end: str) -> Path:
    return RAW_DIR / f'seg_{start}_{end}.parquet'


def _fetch_segment(pro: Any, start: str, end: str, *, refresh: bool) -> pl.DataFrame:
    path = _segment_path(start, end)
    if path.is_file() and not refresh:
        return _normalize(pl.read_parquet(path))
    raw = _retry(pro.new_share, start_date=start, end_date=end)
    raw = raw.select([c for c in FIELDS if c in raw.columns])
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(raw, path)
    return _normalize(raw)


def _build(frames: list[pl.DataFrame]) -> pl.DataFrame:
    frame = pl.concat(frames).unique(subset=['ts_code'], keep='last')
    frame = frame.with_columns(
        (pl.col('price').fill_null(-1.0) == 0.0).alias('price_is_zero')
    ).sort('ts_code')
    if frame['ts_code'].n_unique() != frame.height:
        raise FetchError('ts_code 去重后仍有重复，不落盘')
    return frame


def _crosscheck(frame: pl.DataFrame, basic: pl.DataFrame, quotes: pl.DataFrame,
                base_col: str) -> dict[str, Any]:
    """以 base_col 为基准，与 stock_basic.list_date 对拍。"""
    joined = frame.join(
        basic.select(['ts_code', 'name', 'list_date', 'status', 'exchange',
                      'delist_reason', 'list_date_basis']),
        on='ts_code', how='left',
    ).with_columns(
        (pl.col('list_date') - pl.col(base_col))
        .dt.total_days().alias('diff_days')
    )
    diff = joined.filter(pl.col('diff_days').is_not_null())['diff_days']
    stats: dict[str, Any] = {
        'base_column': base_col,
        'matched': diff.len(),
        'base_null': frame[base_col].null_count(),
        'missing_in_stock_basic': joined.filter(pl.col('list_date').is_null()).height,
        'missing_in_new_share': basic.filter(
            ~pl.col('ts_code').is_in(frame['ts_code'].implode())).height,
        'min': int(diff.min()) if diff.len() else None,
        'max': int(diff.max()) if diff.len() else None,
        'exact_zero': int((diff == 0).sum()),
        'abs_le_1': int((diff.abs() <= 1).sum()),
        'abs_gt_30': int((diff.abs() > 30).sum()),
        'quantiles': {
            q: float(diff.quantile(q, interpolation='nearest'))
            for q in (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)
        } if diff.len() else {},
        'buckets': {
            'abs<=3': int((diff.abs() <= 3).sum()),
            '4-7': int(((diff.abs() > 3) & (diff.abs() <= 7)).sum()),
            '8-30': int(((diff.abs() > 7) & (diff.abs() <= 30)).sum()),
            '31-90': int(((diff.abs() > 30) & (diff.abs() <= 90)).sum()),
            '91-365': int(((diff.abs() > 90) & (diff.abs() <= 365)).sum()),
            '>365': int((diff.abs() > 365).sum()),
        },
    }
    extremes = (
        joined.filter(pl.col('diff_days').is_not_null())
        .join(quotes.select(['ts_code', 'quote_start']), on='ts_code', how='left')
        .with_columns(
            (pl.col('quote_start') - pl.col(base_col))
            .dt.total_days().alias('quote_start_minus_base')
        )
        .sort(pl.col('diff_days').abs(), descending=True)
        .head(20)
    )
    rows = []
    for row in extremes.iter_rows(named=True):
        diff_days = int(row['diff_days'])
        if row['exchange'] == 'BJ' or row['ts_code'].endswith('.BJ'):
            reason = ('北交所/新三板代码：本地 list_date 取新三板挂牌首根行情，'
                      '早于精选层/北交所发行日')
        elif row['delist_reason'] == '旧码迁移':
            reason = '代码迁移旧码：本地 list_date 取官方摘牌/继任口径'
        elif row['list_date_basis'] == 'official_list':
            reason = 'list_date 取 tushare 官方清单，与发行口径不同'
        elif diff_days > 30:
            reason = ('本地 list_date 晚于发行日超 30 天：'
                      '行情首根 bar 晚于发行日，发行后延迟上市或长期停牌')
        elif diff_days < 0:
            reason = 'list_date 早于发行日：代码复用或本地首根 bar 更早'
        else:
            reason = '日差在发行到上市的正常间隔内'
        rows.append({
            'ts_code': row['ts_code'],
            'name': row['name'],
            'exchange': row['exchange'],
            'status': row['status'],
            'ipo_date': str(row['ipo_date']) if row['ipo_date'] else None,
            'issue_date': str(row['issue_date']) if row['issue_date'] else None,
            'list_date': str(row['list_date']),
            'diff_days': diff_days,
            'quote_start': str(row['quote_start']) if row['quote_start'] else None,
            'delist_reason': row['delist_reason'],
            'list_date_basis': row['list_date_basis'],
            'reason': reason,
        })
    stats['extremes'] = rows
    return stats


def _register(path: Path, frame: pl.DataFrame) -> None:
    if MANIFEST.is_file():
        payload = json.loads(MANIFEST.read_text(encoding='utf-8'))
        files = payload.setdefault('files', [])
        rel = path.relative_to(STORE).as_posix()
        entry = next((item for item in files if item.get('path') == rel), None)
        if entry is None:
            entry = {'path': rel}
            files.append(entry)
        entry.update({
            'rows': frame.height,
            'bytes': path.stat().st_size,
            'sha256': sha256_file(path),
            'columns': frame.columns,
            'registered_elsewhere': 'meta/new_share.manifest.json',
            'note': '新股发行价（一只一行）；由 tools/build_new_share.py 生成',
        })
        payload['generated_at'] = date.today().isoformat()
        write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), MANIFEST)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='原子覆盖正式层并登记 manifest')
    parser.add_argument('--refresh', action='store_true', help='忽略缓存重新请求')
    parser.add_argument('--offline', action='store_true', help='只用本地缓存')
    args = parser.parse_args(argv)

    pro = None if args.offline else _pro()
    frames: list[pl.DataFrame] = []
    for start, end in SEGMENTS:
        cached = _segment_path(start, end).is_file()
        if pro is None and not cached:
            raise FetchError(f'缓存缺失且 --offline: {_segment_path(start, end)}')
        frames.append(_fetch_segment(pro, start, end, refresh=args.refresh))
        if pro is not None and not (cached and not args.refresh):
            time.sleep(REQUEST_SLEEP)

    frame = _build(frames)
    basic = pl.read_parquet(STORE / 'meta' / 'stock_basic.parquet')
    quotes = (pl.read_parquet(STORE / 'meta' / 'daily_index.parquet')
              .filter(pl.col('data_type') == 'stock')
              .select(['ts_code', pl.col('start').alias('quote_start')]))
    stats_ipo = _crosscheck(frame, basic, quotes, 'ipo_date')
    stats_issue = _crosscheck(frame, basic, quotes, 'issue_date')
    zeros = frame.filter(pl.col('price_is_zero'))

    sidecar: dict[str, Any] = {
        'dataset': 'new_share.parquet：一只证券一行，发行价（price）与发行/上市日期',
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'builder': 'tools/build_new_share.py',
        'source': 'tushare new_share；3 段串行请求，原始响应缓存在 tmp/new_share_raw/',
        'segments': [f'{s}-{e}' for s, e in SEGMENTS],
        'rows': frame.height,
        'columns': frame.columns,
        'field_semantics': {
            'ipo_date': '上网发行日（tushare new_share.ipo_date）',
            'issue_date': '上市日（tushare new_share.issue_date），首日制度还原用它定位首日',
            'price': '发行价',
        },
        'date_range': {
            'ipo_date_min': str(frame['ipo_date'].min()),
            'ipo_date_max': str(frame['ipo_date'].max()),
            'issue_date_min': str(frame['issue_date'].min()),
            'issue_date_max': str(frame['issue_date'].max()),
        },
        'price_null': frame['price'].null_count(),
        'price_zero': {
            'rows': zeros.height,
            'detail': [
                {key: (str(value) if isinstance(value, date) else value)
                 for key, value in row.items()}
                for row in zeros.select(list(FIELDS)).to_dicts()
            ],
            'treatment': '原样保留；price_is_zero=true 标出，不猜值',
        },
        'crosscheck_ipo_date_vs_list_date': stats_ipo,
        'crosscheck_issue_date_vs_list_date': stats_issue,
        'authority': 'meta/new_share.parquet 是发行价唯一正式路径；不写入 stock_basic，避免同一字段多份来源',
        'target': str(OUT),
    }

    print(f'rows={frame.height} ipo_date={sidecar["date_range"]} '
          f'price_null={sidecar["price_null"]} price_zero={zeros.height}')
    for label, stats in (('ipo_date', stats_ipo), ('issue_date', stats_issue)):
        print(f'对拍 {label}: {json.dumps({k: stats[k] for k in ("matched", "base_null", "missing_in_stock_basic", "min", "max", "exact_zero", "abs_le_1", "abs_gt_30")}, ensure_ascii=False)}')
        print(f'  分桶: {json.dumps(stats["buckets"], ensure_ascii=False)}')
        for row in stats['extremes'][:6]:
            print(f'  极值 {row["ts_code"]} {row["name"]} ipo={row["ipo_date"]} '
                  f'issue={row["issue_date"]} list={row["list_date"]} '
                  f'diff={row["diff_days"]} quote_start={row["quote_start"]} :: {row["reason"]}')
    if zeros.height:
        print(f'  price=0: {sidecar["price_zero"]["detail"]}')

    if not args.apply:
        print('未写盘（加 --apply 落正式层）')
        return 0

    if OUT.is_file():
        old_sha = sha256_file(OUT)
        sidecar['previous_sha256'] = old_sha
    try:
        _rel = Path(OUT).resolve().relative_to((STORE).resolve()).as_posix()
    except ValueError:
        write_parquet_atomic(frame, OUT)
    else:
        write_dataset('new_share', frame, rel_path=_rel,
                      root=STORE, writer='tools/build_new_share.py')
    sidecar['sha256'] = sha256_file(OUT)
    write_text_atomic(json.dumps(sidecar, ensure_ascii=False, indent=2), SIDECAR)
    _register(OUT, frame)
    print(f'已写入 {OUT} sha256={sidecar["sha256"]}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
