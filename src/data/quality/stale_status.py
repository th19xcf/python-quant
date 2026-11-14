#!/usr/bin/env python3

r"""停更 / 退市标的权威状态表。

上一轮盘点实测：本地列存 9,761 个标的里有 743 只末行早于最新交易日，
其中 285 只精确停在 2019-09-17（通达信数据包整体停更批次）。它们大多是
退市或源端停更标的，继续参与选股/回测会引入幸存者偏差与陈旧价格。

``tradable_universe.py`` 已能产出 ``universe_exclusions.parquet``，但生产
链路（推荐股票池、回测）此前没有消费它。本模块把「停更」这一单一事实
抽成一张轻量、可直接过滤的表：

- 输入 ``meta/daily_index.parquet``（实测与磁盘 9,761 个文件完全一致）；
- 输出 ``meta/universe/stale_status.parquet``，全量 9,761 行，带
  ``is_stale / action / stale_batch / lag_calendar_days``；其中
  ``lag_calendar_days`` 是相对基准日的**有符号**日历差（末行晚于基准日为负，
  不再钳成 0）；
- 读侧 :func:`stale_codes` 返回应排除代码集合，供
  :func:`src.advice.universe.list_local_stock_codes` 使用；
- 状态表缺失时只告警、返回空集合（fail-open，不阻断业务）。

只读数据，唯一写入是状态表本身（原子替换）。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic

#: 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = PROJECT_ROOT / 'data' / 'local_store'

STATUS_SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'asset_class': pl.Utf8,
    'market': pl.Utf8,
    'last_trade_date': pl.Date,
    'last_rows': pl.Int64,
    'lag_calendar_days': pl.Int64,
    'is_stale': pl.Boolean,
    'stale_batch': pl.Utf8,
    'reason': pl.Utf8,
    'action': pl.Utf8,
    'detected_at': pl.Date,
}

#: 通达信历史数据包整体停更批次
TDX_FREEZE_DATE = date(2019, 9, 17)

_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}


def status_path(store_root: Path | str = DEFAULT_ROOT) -> Path:
    return Path(store_root) / 'meta' / 'universe' / 'stale_status.parquet'


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def build_stale_status(store_root: Path | str = DEFAULT_ROOT,
                       as_of: date | None = None) -> pl.DataFrame:
    """按覆盖度索引生成全量停更状态表并原子落盘。"""
    root = Path(store_root)
    index_path = root / 'meta' / 'daily_index.parquet'
    if not index_path.is_file():
        raise FileNotFoundError(f'覆盖度索引不存在: {index_path}')
    index = pl.read_parquet(
        index_path,
        columns=['ts_code', 'data_type', 'market', 'rows', 'end'],
    )
    parsed = [_as_date(value) for value in index['end'].to_list()]
    known = [item for item in parsed if item is not None]
    if not known:
        raise ValueError('覆盖度索引没有任何有效 end 日期')
    # 不能用 max(end) 当「今天」：只要有一只标的的源端多补了一天，其余
    # 全部标的都会被判成停更（实测曾把 743 只放大成 9760 只）。以覆盖度
    # 索引里出现次数最多的交易日作为全体基准；同频次时取较新的一天。
    if as_of is not None:
        snapshot = as_of
    else:
        snapshot = (
            pl.DataFrame({'end': known})
            .group_by('end').len()
            .sort(['len', 'end'], descending=[True, True])
            .row(0, named=True)['end']
        )

    rows: list[dict[str, Any]] = []
    for item, end in zip(index.iter_rows(named=True), parsed, strict=False):
        if end is None:
            continue
        stale = end < snapshot
        rows.append({
            'ts_code': str(item['ts_code']),
            'asset_class': str(item['data_type']),
            'market': str(item['market']),
            'last_trade_date': end,
            'last_rows': int(item['rows'] or 0),
            # 有符号日历差：末行晚于基准日（源端比基准更新）记负数。
            # 曾用 max(0, ...) 钳成 0，把「领先 N 天」伪装成「恰好同步」
            # （2026-11-13 实测 300237.SZ end=11-06 / 基准 11-05，lag 被记 0），
            # 与 DG-31「不得静默归零掩盖真实差异」冲突；负值本身是有效信息。
            'lag_calendar_days': (snapshot - end).days,
            'is_stale': stale,
            'stale_batch': 'tdx_2019_09_17' if end == TDX_FREEZE_DATE else '',
            'reason': 'stale_feed' if stale else '',
            'action': 'exclude' if stale else 'keep',
            'detected_at': snapshot,
        })
    frame = pl.DataFrame(rows, schema=STATUS_SCHEMA).sort(['asset_class', 'ts_code'])

    target = status_path(root)
    write_parquet_atomic(frame, target)
    _CACHE.pop(str(target), None)
    # 写盘后按磁盘实测回写同目录 manifest；临时 store（测试）没有 manifest 时静默跳过
    from src.data.manifest_refresh import refresh_manifest_files  # noqa: PLC0415

    refresh_manifest_files(target.parent / 'manifest.json', root, [target])
    stale_count = int(frame['is_stale'].sum())
    logger.info(
        f'[stale-status] {target} 已生成: {frame.height} 标的, '
        f'{stale_count} 只停更, as_of={snapshot}'
    )
    return frame


def load_stale_status(store_root: Path | str = DEFAULT_ROOT) -> pl.DataFrame | None:
    """读取状态表（按 mtime 缓存）；不存在返回 None。"""
    path = status_path(store_root)
    if not path.is_file():
        return None
    mtime = path.stat().st_mtime_ns
    cached = _CACHE.get(str(path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    frame = pl.read_parquet(path)
    _CACHE[str(path)] = (mtime, frame)
    return frame


def stale_codes(store_root: Path | str = DEFAULT_ROOT) -> set[str]:
    """返回应排除的停更代码集合；状态表缺失时告警并返回空集合。"""
    frame = load_stale_status(store_root)
    if frame is None:
        logger.warning(
            f'[stale-status] 状态表不存在，股票池未做停更过滤: {status_path(store_root)}'
        )
        return set()
    if 'action' not in frame.columns:
        return set()
    return set(
        frame.filter(pl.col('action') == 'exclude')['ts_code'].to_list()
    )


def is_stale_code(ts_code: str, store_root: Path | str = DEFAULT_ROOT) -> bool:
    """单代码查询；状态表缺失按 False 处理。"""
    return str(ts_code).upper() in stale_codes(store_root)


def _main(argv: list[str] | None = None) -> int:
    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(description='生成停更/退市标的状态表')
    parser.add_argument('--store-root', default=str(DEFAULT_ROOT))
    parser.add_argument('--as-of', help='快照日 YYYY-MM-DD，缺省取索引最大 end')
    args = parser.parse_args(argv)
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    frame = build_stale_status(args.store_root, as_of)
    by_class = frame.group_by('asset_class').agg(
        pl.col('is_stale').sum().alias('stale'),
        pl.len().alias('total'),
    ).sort('asset_class')
    for row in by_class.iter_rows(named=True):
        logger.info(f"  {row['asset_class']}: {row['stale']}/{row['total']} 只停更")
    return 0


if __name__ == '__main__':
    raise SystemExit(_main())


__all__ = [
    'DEFAULT_ROOT',
    'TDX_FREEZE_DATE',
    'build_stale_status',
    'is_stale_code',
    'load_stale_status',
    'stale_codes',
    'status_path',
]
