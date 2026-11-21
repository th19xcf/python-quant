#!/usr/bin/env python3

"""股票名录（采集域）的本地权威读取。

口径（2026-11-16 起）：``data/local_store`` 是唯一权威来源，日常链路不再读
MySQL，也不做本地与 MySQL 对照。这里只提供两件事：

1. 从 ``meta/stock_basic.parquet`` 取 ``status == 'L'`` 的在市清单与
   ``symbol -> ts_code`` 映射；
2. 名录新鲜度闸门：把名录 manifest 的 ``latest_trade_date`` 与采集终点
   （缺省为最近交易日）比较，落后时由调用方决定判失败或告警，禁止静默使用旧名录。

MySQL 读取路径保留在各调用点原开关（``remote_database_fallback``）之后，
默认不走。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from src.data.local_store.paths import resolve_store_root
from src.data.trading_calendar import previous_trading_day
from src.data.universe_filter import (
    UNIVERSE_SCOPE_BACKTEST,
    UNIVERSE_SCOPE_SELECTION,
)

STOCK_BASIC = Path('meta') / 'stock_basic.parquet'
STOCK_BASIC_MANIFEST = Path('meta') / 'stock_basic.manifest.json'


def names_latest_trade_date(store_root: Path) -> date | None:
    """名录 manifest 记录的数据末日；缺失或不可解析时返回 ``None``。"""
    path = Path(store_root) / STOCK_BASIC_MANIFEST
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    raw = payload.get('latest_trade_date')
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def latest_trading_day(store_root: Path, today: date | None = None) -> date | None:
    """交易日历里的最近交易日（含当日）。"""
    return previous_trading_day(today or date.today(), Path(store_root), inclusive=True)


def freshness_lag_days(store_root: Path, *, as_of: date | None = None,
                       today: date | None = None) -> int | None:
    """名录落后采集终点多少天；``None`` 表示无法判定（manifest 缺失）。"""
    names_date = names_latest_trade_date(store_root)
    target = as_of or latest_trading_day(store_root, today)
    if names_date is None or target is None:
        return None
    return (target - names_date).days


def read_stock_basic(store_root: Path) -> pl.DataFrame:
    """读取本地股票名录（缺失即报错，不回退 MySQL）。"""
    path = Path(store_root) / STOCK_BASIC
    if not path.is_file():
        raise SystemExit(f'本地股票名录缺失：{path}')
    return pl.read_parquet(path, columns=['ts_code', 'name', 'list_date',
                                         'delist_date', 'status', 'market'])


def load_local_universe(store_root: Path, *,
                        scope: str = UNIVERSE_SCOPE_SELECTION
                        ) -> tuple[dict[str, str], list[str]]:
    """返回 ``(symbol -> ts_code, ts_code 清单)``，清单口径由 ``scope`` 决定。

    - ``selection``（默认，选股/交易）：只取 ``status == 'L'`` 的在市标的；
    - ``backtest``（回测）：返回名录全部标的（含 ``status == 'D'``），由调用方
      按 ``delist_date <= as_of`` 截断，避免幸存者偏差。

    两种口径共用同一个 ``status`` 读取，不留第二套隐式规则。
    """
    frame = read_stock_basic(store_root)
    symbols: dict[str, str] = {}
    listed: list[str] = []
    for ts_code, status in frame.select('ts_code', 'status').iter_rows():
        if not ts_code:
            continue
        symbols[str(ts_code).split('.')[0]] = str(ts_code)
        if scope == UNIVERSE_SCOPE_BACKTEST or status == 'L':
            listed.append(str(ts_code))
    return symbols, sorted(listed)


def load_local_codes_with_list_date(
        store_root: Path) -> tuple[list[str], dict[str, date], str]:
    """股本类采集用：本地名录中 ``list_date`` 非空的标的。"""
    frame = read_stock_basic(store_root)
    rows = frame.filter(pl.col('list_date').is_not_null())
    codes = [str(code) for code in rows['ts_code'].to_list()]
    dates = {str(code): value for code, value in
             rows.select('ts_code', 'list_date').iter_rows() if value is not None}
    return codes, dates, 'local stock_basic(list_date 非空)'


def resolve_root(config: object) -> Path:
    """按配置解析列存根（与其它本地读取保持同一入口）。"""
    return resolve_store_root(config)
