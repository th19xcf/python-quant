"""新表驱动的涨跌停/可成交性规则（默认输入，保留 A 侧回退与开关）。

``ExecutionRules`` 的 A 侧按行情快照自算涨跌停；本模块把同一组
``is_tradable`` / ``block_reason`` 接口切到 ``meta/limit/daily``：

- 逐 ``(trade_date, ts_code)`` 查新表 ``can_buy`` / ``can_sell`` / ``suspended``；
- 表里没有该行（窗口外、代码不在表内、文件缺失）→ 回退 ``ExecutionRules``
  自算，行为与切换前一致；
- 开关：环境变量 ``QUANT_LIMIT_INPUT=self`` 强制走 A 侧；默认 ``table``。

月份文件按需加载并只缓存当前月（每月约 6 万股日），避免一次性把 16M 行
读进内存。规则只读表，不写回。
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from src.core.market_rules import ExecutionRules

_DEFAULT_STORE = Path(__file__).resolve().parents[3] / 'data' / 'local_store'


def limit_table_enabled() -> bool:
    """新表是否为默认输入（``QUANT_LIMIT_INPUT=self`` 时关闭）。"""
    return os.environ.get('QUANT_LIMIT_INPUT', 'table').strip().lower() != 'self'


class LimitTableRules(ExecutionRules):
    """A 接口、B 数据：优先取 ``meta/limit/daily``，缺失行回退自算。"""

    STATS: dict[str, int] = {'calls': 0, 'hit': 0, 'fallback': 0}

    def __init__(self, *args: Any, store_root: str | Path | None = None,
                 mode: str | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.store_root = Path(store_root) if store_root else _DEFAULT_STORE
        self.mode = (mode or os.environ.get('QUANT_LIMIT_INPUT', 'table')).lower()
        self._month = ''
        self._rows: dict[tuple[str, str], tuple[bool, bool, bool]] = {}

    def _load_month(self, month: str) -> None:
        if month == self._month:
            return
        self._month = month
        self._rows = {}
        path = self.store_root / 'meta' / 'limit' / 'daily' / f'limit_daily_{month}.parquet'
        if not path.exists():
            return
        frame = pl.read_parquet(
            path, columns=['trade_date', 'ts_code', 'can_buy', 'can_sell',
                           'suspended'])
        self._rows = {
            (str(d), c): (bool(cb), bool(cs), bool(su))
            for d, c, cb, cs, su in frame.iter_rows()
        }

    def _lookup(self, trade_date: Any, ts_code: str) -> tuple[bool, bool, bool] | None:
        if self.mode != 'table' or not trade_date or not ts_code:
            return None
        day = str(trade_date)[:10]
        self._load_month(day[:7].replace('-', ''))
        return self._rows.get((day, ts_code))

    def block_reason(self, side: str, *, prev_close=None, open_price=None,
                     high=None, low=None, close=None, volume=None,
                     ts_code: str = '', is_st=None, trade_date=None):
        self.STATS['calls'] += 1
        row = self._lookup(trade_date, ts_code)
        if row is None:
            self.STATS['fallback'] += 1
            return super().block_reason(
                side, prev_close=prev_close, open_price=open_price, high=high,
                low=low, close=close, volume=volume, ts_code=ts_code,
                is_st=is_st, trade_date=trade_date)
        self.STATS['hit'] += 1
        can_buy, can_sell, suspended = row
        if suspended:
            return 'suspended'
        can = can_buy if side == 'buy' else can_sell
        if can:
            return None
        return 'limit_up' if side == 'buy' else 'limit_down'

    def is_tradable(self, side: str, **kwargs) -> bool:
        return self.block_reason(side, **kwargs) is None
