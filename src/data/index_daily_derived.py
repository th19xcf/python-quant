"""index_daily 派生字段（pre_close / change / pct_chg）重算逻辑。

口径（与 tdx_day_parser 的单日快照一致）：
    pre_close = 同标的「上一交易日」的 close
    change    = close - pre_close
    pct_chg   = change / pre_close * 100（pre_close 为 0 时取 0.0）

「上一交易日」必须由调用方提供权威交易日历（本项目为同一标的的通达信
.day 记录序列），不能取「index_daily 表内的上一行」：该表历史上存在整段
缺失的交易日（如 000001.SH 的 2023 年与 2024-02），用表内上一行会把缺口
之前某天的 close 当作昨收，形成跨日错位。缺少上一交易日收盘价的行保持
原值，不凭空造数。
"""

from collections.abc import Callable, Iterable, Mapping
from datetime import date
from typing import Any

# (ts_code, trade_date) -> 同标的上一交易日 close；无上一交易日返回 None
PrevCloseProvider = Callable[[str, date], float | None]


def recalc_derived_rows(
        rows: Iterable[Mapping[str, Any]],
        prev_close_of: PrevCloseProvider) -> list[dict[str, Any]]:
    """按权威昨收重算派生字段。

    Args:
        rows: 每行至少包含 id、ts_code、trade_date、close
        prev_close_of: 返回同标的上一交易日 close 的函数

    Returns:
        需要写回的 [{id, pre_close, change, pct_chg}]；
        无上一交易日或 close 缺失的行不返回
    """
    updates: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: (item['ts_code'], item['trade_date'])):
        close = row['close']
        if close is None:
            continue
        prev_close = prev_close_of(row['ts_code'], row['trade_date'])
        if prev_close is None:
            continue
        change = float(close) - prev_close
        pct_chg = (change / prev_close) * 100 if prev_close != 0 else 0.0
        updates.append({
            'id': row['id'],
            'pre_close': prev_close,
            'change': change,
            'pct_chg': pct_chg,
        })
    return updates
