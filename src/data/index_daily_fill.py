"""index_daily 缺口补行逻辑（纯函数，供工具脚本与单测复用）。

口径：
- 只生成「TDX 有、库内没有」的日期行，既有键一律跳过，绝不改既有行；
- open/high/low/close 直接用 TDX 解析后的十进制原值（解析阶段已 /100）；
- vol 用 TDX 手值（指数记录原生就是手，不再折 100），amount 用 TDX 元值；
- pre_close = 同标的「TDX 上一交易日」close（用 TDX 序列定位，不看表内上一行，
  避免跨缺口错位）；序列首行没有前一日，pre_close/change/pct_chg 置空；
- change = close - pre_close，pct_chg = change / pre_close * 100（与项目现有
  口径一致：无舍入，pre_close 为 0 时取 0.0）。
"""

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, timedelta
from typing import Any


def contiguous_segments(days: Sequence[date]) -> list[tuple[date, date]]:
    """把升序日期序列切成连续片段（按自然日相邻判定）。"""
    if not days:
        return []
    segments = [(days[0], days[0])]
    for day in days[1:]:
        start, end = segments[-1]
        if day - end == timedelta(days=1):
            segments[-1] = (start, day)
        else:
            segments.append((day, day))
    return segments


def missing_trade_dates(tdx_dates: Iterable[date],
                        existing_dates: Iterable[date]) -> list[date]:
    """TDX 交易日与库内交易日的差集（升序）。"""
    return sorted(set(tdx_dates) - set(existing_dates))


def build_missing_rows(ts_code: str, records: Iterable[Mapping[str, Any]],
                       existing_dates: Iterable[date]) -> list[dict[str, Any]]:
    """按 TDX 记录序列生成缺失行。

    Args:
        ts_code: 指数代码
        records: TDX 解析结果，含 date/open/high/low/close/volume/amount
        existing_dates: 库内已有交易日（只跳过这些键）

    Returns:
        待插入行列表，按 trade_date 升序
    """
    existing = set(existing_dates)
    ordered = sorted(records, key=lambda item: item['date'])
    rows: list[dict[str, Any]] = []
    prev_close: float | None = None

    for record in ordered:
        trade_date = record['date']
        close = float(record['close'])
        if trade_date in existing:
            # 既有行不生成新行，但它仍是后续行的「上一交易日」，需推进昨收
            prev_close = close
            continue

        if prev_close is None:
            pre_close = change = pct_chg = None
        else:
            pre_close = prev_close
            change = close - pre_close
            pct_chg = (change / pre_close) * 100 if pre_close != 0 else 0.0

        amount = record['amount']
        rows.append({
            'ts_code': ts_code,
            'trade_date': trade_date,
            'open': float(record['open']),
            'high': float(record['high']),
            'low': float(record['low']),
            'close': close,
            'pre_close': pre_close,
            'change': change,
            'pct_chg': pct_chg,
            'vol': float(record['volume']),
            'amount': None if amount is None else float(amount),
        })
        prev_close = close

    return rows
