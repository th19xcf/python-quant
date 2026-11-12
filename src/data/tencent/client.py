#!/usr/bin/env python3

"""腾讯行情 HTTP 客户端（离线补数 / 对拍专用）。

定位：**只允许由显式 CLI 调用**，拿到的数据先写入本地列存，再由现有读路径
读取。本模块不得被 ``DataFetcher`` 运行时回退链引用，也不读取
``data.allow_remote_sources``（开关与它完全分离，默认不在任何运行时路径出现）。

接口实测结论（2026-11-03）：

- 日线：``proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get``
  的 ``day`` 字段返回 **不复权原始价**（与本地 TDX 原始价逐行一致），
  行结构为 11 列::

      [date, open, close, high, low, volume, {}, turnover, amount_wan, 0, 0]

  ``volume`` 单位为手；``amount_wan`` 单位为万元，本模块统一换算为「元」。
  该接口可直接替代 ``web.ifzq.gtimg.cn/.../fqkline/get``：后者只给 6 列、
  没有成交额，且默认 ``qfq`` 口径会污染「原始未复权价」契约。

- 分钟：``https://ifzq.gtimg.cn/appstock/app/kline/mkline``，必须带
  ``Referer: https://gu.qq.com/``（用 ``web.ifzq`` 域名会返回 HTML 301）。
  每档最多 320 根，行结构为 8 列::

      [timestamp, open, close, high, low, volume, {}, ?]

  第 7 列为空对象 → **没有成交额**；``volume`` 经与日线全天加总对拍确认单位为手。
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any

import polars as pl
from loguru import logger

from src.data.rate_limit import RateLimiter
from src.utils.code_converter import to_tdx_file_stem

_DAILY_URL = (
    'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get'
    '?param={symbol},day,,,{count},'
)
_MINUTE_URL = (
    'https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={symbol},{freq},,{count}'
)
_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'
    ),
    'Referer': 'https://gu.qq.com/',
    'Accept': 'application/json, text/plain, */*',
}

DAILY_COLUMNS = ('date', 'open', 'high', 'low', 'close', 'volume', 'amount')
MINUTE_COLUMNS = ('datetime', 'open', 'high', 'low', 'close', 'volume', 'amount')
MINUTE_FREQUENCIES = ('m1', 'm5', 'm15', 'm30', 'm60')
#: 腾讯分钟接口的硬限制：每档单次最多返回 320 根
MAX_MINUTE_BARS = 320
#: 每分钟 K 线的每日根数，用于估算可回溯天数（外推，仅供 CLI 提示）
BARS_PER_DAY = {'m1': 240, 'm5': 48, 'm15': 16, 'm30': 8, 'm60': 4}
#: 用 ``amount / (close * volume)`` 判定成交量单位：≈1 为股，≈100 为手
_VOLUME_RATIO_SHARE_MAX = 5.0
_VOLUME_RATIO_HAND_MIN = 20.0
#: 实测只有科创板（688/689）日线成交量以「股」返回，其余（含北交所）为「手」；
#: 指数成交额口径不同（比值远小于 1），用前缀兜底避免误判指数
_STAR_PREFIXES = ('688', '689')

_DAILY_SCHEMA = {
    'date': pl.Date, 'open': pl.Float64, 'high': pl.Float64, 'low': pl.Float64,
    'close': pl.Float64, 'volume': pl.Float64, 'amount': pl.Float64,
}
_MINUTE_SCHEMA = {
    'datetime': pl.Datetime, 'open': pl.Float64, 'high': pl.Float64,
    'low': pl.Float64, 'close': pl.Float64, 'volume': pl.Float64, 'amount': pl.Float64,
}


class TencentError(RuntimeError):
    """腾讯接口返回异常（HTTP / JSON / 业务码错误）。"""


def to_tencent_symbol(ts_code: str) -> str:
    """标准 ts_code → 腾讯符号（与通达信文件名主体一致：sh600000/sz000001/bj430047）。"""
    return to_tdx_file_stem(ts_code)


def _to_float(value: Any) -> float | None:
    """把接口里的数字/空对象/空串统一成 float；不可解析返回 None。"""
    if value is None or value == '' or isinstance(value, dict):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tencent_limiter(min_interval: float, max_retries: int) -> RateLimiter:
    """腾讯客户端的限速口径：只重试网络/解析类异常，日志前缀保留 ``[tencent]``。"""
    return RateLimiter(
        min_interval=min_interval, max_retries=max_retries,
        retry_on=(urllib.error.URLError, TimeoutError, json.JSONDecodeError, TencentError),
        log_prefix='[tencent] ',
    )


class TencentClient:
    """腾讯行情只读客户端（日线 / 分钟）。"""

    def __init__(self, *, timeout: float = 20.0, min_interval: float = 0.05,
                 max_retries: int = 3, headers: dict[str, str] | None = None) -> None:
        self.timeout = float(timeout)
        self.headers = dict(_HEADERS if headers is None else headers)
        self.limiter = _tencent_limiter(min_interval, max_retries)
        self._state_lock = threading.Lock()
        self._consecutive_failures = 0

    # ------------------------------------------------------------------
    # 底层请求
    # ------------------------------------------------------------------
    def _raw_get(self, url: str) -> dict[str, Any]:
        request = urllib.request.Request(url, headers=self.headers)  # noqa: S310
        with urllib.request.urlopen(  # noqa: S310 - 固定 HTTPS 域名，非用户输入 URL
                request, timeout=self.timeout) as response:
            body = response.read().decode('utf-8', errors='ignore')
        if body.lstrip().startswith('<'):
            raise TencentError('接口返回 HTML（可能被重定向/风控），请检查 Referer')
        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise TencentError(f'响应不是 JSON: {body[:120]!r}') from exc
        if not isinstance(payload, dict):
            raise TencentError(f'响应结构异常: {type(payload).__name__}')
        return payload

    def _get(self, url: str) -> dict[str, Any]:
        try:
            payload = self.limiter.call(self._raw_get, url)
        except Exception:
            with self._state_lock:
                self._consecutive_failures += 1
            raise
        with self._state_lock:
            self._consecutive_failures = 0
        return payload

    def consecutive_failures(self) -> int:
        with self._state_lock:
            return self._consecutive_failures

    # ------------------------------------------------------------------
    # 日线（不复权原始价 + 成交额）
    # ------------------------------------------------------------------
    def fetch_daily(self, ts_code: str, count: int = 800) -> pl.DataFrame:
        """拉取单标的日线原始价（按 date 升序）。

        Args:
            ts_code: 标准代码，如 ``600000.SH`` / ``000001.SH``（指数）/ ``510050.SH``
            count: 请求的最大行数（腾讯会按实际历史截断）

        Returns:
            date/open/high/low/close/volume/amount 七列；amount 单位「元」，
            上游缺省时为空。接口无该标的时返回空表。
        """
        symbol = to_tencent_symbol(ts_code)
        payload = self._get(_DAILY_URL.format(symbol=symbol, count=max(1, int(count))))
        if payload.get('code') not in (0, '0'):
            raise TencentError(
                f'日线接口业务错误 code={payload.get("code")} msg={payload.get("msg")}')
        data = payload.get('data')
        if not isinstance(data, dict):
            return pl.DataFrame(schema=_DAILY_SCHEMA)
        node = data.get(symbol)
        if not isinstance(node, dict):
            return pl.DataFrame(schema=_DAILY_SCHEMA)
        rows = node.get('day')
        if not isinstance(rows, list) or not rows:
            return pl.DataFrame(schema=_DAILY_SCHEMA)
        return normalize_daily_volume(parse_daily_rows(rows), ts_code)

    # ------------------------------------------------------------------
    # 分钟（320 根硬限制，无成交额）
    # ------------------------------------------------------------------
    def fetch_minute(self, ts_code: str, freq: str = 'm1',
                     count: int = MAX_MINUTE_BARS) -> pl.DataFrame:
        """拉取单标的分时 K 线（时间升序）。

        注意：``count`` 会被截断到 320（腾讯硬限制）；返回行数超过 320 时
        只能拿到最近 320 根，无法通过翻页向更早回溯。
        """
        if freq not in MINUTE_FREQUENCIES:
            raise ValueError(f'不支持的分钟频率: {freq!r}（可选 {MINUTE_FREQUENCIES}）')
        symbol = to_tencent_symbol(ts_code)
        count = max(1, min(int(count), MAX_MINUTE_BARS))
        payload = self._get(_MINUTE_URL.format(symbol=symbol, freq=freq, count=count))
        if payload.get('code') not in (0, '0'):
            raise TencentError(
                f'分钟接口业务错误 code={payload.get("code")} msg={payload.get("msg")}')
        data = payload.get('data')
        if not isinstance(data, dict):
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        node = data.get(symbol)
        if not isinstance(node, dict):
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        rows = node.get(freq)
        if not isinstance(rows, list) or not rows:
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        return normalize_minute_volume(parse_minute_rows(rows), ts_code)


# ----------------------------------------------------------------------
# 纯解析函数（便于单测，不触发网络）
# ----------------------------------------------------------------------
def parse_daily_rows(rows: list[Any]) -> pl.DataFrame:
    """把 ``newfqkline`` 的 ``day`` 行列表解析成标准日线表。

    仅有 6 列的历史行（旧接口惯例）amount 记为 None，绝不反推补齐。
    """
    dates: list[str] = []
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []
    amounts: list[float | None] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        parsed = [_to_float(row[index]) for index in (1, 2, 3, 4, 5)]
        if any(value is None for value in parsed):
            continue
        date_text = str(row[0])
        amount_wan = _to_float(row[8]) if len(row) > 8 else None
        dates.append(date_text)
        opens.append(parsed[0])  # type: ignore[arg-type]
        closes.append(parsed[1])  # type: ignore[arg-type]
        highs.append(parsed[2])  # type: ignore[arg-type]
        lows.append(parsed[3])  # type: ignore[arg-type]
        volumes.append(parsed[4])  # type: ignore[arg-type]
        amounts.append(None if amount_wan is None else amount_wan * 10000.0)
    if not dates:
        return pl.DataFrame(schema=_DAILY_SCHEMA)
    frame = pl.DataFrame({
        'date': dates,
        'open': opens,
        'high': highs,
        'low': lows,
        'close': closes,
        'volume': volumes,
        'amount': amounts,
    }, schema_overrides={**_DAILY_SCHEMA, 'date': pl.Utf8})
    frame = frame.with_columns(
        pl.col('date').str.strptime(pl.Date, format='%Y-%m-%d', strict=False))
    frame = frame.filter(pl.col('date').is_not_null())
    frame = frame.unique(subset=['date'], keep='last').sort('date')
    return frame.select(list(_DAILY_SCHEMA))


def normalize_daily_volume(frame: pl.DataFrame, ts_code: str = '') -> pl.DataFrame:
    """按成交额反推日线成交量单位，统一换算为「手」。

    腾讯日线对科创板（688/689）返回成交量单位为**股**，其他板块（含北交所）
    为**手**。用 ``amount / (close * volume)`` 的中位数判定口径：≈1 为股，
    ≈100 为手，两者都不是（如指数的成交额口径）保持原值不动。

    实测证据（2026-11-03）：600000/000001/300750 比值≈100，688190/688001/
    688981/689009 比值≈1.00，430047/832000/920001 比值≈100。
    """
    if frame.is_empty() or ts_code[:3] not in _STAR_PREFIXES:
        return frame
    valid = frame.filter(
        (pl.col('volume') > 0) & (pl.col('amount') > 0) & (pl.col('close') > 0))
    if valid.is_empty():
        return frame
    ratio = float(valid.select(
        (pl.col('amount') / (pl.col('close') * pl.col('volume')))
        .median()).item() or 0.0)
    if 0.0 < ratio <= _VOLUME_RATIO_SHARE_MAX:
        logger.info(
            f'[tencent] {ts_code} 日线成交量单位为股（ratio={ratio:.3f}），'
            '按 100 股/手换算')
        return frame.with_columns((pl.col('volume') / 100.0).alias('volume'))
    if ratio < _VOLUME_RATIO_HAND_MIN:
        logger.warning(
            f'[tencent] {ts_code} 成交量单位无法判定（ratio={ratio:.3f}），保持原值')
    return frame


def normalize_minute_volume(frame: pl.DataFrame, ts_code: str = '') -> pl.DataFrame:
    """把分钟线成交量统一换算为「手」。

    分钟接口**没有成交额**，无法用 ``amount/(close*volume)`` 反推口径，只能
    按板块约定处理。实测（2026-11-03）：600000.SH / 000001.SZ / 300750.SZ 的
    m5 全天成交量加总与日线成交量一致（手），688190.SH 的 m5 加总恰为日线的
    100 倍（股）。因此科创板一律按 100 股/手换算。
    """
    if frame.is_empty() or ts_code[:3] not in _STAR_PREFIXES:
        return frame
    logger.info(f'[tencent] {ts_code} 分钟成交量单位为股，按 100 股/手换算')
    return frame.with_columns((pl.col('volume') / 100.0).alias('volume'))


def parse_minute_rows(rows: list[Any]) -> pl.DataFrame:
    """把 ``mkline`` 行列表解析成分钟表；第 7 列为空对象 → amount 恒为 null。"""
    stamps: list[str] = []
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        parsed = [_to_float(row[index]) for index in (1, 2, 3, 4, 5)]
        if any(value is None for value in parsed):
            continue
        stamps.append(str(row[0]))
        opens.append(parsed[0])  # type: ignore[arg-type]
        closes.append(parsed[1])  # type: ignore[arg-type]
        highs.append(parsed[2])  # type: ignore[arg-type]
        lows.append(parsed[3])  # type: ignore[arg-type]
        volumes.append(parsed[4])  # type: ignore[arg-type]
    if not stamps:
        return pl.DataFrame(schema=_MINUTE_SCHEMA)
    frame = pl.DataFrame({
        'datetime': stamps, 'open': opens, 'high': highs, 'low': lows,
        'close': closes, 'volume': volumes,
        'amount': [None] * len(stamps),
    }, schema_overrides={**_MINUTE_SCHEMA, 'datetime': pl.Utf8})
    frame = frame.with_columns(
        pl.col('datetime').str.strptime(pl.Datetime, format='%Y%m%d%H%M', strict=False))
    frame = frame.filter(pl.col('datetime').is_not_null())
    frame = frame.unique(subset=['datetime'], keep='last').sort('datetime')
    return frame.select(list(_MINUTE_SCHEMA))
