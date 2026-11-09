"""通达信网络行情源（日线增量取数）。

协议层复用 ``pytdx``（``mootdx`` 的底层客户端）；不直接用 ``mootdx`` 的
``Quotes.factory(bestip=True)``，因为它的选优只看 TCP 连通性，实测大量
「握手正常但 K 线返回空」的坏节点会被选中。本模块改为「真实 K 线探测」：
逐个候选服务器拉一根 600000 日线，非空才算可用。

口径（与本地 ``.day`` 解析保持一致）：
- 价格为元（协议本身按千分之一 bites 传输，pytdx 已还原）；
- ``vol`` 统一为「手」：指数协议原生为手，个股/基金协议为股，除以 100；
- ``amount`` 为元；
- 日期取交易日（15:00 收盘 K 线），返回按日期升序。

约束：单连接串行、请求间隔 >= ``throttle``（默认 0.35s）、失败重试上限 3 次
（退避 0.5/1.0/2.0s）。连续失败达到阈值后熔断，避免对坏节点持续加压。
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from loguru import logger

from src.utils.code_converter import is_index_code, parse_code

# 候选服务器（2026-11-09 实测：仅首个返回非空 K 线，其余为「握手正常但数据为空」
# 的坏节点，保留作为故障切换备选）。可用 TDX_ONLINE_SERVERS 环境变量覆盖，
# 格式 "ip:port,ip:port"。
_DEFAULT_SERVERS: tuple[tuple[str, int], ...] = (
    ('103.251.85.94', 7709),
    ('117.34.114.13', 7709),
    ('117.34.114.14', 7709),
    ('117.34.114.15', 7709),
    ('117.34.114.16', 7709),
    ('103.221.142.82', 7709),
)

_MARKET_ID = {'SH': 1, 'SZ': 0, 'BJ': 2}
# 日线周期：通达信协议 DAY=4（9 为年线）
_DAY_CATEGORY = 4
_PROBE_CODE = ('SH', '600000')
_MAX_BARS_PER_PAGE = 800
_CONSECUTIVE_FAILURE_LIMIT = 8


class TdxOnlineError(Exception):
    """网络源失败；``reason`` 用于回退统计与排查。"""

    def __init__(self, reason: str, message: str = '') -> None:
        super().__init__(message or reason)
        self.reason = reason


@dataclass
class OnlineQuote:
    """单根日线（价格元、成交量手、成交额元）。"""

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float

    def as_record(self) -> dict[str, Any]:
        return {
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
        }


@dataclass
class TdxOnlineClient:
    """通达信网络行情客户端（单连接串行 + 节流 + 重试 + 熔断）。"""

    servers: Sequence[tuple[str, int]] | None = None
    timeout: float = 8.0
    throttle: float = 0.35
    max_attempts: int = 3
    backoff_base: float = 0.5
    consecutive_failure_limit: int = _CONSECUTIVE_FAILURE_LIMIT

    _api: Any = field(default=None, init=False, repr=False)
    _server: tuple[str, int] | None = field(default=None, init=False)
    _last_request_at: float = field(default=0.0, init=False)
    _consecutive_failures: int = field(default=0, init=False)
    _disabled_reason: str = field(default='', init=False)

    # ---------- 连接与探测 ----------
    @property
    def disabled(self) -> bool:
        return bool(self._disabled_reason)

    @property
    def server(self) -> tuple[str, int] | None:
        return self._server

    def _candidates(self) -> list[tuple[str, int]]:
        if self.servers:
            return list(self.servers)
        env = os.environ.get('TDX_ONLINE_SERVERS', '').strip()
        if env:
            parsed: list[tuple[str, int]] = []
            for item in env.split(','):
                host, _, port = item.strip().partition(':')
                if host:
                    parsed.append((host, int(port or 7709)))
            if parsed:
                return parsed
        return list(_DEFAULT_SERVERS)

    def _connect(self, server: tuple[str, int]) -> Any:
        try:
            from pytdx.hq import TdxHq_API
        except ImportError as exc:  # 可选依赖未安装时回退本地
            raise TdxOnlineError('dependency_missing', '未安装 pytdx') from exc

        api = TdxHq_API(heartbeat=False, auto_retry=False, raise_exception=False)
        if not api.connect(server[0], server[1], time_out=self.timeout):
            raise TdxOnlineError('connect_failed', f'无法连接 {server[0]}:{server[1]}')
        # connect 的 time_out 只作用于建连，读取需单独设置，避免被坏节点挂死
        api.client.settimeout(self.timeout)
        return api

    def _ensure_ready(self) -> Any:
        """返回可用连接；首次调用做真实 K 线探测。"""
        if self._disabled_reason:
            raise TdxOnlineError(self._disabled_reason)
        if self._api is not None:
            return self._api
        last_error = 'no_server'
        for server in self._candidates():
            try:
                api = self._connect(server)
            except TdxOnlineError as exc:
                last_error = exc.reason
                logger.warning(f'网络行情源连接失败 {server[0]}:{server[1]}：{exc}')
                continue
            try:
                self._throttle()
                bars = api.get_security_bars(
                    _DAY_CATEGORY, _MARKET_ID[_PROBE_CODE[0]], _PROBE_CODE[1], 0, 2)
            except Exception as exc:  # noqa: BLE001
                last_error = _classify_error(exc)
                api.disconnect()
                logger.warning(f'网络行情源探测失败 {server[0]}:{server[1]}：{exc}')
                continue
            if not bars:
                last_error = 'empty'
                api.disconnect()
                logger.warning(f'网络行情源无数据（坏节点）{server[0]}:{server[1]}，跳过')
                continue
            self._api = api
            self._server = server
            logger.info(
                f'网络行情源已就绪：{server[0]}:{server[1]}（探测 {len(bars)} 根日线）')
            return api
        self._disabled_reason = last_error
        raise TdxOnlineError(last_error, '所有候选服务器均不可用')

    def close(self) -> None:
        if self._api is not None:
            try:
                self._api.disconnect()
            except Exception as exc:  # noqa: BLE001
                logger.debug(f'关闭网络行情连接时忽略异常：{exc}')
            self._api = None

    # ---------- 节流 / 重试 ----------
    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.throttle:
            time.sleep(self.throttle - elapsed)

    def _call(self, fn: Any, *args: Any) -> Any:
        """带节流与重试的单次协议调用；返回 None/空列表视为失败。"""
        if self._disabled_reason:
            raise TdxOnlineError(self._disabled_reason)
        last_error: str = 'protocol'
        for attempt in range(1, self.max_attempts + 1):
            try:
                api = self._ensure_ready()
                self._throttle()
                result = fn(api, *args)
            except TdxOnlineError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = _classify_error(exc)
                logger.warning(f'网络行情源请求异常（第 {attempt} 次）：{exc}')
                self._reconnect()
                result = None
            finally:
                self._last_request_at = time.monotonic()

            if result:
                self._consecutive_failures = 0
                return result
            last_error = last_error or 'empty'
            if attempt < self.max_attempts:
                time.sleep(self.backoff_base * (2 ** (attempt - 1)))
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.consecutive_failure_limit:
            self._disabled_reason = 'circuit_open'
            logger.error(
                f'网络行情源连续失败 {self._consecutive_failures} 次，熔断并回退本地')
        raise TdxOnlineError(last_error, '网络源返回空或请求失败')

    def _reconnect(self) -> None:
        self.close()
        self._server = None

    # ---------- 对外接口 ----------
    def fetch_daily(self, ts_code: str, count: int = 20) -> list[OnlineQuote]:
        """取最近 ``count`` 根日线（升序）；空结果抛 ``TdxOnlineError('empty')``。"""
        symbol, market = parse_code(ts_code)
        market_id = _MARKET_ID.get(market.upper())
        if market_id is None:
            raise TdxOnlineError('unsupported_market', f'不支持的市场：{ts_code}')
        count = max(1, min(int(count), _MAX_BARS_PER_PAGE))

        if is_index_code(ts_code):
            def call(api: Any, *args: Any) -> Any:
                return api.get_index_bars(_DAY_CATEGORY, market_id, symbol, 0, count)
        else:
            def call(api: Any, *args: Any) -> Any:
                return api.get_security_bars(_DAY_CATEGORY, market_id, symbol, 0, count)

        bars = self._call(call)
        quotes: list[OnlineQuote] = []
        index_like = is_index_code(ts_code)
        for bar in bars:
            try:
                day = date(int(bar['year']), int(bar['month']), int(bar['day']))
            except (KeyError, TypeError, ValueError):
                continue
            vol = float(bar['vol'])
            quotes.append(OnlineQuote(
                date=day,
                open=float(bar['open']),
                high=float(bar['high']),
                low=float(bar['low']),
                close=float(bar['close']),
                volume=vol if index_like else vol / 100.0,
                amount=float(bar['amount']),
            ))
        if not quotes:
            raise TdxOnlineError('empty', f'{ts_code} 网络源无日线')
        quotes.sort(key=lambda item: item.date)
        return quotes

    def fetch_xdxr(self, ts_code: str) -> list[dict[str, Any]]:
        """取除权除息信息；仅用于核验，不参与 stock_daily 写入。"""
        symbol, market = parse_code(ts_code)
        market_id = _MARKET_ID.get(market.upper())
        if market_id is None:
            raise TdxOnlineError('unsupported_market', f'不支持的市场：{ts_code}')
        rows = self._call(
            lambda api, *_: api.get_xdxr_info(market_id, symbol))
        return list(rows or [])


def _classify_error(exc: BaseException) -> str:
    text = f'{type(exc).__name__}: {exc}'.lower()
    if isinstance(exc, (TimeoutError, OSError)) or 'timeout' in text:
        return 'timeout'
    if 'reset' in text or 'disconnect' in text or 'broken pipe' in text:
        return 'kicked'
    return 'protocol'
