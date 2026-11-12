"""跨数据源共享的串行限速 + 指数退避重试。

全项目只保留这一份实现：腾讯行情（``tencent/client.py``）、财务采集
（``financials/collector.py``）、跨源对拍（``quality/cross_check.py``）
原先各有一份独立拷贝，改一次频率要改三处，漏改一处就有源仍按旧频率
打请求、触发对端限速封禁的现实风险。

各处差异一律用显式参数表达，不在调用方重写逻辑：

- ``min_interval``：相邻两次请求之间的最小间隔（秒），全局限速而非
  每线程一份，才能真正确保对端看到的总 QPS 受控；
- ``max_retries`` / ``backoff_base`` / ``max_delay``：失败重试与指数退避；
- ``retry_on``：命中才重试的异常类型。腾讯只重试网络/解析类异常，
  对拍源对第三方 SDK 抛出的任意异常都重试（异常类型不可预知）；
- ``log_prefix``：重试日志前缀，保留各源原有的日志检索习惯。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, TypeVar

from loguru import logger

T = TypeVar('T')


class RateLimiter:
    """线程共享的最小请求间隔 + 可选指数退避重试。"""

    def __init__(self, min_interval: float, max_retries: int = 0,
                 backoff_base: float = 1.0, max_delay: float = 8.0, *,
                 retry_on: tuple[type[BaseException], ...] = (Exception,),
                 log_prefix: str = '远程源') -> None:
        if min_interval < 0:
            raise ValueError('min_interval 不能为负')
        self.min_interval = float(min_interval)
        self.max_retries = int(max_retries)
        self.backoff_base = float(backoff_base)
        self.max_delay = float(max_delay)
        self.retry_on = tuple(retry_on)
        self.log_prefix = log_prefix
        self._lock = threading.Lock()
        self._last_call = 0.0
        self.calls = 0
        self.retries = 0

    def _wait_turn(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.monotonic()
            self.calls += 1

    def wait(self) -> None:
        """只做间隔控制、不做重试的入口（财务采集各分节共用）。"""
        self._wait_turn()

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        last_error: BaseException | None = None
        for attempt in range(self.max_retries + 1):
            self._wait_turn()
            try:
                return func(*args, **kwargs)
            except self.retry_on as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                delay = min(self.max_delay, self.backoff_base * (2 ** attempt))
                self.retries += 1
                logger.warning(
                    f'{self.log_prefix}请求失败（第 {attempt + 1} 次），'
                    f'{delay:.1f}s 后重试: {exc}')
                time.sleep(delay)
        # 防御性分支：循环结束后理论上必有异常记录；用显式异常替代 assert，
        # 避免 python -O 下 assert 被剥离后 raise None 变成难以定位的错误。
        if last_error is None:  # pragma: no cover
            raise RuntimeError('重试结束但未记录到异常')
        raise last_error
