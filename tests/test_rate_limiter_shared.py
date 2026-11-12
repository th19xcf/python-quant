"""限流器抽成单份实现后的等价守卫。

三处调用方（腾讯行情 / 财务采集 / 跨源对拍）原先各有一份独立拷贝，本文件
锁定两件事：

1. 抽取后仍是**同一份**实现，不再允许某个源偷偷保留自己的拷贝；
2. 抽取后的实际调用间隔与抽取前**逐次相等**。基线列表由抽取前的三份旧实现
   用虚拟时钟实测得到，不是按新实现反推，因此实现一旦漂移就会红。
"""

from __future__ import annotations

import time

import pytest

from src.data import rate_limit
from src.data.quality import cross_check
from src.data.tencent import client as tencent_client


class _Clock:
    """虚拟时钟：sleep 推进时间，便于逐次记录睡眠时长。"""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 9))
        self.now += seconds


@pytest.fixture()
def clock(monkeypatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(time, 'sleep', fake.sleep)
    monkeypatch.setattr(time, 'monotonic', fake.monotonic)
    return fake


def test_only_one_implementation_is_shared() -> None:
    """抽取目的就是「改频率只改一处」，任何模块不得再自带实现。"""
    assert cross_check.RateLimiter is rate_limit.RateLimiter
    assert tencent_client.RateLimiter is rate_limit.RateLimiter


def test_tencent_interval_and_backoff_unchanged(clock: _Clock) -> None:
    """腾讯口径：min_interval=0.05、退避 1.0/2.0，两次失败后成功。"""
    limiter = tencent_client.TencentClient(min_interval=0.05, max_retries=2).limiter
    attempts = {'n': 0}

    def flaky() -> str:
        attempts['n'] += 1
        if attempts['n'] <= 2:
            raise tencent_client.TencentError('boom')
        return 'ok'

    limiter.call(lambda: 'ok')
    limiter.call(flaky)
    limiter.call(lambda: 'ok')
    assert clock.sleeps == [0.05, 1.0, 2.0, 0.05]


def test_tencent_only_retries_network_and_parse_errors(clock: _Clock) -> None:
    """腾讯原实现只重试网络/解析类异常，抽出来后不能放大成「任意异常都重试」。"""
    limiter = tencent_client.TencentClient(min_interval=0.0, max_retries=3).limiter
    attempts = {'n': 0}

    def boom() -> str:
        attempts['n'] += 1
        raise ValueError('不是网络错误')

    with pytest.raises(ValueError):
        limiter.call(boom)
    assert attempts['n'] == 1, '非重试类异常必须一次即抛，不得退避重试'


def test_cross_check_interval_and_backoff_unchanged(clock: _Clock) -> None:
    """对拍口径：akshare 最小间隔 3 秒、退避基数 5 秒（东财高频会临时封 IP）。"""
    limiter = cross_check.RateLimiter(*cross_check.RATE_LIMITS['akshare'])
    state = {'n': 0}

    def fail_once() -> str:
        state['n'] += 1
        if state['n'] == 1:
            raise RuntimeError('temporary')
        return 'ok'

    limiter.call(lambda: 'ok')
    limiter.call(fail_once)
    assert clock.sleeps == [3.0, 5.0]


def test_cross_check_retries_arbitrary_third_party_errors(clock: _Clock) -> None:
    """对拍源沿用旧实现的盲捕口径：第三方 SDK 异常类型不可预知，一律重试。"""
    limiter = cross_check.RateLimiter(0.0, 1, 0.0, 0.0)
    state = {'n': 0}

    def flaky() -> str:
        state['n'] += 1
        if state['n'] == 1:
            raise KeyError('第三方 SDK 抛出任意异常')
        return 'ok'

    assert limiter.call(flaky) == 'ok'
    assert state['n'] == 2


def test_financials_intervals_unchanged(clock: _Clock) -> None:
    """财务采集口径：估值 0.15 秒、接口 0.25 秒，两个节流器交替调用。"""
    valuation = rate_limit.RateLimiter(0.15)
    request = rate_limit.RateLimiter(0.25)
    for _ in range(3):
        valuation.wait()
        request.wait()
    assert clock.sleeps == [0.15, 0.1, 0.05, 0.2]


def test_baseline_assertion_is_not_vacuous(clock: _Clock) -> None:
    """反例自证：把间隔改一点点，上面的逐次相等断言就必须失败。"""
    drifted = rate_limit.RateLimiter(0.06)
    drifted.wait()
    drifted.wait()
    assert clock.sleeps == [0.06]
    assert clock.sleeps != [0.05], '间隔漂移时基线断言必须能红'
