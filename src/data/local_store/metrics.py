#!/usr/bin/env python3
"""取数路径打点（命中本地 / 回退 TDX / 回退远程）与耗时统计。

进程内计数器，线程安全；用于运行期观测取数是否退化。可通过
:func:`snapshot` 读取当前累计值与最近一次耗时，无需引入外部监控依赖。
"""

import threading
from typing import Any, Dict

_LOCK = threading.RLock()
_COUNTERS: Dict[str, int] = {}
_LAST_MS: Dict[str, float] = {}
_LAST_OUTCOME: Dict[str, str] = {}

ROUTE_LOCAL = 'local_store'
ROUTE_TDX = 'tdx'
ROUTE_DB = 'remote_db'
ROUTE_SOURCE = 'remote_source'
ROUTE_CACHE = 'cache'


def record(route: str, *, hit: bool, elapsed_ms: float) -> None:
    """记录一次取数路径结果与耗时。"""
    key = f'{route}:{"hit" if hit else "miss"}'
    with _LOCK:
        _COUNTERS[key] = _COUNTERS.get(key, 0) + 1
        _LAST_MS[route] = round(float(elapsed_ms), 3)
        _LAST_OUTCOME[route] = 'hit' if hit else 'miss'


def snapshot() -> Dict[str, Any]:
    """返回累计计数、最近耗时与最近结果。"""
    with _LOCK:
        return {
            'counters': dict(_COUNTERS),
            'last_ms': dict(_LAST_MS),
            'last_outcome': dict(_LAST_OUTCOME),
        }


def reset() -> None:
    """清空统计（测试用）。"""
    with _LOCK:
        _COUNTERS.clear()
        _LAST_MS.clear()
        _LAST_OUTCOME.clear()
