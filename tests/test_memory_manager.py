#!/usr/bin/env python3

"""
MemoryManager 回归测试

重点锁定 R8 修复的两个行为：
1. 内存趋势预测超过 high 阈值时只触发一次 GC，不得与 _take_preventive_measures 互递归
2. 缓解措施不再调用已删除的空实现 _clean_expired_cache（也不再有"已清理过期缓存"假日志）
"""

import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

import src.utils.memory_manager as memory_manager_module
from src.utils.memory_manager import MemoryManager


def _make_rising_history(manager: MemoryManager) -> None:
    """构造 5 条持续上涨的内存历史，使线性预测结果超过 high(80%)"""
    now = time.time()
    manager.memory_history = [
        {
            "timestamp": now - 400 + i * 100,
            "memory_usage": {"process_percent": 60 + i * 6},
        }
        for i in range(5)
    ]


def test_predict_memory_usage_does_not_recurse():
    """预测超过 high 时只回收一次垃圾，不回调 _take_preventive_measures"""
    manager = MemoryManager()
    _make_rising_history(manager)

    calls = []
    manager._collect_garbage = lambda force=False: calls.append(force)

    manager._predict_memory_usage()

    assert calls == [False], f"应只触发一次非强制 GC，实际={calls}"


def test_preventive_measures_terminates():
    """预防措施（GC + 预测）能正常返回，不会因互递归抛出 RecursionError"""
    manager = MemoryManager()
    _make_rising_history(manager)

    calls = []
    manager._collect_garbage = lambda force=False: calls.append(force)

    manager._take_preventive_measures()

    # 预防措施自身的 GC + 预测触发的 GC
    assert calls == [False, False], f"应触发两次非强制 GC，实际={calls}"


def _diff(traceback_text, size_diff):
    """构造 compare_to 差异项替身（只需 traceback 与 size_diff）。"""
    return SimpleNamespace(traceback=traceback_text, size_diff=size_diff, count_diff=1)


def test_growth_confirmed_only_after_three_consecutive_snapshots():
    """同一位置连续 3 次快照增长且累计超阈值，第 3 次才判定为泄漏候选"""
    manager = MemoryManager()
    diff = _diff('renderer.py:52', 600_000)

    assert manager._classify_growth([diff])[0] == []
    assert manager._classify_growth([diff])[0] == []
    confirmed, single = manager._classify_growth([diff])

    assert confirmed == [diff]
    assert single == []


def test_three_growths_below_total_threshold_not_confirmed():
    """连续 3 次增长但累计不足 1MB，不得判定为泄漏"""
    manager = MemoryManager()
    diff = _diff('renderer.py:52', 100_000)

    for _ in range(3):
        confirmed, _single = manager._classify_growth([diff])

    assert confirmed == []


def test_growth_streak_resets_after_gap():
    """中间一次快照未增长，连续计数必须清零"""
    manager = MemoryManager()
    growing = _diff('renderer.py:52', 600_000)

    manager._classify_growth([growing])
    manager._classify_growth([growing])
    manager._classify_growth([_diff('other.py:7', 10_000)])  # renderer 中断
    confirmed, single = manager._classify_growth([growing])

    assert confirmed == []
    assert single == [growing]


def test_take_snapshot_collects_garbage_before_snapshot(monkeypatch):
    """拍快照前必须先走带冷却的 GC，避免把瞬时对象算成泄漏"""
    manager = MemoryManager()
    calls = []
    manager._collect_garbage = lambda force=False: calls.append(force)
    manager._trim_linecache_cache = lambda: None
    manager._analyze_memory_snapshot = lambda snapshot: None
    monkeypatch.setattr(
        memory_manager_module.tracemalloc, 'take_snapshot', lambda: SimpleNamespace()
    )

    manager._take_memory_snapshot()

    assert calls == [False]
    assert len(manager.memory_snapshots) == 1
