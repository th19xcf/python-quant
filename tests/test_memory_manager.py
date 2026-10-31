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

sys.path.insert(0, str(Path(__file__).parent.parent))

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
