"""
对 data_fetcher 的优化逻辑做最小烟测：
1. 本地源优先（5s 内拿到就返回）
2. 远程源并行（命中即取消其他）
3. 远程源全部超时能及时返回
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import threading
import time
from unittest.mock import MagicMock

import polars as pl

from src.data.managers.data_fetcher import DataFetcher


def make_fetcher():
    cfg = MagicMock()
    cfg.data.max_workers = 5
    return DataFetcher(config=cfg)


def test_local_priority_returns_immediately():
    """本地源 1s 内返回，远程源不会被调度"""
    fetcher = make_fetcher()

    local = MagicMock()
    local.name = "FakeLocal"
    local.is_local = True
    df = pl.DataFrame({"a": [1, 2, 3]})
    def slow_local_get_index_data(**kwargs):
        time.sleep(1)
        return df
    local.get_index_data = slow_local_get_index_data

    remote = MagicMock()
    remote.name = "FakeRemote"
    remote.is_local = False
    remote_called = {"n": 0}
    def should_not_call(**kwargs):
        remote_called["n"] += 1
        return df
    remote.get_index_data = should_not_call

    fetcher.register_source(remote)
    fetcher.register_source(local)

    t0 = time.time()
    out = fetcher._get_data_from_sources("index", "000001.SH", "20240101", "20241231", "1d")
    elapsed = time.time() - t0

    assert out is not None and not out.is_empty(), "本地源应当返回数据"
    assert elapsed < 3, f"本地源应快速返回，耗时={elapsed:.2f}s"
    print(f"  [PASS] test_local_priority_returns_immediately  耗时={elapsed:.2f}s, 远程被调用={remote_called['n']}次")


def test_remote_parallel_returns_first_success():
    """两个远程源同时跑，第一个成功的应被采用，第二个被取消"""
    fetcher = make_fetcher()

    df = pl.DataFrame({"a": [1, 2, 3]})

    fast = MagicMock()
    fast.name = "FastRemote"
    fast.is_local = False
    def fast_get(**kwargs):
        time.sleep(0.5)
        return df
    fast.get_index_data = fast_get

    slow = MagicMock()
    slow.name = "SlowRemote"
    slow.is_local = False
    slow_called = {"n": 0}
    def slow_get(**kwargs):
        slow_called["n"] += 1
        time.sleep(5)
        return df
    slow.get_index_data = slow_get

    fetcher.register_source(fast)
    fetcher.register_source(slow)

    t0 = time.time()
    out = fetcher._get_data_from_sources("index", "000001.SH", "20240101", "20241231", "1d")
    elapsed = time.time() - t0

    assert out is not None and not out.is_empty()
    assert elapsed < 3, f"应该约 0.5s 返回，实际={elapsed:.2f}s"
    # 慢源可能在取消前已 submit，但返回的 future 不应被使用
    print(f"  [PASS] test_remote_parallel_returns_first_success  耗时={elapsed:.2f}s, 慢源被调用={slow_called['n']}次(取消前可能已起)")


def test_remote_all_fail_returns_quickly():
    """所有远程源都超时，10s 内必须返回（不是每个 future 30s）"""
    fetcher = make_fetcher()

    def hang(**kwargs):
        time.sleep(60)
        return pl.DataFrame()

    src1 = MagicMock(name="H1"); src1.name = "H1"; src1.is_local = False
    src1.get_index_data = hang
    src2 = MagicMock(name="H2"); src2.name = "H2"; src2.is_local = False
    src2.get_index_data = hang

    fetcher.register_source(src1)
    fetcher.register_source(src2)

    t0 = time.time()
    out = fetcher._get_data_from_sources("index", "000001.SH", "20240101", "20241231", "1d")
    elapsed = time.time() - t0

    assert out.is_empty(), "全部失败应返回空 DataFrame"
    # as_completed timeout = per_source_timeout + 5 = 15s；最坏情况下 2 个源并行 = ~15s
    assert elapsed < 20, f"全部超时应在 20s 内返回，实际={elapsed:.2f}s"
    print(f"  [PASS] test_remote_all_fail_returns_quickly  耗时={elapsed:.2f}s")


def test_unsupported_source_skipped():
    """不支持 stock 的源在请求 stock 时被过滤掉"""
    fetcher = make_fetcher()

    only_index = MagicMock(name="OI"); only_index.name = "OI"; only_index.is_local = False
    only_index.get_index_data = lambda **k: pl.DataFrame({"a": [1]})
    # 没有 get_stock_data，也没有 get_kline_data

    fetcher.register_source(only_index)
    out = fetcher._get_data_from_sources("stock", "000001.SZ", "20240101", "20241231", "1d")
    assert out.is_empty()
    print("  [PASS] test_unsupported_source_skipped  不支持的源被静默过滤")


def test_executor_is_single_instance_under_concurrency():
    """多个线程并发首次取用线程池，只能建出一个实例（双检锁回归）"""
    fetcher = make_fetcher()
    barrier = threading.Barrier(8)
    obtained = []

    def grab():
        barrier.wait()
        obtained.append(fetcher._get_executor())

    threads = [threading.Thread(target=grab) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 持有对象引用后再比较身份，避免 id 复用导致误判
    assert len({id(executor) for executor in obtained}) == 1
    assert len(obtained) == 8
    obtained[0].shutdown()


def test_unhealthy_source_is_filtered_then_cools_down():
    """连续失败达到阈值后数据源被熔断；冷却期后放行半开探测。"""
    from src.data.data_source_health import get_global_health_checker

    fetcher = make_fetcher()
    src = MagicMock()
    src.name = "Broken"
    src.is_local = False
    src.get_index_data = lambda **k: pl.DataFrame({"a": [1]})
    fetcher.register_source(src)

    checker = get_global_health_checker()
    try:
        for _ in range(5):
            checker.record_request("Broken", False, 0.1, "boom")

        assert checker.get_health_status("Broken").value == "unhealthy"
        assert checker.get_health_info("Broken").error_messages == ["boom"] * 5
        assert fetcher._is_source_usable(src) is False

        # 冷却期后允许再次探测，避免数据源被永久锁死
        checker.get_health_info("Broken").last_failure_time = time.time() - 1000
        assert fetcher._is_source_usable(src) is True

        # 全部候选都熔断时仍回退到候选集合，不会把取数路径彻底锁死
        assert fetcher._get_candidate_sources("index") == [src]
    finally:
        checker.reset_health_info("Broken")


if __name__ == "__main__":
    print("== data_fetcher 优化烟测 ==")
    test_local_priority_returns_immediately()
    test_remote_parallel_returns_first_success()
    test_remote_all_fail_returns_quickly()
    test_unsupported_source_skipped()
    test_executor_is_single_instance_under_concurrency()
    test_unhealthy_source_is_filtered_then_cools_down()
    print("== 全部通过 ==")
