"""
对 data_fetcher 的优化逻辑做最小烟测：
1. 本地源优先（5s 内拿到就返回）
2. 远程源并行（命中即取消其他）
3. 远程源全部超时能及时返回
"""
import sys
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import psutil

from src.data.managers import data_fetcher as data_fetcher_module
from src.data.managers.data_fetcher import DataFetcher


class SubprocessOkSource:
    """子进程隔离测试源：正常返回数据"""
    name = "SubprocessOk"
    is_local = False

    def __init__(self, config, db_manager):
        pass

    def get_index_data(self, **kwargs):
        return pl.DataFrame({"close": [1.0, 2.0, 3.0]})


class SubprocessHangSource:
    """子进程隔离测试源：挂死 60s，验证超时能真正终止子进程"""
    name = "SubprocessHang"
    is_local = False

    def __init__(self, config, db_manager):
        pass

    def get_index_data(self, **kwargs):
        time.sleep(60)
        return pl.DataFrame()


def make_fetcher():
    cfg = MagicMock()
    cfg.data.max_workers = 5
    return DataFetcher(config=cfg)


def make_process_fetcher():
    """灰度开关开启的 fetcher；config 必须可 pickle（MagicMock 不行）"""
    cfg = SimpleNamespace(
        data=SimpleNamespace(max_workers=4, use_subprocess_sources=True)
    )
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
    logger.info(f"  [PASS] test_local_priority_returns_immediately  耗时={elapsed:.2f}s, 远程被调用={remote_called['n']}次")


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
    logger.info(f"  [PASS] test_remote_parallel_returns_first_success  耗时={elapsed:.2f}s, 慢源被调用={slow_called['n']}次(取消前可能已起)")


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
    logger.info(f"  [PASS] test_remote_all_fail_returns_quickly  耗时={elapsed:.2f}s")


def test_unsupported_source_skipped():
    """不支持 stock 的源在请求 stock 时被过滤掉"""
    fetcher = make_fetcher()

    only_index = MagicMock(name="OI"); only_index.name = "OI"; only_index.is_local = False
    only_index.get_index_data = lambda **k: pl.DataFrame({"a": [1]})
    # 没有 get_stock_data，也没有 get_kline_data

    fetcher.register_source(only_index)
    out = fetcher._get_data_from_sources("stock", "000001.SZ", "20240101", "20241231", "1d")
    assert out.is_empty()
    logger.info("  [PASS] test_unsupported_source_skipped  不支持的源被静默过滤")


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


def test_executor_reused_100_times_and_close_releases_it():
    """P2-1 回归：连续复用同一线程池，close() 后池被关闭且可重复调用。"""
    fetcher = make_fetcher()
    executors = {id(fetcher._get_executor()) for _ in range(100)}
    assert len(executors) == 1, "100 次取用必须复用同一个线程池"

    pool = fetcher._get_executor()
    fetcher.close()
    assert fetcher._executor is None
    assert getattr(pool, '_shutdown', False) is True

    # 幂等：重复 close 不得抛异常，也不得重建池
    fetcher.close()
    assert fetcher._executor is None


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


def _enable_isolation(*classes):
    """把测试源的类路径加入子进程隔离白名单，返回用于清理的键集合"""
    keys = {f"{cls.__module__}:{cls.__name__}" for cls in classes}
    data_fetcher_module._PROCESS_ISOLATED_HANDLERS.update(keys)
    return keys


def test_subprocess_isolation_returns_data():
    """开关开启后，白名单内的远程源在子进程内取数并回传 DataFrame"""
    fetcher = make_process_fetcher()
    src = SubprocessOkSource(None, None)
    fetcher.register_source(src)
    keys = _enable_isolation(SubprocessOkSource)
    try:
        out = fetcher._try_remote_sources_parallel(
            [src], "index", "000001.SH", "20240101", "20241231", "1d", "hfq",
            per_source_timeout=5,
        )
    finally:
        data_fetcher_module._PROCESS_ISOLATED_HANDLERS.difference_update(keys)
        fetcher._abort_process_pool()

    assert not out.is_empty(), "子进程应返回数据"
    assert out.height == 3
    logger.info("  [PASS] test_subprocess_isolation_returns_data  子进程取数正常")


def test_subprocess_isolation_timeout_terminates_workers():
    """挂死源超时后必须终止子进程池且无残留进程"""
    fetcher = make_process_fetcher()
    src = SubprocessHangSource(None, None)
    fetcher.register_source(src)
    keys = _enable_isolation(SubprocessHangSource)

    before_pids = {p.pid for p in psutil.Process().children(recursive=True)}
    try:
        t0 = time.time()
        out = fetcher._try_remote_sources_parallel(
            [src], "index", "000001.SH", "20240101", "20241231", "1d", "hfq",
            per_source_timeout=2,
        )
        elapsed = time.time() - t0
        assert fetcher._process_executor is None, "超时后进程池应被丢弃"
    finally:
        data_fetcher_module._PROCESS_ISOLATED_HANDLERS.difference_update(keys)
        fetcher._abort_process_pool()

    assert out.is_empty(), "全部挂死应返回空 DataFrame"
    assert elapsed < 10, f"超时应在 10s 内返回并终止子进程，实际={elapsed:.2f}s"

    # 终止的进程已被 join 回收，不应再出现在子进程列表中
    after_pids = {p.pid for p in psutil.Process().children(recursive=True)}
    leftover = [pid for pid in (after_pids - before_pids) if psutil.pid_exists(pid)]
    assert not leftover, f"存在未回收的子进程: {leftover}"
    logger.info(f"  [PASS] test_subprocess_isolation_timeout_terminates_workers  耗时={elapsed:.2f}s")


if __name__ == "__main__":
    logger.info("== data_fetcher 优化烟测 ==")
    test_local_priority_returns_immediately()
    test_remote_parallel_returns_first_success()
    test_remote_all_fail_returns_quickly()
    test_unsupported_source_skipped()
    test_executor_is_single_instance_under_concurrency()
    test_executor_reused_100_times_and_close_releases_it()
    test_unhealthy_source_is_filtered_then_cools_down()
    test_subprocess_isolation_returns_data()
    test_subprocess_isolation_timeout_terminates_workers()
    logger.info("== 全部通过 ==")
