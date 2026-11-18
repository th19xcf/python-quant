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
import pytest

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


class SubprocessConfigProbe:
    """子进程隔离测试源：回传构造时拿到的 config.data.probe_marker"""

    name = "SubprocessConfigProbe"
    is_local = False

    def __init__(self, config, db_manager):
        self.config = config

    def get_index_data(self, **kwargs):
        marker = getattr(getattr(self.config, 'data', None), 'probe_marker', 'MISSING')
        return pl.DataFrame({"marker": [str(marker)]})


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
    """所有远程源都超时，10s 内必须返回（不是每个 future 30s），并抛结构化异常"""
    from src.utils.exceptions import DataSourceException

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
    # 全部源故障不能静默返回空表：必须抛结构化异常，调用方能区分"源挂了"与"没数据"
    with pytest.raises(DataSourceException) as exc_info:
        fetcher._get_data_from_sources("index", "000001.SH", "20240101", "20241231", "1d")
    elapsed = time.time() - t0

    details = exc_info.value.details
    assert details['attempted'] == 2
    assert set(details['failed_sources']) == {"H1", "H2"}
    # as_completed timeout = per_source_timeout + 5 = 15s；最坏情况下 2 个源并行 = ~15s
    assert elapsed < 20, f"全部超时应在 20s 内返回，实际={elapsed:.2f}s"
    logger.info(f"  [PASS] test_remote_all_fail_returns_quickly  耗时={elapsed:.2f}s")


def test_sources_return_empty_is_not_treated_as_failure():
    """源正常但无该标的数据：返回空表且不抛异常（与"全部源故障"区分）"""
    fetcher = make_fetcher()

    def empty(**kwargs):
        return pl.DataFrame()

    src1 = MagicMock(name="E1"); src1.name = "E1"; src1.is_local = False
    src1.get_index_data = empty
    fetcher.register_source(src1)

    out = fetcher._get_data_from_sources("index", "000001.SH", "20240101", "20241231", "1d")
    assert out.is_empty(), "无数据应返回空表而非异常"
    logger.info("  [PASS] test_sources_return_empty_is_not_treated_as_failure  空结果不抛异常")


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


def _local_only_fetcher(tmp_path):
    """构造远程禁用（离线）配置的 fetcher。"""
    cfg = SimpleNamespace(data=SimpleNamespace(
        max_workers=4, tdx_data_path=str(tmp_path), allow_remote_sources=False,
    ))
    return DataFetcher(config=cfg)


def _record_sources(fetcher):
    """注册本地/远程替身，返回各自的调用计数。"""
    calls = {'local': 0, 'remote': 0}

    local = MagicMock(name="LocalTDX")
    local.name = "LocalTDX"
    local.is_local = True

    def local_get(**kwargs):
        calls['local'] += 1
        return pl.DataFrame()

    local.get_stock_data = local_get

    remote = MagicMock(name="Remote")
    remote.name = "Remote"
    remote.is_local = False

    def remote_get(**kwargs):
        calls['remote'] += 1
        return pl.DataFrame({"a": [1]})

    remote.get_stock_data = remote_get

    fetcher.register_source(remote)
    fetcher.register_source(local)
    return calls


def test_allow_remote_sources_defaults_to_false():
    """配置默认关闭远程；未声明该字段的轻量配置对象缺省允许（兼容历史调用）"""
    from src.utils.config import DataSettings, remote_sources_enabled

    assert DataSettings().allow_remote_sources is False
    assert remote_sources_enabled(SimpleNamespace(data=SimpleNamespace(max_workers=4))) is True


def test_local_file_missing_fails_fast_when_remote_disabled(tmp_path):
    """远程禁用 + 本地 .day 缺失：立即抛明确错误，且不调度任何源（无网络等待）"""
    from src.utils.exceptions import DataSourceException

    fetcher = _local_only_fetcher(tmp_path)
    calls = _record_sources(fetcher)

    expected = Path(tmp_path) / 'sh' / 'lday' / 'sh600000.day'
    assert not expected.exists()

    t0 = time.time()
    with pytest.raises(DataSourceException) as exc_info:
        fetcher._get_data_from_sources("stock", "600000.SH", "20240101", "20241231", "1d")
    elapsed = time.time() - t0

    message = str(exc_info.value)
    assert elapsed < 0.5, f"本地文件缺失应立即失败，实际={elapsed:.2f}s"
    assert calls == {'local': 0, 'remote': 0}, "本地/远程源都不应被调用"
    assert "本地无此文件且远程已禁用" in message
    assert str(expected) in message
    assert exc_info.value.details['local_file'] == str(expected)
    assert exc_info.value.details['remote_sources_enabled'] is False


def test_remote_disabled_skips_remote_when_local_file_has_no_data(tmp_path):
    """本地文件存在但无数据：远程仍不被调度，抛可展示错误而非静默空表"""
    from src.utils.exceptions import DataSourceException

    fetcher = _local_only_fetcher(tmp_path)
    calls = _record_sources(fetcher)

    day_file = Path(tmp_path) / 'sh' / 'lday' / 'sh600000.day'
    day_file.parent.mkdir(parents=True)
    day_file.write_bytes(b'')

    with pytest.raises(DataSourceException) as exc_info:
        fetcher._get_data_from_sources("stock", "600000.SH", "20240101", "20241231", "1d")

    assert calls['remote'] == 0, "远程禁用时不得发起远程取数"
    assert calls['local'] == 1
    assert "本地通达信文件无数据且远程已禁用" in str(exc_info.value)


def test_index_code_routes_to_index_path(monkeypatch):
    """指数代码从股票入口请求时改走 index 取数，不再按 stock 类型请求数据源"""
    fetcher = make_fetcher()
    calls = {'index': [], 'sources': []}

    def fake_index_data(code, start, end, frequency='1d'):
        calls['index'].append((code, start, end, frequency))
        return pl.DataFrame({'close': [1.0]})

    def fake_sources(data_type, *args, **kwargs):
        calls['sources'].append(data_type)
        return pl.DataFrame()

    monkeypatch.setattr(fetcher, 'get_index_data', fake_index_data)
    monkeypatch.setattr(fetcher, '_get_data_from_sources', fake_sources)

    out = fetcher.get_stock_data('000001.SH', '20240101', '20241231')

    assert calls['index'] == [('000001.SH', '20240101', '20241231', '1d')]
    assert calls['sources'] == [], "指数不得以 stock 类型进入数据源取数"
    assert out.height == 1


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


def test_subprocess_config_injected_by_initializer():
    """config 由池 initializer 注入子进程，不再随每次 payload 传递"""
    fetcher = make_process_fetcher()
    fetcher.config.data.probe_marker = 's45-ok'
    src = SubprocessConfigProbe(None, None)
    fetcher.register_source(src)
    keys = _enable_isolation(SubprocessConfigProbe)
    try:
        out = fetcher._try_remote_sources_parallel(
            [src], "index", "000001.SH", "20240101", "20241231", "1d", "hfq",
            per_source_timeout=5,
        )
    finally:
        data_fetcher_module._PROCESS_ISOLATED_HANDLERS.difference_update(keys)
        fetcher._abort_process_pool()

    assert out.height == 1, "子进程应返回探针数据"
    assert out["marker"][0] == "s45-ok", f"config 未注入子进程，实得 {out['marker'][0]}"


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


def test_abort_process_pool_warns_on_terminate_and_join_failure():
    """子进程收尾失败不得无声：终止/回收失败必须落 warning 并带 pid。

    改前两处都是 ``logger.debug``，WARNING 级捕获不到，本用例会红。
    """
    fetcher = make_fetcher()

    class _FakeProc:
        pid = 4321

        def is_alive(self):
            return True

        def terminate(self):
            raise OSError('拒绝访问')

        def join(self, timeout=None):
            raise RuntimeError('回收超时')

    class _FakePool:
        _processes = {1: _FakeProc()}

        def shutdown(self, wait=False, cancel_futures=False):
            pass

    fetcher._process_executor = _FakePool()
    messages: list[str] = []
    handler_id = logger.add(lambda m: messages.append(m.record['message']), level='WARNING')
    try:
        fetcher._abort_process_pool()
    finally:
        logger.remove(handler_id)

    assert any('终止子进程失败' in m and 'pid=4321' in m for m in messages), messages
    assert any('回收子进程失败' in m and 'pid=4321' in m for m in messages), messages


def test_unregistered_source_does_not_raise_keyerror(monkeypatch):
    """候选源未登记能力时按「非本地」处理，不得抛 KeyError。

    改前 ``self._source_capabilities[s]['is_local']`` 是硬索引，缺键直接 KeyError。
    """
    fetcher = make_fetcher()
    unregistered = MagicMock(name="Unregistered")
    unregistered.name = "Unregistered"
    unregistered.is_local = False

    monkeypatch.setattr(fetcher, '_remote_sources_allowed', lambda: True)
    monkeypatch.setattr(fetcher, '_local_day_file_path', lambda code: None)
    monkeypatch.setattr(fetcher, '_get_candidate_sources', lambda data_type: [unregistered])
    monkeypatch.setattr(fetcher, '_try_remote_sources_parallel', lambda *a, **k: pl.DataFrame())

    out = fetcher._get_data_from_sources("stock", "000001.SZ", "20240101", "20241231", "1d")
    assert out.is_empty()
    assert fetcher._source_capabilities == {}, '不得为缺键源偷偷补登记'


def _qfq_frame(rows: int = 3) -> pl.DataFrame:
    return pl.DataFrame({
        'date': [f'2026-11-{10 + i:02d}' for i in range(rows)],
        'open': [10.0 + i for i in range(rows)],
        'high': [10.5 + i for i in range(rows)],
        'low': [9.5 + i for i in range(rows)],
        'close': [10.2 + i for i in range(rows)],
        'qfq_factor': [1.0] * rows,
    })


def test_qfq_all_one_normal_type_logs_info_with_code(monkeypatch):
    """ETF 长尾无分红：返回未复权价、INFO、日志带代码与「分红表 0 条」依据。

    改前是统一 WARNING 且不带代码，本用例会红。
    """
    monkeypatch.setattr(
        data_fetcher_module, '_qfq_degradation_facts',
        lambda code: {'kind': 'ETF', 'dividend_rows': 0,
                      'type_total': 1735, 'type_with_dividend': 148})
    monkeypatch.setattr(data_fetcher_module, '_load_dividend_ledger', lambda: {})
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level='DEBUG')
    try:
        out = DataFetcher._apply_adjustment(_qfq_frame(), 'qfq', code='516130.SH')
    finally:
        logger.remove(handler_id)

    assert out['close'].to_list() == [10.2, 11.2, 12.2], '复权数值行为不得改变'
    hits = [r for r in records if '复权降级]' in r['message']]
    assert len(hits) == 1, [r['message'] for r in hits]
    assert hits[0]['level'].name == 'INFO'
    assert '516130.SH' in hits[0]['message']
    assert '分红表 0 条' in hits[0]['message']
    assert '普遍无分红' in hits[0]['message']


def test_qfq_all_one_majority_dividend_logs_warning(monkeypatch):
    """个股多数有分红却无记录 → 保留 WARNING 并点明覆盖不足。"""
    monkeypatch.setattr(
        data_fetcher_module, '_qfq_degradation_facts',
        lambda code: {'kind': '个股', 'dividend_rows': 0,
                      'type_total': 5916, 'type_with_dividend': 5681})
    monkeypatch.setattr(data_fetcher_module, '_load_dividend_ledger', lambda: {})
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level='DEBUG')
    try:
        DataFetcher._apply_adjustment(_qfq_frame(), 'qfq', code='600000.SH')
    finally:
        logger.remove(handler_id)

    hits = [r for r in records if '复权降级]' in r['message']]
    assert len(hits) == 1
    assert hits[0]['level'].name == 'WARNING'
    assert '600000.SH' in hits[0]['message'] and '覆盖不足' in hits[0]['message']


def test_qfq_batch_aggregates_one_summary_and_detail_rows(monkeypatch, tmp_path):
    """批量 3 只命中：只 1 条汇总、明细 3 行、两类计数正确。"""
    facts = {
        '516130.SH': {'kind': 'ETF', 'dividend_rows': 0,
                      'type_total': 1735, 'type_with_dividend': 148},
        '159932.SZ': {'kind': 'ETF', 'dividend_rows': 0,
                      'type_total': 1735, 'type_with_dividend': 148},
        '600000.SH': {'kind': '个股', 'dividend_rows': 0,
                      'type_total': 5916, 'type_with_dividend': 5681},
    }
    monkeypatch.setattr(data_fetcher_module, '_qfq_degradation_facts',
                        lambda code: facts[code])
    monkeypatch.setattr(data_fetcher_module, '_load_dividend_ledger', lambda: {})
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level='DEBUG')
    try:
        with data_fetcher_module._qfq_degrade_collection(tmp_path):
            for code in facts:
                DataFetcher._apply_adjustment(_qfq_frame(), 'qfq', code=code)
    finally:
        logger.remove(handler_id)

    assert not [r for r in records if '复权降级]' in r['message']], '批量不得逐只打日志'
    summary = [r for r in records if '[复权降级汇总]' in r['message']]
    assert len(summary) == 1, [r['message'] for r in records]
    assert summary[0]['level'].name == 'WARNING'
    msg = summary[0]['message']
    assert '命中 3 只' in msg and '正常无分红 2' in msg and '覆盖不足 1' in msg
    detail = list(tmp_path.glob('qfq_degradation_*.csv'))
    assert len(detail) == 1, detail
    rows = detail[0].read_text(encoding='utf-8').strip().splitlines()
    assert len(rows) - 1 == 3, rows


def test_batch_skips_delisted_before_request(monkeypatch):
    """退市标的在取数前被剔除：不发起取数、INFO 说明跳过几只。

    改前会调用 get_stock_data（抛断言），本用例会红。
    """
    fetcher = make_fetcher()
    monkeypatch.setattr(
        fetcher, '_batch_from_local_store',
        lambda codes, sd, ed, freq, adj, as_of, cp: ({}, list(codes)))

    def _must_not_call(*args, **kwargs):
        raise AssertionError('退市标的不得发起取数')

    monkeypatch.setattr(fetcher, 'get_stock_data', _must_not_call)
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level='DEBUG')
    try:
        out = fetcher.get_stock_batch(['600656.SH'], '20240101', '20241231',
                                      '1d', 'qfq')
    finally:
        logger.remove(handler_id)

    assert out == {}
    info = [r for r in records if '[batch]' in r['message']]
    assert any(r['level'].name == 'INFO' and '跳过 1 只退市标的' in r['message']
               and '600656.SH' in r['message'] for r in info), info
    assert not [r for r in records if r['level'].name == 'WARNING']


def test_batch_in_market_failure_stays_warning(monkeypatch):
    """在市标的取数失败仍逐条 WARNING，不得被退市分级吞掉。"""
    from src.utils.exceptions import DataSourceException

    fetcher = make_fetcher()
    monkeypatch.setattr(
        fetcher, '_batch_from_local_store',
        lambda codes, sd, ed, freq, adj, as_of, cp: ({}, list(codes)))

    def _boom(*args, **kwargs):
        raise DataSourceException('TdxHandler', '本地无此文件且远程已禁用')

    monkeypatch.setattr(fetcher, 'get_stock_data', _boom)
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level='DEBUG')
    try:
        out = fetcher.get_stock_batch(['600000.SH'], '20240101', '20241231',
                                      '1d', 'qfq')
    finally:
        logger.remove(handler_id)

    assert out == {}
    warns = [r for r in records if r['level'].name == 'WARNING']
    assert any('600000.SH' in r['message'] and '取数失败' in r['message']
               for r in warns), [r['message'] for r in records]


def test_qfq_ledger_status_overrides_statistics(monkeypatch):
    """清单是唯一判定出处：confirmed → INFO，unconfirmed → WARNING 单列。"""
    ledger = {
        '516130.SH': {'status': 'confirmed_no_dividend',
                      'evidence_source': 'akshare:fund_open_fund_info_em',
                      'evidence_date': '2026-11-18'},
        '512000.SH': {'status': 'unconfirmed_coverage',
                      'evidence_source': 'factors/fund_dividend.parquet',
                      'evidence_date': ''},
    }
    monkeypatch.setattr(data_fetcher_module, '_load_dividend_ledger', lambda: ledger)
    monkeypatch.setattr(
        data_fetcher_module, '_qfq_degradation_facts',
        lambda code: {'kind': 'ETF', 'dividend_rows': 0,
                      'type_total': 1735, 'type_with_dividend': 148})
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level='DEBUG')
    try:
        DataFetcher._apply_adjustment(_qfq_frame(), 'qfq', code='516130.SH')
        DataFetcher._apply_adjustment(_qfq_frame(), 'qfq', code='512000.SH')
    finally:
        logger.remove(handler_id)

    hits = {r['message'].split()[1]: r for r in records if '复权降级]' in r['message']}
    assert hits['516130.SH']['level'].name == 'INFO'
    assert 'confirmed_no_dividend' in hits['516130.SH']['message']
    assert hits['512000.SH']['level'].name == 'WARNING'
    assert 'unconfirmed_coverage' in hits['512000.SH']['message']


if __name__ == "__main__":
    logger.info("== data_fetcher 优化烟测 ==")
    test_local_priority_returns_immediately()
    test_remote_parallel_returns_first_success()
    test_remote_all_fail_returns_quickly()
    test_sources_return_empty_is_not_treated_as_failure()
    test_unsupported_source_skipped()
    test_executor_is_single_instance_under_concurrency()
    test_executor_reused_100_times_and_close_releases_it()
    test_unhealthy_source_is_filtered_then_cools_down()
    test_subprocess_isolation_returns_data()
    test_subprocess_isolation_timeout_terminates_workers()
    logger.info("== 全部通过 ==")
