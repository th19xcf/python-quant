"""单入口行情末日断言与基金/因子刷新步骤的回归测试。

覆盖：
1. 行情末日 != 最近交易日时 build 步判失败，并使退出码为 1（负向用例）；
2. 末日相等时通过（证明断言不是恒真）；
3. 基金行情 / 复权因子在已最新时跳过、在源仍落后时判失败；
4. 新增两步位于构建之后、洁净集之前，备份与门禁仍是末尾两步。
"""

import json
import os
import struct
import subprocess
import sys
import time
import types
from datetime import date, datetime, timedelta

import polars as pl
import pytest

import src.data.local_store.fund_factors as fund_module
from src.data.local_store.build import build_frame
from tools import daily_update as du
from tools.daily_update import LocalFactorStore, LocalQuoteStore


def _write_day(tdx, market: str, code: str, records) -> None:
    """合成通达信 ``.day``：day/open/high/low/close/volume/amount，价格 ×100。"""
    path = tdx / market / 'lday' / f'{market}{code}.day'
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                '<IIIIIfII', int(day), int(open_ * 100), int(high * 100),
                int(low * 100), int(close * 100), float(amount), int(volume * 100), 0))


def _records(count: int, start: date = date(2024, 1, 2), amount: float = 1000.0):
    rows = []
    for i in range(count):
        day = start.replace(day=start.day + i)
        close = 10.0 + i * 0.5
        rows.append((day.strftime('%Y%m%d'), close, close + 0.5,
                     close - 0.5, close, 100.0 + i, amount))
    return rows


def _write_daily_index(root, rows: list[tuple[str, str, date]]) -> None:
    path = root / 'meta' / 'daily_index.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [row[0] for row in rows],
        'data_type': [row[1] for row in rows],
        # 真实 meta/daily_index.parquet 含 market（_META_SCHEMA 的第三列），
        # build_stale_status 会读它；fixture 按 ts_code 后缀补齐，保持同构。
        'market': [row[0].rsplit('.', 1)[-1].lower() for row in rows],
        'rows': [10] * len(rows),
        'start': [date(2026, 11, 1)] * len(rows),
        'end': [row[2] for row in rows],
    }).write_parquet(path)


def _write_factor(root, name: str, codes: list[str], trade_day: date) -> None:
    path = root / 'factors' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': codes,
        'trade_date': [trade_day] * len(codes),
        'qfq_factor': [1.0] * len(codes),
        'hfq_factor': [1.0] * len(codes),
    }).write_parquet(path)


def _build_args(**overrides) -> types.SimpleNamespace:
    base = {'start': None, 'end': None, 'skip_fund_refresh': False,
            'skip_factor_refresh': False}
    base.update(overrides)
    return types.SimpleNamespace(**base)


def test_daily_index_end_mismatch_fails_build_step(tmp_path, monkeypatch):
    """负向用例：daily_index 停在 11-13、最近交易日 11-16 → 必红且退出码 1。"""
    _write_daily_index(tmp_path, [('600000.SH', 'stock', date(2026, 11, 13))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 codes=None, range_label='2026-11-13~2026-11-16',
                                 expected=set(), plan_quote=set())
    args = _build_args()
    monkeypatch.setattr(du, '_run_build', lambda **kwargs: 0)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2026, 11, 16))
    monkeypatch.setattr(du, 'DAILY_LOG', tmp_path / 'daily.log')

    ok, detail = du._step_build(plan, args)
    assert ok is False
    assert '2026-11-13' in detail and '2026-11-16' in detail

    outcomes = du._run_steps([
        ('build', '本地行情构建 + 末日断言', True, lambda: du._step_build(plan, args)),
    ])
    assert du._print_run_summary(plan, outcomes, {}, 0.0) == 1


def test_daily_index_end_match_passes_build_step(tmp_path, monkeypatch):
    _write_daily_index(tmp_path, [('600000.SH', 'stock', date(2026, 11, 16))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 codes=None, range_label='2026-11-16~2026-11-16',
                                 expected=set(), plan_quote=set())
    monkeypatch.setattr(du, '_run_build', lambda **kwargs: 0)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2026, 11, 16))

    ok, detail = du._step_build(plan, _build_args())
    assert ok is True
    assert '=' in detail


def test_build_targets_registers_expected_and_keeps_plan_frozen(tmp_path):
    """预期集合取构建同一判定：同日末行修订要登记，且冻结计划上可原地更新。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    root = tmp_path / 'data' / 'local_store'
    store = du.LocalQuoteStore(root)
    factor_store = du.LocalFactorStore(root)
    store.write_daily('stock', '600000.SH',
                      build_frame('stock', '600000.SH', tdx / 'sh' / 'lday' / 'sh600000.day',
                                  factor_store), full=True)
    store.flush_meta()
    plan = du._Plan(
        root=root, config=types.SimpleNamespace(
            data=types.SimpleNamespace(tdx_data_path=str(tdx))),
        store=store, factor_store=factor_store, data_types=['stock'], codes=None,
        latest=date(2024, 1, 4), coverage={}, gaps=[], skipped=[], inputs={},
        expected=set(), plan_quote=set(), range_label='', marker_ts=0.0)

    assert du._build_targets(plan) == set()

    _write_day(tdx, 'sh', '600000', _records(3, amount=9999.0))
    got = du._build_targets(plan)
    assert got == {'quotes/stock/sh/600000.SH.parquet',
                   'quotes/stock/sh/600000.SH.manifest.json'}
    plan.expected.update(got)
    plan.plan_quote.update(got)
    assert got <= plan.expected and got <= plan.plan_quote


def test_fund_quote_step_skips_when_up_to_date(tmp_path, monkeypatch):
    _write_daily_index(tmp_path, [('510300.SH', 'fund', date(2026, 11, 16))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16))

    def _boom(*args, **kwargs):
        raise AssertionError('末日已最新时不应重算')

    monkeypatch.setattr(du, '_run_module', _boom)
    ok, detail = du._step_fund_quotes(plan, _build_args())
    assert ok is True
    assert '跳过' in detail


def test_fund_quote_step_fails_when_source_still_behind(tmp_path, monkeypatch):
    """源端仍停在 11-13 时不能绿：跑完仍是旧末日即判失败并登记白名单。"""
    _write_daily_index(tmp_path, [('510300.SH', 'fund', date(2026, 11, 13))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 store=du.LocalQuoteStore(tmp_path),
                                 expected=set(), plan_quote=set())
    monkeypatch.setattr(du, '_run_module', lambda cmd, label: 0)

    ok, detail = du._step_fund_quotes(plan, _build_args())
    assert ok is False
    assert '11-13' in detail
    assert any(rel.startswith('quotes/fund/') for rel in plan.expected)


def test_factor_step_skips_when_aligned(tmp_path):
    _write_daily_index(tmp_path, [
        ('600000.SH', 'stock', date(2026, 11, 16)),
        ('510300.SH', 'fund', date(2026, 11, 16)),
    ])
    _write_factor(tmp_path, 'stock_adj_factor.parquet', ['600000.SH'], date(2026, 11, 16))
    _write_factor(tmp_path, 'fund_adj_factor.parquet', ['510300.SH'], date(2026, 11, 16))
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 store=None, factor_store=None)

    ok, detail = du._step_factors(plan, _build_args())
    assert ok is True
    assert '跳过' in detail


def test_step_plan_keeps_hard_order_and_new_steps():
    keys = [key for key, _, _ in du.STEP_PLAN]
    # 2026-11-22：新增 futures / dividend_monthly / breadth_ma / state_vector / macro /
    # st_daily / limit_daily 与收尾只读闸门 dataset_health；2026-11-24 新增 events 与
    # margin（两融明细，T+1）；2026-11-25 新增 suspension（停复牌日表），共 33 步
    assert len(keys) == 33
    assert keys.index('pool') < keys.index('suspension') < keys.index('manifest')
    assert not {key: critical for key, _, critical in du.STEP_PLAN}['suspension']
    assert keys[-3:] == ['backup', 'gate', 'dataset_health']
    assert keys.index('build') < keys.index('fund_quote') < keys.index('factor')
    assert keys.index('dividend') < keys.index('events') < keys.index('sw_index') < keys.index('clean_set')
    assert keys.index('valuation') < keys.index('margin') < keys.index('index_valuation')
    assert not {key: critical for key, _, critical in du.STEP_PLAN}['events']
    assert not {key: critical for key, _, critical in du.STEP_PLAN}['margin']
    assert keys.index('factor_panel') < keys.index('b_topn_picks') < keys.index('manifest')
    assert not {key: critical for key, _, critical in du.STEP_PLAN}['sw_index']
    assert not {key: critical for key, _, critical in du.STEP_PLAN}['b_topn_picks']
    assert all(critical for key, _, critical in du.STEP_PLAN
               if key in {'fund_quote', 'factor'})


def _write_quotes(root, codes: list[str], end: date) -> None:
    store = LocalQuoteStore(root, enabled=True)
    dates = [date(2026, 11, 12), end] if end > date(2026, 11, 12) else [end]
    for code in codes:
        store.write_daily('fund', code, pl.DataFrame({
            'date': dates,
            'ts_code': [code] * len(dates),
            'open': [10.0] * len(dates),
            'high': [10.1] * len(dates),
            'low': [9.9] * len(dates),
            'close': [10.0] * len(dates),
            'pre_close': [10.0] * len(dates),
            'volume': [1000.0] * len(dates),
            'amount': [1e4] * len(dates),
            'pct_chg': [0.0] * len(dates),
            'change': [0.0] * len(dates),
            'qfq_factor': [1.0] * len(dates),
            'hfq_factor': [1.0] * len(dates),
        }))
    store.flush_meta()


def _factor_plan(root) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        root=root, latest=date(2026, 11, 16), codes=None,
        range_label='2026-11-12~2026-11-16',
        store=LocalQuoteStore(root, enabled=True),
        factor_store=LocalFactorStore(root, enabled=True),
        expected=set(), plan_quote=set())


def test_build_step_defers_factor_verify_when_fund_lags(tmp_path, monkeypatch):
    """基金行情/因子落后个股时，第 4 步不得因因子副本断言失败。

    断言已收窄到第 6 步刷新之后执行，所以第 4 步必须显式带
    ``--skip-factor-verify``，否则 build 收尾门禁必然红在基金上。
    """
    _write_quotes(tmp_path, ['510300.SH'], date(2026, 11, 13))
    _write_daily_index(tmp_path, [
        ('600000.SH', 'stock', date(2026, 11, 16)),
        ('510300.SH', 'fund', date(2026, 11, 13)),
    ])
    _write_factor(tmp_path, 'fund_adj_factor.parquet', ['510300.SH'], date(2026, 11, 13))
    plan = _factor_plan(tmp_path)
    captured: dict = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(du, '_run_build', fake_build)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2026, 11, 16))

    ok, _detail = du._step_build(plan, _build_args())
    assert ok is True
    assert captured['skip_factor_verify'] is True


def test_fund_lag_flow_reaches_factor_step(tmp_path, monkeypatch):
    """起点「基金行情与因子落后个股」：流程不卡第 4 步，且第 6 步可达并跑完。"""
    _write_quotes(tmp_path, ['510300.SH'], date(2026, 11, 13))
    _write_daily_index(tmp_path, [
        ('600000.SH', 'stock', date(2026, 11, 16)),
        ('510300.SH', 'fund', date(2026, 11, 13)),
    ])
    _write_factor(tmp_path, 'fund_adj_factor.parquet', ['510300.SH'], date(2026, 11, 13))
    _write_factor(tmp_path, 'stock_adj_factor.parquet', ['600000.SH'], date(2026, 11, 16))
    plan = _factor_plan(tmp_path)
    order: list[str] = []

    def fake_run_module(cmd, label, verbose=False):
        order.append(label)
        if label == '基金行情刷新':
            _write_quotes(tmp_path, ['510300.SH'], date(2026, 11, 16))
            _write_daily_index(tmp_path, [
                ('600000.SH', 'stock', date(2026, 11, 16)),
                ('510300.SH', 'fund', date(2026, 11, 16)),
            ])
        return 0

    def fake_rebuild(root, dividends, splits, codes=None, dry_run=False):
        order.append('基金因子重算')
        _write_factor(root, 'fund_adj_factor.parquet', list(codes or []), date(2026, 11, 16))
        return {'factor_rows': len(codes or []), 'factor_codes': len(codes or [])}

    monkeypatch.setattr(du, '_run_module', fake_run_module)
    monkeypatch.setattr(du, '_run_build', lambda **kwargs: 0)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2026, 11, 16))
    monkeypatch.setattr(fund_module, 'rebuild_fund_factors', fake_rebuild)
    monkeypatch.setattr(du, 'DAILY_LOG', tmp_path / 'daily.log')

    outcomes = du._run_steps([
        ('build', '本地行情构建 + 末日断言', True, lambda: du._step_build(plan, _build_args())),
        ('fund_quote', '基金行情刷新', True, lambda: du._step_fund_quotes(plan, _build_args())),
        ('factor', '复权因子刷新', True, lambda: du._step_factors(plan, _build_args())),
    ])
    assert [item.ok for item in outcomes] == [True, True, True], [
        item.detail for item in outcomes]
    assert order == ['基金行情刷新', '基金因子重算']
    assert '副本一致' in outcomes[-1].detail


def test_factor_step_fails_when_refresh_does_not_advance(tmp_path, monkeypatch):
    """反向用例：基金确实滞后且重算没推进末日时，第 6 步断言仍要判红。"""
    _write_quotes(tmp_path, ['510300.SH'], date(2026, 11, 16))
    _write_daily_index(tmp_path, [('510300.SH', 'fund', date(2026, 11, 16))])
    _write_factor(tmp_path, 'fund_adj_factor.parquet', ['510300.SH'], date(2026, 11, 13))
    plan = _factor_plan(tmp_path)
    monkeypatch.setattr(
        fund_module, 'rebuild_fund_factors',
        lambda *args, **kwargs: {'factor_rows': 1, 'factor_codes': 1})

    ok, detail = du._step_factors(plan, _build_args())
    assert ok is False
    assert '仍落后' in detail


# ----------------------------------------------------------------------
# 通达信日结落地守卫（build 前置）
# ----------------------------------------------------------------------
def _landing_plan(root, tdx, latest: date) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        root=root, latest=latest, codes=None, range_label='',
        store=du.LocalQuoteStore(root),
        config=types.SimpleNamespace(
            data=types.SimpleNamespace(tdx_data_path=str(tdx))),
        expected=set(), plan_quote=set())


def test_tdx_lday_end_reads_last_record_date(tmp_path):
    """末日探测复用既有 .day 解析：多文件取最大末根。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))                        # 末根 2024-01-04
    _write_day(tdx, 'sz', '000001', _records(5))                        # 末根 2024-01-06
    assert du._tdx_lday_end(tdx) == date(2024, 1, 6)
    assert du._tdx_lday_end(tdx, target=date(2024, 1, 4)) == date(2024, 1, 4)
    assert du._tdx_lday_end(tmp_path / 'absent') is None


def test_tdx_landing_guard_passes_without_wait_when_landed(tmp_path, monkeypatch):
    """目标日已落地：零等待直接通过，返回空 note（不污染 build 明细）。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    plan = _landing_plan(tmp_path, tdx, date(2024, 1, 4))
    slept: list[float] = []
    monkeypatch.setattr(du.time, 'sleep', lambda seconds: slept.append(seconds))

    ok, note = du._await_tdx_landing(plan, _build_args(), plan.latest)
    assert ok is True and note == '' and slept == []


def test_tdx_landing_guard_retries_every_5min_then_passes(tmp_path, monkeypatch):
    """末根落后目标：每 5 分钟重试，落地后放行并回报等待时长。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))                        # 11-13 口径：落后一天
    plan = _landing_plan(tmp_path, tdx, date(2024, 1, 5))
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        _write_day(tdx, 'sh', '600000', _records(4))                    # 模拟日结落地

    monkeypatch.setattr(du.time, 'sleep', fake_sleep)

    ok, note = du._await_tdx_landing(plan, _build_args(), plan.latest)
    assert ok is True
    assert sleeps == [du.TDX_LANDING_POLL_SECONDS]
    assert '等待' in note and '2024-01-05' in note


def test_tdx_landing_guard_timeout_fails_build_step(tmp_path, monkeypatch):
    """超时仍落后：build 步判失败并报「通达信数据未落地」，且不构建、不写盘。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    _write_daily_index(tmp_path, [('600000.SH', 'stock', date(2024, 1, 4))])
    plan = _landing_plan(tmp_path, tdx, date(2024, 1, 5))
    args = _build_args(tdx_landing_max_wait=0)

    def _no_sleep(_seconds: float) -> None:
        raise AssertionError('等待上限为 0 时不应再 sleep')

    def _no_build(**_kwargs):
        raise AssertionError('守卫未过时不应进入构建')

    monkeypatch.setattr(du.time, 'sleep', _no_sleep)
    monkeypatch.setattr(du, '_run_build', _no_build)

    ok, detail = du._step_build(plan, args)
    assert ok is False
    assert '通达信数据未落地' in detail and '2024-01-05' in detail


def test_tdx_landing_guard_can_be_disabled(tmp_path, monkeypatch):
    """开关关闭时不等落地，按原逻辑继续（守卫不改变后续断言口径）。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    _write_daily_index(tmp_path, [('600000.SH', 'stock', date(2024, 1, 5))])
    plan = _landing_plan(tmp_path, tdx, date(2024, 1, 5))
    args = _build_args(tdx_landing_guard=False)

    monkeypatch.setattr(du.time, 'sleep',
                        lambda seconds: (_ for _ in ()).throw(AssertionError('不应等待')))
    monkeypatch.setattr(du, '_run_build', lambda **kwargs: 0)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2024, 1, 5))

    ok, detail = du._step_build(plan, args)
    assert ok is True
    assert '2024-01-05' in detail


# ----------------------------------------------------------------------
# 启动器层：控制台落盘 + 未收尾陈旧锁告警
# ----------------------------------------------------------------------
def test_run_module_persists_console_log(tmp_path, monkeypatch):
    """子进程 stdout/stderr 必须落盘到 logs/daily_YYYYMMDD_console.log。"""
    monkeypatch.setattr(du, 'CONSOLE_LOG_DIR', tmp_path)
    code = 'import sys; print("child-out"); print("child-err", file=sys.stderr)'

    rc = du._run_module([sys.executable, '-c', code], 'probe')

    assert rc == 0
    text = (tmp_path / f'daily_{date.today():%Y%m%d}_console.log').read_text(
        encoding='utf-8')
    assert 'child-out' in text and 'child-err' in text


def test_acquire_lock_alerts_unfinished_run(tmp_path, monkeypatch):
    """锁在 + pid 已死 + 日志无 run_end → 写 stale_lock_unfinished_run 告警后接管。"""
    lock = tmp_path / 'daily_update.lock'
    log_file = tmp_path / 'daily_update.log'
    lock.write_text(json.dumps({'pid': 424242, 'started_at': '2026-11-25T16:11:00'}),
                    encoding='utf-8')
    log_file.write_text(
        json.dumps({'event': 'run_start', 'pid': 424242, 'argv': ['--apply']}) + '\n'
        + json.dumps({'event': 'step_start', 'step': 'build', 'pid': 424242}) + '\n',
        encoding='utf-8')
    monkeypatch.setattr(du, 'LOCK_PATH', lock)
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    monkeypatch.setattr(du, '_pid_alive', lambda pid: False)

    acquired = du._acquire_lock({'run_id': 'r-test', 'pid': 424242})

    assert acquired == lock and lock.is_file()
    events = [json.loads(line) for line in log_file.read_text(
        encoding='utf-8').splitlines() if line.strip()]
    alerts = [item for item in events if item.get('event') == 'stale_lock_unfinished_run']
    assert len(alerts) == 1
    assert alerts[0]['pid'] == 424242
    assert alerts[0]['lock_mtime'] != '?'
    assert alerts[0]['last_event'].get('event') == 'step_start'


# ----------------------------------------------------------------------
# 2026-11-26：被活跃锁挡下必须留痕（可区分「没跑」与「被挡」）
# ----------------------------------------------------------------------
def test_acquire_lock_blocked_logs_event(tmp_path, monkeypatch):
    """活跃锁挡下时写 lock_blocked（持锁 pid / 锁 mtime / 目标交易日），再退出。"""
    lock = tmp_path / 'daily_update.lock'
    log_file = tmp_path / 'daily_update.log'
    # 时间戳必须相对当前：写死历史时刻会在 12h 后越过陈旧锁阈值（LOCK_MAX_AGE_HOURS），
    # 被归档接管而不再抛 SystemExit（2026-11-27 实测该用例随时间失效）。
    started_at = (datetime.now() - timedelta(hours=1)).isoformat(timespec='seconds')
    lock.write_text(json.dumps({'pid': 4242, 'started_at': started_at}),
                    encoding='utf-8')
    monkeypatch.setattr(du, 'LOCK_PATH', lock)
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    monkeypatch.setattr(du, '_pid_alive', lambda pid: True)

    with pytest.raises(SystemExit) as excinfo:
        du._acquire_lock({'run_id': 'r-test'}, target='2026-11-26')
    assert '已有数据更新进程在运行' in str(excinfo.value)

    events = [json.loads(line) for line in log_file.read_text(
        encoding='utf-8').splitlines() if line.strip()]
    blocked = [item for item in events if item.get('event') == 'lock_blocked']
    assert len(blocked) == 1
    assert blocked[0]['holder_pid'] == 4242
    assert blocked[0]['pid'] == os.getpid()
    assert blocked[0]['lock_mtime'] != '?'
    assert blocked[0]['target_date'] == '2026-11-26'
    assert lock.is_file(), '被挡时不得归档/删除他人持有的锁'


# ----------------------------------------------------------------------
# 2026-11-26：月度分红跳过必须留痕（未到期 / 超时）
# ----------------------------------------------------------------------
def _dividend_args(**overrides) -> types.SimpleNamespace:
    base = {'skip_dividend_monthly': False, 'allow_remote': True,
            'dividend_monthly_days': 28}
    base.update(overrides)
    return types.SimpleNamespace(**base)


def _dividend_plan(latest: date = date(2026, 11, 26)) -> types.SimpleNamespace:
    return types.SimpleNamespace(latest=latest, expected=set())


def _read_events(log_file) -> list[dict]:
    return [json.loads(line) for line in log_file.read_text(
        encoding='utf-8').splitlines() if line.strip()]


def test_dividend_monthly_not_due_logs_event(tmp_path, monkeypatch):
    """未到期跳过：写 dividend_monthly_skip（原因 / 上次成功 / 距今天数）。"""
    log_file = tmp_path / 'daily_update.log'
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    monkeypatch.setattr(du, '_dividend_monthly_states', lambda: [
        (tmp_path, {'status': 'done',
                    'updated_at': f'{date.today().isoformat()}T00:00:00'})])

    ok, detail = du._step_dividend_monthly(_dividend_plan(), _dividend_args(dividend_monthly_days=28))

    assert ok is True and '未到期' in detail
    events = [item for item in _read_events(log_file)
              if item.get('event') == 'dividend_monthly_skip']
    assert len(events) == 1
    assert events[0]['reason'] == 'not_due'
    assert events[0]['last_success'] == date.today().isoformat()
    assert events[0]['days_since_success'] == 0
    assert events[0]['target_date'] == '2026-11-26'


def test_dividend_monthly_timeout_logs_event(tmp_path, monkeypatch):
    """超时被杀：写 dividend_monthly_skip（reason=timeout，无成功记录时 days=null）。"""
    log_file = tmp_path / 'daily_update.log'
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    monkeypatch.setattr(du, '_dividend_monthly_states', lambda: [])
    monkeypatch.setattr(du, '_run_module', lambda cmd, label, timeout=None: 124)

    ok, _detail = du._step_dividend_monthly(_dividend_plan(), _dividend_args())

    assert ok is False
    events = [item for item in _read_events(log_file)
              if item.get('event') == 'dividend_monthly_skip']
    assert len(events) == 1
    assert events[0]['reason'] == 'timeout'
    assert events[0]['last_success'] is None and events[0]['days_since_success'] is None
    assert events[0]['target_date'] == '2026-11-26'


def test_dividend_monthly_lag_warning_only_when_overdue(tmp_path, monkeypatch):
    """体检告警：滞后超阈值才出 warning 行，未超阈值保持安静。"""
    recent = {'status': 'done', 'updated_at': f'{date.today().isoformat()}T00:00:00'}
    monkeypatch.setattr(du, '_dividend_monthly_states', lambda: [(tmp_path, recent)])
    assert du._dividend_monthly_lag_warning(28) is None

    stale = {'status': 'done',
             'updated_at': f'{(date.today() - timedelta(days=400)).isoformat()}T00:00:00'}
    monkeypatch.setattr(du, '_dividend_monthly_states', lambda: [(tmp_path, stale)])
    line = du._dividend_monthly_lag_warning(28)
    assert line is not None and '[WARNING]' in line and 'dividend_monthly' in line


# ----------------------------------------------------------------------
# 2026-11-26：--detach 脱离父终端（父进程被树杀后子进程仍存活）
# ----------------------------------------------------------------------
@pytest.mark.skipif(os.name != 'nt', reason='进程树脱离验证只在 Windows 有意义')
def test_detached_child_survives_parent_tree_kill(tmp_path):
    """拉起方被 taskkill /F /T 硬杀后，脱离拉起的子进程仍在跑并继续写心跳。"""
    heartbeat = tmp_path / 'heartbeat.txt'
    log_path = tmp_path / 'console.log'
    child_code = (
        'import sys, time\n'
        'path = sys.argv[1]\n'
        'for _ in range(20):\n'
        '    open(path, "a", encoding="utf-8").write("x")\n'
        '    time.sleep(0.5)\n'
    )
    wrapper_code = (
        'import sys, time\n'
        'from pathlib import Path\n'
        'from tools import daily_update as du\n'
        f'pid = du._spawn_detached([sys.executable, "-c", {child_code!r}, '
        f'{str(heartbeat)!r}], Path({str(log_path)!r}))\n'
        'print(pid, flush=True)\n'
        'time.sleep(120)\n'
    )
    wrapper = subprocess.Popen([sys.executable, '-c', wrapper_code],
                               cwd=str(du.ROOT), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    pid = 0
    try:
        line = wrapper.stdout.readline().strip()
        assert line.isdigit(), f'中继未回报 pid：{line!r} / {wrapper.stderr.read()[:500]}'
        pid = int(line)
        deadline = time.time() + 10
        while time.time() < deadline and not (heartbeat.is_file()
                                              and heartbeat.stat().st_size > 0):
            time.sleep(0.2)
        assert du._pid_alive(pid), '脱离拉起的子进程没起来'

        # 模拟 2026-11-25 16:11 的事故：父终端按进程树硬杀拉起方
        subprocess.run(['taskkill', '/F', '/T', '/PID', str(wrapper.pid)],
                       capture_output=True, text=True)
        wrapper.wait(timeout=10)
        before = heartbeat.read_text(encoding='utf-8')
        time.sleep(2.0)

        assert du._pid_alive(pid), '父进程被树杀后子进程也没了（未真正脱离）'
        assert len(heartbeat.read_text(encoding='utf-8')) > len(before), \
            '子进程存活但心跳停了'
    finally:
        if pid:
            subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)],
                           capture_output=True, text=True)
        if wrapper.poll() is None:
            wrapper.kill()


def test_dataset_writer_registers_parent_manifest(tmp_path):
    """写入方登记必须覆盖父目录 manifest（2026-11-25 那轮的两处预期外改动的根因）。"""
    for rel in ('meta/events/manifest.json', 'meta/financials/manifest.json',
                'meta/registry/standalone_writes.jsonl'):
        assert rel in (du.SIDE_OUTPUTS + du.AUX_EVENTS_OUTPUTS + du.AUX_VALUATION_OUTPUTS)


def test_verify_still_flags_unregistered_change(tmp_path):
    """登记补齐后判据未放宽：未登记的数据件仍报预期外，已登记的不报。"""
    root = tmp_path / 'store'
    (root / 'meta' / 'events').mkdir(parents=True)
    rogue = root / 'meta' / 'events' / 'rogue.parquet'
    rogue.write_bytes(b'abc')
    registered = root / 'meta' / 'events' / 'events.parquet'
    registered.write_bytes(b'def')
    report = du._verify(
        root, {'meta/events/rogue.parquet': 3, 'meta/events/events.parquet': 3},
        {'meta/events/events.parquet', 'meta/events/manifest.json'}, {}, {})
    assert report['unexpected'] == ['meta/events/rogue.parquet']
    assert '预期外改动 meta/events/rogue.parquet' in report['problems']


def test_manifest_step_registers_quotes_manifest(tmp_path, monkeypatch):
    """行情目录总账必须登记为预期写件（2026-11-27 实测：差异 1 件致致命 exit=1）。

    ``quotes/manifest.json`` 由 build 收尾与对账步按磁盘实测回写，没有逐件产者；
    不登记就会被 ``_verify`` 判成「预期外改动」。
    """
    root = tmp_path / 'store'
    shard = root / 'quotes' / 'stock' / 'sh'
    shard.mkdir(parents=True)
    pl.DataFrame({'date': [date(2026, 11, 27)], 'ts_code': ['600000.SH'],
                  'close': [10.0]}).write_parquet(shard / '600000.SH.parquet')
    (root / 'quotes' / 'manifest.json').write_text(
        json.dumps({'files': 0, 'rows': 0, 'bytes': 0}), encoding='utf-8')
    monkeypatch.setattr(du, '_sync_disk_scan_ledger', lambda root: 'stub')
    monkeypatch.setattr(du, 'MANIFEST_FACTS_CACHE', tmp_path / 'facts.json')
    plan = du._Plan(
        root=root, config=types.SimpleNamespace(
            data=types.SimpleNamespace(tdx_data_path=None)),
        store=None, factor_store=None, data_types=['stock'], codes=None,
        latest=date(2026, 11, 27), coverage={}, gaps=[], skipped=[], inputs={},
        expected={'quotes/stock/sh/600000.SH.parquet'}, plan_quote=set(),
        range_label='', marker_ts=0.0)
    args = types.SimpleNamespace(manifest_full_hash=False,
                                 max_gap_days=du.DEFAULT_MAX_GAP_DAYS)

    ok, detail = du._step_manifest(plan, args, {})

    assert du.QUOTES_MANIFEST_REL in plan.expected
    assert '预期外改动' not in detail
    assert ok is True, detail


def test_manifest_step_refreshes_quotes_manifest_after_late_write(tmp_path, monkeypatch):
    """build 收尾回写后 futures 又追加行情：对账步必须再按磁盘回写总账。

    复刻 2026-11-27：build 收尾时总账 rows=1，之后第 6 步追加 1 个合约的当日 bar，
    磁盘已经是 2 行；manifest 步若不按磁盘再回写一次，总账会永久落后（153,896 vs
    153,956），``_verify`` 会把差异判成致命失败。
    """
    root = tmp_path / 'store'
    stock = root / 'quotes' / 'stock' / 'sh'
    stock.mkdir(parents=True)
    pl.DataFrame({'date': [date(2026, 11, 27)], 'ts_code': ['600000.SH'],
                  'close': [10.0]}).write_parquet(stock / '600000.SH.parquet')
    futures = root / 'quotes' / 'futures' / 'cffex'
    futures.mkdir(parents=True)
    pl.DataFrame({'trade_date': ['20261127'], 'ts_code': ['IF2412'],
                  'close': [4000.0]}).write_parquet(futures / 'IF2412.parquet')
    # build 收尾时刻的总账快照：只统计到个股那 1 行（futures 尚未写入）
    (root / 'quotes' / 'manifest.json').write_text(
        json.dumps({
            'files': 1, 'rows': 1, 'bytes': 0,
            'universe': {'stock': {
                'files': 1, 'rows': 1, 'bytes': 0,
                'markets_detail': {'sh': {'files': 1, 'rows': 1, 'bytes': 0}}}},
        }), encoding='utf-8')
    monkeypatch.setattr(du, '_sync_disk_scan_ledger', lambda root: 'stub')
    monkeypatch.setattr(du, 'MANIFEST_FACTS_CACHE', tmp_path / 'facts.json')
    plan = du._Plan(
        root=root, config=types.SimpleNamespace(
            data=types.SimpleNamespace(tdx_data_path=None)),
        store=None, factor_store=None, data_types=['stock'], codes=None,
        latest=date(2026, 11, 27), coverage={}, gaps=[], skipped=[], inputs={},
        expected={'quotes/stock/sh/600000.SH.parquet',
                  'quotes/futures/cffex/IF2412.parquet'},
        plan_quote=set(), range_label='', marker_ts=0.0)
    args = types.SimpleNamespace(manifest_full_hash=False,
                                 max_gap_days=du.DEFAULT_MAX_GAP_DAYS)

    ok, detail = du._step_manifest(plan, args, {})

    payload = json.loads((root / 'quotes' / 'manifest.json').read_text(encoding='utf-8'))
    assert payload['rows'] == 2
    assert payload['universe']['futures']['rows'] == 1
    assert du._summary_problems(root) == []
    assert ok is True, detail


def test_valuation_timeout_scales_with_remaining_codes(tmp_path, monkeypatch):
    """估值步硬超时必须按剩余标的数缩放，固定 1,500s 会截断全量刷新。"""
    root = tmp_path / 'store'
    (root / 'meta' / 'financials').mkdir(parents=True)
    codes = ['600000.SH', '000001.SZ', '600519.SH', '000002.SZ', '300750.SZ']
    pl.DataFrame({
        'ts_code': codes, 'name': ['x'] * 5,
        'list_date': [date(2000, 1, 1)] * 5, 'delist_date': [None] * 5,
        'status': ['L'] * 5, 'market': ['主板'] * 5,
    }).write_parquet(root / 'meta' / 'stock_basic.parquet')
    pl.DataFrame({'ts_code': codes, 'trade_date': [date(2026, 11, 26)] * 5},
                 schema={'ts_code': pl.String, 'trade_date': pl.Date}).write_parquet(
        root / 'meta' / 'financials' / 'valuation_daily.parquet')
    # 断点：5 只里已补完 3 只，剩余 2 只待采
    parts = root / '_staging' / 'financials_parts_20261127'
    parts.mkdir(parents=True)
    (parts / 'progress.json').write_text(json.dumps({
        'valuation_ranges': {'2026-11-27_2026-11-27': codes[:3]}}), encoding='utf-8')
    captured: dict[str, object] = {}

    def _fake_run(cmd, label, timeout=None, **_kwargs):
        captured['timeout'] = timeout
        return 0

    monkeypatch.setattr(du, '_run_module', _fake_run)
    plan = types.SimpleNamespace(root=root, latest=date(2026, 11, 27))
    args = types.SimpleNamespace(skip_valuation=False, allow_remote=True)

    _ok, detail = du._run_valuation(plan, args)

    expected = du.VALUATION_TIMEOUT_BASE + int(2 * du.VALUATION_SECONDS_PER_CODE)
    assert captured['timeout'] == expected
    assert '待采 2 只' in detail


def test_valuation_timeout_budget_bounds():
    """预算的上下界：进度不可知取上限，零待采取基础段，超大余量被上限截断。"""
    assert du._valuation_timeout_budget(None) == du.VALUATION_TIMEOUT_MAX
    assert du._valuation_timeout_budget(0) == du.VALUATION_TIMEOUT_BASE
    assert du._valuation_timeout_budget(100_000) == du.VALUATION_TIMEOUT_MAX


def test_sync_disk_scan_ledger_only_touches_backfilled_entries(tmp_path, monkeypatch):
    """只刷新 evidence=disk_scan 的登记：受控写入的数据集 writer 记录不被覆盖。"""
    root = tmp_path / 'store'
    registry = root / 'meta' / 'registry'
    registry.mkdir(parents=True)
    (registry / 'auto_register.json').write_text(json.dumps({'version': 1, 'datasets': {
        'quotes_stock': {'last_write': {'evidence': 'disk_scan'}},
        'events': {'last_write': {'evidence': 'write_dataset'}},
    }}), encoding='utf-8')
    captured: dict[str, list[str]] = {}

    def _fake_run(cmd, label, timeout=None):
        captured['cmd'] = cmd
        return 0

    monkeypatch.setattr(du, '_run_module', _fake_run)
    detail = du._sync_disk_scan_ledger(root)

    assert captured['cmd'][-3:] == ['--only', 'quotes_stock', '--force']
    assert detail == '1 个 rc=0'


# ----------------------------------------------------------------------
# 2026-11-26：--detach 启动失败必须返回非零（并留 detach_failed）
# ----------------------------------------------------------------------
def _detach_stub(monkeypatch, tmp_path, pid: int):
    log_file = tmp_path / 'daily_update.log'
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    monkeypatch.setattr(du, '_spawn_detached', lambda cmd, log_path: pid)
    monkeypatch.setattr(du, '_console_log_path', lambda day=None: tmp_path / 'console.log')
    return log_file


def test_detach_without_pid_returns_nonzero(tmp_path, monkeypatch):
    """中继没回报 pid：写 detach_failed(no_pid) 并返回非零，不能静默成功。"""
    log_file = _detach_stub(monkeypatch, tmp_path, 0)
    monkeypatch.setattr(du, '_pid_alive', lambda pid: True)

    assert du.main(['--detach', '--apply']) == 3

    failed = [e for e in _read_events(log_file) if e.get('event') == 'detach_failed']
    assert len(failed) == 1
    assert failed[0]['reason'] == 'no_pid' and failed[0]['pid'] == 0


def test_detach_start_check_waits_ten_seconds_and_fails(tmp_path, monkeypatch):
    """默认自检窗口 10 秒；窗口内无 run_start 且仍存活 → 记 detach_failed 并非零。"""
    log_file = _detach_stub(monkeypatch, tmp_path, 777)
    monkeypatch.setattr(du, '_pid_alive', lambda pid: True)
    monkeypatch.setattr(du, 'DETACH_START_CHECK_SECONDS', 0.0)

    assert du.main(['--detach', '--apply']) == 3

    failed = [e for e in _read_events(log_file) if e.get('event') == 'detach_failed']
    assert len(failed) == 1
    assert failed[0]['pid'] == 777 and 'run_start' in failed[0]['reason']


def test_detach_window_default_is_ten_seconds():
    assert du.DETACH_START_CHECK_SECONDS == 10.0


def test_detach_passes_when_run_start_seen(tmp_path, monkeypatch):
    log_file = _detach_stub(monkeypatch, tmp_path, 777)
    log_file.write_text(json.dumps({'event': 'run_start', 'pid': 777}) + '\n',
                        encoding='utf-8')
    monkeypatch.setattr(du, '_pid_alive', lambda pid: True)

    assert du.main(['--detach', '--apply']) == 0
    assert not [e for e in _read_events(log_file) if e.get('event') == 'detach_failed']


def test_detach_not_misjudged_when_child_finished_within_window(tmp_path, monkeypatch):
    """子进程 10 秒内正常跑完（已退出但有 run_start/run_end）不得判启动失败。"""
    log_file = _detach_stub(monkeypatch, tmp_path, 777)
    log_file.write_text(
        json.dumps({'event': 'run_start', 'pid': 777}) + '\n'
        + json.dumps({'event': 'run_end', 'pid': 777, 'exit': 0}) + '\n',
        encoding='utf-8')
    monkeypatch.setattr(du, '_pid_alive', lambda pid: False)

    assert du.main(['--detach', '--apply']) == 0
    assert not [e for e in _read_events(log_file) if e.get('event') == 'detach_failed']


# ----------------------------------------------------------------------
# 2026-11-26：非致命步骤失败落进 run_end（只记录、不改退出码）
# ----------------------------------------------------------------------
def test_run_end_records_failed_noncritical(tmp_path, monkeypatch):
    log_file = tmp_path / 'daily_update.log'
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    monkeypatch.setattr(du, '_write_staging', lambda *args, **kwargs: None)
    monkeypatch.setattr(du, '_mark_applied', lambda *args, **kwargs: None)
    monkeypatch.setattr(du, '_print_run_summary', lambda *args, **kwargs: 0)
    steps = [
        ('suspension', '停复牌日表', False, lambda: (False, 'rc=7')),
        ('breadth', '市场宽度', False, lambda: (True, 'ok')),
    ]
    monkeypatch.setattr(du, '_step_list', lambda plan, args, state: steps)
    plan = types.SimpleNamespace(latest=date(2026, 11, 26), gaps=[], inputs={},
                                 range_label='')
    args = types.SimpleNamespace(batch='t', target_holdings=False, capital=1e9)

    assert du._run_apply(plan, args) == 0

    end = [e for e in _read_events(log_file) if e.get('event') == 'run_end'][0]
    assert end['exit'] == 0, '非致命失败不得改退出码'
    assert end['failed'] == ['suspension']
    assert end['failed_noncritical'] == [
        {'step': 'suspension', 'name': '停复牌日表', 'exit': 7}]


def test_noncritical_failure_enters_health_warning_lines():
    """体检既有 warning 通道汇总：出 WARNING 行且不改 error 计数口径。"""
    state = {'failed_noncritical': [{'step': 'suspension', 'name': '停复牌日表',
                                     'exit': 7, 'detail': 'rc=7'}]}
    lines = du._noncritical_warning_lines(state)
    assert len(lines) == 1
    assert '[WARNING]' in lines[0] and 'suspension' in lines[0] and 'exit=7' in lines[0]
    assert du._noncritical_warning_lines({}) == []


# ----------------------------------------------------------------------
# 2026-11-26：批次被拒必须留痕（batch_rejected 可检索）
# ----------------------------------------------------------------------
def test_batch_rejected_already_applied_logs_event(tmp_path, monkeypatch):
    log_file = tmp_path / 'daily_update.log'
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    batch_dir = tmp_path / '_staging' / 'daily_20261126'
    batch_dir.mkdir(parents=True)
    (batch_dir / 'input_set.json').write_text(json.dumps(
        {'status': 'applied', 'applied_at': '2026-11-26T09:00:00'}), encoding='utf-8')

    with pytest.raises(SystemExit):
        du._write_staging(batch_dir, [], {'input_hash': 'abc'})

    rejected = [e for e in _read_events(log_file) if e.get('event') == 'batch_rejected']
    assert len(rejected) == 1
    assert rejected[0]['batch'] == 'daily_20261126'
    assert rejected[0]['reason'] == 'already_applied'
    assert '已于' in rejected[0]['detail']


def test_batch_rejected_input_hash_mismatch_logs_event(tmp_path, monkeypatch):
    log_file = tmp_path / 'daily_update.log'
    monkeypatch.setattr(du, 'DAILY_LOG', log_file)
    batch_dir = tmp_path / '_staging' / 'b2'
    batch_dir.mkdir(parents=True)
    (batch_dir / 'input_set.json').write_text(json.dumps(
        {'status': 'planned', 'input_hash': 'old'}), encoding='utf-8')

    with pytest.raises(SystemExit):
        du._write_staging(batch_dir, [], {'input_hash': 'new'})

    rejected = [e for e in _read_events(log_file) if e.get('event') == 'batch_rejected']
    assert len(rejected) == 1
    assert rejected[0]['reason'] == 'input_hash_mismatch'
    assert rejected[0]['existing_input_hash'] == 'old'
    assert rejected[0]['input_hash'] == 'new'
