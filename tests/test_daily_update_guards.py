"""单入口行情末日断言与基金/因子刷新步骤的回归测试。

覆盖：
1. 行情末日 != 最近交易日时 build 步判失败，并使退出码为 1（负向用例）；
2. 末日相等时通过（证明断言不是恒真）；
3. 基金行情 / 复权因子在已最新时跳过、在源仍落后时判失败；
4. 新增两步位于构建之后、洁净集之前，备份与门禁仍是末尾两步。
"""

import struct
import types
from datetime import date

import polars as pl

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
    assert len(keys) == 22
    assert keys[-2:] == ['backup', 'gate']
    assert keys.index('build') < keys.index('fund_quote') < keys.index('factor')
    assert keys.index('dividend') < keys.index('sw_index') < keys.index('clean_set')
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
