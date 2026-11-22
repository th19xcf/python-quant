"""规格驱动层 ``compute`` 的单元回归：数据装载、因子面板与剔除条件。

这些函数决定「调仓日选谁」，错一位就会系统性偏移回测结论，因此逐条固定
输入与可观测输出。行情来自真实 polars 帧（只做最小样本），估值/上市日写
临时 parquet，不 mock 被验证的筛选与分档逻辑。
"""

from datetime import date

import numpy as np
import polars as pl
import pytest

from src.backtest.runner import compute


def _frame(days, closes, volumes=None, amounts=None):
    n = len(days)
    payload = {
        'date': days,
        'open': closes,
        'high': closes,
        'low': closes,
        'close': closes,
        'volume': volumes if volumes is not None else [1e6] * n,
    }
    if amounts is not None:
        payload['amount'] = amounts
    return pl.DataFrame(payload)


def _ctx(data, days=None, quoted=None, st_status=None, allowed=None):
    days = days if days is not None else sorted({d for df in data.values()
                                                 for d in df['date'].to_list()})
    quoted = quoted or {d: set(data) for d in days}
    return compute.Context(data=data, days=days, quoted=quoted,
                           st_status=st_status or {}, allowed=allowed)


def _write_valuation(tmp_path, rows):
    path = tmp_path / 'valuation.parquet'
    pl.DataFrame(
        rows,
        schema={'ts_code': pl.Utf8, 'trade_date': pl.Date,
                'pe': pl.Float64, 'pb': pl.Float64},
        orient='row',
    ).write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# 分位切分
# ---------------------------------------------------------------------------
def test_unit_fraction_buckets_rejects_non_unit_fraction():
    assert compute.unit_fraction_buckets(0.2, 'pct') == 5
    assert compute.unit_fraction_buckets(0.1, 'pct') == 10
    for bad in (0.0, 1.0, 0.15, 0.03):
        with pytest.raises(compute.SpecError):
            compute.unit_fraction_buckets(bad, 'pct')


def test_bucket_slices_breaks_ties_by_code_and_honours_min_names():
    values = {'B': 1.0, 'A': 1.0, 'C': 3.0, 'D': 2.0}
    asc = compute.bucket_slices(values, buckets=2, min_names=2)
    assert asc == [['A', 'B'], ['D', 'C']]  # 同值按代码升序，保证跨进程一致
    desc = compute.bucket_slices(values, buckets=2, min_names=2, direction='desc')
    assert desc == [['C', 'D'], ['B', 'A']]
    assert compute.bucket_slices(values, buckets=2, min_names=5) == []


# ---------------------------------------------------------------------------
# 日历 / 有行情集合
# ---------------------------------------------------------------------------
def test_build_calendar_and_rebalance_days():
    d1, d2 = date(2023, 1, 30), date(2023, 1, 31)
    d3 = date(2023, 2, 1)
    calendar, month_ends, prev_day = compute.build_calendar({
        'A': _frame([d1, d2], [10.0, 11.0]),
        'B': _frame([d2, d3], [10.0, 11.0]),
    })
    assert calendar == [d1, d2, d3]
    assert month_ends == [d2, d3]
    assert prev_day == {d2: d1, d3: d2}

    quarter_ends = [date(2023, 3, 31), date(2023, 6, 30), date(2023, 7, 31)]
    assert compute.rebalance_days(quarter_ends, 'monthly', date(2023, 4, 1)) == [
        date(2023, 6, 30), date(2023, 7, 31)]
    assert compute.rebalance_days(quarter_ends, 'quarterly', date(2023, 1, 1)) == [
        date(2023, 3, 31), date(2023, 6, 30)]
    assert compute.rebalance_days(quarter_ends, 'semi', date(2023, 1, 1)) == [
        date(2023, 6, 30)]


def test_quoted_sets_only_marks_days_with_a_bar():
    d1, d2, d3 = date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5)
    sets = compute.quoted_sets({
        'A': _frame([d1, d3], [10.0, 11.0]),
        'B': _frame([d2], [10.0]),
    }, [d2, d3])
    assert sets == {d2: {'B'}, d3: {'A'}}


# ---------------------------------------------------------------------------
# 行情装载：存在性与空行情两条失败路径
# ---------------------------------------------------------------------------
class _FakeFetcher:
    """只返回 600001.SH 的行情，用于区分「本地不存在」与「读回空」."""

    def __init__(self, config, db_manager=None):
        del config, db_manager

    def get_stock_batch(self, codes, start, end, freq, adjustment):
        del start, end, freq, adjustment
        return {code: _frame([date(2023, 1, 3)], [10.0])
                for code in codes if code == '600001.SH'}


def test_load_universe_missing_and_empty_paths(tmp_path, monkeypatch):
    quotes = tmp_path / 'data' / 'local_store' / 'quotes' / 'stock'
    quotes.mkdir(parents=True)
    for code in ('600001.SH', '000001.SZ'):
        pl.DataFrame({'x': [1]}).write_parquet(quotes / f'{code}.parquet')

    monkeypatch.setattr(compute, 'ROOT', tmp_path)
    monkeypatch.setattr(compute, 'DataFetcher', _FakeFetcher)
    period = {'start': '2023-01-03', 'end': '2023-01-31',
              'warmup_start': '2023-01-01'}

    assert compute.local_stock_codes() == ['000001.SZ', '600001.SH']

    data = compute.load_universe(period, {'mode': 'all'}, adjustment='qfq')
    assert list(data) == ['600001.SH']

    with pytest.raises(compute.SpecError):  # 本地列存里根本没有
        compute.load_universe(period, {'mode': 'explicit', 'codes': ['600999.SH']})
    with pytest.raises(compute.SpecError):  # 本地存在但读回空行情
        compute.load_universe(period, {'mode': 'explicit', 'codes': ['000001.SZ']})


def test_drop_contract_violations_keeps_good_and_records_bad():
    d1 = date(2023, 1, 3)
    good = _frame([d1], [10.0])
    bad = _frame([d1], [10.0]).drop('close')
    kept, violations = compute.drop_contract_violations({'GOOD': good, 'BAD': bad})
    assert list(kept) == ['GOOD']
    assert len(violations) == 1
    record = violations[0]
    assert record['code'] == 'BAD'
    assert record['rows'] == 1
    assert record['reason']


# ---------------------------------------------------------------------------
# 因子面板
# ---------------------------------------------------------------------------
def test_vol_panel_skips_short_zero_variance_and_non_finite_returns():
    days = [date(2023, 1, i) for i in (3, 4, 5, 6, 9)]
    data = {
        'A': _frame(days, [10.0, 11.0, 12.0, 13.0, 14.0]),
        'B': _frame(days[:3], [10.0, 11.0, 12.0]),       # 行数不足 window
        'C': _frame(days, [10.0, 0.0, 12.0, 13.0, 14.0]),  # 含非有限收益
        'D': _frame(days, [10.0] * 5),                    # 方差为 0
    }
    panel = compute.vol_panel(_ctx(data, days=days), {'window': 3})
    assert panel[days[0]] == {}
    assert panel[days[1]] == {}
    assert set(panel[days[3]]) == {'A'}
    assert set(panel[days[4]]) == {'A'}
    assert panel[days[4]]['A'] > 0


def test_value_panels_and_bp_expensive_exclusion(tmp_path, monkeypatch):
    d1 = date(2023, 1, 3)
    monkeypatch.setattr(compute, 'VALUATION', _write_valuation(tmp_path, [
        ('A', d1, 10.0, 0.5),
        ('B', d1, 20.0, 1.0),
        ('C', d1, -5.0, 2.0),
        ('D', d1, None, 4.0),
    ]))
    days = [d1]
    quoted = {d1: {'A', 'B', 'C', 'D'}}
    ctx = _ctx({}, days=days, quoted=quoted)

    assert compute.bp_panel(ctx, {})[d1] == {
        'A': 2.0, 'B': 1.0, 'C': 0.5, 'D': 0.25}
    assert set(compute.ep_panel(ctx, {})[d1]) == {'A', 'B'}  # 负 PE / 空 PE 跳过

    expensive = compute.exclude_bp_expensive_pct(
        ctx, {'pct': 0.5, 'min_names': 2}, d1, {'A', 'B', 'C', 'D'})
    assert expensive == {'C', 'D'}  # BP 最低（估值最贵）的半档
    assert compute.exclude_bp_expensive_pct(
        ctx, {'pct': 0.5, 'min_names': 5}, d1, {'A', 'B'}) is None

    pe_out = compute.exclude_pe_negative_or_expensive_pct(
        ctx, {'upper_quantile': 0.5}, d1, {'A', 'B', 'C', 'D'})
    assert pe_out == {'B', 'C'}          # 负 PE + 最贵 PE
    assert compute.exclude_pe_negative_or_expensive_pct(
        ctx, {'upper_quantile': 0.5}, d1, {'C'}) == {'C'}


def test_vol_high_exclusion_and_missing_day():
    days = [date(2023, 1, i) for i in (3, 4, 5)]
    data = {
        'A': _frame(days, [10.0, 11.0, 12.5]),
        'E': _frame(days, [10.0, 10.1, 10.2]),
    }
    ctx = _ctx(data, days=days)
    params = {'pct': 0.5, 'window': 2, 'min_names': 2}
    excluded = compute.exclude_vol_high_pct(ctx, params, days[-1], {'A', 'E'})
    assert len(excluded) == 1 and excluded <= {'A', 'E'}
    assert compute.exclude_vol_high_pct(
        ctx, params, date(2023, 2, 1), {'A', 'E'}) is None  # 当日不在面板


# ---------------------------------------------------------------------------
# 剔除条件：上市天数 / ST / 流动性
# ---------------------------------------------------------------------------
def test_exclude_new_listing_falls_back_to_first_bar(tmp_path, monkeypatch):
    all_days = [date(2023, 1, i) for i in (3, 4, 5, 6, 9, 10, 11, 12, 13, 16)]
    monkeypatch.setattr(compute, 'VALUATION', _write_valuation(
        tmp_path, [(f'X{i}', d, 10.0, 1.0) for i, d in enumerate(all_days)]))
    basic = tmp_path / 'stock_basic.parquet'
    pl.DataFrame(
        [('OLD', all_days[0]), ('NEW', all_days[-3])],
        schema={'ts_code': pl.Utf8, 'list_date': pl.Date},
        orient='row',
    ).write_parquet(basic)
    monkeypatch.setattr(compute, 'STOCK_BASIC', basic)

    data = {
        'OLD': _frame(all_days, [10.0] * len(all_days)),
        'NEW': _frame(all_days, [10.0] * len(all_days)),
        'UNKNOWN': _frame(all_days, [10.0] * len(all_days)),
    }
    ctx = _ctx(data, days=[all_days[-1]], quoted={all_days[-1]: set(data)})
    excluded = compute.exclude_new_listing(
        ctx, {'min_trading_days': 5}, all_days[-1], {'OLD', 'NEW', 'UNKNOWN'})
    assert excluded == {'NEW'}  # UNKNOWN 无 list_date，退回首根 bar 视为老股


def test_exclude_st_handles_bool_segments_and_unknown():
    day = date(2023, 1, 5)
    stamp = day.isoformat()
    ctx = _ctx({}, days=[day], quoted={day: {'A', 'B', 'C', 'D', 'E'}}, st_status={
        'A': None,
        'B': True,
        'C': [{'start_date': '2023-01-01', 'end_date': '2023-01-10', 'is_st': True}],
        'D': [{'start_date': '2023-02-01', 'end_date': '2023-02-10', 'is_st': True}],
        'E': False,
    })
    assert compute.exclude_st(ctx, {}, day, {'A', 'B', 'C', 'D', 'E'}) == {'B', 'C'}
    assert compute._st_at(None, stamp) is False
    assert compute._st_at(True, stamp) is True


def test_exclude_low_liquidity_uses_full_window_only():
    days = [date(2023, 1, i) for i in (3, 4, 5)]
    data = {
        'A': _frame(days, [10.0] * 3, amounts=[10.0, 10.0, 10.0]),
        'B': _frame(days, [10.0] * 3, amounts=[20.0, 20.0, 20.0]),
        'C': _frame(days, [10.0] * 3, amounts=[30.0, 30.0, 30.0]),
        'D': _frame(days, [10.0] * 3, amounts=[40.0, 40.0, 40.0]),
        'SHORT': _frame(days[:2], [10.0] * 2, amounts=[1.0, 1.0]),
        'NOAMT': _frame(days, [10.0] * 3),
    }
    ctx = _ctx(data, days=[days[-1]],
               quoted={days[-1]: set(data)})
    picked = compute.exclude_low_liquidity_pct(
        ctx, {'window': 3, 'pct': 0.5}, days[-1], set(data))
    assert picked == {'A', 'B'}  # 成交额最低的半档；窗口不足/无 amount 的跳过


# ---------------------------------------------------------------------------
# 懒加载缓存与交易日来源
# ---------------------------------------------------------------------------
def test_context_caches_factor_valuation_and_arrays(tmp_path, monkeypatch):
    d1 = date(2023, 1, 3)
    monkeypatch.setattr(compute, 'VALUATION', _write_valuation(
        tmp_path, [('A', d1, 10.0, 1.0)]))
    basic = tmp_path / 'stock_basic.parquet'
    pl.DataFrame(
        [('A', d1)],
        schema={'ts_code': pl.Utf8, 'list_date': pl.Date},
        orient='row',
    ).write_parquet(basic)
    monkeypatch.setattr(compute, 'STOCK_BASIC', basic)

    data = {'A': _frame([d1], [10.0], amounts=[1000.0])}
    ctx = _ctx(data, days=[d1])

    assert ctx.factor('vol', {'window': 1}) is ctx.factor('vol', {'window': 1})
    assert ctx.valuation() is ctx.valuation()
    assert ctx.list_dates() == {'A': d1}
    assert ctx.list_dates() is ctx.list_dates()
    assert ctx.trading_days() == [d1]
    assert ctx.trading_days() is ctx.trading_days()
    assert ctx.amount_arrays() is ctx.amount_arrays()
    assert compute.load_trading_days() == [d1]
