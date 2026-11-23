"""可交易性过滤的逐项单测：每个开关、全关等价、截面过小与缺表跳过。"""

from datetime import date

import polars as pl
import pytest

from src.risk.tradability import (
    TradabilityConfig,
    TradabilityFilter,
    latest_mean_amount,
)

DAY = date(2023, 1, 5)
DAYS = [date(2023, 1, i) for i in (3, 4, 5)]


def _store(tmp_path):
    root = tmp_path / 'store'
    (root / 'meta' / 'limit' / 'daily').mkdir(parents=True)
    rows = [
        # ts_code, board, st_status, listing_days, phase, prev_close, pct,
        # basis, up, down, touch_up, touch_down, close_at_up, close_at_down,
        # one_word_up, one_word_down, suspended, can_buy, can_sell
        ('SUSP', '深市主板', 'non_st', 500, 'normal', 10.0, 0.10, 'x', 11.0, 9.0,
         False, False, False, False, False, False, True, False, False),
        ('ST', '深市主板', 'st', 500, 'normal', 10.0, 0.05, 'x', 10.5, 9.5,
         False, False, False, False, False, False, False, True, True),
        ('NEW', '深市主板', 'non_st', 20, 'normal', 10.0, 0.10, 'x', 11.0, 9.0,
         False, False, False, False, False, False, False, True, True),
        ('UP', '深市主板', 'non_st', 500, 'normal', 10.0, 0.10, 'x', 11.0, 9.0,
         True, False, True, False, True, False, False, False, True),
        ('DOWN', '深市主板', 'non_st', 500, 'normal', 10.0, 0.10, 'x', 11.0, 9.0,
         False, True, False, True, False, True, False, True, False),
        ('OK', '深市主板', 'non_st', 500, 'normal', 10.0, 0.10, 'x', 11.0, 9.0,
         False, False, False, False, False, False, False, True, True),
        ('DELIST', '深市主板', 'non_st', 500, 'normal', 10.0, 0.10, 'x', 11.0, 9.0,
         False, False, False, False, False, False, False, True, True),
    ]
    pl.DataFrame(
        [(DAY, *r) for r in rows],
        schema={'trade_date': pl.Date, 'ts_code': pl.Utf8, 'board': pl.Utf8,
                'st_status': pl.Utf8, 'listing_days': pl.Int32,
                'listing_phase': pl.Utf8, 'prev_close': pl.Float64,
                'limit_pct': pl.Float64, 'limit_basis': pl.Utf8,
                'limit_up_price': pl.Float64, 'limit_down_price': pl.Float64,
                'touch_up': pl.Boolean, 'touch_down': pl.Boolean,
                'close_at_up': pl.Boolean, 'close_at_down': pl.Boolean,
                'one_word_up': pl.Boolean, 'one_word_down': pl.Boolean,
                'suspended': pl.Boolean, 'can_buy': pl.Boolean,
                'can_sell': pl.Boolean},
        orient='row',
    ).write_parquet(root / 'meta' / 'limit' / 'daily' / 'limit_daily_202301.parquet')

    pl.DataFrame(
        [('DELIST', '退市甲', date(2022, 12, 1), date(2023, 2, 1)),
         ('OK', '正常甲', date(2020, 1, 1), None)],
        schema={'ts_code': pl.Utf8, 'name': pl.Utf8,
                'start_date': pl.Date, 'end_date': pl.Date},
        orient='row',
    ).write_parquet(root / 'meta' / 'name_history.parquet')

    (root / 'factors').mkdir()
    pl.DataFrame(
        [(DAY, 'OK', 0.02), (DAY, 'SUSP', 0.01)],
        schema={'trade_date': pl.Date, 'ts_code': pl.Utf8, 'turn': pl.Float64},
        orient='row',
    ).write_parquet(root / 'factors' / 'stock_turnover.parquet')
    return root


CODES = ['SUSP', 'ST', 'NEW', 'UP', 'DOWN', 'OK', 'DELIST']


def _filter(root, **kwargs):
    kwargs.setdefault('min_universe', 1)
    return TradabilityFilter(TradabilityConfig(enabled=True, **kwargs), root)


def test_each_item_excludes_expected_code(tmp_path):
    root = _store(tmp_path)
    cases = {
        'suspend': ('SUSP', {}),
        'st': ('ST', {}),
        'new_listing': ('NEW', {}),
        'limit_up': ('UP', {}),
        'limit_down': ('DOWN', {}),
    }
    for item, (expected, extra) in cases.items():
        flt = _filter(root, delisting=False,
                      **{name: name == item for name in cases} | extra)
        result = flt.filter_day(DAY, CODES)
        assert result.codes == {expected}, (item, result.codes)
        assert len(result.by_item[item]) == 1


def test_delisting_and_thresholds(tmp_path):
    root = _store(tmp_path)
    flt = _filter(root, suspend=False, st=False, new_listing=False,
                  limit_up=False, limit_down=False,
                  min_amount=5e7, min_turnover=0.002)
    result = flt.filter_day(
        DAY, CODES,
        amount={'OK': 1e8, 'ST': 1e6},
        turnover={'OK': 0.02, 'SUSP': 0.001})
    assert result.by_item['delisting'] == {'DELIST'}
    assert 'DELIST' in result.codes
    assert 'ST' in result.codes          # 成交额 100 万 < 5000 万
    assert 'SUSP' in result.codes        # 换手 0.1% < 0.2%


def test_all_off_is_noop_and_reads_nothing(tmp_path):
    empty = tmp_path / 'nowhere'          # 目录不存在，若读文件必然报错
    flt = TradabilityFilter(TradabilityConfig(enabled=False), empty)
    result = flt.filter_day(DAY, CODES)
    assert result.codes == frozenset() and result.by_item == {}

    flt2 = TradabilityFilter(
        TradabilityConfig(enabled=True, suspend=False, st=False,
                          delisting=False, new_listing=False,
                          limit_up=False, limit_down=False), empty)
    assert flt2.filter_day(DAY, CODES).codes == frozenset()


def test_missing_limit_table_skips_items_with_warning(tmp_path, caplog):
    root = _store(tmp_path)
    (root / 'meta' / 'limit' / 'daily' / 'limit_daily_202301.parquet').unlink()
    flt = _filter(root)
    result = flt.filter_day(DAY, CODES)
    assert result.codes == {'DELIST'}       # 只有不依赖涨跌停表的退市项生效
    skipped = {s.item for s in result.statuses if not s.applied}
    assert {'suspend', 'st', 'new_listing', 'limit_up', 'limit_down'} <= skipped


def test_small_cross_section_skips_everything(tmp_path):
    root = _store(tmp_path)
    flt = _filter(root, min_universe=100)
    result = flt.filter_day(DAY, CODES)
    assert result.codes == frozenset() and result.by_item == {}


def test_latest_mean_amount_requires_full_window():
    import numpy as np
    dates = np.array(DAYS, dtype='datetime64[D]')
    values = np.array([1.0, 2.0, 3.0])
    assert latest_mean_amount(dates, values, DAY, 3) == pytest.approx(2.0)
    assert latest_mean_amount(dates, values, DAY, 4) is None


def test_research_and_live_defaults_are_separate():
    assert TradabilityConfig.research_default().enabled is False
    assert TradabilityConfig.live_default().enabled is True
    from src.advice.config import TradabilityFilterConfig

    assert TradabilityFilterConfig().enabled is True
