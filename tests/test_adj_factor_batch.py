"""复权因子批量流程测试：分块预取 + 每块单事务。"""

from contextlib import contextmanager
from datetime import date

import polars as pl
import pytest

from src.data.adj_factor_calculator import AdjFactorCalculator


class _FakeSession:
    def rollback(self):
        pass


class _FakeDB:
    def __init__(self):
        self.transactions = 0

    def get_session(self):
        return _FakeSession()

    def transaction(self):
        self.transactions += 1
        self._call_count = getattr(self, '_call_count', 0)

        @contextmanager
        def _ctx():
            yield self

        return _ctx()


def _prices(code: str) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [code, code],
        'trade_date': [date(2024, 1, 2), date(2024, 1, 3)],
        'open': [1.0, 1.0], 'high': [1.0, 1.0],
        'low': [1.0, 1.0], 'close': [1.0, 2.0],
    })


def test_calculate_factors_skips_no_ex_rights_event():
    """no_ex_rights=true 的事件不参与因子计算，但事件行仍可正常读取。"""
    prices = pl.DataFrame({
        'ts_code': ['600306.SH'] * 3,
        'trade_date': [date(2023, 12, 22), date(2023, 12, 25), date(2023, 12, 26)],
        'open': [10.0, 10.0, 9.0],
        'close': [10.0, 10.0, 9.0],
    })
    dividends = pl.DataFrame({
        'ts_code': ['600306.SH'],
        'ex_date': [date(2023, 12, 26)],
        'cash_div': [0.0],
        'share_div': [0.85],
        'rights_issue_ratio': [0.0],
        'rights_issue_price': [0.0],
        'no_ex_rights': [True],
    })

    result = AdjFactorCalculator.calculate_factors(prices, dividends)

    # 事件被排除：三行因子全为 1.0，除权日不再出现人为跳变
    assert result['qfq_factor'].to_list() == [1.0, 1.0, 1.0]
    assert result['hfq_factor'].to_list() == [1.0, 1.0, 1.0]
    # 输入事件行未被丢弃/改写，快照仍可正常读取
    assert dividends.height == 1
    assert dividends['no_ex_rights'].to_list() == [True]

    # 对照：同一事件不标记时正常参与计算
    control = AdjFactorCalculator.calculate_factors(
        prices, dividends.with_columns(pl.lit(False).alias('no_ex_rights')))
    f = 10.0 / (10.0 * (1.0 + 0.85))
    assert control['qfq_factor'].to_list() == pytest.approx([f, f, 1.0])
    assert control['hfq_factor'].to_list() == pytest.approx([1.0, 1.0, 1.0 + 0.85])


def test_calculate_factors_share_div_fix_overrides_original():
    """share_div_fix 非空时覆盖原始 share_div，原始值仍保留在输入帧里。"""
    prices = pl.DataFrame({
        'ts_code': ['600306.SH'] * 2,
        'trade_date': [date(2023, 12, 25), date(2023, 12, 26)],
        'open': [10.0, 9.0], 'close': [10.0, 9.0],
    })
    dividends = pl.DataFrame({
        'ts_code': ['600306.SH'], 'ex_date': [date(2023, 12, 26)],
        'cash_div': [0.0], 'share_div': [0.85],
        'rights_issue_ratio': [0.0], 'rights_issue_price': [0.0],
        'no_ex_rights': [False],
        'share_div_fix': [0.471744], 'cash_div_fix': [None],
    })

    result = AdjFactorCalculator.calculate_factors(prices, dividends)

    f = 1.0 / (1.0 + 0.471744)
    assert result['qfq_factor'].to_list() == pytest.approx([f, 1.0])
    assert result['hfq_factor'].to_list() == pytest.approx([1.0, 1.0 + 0.471744])
    # 原始 share_div 不被改写
    assert dividends['share_div'].to_list() == [0.85]


def test_calculate_factors_no_ex_rights_ignores_fix_columns():
    """no_ex_rights=true 优先级最高：即使 *_fix 有值也不参与计算。"""
    prices = pl.DataFrame({
        'ts_code': ['600306.SH'] * 2,
        'trade_date': [date(2023, 12, 25), date(2023, 12, 26)],
        'open': [10.0, 10.0], 'close': [10.0, 10.0],
    })
    dividends = pl.DataFrame({
        'ts_code': ['600306.SH'], 'ex_date': [date(2023, 12, 26)],
        'cash_div': [0.0], 'share_div': [0.85],
        'rights_issue_ratio': [0.0], 'rights_issue_price': [0.0],
        'no_ex_rights': [True],
        'share_div_fix': [0.5], 'cash_div_fix': [0.1],
    })

    result = AdjFactorCalculator.calculate_factors(prices, dividends)

    assert result['qfq_factor'].to_list() == [1.0, 1.0]
    assert result['hfq_factor'].to_list() == [1.0, 1.0]
    # 事件行与修正值原样可读
    assert dividends['share_div_fix'].to_list() == [0.5]
    assert dividends['cash_div_fix'].to_list() == [0.1]


def test_calculate_factors_cash_div_fix_overrides_original():
    """cash_div_fix 非空时覆盖原始 cash_div（601258 补漏记 0.04）。"""
    prices = pl.DataFrame({
        'ts_code': ['601258.SH'] * 2,
        'trade_date': [date(2019, 12, 27), date(2019, 12, 30)],
        'open': [1.46, 1.43], 'close': [1.46, 1.45],
    })
    dividends = pl.DataFrame({
        'ts_code': ['601258.SH'], 'ex_date': [date(2019, 12, 30)],
        'cash_div': [0.0], 'share_div': [0.56416],
        'rights_issue_ratio': [0.0], 'rights_issue_price': [0.0],
        'no_ex_rights': [False],
        'share_div_fix': [0.0], 'cash_div_fix': [0.04],
    })

    result = AdjFactorCalculator.calculate_factors(prices, dividends)

    f = (1.46 - 0.04) / 1.46
    assert result['qfq_factor'].to_list() == pytest.approx([f, 1.0])
    assert result['hfq_factor'].to_list() == pytest.approx([1.0, 1.46 / 1.42])
    assert dividends['cash_div'].to_list() == [0.0]


def test_calculate_factors_merges_same_day_split_rows():
    """同一除权日被拆成「配股」「送股」两行时按合并事件计算，不逐行连乘。"""
    prices = pl.DataFrame({
        'ts_code': ['000002.SZ'] * 2,
        'trade_date': [date(1991, 5, 27), date(1991, 6, 8)],
        'close': [11.29, 7.95],
    })
    dividends = pl.DataFrame({
        'ts_code': ['000002.SZ'] * 2,
        'ex_date': [date(1991, 6, 8)] * 2,
        'cash_div': [0.0, 0.0],
        'share_div': [0.0, 0.2],
        'rights_issue_ratio': [0.5, 0.0],
        'rights_issue_price': [4.4, 0.0],
        'no_ex_rights': [False, False],
    })

    result = AdjFactorCalculator.calculate_factors(prices, dividends)

    merged = (11.29 + 4.4 * 0.5) / (11.29 * (1 + 0.2 + 0.5))
    assert result['qfq_factor'].to_list() == pytest.approx([merged, 1.0])
    assert result['hfq_factor'].to_list() == pytest.approx([1.0, 1.0 / merged])


def test_batch_prefetches_per_chunk_and_commits_once(monkeypatch):
    db = _FakeDB()
    calculator = AdjFactorCalculator(db, tdx_handler=None)
    calls = {'dividends': 0, 'prices': 0, 'save': 0, 'update': 0}

    empty_dividends = pl.DataFrame(schema={
        'ts_code': pl.String, 'ex_date': pl.Date,
        'cash_div': pl.Float64, 'share_div': pl.Float64,
        'rights_issue_ratio': pl.Float64, 'rights_issue_price': pl.Float64,
    })

    def fake_dividends(codes):
        calls['dividends'] += 1
        return empty_dividends

    def fake_prices(codes, start, end):
        calls['prices'] += 1
        return pl.concat([_prices(code) for code in codes])

    monkeypatch.setattr(calculator, '_get_dividends_bulk', fake_dividends)
    monkeypatch.setattr(calculator, '_get_prices_bulk', fake_prices)
    monkeypatch.setattr(calculator, 'save_adj_factors',
                        lambda df, commit=False: calls.__setitem__('save', calls['save'] + 1))
    monkeypatch.setattr(calculator, 'update_stock_daily_adj',
                        lambda df, commit=False: calls.__setitem__('update', calls['update'] + 1))

    calculator.batch_calculate_and_save(['A', 'B', 'C'], chunk_size=2)

    # 3 只股票分 2 块：每块各 1 次分红查询、1 次价格查询、1 次事务
    assert calls['dividends'] == 2
    assert calls['prices'] == 2
    assert calls['save'] == 2
    assert calls['update'] == 2
    assert db.transactions == 2
