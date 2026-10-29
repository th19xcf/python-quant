"""复权因子批量流程测试：分块预取 + 每块单事务。"""

from contextlib import contextmanager
from datetime import date

import polars as pl

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
