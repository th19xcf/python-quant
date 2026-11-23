"""LimitTableRules：新表优先、缺行回退 A 侧自算、开关强制自算。

只用 tmp_path 下的月份 parquet，不读正式 data/local_store。
"""
from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.backtest.engine.limit_table_rules import LimitTableRules, limit_table_enabled

BAR = {'prev_close': 10.0, 'open_price': 10.0, 'high': 10.2, 'low': 9.9, 'close': 10.1,
       'volume': 1e6, 'ts_code': '600000.SH', 'is_st': False}


@pytest.fixture(autouse=True)
def _reset_stats(monkeypatch):
    monkeypatch.delenv('QUANT_LIMIT_INPUT', raising=False)
    LimitTableRules.STATS.update(calls=0, hit=0, fallback=0)


def _write_month(root, rows):
    path = root / 'meta' / 'limit' / 'daily' / 'limit_daily_202406.parquet'
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows, schema={'trade_date': pl.Date, 'ts_code': pl.Utf8,
                               'can_buy': pl.Boolean, 'can_sell': pl.Boolean,
                               'suspended': pl.Boolean}).write_parquet(path)


def test_switch_defaults_to_table_and_self_disables(monkeypatch):
    assert limit_table_enabled()
    monkeypatch.setenv('QUANT_LIMIT_INPUT', ' SELF ')
    assert not limit_table_enabled()


def test_table_row_decides_block_reason(tmp_path):
    _write_month(tmp_path, [
        {'trade_date': date(2024, 6, 3), 'ts_code': '600000.SH',
         'can_buy': False, 'can_sell': True, 'suspended': False},
        {'trade_date': date(2024, 6, 4), 'ts_code': '600000.SH',
         'can_buy': True, 'can_sell': False, 'suspended': False},
        {'trade_date': date(2024, 6, 5), 'ts_code': '600000.SH',
         'can_buy': True, 'can_sell': True, 'suspended': True},
    ])
    rules = LimitTableRules(store_root=tmp_path, mode='table')

    # 表里写「买不进」即使自算可成交也以表为准
    assert rules.block_reason('buy', trade_date=date(2024, 6, 3), **BAR) == 'limit_up'
    assert rules.block_reason('sell', trade_date=date(2024, 6, 3), **BAR) is None
    assert rules.block_reason('sell', trade_date='2024-06-04', **BAR) == 'limit_down'
    assert rules.block_reason('buy', trade_date='2024-06-05 00:00:00', **BAR) == 'suspended'
    assert rules.is_tradable('buy', trade_date=date(2024, 6, 4), **BAR)
    assert LimitTableRules.STATS == {'calls': 5, 'hit': 5, 'fallback': 0}


def test_missing_row_month_or_file_falls_back_to_self_compute(tmp_path):
    _write_month(tmp_path, [
        {'trade_date': date(2024, 6, 3), 'ts_code': '000001.SZ',
         'can_buy': False, 'can_sell': False, 'suspended': False},
    ])
    rules = LimitTableRules(store_root=tmp_path, mode='table')

    # 代码不在表内 → 自算（开盘平开，可成交）
    assert rules.block_reason('buy', trade_date=date(2024, 6, 3), **BAR) is None
    # 月份文件不存在 → 自算
    assert rules.block_reason('buy', trade_date=date(2024, 7, 1), **BAR) is None
    # 自算判定照常生效：开盘一字涨停买不进
    up = dict(BAR, open_price=11.0, high=11.0, low=11.0, close=11.0)
    assert rules.block_reason('buy', trade_date=date(2024, 7, 1), **up) == 'limit_up'
    assert LimitTableRules.STATS['fallback'] == 3
    assert LimitTableRules.STATS['hit'] == 0


def test_self_mode_and_missing_keys_skip_table(tmp_path):
    _write_month(tmp_path, [
        {'trade_date': date(2024, 6, 3), 'ts_code': '600000.SH',
         'can_buy': False, 'can_sell': False, 'suspended': True},
    ])
    forced = LimitTableRules(store_root=tmp_path, mode='self')
    assert forced.block_reason('buy', trade_date=date(2024, 6, 3), **BAR) is None

    table = LimitTableRules(store_root=tmp_path, mode='table')
    no_date = table.block_reason('buy', trade_date=None, **BAR)
    no_code = table.block_reason('buy', trade_date=date(2024, 6, 3),
                                 **dict(BAR, ts_code=''))
    assert no_date is None and no_code is None
    assert LimitTableRules.STATS['hit'] == 0


def test_month_cache_reloads_only_on_month_change(tmp_path, monkeypatch):
    _write_month(tmp_path, [
        {'trade_date': date(2024, 6, 3), 'ts_code': '600000.SH',
         'can_buy': True, 'can_sell': True, 'suspended': False},
    ])
    reads = []
    original = pl.read_parquet
    monkeypatch.setattr(pl, 'read_parquet',
                        lambda *a, **k: reads.append(a[0]) or original(*a, **k))
    rules = LimitTableRules(store_root=tmp_path, mode='table')
    for _ in range(3):
        rules.block_reason('buy', trade_date=date(2024, 6, 3), **BAR)
    assert len(reads) == 1
    rules.block_reason('buy', trade_date=date(2024, 7, 1), **BAR)   # 换月，文件缺失
    rules.block_reason('buy', trade_date=date(2024, 6, 3), **BAR)   # 换回 6 月需重读
    assert len(reads) == 2


def test_env_mode_used_when_not_passed(tmp_path, monkeypatch):
    monkeypatch.setenv('QUANT_LIMIT_INPUT', 'self')
    assert LimitTableRules(store_root=tmp_path).mode == 'self'
    monkeypatch.delenv('QUANT_LIMIT_INPUT')
    assert LimitTableRules(store_root=tmp_path).mode == 'table'
