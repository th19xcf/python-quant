"""分红覆盖检查：缺分红必须在输出里显式标注，不能被静默忽略。

覆盖查询用临时因子目录（不读真实数据）；整链路用例用合成行情跑
``run_rule``，旧代码输出里没有 ``dividend_coverage`` 字段，本用例在其上必红。
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.quant.strategy_lab import backtest, coverage, switch_timing
from src.quant.strategy_lab.rules import validate_rule

NUM = '510999.SH'
DEN = '510998.SH'


def _dividend_frame(codes: list[str]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': codes,
        'ex_date': [date(2024, 6, 20)] * len(codes),
        'record_date': [date(2024, 6, 19)] * len(codes),
        'cash_amount': [0.05] * len(codes),
        'source': ['test'] * len(codes),
        'fetched_at': [date(2024, 6, 21)] * len(codes),
    })


def _factor_frame(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [row[0] for row in rows],
        'trade_date': [row[1] for row in rows],
        'qfq_factor': [1.0] * len(rows),
        'hfq_factor': [row[2] for row in rows],
    })


# ---------------------------------------------------------------------------
# 覆盖查询
# ---------------------------------------------------------------------------

def test_no_record_and_no_step_is_uncovered(tmp_path):
    factors = tmp_path / 'factors'
    factors.mkdir()
    _dividend_frame(['510300.SH']).write_parquet(factors / 'fund_dividend.parquet')
    _factor_frame([('510300.SH', date(2024, 1, 2), 1.0),
                   ('588000.SH', date(2024, 1, 2), 1.0)]).write_parquet(
        factors / 'fund_adj_factor.parquet')

    result = coverage.dividend_coverage(['510300.SH', '588000.SH'], root=factors)

    assert result['ok'] is False
    assert result['checked'] == 2
    assert [item['code'] for item in result['issues']] == ['588000.SH']
    assert result['issues'][0]['reason'] == '无分红记录，且复权因子无阶跃'
    assert result['message'] == '1 只标的分红覆盖不足，收益可能低估'


def test_dividend_record_covers(tmp_path):
    factors = tmp_path / 'factors'
    factors.mkdir()
    _dividend_frame(['510300.SH', '588000.SH']).write_parquet(
        factors / 'fund_dividend.parquet')

    result = coverage.dividend_coverage(['510300.SH', '588000.SH'], root=factors)

    assert result['ok'] is True
    assert result['issues'] == []
    assert result['message'] == ''


def test_factor_step_covers_even_without_dividend_table(tmp_path):
    factors = tmp_path / 'factors'
    factors.mkdir()
    _factor_frame([('588000.SH', date(2024, 1, 2), 1.0),
                   ('588000.SH', date(2024, 5, 8), 1.0312)]).write_parquet(
        factors / 'fund_adj_factor.parquet')

    result = coverage.dividend_coverage(['588000.SH'], root=factors)

    assert result['ok'] is True
    assert result['checked'] == 1


def test_missing_tables_report_reason(tmp_path):
    result = coverage.dividend_coverage(['510500.SH'], root=tmp_path / 'factors')

    assert result['ok'] is False
    assert '分红表 fund_dividend.parquet 缺失' in result['issues'][0]['reason']


def test_stock_codes_are_out_of_scope(tmp_path):
    result = coverage.dividend_coverage(['000001.SZ', '600000.SH'],
                                        root=tmp_path / 'factors')

    assert result['ok'] is True
    assert result['checked'] == 0
    assert result['message'] == ''


# ---------------------------------------------------------------------------
# 整链路：合成行情跑 run_rule
# ---------------------------------------------------------------------------

def _weekdays(count: int) -> list[date]:
    days: list[date] = []
    day = date(2024, 1, 2)
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


def _rule(days: list[date]) -> dict:
    return validate_rule({
        'rule_version': 'strategy_lab/v1',
        'rule_type': 'asset_switch',
        'name': '分红覆盖检查',
        'period': {'start': days[0].isoformat(), 'end': days[-1].isoformat()},
        'legs': [{'code': NUM, 'label': '分子'}, {'code': DEN, 'label': '分母'}],
        'switch': {'numerator': NUM, 'denominator': DEN, 'window': 3,
                   'entry': 0.02, 'exit': 0.02, 'min_hold_days': 2,
                   'initial': 'denominator'},
        'positions': {'mode': 'all_in'},
        'execution': {'price': 'next_open', 'delay_days': 1},
    })


def _run_switch(tmp_path, monkeypatch, factors):
    days = _weekdays(40)
    paths = {}
    for code in (NUM, DEN):
        opens = ([round(4.0 * 1.04 ** i, 4) for i in range(len(days))]
                 if code == NUM else [1.0] * len(days))
        path = tmp_path / f'{code}.parquet'
        pl.DataFrame({
            'date': days,
            'open': opens,
            'high': [price * 1.005 for price in opens],
            'low': [price * 0.995 for price in opens],
            'close': opens,
            'volume': [1e8] * len(days),
            'hfq_factor': [2.0] * len(days),
        }).write_parquet(path)
        paths[code] = path
    monkeypatch.setattr(switch_timing, '_quote_path', lambda code: paths[code])
    monkeypatch.setattr(backtest, '_load_st_status', lambda *a, **k: {})
    monkeypatch.setattr(backtest, '_engine_dividends', lambda codes: {})
    monkeypatch.setattr(backtest, 'load_benchmark_close', lambda code: {})
    monkeypatch.setattr(coverage, 'DEFAULT_FACTOR_DIR', factors)
    return backtest.run_rule(_rule(days))


def test_run_flags_missing_dividend(tmp_path, monkeypatch):
    factors = tmp_path / 'factors'
    factors.mkdir()
    # 分母有分红记录、分子没有：分子必须被标出来
    _dividend_frame([DEN]).write_parquet(factors / 'fund_dividend.parquet')

    payload = _run_switch(tmp_path, monkeypatch, factors)

    assert payload['status'] == 'ok'
    result = payload['dividend_coverage']
    assert result['ok'] is False
    assert [item['code'] for item in result['issues']] == [NUM]
    assert result['message'] == '1 只标的分红覆盖不足，收益可能低估'


def test_run_passes_when_covered(tmp_path, monkeypatch):
    factors = tmp_path / 'factors'
    factors.mkdir()
    _dividend_frame([NUM, DEN]).write_parquet(factors / 'fund_dividend.parquet')

    payload = _run_switch(tmp_path, monkeypatch, factors)

    assert payload['status'] == 'ok'
    result = payload['dividend_coverage']
    assert result['ok'] is True
    assert result['message'] == ''
    assert result['issues'] == []
