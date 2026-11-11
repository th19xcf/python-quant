#!/usr/bin/env python3

"""选池过滤：逐条规则的边界用例（含 1.5 元 / 120 交易日 / 1,000 万三条边界）。"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from src.data.universe_filter import (
    DOC_PATH,
    RULE_BOARD,
    RULE_DELISTED,
    RULE_DELISTING,
    RULE_FINANCIAL_RISK,
    RULE_ILLIQUID,
    RULE_LOW_CIRC_MV,
    RULE_LOW_PRICE,
    RULE_NEW_LISTING,
    RULE_NO_QUOTE,
    RULE_ORDER,
    RULE_RESUMPTION,
    RULE_ST,
    RULE_SUPERSEDED,
    RULE_SUSPENDED,
    RULE_SUSPENSION_COUNT,
    RULES_VERSION,
    UniverseFilterConfig,
    filter_universe,
)


def _weekdays(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


DATES = _weekdays(date(2025, 10, 1), date(2026, 11, 9))
AS_OF = DATES[-1]


def _write_index(root: Path, dates: list[date]) -> None:
    """基准指数日历（交易日历的权威来源）。"""
    directory = root / 'quotes' / 'index' / 'sh'
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': dates,
                  'close': [3000.0] * len(dates)}).write_parquet(
        directory / '000001.SH.parquet')


def _write_daily_index(root: Path, spans: dict[str, tuple[date, date]]) -> None:
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    rows = {
        'ts_code': list(spans),
        'data_type': ['stock'] * len(spans),
        'market': [code.rsplit('.', 1)[-1].lower() for code in spans],
        'rows': [len(DATES)] * len(spans),
        'start': [span[0] for span in spans.values()],
        'end': [span[1] for span in spans.values()],
    }
    pl.DataFrame(rows).write_parquet(root / 'meta' / 'daily_index.parquet')


def _write_quotes(root: Path, code: str, dates: list[date], *,
                  close: float = 10.0, amount: float = 5.0e7) -> None:
    market = code.rsplit('.', 1)[-1].lower()
    directory = root / 'quotes' / 'stock' / market
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': dates,
                  'close': [close] * len(dates),
                  'amount': [amount] * len(dates)}).write_parquet(
        directory / f'{code}.parquet')


def _write_suspension(root: Path, rows: list[tuple[str, date]]) -> None:
    directory = root / 'meta' / 'suspension'
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': [item[0] for item in rows],
                  'trade_date': [item[1] for item in rows]}).write_parquet(
        directory / 'suspension_daily.parquet')


def _write_st_intervals(root: Path, codes: list[str], start: date,
                        end: date) -> None:
    directory = root / 'meta' / 'st'
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': codes,
        'start_date': [start] * len(codes),
        'end_date': [end] * len(codes),
        'flag': ['*ST'] * len(codes),
        'source': ['test'] * len(codes),
        'precision': ['day'] * len(codes),
        'fetched_at': [datetime(2026, 1, 1)] * len(codes),
    }, schema_overrides={'fetched_at': pl.Datetime('us')}).write_parquet(
        directory / 'st_intervals.parquet')


def _write_stock_basic(root: Path,
                       rows: list[tuple[str, str, date | None]]) -> None:
    """`meta/stock_basic.parquet`：退市判定的唯一数据源（status + delist_date）。"""
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [row[0] for row in rows],
        'name': [row[0] for row in rows],
        'status': [row[1] for row in rows],
        'delist_date': [row[2] for row in rows],
    }).write_parquet(root / 'meta' / 'stock_basic.parquet')


def _write_name_history(
        root: Path, rows: list[tuple[str, str, date, date | None]]) -> None:
    """`meta/name_history.parquet`：PIT 历史简称（退市判定第 ② 路）。"""
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [row[0] for row in rows],
        'name': [row[1] for row in rows],
        'start_date': [row[2] for row in rows],
        'end_date': [row[3] for row in rows],
    }).write_parquet(root / 'meta' / 'name_history.parquet')


def _setup(root: Path, codes: list[str], *,
           starts: dict[str, date] | None = None) -> None:
    _write_index(root, DATES)
    spans = {code: ((starts or {}).get(code, DATES[0]), AS_OF) for code in codes}
    _write_daily_index(root, spans)
    for code in codes:
        first = spans[code][0]
        _write_quotes(root, code, [day for day in DATES if day >= first])


def test_st_filter(tmp_path: Path) -> None:
    codes = ['600900.SH', '600901.SH']
    _setup(tmp_path, codes)
    _write_st_intervals(tmp_path, ['600900.SH'], date(2026, 1, 1), date(2026, 12, 31))
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.kept == ['600901.SH']
    assert result.dropped[RULE_ST] == ['600900.SH']


def test_suspension_rules(tmp_path: Path) -> None:
    codes = ['600910.SH', '600911.SH', '600912.SH']
    _setup(tmp_path, codes)
    rows: list[tuple[str, date]] = [('600910.SH', AS_OF)]
    rows += [('600911.SH', day) for day in DATES[-8:-2]]   # 近 20 日 6 天
    rows += [('600912.SH', day) for day in DATES[-9:-4]]   # 近 20 日 5 天（边界内）
    _write_suspension(tmp_path, rows)
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.dropped[RULE_SUSPENDED] == ['600910.SH']
    assert result.dropped[RULE_SUSPENSION_COUNT] == ['600911.SH']
    assert result.kept == ['600912.SH']


def test_resumption_day(tmp_path: Path) -> None:
    codes = ['600920.SH']
    _setup(tmp_path, codes)
    _write_suspension(tmp_path, [('600920.SH', DATES[-2])])
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.dropped[RULE_RESUMPTION] == ['600920.SH']
    assert result.kept == []


def test_new_listing_boundary(tmp_path: Path) -> None:
    # 恰好 120 个交易日（含上市首日）保留；119 个剔除
    exact = DATES[-120]
    short = DATES[-119]
    codes = ['600930.SH', '600931.SH']
    _setup(tmp_path, codes, starts={'600930.SH': exact, '600931.SH': short})
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.kept == ['600930.SH']
    assert result.dropped[RULE_NEW_LISTING] == ['600931.SH']


def test_price_boundary(tmp_path: Path) -> None:
    codes = ['600940.SH', '600941.SH']
    _setup(tmp_path, codes)
    _write_quotes(tmp_path, '600940.SH', DATES, close=1.5)
    _write_quotes(tmp_path, '600941.SH', DATES, close=1.49)
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.kept == ['600940.SH']
    assert result.dropped[RULE_LOW_PRICE] == ['600941.SH']


def test_liquidity_boundary(tmp_path: Path) -> None:
    codes = ['600950.SH', '600951.SH']
    _setup(tmp_path, codes)
    _write_quotes(tmp_path, '600950.SH', DATES, amount=1.0e7)
    _write_quotes(tmp_path, '600951.SH', DATES, amount=9.99e6)
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.kept == ['600950.SH']
    assert result.dropped[RULE_ILLIQUID] == ['600951.SH']


def test_board_rules(tmp_path: Path) -> None:
    codes = ['900001.SH', '400001.OC', '920001.BJ', '688001.SH', '300001.SZ']
    _setup(tmp_path, codes)
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert set(result.dropped[RULE_BOARD]) == {'900001.SH', '400001.OC', '920001.BJ'}
    assert set(result.kept) == {'688001.SH', '300001.SZ'}

    keep_bse = filter_universe(AS_OF, codes, store_root=tmp_path,
                               config=UniverseFilterConfig(include_bse=True))
    assert '920001.BJ' in keep_bse.kept


def test_delisting_rules(tmp_path: Path) -> None:
    """退市判定只认 stock_basic：已退市 / 整理期 / 正常三态。"""
    codes = ['600960.SH', '600961.SH', '600962.SH', '600963.SH']
    _setup(tmp_path, codes)
    _write_stock_basic(tmp_path, [
        ('600960.SH', 'D', DATES[-5]),                  # 已退市（delist_date 早于 as_of）
        ('600961.SH', 'L', AS_OF + timedelta(days=10)),  # 整理期（剩余 ≈7 个交易日 ≤30）
        ('600962.SH', 'L', None),                        # 正常在市
        ('600963.SH', 'L', AS_OF + timedelta(days=400)),  # 远期退市公告，不在整理期
    ])
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.dropped[RULE_DELISTED] == ['600960.SH']
    assert result.dropped[RULE_DELISTING] == ['600961.SH']
    assert result.kept == ['600962.SH', '600963.SH']
    assert result.unknowns['delist'] == 0


def test_delisting_dual_path(tmp_path: Path) -> None:
    """双路并集：已退市 / 整理期（PIT 简称含退）/ 正常 / D 无日期 fail-closed。

    另附一条「简称在未来才变退」的反例，证明第 ② 路只认 ``as_of`` 当日已生效段，
    不拿当前简称倒推历史（无前视）。
    """
    codes = ['600980.SH', '600981.SH', '600982.SH', '600983.SH', '600984.SH']
    _setup(tmp_path, codes)
    _write_stock_basic(tmp_path, [
        ('600980.SH', 'D', DATES[-5]),   # ① 已退市（delist_date 早于 as_of）
        ('600981.SH', 'L', None),        # ② 整理期：只有 PIT 简称能识别
        ('600982.SH', 'L', None),        # ③ 正常
        ('600983.SH', 'D', None),        # ④ status=D 无 delist_date → fail-closed
        ('600984.SH', 'L', None),        # ⑤ 简称未来才变「退」→ 当日不得命中
    ])
    _write_name_history(tmp_path, [
        ('600981.SH', 'ST某某', date(2025, 1, 1), date(2026, 10, 22)),
        ('600981.SH', '某某退', date(2026, 10, 23), None),
        ('600982.SH', '某某股份', date(2025, 1, 1), None),
        ('600984.SH', '某某科技', date(2025, 1, 1), None),
        ('600984.SH', '某某退', date(2026, 12, 1), None),
    ])
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.dropped[RULE_DELISTED] == ['600980.SH', '600983.SH']
    assert result.dropped[RULE_DELISTING] == ['600981.SH']
    assert result.kept == ['600982.SH', '600984.SH']
    assert result.unknowns['delist_name'] == 0

    off = filter_universe(
        AS_OF, codes, store_root=tmp_path,
        config=UniverseFilterConfig(enable_name_based_delisting=False))
    assert off.dropped[RULE_DELISTING] == []
    assert '600981.SH' in off.kept


def test_delisted_without_date_is_dropped(tmp_path: Path) -> None:
    """status=D 但 delist_date 为空，同样按已退市剔除（fail-closed）。"""
    codes = ['600970.SH']
    _setup(tmp_path, codes)
    _write_stock_basic(tmp_path, [('600970.SH', 'D', None)])
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert result.dropped[RULE_DELISTED] == ['600970.SH']
    assert result.kept == []


def test_superseded_code(tmp_path: Path) -> None:
    old, new = '000043.SZ', '000002.SZ'
    _setup(tmp_path, [old, new])
    pl.DataFrame({'old_ts_code': [old], 'new_ts_code': [new]}).write_parquet(
        tmp_path / 'meta' / 'code_migration.parquet')
    result = filter_universe(AS_OF, [old, new], store_root=tmp_path)
    assert result.dropped[RULE_SUPERSEDED] == [old]
    assert result.kept == [new]


def _write_valuation(root: Path, code: str, circ_mv: float) -> None:
    directory = root / 'meta' / 'financials'
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': [code], 'trade_date': [AS_OF],
                  'close': [10.0], 'circ_mv': [circ_mv]}).write_parquet(
        directory / 'valuation_daily.parquet')


def test_market_cap_floor_off_by_default(tmp_path: Path) -> None:
    codes = ['600960.SH']
    _setup(tmp_path, codes)
    _write_valuation(tmp_path, '600960.SH', 5.0e8)
    default = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert default.kept == codes
    enabled = filter_universe(
        AS_OF, codes, store_root=tmp_path,
        config=UniverseFilterConfig(enable_circ_mv_floor=True))
    assert enabled.dropped[RULE_LOW_CIRC_MV] == codes


def _write_financials(root: Path, code: str, equity: float) -> None:
    directory = root / 'meta' / 'financials'
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [code], 'end_date': [date(2025, 12, 31)],
        'ann_date': [date(2026, 4, 20)], 'total_equity': [equity],
    }).write_parquet(directory / 'balance.parquet')
    pl.DataFrame({
        'ts_code': [code] * 2,
        'end_date': [date(2024, 12, 31), date(2025, 12, 31)],
        'ann_date': [date(2025, 4, 20), date(2026, 4, 20)],
        'parent_net_profit': [-1.0e7, -2.0e7],
    }).write_parquet(directory / 'income.parquet')


def test_financial_risk_off_by_default_and_on(tmp_path: Path) -> None:
    codes = ['600970.SH']
    _setup(tmp_path, codes)
    _write_financials(tmp_path, '600970.SH', equity=5.0e8)
    default = filter_universe(AS_OF, codes, store_root=tmp_path)
    assert default.kept == codes
    enabled = filter_universe(
        AS_OF, codes, store_root=tmp_path,
        config=UniverseFilterConfig(enable_financial_risk=True))
    assert enabled.dropped[RULE_FINANCIAL_RISK] == codes


def test_no_data_reports_no_quotes(tmp_path: Path) -> None:
    result = filter_universe(AS_OF, ['600000.SH'], store_root=tmp_path)
    assert result.kept == []
    assert result.dropped[RULE_NO_QUOTE] == ['600000.SH']


def test_result_carries_version_and_doc_path(tmp_path: Path) -> None:
    codes = ['600980.SH']
    _setup(tmp_path, codes)
    result = filter_universe(AS_OF, codes, store_root=tmp_path)
    payload = result.as_dict()
    assert payload['version'] == RULES_VERSION
    assert payload['doc_path'] == DOC_PATH
    assert list(payload['rule_counts']) == list(RULE_ORDER)
    assert result.kept_count == 1
