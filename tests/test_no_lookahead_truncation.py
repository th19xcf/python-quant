"""截断重跑防前视不变量（slow，只读本地真实列存）。

核心不变量：把原始输入「物理删掉」某一时点 ``t`` 之后的行后重跑，得到的
「时点 t 及以前」的结论，必须与全量数据重跑的结果逐行一致。不是给推理逻辑传
``as_of`` 做过滤，而是让输入文件本身只包含 ``<= t`` 的数据。

覆盖两条线：
1. ST 状态推断：``src/data/st_history.py``（区间表 + 覆盖窗口 + ``status_at``）
   与 ``src/data/st_price_infer.py``（行情反推疑似区间）。
2. ``src/data/quality/tradable_universe.py``（排除清单 + ``is_tradable``）。

抽样口径（确定性常量，不用随机种子）：
- 11 只股票：`000004/000017`（多次 ST 状态切换，跨 2006/2010/2020/2025）、
  `000024`（2015-12-07 退市）、`000562`（2014-12-09 退市，含跌停跳变）、
  `601268`（2014-04-28 退市）、`000425`（2011-03-16 跌停跳变）、
  `600052/600069/600078`（沪市，ST 分别落在 2007/2015/2021）、
  `600211`（S 股简称区间）、`600000/000001`（从未 ST 的对照）。
- 5 个基准指数（`000001.SH/399001.SZ/399002.SZ/399003.SZ/000300.SH`）：
  交易日历以它们为权威来源，必须一起截断才能得到 t 时点的日历。
- 3 只基金：`150190`（尾部/全段占位行情）、`150260`（2018-02-05 跳变）、
  `159100`（2026 年尾部占位）。

t 清单（6 个，覆盖不同年份、退市前后与 ST 切换前后）：
2008-06-30 / 2011-12-30 / 2014-12-31 / 2015-12-31 / 2022-12-30 / 2026-11-19。

比对范围：区间表按「起点 + 该时点是否仍处于区间内」投影；``status_at`` 按
状态/flag/疑似标记/区间起点；``is_tradable`` 与 ``exclusion_reasons`` 按
(原因码, 严重度, 末次交易日) 集合。行数类字段（expected_rows/missing_rows）
天然随截断变化，不参与比对。

已知不一致（本测试以 xfail 登记，未改任何推断逻辑）：
1. 沪市覆盖窗口的存在性取决于「该标的是否有任何 ST 公告」——全量构建会因未来的
   公告给 `600069.SH/600078.SH` 补出 2005 起的 coverage，t 时点截断构建则没有，
   于是同一日期从 ``non_st`` 变成 ``unknown``（``st_history.build_store`` 的
   ``all_codes`` 由未来公告集合决定）。
2. ``st_price_infer`` 开区间末端由 t 之后的价格决定：如 `000004.SZ` 2022-04-29
   段全量到 2023-06-26，t=2022-12-30 截断则止于 2022-12-21（前向 breach 搜索
   读到未来 K 线）。
3. ``stale_feed`` 是「构建快照级」判定：全量表的 ``last_trade_date`` 在未来，
   使该原因在历史日期被跳过；t 时点截断后 `000017.SZ` 等长期停牌标的在
   2008-06-30 被判不可交易，全量判可交易。
4. ``placeholder_price`` 用整段尾部判断：`150190.SZ` 的占位尾行在 2015-2020，
   全量表因此把它在 2008/2011 等上市前的日期也判为 block，截断表无此原因。
"""

from __future__ import annotations

import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from src.data import st_history, st_price_infer
from src.data.local_store.paths import resolve_store_root
from src.data.quality import tradable_universe as tu
from src.utils.config import get_config

pytestmark = pytest.mark.slow

REAL_ROOT = resolve_store_root(get_config())

STOCKS = [
    '000004.SZ', '000017.SZ', '000024.SZ', '000562.SZ', '000425.SZ',
    '600052.SH', '600069.SH', '600078.SH', '600211.SH', '600000.SH', '000001.SZ',
]
INDICES = ['000001.SH', '399001.SZ', '399002.SZ', '399003.SZ', '000300.SH']
FUNDS = ['150190.SZ', '150260.SZ', '159100.SZ']
ALL_CODES = STOCKS + INDICES + FUNDS
KIND = {code: ('index' if code in INDICES else 'fund' if code in FUNDS else 'stock')
        for code in ALL_CODES}

T_LIST = [
    date(2008, 6, 30),
    date(2011, 12, 30),
    date(2014, 12, 31),
    date(2015, 12, 31),
    date(2022, 12, 30),
    date(2026, 11, 19),
]
FULL_END = date(2026, 11, 20)

_REQUIRED = [
    'meta/daily_index.parquet',
    'meta/stock_basic.parquet',
    'meta/name_history.parquet',
    'meta/st/raw/sz_change_name.parquet',
    'meta/st/raw/cninfo_notices.parquet',
    'meta/st/raw/closure_evidence.parquet',
    'meta/st/raw/reviewed_conflicts.parquet',
    'meta/st/raw/reviewed_coverage.parquet',
    'meta/st/raw/reviewed_pending.parquet',
    'meta/st/raw/em_org_basic.parquet',
    'meta/st/supplement/supplement_intervals.parquet',
    'meta/st/supplement/supplement_coverage.parquet',
    'meta/st_status/st_status_history.parquet',
    'meta/trading_calendar/session_ledger.parquet',
    'factors/stock_dividend.parquet',
]


def _quote_path(root: Path, code: str) -> Path:
    return root / 'quotes' / KIND[code] / code[-2:].lower() / f'{code}.parquet'


def _as_date(value: object) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text[:8], fmt).date()
        except ValueError:
            continue
    return None


def _write(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


def _materialize(dst: Path, cutoff: date | None) -> None:  # noqa: C901 - 截断规则线性展开
    """把真实列存里抽样标的数据复制到临时目录；``cutoff`` 非空时物理删掉之后的行。"""
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    di = pl.read_parquet(REAL_ROOT / 'meta/daily_index.parquet')
    di = di.filter(pl.col('ts_code').is_in(ALL_CODES))
    di_rows = {row['ts_code']: row for row in di.to_dicts()}
    kept: list[dict] = []
    for code in ALL_CODES:
        src = _quote_path(REAL_ROOT, code)
        if not src.is_file():
            continue
        frame = pl.read_parquet(src)
        if cutoff is not None:
            frame = frame.filter(pl.col('date') <= cutoff)
        if frame.height == 0:
            continue
        out = _quote_path(dst, code)
        _write(out, frame)
        row = di_rows.get(code)
        if row is None:
            continue
        row = dict(row)
        row['rows'] = frame.height
        row['start'] = frame['date'].min()
        row['end'] = frame['date'].max()
        row['file_size'] = out.stat().st_size
        row['source_mtime_ns'] = out.stat().st_mtime_ns
        kept.append(row)
    _write(dst / 'meta/daily_index.parquet',
           pl.DataFrame(kept, schema=di.schema))

    basic = pl.read_parquet(REAL_ROOT / 'meta/stock_basic.parquet')
    basic = basic.filter(pl.col('ts_code').is_in(ALL_CODES))
    if cutoff is not None:
        basic = basic.with_columns(
            pl.when(pl.col('delist_date') > cutoff).then(None)
            .otherwise(pl.col('delist_date')).alias('delist_date'))
    _write(dst / 'meta/stock_basic.parquet', basic)

    names = pl.read_parquet(REAL_ROOT / 'meta/name_history.parquet')
    names = names.filter(pl.col('ts_code').is_in(ALL_CODES))
    if cutoff is not None:
        # t 时点不知道当前名称何时结束 -> 末段 end_date 置空
        names = names.filter(pl.col('start_date') <= cutoff).with_columns(
            pl.when(pl.col('end_date').is_null() | (pl.col('end_date') > cutoff))
            .then(None).otherwise(pl.col('end_date')).alias('end_date'))
    _write(dst / 'meta/name_history.parquet', names)

    stock_codes = [code[:6] for code in STOCKS]
    sz = pl.read_parquet(REAL_ROOT / 'meta/st/raw/sz_change_name.parquet')
    sz = sz.filter(pl.col('code').str.slice(0, 6).is_in(stock_codes))
    notices = pl.read_parquet(REAL_ROOT / 'meta/st/raw/cninfo_notices.parquet')
    notices = notices.filter(pl.col('sec_code').is_in(stock_codes))
    closure = pl.read_parquet(REAL_ROOT / 'meta/st/raw/closure_evidence.parquet')
    closure = closure.filter(pl.col('ts_code').is_in(ALL_CODES))
    conflicts = pl.read_parquet(REAL_ROOT / 'meta/st/raw/reviewed_conflicts.parquet')
    conflicts = conflicts.filter(pl.col('ts_code').is_in(ALL_CODES))
    review_cov = pl.read_parquet(REAL_ROOT / 'meta/st/raw/reviewed_coverage.parquet')
    review_cov = review_cov.filter(pl.col('ts_code').is_in(ALL_CODES))
    pending = pl.read_parquet(REAL_ROOT / 'meta/st/raw/reviewed_pending.parquet')
    pending = pending.filter(pl.col('ts_code').is_in(ALL_CODES))
    if cutoff is not None:
        sz = sz.filter(pl.col('change_date') <= cutoff)
        notices = notices.filter(pl.col('publish_date') <= cutoff)
        closure = closure.filter(pl.col('close_date') <= cutoff)
        conflicts = conflicts.filter(pl.col('start_date') <= cutoff).with_columns(
            pl.when(pl.col('end_date').is_not_null() & (pl.col('end_date') > cutoff))
            .then(None).otherwise(pl.col('end_date')).alias('end_date'))
        pending = pending.filter(pl.col('start_date') <= cutoff).with_columns(
            pl.when(pl.col('end_date').is_not_null() & (pl.col('end_date') > cutoff))
            .then(None).otherwise(pl.col('end_date')).alias('end_date'))
        review_cov = review_cov.filter(pl.col('coverage_start') <= cutoff)
        review_cov = review_cov.with_columns(
            pl.when(pl.col('coverage_end').is_null() | (pl.col('coverage_end') > cutoff))
            .then(pl.lit(cutoff)).otherwise(pl.col('coverage_end'))
            .alias('coverage_end'))

    raw_tables = {
        'sz_change_name': sz,
        'cninfo_notices': notices,
        'closure_evidence': closure,
        'reviewed_conflicts': conflicts,
        'reviewed_coverage': review_cov,
        'reviewed_pending': pending,
    }
    for name, frame in raw_tables.items():
        _write(dst / 'meta/st/raw' / f'{name}.parquet', frame)

    # 现简称/曾用名登记表没有日期列，无法按 t 截断；只保留抽样标的。
    em = pl.read_parquet(REAL_ROOT / 'meta/st/raw/em_org_basic.parquet')
    em = em.filter(pl.col('secucode').str.slice(0, 6).is_in(
        [code[:6] for code in ALL_CODES]))
    _write(dst / 'meta/st/raw/em_org_basic.parquet', em)

    supp_iv = pl.read_parquet(
        REAL_ROOT / 'meta/st/supplement/supplement_intervals.parquet')
    supp_iv = supp_iv.filter(pl.col('ts_code').is_in(ALL_CODES))
    supp_cov = pl.read_parquet(
        REAL_ROOT / 'meta/st/supplement/supplement_coverage.parquet')
    supp_cov = supp_cov.filter(pl.col('ts_code').is_in(ALL_CODES))
    if cutoff is not None:
        supp_iv = supp_iv.filter(pl.col('start_date') <= cutoff).with_columns(
            pl.when(pl.col('end_date').is_not_null() & (pl.col('end_date') > cutoff))
            .then(None).otherwise(pl.col('end_date')).alias('end_date'))
        supp_cov = supp_cov.filter(pl.col('coverage_start') <= cutoff)
        supp_cov = supp_cov.with_columns(
            pl.when(pl.col('coverage_end').is_null() | (pl.col('coverage_end') > cutoff))
            .then(pl.lit(cutoff)).otherwise(pl.col('coverage_end'))
            .alias('coverage_end'))
    _write(dst / 'meta/st/supplement/supplement_intervals.parquet', supp_iv)
    _write(dst / 'meta/st/supplement/supplement_coverage.parquet', supp_cov)

    snapshots = pl.read_parquet(
        REAL_ROOT / 'meta/st_status/st_status_history.parquet')
    snapshots = snapshots.filter(pl.col('code').is_in(ALL_CODES))
    if cutoff is not None:
        snapshots = snapshots.filter(pl.col('as_of') <= cutoff)
    _write(dst / 'meta/st_status/st_status_history.parquet', snapshots)

    ledger = pl.read_parquet(
        REAL_ROOT / 'meta/trading_calendar/session_ledger.parquet')
    if cutoff is not None:
        ledger = ledger.filter(pl.col('date') <= cutoff)
    _write(dst / 'meta/trading_calendar/session_ledger.parquet', ledger)

    dividend = pl.read_parquet(REAL_ROOT / 'factors/stock_dividend.parquet')
    dividend = dividend.filter(pl.col('ts_code').is_in(ALL_CODES))
    if cutoff is not None:
        mask = []
        for ex, report in zip(dividend['ex_date'].to_list(),
                              dividend['report_date'].to_list(), strict=True):
            ex_day, report_day = _as_date(ex), _as_date(report)
            if ex_day is not None:
                mask.append(ex_day <= cutoff)
            else:
                mask.append(report_day is None or report_day <= cutoff)
        dividend = dividend.filter(pl.Series(mask))
    _write(dst / 'factors/stock_dividend.parquet', dividend)


def _run_pipelines(root: Path, as_of: date) -> None:
    st_history.build_store(root, as_of=as_of)
    st_price_infer.run(root)
    tu.build_exclusions(root, tdx_root=None, basic_tables=None,
                        output_dir=root / 'meta' / 'universe', as_of=as_of)


@pytest.fixture(scope='module')
def baseline(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if not all((REAL_ROOT / rel).is_file() for rel in _REQUIRED):
        pytest.skip(f'本地列存缺少输入，跳过：{REAL_ROOT}')
    root = tmp_path_factory.mktemp('pit_full')
    assert REAL_ROOT not in root.parents
    _materialize(root, None)
    _run_pipelines(root, FULL_END)
    return root


@pytest.fixture(scope='module', params=T_LIST, ids=[t.isoformat() for t in T_LIST])
def cutoff_run(request: pytest.FixtureRequest, baseline: Path,
               tmp_path_factory: pytest.TempPathFactory) -> tuple[date, Path]:
    t = request.param
    root = tmp_path_factory.mktemp(f'pit_{t.isoformat()}')
    assert REAL_ROOT not in root.parents
    _materialize(root, t)
    _run_pipelines(root, t)
    return t, root


def _open_at(end: date | None, day: date) -> bool:
    return end is None or end >= day


def _compare_intervals(full: Path, trunc: Path, t: date, path_fn,
                       label: str, fields: tuple[str, ...]) -> list[str]:
    diffs: list[str] = []
    full_rows = pl.read_parquet(path_fn(full))
    trunc_rows = pl.read_parquet(path_fn(trunc))
    for code in STOCKS:
        known = [row for row in full_rows.filter(pl.col('ts_code') == code).to_dicts()
                 if row['start_date'] <= t]
        seen = trunc_rows.filter(pl.col('ts_code') == code).to_dicts()
        for row in seen:
            match = [item for item in known if item['start_date'] == row['start_date']]
            if not match:
                diffs.append(f'{t} {label} 多出 {code} {row["start_date"]}')
                continue
            base = match[0]
            for field in fields:
                if base[field] != row[field]:
                    diffs.append(f'{t} {label} {field} {code} '
                                 f'{row["start_date"]} {base[field]}->{row[field]}')
            if _open_at(row['end_date'], t) != _open_at(base['end_date'], t):
                diffs.append(f'{t} {label} 开闭 {code} {row["start_date"]} '
                             f'{base["end_date"]}->{row["end_date"]}')
            elif row['end_date'] is not None and row['end_date'] < t \
                    and base['end_date'] != row['end_date']:
                diffs.append(f'{t} {label} 末端 {code} {row["start_date"]} '
                             f'{base["end_date"]}->{row["end_date"]}')
        seen_starts = {row['start_date'] for row in seen}
        for row in known:
            if row['start_date'] not in seen_starts:
                diffs.append(f'{t} {label} 缺失 {code} {row["start_date"]}'
                             f' 末端={row["end_date"]}')
    return diffs


def _st_signature(result: st_history.StHistoryResult) -> tuple:
    return (result.status, result.flag, result.suspected,
            result.start if result.status == st_history.STATUS_ST else None)


def _compare_st_queries(full: Path, trunc: Path, t: date) -> list[str]:
    diffs: list[str] = []
    for code in STOCKS:
        path = _quote_path(full, code)
        if not path.is_file():
            continue
        bars = [day for day in
                pl.read_parquet(path, columns=['date'])['date'].to_list() if day <= t]
        picks = set(bars[::40]) | {t}
        for day in sorted(picks):
            for include in (False, True):
                base = st_history.status_at(full, code, day, include_suspected=include)
                cut = st_history.status_at(trunc, code, day, include_suspected=include)
                if _st_signature(base) != _st_signature(cut):
                    diffs.append(f'{t} 状态 {code} {day} 含疑似={include} '
                                 f'{_st_signature(base)}->{_st_signature(cut)}')
    return diffs


def _compare_universe(full: Path, trunc: Path, t: date) -> list[str]:
    diffs: list[str] = []
    base_ex = tu.load_exclusions(full)
    cut_ex = tu.load_exclusions(trunc)
    for code in ALL_CODES:
        path = _quote_path(full, code)
        if not path.is_file():
            continue
        bars = {day for day in
                pl.read_parquet(path, columns=['date'])['date'].to_list() if day <= t}
        picks = set(sorted(bars)[::100]) | {t}
        for frame in (base_ex, cut_ex):
            hits = frame.filter(pl.col('ts_code') == code)
            if hits.height:
                picks |= {day for day in hits['last_trade_date'].to_list()
                          if day is not None and day <= t}
        for day in sorted(picks):
            base_reasons = tu.exclusion_reasons(code, day, exclusions=base_ex)
            cut_reasons = tu.exclusion_reasons(code, day, exclusions=cut_ex)
            base_set = sorted((item['reason_code'], item['severity'],
                               item['last_trade_date']) for item in base_reasons)
            cut_set = sorted((item['reason_code'], item['severity'],
                              item['last_trade_date']) for item in cut_reasons)
            if base_set != cut_set:
                diffs.append(f'{t} 原因 {code} {day} {base_set}->{cut_set}')
            for strict in (False, True):
                if (tu.is_tradable(code, day, exclusions=base_ex, strict=strict)
                        != tu.is_tradable(code, day, exclusions=cut_ex, strict=strict)):
                    diffs.append(f'{t} 可交易 {code} {day} strict={strict}')
    return diffs


def _report(diffs: list[str], t: date) -> str:
    head = '；'.join(diffs[:5])
    return f't={t} 共 {len(diffs)} 处不一致，前 5 条：{head}'


@pytest.mark.xfail(strict=False, reason='已知 4 类前视依赖，见模块 docstring；修好后删除标记')
def test_st_chain_truncation(baseline: Path, cutoff_run: tuple[date, Path]) -> None:
    t, trunc = cutoff_run
    diffs = _compare_intervals(
        baseline, trunc, t, st_history.intervals_path, 'ST区间', ('flag',))
    diffs += _compare_intervals(
        baseline, trunc, t, st_price_infer.price_path, '疑似区间',
        ('flag', 'excluded_s_share'))
    diffs += _compare_st_queries(baseline, trunc, t)
    assert not diffs, _report(diffs, t)


@pytest.mark.xfail(strict=False, reason='已知 4 类前视依赖，见模块 docstring；修好后删除标记')
def test_tradable_universe_truncation(baseline: Path,
                                      cutoff_run: tuple[date, Path]) -> None:
    t, trunc = cutoff_run
    diffs = _compare_universe(baseline, trunc, t)
    assert not diffs, _report(diffs, t)
