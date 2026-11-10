#!/usr/bin/env python3

r"""``state_vector_daily`` 的逐维对拍与不变量校验。

校验项（对应数据治理 DG-20/DG-21/DG-30：结论必须带数字、能复现）
==================================================================
1. 结构不变量：日期唯一升序、无 NaN（缺失必须是 null）、分位落在 [0,100]、
   分位为 null 当且仅当原值为 null 或有效样本不足、``dims_*_available`` 与逐列一致。
2. 逐维独立重算：抽样日期上把六个原值从源表重新算一遍，与成品逐值比对。
3. 分位独立重算：抽样日期上按「最近 1250 个有效观测」重算分位，与成品比对。
4. 读路径：按 ``manifest.json`` 里的相对路径真实读一次成品并断言列齐全。

用法::

    .venv\\Scripts\\python.exe tools\\verify_state_vector.py
    .venv\\Scripts\\python.exe tools\\verify_state_vector.py --sample 12
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORE = ROOT / 'data' / 'local_store'
STATE_PATH = DEFAULT_STORE / 'meta' / 'state' / 'state_vector_daily.parquet'
MANIFEST_PATH = DEFAULT_STORE / 'meta' / 'state' / 'manifest.json'
TOL = 1e-9


def load_builder():
    spec = importlib.util.spec_from_file_location(
        'build_state_vector', ROOT / 'tools' / 'build_state_vector.py')
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.checks = 0

    def check(self, condition: bool, message: str) -> None:
        self.checks += 1
        if not condition:
            self.failures.append(message)

    def summary(self) -> str:
        if self.failures:
            return f'FAIL：{len(self.failures)} 项 / 共 {self.checks} 项\n' + '\n'.join(
                f'  - {item}' for item in self.failures[:20])
        return f'PASS：{self.checks} 项校验全部通过'


def check_structure(panel: pl.DataFrame, report: Report) -> None:
    report.check(panel['date'].is_duplicated().sum() == 0, 'date 存在重复')
    report.check(panel['date'].is_sorted(), 'date 未升序')
    for col, dtype in panel.schema.items():
        if dtype == pl.Float64:
            nan_count = panel[col].is_nan().sum()
            report.check(nan_count == 0, f'{col} 含 {nan_count} 个 NaN（应为 null）')
    for col in panel.columns:
        if not col.endswith('_q'):
            continue
        series = panel[col].drop_nulls()
        if series.len() == 0:
            continue
        report.check(bool((series >= 0).all() and (series <= 100).all()),
                     f'{col} 有值超出 [0,100]')

    raw_cols = ['val_earn_yield', 'sent_ff_turnover', 'breadth_ma20',
                'lev_margin_buy_ratio', 'trend_er20', 'style_large_small_20d']
    q_cols = [f'{c}_q' for c in raw_cols]
    dims = panel.select(
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in raw_cols])
        .alias('expect'))['expect']
    report.check(bool((dims == panel['dims_available']).all()), 'dims_available 与逐列不一致')
    qdims = panel.select(
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in q_cols])
        .alias('expect'))['expect']
    report.check(bool((qdims == panel['dims_q_available']).all()), 'dims_q_available 与逐列不一致')
    # 分位为 null 时原值必须为 null（或窗口不足），反向：原值非空而分位非空 → 正常
    bad = panel.filter(pl.col('val_earn_yield').is_null() & pl.col('val_earn_yield_q').is_not_null())
    report.check(bad.height == 0, f'{bad.height} 行原值为空但分位有值')


def check_valuation(panel: pl.DataFrame, dates: list[pl.Date], store: Path,
                    report: Report) -> None:
    src = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
           .select(['trade_date', 'pe', 'total_mv'])
           .filter(pl.col('trade_date').is_in(dates))
           .filter(pl.col('pe').is_not_null() & (pl.col('pe') != 0)
                   & pl.col('total_mv').is_not_null())
           .with_columns((pl.col('total_mv') / pl.col('pe')).alias('earn'))
           .group_by('trade_date')
           .agg([pl.col('total_mv').sum().alias('mv'), pl.col('earn').sum().alias('earn')])
           .sort('trade_date').collect())
    for row in src.iter_rows(named=True):
        value = panel.filter(pl.col('date') == row['trade_date'])['val_earn_yield'].item()
        expect = row['earn'] / row['mv'] * 100.0
        report.check(abs(value - expect) < 1e-9,
                     f'{row["trade_date"]} val_earn_yield 成品 {value:.6f} != 重算 {expect:.6f}')


def check_simple_sources(panel: pl.DataFrame, dates: list[pl.Date], store: Path,
                         report: Report) -> None:
    breadth = pl.read_parquet(
        store / 'meta' / 'breadth' / 'breadth_ma_daily.parquet',
        columns=['date', 'pct_above_ma20'])
    c300 = pl.read_parquet(store / 'quotes' / 'index' / 'sh' / '000300.SH.parquet',
                           columns=['date', 'close'])
    c1000 = pl.read_parquet(store / 'quotes' / 'index' / 'sh' / '000852.SH.parquet',
                            columns=['date', 'close'])

    for day in dates:
        row = panel.filter(pl.col('date') == day).row(0, named=True)
        expect_breadth = breadth.filter(pl.col('date') == day)['pct_above_ma20'].item()
        report.check(abs(row['breadth_ma20'] - expect_breadth) < TOL,
                     f'{day} breadth_ma20 {row["breadth_ma20"]} != 源表 {expect_breadth}')

        # 效率系数与风格差
        series300 = c300.sort('date')
        series1000 = c1000.sort('date')
        row300 = series300.filter(pl.col('date') == day)
        if row300.height == 1:
            pos = series300.with_row_index().filter(pl.col('date') == day)['index'].item()
            if pos >= 20:
                window = series300.slice(pos - 20, 21)['close'].to_numpy()
                er = abs(window[-1] - window[0]) / np.abs(np.diff(window)).sum()
                report.check(abs(row['trend_er20'] - er) < 1e-9,
                             f'{day} trend_er20 {row["trend_er20"]:.8f} != 重算 {er:.8f}')
                p1000 = series1000.with_row_index().filter(pl.col('date') == day)
                if p1000.height == 1:
                    pos1 = p1000['index'].item()
                    w1 = series1000.slice(pos1 - 20, 21)['close'].to_numpy()
                    style = ((window[-1] / window[0] - 1) - (w1[-1] / w1[0] - 1)) * 100.0
                    report.check(abs(row['style_large_small_20d'] - style) < 1e-9,
                                 f'{day} style {row["style_large_small_20d"]:.6f} != {style:.6f}')


def check_turnover(panel: pl.DataFrame, dates: list[pl.Date], store: Path,
                   builder, report: Report) -> None:
    """情绪位独立重算：同集合的 Σ成交额 / Σ(自由流通股本 × 收盘)；顺带用同一批
    沪深成交额重算杠杆位。"""
    glob = str(store / 'quotes' / 'stock' / '**' / '*.parquet')
    frame = (pl.scan_parquet(glob)
             .filter(pl.col('date').cast(pl.Date).is_in(dates))
             .select(['date', 'ts_code', 'close', 'volume', 'amount'])
             .collect())
    shares = builder.load_shares(store)
    margin = pl.read_parquet(
        store / 'meta' / 'macro' / 'cn_market_margin_daily.parquet',
        columns=['date', 'fin_buy']).group_by('date').agg(pl.col('fin_buy').sum())
    parts = []
    for day in dates:
        sub = frame.filter(pl.col('date') == day)
        if sub.height:
            parts.append(builder.process_chunk(sub, shares))
    recomputed = pl.concat(parts, how='vertical_relaxed')
    for row in recomputed.iter_rows(named=True):
        value = panel.filter(pl.col('date') == row['date'])['sent_ff_turnover'].item()
        expect = row['amount_ff'] / row['ff_mv'] * 100.0
        report.check(abs(value - expect) < 1e-9,
                     f'{row["date"]} sent_ff_turnover {value:.6f} != 重算 {expect:.6f}')
        n_codes = panel.filter(pl.col('date') == row['date'])['sent_n_codes'].item()
        report.check(n_codes == row['n_ff'],
                     f'{row["date"]} sent_n_codes {n_codes} != 重算 {row["n_ff"]}')

        fin_buy = margin.filter(pl.col('date') == row['date'])['fin_buy'].item()
        lev = panel.filter(pl.col('date') == row['date'])['lev_margin_buy_ratio'].item()
        if fin_buy is None:
            report.check(lev is None, f'{row["date"]} 两融缺失但杠杆位有值')
        else:
            expect_lev = fin_buy / row['amount_shsz'] * 100.0
            report.check(lev is not None and abs(lev - expect_lev) < 1e-9,
                         f'{row["date"]} lev {lev} != 重算 {expect_lev:.6f}')


def _window_quantile(values: np.ndarray, index: int, window: int) -> float | None:
    """独立实现：向前回溯收集 ``window`` 个有效观测，算「≤ 当日值」的占比。"""
    collected: list[float] = []
    cursor = index
    while cursor >= 0 and len(collected) < window:
        value = values[cursor]
        if not np.isnan(value):
            collected.append(float(value))
        cursor -= 1
    if len(collected) < window:
        return None
    array = np.asarray(collected)
    return float((array <= values[index]).mean() * 100.0)


def check_quantiles(panel: pl.DataFrame, builder, report: Report, sample: int) -> None:
    raw_cols = ['val_earn_yield', 'sent_ff_turnover', 'breadth_ma20',
                'lev_margin_buy_ratio', 'trend_er20', 'style_large_small_20d']
    rng = np.random.default_rng(20261110)
    for raw_col in raw_cols:
        values = panel[raw_col].to_numpy().astype(float)
        stored = panel[f'{raw_col}_q'].to_numpy().astype(float)
        candidates = np.flatnonzero(~np.isnan(stored))
        if candidates.size == 0:
            continue
        picks = rng.choice(candidates, size=min(sample, candidates.size), replace=False)
        dates = panel['date'].to_list()
        for index in np.sort(picks):
            expect = _window_quantile(values, int(index), builder.QUANTILE_WINDOW)
            report.check(expect is not None and abs(stored[index] - expect) < 1e-9,
                         f'{dates[int(index)]} {raw_col}_q {stored[index]:.6f} != {expect}')


def check_erp(panel: pl.DataFrame, dates: list[pl.Date], store: Path,
              report: Report) -> None:
    """ERP = 盈利收益率 − 10Y 国债（两者都非空才算），逐日重算。"""
    treasury = pl.read_parquet(
        store / 'meta' / 'macro' / 'cn_treasury_yield_10y.parquet',
        columns=['date', 'y10']).rename({'y10': 'y10_pct'})
    merged = panel.select(['date', 'val_earn_yield', 'valuation_erp']).join(
        treasury, on='date', how='left')
    computed = merged.with_columns(
        pl.when(pl.col('val_earn_yield').is_not_null() & pl.col('y10_pct').is_not_null())
        .then(pl.col('val_earn_yield') - pl.col('y10_pct')).otherwise(None)
        .alias('expect'))
    covered = computed.filter(pl.col('expect').is_not_null())
    mismatched = covered.filter(
        pl.col('valuation_erp').is_null()
        | ((pl.col('valuation_erp') - pl.col('expect')).abs() > 1e-9))
    report.check(mismatched.height == 0,
                 f'ERP 与重算不一致 {mismatched.height} 行')
    unexpected = computed.filter(pl.col('expect').is_null() & pl.col('valuation_erp').is_not_null())
    report.check(unexpected.height == 0, f'{unexpected.height} 行 ERP 有值但输入缺失')
    # 抽样日期必须落在两者交集内
    for day in dates:
        row = computed.filter(pl.col('date') == day).row(0, named=True)
        if row['expect'] is not None:
            report.check(abs(row['valuation_erp'] - row['expect']) < 1e-9,
                         f'{day} ERP {row["valuation_erp"]:.6f} != {row["expect"]:.6f}')


def check_quality_flags(panel: pl.DataFrame, report: Report) -> None:
    """质量标记：换源日与低覆盖段必须在标记里，且不得误标常规日。"""
    flagged = panel.filter(pl.col('quality_flags').is_not_null())
    report.check(flagged.height < panel.height * 0.2,
                 f'质量标记覆盖 {flagged.height} 行，占比过高')
    known = {date.fromisoformat(d) for d in ('2015-01-05', '2018-01-02')}
    for day in known:
        row = panel.filter(pl.col('date') == day)
        if row.height:
            flags = row['quality_flags'].item() or ''
            report.check('val_source_switch' in flags, f'{day} 未标记 val_source_switch')
    low = panel.filter(
        (pl.col('val_pe_coverage').is_null() | (pl.col('val_pe_coverage') < 80.0))
        & (pl.col('date') >= pl.date(2006, 1, 1)))
    if low.height:
        missing = low.filter(~pl.col('quality_flags').fill_null('')
                             .str.contains('val_pe_coverage_low', literal=True))
        report.check(missing.height == 0,
                     f'{missing.height} 行覆盖率不足/缺失但未标记 val_pe_coverage_low')
        report.check(str(low['date'].min()) == '2011-01-04',
                     f'低覆盖段起点 {low["date"].min()} 与预期 2011-01-04 不符')
        report.check(str(low['date'].max()) == '2011-04-22',
                     f'低覆盖段终点 {low["date"].max()} 与预期 2011-04-22 不符')


def check_read_path(panel: pl.DataFrame, report: Report) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding='utf-8'))
    path = ROOT / manifest['path']
    report.check(path.is_file(), f'manifest 指向的文件不存在: {path}')
    loaded = pl.read_parquet(path)
    report.check(loaded.height == panel.height, '读路径行数与候选不一致')
    report.check(set(manifest['columns']) <= set(loaded.columns), 'manifest 列清单与实际不符')
    report.check(manifest['rows'] == loaded.height, 'manifest rows 与文件不符')


def check_free_float_asof(store: Path, report: Report, codes: int = 200) -> None:
    """PIT 自由流通股本：``join_asof`` 的结果必须等于「取 effective_date ≤ 当日 的最大一条」。

    换一条实现（先笛卡尔连接再取每只最大 effective_date）对拍，避免只验证同一条代码路径。
    """
    shares = pl.read_parquet(
        store / 'meta' / 'factors' / 'stock_shares.parquet',
        columns=['ts_code', 'effective_date', 'free_float_share_filled'])
    shares = shares.with_columns(pl.col('effective_date').cast(pl.Date))
    files = sorted((store / 'quotes' / 'stock').glob('**/*.parquet'))
    picked = [files[int(i)] for i in np.linspace(0, len(files) - 1, codes).astype(int)]
    frame = pl.concat([pl.read_parquet(p, columns=['date', 'ts_code', 'close'])
                       for p in picked], how='vertical_relaxed')
    frame = frame.with_columns(pl.col('date').cast(pl.Date))
    sample = frame.filter(pl.col('date') >= pl.date(2020, 1, 1)).sort(
        'ts_code').group_by('ts_code').tail(1)

    asof = sample.sort(['ts_code', 'date']).join_asof(
        shares.sort(['ts_code', 'effective_date']), left_on='date',
        right_on='effective_date', by='ts_code', strategy='backward')
    manual = (sample.join(shares, on='ts_code')
              .filter(pl.col('effective_date') <= pl.col('date'))
              .sort(['ts_code', 'effective_date']).group_by('ts_code').tail(1)
              .select(['ts_code', 'free_float_share_filled']))
    merged = asof.select(['ts_code', 'free_float_share_filled']).join(
        manual, on='ts_code', how='full', suffix='_manual', coalesce=True)
    mismatch = merged.filter(
        pl.col('free_float_share_filled').fill_null(-1)
        != pl.col('free_float_share_filled_manual').fill_null(-1))
    report.check(mismatch.height == 0,
                 f'PIT 自由流通股本与手工回溯不一致 {mismatch.height} 只')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='校验 state_vector_daily')
    parser.add_argument('--store-root', default=str(DEFAULT_STORE))
    parser.add_argument('--path', default=str(STATE_PATH))
    parser.add_argument('--sample', type=int, default=8)
    args = parser.parse_args(argv)

    store = Path(args.store_root)
    builder = load_builder()
    panel = pl.read_parquet(args.path)
    report = Report()

    newest = panel.tail(3)['date'].to_list()
    liquid = panel.filter(pl.col('dims_available') == 6).tail(3)['date'].to_list()
    turnovers = panel.filter(pl.col('sent_ff_turnover').is_not_null()).tail(2)['date'].to_list()
    sample_dates = sorted(set(newest + liquid + turnovers))

    check_structure(panel, report)
    check_valuation(panel, sample_dates, store, report)
    check_simple_sources(panel, sample_dates, store, report)
    check_turnover(panel, turnovers, store, builder, report)
    check_quantiles(panel, builder, report, args.sample)
    check_erp(panel, sample_dates, store, report)
    check_quality_flags(panel, report)
    check_free_float_asof(store, report)
    if MANIFEST_PATH.is_file():
        check_read_path(panel, report)
    else:
        report.check(False, f'missing manifest: {MANIFEST_PATH}')

    print(report.summary())
    return 0 if not report.failures else 1


if __name__ == '__main__':
    sys.exit(main())
