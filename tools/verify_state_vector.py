#!/usr/bin/env python3

r"""``state_vector_daily`` 的逐维对拍与不变量校验。

校验项（对应数据治理 DG-20/DG-21/DG-30：结论必须带数字、能复现）
==================================================================
1. 结构不变量：日期唯一升序、无 NaN（缺失必须是 null）、分位落在 [0,100]、
   分位为 null 当且仅当原值为 null 或有效样本不足、``dims_*_available`` 与逐列一致。
2. 逐维独立重算：抽样日期上把六个原值从源表重新算一遍，与成品逐值比对。
2b. 估值位缩尾：抽样日与全序列上按 ``clip(1/pe, q_lo, q_hi)`` 独立重算主列，
   并核对对照列 ``val_earn_yield_unwinsor`` 仍是未缩尾口径；再用恒等变换
   （0%/100% 分位，等价不缩尾）模拟旧口径，断言检测器**必须报出差异**，
   防止缩尾参数被静默改成无效值。
2c. 空值语义：主列 null 必须严格等于「覆盖率 < 80% 或聚合盈利 ≤ 0」的交易日集合，
   空值计数与逐列 null 数写进断言（全市场亏损期该维为 null 的规则不许变）。
3. 分位独立重算：抽样日期上按「最近 1250 个有效观测」重算分位，与成品比对。
4. 读路径：按 ``manifest.json`` 里的相对路径真实读一次成品并断言列齐全。

用法::

    .venv\\Scripts\\python.exe tools\\verify_state_vector.py
    .venv\\Scripts\\python.exe tools\\verify_state_vector.py --sample 12
"""

from __future__ import annotations

import argparse
import hashlib
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


def _valuation_aggregates(store: Path, dates: list[pl.Date] | None,
                          lower: float, upper: float) -> pl.DataFrame:
    """独立聚合：逐日 ``Σ(total_mv × clip(1/pe, q_lo, q_hi))`` / Σtotal_mv。

    与产出脚本的差别：这里用 group-by 后显式 ``clip``，不依赖窗口表达式；
    ``lower=0.0 / upper=1.0`` 即恒等变换，用来模拟「未缩尾」并验证断言可失败。
    """
    lf = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
          .select(['trade_date', 'pe', 'total_mv'])
          .filter(pl.col('pe').is_not_null() & (pl.col('pe') != 0)
                  & pl.col('total_mv').is_not_null()))
    if dates is not None:
        lf = lf.filter(pl.col('trade_date').is_in(dates))
    lf = lf.with_columns([
        (pl.col('total_mv') / pl.col('pe')).alias('earn'),
        (1.0 / pl.col('pe')).alias('ey'),
    ]).with_columns([
        pl.col('ey').quantile(lower).over('trade_date').alias('q_lo'),
        pl.col('ey').quantile(upper).over('trade_date').alias('q_hi'),
    ]).with_columns(
        (pl.col('total_mv') * pl.col('ey').clip(pl.col('q_lo'), pl.col('q_hi')))
        .alias('earn_w'))
    return lf.group_by('trade_date').agg([
        pl.col('total_mv').sum().alias('mv'),
        pl.col('earn').sum().alias('earn'),
        pl.col('earn_w').sum().alias('earn_w'),
        pl.len().alias('n'),
    ]).collect()


def check_valuation(panel: pl.DataFrame, dates: list[pl.Date], store: Path,
                    builder, report: Report) -> None:
    lo, hi = builder.VAL_EY_WINSOR_LOWER, builder.VAL_EY_WINSOR_UPPER
    src = _valuation_aggregates(store, dates, lo, hi)
    identity = _valuation_aggregates(store, dates, 0.0, 1.0)
    detectable = 0
    for row in src.iter_rows(named=True):
        day = row['trade_date']
        expect = row['earn_w'] / row['mv'] * 100.0
        value = panel.filter(pl.col('date') == day)['val_earn_yield'].item()
        report.check(abs(value - expect) < 1e-9,
                     f'{day} val_earn_yield 成品 {value:.6f} != 缩尾重算 {expect:.6f}')
        expect_old = row['earn'] / row['mv'] * 100.0
        old = panel.filter(pl.col('date') == day)['val_earn_yield_unwinsor'].item()
        report.check(abs(old - expect_old) < 1e-9,
                     f'{day} val_earn_yield_unwinsor 对照列 {old:.6f} != 未缩尾重算 '
                     f'{expect_old:.6f}')
        id_row = identity.filter(pl.col('trade_date') == day)
        if id_row.height:
            expect_id = id_row['earn_w'].item() / id_row['mv'].item() * 100.0
            if abs(expect_id - expect) > 1e-12:
                # 恒等变换（未缩尾）与成品必须不同，否则说明缩尾没生效
                detectable += 1
                report.check(abs(value - expect_id) > 1e-12,
                             f'{day} 缩尾未生效：成品等于未缩尾口径 {expect_id:.6f}')
    report.check(detectable > 0,
                 '抽样日上缩尾与恒等变换完全相同，缩尾断言无法失败（校验形同虚设）')


def check_valuation_full(panel: pl.DataFrame, store: Path, builder,
                         report: Report) -> None:
    """全序列重算 + 空值语义：主列 null 必须等于门控失败日集合。"""
    lo, hi = builder.VAL_EY_WINSOR_LOWER, builder.VAL_EY_WINSOR_UPPER
    src = _valuation_aggregates(store, None, lo, hi)
    n_row = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
             .group_by('trade_date').agg(pl.len().alias('n_row')).collect())
    joined = (src.join(n_row, on='trade_date', how='left')
              .join(panel.select(['date', 'val_earn_yield', 'val_earn_yield_unwinsor',
                                  'val_pe_coverage']),
                    left_on='trade_date', right_on='date', how='inner')
              .with_columns([
                  pl.when((pl.col('mv') > 0) & (pl.col('earn_w') > 0)
                          & (pl.col('n') >= pl.col('n_row') * builder.VAL_MIN_PE_COVERAGE))
                  .then(pl.col('earn_w') / pl.col('mv') * 100.0).otherwise(None)
                  .alias('_expect'),
                  pl.when((pl.col('mv') > 0) & (pl.col('earn') > 0)
                          & (pl.col('n') >= pl.col('n_row') * builder.VAL_MIN_PE_COVERAGE))
                  .then(pl.col('earn') / pl.col('mv') * 100.0).otherwise(None)
                  .alias('_expect_old'),
              ]))
    # 逐值：两侧都非空时差值必须为 0；空值与非空必须一一对应（分别判 null，避免 null 参与比较）
    value_bad = joined.filter(
        pl.col('_expect').is_not_null() & pl.col('val_earn_yield').is_not_null()
        & ((pl.col('val_earn_yield') - pl.col('_expect')).abs() > 1e-9))
    report.check(value_bad.height == 0,
                 f'{value_bad.height} 天主列与全序列缩尾重算不一致')
    null_mismatch = joined.filter(
        pl.col('_expect').is_null() != pl.col('val_earn_yield').is_null())
    report.check(null_mismatch.height == 0,
                 f'{null_mismatch.height} 天的主列空值与「覆盖率≥80% 且聚合盈利>0」门控不符')
    old_bad = joined.filter(
        pl.col('_expect_old').is_not_null() & pl.col('val_earn_yield_unwinsor').is_not_null()
        & ((pl.col('val_earn_yield_unwinsor') - pl.col('_expect_old')).abs() > 1e-9))
    report.check(old_bad.height == 0,
                 f'{old_bad.height} 天对照列与未缩尾重算不一致')
    cover_bad = joined.filter(
        pl.col('val_pe_coverage').is_not_null()
        & ((pl.col('val_pe_coverage')
            - pl.col('n') / pl.col('n_row') * 100.0).abs() > 1e-9))
    report.check(cover_bad.height == 0, f'{cover_bad.height} 天 val_pe_coverage 与重算不符')
    report.check(int(joined['val_earn_yield'].is_null().sum())
                 == int(joined['_expect'].is_null().sum()),
                 '主列空值计数与门控重算的空值计数不一致')


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


def check_quality_flags(panel: pl.DataFrame, builder, report: Report) -> None:
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
    # 口径变更标记：缩尾差 ≥ 阈值的交易日必须打标记（漏标即可失败）
    shift = panel.filter(
        pl.col('val_earn_yield').is_not_null()
        & pl.col('val_earn_yield_unwinsor').is_not_null()
        & ((pl.col('val_earn_yield') - pl.col('val_earn_yield_unwinsor')).abs()
           >= builder.VAL_WINSOR_FLAG_PP))
    if shift.height:
        missing = shift.filter(~pl.col('quality_flags').fill_null('')
                               .str.contains('val_earn_yield_winsor', literal=True))
        report.check(missing.height == 0,
                     f'{missing.height} 天缩尾差 ≥ {builder.VAL_WINSOR_FLAG_PP}pp 但未标记 '
                     f'val_earn_yield_winsor')


def check_read_path(panel: pl.DataFrame, report: Report) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding='utf-8'))
    path = ROOT / manifest['path']
    report.check(path.is_file(), f'manifest 指向的文件不存在: {path}')
    loaded = pl.read_parquet(path)
    report.check(loaded.height == panel.height, '读路径行数与候选不一致')
    report.check(set(manifest['columns']) <= set(loaded.columns), 'manifest 列清单与实际不符')
    report.check(manifest['rows'] == loaded.height, 'manifest rows 与文件不符')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    report.check(manifest.get('sha256') == digest, 'manifest sha256 与实际文件不一致')
    report.check(str(manifest.get('method_version', '')).startswith('state_vector_v'),
                 f'manifest method_version 缺失: {manifest.get("method_version")}')
    change = manifest.get('caliber_change') or {}
    if change:
        report.check(change.get('to_method_version') == manifest.get('method_version'),
                     'caliber_change.to_method_version 与 method_version 不一致')
        report.check(change.get('sha256_before') is not None,
                     'caliber_change 未记录旧 SHA256（DG-11 留证缺失）')
        report.check(int(change.get('common_days', 0)) > 0,
                     'caliber_change.common_days 缺失')


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
    check_valuation(panel, sample_dates, store, builder, report)
    check_valuation_full(panel, store, builder, report)
    check_simple_sources(panel, sample_dates, store, report)
    check_turnover(panel, turnovers, store, builder, report)
    check_quantiles(panel, builder, report, args.sample)
    check_erp(panel, sample_dates, store, report)
    check_quality_flags(panel, builder, report)
    check_free_float_asof(store, report)
    # 读路径只在「校验对象就是正式路径」时执行；校验候选文件时显式说明跳过，
    # 避免用旧正式表冒充候选的读路径证据
    if MANIFEST_PATH.is_file() and Path(args.path).resolve() == STATE_PATH.resolve():
        check_read_path(panel, report)
    elif MANIFEST_PATH.is_file():
        print('[verify] 校验对象是候选文件，跳过 manifest 读路径检查（替换后再跑一次）')
    else:
        report.check(False, f'missing manifest: {MANIFEST_PATH}')

    print(report.summary())
    return 0 if not report.failures else 1


if __name__ == '__main__':
    sys.exit(main())
