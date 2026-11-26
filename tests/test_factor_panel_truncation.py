"""因子面板截断不变量（2026-11-26）：删掉 t 之后的行情重算，t 及以前的因子值必须逐值不变。

覆盖推荐打分用的技术因子面板 ``src.advice.factor_panel.compute_factor_panel``
（动量 / 波动 / 量能 / 风险 / RSI 预热 / 均线偏离 / 流动性等，含 ``extended=True`` 的扩展因子）。

只要任何一列用到了全局统计量（全样本标准化、bfill、整段 rank、center=True 的滚动窗口），
或按「整段行数」做了判断，截断前后 t 当日的值就会不同，本测试即失败。

``history_rows`` 是「该标的整段行数」，按定义随截断变化，不参与比对（打分侧只用它
判断历史是否够长；若被用于当日打分即构成前视，另行登记）。

两组：
1. 合成数据（快）：3 只标的，含停牌缺行、新股中途上市，t 取 4 个位置；
2. 真实行情（slow）：6 只股票，t 取 3 个历史交易日。
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.advice.factor_panel import SCORE_COLUMNS, compute_factor_panel

#: 按定义随截断变化、不参与比对的列
VARIANT_BY_DEFINITION = {'history_rows'}
RTOL, ATOL = 1e-9, 1e-12


def _synthetic() -> pl.DataFrame:
    rng = np.random.default_rng(20261126)
    dates = pd.bdate_range('2022-01-03', periods=320)
    frames = []
    for code, start, drift in (('600000.SH', 0, 0.0004), ('000001.SZ', 0, -0.0002),
                               ('603999.SH', 90, 0.0010)):          # 第三只中途上市
        n = len(dates) - start
        close = 10.0 * np.cumprod(1 + rng.normal(drift, 0.02, n))
        open_ = close * (1 + rng.normal(0, 0.004, n))
        high = np.maximum(open_, close) * (1 + rng.uniform(0, 0.01, n))
        low = np.minimum(open_, close) * (1 - rng.uniform(0, 0.01, n))
        volume = rng.uniform(1e5, 1e6, n)
        f = pl.DataFrame({'date': dates[start:], 'ts_code': code, 'open': open_, 'high': high,
                          'low': low, 'close': close, 'volume': volume,
                          'amount': volume * close * 100, 'raw_amount': volume * close * 100})
        if code == '000001.SZ':                                      # 停牌：整段缺行
            f = f.filter(~pl.int_range(pl.len()).is_between(150, 162))
        frames.append(f)
    return pl.concat(frames).with_columns(pl.col('date').cast(pl.Date))


def _compare(full: pl.DataFrame, cut: pl.DataFrame, t: date) -> list[str]:
    cols = [c for c in SCORE_COLUMNS if c in full.columns and c in cut.columns
            and c not in VARIANT_BY_DEFINITION]
    cols += [c for c in full.columns if c not in cols and c in cut.columns
             and c not in VARIANT_BY_DEFINITION
             and c not in {'date', 'ts_code', 'open', 'high', 'low', 'close', 'volume',
                           'amount', 'raw_amount'}
             and full.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)]
    a = full.filter(pl.col('date') <= t).sort(['ts_code', 'date'])
    b = cut.sort(['ts_code', 'date'])
    assert a.height == b.height, (a.height, b.height)
    bad = []
    for col in cols:
        x = a[col].cast(pl.Float64).to_numpy()
        y = b[col].cast(pl.Float64).to_numpy()
        nan_x, nan_y = np.isnan(x), np.isnan(y)
        same_nan = nan_x == nan_y
        close = np.isclose(x, y, rtol=RTOL, atol=ATOL) | (nan_x & nan_y)
        if not (same_nan.all() and close.all()):
            idx = int(np.flatnonzero(~(same_nan & close))[0])
            bad.append(f'{col}: {a["ts_code"][idx]} {a["date"][idx]} 全量={x[idx]} 截断={y[idx]}')
    return bad


@pytest.mark.parametrize('extended', [False, True])
def test_factor_panel_truncation_synthetic(extended: bool) -> None:
    raw = _synthetic()
    full = compute_factor_panel(raw, extended=extended)
    days = sorted(raw['date'].unique().to_list())
    problems = []
    for t in (days[60], days[140], days[200], days[-2]):
        cut = compute_factor_panel(raw.filter(pl.col('date') <= t), extended=extended)
        problems += [f't={t} {p}' for p in _compare(full, cut, t)]
    assert not problems, '截断前后因子值不一致（疑似前视）：\n' + '\n'.join(problems[:20])


@pytest.mark.slow
def test_factor_panel_truncation_real_quotes() -> None:
    from src.data.local_store.paths import resolve_store_root
    from src.utils.config import get_config

    root = resolve_store_root(get_config())
    codes = ['600000.SH', '600519.SH', '000001.SZ', '000002.SZ', '300750.SZ', '601318.SH']
    frames = []
    for code in codes:
        path = root / 'quotes' / 'stock' / code[-2:].lower() / f'{code}.parquet'
        if not path.is_file():
            continue
        df = pl.read_parquet(path, columns=['date', 'ts_code', 'open', 'high', 'low', 'close',
                                            'volume', 'amount', 'hfq_factor'])
        fac = pl.col('hfq_factor').fill_null(strategy='forward').fill_null(1.0)
        frames.append(df.with_columns([
            (pl.col(c) * fac).alias(c) for c in ('open', 'high', 'low', 'close')
        ] + [pl.col('amount').alias('raw_amount'), pl.col('date').cast(pl.Date)])
            .filter(pl.col('date') >= date(2017, 1, 1)).drop('hfq_factor'))
    if not frames:
        pytest.skip('本地行情不可用')
    raw = pl.concat(frames, how='vertical_relaxed')
    full = compute_factor_panel(raw)
    problems = []
    for t in (date(2019, 6, 28), date(2021, 12, 31), date(2024, 9, 30)):
        cut = compute_factor_panel(raw.filter(pl.col('date') <= t))
        problems += [f't={t} {p}' for p in _compare(full, cut, t)]
    assert not problems, '截断前后因子值不一致（疑似前视）：\n' + '\n'.join(problems[:20])
