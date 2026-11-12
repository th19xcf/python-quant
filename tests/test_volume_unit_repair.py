"""volume 单位修复的两条验收判据（固化在测试里，不依赖一次性脚本）。

判据（2026-11-12 确认，替换原「换手率 ∈ 11%~15%」门槛）：

1. **整手残差**：``|volume_new * 100 / (amount / close) - 1| <= 1e-6``
2. **邻居量级**：``volume_new / median(同标的 ±20 自然日内、非修复日的 volume)``
   落在 ``[0.2, 20]``

为什么弃用「换手率 ∈ 11%~15%」：该区间是从「窗口内影响 11%~53%」误推出来的，
不是实测门槛。19 行实测只有 9 行落在区间内（实际 1.46%~25.85%），用它会把
已经修对的行判成错。

另固化一条**前置条件**（缺口 G-087）：``amount / close`` 反算只在 ``close`` 与
VWAP 同量级时有效。上市首日 / 无涨跌幅限制日 / 极端振幅日 VWAP 远低于 close，
反算会把本来正确的 volume 判成偏大 3 倍多。``VWAP = amount / (volume * 100)``。
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
QUOTE_DIR = REPO_ROOT / 'data' / 'local_store' / 'quotes' / 'stock'
#: 19 行修复的留痕证据（DG-32：证据进 docs/evidence/，不再在行情分片里加标记列）
EVIDENCE = REPO_ROOT / 'docs' / 'evidence' / 'volume_unit_repair_20261112' / 'rows.csv'

#: 2015 后已修复的 7 只（19 行分布在这 7 个文件里）
FIXED_CODES = ('000725.SZ', '600010.SH', '600157.SH', '600221.SH',
               '601288.SH', '601868.SH', '601988.SH')
BASIS = 'inferred_from_amount_close'
ROUND_LOT_TOL = 1e-6
NEIGHBOR_LO, NEIGHBOR_HI = 0.2, 20.0
NEIGHBOR_DAYS = 20
#: G-087：`amount/close` 反算失效的 7 行（首日 / 无涨跌幅限制日）
INVERSE_INVALID_ROWS = (
    ('301487.SZ', '2023-08-09'), ('301551.SZ', '2024-09-30'),
    ('301618.SZ', '2024-09-30'), ('601091.SH', '2026-09-18'),
    ('920091.BJ', '2025-11-21'), ('920176.BJ', '2026-07-27'),
    ('920693.BJ', '2023-10-27'),
)
#: 修复的目标筛选区间（比值 = (amount/close)/(volume*100)）
SELECT_LO, SELECT_HI = 90.0, 110.0


def _path(code: str) -> Path:
    market = code.split('.')[1].lower()
    return QUOTE_DIR / market / f'{code}.parquet'


def _read(code: str) -> pl.DataFrame:
    return pl.read_parquet(_path(code))


def round_lot_residual(volume: float, amount: float, close: float) -> float:
    """整手取整后与 ``amount/close`` 反算股数的相对偏离。"""
    return abs(volume * 100.0 / (amount / close) - 1.0)


def neighbor_scale(volume: float, neighbor_median: float) -> float:
    """修复值与同标的邻近交易日成交量中位数的比值。"""
    return volume / neighbor_median


def neighbor_median(df: pl.DataFrame, day, exclude: list, days: int = NEIGHBOR_DAYS):
    """同一标的、`day` 前后 `days` 个自然日内（排除修复日）的 volume 中位数。"""
    near = df.filter(
        (~pl.col('date').is_in(exclude))
        & ((pl.col('date') - pl.lit(day)).dt.total_days().abs() <= days))
    return near['volume'].median()


def amount_close_inverse_valid(close: float, vwap: float,
                               max_gap: float = 0.2) -> bool:
    """``amount/close`` 反算是否可用：close 与 VWAP 偏离不超过 ``max_gap``。"""
    return abs(close / vwap - 1.0) <= max_gap


def _fixed_rows() -> pl.DataFrame:
    """19 行修复记录：日期/只数取自证据文件，值回读行情分片对拍。"""
    evidence = pl.read_csv(EVIDENCE).with_columns(
        pl.col('date').str.to_date())
    parts = []
    for code, grp in evidence.group_by('ts_code', maintain_order=True):
        raw = _read(code[0])
        joined = grp.select(['date', 'volume_new']).join(raw, on='date', how='inner')
        parts.append(joined.select([
            pl.lit(code[0]).alias('ts_code'), 'date',
            pl.col('volume').alias('volume'), 'volume_new', 'amount', 'close']))
    return pl.concat(parts) if parts else pl.DataFrame()


def test_evidence_marks_exactly_19_rows_in_7_codes():
    d = _fixed_rows()
    assert d.height == 19, f'证据 19 行与行情对上的只有 {d.height} 行'
    assert d['ts_code'].n_unique() == 7
    # 证据记的修复值与行情里的现值必须逐行一致，否则证据与数据已经脱节
    mismatched = [(r['ts_code'], str(r['date']), r['volume'], r['volume_new'])
                  for r in d.iter_rows(named=True) if r['volume'] != r['volume_new']]
    assert not mismatched, f'证据与行情现值不一致: {mismatched}'
    # 修复标记不再落在行情分片里（DG-32：证据归 docs/evidence/，数据表保持统一 schema）
    for code in FIXED_CODES:
        assert 'basis' not in _read(code).columns, f'{code} 仍带 basis 列'
    # 每个文件的证据行数必须与证据文件自身分组一致（防止 join 少配）
    ev = pl.read_csv(EVIDENCE)
    for code in FIXED_CODES:
        assert (d.filter(pl.col('ts_code') == code).height
                == ev.filter(pl.col('ts_code') == code).height), f'{code} 证据行未全部落到行情'


def test_repaired_rows_satisfy_round_lot_residual():
    d = _fixed_rows()
    bad = [(r['ts_code'], str(r['date']))
           for r in d.iter_rows(named=True)
           if round_lot_residual(r['volume'], r['amount'], r['close']) > ROUND_LOT_TOL]
    assert not bad, f'整手残差 > 1ppm: {bad}'


def test_repaired_rows_satisfy_neighbor_scale():
    d = _fixed_rows()
    bad = []
    for code, grp in d.group_by('ts_code', maintain_order=True):
        raw = _read(code[0])
        days = grp['date'].to_list()
        for r in grp.iter_rows(named=True):
            med = neighbor_median(raw, r['date'], days)
            assert med is not None and med > 0, f'{code[0]} {r["date"]} 邻居中位量为空'
            ratio = neighbor_scale(r['volume'], med)
            if not (NEIGHBOR_LO <= ratio <= NEIGHBOR_HI):
                bad.append((code[0], str(r['date']), round(ratio, 3)))
    assert not bad, f'修复值/邻居中位量超出 [0.2,20]: {bad}'


def test_round_lot_criterion_rejects_wrong_values():
    """判据必须能红：未修（1/100）与错放大 100 倍都必须被判不合格。"""
    d = _fixed_rows()
    for r in d.iter_rows(named=True):
        for bad_volume in (r['volume'] / 100.0, r['volume'] * 100.0):
            assert round_lot_residual(bad_volume, r['amount'], r['close']) > ROUND_LOT_TOL


def test_neighbor_scale_criterion_rejects_wrong_values():
    """判据必须能红：量级错 100 倍（任一方向）必须越界。"""
    d = _fixed_rows()
    checked = 0
    for code, grp in d.group_by('ts_code', maintain_order=True):
        raw = _read(code[0])
        days = grp['date'].to_list()
        for r in grp.iter_rows(named=True):
            med = neighbor_median(raw, r['date'], days)
            for bad_volume in (r['volume'] / 100.0, r['volume'] * 100.0):
                ratio = neighbor_scale(bad_volume, med)
                assert not (NEIGHBOR_LO <= ratio <= NEIGHBOR_HI), \
                    f'{code[0]} {r["date"]} 量级错 100 倍却仍在区间内: {ratio}'
                checked += 1
    assert checked == 38


def test_amount_close_inverse_invalid_on_extreme_days():
    """G-087：这 7 行的 volume 与 amount 在 VWAP 口径下自洽，反算失效而非 volume 错。

    VWAP 落在当日 [low, high] 内且 close 远高于 VWAP；比值落不进
    `[90, 110]` 的修复目标区间，因此本轮修复正确地跳过了它们。
    """
    for code, day in INVERSE_INVALID_ROWS:
        d = _read(code).filter(pl.col('date') == pl.lit(day).str.to_date())
        assert d.height == 1, f'{code} {day} 行数 {d.height}'
        r = d.row(0, named=True)
        vwap = r['amount'] / (r['volume'] * 100.0)
        assert r['low'] <= vwap <= r['high'], \
            f'{code} {day} VWAP {vwap} 不在 [{r["low"]}, {r["high"]}]'
        assert not amount_close_inverse_valid(r['close'], vwap), \
            f'{code} {day} close/VWAP 偏离在容差内，反算应可用'
        ratio = (r['amount'] / r['close']) / (r['volume'] * 100.0)
        assert not (SELECT_LO <= ratio <= SELECT_HI), \
            f'{code} {day} 比值 {ratio} 落入修复目标区间，筛选口径与登记不一致'
