"""产出 ``data/local_store/meta/anomalies/fund_fake_bar_bars.csv``（ETF/基金「源端假收盘」台账）。

## 判定规则（显式化）

1. **候选集**：单日涨幅 ≥ 9.9%、且为「孤立脉冲」的 ETF bar。该候选表
   ``etf_audit_spike_isolated.csv`` 由 2026-11-05 的一次性取证脚本产出，**原始扫描脚本未留档**，
   因此候选集的生成条件只能按已归档结果回放，无法在本脚本内重算。
2. **分类**（``etf_fakebar_classification.csv`` 的 ``sub_class`` 列）：以「次日开盘跳空」为强证据：
   - ``next_open_gap_pct <= -4`` → ``源端假收盘（次日跳空低开≥4%）``
   - ``|next_open_gap_pct| <= 3.2`` → ``真实行情``
   - 缺次日行 → ``证据不足（缺次日行）``
   - 其余 → ``待定``
3. **台账** 只收 ``sub_class`` 含「源端假收盘」的行。

近似的「close==high ∩ 涨幅≥9.4% ∩ 次日跳空≤−4%」不是本台账的生成规则：该近似会多收
2024-10-08 全市场涨停回落等真实行情，且 588810.SH 的 close≠high 仍入账。

## 数值口径（均可由本地行情复算）

- ``prev_close`` = 前一行 close；``next_open`` = 下一行 open
- ``ret`` = close/prev_close − 1
- ``next_day_back`` = next_close/prev_close − 1
- ``vwap`` = amount / (volume × 100)（volume 单位为手，1 手 = 100 份）
- ``close_vs_vwap`` = close/vwap − 1
- ``vol_vs_prev20`` = volume / median(前 20 个交易日的 volume)；不足 20 个前序 bar 时取全部可用前序（``min_samples=1``），
  例：588810.SH 2025-02-12 上市仅 18 个前序 bar，台账 0.38x 即按 18 个前序中位数算得
- ``close_eq_high`` = close == high
- ``old_eq_new`` = 本地 close == 通达信 .day 源端 close
- ``at_limit10/at_limit20`` 与 ``sub_class`` 取自归档分类表（原始阈值未留档）
- note 里的 ``ret`` / ``next_day_back`` / ``close_vs_vwap`` / ``vol_vs_prev20`` 取归档分类表的值：
  原始取证用的是未四舍五入的价格，本地落盘 3 位小数，直接重算在个别行会差 0.01pp
  （已实测 1 行：159287.SZ 2026-03-17，归档 ret=20.04%，按 1.384/1.153−1 重算为 20.03%），
  脚本会把这类差异以 ``[舍入]`` 前缀逐条打印，不计入台账比对差异。

用法::

    python tools/anomalies/build_fund_fake_bar_bars.py            # 打印结果并与现有台账逐行比对
    python tools/anomalies/build_fund_fake_bar_bars.py --write    # 覆盖写台账（需显式开启）
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[2]
STORE = REPO / 'data' / 'local_store'
LEDGER = STORE / 'meta' / 'anomalies' / 'fund_fake_bar_bars.csv'
CANDIDATES = Path(r'E:\www\tmp\etf_audit_spike_isolated.csv')
CLASSIFICATION = Path(r'E:\www\tmp\etf_fakebar_classification.csv')
BENCH = ['000300.SH', '000001.SH', '000688.SH', '399001.SZ', '399006.SZ']
DETECTED_AT = '2026-11-05'
COLUMNS = ['ts_code', 'trade_date', 'close', 'vwap', 'next_open_gap_pct', 'index_benchmark',
           'index_return_pct', 'evidence_strength', 'detected_at', 'note']
HEADER_TAIL = '; 基准为5只参考指数中当日涨幅最高者（代理口径，非标的跟踪指数）'
FAKE_TAG = '源端假收盘'


def quote_path(kind: str, code: str) -> Path:
    return STORE / 'quotes' / kind / code[-2:].lower() / f'{code}.parquet'


def bench_returns() -> dict:
    out: dict = {}
    for code in BENCH:
        d = pl.read_parquet(quote_path('index', code), columns=['date', 'close']).sort('date')
        d = d.with_columns((pl.col('close') / pl.col('close').shift(1) - 1).alias('r'))
        for r in d.iter_rows(named=True):
            if r['r'] is not None:
                out.setdefault(r['date'], {})[code] = r['r']
    return out


def features(code: str, trade_date: str) -> dict:
    d = pl.read_parquet(
        quote_path('fund', code),
        columns=['date', 'open', 'high', 'close', 'volume', 'amount'],
    ).sort('date')
    d = d.with_columns([
        pl.col('close').shift(1).alias('pc'),
        pl.col('open').shift(-1).alias('nopen'),
        pl.col('close').shift(-1).alias('nclose'),
        pl.col('volume').shift(1).rolling_median(20, min_samples=1).alias('med20'),
    ])
    target = date(int(trade_date[:4]), int(trade_date[5:7]), int(trade_date[8:10]))
    row = d.filter(pl.col('date') == target)
    if row.height != 1:
        raise SystemExit(f'{code} {trade_date}: 本地无该交易日或存在重复行')
    x = row.row(0, named=True)
    vwap = x['amount'] / (x['volume'] * 100)
    return {
        'pc': x['pc'], 'nopen': x['nopen'], 'nclose': x['nclose'],
        'ret': x['close'] / x['pc'] - 1,
        'gap': x['nopen'] / x['close'] - 1,
        'back': x['nclose'] / x['pc'] - 1,
        'vwap': vwap, 'cvv': x['close'] / vwap - 1,
        'volr': (x['volume'] / x['med20']) if x['med20'] else None,
        'ceh': x['close'] == x['high'], 'close': x['close'],
    }


def fmt(v: float, nd: int) -> str:
    return str(round(float(v), nd))


def build() -> tuple[pl.DataFrame, list[str], int]:
    if not CANDIDATES.exists() or not CLASSIFICATION.exists():
        raise SystemExit(
            f'缺少归档取证输入：{CANDIDATES} / {CLASSIFICATION}\n'
            '这两个文件是 2026-11-05 取证产物，原始扫描脚本未留档，无法由本地行情重算候选集。')
    with CLASSIFICATION.open(encoding='utf-8') as f:
        cls = {r['ts_code'] + r['date']: r for r in csv.DictReader(f)}
    with CANDIDATES.open(encoding='utf-8') as f:
        cand = list(csv.DictReader(f))
    bench = bench_returns()

    out = []
    rounding: list[str] = []
    for c in cand:
        key = c['ts_code'] + c['date']
        meta = cls.get(key)
        if meta is None:
            raise SystemExit(f'{key} 在分类表中缺失')
        if FAKE_TAG not in meta['sub_class']:
            continue
        f = features(c['ts_code'], c['date'])
        irs = bench.get(date(int(c['date'][:4]), int(c['date'][5:7]), int(c['date'][8:10])), {})
        if not irs:
            raise SystemExit(f'{key} 当日无基准指数行情')
        idx = max(irs, key=irs.get)
        # 台账 note 里的比率字段取归档分类表的值：原始取证用的是未四舍五入的价格，
        # 本地行情落盘为 3 位小数，直接重算会在个别行产生 0.01pp 级差异（见 rounding 列表）。
        for name, mine, archived in (
            ('ret', fmt(f['ret'] * 100, 2), meta['ret_pct']),
            ('next_day_back', fmt(f['back'] * 100, 2), meta['next_day_back_pct']),
            ('close_vs_vwap', fmt(f['cvv'] * 100, 2), meta['close_vs_vwap_pct']),
            ('vol_vs_prev20', fmt(f['volr'], 2), meta['vol_vs_prev20']),
        ):
            if mine != archived:
                rounding.append(f"{key} {name}: 归档={archived} 本地重算={mine}")
        note = (
            f"prev_close={fmt(f['pc'], 3)}; next_open={fmt(f['nopen'], 3)}; "
            f"ret={meta['ret_pct']}%; next_day_back={meta['next_day_back_pct']}%; "
            f"close_vs_vwap={meta['close_vs_vwap_pct']}%; "
            f"vol_vs_prev20={meta['vol_vs_prev20']}x; "
            f"close_eq_high={meta['close_eq_high']}; at_limit10={meta['at_limit10']}; "
            f"at_limit20={meta['at_limit20']}; old_eq_new={meta['old_eq_new']}; "
            f"sub_class={meta['sub_class']}" + HEADER_TAIL
        )
        out.append({
            'ts_code': c['ts_code'], 'trade_date': c['date'],
            'close': fmt(f['close'], 3), 'vwap': fmt(f['vwap'], 4),
            'next_open_gap_pct': fmt(f['gap'] * 100, 2),
            'index_benchmark': idx, 'index_return_pct': fmt(irs[idx] * 100, 2),
            'evidence_strength': 'strong', 'detected_at': DETECTED_AT, 'note': note,
        })
    df = pl.DataFrame(out, schema=dict.fromkeys(COLUMNS, pl.Utf8)).sort(['ts_code', 'trade_date'])
    return df, rounding, len(df)


def compare(df: pl.DataFrame) -> int:
    if not LEDGER.exists():
        print(f'[i] 现有台账不存在：{LEDGER}')
        return 1
    old = pl.read_csv(LEDGER, schema_overrides=dict.fromkeys(COLUMNS, pl.Utf8))
    diffs = 0
    if old.height != df.height:
        print(f'[差异] 行数 {old.height} vs {df.height}')
        diffs += 1
    old_map = {(r['ts_code'], r['trade_date']): r for r in old.iter_rows(named=True)}
    new_map = {(r['ts_code'], r['trade_date']): r for r in df.iter_rows(named=True)}
    for k in sorted(set(old_map) - set(new_map)):
        print(f'[差异] 仅台账有 {k}')
        diffs += 1
    for k in sorted(set(new_map) - set(old_map)):
        print(f'[差异] 仅重算有 {k}')
        diffs += 1
    for k in sorted(set(old_map) & set(new_map)):
        for col in COLUMNS:
            if old_map[k][col] != new_map[k][col]:
                print(f'[差异] {k} {col}: 台账={old_map[k][col]!r} 重算={new_map[k][col]!r}')
                diffs += 1
    print(f'[比对] 台账 {old.height} 行 / 重算 {df.height} 行，逐字段差异 {diffs} 处')
    return diffs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--write', action='store_true', help='覆盖写台账（默认只比对）')
    args = ap.parse_args()
    df, rounding, _ = build()
    print(df.select(['ts_code', 'trade_date', 'close', 'vwap', 'next_open_gap_pct',
                     'index_benchmark', 'index_return_pct']))
    for r in rounding:
        print(f'[舍入] {r}')
    diffs = compare(df)
    if args.write:
        if diffs:
            print('[拒绝] 与现有台账不一致，按规则不覆盖；差异见上。', file=sys.stderr)
            return 1
        df.write_csv(LEDGER)
        print(f'[写入] {LEDGER} ({df.height} 行)')
    return 0 if diffs == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
