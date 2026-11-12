"""产出 ``docs/evidence/no_ex_rights_events/no_ex_rights_events.csv``（除权事件异常台账）。

## 判定规则（显式化）

分两类，规则强度不同：

### A. ``dividend_event``（5 行，可由规则唯一派生）

台账 = 分红表 ``stock_dividend.parquet`` 中**被标注过异常修正**的事件行：

- ``no_ex_rights == True``，或
- ``share_div_fix`` 非空，或
- ``cash_div_fix`` 非空

这三条并集在当前表上恰好 5 行（600225.SH / 600306.SH / 601258.SH / 603555.SH / 603603.SH）。
每行的 ``basis`` 按表内标记分支：

- 仅 ``no_ex_rights=True``：原始价未按送转比例除权（重整/转增不除权），只置标记。
- ``share_div_fix`` 非空：比例失真，按实测 ``f = 前收 / baostock preclose − 1``（6 位小数）修正。
- ``cash_div_fix`` 非空：送转不生效 + 补漏记现金 ``cash_div_fix``。

``share_div_original`` / ``cash_div_original`` 取**改前**归档表
``logs/no_ex_rights_fix_20261105/factors/stock_dividend.parquet`` 的同 ``(ts_code, ex_date)`` 原始值；
``prev_close`` / ``event_close`` / ``gap_pct`` / ``baostock_preclose`` 取 baostock 二次源
``_staging/backfill_20190917/new/{ts_code}.parquet``（原始取证用的就是这一份，不是本地 store）。

### B. ``share_change_no_dividend_row``（2 行，**无法由规则唯一还原**）

判据原描述：股本表有「转增股上市」，分红表无对应除权事件，且当日原始价没有按送转比例跳空。

用该判据的朴素实现（``stock_shares`` 中 ``change_reason`` 含「转增」且同一 ``(ts_code, effective_date)``
在分红表无 ``share_div > 0`` 的行）在现有数据上会得到 **653 个候选 / 577 只**，远多于台账的 2 行：
候选里绝大多数是正常转增（分红表以其他形式记录了送转，或当日停牌无 bar），
要靠当时的人工上下文（重整计划执行、转增不除权）才能筛到最终 2 只。
因此这 2 行按**已定稿人工清单**回放，脚本内保留 ``SHARE_ONLY`` 常量并断言其在股本表中的证据仍成立。

用法::

    python tools/anomalies/build_no_ex_rights_events.py            # 打印结果并与现有台账逐行比对
    python tools/anomalies/build_no_ex_rights_events.py --write    # 覆盖写台账（需显式开启）
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
# 2026-11-12 起台账属一次性核对证据，落 docs/evidence 随 Git 留存，不再写正式层
LEDGER = REPO / 'docs' / 'evidence' / 'no_ex_rights_events' / 'no_ex_rights_events.csv'
BK_DIV = REPO / 'logs' / 'no_ex_rights_fix_20261105' / 'factors' / 'stock_dividend.parquet'
SHARES = STORE / 'meta' / 'factors' / 'stock_shares.parquet'
STAGING = REPO / '_staging' / 'backfill_20190917' / 'new'
DETECTED_AT = '2026-11-05'
COLUMNS = ['ts_code', 'ex_date', 'kind', 'share_div_original', 'cash_div_original',
           'prev_close', 'event_close', 'gap_pct', 'baostock_preclose', 'basis', 'detected_at']

# B 类：已定稿人工清单（朴素规则给出 653 候选，不能唯一还原，理由见模块 docstring）
SHARE_ONLY = [('300116.SZ', '2020-03-31'), ('300362.SZ', '2021-06-15')]

BASIS_NO_FIX = ('原始价未按该送转比例除权（当日收盘/前收与事件前连续），'
                '判定为重整/转增不除权；原始 share_div 保留、仅置 no_ex_rights=true')
BASIS_SHARE_FIX = ('比例失真按实测 f 修正：share_div_fix={fix}'
                   '（前收 {prev} / baostock preclose {bs} − 1，原始值未四舍五入计算）；'
                   '原始 share_div {raw} 保留、no_ex_rights 改回 false')
BASIS_CASH_FIX = ('送转不生效 + 补漏记现金 {cash}：share_div_fix={fix}、cash_div_fix={cash}'
                  '（前收 {prev} − {cash} = baostock preclose {bs}）；'
                  '原始 share_div {raw} / cash_div {raw_cash} 保留、no_ex_rights 改回 false')
BASIS_SHARE_ONLY = ('股本表有“转增股上市”，分红表无对应除权事件；当日原始价无除权跳空，'
                    '判定为重整转增不除权，不补事件行，仅入异常清单')


def to_date(s: str) -> date:
    return date(int(s[:4]), int(s[5:7]), int(s[8:10]))


def event_quotes(ts_code: str, d: date) -> tuple[float, float, float]:
    """返回 (prev_close, event_close, baostock_preclose)，均取 baostock 二次源。"""
    q = pl.read_parquet(STAGING / f'{ts_code}.parquet', columns=['date', 'close', 'pre_close']).sort('date')
    prev = q.filter(pl.col('date') < d).tail(1)
    cur = q.filter(pl.col('date') == d)
    if prev.height != 1 or cur.height != 1:
        raise SystemExit(f'{ts_code} {d}: baostock 二次源缺事件日或前一交易日')
    return float(prev['close'][0]), float(cur['close'][0]), float(cur['pre_close'][0])


def dividend_events() -> list[dict]:
    cur = pl.read_parquet(STORE / 'factors' / 'stock_dividend.parquet')
    old = pl.read_parquet(BK_DIV)
    sel = cur.filter((pl.col('no_ex_rights') == True)  # noqa: E712
                     | pl.col('share_div_fix').is_not_null()
                     | pl.col('cash_div_fix').is_not_null())
    raw = {(r['ts_code'], r['ex_date']): r for r in old.select(
        ['ts_code', 'ex_date', 'share_div', 'cash_div']).iter_rows(named=True)}

    out = []
    for r in sel.sort('ts_code').iter_rows(named=True):
        key = (r['ts_code'], r['ex_date'])
        if key not in raw:
            raise SystemExit(f'{key} 在改前归档分红表中缺失，无法取原始值')
        d = r['ex_date']
        prev, close, bs = event_quotes(r['ts_code'], d)
        fix_s, fix_c = r['share_div_fix'], r['cash_div_fix']
        if fix_c is not None:
            basis = BASIS_CASH_FIX.format(cash=fix_c, fix=fix_s, prev=prev, bs=bs,
                                          raw=raw[key]['share_div'],
                                          raw_cash=f"{raw[key]['cash_div']:g}")
        elif fix_s is not None:
            fix = round(prev / bs - 1, 6)
            basis = BASIS_SHARE_FIX.format(fix=fix, prev=prev, bs=bs, raw=raw[key]['share_div'])
        else:
            basis = BASIS_NO_FIX
        out.append({
            'ts_code': r['ts_code'], 'ex_date': str(d), 'kind': 'dividend_event',
            'share_div_original': str(raw[key]['share_div']),
            'cash_div_original': str(raw[key]['cash_div']),
            'prev_close': str(prev), 'event_close': str(close),
            'gap_pct': str(round((close / prev - 1) * 100, 4)),
            'baostock_preclose': str(bs), 'basis': basis, 'detected_at': DETECTED_AT,
        })
    if len(out) != 5:
        raise SystemExit(f'A 类事件派生得到 {len(out)} 行，与台账 5 行不符，规则可能已失效')
    return out


def share_only_events() -> list[dict]:
    sh = pl.read_parquet(SHARES)
    out = []
    for ts_code, ex in SHARE_ONLY:
        d = to_date(ex)
        hit = sh.filter((pl.col('ts_code') == ts_code) & (pl.col('effective_date') == d)
                        & pl.col('change_reason').str.contains('转增'))
        if hit.height != 1:
            raise SystemExit(f'{ts_code} {ex}: 股本表「转增股上市」证据不成立')
        prev, close, bs = event_quotes(ts_code, d)
        out.append({
            'ts_code': ts_code, 'ex_date': ex, 'kind': 'share_change_no_dividend_row',
            'share_div_original': None, 'cash_div_original': None,
            'prev_close': str(prev), 'event_close': str(close),
            'gap_pct': str(round((close / prev - 1) * 100, 4)),
            'baostock_preclose': str(bs), 'basis': BASIS_SHARE_ONLY, 'detected_at': DETECTED_AT,
        })
    return out


def naive_share_candidates() -> int:
    """朴素规则的候选数，仅用于说明 B 类不可唯一还原。"""
    sh = pl.read_parquet(SHARES).filter(pl.col('change_reason').str.contains('转增'))
    div = pl.read_parquet(STORE / 'factors' / 'stock_dividend.parquet').filter(pl.col('share_div') > 0)
    have = set(zip(div['ts_code'].to_list(), div['ex_date'].to_list(), strict=False))
    return sum(1 for r in sh.iter_rows(named=True)
               if (r['ts_code'], r['effective_date']) not in have)


def build() -> pl.DataFrame:
    if not BK_DIV.exists():
        raise SystemExit(f'缺少改前归档分红表：{BK_DIV}')
    if not STAGING.is_dir():
        raise SystemExit(f'缺少 baostock 二次源目录：{STAGING}')
    rows = dividend_events() + share_only_events()
    return pl.DataFrame(rows, schema=dict.fromkeys(COLUMNS, pl.Utf8))


def compare(df: pl.DataFrame) -> int:
    if not LEDGER.exists():
        print(f'[i] 现有台账不存在：{LEDGER}')
        return 1
    old = pl.read_csv(LEDGER, schema_overrides=dict.fromkeys(COLUMNS, pl.Utf8))
    diffs = 0
    if old.height != df.height:
        print(f'[差异] 行数 {old.height} vs {df.height}')
        diffs += 1
    old_map = {(r['ts_code'], r['ex_date']): r for r in old.iter_rows(named=True)}
    new_map = {(r['ts_code'], r['ex_date']): r for r in df.iter_rows(named=True)}
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
    if old.height == df.height and diffs == 0 and \
            old['ts_code'].to_list() != df['ts_code'].to_list():
        print('[顺序] 行序与历史台账不同（内容逐字段一致）：'
              f'台账={old["ts_code"].to_list()} 重算={df["ts_code"].to_list()}')
    print(f'[比对] 台账 {old.height} 行 / 重算 {df.height} 行，逐字段差异 {diffs} 处')
    return diffs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--write', action='store_true', help='覆盖写台账（默认只比对）')
    args = ap.parse_args()
    df = build()
    print(df.select(['ts_code', 'ex_date', 'kind', 'share_div_original', 'cash_div_original',
                     'prev_close', 'event_close', 'gap_pct', 'baostock_preclose']))
    print(f'[说明] B 类朴素规则候选数 = {naive_share_candidates()}（台账仅 2 行，需人工上下文筛定）')
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
