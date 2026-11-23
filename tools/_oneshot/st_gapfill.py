#!/usr/bin/env python3
"""补齐本地 ST 区间表的缺口（2026-11-21）。

做法：
1. 备份 `st_intervals.parquet` -> `st_intervals_backup_20261121.parquet`（不覆盖式改写）。
2. 用 tushare `namechange` 重建的区间与本地表做区间级差集，得到「tushare 有、本地没有」的缺口段。
3. 用本地行情做独立验证：ST 股主板 ±5% 涨跌幅，逐段统计 |日收益| <= 5.3% 的占比
   （创业板/科创板 2020-08-24 起 ±20%，按板块放宽）。>=95% 判 confirmed，<85% 判 contradicted，
   其余 ambiguous —— 不把 tushare 当唯一真值。
4. 只把 confirmed 的缺口写入权威表（source=tushare_namechange_gapfill）；contradicted / ambiguous
   只记录、不写入。反向差异（本地有、tushare 没有）同样做涨跌幅验证，只出审计 CSV，不删任何行。

产物：`work/ST退市过滤_2026-11-21/gap_audit.csv`、`local_only_audit.csv`、`gapfill_summary.json`
"""
from __future__ import annotations

import calendar
import glob
import json
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402

ST_DIR = ROOT / 'data/local_store/meta/st'
LOCAL = ST_DIR / 'st_intervals.parquet'
BACKUP = ST_DIR / 'st_intervals_backup_20261121.parquet'
TS = ST_DIR / 'st_tushare_intervals_20261121.parquet'
OUT = ROOT / 'work' / 'ST退市过滤_2026-11-21'
QUOTES = ROOT / 'data/local_store/quotes/stock'

ONE_DAY = timedelta(days=1)
OPEN_END = date(2099, 12, 31)
M0 = 2015 * 12 + 0          # 2015-01
M1 = 2026 * 12 + 9          # 2026-10


def quote_files() -> Dict[str, Path]:
    return {Path(p).stem: Path(p) for p in glob.glob(str(QUOTES / '**/*.parquet'), recursive=True)}


def price_stats(code: str, s: date, e: date, files: Dict[str, Path],
                pad: int = 0) -> Dict[str, float | None]:
    p = files.get(code)
    if p is None:
        return {}
    d = pl.read_parquet(p, columns=['date', 'pct_chg', 'close', 'pre_close']).drop_nulls(
        subset=['date', 'close', 'pre_close']).sort('date')
    if d.height < 7:
        return {}
    pc = d['pct_chg'].to_numpy().astype('float64')
    close = d['close'].to_numpy().astype('float64')
    pre = d['pre_close'].to_numpy().astype('float64')
    with np.errstate(divide='ignore', invalid='ignore'):
        calc = np.where(pre > 0, close / pre - 1, np.nan)
    ret = np.where(np.isfinite(pc), pc / 100.0, calc)
    dt = np.array(d['date'].to_list(), dtype='datetime64[D]')
    lo = np.searchsorted(dt, np.datetime64(s), 'left')
    hi = np.searchsorted(dt, np.datetime64(min(e, date(2026, 12, 31))), 'right')
    if pad:
        lo, hi = max(0, lo - pad), min(len(dt), hi + pad)
    if hi - lo < 6:
        return {}
    ret, d2 = ret[lo:hi], dt[lo:hi]
    gem = str(code).startswith(('300', '301', '688'))
    narrow = ~(gem & (d2 >= np.datetime64('2020-08-24')))
    ok = np.isfinite(ret) & (np.abs(np.nan_to_num(ret, nan=9.9)) <= 0.053)
    return {
        'days': int(len(ret)),
        'days5': int(narrow.sum()),
        'match5': round(float(ok[narrow].mean()), 4) if narrow.any() else None,
        'pad': pad,
    }


def verdict_of(st: Dict[str, object]) -> str:
    if not st or st.get('match5') is None or int(st.get('days5') or 0) < 5:
        return 'unknown'
    m = float(st['match5'])
    return 'confirmed' if m >= 0.95 else 'contradicted' if m < 0.85 else 'ambiguous'


def classify(code: str, a: date, b: date, files: Dict[str, Path]):
    """先按段内验证；段太短（边界毛刺）时扩窗 ±10 个交易日验证区间所处的涨跌幅制度。"""
    st = price_stats(code, a, b, files)
    v = verdict_of(st)
    if v == 'unknown':
        st2 = price_stats(code, a, b, files, pad=10)
        if st2 and st2.get('match5') is not None and int(st2.get('days5') or 0) >= 10:
            if float(st2['match5']) >= 0.95:
                return st2, 'confirmed_context'
    return st, v


def gaps_of(ts_rows: List[Tuple[date, date]], local_rows: List[Tuple[date, date]]
            ) -> List[Tuple[date, date]]:
    out: List[Tuple[date, date]] = []
    locals_sorted = sorted(local_rows)
    for s, e in sorted(ts_rows):
        cur = s
        for ls, le in locals_sorted:
            if le < cur:
                continue
            if ls > e:
                break
            if ls > cur:
                out.append((cur, min(e, ls - ONE_DAY)))
            cur = max(cur, le + ONE_DAY)
            if cur > e:
                break
        if cur <= e:
            out.append((cur, e))
    return [(a, b) for a, b in out if a <= b]


def months_in(s: date, e: date) -> set:
    if s > e:
        return set()
    a = max(M0, s.year * 12 + s.month - 1)
    b = min(M1, e.year * 12 + e.month - 1)
    return set(range(a, b + 1)) if a <= b else set()


def to_rows(df: pl.DataFrame) -> Dict[str, List[Tuple[date, date]]]:
    out: Dict[str, List[Tuple[date, date]]] = {}
    for c, s, e in zip(df['ts_code'].to_list(), df['start_date'].to_list(),
                       df['end_date'].to_list(), strict=False):
        out.setdefault(c, []).append((s, e or OPEN_END))
    return out


_MONTH_CACHE: Dict[str, set] = {}


def traded_months(code: str, files: Dict[str, Path]) -> set:
    """该股票在本地行情里有 bar 的月份（限制在 2015-01~2026-10 网格内）。"""
    if code in _MONTH_CACHE:
        return _MONTH_CACHE[code]
    p = files.get(code)
    out: set = set()
    if p is not None:
        d = pl.read_parquet(p, columns=['date'])
        for x in d['date'].to_list():
            m = x.year * 12 + x.month - 1
            if M0 <= m <= M1:
                out.add(m)
    _MONTH_CACHE[code] = out
    return out


def audit_segments(code_rows: Dict[str, List[Tuple[date, date]]], files: Dict[str, Path],
                   kinds: Tuple[str, ...]):
    rows: List[dict] = []
    cm = dict.fromkeys(kinds, 0)
    mend = dict.fromkeys(kinds, 0)
    no_data_cm = 0
    for code, segs in code_rows.items():
        traded = traded_months(code, files)
        for a, b in segs:
            st, v = classify(code, a, b, files)
            months = months_in(a, b)
            tradeable = months & traded
            no_data_cm += len(months) - len(tradeable)
            cm[v] += len(tradeable)
            mend[v] += sum(1 for m in tradeable
                           if a <= date(m // 12, m % 12 + 1,
                                        calendar.monthrange(m // 12, m % 12 + 1)[1]) <= b)
            rows.append({'ts_code': code, 'start': a, 'end': b,
                         'days': st.get('days'), 'days5': st.get('days5'),
                         'match5': st.get('match5'), 'pad': st.get('pad', 0),
                         'verdict': v, 'code_months': len(months),
                         'tradeable_months': len(tradeable)})
    return rows, cm, mend, no_data_cm


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    if not BACKUP.exists():
        shutil.copy2(LOCAL, BACKUP)

    # 始终从备份重建，保证脚本可重复执行、不会累积重复行
    local = pl.read_parquet(BACKUP)
    ts = pl.read_parquet(TS)
    files = quote_files()
    kinds = ('confirmed', 'confirmed_context', 'ambiguous', 'contradicted', 'unknown')

    lm, tm = to_rows(local), to_rows(ts)

    gap_src: Dict[str, List[Tuple[date, date]]] = {}
    for code, t_segs in tm.items():
        g = gaps_of(t_segs, lm.get(code, []))
        if g:
            gap_src[code] = g

    gap_rows, gap_cm, gap_mend, gap_no_data = audit_segments(gap_src, files, kinds)
    lo_rows, lo_cm, _, lo_no_data = audit_segments(lm, files, kinds)

    gdf = pl.DataFrame(gap_rows, infer_schema_length=None)
    ldf = pl.DataFrame(lo_rows, infer_schema_length=None)
    gdf.write_csv(OUT / 'gap_audit.csv')
    ldf.write_csv(OUT / 'local_only_audit.csv')

    add = gdf.filter(pl.col('verdict').is_in(['confirmed', 'confirmed_context']))
    now = datetime.now().replace(microsecond=0)
    adds = add.select([
        pl.col('ts_code'),
        pl.col('start').alias('start_date'),
        pl.when(pl.col('end') >= pl.lit(OPEN_END)).then(None).otherwise(pl.col('end')).alias('end_date'),
        pl.lit('st').alias('flag'),
        pl.lit('tushare_namechange_gapfill').alias('source'),
        pl.lit('exact').alias('precision'),
        pl.lit(now).alias('fetched_at'),
        pl.concat_str([
            pl.lit('limit5_match='), pl.col('match5').cast(pl.Utf8),
            pl.when(pl.col('verdict') == 'confirmed_context')
            .then(pl.lit(';window=pad10')).otherwise(pl.lit('')),
        ]).alias('evidence'),
        pl.lit(now).alias('raw_fetched_at'),
    ]).with_columns(
        pl.col('fetched_at').cast(local['fetched_at'].dtype),
        pl.col('raw_fetched_at').cast(local['raw_fetched_at'].dtype),
    ).select(local.columns)

    before_rows, before_codes = local.height, local['ts_code'].n_unique()
    merged = pl.concat([local, adds], how='vertical_relaxed').sort(['ts_code', 'start_date'])
    write_parquet_atomic(merged, LOCAL)

    summary = {
        'backup': str(BACKUP.relative_to(ROOT)),
        'ts_segments': ts.height,
        'local_rows_before': before_rows,
        'local_codes_before': before_codes,
        'gap_segments': gdf.height,
        'gap_code_months_all': int(gdf['code_months'].sum()),
        'gap_code_months_no_quote': gap_no_data,
        'gap_code_months_tradeable': int(gdf['tradeable_months'].sum()),
        'gap_tradeable_by_verdict': gap_cm,
        'gap_tradeable_month_ends_by_verdict': gap_mend,
        'gap_segments_by_verdict': {k: int((gdf['verdict'] == k).sum()) for k in kinds},
        'written_rows': adds.height,
        'written_tradeable_code_months': int(add['tradeable_months'].sum()),
        'rows_after': merged.height,
        'codes_after': merged['ts_code'].n_unique(),
        'local_only_segments': ldf.height,
        'local_only_code_months_tradeable': int(ldf['tradeable_months'].sum()),
        'local_only_code_months_no_quote': lo_no_data,
        'local_only_tradeable_by_verdict': lo_cm,
        'local_only_segments_by_verdict': {k: int((ldf['verdict'] == k).sum()) for k in kinds},
        'note': ('tushare 缺口仅在涨跌幅独立验证 confirmed 时写入；边界毛刺（<5 个交易日）用 ±10 日扩窗验证'
                 '制度归属，记 confirmed_context。ambiguous/contradicted/unknown 只记录不写入。'
                 '统计已剔除本地无行情月份（no_quote），那部分不进入回测。'
                 '反向差异（本地独有）不删除任何行；本地独有且 confirmed 视为 tushare 缺失而非本地误标。'),
    }
    (OUT / 'gapfill_summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
