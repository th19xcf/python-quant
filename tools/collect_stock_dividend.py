#!/usr/bin/env python3
"""分红送转事件表采集与本地合并（只写 parquet，不触碰 MySQL）。

数据源：东方财富 RPT_SHAREBONUS_DET（经 akshare ``stock_fhps_em`` 按报告期拉全市场），
每个 (代码, 报告期) 一行，含预案公告日、股权登记日、除权除息日、最新公告日、
方案进度、每 10 股派现/送转比例、总股本。

设计要点：
1. 分报告期抓取、逐期缓存，支持断点续跑与失败清单；
2. 与本地存量合并时保留全部存量行（含 2016 年前、东财缺口的票），只做增补与富化；
3. 新增 ``plan_notice_date`` / ``notice_date`` / ``progress`` / ``report_period`` /
   ``total_shares``，为点时查询提供“公告日”和“除权除息日”两个可区分时间；
4. ``total_div`` 源端不提供，统一置空，不按现金×股本臆造；
5. 最终整表原子替换，并同步 meta.json 的分红行数。

用法：
    python tools/collect_stock_dividend.py --dry-run
    python tools/collect_stock_dividend.py
    python tools/collect_stock_dividend.py --start-year 2016 --end-period 20260930
"""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'
DEFAULT_CACHE = PROJECT_ROOT / 'data/local_store/factors/rebuild/stock_dividend_parts'
DEFAULT_REPORT = PROJECT_ROOT / 'reports/stock_dividend_20261105'
SOURCE_NAME = 'em_fhps_bulk'
# 交易所除权参考价校验用的本地行情根目录与现金容差
_QUOTE_ROOT = PROJECT_ROOT / 'data/local_store/quotes/stock'
QUOTE_CASH_TOL = 1e-3

FINAL_SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'symbol': pl.Utf8,
    'name': pl.Utf8,
    'dividend_year': pl.Utf8,
    'report_date': pl.Utf8,
    'record_date': pl.Date,
    'ex_date': pl.Date,
    'pay_date': pl.Utf8,
    'cash_div': pl.Float64,
    'share_div': pl.Float64,
    'total_div': pl.Float64,
    'rights_issue_price': pl.Float64,
    'rights_issue_ratio': pl.Float64,
    'source': pl.Utf8,
    'report_period': pl.Date,
    'plan_notice_date': pl.Date,
    'notice_date': pl.Date,
    'progress': pl.Utf8,
    'total_shares': pl.Float64,
}
NEW_COLUMNS = ('report_period', 'plan_notice_date', 'notice_date', 'progress', 'total_shares')


# ----------------------------------------------------------------------
# 报告期与抓取
# ----------------------------------------------------------------------
def quarter_periods(start_year: int, end_period: str) -> list[str]:
    """生成 [start_year-1 年 Q2, end_period] 的全部季末报告期（YYYYMMDD）。"""
    periods: list[str] = []
    for year in range(start_year - 1, int(end_period[:4]) + 1):
        for md in ('0331', '0630', '0930', '1231'):
            p = f'{year}{md}'
            if p >= f'{start_year - 1}0630' and p <= end_period:
                periods.append(p)
    return periods


def _ts_code_expr() -> pl.Expr:
    sym = pl.col('symbol')
    return pl.when(sym.str.starts_with('6')).then(sym + pl.lit('.SH')).when(
        sym.str.starts_with('92') | sym.str.starts_with('8') | sym.str.starts_with('4')
    ).then(sym + pl.lit('.BJ')).otherwise(sym + pl.lit('.SZ'))


def _is_a_share(symbol: str) -> bool:
    if not symbol.isdigit() or len(symbol) != 6:
        return False
    if symbol.startswith('900') or symbol.startswith('200'):
        return False
    return symbol[0] in '0369'


def normalize_period(df: Any, period: str, start_date: date) -> pl.DataFrame:
    """把 akshare 单报告期 DataFrame 归一为本地事件表结构。"""
    required = {'代码', '名称', '预案公告日', '股权登记日', '除权除息日',
                '现金分红-现金分红比例', '送转股份-送转总比例', '方案进度',
                '最新公告日期', '总股本'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f'报告期 {period} 返回缺少字段: {sorted(missing)}')
    raw = pl.from_pandas(df, include_index=False)
    period_date = datetime.strptime(period, '%Y%m%d').date()
    out = raw.select([
        pl.col('代码').cast(pl.Utf8).str.zfill(6).alias('symbol'),
        pl.col('名称').cast(pl.Utf8).alias('name'),
        pl.lit(period[:4]).alias('dividend_year'),
        pl.lit(period_date.isoformat()).alias('report_date'),
        pl.col('股权登记日').cast(pl.Date, strict=False).alias('record_date'),
        pl.col('除权除息日').cast(pl.Date, strict=False).alias('ex_date'),
        pl.lit(None, dtype=pl.Utf8).alias('pay_date'),
        (pl.col('现金分红-现金分红比例').cast(pl.Float64, strict=False).fill_null(0.0)
         / 10.0).alias('cash_div'),
        (pl.col('送转股份-送转总比例').cast(pl.Float64, strict=False).fill_null(0.0)
         / 10.0).alias('share_div'),
        pl.lit(None, dtype=pl.Float64).alias('total_div'),
        pl.lit(None, dtype=pl.Float64).alias('rights_issue_price'),
        pl.lit(None, dtype=pl.Float64).alias('rights_issue_ratio'),
        pl.lit(SOURCE_NAME).alias('source'),
        pl.lit(period_date, dtype=pl.Date).alias('report_period'),
        pl.col('预案公告日').cast(pl.Date, strict=False).alias('plan_notice_date'),
        pl.col('最新公告日期').cast(pl.Date, strict=False).alias('notice_date'),
        pl.col('方案进度').cast(pl.Utf8).alias('progress'),
        pl.col('总股本').cast(pl.Float64, strict=False).alias('total_shares'),
    ]).with_columns(_ts_code_expr().alias('ts_code'))
    out = out.filter(pl.col('symbol').map_elements(_is_a_share, return_dtype=pl.Boolean))
    keep = (pl.col('ex_date') >= pl.lit(start_date)) | (
        pl.col('ex_date').is_null() & (pl.col('plan_notice_date') >= pl.lit(start_date))
    )
    return out.filter(keep)


def fetch_period(
    period: str,
    cache_dir: Path,
    retries: int = 3,
    sleep_s: float = 0.25,
    start_date: date | None = None,
) -> tuple[pl.DataFrame | None, dict[str, Any] | None]:
    """抓取单个报告期，优先读缓存；返回 (归一数据, 失败记录)。"""
    start_date = start_date or date(2016, 1, 1)
    cache_path = cache_dir / f'{period}.parquet'
    if cache_path.exists():
        cached = pl.read_parquet(cache_path)
        if set(FINAL_SCHEMA) <= set(cached.columns):
            return cached, None
    import akshare as ak  # noqa: PLC0415

    last_error = ''
    for attempt in range(1, retries + 1):
        try:
            raw = ak.stock_fhps_em(date=period)
            if raw is None or len(raw) == 0:
                return pl.DataFrame(schema=FINAL_SCHEMA), None
            frame = normalize_period(raw, period, start_date)
            cache_dir.mkdir(parents=True, exist_ok=True)
            tmp = cache_path.with_suffix('.parquet.tmp')
            frame.write_parquet(tmp, compression='zstd', statistics=True)
            os.replace(tmp, cache_path)
            return frame, None
        except Exception as exc:  # noqa: BLE001
            last_error = f'{type(exc).__name__}: {exc}'
            time.sleep(1.0 * attempt)
    return None, {'ts_code': 'ALL', 'report_period': period,
                  'error_type': last_error.split(':')[0], 'retries': retries,
                  'error': last_error}


# ----------------------------------------------------------------------
# 合并
# ----------------------------------------------------------------------
def _prepare_existing(existing: pl.DataFrame) -> list[dict[str, Any]]:
    for col in NEW_COLUMNS:
        if col not in existing.columns:
            existing = existing.with_columns(
                pl.lit(None, dtype=FINAL_SCHEMA[col]).alias(col))
    existing = existing.select([pl.col(c).cast(FINAL_SCHEMA[c], strict=False)
                                for c in FINAL_SCHEMA])
    return existing.to_dicts()


def _new_row(src: dict[str, Any]) -> dict[str, Any]:
    row = {c: src.get(c) for c in FINAL_SCHEMA}
    row['total_div'] = None
    return row


def _enrich(row: dict[str, Any], src: dict[str, Any]) -> None:
    for col in NEW_COLUMNS:
        if src.get(col) is not None:
            row[col] = src[col]
    for col in ('record_date', 'ex_date', 'report_period'):
        if row.get(col) is None and src.get(col) is not None:
            row[col] = src[col]
    if row.get('report_date') is None and src.get('report_date') is not None:
        row['report_date'] = src['report_date']
    if row.get('dividend_year') is None and src.get('dividend_year') is not None:
        row['dividend_year'] = src['dividend_year']
    if not row.get('name') and src.get('name'):
        row['name'] = src['name']
    if not row.get('symbol') and src.get('symbol'):
        row['symbol'] = src['symbol']


AMOUNT_TOL = 1e-4


def _amount_key(src: dict[str, Any]) -> tuple[float, float]:
    return float(src.get('cash_div') or 0.0), float(src.get('share_div') or 0.0)


def _amount_match(
    left: tuple[float, float],
    right: tuple[float, float],
    tol: float = AMOUNT_TOL,
) -> bool:
    return abs(left[0] - right[0]) <= tol and abs(left[1] - right[1]) <= tol


def _year_of(src: dict[str, Any]) -> str:
    year = src.get('dividend_year')
    if year is None and src.get('report_period') is not None:
        year = str(src['report_period'].year)
    return str(year or '')


def _event_type(src: dict[str, Any]) -> tuple[bool, bool, bool]:
    """事件类型签名：现金 / 送转 / 配股，三类允许同日并存。"""
    return (float(src.get('cash_div') or 0.0) > 0,
            float(src.get('share_div') or 0.0) > 0,
            float(src.get('rights_issue_ratio') or 0.0) > 0)


class _Merger:
    """把新事件并入存量行，保留全部存量并记录同日金额冲突。"""

    def __init__(self, existing: pl.DataFrame) -> None:
        self.rows = _prepare_existing(existing)
        self.stats: dict[str, Any] = {
            'matched_by_ex': 0, 'matched_by_plan': 0,
            'appended_same_day': 0, 'appended_plan': 0, 'enriched_rows': 0,
            'filled_ex_date': 0, 'filled_record_date': 0, 'amount_conflicts': 0,
            'same_day_conflict_rows': 0, 'claimed_placeholder': 0,
            'duplicate_source_rows': 0}
        self.claimed: set[int] = set()
        self.appends: list[dict[str, Any]] = []
        self.conflicts: list[dict[str, Any]] = []
        self.by_ex: dict[tuple[str, date], list[int]] = {}
        self.by_placeholder: dict[tuple[str, str], list[tuple[tuple[float, float], int]]] = {}
        self._index()

    def _index(self) -> None:
        for idx, row in enumerate(self.rows):
            code, ex = row.get('ts_code'), row.get('ex_date')
            if not code:
                continue
            if ex is not None:
                self.by_ex.setdefault((code, ex), []).append(idx)
            else:
                self.by_placeholder.setdefault((code, _year_of(row)), []).append(
                    (_amount_key(row), idx))

    def _plan_hit(self, code: str, year: str, amt: tuple[float, float]) -> int | None:
        for cand_amt, cand_idx in self.by_placeholder.get((code, year), []):
            if cand_idx not in self.claimed and _amount_match(cand_amt, amt):
                return cand_idx
        return None

    def _attach(self, idx: int, src: dict[str, Any], match_type: str) -> None:
        row = self.rows[idx]
        if row.get('ex_date') is None and src.get('ex_date') is not None:
            self.stats['filled_ex_date'] += 1
        if row.get('record_date') is None and src.get('record_date') is not None:
            self.stats['filled_record_date'] += 1
        _enrich(row, src)
        self.stats['enriched_rows'] += 1
        self.stats[f'matched_by_{match_type}'] += 1

    def _claim(self, idx: int, src: dict[str, Any]) -> None:
        self.claimed.add(idx)
        self.stats['claimed_placeholder'] += 1
        self._attach(idx, src, 'plan')

    def _same_day(self, grouped: dict[tuple[str, date], list[dict[str, Any]]]) -> None:
        for (code, ex), srcs in grouped.items():
            cands = self.by_ex.get((code, ex), [])
            used: set[int] = set()
            unmatched: list[dict[str, Any]] = []
            for src in srcs:
                amt = _amount_key(src)
                hit = next((i for i in cands if i not in used
                            and _amount_match(_amount_key(self.rows[i]), amt)), None)
                if hit is None:
                    unmatched.append(src)
                else:
                    used.add(hit)
                    self._attach(hit, src, 'ex')
            # 存量已有该除权日事件时不再补录：原先 capacity = len(srcs) - len(cands)
            # 在“源端同日 2 行、存量 1 行”时会算出 1，把同一笔分红被 em 按第二个
            # 报告期重复披露的行追加成新事件（300760 2025-05-29 的 0.56 即由此补录
            # 进来）。只有该键没有任何存量事件时，才按事件类型补录（现金 / 送转 /
            # 配股），且同类型最多补一条，避免同类型重复披露被当成多笔事件。
            allowed_types = set() if cands else {_event_type(src) for src in unmatched}
            for src in unmatched:
                amt = _amount_key(src)
                cand = (self._plan_hit(code, _year_of(src), amt)
                        if amt[0] > 0 or amt[1] > 0 else None)
                if cand is not None:
                    self._claim(cand, src)
                elif _event_type(src) in allowed_types:
                    self.appends.append(_new_row(src))
                    self.stats['appended_same_day'] += 1
                    allowed_types.discard(_event_type(src))
                else:
                    self._conflict(code, ex, cands, src)

    def _conflict(
        self,
        code: str,
        ex: date,
        cands: list[int],
        src: dict[str, Any],
    ) -> None:
        self.stats['same_day_conflict_rows'] += 1
        self.stats['amount_conflicts'] += 1
        amt = _amount_key(src)
        local_row = self.rows[cands[0]] if cands else {}
        local_amt = _amount_key(local_row)
        if len(cands) == 1:
            # 金额存疑不代表公告日/进度不可用，点时字段仍按源端补齐
            _enrich(self.rows[cands[0]], src)
            self.stats['enriched_rows'] += 1
        self.conflicts.append({'ts_code': code, 'ex_date': ex,
                               'local_cash': local_amt[0], 'local_share': local_amt[1],
                               'src_cash': amt[0], 'src_share': amt[1],
                               'local_rights': float(
                                   local_row.get('rights_issue_ratio') or 0.0),
                               'src_rights': float(src.get('rights_issue_ratio') or 0.0)})

    def _dedup_source_rows(
        self, rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """同日同键的源端重复披露去重（按 ``(每股现金, 每股送转)`` 判重）。

        东财按报告期返回，同一个除权日常被相邻两个报告期各披露一行。金额完全
        相同的两行是同一笔事件的重复披露，只保留先出现的一行（报告期最早），
        重复计数记入 ``duplicate_source_rows``。判重键带金额，因此同日“现金分红
        + 送转”两条不同类型事件不会被误吞（000002 这类同日多类型事件仍按两条
        正常入库）。
        """
        seen: set[tuple[str, date, float, float]] = set()
        kept: list[dict[str, Any]] = []
        for src in rows:
            cash, share = _amount_key(src)
            key = (src['ts_code'], src['ex_date'], round(cash, 6), round(share, 6))
            if key in seen:
                self.stats['duplicate_source_rows'] += 1
                continue
            seen.add(key)
            kept.append(src)
        return kept

    def _plans(self, plans: list[dict[str, Any]]) -> None:
        for src in plans:
            idx = self._plan_hit(src['ts_code'], _year_of(src), _amount_key(src))
            if idx is not None:
                self._claim(idx, src)
            else:
                self.appends.append(_new_row(src))
                self.stats['appended_plan'] += 1

    def run(self, new: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any], list[dict[str, Any]]]:
        rows = new.to_dicts()
        implemented = self._dedup_source_rows(
            [r for r in rows if r.get('ex_date') is not None])
        plans = [r for r in rows if r.get('ex_date') is None]
        grouped: dict[tuple[str, date], list[dict[str, Any]]] = {}
        for src in implemented:
            grouped.setdefault((src['ts_code'], src['ex_date']), []).append(src)
        self._same_day(grouped)
        self._plans(plans)
        for row in self.rows:
            row['total_div'] = None
        final = pl.DataFrame([*self.rows, *self.appends], schema=FINAL_SCHEMA, strict=False)
        final = final.sort(['ts_code', 'ex_date', 'report_period'], nulls_last=True)
        return final, self.stats, self.conflicts


def merge_frames(
    existing: pl.DataFrame,
    new: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    """存量行全部保留；新行按同日金额配对，缺事件才补录，其余记冲突。

    同一除权日可能并存多笔分红（年度 + 季度/特别分红、现金 + 送转），因此
    不能简单按 ``(ts_code, ex_date)`` 折叠。策略：
    1. 源端同日同键先按 (每股现金, 每股送转) 去重，重复披露只保留一行；
    2. 同日按 (每股现金, 每股送转) 与存量行一一配对；
    3. 该 ``(ts_code, ex_date)`` 已有存量事件时不再补录：未配对源端行一律记为
       金额冲突。历史上按 ``源端笔数 - 存量笔数`` 补录，会把同一笔分红被东财
       按第二个报告期重复披露的行追加成新事件（300760 2025-05-29 的 0.56 即
       由此产生）；
    4. 该键没有任何存量事件时，按事件类型（现金 / 送转 / 配股）补录，每类
       最多一条，避免同类型重复披露被当成多笔事件。
    """
    return _Merger(existing).run(new)


def _quote_frame(
    code: str, cache: dict[str, pl.DataFrame | None],
) -> pl.DataFrame | None:
    """按代码读取本地行情（只取校验所需列），缺失或读取失败返回 None。"""
    if code in cache:
        return cache[code]
    market = code.rsplit('.', 1)[-1].lower()
    path = _QUOTE_ROOT / market / f'{code}.parquet'
    frame: pl.DataFrame | None = None
    if path.exists():
        try:
            frame = pl.read_parquet(
                path, columns=['date', 'close', 'pre_close', 'qfq_factor']).sort('date')
        except (OSError, pl.PolarsError):
            frame = None
    cache[code] = frame
    return frame


def quote_implied_cash(
    code: str,
    ex_date: date,
    share_div: float,
    rights_ratio: float,
    cache: dict[str, pl.DataFrame | None],
) -> float | None:
    """用交易所除权参考价反推每股现金分红，无法可靠反推时返回 None。

    本地行情 ``pre_close`` 不是“上一交易日原始收盘价”，而是按 qfq 因子还原的
    除权参考价（见 ``src/data/local_store/build.py``）：除权日当天
    ``pre_close = (prev_close - cash + rights_price*ratio) / (1 + share + ratio)``，
    因此纯现金 / 现金+送转事件可反解 ``cash = prev_close - pre_close*(1+share)``。

    只有送转、只有配股的冲突跳过校验：前者参考价里不含现金信息，后者还需要
    配股价。若行情缺该日、缺上一交易日，或前后 qfq 因子相同（说明该日参考价
    未被复权调整、因子快照无事件），同样返回 None，避免误判。
    """
    if rights_ratio > 0:
        return None
    frame = _quote_frame(code, cache)
    if frame is None or frame.is_empty():
        return None
    dates = frame['date'].to_list()
    try:
        pos = dates.index(ex_date)
    except ValueError:
        return None
    if pos == 0:
        return None
    prev_close = frame['close'][pos - 1]
    ref_pre_close = frame['pre_close'][pos]
    factor_prev = frame['qfq_factor'][pos - 1]
    factor_cur = frame['qfq_factor'][pos]
    if None in (prev_close, ref_pre_close, factor_prev, factor_cur):
        return None
    if factor_cur == 0 or abs(factor_prev / factor_cur - 1.0) < 1e-12:
        return None
    return float(prev_close) - float(ref_pre_close) * (1.0 + share_div)


def cninfo_table(code: str, cache_dir: Path) -> pl.DataFrame:
    """巨潮单票分红明细（仅用于同日金额冲突仲裁，按代码缓存）。"""
    symbol = code.split('.')[0]
    cache_path = cache_dir / 'cninfo' / f'{symbol}.parquet'
    if cache_path.exists():
        return pl.read_parquet(cache_path)
    import akshare as ak  # noqa: PLC0415

    empty = pl.DataFrame(schema={'ex_date': pl.Date, 'cash_div': pl.Float64,
                                 'share_div': pl.Float64, 'pay_date': pl.Utf8,
                                 'notice_date': pl.Date})
    try:
        raw = ak.stock_dividend_cninfo(symbol=symbol)
    except Exception:  # noqa: BLE001
        return empty
    need = {'除权日', '派息比例', '送股比例', '转增比例', '派息日', '实施方案公告日期'}
    if raw is None or len(raw) == 0 or not need <= set(raw.columns):
        return empty
    out = pl.from_pandas(raw, include_index=False).select([
        pl.col('除权日').cast(pl.Date, strict=False).alias('ex_date'),
        (pl.col('派息比例').cast(pl.Float64, strict=False).fill_null(0.0) / 10.0).alias('cash_div'),
        ((pl.col('送股比例').cast(pl.Float64, strict=False).fill_null(0.0)
          + pl.col('转增比例').cast(pl.Float64, strict=False).fill_null(0.0)) / 10.0).alias('share_div'),
        pl.col('派息日').cast(pl.Date, strict=False).cast(pl.Utf8).alias('pay_date'),
        pl.col('实施方案公告日期').cast(pl.Date, strict=False).alias('notice_date'),
    ]).drop_nulls('ex_date')
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_path.with_suffix('.parquet.tmp')
    out.write_parquet(tmp, compression='zstd', statistics=True)
    os.replace(tmp, cache_path)
    return out


def reconcile_conflicts(
    frame: pl.DataFrame,
    conflicts: list[dict[str, Any]],
    cache_dir: Path,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """用巨潮仲裁同日金额冲突：本地对则保留，源端对则修正，双方都不对则留证。

    采纳源端数值（``corrected_to_source``）之前，再用交易所除权参考价
    （行情 ``pre_close``，见 :func:`quote_implied_cash`）复核：反推的现金分红与
    源端候选差超过 ``QUOTE_CASH_TOL`` 时判定源端不可信，记 ``unresolved`` 并
    保留本地值。只有送转、只有配股的冲突不参与参考价校验。
    """
    stats: dict[str, Any] = {'checked': 0, 'kept_local': 0, 'corrected_to_source': 0,
                             'unresolved': 0, 'quote_checked': 0,
                             'quote_mismatch': 0, 'unresolved_rows': []}
    if not conflicts:
        return frame, stats
    tables = {c['ts_code']: cninfo_table(c['ts_code'], cache_dir)
              for c in {c['ts_code']: c for c in conflicts}.values()}
    quotes: dict[str, pl.DataFrame | None] = {}
    replacements: list[dict[str, Any]] = []
    for item in conflicts:
        stats['checked'] += 1
        table = tables[item['ts_code']]
        same = table.filter(pl.col('ex_date') == item['ex_date'])
        cn_amts = [_amount_key(r) for r in same.to_dicts()]
        local = (item['local_cash'], item['local_share'])
        source = (item['src_cash'], item['src_share'])
        if any(_amount_match(local, a, tol=1e-3) for a in cn_amts):
            stats['kept_local'] += 1
        elif any(_amount_match(source, a, tol=1e-3) for a in cn_amts):
            implied = quote_implied_cash(
                item['ts_code'], item['ex_date'],
                item.get('src_share', 0.0), item.get('src_rights', 0.0), quotes)
            if implied is not None:
                stats['quote_checked'] += 1
                if abs(implied - item['src_cash']) > QUOTE_CASH_TOL:
                    stats['quote_mismatch'] += 1
                    stats['unresolved'] += 1
                    stats['unresolved_rows'].append(
                        {**item, 'reason': 'quote_mismatch', 'quote_cash': implied})
                    continue
            stats['corrected_to_source'] += 1
            replacements.append(item)
        else:
            stats['unresolved'] += 1
            stats['unresolved_rows'].append(item)
    if replacements:
        cash_expr: pl.Expr = pl.col('cash_div')
        share_expr: pl.Expr = pl.col('share_div')
        for item in replacements:
            cond = ((pl.col('ts_code') == item['ts_code'])
                    & (pl.col('ex_date') == item['ex_date'])
                    & ((pl.col('cash_div') - item['local_cash']).abs() <= 1e-4)
                    & ((pl.col('share_div') - item['local_share']).abs() <= 1e-4))
            cash_expr = pl.when(cond).then(pl.lit(item['src_cash'])).otherwise(cash_expr)
            share_expr = pl.when(cond).then(pl.lit(item['src_share'])).otherwise(share_expr)
        frame = frame.with_columns(cash_expr.alias('cash_div'), share_expr.alias('share_div'))
    return frame, stats


# ----------------------------------------------------------------------
# 统计与输出
# ----------------------------------------------------------------------
def frame_stats(frame: pl.DataFrame) -> dict[str, Any]:
    for col in NEW_COLUMNS:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=FINAL_SCHEMA[col]).alias(col))
    ev = frame.filter(pl.col('ex_date').is_not_null())
    by_year = (ev.with_columns(pl.col('ex_date').dt.year().alias('y'))
               .group_by('y').agg(pl.len().alias('n')).sort('y').to_dicts())
    total = frame.height
    null_ex = total - ev.height
    return {
        'rows': total,
        'codes': frame['ts_code'].n_unique(),
        'event_codes': ev['ts_code'].n_unique(),
        'ex_date_null': null_ex,
        'ex_date_null_rate': round(null_ex / total, 4) if total else 0.0,
        'latest_ex_date': str(ev['ex_date'].max()) if ev.height else None,
        'earliest_ex_date': str(ev['ex_date'].min()) if ev.height else None,
        'year_dist': {str(r['y']): r['n'] for r in by_year},
        'rows_2026': ev.filter(pl.col('ex_date') >= date(2026, 1, 1)).height,
        'codes_2026': ev.filter(pl.col('ex_date') >= date(2026, 1, 1))['ts_code'].n_unique(),
        'has_plan_notice': frame.filter(pl.col('plan_notice_date').is_not_null()).height,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                   encoding='utf-8', newline='\n')
    os.replace(tmp, path)


def load_existing(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f'本地分红表不存在: {path}')
    return pl.read_parquet(path)


def update_meta(out_path: Path, rows: int) -> None:
    meta_path = out_path.parent / 'meta.json'
    if not meta_path.exists():
        return
    payload = json.loads(meta_path.read_text(encoding='utf-8'))
    payload['dividend_rows'] = rows
    payload['dividend_updated_at'] = datetime.now().isoformat(timespec='seconds')
    payload['dividend_source'] = 'em_fhps_bulk+local_preserved'
    payload['dividend_columns'] = list(FINAL_SCHEMA)
    tmp = meta_path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                   encoding='utf-8', newline='\n')
    os.replace(tmp, meta_path)


def select_as_of(
    frame: pl.DataFrame,
    as_of: date,
    *,
    implemented_only: bool = False,
) -> pl.DataFrame:
    """点时过滤：只保留公告日不晚于 ``as_of`` 的记录。

    公告日取 ``plan_notice_date``（预案公告日），缺失时退回 ``notice_date``
    （实施/最新公告日）。两者都缺失的历史记录视为“公告日未知”，不纳入结果，
    避免把尚不可见的事件泄露到时点查询中。
    """
    visible = pl.coalesce([pl.col('plan_notice_date'), pl.col('notice_date')])
    out = frame.filter(visible.is_not_null() & (visible <= pl.lit(as_of)))
    if implemented_only:
        out = out.filter(pl.col('ex_date').is_not_null()
                         & (pl.col('ex_date') <= pl.lit(as_of)))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='采集全市场分红送转事件并合并到本地 parquet')
    parser.add_argument('--out', type=Path, default=DEFAULT_OUT)
    parser.add_argument('--cache', type=Path, default=DEFAULT_CACHE)
    parser.add_argument('--report-dir', type=Path, default=DEFAULT_REPORT)
    parser.add_argument('--start-year', type=int, default=2016)
    parser.add_argument('--end-period', default='20260930')
    parser.add_argument('--retries', type=int, default=3)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--skip-fetch', action='store_true', help='只用缓存，不访问网络')
    parser.add_argument('--no-reconcile', dest='reconcile', action='store_false',
                        help='跳过同日金额冲突的巨潮仲裁')
    args = parser.parse_args(argv)

    start_date = date(args.start_year, 1, 1)
    periods = quarter_periods(args.start_year, args.end_period)
    existing = load_existing(args.out)
    before = frame_stats(existing)
    args.report_dir.mkdir(parents=True, exist_ok=True)
    progress_path = args.report_dir / 'progress.json'
    failed_csv = args.report_dir / 'failed.csv'

    frames: list[pl.DataFrame] = []
    failures: list[dict[str, Any]] = []
    progress: dict[str, Any] = {'source': SOURCE_NAME, 'periods': periods,
                                'completed': [], 'failed': [], 'updated_at': ''}
    for i, period in enumerate(periods, 1):
        if args.skip_fetch and not (args.cache / f'{period}.parquet').exists():
            continue
        frame, failure = fetch_period(period, args.cache, retries=args.retries,
                                      sleep_s=0.25, start_date=start_date)
        if failure is not None:
            failures.append(failure)
            progress['failed'].append(period)
            print(f'[{i}/{len(periods)}] {period} FAILED {failure["error_type"]}',
                  flush=True)
        elif frame is not None:
            frames.append(frame)
            progress['completed'].append(period)
            print(f'[{i}/{len(periods)}] {period} rows={frame.height}', flush=True)
        progress['updated_at'] = datetime.now().isoformat(timespec='seconds')
        write_json(progress_path, progress)
        time.sleep(0.25)

    if failures:
        import csv  # noqa: PLC0415

        with failed_csv.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=['ts_code', 'report_period',
                                                    'error_type', 'retries', 'error'])
            writer.writeheader()
            writer.writerows(failures)

    if not frames:
        print('没有可用新数据，未写盘')
        return 0
    new = pl.concat(frames, how='vertical_relaxed') if len(frames) > 1 else frames[0]
    final, merge_stats, conflicts = merge_frames(existing, new)
    if args.reconcile and conflicts:
        final, reconcile_stats = reconcile_conflicts(final, conflicts, args.cache)
    else:
        reconcile_stats = {'checked': 0, 'kept_local': 0, 'corrected_to_source': 0,
                           'unresolved': 0, 'quote_checked': 0,
                           'quote_mismatch': 0, 'unresolved_rows': []}
    after = frame_stats(final)

    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'source': SOURCE_NAME,
        'periods_requested': periods,
        'periods_failed': [f['report_period'] for f in failures],
        'new_source_rows': new.height,
        'new_source_codes': new['ts_code'].n_unique(),
        'new_source_implemented': new.filter(pl.col('ex_date').is_not_null()).height,
        'new_source_plans': new.filter(pl.col('ex_date').is_null()).height,
        'merge': merge_stats,
        'reconcile': reconcile_stats,
        'before': before,
        'after': after,
    }

    # 源端与存量的已实施事件差集（用于证据）
    existing_ev = existing.filter(pl.col('ex_date').is_not_null(),
                                  pl.col('ex_date') >= pl.lit(start_date))
    new_ev = new.filter(pl.col('ex_date').is_not_null())
    report['source_only_events'] = new_ev.join(
        existing_ev, on=['ts_code', 'ex_date'], how='anti').height
    report['local_only_events'] = existing_ev.join(
        new_ev, on=['ts_code', 'ex_date'], how='anti').height
    report['amount_conflict_sample'] = (
        new_ev.join(existing_ev, on=['ts_code', 'ex_date'], how='inner', suffix='_loc')
        .filter((pl.col('cash_div') - pl.col('cash_div_loc')).abs() > 1e-4)
        .select(['ts_code', 'ex_date', 'cash_div', 'cash_div_loc'])
        .head(20).to_dicts())

    write_json(args.report_dir / 'verification.json', report)
    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0

    tmp = args.out.with_suffix('.parquet.tmp')
    final.write_parquet(tmp, compression='zstd', statistics=True)
    os.replace(tmp, args.out)
    update_meta(args.out, final.height)
    print(f'已写入 {args.out}：{final.height:,} 行（原 {existing.height:,} 行，'
          f'新增 {merge_stats["appended_same_day"] + merge_stats["appended_plan"]:,} 行）')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
