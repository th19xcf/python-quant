"""把逐只取证拿到的个股分红事件补入 ``factors/stock_dividend.parquet``（DG-35 一次改完）。

流程：解析原始行 → 上市窗口过滤 → 单位/参考价两道金额对拍 →
候选表落 ``_staging/`` → 逐值对拍 → 旧表快照进 ``_backup/snapshots/`` →
``write_parquet_atomic`` 原子替换（新增 ``fetched_at`` 抓取时间列）→
重算受影响标的复权因子并刷新行情内嵌因子副本与 factors manifest。

只补「本地无记录、逐只接口有记录」的代码；不覆盖任何既有行。
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from src.data.atomic_write import write_parquet_atomic
from src.data.local_store.factor_rebuild import rebuild_factors_from_local
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore
from src.data.staging import staging_dir

ROOT = Path(__file__).resolve().parents[2]
STORE = ROOT / 'data' / 'local_store'
DIV_PATH = STORE / 'factors' / 'stock_dividend.parquet'
QUOTE_ROOT = STORE / 'quotes' / 'stock'
_EM_AMOUNT = re.compile(r'派\s*(\d+(?:\.\d+)?)\s*元')
_QUOTE_TOL = 0.005


def parse_rows(raw_path: Path) -> tuple[pl.DataFrame, dict[str, Any]]:
    """解析取证明细为待补行（不含上市窗口过滤与金额对拍）。"""
    raw = pl.read_csv(raw_path, infer_schema_length=0)
    stats: dict[str, Any] = {'raw_rows': raw.height}
    rows: list[dict[str, Any]] = []
    for rec in raw.to_dicts():
        code = (rec.get('ts_code') or '').strip()
        if not code:
            continue
        rows.append({
            'ts_code': code,
            'source': rec.get('source'),
            'symbol': rec.get('symbol'),
            'name': rec.get('name'),
            'dividend_year': rec.get('dividend_year'),
            'report_date': rec.get('report_date') or None,
            'report_period': rec.get('report_period') or None,
            'record_date': rec.get('record_date') or None,
            'ex_date': rec.get('ex_date') or None,
            'pay_date': rec.get('pay_date') or None,
            'plan_notice_date': rec.get('plan_notice_date') or None,
            'notice_date': rec.get('notice_date') or None,
            'progress': rec.get('progress') or None,
            'cash_div': float(rec['cash_div']) if rec.get('cash_div') else None,
            'share_div': float(rec['share_div']) if rec.get('share_div') else None,
            'total_shares': float(rec['total_shares']) if rec.get('total_shares') else None,
            'total_div': None,
            'rights_issue_price': None,
            'rights_issue_ratio': None,
            'plan_profile': rec.get('plan_profile') or '',
            'fetched_at': rec.get('fetched_at') or None,
        })
    return pl.DataFrame(rows), stats


def drop_out_of_window(frame: pl.DataFrame) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """剔除事件日全部早于/晚于本代码本地行情区间之外的行。

    北交所 920 段与部分重组标的的源端历史会带上「前身/上市前（新三板）」记录
    （601399.SH 有 2011 年记录而 2020 年才上市，920011.BJ 有 2024 年记录而
    本地行情自 2026-04-08 起），这类事件不在本代码行情区间内，既无法用除息
    参考价验证，也不影响本代码复权因子，一律不写入并单独留证。
    """
    keep: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for rec in frame.to_dicts():
        anchor = rec.get('ex_date') or rec.get('report_period') or rec.get('report_date')
        anchor_date = date.fromisoformat(anchor) if anchor else None
        window = _quote_window(rec['ts_code'])
        reason = ''
        if anchor_date is not None and window is not None:
            start, end = window
            if anchor_date < start:
                reason = f'{rec["ts_code"]} 事件日 {anchor_date} 早于本地行情起点 {start}'
            elif anchor_date > end:
                reason = f'{rec["ts_code"]} 事件日 {anchor_date} 晚于本地行情终点 {end}'
        if reason:
            rejected.append({'ts_code': rec['ts_code'], 'ex_date': rec.get('ex_date'),
                             'source': rec.get('source'), 'reason': reason})
            continue
        keep.append(rec)
    return (pl.DataFrame(keep, schema=frame.schema) if keep
            else frame.head(0)), rejected


def _quote_window(code: str) -> tuple[date, date] | None:
    """本代码本地行情的 [首日, 末日]；无行情文件时返回 None（不做窗口剔除）。"""
    market = code.rsplit('.', 1)[-1].lower()
    path = QUOTE_ROOT / market / f'{code}.parquet'
    if not path.exists():
        return None
    try:
        dates = pl.read_parquet(path, columns=['date'])['date']
    except (OSError, pl.PolarsError):
        return None
    if dates.len() == 0:
        return None
    return dates.min(), dates.max()


def check_units(frame: pl.DataFrame) -> dict[str, Any]:
    """单位对拍：「10派X元」文本里的金额必须等于 每股现金×10。"""
    checked = matched = 0
    mismatch: list[str] = []
    for rec in frame.to_dicts():
        text = rec.get('plan_profile') or ''
        match = _EM_AMOUNT.search(text)
        if not match or rec.get('cash_div') is None:
            continue
        checked += 1
        amount = float(match.group(1))
        if abs(amount - rec['cash_div'] * 10.0) <= 1e-6:
            matched += 1
        else:
            mismatch.append(f'{rec["ts_code"]}|{rec.get("ex_date")}|{text[:40]}|'
                            f'解析={rec["cash_div"] * 10}')
    return {'checked': checked, 'matched': matched, 'mismatch': mismatch}


def _quote_prev_ref(code: str, ex_date: date) -> tuple[float, float] | None:
    market = code.rsplit('.', 1)[-1].lower()
    path = QUOTE_ROOT / market / f'{code}.parquet'
    if not path.exists():
        return None
    try:
        frame = pl.read_parquet(path, columns=['date', 'close', 'pre_close']).sort('date')
    except (OSError, pl.PolarsError):
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
    if prev_close is None or ref_pre_close is None:
        return None
    return float(prev_close), float(ref_pre_close)


def check_reference_price(frame: pl.DataFrame) -> dict[str, Any]:
    """除息参考价对拍：cash ≈ prev_close − pre_close×(1+送转)。"""
    checked = matched = 0
    no_adjust: list[str] = []
    mismatch: list[str] = []
    missing_quote: list[str] = []
    for rec in frame.to_dicts():
        if rec.get('ex_date') is None or rec.get('cash_div') is None:
            continue
        ex_date = (rec['ex_date'] if isinstance(rec['ex_date'], date)
                   else date.fromisoformat(rec['ex_date']))
        pair = _quote_prev_ref(rec['ts_code'], ex_date)
        if pair is None:
            missing_quote.append(f'{rec["ts_code"]}|{rec["ex_date"]}')
            continue
        prev_close, ref_pre_close = pair
        share = rec.get('share_div') or 0.0
        implied = prev_close - ref_pre_close * (1.0 + share)
        if abs(ref_pre_close - prev_close) < 1e-6:
            no_adjust.append(f'{rec["ts_code"]}|{rec["ex_date"]}')
            continue
        checked += 1
        if abs(implied - rec['cash_div']) <= _QUOTE_TOL:
            matched += 1
        else:
            mismatch.append(f'{rec["ts_code"]}|{rec["ex_date"]}|解析={rec["cash_div"]:.4f}'
                            f'|参考价反推={implied:.4f}')
    return {'checked': checked, 'matched': matched, 'mismatch': mismatch,
            'no_ref_adjust': no_adjust, 'missing_quote': missing_quote}


def dedupe(frame: pl.DataFrame) -> pl.DataFrame:
    """同 (代码, 除息日, 每股现金, 每股送转) 去重：东财优先于巨潮。"""
    return (frame.with_columns(
        pl.when(pl.col('source').str.starts_with('em:')).then(0).otherwise(1)
        .alias('_rank'))
        .sort(['_rank'])
        .unique(subset=['ts_code', 'ex_date', 'cash_div', 'share_div'],
                keep='first', maintain_order=True)
        .drop('_rank'))


def build_candidate(frame: pl.DataFrame, old: pl.DataFrame) -> pl.DataFrame:
    """候选表 = 旧表（fetched_at 为空）+ 新行；列集合与顺序沿用旧表并追加 fetched_at。"""
    schema = dict(old.schema)
    schema['fetched_at'] = pl.Datetime
    old_part = old.with_columns(pl.lit(None, dtype=pl.Datetime).alias('fetched_at'))
    records = frame.to_dicts()
    columns: dict[str, pl.Series] = {}
    for col, dtype in schema.items():
        values: list[Any] = []
        for rec in records:
            if col == 'fetched_at':
                values.append(datetime.fromisoformat(rec['fetched_at'])
                              if rec.get('fetched_at') else None)
            else:
                values.append(rec.get(col))
        series = pl.Series(col, values)
        if dtype == pl.Date:
            series = series.cast(pl.String).str.to_date('%Y-%m-%d', strict=False)
        elif dtype == pl.Datetime:
            # 取证 CSV 里带微秒（2026-11-18T16:34:54.737777），datetime.fromisoformat 已解析
            series = series.cast(pl.Datetime('us'), strict=False)
        else:
            series = series.cast(dtype, strict=False)
        columns[col] = series
    new_part = pl.DataFrame(columns)
    return pl.concat([old_part, new_part], how='vertical_relaxed').select(list(schema))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rows', required=True, help='逐只取证原始行 CSV')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    raw_frame, parse_stats = parse_rows(Path(args.rows))
    # 单位对拍在窗口过滤前做：19 条外部历史记录同样要证明「10 派 X 元」没被当成 X
    unit = check_units(raw_frame)
    frame, rejected = drop_out_of_window(raw_frame)
    frame = dedupe(frame)
    ref = check_reference_price(frame)

    old = pl.read_parquet(DIV_PATH)
    # 反例自测：用既有分红行（行情窗口内）跑同一套参考价对拍，证明该判据有效
    sample = (old.filter(pl.col('ex_date').is_not_null(), pl.col('cash_div') > 0)
              .sort('ex_date', descending=True).head(120))
    selftest = check_reference_price(sample)
    codes = sorted(frame['ts_code'].unique().to_list())
    overlap = set(codes) & set(old['ts_code'].unique().to_list())
    if overlap:
        raise RuntimeError(f'补数代码与既有分红记录重叠，拒绝覆盖：{sorted(overlap)[:5]}')

    candidate = build_candidate(frame, old)
    dup = frame.group_by(['ts_code', 'ex_date']).len().filter(pl.col('len') > 1)
    stats: dict[str, Any] = {
        **parse_stats,
        'candidate_rows': frame.height, 'candidate_codes': len(codes),
        'rejected_rows': len(rejected), 'rejected_sample': rejected[:5],
        'dup_keys': dup.height,
        'unit_check': {'checked': unit['checked'], 'matched': unit['matched'],
                       'mismatch_n': len(unit['mismatch']),
                       'mismatch_sample': unit['mismatch'][:5]},
        'ref_check': {'checked': ref['checked'], 'matched': ref['matched'],
                      'mismatch_n': len(ref['mismatch']),
                      'mismatch_sample': ref['mismatch'][:5],
                      'no_ref_adjust_n': len(ref['no_ref_adjust']),
                      'missing_quote_n': len(ref['missing_quote'])},
        'self_test_existing': {
            'checked': selftest['checked'], 'matched': selftest['matched'],
            'mismatch_n': len(selftest['mismatch']),
            'mismatch_sample': selftest['mismatch'][:3],
            'no_ref_adjust_n': len(selftest['no_ref_adjust']),
            'missing_quote_n': len(selftest['missing_quote'])},
    }
    if unit['mismatch']:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        raise RuntimeError('单位对拍失败，拒绝写入')

    stage = staging_dir('stock_dividend_probe_backfill', with_date=True)
    stage.mkdir(parents=True, exist_ok=True)
    if rejected:
        rejected_frame = pl.DataFrame(rejected)
        rejected_frame.write_csv(stage / 'window_rejected.csv')
        summary = (rejected_frame.group_by('ts_code').agg(
            pl.len().alias('records'),
            pl.col('ex_date').min().alias('first_ex_date'),
            pl.col('ex_date').max().alias('last_ex_date'),
            pl.col('reason').first().alias('reason')).sort('ts_code'))
        summary.write_csv(stage / 'window_rejected_summary.csv')
        stats['window_rejected'] = str(stage / 'window_rejected_summary.csv')
    if frame.height == 0:
        # 无可补行：不改动正式表（不加空列、不重建因子），只留证并退出
        report = ROOT / 'work' / '个股分红取证_2026-11-18' / 'backfill_stats.json'
        report.write_text(json.dumps(stats, ensure_ascii=False, indent=2, default=str),
                          encoding='utf-8')
        print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
        print('无本地行情窗口内的补入行：stock_dividend 与复权因子均未改动')
        return 0

    cand_path = stage / 'stock_dividend_candidate.parquet'
    write_parquet_atomic(candidate, cand_path, compression='zstd', statistics=True)
    stats['candidate'] = str(cand_path)

    back = pl.read_parquet(cand_path)
    back_new = back.filter(pl.col('fetched_at').is_not_null())
    back_old = back.filter(pl.col('fetched_at').is_null()).drop('fetched_at')
    assert back_new.height == frame.height, '新增行数不守恒'
    assert back_old.equals(old), '既有行被改动'
    assert back.height == old.height + frame.height, '总行数不守恒'
    sort_keys = ['ts_code', 'ex_date']
    assert back_new.select(['ts_code', 'ex_date', 'cash_div', 'share_div']).sort(
        sort_keys).equals(
        frame.select(['ts_code', 'ex_date', 'cash_div', 'share_div']).sort(sort_keys)), \
        '新增行逐值对拍不一致'

    if args.dry_run:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        return 0

    snap = ROOT / '_backup' / 'snapshots' / f'stock_dividend_{date.today():%Y-%m-%d}'
    snap.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DIV_PATH, snap / 'stock_dividend.parquet.bak')
    old.write_csv(snap / 'stock_dividend_before.csv')

    factor_store = LocalFactorStore(STORE)
    factor_before = pl.read_parquet(factor_store.factor_path).filter(
        pl.col('ts_code').is_in(codes))
    shutil.copy2(factor_store.factor_path, snap / 'stock_adj_factor.parquet.bak')

    write_parquet_atomic(candidate, DIV_PATH, compression='zstd', statistics=True)
    assert pl.read_parquet(DIV_PATH).height == candidate.height, '替换后行数不一致'

    quote_store = LocalQuoteStore(STORE)
    rebuild = rebuild_factors_from_local(
        quote_store, factor_store, codes=codes, include_existing=True)
    factor_after = pl.read_parquet(factor_store.factor_path).filter(
        pl.col('ts_code').is_in(codes))

    before_q = (factor_before.group_by('ts_code').agg(
        pl.col('qfq_factor').min().alias('qfq_min_before'), pl.len().alias('rows_before')))
    after_q = (factor_after.group_by('ts_code').agg(
        pl.col('qfq_factor').min().alias('qfq_min_after'), pl.len().alias('rows_after')))
    changed = after_q.filter(pl.col('qfq_min_after') < 1.0)

    stats.update({
        'snapshot': str(snap),
        'dividend_rows_after': candidate.height,
        'dividend_codes_after': candidate['ts_code'].n_unique(),
        'factor_rebuild': {k: v for k, v in rebuild.items() if k != 'quote_factor_refresh'},
        'factor_codes': after_q.height,
        'factor_rows': factor_after.height,
        'factor_changed_codes': changed.height,
        'qfq_min_after': float(after_q['qfq_min_after'].min()) if after_q.height else None,
        'rows_before': before_q.height,
    })
    report = ROOT / 'work' / '个股分红取证_2026-11-18' / 'backfill_stats.json'
    report.write_text(json.dumps(stats, ensure_ascii=False, indent=2, default=str),
                      encoding='utf-8')
    print(json.dumps({k: v for k, v in stats.items() if k != 'rejected_sample'},
                     ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
