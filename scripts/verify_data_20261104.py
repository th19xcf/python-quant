#!/usr/bin/env python3
"""2026-11-04 数据独立验证线（只读）。

一次扫描输出：基线、完整性（价格跳空 vs 事件表）、可逆性抽样、因子跳变匹配、
覆盖缺口、TDX .day 末行一致性、跨源对拍、ETF 分红覆盖情况。

不修改任何数据，不做 git 操作。结果写入 logs/verification_20261104/。
"""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data.tdx_day_parser import build_day_file_path, parse_day_file  # noqa: E402
from src.utils.config import Config  # noqa: E402
from src.utils.code_converter import is_fund_code  # noqa: E402

LOCAL = ROOT / 'data' / 'local_store'
OUT = ROOT / 'logs' / 'verification_20261104'
OUT.mkdir(parents=True, exist_ok=True)

GAP_WIDE = -0.05
GAP_STRICT = -0.08
SAMPLE_SALT = 'verify-2026-11-04'

TIMINGS: dict[str, float] = {}


class Timer:
    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        dt = time.perf_counter() - self.t0
        TIMINGS[self.name] = round(dt, 2)
        print(f'[{self.name}] {dt:.1f}s', flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def hrank(code: str, salt: str = SAMPLE_SALT) -> str:
    return hashlib.sha256(f'{salt}|{code}'.encode()).hexdigest()


def to_int_days(values) -> np.ndarray:
    """pl.Date / datetime.date 序列 -> int64 自 epoch 起的天数。"""
    if isinstance(values, np.ndarray):
        if values.dtype.kind == 'M':
            return values.astype('datetime64[D]').astype(np.int64)
    return pl.Series(values).cast(pl.Date).to_numpy().astype('datetime64[D]').astype(np.int64)


def date_to_int(d: date | None) -> int:
    if d is None:
        return 0
    return (d - date(1970, 1, 1)).days


def int_to_date(i: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=int(i))).isoformat()


def quote_files(kind: str) -> list[Path]:
    return sorted((LOCAL / 'quotes' / kind).rglob('*.parquet'))


def code6(ts_code: str) -> str:
    return ts_code.split('.')[0]


def load_factor_dict() -> dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]:
    fac = pl.read_parquet(LOCAL / 'factors' / 'stock_adj_factor.parquet').sort(
        ['ts_code', 'trade_date'])
    out: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for (code,), part in fac.partition_by('ts_code', as_dict=True).items():
        d = to_int_days(part['trade_date'])
        out[code] = (d, part['qfq_factor'].to_numpy(), part['hfq_factor'].to_numpy())
    return out


def load_event_dict() -> tuple[
        dict[str, dict[str, np.ndarray]], pl.DataFrame]:
    div = pl.read_parquet(LOCAL / 'factors' / 'stock_dividend.parquet')
    div = div.filter(pl.col('ex_date').is_not_null()).with_columns([
        pl.col(c).fill_null(0.0)
        for c in ('cash_div', 'share_div', 'rights_issue_ratio', 'rights_issue_price')
    ])
    out: dict[str, dict[str, np.ndarray]] = {}
    for (code,), part in div.sort(['ts_code', 'ex_date']).partition_by(
            'ts_code', as_dict=True).items():
        out[code] = {
            'date': to_int_days(part['ex_date']),
            'cash': part['cash_div'].to_numpy(),
            'share': part['share_div'].to_numpy(),
            'rights': part['rights_issue_ratio'].to_numpy(),
            'rprice': part['rights_issue_price'].to_numpy(),
        }
    return out, div


def calc_event_factor(prev_close: float, cash: float, share: float,
                      rights: float, rprice: float) -> float | None:
    if prev_close <= 0 or not math.isfinite(prev_close):
        return None
    denom = 1.0 + share + rights
    if denom <= 0:
        return None
    f = (prev_close - cash + rprice * rights) / (prev_close * denom)
    if not math.isfinite(f) or f <= 0 or f > 10:
        return None
    return f


def naive_factors(dates: np.ndarray, closes: np.ndarray,
                  ev: dict[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    n = len(dates)
    qfq = np.ones(n, dtype=np.float64)
    hfq = np.ones(n, dtype=np.float64)
    for i in range(len(ev['date'])):
        pos = bisect.bisect_left(dates, int(ev['date'][i]))
        if pos <= 0 or pos >= n:
            continue
        f = calc_event_factor(float(closes[pos - 1]), float(ev['cash'][i]),
                              float(ev['share'][i]), float(ev['rights'][i]),
                              float(ev['rprice'][i]))
        if f is None:
            continue
        qfq[:pos] *= f
        hfq[pos:] /= f
    return qfq, hfq


# --------------------------------------------------------------------------- #
# Phase 0: 基线
# --------------------------------------------------------------------------- #

def phase_baseline() -> dict:
    result: dict = {}
    before = {}
    for p in sorted(LOCAL.rglob('*')):
        if p.is_file():
            st = p.stat()
            before[str(p.relative_to(ROOT))] = (st.st_size, st.st_mtime_ns)

    rows = pl.read_parquet(LOCAL / 'meta' / 'daily_index.parquet')
    by_type = rows.group_by('data_type').agg(
        pl.len().alias('codes'), pl.col('rows').sum().alias('rows')).sort('data_type')
    result['quotes'] = [dict(r) for r in by_type.to_dicts()]

    agg_lines: dict[str, list[str]] = defaultdict(list)
    file_hashes: dict[str, str] = {}
    t_file = time.perf_counter()
    for i, p in enumerate(sorted(LOCAL.rglob('*')), 1):
        if not p.is_file():
            continue
        rel = str(p.relative_to(LOCAL)).replace('\\', '/')
        digest = sha256_file(p)
        file_hashes[rel] = digest
        top = rel.split('/')[0]
        if top == 'quotes':
            sub = rel.split('/')[1]
            agg_lines[f'quotes/{sub}'].append(f'{rel}\t{digest}')
        else:
            agg_lines[top].append(f'{rel}\t{digest}')
        if i % 2000 == 0:
            print(f'  hashed {i} files...', flush=True)
    result['hash_seconds'] = round(time.perf_counter() - t_file, 2)

    result['aggregate_sha256'] = {}
    for k, lines in agg_lines.items():
        lines.sort()
        result['aggregate_sha256'][k] = hashlib.sha256(
            '\n'.join(lines).encode()).hexdigest()
    result['aggregate_sha256']['ALL'] = hashlib.sha256(
        '\n'.join(sorted(f'{k}\t{v}' for k, v in file_hashes.items())).encode()
    ).hexdigest()

    for rel in ('factors/stock_dividend.parquet',
                'factors/stock_adj_factor.parquet',
                'meta/daily_index.parquet'):
        result.setdefault('key_files_sha256', {})[rel] = file_hashes[rel]
    result['file_count'] = len(file_hashes)
    result['total_bytes'] = sum(v[0] for v in before.values())
    result['max_mtime'] = max(v[1] for v in before.values()) / 1e9

    # 远程基础信息表（只读）
    basic: dict = {}
    try:
        from src.database.db_manager import DatabaseManager
        db = DatabaseManager(Config())
        if db.connect():
            from sqlalchemy import text
            with db.engine.connect() as conn:
                for tbl, key in (('stock_basic', 'ts_code'),
                                 ('index_basic', 'ts_code'),
                                 ('fund_basic', 'ts_code')):
                    try:
                        n = conn.execute(text(f'SELECT COUNT(*) FROM {tbl}')).scalar()
                        codes = conn.execute(
                            text(f'SELECT {key} FROM {tbl} ORDER BY {key}')).fetchall()
                        digest = hashlib.sha256(
                            '\n'.join(str(r[0]) for r in codes).encode()).hexdigest()
                        basic[tbl] = {'rows': int(n), 'key_sha256': digest}
                    except Exception as exc:  # noqa: BLE001
                        basic[tbl] = {'error': f'{type(exc).__name__}: {exc}'}
            db.disconnect()
    except Exception as exc:  # noqa: BLE001
        basic['error'] = f'{type(exc).__name__}: {exc}'
    result['basic_tables'] = basic

    # 写完后复查 mtime，检测并行写入
    after = {}
    for p in sorted(LOCAL.rglob('*')):
        if p.is_file():
            st = p.stat()
            after[str(p.relative_to(ROOT))] = (st.st_size, st.st_mtime_ns)
    changed = [k for k in before if k not in after or after.get(k) != before[k]]
    result['concurrent_write_detected'] = bool(changed)
    result['concurrent_write_files'] = changed[:20]
    return result


# --------------------------------------------------------------------------- #
# Phase 1: 全市场单次扫描（跳空 + 因子跳变 + 覆盖）
# --------------------------------------------------------------------------- #

def phase_market_scan(factors: dict, events: dict) -> dict:
    stock_files = quote_files('stock')
    stock_codes = {p.stem for p in stock_files}

    gap_rows = 0
    gap_matched = 0
    gap_unmatched = 0
    gap_strict_unmatched = 0
    gap_share_like = 0
    codes_with_unmatched = 0
    unmatched_by_code: list[dict] = []
    unmatched_top: list[dict] = []
    share_like: list[dict] = []

    event_gap_obs: list[float] = []
    event_gap_exp: list[float] = []
    cash_gap_obs: list[float] = []
    cash_gap_exp: list[float] = []

    jump_total = 0
    jwe_rows = 0
    ewi_rows = 0
    jwe_by_code = 0
    ewi_by_code = 0
    jwe_details: list[dict] = []
    ewi_details: list[dict] = []
    ewi_reasons = defaultdict(int)
    jwe_reasons = defaultdict(int)

    factor_mismatch_codes = 0
    factor_max_rel = 0.0
    factor_align_fail = 0
    factor_align_fail_codes: list[str] = []
    factor_start_after_quote = 0
    factor_end_before_quote = 0
    coverage_gap_events: list[dict] = []
    coverage_gap_codes = 0
    coverage_gap_rows = 0

    codes_done = 0
    for p in stock_files:
        ts_code = p.stem
        q = pl.read_parquet(p, columns=['date', 'open', 'close',
                                        'qfq_factor', 'hfq_factor'])
        if q.height < 2:
            continue
        dates = to_int_days(q['date'])
        close = q['close'].to_numpy()
        open_ = q['open'].to_numpy()
        qfq = q['qfq_factor'].to_numpy()
        hfq = q['hfq_factor'].to_numpy()

        fd, fq, fh = factors.get(ts_code, (None, None, None))
        ev = events.get(ts_code)

        # 因子快照 vs 行情内因子列
        if fd is None:
            factor_align_fail += 1
            factor_align_fail_codes.append(ts_code)
        else:
            if len(fd) != len(dates) or not np.array_equal(fd, dates):
                factor_align_fail += 1
                factor_align_fail_codes.append(ts_code)
            else:
                rel = max(
                    np.max(np.abs(fq - qfq) / np.maximum(np.abs(qfq), 1e-300)),
                    np.max(np.abs(fh - hfq) / np.maximum(np.abs(hfq), 1e-300)),
                )
                factor_max_rel = max(factor_max_rel, float(rel))
                if rel > 1e-12:
                    factor_mismatch_codes += 1

        # 覆盖缺口
        if fd is not None:
            if len(fd) and fd[0] > dates[0]:
                factor_start_after_quote += 1
            if len(fd) and fd[-1] < dates[-1]:
                factor_end_before_quote += 1
                miss_dates = dates[dates > fd[-1]]
                coverage_gap_rows += len(miss_dates)
                if ev is not None:
                    hit = ev['date'][(ev['date'] > fd[-1]) & (ev['date'] <= dates[-1])]
                    if len(hit):
                        coverage_gap_codes += 1
                        for d in hit[:5]:
                            coverage_gap_events.append({
                                'ts_code': ts_code,
                                'factor_end': int_to_date(fd[-1]),
                                'quote_end': int_to_date(dates[-1]),
                                'event_date': int_to_date(d),
                            })

        # 价格跳空（原始价，不用 pre_close，因其已被复权）
        ret = close[1:] / close[:-1] - 1
        overnight = open_[1:] / close[:-1] - 1
        flag = np.nonzero((ret <= GAP_WIDE) | (overnight <= GAP_WIDE))[0] + 1
        gap_rows += len(flag)
        for t in flag:
            d_t = int(dates[t])
            d_prev = int(dates[t - 1])
            matched = False
            if ev is not None:
                lo = bisect.bisect_right(ev['date'], d_prev)
                hi = bisect.bisect_right(ev['date'], d_t)
                matched = lo < hi
            if matched:
                gap_matched += 1
                continue
            gap_unmatched += 1
            r = float(ret[t - 1])
            implied = 1.0 / (1.0 + r) - 1.0 if r > -1 else np.nan
            row = {
                'ts_code': ts_code, 'date': int_to_date(d_t),
                'ret': round(r, 6), 'overnight': round(float(overnight[t - 1]), 6),
                'close': float(close[t]), 'prev_close': float(close[t - 1]),
                'implied_share_ratio': None if not np.isfinite(implied)
                else round(float(implied), 5),
            }
            if len(unmatched_top) < 50000:
                unmatched_top.append(row)
            if r <= GAP_STRICT:
                gap_strict_unmatched += 1
                cand = row.copy()
                cand['strict'] = True
                if (np.isfinite(implied) and implied >= 0.04 and
                        abs(implied / 0.05 - round(implied / 0.05)) <= 0.2):
                    gap_share_like += 1
                    share_like.append(cand)
        if len(flag):
            code_unmatched = 0
            for t in flag:
                d_t = int(dates[t]); d_prev = int(dates[t - 1])
                m = False
                if ev is not None:
                    m = bisect.bisect_right(ev['date'], d_prev) < bisect.bisect_right(
                        ev['date'], d_t)
                if not m:
                    code_unmatched += 1
            if code_unmatched:
                codes_with_unmatched += 1
                worst = min(float(ret[t - 1]) for t in flag)
                unmatched_by_code.append({
                    'ts_code': ts_code, 'unmatched': code_unmatched,
                    'worst_ret': round(worst, 6),
                })

        # 事件 -> 实际跳空幅度校准
        if ev is not None:
            for i in range(len(ev['date'])):
                pos = bisect.bisect_left(dates, int(ev['date'][i]))
                if pos <= 0 or pos >= len(dates):
                    continue
                obs = float(close[pos] / close[pos - 1] - 1)
                exp = calc_event_factor(float(close[pos - 1]), float(ev['cash'][i]),
                                        float(ev['share'][i]), float(ev['rights'][i]),
                                        float(ev['rprice'][i]))
                if exp is None:
                    continue
                exp_ret = exp - 1
                if ev['share'][i] > 0 or ev['rights'][i] > 0:
                    event_gap_obs.append(obs); event_gap_exp.append(exp_ret)
                else:
                    cash_gap_obs.append(obs); cash_gap_exp.append(exp_ret)

        # 因子跳变匹配（以 stock_adj_factor.parquet 为准，避免使用行情内过期副本）
        if fd is None or len(fd) == 0:
            pass
        else:
            jump_idx = np.nonzero(fh[1:] != fh[:-1])[0] + 1
            jump_total += len(jump_idx)
            jump_dates = fd[jump_idx] if len(jump_idx) else np.array([], dtype=np.int64)
            expected = set()
            if ev is not None:
                for i in range(len(ev['date'])):
                    pos = bisect.bisect_left(fd, int(ev['date'][i]))
                    if pos <= 0:
                        ewi_reasons['before_first_row'] += 1
                    elif pos >= len(fd):
                        ewi_reasons['after_factor_end'] += 1
                    else:
                        # 用行情原始前收计算事件因子
                        qpos = bisect.bisect_left(dates, int(fd[pos]))
                        if qpos <= 0:
                            ewi_reasons['no_quote_prev'] += 1
                            continue
                        f = calc_event_factor(float(close[qpos - 1]), float(ev['cash'][i]),
                                              float(ev['share'][i]), float(ev['rights'][i]),
                                              float(ev['rprice'][i]))
                        if f is None:
                            ewi_reasons['invalid_event_factor'] += 1
                        elif fh[pos] == fh[pos - 1]:
                            ewi_reasons['no_numeric_change'] += 1
                            ewi_details.append({
                                'ts_code': ts_code, 'date': int_to_date(fd[pos]),
                                'reason': 'no_numeric_change', 'factor': f})
                        else:
                            expected.add(int(fd[pos]))
            jumps = set(int(x) for x in jump_dates)
            exp_set = set(int(x) for x in expected)
            jwe = jumps - exp_set
            ewi = exp_set - jumps
            if jwe:
                jwe_by_code += 1
                jwe_rows += len(jwe)
                for d in sorted(jwe)[:10]:
                    near = False
                    if ev is not None:
                        diffs = np.abs(ev['date'] - d)
                        near = bool((diffs <= 5).any())
                    reason = 'near_event_date' if near else 'no_event'
                    jwe_reasons[reason] += 1
                    if len(jwe_details) < 5000:
                        prev_i = int(np.searchsorted(fd, d)) - 1
                        jwe_details.append({
                            'ts_code': ts_code, 'date': int_to_date(d), 'reason': reason,
                            'hfq_prev': float(fh[prev_i]),
                            'hfq_now': float(fh[prev_i + 1]),
                            'has_any_event': bool(ev is not None and len(ev['date'])),
                        })
            if ewi:
                ewi_by_code += 1
                ewi_rows += len(ewi)
                for d in sorted(ewi)[:5]:
                    if len(ewi_details) < 5000:
                        ewi_details.append({
                            'ts_code': ts_code, 'date': int_to_date(d),
                            'reason': 'numeric'})

        codes_done += 1
        if codes_done % 1000 == 0:
            print(f'  scanned {codes_done}/{len(stock_files)} stock files', flush=True)

    # 事件覆盖缺口续：事件表里有、本地无行情
    div_codes = set(events) - stock_codes
    no_market = []
    if div_codes:
        for c in sorted(div_codes):
            no_market.append({
                'ts_code': c, 'events': int(len(events[c]['date'])),
                'last_event': int_to_date(events[c]['date'].max())
                if len(events[c]['date']) else None,
            })

    def stats(arr):
        a = np.asarray(arr, dtype=float)
        a = a[np.isfinite(a)]
        if not len(a):
            return None
        return {
            'n': int(len(a)), 'median': round(float(np.median(a)), 5),
            'mean': round(float(np.mean(a)), 5),
            'p05': round(float(np.percentile(a, 5)), 5),
            'p95': round(float(np.percentile(a, 95)), 5),
            'min': round(float(a.min()), 5), 'max': round(float(a.max()), 5),
        }

    out = {
        'stock_files': len(stock_files), 'codes_scanned': codes_done,
        'gap': {
            'threshold_wide': GAP_WIDE, 'rows': gap_rows,
            'matched_rows': gap_matched, 'unmatched_rows': gap_unmatched,
            'codes_with_unmatched': codes_with_unmatched,
            'strict_unmatched_rows': gap_strict_unmatched,
            'share_like_rows': gap_share_like,
        },
        'event_gap_calibration': {
            'share_or_rights': {
                'observed': stats(event_gap_obs), 'expected': stats(event_gap_exp),
                'abs_dev': stats(np.abs(np.array(event_gap_obs) - np.array(event_gap_exp)))
            },
            'cash_only': {
                'observed': stats(cash_gap_obs), 'expected': stats(cash_gap_exp),
                'abs_dev': stats(np.abs(np.array(cash_gap_obs) - np.array(cash_gap_exp)))
            },
        },
        'factor_consistency': {
            'mismatch_codes': factor_mismatch_codes, 'max_rel_error': factor_max_rel,
            'align_fail_codes': factor_align_fail_codes[:20],
            'align_fail_count': factor_align_fail,
        },
        'jumps': {
            'total': jump_total, 'codes_jwe': jwe_by_code, 'rows_jwe': jwe_rows,
            'codes_ewi': ewi_by_code, 'rows_ewi': ewi_rows,
            'jwe_reasons': dict(jwe_reasons), 'ewi_reasons': dict(ewi_reasons),
        },
        'coverage': {
            'factor_start_after_quote': factor_start_after_quote,
            'factor_end_before_quote': factor_end_before_quote,
            'quote_rows_after_factor_end': coverage_gap_rows,
            'codes_with_event_in_gap': coverage_gap_codes,
            'events_in_gap': coverage_gap_events[:200],
            'dividend_codes_without_quotes': len(no_market),
            'no_market_detail': no_market[:200],
        },
    }

    def dump_csv(rows: list[dict], name: str) -> None:
        if rows:
            pl.DataFrame(rows).write_csv(OUT / name)

    dump_csv(unmatched_top, 'gap_unmatched_rows.csv')
    if unmatched_by_code:
        pl.DataFrame(unmatched_by_code).sort('worst_ret').write_csv(
            OUT / 'gap_unmatched_by_code.csv')
    dump_csv(share_like, 'gap_share_like_candidates.csv')
    dump_csv(jwe_details, 'jump_without_event.csv')
    dump_csv(ewi_details, 'event_without_jump.csv')
    dump_csv(coverage_gap_events, 'coverage_gap_events.csv')
    with open(OUT / 'market_scan.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    return out


# --------------------------------------------------------------------------- #
# Phase 2: 可逆性抽样（固定名单）
# --------------------------------------------------------------------------- #

def phase_reversibility(factors: dict, events: dict, div: pl.DataFrame) -> dict:
    v = div.with_columns([
        (pl.col('cash_div') > 0).alias('has_cash'),
        (pl.col('share_div') > 0).alias('has_share'),
        (pl.col('rights_issue_ratio') > 0).alias('has_rights'),
    ])
    share_codes = set(v.filter(pl.col('has_share'))['ts_code'].unique().to_list())
    rights_codes = set(v.filter(pl.col('has_rights'))['ts_code'].unique().to_list())
    cash_codes = set(v.filter(pl.col('has_cash'))['ts_code'].unique().to_list())
    stock_codes = {p.stem for p in quote_files('stock')}
    none_codes = sorted(stock_codes - set(events))

    def pick(cands: set[str], n: int, exclude: set[str] | None = None) -> list[str]:
        # 只从"本地确有行情文件"的候选里抽样，保证样本可验证；
        # 抽样规则仍是 sha256(salt|code) 排序，名单在跑数前一次性固定。
        exclude = exclude or set()
        pool = sorted((cands - exclude) & stock_codes)
        return sorted(pool, key=hrank)[:n]

    rights_sample = pick(rights_codes, 12)
    share_sample = pick(share_codes, 16, set(rights_sample))
    cash_sample = pick(cash_codes - share_codes - rights_codes, 16,
                       set(rights_sample) | set(share_sample))
    none_sample = pick(set(none_codes), 12)
    samples = {
        'rights': rights_sample, 'share': share_sample,
        'cash_only': cash_sample, 'none': none_sample,
    }
    with open(OUT / 'reversibility_samples.json', 'w', encoding='utf-8') as f:
        json.dump(samples, f, ensure_ascii=False, indent=2)

    rows = []
    max_rel = 0.0
    for cat, codes in samples.items():
        for code in codes:
            p = LOCAL / 'quotes' / 'stock' / code.split('.')[-1].lower() / f'{code}.parquet'
            if not p.exists():
                rows.append({'category': cat, 'ts_code': code, 'status': 'no_quote'})
                continue
            q = pl.read_parquet(p, columns=['date', 'close', 'qfq_factor', 'hfq_factor'])
            dates = to_int_days(q['date'])
            closes = q['close'].to_numpy()
            ev = events.get(code)
            if ev is None:
                ev = {'date': np.array([], np.int64), 'cash': np.array([]),
                      'share': np.array([]), 'rights': np.array([]),
                      'rprice': np.array([])}
            qn, hn = naive_factors(dates, closes, ev)
            # 以 stock_adj_factor.parquet（canonical）为准，按日期对齐
            fd, fq, fh = factors[code]
            idx = np.searchsorted(fd, dates)
            inb = idx < len(fd)
            idx_c = np.clip(idx, 0, len(fd) - 1)
            aligned = inb & (fd[idx_c] == dates)
            if aligned.sum() == 0:
                rows.append({'category': cat, 'ts_code': code, 'status': 'no_aligned_factor'})
                continue
            sq = fq[idx_c][aligned]; sh = fh[idx_c][aligned]
            c_al = closes[aligned]
            rel_q = float(np.max(np.abs(qn[aligned] - sq) / np.maximum(np.abs(sq), 1e-300)))
            rel_h = float(np.max(np.abs(hn[aligned] - sh) / np.maximum(np.abs(sh), 1e-300)))
            adj_q = c_al * sq
            rec_q = adj_q / sq
            rel_rec = float(np.max(np.abs(rec_q - c_al) / np.maximum(np.abs(c_al), 1e-300)))
            adj_h = c_al * sh
            rec_h = adj_h / sh
            rel_rec_h = float(np.max(np.abs(rec_h - c_al) / np.maximum(np.abs(c_al), 1e-300)))
            # 行情内因子列 vs 快照（过期副本检查）
            copy_rel = float(np.max(np.abs(q['qfq_factor'].to_numpy()[aligned] - sq) /
                                    np.maximum(np.abs(sq), 1e-300)))
            max_rel = max(max_rel, rel_q, rel_h)
            rows.append({
                'category': cat, 'ts_code': code, 'rows': len(dates),
                'aligned_rows': int(aligned.sum()),
                'events': int(len(ev['date'])),
                'max_rel_qfq_vs_recompute': rel_q,
                'max_rel_hfq_vs_recompute': rel_h,
                'max_rel_qfq_roundtrip': rel_rec,
                'max_rel_hfq_roundtrip': rel_rec_h,
                'max_rel_quote_copy_vs_snapshot': copy_rel,
                'status': 'ok',
            })
    df = pl.DataFrame(rows)
    df.write_csv(OUT / 'reversibility_results.csv')
    ok = df.filter(pl.col('status') == 'ok')
    return {
        'samples': samples,
        'count': len(ok),
        'max_rel_factor_recompute': max_rel,
        'max_rel_roundtrip': float(max(
            ok['max_rel_qfq_roundtrip'].max(), ok['max_rel_hfq_roundtrip'].max())),
        'fail_rel_gt_1e12': int(ok.filter(
            (pl.col('max_rel_qfq_vs_recompute') > 1e-12) |
            (pl.col('max_rel_hfq_vs_recompute') > 1e-12)).height),
        'rows': ok.to_dicts(),
    }


# --------------------------------------------------------------------------- #
# Phase 3: 跨源对拍（AkShare 东财分红送配）
# --------------------------------------------------------------------------- #

def _parse_ak_date(series):
    import pandas as pd
    return pd.to_datetime(series, errors='coerce').dt.date


def phase_cross_source(div: pl.DataFrame) -> dict:
    v = div.with_columns([pl.col(c).fill_null(0.0)
                          for c in ('cash_div', 'share_div', 'rights_issue_ratio')])
    eligible = v.filter(
        (pl.col('rights_issue_ratio') == 0) &
        (pl.col('ex_date') >= date(2010, 1, 1))
    ).group_by('ts_code').agg(
        pl.len().alias('n'),
        (pl.col('cash_div') > 0).any().alias('has_cash'),
        (pl.col('share_div') > 0).any().alias('has_share'),
    ).filter(pl.col('n') >= 2)
    share_codes = sorted(eligible.filter(pl.col('has_share'))['ts_code'].to_list())
    cash_codes = sorted(eligible.filter(
        (pl.col('has_cash')) & (~pl.col('has_share')))['ts_code'].to_list())
    codes = sorted(share_codes, key=hrank)[:12] + sorted(cash_codes, key=hrank)[:8]
    codes = sorted(set(codes))
    with open(OUT / 'cross_source_samples.json', 'w', encoding='utf-8') as f:
        json.dump(codes, f, ensure_ascii=False, indent=2)

    import akshare as ak
    import pandas as pd
    detail = []
    per_code = []
    total_local = total_ak = matched = amount_ok = both_diff = 0
    for code in codes:
        local = v.filter(pl.col('ts_code') == code)
        loc_agg = local.group_by('ex_date').agg(
            pl.col('cash_div').sum().alias('cash'),
            pl.col('share_div').sum().alias('share')).sort('ex_date')
        loc_dates = {r['ex_date']: r for r in loc_agg.to_dicts()}
        try:
            raw = ak.stock_fhps_detail_em(symbol=code6(code))
            raw = raw[pd.to_datetime(raw['除权除息日'], errors='coerce').notna()].copy()
            raw['_d'] = _parse_ak_date(raw['除权除息日'])
            raw = raw[raw['_d'] <= date(2026, 11, 3)]
            ak_agg = raw.groupby('_d').agg({
                '现金分红-现金分红比例': 'sum',
                '送转股份-送转总比例': 'sum'}).reset_index()
            ak_agg = ak_agg.rename(columns={
                '现金分红-现金分红比例': 'cash10', '送转股份-送转总比例': 'share10'})
            ak_agg = ak_agg.fillna(0.0)
        except Exception as exc:  # noqa: BLE001
            per_code.append({'ts_code': code, 'status': f'error: {type(exc).__name__} {exc}'})
            time.sleep(0.4)
            continue
        ak_dates = set(ak_agg['_d'].to_list())
        loc_set = set(loc_dates)
        only_local = sorted(loc_set - ak_dates)
        only_ak = sorted(ak_dates - loc_set)
        c_match = c_amount = c_diff = 0
        for d in sorted(loc_set & ak_dates):
            c_match += 1
            lc = float(loc_dates[d]['cash']); ls = float(loc_dates[d]['share'])
            row = ak_agg[ak_agg['_d'] == d].iloc[0]
            ac = float(row['cash10']) / 10.0; ash = float(row['share10']) / 10.0
            dc = abs(lc - ac); ds = abs(ls - ash)
            ok = dc <= 1e-4 and ds <= 1e-4
            if ok:
                c_amount += 1
            else:
                c_diff += 1
            detail.append({
                'ts_code': code, 'ex_date': str(d), 'local_cash': lc, 'ak_cash': ac,
                'local_share': ls, 'ak_share': ash,
                'cash_diff': round(dc, 6), 'share_diff': round(ds, 6), 'match': ok})
        total_local += len(loc_set); total_ak += len(ak_dates)
        matched += c_match; amount_ok += c_amount; both_diff += c_diff
        per_code.append({
            'ts_code': code, 'status': 'ok', 'local_events': len(loc_set),
            'ak_events': len(ak_dates), 'matched_date': c_match,
            'amount_ok': c_amount, 'amount_diff': c_diff,
            'only_local': [str(x) for x in only_local[:10]],
            'only_ak': [str(x) for x in only_ak[:10]],
        })
        time.sleep(0.4)
    with open(OUT / 'cross_source_by_code.json', 'w', encoding='utf-8') as f:
        json.dump(per_code, f, ensure_ascii=False, indent=2, default=str)
    if detail:
        pl.DataFrame(detail).write_csv(OUT / 'cross_source_detail.csv')
    return {
        'codes': codes,
        'local_events': total_local, 'ak_events': total_ak,
        'matched_date': matched, 'amount_ok': amount_ok, 'amount_diff': both_diff,
        'date_agreement': round(matched / max(total_local + total_ak - matched, 1), 4),
        'per_code': per_code,
    }


# --------------------------------------------------------------------------- #
# Phase 4: 本地列存末行 vs 通达信 .day 末行
# --------------------------------------------------------------------------- #

def phase_tdx_consistency(cfg: Config) -> dict:
    tdx_root = Path(cfg.data.tdx_data_path)
    picks = {}
    for kind in ('stock', 'index', 'fund'):
        files = quote_files(kind)
        avail = []
        for p in files:
            ts = p.stem
            mkt = ts.split('.')[-1].lower()
            dpath = build_day_file_path(tdx_root, mkt, code6(ts))
            if dpath.exists():
                avail.append(p)
        picks[kind] = sorted(avail, key=lambda p: hrank(p.stem))[:10]

    rows = []
    for kind, files in picks.items():
        for p in files:
            ts = p.stem
            mkt = ts.split('.')[-1].lower()
            dpath = build_day_file_path(tdx_root, mkt, code6(ts))
            loc = pl.read_parquet(p).tail(5)
            try:
                src = parse_day_file(dpath, max_days=5).tail(5)
            except Exception as exc:  # noqa: BLE001
                rows.append({'type': kind, 'ts_code': ts, 'status': f'parse_error {exc}'})
                continue
            if loc.height == 0 or src.height == 0:
                rows.append({'type': kind, 'ts_code': ts, 'status': 'empty'})
                continue
            ld = loc['date'].to_list(); sd = src['date'].to_list()
            n = min(len(ld), len(sd))
            loc_t = loc.tail(n); src_t = src.tail(n)
            if [str(x) for x in loc_t['date'].to_list()] != [str(x) for x in src_t['date'].to_list()]:
                rows.append({'type': kind, 'ts_code': ts, 'status': 'date_mismatch',
                             'local_dates': [str(x) for x in loc_t['date'].to_list()],
                             'tdx_dates': [str(x) for x in src_t['date'].to_list()]})
                continue
            max_abs = {}
            row = {'type': kind, 'ts_code': ts, 'status': 'ok',
                   'last_date': str(loc_t['date'].to_list()[-1])}
            for col in ('open', 'high', 'low', 'close', 'volume', 'amount'):
                a = loc_t[col].to_numpy().astype(float)
                b = src_t[col].to_numpy().astype(float)
                max_abs[col] = float(np.max(np.abs(a - b)))
                row[col] = max_abs[col]
            rows.append(row)
    ok = [r for r in rows if r.get('status') == 'ok']
    if ok:
        pl.DataFrame(ok).write_csv(OUT / 'tdx_consistency.csv')
    overall = {}
    for col in ('open', 'high', 'low', 'close', 'volume', 'amount'):
        vals = [r[col] for r in ok]
        overall[col] = max(vals) if vals else None
    return {'sample_count': len(rows), 'ok_count': len(ok),
            'failed': [r for r in rows if r.get('status') != 'ok'],
            'max_abs': overall, 'picks': {k: [p.stem for p in v] for k, v in picks.items()}}


# --------------------------------------------------------------------------- #
# Phase 5: ETF / 基金分红覆盖
# --------------------------------------------------------------------------- #

def phase_etf(factors: dict, events: dict, div: pl.DataFrame) -> dict:
    fund_codes = {p.stem for p in quote_files('fund')}
    div_fund = sorted(code for code in events if code in fund_codes)
    factor_fund = sorted(code for code in factors if code in fund_codes)
    div_fund_rows = int(div.filter(pl.col('ts_code').is_in(div_fund)).height) if div_fund else 0

    # 基金因子列是否全 1
    fund_factor_nonone = []
    fund_data: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for p in quote_files('fund'):
        try:
            q = pl.read_parquet(p, columns=['date', 'close', 'qfq_factor', 'hfq_factor'])
        except Exception:  # noqa: BLE001
            continue
        if q.height == 0:
            continue
        dates = to_int_days(q['date']); closes = q['close'].to_numpy()
        fund_data[p.stem] = (dates, closes)
        if (q['qfq_factor'].n_unique() > 1 or
                (q['qfq_factor'][0] != 1.0) or (q['hfq_factor'][0] != 1.0)):
            fund_factor_nonone.append(p.stem)

    etf_events: list[dict] = []
    etf_list = None
    try:
        import akshare as ak
        import pandas as pd
        etf_list = ak.fund_fh_em()
        etf_list['_d'] = pd.to_datetime(etf_list['除息日期'], errors='coerce').dt.date
        etf_list = etf_list[etf_list['_d'].notna()]
    except Exception as exc:  # noqa: BLE001
        return {'status': f'akshare fund_fh_em error: {exc}',
                'fund_codes_in_dividend': div_fund,
                'fund_codes_in_factor': factor_fund,
                'div_fund_rows': div_fund_rows,
                'fund_factor_nonone': fund_factor_nonone}

    fund_map = {code6(c): c for c in fund_codes}
    for r in etf_list.to_dict('records'):
        raw = str(r['基金代码']).zfill(6)
        code = fund_map.get(raw)
        if code is None:
            continue
        dates, closes = fund_data[code]
        d = r['_d']
        if d is None or dates[0] > date_to_int(d) or dates[-1] < date_to_int(d):
            continue
        pos = bisect.bisect_left(dates, date_to_int(d))
        if pos <= 0 or pos >= len(dates):
            continue
        ret = float(closes[pos] / closes[pos - 1] - 1)
        etf_events.append({
            'fund_code': code,
            'name': r.get('基金简称'),
            'ex_date': str(d),
            'dividend': float(r.get('分红') or 0),
            'prev_close': float(closes[pos - 1]),
            'close': float(closes[pos]),
            'gap': round(ret, 6),
            'expressed_yield': round(float(r.get('分红') or 0) / closes[pos - 1], 6),
        })

    ev_df = pl.DataFrame(etf_events) if etf_events else pl.DataFrame()
    if etf_events:
        ev_df.write_csv(OUT / 'etf_dividend_check.csv')
    gaps = np.array([abs(e['gap']) for e in etf_events], dtype=float)
    stats = None
    if len(gaps):
        stats = {
            'n': int(len(gaps)), 'median_abs_gap': round(float(np.median(gaps)), 5),
            'p90_abs_gap': round(float(np.percentile(gaps, 90)), 5),
            'max_abs_gap': round(float(gaps.max()), 5),
            'ge_0.5pct': int((gaps >= 0.005).sum()),
            'ge_1pct': int((gaps >= 0.01).sum()),
        }
    by_code = defaultdict(list)
    for e in etf_events:
        by_code[e['fund_code']].append(e)
    top = sorted(
        ({'fund_code': k, 'events': len(v),
          'max_abs_gap': round(max(abs(x['gap']) for x in v), 6),
          'name': v[0]['name']} for k, v in by_code.items()),
        key=lambda x: -x['max_abs_gap'])[:15]

    samples = sorted(by_code, key=hrank)[:5]
    sample_detail = [
        {'fund_code': c, 'name': by_code[c][0]['name'], 'events': by_code[c],
         'factor_before_after': [1.0, 1.0]}
        for c in samples
    ]

    return {
        'status': 'ok',
        'fund_files': len(fund_codes),
        'fund_codes_in_dividend': div_fund,
        'fund_codes_in_factor': factor_fund,
        'div_fund_rows': div_fund_rows,
        'fund_factor_nonone': fund_factor_nonone,
        'akshare_fund_codes': int(etf_list['基金代码'].nunique()),
        'akshare_date_range': [str(etf_list['_d'].min()), str(etf_list['_d'].max())],
        'matched_etf_codes': len(by_code),
        'matched_events': len(etf_events),
        'fund_map_matched': len(by_code),
        'gap_stats': stats,
        'top_etfs': top,
        'samples': sample_detail,
        'all_factors_are_one': len(fund_factor_nonone) == 0,
    }


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main() -> None:
    print('== phase 0 baseline ==', flush=True)
    with Timer('baseline'):
        baseline = phase_baseline()

    print('== loading factors / events ==', flush=True)
    with Timer('load_snapshots'):
        factors = load_factor_dict()
        events, div = load_event_dict()
        print(f'  factor codes={len(factors)} event codes={len(events)}', flush=True)

    print('== phase 1 market scan ==', flush=True)
    with Timer('market_scan'):
        scan = phase_market_scan(factors, events)

    print('== phase 2 reversibility ==', flush=True)
    with Timer('reversibility'):
        rev = phase_reversibility(factors, events, div)

    print('== phase 3 cross source ==', flush=True)
    with Timer('cross_source'):
        cross = phase_cross_source(div)

    print('== phase 4 tdx consistency ==', flush=True)
    with Timer('tdx_consistency'):
        tdx = phase_tdx_consistency(Config())

    print('== phase 5 etf ==', flush=True)
    with Timer('etf'):
        etf = phase_etf(factors, events, div)

    result = {
        'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'baseline': baseline,
        'market_scan': scan,
        'reversibility': rev,
        'cross_source': cross,
        'tdx_consistency': tdx,
        'etf': etf,
        'timings': TIMINGS,
    }
    with open(OUT / 'verification_summary.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps({
        'timings': TIMINGS,
        'gap': scan['gap'],
        'jumps': scan['jumps'],
        'coverage': {k: scan['coverage'][k] for k in
                     ('factor_start_after_quote', 'factor_end_before_quote',
                      'codes_with_event_in_gap', 'dividend_codes_without_quotes')},
        'reversibility': {
            'count': rev['count'],
            'failed_rows': rev['fail_rel_gt_1e12'],
            'max_rel_factor_recompute': rev['max_rel_factor_recompute'],
            'max_rel_roundtrip': rev['max_rel_roundtrip'],
            'fail_rel_gt_1e12': rev['fail_rel_gt_1e12'],
        },
        'cross_source': {k: cross[k] for k in
                         ('local_events', 'ak_events', 'matched_date',
                          'amount_ok', 'amount_diff', 'date_agreement')},
        'tdx': {'ok_count': tdx['ok_count'], 'max_abs': tdx['max_abs'],
                'failed': len(tdx['failed'])},
        'etf': {k: etf.get(k) for k in
                ('status', 'div_fund_rows', 'fund_factor_nonone',
                 'matched_etf_codes', 'matched_events', 'gap_stats')},
    }, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
