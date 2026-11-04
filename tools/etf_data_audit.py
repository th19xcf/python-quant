#!/usr/bin/env python3
"""ETF/LOF/封闭式基金本地数据可行性审计（只读）。

用途：回答「针对 ETF 有什么策略、数据是否支持」，所有数字直接读取
``data/local_store`` 与 MySQL ``fund_basic`` / ``fund_daily`` / ``stock_minute``。

运行::

    .venv\\Scripts\\python.exe tools/etf_data_audit.py

产物（均在 ``reports/etf_audit/``，该目录已在 .gitignore 中忽略）::

    fund_universe.csv     每只基金的覆盖度、字段质量、流动性、占位标记
    audit_summary.json    汇总数字（供报告引用）

不改生产代码，不写数据库，不提交 git。
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
STORE = PROJECT_ROOT / 'data' / 'local_store'
QUOTES = STORE / 'quotes' / 'fund'
FACTORS = STORE / 'factors'
ANOMALIES = STORE / 'meta' / 'anomalies'
OUT_DIR = PROJECT_ROOT / 'reports' / 'etf_audit'
OUT_DIR.mkdir(parents=True, exist_ok=True)

ETF_PREFIXES = ('159', '51', '52', '56', '58', '520', '530', '561', '562', '563')
# 仅用于粗分类（精确分类以 fund_basic.fund_type 为准）
CLOSED_PREFIXES = ('184', '500', '501', '502', '505', '506', '507', '508')
LOF_PREFIXES = ('160', '161', '162', '163', '164', '165', '166', '167', '168', '169',
                '150', '151', '152', '153', '154', '155', '156', '157', '158', '18')


def prefix_of(ts_code: str) -> str:
    return ts_code.split('.')[0][:3]


def rough_kind(ts_code: str) -> str:
    p = prefix_of(ts_code)
    if p in ETF_PREFIXES:
        return 'ETF'
    if p in CLOSED_PREFIXES:
        return '封闭式/REITs'
    return 'LOF/其他'


def load_db():
    from sqlalchemy import create_engine, text

    from src.utils.config import get_config

    d = get_config().database
    url = (f'mysql+pymysql://{d.username}:{d.password}@{d.host}:{d.port}/'
           f'{d.database}?charset={d.charset}')
    engine = create_engine(url, connect_args={'connect_timeout': 10})
    with engine.connect() as s:
        basic = pl.DataFrame(
            [dict(r._mapping) for r in
             s.execute(text('select ts_code, symbol, name, fund_type, market, '
                            'list_date, delist_date, status from fund_basic')).all()])
        minute_n = int(s.execute(text('select count(*) from stock_minute')).scalar() or 0)
        fund_daily_n = int(s.execute(text('select count(*) from fund_daily')).scalar() or 0)
        fund_daily_cols = [r[0] for r in s.execute(text('show columns from fund_daily'))]
    return basic, {
        'stock_minute_rows': minute_n,
        'fund_daily_rows': fund_daily_n,
        'fund_daily_columns': fund_daily_cols,
    }


def meta_funds() -> pl.DataFrame:
    df = pl.read_parquet(STORE / 'meta' / 'daily_index.parquet')
    return df.filter(pl.col('data_type') == 'fund')


def scan_fund_files(meta: pl.DataFrame, calendar: list[date]) -> tuple[pl.DataFrame, dict]:
    """逐文件读取 2615 只基金：覆盖、字段完整度、占位检测、流动性。"""
    rows = []
    agg = {
        'rows': 0, 'nulls': dict.fromkeys([
            'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount',
            'pct_chg', 'change', 'qfq_factor', 'hfq_factor'], 0),
        'zero_amount': 0, 'zero_volume': 0, 'nonpositive_close': 0,
        'bad_ohlc': 0, 'dup_date': 0,
        'all_one': 0, 'tail_one': 0, 'one_rows': 0,
        'amount_sum': 0.0, 'volume_sum': 0.0,
    }
    win_start = calendar[0] if calendar else date(2025, 1, 1)
    for r in meta.iter_rows(named=True):
        code = r['ts_code']
        path = QUOTES / r['market'] / f'{code}.parquet'
        if not path.exists():
            rows.append({'ts_code': code, 'market': r['market'], 'exists': False})
            continue
        df = pl.read_parquet(path)
        n = df.height
        if n == 0:
            rows.append({'ts_code': code, 'market': r['market'], 'exists': True,
                         'rows': 0})
            continue
        agg['rows'] += n
        for c in agg['nulls']:
            if c in df.columns:
                agg['nulls'][c] += int(df[c].null_count())
        close = df['close'].to_numpy()
        amount = df['amount'].to_numpy()
        volume = df['volume'].to_numpy()
        agg['zero_amount'] += int(np.sum(amount == 0))
        agg['zero_volume'] += int(np.sum(volume == 0))
        agg['nonpositive_close'] += int(np.sum(close <= 0))
        agg['bad_ohlc'] += int(np.sum(
            (df['high'].to_numpy() < df['low'].to_numpy()) |
            (df['high'].to_numpy() < df['open'].to_numpy()) |
            (df['low'].to_numpy() > df['open'].to_numpy())))
        agg['dup_date'] += int(n - df['date'].n_unique())
        ones = close == 1.0
        agg['one_rows'] += int(ones.sum())
        all_one = bool(ones.all())
        if all_one:
            agg['all_one'] += 1
        # 尾部连续 1.0
        trailing = 0
        for v in close[::-1]:
            if v == 1.0:
                trailing += 1
            else:
                break
        if not all_one and trailing >= 20:
            agg['tail_one'] += 1
        # 近 250 日流动性
        dfw = df.filter(pl.col('date') >= win_start) if 'date' in df.columns else df
        days = dfw.height
        avg_amount = float(dfw['amount'].mean()) if days else float('nan')
        avg_volume = float(dfw['volume'].mean()) if days else float('nan')
        last_date = str(df['date'][-1])
        rows.append({
            'ts_code': code, 'market': r['market'], 'exists': True,
            'rows': n, 'start': str(df['date'][0]), 'end': last_date,
            'all_one': all_one, 'one_rows': int(ones.sum()), 'tail_one': trailing,
            'days_250': days, 'avg_amount_250': avg_amount,
            'avg_volume_250': avg_volume,
            'last_close': float(close[-1]),
            'last_amount': float(amount[-1]),
            'last_volume': float(volume[-1]),
        })
        agg['amount_sum'] += float(np.nansum(amount))
        agg['volume_sum'] += float(np.nansum(volume))
    return pl.DataFrame(rows), agg


def main() -> None:
    meta = meta_funds()
    basic, db_info = load_db()
    names = json.loads((STORE / 'meta' / 'names.json').read_text(encoding='utf-8'))

    # 交易日历：用 510300.SH 最近 260 行，窗口取最后 250 个交易日
    cal_df = pl.read_parquet(QUOTES / 'sh' / '510300.SH.parquet').select('date')
    calendar = list(cal_df['date'].to_list()[-250:])
    win_start = calendar[0]
    print(f'[audit] 流动性窗口: {win_start} ~ {calendar[-1]} ({len(calendar)} 个交易日)')

    universe, agg = scan_fund_files(meta, calendar)

    # ---- 1. 标的范围 --------------------------------------------------
    n_fund_local = meta.height
    by_market = meta['market'].value_counts().sort('market')
    local_codes = set(meta['ts_code'].to_list())
    db_codes = set(basic['ts_code'].to_list())
    names_cov = sum(1 for c in local_codes if names.get(c))
    kind_counts = pl.DataFrame({
        'ts_code': list(local_codes),
    }).with_columns(
        pl.col('ts_code').map_elements(rough_kind, return_dtype=pl.Utf8).alias('kind')
    )['kind'].value_counts().sort('kind')
    db_type = basic['fund_type'].value_counts().sort('fund_type') if 'fund_type' in basic.columns else pl.DataFrame()

    # 占位/无行情清单
    ph = pl.read_csv(ANOMALIES / 'placeholder_quote_codes.csv')
    nq = pl.read_csv(ANOMALIES / 'no_local_quote_codes.csv')
    stale = pl.read_csv(ANOMALIES / 'source_stale_codes.csv')
    ph_codes = set(ph['ts_code'].to_list())
    ph_kind = pl.DataFrame({'ts_code': sorted(ph_codes)}).with_columns(
        pl.col('ts_code').map_elements(rough_kind, return_dtype=pl.Utf8).alias('kind')
    )['kind'].value_counts().sort('kind')
    ph_db_type = basic.filter(pl.col('ts_code').is_in(sorted(ph_codes)))
    nq_types = 'stock' if all(c.split('.')[0][0] in '036' for c in nq['ts_code'].to_list()) else 'mixed'
    stale_by_type = stale['data_type'].value_counts().sort('data_type')

    # ---- 2. 行情覆盖 --------------------------------------------------
    starts = universe['start'].str.to_date('%Y-%m-%d', strict=False)
    buckets = {
        '<2010': int((starts < date(2010, 1, 1)).sum()),
        '2010-2014': int(((starts >= date(2010, 1, 1)) & (starts < date(2015, 1, 1))).sum()),
        '2015-2018': int(((starts >= date(2015, 1, 1)) & (starts < date(2019, 1, 1))).sum()),
        '2019-2022': int(((starts >= date(2019, 1, 1)) & (starts < date(2023, 1, 1))).sum()),
        '2023+': int((starts >= date(2023, 1, 1)).sum()),
    }
    latest = max(calendar)
    end_dates = universe['end'].str.to_date('%Y-%m-%d', strict=False)
    at_latest = int((end_dates == latest).sum())
    stale_n = int((end_dates < latest).sum())
    rows_per = universe['rows']

    # ---- 3. 字段完整度 ------------------------------------------------
    total_rows = agg['rows']
    null_rates = {c: (v / total_rows if total_rows else 0.0)
                  for c, v in agg['nulls'].items()}
    # 复权因子快照
    faf = pl.read_parquet(FACTORS / 'fund_adj_factor.parquet')
    faf_codes = faf['ts_code'].n_unique()
    faf_rows = faf.height
    faf_non1_qfq = int((faf['qfq_factor'] != 1.0).sum())
    faf_non1_hfq = int((faf['hfq_factor'] != 1.0).sum())
    faf_range = (str(faf['trade_date'].min()), str(faf['trade_date'].max()))
    div = pl.read_parquet(FACTORS / 'fund_dividend.parquet')
    sc = pl.read_parquet(FACTORS / 'fund_share_change.parquet')
    turn = pl.read_parquet(FACTORS / 'stock_turnover.parquet')
    turn_fund = sorted(set(turn['ts_code'].to_list()) & local_codes)

    # ---- 4. 与 TDX .day 抽样比对 --------------------------------------
    from src.data.tdx_day_parser import parse_day_file

    tdx_root = Path('d:/zxzq/vipdoc')
    samples = ['510300.SH', '510050.SH', '510500.SH', '510880.SH',
               '159915.SZ', '512880.SH', '518880.SH', '511010.SH']
    tdx_cmp = []
    for code in samples:
        p = QUOTES / ('sh' if code.endswith('SH') else 'sz') / f'{code}.parquet'
        day = tdx_root / ('sh' if code.endswith('SH') else 'sz') / 'lday' / \
            f'{("sh" if code.endswith("SH") else "sz")}{code.split(".")[0]}.day'
        if not p.exists() or not day.exists():
            continue
        loc = pl.read_parquet(p).sort('date').with_columns(pl.col('date').cast(pl.Utf8))
        src = parse_day_file(day).sort('date').with_columns(pl.col('date').cast(pl.Utf8))
        j = loc.select(['date', 'open', 'high', 'low', 'close', 'volume', 'amount']).join(
            src, on='date', how='inner', suffix='_tdx')
        d = {
            'ts_code': code,
            'common_bars': j.height,
            'max_close_diff': float((j['close'] - j['close_tdx']).abs().max()),
            'max_amount_diff': float((j['amount'] - j['amount_tdx']).abs().max()),
            'max_volume_diff': float((j['volume'] - j['volume_tdx']).abs().max()),
            'implied_amount': float((j['amount'] / (j['volume'] * 100 * j['close'])).median()),
            'last_date_local': str(loc['date'][-1]),
            'last_date_tdx': str(src['date'][-1]),
        }
        tdx_cmp.append(d)
    # 分钟线
    minute_files = {m: len(list((tdx_root / m / 'minline').glob('*')))
                    for m in ('sh', 'sz')}
    fz_files = {m: len(list((tdx_root / m / 'fzline').glob('*')))
                for m in ('sh', 'sz')}

    # ---- 5. 流动性 ----------------------------------------------------
    liq = universe.filter(pl.col('days_250') >= 100)
    amt = liq['avg_amount_250'].to_numpy()
    quant = np.nanpercentile(amt, [10, 50, 90]) if len(amt) else [np.nan] * 3
    over50 = int(np.nansum(amt > 5e7))
    over100 = int(np.nansum(amt > 1e8))
    # 按基金类型拆分
    liq = liq.with_columns(
        pl.col('ts_code').map_elements(rough_kind, return_dtype=pl.Utf8).alias('kind'))
    liq_kind = liq.group_by('kind').agg([
        pl.len().alias('codes'),
        pl.col('avg_amount_250').median().alias('median_amount'),
        (pl.col('avg_amount_250') > 5e7).sum().alias('gt50m'),
        (pl.col('avg_amount_250') > 1e8).sum().alias('gt100m'),
    ]).sort('codes', descending=True)

    # ---- 6. 汇总 ------------------------------------------------------
    summary = {
        'as_of': str(latest),
        'universe': {
            'fund_codes_local': n_fund_local,
            'by_market': by_market.to_dicts(),
            'rough_kind': kind_counts.to_dicts(),
            'fund_basic_rows': basic.height,
            'fund_basic_by_type': db_type.to_dicts(),
            'fund_basic_placeholder_names': int(
                basic.filter(pl.col('name').str.starts_with('股票')).height),
            'local_not_in_basic': sorted(local_codes - db_codes),
            'basic_not_in_local': sorted(db_codes - local_codes),
            'names_json_coverage': names_cov,
            'placeholder_codes': ph.height,
            'placeholder_kind': ph_kind.to_dicts(),
            'placeholder_in_basic': ph_db_type.height,
            'placeholder_basic_type': (
                ph_db_type['fund_type'].value_counts().to_dicts()
                if 'fund_type' in ph_db_type.columns else []),
            'no_quote_rows': nq.height,
            'no_quote_guess_type': nq_types,
            'stale_rows': stale.height,
            'stale_by_type': stale_by_type.to_dicts(),
        },
        'coverage': {
            'total_rows': total_rows,
            'start_buckets': buckets,
            'codes_before_2015': buckets['<2010'] + buckets['2010-2014'],
            'share_before_2015': round((buckets['<2010'] + buckets['2010-2014']) / n_fund_local, 4),
            'start_2019_plus': buckets['2019-2022'] + buckets['2023+'],
            'share_start_2019_plus': round((buckets['2019-2022'] + buckets['2023+']) / n_fund_local, 4),
            'rows_median': float(rows_per.median()),
            'rows_p10': float(rows_per.quantile(0.1)),
            'rows_p90': float(rows_per.quantile(0.9)),
            'end_at_latest': at_latest,
            'stale_codes': stale_n,
            'stale_share': round(stale_n / n_fund_local, 4),
        },
        'fields': {
            'columns': ['date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
                        'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor'],
            'null_rates': {k: round(v, 6) for k, v in null_rates.items()},
            'zero_amount_rows': agg['zero_amount'],
            'zero_volume_rows': agg['zero_volume'],
            'nonpositive_close_rows': agg['nonpositive_close'],
            'bad_ohlc_rows': agg['bad_ohlc'],
            'dup_date_rows': agg['dup_date'],
            'all_one_codes': agg['all_one'],
            'tail_one_codes': agg['tail_one'],
            'one_rows': agg['one_rows'],
            'fund_factor_codes': faf_codes,
            'fund_factor_rows': faf_rows,
            'fund_factor_vs_quote_rows': faf_rows - total_rows,
            'fund_factor_range': faf_range,
            'qfq_non1_rows': faf_non1_qfq,
            'hfq_non1_rows': faf_non1_hfq,
            'dividend_rows': div.height,
            'dividend_codes': div['ts_code'].n_unique() if div.height else 0,
            'share_change_rows': sc.height,
            'share_change_codes': sc['ts_code'].n_unique() if sc.height else 0,
            'turnover_rows': turn.height,
            'turnover_codes': turn['ts_code'].n_unique(),
            'turnover_fund_codes': turn_fund,
        },
        'tdx_check': {
            'samples': tdx_cmp,
            'minline_files': minute_files,
            'fzline_files': fz_files,
            'mysql_stock_minute_rows': db_info['stock_minute_rows'],
            'mysql_fund_daily_rows': db_info['fund_daily_rows'],
            'mysql_fund_daily_columns': db_info['fund_daily_columns'],
        },
        'liquidity': {
            'window': [str(win_start), str(latest)],
            'codes_with_100_days': liq.height,
            'avg_amount_p10': float(quant[0]),
            'avg_amount_p50': float(quant[1]),
            'avg_amount_p90': float(quant[2]),
            'codes_gt_50m': over50,
            'codes_gt_100m': over100,
            'by_kind': liq_kind.to_dicts(),
            'turnover_note': '本地无基金份额/规模表，turnover 无法计算',
        },
    }
    (OUT_DIR / 'audit_summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    universe.write_csv(OUT_DIR / 'fund_universe.csv')
    liq.write_csv(OUT_DIR / 'fund_liquidity.csv')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
