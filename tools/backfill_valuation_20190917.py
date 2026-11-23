"""回补 2019-09-17 截断标的的日频估值行（close/total_mv/circ_mv）。

背景：``valuation_daily`` 在 2019-09-18 之后对一批已退市标的为 0 行，原因是
估值采集域只含 ``stock_basic.status == 'L'`` 的在市清单，退市后不再采集，而
2019-11-09 的全量重写又把这些标的历史回补行覆盖掉。行情已用 baostock 补到
退市日，股本在 ``meta/factors/stock_shares.parquet`` 有逐次变更记录。

口径：
- ``total_mv = close × total_share``、``circ_mv = close × float_share``，单位
  跟随现有表（tushare daily_basic 为万元）；``free_float_share`` 是自由流通
  股本，只用于指数/自由流通口径，**不**用于 circ_mv。
- pe/pb/ps/dv_ratio 无本地可验证来源，显式写 null，``source`` 段标 ``none``。
- 写入只走 ``store.coerce_schema`` + ``store.merge_parts``（分片合并、逐字段
  取最后非空），不直接改正式表；写前备份并记 SHA256。

用法::

    .venv\\Scripts\\python.exe tools\\backfill_valuation_20190917.py --dry-run
    .venv\\Scripts\\python.exe tools\\backfill_valuation_20190917.py --apply
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.dataset_contracts import contract_for_key, validate_frame  # noqa: E402
from src.data.financials import store  # noqa: E402

STORE = ROOT / 'data' / 'local_store'
CUTOFF = dt.date(2019, 9, 18)
SOURCE = ('close=local_quotes|pe=none|pe_static=none|pb=none|ps=none|dv=none|'
          'mv=quote_x_shares|circ_mv=quote_x_shares')


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _mv_scale(val: pl.DataFrame, shares: pl.DataFrame) -> float:
    """由现存重叠样本反推市值单位：1.0=万元/万股，1e-4=万元/股。"""
    last_sh = shares.sort('effective_date').group_by('ts_code').agg(
        pl.col('total_share').drop_nulls().last().alias('total_share'))
    last_day = val['trade_date'].max()
    j = (val.filter(pl.col('trade_date') == last_day)
         .select('ts_code', 'close', 'total_mv')
         .join(last_sh, on='ts_code', how='inner')
         .filter((pl.col('total_share') > 0) & (pl.col('total_mv') > 0)
                 & (pl.col('close') > 0)))
    ratio = (j['total_mv'] / (j['close'] * j['total_share'])).median()
    for scale in (1.0, 1e-4):
        if ratio is not None and abs(ratio / scale - 1) < 1e-3:
            return scale
    raise SystemExit(f'市值单位无法判定，样本比值中位数={ratio}')


def build_rows(codes: list[str], val: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    shares = pl.read_parquet(STORE / 'meta/factors/stock_shares.parquet')
    scale = _mv_scale(val, shares)
    sh_per_share = 1e4 if scale == 1.0 else 1.0
    qfiles = {p.stem: p for p in (STORE / 'quotes/stock').rglob('*.parquet')}
    frames, stat = [], {}
    for code in codes:
        path = qfiles.get(code)
        if path is None:
            stat[code] = {'quote_rows': 0, 'filled': 0, 'reason': 'no_quote_file'}
            continue
        q = (pl.read_parquet(path, columns=['date', 'close', 'volume', 'amount', 'pct_chg'])
             .rename({'date': 'trade_date'})
             .filter((pl.col('trade_date') >= CUTOFF) & pl.col('close').is_not_null()
                     & (pl.col('close') > 0))
             .sort('trade_date'))
        if q.is_empty():
            stat[code] = {'quote_rows': 0, 'filled': 0, 'reason': 'no_quote_after_cutoff'}
            continue
        sh = (shares.filter(pl.col('ts_code') == code)
              .sort('effective_date')
              .with_columns([pl.col(c).forward_fill().alias(c)
                             for c in ('total_share', 'float_share')])
              .select(pl.col('effective_date').alias('trade_date'),
                      'total_share', 'float_share'))
        q = q.join_asof(sh, on='trade_date', strategy='backward').sort('trade_date')
        q = q.with_columns([
            pl.lit(code).alias('ts_code'),
            (pl.col('close') * pl.col('total_share') * scale).alias('total_mv'),
            (pl.col('close') * pl.col('float_share') * scale).alias('circ_mv'),
            (pl.col('volume') * 100.0
             / (pl.col('float_share') * sh_per_share) * 100.0).alias('turn'),
            (pl.col('pct_chg').abs() / pl.col('amount')).alias('illiq'),
        ])
        frames.append(q.select('ts_code', 'trade_date', 'close', 'total_mv', 'circ_mv',
                               'volume', 'float_share', 'turn', 'illiq'))
        stat[code] = {'quote_rows': q.height, 'filled': q.height,
                      'circ_mv_null': q['circ_mv'].null_count()}
    detail = pl.concat(frames, how='diagonal_relaxed') if frames else pl.DataFrame()
    # 组装契约字段（顺序与 store.SCHEMAS['valuation'] 一致）
    out = detail.select(
        'ts_code', 'trade_date', 'close',
        pl.lit(None, dtype=pl.Float64).alias('pe'),
        pl.lit(None, dtype=pl.Float64).alias('pe_static'),
        pl.lit(None, dtype=pl.Float64).alias('pb'),
        pl.lit(None, dtype=pl.Float64).alias('ps'),
        pl.lit(None, dtype=pl.Float64).alias('dv_ratio'),
        'total_mv', 'circ_mv',
        pl.lit(SOURCE).alias('source'),
        pl.lit(dt.datetime.now()).alias('fetched_at'),
    )
    return store.coerce_schema(out, 'valuation'), {'scale': scale, 'rows': stat,
                                                   'detail': detail}


def sample_summary(detail: pl.DataFrame) -> dict:
    if detail.is_empty():
        return {}
    sample = {}
    for code in detail['ts_code'].unique().sort().to_list()[:5]:
        x = detail.filter(pl.col('ts_code') == code)
        sample[code] = {
            'rows': x.height,
            'total_mv_median': round(x['total_mv'].median() or 0, 2),
            'circ_mv_median': round(x['circ_mv'].median() or 0, 2),
            'turn_median_pct': round(x['turn'].median() or 0, 4),
            'illiq_median': float(f'{x["illiq"].median() or 0:.6g}'),
        }
    return {
        'sample': sample,
        'coverage_before': {'total_mv': 0.0, 'turn': 0.0, 'illiq': 0.0},
        'coverage_after': {
            'total_mv': round(detail['total_mv'].is_not_null().mean(), 4),
            'turn': round(detail['turn'].is_not_null().mean(), 4),
            'illiq': round(detail['illiq'].is_not_null().mean(), 4),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    val_path = STORE / 'meta/financials/valuation_daily.parquet'
    val = pl.read_parquet(val_path, columns=['ts_code', 'trade_date', 'close', 'total_mv'])
    last = val.group_by('ts_code').agg(pl.col('trade_date').max())
    gap_codes = sorted(last.filter(pl.col('trade_date') < CUTOFF)['ts_code'].to_list())
    print(json.dumps({'gap_tickers': len(gap_codes), 'cutoff': str(CUTOFF)}))

    part, meta = build_rows(gap_codes, val)
    contract = contract_for_key('valuation_daily')
    validate_frame(contract, part)  # 契约校验：字段/类型
    detail = meta['detail']
    summary = {
        'gap_tickers': len(gap_codes),
        'gap_tickers_with_quotes': int((pl.Series([v['filled'] for v in meta['rows'].values()]) > 0).sum()),
        'gap_tickers_no_quote': [c for c, v in meta['rows'].items() if v['filled'] == 0],
        'new_rows': part.height,
        'mv_scale': meta['scale'],
        **sample_summary(detail),
    }
    out_dir = ROOT / 'work' / 'valuation_backfill_20261122'
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / 'summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    detail.write_parquet(out_dir / 'sample_factors.parquet')
    if args.dry_run or not args.apply:
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    backup = ROOT / 'logs' / 'valuation_backfill_20261122'
    backup.mkdir(parents=True, exist_ok=True)
    before = _sha256(val_path)
    shutil.copy2(val_path, backup / 'valuation_daily.parquet')
    (backup / 'sha256_before.txt').write_text(before, encoding='utf-8')

    pdir = store.parts_dir('valuation', STORE)
    pdir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime('%Y%m%dT%H%M%S')
    part_path = pdir / f'backfill_20190917_{stamp}.parquet'
    part.write_parquet(part_path)
    rows = store.merge_parts('valuation', STORE)

    after = pl.read_parquet(val_path, columns=['ts_code', 'trade_date'])
    last2 = after.group_by('ts_code').agg(pl.col('trade_date').max())
    still = last2.filter(pl.col('trade_date') < CUTOFF)['ts_code'].to_list()
    dup = after.is_duplicated().sum()
    summary.update({'merged_rows': rows, 'sha256_before': before,
                    'sha256_after': _sha256(val_path),
                    'gap_tickers_after': len(still),
                    'residual_no_quote': [c for c in still if c in meta['rows']],
                    'duplicate_keys': int(dup)})
    (out_dir / 'summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    (backup / 'sha256_after.txt').write_text(summary['sha256_after'], encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
