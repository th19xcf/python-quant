#!/usr/bin/env python3

r"""按契约回补退市股缺失的个股日线分片（tushare daily + adj_factor）。

适用对象：``stock_basic.status='D'`` 但在 ``quotes/stock/{market}/`` 下没有分片的代码。
写入一律走 ``write_dataset``（契约校验 + 原子落盘 + ledger 登记），不裸写。
复权口径与 2026-11-20 退市补采批次一致：``hfq_factor = close_hfq/close``，
``qfq_factor = hfq_factor / hfq_factor[-1]``，``pre_close = close.shift(1) * qfq.shift(1)/qfq``。

用法::

    python tools/backfill_delisted_shards.py --max-codes 3      # 试采
    python tools/backfill_delisted_shards.py --commit           # 全量回补
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.dataset_contracts import (  # noqa: E402
    contract_for_key,
    resolve_files,
    write_dataset,
)

DEFAULT_ROOT = ROOT / 'data' / 'local_store'
FORMAL_COLS = ['date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
               'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor']


def _pro() -> Any:
    import tushare as ts  # noqa: PLC0415

    token = (ROOT / '.secrets' / 'tushare_token.txt').read_text(encoding='utf-8').strip()
    if not token:
        raise SystemExit('tushare token 为空')
    return ts.pro_api(token)


def _missing_codes(root: Path) -> pl.DataFrame:
    basic = pl.read_parquet(str(resolve_files(contract_for_key('stock_basic'), root)[0]))
    have = {p.name[:-len('.parquet')] for p in resolve_files(contract_for_key('quotes_stock'), root)}
    return (basic.filter(pl.col('status') == 'D')
            .filter(~pl.col('ts_code').is_in(list(have)))
            .with_columns(pl.col('delist_date').cast(pl.Utf8).str.slice(0, 10))
            .sort('delist_date'))


def _migration_map(root: Path) -> dict[str, str]:
    path = root / 'meta' / 'universe' / 'code_migration.parquet'
    if not path.is_file():
        return {}
    frame = pl.read_parquet(path)
    cols = set(frame.columns)
    old = 'old_code' if 'old_code' in cols else 'from_code' if 'from_code' in cols else None
    new = 'new_code' if 'new_code' in cols else 'to_code' if 'to_code' in cols else None
    if old is None or new is None:
        return {}
    return dict(zip(frame[old].to_list(), frame[new].to_list()))


def _fetch(pro: Any, code: str, delist: str, sleep: float) -> pl.DataFrame:
    end = delist.replace('-', '') if delist else '20261231'
    daily = pro.daily(ts_code=code, start_date='19900101', end_date=end)
    time.sleep(sleep)
    adj = pro.adj_factor(ts_code=code, start_date='19900101', end_date=end)
    time.sleep(sleep)
    if daily is None or daily.empty:
        raise ValueError('daily 返回空')
    if adj is None or adj.empty:
        raise ValueError('adj_factor 返回空')
    d = pl.DataFrame(daily).with_columns(
        (pl.col('trade_date').str.slice(0, 4) + '-' + pl.col('trade_date').str.slice(4, 2)
         + '-' + pl.col('trade_date').str.slice(6, 2)).alias('date'))
    a = pl.DataFrame(adj).select(
        (pl.col('trade_date').str.slice(0, 4) + '-' + pl.col('trade_date').str.slice(4, 2)
         + '-' + pl.col('trade_date').str.slice(6, 2)).alias('date'),
        pl.col('adj_factor').cast(pl.Float64))
    frame = d.join(a, on='date', how='inner')
    if frame.height != d.height:
        raise ValueError(f'日线与复权因子日期不齐 {d.height} vs {frame.height}')
    return frame.sort('date').with_columns([
        pl.lit(code).alias('ts_code'),
        pl.col('vol').cast(pl.Float64).alias('volume'),
        (pl.col('amount').cast(pl.Float64) * 1000.0).alias('amount'),
        (pl.col('open') * pl.col('adj_factor')).alias('open_hfq'),
        (pl.col('high') * pl.col('adj_factor')).alias('high_hfq'),
        (pl.col('low') * pl.col('adj_factor')).alias('low_hfq'),
        (pl.col('close') * pl.col('adj_factor')).alias('close_hfq'),
    ])


def _derive(src: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    s = src.with_columns(pl.col('date').str.strptime(pl.Date, '%Y-%m-%d')).sort('date')
    hfq = pl.col('close_hfq') / pl.col('close')
    hfq_last = float(s['close_hfq'][-1] / s['close'][-1])
    qfq = hfq / hfq_last
    pre_close = pl.col('close').shift(1) * (qfq.shift(1) / qfq)
    frame = s.with_columns([
        hfq.alias('hfq_factor'), qfq.alias('qfq_factor'), pre_close.alias('pre_close'),
    ]).with_columns([
        (pl.col('close') - pl.col('pre_close')).alias('change'),
        pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
        .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
        .otherwise(None).alias('pct_chg'),
    ]).select(FORMAL_COLS).sort('date')
    recon = (frame['close'] * frame['hfq_factor'] - s['close_hfq']).abs() / s['close_hfq']
    stats = {
        'rows': frame.height,
        'start': str(frame['date'][0]),
        'end': str(frame['date'][-1]),
        'duplicate_dates': frame.height - frame['date'].n_unique(),
        'ohlcv_null': int(sum(frame[c].is_null().sum()
                              for c in ('open', 'high', 'low', 'close', 'volume'))),
        # 个别 1990s 退市股源端 amount 缺 1 行，留空不猜；只计数不阻断
        'amount_null': int(frame['amount'].is_null().sum()),
        'hfq_recon_max_rel_err': float(recon.max()),
        'pre_close_null': int(frame['pre_close'].is_null().sum()),
    }
    return frame, stats


def _update_quotes_manifest(root: Path, added: dict[str, int]) -> dict[str, Any]:
    path = root / 'quotes' / 'manifest.json'
    mf = json.loads(path.read_text(encoding='utf-8'))
    before = {k: mf.get(k) for k in ('files', 'rows', 'bytes')}
    mf['files'] = int(mf['files']) + len(added)
    mf['rows'] = int(mf['rows']) + sum(added.values())
    mf['bytes'] = int(mf['bytes']) + sum(
        (root / 'quotes' / 'stock' / c.rsplit('.', 1)[1].lower() / f'{c}.parquet').stat().st_size
        for c in added)
    if isinstance(mf.get('counting'), dict):
        for key in ('files', 'rows', 'bytes'):
            mf['counting'][key] = mf[key]
    stock = (mf.get('universe') or {}).get('stock')
    if isinstance(stock, dict):
        for key in ('files', 'rows', 'bytes'):
            if isinstance(stock.get(key), (int, float)):
                stock[key] = stock[key] + (len(added) if key == 'files'
                                            else sum(added.values()) if key == 'rows'
                                            else mf['bytes'] - int(before['bytes']))
    mf['generated_at'] = datetime.now().isoformat(timespec='seconds')
    path.write_text(json.dumps(mf, ensure_ascii=False, indent=2), encoding='utf-8')
    return {'files': mf['files'], 'rows': mf['rows'], 'bytes': mf['bytes']}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='回补退市股缺失日线分片')
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--max-codes', type=int, default=None)
    parser.add_argument('--commit', action='store_true', help='落生产列存；缺省只试采')
    parser.add_argument('--sleep', type=float, default=0.15)
    # 独立写入放行（check_write_context 直接读 sys.argv / 环境变量）
    parser.add_argument('--allow-standalone', action='store_true')
    parser.add_argument('--standalone-reason', default=None)
    parser.add_argument('--output', type=Path,
                        default=ROOT / 'logs' / 'delisted_shards_20261122' / 'summary.json')
    args = parser.parse_args(argv)

    targets = _missing_codes(args.root)
    if args.max_codes:
        targets = targets.head(args.max_codes)
    migration = _migration_map(args.root)
    pro = _pro()
    done: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for rec in targets.iter_rows(named=True):
        code, delist, name = rec['ts_code'], rec['delist_date'], rec.get('name')
        if code in migration:
            rows.append({'ts_code': code, 'name': name, 'delist_date': delist,
                         'status': 'skipped', 'reason': f'code_migration → {migration[code]}'})
            continue
        try:
            src = _fetch(pro, code, delist, args.sleep)
            frame, stats = _derive(src)
            if stats['duplicate_dates'] or stats['ohlcv_null']:
                raise ValueError(f'质量不符 dup={stats["duplicate_dates"]} null={stats["ohlcv_null"]}')
            if delist and stats['end'] > delist:
                raise ValueError(f'末行 {stats["end"]} 越过退市日 {delist}')
            status = 'fetched'
            if args.commit:
                market = code.rsplit('.', 1)[1].lower()
                write_dataset('quotes_stock', frame,
                              rel_path=f'quotes/stock/{market}/{code}.parquet',
                              root=args.root, sort_by='date',
                              writer='tools/backfill_delisted_shards.py')
                done[code] = frame.height
                status = 'committed'
            rows.append({'ts_code': code, 'name': name, 'delist_date': delist,
                         'status': status, **stats})
        # 兜底理由：单只失败不能中断整批，逐只留证后继续
        except Exception as exc:  # noqa: BLE001
            rows.append({'ts_code': code, 'name': name, 'delist_date': delist,
                         'status': 'unreachable', 'reason': f'{type(exc).__name__}: {exc}'})
        print(f"[{rows[-1]['status']}] {code} {rows[-1].get('rows', '')}", flush=True)

    manifest = _update_quotes_manifest(args.root, done) if done else None
    if done:
        for code in done:
            market = code.rsplit('.', 1)[1].lower()
            target = args.root / 'quotes' / 'stock' / market / f'{code}.parquet'
            for item in rows:
                if item['ts_code'] == code:
                    item['sha256'] = _sha256(target)
    summary = {
        'run_at': datetime.now().isoformat(timespec='seconds'),
        'targets': targets.height, 'committed': len(done),
        'unreachable': sum(1 for r in rows if r['status'] == 'unreachable'),
        'skipped': sum(1 for r in rows if r['status'] == 'skipped'),
        'rows_added': sum(done.values()), 'manifest': manifest, 'detail': rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in summary.items() if k != 'detail'}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
