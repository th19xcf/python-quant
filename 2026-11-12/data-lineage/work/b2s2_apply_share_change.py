# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""把 35 个通过三条准入判据的「事件表缺登记」事件补进 fund_share_change.parquet。

口径：
* share_ratio = 因子台阶 step（价格隐含，已由 C1/C2/C3 验证）；
* ex_date = 生效交易日前一交易日（分级基金定期份额折算的折算基准日语义），
  这样 ``_next_trade_date(trade_dates, ex_date)`` 正好落在观测到台阶的交易日；
* split_type = 份额折算（分级基金定期折算，折算有增有减，与 step 方向一致）；
* source = ``price_implied:tdx_day_v1``，与既有 akshare 行字段级可区分（DG-19）。

因子侧一行不动。写前快照到 ``_backup/fund_share_change_20261113/``。
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.atomic_write import write_parquet_atomic
from src.data.local_store.fund_factors import (
    FUND_SHARE_CHANGE_FILENAME,
    SHARE_CHANGE_COLUMNS,
    _next_trade_date,
)

WORK = Path(__file__).parent
ROOT = Path(r'e:\code\python\quant')
FACTORS = ROOT / 'data' / 'local_store' / 'factors'
QUOTES = ROOT / 'data' / 'local_store' / 'quotes' / 'fund'
SNAP = ROOT / '_backup' / 'fund_share_change_20261113'
SOURCE = 'price_implied:tdx_day_v1'


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest().upper()


def quotes(code: str) -> pl.DataFrame:
    ex = 'sh' if code.endswith('.SH') else 'sz'
    return pl.read_parquet(QUOTES / ex / f'{code}.parquet').select('date').sort('date')


def main() -> None:
    admit = json.loads((WORK / 'b2s2_admit42.json').read_text(encoding='utf-8'))
    admitted = [r for r in admit['rows'] if r['admit']]
    old_path = FACTORS / FUND_SHARE_CHANGE_FILENAME
    old = pl.read_parquet(old_path)
    old_sha, old_rows = sha(old_path), old.height

    qcache: dict[str, pl.DataFrame] = {}
    new_rows = []
    now = datetime.now().replace(microsecond=0)
    for r in sorted(admitted, key=lambda x: (x['ts_code'], x['date'])):
        code, d = r['ts_code'], date.fromisoformat(r['date'])
        if code not in qcache:
            qcache[code] = quotes(code)
        dates = [x for (x,) in qcache[code].select('date').iter_rows()]
        pos = dates.index(d) if d in dates else None
        assert pos is not None and pos > 0, (code, d, 'no previous trade day')
        new_rows.append({
            'ts_code': code,
            'ex_date': dates[pos - 1],
            'split_type': '份额折算',
            'share_ratio': float(r['step']),
            'source': SOURCE,
            'fetched_at': now,
        })

    add = pl.DataFrame(new_rows).select(
        'ts_code',
        pl.col('ex_date').cast(old.schema['ex_date']),
        'split_type',
        pl.col('share_ratio').cast(old.schema['share_ratio']),
        'source',
        pl.col('fetched_at').cast(old.schema['fetched_at']),
    ).select(list(SHARE_CHANGE_COLUMNS))

    keys = ['ts_code', 'ex_date', 'split_type']
    keys_old = set(zip(old['ts_code'].to_list(), old['ex_date'].to_list()))
    dup = [k for k in zip(add['ts_code'].to_list(), add['ex_date'].to_list()) if k in keys_old]
    assert not dup, f'keys already exist: {dup[:5]}'
    assert add['share_ratio'].null_count() == 0
    assert add.height == len(admitted), (add.height, len(admitted))

    SNAP.mkdir(parents=True, exist_ok=True)
    snap_path = SNAP / FUND_SHARE_CHANGE_FILENAME
    shutil.copy2(old_path, snap_path)
    assert sha(snap_path) == old_sha

    new = pl.concat([old.select(list(SHARE_CHANGE_COLUMNS)), add], how='vertical').sort(keys)
    write_parquet_atomic(new, old_path, compression='zstd', statistics=True)

    after = pl.read_parquet(old_path)
    after_sha = sha(old_path)

    # DG-20 ②：剔除 35 条新增后，剩余行必须与改前逐值相等
    restored = after.join(add.select(keys), on=keys, how='anti').sort(keys)
    unchanged = restored.equals(old.select(list(SHARE_CHANGE_COLUMNS)).sort(keys))
    common = after.join(old.select(list(SHARE_CHANGE_COLUMNS)), on=list(SHARE_CHANGE_COLUMNS),
                        how='anti').height

    # 抽样断言：ex_date 经 _next_trade_date 必须落到观测台阶日，且 ratio == step
    samples = []
    for r in admitted[::7]:
        code, d = r['ts_code'], date.fromisoformat(r['date'])
        dates = [x for (x,) in quotes(code).select('date').iter_rows()]
        ex = dates[dates.index(d) - 1]
        eff = _next_trade_date(dates, ex)
        samples.append({'ts_code': code, 'target': d.isoformat(), 'ex_date': ex.isoformat(),
                        'effective': eff.isoformat() if eff else None,
                        'ratio': r['step'], 'aligned': eff == d})

    # 全局断言：每条新事件的生效日都应在正式因子里
    fac = pl.read_parquet(FACTORS / 'fund_adj_factor.parquet').select(
        'ts_code', 'trade_date', 'qfq_factor').sort(['ts_code', 'trade_date'])
    fac = fac.with_columns(
        (pl.col('qfq_factor') / pl.col('qfq_factor').shift(1).over('ts_code')).alias('step'))
    eff_rows = []
    for r, row in zip(sorted(admitted, key=lambda x: (x['ts_code'], x['date'])), new_rows):
        eff_rows.append({'ts_code': r['ts_code'], 'trade_date': date.fromisoformat(r['date']),
                         'ratio': float(r['step'])})
    eff = pl.DataFrame(eff_rows).with_columns(pl.col('trade_date').cast(fac.schema['trade_date']))
    chk = eff.join(fac, on=['ts_code', 'trade_date'], how='left')
    miss = chk.filter(pl.col('step').is_null()).height
    worst = chk.filter(pl.col('step').is_not_null()).select(
        (pl.col('step') - pl.col('ratio')).abs().max().alias('d')).item()

    payload = {
        'before': {'bytes': snap_path.stat().st_size, 'rows': old_rows, 'sha256': old_sha,
                   'snapshot': str(snap_path)},
        'after': {'bytes': old_path.stat().st_size, 'rows': after.height,
                  'sha256': after_sha, 'delta': after.height - old_rows},
        'existing_rows_unchanged': bool(unchanged),
        'existing_rows_not_in_old_combo': common,
        'new_rows': add.height,
        'new_codes': add['ts_code'].n_unique(),
        'ex_date_min': str(add['ex_date'].min()),
        'ex_date_max': str(add['ex_date'].max()),
        'ratio_min': add['share_ratio'].min(),
        'ratio_max': add['share_ratio'].max(),
        'sample_align': samples,
        'effective_day_missing_in_factor': miss,
        'max_abs_step_minus_ratio': worst,
        'source_counts_before': dict(old.group_by('source').agg(pl.len()).rows()),
        'source_counts_after': dict(after.group_by('source').agg(pl.len()).rows()),
    }
    (WORK / 'b2s2_apply_share_change.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
    for k, v in payload.items():
        print(k, '=', v)


if __name__ == '__main__':
    main()
