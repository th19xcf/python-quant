"""把逐只取证拿到的基金分红事件补入 ``factors/fund_dividend.parquet``（DG-35 一次改完）。

流程：解析原始行 → 候选表（旧表 + 新行）落 ``_staging/`` → 逐值对拍 →
旧表快照进 ``_backup/snapshots/fund_dividend_<日期>/`` → ``write_parquet_atomic``
原子替换 → 增量重算受影响基金复权因子并回填 manifest。

只补「本地无记录、逐只接口有记录」的代码；不覆盖任何既有行。
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.atomic_write import write_parquet_atomic
from src.data.local_store.fund_factors import (
    DIVIDEND_COLUMNS,
    FUND_DIVIDEND_FILENAME,
    FUND_SHARE_CHANGE_FILENAME,
    rebuild_fund_factors,
)
from src.data.staging import staging_dir

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
SOURCE_TAG = 'akshare:fund_open_fund_info_em(分红送配详情)'
_AMOUNT = re.compile(r'(\d+(?:\.\d+)?)')


def parse_cash(text: str) -> float | None:
    """「每10份派现金0.3700元」→ 0.037（每份）；「每份派现金0.05元」→ 0.05。"""
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    # 先去掉「每10份」「每份」，否则正则首个数字会命中份额数（10）而不是金额
    body = s.replace('每10份', '').replace('每份', '')
    match = _AMOUNT.search(body)
    if not match:
        return None
    value = float(match.group(1))
    return value / 10.0 if '每10份' in s else value


def parse_rows(raw_path: Path, fetched_at: datetime) -> pl.DataFrame:
    raw = pl.read_csv(raw_path, infer_schema_length=0)
    rows: list[dict] = []
    bad: list[str] = []
    for rec in raw.to_dicts():
        code = rec['ts_code']
        ex = (rec.get('除息日') or '').strip()[:10]
        rec_date = (rec.get('权益登记日') or '').strip()[:10]
        cash = parse_cash(rec.get('每10份分红'))
        if not ex or cash is None or cash <= 0:
            bad.append(f'{code}|{ex}|{rec.get("每10份分红")}')
            continue
        rows.append({
            'ts_code': code,
            'ex_date': date.fromisoformat(ex),
            'record_date': date.fromisoformat(rec_date) if rec_date else None,
            'cash_amount': cash,
            'source': SOURCE_TAG,
            'fetched_at': fetched_at,
        })
    if bad:
        raise ValueError(f'原始行无法解析 {len(bad)} 条：{bad[:5]}')
    frame = pl.DataFrame(rows, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'record_date': pl.Date,
        'cash_amount': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
    })
    # 同代码同除息日重复：取最大金额（与 fund_fh_em 归一化口径一致）
    return (frame.sort('cash_amount', descending=True)
            .unique(subset=['ts_code', 'ex_date'], keep='first', maintain_order=True)
            .select(DIVIDEND_COLUMNS).sort(['ts_code', 'ex_date']))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rows', required=True, help='逐只取证原始行 CSV')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    fetched_at = datetime.now()
    target = STORE / 'factors' / FUND_DIVIDEND_FILENAME
    old = pl.read_parquet(target).select(DIVIDEND_COLUMNS)
    new = parse_rows(Path(args.rows), fetched_at)

    overlap = set(old['ts_code'].unique().to_list()) & set(new['ts_code'].unique().to_list())
    if overlap:
        raise RuntimeError(f'新行与既有分红表代码重叠，拒绝覆盖：{sorted(overlap)[:5]}')

    merged = pl.concat([old, new], how='vertical_relaxed').select(DIVIDEND_COLUMNS)
    dup = merged.group_by(['ts_code', 'ex_date']).len().filter(pl.col('len') > 1)
    if dup.height:
        raise RuntimeError(f'合并后出现 (ts_code, ex_date) 重复 {dup.height} 组')

    # 候选落 _staging（DG-5）
    stage = staging_dir('fund_dividend_backfill', with_date=True)
    stage.mkdir(parents=True, exist_ok=True)
    candidate = stage / 'fund_dividend_candidate.parquet'
    write_parquet_atomic(merged, candidate, compression='zstd', statistics=True)

    # 逐值对拍：候选回读的新增行 == 解析结果，旧行逐列不变
    back = pl.read_parquet(candidate)
    back_new = back.filter(pl.col('source') == SOURCE_TAG).sort(['ts_code', 'ex_date'])
    back_old = back.filter(pl.col('source') != SOURCE_TAG).sort(['ts_code', 'ex_date'])
    assert back_new.equals(new.sort(['ts_code', 'ex_date'])), '新增行逐值对拍不一致'
    assert back_old.equals(old.sort(['ts_code', 'ex_date'])), '既有行被改动'
    assert back.height == old.height + new.height, '行数守恒不成立'

    stats = {
        'old_rows': old.height, 'old_codes': old['ts_code'].n_unique(),
        'new_rows': new.height, 'new_codes': new['ts_code'].n_unique(),
        'merged_rows': merged.height,
        'new_codes_list': sorted(new['ts_code'].unique().to_list()),
        'source': SOURCE_TAG,
        'fetched_at': fetched_at.isoformat(timespec='seconds'),
        'candidate': str(candidate),
    }
    if args.dry_run:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        return 0

    # 改前快照（DG-11）：进仓库根 _backup，不动正式目录
    snap = ROOT / '_backup' / 'snapshots' / f'fund_dividend_{date.today():%Y-%m-%d}'
    snap.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target, snap / f'{FUND_DIVIDEND_FILENAME}.bak.parquet')
    old.write_csv(snap / 'fund_dividend_before.csv')

    write_parquet_atomic(merged, target, compression='zstd', statistics=True)
    assert pl.read_parquet(target).height == merged.height, '替换后行数不一致'

    share = pl.read_parquet(STORE / 'factors' / FUND_SHARE_CHANGE_FILENAME)
    factor_stats = rebuild_fund_factors(
        STORE, merged, share, codes=stats['new_codes_list'], dry_run=False)

    factors = pl.read_parquet(STORE / 'factors' / 'fund_adj_factor.parquet') \
        .filter(pl.col('ts_code').is_in(stats['new_codes_list']))
    check = (factors.group_by('ts_code')
             .agg(pl.col('qfq_factor').min().alias('qfq_min'),
                  pl.len().alias('rows'))
             .sort('ts_code'))
    stats['factor_rows'] = factors.height
    stats['factor_codes'] = check.height
    stats['qfq_min_overall'] = float(check['qfq_min'].min())
    stats['factor_stats_elapsed'] = factor_stats.get('elapsed_s')
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(check)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
