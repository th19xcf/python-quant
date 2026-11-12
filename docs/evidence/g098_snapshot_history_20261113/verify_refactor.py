#!/usr/bin/env python3
"""G-098 引用改造验收：11 处覆盖统计 + 2 处 source 的改前/改后逐值对比，兼 DG-23 端到端。

「改前」不依赖已删除的日期文件本体，而是按旧 kind 的**语义**在 `_backup` 快照上复算：
latest_glob_distinct = mtime 最新那份文件内的 code 去重数；latest_glob_max = 该文件 as_of 最大值。
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.data import industry as industry_mod  # noqa: E402
from src.data import risk_flags, st_history  # noqa: E402
from src.data.quality import freshness  # noqa: E402
from src.utils.config import get_config  # noqa: E402
from src.data.local_store.store import resolve_store_root  # noqa: E402

STORE = resolve_store_root(get_config())
BACKUP_ST = ROOT / '_backup' / 'meta' / 'st_status_20261113'
BACKUP_IND = ROOT / '_backup' / 'meta' / 'industry_20261113'
CONFIG = json.loads((ROOT / 'src/data/quality/freshness_config.json').read_text(encoding='utf-8'))


def _old_latest_file(backup: Path) -> Path:
    files = sorted(backup.glob('*_*.parquet'))
    return max(files, key=lambda p: p.stat().st_mtime_ns)


def _old_distinct(backup: Path) -> int:
    return pl.read_parquet(_old_latest_file(backup), columns=['code'])['code'].n_unique()


def _old_max(backup: Path) -> str:
    frame = pl.read_parquet(_old_latest_file(backup), columns=['as_of'])
    return str(frame['as_of'].max())


def main() -> int:
    rows: list[dict[str, object]] = []
    old_st, old_ind = _old_distinct(BACKUP_ST), _old_distinct(BACKUP_IND)
    old_st_max, old_ind_max = _old_max(BACKUP_ST), _old_max(BACKUP_IND)
    ok = True
    for spec in freshness.parse_config(CONFIG):
        key = spec.key
        coverage = spec.coverage or {}
        source = spec.source or {}
        result = freshness.check_dataset(STORE, spec, date(2026, 11, 13))
        for slot, after in (('count', result.covered_count),
                            ('expected', result.expected_count)):
            spec_part = coverage.get(slot) or {}
            if spec_part.get('kind') == 'latest_as_of_distinct':
                backup = BACKUP_ST if 'st_status' in spec_part['path'] else BACKUP_IND
                before = _old_distinct(backup)
                rows.append({'item': f'{key}.coverage.{slot}', 'before': before,
                             'after': after})
                ok = ok and before == after
        if source.get('kind') == 'parquet_max' and source['column'] == 'as_of' \
                and ('st_status_history' in source['path']
                     or 'industry_history' in source['path']):
            backup = BACKUP_ST if 'st_status_history' in source['path'] else BACKUP_IND
            before = _old_max(backup)
            after = result.latest_date.isoformat() if result.latest_date else None
            rows.append({'item': f'{key}.source', 'before': before, 'after': after})
            ok = ok and before == after

    for row in rows:
        flag = 'OK ' if row['before'] == row['after'] else 'DIFF'
        print(f'{flag} {row["item"]:<28} 改前={row["before"]}  改后={row["after"]}')
    print(f'\n配置项合计 {len(rows)} 处，逐值相等={ok}')

    print('\n---- DG-23 真实读路径端到端 ----')
    snap, day = risk_flags.load_snapshot(STORE, date(2026, 11, 10))
    print(f'risk_flags.load_snapshot(2026-11-10) -> {snap.height} 行, day={day}')
    detail = risk_flags.get_st_status_detail('000004.SZ', date(2026, 11, 10))
    print(f'get_st_status_detail(000004.SZ, 2026-11-10) -> status={detail["status"]}, '
          f'as_of={detail["as_of"]}, source={detail["source"]}')
    print(f'risk_flags.snapshot_days -> {[str(d) for d in risk_flags.snapshot_days(STORE)]}')
    series = st_history._snapshot_series(STORE)
    print(f'st_history._snapshot_series -> {len(series)} 层, 首层 {series[0][1]}, 末层 {series[-1][1]}')

    ind, ind_day = industry_mod.load_snapshot(STORE, date(2026, 11, 10))
    print(f'industry.load_snapshot(2026-11-10) -> {ind.height} 行, day={ind_day}')
    got = industry_mod.get_industry('600000.SH', date(2026, 11, 10))
    print(f'get_industry(600000.SH, 2026-11-10) -> {got["industry_l1"]}/{got["industry_l2"]}, '
          f'as_of={got["as_of"]}, status={got["status"]}')

    # 逐值对拍：读到的每一层必须与 _backup 里同日的原文件逐字节等价（按 code 排序）
    for backup, loader in ((BACKUP_ST, risk_flags.load_snapshot),
                           (BACKUP_IND, industry_mod.load_snapshot)):
        for path in sorted(backup.glob('*_*.parquet')):
            src = pl.read_parquet(path)
            target = src['as_of'][0]
            loaded, got_day = loader(STORE, target)
            same = loaded.select(src.columns).sort('code').equals(src.sort('code'))
            print(f'  {path.name}: 读回 {loaded.height} 行, as_of={got_day}, 逐值相等={same}')
            ok = ok and same and got_day == target
    print(f'\n总体: {"PASS" if ok else "FAIL"}')
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
