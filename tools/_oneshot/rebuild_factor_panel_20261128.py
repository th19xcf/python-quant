#!/usr/bin/env python3
"""受控全量重建 factor_panel 并出前后对拍（2026-11-28）。

背景
----
2026-11-11 的 income.net_profit「归母→合并」口径修正与后续 balance/cashflow 重采
未回填到 ``factor_panel`` 历史行（面板长期靠增量 upsert 近期日期维护），导致旧面板
历史段仍是旧口径。本脚本按「改前 DG-22 快照 → dry-run 一次 → apply 一次（DG-35 不跑
第二轮）→ 与快照逐列对拍 → 归因」执行受控全量重建。

对拍要点
--------
- 逐列差异行数（ep/bp/dy/b_ocf_ttm/roe_ttm/accruals/gross_margin/equity_basis/industry_l1）；
- accruals 符号翻转行数；
- accruals 差异归因：按 signal_date 当时最新财报（或同年上一期）``net_profit !=
  parent_net_profit`` 判定是否可归因到净利口径修正未回填；剩余单列；
- 保留校验：``equity_basis`` 标记与 2015–2017 bp/dy 补丁（``total_equity_incl_minority``）。

用法::

    .venv\\Scripts\\python.exe tools\\_oneshot\\rebuild_factor_panel_20261128.py
"""
from __future__ import annotations

import glob
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
PANEL = ROOT / 'data/local_store/factors/factor_panel.parquet'
INCOME = ROOT / 'data/local_store/meta/financials/income.parquet'
STAGING = ROOT / '_staging/income_netprofit_fix_20261128'
KEY = ['ts_code', 'signal_date']
NUM = ['ep', 'bp', 'dy', 'b_ocf_ttm', 'roe_ttm', 'accruals', 'gross_margin']
STR = ['equity_basis', 'industry_l1']
TH = 1e-6


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _snapshot() -> Path:
    dst = ROOT / '_backup' / f'factor_panel_{datetime.now():%Y%m%d_%H%M%S}'
    dst.mkdir(parents=True, exist_ok=True)
    shutil.copy2(PANEL, dst / 'factor_panel.parquet')
    if _sha(PANEL) != _sha(dst / 'factor_panel.parquet'):
        raise SystemExit('DG-22 快照 SHA 不符')
    return dst


def _run(flag: str) -> None:
    r = subprocess.run([sys.executable, str(ROOT / 'tools/factor_panel.py'), flag],
                       cwd=str(ROOT), capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f'factor_panel {flag} 失败：{r.stderr[-800:]}')


def _diff(old: Path, new: Path) -> dict:
    ok = pl.scan_parquet(old).select(KEY).collect()
    nk = pl.scan_parquet(new).select(KEY).collect()
    new_only = nk.join(ok, on=KEY, how='anti')
    years = sorted(pl.scan_parquet(new).select(pl.col('signal_date').dt.year().alias('y'))
                   .unique().collect()['y'].to_list())
    num = {c: {'changed': 0, 'null_to_val': 0, 'val_to_null': 0} for c in NUM}
    strd = dict.fromkeys(STR, 0)
    signflip = 0
    acc_rows = []
    for y in years:
        j = (pl.scan_parquet(old).filter(pl.col('signal_date').dt.year() == y)
             .join(pl.scan_parquet(new).filter(pl.col('signal_date').dt.year() == y),
                   on=KEY, how='inner', suffix='_new').collect())
        for c in NUM:
            a, b = j[c], j[c + '_new']
            both = a.is_not_null() & b.is_not_null()
            d = both & ((a - b).abs() > TH)
            num[c]['changed'] += int(d.sum())
            num[c]['null_to_val'] += int((a.is_null() & b.is_not_null()).sum())
            num[c]['val_to_null'] += int((a.is_not_null() & b.is_null()).sum())
            if c == 'accruals':
                signflip += int((d & (a * b < 0)).sum())
        acc_rows.append(j.filter(
            ((pl.col('accruals') - pl.col('accruals_new')).abs() > TH)
            & pl.col('accruals').is_not_null()
            & pl.col('accruals_new').is_not_null()).select(KEY))
        for c in STR:
            strd[c] += int((j[c] != j[c + '_new']).sum())
        del j

    adr = pl.concat(acc_rows)
    inc = (pl.read_parquet(INCOME, columns=['ts_code', 'end_date', 'ann_date',
                                            'net_profit', 'parent_net_profit'])
           .filter(pl.col('ann_date').is_not_null()).sort(['ts_code', 'end_date'])
           .with_columns([((pl.col('net_profit') - pl.col('parent_net_profit')).abs() > 0.5)
                          .alias('cal'), pl.col('end_date').dt.year().alias('_y')])
           .with_columns(pl.col('cal').shift(1).over(['ts_code', '_y']).fill_null(False)
                         .alias('cal_prev')))
    inc = inc.with_columns((pl.col('cal') | pl.col('cal_prev')).alias('cal_any')).sort(['ts_code', 'ann_date'])
    m = (adr.sort(['ts_code', 'signal_date'])
         .join_asof(inc.select(['ts_code', 'ann_date', 'cal_any']),
                    left_on='signal_date', right_on='ann_date', by='ts_code',
                    strategy='backward'))
    attr = int(m['cal_any'].fill_null(False).sum())
    unattr_ex = m.filter(~pl.col('cal_any').fill_null(False)).select(KEY).head(5).to_dicts()

    eb = (pl.scan_parquet(new).group_by('equity_basis').len()
          .sort('len', descending=True).collect().to_dicts())
    pre = pl.scan_parquet(new).filter(pl.col('signal_date') < datetime(2018, 1, 1).date()).select(
        pl.len().alias('n'), pl.col('bp').is_not_null().sum().alias('bp_nn'),
        (pl.col('equity_basis') == 'total_equity_incl_minority').sum().alias('patch')).collect().to_dicts()[0]
    return {
        'rows_old': ok.height, 'rows_new': nk.height,
        'new_only_rows': new_only.height,
        'numeric_diff': num, 'string_diff': strd, 'accruals_signflip': signflip,
        'accruals_diff_rows': adr.height, 'attributed_net_caliber': attr,
        'unattributed': adr.height - attr, 'unattributed_examples': unattr_ex,
        'equity_basis_new': eb, 'pre2018_bp_dy_patch': pre,
    }


def main() -> int:
    snap = _snapshot()
    old = snap / 'factor_panel.parquet'
    _run('--dry-run')
    _run('--apply')
    ev = {'snapshot': str(snap.relative_to(ROOT)), 'sha_old': _sha(old),
          'sha_new': _sha(PANEL), 'at': datetime.now().isoformat(timespec='seconds'),
          **_diff(old, PANEL)}
    STAGING.mkdir(parents=True, exist_ok=True)
    (STAGING / 'fp_rebuild_after.json').write_text(
        json.dumps(ev, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps({k: ev[k] for k in ['rows_old', 'rows_new', 'new_only_rows',
                                         'accruals_signflip', 'accruals_diff_rows',
                                         'attributed_net_caliber', 'unattributed']},
                     ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
