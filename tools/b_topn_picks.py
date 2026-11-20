#!/usr/bin/env python3
"""B（股东盈余比）行业分层 Top50 月度候选名单：只出名单，不下单、不跑回测。

口径（与 ``src/backtest/strategies/b_topn_strategy.py`` 一致）
------------------------------------------------------------
- B = 经营现金流TTM / 总市值，直接取正式因子面板的 ``b_ocf_ttm`` 列（该列
  已是比率，禁止再除以 total_mv）；空值或 <= 0 的标的不参与排名。
- 行业分层：按申万一级行业分组，行业内按 ``(-B, ts_code)`` 取前
  ``industry_top_k`` 只；合并后再按 ``(-B, ts_code)`` 取前 ``top_n`` 只等权。
- ``industry_top_k`` 缺省 ``ceil(top_n / 31)``（50 只 → 2 只/行业）。
- 行业口径取面板自带 ``industry_l1``（申万一级代码）：面板构建时已按
  ``meta/industry_pit/industry_pit_sw.parquet`` 的 in_date/out_date 做 PIT
  asof 合并，本入口直接复用，不再二次 join。

产出
----
``reports/b_topn_picks/候选名单_B行业前50_<signal_date>.csv``
    列：signal_date / ts_code / industry_l1 / b_ocf_ttm / weight（等权，合计 1.0）。

用法
----
    .venv\\Scripts\\python.exe tools/b_topn_picks.py
    .venv\\Scripts\\python.exe tools/b_topn_picks.py --date 2026-11-19
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.local_store.paths import default_store_root  # noqa: E402

PANEL_REL = Path('factors') / 'factor_panel.parquet'
OUT_DIR = ROOT / 'reports' / 'b_topn_picks'
COLUMNS = ['signal_date', 'ts_code', 'industry_l1', 'b_ocf_ttm', 'weight']
#: 申万一级行业数量（策略默认值 n_industries=31），用于推导行业内在取只数。
N_INDUSTRIES = 31


def load_panel(root: Path | None = None) -> pl.DataFrame:
    """读取正式因子面板（只取选股需要的四列）。"""
    base = root if root is not None else default_store_root()
    path = Path(base) / PANEL_REL
    if not path.is_file():
        raise SystemExit(f'因子面板缺失：{path}')
    return pl.read_parquet(
        path, columns=['ts_code', 'signal_date', 'b_ocf_ttm', 'industry_l1'])


def build_picks(panel: pl.DataFrame, signal_date: date | None = None,
                top_n: int = 50, industry_top_k: int | None = None) -> pl.DataFrame:
    """行业内取前 k → 合并取前 top_n → 等权。

    Args:
        panel: 至少含 ts_code / signal_date / b_ocf_ttm / industry_l1 的面板。
        signal_date: 目标信号日；缺省取面板内最新交易日。
        top_n: 合并后的最终只数（默认 50）。
        industry_top_k: 每个行业保留只数；缺省 ``ceil(top_n / 31)``。

    Returns:
        ``signal_date, ts_code, industry_l1, b_ocf_ttm, weight``，权重合计 1.0。
    """
    if top_n <= 0:
        raise ValueError('top_n 必须为正整数')
    k = industry_top_k if industry_top_k is not None else math.ceil(top_n / N_INDUSTRIES)
    if k <= 0:
        raise ValueError('industry_top_k 必须为正整数')

    frame = panel.select(['ts_code', 'signal_date', 'b_ocf_ttm', 'industry_l1'])
    if signal_date is None:
        frame = frame.filter(pl.col('signal_date') == pl.col('signal_date').max())
    else:
        frame = frame.filter(pl.col('signal_date') == signal_date)
    frame = frame.filter(
        pl.col('b_ocf_ttm').is_not_null() & (pl.col('b_ocf_ttm') > 0)
        & pl.col('industry_l1').is_not_null() & (pl.col('industry_l1') != ''))
    empty = pl.DataFrame(schema={
        'signal_date': panel.schema['signal_date'], 'ts_code': pl.String,
        'industry_l1': pl.String, 'b_ocf_ttm': pl.Float64, 'weight': pl.Float64})
    if frame.is_empty():
        return empty

    # 行业内排序取前 k：先按 (行业, -B, 代码) 排好，再用行号判名次
    frame = frame.sort(['industry_l1', 'b_ocf_ttm', 'ts_code'],
                       descending=[False, True, False])
    frame = frame.with_columns(
        pl.int_range(1, pl.len() + 1).over('industry_l1').alias('_rank'))
    picks = (frame.filter(pl.col('_rank') <= k)
             .sort(['b_ocf_ttm', 'ts_code'], descending=[True, False])
             .head(top_n)
             .select(['signal_date', 'ts_code', 'industry_l1', 'b_ocf_ttm']))

    n = picks.height
    weights = [1.0 / n] * n
    # 把浮点舍入残差并入最后一只，保证顺序求和等于 1.0
    weights[-1] += 1.0 - math.fsum(weights)
    return picks.with_columns(pl.Series('weight', weights)).select(COLUMNS)


def _industry_names(root: Path | None) -> dict[str, str]:
    """申万一级「代码 → 名称」映射；元数据缺失时返回空映射（只影响摘要打印）。"""
    base = root if root is not None else default_store_root()
    path = Path(base) / 'meta' / 'industry_pit' / 'industry_pit_sw.parquet'
    if not path.is_file():
        return {}
    try:
        frame = pl.read_parquet(
            path, columns=['industry_l1', 'industry_l1_name']).unique()
    except Exception:  # noqa: BLE001 - 名称只用于摘要，失败不影响名单
        return {}
    return {str(code): str(name) for code, name in frame.rows() if code and name}


def main() -> int:
    ap = argparse.ArgumentParser(description='B 股东盈余比行业分层 TopN 候选名单（只出名单）')
    ap.add_argument('--date', default=None, help='信号日 YYYY-MM-DD（缺省：面板最新交易日）')
    ap.add_argument('--top-n', type=int, default=50, help='合并后取前 N 只（默认 50）')
    ap.add_argument('--industry-top-k', type=int, default=None,
                    help='行业内保留只数（缺省 ceil(top_n/31)，50 只即 2）')
    ap.add_argument('--store-root', default=None, help='local_store 根（默认 data/local_store）')
    ap.add_argument('--out-dir', default=str(OUT_DIR), help='输出目录')
    args = ap.parse_args()

    root = Path(args.store_root) if args.store_root else None
    panel = load_panel(root)
    day = date.fromisoformat(args.date) if args.date else None
    picks = build_picks(panel, day, top_n=args.top_n,
                        industry_top_k=args.industry_top_k)
    if picks.is_empty():
        target = day or '最新交易日'
        print(f'NO_PICKS {target}：面板无可用截面（B>0 且行业非空），不落盘', flush=True)
        return 0

    signal_day = str(picks['signal_date'][0])[:10]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'候选名单_B行业前{args.top_n}_{signal_day}.csv'
    tmp = path.with_name(path.name + '.tmp')
    picks.write_csv(tmp)
    os.replace(tmp, path)

    k = args.industry_top_k if args.industry_top_k is not None else math.ceil(args.top_n / N_INDUSTRIES)
    counts = picks.group_by('industry_l1').len().sort('len', descending=True)
    names = _industry_names(root)
    top3 = ' / '.join(
        f'{names.get(str(row[0])) or row[0]} {row[1]}' for row in counts.head(3).rows())
    total = math.fsum(picks['weight'].to_list())
    print(f'[候选名单] signal_date={signal_day} | {picks.height} 只 | '
          f'行业 {counts.height} 个 | 行业内取前 {k} | weight 合计 {total:.12f}')
    print(f'行业前3：{top3}')
    print(f'输出：{path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
