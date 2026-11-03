#!/usr/bin/env python3

"""风险过滤最小冒烟（功能验证，非策略评估）。

三种模式：

    python scripts/smoke_risk_filters.py --mode event      # 2025 小样本回放，事件过滤生效
    python scripts/smoke_risk_filters.py --mode off        # 同上，三块过滤全关（对照旧实现）
    python scripts/smoke_risk_filters.py --mode snapshot   # 2026-11-03 快照审计 ST/行业

不跑完整 A/B、不做参数寻优：只验证「开关有没有接上、剔除能不能逐条核对」。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
from loguru import logger

from src.advice.config import RiskFilterConfig, default_config
from src.advice.data import panel_trading_dates
from src.advice.replay import _ReplayTier
from src.advice.risk_filters import RiskContext
from src.advice.scoring import attach_score
from src.advice.universe import build_meta, load_name_map
from src.utils.config import get_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = PROJECT_ROOT / 'reports' / 'advice' / 'calibration'
SNAPSHOT_DATE = date(2026, 11, 3)


def _load(cache_dir: Path) -> tuple[pl.DataFrame, list[str]]:
    factors = pl.read_parquet(cache_dir / 'factor_panel.parquet')
    universe = json.loads((cache_dir / 'universe.json').read_text(encoding='utf-8'))
    return factors, list(universe['codes'])


def _store_root(explicit: str | None) -> Path:
    return Path(explicit) if explicit else Path(get_config().data.local_store_path)


def _names(root: Path) -> dict[str, str]:
    return load_name_map(root / 'meta' / 'names.json')


def _pick_codes(root: Path, pool: list[str], limit: int) -> list[str]:
    events = pl.read_parquet(root / 'meta' / 'events' / 'events.parquet')
    with_events = set(events['ts_code'].to_list())
    picked = [code for code in pool if code in with_events][:limit]
    if not picked:
        raise SystemExit('股票池与事件表没有交集，无法做事件冒烟')
    return picked


def _run(cache_dir: Path, root: Path, codes: list[str],
         filters_on: bool) -> dict:
    factors, _ = _load(cache_dir)
    frame = factors.filter((pl.col('date') >= date(2025, 1, 1))
                           & (pl.col('date') <= date(2025, 12, 31))
                           & pl.col('ts_code').is_in(codes))
    config = default_config()
    if hasattr(config, 'risk'):
        config = replace(config, risk=RiskFilterConfig(
            st_enabled=filters_on,
            industry_enabled=filters_on,
            event_enabled=filters_on,
        ))
    tier = config.tier('short')
    scored = attach_score(frame, tier)
    dates = panel_trading_dates(scored)
    meta = build_meta(codes, _names(root), {})
    runner = _ReplayTier(
        SimpleNamespace(config=config, store_root=root),
        tier, scored, dates, meta, 1_000_000.0)
    summary = runner.run().summary()
    summary.pop('sample_rows', None)
    summary.pop('equity_curve', None)
    names = _names(root)
    summary['filter_audit'] = [
        {**item, 'name': names.get(item['ts_code'], '')}
        for item in summary.get('filter_audit', [])
    ]
    return summary


def _snapshot(root: Path, pool: list[str], limit: int) -> dict:
    context = RiskContext(RiskFilterConfig(), root, [SNAPSHOT_DATE])
    names = _names(root)
    st_codes: list[dict] = []
    industry_counts: dict[str, int] = {}
    unknown_industry = 0
    unknown_st = 0
    for code in pool:
        status = context.st_status(code, SNAPSHOT_DATE)
        if status == 'st':
            st_codes.append({'ts_code': code, 'name': names.get(code, '')})
        elif status == 'unknown':
            unknown_st += 1
        bucket, source = context.industry(code, SNAPSHOT_DATE)
        industry_counts[bucket] = industry_counts.get(bucket, 0) + 1
        if bucket == 'unknown':
            unknown_industry += 1
    industry_counts.pop('unknown', None)
    return {
        'date': SNAPSHOT_DATE.isoformat(),
        'pool_size': len(pool),
        'st_dropped': len(st_codes),
        'st_samples': st_codes[:limit],
        'st_unknown': unknown_st,
        'industry_groups': len(industry_counts),
        'industry_unknown': unknown_industry,
        'industry_top': sorted(industry_counts.items(),
                               key=lambda item: item[1], reverse=True)[:10],
        'filter_stats': context.describe(),
        'notes': context.notes(0.25),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['event', 'off', 'snapshot'], required=True)
    parser.add_argument('--codes', type=int, default=40)
    parser.add_argument('--cache', default=str(DEFAULT_CACHE))
    parser.add_argument('--store-root', default=None)
    parser.add_argument('--out', default=None, help='把结果写成 UTF-8 JSON')
    args = parser.parse_args()
    cache_dir = Path(args.cache)
    root = _store_root(args.store_root)
    factors, pool = _load(cache_dir)

    if args.mode == 'snapshot':
        report = _snapshot(root, pool, 10)
    else:
        codes = _pick_codes(root, pool, args.codes)
        report = _run(cache_dir, root, codes, filters_on=(args.mode == 'event'))
        report['codes'] = len(codes)
    if args.out:
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2),
                                  encoding='utf-8')
    logger.info(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
