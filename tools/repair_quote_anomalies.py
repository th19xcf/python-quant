#!/usr/bin/env python3

r"""按契约巡检违规清单就地修复**可判定**的行情瑕疵（幂等、默认 dry-run）。

设计原则
========
只改「有唯一正确值」的列，绝不编造源端缺失值：

- ``pct_chg_mismatch`` / ``change_mismatch``：用项目权威口径重算派生列
  ``pct_chg = (close/pre_close - 1)*100``、``change = close - pre_close``。
  与 ``src/data/local_store/build.py:198-202`` 及
  ``src/data/quality/rules.py:332-355`` 同一公式、同一 1e-6 容差。
- ``price_non_positive`` 且 OHLC 全空：整行删除（源端坏行，保留会污染
  pre_close 链），连带消除该行的 required_columns / change_mismatch。
- ``high_lt_open_close`` / ``low_gt_open_close``：把 high/low clip 到
  ``[min(open,close), max(open,close)]``。这是物理上不可能的取值，clip
  不改变 close / 收益，只纠正不可能的极值。

2026-11-07 23:3x 扩展（第二轮收口）：
- ``pre_close_mismatch`` 中 **``pre_close <= 0``** 的行有唯一正确值：用
  上一交易日 close 按 qfq_factor 折算（与 ``rules.rule_pre_close_mismatch``
  同一公式），连带重算 change / pct_chg。非零的前收差异多为除权跳空或
  板块指数重编，属合法口径，**不动**。
- ``amount_dimension`` 中 **1996 年后 ratio∈(50,200) 或 ≈0.01** 的行是
  volume 被整体缩放了 100 倍（少写 100 倍 / 单位变成「股」），amount 可信，
  统一用 ``amount/(close*100)`` 反推 volume。1996 年前 volume 单位本就不是
  「手」，规则乘 100 不适用，不改数据；ratio<0.005（源端 amount 坏值）与
  ratio≈0.1（10 倍错位）无唯一正确值，不修。

**不修**的源端瑕疵（写入隔离台账，不编造数据）：
``volume_invalid``、``required_columns`` 的 null volume/amount、
``amount_dimension`` 的早年单位口径与收盘价代理均价误差、
``weekend_trade_date``（1996 年前周六合法交易）、``pre_close_mismatch``
的非零前收差异（除权跳空 / 指数重编）。

用法::

    python tools/repair_quote_anomalies.py --violations <巡检_violations.csv>
    python tools/repair_quote_anomalies.py --violations <...> --commit

产出：``data/local_store/meta/anomalies/quarantine_rows.parquet`` 与
``reports/data_audit_2026-11-07/行情修复报告_2026-11-07.json``。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = PROJECT_ROOT / 'data' / 'local_store'

#: 可就地修复的规则
FIXABLE_RULES = frozenset({
    'pct_chg_mismatch', 'change_mismatch', 'price_non_positive',
    'high_lt_open_close', 'low_gt_open_close',
    'pre_close_mismatch', 'amount_dimension',
})

#: 量纲规则适用起点：1996 年前 volume 单位非「手」，乘 100 的判据不成立
DIMENSION_RULE_START = date(1996, 1, 1)
#: 100 倍错位带宽：ratio>50 表示 volume 少写 100 倍（amount 正常）；
#: ratio≈0.01 表示 volume 单位变成「股」而规则按「手」算，多算 100 倍。
#: 两种方向都可用 volume = amount/(close*100) 唯一反推。
#: ratio<0.005 是源端 amount 坏值（成交额几十元），ratio≈0.1 是 10 倍
#: 错位，两者无唯一正确值，不修、登记口径。
VOLUME_RESCALE_BAND = (50.0, 200.0)
VOLUME_RESCALE_LOW = (0.005, 0.02)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='就地修复可判定的行情瑕疵')
    parser.add_argument('--violations', required=True,
                        help='契约巡检产出的 *_violations.csv')
    parser.add_argument('--store-root', default='',
                        help='本地列存根目录（默认 data/local_store）')
    parser.add_argument('--commit', action='store_true',
                        help='真正写回生产 parquet（默认 dry-run）')
    parser.add_argument('--report', default='',
                        help='修复报告输出路径（默认 reports/ 下）')
    return parser.parse_args(argv)


def _market_of(code: str) -> str:
    return code.rsplit('.', 1)[-1].lower()


def _type_map(root: Path) -> dict[str, str]:
    di = pl.read_parquet(root / 'meta' / 'daily_index.parquet')
    return dict(zip(di['ts_code'].to_list(), di['data_type'].to_list(),
                    strict=False))


def _load_targets(violations: Path) -> dict[str, dict[str, set[date]]]:
    """按标的聚合成 {code: {rule: {dates}}}，只保留可修规则。"""
    frame = pl.read_csv(violations)
    out: dict[str, dict[str, set[date]]] = defaultdict(
        lambda: defaultdict(set))
    for row in frame.filter(pl.col('rule_id').is_in(list(FIXABLE_RULES))).iter_rows(named=True):
        raw = row.get('trade_date')
        if raw is None or raw == '':
            continue
        d = raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
        out[str(row['ts_code'])][str(row['rule_id'])].add(d)
    return out


def _repair_frame(frame: pl.DataFrame,
                  rules: dict[str, set[date]]) -> tuple[pl.DataFrame, dict[str, int]]:
    """对单个标的行情做修复，返回 (新表, 各类修复行数)。"""
    counts = {'pct_chg_recomputed': 0, 'change_recomputed': 0,
              'ohlc_clipped': 0, 'rows_dropped': 0,
              'pre_close_rebuilt': 0, 'volume_rescaled': 0}
    dates_col = 'date'
    if dates_col not in frame.columns:
        return frame, counts
    # shift() 依赖时间顺序；列存本身有序，显式排序保证前收派生正确
    frame = frame.sort(dates_col)

    # 1) pre_close <= 0 用上一交易日 close 折算重建，连带重算派生列
    bad_pc = rules.get('pre_close_mismatch', set())
    if bad_pc and {'pre_close', 'close', 'change', 'pct_chg'} <= set(frame.columns):
        prev_close = pl.col('close').shift(1)
        if 'qfq_factor' in frame.columns:
            expected = (
                pl.when(pl.col('qfq_factor') != 0)
                .then(prev_close * pl.col('qfq_factor').shift(1) / pl.col('qfq_factor'))
                .otherwise(None)
            )
        else:
            expected = prev_close
        # 幂等判据：pre_close<=0（原始坏行）或已等于折算前收（上一轮修过
        # pre_close 但派生列没跟上）。非零且不等于前收的是除权跳空/指数重编，
        # 属合法口径，绝不覆盖。
        pc_mask = (
            pl.col(dates_col).is_in(list(bad_pc))
            & expected.is_not_null()
            & (expected > 0)
            & (
                (pl.col('pre_close') <= 0)
                | ((pl.col('pre_close') - expected).abs() < 1e-6)
            )
        )
        # 物化成列：with_columns 内所有表达式基于同一输入快照，直接复用
        # pc_mask 会在 pre_close 改写后失效（曾导致 change/pct_chg 未更新）。
        frame = frame.with_columns(pc_mask.alias('_pc_fix'))
        n_pc = frame.filter(pl.col('_pc_fix')).height
        if n_pc:
            frame = frame.with_columns(
                pl.when(pl.col('_pc_fix')).then(expected)
                .otherwise(pl.col('pre_close')).alias('pre_close'))
            frame = frame.with_columns([
                pl.when(pl.col('_pc_fix')).then(pl.col('close') - pl.col('pre_close'))
                .otherwise(pl.col('change')).alias('change'),
                pl.when(pl.col('_pc_fix')).then(
                    (pl.col('close') / pl.col('pre_close') - 1) * 100)
                .otherwise(pl.col('pct_chg')).alias('pct_chg'),
            ])
            counts['pre_close_rebuilt'] = n_pc
        frame = frame.drop('_pc_fix')

    # 3) 删除 OHLC 全空的坏行（仅限 price_non_positive 命中日期）
    bad_dates = rules.get('price_non_positive', set())
    ohlc = ['open', 'high', 'low', 'close']
    if bad_dates and all(c in frame.columns for c in ohlc):
        null_all = pl.all_horizontal([pl.col(c).is_null() for c in ohlc])
        drop_mask = pl.col(dates_col).is_in(list(bad_dates)) & null_all
        n_drop = frame.filter(drop_mask).height
        if n_drop:
            frame = frame.filter(~drop_mask)
            counts['rows_dropped'] = n_drop
        bad_dates = set()  # 非空但非全空的行不处理，留待人工核对

    # 3b) 重算派生列（只对违规日期且 pre_close > 0）
    pct_dates = rules.get('pct_chg_mismatch', set()) | rules.get('change_mismatch', set())
    if pct_dates and {'pre_close', 'close', 'pct_chg'} <= set(frame.columns):
        fixable = (pl.col(dates_col).is_in(list(pct_dates))
                   & (pl.col('pre_close') > 0)
                   & pl.col('close').is_not_null())
        derived = (pl.col('close') / pl.col('pre_close') - 1) * 100
        n_pct = frame.filter(
            fixable & ((pl.col('pct_chg') - derived).abs() > 1e-6)
        ).height
        frame = frame.with_columns(
            pl.when(fixable).then(derived).otherwise(pl.col('pct_chg'))
            .alias('pct_chg'))
        counts['pct_chg_recomputed'] = n_pct
        if 'change' in frame.columns:
            n_chg = frame.filter(
                fixable & ((pl.col('change') - (pl.col('close') - pl.col('pre_close'))).abs() > 1e-6)
            ).height
            frame = frame.with_columns(
                pl.when(fixable).then(pl.col('close') - pl.col('pre_close'))
                .otherwise(pl.col('change')).alias('change'))
            counts['change_recomputed'] = n_chg

    # 4) volume 被源端少写 100 倍：amount 可信，反推成交量
    ad_dates = rules.get('amount_dimension', set())
    if ad_dates and {'amount', 'close', 'volume'} <= set(frame.columns):
        ratio = pl.col('amount') / (pl.col('close') * pl.col('volume') * 100)
        shifted_100x = (
            ((ratio > VOLUME_RESCALE_BAND[0]) & (ratio < VOLUME_RESCALE_BAND[1]))
            | ((ratio > VOLUME_RESCALE_LOW[0]) & (ratio < VOLUME_RESCALE_LOW[1]))
        )
        vol_mask = (
            pl.col(dates_col).is_in(list(ad_dates))
            & (pl.col(dates_col) >= DIMENSION_RULE_START)
            & (pl.col('amount') > 0) & (pl.col('close') > 0)
            & shifted_100x
        )
        n_vol = frame.filter(vol_mask).height
        if n_vol:
            frame = frame.with_columns(
                pl.when(vol_mask).then(pl.col('amount') / (pl.col('close') * 100))
                .otherwise(pl.col('volume')).alias('volume'))
            counts['volume_rescaled'] = n_vol

    # 5) OHLC clip（物理不可能的 high/low）
    hi_dates = rules.get('high_lt_open_close', set())
    lo_dates = rules.get('low_gt_open_close', set())
    if (hi_dates or lo_dates) and {'open', 'close', 'high', 'low'} <= set(frame.columns):
        upper = pl.max_horizontal(['open', 'close'])
        lower = pl.min_horizontal(['open', 'close'])
        hi_mask = (pl.col(dates_col).is_in(list(hi_dates))
                   & (pl.col('high') < upper - 1e-6))
        lo_mask = (pl.col(dates_col).is_in(list(lo_dates))
                   & (pl.col('low') > lower + 1e-6))
        n_clip = frame.filter(hi_mask | lo_mask).height
        if n_clip:
            frame = frame.with_columns([
                pl.when(hi_mask).then(upper).otherwise(pl.col('high')).alias('high'),
                pl.when(lo_mask).then(lower).otherwise(pl.col('low')).alias('low'),
            ])
            counts['ohlc_clipped'] = n_clip
    return frame, counts


def _write_atomic(path: Path, frame: pl.DataFrame) -> None:
    tmp = path.with_suffix('.parquet.tmp')
    frame.write_parquet(tmp, compression='zstd', statistics=True)
    os.replace(tmp, path)


def _build_quarantine(violations: Path, targets: dict[str, dict[str, set[date]]],
                      root: Path, commit: bool) -> dict[str, Any]:
    """把「未修、且属结构性错误」的行登记为隔离台账。"""
    structural = frozenset({'volume_invalid', 'required_columns', 'price_non_positive'})
    frame = pl.read_csv(violations)
    rows = frame.filter(pl.col('rule_id').is_in(list(structural)))
    if rows.height == 0:
        return {'rows': 0, 'codes': 0, 'written': False}
    grouped = (rows.group_by(['ts_code', 'trade_date'])
               .agg(pl.col('rule_id').unique().sort().str.join(',').alias('rule_ids'))
               .sort(['ts_code', 'trade_date']))
    out = root / 'meta' / 'anomalies' / 'quarantine_rows.parquet'
    info = {'rows': grouped.height, 'codes': grouped['ts_code'].n_unique(),
            'path': str(out), 'written': commit}
    if commit:
        _write_atomic(out, grouped)
    return info


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = Path(args.store_root) if args.store_root else DEFAULT_ROOT
    violations = Path(args.violations)
    if not violations.is_file():
        logger.error(f'违规清单不存在: {violations}')
        return 1
    targets = _load_targets(violations)
    types = _type_map(root)
    totals: dict[str, int] = defaultdict(int)
    per_code: list[dict[str, Any]] = []
    missing: list[str] = []

    for code, rules in sorted(targets.items()):
        data_type = types.get(code)
        if data_type is None:
            missing.append(code)
            continue
        path = root / 'quotes' / data_type / _market_of(code) / f'{code}.parquet'
        if not path.is_file():
            missing.append(code)
            continue
        frame = pl.read_parquet(path)
        before = frame.height
        fixed, counts = _repair_frame(frame, rules)
        changed = any(v for k, v in counts.items() if k != 'rows_dropped') or counts['rows_dropped']
        if not changed:
            continue
        if args.commit:
            _write_atomic(path, fixed)
        for key, value in counts.items():
            totals[key] += value
        per_code.append({'ts_code': code, 'rows_before': before,
                         'rows_after': fixed.height, **counts})

    quarantine = _build_quarantine(violations, targets, root, args.commit)
    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'violations_source': str(violations),
        'commit': args.commit,
        'codes_scanned': len(targets),
        'codes_missing': missing,
        'totals': dict(totals),
        'per_code': per_code,
        'quarantine': quarantine,
        'not_repaired': {
            'amount_dimension': '1996 年前单位口径 + 收盘价代理均价的固有误差，规则已按历史窗口与数量级带宽收窄；非 100 倍错位不修数据',
            'weekend_trade_date': '1996 年前周六为合法交易日，规则已加历史起点，不删行',
            'pre_close_mismatch': '仅修 pre_close<=0；非零前收差异属除权跳空/板块指数重编，不覆盖',
            'volume_invalid / required_columns': '源端 null，无法编造，写入隔离台账',
        },
    }
    report_path = (Path(args.report) if args.report else
                   PROJECT_ROOT / 'reports' / 'data_audit_2026-11-07'
                   / '行情修复报告_2026-11-07.json')
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                           encoding='utf-8')
    logger.info(f'行情修复{"已提交" if args.commit else "预演"}：'
                f'标的 {len(per_code)} 只，汇总 {dict(totals)}，'
                f'隔离 {quarantine["rows"]} 行，报告 {report_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
