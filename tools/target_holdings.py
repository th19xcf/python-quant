#!/usr/bin/env python3
"""v3 每日目标持仓清单：只出清单与台账，不下单、不改引擎、不改数据。

口径（与 ``tools/factor_weighting.py`` 的 v3 基准完全一致，不新造规则）
------------------------------------------------------------------
- 选股：全市场 A 股，月末横截面剔除 60 日波动率最高 20% + 剔除上市不满
  250 个交易日，其余全部保留；直接复用 ``factor_lowvol_quantile`` /
  ``factor_lowvol_variants`` / ``factor_exclusion_list`` 的既有实现。
- 权重：等权，合计 0.99（1% 现金缓冲），本金默认 10 亿（v3 容量上限）。
- 执行语义：月末 T 日收盘出信号、T+1 开盘成交、月频再平衡；本脚本只把
  「最新月末信号」翻译成某交易日的目标持仓，不做撮合。
- 参考价：清单用**交易日原始收盘价**（``raw_close``，未复权，实际可成交价）
  折算整手股数；信号本身仍用 hfq 收盘（与回测同源）。

产出
----
``reports/target_holdings/目标持仓_<交易日>.csv``
    代码/名称/目标权重/目标金额/参考价/参考价日期/目标股数/变动类型/变动金额。
``reports/target_holdings/目标持仓台账.csv``
    历次清单的目标权重与差异，按交易日追加（同一交易日重跑覆盖该日行，
    不动其它日期），供下期比较。

用法
----
    .venv\\Scripts\\python.exe tools/target_holdings.py --date 2026-11-13
    .venv\\Scripts\\python.exe tools/target_holdings.py --capital 500000000
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import baseline_pv_val_equal_weight as base
import factor_exclusion_list as e
import factor_lowvol_quantile as f
import factor_lowvol_variants as v
import factor_weighting as w
from loguru import logger

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
LOT_SIZE = 100
DEFAULT_CAPITAL = 1e9
BUDGET = 1.0 - f.CASH_BUFFER
STOCK_BASIC = ROOT / 'data/local_store/meta/stock_basic.parquet'
OUT_DIR = ROOT / 'reports/target_holdings'

LIST_COLUMNS = ['代码', '名称', '目标权重', '目标金额', '参考价', '参考价日期',
                '目标股数', '变动类型', '变动金额']
LEDGER_COLUMNS = ['交易日', '信号日', '资金规模', '代码', '名称', '目标权重',
                  '目标金额', '参考价', '目标股数', '变动类型', '变动金额']
CHANGE_ORDER = {'清仓': 0, '减仓': 1, '新建': 2, '加仓': 3, '不变': 4}


def month_end_signal(calendar: List[date], day: date) -> date:
    """day 所在月份之前最近一个月末交易日（该日收盘信号已在 day 前开盘成交）。

    月频信号只在月末收盘产生、次一交易日开盘成交，因此某交易日的目标持仓
    由「上一个完整月份」的月末信号决定；day 本身是月末时，其收盘信号要到
    下一交易日开盘才成交，当日仍沿用上一个月末。
    """
    months: Dict[Tuple[int, int], date] = {}
    for item in calendar:
        if item >= day:
            break
        months[(item.year, item.month)] = item
    prior = [value for key, value in months.items()
             if key < (day.year, day.month)]
    if not prior:
        raise RuntimeError(f'交易日历在 {day} 之前没有完整月份')
    return max(prior)


def latest_local_day() -> date:
    """本地行情覆盖到的最新交易日（只读 ``daily_index`` 元数据）。"""
    import polars as pl  # noqa: PLC0415

    path = ROOT / 'data/local_store/meta/daily_index.parquet'
    frame = pl.read_parquet(path, columns=['data_type', 'end']).filter(
        pl.col('data_type') == 'stock')
    if frame.is_empty():
        raise SystemExit(f'daily_index 中无 stock 覆盖记录：{path}')
    return frame['end'].max()


def load_names() -> Dict[str, str]:
    import polars as pl  # noqa: PLC0415

    frame = pl.read_parquet(STOCK_BASIC, columns=['ts_code', 'name'])
    return {row['ts_code']: (row['name'] or '') for row in frame.to_dicts()}


def reference_prices(data: Dict[str, object], day: date) -> Dict[str, Tuple[float, date]]:
    """各标的 ≤ day 的最近一条原始收盘价（停牌标的回溯到最近成交日）。"""
    want = np.datetime64(day, 'D')
    out: Dict[str, Tuple[float, date]] = {}
    for code, df in data.items():
        dates = df['date'].to_numpy()
        k = int(np.searchsorted(dates, want, side='right')) - 1
        if k < 0:
            continue
        prices = df['raw_close'].to_numpy()
        if not np.isfinite(prices[k]) or prices[k] <= 0:
            continue
        out[code] = (float(prices[k]), date.fromisoformat(str(dates[k])[:10]))
    return out


def read_ledger(path: Path) -> List[Dict[str, str]]:
    if not path.is_file():
        return []
    with path.open('r', encoding='utf-8-sig', newline='') as fh:
        return list(csv.DictReader(fh))


def write_csv_atomic(path: Path, columns: List[str], rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w', encoding='utf-8-sig', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator='\n')
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in columns})
    os.replace(tmp, path)


def previous_targets(ledger: List[Dict[str, str]], trade_day: str,
                     ) -> Tuple[str | None, Dict[str, Tuple[float, float, str]]]:
    """上一份清单（严格早于本交易日）。返回 (上一交易日, {代码: (权重, 金额, 名称)})。"""
    days = sorted({row['交易日'] for row in ledger if row['交易日'] < trade_day})
    if not days:
        return None, {}
    prev_day = days[-1]
    prev: Dict[str, Tuple[float, float, str]] = {}
    for row in ledger:
        if row['交易日'] != prev_day:
            continue
        prev[row['代码']] = (float(row['目标权重']), float(row['目标金额']), row['名称'])
    return prev_day, prev


def classify(weight: float, old_weight: float) -> str:
    if old_weight == 0.0:
        return '新建' if weight > 0 else '不变'
    if weight == 0.0:
        return '清仓'
    if abs(weight - old_weight) <= 1e-12 * max(1.0, abs(old_weight)):
        return '不变'
    return '加仓' if weight > old_weight else '减仓'


def main() -> int:
    parser = argparse.ArgumentParser(description='v3 每日目标持仓清单（只出清单与台账）')
    parser.add_argument('--date', default=None,
                        help='交易日 YYYY-MM-DD（缺省：本地行情最新交易日）')
    parser.add_argument('--capital', type=float, default=DEFAULT_CAPITAL,
                        help='资金规模（元，缺省 10 亿，v3 容量上限）')
    parser.add_argument('--out-dir', default=str(OUT_DIR))
    parser.add_argument('--ledger', default=None,
                        help='台账路径（缺省 <out-dir>/目标持仓台账.csv）')
    args = parser.parse_args()

    if args.capital <= 0:
        raise SystemExit('资金规模必须为正数')

    # 先按 A 股交易日历判定交易日与最新可用日，避免非交易日白跑一次全量加载。
    all_days = e.load_trading_days()
    latest_day = min(latest_local_day(), all_days[-1])
    trade_day = date.fromisoformat(args.date) if args.date else latest_day
    if trade_day not in set(all_days):
        print(f'NO_LIST {trade_day} 非交易日（不在 A 股交易日历内），不生成清单',
              flush=True)
        return 2
    if trade_day > latest_day:
        print(f'NO_LIST {trade_day} 行情未覆盖（本地最新交易日 {latest_day}），不生成清单',
              flush=True)
        return 2

    data = base.load_universe(WARMUP_START, trade_day)
    data, violations = base.drop_contract_violations(data)
    calendar, _month_ends, _prev_day = f.build_calendar(data)
    if not calendar:
        raise SystemExit('行情日历为空，无法生成清单')

    signal_day = month_end_signal(calendar, trade_day)
    if signal_day < FIRST_SIGNAL:
        raise SystemExit(f'信号日 {signal_day} 早于首个可用信号 {FIRST_SIGNAL}')

    panel = f.compute_vol_panel(data, [signal_day])
    members, short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, [signal_day])
    if signal_day not in members or signal_day not in quoted:
        raise SystemExit(f'信号日 {signal_day} 横截面不足（{short}），无法生成清单')

    quoted_set = set(quoted[signal_day])
    high_vol = set(members[signal_day][4] or ()) & quoted_set
    list_dates = e.load_list_dates(data)
    new_listing = w.new_listing_set(
        data, sorted(quoted_set), list_dates, all_days, signal_day)
    codes = sorted(quoted_set - high_vol - new_listing)
    if not codes:
        raise SystemExit(f'信号日 {signal_day} 剔除后成分为空')

    names = load_names()
    prices = reference_prices(data, trade_day)
    weight = BUDGET / len(codes)
    trade_str = trade_day.isoformat()
    signal_str = signal_day.isoformat()

    ledger_path = Path(args.ledger) if args.ledger else Path(args.out_dir) / '目标持仓台账.csv'
    ledger = read_ledger(ledger_path)
    prev_day, prev = previous_targets(ledger, trade_str)

    rows: List[Dict[str, object]] = []
    missing_price = 0
    for code in codes:
        amount = round(weight * args.capital, 2)
        price, price_day = prices.get(code, (0.0, trade_day))
        if price <= 0:
            missing_price += 1
            shares = 0
        else:
            shares = int(math.floor(amount / (price * LOT_SIZE))) * LOT_SIZE
        old_weight, old_amount, old_name = prev.get(code, (0.0, 0.0, ''))
        change = classify(weight, old_weight)
        rows.append({
            '代码': code,
            '名称': names.get(code, '') or old_name,
            '目标权重': f'{weight:.10f}',
            '目标金额': f'{amount:.2f}',
            '参考价': f'{price:.4f}',
            '参考价日期': price_day.isoformat(),
            '目标股数': shares,
            '变动类型': change,
            '变动金额': f'{amount - old_amount:.2f}',
        })

    for code, (old_weight, old_amount, old_name) in prev.items():
        if code in quoted_set:
            continue
        if not any(row['代码'] == code for row in rows):
            rows.append({
                '代码': code,
                '名称': old_name,
                '目标权重': '0.0000000000',
                '目标金额': '0.00',
                '参考价': '0.0000',
                '参考价日期': trade_str,
                '目标股数': 0,
                '变动类型': '清仓',
                '变动金额': f'{-old_amount:.2f}',
            })

    rows.sort(key=lambda row: (CHANGE_ORDER[row['变动类型']], row['代码']))
    list_path = Path(args.out_dir) / f'目标持仓_{trade_str}.csv'
    write_csv_atomic(list_path, LIST_COLUMNS, rows)

    ledger_rows: List[Dict[str, object]] = [
        {
            '交易日': trade_str, '信号日': signal_str,
            '资金规模': f'{args.capital:.2f}',
            '代码': row['代码'], '名称': row['名称'],
            '目标权重': row['目标权重'], '目标金额': row['目标金额'],
            '参考价': row['参考价'], '目标股数': row['目标股数'],
            '变动类型': row['变动类型'], '变动金额': row['变动金额'],
        }
        for row in rows
    ]
    kept = [row for row in ledger if row['交易日'] != trade_str]
    merged = [dict(row) for row in kept] + ledger_rows
    merged.sort(key=lambda row: (row['交易日'], str(row['代码'])))
    write_csv_atomic(ledger_path, LEDGER_COLUMNS, merged)

    counts = dict.fromkeys(CHANGE_ORDER, 0)
    for row in rows:
        counts[row['变动类型']] += 1
    total_amount = sum(float(row['目标金额']) for row in rows)
    traded_amount = sum(float(row['目标金额']) for row in rows if row['目标股数'] > 0)
    turnover = sum(abs(float(row['变动金额'])) for row in rows)
    logger.debug('剔除明细：quoted {} / 高波 {} / 次新 {} / 成分 {}',
                 len(quoted_set), len(high_vol), len(new_listing), len(codes))
    if violations:
        logger.debug('契约预筛剔除 {} 只', len(violations))

    print(f'HOLDINGS trade_date={trade_str} signal_date={signal_str} '
          f'capital={args.capital:.0f} names={len(codes)} '
          f'target_amount_sum={total_amount:.2f} traded_amount_sum={traded_amount:.2f} '
          f'turnover_amount={turnover:.2f} prev_date={prev_day or "无"}', flush=True)
    print('CHANGES ' + ' '.join(f'{key}={counts[key]}' for key in CHANGE_ORDER)
          + f' missing_price={missing_price}', flush=True)
    print(f'OUT {list_path}', flush=True)
    print(f'LEDGER {ledger_path}', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
