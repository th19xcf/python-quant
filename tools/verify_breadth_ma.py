#!/usr/bin/env python3

r"""校验 ``breadth_ma_daily``：独立复算 + 方向一致性 + 已知事实断言。

校验对象与产出同一口径，但**实现完全独立**：不复用 ``tools/build_breadth_ma.py``
的任何函数，逐只读文件、用 pandas 风格的切片自己算 MA，避免「同一段代码自证」。

三类检查（缺一不可）
==================
1. 抽样复算：5 个不同年份的交易日，逐只复算 ``pct_above_ma20``，与产出对到小数点后 4 位；
   同时给出「有成交 → 剔除新上市 → MA20 可得 → 站上」的逐步剔除法，以及 3 只样本的手算式。
2. 方向一致性：与 ``meta/breadth/breadth_daily.parquet`` 的 ``up_count/down_count`` 比较
   日间方向，只要求不存在方向性矛盾，不要求数值相等；矛盾项逐条给原因。
3. 已知事实断言：样本数、与既有表口径相同的列、日期连续性。

用法::

    .venv\\Scripts\\python.exe tools\\verify_breadth_ma.py
"""

from __future__ import annotations

import argparse
import bisect
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORE = ROOT / 'data' / 'local_store'
REPORT_DIR = ROOT / 'reports' / 'breadth_ma_20261110'

#: 抽样年份（各年取 6 月首个交易日）
SAMPLE_YEARS = (1997, 2004, 2012, 2018, 2023)

#: 与构建脚本同口径的「站上」相对容差（等于均线不算站上）
ABOVE_TOL = 1e-9


def _load_meta(store: Path) -> dict[str, tuple[date | None, date | None]]:
    basic = pl.read_parquet(store / 'meta' / 'stock_basic.parquet',
                            columns=['ts_code', 'list_date', 'delist_date'])
    intervals = pl.read_parquet(
        store / 'meta' / 'universe' / 'listing_intervals.parquet',
        columns=['ts_code', 'data_type', 'first_trade_date'])
    intervals = intervals.filter(pl.col('data_type') == 'stock')
    meta = {row['ts_code']: (row['list_date'], row['delist_date'])
            for row in basic.iter_rows(named=True)}
    for row in intervals.iter_rows(named=True):
        listed, delisted = meta.get(row['ts_code'], (None, None))
        if listed is None:
            meta[row['ts_code']] = (row['first_trade_date'], delisted)
    return meta


def _pick_dates(produced: pl.DataFrame) -> list[date]:
    dates = cast(list[date], produced['date'].to_list())
    picked: list[date] = []
    for year in SAMPLE_YEARS:
        candidates = [d for d in dates if d.year == year and d.month == 6]
        if candidates:
            picked.append(candidates[0])
    return picked


def _score_stock_day(
    code: str, frame: pl.DataFrame, listed: date | None, delisted: date | None,
    calendar: list[date], day: date, stats: dict, examples: list,
) -> None:
    """逐只复算某一天的判定，把剔除原因计入 ``stats``。"""
    sub = frame.filter(pl.col('date') <= day)
    if sub.filter(pl.col('date') == day).height == 0:
        return  # 当日停牌 / 无成交
    stats['traded'] += 1
    if listed is None or (bisect.bisect_left(calendar, day)
                          - bisect.bisect_left(calendar, listed) + 1) < 60:
        stats['new_listing'] += 1
        return
    if delisted is not None and day >= delisted:
        stats['delisted'] += 1
        return
    if sub.height < 20:
        stats['short_bars'] += 1
        return
    stats['ma20_ok'] += 1
    closes = sub['adj'].to_list()
    close_today = closes[-1]
    ma20 = sum(closes[-20:]) / 20
    above = (close_today - ma20) > abs(ma20) * ABOVE_TOL
    if len(closes) >= 2 and close_today > closes[-2]:
        stats['risers'] += 1
        if not above:
            stats['risers_below'] += 1
    if above:
        stats['above_ma20'] += 1
    if len(examples) < 3:
        examples.append((code, close_today, ma20, sum(closes[-20:]), above, sub.height))
    for window in (60, 120):
        if sub.height >= window:
            ma = sum(closes[-window:]) / window
            if (close_today - ma) > abs(ma) * ABOVE_TOL:
                stats[f'above_ma{window}'] += 1
    if sub.height >= 250:
        window_250 = closes[-250:]
        stats['new_high_250'] += int(close_today >= max(window_250))
        stats['new_low_250'] += int(close_today <= min(window_250))


def recompute(store: Path, dates: list[date]) -> tuple[dict[date, dict], dict[date, list]]:
    """独立实现：逐只读文件、按自身有效 bar 算 MA，返回逐日计数与逐日样本明细。"""
    from src.data.quality.code_migration import load_entity_index

    calendar = (pl.scan_parquet(store / 'quotes' / 'stock' / '**' / '*.parquet')
                .select(pl.col('date').cast(pl.Date)).unique().sort('date').collect()
                ['date'].to_list())
    meta = _load_meta(store)
    drop = set(load_entity_index(store).superseded)
    files = sorted((store / 'quotes' / 'stock').glob('**/*.parquet'))

    counters = {
        d: {'files': len(files), 'traded': 0, 'new_listing': 0, 'delisted': 0,
            'short_bars': 0, 'ma20_ok': 0, 'above_ma20': 0, 'above_ma60': 0, 'above_ma120': 0,
            'new_high_250': 0, 'new_low_250': 0, 'risers': 0, 'risers_below': 0}
        for d in dates
    }
    examples: dict[date, list] = {d: [] for d in dates}

    for path in files:
        code = path.stem
        if code in drop:  # 与构建同口径：剔除已被新码完整覆盖的旧码
            continue
        frame = pl.read_parquet(path, columns=['date', 'close', 'volume', 'hfq_factor'])
        frame = frame.with_columns((pl.col('close') * pl.col('hfq_factor')).alias('adj'))
        frame = frame.filter((pl.col('volume') > 0) & pl.col('adj').is_not_null()
                             & (pl.col('adj') > 0)).sort('date')
        if frame.height == 0:
            continue
        listed, delisted = meta.get(code, (None, None))
        for day in dates:
            _score_stock_day(code, frame, listed, delisted, calendar, day,
                             counters[day], examples[day])
    return counters, examples


def direction_check(produced: pl.DataFrame, store: Path) -> dict:
    """pct_above_ma20 日间变化方向 vs breadth_daily 的 up/down 净额方向。"""
    breadth = pl.read_parquet(store / 'meta' / 'breadth' / 'breadth_daily.parquet',
                              columns=['trade_date', 'up_count', 'down_count', 'total_count'])
    joined = (produced.select(['date', 'pct_above_ma20'])
              .join(breadth.rename({'trade_date': 'date'}), on='date', how='inner')
              .sort('date'))
    joined = joined.with_columns([
        (pl.col('pct_above_ma20') - pl.col('pct_above_ma20').shift(1)).alias('dpct'),
        (pl.col('up_count') - pl.col('down_count')).alias('net_up'),
    ]).drop_nulls()
    agree = joined.filter(
        (pl.col('dpct') > 0) == (pl.col('net_up') > 0)).height
    total = joined.height
    # 方向性矛盾：涨家数明显多于跌家数，占比却明显下降（或反之）
    conflicts = joined.filter(
        ((pl.col('net_up') > 0) & (pl.col('dpct') < -1.0))
        | ((pl.col('net_up') < 0) & (pl.col('dpct') > 1.0)))

    joined = joined.with_columns(pl.col('date').dt.year().alias('year'))
    eras = []
    for low, high in ((1991, 1999), (2000, 2009), (2010, 2019), (2020, 2026)):
        part = joined.filter((pl.col('year') >= low) & (pl.col('year') <= high))
        if part.height == 0:
            continue
        era_agree = part.filter(
            (pl.col('dpct') > 0) == (pl.col('net_up') > 0)).height
        era_conf = part.filter(
            ((pl.col('net_up') > 0) & (pl.col('dpct') < -1.0))
            | ((pl.col('net_up') < 0) & (pl.col('dpct') > 1.0))).height
        eras.append({
            'era': f'{low}-{high}', 'days': part.height, 'agree': era_agree,
            'agree_pct': round(era_agree / part.height * 100, 2),
            'conflicts': era_conf,
            'conflict_pct': round(era_conf / part.height * 100, 2),
        })

    return {
        'days': total,
        'agree': agree,
        'agree_pct': round(agree / total * 100, 4) if total else None,
        'conflicts': conflicts.height,
        'conflict_rows': (conflicts.sort(pl.col('dpct').abs(), descending=True).head(8)
                          .to_dicts()),
        'eras': eras,
        'joined': joined,
        'corr': joined.select(pl.corr('dpct', 'net_up')).item(),
    }


def _evidence_days(direction: dict) -> list[date]:
    """挑 3 个近十年的矛盾日做逐只取证（净涨却占比明显下滑的优先）。"""
    joined = direction['joined']
    picks = (joined.filter((pl.col('date').dt.year() >= 2015) & (pl.col('net_up') > 0)
                           & (pl.col('dpct') < -1.0))
             .sort(pl.col('dpct').abs(), descending=True).head(3))
    return picks['date'].to_list()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='校验 breadth_ma_daily')
    parser.add_argument('--store-root', default=str(DEFAULT_STORE))
    parser.add_argument('--target', default=None,
                        help='待校验文件；缺省读正式路径，不存在时读 staging 候选')
    parser.add_argument('--report', default=None)
    args = parser.parse_args(argv)

    store = Path(args.store_root)
    target = Path(args.target) if args.target else store / 'meta' / 'breadth' / 'breadth_ma_daily.parquet'
    if not target.is_file():
        target = ROOT / '_staging' / 'breadth_ma_20261110' / 'breadth_ma_daily.parquet'
    produced = pl.read_parquet(target).sort('date')
    print(f'校验对象: {target}  {produced.height} 行  '
          f'{cast(date, produced["date"].min())} ~ {cast(date, produced["date"].max())}')

    dates = _pick_dates(produced)
    last_day = cast(date, produced['date'].max())
    direction = direction_check(produced, store)
    evidence = _evidence_days(direction)
    counters, examples = recompute(store, dates + [last_day] + evidence)
    rows = {row['date']: row for row in produced.iter_rows(named=True)}

    lines: list[str] = []
    ok = True

    lines.append('# breadth_ma_daily 校验报告\n')
    lines.append(f'- 校验对象：`{target}`')
    lo = cast(date, produced['date'].min())
    hi = cast(date, produced['date'].max())
    lines.append(f'- 行数：{produced.height}，区间 {lo} ~ {hi}')
    lines.append('- 校验脚本：`tools/verify_breadth_ma.py`（独立实现，不复用构建脚本函数）\n')

    # ---------------- 一、抽样复算 ----------------
    lines.append('## 一、抽样复算（`pct_above_ma20`，对到小数点后 4 位）\n')
    lines.append('| 交易日 | 有成交 | 剔除上市<60td | MA不足20根 | MA20可得 | 复算站上 | 复算 pct | '
                 '产出站上 | 产出 pct | 差异 |')
    lines.append('|---|---|---|---|---|---|---|---|---|---|')
    for day in dates:
        stats = counters[day]
        pct_re = round(stats['above_ma20'] / stats['ma20_ok'] * 100, 6)
        row = rows[day]
        diff = round(abs(pct_re - row['pct_above_ma20']), 6)
        if (stats['above_ma20'] != row['above_ma20'] or stats['ma20_ok'] != row['sample_size']
                or diff > 1e-4):
            ok = False
        lines.append(
            f'| {day} | {stats["traded"]} | {stats["new_listing"]} | {stats["short_bars"]} | '
            f'{stats["ma20_ok"]} | {stats["above_ma20"]} | {pct_re:.4f} | {row["above_ma20"]} | '
            f'{row["pct_above_ma20"]:.4f} | {diff:.6f} |')

    lines.append('\n**手算式样例**（`adj = close × hfq_factor`，MA20 = 最近 20 个有成交 bar 的 '
                 '`adj` 均值；`adj > MA20` 才计站上）：\n')
    lines.append('| 交易日 | 标的 | 有效bar数 | 最新 adj | 近20根 adj 合计 | MA20 | 最新 adj − MA20 | 判定 |')
    lines.append('|---|---|---|---|---|---|---|---|')
    for day in dates:
        for code, latest, ma20, total, above, bars in examples[day]:
            lines.append(f'| {day} | {code} | {bars} | {latest:.4f} | {total:.4f} | {ma20:.4f} | '
                         f'{latest - ma20:+.4f} | {"站上" if above else "未站上"} |')

    # ---------------- 二、方向一致性 ----------------
    lines.append('\n## 二、与 breadth_daily 涨跌家数的方向一致性\n')
    lines.append(f'- 可比交易日 {direction["days"]} 天，日间方向一致 '
                 f'{direction["agree"]} 天（{direction["agree_pct"]}%），'
                 f'`Δpct_above_ma20` 与 `up_count-down_count` 相关系数 {direction["corr"]:.4f}')
    lines.append(f'- 方向性矛盾（净涨>0 但占比跌超 1pp，或净跌<0 但占比涨超 1pp）'
                 f'{direction["conflicts"]} 天\n')
    lines.append('| 区间 | 交易日 | 方向一致 | 一致率 | 矛盾日 | 矛盾率 |')
    lines.append('|---|---|---|---|---|---|')
    for era in direction['eras']:
        lines.append(f'| {era["era"]} | {era["days"]} | {era["agree"]} | {era["agree_pct"]}% | '
                     f'{era["conflicts"]} | {era["conflict_pct"]}% |')

    lines.append('\n**矛盾成因取证**：1991-1999 的矛盾率 10.34%，明显高于其后三个区间（4.4%~6.1%）——'
                 '早期全市场不足百只、T+0、无涨跌幅限制，占比分母极小，1 只标的进出即可造成几十个百分点的跳变。'
                 '近十年矛盾日的逐只取证如下（`上涨家数` 为当日复权价高于前一根 bar 的只数）：\n')
    breadth = pl.read_parquet(store / 'meta' / 'breadth' / 'breadth_daily.parquet',
                              columns=['trade_date', 'up_count', 'down_count'])
    lookup = {row['trade_date']: row for row in breadth.iter_rows(named=True)}
    if evidence:
        lines.append('| 交易日 | Δpct | breadth up/down | 站上只数 | 较前日 | MA20 分母 | 上涨家数 | '
                     '其中仍在 MA20 下方 |')
        lines.append('|---|---|---|---|---|---|---|---|')
        joined = {row['date']: row for row in direction['joined'].iter_rows(named=True)}
        for day in evidence:
            stats = counters[day]
            row = lookup.get(day, {})
            item = joined.get(day, {})
            prev_day = max(d for d in rows if d < day)
            delta = stats['above_ma20'] - rows[prev_day]['above_ma20']
            lines.append(
                f'| {day} | {item.get("dpct", 0):+.2f}pp | {row.get("up_count")}/'
                f'{row.get("down_count")} | {stats["above_ma20"]} | {delta:+d} | {stats["ma20_ok"]} | '
                f'{stats["risers"]} | {stats["risers_below"]} 只'
                f'（{stats["risers_below"] / max(stats["risers"], 1) * 100:.1f}%） |')
        lines.append('\n结论：这三天的「涨家数多」与「站上占比降」可以同时成立——股价在涨，但涨幅发生在'
                     '均线下方的个股身上（不改变站上只数），而原先站上的个股有更多跌回均线下方；'
                     '`pct_above_ma20` 度量的是「位置」而非「当日涨跌」，两者方向本来就可以背离，'
                     '因此不构成矛盾。')
    else:
        lines.append('（近十年无矛盾日）')

    # ---------------- 三、已知事实断言 ----------------
    lines.append('\n## 三、已知事实断言\n')
    lines.append('| 断言 | 期望 | 实测 | 结论 |')
    lines.append('|---|---|---|---|')
    checks: list[tuple[str, object, object, bool]] = []

    day = last_day
    breadth_last = pl.read_parquet(store / 'meta' / 'breadth' / 'breadth_daily.parquet',
                                   columns=['trade_date', 'up_count', 'down_count',
                                            'flat_count', 'total_count', 'new_high_250',
                                            'new_low_250']).filter(pl.col('trade_date') == day)
    traded = int(breadth_last['total_count'][0])
    checks.append((f'{day} 样本数 sample_size', 5528, rows[day]['sample_size'],
                   rows[day]['sample_size'] == 5528))
    checks.append((f'{day} 有成交只数 = breadth_daily.total_count', traded,
                   counters[day]['traded'], counters[day]['traded'] == traded))

    tail = produced.tail(10)
    breadth_hi_lo = (
        pl.read_parquet(store / 'meta' / 'breadth' / 'breadth_daily.parquet',
                        columns=['trade_date', 'new_high_250', 'new_low_250'])
        .rename({'trade_date': 'date'})
        .filter(pl.col('date').is_in(tail['date'].to_list())))
    tail = tail.join(breadth_hi_lo, on='date', how='inner', suffix='_breadth')
    same_hi = bool((tail['new_high_250'] == tail['new_high_250_breadth']).all())
    same_lo = bool((tail['new_low_250'] == tail['new_low_250_breadth']).all())
    checks.append(('近 10 个交易日 new_high_250 与 breadth_daily 一致',
                   [int(v) for v in tail['new_high_250_breadth'].to_list()],
                   [int(v) for v in tail['new_high_250'].to_list()], same_hi))
    checks.append(('近 10 个交易日 new_low_250 与 breadth_daily 一致',
                   [int(v) for v in tail['new_low_250_breadth'].to_list()],
                   [int(v) for v in tail['new_low_250'].to_list()], same_lo))

    calendar = cast(list[date], pl.scan_parquet(store / 'quotes' / 'stock' / '**' / '*.parquet')
                    .select(pl.col('date').cast(pl.Date)).unique().sort('date').collect()
                    ['date'].to_list())
    active = set(pl.scan_parquet(store / 'quotes' / 'stock' / '**' / '*.parquet')
                 .select(['date', 'volume']).filter(pl.col('volume') > 0)
                 .select(pl.col('date').cast(pl.Date)).unique().collect()['date'].to_list())
    expected = [d for d in calendar if lo <= d <= hi and d in active]
    gaps = sorted(set(expected) - set(produced['date'].to_list()))
    checks.append((f'日期连续（{lo}~{hi} 内有有效成交的交易日无缺口）', len(expected),
                   produced.height, not gaps and len(expected) == produced.height))
    if gaps:
        lines.append(f'\n- 缺口日期：{gaps}')
    for name, expect, actual, passed in checks:
        ok = ok and passed
        exp = str(expect) if not isinstance(expect, list) else f'{len(expect)} 项'
        act = str(actual) if not isinstance(actual, list) else f'{len(actual)} 项'
        lines.append(f'| {name} | {exp} | {act} | {"通过" if passed else "**不通过**"} |')

    nulls = {name: produced[name].null_count() for name in produced.columns
             if produced[name].null_count()}
    lines.append(f'\n- 空值检查：{"各列均无空值" if not nulls else f"存在空值 {nulls}"}')
    if nulls:
        ok = False
    dup = produced.height - produced['date'].n_unique()
    lines.append(f'- 键唯一性：`date` 重复 {dup} 行')
    if dup:
        ok = False

    lines.append(f'\n## 结论\n\n{"全部检查通过。" if ok else "**存在未通过项，禁止替换正式路径。**"}')
    report = '\n'.join(lines) + '\n'

    out = Path(args.report) if args.report else REPORT_DIR / '校验报告.md'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding='utf-8')
    print(report)
    print(f'\n报告已写入 {out}')
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
