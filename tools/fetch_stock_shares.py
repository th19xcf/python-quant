#!/usr/bin/env python3

"""股本事实表采集入口：拉取 → 合并 → 覆盖率/抽查/交叉校验报告。

产物：``data/local_store/meta/factors/stock_shares.parquet``（单位：股）。
中间件（分块 part、进度、失败清单）在 ``tmp/stock_shares/``，可断点续跑。

用法::

    python tools/fetch_stock_shares.py --limit 20      # 小批试跑
    python tools/fetch_stock_shares.py                 # 全量采集 + 报告
    python tools/fetch_stock_shares.py --assemble-only # 只合并已有分块
    python tools/fetch_stock_shares.py --report-only   # 只出报告，不联网
"""
# ruff: noqa: T201  # 采集脚本面向终端输出

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402
from sqlalchemy import text  # noqa: E402

from src.data.local_store.share_capital import (  # noqa: E402
    assemble_shares,
    collect_shares,
    coverage_report,
    default_work_dir,
    fetch_cninfo_history,
    fetch_current_info,
    fetch_market_snapshot,
    load_dividend_events,
    read_shares,
    shares_as_of,
    shares_frame,
)
from src.data.local_store.store import (  # noqa: E402
    get_local_quote_store,
    resolve_store_root,
)
from src.data.sources import baostock_schema  # noqa: E402
from src.database.db_manager import DatabaseManager  # noqa: E402
from src.utils.code_converter import to_baostock_code  # noqa: E402
from src.utils.config import get_config  # noqa: E402

#: 抽查标的：主板 / 创业板 / 北交所各一只（北交所优先取 920 段）。
SAMPLE_CODES = ('600000.SH', '300750.SZ', '920001.BJ')


def _float(value: Any) -> float:
    """把 polars 聚合返回值（Any 联合类型）收敛成 float，便于格式化与比较。"""
    return float(value)


def load_universe() -> tuple[list[str], dict[str, date], str]:
    """从 stock_basic 取 list_date 非空的标的（5899→5811 口径），DB 不可用则退本地行情。"""
    cfg = get_config()
    try:
        dbm = DatabaseManager(cfg)
        dbm.connect()
        session = dbm.get_session()
        rows = session.execute(text(
            "SELECT ts_code, list_date FROM stock_basic "
            'WHERE list_date IS NOT NULL ORDER BY ts_code')).all()
        if rows:
            return ([row[0] for row in rows],
                    {row[0]: row[1] for row in rows if row[1] is not None},
                    'stock_basic(list_date 非空)')
    except Exception as exc:  # noqa: BLE001 - 只读库偶发连不上，退回本地标的
        print(f'[warn] 读取 stock_basic 失败，改用本地行情标的: {exc}')
    store = get_local_quote_store(cfg)
    codes = store.codes('stock')
    return codes, {}, 'local_quotes(回退)'


def _print_table(rows: list[dict[str, object]]) -> None:
    if not rows:
        print('  （无数据）')
        return
    headers = list(rows[0].keys())
    widths = {key: max(len(str(key)), *(len(str(row[key])) for row in rows))
              for key in headers}
    print('  ' + '  '.join(str(key).ljust(widths[key]) for key in headers))
    print('  ' + '  '.join('-' * widths[key] for key in headers))
    for row in rows:
        print('  ' + '  '.join(str(row[key]).ljust(widths[key]) for key in headers))


def report_coverage(universe: list[str], root: Path) -> list[str]:
    """覆盖率 + 缺失清单（落 tmp/stock_shares/missing.csv）。"""
    payload = coverage_report(universe, root)
    missing = payload['missing']
    print(f'\n[1] 覆盖率：{payload["covered"]}/{payload["universe"]} 只 '
          f'({payload["covered"] / max(1, payload["universe"]):.2%})，'
          f'事实表 {payload["rows"]} 行')
    if missing:
        path = default_work_dir() / 'missing.csv'
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({'ts_code': missing}).write_csv(path)
        print(f'  缺失 {len(missing)} 只，清单: {path}')
        print(f'  前 10 只缺失: {missing[:10]}')
    return missing


def report_sources(root: Path) -> None:
    """各源占比与时间深度。"""
    frame = read_shares(root=root)
    if frame is None or frame.is_empty():
        return
    grouped = frame.group_by('source').agg(
        pl.len().alias('行数'),
        pl.col('ts_code').n_unique().alias('标的数'),
        pl.col('effective_date').min().alias('最早生效日'),
        pl.col('effective_date').max().alias('最新生效日'),
    ).sort('标的数', descending=True)
    print('\n[2] 来源占比')
    _print_table(grouped.to_dicts())
    print(f'  自由流通股本非空行数: '
          f'{frame.filter(pl.col("free_float_share").is_not_null()).height}')


def check_samples(root: Path, *, today: date) -> None:
    """抽查主板/创业板/北交所各一只，本地值 vs 东财快照 vs 巨潮最新（单位：股）。"""
    print('\n[3] 抽查（主板/创业板/北交所：本地 as_of 今日 vs 两个源端）')
    rows: list[dict[str, object]] = []
    for code in SAMPLE_CODES:
        snap = shares_as_of(code, today, root)
        note = ''
        try:
            raw = fetch_current_info(code)
        except Exception as exc:  # noqa: BLE001
            raw = None
            note = f'东财快照失败: {exc}'[:50]
        total, float_share = raw if raw else (None, None)
        try:
            cn = fetch_cninfo_history(code, fetched_at=datetime.now(),
                                      today=today, throttle=0.0)
            cn_last = cn.tail(1).to_dicts()[0] if not cn.is_empty() else None
        except Exception as exc:  # noqa: BLE001
            cn_last = None
            note = (note + f' 巨潮失败: {exc}'[:80]).strip()
        cn_total = cn_last['total_share'] if cn_last else None
        cn_float = cn_last['float_share'] if cn_last else None
        total_err = (abs(snap.total_share - total) / total) if (snap and total) else None
        cn_err = (abs(snap.total_share - cn_total) / cn_total
                  if (snap and cn_total) else None)
        rows.append({
            'ts_code': code,
            '本地总股本': snap.total_share if snap else 'NULL',
            '东财总股本': total or 'NULL',
            '东财偏差': f'{total_err:.6%}' if total_err is not None else '-',
            '巨潮总股本': cn_total or 'NULL',
            '巨潮偏差': f'{cn_err:.6%}' if cn_err is not None else '-',
            '本地流通股本': (snap.float_share if snap and snap.float_share else 'NULL'),
            '东财流通股本': float_share or 'NULL',
            '巨潮流通股本': cn_float or 'NULL',
            '备注': note,
        })
    _print_table(rows)


def check_snapshot_bulk(root: Path) -> None:
    """全量末行 vs 全市场快照（市值÷最新价），量化整表与当前值的一致性。"""
    print('\n[3b] 全量末行 vs 全市场快照（总市值/流通市值 ÷ 最新价）')
    snapshot = fetch_market_snapshot()
    frame = read_shares(root=root)
    if frame is None or frame.is_empty() or not snapshot:
        print('  （事实表或快照为空）')
        return
    last = (frame.sort('effective_date').group_by('ts_code').agg(
        pl.col('total_share').last().alias('total_share'),
        pl.col('float_share').last().alias('float_share'),
        pl.col('effective_date').last().alias('last_date'))).to_dicts()
    total_err: list[float] = []
    float_err: list[float] = []
    bad: list[tuple[float, str, int, int]] = []
    for row in last:
        snap = snapshot.get(row['ts_code'])
        if not snap or not snap[0]:
            continue
        error = abs(row['total_share'] - snap[0]) / snap[0]
        total_err.append(error)
        if error > 0.02:
            bad.append((error, row['ts_code'], row['total_share'], snap[0]))
        if snap[1] and row['float_share']:
            float_err.append(abs(row['float_share'] - snap[1]) / snap[1])
    for label, values in (('总股本', total_err), ('流通股本', float_err)):
        if not values:
            continue
        stats = pl.Series(values)
        print(f'  {label}: n={len(values)}，中位数 {_float(stats.median()):.4%}，'
              f'p90 {_float(stats.quantile(0.9)):.4%}，'
              f'偏差>2% {sum(1 for v in values if v > 0.02)} 只')
    bad.sort(reverse=True)
    if bad:
        _print_table([
            {'ts_code': code, '末行总股本': local, '快照推算': snap_total,
             '偏差': f'{error:.2%}'}
            for error, code, local, snap_total in bad[:8]])


def check_dividend_chain(list_dates: dict[str, date], root: Path) -> None:
    """内部一致性：上市日股本 × 累计送转 是否能回到当前股本。

    起点取「上市日及之后的第一行」——上市前的股份沿革（含 em 的早期口径）
    不参与交易，拿它做基线会放大偏差。
    """
    dividends = load_dividend_events(root)
    frame = read_shares(root=root)
    print('\n[5] 送转连乘一致性（上市日股本 × Π(1+share_div) → 当前股本）')
    if frame is None or frame.is_empty():
        print('  （事实表为空）')
        return
    checked = 0
    passed = 0
    errors: list[float] = []
    worst: list[tuple[float, str, float, float]] = []
    derived = 0
    stitched = 0
    step_total = 0
    step_explained = 0
    unexplained_codes: dict[str, int] = {}
    for keys, sub in frame.partition_by('ts_code', as_dict=True).items():
        code = keys if isinstance(keys, str) else keys[0]
        sub = sub.sort('effective_date')
        if sub['source'].n_unique() > 1:
            stitched += 1
        if sub['source'][0].startswith('derived'):
            derived += 1
            continue
        start = list_dates.get(code)
        if start is not None:
            sub = sub.filter(pl.col('effective_date') >= start)
        if sub.is_empty():
            continue
        steps = sub.select(['effective_date', 'total_share']).to_dicts()
        first_date = steps[0]['effective_date']
        first_total = steps[0]['total_share']
        current_total = steps[-1]['total_share']
        # 逐步归因：相邻两行的股本比例能否被区间内的送转比例解释。
        events = dividends.get(code, ())
        for prev, cur in zip(steps[:-1], steps[1:], strict=False):
            step_total += 1
            ratio = cur['total_share'] / prev['total_share']
            hit = any(
                abs((1.0 + share_div) - ratio) / ratio <= 0.005
                for day, share_div in events
                if prev['effective_date'] < day <= cur['effective_date'])
            if hit:
                step_explained += 1
            else:
                unexplained_codes[code] = unexplained_codes.get(code, 0) + 1
        growth = 1.0
        for day, share_div in events:
            if day > first_date:
                growth *= 1.0 + share_div
        if growth <= 1.0 or current_total <= 0:
            continue
        checked += 1
        implied = first_total * growth
        error = abs(implied - current_total) / current_total
        errors.append(error)
        if error <= 0.02:
            passed += 1
        worst.append((error, code, implied, current_total))
    print(f'  拼接序列（巨潮前缀+东财后缀）{stitched} 只；倒推序列 {derived} 只单列')
    print(f'  闭环校验：{checked} 只，连乘能回到当前值（≤2%）{passed} 只 '
          f'({passed / max(1, checked):.1%})')
    if errors:
        stats = pl.Series(errors)
        print(f'  闭环偏差分布：中位数 {_float(stats.median()):.2%}，'
              f'p90 {_float(stats.quantile(0.9)):.2%}')
    print(f'  逐步归因：共 {step_total} 个股本变动步，'
          f'可由送转比例解释 {step_explained} 步 '
          f'({step_explained / max(1, step_total):.1%})')
    print('  未解释步 = 增发/配股/回购注销/可转债转股等，分红表不含这些比例；'
          '因此「纯送转倒推」在全历史上必然低估，越往前误差越大，'
          '本项目仅在双源都缺失时才用它兜底。')
    top_unexplained = sorted(unexplained_codes.items(), key=lambda item: -item[1])[:5]
    if top_unexplained:
        print(f'  未解释步最多的标的: {top_unexplained}')
    worst.sort(reverse=True)
    _print_table([
        {'ts_code': code, '连乘推算': int(implied), '实际当前': int(current),
         '相对偏差': f'{error:.2%}'}
        for error, code, implied, current in worst[:8]])


def _turnover_pairs(shares_frame_code: pl.DataFrame, quote: pl.DataFrame,
                    turnover: pl.DataFrame) -> pl.DataFrame:
    """按日期对齐行情/换手率/适用流通股本，返回含理论与实际换手率的帧。"""
    merged = (quote.select(['date', 'volume'])
              .join(turnover.select(['trade_date', 'turn']),
                    left_on='date', right_on='trade_date', how='inner'))
    if merged.is_empty():
        return merged
    return (merged
            .sort('date')
            .join_asof(shares_frame_code.select(['effective_date', 'float_share']),
                       left_on='date', right_on='effective_date',
                       strategy='backward'))


def check_turnover(root: Path, *, n_codes: int = 20, n_days: int = 20) -> None:
    """换手率交叉校验：turn ≈ volume(手)×100 / float_share（turn 为百分数）。"""
    print('\n[4] 换手率交叉校验（turn ≈ volume×100 / float_share）')
    turnover_path = root / 'factors' / 'stock_turnover.parquet'
    if not turnover_path.exists():
        print('  待对方完成后补做：stock_turnover.parquet 尚不存在')
        return
    turnover = pl.read_parquet(turnover_path)
    shares = read_shares(root=root)
    if shares is None or shares.is_empty() or turnover.is_empty():
        print('  （股本表或换手率表为空）')
        return
    usable = sorted(set(turnover['ts_code'].unique().to_list())
                    & set(shares['ts_code'].unique().to_list()))
    print(f'  turnover 覆盖 {turnover["ts_code"].n_unique()} 只，'
          f'与股本表交集 {len(usable)} 只；按可用标的全部比对（目标 20 只）')
    if not usable:
        print('  无交集，待对方回补后补做')
        return
    store = get_local_quote_store(get_config())
    deviations: list[float] = []
    per_code: list[dict[str, object]] = []
    for code in usable[:n_codes]:
        quote = store.read_file('stock', code)
        series = shares_frame(code, root)
        if quote is None or series is None:
            continue
        code_turn = (turnover.filter(pl.col('ts_code') == code)
                     .sort('trade_date').tail(n_days))
        joined = _turnover_pairs(series, quote, code_turn)
        joined = joined.filter(
            pl.col('float_share').is_not_null() & (pl.col('float_share') > 0)
            & (pl.col('turn') > 0) & (pl.col('volume') > 0))
        if joined.is_empty():
            continue
        joined = joined.with_columns(
            ((pl.col('volume') * 100 / pl.col('float_share')) * 100)
            .alias('turn_theory'))
        error = ((joined['turn_theory'] - joined['turn']).abs() / joined['turn'])
        deviations.extend(error.to_list())
        per_code.append({
            'ts_code': code,
            '样本日数': joined.height,
            'turn 中位数': f'{_float(joined["turn"].median()):.4f}%',
            '理论中位数': f'{_float(joined["turn_theory"].median()):.4f}%',
            '偏差中位数': f'{_float(error.median()):.2%}',
            '偏差最大': f'{_float(error.max()):.2%}',
        })
    _print_table(per_code)
    if deviations:
        stats = pl.Series(deviations)
        print(f'  总体偏差：n={len(deviations)}，中位数 {_float(stats.median()):.3%}，'
              f'p90 {_float(stats.quantile(0.9)):.3%}，最大 {_float(stats.max()):.3%}')
        within = sum(1 for value in deviations if value <= 0.05)
        print(f'  偏差 ≤5% 的样本占比 {within / len(deviations):.1%}')


def check_baostock_float(universe: list[str], root: Path, *, n_codes: int = 20,
                         n_days: int = 20, today: date | None = None) -> None:
    """补充校验：baostock ``turn`` 反推流通股本，与本表流通股本比对。

    ``implied_float = volume / (turn/100) = volume × 100 / turn``，
    取区间中位数，规避个别异常成交日。
    """
    print('\n[4b] baostock 反推流通股本交叉校验（独立于 turnover 表）')
    try:
        import baostock as bs  # noqa: PLC0415
    except ImportError:
        print('  baostock 未安装，跳过')
        return
    if bs.login().error_code != '0':
        print('  baostock 登录失败，跳过')
        return
    end = (today or date.today())
    start = end - timedelta(days=n_days * 3)
    codes = [code for code in universe if not code.endswith('.BJ')][:n_codes * 3]
    picked: list[str] = []
    step = max(1, len(codes) // n_codes) if codes else 1
    picked = codes[::step][:n_codes]
    rows: list[dict[str, object]] = []
    errors: list[float] = []
    for code in picked:
        snap = shares_as_of(code, end, root)
        if snap is None or not snap.float_share:
            continue
        rs = bs.query_history_k_data_plus(
            to_baostock_code(code), 'date,code,volume,turn',
            start_date=start.isoformat(), end_date=end.isoformat(),
            frequency='d', adjustflag='3')
        if rs is None or rs.error_code != '0':
            continue
        frame = pl.from_pandas(baostock_schema.result_to_frame(rs))
        if frame.is_empty():
            continue
        frame = frame.with_columns(
            [pl.col('volume').cast(pl.Float64, strict=False),
             pl.col('turn').cast(pl.Float64, strict=False)])
        frame = frame.filter((pl.col('turn') > 0) & (pl.col('volume') > 0))
        if frame.is_empty():
            continue
        implied = _float((frame['volume'] * 100 / frame['turn']).median())
        if not implied:
            continue
        error = abs(implied - snap.float_share) / snap.float_share
        errors.append(error)
        rows.append({'ts_code': code,
                     '本表流通股本': snap.float_share,
                     'baostock 反推': int(implied),
                     '偏差': f'{error:.2%}'})
    bs.logout()
    _print_table(rows[:12])
    if errors:
        stats = pl.Series(errors)
        print(f'  样本 {len(errors)} 只，偏差中位数 {_float(stats.median()):.3%}，'
              f'最大 {_float(stats.max()):.3%}，≤2% 占比 '
              f'{sum(1 for value in errors if value <= 0.02) / len(errors):.1%}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='股本事实表采集与校验')
    parser.add_argument('--limit', type=int, default=None, help='只采前 N 只（试跑）')
    parser.add_argument('--workers', type=int, default=4, help='并发请求数（默认 4）')
    parser.add_argument('--throttle', type=float, default=0.15,
                        help='每请求后的节流秒数（默认 0.15）')
    parser.add_argument('--chunk-size', type=int, default=200, help='每块标的数')
    parser.add_argument('--refresh', action='store_true', help='忽略进度，整批重采')
    parser.add_argument('--assemble-only', action='store_true', help='只合并已有分块')
    parser.add_argument('--report-only', action='store_true', help='只出报告，不联网')
    parser.add_argument('--skip-snapshot', action='store_true',
                        help='不拉全市场快照（选源校验退化，省 ~84s）')
    parser.add_argument('--skip-verify', action='store_true', help='跳过联网抽查/校验')
    args = parser.parse_args(argv)

    cfg = get_config()
    root = resolve_store_root(cfg)
    work = default_work_dir()
    print(f'[cfg] 列存根目录: {root}')
    print(f'[cfg] 中间件目录: {work}')

    if args.report_only:
        universe, list_dates, source = load_universe()
        print(f'\n[universe] {len(universe)} 只（{source}）')
        report_coverage(universe, root)
        report_sources(root)
        if not args.skip_verify:
            today = date.today()
            check_samples(root, today=today)
            check_snapshot_bulk(root)
            check_turnover(root)
            check_dividend_chain(list_dates, root)
            check_baostock_float(universe, root, today=today)
        return 0

    started = datetime.now()
    if not args.assemble_only:
        universe, list_dates, source = load_universe()
        print(f'\n[universe] {len(universe)} 只（{source}）')
        snapshot = {} if args.skip_snapshot else fetch_market_snapshot(cache_dir=work)
        if snapshot:
            print(f'[snapshot] 全市场快照覆盖 {len(snapshot)} 只，用于选源校验')
        stats = collect_shares(
            universe, root=root, work_dir=work, snapshot=snapshot,
            list_dates=list_dates, workers=args.workers,
            chunk_size=args.chunk_size, throttle=args.throttle,
            limit=args.limit, refresh=args.refresh)
        print(f'\n[collect] 本轮目标 {stats["target"]} 只，ok {stats["ok"]}，'
              f'derived {stats["derived"]}，empty {stats["empty"]}，'
              f'failed {stats["failed"]}，行 {stats["rows"]}，'
              f'耗时 {stats["elapsed_s"]}s（续跑只统计本轮实际抓取的块）')
        if stats['target']:
            print(f'  失败率 {stats["failed"] / stats["target"]:.2%}，'
                  f'失败清单: {stats["failed_path"]}')
        for item in stats['failed_list'][:8]:
            print(f'  - {item["ts_code"]}: {item["error"]}')
    else:
        universe, list_dates, _source = load_universe()

    assembled = assemble_shares(root, work)
    print(f'\n[assemble] {assembled["path"]}：{assembled["rows"]} 行 / '
          f'{assembled["codes"]} 只 / {assembled["parts"]} 个分块')
    print(f'[time] 本轮总耗时 {(datetime.now() - started).total_seconds():.1f}s')

    report_coverage(universe, root)
    report_sources(root)
    if not args.skip_verify:
        today = date.today()
        check_samples(root, today=today)
        check_snapshot_bulk(root)
        check_turnover(root)
        check_dividend_chain(list_dates, root)
        check_baostock_float(universe, root, today=today)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
