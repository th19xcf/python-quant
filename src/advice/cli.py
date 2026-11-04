#!/usr/bin/env python3

"""每日推荐命令行入口。

用法：
    python -m src.advice --as-of 2026-10-19 --equity 1000000
    python -m src.advice --replay --start 2024-11-01 --end 2026-10-19
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
from loguru import logger

from src.advice.config import (
    OBSERVE_ONLY_TIERS,
    AdviceConfig,
    default_config,
    default_output_tiers,
)
from src.advice.daily_store import list_daily_snapshots, load_daily_snapshot
from src.advice.pipeline import AdviceEngine
from src.advice.replay import run_replay
from src.utils.config import get_config


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _write_snapshot(result: dict, out_dir: Path, equity: float) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / 'latest.json'
    payload = {
        'trade_date': result['trade_date'],
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'equity': equity,
        'is_sample': False,
        'tiers': result['tiers'],
        'items': result['items'],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding='utf-8')
    return path


def mark_observe_only(tiers: dict[str, dict]) -> dict[str, dict]:
    """给「仅观察」档位附加 ``observe_only`` 标记，数值原样保留。

    显式请求 short 时不报错，而是产出带标记的结果，避免既有回放/归档中断；
    默认调用不会走到这里，因为 `--tiers` 与引擎默认都已排除 short。
    """
    marked: dict[str, dict] = {}
    for key, payload in tiers.items():
        item = dict(payload)
        item['observe_only'] = key in OBSERVE_ONLY_TIERS
        marked[key] = item
    return marked


def _run_generate(args: argparse.Namespace) -> int:
    as_of = _parse_date(args.as_of)
    now = datetime.now()
    if (as_of is None and not args.force
            and (now.hour < 15 or (now.hour == 15 and now.minute < 30))):
        logger.warning('未到收盘后（15:30 之后），如需生成今日清单请加 --force')
        return 2
    config: AdviceConfig = default_config(
        factor_set=args.factor_set,
        soft_filter_enabled=not args.no_soft_filter)
    engine = AdviceEngine(config=config)
    daily_dir = Path(args.daily_dir) if args.daily_dir else Path(args.out_dir) / 'daily'
    result = engine.generate(as_of=as_of, equity=args.equity,
                             tiers=args.tiers, state_dir=args.state_dir,
                             daily_dir=daily_dir, persist_daily=not args.no_daily)
    frame = result.to_frame()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.write_csv(out_dir / 'all.csv')
    summary = result.summary()
    summary['tiers'] = mark_observe_only(summary['tiers'])
    out_dir.joinpath('summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    _write_snapshot(
        {'trade_date': result.trade_date.isoformat(), 'tiers': summary['tiers'],
         'items': result.to_dicts()},
        out_dir, args.equity)
    if OBSERVE_ONLY_TIERS.intersection(result.tiers):
        logger.warning('[advice] 短线档位仅观察（未通过样本外），输出已标记 observe_only')
    logger.info(
        f"[advice] 生成完成 {result.trade_date}: {len(frame)} 行 → {out_dir}")
    for key, advice in result.tiers.items():
        counts = advice.summary()['counts']
        logger.info(f"[advice] {key} {advice.label}: {counts}")
    return 0


def _run_replay(args: argparse.Namespace) -> int:
    results = run_replay(
        config=default_config(
            factor_set=args.factor_set,
            soft_filter_enabled=not args.no_soft_filter),
        start=_parse_date(args.start),
        end=_parse_date(args.end),
        max_symbols=args.max_symbols,
        initial_equity=args.equity,
        tiers=args.tiers,
    )
    if not results:
        logger.error('[replay] 无回放结果')
        return 1
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = mark_observe_only(
        {key: item.summary() for key, item in results.items()})
    out_dir.joinpath('replay_summary.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    diagnostics = {key: item.diagnostics for key, item in results.items()}
    out_dir.joinpath('replay_diagnostics.json').write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding='utf-8')
    for key, item in results.items():
        if item.equity_curve:
            pl.DataFrame(
                item.equity_curve, schema=['date', 'equity'], orient='row'
            ).write_csv(out_dir / f'replay_equity_{key}.csv')
        if item.daily:
            pl.DataFrame(item.daily).write_csv(
                out_dir / f'replay_daily_{key}.csv')
        if item.positions:
            pl.DataFrame(item.positions).write_csv(
                out_dir / f'replay_positions_{key}.csv')
        if item.position_daily:
            pl.DataFrame(item.position_daily).write_csv(
                out_dir / f'replay_position_daily_{key}.csv')
        samples = item.sample_rows
        if samples:
            pl.DataFrame(samples).write_csv(out_dir / f'replay_sample_{key}.csv')
        logger.info(f"[replay] {key} {item.label}: {item.summary()}")
    return 0


def _daily_dir(args: argparse.Namespace) -> Path:
    return Path(args.daily_dir) if args.daily_dir else Path(args.out_dir) / 'daily'


def _run_daily_view(args: argparse.Namespace) -> int:
    directory = _daily_dir(args)
    if args.daily_list:
        entries = list_daily_snapshots(directory)
        if not entries:
            sys.stdout.write(f'[daily] 暂无归档: {directory}\n')
            return 1
        for entry in entries:
            sys.stdout.write(
                f"{entry['trade_date']}  {entry['items']}  {entry['path']}\n")
        return 0
    parsed = _parse_date(args.daily_show)
    if parsed is None:
        sys.stdout.write(f'[daily] 日期无效: {args.daily_show}\n')
        return 1
    payload = load_daily_snapshot(directory, parsed)
    if payload is None:
        sys.stdout.write(f'[daily] 找不到归档: {directory}/{args.daily_show}\n')
        return 1
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='每日收盘后推荐清单')
    parser.add_argument('--as-of', help='推荐基准日 YYYY-MM-DD（默认最新交易日）')
    parser.add_argument('--equity', type=float, default=1_000_000.0,
                        help='组合总资金（元），默认 100 万')
    parser.add_argument('--tiers', nargs='+', default=default_output_tiers(),
                        help='档位：mid/long（short 仅观察，需显式指定并会被标记）')
    parser.add_argument('--state-dir', default=None, help='持仓状态目录')
    parser.add_argument('--out-dir',
                        default=str(_project_root() / 'reports' / 'advice'),
                        help='输出目录')
    parser.add_argument('--force', action='store_true',
                        help='忽略收盘后 15:30 的时间检查')
    parser.add_argument('--replay', action='store_true', help='运行历史回放')
    parser.add_argument('--start', help='回放开始日 YYYY-MM-DD')
    parser.add_argument('--end', help='回放结束日 YYYY-MM-DD')
    parser.add_argument('--max-symbols', type=int, default=300,
                        help='回放抽样标的数，0 表示全市场')
    parser.add_argument('--factor-set', choices=['legacy', 'extended'],
                        default='extended',
                        help='因子集：legacy=原 12 因子，extended=扩展因子集')
    parser.add_argument('--no-soft-filter', action='store_true',
                        help='关闭 extended 模式的软筛选（用于 A/B）')
    parser.add_argument('--daily-dir', default=None,
                        help='每日归档目录（默认 out-dir/daily）')
    parser.add_argument('--no-daily', action='store_true', help='本次不写每日归档')
    parser.add_argument('--daily-list', action='store_true',
                        help='列出已归档的交易日')
    parser.add_argument('--daily-show', default=None,
                        help='查看某交易日归档 YYYY-MM-DD')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _ = get_config()
    if args.daily_list or args.daily_show:
        return _run_daily_view(args)
    if args.replay:
        return _run_replay(args)
    return _run_generate(args)


if __name__ == '__main__':
    raise SystemExit(main())
