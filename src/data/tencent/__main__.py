#!/usr/bin/env python3

"""``python -m src.data.tencent`` 的 CLI 入口（显式联网开关，默认关闭）。

用法::

    # 预演：盘点落后标的，不落盘
    python -m src.data.tencent fill-daily --allow-remote --dry-run

    # 正式补齐（默认个股+指数；--data-type all 含基金）
    python -m src.data.tencent fill-daily --allow-remote --data-type all

    # 指定标的 / 指定日期增量
    python -m src.data.tencent fill-daily --allow-remote --codes 600000.SH,000001.SZ
    python -m src.data.tencent fill-daily --allow-remote --since 2026-10-20

    # 抽 20 只与 TDX .day 重叠区间对拍
    python -m src.data.tencent verify-daily --allow-remote --sample 20

    # 分钟线取数（每档最多 320 根，无成交额）
    python -m src.data.tencent minute --allow-remote --codes 600000.SH --freq m1

联网闸门：所有子命令都必须显式传 ``--allow-remote``；本工具与
``data.allow_remote_sources`` 完全解耦，不会改动它的默认值。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

from loguru import logger

from src.data.local_store import LocalQuoteStore, resolve_store_root
from src.data.tencent.client import MAX_MINUTE_BARS, MINUTE_FREQUENCIES, TencentClient
from src.data.tencent.daily_fill import DEFAULT_DATA_TYPES, fill_daily_gap, verify_daily
from src.data.tencent.minute import (
    fetch_minute_batch,
    format_minute_brief,
    write_summary_csv,
)


def _parse_date(value: str) -> date:
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f'无法解析日期: {value!r}（应为 YYYY-MM-DD）')


def _parse_types(value: str) -> tuple[str, ...]:
    parts = tuple(part.strip().lower() for part in value.split(',') if part.strip())
    if 'all' in parts:
        return ('stock', 'index', 'fund')
    unknown = set(parts) - {'stock', 'index', 'fund'}
    if unknown:
        raise argparse.ArgumentTypeError(f'未知 data-type: {sorted(unknown)}')
    return parts or DEFAULT_DATA_TYPES


def _split_codes(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [part.strip() for part in value.split(',') if part.strip()]


def _make_store(config: Any) -> LocalQuoteStore:
    return LocalQuoteStore(resolve_store_root(config), enabled=True)


def _write_report(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f'[tencent] 统计已写入 {target}')


def _require_remote(allow: bool) -> bool:
    if allow:
        return True
    logger.error(
        '拒绝联网：必须显式传 --allow-remote（本工具与 data.allow_remote_sources 解耦）')
    return False


def _cmd_fill_daily(args: argparse.Namespace) -> int:
    if not _require_remote(args.allow_remote):
        return 2
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    store = _make_store(config)
    client = TencentClient(min_interval=args.min_interval)
    stats = fill_daily_gap(
        config, store, client,
        codes=_split_codes(args.codes),
        data_types=_parse_types(args.data_type),
        since=args.since,
        latest_trade_date=args.latest_trade_date,
        workers=args.workers,
        dry_run=args.dry_run,
        max_symbols=args.max_symbols,
        max_gap_days=args.max_gap_days or None,
        guard_sample_size=args.guard_sample,
        force=args.force,
    )
    logger.info(f'[tencent] fill-daily 统计: {json.dumps(stats, ensure_ascii=False)}')
    _write_report(args.report, stats)
    if stats.get('aborted') == 'guard_failed':
        return 3
    return 0


def _cmd_verify_daily(args: argparse.Namespace) -> int:
    if not _require_remote(args.allow_remote):
        return 2
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    store = _make_store(config)
    client = TencentClient(min_interval=args.min_interval)
    result = verify_daily(
        config, store, client,
        codes=_split_codes(args.codes), sample=args.sample,
        start=args.start, end=args.end, count=args.count)
    for field, stat in result['fields'].items():
        logger.info(
            f'[tencent] {field}: max_abs={stat["max_abs"]:.6g} '
            f'max_rel={stat["max_rel"]:.3e} diff_rows={stat["diff_rows"]}')
    for item in result['results']:
        logger.info(f'[tencent] {item}')
    mismatches = [item for item in result['results']
                  if item.get('status') == 'price_mismatch']
    _write_report(args.report, result)
    return 1 if mismatches else 0


def _cmd_minute(args: argparse.Namespace) -> int:
    if not _require_remote(args.allow_remote):
        return 2
    client = TencentClient(min_interval=args.min_interval)
    codes = _split_codes(args.codes) or []
    summary = fetch_minute_batch(
        client, codes, args.freq, args.count,
        output_dir=args.output_dir)
    logger.info(f'[tencent] {format_minute_brief(summary)}')
    logger.info(f'[tencent] 失败: {summary["failures"]}')
    if args.report:
        _write_report(args.report, summary)
    if args.summary_csv:
        path = write_summary_csv(args.summary_csv, summary)
        logger.info(f'[tencent] 汇总 CSV: {path}')
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m src.data.tencent',
        description='腾讯行情离线补数（只写本地，运行时读路径不联网）')
    sub = parser.add_subparsers(dest='command', required=True)

    fill = sub.add_parser('fill-daily', help='补齐本地日线缺口（原始价+成交额）')
    fill.add_argument('--allow-remote', action='store_true', help='显式允许联网（必填）')
    fill.add_argument('--codes', default=None, help='限定标的，逗号分隔（600000.SH 或 600000）')
    fill.add_argument('--data-type', default=','.join(DEFAULT_DATA_TYPES),
                      help='stock,index,fund,all（默认 stock,index）')
    fill.add_argument('--since', type=_parse_date, default=None, help='只追加该日期之后的行')
    fill.add_argument('--latest-trade-date', type=_parse_date, default=None,
                      help='显式指定目标交易日（默认自动探测，盘中会丢弃当天未完成 K）')
    fill.add_argument('--workers', type=int, default=6, help='并发线程数')
    fill.add_argument('--min-interval', type=float, default=0.05,
                      help='全局最小请求间隔（秒）')
    fill.add_argument('--dry-run', action='store_true', help='只盘点，不写任何数据')
    fill.add_argument('--max-symbols', type=int, default=None, help='调试用，限制处理标的数')
    fill.add_argument('--max-gap-days', type=int, default=90,
                      help='末根距目标交易日超过该天数不回填（0 表示不设闸门，默认 90）')
    fill.add_argument('--guard-sample', type=int, default=5,
                      help='写前抽样对拍数量（0 表示跳过闸门）')
    fill.add_argument('--force', action='store_true', help='闸门不达标时仍强制写入')
    fill.add_argument('--report', default=None, help='把统计 JSON 写到指定路径')
    fill.set_defaults(func=_cmd_fill_daily)

    verify = sub.add_parser('verify-daily', help='腾讯原始价 vs TDX .day 对拍（只读）')
    verify.add_argument('--allow-remote', action='store_true', help='显式允许联网（必填）')
    verify.add_argument('--codes', default=None, help='指定标的，逗号分隔（默认抽样 20 只）')
    verify.add_argument('--sample', type=int, default=20, help='抽样数量')
    verify.add_argument('--start', type=_parse_date, default=None)
    verify.add_argument('--end', type=_parse_date, default=None)
    verify.add_argument('--count', type=int, default=800, help='每只请求的日线行数')
    verify.add_argument('--min-interval', type=float, default=0.05)
    verify.add_argument('--report', default=None)
    verify.set_defaults(func=_cmd_verify_daily)

    minute = sub.add_parser('minute', help='分钟线取数（每档上限 320 根，无成交额）')
    minute.add_argument('--allow-remote', action='store_true', help='显式允许联网（必填）')
    minute.add_argument('--codes', required=True, help='逗号分隔的标的列表')
    minute.add_argument('--freq', default='m1', choices=list(MINUTE_FREQUENCIES))
    minute.add_argument('--count', type=int, default=MAX_MINUTE_BARS,
                        help=f'请求根数（上限 {MAX_MINUTE_BARS}）')
    minute.add_argument('--min-interval', type=float, default=0.05)
    minute.add_argument('--output-dir', default=None, help='逐票 CSV 输出目录')
    minute.add_argument('--summary-csv', default=None, help='汇总 CSV 路径')
    minute.add_argument('--report', default=None)
    minute.set_defaults(func=_cmd_minute)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == '__main__':
    sys.exit(main())
