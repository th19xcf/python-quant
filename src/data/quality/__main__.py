#!/usr/bin/env python3

"""``python -m src.data.quality`` 命令行入口。

子命令：
    check       数据契约巡检（local / tdx，可按标的/区间/全市场）
    baseline    把当前 error 级违规登记为已知基线（需显式确认）
    anomalies   查询已知异常台账
    cross-check 跨源抽样对拍（BaoStock / AkShare，只读）
    factor-check 复权因子全市场准确性核验（只读）
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from loguru import logger

from src.data.local_store import LocalQuoteStore, resolve_store_root
from src.data.quality.anomalies import iter_anomalies, query_anomalies
from src.data.quality.checker import (
    FrameSource,
    LocalStoreFrameSource,
    QualityChecker,
    TdxFrameSource,
)
from src.data.quality.cross_check import (
    build_adapters,
    build_remote_config,
    default_date_range,
    run_cross_check,
    sample_codes,
)
from src.data.quality.exemptions import (
    DEFAULT_EXEMPTION_PATH,
    build_baseline,
    write_exemptions,
)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(value[:10] if '-' in value else value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f'无法解析日期: {value}')


def _split_codes(value: str | None) -> list[str] | None:
    if not value:
        return None
    codes = [item.strip() for item in value.split(',') if item.strip()]
    return codes or None


def _make_store(config: object) -> LocalQuoteStore:
    return LocalQuoteStore(resolve_store_root(config), enabled=True)


def _store_latest_date(store: LocalQuoteStore, data_type: str = 'stock') -> date:
    ends = []
    for code in store.codes(data_type):
        coverage = store.coverage(data_type, code)
        if coverage is not None:
            ends.append(coverage.end)
    return max(ends) if ends else date.today()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m src.data.quality',
        description='数据准确性巡检、异常台账与跨源对拍',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    check = sub.add_parser('check', help='执行数据契约巡检')
    check.add_argument('--codes', help='逗号分隔的 ts_code；缺省配合 --all 全市场')
    check.add_argument('--all', action='store_true', help='全市场巡检（默认行为）')
    check.add_argument('--start', help='起始日期 YYYY-MM-DD，仅过滤违规行，不截断历史')
    check.add_argument('--end', help='结束日期 YYYY-MM-DD')
    check.add_argument('--data-type', choices=('stock', 'index', 'fund', 'all'),
                       default='all')
    check.add_argument('--source', choices=('local', 'tdx'), default='local',
                       help='local=本地列存（默认）；tdx=通达信原始 .day')
    check.add_argument('--latest-trade-date', help='交易日上界，默认今天')
    check.add_argument('--amount-tolerance', type=float, default=0.25,
                       help='amount/(close*volume*100) 允许的相对偏差，默认 0.25')
    check.add_argument('--dry-run', action='store_true', help='只计算不写报告')
    check.add_argument('--output-dir', default='logs')
    check.add_argument('--fail-on-error', action='store_true',
                       help='存在 error 级违规时返回退出码 1')

    baseline = sub.add_parser(
        'baseline',
        help='把当前 error 级违规登记为已知基线（精确到规则+标的+日期）',
    )
    baseline.add_argument('--reason', required=True,
                          help='登记理由，会写入每条基线记录，便于审计')
    baseline.add_argument('--confirm', required=True, choices=('REBASELINE',),
                          help='显式确认覆盖已有基线文件，必须填 REBASELINE')
    baseline.add_argument('--expires', default='',
                          help='可选到期日 YYYY-MM-DD，到期后豁免自动失效')
    baseline.add_argument('--output', default=str(DEFAULT_EXEMPTION_PATH),
                          help='基线文件输出路径')
    baseline.add_argument('--codes', help='逗号分隔的 ts_code；缺省为全市场')
    baseline.add_argument('--data-type',
                          choices=('stock', 'index', 'fund', 'all'),
                          default='all')
    baseline.add_argument('--source', choices=('local', 'tdx'), default='local')
    baseline.add_argument('--latest-trade-date', help='交易日上界，默认今天')
    baseline.add_argument('--amount-tolerance', type=float, default=0.25)
    baseline.add_argument('--dry-run', action='store_true',
                          help='只统计拟登记条数，不写文件')

    anomalies = sub.add_parser('anomalies', help='查询已知异常台账')
    anomalies.add_argument('--code', help='ts_code，如 000001.SH')
    anomalies.add_argument('--date', help='交易日期 YYYY-MM-DD')
    anomalies.add_argument('--context', choices=('raw', 'local', 'tdx', 'factor', 'legacy'),
                          help='限定上下文，如 local/factor/legacy')
    anomalies.add_argument('--json', action='store_true', help='输出 JSON 数组')

    cross = sub.add_parser('cross-check', help='跨源抽样对拍')
    cross.add_argument('--sample', type=int, default=40, help='抽样标的数，默认 40（建议 30-50）')
    cross.add_argument('--years', type=int, default=3, help='每只回溯年数，默认 3')
    cross.add_argument('--source', choices=('baostock', 'akshare', 'both'),
                       default='both')
    cross.add_argument('--start', help='起始日期，缺省按 --years 回溯')
    cross.add_argument('--end', help='结束日期，缺省本地列存最新日期')
    cross.add_argument('--seed', type=int, default=42)
    cross.add_argument('--max-consecutive-empty', type=int, default=3,
                       help='单源连续空结果达到该值后提前停止')
    cross.add_argument('--dry-run', action='store_true', help='只计算不写报告')
    cross.add_argument('--output-dir', default='logs')

    factor = sub.add_parser('factor-check', help='复权因子全市场准确性核验（只读）')
    factor.add_argument('--sample', type=int, default=60,
                        help='独立复算抽样标的数，默认 60（建议 >= 50）')
    factor.add_argument('--seed', type=int, default=42)
    factor.add_argument('--dry-run', action='store_true', help='只计算不写报告')
    factor.add_argument('--output-dir', default='logs')
    return parser


def _make_source(args: argparse.Namespace, config: object) -> FrameSource | None:
    """按 --source 构造只读数据源；通达信目录缺失时返回 None。"""
    if args.source == 'local':
        return LocalStoreFrameSource(_make_store(config))
    tdx_path = Path(config.data.tdx_data_path)  # type: ignore[attr-defined]
    if not tdx_path.is_dir():
        logger.error(f'通达信目录不存在: {tdx_path}')
        return None
    return TdxFrameSource(tdx_path)


def _make_checker(args: argparse.Namespace, config: object,
                  source: FrameSource) -> QualityChecker:
    """按公共参数构造巡检器（check 与 baseline 共用）。"""
    return QualityChecker(
        source=source,
        latest_trade_date=_parse_date(args.latest_trade_date) or date.today(),
        amount_tolerance=getattr(args, 'amount_tolerance', 0.25),
    )


def _run_check(args: argparse.Namespace) -> int:
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    source = _make_source(args, config)
    if source is None:
        return 2

    codes = _split_codes(args.codes) if args.codes else None
    data_types = ('stock', 'index') if args.data_type == 'all' else (args.data_type,)
    checker = _make_checker(args, config, source)
    report = checker.run(
        codes=codes,
        data_types=data_types,
        start=_parse_date(args.start),
        end=_parse_date(args.end),
        write_report=not args.dry_run,
        output_dir=args.output_dir,
    )
    logger.info(report.summary_text())
    if report.by_rule:
        for rule_id, count in sorted(report.by_rule.items()):
            unique = report.by_rule_unique.get(rule_id, count)
            logger.info(f'  - {rule_id}: {count} 条违规 / {unique} 个唯一 (标的,日期)')
    if report.by_severity:
        logger.info(f'  severity: {report.by_severity}')
    if report.known_by_anomaly:
        logger.info(f'  台账命中: {report.known_by_anomaly}')
    if report.exempt_errors:
        logger.info(f'  已登记基线豁免 error: {report.exempt_errors} 条')
    if report.missing_symbols:
        logger.warning(f'缺数据标的 {len(report.missing_symbols)} 个: '
                       f'{report.missing_symbols[:10]}')
    if report.report_files:
        for name, path in report.report_files.items():
            logger.info(f'  报告 {name}: {path}')
    else:
        logger.info('  dry-run：未写报告')
    # 只认「未登记为已知基线」的 error；基线缺失时 actionable_errors 等于
    # 全部 error，行为退化为原有的 fail-closed 门禁。
    if args.fail_on_error and report.actionable_errors:
        logger.error(f'存在 {report.actionable_errors} 条未登记的 error 级违规')
        return 1
    return 0


def _run_baseline(args: argparse.Namespace) -> int:
    """跑一次全量巡检，把 error 级违规指纹登记为已知基线。

    只在显式给出 ``--confirm REBASELINE`` 且非 dry-run 时覆盖基线文件；
    用于「确认这批历史瑕疵无法修复、后续只关心新增回归」的运维场景。
    """
    from src.utils.config import Config  # noqa: PLC0415

    expires = _parse_date(args.expires) if args.expires else None
    config = Config()
    source = _make_source(args, config)
    if source is None:
        return 2
    data_types = ('stock', 'index') if args.data_type == 'all' else (args.data_type,)
    checker = _make_checker(args, config, source)
    report = checker.run(
        codes=_split_codes(args.codes),
        data_types=data_types,
        write_report=False,
    )
    records = build_baseline(
        report.violations,
        reason=args.reason,
        expires=expires.isoformat() if expires else '',
    )
    logger.info(
        f'基线生成: source={args.source} symbols={report.symbols_scanned} '
        f'rows={report.rows_scanned} errors={report.by_severity.get("error", 0)} '
        f'fingerprints={len(records)}'
    )
    if args.dry_run:
        logger.info('dry-run：未写基线文件')
        return 0
    written = write_exemptions(records, Path(args.output))
    logger.info(f'基线已写入 {args.output}（{written} 条，reason={args.reason}）')
    return 0


def _run_anomalies(args: argparse.Namespace) -> int:
    trade_date = _parse_date(args.date)
    if args.code or trade_date:
        payload = query_anomalies(args.code or '', trade_date, context=args.context)
        if args.json:
            logger.info(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0
        current = payload['date'] or '-'
        if not payload['known']:
            logger.info(f"{payload['ts_code']} | {current} | 未命中已知异常 | "
                        f"advice={payload['advice']}")
            return 0
        logger.info(f"{payload['ts_code']} | {current} | 命中 {payload['hit_count']} 条 | "
                    f"advice={payload['advice']}")
        for hit in payload['hits']:
            logger.info(f"  {hit['anomaly_id']} | {hit['code']} | "
                        f"severity={hit['severity']} | action={hit['action']}")
            logger.info(f"    原因: {hit['reason']}")
            logger.info(f"    证据: {hit['evidence']}")
            logger.info(f"    来源: {hit['source']} | detected_at={hit['detected_at']}")
        return 0
    records = list(iter_anomalies())
    if args.json:
        logger.info(json.dumps([record.as_dict() for record in records],
                               ensure_ascii=False, indent=2))
        return 0
    if not records:
        logger.info('未命中任何已知异常')
        return 0
    for record in records:
        span = (
            f'{record.dates[0]}..{record.dates[-1]} ({len(record.dates)} 个日期)'
            if record.dates else
            f'{record.start or "-"}..{record.end or "-"}'
        )
        logger.info(f'{record.anomaly_id} | {record.ts_code} | {record.issue_type} | '
                    f'{record.action} | severity={record.severity} | '
                    f'scope={record.scope} | status={record.status} | {span}')
        logger.info(f'  原因: {record.reason}')
        logger.info(f'  来源: {record.source}')
    return 0


def _run_cross_check(args: argparse.Namespace) -> int:
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    store = _make_store(config)
    end = _parse_date(args.end) or _store_latest_date(store)
    start = _parse_date(args.start) or default_date_range(args.years, end=end)[0]
    codes = sample_codes(store, count=args.sample, seed=args.seed,
                         start=start, end=end)
    if not codes:
        logger.error('没有可抽样的标的（本地列存为空或区间无覆盖）')
        return 2
    logger.info(f'抽样 {len(codes)} 只，区间 {start}..{end}')

    sources = ('baostock', 'akshare') if args.source == 'both' else (args.source,)
    remote_config = build_remote_config()
    adapters = build_adapters(sources, remote_config)
    report = run_cross_check(
        store=store,
        codes=codes,
        start=start,
        end=end,
        sources=sources,
        adapters=adapters,
        max_consecutive_empty=args.max_consecutive_empty,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
    )
    summary = report.summary()
    logger.info(f'对拍完成: {summary}')
    for name, path in report.report_files.items():
        logger.info(f'  报告 {name}: {path}')
    if args.dry_run:
        logger.info('  dry-run：未写报告')
    return 0


def _run_factor_check(args: argparse.Namespace) -> int:
    from src.data.local_store.store import LocalFactorStore  # noqa: PLC0415
    from src.data.quality.factors import audit_factors  # noqa: PLC0415
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    store = _make_store(config)
    factor_store = LocalFactorStore(store.root, enabled=True)
    result = audit_factors(
        store=store,
        factor_store=factor_store,
        output_dir=args.output_dir,
        sample_size=args.sample,
        seed=args.seed,
        write_report=not args.dry_run,
    )
    payload = result.payload
    logger.info(f'核验完成，耗时 {payload["elapsed_s"]}s')
    logger.info(f'覆盖率: {payload["coverage"]}')
    logger.info(f'重算: {payload["recompute"]["codes_checked"]} 只，'
                f'同算法 {payload["recompute"]["codes_same_algorithm"]} 只，'
                f'差异 {payload["recompute"]["codes_diff"]} 只')
    logger.info(f'抽样: {payload["sample"]["size"]} 只，'
                f'max_rel={payload["sample"]["max_rel_vs_snapshot"]:.3e}，'
                f'可逆 max_rel={payload["sample"]["reversible_max_rel"]:.3e}')
    logger.info(f'跳变反查: {payload["jumps"]}')
    for line in payload['conclusions']:
        logger.info(f'结论: {line}')
    if payload.get('report_files'):
        for name, path in payload['report_files'].items():
            logger.info(f'  报告 {name}: {path}')
    else:
        logger.info('  dry-run：未写报告')
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == 'check':
            return _run_check(args)
        if args.command == 'baseline':
            return _run_baseline(args)
        if args.command == 'anomalies':
            return _run_anomalies(args)
        if args.command == 'cross-check':
            return _run_cross_check(args)
        if args.command == 'factor-check':
            return _run_factor_check(args)
    except ValueError as exc:
        logger.error(str(exc))
        return 2
    parser.error(f'未知子命令: {args.command}')
    return 2


if __name__ == '__main__':
    sys.exit(main())
