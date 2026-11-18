"""分红月度全量刷新单一入口（供定时任务直接调用）。

一条命令按固定顺序串行完成五个阶段，任一阶段可中断续跑::

    1. 个股批量      tools/collect_stock_dividend.py   按报告期全市场拉取
    2. 基金逐只取证  tools/probe_fund_dividend.py      逐只 2 次请求，串行限流
    3. 基金入库      tools/backfill_fund_dividend.py   把取证行合并进正式表并重建因子
    4. 台账重建      tools/build_no_dividend_ledger.py 仅在逐只取证完整时执行
    5. 个股因子      rebuild_factors_from_local        只重建有新增除息事件的标的

续跑与限流::

    - 断点落 ``<work-dir>/state.json``（阶段级）；逐只取证另有 progress.csv /
      failed.csv / rows.csv，个股批量有逐报告期缓存与 progress.json。
    - 全流程串行，不并发；基金逐只默认 ``--interval 2 --throttle 0.5``，个股批量
      每报告期间隔 0.25 秒。
    - 取证阶段遇限流/封禁由子工具返回 3：本入口立即停手、不推进后续阶段，
      用同一命令重跑即可从断点继续。

用法（完整命令与预计耗时见 ``docs/分红数据刷新指南.md``）::

    python tools/refresh_dividend_monthly.py --apply
    python tools/refresh_dividend_monthly.py               # dry-run，只打印计划
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_text_atomic  # noqa: E402
from tools.refresh_dividend_daily import _quarter_periods  # noqa: E402

STORE = ROOT / 'data' / 'local_store'
STATE_FILE = 'state.json'
#: 阶段顺序与预计耗时（分钟，仅用于 dry-run 提示与文档口径）。
PHASES: tuple[tuple[str, str, str], ...] = (
    ('stock_bulk', '个股按报告期批量刷新', '约 8~15 分钟（2016 年起 43 个报告期）'),
    ('fund_probe', '基金逐只 probe', '约 90~150 分钟（1,696 只 × 约 3~5 秒）'),
    ('fund_ingest', '基金分红入库 + 因子重建', '约 1 分钟'),
    ('ledger', '无分红台账重建', '约 1 分钟'),
    ('stock_factors', '个股受影响标的因子重建', '约 1~10 分钟'),
)


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {'started_at': datetime.now().isoformat(timespec='seconds'), 'phases': {}}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {'started_at': datetime.now().isoformat(timespec='seconds'), 'phases': {}}
    payload.setdefault('phases', {})
    return payload


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state['updated_at'] = datetime.now().isoformat(timespec='seconds')
    write_text_atomic(json.dumps(state, ensure_ascii=False, indent=2) + '\n', path)


def _run(cmd: list[str]) -> int:
    print('$ ' + ' '.join(cmd), flush=True)
    return subprocess.run(cmd, cwd=str(ROOT)).returncode


def _py(script: str) -> str:
    return str(ROOT / 'tools' / script)


def _event_keys(path: Path) -> set[tuple[str, str]]:
    if not path.is_file():
        return set()
    frame = pl.read_parquet(path, columns=['ts_code', 'ex_date'])
    frame = frame.filter(pl.col('ex_date').is_not_null())
    return set(zip(frame['ts_code'].to_list(),
                   [str(value) for value in frame['ex_date'].to_list()], strict=False))


def _monthly_work_dir(target: date) -> Path:
    return ROOT / 'work' / f'分红月度全量_{target:%Y-%m-%d}'


def _stock_phase(work: Path, start_year: int, end_period: str) -> tuple[int, dict[str, Any]]:
    report = work / '个股报告期批量'
    before = report / 'before_events.parquet'
    report.mkdir(parents=True, exist_ok=True)
    table = STORE / 'factors' / 'stock_dividend.parquet'
    if not before.is_file():
        # 阶段前事件快照：用于阶段 5 精确圈定「本次新增除息事件」的标的
        keys = pl.read_parquet(table, columns=['ts_code', 'ex_date']).filter(
            pl.col('ex_date').is_not_null())
        keys.write_parquet(before)
    code = _run([sys.executable, _py('collect_stock_dividend.py'),
                 '--cache', str(report / 'cache'), '--report-dir', str(report),
                 '--start-year', str(start_year), '--end-period', end_period])
    return code, {'before_snapshot': str(before), 'report_dir': str(report)}


def _probe_phase(work: Path, interval: float, throttle: float, limit: int) -> tuple[int, dict]:
    probe = work / '基金逐只取证'
    cmd = [sys.executable, _py('probe_fund_dividend.py'), '--all-funds',
           '--work-dir', str(probe), '--interval', str(interval),
           '--throttle', str(throttle)]
    if limit > 0:
        cmd += ['--limit', str(limit)]
    return _run(cmd), {'probe_dir': str(probe)}


def _rows_codes(rows_csv: Path) -> set[str]:
    if not rows_csv.is_file():
        return set()
    frame = pl.read_csv(rows_csv, infer_schema_length=0)
    return set(frame['ts_code'].to_list()) if 'ts_code' in frame.columns else set()


def _fund_ingest_phase(work: Path) -> tuple[int, dict[str, Any]]:
    rows_csv = work / '基金逐只取证' / 'rows.csv'
    codes = _rows_codes(rows_csv)
    if not codes:
        return 0, {'skipped': True, 'reason': '无取证命中行（rows.csv 缺失或为空）'}
    table = STORE / 'factors' / 'fund_dividend.parquet'
    present = set(pl.read_parquet(table, columns=['ts_code'])['ts_code'].unique().to_list())
    if codes <= present:
        return 0, {'skipped': True, 'reason': '命中代码已全部在正式表（幂等重跑）'}
    if codes & present:
        overlap = sorted(codes & present)
        logger.error('命中代码与正式表部分重叠，拒绝覆盖：{}', overlap[:5])
        return 1, {'overlap_codes': overlap[:20], 'overlap_count': len(overlap)}
    code = _run([sys.executable, _py('backfill_fund_dividend.py'),
                 '--rows', str(rows_csv)])
    return code, {'rows_csv': str(rows_csv), 'codes': len(codes)}


def _ledger_phase(work: Path) -> tuple[int, dict[str, Any]]:
    probe = work / '基金逐只取证'
    progress = probe / 'progress.csv'
    if not progress.is_file():
        return 0, {'skipped': True, 'reason': '无 progress.csv，保留旧台账'}
    done = pl.read_csv(progress, infer_schema_length=0)['ts_code'].n_unique()
    # 完整性闸门：逐只取证未跑完全量时不做台账重建，避免把已确认基金降级为未确认
    ledger = pl.read_parquet(
        STORE / 'meta' / 'dividend_status' / 'no_dividend_ledger.parquet',
        columns=['ts_code', 'kind', 'status'])
    expected = ledger.filter(pl.col('kind').is_in(['ETF', 'LOF', '分级', '未知'])).height
    if done < expected:
        return 0, {'skipped': True,
                   'reason': f'逐只取证未完成（{done}/{expected}），保留旧台账'}
    code = _run([sys.executable, _py('build_no_dividend_ledger.py'),
                 '--evidence', str(progress), '--preserve-non-stock'])
    return code, {'progress': str(progress), 'probed_codes': done}


def _stock_factor_phase(work: Path) -> tuple[int, dict[str, Any]]:
    report = work / '个股报告期批量'
    before = report / 'before_events.parquet'
    if not before.is_file():
        return 0, {'skipped': True, 'reason': '无阶段前事件快照'}
    table = STORE / 'factors' / 'stock_dividend.parquet'
    added = _event_keys(table) - _event_keys(before)
    codes = sorted({code for code, _ in added})
    if not codes:
        return 0, {'skipped': True, 'reason': '无新增除息事件'}
    from src.data.local_store.factor_rebuild import rebuild_factors_from_local  # noqa: PLC0415
    from src.data.local_store.store import LocalFactorStore, LocalQuoteStore  # noqa: PLC0415
    from tools.refresh_dividend_daily import _quote_factor_mismatch  # noqa: PLC0415

    started = time.perf_counter()
    stats = rebuild_factors_from_local(
        LocalQuoteStore(STORE), LocalFactorStore(STORE),
        codes=codes, include_existing=True, resume=False)
    return 0, {
        'affected_codes': codes, 'new_events': len(added),
        'factor_rebuild': {k: v for k, v in stats.items() if k != 'quote_factor_refresh'},
        'mismatch_codes': _quote_factor_mismatch('stock', codes),
        'elapsed_s': round(time.perf_counter() - started, 2),
    }


def _print_plan(target: date, work: Path, start_year: int, end_period: str,
                state: dict[str, Any]) -> None:
    print(f'分红月度全量（dry-run）目标日 {target}，工作目录 {work}')
    print(f'  工作日：先跑个股批量（--start-year {start_year} --end-period {end_period}）')
    print('  续跑：重跑 --apply 会跳过 state.json 中已完成的阶段')
    for key, label, estimate in PHASES:
        done = (state.get('phases', {}).get(key) or {}).get('status')
        print(f'  - {key:14s} {label}  [{done or "待执行"}]  {estimate}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='分红月度全量刷新单一入口')
    parser.add_argument('--apply', action='store_true', help='不带则 dry-run')
    parser.add_argument('--date', default=None, help='目标日（默认今天）')
    parser.add_argument('--work-dir', default=None, help='工作/断点目录（默认 work/分红月度全量_日期）')
    parser.add_argument('--start-year', type=int, default=2016)
    parser.add_argument('--end-period', default='', help='个股最新报告期（默认最近季末）')
    parser.add_argument('--interval', type=float, default=2.0, help='基金逐只两次请求间隔秒')
    parser.add_argument('--throttle', type=float, default=0.5, help='基金逐只之间的间隔秒')
    parser.add_argument('--limit', type=int, default=0, help='本轮最多取证只数（0=全量）')
    args = parser.parse_args(argv)

    target = date.fromisoformat(args.date) if args.date else date.today()
    end_period = args.end_period or _quarter_periods(target)[0]
    work = Path(args.work_dir) if args.work_dir else _monthly_work_dir(target)
    state_path = work / STATE_FILE
    state = _load_state(state_path)

    if not args.apply:
        _print_plan(target, work, args.start_year, end_period, state)
        print('未带 --apply，未执行任何采集或写盘。')
        return 0

    work.mkdir(parents=True, exist_ok=True)
    state.update({'target': target.isoformat(), 'work_dir': str(work),
                  'end_period': end_period, 'status': 'running'})
    _save_state(state_path, state)

    steps = [
        ('stock_bulk', lambda: _stock_phase(work, args.start_year, end_period)),
        ('fund_probe', lambda: _probe_phase(work, args.interval, args.throttle, args.limit)),
        ('fund_ingest', lambda: _fund_ingest_phase(work)),
        ('ledger', lambda: _ledger_phase(work)),
        ('stock_factors', lambda: _stock_factor_phase(work)),
    ]
    for key, runner in steps:
        mark = state['phases'].get(key) or {}
        if mark.get('status') == 'done':
            print(f'[skip] {key} 已完成（{mark.get("finished_at")}）', flush=True)
            continue
        started = time.perf_counter()
        print(f'\n===== {key} 开始 =====', flush=True)
        code, detail = runner()
        elapsed = round(time.perf_counter() - started, 2)
        state['phases'][key] = {
            'status': 'aborted' if code == 3 else ('done' if code == 0 else 'failed'),
            'rc': code, 'elapsed_s': elapsed,
            'finished_at': datetime.now().isoformat(timespec='seconds'),
            **detail,
        }
        _save_state(state_path, state)
        print(f'===== {key} 结束 rc={code} 用时 {elapsed:.1f}s =====', flush=True)
        if code != 0:
            state['status'] = 'aborted' if code == 3 else 'failed'
            _save_state(state_path, state)
            summary = {'status': state['status'], 'failed_phase': key,
                       'work_dir': str(work), 'state': str(state_path)}
            print(json.dumps(summary, ensure_ascii=False), flush=True)
            return code

    state['status'] = 'done'
    _save_state(state_path, state)
    scalars = {
        'status': 'done', 'work_dir': str(work), 'state': str(state_path),
        'phases': {key: {'rc': item.get('rc'), 'elapsed_s': item.get('elapsed_s')}
                   for key, item in state['phases'].items()},
    }
    print(json.dumps(scalars, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
