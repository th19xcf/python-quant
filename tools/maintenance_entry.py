#!/usr/bin/env python3
r"""数据维护统一入口：按当天是否交易日自动路由「日更 / 周度维护」，并用入口锁防撞车。

两套流程原样复用、不改内部逻辑：

- 交易日 → ``tools/daily_update.py --apply --allow-remote``（现有日更，35 步 STEP_PLAN）；
- 非交易日 → 周六 09:00 定时任务的周度维护批（见 ``weekly_steps``）：单步失败记录后
  继续下一步，结束时汇总失败清单并以非零退出码收尾。

锁口径与 ``tools/daily_update.py`` 一致：``_locks/maintenance_entry.lock`` 用
``O_CREAT|O_EXCL`` 独占创建，内容含 pid 与 started_at；后到的一方**不等待、不重试**，
直接退出（退出码 3）并向 ``logs/maintenance_entry.log`` 追加一行「已有实例在跑」。
持锁进程已死或持锁超过 ``QUANT_MAINT_LOCK_MAX_HOURS``（默认 12h）判陈旧，归档为
``*.stale.<时间戳>`` 后接管。入口同时检查 ``_locks/daily_update.lock``，避免有人绕过
本入口直接跑日更时两套流程并发写 ``data/local_store``。

用法::

    # 自动判定（交易日跑日更，非交易日跑周度维护）
    python tools/maintenance_entry.py
    # 手动强制（补跑用）
    python tools/maintenance_entry.py --force daily
    python tools/maintenance_entry.py --force weekly
    # 只看判定与步骤，不取锁、不执行
    python tools/maintenance_entry.py --dry-run
    # 指定日期判定（补跑某天时核对路由）
    python tools/maintenance_entry.py --date 2026-11-28 --dry-run
    # 透传参数给所选流程（``--`` 之后原样附加）
    python tools/maintenance_entry.py --force daily -- --detach --target-holdings
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

#: 入口锁：同一时间只允许一个维护实例；与日更锁分文件，互不覆盖。
LOCK_PATH = ROOT / '_locks' / 'maintenance_entry.lock'
#: 日更自己的入口锁：直接跑 daily_update 时也会持有，这里并查。
DAILY_LOCK_PATH = ROOT / '_locks' / 'daily_update.lock'
#: 单行日志（start / skip / end）。
LOG_PATH = ROOT / 'logs' / 'maintenance_entry.log'
LOCK_MAX_AGE_HOURS = float(os.environ.get('QUANT_MAINT_LOCK_MAX_HOURS', '12'))

#: 退出码：2=参数错误，3=已有实例在跑（撞锁），其余原样透传所选流程的退出码。
EXIT_USAGE = 2
EXIT_LOCK_BUSY = 3

#: 交易日流程：现有日更入口，参数与 docs/分红数据刷新指南.md 一致。
DAILY_STEPS: tuple[tuple[str, list[str]], ...] = (
    ('daily_update', [sys.executable, 'tools/daily_update.py', '--apply', '--allow-remote']),
)


def _last_trading_day(day: date) -> date:
    """不晚于 ``day`` 的最近交易日；周度批在周六跑，样本截止不能停在周末。"""
    try:
        from src.data.trading_calendar import previous_trading_day  # noqa: PLC0415

        resolved = previous_trading_day(day, ROOT, inclusive=True)
        if resolved is not None:
            return resolved
    except Exception as exc:  # noqa: BLE001 - 日历异常不应让入口失效
        sys.stderr.write(f'[maint] 交易日历不可用（{exc}），按周一~周五回退\n')
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def weekly_steps(day: date) -> tuple[tuple[str, list[str]], ...]:
    """非交易日周度维护批：五组步骤，顺序与周六 09:00 定时任务 prompt 一致。

    ① 估值与三大报表；② 股票分红、股本变动；③ 公告事件（先抓取、再入库）；
    ④ ST 历史区间；⑤ 研究输入层重建（样本 → analysis_monthly → 新因子）。
    外部采集在两处显式 ``--workers 1``：prompt 要求单连接串行 + 节流，脚本自身
    默认是并发（backfill_events 2 / fetch_stock_shares 4），此处按 prompt 收紧。
    样本构建的 ``--end`` 取不晚于当天的最近交易日（周六跑批时即上周五），
    不用字面当天，避免样本停在周末。
    """
    py = sys.executable
    return (
        ('① collect_financials', [py, 'tools/collect_financials.py']),
        ('② collect_stock_dividend', [py, 'tools/collect_stock_dividend.py']),
        ('② fetch_stock_shares', [py, 'tools/fetch_stock_shares.py', '--workers', '1']),
        ('③ backfill_events_fetch', [py, 'tools/backfill_events.py',
                                     '--allow-remote', '--workers', '1']),
        ('③ backfill_events_import', [py, 'tools/backfill_events.py',
                                      '--import-only', '--commit']),
        ('④ sync_st_name_history', [py, 'tools/sync_st_name_history.py', 'apply']),
        ('⑤ prepare_monthly_samples', [py, 'tools/research/prepare_monthly_samples.py',
                                       '--end', _last_trading_day(day).isoformat()]),
        ('⑤ build_analysis_monthly', [py, 'tools/research/build_analysis_monthly.py']),
        ('⑤ build_new_factors', [py, 'tools/research/build_new_factors.py']),
    )


def _now() -> datetime:
    return datetime.now()


def _log(line: str) -> None:
    """追加一行到统一日志；写日志失败不阻断维护流程。"""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    stamp = _now().isoformat(timespec='seconds')
    try:
        with LOG_PATH.open('a', encoding='utf-8') as handle:
            handle.write(f'{stamp} {line}\n')
    except OSError as exc:
        sys.stderr.write(f'[maint] 日志写入失败（{LOG_PATH}）：{exc}\n')


def _pid_alive(pid: int) -> bool:
    """进程存活判定（不发信号，避免误杀）；判定失败按存活处理，宁可拒绝也不覆盖。"""
    if pid <= 0:
        return False
    if os.name == 'nt':
        try:
            import ctypes  # noqa: PLC0415 - 仅 Windows

            handle = ctypes.windll.kernel32.OpenProcess(0x00100000, False, pid)
        except Exception:  # noqa: BLE001 - 判定失败按存活处理
            return True
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _is_trading_day(day: date) -> bool:
    """交易日判定：走仓库交易日历；日历不可用时退化为周一~周五并打告警。"""
    try:
        from src.data.trading_calendar import is_trading_day  # noqa: PLC0415

        return bool(is_trading_day(day, ROOT))
    except Exception as exc:  # noqa: BLE001 - 日历异常不应让入口失效
        sys.stderr.write(f'[maint] 交易日历不可用（{exc}），按周一~周五判定\n')
        return day.weekday() < 5


def decide_mode(day: date, forced: str | None) -> str:
    """返回 ``'daily'`` 或 ``'weekly'``；``forced`` 优先于日期判定。"""
    if forced in ('daily', 'weekly'):
        return forced
    return 'daily' if _is_trading_day(day) else 'weekly'


def _read_lock(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def _lock_age_hours(info: dict) -> float:
    try:
        return (_now() - datetime.fromisoformat(str(info.get('started_at')))).total_seconds() / 3600
    except (TypeError, ValueError):
        return LOCK_MAX_AGE_HOURS + 1.0


def _holder(path: Path) -> dict | None:
    """返回活跃持锁者信息；无锁、陈旧锁（进程已死或超龄）返回 ``None``。"""
    if not path.is_file():
        return None
    info = _read_lock(path)
    pid = int(info.get('pid') or 0)
    if pid == os.getpid():
        return None
    if _pid_alive(pid) and _lock_age_hours(info) <= LOCK_MAX_AGE_HOURS:
        return {'pid': pid, 'started_at': info.get('started_at', '?'), 'lock': str(path)}
    return None


def acquire_lock(mode: str, day: date) -> Path | None:
    """取入口锁；撞到活跃锁返回 ``None``（调用方退出码 3）。

    陈旧锁（持锁进程已死或超龄）归档为 ``*.stale.<时间戳>`` 后接管，与日更口径一致。
    """
    for other in (DAILY_LOCK_PATH,):
        busy = _holder(other)
        if busy:
            _log(f'[skip] 已有实例在跑（日更入口锁）holder_pid={busy["pid"]} '
                 f'started={busy["started_at"]} lock={busy["lock"]}')
            return None

    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'pid': os.getpid(),
        'started_at': _now().isoformat(timespec='seconds'),
        'mode': mode,
        'trade_day': day.isoformat(),
    }
    for _ in range(2):
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            busy = _holder(LOCK_PATH)
            if busy:
                _log(f'[skip] 已有实例在跑 holder_pid={busy["pid"]} '
                     f'started={busy["started_at"]} lock={busy["lock"]}')
                return None
            archived = LOCK_PATH.with_name(
                f'{LOCK_PATH.name}.stale.{_now():%Y%m%d%H%M%S}')
            try:
                os.replace(LOCK_PATH, archived)
            except OSError:
                continue
            continue
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return LOCK_PATH
    raise SystemExit(f'[maint] 两次尝试仍未取得入口锁：{LOCK_PATH}')


def release_lock(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        sys.stderr.write(f'[maint] 锁释放失败（需人工清理 {path}）：{exc}\n')


def steps_for(mode: str, day: date, passthrough: list[str]) -> tuple[tuple[str, list[str]], ...]:
    """按模式取步骤清单；透传参数只附加到最后一步（日更本身/周度末步）。"""
    base = list(DAILY_STEPS if mode == 'daily' else weekly_steps(day))
    if passthrough:
        label, cmd = base[-1]
        base[-1] = (label, [*cmd, *passthrough])
    return tuple(base)


def run_steps(steps: tuple[tuple[str, list[str]], ...], *, dry_run: bool = False,
              continue_on_error: bool = False) -> tuple[int, list[str]]:
    """顺序执行；返回 (退出码, 失败步骤标签)。

    ``continue_on_error=False`` 时任一非零即停（日更只有一个子进程，原样透传其退出码）；
    ``True`` 时记录失败继续跑完，最后返回 1 作为汇总退出码（周度批按 prompt 要求）。
    """
    failures: list[str] = []
    for label, cmd in steps:
        print(f'[maint] >> {label}: {" ".join(cmd)}', flush=True)
        if dry_run:
            continue
        started = time.perf_counter()
        rc = subprocess.run(cmd, cwd=str(ROOT), check=False).returncode
        print(f'[maint] << {label}: rc={rc} elapsed={time.perf_counter() - started:.1f}s',
              flush=True)
        if rc != 0:
            failures.append(f'{label}（rc={rc}）')
            if not continue_on_error:
                return rc, failures
    if failures:
        return 1, failures
    return 0, failures


def _parse(argv: list[str] | None) -> tuple[argparse.Namespace, list[str]]:
    raw = list(sys.argv[1:] if argv is None else argv)
    passthrough: list[str] = []
    if '--' in raw:
        split = raw.index('--')
        raw, passthrough = raw[:split], raw[split + 1:]
    parser = argparse.ArgumentParser(
        prog='python tools/maintenance_entry.py',
        description='数据维护统一入口：交易日跑日更，非交易日跑周度维护',
    )
    parser.add_argument('--force', choices=('daily', 'weekly'),
                        help='强制指定跑哪套（手动补跑用）；缺省按当天是否交易日自动判定')
    parser.add_argument('--date', type=date.fromisoformat, default=None,
                        help='用于判定路由的日期（YYYY-MM-DD，默认今天）')
    parser.add_argument('--dry-run', action='store_true',
                        help='只打印判定与步骤，不取锁、不执行')
    args = parser.parse_args(raw)
    return args, passthrough


def main(argv: list[str] | None = None) -> int:
    args, passthrough = _parse(argv)
    day = args.date or date.today()
    mode = decide_mode(day, args.force)
    steps = steps_for(mode, day, passthrough)
    print(f'[maint] 日期 {day} {day.strftime("%A")} → {mode}'
          f'（{"强制" if args.force else "按交易日历自动判定"}）', flush=True)

    if args.dry_run:
        for label, cmd in steps:
            print(f'[maint]   {label}: {" ".join(cmd)}')
        return 0

    lock = acquire_lock(mode, day)
    if lock is None:
        sys.stderr.write(f'[maint] 已有实例在跑，本次退出（退出码 {EXIT_LOCK_BUSY}）；'
                         f'见 {LOG_PATH}\n')
        return EXIT_LOCK_BUSY

    started = time.perf_counter()
    _log(f'[start] mode={mode} day={day} pid={os.getpid()}')
    try:
        rc, failures = run_steps(steps, continue_on_error=(mode == 'weekly'))
        if failures:
            print(f'[maint] 失败清单（{len(failures)}/{len(steps)}）：'
                  f'{"；".join(failures)}', flush=True)
            _log(f'[fail] mode={mode} day={day} steps={";".join(failures)}')
    finally:
        _log(f'[end] mode={mode} day={day} pid={os.getpid()} '
             f'elapsed={time.perf_counter() - started:.1f}s')
        release_lock(lock)
    return rc


if __name__ == '__main__':
    sys.exit(main())
