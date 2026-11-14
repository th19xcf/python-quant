"""运行中可观测：进度文件与净值增量文件（只读观测，不改变计算）。

- ``progress.json``：每 250 个交易日或每 5 秒（先到者）原子覆盖写一次；
- ``daily_nav.partial.csv``：分段追加，成功收尾后删除，崩溃时保留供排查；
- 可用 ``--no-progress`` 关闭；关闭时仅少写这两个文件，最终 metrics 与开启时逐位一致。

接入方式：在运行进程内临时包装引擎的逐日主循环，把引擎自己的权益曲线换成
带观察者的 list 子类。不改引擎源码、不改任何数值路径。
"""
from __future__ import annotations

import contextlib
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


def _to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _provisional(points: List[Tuple[date, float]]) -> Dict[str, Any]:
    if len(points) < 2:
        return {'provisional': True, 'points': len(points),
                'annual': None, 'max_drawdown': None}
    start_day, start_eq = points[0]
    end_day, end_eq = points[-1]
    years = (end_day - start_day).days / 365.25
    annual = (end_eq / start_eq) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    peak, max_dd = points[0][1], 0.0
    for _, equity in points:
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0)
    return {
        'provisional': True,
        'points': len(points),
        'start': start_day.isoformat(),
        'end': end_day.isoformat(),
        'annual': round(annual, 6),
        'max_drawdown': round(max_dd, 6),
    }


class ProgressReporter:
    def __init__(self, run_dir: Path, enabled: bool = True,
                 every_days: int = 250, every_seconds: float = 5.0):
        self.run_dir = run_dir
        self.enabled = enabled
        self.every_days = every_days
        self.every_seconds = every_seconds
        self.total_days = 0
        self.processed = 0
        self.points: List[Tuple[date, float]] = []
        self.current_date: str | None = None
        self.started_at = datetime.now()
        self._last_write = 0.0
        self._nav_count = 0
        self._nav_handle = None
        self._nav_path = run_dir / 'daily_nav.partial.csv'
        self._progress_path = run_dir / 'progress.json'

    # -- 引擎挂钩 ---------------------------------------------------------
    def start(self, total_days: int) -> None:
        self.total_days = total_days
        if not self.enabled:
            return
        self.run_dir.mkdir(parents=True, exist_ok=True)
        # 句柄跨多次 flush 存活，由 finish()/close()/fail() 负责关闭，故不用 with。
        self._nav_handle = open(  # noqa: SIM115
            self._nav_path, 'w', encoding='utf-8', newline='\n')
        self._nav_handle.write('date,equity\n')
        self._nav_handle.flush()
        self._write_progress('running')
        self._last_write = time.monotonic()

    def observe(self, item: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        day = _to_date(item.get('date'))
        equity = item.get('equity')
        if day is None or equity is None:
            return
        self.processed += 1
        self.points.append((day, float(equity)))
        self.current_date = day.isoformat()
        now = time.monotonic()
        if (self.processed % self.every_days == 0
                or now - self._last_write >= self.every_seconds):
            self._flush()

    def _flush(self) -> None:
        self._write_progress('running')
        if self._nav_handle is not None:
            new = self.points[self._nav_count:]
            self._nav_handle.writelines(f'{d.isoformat()},{e:.2f}\n' for d, e in new)
            self._nav_handle.flush()
            self._nav_count = len(self.points)
        self._last_write = time.monotonic()

    def _write_progress(self, status: str) -> None:
        percent = (100.0 * self.processed / self.total_days) if self.total_days else 0.0
        payload = {
            'status': status,
            'updated_at': datetime.now().isoformat(timespec='seconds'),
            'started_at': self.started_at.isoformat(timespec='seconds'),
            'current_date': self.current_date,
            'processed_days': self.processed,
            'total_days': self.total_days,
            'percent': round(percent, 2),
            'metrics_window': _provisional(self.points),
        }
        self.run_dir.mkdir(parents=True, exist_ok=True)
        tmp = self._progress_path.with_name(self._progress_path.name + '.tmp')
        import json
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                       encoding='utf-8', newline='\n')
        os.replace(tmp, self._progress_path)

    def finish(self, metrics: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        self._flush()
        if self._nav_handle is not None:
            self._nav_handle.close()
            self._nav_handle = None
        payload = {
            'status': 'finished',
            'updated_at': datetime.now().isoformat(timespec='seconds'),
            'started_at': self.started_at.isoformat(timespec='seconds'),
            'current_date': self.current_date,
            'processed_days': self.processed,
            'total_days': self.total_days,
            'percent': 100.0 if self.total_days else 0.0,
            'metrics_window': {
                'provisional': False,
                'annual': metrics.get('annual'),
                'max_drawdown': metrics.get('max_drawdown'),
            },
            'nav_partial_policy': 'deleted_after_success',
        }
        import json
        tmp = self._progress_path.with_name(self._progress_path.name + '.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                       encoding='utf-8', newline='\n')
        os.replace(tmp, self._progress_path)
        self._nav_path.unlink(missing_ok=True)

    def close(self) -> None:
        if self._nav_handle is not None:
            self._nav_handle.close()
            self._nav_handle = None

    def fail(self, message: str) -> None:
        """失败时落盘 status=failed，并保留 daily_nav.partial.csv 供排查。"""
        if not self.enabled:
            return
        if self._nav_handle is not None:
            self._nav_handle.close()
            self._nav_handle = None
        self._write_progress('failed')
        import json
        payload = json.loads(self._progress_path.read_text(encoding='utf-8'))
        payload['error'] = message[:1000]
        payload['nav_partial_policy'] = 'kept_on_failure'
        tmp = self._progress_path.with_name(self._progress_path.name + '.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                       encoding='utf-8', newline='\n')
        os.replace(tmp, self._progress_path)


    def stop(self) -> None:
        """用户停止：落盘 status=stopped，保留 daily_nav.partial.csv 供续跑排查。"""
        if not self.enabled:
            return
        if self._nav_handle is not None:
            self._nav_handle.close()
            self._nav_handle = None
        self._write_progress('stopped')
        import json
        payload = json.loads(self._progress_path.read_text(encoding='utf-8'))
        payload['error'] = '用户停止：未完成，partial 净值保留在 run 目录'
        payload['nav_partial_policy'] = 'kept_on_stop'
        tmp = self._progress_path.with_name(self._progress_path.name + '.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                       encoding='utf-8', newline='\n')
        os.replace(tmp, self._progress_path)


class _ProgressCurve(list):
    """list 子类：追加权益点时同步给观察者，其余行为与 list 完全一致。"""

    def __init__(self, base: List[Dict[str, Any]], observer: Callable[[Dict[str, Any]], None]):
        super().__init__(base)
        self._observer = observer

    def append(self, item: Dict[str, Any]) -> None:
        super().append(item)
        self._observer(item)


@contextlib.contextmanager
def progress_scope(reporter: ProgressReporter) -> Iterator[None]:
    """临时包装引擎逐日主循环；退出时恢复原方法。"""
    if not reporter.enabled:
        yield
        return
    from src.backtest.engine.execution_core import EventBacktestRunner

    original = EventBacktestRunner._run_trading_loop

    def patched(inner_self, *args, **kwargs):
        ctx = kwargs.get('ctx')
        total = len(kwargs.get('sorted_dates') or [])
        reporter.start(total)
        if ctx is not None:
            ctx.equity_curve = _ProgressCurve(ctx.equity_curve, reporter.observe)
        return original(inner_self, *args, **kwargs)

    EventBacktestRunner._run_trading_loop = patched
    try:
        yield
    finally:
        EventBacktestRunner._run_trading_loop = original
