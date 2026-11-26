#!/usr/bin/env python3

r"""每日数据巡检门禁（不改任何数据表、不触发采集或构建）。

唯一写入：``meta/data_version.json`` 版本台账——第 5 项在同 digest 时不写，
版本变化才追加一条；``--no-record-version`` 可退回纯只读。

把五条最容易「静默失效」的链路收成一个退出码，供每日定时任务调用：

1. **因子一致性**：行情内嵌 qfq/hfq 副本 vs 权威快照，要求
   stock/index/fund 的 ``mismatch_codes`` 全为 0；
2. **新鲜度**：``freshness_config.json`` 登记的数据集不得出现 critical
   （warn 只记录、不阻断：滞后 1 个交易日是常态）；
3. **关键资产备份**：``backup_critical_assets.py`` 的 manifest 必须存在且
   sha256 全部匹配，防止快照再次整体丢失后无处可恢复；
4. **保鲜体检（13 行汇总）**：``freshness`` 的 12 张登记表 + 行情全集
   巡检，出现 ``error`` 即失败、``warning`` 只记录——与
   ``python -m src.data.quality.freshness --summary`` 同一判据；
5. **版本记录 + 派生表版本审计**：先 ``ensure_current`` 补记当日基础表版本
   （同 digest 不写），再 ``audit_derived``；派生表文件缺失即失败，
   ``stale`` / ``unstamped`` 只记录（与 ``python tools/data_version.py audit``
   同一判据，不触发任何重建）。同一表在上一份 ``gate_*.json`` 里也 ``stale``
   时打印「连续两次落后」并写入 ``stale_two_days``，仍只记录。

输出 JSON 报告到 ``artifacts/data_gate/``；任一项失败返回 1。

用法::

    .venv\Scripts\python.exe tools/daily_data_gate.py
    .venv\Scripts\python.exe tools/daily_data_gate.py --backup-dest E:/quant_critical_assets
    .venv\Scripts\python.exe tools/daily_data_gate.py --skip-backup-check
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_STORE_ROOT = ROOT / 'data' / 'local_store'
DEFAULT_CONFIG = ROOT / 'src' / 'data' / 'quality' / 'freshness_config.json'
DEFAULT_OUT_DIR = ROOT / 'artifacts' / 'data_gate'
DEFAULT_BACKUP_DEST = ROOT / 'backups' / 'critical_assets'


def _check_factors(store_root: Path) -> dict[str, Any]:
    from src.data.local_store.store import (  # noqa: PLC0415
        LocalFactorStore,
        LocalQuoteStore,
        verify_quote_factor_consistency,
    )

    quote_store = LocalQuoteStore(store_root, enabled=True)
    factor_store = LocalFactorStore(store_root, enabled=True)
    detail = verify_quote_factor_consistency(quote_store, factor_store)
    if 'error' in detail:
        return {'passed': False, 'error': detail['error'], 'detail': detail}
    mismatch = {
        kind: stats.get('mismatch_codes', 0)
        for kind, stats in detail.items() if isinstance(stats, dict)
    }
    return {
        'passed': all(count == 0 for count in mismatch.values()),
        'mismatch_codes': mismatch,
        'detail': detail,
    }


def _check_freshness(store_root: Path, config_path: Path, today: date) -> dict[str, Any]:
    from src.data.quality.freshness import check_all, load_config  # noqa: PLC0415

    results = check_all(store_root, load_config(config_path), today)
    critical = [r.label for r in results if r.severity == 'critical']
    warn = [r.label for r in results if r.severity == 'warn']
    return {
        'passed': not critical,
        'total': len(results),
        'ok': sum(1 for r in results if r.severity == 'ok'),
        'warn': len(warn),
        'critical': len(critical),
        'critical_labels': critical,
        'warn_labels': warn,
    }


def _check_freshness_summary(store_root: Path) -> dict[str, Any]:
    """保鲜体检 13 行汇总（统一入口 ``freshness`` 的表体检）。"""
    from src.data.quality.freshness import run_check  # noqa: PLC0415

    try:
        report = run_check(store_root)
    except (FileNotFoundError, ValueError) as exc:
        return {'passed': False, 'error': f'{type(exc).__name__}: {exc}'}
    return {
        'passed': report.ok,
        'tables': len(report.rows),
        'universe': report.universe,
        'errors': [row.rel_path for row in report.errors],
        'warnings': [row.rel_path for row in report.warnings],
    }


def _previous_gate_report(out_dir: Path, today: date) -> Path | None:
    """取最近一份「today 之前」的门禁报告（``gate_YYYYMMDD.json``）。"""
    best: tuple[date, Path] | None = None
    if not out_dir.is_dir():
        return None
    for path in out_dir.glob('gate_*.json'):
        try:
            day = datetime.strptime(path.stem.split('_', 1)[1], '%Y%m%d').date()
        except (IndexError, ValueError):
            continue
        if day >= today:
            continue
        if best is None or day > best[0]:
            best = (day, path)
    return best[1] if best else None


def _persistent_stale(stale_now: list[str], today: date,
                      out_dir: Path) -> dict[str, Any]:
    """同一派生表在上一份门禁报告里也 ``stale`` → 报「连续两日未消」。

    只读历史报告，不写盘、不改判定：命中项仅记录，仍不阻断门禁。
    """
    previous = _previous_gate_report(out_dir, today)
    if previous is None:
        return {'compared_with': None, 'tables': []}
    try:
        payload = json.loads(previous.read_text(encoding='utf-8'))
        before = payload['checks']['version_audit'].get('stale') or []
    except (OSError, KeyError, TypeError, ValueError):
        return {'compared_with': previous.name, 'tables': [], 'error': '上一份报告不可读'}
    return {'compared_with': previous.name,
            'tables': sorted(set(stale_now) & {str(name) for name in before})}


def _check_version_audit(store_root: Path, record: bool = True,
                         today: date | None = None,
                         out_dir: Path | None = None) -> dict[str, Any]:
    """版本记录 + 派生表版本审计（与 ``tools/data_version.py`` 同判据，不重建）。

    ``record=True`` 时先 ``ensure_current``：台账缺失或磁盘版本已变才追加一版
    （同 digest 不写），保证 ``audit`` 比的是当日基础表版本。全流程只写
    ``meta/data_version.json``，不碰任何数据表。
    """
    from src.data.data_version import audit_derived, ensure_current  # noqa: PLC0415

    try:
        recorded = ensure_current(store_root, note='daily gate').get('version') if record else None
        result = audit_derived(store_root)
    except Exception as exc:  # noqa: BLE001
        return {'passed': False, 'error': f'{type(exc).__name__}: {exc}'}
    counts = result['counts']
    stale = [row['table'] for row in result['rows'] if row['status'] == 'stale']
    return {
        'passed': counts['missing'] == 0,
        'recorded_version': recorded,
        'version': result['current']['version'],
        'counts': counts,
        'missing': [row['table'] for row in result['rows']
                    if row['status'] == 'missing'],
        'stale': stale,
        'stale_two_days': _persistent_stale(
            stale, today or date.today(), out_dir or DEFAULT_OUT_DIR),
    }


def _check_backups(dests: list[Path]) -> dict[str, Any]:
    sys.path.insert(0, str(ROOT / 'tools'))
    try:
        module = importlib.import_module('backup_critical_assets')
    except Exception as exc:  # noqa: BLE001
        return {'passed': False, 'error': f'无法加载备份工具: {exc}', 'dests': {}}

    results: dict[str, Any] = {}
    for dest in dests:
        try:
            passed, failed, problems = module.verify(ROOT, dest)
            results[str(dest)] = {
                'passed': failed == 0,
                'verified_files': passed,
                'failed': failed,
                'problems': problems[:10],
            }
        except Exception as exc:  # noqa: BLE001
            results[str(dest)] = {
                'passed': False,
                'error': f'{type(exc).__name__}: {exc}',
            }
    return {
        'passed': all(item.get('passed') for item in results.values()) and bool(results),
        'dests': results,
    }


def run_gate(
    store_root: Path = DEFAULT_STORE_ROOT,
    config_path: Path = DEFAULT_CONFIG,
    today: date | None = None,
    backup_dests: list[Path] | None = None,
    skip_backup_check: bool = False,
    record_version: bool = True,
    out_dir: Path = DEFAULT_OUT_DIR,
) -> dict[str, Any]:
    """执行全部检查，返回结构化报告（由 CLI 决定退出码与落盘）。"""
    today = today or date.today()
    checks: dict[str, Any] = {
        'factor_consistency': _check_factors(store_root),
        'freshness': _check_freshness(store_root, config_path, today),
        'freshness_summary': _check_freshness_summary(store_root),
        'version_audit': _check_version_audit(
            store_root, record=record_version, today=today, out_dir=out_dir),
    }
    if skip_backup_check:
        checks['backup'] = {'passed': True, 'skipped': True, 'dests': {}}
    else:
        checks['backup'] = _check_backups(
            backup_dests or [DEFAULT_BACKUP_DEST])
    return {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'as_of': today.isoformat(),
        'store_root': str(store_root),
        'checks': checks,
        'passed': all(item.get('passed') for item in checks.values()),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='每日数据只读巡检门禁')
    parser.add_argument('--store-root', type=Path, default=DEFAULT_STORE_ROOT)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--out-dir', type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument('--today', type=lambda s: date.fromisoformat(s),
                        default=None, help='检查基准日 YYYY-MM-DD')
    parser.add_argument('--backup-dest', action='append', type=Path, default=None,
                        help='备份目录，可重复；缺省用项目内 backups/critical_assets')
    parser.add_argument('--skip-backup-check', action='store_true',
                        help='跳过备份 manifest 校验')
    parser.add_argument('--no-record-version', action='store_true',
                        help='只审计不记录：不写 meta/data_version.json（默认会先补记一版）')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_gate(
        store_root=args.store_root,
        config_path=args.config,
        today=args.today,
        backup_dests=args.backup_dest,
        skip_backup_check=args.skip_backup_check,
        record_version=not args.no_record_version,
        out_dir=args.out_dir,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f'gate_{report["as_of"].replace("-", "")}.json'
    out_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')

    print(f'数据巡检门禁 {"通过" if report["passed"] else "失败"}（{report["as_of"]}）')
    for name, item in report['checks'].items():
        mark = 'OK' if item.get('passed') else 'FAIL'
        if name == 'factor_consistency':
            print(f'  [{mark}] 因子一致性 mismatch_codes={item.get("mismatch_codes", item.get("error"))}')
        elif name == 'freshness':
            print(f'  [{mark}] 新鲜度 ok={item.get("ok")} warn={item.get("warn")} '
                  f'critical={item.get("critical")}')
        elif name == 'freshness_summary':
            print(f'  [{mark}] 保鲜体检 表 {item.get("tables")} 行 error '
                  f'{len(item.get("errors") or [])} / warning '
                  f'{len(item.get("warnings") or [])}'
                  + (f' 错: {item["errors"]}' if item.get('errors') else '')
                  + (f' 异常: {item["error"]}' if item.get('error') else ''))
        elif name == 'version_audit':
            counts = item.get('counts') or {}
            two_days = item.get('stale_two_days') or {}
            print(f'  [{mark}] 版本审计 {item.get("version")} 派生 ok '
                  f'{counts.get("ok")} / 落后 {counts.get("stale")} / '
                  f'未标记 {counts.get("unstamped")} / 缺失 {counts.get("missing")}'
                  + (f' 缺: {item["missing"]}' if item.get('missing') else '')
                  + (f' 连续两次落后: {two_days["tables"]}'
                     f'（对比 {two_days["compared_with"]}）'
                     if two_days.get('tables') else '')
                  + (f' 异常: {item["error"]}' if item.get('error') else ''))
        else:
            print(f'  [{mark}] 备份校验 {item.get("dests") or item.get("skipped")}')
    print(f'报告: {out_path}')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
