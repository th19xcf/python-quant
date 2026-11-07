#!/usr/bin/env python3

r"""每日数据只读巡检门禁（不写 ``data/**``、不触发采集或构建）。

把三条最容易「静默失效」的链路收成一个退出码，供每日定时任务调用：

1. **因子一致性**：行情内嵌 qfq/hfq 副本 vs 权威快照，要求
   stock/index/fund 的 ``mismatch_codes`` 全为 0；
2. **新鲜度**：``freshness_config.json`` 登记的数据集不得出现 critical
   （warn 只记录、不阻断：滞后 1 个交易日是常态）；
3. **关键资产备份**：``backup_critical_assets.py`` 的 manifest 必须存在且
   sha256 全部匹配，防止快照再次整体丢失后无处可恢复。

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
) -> dict[str, Any]:
    """执行全部检查，返回结构化报告（由 CLI 决定退出码与落盘）。"""
    today = today or date.today()
    checks: dict[str, Any] = {
        'factor_consistency': _check_factors(store_root),
        'freshness': _check_freshness(store_root, config_path, today),
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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_gate(
        store_root=args.store_root,
        config_path=args.config,
        today=args.today,
        backup_dests=args.backup_dest,
        skip_backup_check=args.skip_backup_check,
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
        else:
            print(f'  [{mark}] 备份校验 {item.get("dests") or item.get("skipped")}')
    print(f'报告: {out_path}')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
