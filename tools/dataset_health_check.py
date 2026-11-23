#!/usr/bin/env python3

r"""每日数据集体检：按契约注册表扫磁盘，输出一页可读报告。

检查项（与 ``src/data/dataset_contracts.py`` 共用同一份契约）：

1. 文件存在性 / 行数（空表即异常）；
2. schema：列集合、列序、dtype；
3. 日期列：必须存在且为 ``Date``；
4. 空值率：契约声明的关键列（日期、主键）出现任何空值即异常；
5. ``max_date`` 与 freshness 阈值：滞后超过 ``max_lag_days`` 自然日即异常；
6. 登记对账（warn，不计入退出码）：ledger 最近一次写入是否落后于磁盘、
   目录级 manifest 登记行数是否落后于磁盘实测（D-01 类滞后会自动暴露）。

只读：不写 ``data/**``，只写 ``artifacts/dataset_health/`` 下的报告。
异常时退出码为 1，可直接挂日更收尾或 CI。

用法::

    .venv\\Scripts\\python.exe tools/dataset_health_check.py
    .venv\\Scripts\\python.exe tools/dataset_health_check.py --only quotes_stock,st_daily
    .venv\\Scripts\\python.exe tools/dataset_health_check.py --as-of 2026-11-20 --json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.dataset_contracts import (  # noqa: E402
    DatasetContract,
    contract_keys,
    iter_contracts,
    registration_summary,
    resolve_files,
    validate_schema,
)

DEFAULT_ROOT = ROOT / 'data' / 'local_store'
DEFAULT_OUT_DIR = ROOT / 'artifacts' / 'dataset_health'

ERROR = 'error'
WARN = 'warn'


def _parse_date(value: str) -> date:
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(value[:10] if '-' in value else value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f'无法解析日期: {value}')


def _as_date(value: Any) -> date | None:
    """把 Date / datetime / ISO 字符串统一成 date（期货日期列是 String）。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _manifest_rows(payload: dict[str, Any], contract: DatasetContract) -> int | None:
    """从目录级 manifest 取本数据集的登记行数（取不到返回 None）。"""
    if contract.key.startswith('quotes_'):
        kind = contract.key.split('_', 1)[1]
        universe = payload.get('universe') or {}
        return (universe.get(kind) or {}).get('rows')
    entries = payload.get('files')
    if not isinstance(entries, list):
        return payload.get('rows')
    prefix = contract.manifest_prefix or ''
    total = 0
    hit = False
    for item in entries:
        if not isinstance(item, dict):
            continue
        path = str(item.get('path') or '').replace('\\', '/')
        if prefix and not path.startswith(prefix):
            continue
        rows = item.get('rows')
        if isinstance(rows, int):
            total += rows
            hit = True
    return total if hit else None


def _scan(files: list[Path], contract: DatasetContract) -> dict[str, Any]:
    """一次扫描拿到行数、日期边界与关键列空值数。"""
    null_cols = [c for c in contract.non_nullable if c != contract.date_column]
    exprs = [
        pl.len().alias('rows'),
        pl.col(contract.date_column).min().alias('min_date'),
        pl.col(contract.date_column).max().alias('max_date'),
        pl.col(contract.date_column).null_count().alias('null_date'),
    ]
    exprs += [pl.col(c).null_count().alias(f'null_{c}') for c in null_cols]
    frame = pl.scan_parquet([str(p) for p in files]).select(exprs).collect()
    return frame.row(0, named=True)


def check_dataset(contract: DatasetContract, root: Path, today: date,
                  ledger: dict[str, Any]) -> dict[str, Any]:
    findings: list[dict[str, str]] = []
    files = resolve_files(contract, root)
    result: dict[str, Any] = {
        'key': contract.key,
        'label': contract.label,
        'category': contract.category,
        'partition': contract.partition,
        'path_pattern': contract.pattern,
        'files': len(files),
        'rows': None,
        'min_date': None,
        'max_date': None,
        'lag_days': None,
        'date_column': contract.date_column,
        'null_rates': {},
        'findings': findings,
    }
    if not files:
        findings.append({'level': ERROR, 'kind': 'missing',
                         'detail': f'未找到任何文件：{contract.pattern}'})
        result['status'] = 'missing'
        return result

    try:
        validate_schema(contract, pl.read_parquet_schema(files[0]))
    # 兜底理由：schema 读不出或契约不符都要落成体检项，而不是让整轮中断
    except Exception as exc:  # noqa: BLE001
        findings.append({'level': ERROR, 'kind': 'schema', 'detail': f'{files[0].name}: {exc}'})

    try:
        stats = _scan(files, contract)
    # 兜底理由：单数据集读失败不能让其余数据集失去体检结果
    except Exception as exc:  # noqa: BLE001
        findings.append({'level': ERROR, 'kind': 'scan', 'detail': f'扫描失败: {exc}'})
        result['status'] = 'error'
        return result

    rows = int(stats['rows'])
    min_date = _as_date(stats['min_date'])
    max_date = _as_date(stats['max_date'])
    result['rows'] = rows
    result['min_date'] = min_date.isoformat() if min_date else None
    result['max_date'] = max_date.isoformat() if max_date else None
    if rows == 0:
        findings.append({'level': ERROR, 'kind': 'empty', 'detail': '全表 0 行'})

    if rows:
        for column in contract.non_nullable:
            key = 'null_date' if column == contract.date_column else f'null_{column}'
            nulls = int(stats.get(key) or 0)
            ratio = nulls / rows
            result['null_rates'][column] = round(ratio, 6)
            if nulls:
                findings.append({
                    'level': ERROR, 'kind': 'nulls',
                    'detail': f'{column} 空值 {nulls:,}/{rows:,}（{ratio:.2%}）'})

    if max_date is not None and contract.max_lag_days is not None:
        lag = (today - max_date).days
        result['lag_days'] = lag
        if lag > contract.max_lag_days:
            findings.append({
                'level': ERROR, 'kind': 'stale',
                'detail': f'max_date={max_date} 滞后 {lag} 自然日'
                          f'（阈值 {contract.max_lag_days}）'})

    entry = (ledger.get('datasets') or {}).get(contract.key) or {}
    last_write = entry.get('last_write') or {}
    result['ledger_max_date'] = last_write.get('max_date')
    if not last_write:
        findings.append({'level': WARN, 'kind': 'unregistered',
                         'detail': 'ledger 无写入登记（尚未走受控写入或为历史数据）'})
    elif last_write.get('max_date') and result['max_date'] \
            and str(last_write['max_date']) < str(result['max_date']):
        findings.append({
            'level': WARN, 'kind': 'ledger_lag',
            'detail': f'ledger max_date={last_write["max_date"]} < 磁盘 {result["max_date"]}'})

    if contract.manifest:
        manifest_path = root / contract.manifest
        if manifest_path.is_file():
            try:
                payload = json.loads(manifest_path.read_text(encoding='utf-8'))
            # 兜底理由：manifest 损坏必须显式告警，不能被当成「未登记」静默跳过
            except (OSError, ValueError) as exc:
                payload = None
                findings.append({
                    'level': WARN, 'kind': 'manifest_invalid',
                    'detail': f'{contract.manifest} 无法解析：{exc}'})
            declared = _manifest_rows(payload, contract) if payload is not None else None
            result['manifest_rows'] = declared
            if declared is not None and rows and declared != rows:
                findings.append({
                    'level': WARN, 'kind': 'manifest_lag',
                    'detail': f'{contract.manifest} 登记 {declared:,} 行，磁盘实测 {rows:,} 行'
                              f'（差 {rows - declared:+,}）'})
        else:
            findings.append({'level': WARN, 'kind': 'manifest_missing',
                             'detail': f'缺少目录级 manifest：{contract.manifest}'})

    errors = [f for f in findings if f['level'] == ERROR]
    result['status'] = 'error' if errors else ('warn' if findings else 'ok')
    return result


def _fmt_int(value: Any) -> str:
    return f'{value:,}' if isinstance(value, int) else '—'


def render_markdown(results: list[dict[str, Any]], today: date,
                    ledger_updated: str | None) -> str:
    errors = sum(r['status'] == 'error' for r in results)
    warns = sum(r['status'] == 'warn' for r in results)
    lines = [
        f'# 数据集体检报告 {today.isoformat()}',
        '',
        f'- 覆盖数据集：{len(results)}；异常 {errors}；仅告警 {warns}；健康 {len(results) - errors - warns}',
        f'- 自动登记 ledger 最近更新：{ledger_updated or "（无）"}',
        '- 口径：行数与日期边界按磁盘实测（扫描 parquet），不使用任何人工登记值',
        '',
        '| 数据集 | 文件 | 行数 | 日期列 | min_date | max_date | 滞后(天) | 状态 |',
        '|---|---:|---:|---|---|---|---:|---|',
    ]
    for r in results:
        lines.append(
            f'| {r["key"]}（{r["label"]}） | {_fmt_int(r["files"])} | {_fmt_int(r["rows"])} '
            f'| {r["date_column"]} | {r["min_date"] or "—"} | {r["max_date"] or "—"} '
            f'| {r["lag_days"] if r["lag_days"] is not None else "—"} | {r["status"]} |')
    details = [(r, f) for r in results for f in r['findings']]
    if details:
        lines += ['', '## 异常与告警明细', '']
        for r, f in details:
            lines.append(f'- [{f["level"]}] {r["key"]} · {f["kind"]}：{f["detail"]}')
    return '\n'.join(lines) + '\n'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python tools/dataset_health_check.py',
        description='按数据集契约做每日只读体检（不写 data/**）')
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT,
                        help='列存根目录，默认 data/local_store')
    parser.add_argument('--as-of', type=_parse_date, default=None,
                        help='观察日（默认系统当天，用于新鲜度判定）')
    parser.add_argument('--only', default=None,
                        help='只查指定数据集，逗号分隔（如 quotes_stock,st_daily）')
    parser.add_argument('--out-dir', type=Path, default=DEFAULT_OUT_DIR,
                        help='报告输出目录，默认 artifacts/dataset_health')
    parser.add_argument('--json', action='store_true', help='同时以 JSON 输出报告')
    args = parser.parse_args(argv)

    today = args.as_of or date.today()
    keys = [k.strip() for k in args.only.split(',') if k.strip()] if args.only else None
    if keys:
        unknown = [k for k in keys if k not in contract_keys()]
        if unknown:
            print(f'未注册的数据集：{unknown}；可用：{contract_keys()}', file=sys.stderr)
            return 2
    contracts = iter_contracts(keys)
    ledger = registration_summary(args.root)

    results = [check_dataset(c, args.root, today, ledger) for c in contracts]
    errors = sum(r['status'] == 'error' for r in results)
    warns = sum(r['status'] == 'warn' for r in results)

    print(f'[体检] {today.isoformat()} 覆盖 {len(results)} 个数据集：'
          f'异常 {errors} / 告警 {warns} / 健康 {len(results) - errors - warns}')
    for r in results:
        flag = '' if r['status'] == 'ok' else f' <- {r["status"]}'
        print(f'  {r["key"]:<20} files={_fmt_int(r["files"]):>7} rows={_fmt_int(r["rows"]):>12} '
              f'max_date={r["max_date"] or "—":<10} lag={r["lag_days"]}{flag}')
        for finding in r['findings']:
            print(f'      [{finding["level"]}] {finding["kind"]}: {finding["detail"]}')

    args.out_dir.mkdir(parents=True, exist_ok=True)
    report = {
        'as_of': today.isoformat(),
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'summary': {'datasets': len(results), 'error': errors, 'warn': warns,
                    'ok': len(results) - errors - warns},
        'ledger_updated_at': ledger.get('updated_at'),
        'results': results,
    }
    md_path = args.out_dir / f'{today.isoformat()}.md'
    md_path.write_text(
        render_markdown(results, today, ledger.get('updated_at')), encoding='utf-8')
    if args.json:
        json_path = args.out_dir / f'{today.isoformat()}.json'
        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    latest = args.out_dir / 'latest.md'
    latest.write_text(md_path.read_text(encoding='utf-8'), encoding='utf-8')
    print(f'[体检] 报告：{md_path}')
    from src.data.dataset_contracts import unregistered_writers  # noqa: PLC0415

    unregistered = unregistered_writers()
    if unregistered:
        print(f'[体检] 未登记写数据脚本 {len(unregistered)} 个'
              f'（tools/ 下写数据但不在步骤清单）：' + '、'.join(unregistered))
    from src.data.raw_write_scan import scan_raw_writes  # noqa: PLC0415

    raw = scan_raw_writes()
    flag = '告警：新增裸写' if raw['new_writes'] else '未超基线'
    print(f'[体检] 数据集裸写 tools {raw["tools_files"]} 文件/{raw["tools_calls"]} 处，'
          f'src {raw["src_files"]} 文件/{raw["src_calls"]} 处；'
          f'已豁免 {raw["exempt_calls"]} 处；'
          f'清单口径 {raw["doc_scope_calls"]} 处 vs 基线 {raw["baseline"]}，'
          f'差 {raw["delta"]:+d}，{flag}')
    from src.data.quality.fund_price_basis import registry_consistency  # noqa: PLC0415

    pseudo_issues = registry_consistency(args.root)
    if pseudo_issues:
        print('[体检] 基金伪数据台账告警：' + '；'.join(pseudo_issues))
    else:
        print('[体检] 基金伪数据台账一致：段数/行数合计/每基金段数上限均通过')
    return 1 if errors or unregistered or pseudo_issues else 0


if __name__ == '__main__':
    raise SystemExit(main())
