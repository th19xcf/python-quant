#!/usr/bin/env python3
"""`logs/data_changelog.csv` 收口校验：只增不减、末行完整、与备份镜像三项比对。

三条硬校验（任一不满足即退出码 1）：

1. **末行完整**：文件必须以 ``\\n`` 结尾，且末行的列数与表头一致（可解析）。
2. **只增不减**：与基线（默认 ``G:\\git\\python-quant\\logs\\data_changelog.csv``）
   比对，当前行数不得少于基线，且基线内容必须是当前文件的**字节前缀**。
3. **三项比对**：输出当前与基线的 SHA256、字节数、行数；行数相同而 SHA256/字节
   不同，说明历史行被重写（同样判失败）。

值域缺陷（``action`` 越界、``bytes=-1``、空 sha256、反斜杠路径等）只统计并打印，
不作为失败条件——2026-11-05 那批历史行本就有这些形态，且按约定不回写、不重排。

用法::

    python tools/check_changelog.py
    python tools/check_changelog.py --file tmp.csv --baseline tmp_base.csv
    python tools/check_changelog.py --no-baseline
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / 'logs' / 'data_changelog.csv'
DEFAULT_BASELINE = Path(r'G:\git\python-quant\logs\data_changelog.csv')
DEFAULT_FAILURES = ROOT / 'logs' / 'data_changelog_failures.csv'
EXPECTED_FIELDS = [
    'run_id', 'timestamp', 'task', 'action', 'path',
    'bytes', 'sha256', 'prev_sha256', 'operator', 'reason',
]
ACTIONS = {'create', 'update', 'delete'}
BOM = b'\xef\xbb\xbf'


@dataclass
class LedgerReport:
    """单个台账文件的体检结果。"""

    path: Path
    size_bytes: int = 0
    sha256: str = ''
    row_count: int = 0
    ends_with_newline: bool = False
    has_bom: bool = False
    crlf_rows: int = 0
    header_ok: bool = False
    bad_column_rows: list[int] = field(default_factory=list)
    value_violations: dict[str, int] = field(default_factory=dict)

    @property
    def last_row_complete(self) -> bool:
        return self.ends_with_newline and not self.bad_column_rows


@dataclass
class BaselineReport:
    """与基线（G: 镜像）的比对结果。"""

    path: Path
    available: bool
    row_count: int = 0
    size_bytes: int = 0
    sha256: str = ''
    rows_regressed: bool = False
    is_prefix: bool = False
    same_row_count_bytes_differ: bool = False


@dataclass
class CheckResult:
    ledger: LedgerReport
    baseline: BaselineReport | None
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _parse_rows(lines: list[str]) -> list[list[str]]:
    return [next(csv.reader([line])) if line else [] for line in lines]


def inspect_ledger(path: Path) -> LedgerReport:
    """读取并体检台账：编码、行尾、末行完整、列数、值域。"""
    raw = path.read_bytes()
    report = LedgerReport(path=path, size_bytes=len(raw), sha256=_sha256(raw))
    report.has_bom = raw.startswith(BOM)
    text = raw[len(BOM):].decode('utf-8') if report.has_bom else raw.decode('utf-8')

    report.ends_with_newline = text.endswith('\n')
    lines = text.split('\n')
    if lines and lines[-1] == '':
        lines = lines[:-1]
    report.crlf_rows = sum(1 for line in lines if line.endswith('\r'))
    lines = [line[:-1] if line.endswith('\r') else line for line in lines]
    if not lines:
        return report

    header = next(csv.reader([lines[0]]))
    report.header_ok = header == EXPECTED_FIELDS
    width = len(header)
    rows = _parse_rows(lines[1:])
    report.row_count = len(rows)

    violations = {'action': 0, 'bytes': 0, 'sha256': 0, 'path': 0}
    for offset, cells in enumerate(rows, start=2):
        if len(cells) != width:
            report.bad_column_rows.append(offset)
            continue
        record = dict(zip(header, cells, strict=True))
        if record.get('action', '') not in ACTIONS:
            violations['action'] += 1
        size = record.get('bytes', '').strip()
        if size and not size.isdigit():
            violations['bytes'] += 1
        digest = record.get('sha256', '').strip()
        if not digest or not (
            len(digest) == 64 and all(c in '0123456789abcdefABCDEF' for c in digest)
        ):
            violations['sha256'] += 1
        rel = record.get('path', '')
        if not rel or '\\' in rel or Path(rel).is_absolute() or rel.startswith('../'):
            violations['path'] += 1
    report.value_violations = {k: v for k, v in violations.items() if v}
    return report


def compare_baseline(current: LedgerReport, baseline_path: Path) -> BaselineReport:
    """与基线比对行数、SHA256、字节数，并判定「基线是否为当前文件前缀」。"""
    if not baseline_path.exists():
        return BaselineReport(path=baseline_path, available=False)
    base_raw = baseline_path.read_bytes()
    current_raw = current.path.read_bytes()
    base_text = base_raw.decode('utf-8')
    base_lines = base_text.split('\n')
    # 无论基线是否以换行结尾，都只统计「完整行」；末段（空串或残行）不计入
    base_lines = base_lines[:-1]
    base_rows = max(len(base_lines) - 1, 0)
    return BaselineReport(
        path=baseline_path,
        available=True,
        row_count=base_rows,
        size_bytes=len(base_raw),
        sha256=_sha256(base_raw),
        rows_regressed=current.row_count < base_rows,
        is_prefix=current_raw.startswith(base_raw),
        same_row_count_bytes_differ=(
            current.row_count == base_rows and current_raw != base_raw
        ),
    )


def inspect_failures(path: Path) -> list[str]:
    """读取记账失败清单，返回失败记录（不含表头）。"""
    if not path.exists():
        return []
    rows = list(csv.reader(io.StringIO(path.read_text(encoding='utf-8'))))
    return [','.join(row) for row in rows[1:] if any(cell.strip() for cell in row)]


def check_ledger(
    ledger_path: Path = DEFAULT_LEDGER,
    baseline_path: Path | None = DEFAULT_BASELINE,
    failures_path: Path | None = None,
) -> CheckResult:
    """执行三条硬校验 + 记账失败清单校验，返回结构化结果（不打印、不退出）。"""
    ledger = inspect_ledger(ledger_path)
    baseline = compare_baseline(ledger, baseline_path) if baseline_path else None
    failures: list[str] = []

    if ledger.has_bom:
        failures.append(f'文件带 UTF-8 BOM：{ledger.path}')
    if not ledger.ends_with_newline:
        failures.append('末行未以换行结尾（疑似被截断）')
    if not ledger.header_ok:
        failures.append(f'表头与 10 列约定不符：{ledger.path}')
    if ledger.bad_column_rows:
        preview = ', '.join(str(n) for n in ledger.bad_column_rows[:5])
        failures.append(
            f'{len(ledger.bad_column_rows)} 行列数不符（行号 {preview} …）')

    if failures_path is not None:
        recorded = inspect_failures(failures_path)
        if recorded:
            preview = ' | '.join(recorded[:3])
            failures.append(
                f'记账失败 {len(recorded)} 条（数据已落盘但未入台账）：'
                f'{failures_path}：{preview}')

    if baseline is not None:
        if not baseline.available:
            failures.append(f'基线文件不存在，无法校验只增不减：{baseline.path}')
        else:
            if baseline.rows_regressed:
                failures.append(
                    f'行数倒退：当前 {ledger.row_count} 行 < 基线 {baseline.row_count} 行')
            if not baseline.is_prefix:
                failures.append(
                    '基线不是当前文件的字节前缀：历史内容被改写/重排，违反只增不减')
            if baseline.same_row_count_bytes_differ:
                failures.append(
                    f'行数同为 {baseline.row_count} 但字节/SHA256 不同：内容被原地改写')

    return CheckResult(ledger=ledger, baseline=baseline, failures=failures)


def render(result: CheckResult) -> str:
    ledger = result.ledger
    lines = [
        f'台账：{ledger.path}',
        f'  SHA256={ledger.sha256}',
        f'  字节={ledger.size_bytes}  行数={ledger.row_count}',
        f'  末行完整={ledger.last_row_complete}  BOM={ledger.has_bom}  '
        f'CRLF行={ledger.crlf_rows}  表头10列={ledger.header_ok}',
    ]
    if ledger.bad_column_rows:
        lines.append(f'  列数不符行号：{ledger.bad_column_rows[:20]}')
    if ledger.value_violations:
        lines.append(
            '  历史值域缺陷（仅统计，不回写）：'
            + '，'.join(f'{k}={v}' for k, v in ledger.value_violations.items()))
    base = result.baseline
    if base is not None:
        if not base.available:
            lines.append(f'基线：{base.path}（不存在）')
        else:
            lines.append(
                f'基线：{base.path}\n'
                f'  SHA256={base.sha256}\n'
                f'  字节={base.size_bytes}  行数={base.row_count}\n'
                f'  前缀关系={base.is_prefix}  行数倒退={base.rows_regressed}')
    if result.failures:
        lines.append('结论：FAIL')
        for item in result.failures:
            lines.append(f'  - {item}')
    else:
        lines.append('结论：PASS')
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='台账收口校验（只增不减/末行完整/G: 比对）')
    parser.add_argument('--file', type=Path, default=DEFAULT_LEDGER, help='待校验台账')
    parser.add_argument('--baseline', type=Path, default=DEFAULT_BASELINE,
                        help='基线文件（默认 G: 镜像）')
    parser.add_argument('--no-baseline', action='store_true', help='跳过基线与只增不减校验')
    parser.add_argument('--failures', type=Path, default=DEFAULT_FAILURES,
                        help='记账失败清单；非空即判失败')
    parser.add_argument('--no-failures', action='store_true', help='跳过记账失败清单校验')
    args = parser.parse_args(argv)

    baseline = None if args.no_baseline else args.baseline
    failures_path = None if args.no_failures else args.failures
    result = check_ledger(args.file, baseline, failures_path)
    print(render(result))
    return 0 if result.ok else 1


if __name__ == '__main__':
    sys.exit(main())
