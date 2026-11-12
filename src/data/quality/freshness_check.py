"""数据新鲜度自检：把「漏跑」变成当场可见的失败。

判定分档（交易日滞后，基准为 ``quotes/stock`` 各标的 ``end`` 的最大值）
======================================================================
- ``0 ~ LAG_OK_MAX``：正常取数窗口（当天/次日的行情本来就可能尚未落盘）
- ``LAG_OK_MAX+1 ~ LAG_WARN_MAX``：``warning``
- ``> LAG_WARN_MAX``：``error``，命令行以非零退出码结束

基础信息表（``fund_basic`` / ``index_basic`` / ``stock_basic``）不是日频，
不套日期阈值：改为比较「行情目录里存在、基础表未登记的标的数」，
0 为 ``ok``、``1 ~ BASE_INFO_WARN_MAX`` 为 ``warning``、更多为 ``error``。
已决策的例外（如封闭式基金 505888.SH）登记在 ``KNOWN_BASE_INFO_EXCEPTIONS``，
命中后判为 ``known_exception``：仍在明细里逐行列出，但不计入 warning / error。

覆盖口径来自 ``docs/数据权威路径清单.md``：本模块的 ``TABLE_SPECS`` 每一条
都必须在清单里登记，未登记即抛错（``_assert_doc_registered``）。
新增登记表时同步在这里加一行并注明 ``kind``。

用法::

    python -m src.data.quality.freshness_check
    python -m src.data.quality.freshness_check --no-doc-guard
"""
# ruff: noqa: T201  # 自检命令面向终端输出

from __future__ import annotations

import argparse
import sys
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import polars as pl

KIND_DAILY = 'daily'
KIND_EVENT = 'event'
KIND_COVERAGE = 'coverage'
KIND_BASE_INFO = 'base_info'

#: 0~1 个交易日滞后属正常取数窗口
LAG_OK_MAX = 1
#: 2~5 个交易日滞后报 warning，超过报 error
LAG_WARN_MAX = 5
#: 基础信息表允许的新增标的数上限（超过报 error）
BASE_INFO_WARN_MAX = 5

#: 已知且已决策的例外：行情里有、基础表故意不登记。
#: 命中后仍逐行列出，但判为 ``known_exception``，不计入 warning / error。
KNOWN_BASE_INFO_EXCEPTIONS: dict[str, dict[str, str]] = {
    '505888.SH': {
        'reason': '封闭式基金，全市场仅此 1 只，2019 年已退市；不为其在 fund_basic 建表记录',
        'decided': '2026-11-10',
    },
}

#: 权威行情目录（股票），其 ``date`` 最大值即「行情末日」
QUOTES_ROOT = Path('quotes') / 'stock'

ROOT = Path(__file__).resolve().parents[3]
DOC_PATH = ROOT / 'docs' / '数据权威路径清单.md'


@dataclass(frozen=True)
class TableSpec:
    """一张需要纳入新鲜度自检的本地正式表。"""

    label: str
    rel_path: str
    kind: str
    date_column: str | None = None
    quotes_dir: str | None = None


#: 与 ``docs/数据权威路径清单.md`` 一一对应；新增登记表时在此追加
TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec('个股分红', 'factors/stock_dividend.parquet', KIND_EVENT, 'notice_date'),
    TableSpec('个股复权因子', 'factors/stock_adj_factor.parquet', KIND_DAILY, 'trade_date'),
    TableSpec('市场宽度', 'meta/breadth/breadth_daily.parquet', KIND_DAILY, 'trade_date'),
    TableSpec('基金基础信息', 'meta/fund_basic.parquet', KIND_BASE_INFO,
              quotes_dir='quotes/fund'),
    TableSpec('指数基础信息', 'meta/index_basic.parquet', KIND_BASE_INFO,
              quotes_dir='quotes/index'),
    TableSpec('股票基础信息', 'meta/stock_basic.parquet', KIND_BASE_INFO,
              quotes_dir='quotes/stock'),
    TableSpec('ST 覆盖窗口', 'meta/st/st_coverage.parquet', KIND_COVERAGE, 'coverage_end'),
    TableSpec('多资产 ETF 候选池', 'meta/multi_asset/etf_pool.parquet', KIND_COVERAGE,
              'coverage_end'),
)


@dataclass
class FreshnessRow:
    """单张表的自检结果。"""

    label: str
    rel_path: str
    kind: str
    data_end: date | None
    quotes_end: date | None
    lag: int | None
    verdict: str
    detail: str = ''


@dataclass
class FreshnessReport:
    """全量自检结果。"""

    quotes_end: date | None
    rows: list[FreshnessRow] = field(default_factory=list)
    doc_uncovered: list[str] = field(default_factory=list)

    @property
    def errors(self) -> list[FreshnessRow]:
        return [row for row in self.rows if row.verdict == 'error']

    @property
    def warnings(self) -> list[FreshnessRow]:
        return [row for row in self.rows if row.verdict == 'warning']

    @property
    def known_exceptions(self) -> list[FreshnessRow]:
        return [row for row in self.rows if row.verdict == 'known_exception']

    @property
    def ok(self) -> bool:
        return not self.errors


def classify_lag(lag: int) -> str:
    """按交易日滞后分档。"""
    if lag <= LAG_OK_MAX:
        return 'ok'
    if lag <= LAG_WARN_MAX:
        return 'warning'
    return 'error'


def classify_missing(missing: int) -> str:
    """按基础表未登记的新增标的数分档。"""
    if missing <= 0:
        return 'ok'
    if missing <= BASE_INFO_WARN_MAX:
        return 'warning'
    return 'error'


def quotes_end(store_root: Path) -> date | None:
    """``quotes/stock`` 全量日线的最大 ``date``。"""
    root = store_root / QUOTES_ROOT
    if not root.is_dir():
        return None
    # 只取 date：投影子集读取，多余列不应让新鲜度检查整体失败
    found = pl.scan_parquet(
        str(root / '**' / '*.parquet'), extra_columns='ignore').select(
        pl.col('date').max()).collect().item()
    return found


def table_data_end(path: Path, column: str) -> date | None:
    """单表指定日期列的最大值；列缺失或无值返回 ``None``。"""
    try:
        frame = pl.scan_parquet(path).select(pl.col(column).max()).collect()
    except pl.exceptions.ColumnNotFoundError:
        return None
    value = frame.item()
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    if isinstance(value, date):
        return value
    return None


def count_lag(calendar: list[date], start: date | None, end: date | None) -> int | None:
    """``(start, end]`` 区间内的交易日数。"""
    if start is None or end is None:
        return None
    if start >= end:
        return 0
    right = bisect_right(calendar, end)
    left = bisect_right(calendar, start)
    return max(right - left, 0)


def _base_info_row(store_root: Path, spec: TableSpec, quotes_end_date: date) -> FreshnessRow:
    """基础信息表：按行情里出现、表内未登记的标的数判定。"""
    path = store_root / spec.rel_path
    if not path.is_file():
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'正式表不存在: {path}')
    quotes_dir = store_root / (spec.quotes_dir or '')
    quoted = {item.stem for item in quotes_dir.rglob('*.parquet')} if quotes_dir.is_dir() else set()
    registered = set(pl.read_parquet(path, columns=['ts_code'])['ts_code'].to_list())
    missing = sorted(quoted - registered)
    known = [code for code in missing if code in KNOWN_BASE_INFO_EXCEPTIONS]
    unknown = [code for code in missing if code not in KNOWN_BASE_INFO_EXCEPTIONS]
    if not missing:
        verdict, detail = 'ok', '无新增标的'
    elif not unknown:
        verdict = 'known_exception'
        detail = f'已知例外 {len(known)} 只: ' + ', '.join(
            f'{code}（{KNOWN_BASE_INFO_EXCEPTIONS[code]["reason"]}，决策 '
            f'{KNOWN_BASE_INFO_EXCEPTIONS[code]["decided"]}）' for code in known)
    else:
        verdict = classify_missing(len(unknown))
        detail = f'新增未登记 {len(unknown)} 只: ' + ', '.join(unknown[:5])
        if known:
            detail += f'；另有已知例外 {len(known)} 只: ' + ', '.join(known)
    return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                        None, verdict, detail)


def collect_rows(store_root: Path, calendar: list[date],
                 quotes_end_date: date) -> list[FreshnessRow]:
    """按 ``TABLE_SPECS`` 逐表求「数据末日 / 行情末日 / 滞后 / 判定」。"""
    rows: list[FreshnessRow] = []
    for spec in TABLE_SPECS:
        if spec.kind == KIND_BASE_INFO:
            rows.append(_base_info_row(store_root, spec, quotes_end_date))
            continue
        path = store_root / spec.rel_path
        if not path.is_file():
            rows.append(FreshnessRow(spec.label, spec.rel_path, spec.kind, None,
                                     quotes_end_date, None, 'error',
                                     f'正式表不存在: {path}'))
            continue
        data_end = table_data_end(path, spec.date_column or '')
        if data_end is None:
            rows.append(FreshnessRow(spec.label, spec.rel_path, spec.kind, None,
                                     quotes_end_date, None, 'error',
                                     f'日期列为空或不存在: {spec.date_column}'))
            continue
        lag = count_lag(calendar, data_end, quotes_end_date)
        verdict = classify_lag(lag if lag is not None else 0)
        detail = f'滞后 {lag} 个交易日' if lag else '无滞后'
        rows.append(FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end,
                                 quotes_end_date, lag, verdict, detail))
    return rows


def _doc_paths(doc_text: str) -> set[str]:
    """清单全文里反引号包裹的固定路径（用于登记校验，宽松匹配）。"""
    out: set[str] = set()
    for token in doc_text.split('`'):
        token = token.strip()
        if '/' not in token or token.startswith('http') or ' ' in token:
            continue
        if '*' in token or '{' in token or '<' in token or token.startswith('['):
            continue
        out.add(token)
    return out


def _doc_formal_paths(doc_text: str) -> set[str]:
    """只取表格「唯一正式路径」列，用于「尚未纳入自检」的提示清单。"""
    out: set[str] = set()
    for line in doc_text.splitlines():
        if not line.lstrip().startswith('|'):
            continue
        cells = line.split('|')
        if len(cells) < 4:
            continue
        for token in cells[2].split('`'):
            token = token.strip()
            if '/' not in token or token.startswith('http') or ' ' in token:
                continue
            if '*' in token or '{' in token or '<' in token or '_backup' in token:
                continue
            out.add(token)
    return out


def _assert_doc_registered(doc_path: Path) -> list[str]:
    """校验 ``TABLE_SPECS`` 均已登记；返回清单里未被自检覆盖的固定路径。"""
    text = doc_path.read_text(encoding='utf-8')
    registered = _doc_paths(text)
    missing = [spec.rel_path for spec in TABLE_SPECS if spec.rel_path not in registered]
    if missing:
        raise ValueError('以下自检表未在权威清单登记: ' + ', '.join(missing))
    known = {spec.rel_path for spec in TABLE_SPECS}
    return sorted(path for path in _doc_formal_paths(text) - known
                  if path.endswith('.parquet'))


def run_check(store_root: Path, *, calendar: list[date] | None = None,
              doc_path: Path | None = DOC_PATH) -> FreshnessReport:
    """执行全量新鲜度自检。"""
    end = quotes_end(store_root)
    if end is None:
        raise FileNotFoundError(f'未找到权威行情目录或日线为空: {store_root / QUOTES_ROOT}')
    if calendar is None:
        from src.data.trading_calendar import get_calendar

        calendar = list(get_calendar(store_root, as_of=end).authoritative_dates())
    report = FreshnessReport(quotes_end=end, rows=collect_rows(store_root, calendar, end))
    if doc_path is not None and doc_path.is_file():
        report.doc_uncovered = _assert_doc_registered(doc_path)
    return report


def _fmt(value: date | None) -> str:
    return value.isoformat() if value else '-'


def format_report(report: FreshnessReport) -> str:
    """把结果渲染成「路径 / 数据末日 / 行情末日 / 滞后交易日 / 判定」表。"""
    header = ('路径', '数据末日', '行情末日', '滞后交易日', '判定', '说明')
    body = [(row.rel_path, _fmt(row.data_end), _fmt(row.quotes_end),
             '-' if row.lag is None else str(row.lag), row.verdict, row.detail)
            for row in report.rows]
    widths = [max(len(str(line[i])) for line in (header, *body)) for i in range(5)]
    lines = ['  '.join(str(header[i]).ljust(widths[i]) for i in range(5)) + '  说明',
             '-' * (sum(widths) + 2 * 5 + 8)]
    for line in body:
        lines.append('  '.join(str(line[i]).ljust(widths[i]) for i in range(5)) + f'  {line[5]}')
    lines.append('')
    lines.append(f'行情末日 {_fmt(report.quotes_end)}；'
                 f'error {len(report.errors)} 条 / warning {len(report.warnings)} 条 / '
                 f'known_exception {len(report.known_exceptions)} 条')
    if report.doc_uncovered:
        lines.append(f'权威清单中尚未纳入自检的固定路径 {len(report.doc_uncovered)} 条: '
                     + ', '.join(report.doc_uncovered))
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='数据新鲜度自检：本地正式表末日落后的交易日数')
    parser.add_argument('--no-doc-guard', action='store_true', help='跳过权威清单登记校验')
    parser.add_argument('--json', action='store_true', help='以 JSON 输出')
    args = parser.parse_args(argv)

    from src.data.local_store.paths import resolve_store_root
    from src.utils.config import get_config

    store_root = resolve_store_root(get_config())
    report = run_check(store_root, doc_path=None if args.no_doc_guard else DOC_PATH)
    if args.json:
        import json

        print(json.dumps({
            'quotes_end': _fmt(report.quotes_end),
            'rows': [{'label': row.label, 'path': row.rel_path,
                      'data_end': _fmt(row.data_end), 'quotes_end': _fmt(row.quotes_end),
                      'lag': row.lag, 'verdict': row.verdict, 'detail': row.detail}
                     for row in report.rows],
            'doc_uncovered': report.doc_uncovered,
        }, ensure_ascii=False, indent=2))
    else:
        print(format_report(report))
    return 0 if report.ok else 1


if __name__ == '__main__':
    sys.exit(main())
