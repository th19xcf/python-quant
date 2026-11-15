"""数据新鲜度自检：把「漏跑」变成当场可见的失败。

判定分档（交易日滞后，基准为 ``quotes/stock`` 各标的 ``end`` 的最大值）
======================================================================
- ``0 ~ LAG_OK_MAX``：正常取数窗口（当天/次日的行情本来就可能尚未落盘）
- ``LAG_OK_MAX+1 ~ LAG_WARN_MAX``：``warning``
- ``> LAG_WARN_MAX``：``error``，命令行以非零退出码结束

三层分工
========
- **数据层（全集）**：``universe`` 口径对 ``quotes/{stock,fund,index}`` 里**每只
  有行情的标的**生效，不按候选池过滤；行情文件未进入 ``meta/daily_index.parquet``
  或末根断裂且未登记 ``meta/universe/stale_status.parquet`` 都算缺口。被候选池
  流动性规则排除的标的同样要有行情、估值、分红不中断的结论。
- **表级**：``daily`` / ``event`` / ``coverage`` / ``base_info`` / ``valuation`` /
  ``st_watermark`` 逐表判，基准一律是全集行情末日。
- **视图层**：``pool`` 只判候选池这个视图是否新鲜（覆盖末日滞后、池内标的是否
  有行情），最多 ``warning``，绝不升级为 ``error``；未入池不等于数据缺口。

按 ``kind`` 分八种口径：

- ``universe``：数据层全集（行情目录标的数、日更缺口计划登记、未登记断裂）；
- ``daily``：日频表，按 ``trade_date`` 等日期列与行情末日比交易日滞后；
- ``event``：事件表，按**文件刷新日期**判是否跟上日更（事件本身稀疏，
  连续几天无公告属合法状态，不能拿事件末日当缺口）；``lag_unit='calendar'``
  时按自然日比（30/90 天）；
- ``coverage``：覆盖窗口表，按 ``coverage_end`` 比交易日滞后；
- ``base_info``：名录，同时判「行情有而名录无」（未登记标的数）与
  「名录日期陈旧」（**最后检查日期**距行情末日的交易日数：名录每日检查、
  有变更才落盘，无变更时 parquet mtime 不动，因此按 manifest 的
  ``last_checked_at`` 判，无该字段才回退 mtime）；
- ``valuation``：估值表，除日期滞后外再判「行情有而估值无」
  （行情末日仍在交易的标的在当日估值截面中缺失的条数）；
- ``pool``：**视图层**，只判候选池覆盖末日滞后与池内标的缺行情，不判未入池；
- ``st_watermark``：ST 区间表，以 ``meta/st/coverage.json`` 的
  ``snapshot_date`` 为构建水位，水位落后即区间表没跟上日更。

已决策的例外（如封闭式基金 505888.SH）登记在 ``KNOWN_BASE_INFO_EXCEPTIONS``，
命中后判为 ``known_exception``：仍在明细里逐行列出，但不计入 warning / error。

覆盖口径来自 ``docs/数据权威路径清单.md``：本模块的 ``TABLE_SPECS`` 每一条
都必须在清单里登记，未登记即抛错（``_assert_doc_registered``）。
新增登记表时同步在这里加一行并注明 ``kind``。

输出约定：正常时只打印一行汇总；出现 ``warning`` / ``error`` 才打印逐表明细
（error 红、warning 黄，仅 TTY 或 ``--color always``）；``--detail`` 可强制明细，
``--summary`` 只出单行（供日更收尾调用）。

用法::

    python -m src.data.quality.freshness_check
    python -m src.data.quality.freshness_check --summary
    python -m src.data.quality.freshness_check --detail --no-doc-guard
"""
# ruff: noqa: T201  # 自检命令面向终端输出

from __future__ import annotations

import argparse
import json
import sys
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import polars as pl

KIND_DAILY = 'daily'
KIND_EVENT = 'event'
KIND_COVERAGE = 'coverage'
KIND_BASE_INFO = 'base_info'
KIND_VALUATION = 'valuation'
KIND_POOL = 'pool'
KIND_ST_WATERMARK = 'st_watermark'
KIND_UNIVERSE = 'universe'

#: 数据层全集口径：这三类行情目录里的每只标的都必须进入日更缺口计划。
UNIVERSE_KINDS = ('stock', 'fund', 'index')
#: 已定性的停更台账（数据源无新增/退市），登记在此的标的断裂不算缺口。
STALE_STATUS_PATH = Path('meta') / 'universe' / 'stale_status.parquet'

#: 0~1 个交易日滞后属正常取数窗口
LAG_OK_MAX = 1
#: 2~5 个交易日滞后报 warning，超过报 error
LAG_WARN_MAX = 5
#: 基础信息表允许的新增标的数 / 估值缺失条数上限（超过报 error）
BASE_INFO_WARN_MAX = 5
#: 名录周更周期（自然日），与 ``daily_update --basic-refresh-days`` 默认值一致
BASE_INFO_REFRESH_DAYS = 7
#: 7 个自然日 ≈ 5 个交易日；告警线 = 周更周期换算后的交易日 + 2 个交易日缓冲
BASE_INFO_SCHEDULE_DAYS = 5
BASE_INFO_BUFFER_DAYS = 2
#: 名录刷新日期滞后：≤7 交易日正常，8~10 交易日 warning，>10（两周）error
BASE_INFO_LAG_WARN = BASE_INFO_SCHEDULE_DAYS + BASE_INFO_BUFFER_DAYS
BASE_INFO_LAG_ERROR = 10
#: 公告事件按自然日：超过 30 天 warning、90 天 error
EVENT_LAG_WARN_DAYS = 30
EVENT_LAG_ERROR_DAYS = 90

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
    #: 估值表用：行情目录（判「行情有而估值无」）
    coverage_dir: str | None = None
    #: 滞后单位：trading（交易日）或 calendar（自然日，事件表用）
    lag_unit: str = 'trading'


#: 与 ``docs/数据权威路径清单.md`` 一一对应；新增登记表时在此追加
TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec('个股分红', 'factors/stock_dividend.parquet', KIND_EVENT, 'notice_date'),
    TableSpec('个股复权因子', 'factors/stock_adj_factor.parquet', KIND_DAILY, 'trade_date'),
    TableSpec('市场宽度', 'meta/breadth/breadth_daily.parquet', KIND_DAILY, 'trade_date'),
    TableSpec('估值日频', 'meta/financials/valuation_daily.parquet', KIND_VALUATION,
              'trade_date', coverage_dir='quotes/stock'),
    TableSpec('公告事件', 'meta/events/events.parquet', KIND_EVENT, 'publish_time',
              lag_unit='calendar'),
    TableSpec('基金分红', 'factors/fund_dividend.parquet', KIND_DAILY, 'ex_date',
              lag_unit='calendar'),
    TableSpec('基金基础信息', 'meta/fund_basic.parquet', KIND_BASE_INFO,
              quotes_dir='quotes/fund'),
    TableSpec('指数基础信息', 'meta/index_basic.parquet', KIND_BASE_INFO,
              quotes_dir='quotes/index'),
    TableSpec('股票基础信息', 'meta/stock_basic.parquet', KIND_BASE_INFO,
              quotes_dir='quotes/stock'),
    TableSpec('ST 区间', 'meta/st/st_intervals.parquet', KIND_ST_WATERMARK),
    TableSpec('ST 覆盖窗口', 'meta/st/st_coverage.parquet', KIND_COVERAGE, 'coverage_end'),
    TableSpec('多资产 ETF 候选池', 'meta/multi_asset/etf_pool.parquet', KIND_POOL,
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
    #: 缺口：日期型表为缺失交易日/自然日数，名录与估值表为缺失标的数
    gap: int | None = None
    #: 事件表的文件刷新日期（判定滞后的基准；事件末日只作说明）
    refresh_date: date | None = None


@dataclass
class FreshnessReport:
    """全量自检结果。"""

    quotes_end: date | None
    rows: list[FreshnessRow] = field(default_factory=list)
    doc_uncovered: list[str] = field(default_factory=list)
    #: 数据层全集：各行情目录的标的数（stock / fund / index）
    universe: dict[str, int] = field(default_factory=dict)

    @property
    def universe_total(self) -> int:
        return sum(self.universe.values())

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

    @property
    def max_lag(self) -> int:
        values = [row.lag for row in self.rows if row.lag is not None]
        return max(values) if values else 0


def classify_lag(lag: int) -> str:
    """按交易日滞后分档。"""
    if lag <= LAG_OK_MAX:
        return 'ok'
    if lag <= LAG_WARN_MAX:
        return 'warning'
    return 'error'


def classify_calendar_lag(days: int) -> str:
    """按自然日滞后分档（事件表）。"""
    if days <= EVENT_LAG_WARN_DAYS:
        return 'ok'
    if days <= EVENT_LAG_ERROR_DAYS:
        return 'warning'
    return 'error'


def classify_base_info_lag(lag: int) -> str:
    """按名录刷新日期滞后分档（交易日）。

    周更周期 7 自然日 ≈ 5 个交易日，告警线再加 2 个交易日缓冲 = 7：
    ≤7 正常、8~10 ``warning``、>10（两周）``error``。名录停更属静默漏票，
    新上市股票/新发指数进不来不会触发任何校验，所以两周以上直接判红。
    """
    if lag <= BASE_INFO_LAG_WARN:
        return 'ok'
    if lag <= BASE_INFO_LAG_ERROR:
        return 'warning'
    return 'error'


def classify_missing(missing: int) -> str:
    """按基础表未登记的新增标的数分档。"""
    if missing <= 0:
        return 'ok'
    if missing <= BASE_INFO_WARN_MAX:
        return 'warning'
    return 'error'


_SEVERITY = {'ok': 0, 'known_exception': 1, 'warning': 2, 'error': 3}


def _worst(*verdicts: str) -> str:
    """取最严重的判定（ok < known_exception < warning < error）。"""
    return max(verdicts, key=lambda item: _SEVERITY.get(item, 0))


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
    if isinstance(value, datetime):
        return value.date()
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


def _file_date(path: Path) -> date | None:
    """文件刷新日期（本地时区）。"""
    try:
        return date.fromtimestamp(path.stat().st_mtime)
    except OSError:
        return None


def _checked_date(path: Path) -> date | None:
    """名录的「最后检查日期」：manifest 的 ``last_checked_at``。

    名录每日检查、有变更才落盘，无变更时 parquet 的 mtime 长期不动，因此新鲜度
    必须按检查日期判，否则会天天误报陈旧；没有该字段时回退到文件 mtime。
    """
    manifest = path.with_name(path.stem + '.manifest.json')
    if manifest.is_file():
        try:
            payload = json.loads(manifest.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            payload = {}
        value = payload.get('last_checked_at')
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                pass
    return _file_date(path)


def _write_date(path: Path) -> date | None:
    """行情文件末根日期；只用于估值缺失标的的二次确认。"""
    try:
        value = pl.scan_parquet(path).select(pl.col('date').max()).collect().item()
    except (OSError, pl.exceptions.ColumnNotFoundError):
        return None
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def _base_info_row(store_root: Path, spec: TableSpec, quotes_end_date: date,
                   calendar: list[date]) -> FreshnessRow:
    """名录：同时判「行情有而名录无」与「名录刷新日期陈旧」。"""
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
        missing_verdict, missing_detail = 'ok', '无新增标的'
    elif not unknown:
        missing_verdict = 'known_exception'
        missing_detail = f'已知例外 {len(known)} 只: ' + ', '.join(
            f'{code}（{KNOWN_BASE_INFO_EXCEPTIONS[code]["reason"]}，决策 '
            f'{KNOWN_BASE_INFO_EXCEPTIONS[code]["decided"]}）' for code in known)
    else:
        missing_verdict = classify_missing(len(unknown))
        missing_detail = f'新增未登记 {len(unknown)} 只: ' + ', '.join(unknown[:5])
        if known:
            missing_detail += f'；另有已知例外 {len(known)} 只: ' + ', '.join(known)

    built = _file_date(path)
    data_end = _checked_date(path)
    lag = count_lag(calendar, data_end, quotes_end_date)
    stale_verdict = classify_base_info_lag(lag if lag is not None else 0)
    if data_end is None:
        stale_detail = '最后检查日期不可读'
    else:
        stale_detail = f'最后检查 {_fmt(data_end)}'
        if built is not None and built != data_end:
            stale_detail += f'（上次落盘 {_fmt(built)}）'
        stale_detail += f'，滞后 {lag} 个交易日'
    return FreshnessRow(
        spec.label, spec.rel_path, spec.kind, data_end, quotes_end_date, lag,
        _worst(missing_verdict, stale_verdict),
        f'{stale_detail}；{missing_detail}', gap=len(unknown))


def _valuation_row(store_root: Path, spec: TableSpec, quotes_end_date: date,
                   calendar: list[date]) -> FreshnessRow:
    """估值表：日期滞后 + 「行情有而估值无」。"""
    path = store_root / spec.rel_path
    if not path.is_file():
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'正式表不存在: {path}')
    data_end = table_data_end(path, spec.date_column or '')
    if data_end is None:
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'日期列为空或不存在: {spec.date_column}')
    lag = count_lag(calendar, data_end, quotes_end_date)
    verdict = classify_lag(lag if lag is not None else 0)
    detail = f'滞后 {lag} 个交易日' if lag else '无滞后'

    gap = 0
    quotes_dir = store_root / (spec.coverage_dir or '')
    if data_end == quotes_end_date and quotes_dir.is_dir():
        registered = set(
            pl.scan_parquet(path)
            .filter(pl.col(spec.date_column) == quotes_end_date)
            .select('ts_code').collect()['ts_code'].to_list())
        candidates = [item for item in quotes_dir.rglob('*.parquet')
                      if item.stem not in registered]
        # 只把「行情末日仍在交易」的标的算缺口，停牌/退市不算
        missing = sorted(item.stem for item in candidates
                         if _write_date(item) == quotes_end_date)
        gap = len(missing)
        if missing:
            detail += f'；行情末日有行情但估值缺失 {gap} 只: ' + ', '.join(missing[:5])
            verdict = _worst(verdict, classify_missing(gap))
    return FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end, quotes_end_date,
                        lag, verdict, detail, gap=gap)


def _st_watermark_row(store_root: Path, spec: TableSpec, quotes_end_date: date,
                      calendar: list[date]) -> FreshnessRow:
    """ST 区间表：以 ``meta/st/coverage.json`` 的快照水位判定是否跟上日更。"""
    path = store_root / spec.rel_path
    if not path.is_file():
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'正式表不存在: {path}')
    coverage_path = store_root / 'meta' / 'st' / 'coverage.json'
    if not coverage_path.is_file():
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'构建水位不可读: {coverage_path}')
    try:
        payload = json.loads(coverage_path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'构建水位不可读: {exc}')
    raw = payload.get('snapshot_date')
    data_end = None
    if isinstance(raw, str) and raw.strip():
        try:
            data_end = date.fromisoformat(raw.strip()[:10])
        except ValueError:
            data_end = None
    if data_end is None:
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', 'coverage.json 缺少 snapshot_date')
    lag = count_lag(calendar, data_end, quotes_end_date)
    verdict = classify_lag(lag if lag is not None else 0)
    detail = f'快照水位 {_fmt(data_end)}，区间表滞后 {lag} 个交易日' \
        if lag else f'快照水位 {_fmt(data_end)}，无滞后'
    return FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end, quotes_end_date,
                        lag, verdict, detail, gap=lag)


def _date_row(spec: TableSpec, path: Path, quotes_end_date: date,
              calendar: list[date]) -> FreshnessRow:
    """日期型表：按 ``lag_unit`` 求与行情末日的滞后。

    事件表（``KIND_EVENT``）的滞后基准是**文件刷新日期**，不是事件末日：
    连续几天没有新公告是合法状态，只有「表没再刷新」才是缺口。
    """
    data_end = table_data_end(path, spec.date_column or '')
    if data_end is None:
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'日期列为空或不存在: {spec.date_column}')
    if spec.kind == KIND_EVENT:
        refresh = _file_date(path)
        if refresh is None:
            return FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end,
                                quotes_end_date, None, 'error', '文件刷新日期不可读')
        if spec.lag_unit == 'calendar':
            lag = max((quotes_end_date - refresh).days, 0)
            verdict = classify_calendar_lag(lag)
            detail = (f'事件末日 {_fmt(data_end)}；表刷新 {_fmt(refresh)}，'
                      f'滞后 {lag} 个自然日')
        else:
            lag = count_lag(calendar, refresh, quotes_end_date)
            verdict = classify_lag(lag if lag is not None else 0)
            detail = (f'事件末日 {_fmt(data_end)}；表刷新 {_fmt(refresh)}，'
                      f'滞后 {lag} 个交易日')
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end, quotes_end_date,
                            lag, verdict, detail, gap=lag, refresh_date=refresh)
    if spec.lag_unit == 'calendar':
        lag = max((quotes_end_date - data_end).days, 0)
        verdict = classify_calendar_lag(lag)
        detail = f'滞后 {lag} 个自然日' if lag else '无滞后'
    else:
        lag = count_lag(calendar, data_end, quotes_end_date)
        verdict = classify_lag(lag if lag is not None else 0)
        detail = f'滞后 {lag} 个交易日' if lag else '无滞后'
    return FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end, quotes_end_date,
                        lag, verdict, detail, gap=lag)


def universe_files(store_root: Path) -> dict[str, set[str]]:
    """各行情目录下的标的集合（数据层全集口径）。"""
    out: dict[str, set[str]] = {}
    for kind in UNIVERSE_KINDS:
        root = store_root / 'quotes' / kind
        out[kind] = {item.stem for item in root.rglob('*.parquet')} if root.is_dir() else set()
    return out


def _universe_row(store_root: Path, quotes_end_date: date,
                  calendar: list[date], files: dict[str, set[str]] | None = None
                  ) -> FreshnessRow:
    """数据层全集：行情目录里的每只标的都必须进入日更缺口计划。

    判据对**全市场有行情的标的**生效，不按候选池过滤：
    - 行情文件未登记 ``meta/daily_index.parquet`` → 日更缺口计划看不到它，算缺口；
    - 末根落后超过 ``LAG_WARN_MAX`` 个交易日且未登记停更台账 → 数据断裂算缺口。
    已登记 ``meta/universe/stale_status.parquet`` 的停更（数据源无新增/退市）属已知
    合法状态，只报数量、不计缺口。
    """
    counts: dict[str, int] = {}
    file_map = files if files is not None else universe_files(store_root)
    for kind in UNIVERSE_KINDS:
        counts[kind] = len(file_map.get(kind, ()))
    total = sum(counts.values())

    registered: set[tuple[str, str]] = set()
    ends: dict[tuple[str, str], date] = {}
    index_path = store_root / 'meta' / 'daily_index.parquet'
    if index_path.is_file():
        frame = pl.read_parquet(index_path, columns=['ts_code', 'data_type', 'end'])
        for row in frame.iter_rows(named=True):
            key = (row['data_type'], row['ts_code'])
            registered.add(key)
            if isinstance(row['end'], date):
                ends[key] = row['end']
    unregistered = sorted(f'{kind}/{code}' for kind in UNIVERSE_KINDS
                          for code in file_map.get(kind, ())
                          if (kind, code) not in registered) if registered else []

    known_stale: set[tuple[str, str]] = set()
    stale_path = store_root / STALE_STATUS_PATH
    if stale_path.is_file():
        stale = pl.read_parquet(stale_path, columns=['ts_code', 'asset_class', 'is_stale'])
        known_stale = {(row['asset_class'], row['ts_code'])
                       for row in stale.iter_rows(named=True) if row['is_stale']}

    breaks: list[tuple[str, str, date, int]] = []
    for (kind, code), end in ends.items():
        if kind not in UNIVERSE_KINDS:
            continue
        lag = count_lag(calendar, end, quotes_end_date)
        if lag is not None and lag > LAG_WARN_MAX and (kind, code) not in known_stale:
            breaks.append((kind, code, end, lag))
    breaks.sort(key=lambda item: item[3], reverse=True)

    gap = len(unregistered) + len(breaks)
    verdict = _worst(classify_missing(len(unregistered)), classify_missing(len(breaks)))
    detail = (f'全集 {total:,} 只（股票 {counts["stock"]:,} / 基金 {counts["fund"]:,} / '
              f'指数 {counts["index"]:,}）；已知停更 {len(known_stale):,} 只'
              f'（stale_status 登记，不计缺口）；未登记断裂 {len(breaks)} 只')
    if not registered:
        detail += '；daily_index 不可读，跳过登记校验'
    if unregistered:
        detail += f'；行情文件未进入日更缺口计划 {len(unregistered)} 只: ' + ', '.join(
            unregistered[:5])
    if breaks:
        detail += '；断裂: ' + ', '.join(
            f'{code}(末根 {_fmt(end)}，滞后 {lag})' for _kind, code, end, lag in breaks[:5])
    return FreshnessRow('行情全集', 'quotes/{stock,fund,index}', KIND_UNIVERSE,
                        quotes_end_date, quotes_end_date, 0, verdict, detail, gap=gap)


def _pool_row(store_root: Path, spec: TableSpec, quotes_end_date: date,
              calendar: list[date]) -> FreshnessRow:
    """候选池：**视图层**只判「视图是否新鲜」，不承担数据完整性判据。

    池是流动性筛选结果，未入池不等于数据有缺口；被排除标的的行情/分红由数据层
    全集判据负责。这里只判覆盖末日滞后（最多 warning）与池内标的缺行情。
    """
    path = store_root / spec.rel_path
    frame = pl.read_parquet(path, columns=['ts_code', spec.date_column or 'coverage_end'])
    data_end = frame[spec.date_column or 'coverage_end'].max()
    if not isinstance(data_end, date):
        return FreshnessRow(spec.label, spec.rel_path, spec.kind, None, quotes_end_date,
                            None, 'error', f'日期列为空或不存在: {spec.date_column}')
    lag = count_lag(calendar, data_end, quotes_end_date) or 0
    # 视图层单独一档：过期只提示刷新视图，绝不升级为 error
    verdict = 'ok' if lag <= LAG_OK_MAX else 'warning'
    detail = f'视图覆盖 {_fmt(data_end)}，滞后 {lag} 个交易日'

    in_pool = set(frame['ts_code'].to_list())
    quotes_dir = store_root / 'quotes' / 'fund'
    quoted = {item.stem for item in quotes_dir.rglob('*.parquet')} if quotes_dir.is_dir() else set()
    missing = sorted(in_pool - quoted)
    gap = len(missing)
    if missing:
        detail += f'；池内无行情 {gap} 只（{missing[0]} 等）'
        verdict = _worst(verdict, 'warning')

    excluded_count = 0
    excluded_path = store_root / 'meta' / 'multi_asset' / 'etf_pool_excluded.json'
    if excluded_path.is_file():
        try:
            payload = json.loads(excluded_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            payload = {}
        codes = payload.get('codes') or []
        excluded_count = int(payload.get('count') or len(codes))
        detail += f'；池内 {len(in_pool)} 只，按入池规则排除 {excluded_count} 只' \
                  '（原因与评估日期见 etf_pool_excluded.json）'
    return FreshnessRow(spec.label, spec.rel_path, spec.kind, data_end, quotes_end_date,
                        lag, verdict, detail, gap=gap)


def collect_rows(store_root: Path, calendar: list[date],
                 quotes_end_date: date,
                 files: dict[str, set[str]] | None = None) -> list[FreshnessRow]:
    """按 ``TABLE_SPECS`` 逐表求「数据末日 / 行情末日 / 滞后 / 缺口 / 判定」。

    首行固定是数据层「行情全集」判据：完整性口径对全市场有行情的标的生效，
    不按候选池过滤；候选池只是视图层的一档。
    """
    rows: list[FreshnessRow] = [_universe_row(store_root, quotes_end_date, calendar, files)]
    for spec in TABLE_SPECS:
        if spec.kind == KIND_BASE_INFO:
            rows.append(_base_info_row(store_root, spec, quotes_end_date, calendar))
            continue
        path = store_root / spec.rel_path
        if not path.is_file():
            rows.append(FreshnessRow(spec.label, spec.rel_path, spec.kind, None,
                                     quotes_end_date, None, 'error',
                                     f'正式表不存在: {path}'))
            continue
        if spec.kind == KIND_VALUATION:
            rows.append(_valuation_row(store_root, spec, quotes_end_date, calendar))
        elif spec.kind == KIND_ST_WATERMARK:
            rows.append(_st_watermark_row(store_root, spec, quotes_end_date, calendar))
        elif spec.kind == KIND_POOL:
            rows.append(_pool_row(store_root, spec, quotes_end_date, calendar))
        else:
            rows.append(_date_row(spec, path, quotes_end_date, calendar))
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
    universe = universe_files(store_root)
    report = FreshnessReport(quotes_end=end,
                             rows=collect_rows(store_root, calendar, end, universe),
                             universe={kind: len(items) for kind, items in universe.items()})
    if doc_path is not None and doc_path.is_file():
        report.doc_uncovered = _assert_doc_registered(doc_path)
    return report


def _fmt(value: date | None) -> str:
    return value.isoformat() if value else '-'


#: 终端色：只对 error / warning 上色，ok 保持默认
_COLORS = {'error': '\033[31m', 'warning': '\033[33m', 'known_exception': '\033[36m'}
_RESET = '\033[0m'


def _paint(text: str, verdict: str, color: bool) -> str:
    if not color or verdict not in _COLORS:
        return text
    return f'{_COLORS[verdict]}{text}{_RESET}'


def format_report(report: FreshnessReport, *, color: bool = False,
                  show_uncovered: bool = False) -> str:
    """把结果渲染成「路径 / 数据末日 / 行情末日 / 滞后 / 缺口 / 判定」表。"""
    header = ('路径', '数据末日', '行情末日', '滞后', '缺口', '判定')
    body = [(row.rel_path, _fmt(row.data_end), _fmt(row.quotes_end),
             '-' if row.lag is None else str(row.lag),
             '-' if row.gap is None else str(row.gap), row.verdict, row.detail)
            for row in report.rows]
    widths = [max(len(str(line[i])) for line in (header, *body)) for i in range(5)]
    lines = ['  '.join(str(header[i]).ljust(widths[i]) for i in range(5)) + '  说明',
             '-' * (sum(widths) + 2 * 5 + 8)]
    for line in body:
        prefix = '  '.join(str(line[i]).ljust(widths[i]) for i in range(5))
        lines.append(_paint(prefix, line[5], color) + f'  {line[6]}')
    lines.append('')
    lines.append(f'行情末日 {_fmt(report.quotes_end)}；'
                 f'error {len(report.errors)} 条 / warning {len(report.warnings)} 条 / '
                 f'known_exception {len(report.known_exceptions)} 条')
    if show_uncovered and report.doc_uncovered:
        lines.append(f'权威清单中尚未纳入自检的固定路径 {len(report.doc_uncovered)} 条: '
                     + ', '.join(report.doc_uncovered))
    return '\n'.join(lines)


def format_summary(report: FreshnessReport) -> str:
    """一行汇总：正常时只看这一行，异常时点名 error / warning 的表。"""
    parts = [
        '[保鲜]',
        f'行情末日 {_fmt(report.quotes_end)}',
        f'全集 {report.universe_total:,} 只',
    ]
    base_rows = [row for row in report.rows if row.kind == KIND_BASE_INFO]
    if base_rows:
        base_lag = max(row.lag or 0 for row in base_rows)
        parts.append(f'名录 {len(base_rows)} 张最大滞后 {base_lag} 天')
    parts += [
        f'表 {len(report.rows)}',
        f'最大滞后 {report.max_lag} 天',
        f'error {len(report.errors)} / warning {len(report.warnings)}',
    ]
    if report.errors:
        parts.append('红: ' + ', '.join(row.label for row in report.errors))
    elif report.warnings:
        parts.append('黄: ' + ', '.join(row.label for row in report.warnings))
    else:
        parts.append('全部正常')
    return ' | '.join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='数据新鲜度自检：本地正式表末日落后的交易日数')
    parser.add_argument('--no-doc-guard', action='store_true', help='跳过权威清单登记校验')
    parser.add_argument('--json', action='store_true', help='以 JSON 输出')
    parser.add_argument('--store-root', default=None, help='local_store 根（默认取项目配置）')
    parser.add_argument('--summary', action='store_true', help='只输出一行汇总（日更收尾用）')
    parser.add_argument('--detail', action='store_true', help='无论是否异常都输出逐表明细')
    parser.add_argument('--color', choices=('auto', 'always', 'never'), default='auto',
                        help='error 红 / warning 黄；auto 仅在 TTY 上色')
    args = parser.parse_args(argv)

    from src.data.local_store.paths import resolve_store_root
    from src.utils.config import get_config

    store_root = (Path(args.store_root) if args.store_root
                  else resolve_store_root(get_config()))
    report = run_check(store_root, doc_path=None if args.no_doc_guard else DOC_PATH)
    color = args.color == 'always' or (args.color == 'auto' and sys.stdout.isatty())
    if args.json:
        print(json.dumps({
            'quotes_end': _fmt(report.quotes_end),
            'universe': report.universe,
            'rows': [{'label': row.label, 'path': row.rel_path,
                      'data_end': _fmt(row.data_end), 'quotes_end': _fmt(row.quotes_end),
                      'lag': row.lag, 'gap': row.gap, 'verdict': row.verdict,
                      'refresh_date': _fmt(row.refresh_date),
                      'detail': row.detail}
                     for row in report.rows],
            'doc_uncovered': report.doc_uncovered,
        }, ensure_ascii=False, indent=2))
    elif args.summary:
        print(format_summary(report))
    elif args.detail or report.errors or report.warnings:
        print(format_report(report, color=color, show_uncovered=args.detail))
        print(format_summary(report))
    else:
        print(format_summary(report))
    return 0 if report.ok else 1


if __name__ == '__main__':
    sys.exit(main())
