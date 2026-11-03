#!/usr/bin/env python3

"""已知数据异常台账。

巡检只能"报警"，不能改数据；但历史源端瑕疵会长期存在，若不在台账中
登记，每次巡检都会被同一批行重复淹没。本模块把已人工确认的问题固化为
可查询的数据结构，供巡检、取数与回测侧统一判断：

- :func:`is_known_anomaly` —— 判断 ``(ts_code, trade_date)`` 是否命中台账；
- :func:`find_anomalies` —— 返回命中的全部记录（可带上下文）；
- :func:`register_anomaly` —— 扩展接口，新增问题无需改巡检代码。

登记内容与来源依据均为静态事实，运行时不做任何网络/数据库访问。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from threading import RLock
from typing import Iterable, Iterator

from src.utils.code_converter import to_ts_code

#: 处置建议：exclude=回测/取数应排除；flag=保留但在结果中标注；ignore=仅记录
ANOMALY_ACTIONS = ('exclude', 'flag', 'ignore')


def _as_date(value: object) -> date | None:
    """把 str/date/datetime 统一成 date，无法解析返回 None。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    return None


def _dates(*values: str) -> tuple[date, ...]:
    """把日期字符串元组转成 date 元组（导入期完成，查询时零解析开销）。"""
    parsed = []
    for value in values:
        parsed_date = _as_date(value)
        if parsed_date is None:
            raise ValueError(f'台账中的日期无法解析: {value!r}')
        parsed.append(parsed_date)
    return tuple(parsed)


def _normalize_code(ts_code: str) -> str:
    """统一为 ``600000.SH`` 口径；无法解析时仅做大写处理。"""
    try:
        return to_ts_code(str(ts_code))
    except (TypeError, ValueError):
        return str(ts_code).strip().upper()


@dataclass(frozen=True)
class KnownAnomaly:
    """一条已知异常登记。

    ``dates`` 与 ``start/end`` 二选一：精确日期优先；区间适合"整段历史都
    受影响"的问题（如复权基期差）。``applies_to`` 用于限定上下文
    （raw/local/factor/database），``any`` 表示不区分。
    """

    anomaly_id: str
    ts_code: str
    issue_type: str
    action: str
    reason: str
    source: str
    dates: tuple[date, ...] = ()
    start: date | None = None
    end: date | None = None
    applies_to: tuple[str, ...] = ('any',)

    def __post_init__(self) -> None:
        if self.action not in ANOMALY_ACTIONS:
            raise ValueError(f'未知处置动作 {self.action!r}，应为 {ANOMALY_ACTIONS}')
        object.__setattr__(self, 'ts_code', _normalize_code(self.ts_code))
        if self.dates:
            object.__setattr__(self, 'dates', tuple(sorted(set(self.dates))))
        object.__setattr__(self, 'applies_to', tuple(self.applies_to))

    def matches(self, ts_code: str, trade_date: object = None,
                context: str | None = None) -> bool:
        """``(代码, 日期)`` 是否命中本条登记。"""
        if _normalize_code(ts_code) != self.ts_code:
            return False
        if context and 'any' not in self.applies_to and context not in self.applies_to:
            return False
        if trade_date is None:
            return True
        current = _as_date(trade_date)
        if current is None:
            return False
        if self.dates:
            return current in self.dates
        if self.start is not None and current < self.start:
            return False
        return not (self.end is not None and current > self.end)

    def as_dict(self) -> dict[str, object]:
        """可 JSON 化的记录。"""
        return {
            'anomaly_id': self.anomaly_id,
            'ts_code': self.ts_code,
            'issue_type': self.issue_type,
            'action': self.action,
            'reason': self.reason,
            'source': self.source,
            'dates': [str(d) for d in self.dates],
            'start': str(self.start) if self.start else None,
            'end': str(self.end) if self.end else None,
            'applies_to': list(self.applies_to),
        }


# ----------------------------------------------------------------------
# 内置台账（已人工确认）
# ----------------------------------------------------------------------
_OHLC_DATES_000001_SH = _dates(
    '1991-05-09', '1991-06-06', '1992-02-13', '1992-03-04', '1992-05-11',
    '1992-06-16', '1993-01-05', '1993-02-15', '1993-04-08', '1994-02-18',
    '1994-08-05', '1994-08-09', '1994-09-28', '1994-10-20', '1995-01-25',
    '1995-04-20',
)
#: 000001.SH 因上述 16 行被构建剔除，下一交易日的 pre_close 仍指向已剔除行
_PRE_CLOSE_GAP_000001_SH = _dates(
    '1991-05-10', '1991-06-07', '1992-02-14', '1992-03-05', '1992-05-12',
    '1992-06-17', '1993-01-06', '1993-02-16', '1993-04-09', '1994-02-21',
    '1994-08-08', '1994-08-10', '1994-09-29', '1994-10-21', '1995-01-26',
    '1995-04-21',
)
_OHLC_DATES_399001_SZ = _dates(
    '1995-03-01', '1995-03-06', '1995-03-10', '1995-03-27', '1995-04-03',
    '1995-04-05', '1995-04-11', '1995-04-24', '1995-04-25',
)
#: 399001.SZ 剔除 9 行后，紧随其后的 8 个交易日 pre_close 参照失效
_PRE_CLOSE_GAP_399001_SZ = _dates(
    '1995-03-02', '1995-03-07', '1995-03-13', '1995-03-28', '1995-04-04',
    '1995-04-06', '1995-04-12', '1995-04-26',
)
_WEEKEND_DATES_399001_SZ = _dates(
    '1991-04-06', '1991-04-13', '1991-04-20', '1991-04-27', '1991-05-04',
    '1991-05-11', '1991-05-18', '1991-05-25', '1991-06-01', '1991-06-08',
    '1991-06-15', '1991-06-22', '1991-06-29', '1991-07-06', '1991-07-13',
    '1991-07-20', '1991-07-27', '1991-08-03', '1991-08-10', '1991-08-17',
    '1991-08-24', '1991-08-31', '1991-09-07', '1991-09-14', '1991-09-21',
    '1991-09-28', '1991-09-29', '1991-10-05', '1991-10-12', '1991-10-19',
    '1991-10-26', '1991-11-02', '1991-11-09', '1991-11-16', '1991-11-23',
    '1991-11-30', '1991-12-07', '1991-12-14', '1991-12-21', '1991-12-28',
    '1992-02-01', '1992-02-02', '1992-05-03', '1992-10-04', '1993-01-03',
)

_BUILTIN_ANOMALIES: tuple[KnownAnomaly, ...] = (
    KnownAnomaly(
        anomaly_id='ohlc_bounds_000001_sh',
        ts_code='000001.SH',
        issue_type='ohlc_bounds',
        action='exclude',
        reason='源端 OHLC 越界：high<low 或 high<max(open,close) / low>min(open,close)',
        source='2026-11-03 通达信 .day 与 index_daily 对拍（16 行，其中 1992-06-16 同时命中 high<low）',
        dates=_OHLC_DATES_000001_SH,
        applies_to=('any',),
    ),
    KnownAnomaly(
        anomaly_id='ohlc_bounds_399001_sz',
        ts_code='399001.SZ',
        issue_type='ohlc_bounds',
        action='exclude',
        reason='源端 OHLC 越界：high<max(open,close) / low>min(open,close)，均为 1995 年早期数据',
        source='2026-11-03 通达信 .day 与 index_daily 对拍（9 行）',
        dates=_OHLC_DATES_399001_SZ,
        applies_to=('any',),
    ),
    KnownAnomaly(
        anomaly_id='pre_close_gap_after_ohlc_drop_000001_sh',
        ts_code='000001.SH',
        issue_type='pre_close_gap',
        action='flag',
        reason=('构建期剔除 16 行 OHLC 瑕疵后，下一交易日 pre_close 仍指向已剔除行，'
                '导致 change/pct_chg 参照缺失；收益率应以连续 close 序列为准'),
        source='2026-11-03 本地列存巡检发现（紧邻 16 条 ohlc_bounds 记录之后）',
        dates=_PRE_CLOSE_GAP_000001_SH,
        applies_to=('local',),
    ),
    KnownAnomaly(
        anomaly_id='pre_close_gap_after_ohlc_drop_399001_sz',
        ts_code='399001.SZ',
        issue_type='pre_close_gap',
        action='flag',
        reason=('构建期剔除 9 行 OHLC 瑕疵后，紧随其后的 8 个交易日 pre_close 参照失效；'
                '收益率应以连续 close 序列为准'),
        source='2026-11-03 本地列存巡检发现（紧邻 9 条 ohlc_bounds 记录之后）',
        dates=_PRE_CLOSE_GAP_399001_SZ,
        applies_to=('local',),
    ),
    KnownAnomaly(
        anomaly_id='weekend_trade_date_399001_sz',
        ts_code='399001.SZ',
        issue_type='weekend_trade_date',
        action='flag',
        reason='1991-1993 早期周线混入日线，落在周六/周日的 45 行；保留原始记录，统计时需剔除',
        source='2026-11-03 通达信 .day 与 index_daily 巡检（45 行）',
        dates=_WEEKEND_DATES_399001_SZ,
        applies_to=('any',),
    ),
    KnownAnomaly(
        anomaly_id='hfq_base_shift_000004_sz',
        ts_code='000004.SZ',
        issue_type='hfq_base_shift',
        action='flag',
        reason='后复权因子相对历史基准存在约 1.2 倍整体偏移；收益率与相对形态不受影响',
        source='2026-11-03 stock_adj_factor 自洽性核查',
        start=date(1990, 1, 1),
        end=date(2026, 12, 31),
        applies_to=('factor',),
    ),
    KnownAnomaly(
        anomaly_id='legacy_volume_unit_sh600547',
        ts_code='600547.SH',
        issue_type='volume_unit_mismatch',
        action='exclude',
        reason=('遗留表 `sh.600547` 的成交量单位为「股」，与标准表「手」相差 100 倍；'
                '该表不在 ORM 模型内且数据止于 2021-06-22，禁止混用'),
        source='2026-11-03 遗留表审计（legacy table `sh.600547`）',
        start=date(1990, 1, 1),
        end=date(2021, 6, 22),
        applies_to=('legacy',),
    ),
)

_REGISTRY: dict[str, KnownAnomaly] = {a.anomaly_id: a for a in _BUILTIN_ANOMALIES}
_LOCK = RLock()


def register_anomaly(record: KnownAnomaly) -> KnownAnomaly:
    """登记一条新异常（扩展接口）；重复 ID 直接报错，避免静默覆盖。"""
    with _LOCK:
        if record.anomaly_id in _REGISTRY:
            raise ValueError(f'异常 ID 已存在: {record.anomaly_id}')
        _REGISTRY[record.anomaly_id] = record
    return record


def iter_anomalies() -> Iterator[KnownAnomaly]:
    """按 ID 稳定排序返回全部登记（含内置与运行时扩展）。"""
    with _LOCK:
        records: Iterable[KnownAnomaly] = list(_REGISTRY.values())
    yield from sorted(records, key=lambda item: item.anomaly_id)


def find_anomalies(ts_code: str, trade_date: object = None,
                   context: str | None = None) -> list[KnownAnomaly]:
    """返回命中的全部登记；无命中返回空列表。"""
    return [
        record for record in iter_anomalies()
        if record.matches(ts_code, trade_date, context)
    ]


def is_known_anomaly(ts_code: str, trade_date: object = None,
                     context: str | None = None) -> bool:
    """``(代码, 日期)`` 是否属于已登记异常（供回测/取数侧主动排除或标注）。"""
    return bool(find_anomalies(ts_code, trade_date, context))


def anomaly_action(ts_code: str, trade_date: object = None,
                   context: str | None = None) -> str | None:
    """返回首条命中登记的处置动作：exclude / flag / ignore / None。"""
    hits = find_anomalies(ts_code, trade_date, context)
    return hits[0].action if hits else None
