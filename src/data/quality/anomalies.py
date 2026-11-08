#!/usr/bin/env python3

"""已知数据异常台账。

巡检只能"报警"，不能改数据；但历史源端瑕疵会长期存在，若不在台账中
登记，每次巡检都会被同一批行重复淹没。本模块把已人工确认的问题固化为
可查询的数据结构，供巡检、取数与回测侧统一判断：

- :func:`is_known_anomaly` —— 判断 ``(ts_code, trade_date)`` 是否命中台账；
- :func:`find_anomalies` —— 返回命中的全部记录（可带上下文）；
- :func:`query_anomalies` —— 下游查询入口，返回命中条目 + 建议动作 + 证据出处；
- :func:`register_anomaly` —— 扩展接口（幂等），新增问题无需改巡检代码。

登记内容与来源依据均为静态事实，运行时不做任何网络/数据库访问；规则级
登记只读取本地清单文件（``data/local_store/meta/anomalies/``），按 mtime 缓存。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Iterator

from src.utils.code_converter import to_ts_code

#: 原始处置动作（向后兼容）：exclude=排除、flag=标注、ignore=仅记录；
#: 统一词表：use=可直接用、avoid=不应使用、warn=可用但需标注
ANOMALY_ACTIONS = ('exclude', 'flag', 'ignore', 'use', 'avoid', 'warn')

#: 原始动作 → 下游三类建议（查询入口统一输出 use / avoid / warn）
ACTION_ADVICE: dict[str, str] = {
    'exclude': 'avoid', 'avoid': 'avoid',
    'flag': 'warn', 'warn': 'warn',
    'ignore': 'use', 'use': 'use',
}
_ADVICE_RANK = {'use': 0, 'warn': 1, 'avoid': 2}

SEVERITY_LEVELS = ('low', 'medium', 'high', 'critical')
_DEFAULT_SEVERITY = {
    'exclude': 'high', 'avoid': 'high',
    'flag': 'medium', 'warn': 'medium',
    'ignore': 'low', 'use': 'low',
}
ANOMALY_SCOPES = ('code', 'rule', 'placeholder')
ANOMALY_STATUSES = ('active', 'pending')

#: 规则级登记的标的清单目录
ANOMALY_LIST_DIR = (
    Path(__file__).resolve().parents[3]
    / 'data' / 'local_store' / 'meta' / 'anomalies'
)


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


#: 规则级清单缓存：list_ref -> (mtime_ns, {code: [(start, end), ...]})
_LIST_CACHE: dict[
    str, tuple[int, dict[str, list[tuple[date | None, date | None]]]]
] = {}


def _load_code_list(
    list_ref: str,
) -> dict[str, list[tuple[date | None, date | None]]]:
    """读取规则级标的清单（CSV 或每行一个代码），按 mtime 缓存。

    清单列约定：``ts_code,start,end``；``start/end`` 可空表示不限。同一标的
    可有多行（多次事件），按区间列表保存。缺文件时返回空字典（规则不命中），
    不让台账查询因清单缺失而报错。
    """
    path = ANOMALY_LIST_DIR / list_ref
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        return {}
    cached = _LIST_CACHE.get(list_ref)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    entries: dict[str, list[tuple[date | None, date | None]]] = {}
    for raw in path.read_text('utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parts = [item.strip() for item in line.split(',')]
        code = parts[0]
        if code.lower() in ('ts_code', '代码'):
            continue
        start = _as_date(parts[1]) if len(parts) > 1 else None
        end = _as_date(parts[2]) if len(parts) > 2 else None
        entries.setdefault(_normalize_code(code), []).append((start, end))
    _LIST_CACHE[list_ref] = (mtime, entries)
    return entries



@dataclass(frozen=True)
class KnownAnomaly:
    """一条已知异常登记。

    ``dates`` 与 ``start/end`` 二选一：精确日期优先；区间适合"整段历史都
    受影响"的问题（如复权基期差）。``applies_to`` 用于限定上下文
    （raw/local/tdx/factor/legacy），``any`` 表示不区分。

    ``scope``：
    - ``code``：单标的问题，``ts_code`` 精确匹配；
    - ``rule``：规则级问题，``ts_code`` 通常为 ``*``，实际标的来自
      ``list_ref`` 指向的清单文件（支持逐标的发生区间）；
    - ``placeholder``：占位项（``status='pending'``），不参与匹配。

    ``severity`` 缺省时按 ``action`` 推导；``action`` 保留原始值，查询入口
    统一归一化为 use / avoid / warn。
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
    severity: str = ''
    evidence: str = ''
    detected_at: date | None = None
    scope: str = 'code'
    list_ref: str | None = None
    status: str = 'active'

    def __post_init__(self) -> None:
        if self.action not in ANOMALY_ACTIONS:
            raise ValueError(f'未知处置动作 {self.action!r}，应为 {ANOMALY_ACTIONS}')
        if self.scope not in ANOMALY_SCOPES:
            raise ValueError(f'未知登记范围 {self.scope!r}，应为 {ANOMALY_SCOPES}')
        if self.status not in ANOMALY_STATUSES:
            raise ValueError(f'未知登记状态 {self.status!r}，应为 {ANOMALY_STATUSES}')
        if self.severity and self.severity not in SEVERITY_LEVELS:
            raise ValueError(f'未知严重级别 {self.severity!r}，应为 {SEVERITY_LEVELS}')
        if self.scope == 'rule' and not self.list_ref:
            raise ValueError('规则级登记必须提供 list_ref 清单文件')
        if self.scope != 'rule' and self.list_ref:
            raise ValueError('仅规则级登记可设置 list_ref')
        object.__setattr__(
            self, 'severity', self.severity or _DEFAULT_SEVERITY[self.action])
        object.__setattr__(self, 'detected_at', _as_date(self.detected_at))
        object.__setattr__(self, 'ts_code', _normalize_code(self.ts_code))
        if self.dates:
            object.__setattr__(self, 'dates', tuple(sorted(set(self.dates))))
        object.__setattr__(self, 'applies_to', tuple(self.applies_to))

    def _matches_rule(self, ts_code: str, trade_date: object) -> bool:
        entries = _load_code_list(self.list_ref or '')
        key = _normalize_code(ts_code)
        if key not in entries:
            return False
        if trade_date is None:
            return True
        current = _as_date(trade_date)
        if current is None:
            return False
        for item_start, item_end in entries[key]:
            if item_start is not None and current < item_start:
                continue
            if item_end is not None and current > item_end:
                continue
            return True
        return False

    def matches(self, ts_code: str, trade_date: object = None,
                context: str | None = None) -> bool:
        """``(代码, 日期)`` 是否命中本条登记。"""
        if self.status != 'active':
            return False
        if context and 'any' not in self.applies_to and context not in self.applies_to:
            return False
        if self.scope == 'rule':
            return self._matches_rule(ts_code, trade_date)
        if _normalize_code(ts_code) != self.ts_code:
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
        """可 JSON 化的记录（``code`` 与 ``issue_type`` 同义）。"""
        return {
            'anomaly_id': self.anomaly_id,
            'ts_code': self.ts_code,
            'code': self.issue_type,
            'issue_type': self.issue_type,
            'action': self.action,
            'advice': ACTION_ADVICE[self.action],
            'severity': self.severity,
            'reason': self.reason,
            'evidence': self.evidence,
            'source': self.source,
            'detected_at': str(self.detected_at) if self.detected_at else None,
            'dates': [str(d) for d in self.dates],
            'start': str(self.start) if self.start else None,
            'end': str(self.end) if self.end else None,
            'applies_to': list(self.applies_to),
            'scope': self.scope,
            'list_ref': self.list_ref,
            'list_size': (
                sum(len(items) for items in _load_code_list(self.list_ref).values())
                if self.list_ref else 0
            ),
            'status': self.status,
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
        reason=('后复权因子（hfq）基期与同源算法差约 16.67%（qfq 相对差为 0）；'
                '复权收益率不受影响，但 hfq 绝对价位不可跨源比较'),
        source='2026-11-03 stock_adj_factor 自洽性核查',
        evidence=('logs/factor_audit_20261103_204206_333018_codes.csv'
                  '#000004.SZ（max_rel_hfq=0.1667，max_rel_qfq=0.0）'),
        detected_at=date(2026, 11, 3),
        severity='low',
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
    KnownAnomaly(
        anomaly_id='duplicate_ex_date_000002_sz',
        ts_code='000002.SZ',
        issue_type='duplicate_ex_date',
        action='avoid',
        reason=('1991-06-08 除权键重复 2 行（akshare_pg 配股 + mysql_remote 旧记录），'
                '复权因子在该日被应用两次；该日复权价与收益率不可直接使用'),
        source='2026-11-04 stock_dividend 快照核查（同代码同 ex_date 两行）',
        evidence=('data/local_store/factors/stock_dividend.parquet'
                  '#000002.SZ@1991-06-08（2 行）；logs/verification_20261104/'
                  'verification_summary.json'),
        detected_at=date(2026, 11, 4),
        severity='high',
        dates=(date(1991, 6, 8),),
        applies_to=('factor',),
    ),
    KnownAnomaly(
        anomaly_id='factor_tail_lag_rule',
        ts_code='*',
        issue_type='factor_tail_lag',
        action='avoid',
        reason=('因子快照末端落后行情末端（缺口内无除权事件）：按快照对齐时缺口日期缺行；'
                '本规则覆盖因子末端 >= 2026-10-01 的 1,604 只标的，缺口区间逐只在清单中给出'),
        source=('2026-11-04 验证线 market_scan 复算（factor_end_before_quote=1709，'
                '其中 105 只历史性落后并入源端停更/历史缺口）'),
        evidence=('logs/verification_20261104/market_scan.json'
                  '#coverage.factor_end_before_quote（缺事件 codes_with_event_in_gap=0）；'
                  '清单 data/local_store/meta/anomalies/factor_tail_lag_codes.csv'),
        detected_at=date(2026, 11, 4),
        severity='medium',
        applies_to=('factor',),
        scope='rule',
        list_ref='factor_tail_lag_codes.csv',
    ),
    KnownAnomaly(
        anomaly_id='no_local_quote_dividend_rule',
        ts_code='*',
        issue_type='no_local_quote',
        action='avoid',
        reason=('157 只标的（950 条历史除权事件）无本地行情：无法重建因子、无法回测；'
                '这些标的的除权事件仅存在于分红快照'),
        source='2026-11-04 验证线 market_scan（dividend_codes_without_quotes=157）',
        evidence=('logs/verification_20261104/market_scan.json'
                  '#coverage.dividend_codes_without_quotes 与 no_market_detail（157 只/950 条）；'
                  '清单 data/local_store/meta/anomalies/no_local_quote_codes.csv'),
        detected_at=date(2026, 11, 4),
        severity='high',
        applies_to=('local', 'factor'),
        scope='rule',
        list_ref='no_local_quote_codes.csv',
    ),
    KnownAnomaly(
        anomaly_id='source_stale_rule',
        ts_code='*',
        issue_type='source_stale',
        action='warn',
        reason=('666 只标的源端停更（stock 241 / index 50 / fund 375）：本地末行日期之后'
                '无数据，查询该日之后的记录应视为缺失；清单给出每只的停更起点'),
        source='2026-11-04 本地列存 daily_index 复算 + logs/stale_symbols_20261103.csv',
        evidence=('data/local_store/meta/daily_index.parquet#end；'
                  'logs/stale_symbols_20261103.csv；'
                  '清单 data/local_store/meta/anomalies/source_stale_codes.csv'),
        detected_at=date(2026, 11, 4),
        severity='medium',
        applies_to=('local', 'factor'),
        scope='rule',
        list_ref='source_stale_codes.csv',
    ),
    KnownAnomaly(
        anomaly_id='low_confidence_rights_issue_rule',
        ts_code='*',
        issue_type='low_confidence',
        action='warn',
        reason=('配股事件仅有同源复核，无独立第二源；参与复权的配股行置信度为 '
                'low_confidence（置信度问题，不是缺陷）'),
        source=('2026-11-04 跨源对拍覆盖核查：cross_source_detail.csv 仅覆盖纯现金/送转样本，'
                '配股无外部源'),
        evidence=('logs/verification_20261104/cross_source_detail.csv；'
                  'data/local_store/factors/stock_dividend.parquet#rights_issue_*；'
                  '清单 data/local_store/meta/anomalies/rights_issue_codes.csv'),
        detected_at=date(2026, 11, 4),
        severity='low',
        applies_to=('factor',),
        scope='rule',
        list_ref='rights_issue_codes.csv',
    ),
    KnownAnomaly(
        anomaly_id='placeholder_quote_rule',
        ts_code='*',
        issue_type='placeholder_quote',
        action='avoid',
        reason=(
            '232 只基金行情含占位桩，合计 22,085 行 close=1.0/volume=0/amount=0：'
            '84 只全序列占位（整只不可用）、135 只仅头部连续占位（上市初期无净值，'
            '后期有真实行情）、12 只中间或分散占位、1 只尾部占位。'
            '价格序列无意义，任何收益率都是假的，整只占位的 84 只禁止进入回测与推荐；'
            '局部占位的另做行级剔除，避免误伤真实行情。'
            '不是数据缺失，是可判定坏数据（close 严格等于 1.0 且零成交）。'
            '因子非 1 时会在事件日产生因子驱动的假 pct_chg：'
            '160217.SZ 4 行中 1 行 +40.68%（2019-09-17，qfq_factor 0.710852→1.0）；'
            '查“单日涨幅异常”前必须先排除本规则标的，否则会捞出一批幽灵。'
        ),
        source=(
            '2026-11-04 副本线占位行情清单；2026-11-07 改为「连续区间全量分段」'
            '重算，判据加严为 close==1.0 且 volume==0 且 amount==0（只看 close '
            '会把新发基金面值 1 元误判），识别标的 94 → 232 只'
        ),
        evidence=(
            '旧判据只认「全序列」与「尾部连续」两种形态，漏掉头部连续段与中间'
            '分散段共 147 只；'
            'data/local_store/meta/anomalies/placeholder_quote_codes.csv'
            '#249 个区间段 / 232 只 / 22,085 行，其中 full=True 84 只；'
            '源端核对：163803.SZ 的 .day 1,456 行全为 close=1.0（源端即占位，'
            '本地忠实复制，不可修）；'
            '160603.SZ 源端 1,894 行中 1,578 行为占位、后段 close 0.9~1.101 为真实净值'
        ),
        detected_at=date(2026, 11, 4),
        severity='high',
        applies_to=('local', 'factor'),
        scope='rule',
        list_ref='placeholder_quote_codes.csv',
    ),
    KnownAnomaly(
        anomaly_id='pending_fund_dividend_adjust',
        ts_code='*',
        issue_type='pending_etf_fund_dividend',
        action='warn',
        reason=('ETF/基金分红是否纳入复权：验证线核查中，结论未出前该占位项不参与匹配'),
        source='2026-11-04 占位登记，等验证线结论后补登',
        evidence='待补：验证线 ETF/基金分红复权核查报告',
        detected_at=date(2026, 11, 4),
        severity='medium',
        applies_to=('factor',),
        scope='placeholder',
        status='pending',
    ),
)

_REGISTRY: dict[str, KnownAnomaly] = {a.anomaly_id: a for a in _BUILTIN_ANOMALIES}
_LOCK = RLock()


def register_anomaly(record: KnownAnomaly) -> KnownAnomaly:
    """登记一条新异常（扩展接口，幂等）。

    同 ID 且内容完全相同的重复登记直接返回已有记录，不产生重复条目；
    同 ID 但内容不同则报错，避免静默覆盖。
    """
    with _LOCK:
        existing = _REGISTRY.get(record.anomaly_id)
        if existing is not None:
            if existing == record:
                return existing
            raise ValueError(f'异常 ID 已存在且内容不同: {record.anomaly_id}')
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
    """返回首条命中登记的处置动作：exclude / flag / ignore / use / avoid / warn。"""
    hits = find_anomalies(ts_code, trade_date, context)
    return hits[0].action if hits else None


def query_anomalies(ts_code: str, trade_date: object = None,
                    context: str | None = None) -> dict[str, Any]:
    """下游查询入口：某标的/日期是否命中已知问题、建议动作与证据出处。

    ``advice`` 统一归一化为 use / avoid / warn；多条命中取最严的一条
    （avoid > warn > use）。无命中时 ``advice='use'``，表示无已知问题。
    """
    hits = find_anomalies(ts_code, trade_date, context)
    advice = 'use'
    for item in hits:
        candidate = ACTION_ADVICE[item.action]
        if _ADVICE_RANK[candidate] > _ADVICE_RANK[advice]:
            advice = candidate
    current = _as_date(trade_date)
    return {
        'ts_code': _normalize_code(ts_code),
        'date': str(current) if current else None,
        'context': context,
        'known': bool(hits),
        'hit_count': len(hits),
        'advice': advice,
        'hits': [
            {
                'anomaly_id': item.anomaly_id,
                'code': item.issue_type,
                'issue_type': item.issue_type,
                'severity': item.severity,
                'action': ACTION_ADVICE[item.action],
                'raw_action': item.action,
                'reason': item.reason,
                'evidence': item.evidence,
                'source': item.source,
                'detected_at': str(item.detected_at) if item.detected_at else None,
                'scope': item.scope,
                'list_ref': item.list_ref,
                'applies_to': list(item.applies_to),
            }
            for item in hits
        ],
    }
