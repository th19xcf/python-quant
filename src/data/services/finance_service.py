#!/usr/bin/env python3

"""「财务分析」窗口的取数服务（只读，不写库、不落盘）。

数据源与降级顺序（实测于 2026-11，项目 venv 内 akshare 可用）：
- 财务指标：东方财富「主要指标」→ 同花顺「财务摘要」→ 空态；
- 估值：东方财富「估值分析」最新一行（PE/PB/市值）；
- 个股新闻：东方财富个股新闻；
- 公告/业绩预告/分红/风险提示：优先复用本地列存（events/分红/ST 快照），
  巨潮公告接口在本项目实测返回空表，因此不作为主路径。

本模块所有外部调用都做异常收敛，任何单源失败只记录 ``errors`` 并继续降级，
不向调用方抛异常。跨线程只传递普通 dict，不携带 Qt 对象。
"""

from __future__ import annotations

import threading
import time as _time
from collections import OrderedDict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import polars as pl
from loguru import logger

from src.utils.code_converter import is_index_code, parse_code

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
CACHE_MAX_ITEMS = 200
CACHE_TTL_SECONDS = 30 * 60
NEWS_LIMIT = 30
LOCAL_EVENT_DAYS = 365
DIVIDEND_DAYS = 730

SOURCE_EM = '东方财富·主要指标'
SOURCE_THS = '同花顺·财务摘要'
SOURCE_VALUE = '东方财富·估值分析'
SOURCE_NEWS = '东方财富·个股新闻'
SOURCE_LOCAL = '本地事件库'

METRIC_DEFS: tuple[tuple[str, str], ...] = (
    ('roe', 'ROE(加权)'),
    ('net_margin', '销售净利率'),
    ('gross_margin', '销售毛利率'),
    ('revenue_yoy', '营收同比'),
    ('profit_yoy', '净利同比'),
    ('eps', '每股收益'),
    ('debt_ratio', '资产负债率'),
    ('current_ratio', '流动比率'),
    ('quick_ratio', '速动比率'),
    ('inventory_turnover', '存货周转率'),
    ('pe_ttm', 'PE(TTM)'),
    ('pe_static', 'PE(静)'),
    ('pb', '市净率'),
    ('total_mv', '总市值'),
    ('float_mv', '流通市值'),
)

PERCENT_METRICS = {
    'roe', 'net_margin', 'gross_margin', 'revenue_yoy', 'profit_yoy',
    'debt_ratio',
}
MONEY_METRICS = {'total_mv', 'float_mv'}

HISTORY_ROWS: tuple[tuple[str, str], ...] = (
    ('revenue', '营业总收入'),
    ('revenue_yoy', '营收同比'),
    ('profit', '归母净利润'),
    ('profit_yoy', '净利同比'),
    ('roe', 'ROE(加权)'),
    ('net_margin', '销售净利率'),
    ('gross_margin', '销售毛利率'),
    ('debt_ratio', '资产负债率'),
)


# ---------------------------------------------------------------------------
# 代码归一（缓存键必须同时包含标的与日期，避免串数据）
# ---------------------------------------------------------------------------
def normalize_security_code(code: Any) -> str:
    """任意格式代码 → 标准 ts_code；无法解析时原样大写返回。"""
    text = str(code or '').strip()
    if not text:
        return ''
    try:
        symbol, market = parse_code(text)
        return f'{symbol}.{market}'
    except (TypeError, ValueError):
        return text.upper()


def _plain_symbol(ts_code: str) -> str:
    return ts_code.split('.')[0] if '.' in ts_code else ts_code


def _em_symbol(ts_code: str) -> str:
    """东财财务指标接口要求带市场后缀（如 600519.SH）。"""
    return ts_code


# ---------------------------------------------------------------------------
# 数值/格式化工具
# ---------------------------------------------------------------------------
def _to_float(value: Any) -> float | None:
    """把 akshare 的混合数值（float / '1.2亿' / '12%' / False）转为 float。"""
    if value is None or value is False:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number != number:  # NaN
            return None
        return number
    text = str(value).strip().replace(',', '')
    if not text or text.lower() in {'nan', 'none', 'false', '-', '--'}:
        return None
    multiplier = 1.0
    if text.endswith('%'):
        text = text[:-1]
    elif text.endswith('亿'):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith('万'):
        multiplier = 1e4
        text = text[:-1]
    elif text.endswith('元'):
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _fmt_number(value: Any, digits: int = 2) -> str:
    number = _to_float(value)
    if number is None:
        return '—'
    return f'{number:,.{digits}f}'


def _fmt_percent(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return '—'
    return f'{number:.2f}%'


def _fmt_money(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return '—'
    if abs(number) >= 1e8:
        return f'{number / 1e8:.2f}亿'
    if abs(number) >= 1e4:
        return f'{number / 1e4:.2f}万'
    return f'{number:.2f}'


def _fmt_metric(key: str, value: Any) -> str:
    if key in PERCENT_METRICS:
        return _fmt_percent(value)
    if key in MONEY_METRICS:
        return _fmt_money(value)
    return _fmt_number(value)


def _date_text(value: Any) -> str:
    """任意日期字段 → ``YYYY-MM-DD``；失败返回空串。"""
    if value is None or value is False:
        return ''
    text = str(value).strip()
    if not text or text.lower() in {'nan', 'none', 'nat'}:
        return ''
    return text[:10]


def _is_empty_frame(frame: Any) -> bool:
    try:
        if frame is None:
            return True
        if hasattr(frame, 'empty'):
            return bool(frame.empty)
        if hasattr(frame, 'height'):
            return int(frame.height) == 0
    except (TypeError, ValueError):
        return True
    return not frame


def _frame_rows(frame: Any) -> list[dict[str, Any]]:
    """DataFrame → dict 列表；兼容 pandas / polars。"""
    if _is_empty_frame(frame):
        return []
    if hasattr(frame, 'to_dicts'):
        return list(frame.to_dicts())
    if hasattr(frame, 'to_dict'):
        try:
            return list(frame.to_dict(orient='records'))
        except (TypeError, ValueError):
            return []
    return []


def _metric_block(key: str, label: str, value: Any) -> dict[str, Any]:
    return {
        'key': key,
        'label': label,
        'value': _to_float(value),
        'display': _fmt_metric(key, value),
    }


# ---------------------------------------------------------------------------
# 财务指标解析
# ---------------------------------------------------------------------------
_EM_REQUIRED = {'REPORT_DATE', 'TOTALOPERATEREVE', 'PARENTNETPROFIT'}

_EM_METRIC_MAP: tuple[tuple[str, str, str], ...] = (
    ('roe', 'ROEJQ', 'ROE(加权)'),
    ('net_margin', 'XSJLL', '销售净利率'),
    ('gross_margin', 'XSMLL', '销售毛利率'),
    ('revenue_yoy', 'TOTALOPERATEREVETZ', '营收同比'),
    ('profit_yoy', 'PARENTNETPROFITTZ', '净利同比'),
    ('eps', 'EPSJB', '每股收益'),
    ('debt_ratio', 'ZCFZL', '资产负债率'),
    ('current_ratio', 'LD', '流动比率'),
    ('quick_ratio', 'SD', '速动比率'),
    ('inventory_turnover', 'CHZZL', '存货周转率'),
)


def _parse_em_financial(frame: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """解析东财「主要指标」。返回 (财务块, 警告列表)。"""
    warnings: list[str] = []
    rows = _frame_rows(frame)
    if not rows:
        return None, ['东财财务指标返回空表']
    missing = _EM_REQUIRED - set(rows[0].keys())
    if missing:
        warnings.append(f'东财财务指标缺少列: {sorted(missing)}')
    # 接口顺序不可依赖：统一按报告期升序后取最后一期，避免把最早一期当最新
    sorted_rows = sorted(rows, key=lambda row: _date_text(row.get('REPORT_DATE')))
    latest = sorted_rows[-1]
    report_period = _date_text(latest.get('REPORT_DATE'))
    if not report_period:
        warnings.append('东财财务指标缺少有效报告期')
        return None, warnings
    notice_date = _date_text(latest.get('NOTICE_DATE'))
    metrics = {
        key: _metric_block(key, label, latest.get(col))
        for key, col, label in _EM_METRIC_MAP
    }
    missing_cols = [col for _, col, _ in _EM_METRIC_MAP
                    if col not in latest]
    if missing_cols:
        warnings.append(f'东财财务指标缺少字段: {missing_cols}')

    # 历史表按报告期升序取最近 4 期
    recent = sorted_rows[-4:]
    periods = [_date_text(row.get('REPORT_DATE')) for row in recent]
    history_values = {
        'revenue': [row.get('TOTALOPERATEREVE') for row in recent],
        'revenue_yoy': [row.get('TOTALOPERATEREVETZ') for row in recent],
        'profit': [row.get('PARENTNETPROFIT') for row in recent],
        'profit_yoy': [row.get('PARENTNETPROFITTZ') for row in recent],
        'roe': [row.get('ROEJQ') for row in recent],
        'net_margin': [row.get('XSJLL') for row in recent],
        'gross_margin': [row.get('XSMLL') for row in recent],
        'debt_ratio': [row.get('ZCFZL') for row in recent],
    }
    history_rows = []
    for key, label in HISTORY_ROWS:
        values = history_values.get(key, [])
        raw = [_to_float(v) for v in values]
        history_rows.append({
            'key': key,
            'label': label,
            'raw': raw,
            'values': [_fmt_metric(key, v) for v in values],
        })
    return {
        'report_period': report_period,
        'notice_date': notice_date,
        'report_name': str(latest.get('REPORT_DATE_NAME') or ''),
        'source': SOURCE_EM,
        'metrics': metrics,
        'history': {'periods': periods, 'rows': history_rows},
        'warnings': warnings,
    }, warnings


_THS_METRIC_MAP: tuple[tuple[str, str, str], ...] = (
    ('roe', '净资产收益率', 'ROE(加权)'),
    ('net_margin', '销售净利率', '销售净利率'),
    ('gross_margin', '销售毛利率', '销售毛利率'),
    ('revenue_yoy', '营业总收入同比增长率', '营收同比'),
    ('profit_yoy', '净利润同比增长率', '净利同比'),
    ('eps', '基本每股收益', '每股收益'),
    ('debt_ratio', '资产负债率', '资产负债率'),
    ('quick_ratio', '速动比率', '速动比率'),
    ('inventory_turnover', '存货周转率', '存货周转率'),
)

_THS_HISTORY_MAP: tuple[tuple[str, str, str], ...] = (
    ('revenue', '营业总收入', '营业总收入'),
    ('revenue_yoy', '营业总收入同比增长率', '营收同比'),
    ('profit', '净利润', '归母净利润'),
    ('profit_yoy', '净利润同比增长率', '净利同比'),
    ('roe', '净资产收益率', 'ROE(加权)'),
    ('net_margin', '销售净利率', '销售净利率'),
    ('gross_margin', '销售毛利率', '销售毛利率'),
    ('debt_ratio', '资产负债率', '资产负债率'),
)


def _parse_ths_financial(frame: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """解析同花顺「财务摘要」（值多为 '1.2亿' / '12.3%' 字符串）。"""
    warnings: list[str] = []
    rows = _frame_rows(frame)
    if not rows:
        return None, ['同花顺财务摘要返回空表']
    if '报告期' not in rows[0]:
        return None, ['同花顺财务摘要缺少「报告期」列']
    sorted_rows = sorted(rows, key=lambda row: _date_text(row.get('报告期')))
    latest = sorted_rows[-1]
    metrics = {}
    for key, col, label in _THS_METRIC_MAP:
        if col not in latest:
            warnings.append(f'同花顺财务摘要缺少字段: {col}')
        metrics[key] = _metric_block(key, label, latest.get(col))
    recent = sorted_rows[-4:]
    periods = [_date_text(row.get('报告期')) for row in recent]
    history_rows = []
    for key, col, label in _THS_HISTORY_MAP:
        values = [row.get(col) for row in recent]
        history_rows.append({
            'key': key,
            'label': label,
            'raw': [_to_float(v) for v in values],
            'values': [_fmt_metric(key, v) for v in values],
        })
    return {
        'report_period': periods[-1] if periods else '',
        'notice_date': '',
        'report_name': '',
        'source': SOURCE_THS,
        'metrics': metrics,
        'history': {'periods': periods, 'rows': history_rows},
        'warnings': warnings,
    }, warnings


# ---------------------------------------------------------------------------
# 默认抓取器（延迟导入 akshare，便于测试注入替身）
# ---------------------------------------------------------------------------
def _default_fetchers() -> dict[str, Callable[[str], Any]]:
    def financial_em(ts_code: str):
        import akshare as ak  # noqa: PLC0415

        return ak.stock_financial_analysis_indicator_em(
            symbol=_em_symbol(ts_code), indicator='按报告期')

    def financial_ths(ts_code: str):
        import akshare as ak  # noqa: PLC0415

        return ak.stock_financial_abstract_ths(
            symbol=_plain_symbol(ts_code), indicator='按报告期')

    def valuation(ts_code: str):
        import akshare as ak  # noqa: PLC0415

        return ak.stock_value_em(symbol=_plain_symbol(ts_code))

    def news(ts_code: str):
        import akshare as ak  # noqa: PLC0415

        return ak.stock_news_em(symbol=_plain_symbol(ts_code))

    return {
        'financial_em': financial_em,
        'financial_ths': financial_ths,
        'valuation': valuation,
        'news': news,
    }


# ---------------------------------------------------------------------------
# 估值
# ---------------------------------------------------------------------------
_VALUE_MAP: tuple[tuple[str, str, str], ...] = (
    ('pe_ttm', 'PE(TTM)', 'PE(TTM)'),
    ('pe_static', 'PE(静)', 'PE(静)'),
    ('pb', '市净率', '市净率'),
    ('total_mv', '总市值', '总市值'),
    ('float_mv', '流通市值', '流通市值'),
)


def _parse_valuation(frame: Any) -> tuple[dict[str, Any] | None, list[str]]:
    rows = _frame_rows(frame)
    if not rows:
        return None, ['估值接口返回空表']
    latest = rows[-1]
    warnings = [f'估值接口缺少字段: {col}'
                for _, col, _ in _VALUE_MAP if col not in latest]
    metrics = {
        key: _metric_block(key, label, latest.get(col))
        for key, col, label in _VALUE_MAP
    }
    if not any(item['value'] is not None for item in metrics.values()):
        return None, warnings + ['估值接口无有效数值']
    return {
        'source': SOURCE_VALUE,
        'as_of': _date_text(latest.get('数据日期')),
        'metrics': metrics,
        'warnings': warnings,
    }, warnings


# ---------------------------------------------------------------------------
# 本地公告/业绩预告/分红/风险提示
# ---------------------------------------------------------------------------
def _resolve_root(config: Any, store_root: Path | str | None) -> Path:
    if store_root:
        return Path(store_root)
    try:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        return resolve_store_root(config if config is not None else get_config())
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'[finance] 本地列存根目录解析失败，跳过本地数据: {exc}')
        return Path('data/local_store')


def _local_event_news(ts_code: str, root: Path, today: date) -> list[dict[str, Any]]:
    from src.data.events import load_events  # noqa: PLC0415

    frame = load_events(root)
    if _is_empty_frame(frame) or 'ts_code' not in frame.columns:
        return []
    cutoff = datetime.combine(today - timedelta(days=LOCAL_EVENT_DAYS), datetime.min.time())
    try:
        subset = frame.filter(pl.col('ts_code') == ts_code)
        if 'publish_time' in subset.columns:
            subset = subset.filter(pl.col('publish_time') >= pl.lit(cutoff))
        subset = subset.sort('publish_time', descending=True).head(NEWS_LIMIT)
    except (pl.exceptions.PolarsError, TypeError, ValueError) as exc:
        logger.warning(f'[finance] 本地事件过滤失败 {ts_code}: {exc}')
        return []
    items: list[dict[str, Any]] = []
    for row in subset.to_dicts():
        title = str(row.get('title') or '').strip()
        if not title:
            continue
        published = str(row.get('publish_time') or '')
        items.append({
            'time': published[:16],
            'date': published[:10],
            'type': str(row.get('event_type') or '公告'),
            'title': title,
            'source': str(row.get('source') or SOURCE_LOCAL),
            'url': '',
            'summary': '',
        })
    return items


def _local_dividend_news(ts_code: str, root: Path, today: date) -> list[dict[str, Any]]:
    from src.data.local_store.store import LocalFactorStore  # noqa: PLC0415

    try:
        frame = LocalFactorStore(root).dividends(ts_code)
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'[finance] 本地分红读取失败 {ts_code}: {exc}')
        return []
    if _is_empty_frame(frame):
        return []
    rows = sorted(_frame_rows(frame),
                  key=lambda row: _date_text(row.get('report_date')),
                  reverse=True)
    cutoff = today - timedelta(days=DIVIDEND_DAYS)
    items: list[dict[str, Any]] = []
    for row in rows:
        report_date = _date_text(row.get('report_date'))
        if not report_date:
            continue
        try:
            if date.fromisoformat(report_date) < cutoff:
                continue
        except ValueError:
            continue
        cash = _to_float(row.get('cash_div'))
        share = _to_float(row.get('share_div'))
        parts = []
        if cash is not None:
            parts.append(f'每股派现 {cash:.4g} 元')
        if share is not None:
            parts.append(f'每股送转 {share:.4g} 股')
        detail = '，'.join(parts) or '分红方案已披露'
        items.append({
            'time': report_date,
            'date': report_date,
            'type': '分红送转',
            'title': f'{row.get("dividend_year") or ""} 年度分红：{detail}',
            'source': '本地分红库',
            'url': '',
            'summary': detail,
        })
        if len(items) >= 5:
            break
    return items


def _local_risk_news(ts_code: str, root: Path, today: date) -> list[dict[str, Any]]:
    try:
        from src.data.risk_flags import get_st_status_detail  # noqa: PLC0415

        detail = get_st_status_detail(ts_code, today, store_root=root)
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'[finance] ST 状态读取失败 {ts_code}: {exc}')
        return []
    if not isinstance(detail, dict) or detail.get('status') != 'st':
        return []
    return [{
        'time': _date_text(detail.get('as_of')) or today.isoformat(),
        'date': _date_text(detail.get('as_of')) or today.isoformat(),
        'type': '风险提示',
        'title': f'当前为风险警示股票（{detail.get("reason") or "ST"}）',
        'source': '本地风险快照',
        'url': '',
        'summary': '该标的处于风险警示状态，涨跌停与交易规则可能不同于普通股票。',
    }]


def _parse_news(frame: Any) -> tuple[list[dict[str, Any]], list[str]]:
    rows = _frame_rows(frame)
    if not rows:
        return [], ['个股新闻接口返回空表']
    warnings = []
    required = {'新闻标题', '发布时间'}
    if required - set(rows[0].keys()):
        warnings.append(f'个股新闻缺少列: {sorted(required - set(rows[0].keys()))}')
    items = []
    for row in rows:
        title = str(row.get('新闻标题') or '').strip()
        if not title:
            continue
        published = str(row.get('发布时间') or '')
        content = str(row.get('新闻内容') or '').strip()
        items.append({
            'time': published[:16],
            'date': published[:10],
            'type': '新闻',
            'title': title,
            'source': str(row.get('文章来源') or '东方财富'),
            'url': str(row.get('新闻链接') or ''),
            'summary': content[:300],
        })
    return items, warnings


# ---------------------------------------------------------------------------
# 缓存（键必须同时包含标的与日期）
# ---------------------------------------------------------------------------
_CACHE: 'OrderedDict[tuple[str, str], tuple[float, dict[str, Any]]]' = OrderedDict()
_CACHE_LOCK = threading.Lock()


def clear_finance_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def _cache_get(ts_code: str, day: str) -> dict[str, Any] | None:
    key = (ts_code, day)
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            return None
        expires, snapshot = entry
        if expires < _time.time():
            _CACHE.pop(key, None)
            return None
        _CACHE.move_to_end(key)
        return snapshot


def _cache_put(ts_code: str, day: str, snapshot: dict[str, Any]) -> None:
    key = (ts_code, day)
    with _CACHE_LOCK:
        _CACHE[key] = (_time.time() + CACHE_TTL_SECONDS, snapshot)
        while len(_CACHE) > CACHE_MAX_ITEMS:
            _CACHE.popitem(last=False)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------
def fetch_finance_snapshot(
    code: Any,
    name: str = '',
    *,
    as_of: Any = None,
    store_root: Path | str | None = None,
    config: Any = None,
    fetchers: dict[str, Callable[[str], Any]] | None = None,
    use_cache: bool = True,
    refresh: bool = False,
) -> dict[str, Any]:
    """构建财务+资讯快照。任何异常都收敛进 ``errors``，不向外抛。"""
    ts_code = normalize_security_code(code)
    day = as_of if isinstance(as_of, date) else date.today()
    day_text = day.isoformat()
    if not ts_code:
        logger.warning('[finance] 标的代码为空，返回空快照')
        return _empty_snapshot('', name, day_text, '标的代码为空')

    if use_cache and not refresh:
        cached = _cache_get(ts_code, day_text)
        if cached is not None:
            logger.info(f'[finance] 命中缓存 {ts_code}@{day_text}')
            return cached

    snapshot = _build_snapshot(ts_code, name, day, store_root, config, fetchers)
    if use_cache and (snapshot.get('report') or snapshot.get('news')):
        _cache_put(ts_code, day_text, snapshot)
    return snapshot


def _empty_snapshot(ts_code: str, name: str, day_text: str,
                    reason: str = '暂无数据') -> dict[str, Any]:
    return {
        'ok': False,
        'code': ts_code,
        'name': name,
        'as_of': day_text,
        'not_applicable': False,
        'report': None,
        'metrics': {},
        'valuation': None,
        'history': {'periods': [], 'rows': []},
        'news': [],
        'industry': '',
        'errors': [],
        'warnings': [reason],
        'sources': {},
    }


def _build_snapshot(
    ts_code: str,
    name: str,
    day: date,
    store_root: Path | str | None,
    config: Any,
    fetchers: dict[str, Callable[[str], Any]] | None,
) -> dict[str, Any]:
    snapshot = _empty_snapshot(ts_code, name, day.isoformat())
    snapshot['warnings'] = []  # 占位警告不能带入正常取数结果
    snapshot['not_applicable'] = is_index_code(ts_code)
    if snapshot['not_applicable']:
        snapshot['warnings'] = ['指数无财报与个股资讯']
        logger.info(f'[finance] {ts_code} 为指数，跳过财报/资讯取数')
        return snapshot

    fetch = dict(fetchers or _default_fetchers())
    root = _resolve_root(config, store_root)

    # 1) 财务指标：东财 → 同花顺 → 空态
    financial = None
    for key in ('financial_em', 'financial_ths'):
        func = fetch.get(key)
        if func is None:
            continue
        try:
            frame = func(ts_code)
        except Exception as exc:  # noqa: BLE001 - 单源失败必须降级
            message = f'{type(exc).__name__}: {exc}'
            logger.warning(f'[finance] 财务源 {key} 失败 {ts_code}: {message}')
            snapshot['errors'].append(
                {'stage': f'financial.{key}', 'source': 'akshare', 'message': message})
            continue
        parsed = None
        try:
            if key == 'financial_em':
                parsed, warnings = _parse_em_financial(frame)
            else:
                parsed, warnings = _parse_ths_financial(frame)
        except (TypeError, ValueError, KeyError, IndexError) as exc:
            message = f'{type(exc).__name__}: {exc}'
            logger.warning(f'[finance] 财务源 {key} 解析失败 {ts_code}: {message}')
            snapshot['errors'].append(
                {'stage': f'financial.{key}.parse', 'source': 'akshare', 'message': message})
            continue
        snapshot['warnings'].extend(warnings)
        if parsed:
            financial = parsed
            snapshot['sources']['financial'] = parsed['source']
            break
        logger.warning(f'[finance] 财务源 {key} 无有效数据 {ts_code}: {warnings}')

    # 2) 估值
    valuation = None
    func = fetch.get('valuation')
    if func is not None:
        try:
            frame = func(ts_code)
            valuation, warnings = _parse_valuation(frame)
            snapshot['warnings'].extend(warnings)
            if valuation:
                snapshot['sources']['valuation'] = valuation['source']
        except Exception as exc:  # noqa: BLE001
            message = f'{type(exc).__name__}: {exc}'
            logger.warning(f'[finance] 估值源失败 {ts_code}: {message}')
            snapshot['errors'].append(
                {'stage': 'valuation', 'source': 'akshare', 'message': message})

    # 3) 本地公告/分红/风险
    local_news: list[dict[str, Any]] = []
    for collector, label in (
            (lambda: _local_event_news(ts_code, root, day), '本地事件'),
            (lambda: _local_dividend_news(ts_code, root, day), '本地分红'),
            (lambda: _local_risk_news(ts_code, root, day), '本地风险'),
    ):
        try:
            local_news.extend(collector())
        except Exception as exc:  # noqa: BLE001
            message = f'{type(exc).__name__}: {exc}'
            logger.warning(f'[finance] {label}读取失败 {ts_code}: {message}')
            snapshot['errors'].append(
                {'stage': label, 'source': 'local', 'message': message})

    # 4) 远程个股新闻
    remote_news: list[dict[str, Any]] = []
    func = fetch.get('news')
    if func is not None:
        try:
            frame = func(ts_code)
            remote_news, warnings = _parse_news(frame)
            snapshot['warnings'].extend(warnings)
            if remote_news:
                snapshot['sources']['news'] = SOURCE_NEWS
            else:
                logger.warning(f'[finance] 个股新闻无有效数据 {ts_code}: {warnings}')
        except Exception as exc:  # noqa: BLE001
            message = f'{type(exc).__name__}: {exc}'
            logger.warning(f'[finance] 个股新闻源失败 {ts_code}: {message}')
            snapshot['errors'].append(
                {'stage': 'news', 'source': 'akshare', 'message': message})

    news = _merge_news(local_news, remote_news)
    industry = _local_industry(ts_code, root, day)
    if not name:
        name = _local_name(ts_code, root)

    if financial:
        snapshot['report'] = {
            'period': financial.get('report_period', ''),
            'notice_date': financial.get('notice_date', ''),
            'report_name': financial.get('report_name', ''),
            'source': financial.get('source', ''),
        }
        snapshot['metrics'] = financial.get('metrics', {})
        snapshot['history'] = financial.get('history', {'periods': [], 'rows': []})
    if valuation:
        snapshot['valuation'] = {
            'as_of': valuation.get('as_of', ''),
            'source': valuation.get('source', ''),
            'metrics': valuation.get('metrics', {}),
        }
    snapshot.update({
        'name': name,
        'news': news,
        'industry': industry,
        'ok': bool(financial or news),
    })
    if not snapshot['ok']:
        snapshot['warnings'].append('财务与资讯均无可展示数据')
    logger.info(
        f'[finance] {ts_code} 取数完成: financial={bool(financial)} '
        f'valuation={bool(valuation)} news={len(news)} errors={len(snapshot["errors"])}')
    return snapshot


def _merge_news(local_items: list[dict[str, Any]],
                remote_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in list(local_items) + list(remote_items):
        if not isinstance(item, dict):
            continue
        title = str(item.get('title') or '').strip()
        if not title:
            continue
        key = (str(item.get('date') or ''), title)
        if key in seen:
            continue
        seen.add(key)
        merged.append({**item, 'title': title})
    merged.sort(key=lambda item: str(item.get('time') or ''), reverse=True)
    return merged[:NEWS_LIMIT]


def _local_industry(ts_code: str, root: Path, day: date) -> str:
    try:
        from src.data.industry import get_industry  # noqa: PLC0415

        info = get_industry(ts_code, day, store_root=root)
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'[finance] 行业读取失败 {ts_code}: {exc}')
        return ''
    if not isinstance(info, dict) or info.get('status') != 'known':
        return ''
    l1 = str(info.get('industry_l1') or '')
    l2 = str(info.get('industry_l2') or '')
    return l2 if l2 and l2 != 'unknown' else (l1 if l1 != 'unknown' else '')


def _local_name(ts_code: str, root: Path) -> str:
    path = root / 'meta' / 'names.json'
    if not path.is_file():
        return ''
    try:
        import json  # noqa: PLC0415

        mapping = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'[finance] 名称快照读取失败 {path}: {exc}')
        return ''
    if not isinstance(mapping, dict):
        return ''
    value = mapping.get(ts_code)
    return str(value) if value else ''
