#!/usr/bin/env python3

r"""构建多资产（境内权益 / 利率债 / 黄金 / 商品）ETF 候选池与数据口径表。

产物（``--apply`` 后）
=====================
- ``data/local_store/meta/multi_asset/etf_pool.parquet``：一只标的一行
- ``data/local_store/meta/multi_asset/etf_gap_detail.csv``：中间断档逐段明细
- ``data/local_store/meta/multi_asset/etf_event_gaps.csv``：未登记除权事件明细
- ``docs/evidence/summary/summary.json``：分档计数与统一起点（一次性证据，随 Git 留存）
- ``data/local_store/meta/multi_asset/manifest.json``：行数 / 口径 / SHA256

候选先写 ``_staging/multi_asset_etf_20261110/``，校验通过后原子替换。

口径
====
- 标的域：``meta/fund_basic.parquet`` 中 ``fund_type='ETF'``，排除货币 ETF；
  退市 ETF 一并纳入（``tier='delisted'``），用于暴露生存偏差。
- 资产分层：黄金股 → 境内权益；商品期货 ETF（159980/159981/159985）→ 商品；
  名称含黄金/白银 → 黄金；511 段或名称含「债」→ 利率债/信用债/可转债；
  513 段或跨境关键词 → 跨境（可选，``in_scope=False``）；其余 → 境内权益。
- 原油无场内 ETF，仅有 LOF，按 ``instrument_type='LOF'``、``in_scope=False`` 记录。
- 流动性：近 60 个交易日 ``amount`` 中位数（元）。
  ``core`` = 上市≥3 年 且 近 60 日中位成交额≥1 亿元 且 存续；
  ``extend`` = 上市≥1 年 且 ≥2,000 万元；``watch`` = 其余存续标的。
- 断档：行区间内缺失的交易日按 ``src.data.trading_calendar`` 权威段比对；
  与 ``meta/universe/internal_gap_inventory.csv``、``meta/anomalies/placeholder_quote_codes.csv``
  对照区分「本地缺失」与「源端无记录」。
- 除权事件：``factors/fund_adj_factor.parquet`` 中 ``hfq_factor`` 相对前一交易日
  变化 >1e-9 即视为一次隐含除权；与 ``meta/fund_corporate_actions_2019plus.parquet``
  逐条对照，差集即为「未登记事件」。

用法::

    .venv\\Scripts\\python.exe tools\\build_multi_asset_etf_pool.py
    .venv\\Scripts\\python.exe tools\\build_multi_asset_etf_pool.py --apply
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
STAGING = ROOT / '_staging' / 'multi_asset_etf_20261110'
TARGET_DIR = STORE / 'meta' / 'multi_asset'

#: 商品期货 ETF（境内仅这 3 只跟踪商品期货指数）
FUTURES_ETF = {'159980.SZ', '159981.SZ', '159985.SZ'}

#: 原油仅有 LOF，无场内 ETF
CRUDE_LOF = {'501018.SH', '160723.SZ', '161129.SZ', '160416.SZ'}

CROSS_KEYWORDS = (
    '纳指', '纳斯达克', '标普', '恒生', '港股', '中概', '美国', '日经', '德国',
    '法国', '亚太', '东南亚', '沙特', '越南', '印度', '海外', '全球', '国际',
    '日韩', '英国', '欧洲', '日本',
)

BROAD_KEYWORDS = (
    '沪深300', '中证500', '中证1000', '中证2000', '上证50', '上证180', '上证380',
    '创业板', '科创50', '科创100', 'A50', 'MSCI', '中证100', '中证800',
    '深证100', '深证成指', '国证2000', '中小盘', '大盘', '宽基', '全指',
)

INDUSTRY_KEYWORDS = (
    '医药', '医疗', '生物', '消费', '食品', '白酒', '家电', '科技', '信息',
    '半导体', '芯片', '军工', '国防', '新能源', '光伏', '电池', '汽车',
    '有色', '化工', '煤炭', '钢铁', '地产', '银行', '证券', '保险', '金融',
    '农业', '养殖', '传媒', '游戏', '计算机', '通信', '环保', '电力', '基建',
    '机械', '建材', '旅游', '教育', '机器人', '人工智能', '云计算', '大数据',
)

COLUMNS = [
    'asset_class', 'sub_class', 'tier', 'in_scope', 'instrument_type', 'ts_code',
    'name', 'fund_type', 'market', 'status', 'list_date', 'delist_date',
    'first_date', 'last_date', 'rows', 'coverage_start', 'coverage_end',
    'missing_days', 'missing_runs_ge3', 'max_gap_days', 'gap_segments',
    'zero_volume_days', 'adv_20d', 'adv_60d', 'adv_120d', 'adv_all',
    'amount_unit_ratio', 'factor_rows', 'factor_last_date', 'factor_missing_days',
    'factor_missing_internal', 'factor_tail_lag_days', 'factor_change_count',
    'ca_event_count', 'ca_last_date', 'implied_event_count', 'event_not_in_ca',
    'event_after_ca_end', 'event_covered_divsplit', 'event_unregistered_anywhere',
    'div_event_count', 'split_event_count', 'event_in_divsplit_not_ca',
    'unregistered_dates', 'gap_kind', 'fake_bar_hits', 'note',
]

#: 权威除权表的登记起点（此前事件不在该表口径内）
CA_SCOPE_START = date(2019, 9, 18)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _calendar(as_of: date) -> list[date]:
    from src.data.trading_calendar import get_calendar

    cal = get_calendar(STORE, as_of=as_of)
    return list(cal.authoritative_dates())


def _classify(code: str, name: str) -> tuple[str, str, str]:
    """返回 (asset_class, sub_class, instrument_type)。"""
    if code in CRUDE_LOF:
        return 'commodity', '原油(LOF)', 'LOF'
    if any(k in name for k in ('货币', '日利', '添益', '添利', '日日鑫', '天天金', '财富宝')):
        return 'cash_like', '交易型货币', 'ETF'
    if '黄金股' in name:
        return 'equity', '行业/主题:黄金股', 'ETF'
    if code in FUTURES_ETF:
        if '豆粕' in name:
            return 'commodity', '农产品:豆粕', 'ETF'
        if '有色' in name:
            return 'commodity', '有色期货', 'ETF'
        return 'commodity', '能化期货', 'ETF'
    if any(k in name for k in ('黄金', '白银', '上海金', '金ETF')):
        return 'gold', '实物黄金', 'ETF'
    if code.startswith('511') or '债' in name:
        if '短融' in name:
            return 'cash_like', '现金替代:短融', 'ETF'
        if '国债' in name:
            return 'bond', '利率债:国债', 'ETF'
        if '政金' in name or '国开' in name or '农发' in name or '口行' in name:
            return 'bond', '利率债:政金债', 'ETF'
        if '地方债' in name:
            return 'bond', '利率债:地方债', 'ETF'
        if '转债' in name:
            return 'bond', '可转债', 'ETF'
        if '城投' in name:
            return 'bond', '信用债:城投', 'ETF'
        if '公司债' in name:
            return 'bond', '信用债:公司债', 'ETF'
        if '科创债' in name:
            return 'bond', '信用债:科创债', 'ETF'
        if '信用' in name or '产业债' in name:
            return 'bond', '信用债:其他', 'ETF'
        return 'bond', '其他债券', 'ETF'
    if code.startswith('513') or any(k in name for k in CROSS_KEYWORDS):
        return 'cross_border', '跨境权益(可选)', 'ETF'
    if any(k in name for k in BROAD_KEYWORDS):
        return 'equity', '宽基', 'ETF'
    for keyword in INDUSTRY_KEYWORDS:
        if keyword in name:
            return 'equity', f'行业/主题:{keyword}', 'ETF'
    return 'equity', '行业/主题:其他', 'ETF'


def _med(values: pl.Series) -> float | None:
    if values.is_empty():
        return None
    value = values.median()
    return float(value) if value is not None else None


def _runs(days: list[date]) -> list[tuple[date, date, int]]:
    """把升序缺失日列表切成连续区间（按自然日相邻判定）。"""
    if not days:
        return []
    out: list[tuple[date, date, int]] = []
    start = prev = days[0]
    count = 1
    for day in days[1:]:
        if day - prev == timedelta(days=1):
            prev = day
            count += 1
        else:
            out.append((start, prev, count))
            start = prev = day
            count = 1
    out.append((start, prev, count))
    return out


def _load_events() -> tuple[dict[str, list[date]], dict[str, list[date]],
                            dict[str, list[date]]]:
    ca = pl.read_parquet(STORE / 'meta' / 'fund_corporate_actions_2019plus.parquet')
    div = pl.read_parquet(STORE / 'factors' / 'fund_dividend.parquet')
    split = pl.read_parquet(STORE / 'factors' / 'fund_share_change.parquet')
    def key(value: Any) -> str:
        return value[0] if isinstance(value, tuple) else value

    ca_map = {key(code): sorted(set(group['date'].to_list()))
              for code, group in ca.group_by('ts_code')}
    div_map = {key(code): sorted(set(d for d in group['ex_date'].to_list() if d))
               for code, group in div.group_by('ts_code')}
    split_map = {key(code): sorted(set(d for d in group['ex_date'].to_list() if d))
                 for code, group in split.group_by('ts_code')}
    return ca_map, div_map, split_map


def _load_gap_inventory() -> dict[str, dict[str, Any]]:
    path = STORE / 'meta' / 'universe' / 'internal_gap_inventory.csv'
    if not path.is_file():
        return {}
    frame = pl.read_csv(path, infer_schema_length=0)
    out: dict[str, dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        out[row['ts_code']] = {
            'internal_missing': int(row.get('internal_missing') or 0),
            'missing_zero_volume': int(row.get('missing_zero_volume') or 0),
            'missing_segments': row.get('missing_segments') or '',
            'skip_reason': row.get('skip_reason') or '',
        }
    return out


def _load_placeholders() -> dict[str, list[tuple[date, date]]]:
    path = STORE / 'meta' / 'anomalies' / 'placeholder_quote_codes.csv'
    if not path.is_file():
        return {}
    frame = pl.read_csv(path, infer_schema_length=0)
    out: dict[str, list[tuple[date, date]]] = {}
    for row in frame.iter_rows(named=True):
        try:
            start = date.fromisoformat(row['start'][:10])
            end = date.fromisoformat(row['end'][:10])
        except (TypeError, ValueError):
            continue
        out.setdefault(row['ts_code'], []).append((start, end))
    return out


def _load_fake_bars() -> dict[str, int]:
    path = STORE / 'meta' / 'anomalies' / 'fund_fake_bar_bars.csv'
    if not path.is_file():
        return {}
    frame = pl.read_csv(path, infer_schema_length=0)
    return {code: int(number) for code, number in frame.group_by('ts_code').len().iter_rows()}


def build(as_of: date) -> tuple[pl.DataFrame, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    calendar = _calendar(as_of)
    cal_set = set(calendar)
    basic = pl.read_parquet(STORE / 'meta' / 'fund_basic.parquet')
    factors = pl.read_parquet(
        STORE / 'factors' / 'fund_adj_factor.parquet',
        columns=['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor'],
    ).filter(pl.col('trade_date') <= as_of)
    factor_map: dict[str, pl.DataFrame] = {
        (code[0] if isinstance(code, tuple) else code): group.sort('trade_date')
        for code, group in factors.group_by('ts_code')
    }
    ca_map, div_map, split_map = _load_events()
    ca_table = pl.read_parquet(STORE / 'meta' / 'fund_corporate_actions_2019plus.parquet')
    ca_scope_end = ca_table['date'].max()
    inventory = _load_gap_inventory()
    placeholders = _load_placeholders()
    fake_bars = _load_fake_bars()

    rows: list[dict[str, Any]] = []
    gap_detail: list[dict[str, Any]] = []
    event_detail: list[dict[str, Any]] = []

    for record in basic.filter(pl.col('fund_type') == 'ETF').iter_rows(named=True):
        code = record['ts_code']
        name = record['name'] or ''
        asset_class, sub_class, instrument = _classify(code, name)
        if asset_class == 'money':
            continue
        market = (record['market'] or code.rsplit('.', 1)[-1]).lower()
        path = STORE / 'quotes' / 'fund' / market / f'{code}.parquet'
        if not path.is_file():
            continue
        quotes = pl.read_parquet(
            path, columns=['date', 'close', 'volume', 'amount']
        ).filter(pl.col('date') <= as_of).sort('date')
        if quotes.is_empty():
            continue

        first = quotes['date'][0]
        last = quotes['date'][-1]
        present = set(quotes['date'].to_list())
        expected = [d for d in calendar if first <= d <= last]
        missing = [d for d in expected if d not in present]

        valid = quotes.filter((pl.col('close') > 0) & (pl.col('volume') > 0)
                              & (pl.col('amount') > 0))
        coverage_start = valid['date'][0] if not valid.is_empty() else None
        coverage_end = last

        runs = _runs(missing)
        runs_ge3 = sum(1 for item in runs if item[2] >= 3)
        max_gap = max((item[2] for item in runs), default=0)
        gap_label = ';'.join(f'{a.isoformat()}~{b.isoformat()}({n})' for a, b, n in runs[:6])

        inv = inventory.get(code, {})
        ph_ranges = placeholders.get(code, [])
        if inv.get('internal_missing', 0) > 0:
            gap_kind = 'local_missing'
        elif ph_ranges:
            gap_kind = 'placeholder'
        elif missing:
            gap_kind = 'source_absent'
        else:
            gap_kind = 'none'

        for a, b, n in runs:
            if n < 3:
                continue
            kind = gap_kind
            if inv.get('missing_zero_volume', 0) and n == inv.get('internal_missing', -1):
                kind = 'suspension_evidence'
            gap_detail.append({
                'ts_code': code, 'asset_class': asset_class, 'name': name,
                'start': a.isoformat(), 'end': b.isoformat(),
                'trading_days': n, 'kind': kind,
                'inventory_internal_missing': inv.get('internal_missing', ''),
                'inventory_missing_zero_volume': inv.get('missing_zero_volume', ''),
                'placeholder_ranges': ';'.join(f'{s}~{e}' for s, e in ph_ranges),
            })

        tradable = quotes.filter(pl.col('amount') > 0)
        adv20 = _med(tradable.tail(20)['amount'])
        adv60 = _med(tradable.tail(60)['amount'])
        adv120 = _med(tradable.tail(120)['amount'])
        adv_all = _med(tradable['amount'])
        unit = quotes.filter((pl.col('volume') > 0) & (pl.col('close') > 0))
        unit_series = (unit['amount'] / (unit['volume'] * unit['close']))
        unit_ratio = _med(unit_series)

        factor = factor_map.get(code)
        if factor is None or factor.is_empty():
            factor_rows = 0
            factor_last = None
            factor_missing = len(expected)
            tail_lag = None
            changes: list[tuple[date, float]] = []
        else:
            factor_rows = factor.height
            factor_last = factor['trade_date'][-1]
            factor_dates = set(factor['trade_date'].to_list())
            factor_missing = sum(1 for d in expected if d not in factor_dates)
            tail_lag = sum(1 for d in calendar if factor_last < d <= last)
            hfq = factor['hfq_factor'].to_list()
            dates = factor['trade_date'].to_list()
            changes = []
            for index in range(1, len(hfq)):
                before, now = hfq[index - 1], hfq[index]
                if before and now and abs(now / before - 1.0) > 1e-9:
                    changes.append((dates[index], now / before))

        implied = {d for d, _ in changes}
        ca_dates = set(ca_map.get(code, []))
        div_dates = set(div_map.get(code, []))
        split_dates = set(split_map.get(code, []))
        unregistered = sorted(d for d in implied - ca_dates if d >= CA_SCOPE_START)
        after_ca_end = [d for d in unregistered if d > ca_scope_end]
        divsplit_only = sorted((div_dates | split_dates) - ca_dates)

        covered = 0
        for day in unregistered:
            ratio = next(r for d, r in changes if d == day)
            # 事件表 ex_date 与因子生效日可相差 1 个交易日（如 2026-04-14 → 04-15）
            div_hit = next((e for e in div_dates if abs((e - day).days) <= 5), None)
            split_hit = next((e for e in split_dates if abs((e - day).days) <= 5), None)
            covered += int(div_hit is not None or split_hit is not None)
            event_detail.append({
                'ts_code': code, 'asset_class': asset_class, 'name': name,
                'date': day.isoformat(), 'hfq_ratio': round(ratio, 8),
                'in_dividend_table': div_hit is not None,
                'dividend_ex_date': div_hit.isoformat() if div_hit else '',
                'in_share_change_table': split_hit is not None,
                'share_ex_date': split_hit.isoformat() if split_hit else '',
                'in_corporate_actions_table': False,
                'after_ca_table_end': day > ca_scope_end,
                'unregistered_anywhere': div_hit is None and split_hit is None,
            })

        list_date = record['list_date']
        delist_date = record['delist_date']
        listed_days = (as_of - list_date).days if list_date else None
        if record['status'] == 'D' or delist_date is not None:
            tier = 'delisted'
        elif adv60 is not None and adv60 >= 1e8 and listed_days is not None and listed_days >= 1095:
            tier = 'core'
        elif adv60 is not None and adv60 >= 2e7 and listed_days is not None and listed_days >= 365:
            tier = 'extend'
        else:
            tier = 'watch'

        in_scope = asset_class in {'equity', 'bond', 'gold', 'commodity'}
        if asset_class == 'equity' and tier == 'watch':
            # 权益 ETF 数量过多，watch 档（成交额<2,000 万或上市<1 年）不进候选
            continue
        if instrument == 'LOF':
            in_scope = False

        note_parts: list[str] = []
        if instrument == 'LOF':
            note_parts.append('非 ETF，原油仅有 LOF 表达')
        if asset_class == 'cross_border':
            note_parts.append('境外权益，可选单列，不计入四类统一起点')
        if tier == 'delisted':
            note_parts.append('已退市，仅用于生存偏差核查')
        if inv.get('skip_reason'):
            note_parts.append(f"缺口台账:{inv['skip_reason']}")

        rows.append({
            'asset_class': asset_class, 'sub_class': sub_class, 'tier': tier,
            'in_scope': in_scope, 'instrument_type': instrument, 'ts_code': code,
            'name': name, 'fund_type': record['fund_type'], 'market': market,
            'status': record['status'], 'list_date': list_date,
            'delist_date': delist_date, 'first_date': first, 'last_date': last,
            'rows': quotes.height,
            'coverage_start': coverage_start, 'coverage_end': coverage_end,
            'missing_days': len(missing), 'missing_runs_ge3': runs_ge3,
            'max_gap_days': max_gap, 'gap_segments': gap_label,
            'zero_volume_days': int((quotes['volume'] == 0).sum()),
            'adv_20d': adv20, 'adv_60d': adv60, 'adv_120d': adv120,
            'adv_all': adv_all, 'amount_unit_ratio': unit_ratio,
            'factor_rows': factor_rows, 'factor_last_date': factor_last,
            'factor_missing_days': factor_missing,
            'factor_missing_internal': max(0, factor_missing - (tail_lag or 0)),
            'factor_tail_lag_days': tail_lag,
            'factor_change_count': len(changes), 'ca_event_count': len(ca_dates),
            'ca_last_date': max(ca_dates) if ca_dates else None,
            'implied_event_count': len(implied), 'event_not_in_ca': len(unregistered),
            'event_after_ca_end': len(after_ca_end),
            'event_covered_divsplit': covered,
            'event_unregistered_anywhere': len(unregistered) - covered,
            'div_event_count': len(div_dates), 'split_event_count': len(split_dates),
            'event_in_divsplit_not_ca': len(divsplit_only),
            'unregistered_dates': ','.join(d.isoformat() for d in unregistered),
            'gap_kind': gap_kind, 'fake_bar_hits': fake_bars.get(code, 0),
            'note': '；'.join(note_parts),
        })

    frame = pl.DataFrame(rows, infer_schema_length=None).select(COLUMNS)
    frame = frame.sort(['asset_class', 'tier', 'adv_60d'], descending=[False, False, True])
    kept = set(frame['ts_code'].to_list())
    gap_detail = [row for row in gap_detail if row['ts_code'] in kept]
    event_detail = [row for row in event_detail if row['ts_code'] in kept]

    summary = _summary(frame, calendar, as_of)
    return frame, gap_detail, event_detail, summary


def _summary(frame: pl.DataFrame, calendar: list[date], as_of: date) -> dict[str, Any]:
    def span(start: date, end: date) -> int:
        return sum(1 for d in calendar if start <= d <= end)

    out: dict[str, Any] = {'as_of': as_of.isoformat(), 'classes': {}}
    scope = frame.filter(pl.col('in_scope'))
    out['total_rows'] = frame.height
    out['in_scope_rows'] = scope.height
    out['out_of_scope_rows'] = frame.filter(~pl.col('in_scope')).height
    out['tier_counts'] = {tier: scope.filter(pl.col('tier') == tier).height
                          for tier in ('core', 'extend', 'watch', 'delisted')}

    def representative(subset: pl.DataFrame) -> dict[str, Any]:
        """同一起点窗口内（最早起点+365 天）成交额最大的标的。"""
        earliest = min(subset['coverage_start'].to_list())
        window = subset.filter(
            pl.col('coverage_start') <= earliest + timedelta(days=365))
        top = window.sort('adv_60d', descending=True).row(0, named=True)
        return {
            'code': top['ts_code'], 'name': top['name'],
            'start': top['coverage_start'].isoformat(),
            'adv_60d': top['adv_60d'],
        }

    for asset_class, prefix in (('equity', ''),
                                ('bond', '利率债'),
                                ('gold', ''),
                                ('commodity', '')):
        subset = scope.filter((pl.col('asset_class') == asset_class)
                              & (pl.col('tier') == 'core'))
        if prefix:
            subset = subset.filter(pl.col('sub_class').str.starts_with(prefix))
        if subset.is_empty():
            out['classes'][asset_class] = {'core': 0}
            continue
        latest = max(subset['coverage_start'].to_list())
        earliest = min(subset['coverage_start'].to_list())
        top = representative(subset)
        out['classes'][asset_class] = {
            'core': subset.height,
            'earliest_start': earliest.isoformat(),
            'latest_start': latest.isoformat(),
            'latest_start_days_to_asof': span(latest, as_of),
            'representative': top,
        }
    all_core = scope.filter(pl.col('tier') == 'core')
    latest_all = max(all_core['coverage_start'].to_list()) if not all_core.is_empty() else None
    out['all_core_latest_start'] = latest_all.isoformat() if latest_all else None
    out['all_core_days_to_asof'] = span(latest_all, as_of) if latest_all else 0
    reps = {asset: out['classes'][asset].get('representative', {}).get('start')
            for asset in ('equity', 'bond', 'gold', 'commodity')}
    rep_dates = [date.fromisoformat(v) for v in reps.values() if v]
    if rep_dates:
        rep_start = max(rep_dates)
        out['representative_start'] = rep_start.isoformat()
        out['representative_days_to_asof'] = span(rep_start, as_of)
        out['representative_codes'] = {
            asset: out['classes'][asset].get('representative', {}).get('code')
            for asset in ('equity', 'bond', 'gold', 'commodity')
        }
    return out


def _write_csv(rows: list[dict[str, Any]], path: Path, columns: list[str],
               encoding: str = 'utf-8') -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with open(tmp, 'w', encoding=encoding, newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator='\n')
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, '') for key in columns})
    tmp.replace(path)


def write_staging(frame: pl.DataFrame, gap_detail: list[dict[str, Any]],
                  event_detail: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Path]:
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    STAGING.mkdir(parents=True, exist_ok=True)
    pool_path = STAGING / 'etf_pool.parquet'
    write_parquet_atomic(frame, pool_path, compression='zstd')
    pool_csv = STAGING / 'etf_pool.csv'
    _write_csv(frame.to_dicts(), pool_csv, frame.columns, encoding='utf-8-sig')
    gap_path = STAGING / 'etf_gap_detail.csv'
    _write_csv(gap_detail, gap_path, [
        'ts_code', 'asset_class', 'name', 'start', 'end', 'trading_days', 'kind',
        'inventory_internal_missing', 'inventory_missing_zero_volume', 'placeholder_ranges',
    ])
    event_path = STAGING / 'etf_event_gaps.csv'
    _write_csv(event_detail, event_path, [
        'ts_code', 'asset_class', 'name', 'date', 'hfq_ratio',
        'in_dividend_table', 'dividend_ex_date', 'in_share_change_table',
        'share_ex_date', 'in_corporate_actions_table', 'after_ca_table_end',
        'unregistered_anywhere',
    ])
    summary_path = STAGING / 'summary.json'
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return {'pool': pool_path, 'csv': pool_csv, 'gap': gap_path,
            'event': event_path, 'summary': summary_path}


def apply_staging(paths: dict[str, Path], summary: dict[str, Any]) -> dict[str, Any]:
    from src.data.atomic_write import write_parquet_atomic, write_text_atomic

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    frame = pl.read_parquet(paths['pool'])
    write_parquet_atomic(frame, TARGET_DIR / 'etf_pool.parquet')
    write_text_atomic(paths['csv'].read_text(encoding='utf-8-sig'),
                      TARGET_DIR / 'etf_pool.csv', encoding='utf-8-sig')
    for key, name in (('gap', 'etf_gap_detail.csv'), ('event', 'etf_event_gaps.csv')):
        write_text_atomic(paths[key].read_text(encoding='utf-8'), TARGET_DIR / name)
    # 2026-11-12 起分档汇总是一次性口径证据，落 docs/evidence 随 Git 留存，
    # 不进正式层、不列进 manifest 的 files（DG-32 入层三问第①问不成立）
    evidence = ROOT / 'docs' / 'evidence' / 'summary'
    evidence.mkdir(parents=True, exist_ok=True)
    write_text_atomic(
        json.dumps(summary, ensure_ascii=False, indent=2), evidence / 'summary.json')

    files = ['etf_pool.parquet', 'etf_pool.csv', 'etf_gap_detail.csv',
             'etf_event_gaps.csv']
    manifest: dict[str, Any] = {
        'path': 'data/local_store/meta/multi_asset/',
        'semantics': '多资产（境内权益/利率债/黄金/商品）ETF 候选池与数据口径',
        'as_of': summary['as_of'],
        'produced_by': 'tools/build_multi_asset_etf_pool.py',
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'rows': frame.height,
        'tier_counts': summary['tier_counts'],
        'columns': frame.columns,
        'files': {},
        'sources': [
            'meta/fund_basic.parquet',
            'meta/daily_index.parquet',
            'quotes/fund/**',
            'factors/fund_adj_factor.parquet',
            'factors/fund_dividend.parquet',
            'factors/fund_share_change.parquet',
            'meta/fund_corporate_actions_2019plus.parquet',
            'meta/anomalies/fund_fake_bar_bars.csv',
            'meta/universe/internal_gap_inventory.csv',
            'meta/anomalies/placeholder_quote_codes.csv',
            'src/data/trading_calendar.py',
        ],
    }
    for name in files:
        target = TARGET_DIR / name
        manifest['files'][name] = {
            'rows': pl.read_parquet(target).height if name.endswith('.parquet') else
                    sum(1 for _ in open(target, encoding='utf-8-sig')) - 1,
            'sha256': _sha256(target),
            'bytes': target.stat().st_size,
        }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    apply_semantics(manifest, 'meta/multi_asset/manifest.json')
    write_text_atomic(
        json.dumps(manifest, ensure_ascii=False, indent=2), TARGET_DIR / 'manifest.json')
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建多资产 ETF 候选池')
    parser.add_argument('--as-of', default='')
    parser.add_argument('--apply', action='store_true', help='写正式目录；缺省只写 staging')
    args = parser.parse_args(argv)

    as_of = (date.fromisoformat(args.as_of) if args.as_of else
             pl.read_parquet(STORE / 'quotes' / 'fund' / 'sh' / '510300.SH.parquet',
                             columns=['date'])['date'].max())
    frame, gap_detail, event_detail, summary = build(as_of)
    paths = write_staging(frame, gap_detail, event_detail, summary)
    print(f"as_of={as_of} rows={frame.height} 分层={summary['tier_counts']}")
    print(f"代表组合起点={summary.get('representative_start')} "
          f"全部 core 起点={summary['all_core_latest_start']}")
    print(f"staging: {STAGING}")
    for asset, info in summary['classes'].items():
        rep = info.get('representative', {})
        print(f"  {asset}: core={info.get('core')} 起点={info.get('earliest_start')}"
              f"~{info.get('latest_start')} 代表={rep.get('code')}({rep.get('start')})")
    if not args.apply:
        print('dry-run：未写正式目录（加 --apply 执行）')
        return 0
    manifest = apply_staging(paths, summary)
    logger.info(f'[multi_asset] 已写入 {TARGET_DIR} 行数={manifest["rows"]}')
    print(f"manifest: {TARGET_DIR / 'manifest.json'}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
