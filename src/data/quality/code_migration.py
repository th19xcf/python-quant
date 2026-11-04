#!/usr/bin/env python3

"""证券代码迁移映射：同一只票的旧代码 → 新代码（只读事实表）。

北交所存量代码切换（``43/83/87`` → ``92``）与主板历史代码变更会让同一只票
在两套代码下重复出现，直接 join 会把 ST 区间、行情、股本拆成两只。本模块
从本地四类来源扫描同票多代码，并用可复核证据落一张只读映射表：

- ``stock_basic``（只读 SELECT，不写库）：名称、上市日、退市日
- ``quotes/**`` 与 ``meta/daily_index.parquet``：行情覆盖窗口
- ``meta/names.json``：简称
- ``meta/st/**``：东方财富曾用名登记册、巨潮公告（旧北交所代码的唯一本地痕迹）

判定依据（至少满足两条，逐条写入 ``evidence``）：

1. 名称完全一致：当前简称相同，或旧简称出现在对方的曾用名链；
2. 日期可衔接：上市日一致且一旧一新，或旧代码末次日 ≤ 新代码首个可交易日；
3. 股本/行情不重叠：两套代码行情窗口无交集（旧代码无本地行情时记 ``None``）。

**禁止只凭代码尾号合并。** 有同一法人但属 A/B 两股的（``600221``/``900945``）
按不同证券排除；IPO 受理伪代码（``A23143.SZ``）与新三板 ``.NQ`` 不参与。

产出：

- ``<store>/meta/code_migration.parquet``
- ``<store>/meta/code_migration_unresolved.csv``
- ``<store>/meta/code_migration_manifest.json``

查询入口 :func:`resolve_ts_code` 只读、带环保护；未映射代码原样返回。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

MAPPING_REL = Path('meta') / 'code_migration.parquet'
UNRESOLVED_REL = Path('meta') / 'code_migration_unresolved.csv'
MANIFEST_REL = Path('meta') / 'code_migration_manifest.json'

OLD_BSE_PREFIXES = ('43', '83', '87')
NEW_BSE_PREFIX = '92'
B_SHARE_PREFIXES = ('20', '90')
A_SUFFIXES = ('.SH', '.SZ', '.BJ')

_MAPPING_SCHEMA = {
    'old_ts_code': pl.Utf8,
    'new_ts_code': pl.Utf8,
    'evidence': pl.Utf8,
    'confidence': pl.Utf8,
    'verified_at': pl.Datetime,
    'org_name_exact': pl.Boolean,
    'name_exact': pl.Boolean,
    'name_in_history': pl.Boolean,
    'listing_date_equal': pl.Boolean,
    'dates_ordered': pl.Boolean,
    'quote_non_overlap': pl.Boolean,
    'quote_overlap_days': pl.Int64,
    'old_last_trade': pl.Date,
    'new_first_trade': pl.Date,
    'old_delist_date': pl.Date,
    'new_list_date': pl.Date,
    'source': pl.Utf8,
}
_UNRESOLVED_SCHEMA = {
    'old_ts_code': pl.Utf8,
    'candidate_new_codes': pl.Utf8,
    'reason': pl.Utf8,
    'detail': pl.Utf8,
    'verified_at': pl.Datetime,
}
_STOCK_BASIC_SCHEMA = {
    'ts_code': pl.Utf8, 'name': pl.Utf8, 'list_date': pl.Date,
    'delist_date': pl.Date, 'status': pl.Utf8,
}
_EM_SCHEMA = {
    'secucode': pl.Utf8, 'name': pl.Utf8, 'former_names': pl.Utf8,
    'org_name': pl.Utf8, 'listing_date': pl.Date,
}
_NOTICE_SCHEMA = {
    'sec_code': pl.Utf8, 'sec_name': pl.Utf8, 'publish_date': pl.Date,
}

_MAPPING_CACHE: dict[tuple[str, int], pl.DataFrame] = {}
_CACHE_LOCK = threading.RLock()


# ----------------------------------------------------------------------
# 路径与查询
# ----------------------------------------------------------------------
def mapping_path(store_root: Path | str) -> Path:
    return Path(store_root) / MAPPING_REL


def unresolved_path(store_root: Path | str) -> Path:
    return Path(store_root) / UNRESOLVED_REL


def manifest_path(store_root: Path | str) -> Path:
    return Path(store_root) / MANIFEST_REL


def load_mapping(store_root: Path | str) -> pl.DataFrame:
    """读取映射表（按文件 mtime 缓存）；不存在时返回空表。"""
    path = mapping_path(store_root)
    if not path.is_file():
        return pl.DataFrame(schema=_MAPPING_SCHEMA)
    key = (str(path), path.stat().st_mtime_ns)
    with _CACHE_LOCK:
        cached = _MAPPING_CACHE.get(key)
        if cached is None:
            cached = pl.read_parquet(path)
            _MAPPING_CACHE[key] = cached
        return cached


def resolve_ts_code(code: str, store_root: Path | str | None = None) -> str:
    """把旧代码解析为最新代码；未映射或成环时原样返回。

    只读，不写任何文件。``store_root`` 缺省时使用项目配置的本地存储根目录。
    """
    text = str(code).strip().upper()
    root = Path(store_root) if store_root is not None else _default_root()
    mapping = load_mapping(root)
    if mapping.height == 0:
        return text
    edges = {row['old_ts_code']: row['new_ts_code'] for row in mapping.to_dicts()}
    seen = {text}
    current = text
    while current in edges:
        nxt = edges[current]
        if nxt in seen:
            logger.warning(f'[code-migration] 检测到代码迁移环: {sorted(seen)}')
            return current
        seen.add(nxt)
        current = nxt
    return current


def detect_cycles(pairs: Iterable[tuple[str, str]]) -> list[list[str]]:
    """返回映射图中的环（每个环按首次出现顺序给出节点）。"""
    edges = {str(old): str(new) for old, new in pairs}
    cycles: list[list[str]] = []
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(node: str) -> None:
        state[node] = 1
        stack.append(node)
        nxt = edges.get(node)
        if nxt is not None:
            if state.get(nxt) == 1:
                start = stack.index(nxt)
                cycles.append(stack[start:] + [nxt])
            elif state.get(nxt, 0) == 0:
                visit(nxt)
        stack.pop()
        state[node] = 2

    for node in edges:
        if state.get(node, 0) == 0:
            visit(node)
    return cycles


def validate_mapping(mapping: pl.DataFrame) -> dict[str, Any]:
    """结构校验：一旧多新、环、自环。"""
    pairs = [(row['old_ts_code'], row['new_ts_code']) for row in mapping.to_dicts()]
    two_targets = (mapping.group_by('old_ts_code').len().filter(pl.col('len') > 1)
                   ['old_ts_code'].to_list())
    self_loops = [old for old, new in pairs if old == new]
    cycles = detect_cycles(pairs)
    return {'pairs': len(pairs), 'two_targets': two_targets,
            'self_loops': self_loops, 'cycles': cycles,
            'ok': not two_targets and not cycles and not self_loops}


# ----------------------------------------------------------------------
# 来源加载
# ----------------------------------------------------------------------
@dataclass
class Sources:
    stock_basic: pl.DataFrame
    daily_index: pl.DataFrame
    names: dict[str, str]
    em: pl.DataFrame
    notices: pl.DataFrame


def _default_root() -> Path:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    return Path(resolve_store_root(get_config()))


def _date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], '%Y-%m-%d').date()
    except ValueError:
        return None


def load_stock_basic(store_root: Path | str) -> pl.DataFrame:
    """只读加载 stock_basic：优先数据库，其次本地 parquet 快照。"""
    local = Path(store_root) / 'meta' / 'stock_basic.parquet'
    if local.is_file():
        frame = pl.read_parquet(local)
    else:
        from src.database.db_manager import DatabaseManager  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        db = DatabaseManager(get_config())
        db.connect()
        try:
            frame = pl.read_database(
                'SELECT ts_code, name, list_date, delist_date, status FROM stock_basic',
                connection=db.engine)
        finally:
            db.disconnect()
    frame = frame.select([
        pl.col('ts_code').cast(pl.Utf8),
        pl.col('name').cast(pl.Utf8).fill_null(''),
        pl.col('list_date').cast(pl.Date, strict=False),
        pl.col('delist_date').cast(pl.Date, strict=False),
        pl.col('status').cast(pl.Utf8).fill_null(''),
    ])
    return frame


def load_sources(store_root: Path | str) -> Sources:
    root = Path(store_root)
    stock_basic = load_stock_basic(root)
    daily_index = pl.read_parquet(root / 'meta' / 'daily_index.parquet').filter(
        pl.col('data_type') == 'stock').select([
            pl.col('ts_code').cast(pl.Utf8), pl.col('start'), pl.col('end')])
    names_file = root / 'meta' / 'names.json'
    names: dict[str, str] = json.loads(names_file.read_text(encoding='utf-8')) \
        if names_file.is_file() else {}
    em_file = root / 'meta' / 'st' / 'raw' / 'em_org_basic.parquet'
    em = pl.read_parquet(em_file) if em_file.is_file() else pl.DataFrame(schema=_EM_SCHEMA)
    notice_file = root / 'meta' / 'st' / 'raw' / 'cninfo_notices.parquet'
    notices = pl.read_parquet(notice_file) if notice_file.is_file() \
        else pl.DataFrame(schema=_NOTICE_SCHEMA)
    return Sources(stock_basic=stock_basic, daily_index=daily_index, names=names,
                   em=em, notices=notices)


# ----------------------------------------------------------------------
# 纯函数：候选与证据
# ----------------------------------------------------------------------
def _norm(name: Any) -> str:
    text = str(name or '').upper()
    text = re.sub(r'[\s　]', '', text)
    return text.replace('＊', '*').replace('（', '(').replace('）', ')')


def is_a_share(code: str) -> bool:
    """A 股代码：沪深北、排除 B 股与 IPO 受理伪代码、排除新三板。"""
    text = str(code).upper()
    if not text.endswith(A_SUFFIXES) or text.startswith('A'):
        return False
    return text[:2] not in B_SHARE_PREFIXES


def _chain(row: dict[str, Any]) -> set[str]:
    parts = re.split(r'→', str(row.get('former_names') or ''))
    parts.append(str(row.get('name') or ''))
    return {_norm(part) for part in parts if _norm(part)}


def _notice_rollup(notices: pl.DataFrame) -> dict[str, dict[str, Any]]:
    """旧代码公告汇总：名称集合 + 首末公告日。"""
    if notices.height == 0:
        return {}
    roll: dict[str, dict[str, Any]] = {}
    for row in notices.to_dicts():
        code = str(row['sec_code']).upper()
        entry = roll.setdefault(code, {'names': set(), 'first': None, 'last': None,
                                       'count': 0})
        name = _norm(row.get('sec_name'))
        if name:
            entry['names'].add(name)
        day = _date(row.get('publish_date'))
        if day is not None:
            entry['first'] = day if entry['first'] is None else min(entry['first'], day)
            entry['last'] = day if entry['last'] is None else max(entry['last'], day)
        entry['count'] += 1
    return roll


def _suffix_of_old_bse(code6: str) -> str:
    return f'{code6}.BJ'


def _local_universe(sources: Sources) -> set[str]:
    codes = set(sources.stock_basic['ts_code'].to_list())
    codes |= set(sources.daily_index['ts_code'].to_list())
    codes |= {code for code in sources.names if is_a_share(code)}
    return {code for code in codes if is_a_share(code)}


@dataclass
class _Facts:
    em: dict[str, dict[str, Any]]
    db: dict[str, dict[str, Any]]
    quote: dict[str, tuple[date, date]]
    notice: dict[str, dict[str, Any]]
    active: set[str]


def _facts(sources: Sources) -> _Facts:
    em: dict[str, dict[str, Any]] = {}
    if sources.em.height:
        for row in sources.em.to_dicts():
            code = str(row['secucode']).upper()
            if is_a_share(code):
                em[code] = row
    db = {row['ts_code']: row for row in sources.stock_basic.to_dicts()}
    quote = {row['ts_code']: (row['start'], row['end'])
             for row in sources.daily_index.to_dicts()}
    notice = {_suffix_of_old_bse(code): entry
              for code, entry in _notice_rollup(sources.notices).items()
              if code[:2] in OLD_BSE_PREFIXES + (NEW_BSE_PREFIX,)}
    active = {code for code, row in db.items()
              if row.get('status') == 'L' and not row.get('delist_date')}
    return _Facts(em=em, db=db, quote=quote, notice=notice, active=active)


def _code_span(code: str, facts: _Facts) -> tuple[date | None, date | None]:
    db = facts.db.get(code)
    first = _date(db['list_date']) if db else None
    last = _date(db['delist_date']) if db else None
    quote = facts.quote.get(code)
    if quote is not None:
        first = quote[0] if first is None else min(first, quote[0])
        if last is None:
            last = quote[1]
    note = facts.notice.get(code)
    if note is not None and note.get('last') is not None \
            and (last is None or note['last'] > last):
        last = note['last']
    return first, last


def _order_pair(a: str, b: str, facts: _Facts) -> tuple[str, str] | None:
    """判断旧→新方向；无法判定返回 None。"""
    a_old_seg = a[:2] in OLD_BSE_PREFIXES
    b_old_seg = b[:2] in OLD_BSE_PREFIXES
    if a_old_seg != b_old_seg:
        return (a, b) if a_old_seg else (b, a)
    a_active, b_active = a in facts.active, b in facts.active
    if a_active != b_active:
        return (b, a) if a_active else (a, b)
    a_start, a_end = _code_span(a, facts)
    b_start, b_end = _code_span(b, facts)
    if a_start and b_start and a_start != b_start:
        return (a, b) if a_start < b_start else (b, a)
    if a_end != b_end:
        if a_end is None:
            return (b, a)
        if b_end is None:
            return (a, b)
        return (a, b) if a_end < b_end else (b, a)
    return None


def _pair_record(old: str, new: str, facts: _Facts, *,
                 source: str, extra_names: set[str] | None = None
                 ) -> dict[str, Any] | None:
    old_em, new_em = facts.em.get(old, {}), facts.em.get(new, {})
    old_db, new_db = facts.db.get(old, {}), facts.db.get(new, {})
    old_note = facts.notice.get(old, {})
    old_names = _chain(old_em) if old_em else set()
    old_names |= set((old_note or {}).get('names') or set())
    old_names |= set(extra_names or set())
    new_names = _chain(new_em) if new_em else set()
    shared = (old_names & new_names) - {''}
    name_exact = bool(_norm(old_em.get('name')) and _norm(old_em.get('name'))
                      == _norm(new_em.get('name')))
    org_exact = bool(old_em.get('org_name')) and _norm(old_em.get('org_name')) \
        == _norm(new_em.get('org_name'))
    listing_equal = bool(_date(old_em.get('listing_date'))
                         and _date(old_em.get('listing_date'))
                         == _date(new_em.get('listing_date'))
                         or (_date(old_db.get('list_date'))
                             and _date(old_db.get('list_date'))
                             == _date(new_db.get('list_date'))))
    old_start, old_end = _code_span(old, facts)
    new_start, new_end = _code_span(new, facts)
    old_first = old_start or _date(old_em.get('listing_date'))
    old_delist = _date(old_db.get('delist_date')) if old_db else None
    new_list = _date(new_db.get('list_date')) if new_db else None
    new_first = new_start or _date(new_em.get('listing_date')) or new_list
    dates_ordered = False
    relation = ''
    if old_end is not None and new_first is not None and old_end <= new_first:
        dates_ordered = True
        relation = f'日期衔接(旧末{old_end}→新始{new_first})'
    elif listing_equal and old not in facts.active and new in facts.active:
        dates_ordered = True
        relation = '上市日期一致且一旧一新'
    elif old_first is not None and new_first is not None and old_first < new_first:
        dates_ordered = True
        relation = f'日期先后(旧始{old_first}→新始{new_first})'

    old_q, new_q = facts.quote.get(old), facts.quote.get(new)
    overlap_days: int | None = None
    quote_non_overlap: bool | None = None
    if old_q is not None and new_q is not None:
        overlap = (min(old_q[1], new_q[1]) - max(old_q[0], new_q[0])).days + 1
        overlap_days = max(overlap, 0)
        quote_non_overlap = overlap <= 0

    name_ok = name_exact or bool(shared)
    date_ok = listing_equal or dates_ordered
    quote_ok = quote_non_overlap is True
    criteria = sum([bool(org_exact or name_ok), date_ok, quote_ok])
    if org_exact and name_ok and date_ok:
        confidence = 'high'
    elif criteria >= 2 and (org_exact or name_ok) and date_ok:
        confidence = 'medium'
    else:
        confidence = 'low'

    parts: list[str] = []
    if org_exact:
        parts.append(f'法人名称一致({old_em.get("org_name")})')
    if name_exact:
        parts.append(f'简称一致({new_em.get("name")})')
    elif shared:
        parts.append(f'曾用名交集({"/".join(sorted(shared)[:3])})')
    if relation:
        parts.append(relation)
    if quote_non_overlap is True and old_q is not None and new_q is not None:
        parts.append(f'行情不重叠(旧{old_q[0]}~{old_q[1]},新{new_q[0]}~{new_q[1]})')
    elif quote_non_overlap is False:
        parts.append(f'行情重叠{overlap_days}天(新代码回填历史)')
    elif old_q is None:
        parts.append('旧代码无本地行情')
    parts.append(f'满足依据{criteria}/3')
    evidence = '；'.join(parts) or '证据不足'

    return {
        'old_ts_code': old, 'new_ts_code': new, 'evidence': evidence,
        'confidence': confidence, 'verified_at': datetime.now(),
        'org_name_exact': org_exact, 'name_exact': name_exact,
        'name_in_history': bool(shared), 'listing_date_equal': listing_equal,
        'dates_ordered': dates_ordered, 'quote_non_overlap': quote_non_overlap,
        'quote_overlap_days': overlap_days, 'old_last_trade': old_q[1] if old_q else None,
        'new_first_trade': new_q[0] if new_q else None,
        'old_delist_date': old_delist, 'new_list_date': new_list,
        'source': source,
    }


def _scan_org_groups(sources: Sources, facts: _Facts, universe: set[str]
                     ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    """同一法人多 A 股代码：法人名称是全市场唯一标识，A/B 股与 IPO 伪码已排除。"""
    records: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    used_old: set[str] = set()
    if not sources.em.height:
        return records, unresolved, used_old
    em_a = sources.em.filter(
        pl.col('secucode').map_elements(is_a_share, return_dtype=pl.Boolean))
    groups = em_a.group_by('org_name').agg(pl.col('secucode').alias('codes')) \
        .filter(pl.col('codes').list.len() > 1)
    for group in groups.to_dicts():
        codes = sorted(set(group['codes']))
        if not any(code in universe for code in codes):
            continue
        if len(codes) > 2:
            unresolved.append({
                'old_ts_code': codes[0], 'candidate_new_codes': '/'.join(codes),
                'reason': 'multiple_codes_same_org',
                'detail': f'同一法人下 {len(codes)} 个 A 股代码，无法唯一确定配对',
                'verified_at': datetime.now()})
            continue
        ordered = _order_pair(codes[0], codes[1], facts)
        if ordered is None:
            unresolved.append({
                'old_ts_code': codes[0], 'candidate_new_codes': codes[1],
                'reason': 'direction_undecidable',
                'detail': '名称/日期/活跃状态均无法判定新旧方向',
                'verified_at': datetime.now()})
            continue
        old, new = ordered
        record = _pair_record(old, new, facts, source='org_name_registry')
        if record is None:
            continue
        records.append(record)
        used_old.add(old)
    return records, unresolved, used_old


def _scan_old_bse(facts: _Facts, used_old: set[str]
                  ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """旧北交所代码：只有巨潮公告留有 sec_code+sec_name，按曾用名链精确匹配。"""
    records: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for old in sorted(facts.notice):
        if old in used_old or old[:2] not in OLD_BSE_PREFIXES:
            continue
        names = facts.notice[old].get('names') or set()
        mentions = [name for name in sorted(names) if len(name) >= 2 and name != 'ST']
        candidates = sorted({code for code, row in facts.em.items()
                             if set(mentions) & _chain(row)})
        if not candidates:
            unresolved.append({
                'old_ts_code': old, 'candidate_new_codes': '',
                'reason': 'no_name_match',
                'detail': f'旧代码公告简称 {sorted(names)} 在曾用名登记册中无对应',
                'verified_at': datetime.now()})
            continue
        if len(candidates) > 1:
            unresolved.append({
                'old_ts_code': old, 'candidate_new_codes': '/'.join(candidates),
                'reason': 'ambiguous_name_match',
                'detail': f'旧代码公告简称命中 {len(candidates)} 个新代码',
                'verified_at': datetime.now()})
            continue
        record = _pair_record(old, candidates[0], facts,
                              source='cninfo_old_bse_code',
                              extra_names=set(mentions))
        if record is not None:
            records.append(record)
            used_old.add(old)
    return records, unresolved


def build_mapping(sources: Sources, *,
                  previous: pl.DataFrame | None = None
                  ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """扫描四类来源，生成映射表与未决清单（纯计算，不写盘）。"""
    facts = _facts(sources)
    universe = _local_universe(sources)
    records, unresolved, used_old = _scan_org_groups(sources, facts, universe)
    more_records, more_unresolved = _scan_old_bse(facts, used_old)
    records.extend(more_records)
    unresolved.extend(more_unresolved)

    mapping = pl.DataFrame(records, schema=_MAPPING_SCHEMA) if records \
        else pl.DataFrame(schema=_MAPPING_SCHEMA)
    if mapping.height:
        mapping = mapping.sort(['old_ts_code', 'new_ts_code'])
    # 幂等：同一对代码且证据/置信度未变时保留原 verified_at
    if previous is not None and previous.height and mapping.height:
        old_stamp = {(row['old_ts_code'], row['new_ts_code'],
                      row['evidence'], row['confidence']): row['verified_at']
                     for row in previous.to_dicts()}
        mapping = mapping.with_columns(
            pl.struct(['old_ts_code', 'new_ts_code', 'evidence', 'confidence'])
            .map_elements(lambda s: old_stamp.get(
                (s['old_ts_code'], s['new_ts_code'], s['evidence'], s['confidence'])),
                return_dtype=pl.Datetime).alias('_keep'))
        mapping = mapping.with_columns(
            pl.coalesce([pl.col('_keep'), pl.col('verified_at')]).alias('verified_at')
        ).drop('_keep')
    unresolved_frame = pl.DataFrame(unresolved, schema=_UNRESOLVED_SCHEMA) \
        if unresolved else pl.DataFrame(schema=_UNRESOLVED_SCHEMA)
    return mapping, unresolved_frame


def _write_atomic(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp.parquet')
    frame.write_parquet(tmp)
    os.replace(tmp, path)


def build_and_write(store_root: Path | str) -> dict[str, Any]:
    root = Path(store_root)
    sources = load_sources(root)
    previous = load_mapping(root)
    mapping, unresolved = build_mapping(sources, previous=previous)
    _write_atomic(mapping, mapping_path(root))
    unresolved.write_csv(unresolved_path(root))
    integrity = validate_mapping(mapping)
    summary = {
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'mappings': mapping.height,
        'by_confidence': dict(mapping.group_by('confidence').len().iter_rows()),
        'by_segment': dict(mapping.with_columns(
            pl.col('old_ts_code').str.slice(0, 2).alias('seg'))
            .group_by('seg').len().iter_rows()),
        'unresolved': unresolved.height,
        'integrity': integrity,
        'sources': {'stock_basic': sources.stock_basic.height,
                    'daily_index': sources.daily_index.height,
                    'em_registry': sources.em.height,
                    'cninfo_notices': sources.notices.height},
    }
    manifest_path(root).parent.mkdir(parents=True, exist_ok=True)
    manifest_path(root).write_text(
        json.dumps(summary, ensure_ascii=False, default=str, indent=2),
        encoding='utf-8')
    logger.info(f'[code-migration] 映射 {mapping.height} 条，未决 {unresolved.height} 条，'
                f'完整性 {integrity["ok"]}')
    return summary


def verify(store_root: Path | str, *, sample: int = 20) -> dict[str, Any]:
    """逐条抽样证据 + 完整性检查，结果写 manifest 旁的 verify.json。"""
    root = Path(store_root)
    mapping = load_mapping(root)
    integrity = validate_mapping(mapping)
    source = load_sources(root)
    facts = _facts(source)
    rows = mapping.to_dicts()
    step = max(len(rows) // sample, 1) if rows else 1
    sampled = rows[::step][:sample]
    details: list[dict[str, Any]] = []
    for row in sampled:
        old_em = facts.em.get(row['old_ts_code'], {})
        new_em = facts.em.get(row['new_ts_code'], {})
        old_db = facts.db.get(row['old_ts_code'], {})
        new_db = facts.db.get(row['new_ts_code'], {})
        old_q, new_q = facts.quote.get(row['old_ts_code']), facts.quote.get(
            row['new_ts_code'])
        details.append({
            'old_ts_code': row['old_ts_code'], 'new_ts_code': row['new_ts_code'],
            'name': {'old': old_em.get('name') or old_db.get('name'),
                     'new': new_em.get('name') or new_db.get('name')},
            'dates': {'old_list': str(old_em.get('listing_date') or old_db.get('list_date')),
                      'old_delist': str(old_db.get('delist_date') or ''),
                      'new_list': str(new_em.get('listing_date') or new_db.get('list_date'))},
            'quotes': {'old': f'{old_q[0]}~{old_q[1]}' if old_q else None,
                       'new': f'{new_q[0]}~{new_q[1]}' if new_q else None},
            'confidence': row['confidence'], 'evidence': row['evidence'],
        })
    result = {'mappings': mapping.height,
              'by_confidence': dict(mapping.group_by('confidence').len().iter_rows()),
              'by_segment': dict(mapping.with_columns(
                  pl.col('old_ts_code').str.slice(0, 2).alias('seg'))
                  .group_by('seg').len().iter_rows()),
              'integrity': integrity, 'sampled': details, 'sample_size': len(details)}
    out = root / 'meta' / 'code_migration_verify.json'
    out.write_text(json.dumps(result, ensure_ascii=False, default=str, indent=2),
                   encoding='utf-8')
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description='证券代码迁移映射（只读）')
    parser.add_argument('--store-root', default=None)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('build', help='扫描本地来源并重建映射表')
    verify_cmd = sub.add_parser('verify', help='抽样证据与完整性校验')
    verify_cmd.add_argument('--sample', type=int, default=20)
    args = parser.parse_args()
    root = Path(args.store_root) if args.store_root else _default_root()
    if args.command == 'build':
        summary = build_and_write(root)
        sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2, default=str) + '\n')
    else:
        result = verify(root, sample=args.sample)
        sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
