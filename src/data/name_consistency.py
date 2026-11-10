"""名称类事实的三方对账（``names.json`` / ``stock_basic`` / ``name_history``）。

一处事实一处权威，本模块只做「对账 + 定位哪一方陈旧」，**不写任何数据文件**：

- ``meta/names.json``：通达信当日码表快照（6 位码 + 市场后缀），覆盖股票 /
  基金 / 指数 / 债券 / REITs，只有当前简称、没有历史；
- ``meta/stock_basic.parquet``：MySQL ``stock_basic`` 的本地导出，含
  ``name`` / ``list_date`` / ``delist_date`` / ``status``；
- ``meta/name_history.parquet``：tushare ``namechange`` 区间表（PIT 历史简称）。

三张差异表分别回答：

1. ``names.json`` vs ``stock_basic.name``：当前简称两处维护有没有打架；
2. ``name_history`` 当前名（``end_date`` 为空那一段）vs ``stock_basic.name``：
   历史表能不能当「当前简称」的信源；
3. ``name_history.min(start_date)`` vs ``stock_basic.list_date``：上市日能不能
   从历史表的首条记录反推。

判定规则：``names.json`` 是当日快照，两方分歧时默认它更「新」；三方都有值时
按「二比一」判定谁陈旧，避免把结论押在单一信源上。
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

NAMES_JSON = 'meta/names.json'
STOCK_BASIC = 'meta/stock_basic.parquet'
NAME_HISTORY = 'meta/name_history.parquet'

#: ``names.json`` 里 ``N``（上市首日）/ ``C``（次新）/ ``XD`` / ``XR`` / ``DR``
#: （除权除息）是行情端临时标记，不是正式简称
_MARKET_PREFIXES = ('XD', 'XR', 'DR', 'N', 'C')


def strip_market_prefix(value: Any) -> str:
    """剥掉行情端临时标记，用于判断「差异是不是只是当日标记」。"""
    text = normalize_name(value).upper()
    for prefix in _MARKET_PREFIXES:
        if text.startswith(prefix) and len(text) > len(prefix):
            return text[len(prefix):]
    return text


TABLES = ('names_json_vs_stock_basic', 'name_history_vs_stock_basic',
          'list_date_vs_history_min_start')

#: ``source`` → 精度档。按日期取简称的读取 API **必须**把它一起返回，
#: 调用方要能一眼看出「公告直证」还是「推断」，不接受裸名称字符串。
PRECISION: dict[str, str] = {
    'tushare_namechange': '公告直证',
    'inferred:strip_G': '最低（推断，非公告直证）',
}
PRECISION_UNMATCHED = '无匹配段'
PRECISION_UNKNOWN = '未登记来源（需核实）'


def precision_of(source: Any) -> str:
    """``source`` → 精度档；未登记的来源按「需核实」处理，不默认当公告直证。"""
    return PRECISION.get(str(source), PRECISION_UNKNOWN)


def is_inferred(source: Any) -> bool:
    """``source`` 是否为推断段（``inferred:*``）。"""
    return str(source).startswith('inferred:')


def normalize_name(value: Any) -> str:
    """全角转半角、去空白后再比：只用于判断「是不是纯格式差异」。"""
    if value is None:
        return ''
    text = unicodedata.normalize('NFKC', str(value))
    return re.sub(r'\s+', '', text)


def st_bucket(value: Any) -> str:
    """名称 → 风险警示档位（``''`` / ``ST`` / ``*ST`` / ``其他``）。

    与 ``tools/build_st_history_from_tushare.py::classify_name`` 同口径：
    先剥离股改标记 ``G``，再按 ``*ST`` / ``ST`` / ``PT`` / 前导 ``*`` 判定。
    """
    text = normalize_name(value).upper()
    if not text:
        return ''
    body = text[1:] if text.startswith('G') else text
    if 'ST' in body:
        return '*ST' if '*ST' in body else 'ST'
    if body.startswith('PT') or text.startswith('*'):
        return '其他'
    return ''


def load_names_json(store_root: Path | str) -> dict[str, str]:
    path = Path(store_root) / NAMES_JSON
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


def load_stock_basic(store_root: Path | str) -> pl.DataFrame:
    path = Path(store_root) / STOCK_BASIC
    if not path.is_file():
        raise FileNotFoundError(f'缺少本地基础表: {path}')
    return pl.read_parquet(
        path, columns=['ts_code', 'name', 'list_date', 'delist_date', 'status'])


def load_name_history(store_root: Path | str) -> pl.DataFrame:
    path = Path(store_root) / NAME_HISTORY
    if not path.is_file():
        raise FileNotFoundError(f'缺少名称历史表: {path}')
    return pl.read_parquet(path)


def _as_date(value: Any) -> date | None:
    """把 ``date`` / ``datetime`` / ``YYYY-MM-DD`` / ``YYYYMMDD`` 归一成 ``date``。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return datetime.strptime(text, '%Y%m%d').date()


def name_at_frame(history: pl.DataFrame, ts_code: str,
                  as_of: Any) -> dict[str, Any]:
    """在一份已加载的 ``name_history`` 上按日期取简称（PIT），**带来源**。

    命中条件为 ``start_date <= as_of`` 且（``end_date`` 为空 或
    ``as_of <= end_date``）——两端都是闭区间，与
    :func:`src.data.st_history.status_at` 同口径，不是 ``<``。同一 ``as_of``
    同时等于前段 ``end_date`` 与后段 ``start_date`` 时取 ``start_date``
    更大的后段。

    返回值固定含 7 个键，**不返回裸名称字符串**：
    ``ts_code`` / ``as_of`` / ``name`` / ``source`` / ``precision`` /
    ``start_date`` / ``end_date``。无命中时 ``name`` 与 ``source`` 为
    ``None``、``precision`` 为「无匹配段」。
    ``source='inferred:strip_G'`` 的精度档是「最低（推断，非公告直证）」，
    调用方必须自行降级，不得当公告级证据使用。
    """
    day = _as_date(as_of)
    payload: dict[str, Any] = {
        'ts_code': ts_code, 'as_of': day, 'name': None, 'source': None,
        'precision': PRECISION_UNMATCHED, 'start_date': None, 'end_date': None}
    if day is None:
        return payload
    hits = history.filter(
        (pl.col('ts_code') == ts_code)
        & (pl.col('start_date') <= day)
        & (pl.col('end_date').is_null() | (pl.col('end_date') >= day)))
    if hits.height == 0:
        return payload
    row = hits.sort('start_date').to_dicts()[-1]
    payload.update({
        'name': row.get('name'),
        'source': row.get('source'),
        'precision': precision_of(row.get('source')),
        'start_date': row.get('start_date'),
        'end_date': row.get('end_date'),
    })
    return payload


def name_at(store_root: Path | str, ts_code: str,
            as_of: Any) -> dict[str, Any]:
    """读取 API：按日期取简称，返回值含 ``source`` 与精度档。

    这是全项目「按日期取简称」的唯一入口，语义与 :func:`name_at_frame`
    完全一致（两端闭区间、同日取后段）。**禁止**只取返回值里的 ``name``
    就当正式简称用：先用 ``source`` / ``precision`` 判精度，``inferred:*``
    段只能当占位，不能当公告级证据。
    """
    return name_at_frame(load_name_history(store_root), ts_code, as_of)


def current_name_frame(history: pl.DataFrame) -> pl.DataFrame:
    """每只标的的「当前名」= ``end_date`` 为空的最新一段（没有则取末段）。

    返回值除 ``history_name`` / ``history_start`` / ``history_end`` 外，
    还带 ``history_source``（原始 ``source``）与 ``history_precision``
    （精度档）：G 股改残留这类的末段现在是 ``inferred:strip_G`` 推断名，
    调用方必须看这两列，不能只拿 ``history_name`` 当正式简称。
    """
    ordered = history.sort(['ts_code', 'start_date', 'end_date'],
                           nulls_last=True)
    last = ordered.group_by('ts_code', maintain_order=True).last()
    return last.select(
        'ts_code', pl.col('name').alias('history_name'),
        pl.col('start_date').alias('history_start'),
        pl.col('end_date').alias('history_end'),
        pl.col('source').alias('history_source'),
        pl.col('source').map_elements(precision_of, return_dtype=pl.Utf8)
        .alias('history_precision'))


def _market(ts_code: str) -> str:
    return ts_code.rsplit('.', 1)[-1] if '.' in ts_code else ''


def _verdict_by_majority(name_sb: str, name_nh: str,
                         name_nj: str | None,
                         history_start: Any = None) -> str:
    """三方按「二比一」判定谁陈旧；``names.json`` 缺失时返回无法仲裁。"""
    if name_nj is None:
        return '无法三方仲裁（names.json 无此码）'
    a, b, c = normalize_name(name_sb), normalize_name(name_nh), normalize_name(name_nj)
    if b == c:
        return 'stock_basic 陈旧'
    if a == c:
        # 历史表这一段若刚起步，是「已生效但另两方没跟上」而非陈旧
        start = history_start
        if isinstance(start, datetime):
            start = start.date()
        if isinstance(start, date) and (date.today() - start).days <= 90:
            return 'name_history 已记录新简称（另两方尚未更新）'
        return 'name_history 陈旧（该段 end_date 未收尾）'
    return '三方互不一致，需人工核对'


def _classify_history_gap(name_sb: str, name_nh: str) -> str:
    """把「历史表当前名 ≠ stock_basic.name」按互斥口径归类。"""
    text = str(name_nh or '').strip()
    other = str(name_sb or '').strip()
    if normalize_name(name_sb) == normalize_name(name_nh):
        return '空格/全角'
    if text.upper().startswith('G') and not other.upper().startswith('G'):
        return 'G股改残留'
    # 「(退)」后缀是 stock_basic 的退市标注，PT 是退市前的老口径，两侧都算
    if '退' in text or '退' in other \
            or text.upper().startswith('PT') or other.upper().startswith('PT'):
        return '退市/PT残留'
    if st_bucket(name_sb) != st_bucket(name_nh):
        return 'ST判定不同'
    return '其他'


def names_json_vs_stock_basic(store_root: Path | str) -> pl.DataFrame:
    """表 1：当前简称（``names.json`` vs ``stock_basic.name``，带历史表佐证）。"""
    basic = load_stock_basic(store_root)
    names = load_names_json(store_root)
    history = current_name_frame(load_name_history(store_root))
    merged = basic.join(history, on='ts_code', how='left')
    rows: list[dict[str, Any]] = []
    for item in merged.iter_rows(named=True):
        ts_code = item['ts_code']
        name_sb, name_nj = item['name'], names.get(ts_code)
        if name_nj is None or name_nj == name_sb:
            continue
        stripped_sb, stripped_nj = strip_market_prefix(name_sb), strip_market_prefix(name_nj)
        if stripped_nj == stripped_sb or stripped_sb.startswith(stripped_nj) \
                or stripped_nj.startswith(stripped_sb):
            category = '行情端临时标记'
            verdict = '非事实冲突（names.json 带 N/C/XD/XR/DR 当日标记或截断）'
        elif st_bucket(name_nj) != st_bucket(name_sb):
            category = 'ST判定不同'
            verdict = _verdict_by_majority(
                name_sb, item['history_name'] or '', name_nj,
                item['history_start'])
        else:
            category = '其他'
            if item['history_name'] is None:
                verdict = '需人工核对（无历史表佐证）'
            else:
                verdict = _verdict_by_majority(
                    name_sb, item['history_name'], name_nj, item['history_start'])
        rows.append({
            'ts_code': ts_code,
            'name_stock_basic': name_sb,
            'name_names_json': name_nj,
            'name_history_current': item['history_name'],
            'st_bucket_stock_basic': st_bucket(name_sb),
            'st_bucket_names_json': st_bucket(name_nj),
            'category': category,
            'verdict': verdict,
        })
    return _frame(rows, ('ts_code', 'name_stock_basic', 'name_names_json',
                         'name_history_current', 'st_bucket_stock_basic',
                         'st_bucket_names_json', 'category', 'verdict'))


def name_history_vs_stock_basic(store_root: Path | str) -> pl.DataFrame:
    """表 2：``name_history`` 当前名 vs ``stock_basic.name``（带来源与精度档）。"""
    basic = load_stock_basic(store_root)
    current = current_name_frame(load_name_history(store_root))
    names = load_names_json(store_root)
    merged = basic.join(current, on='ts_code', how='left')
    rows: list[dict[str, Any]] = []
    for item in merged.iter_rows(named=True):
        name_nh = item['history_name']
        if name_nh is None or name_nh == item['name']:
            continue
        category = _classify_history_gap(item['name'], name_nh)
        if category == '空格/全角':
            verdict = '仅格式差异，非事实冲突'
        elif category == 'G股改残留':
            verdict = 'name_history 陈旧（G 标记已取消，tushare 未收尾）'
        elif category == '退市/PT残留':
            verdict = '两方口径不同（stock_basic 带「(退)」后缀）'
        else:
            verdict = _verdict_by_majority(
                item['name'], name_nh, names.get(item['ts_code']),
                item['history_start'])
        if is_inferred(item['history_source']):
            verdict = f'{verdict}；历史段为推断（{item["history_source"]}），不作公告级证据'
        rows.append({
            'ts_code': item['ts_code'],
            'name_stock_basic': item['name'],
            'name_history_current': name_nh,
            'history_source': item['history_source'],
            'history_precision': item['history_precision'],
            'name_names_json': names.get(item['ts_code']),
            'history_start': item['history_start'],
            'history_end': item['history_end'],
            'st_bucket_stock_basic': st_bucket(item['name']),
            'st_bucket_history': st_bucket(name_nh),
            'status': item['status'],
            'category': category,
            'verdict': verdict,
        })
    return _frame(rows, ('ts_code', 'name_stock_basic', 'name_history_current',
                         'history_source', 'history_precision',
                         'name_names_json', 'history_start', 'history_end',
                         'st_bucket_stock_basic', 'st_bucket_history', 'status',
                         'category', 'verdict'))


def list_date_vs_history_min_start(store_root: Path | str) -> pl.DataFrame:
    """表 3：``stock_basic.list_date`` vs ``name_history.min(start_date)``。"""
    basic = load_stock_basic(store_root)
    first = load_name_history(store_root).group_by('ts_code').agg(
        pl.col('start_date').min().alias('history_min_start'))
    merged = basic.join(first, on='ts_code', how='inner')
    rows: list[dict[str, Any]] = []
    for item in merged.iter_rows(named=True):
        list_date, first_start = item['list_date'], item['history_min_start']
        if list_date is None or first_start is None or list_date == first_start:
            continue
        market = _market(item['ts_code'])
        delta = (first_start - list_date).days
        if market == 'BJ':
            category = '北交所系统性偏差'
            verdict = 'list_date 为准；history 首条多为 2021-11-15（转板/更名起点）'
        else:
            category = '个案偏差'
            verdict = 'list_date 为准；history 首条只是 namechange 首条记录日'
        rows.append({
            'ts_code': item['ts_code'],
            'name': item['name'],
            'market': market,
            'list_date': list_date,
            'history_min_start': first_start,
            'delta_days': delta,
            'category': category,
            'verdict': verdict,
        })
    return _frame(rows, ('ts_code', 'name', 'market', 'list_date',
                         'history_min_start', 'delta_days', 'category', 'verdict'))


def _frame(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame({name: [] for name in columns})
    # ``infer_schema_length=None``：默认只看前 100 行，若某列前 100 行恰好全空
    # （如 ``history_end`` 绝大多数为开区间），后面的日期值会撞 Null 类型报错
    return pl.DataFrame(rows, infer_schema_length=None).select(list(columns))


def build_tables(store_root: Path | str) -> dict[str, pl.DataFrame]:
    return {
        'names_json_vs_stock_basic': names_json_vs_stock_basic(store_root),
        'name_history_vs_stock_basic': name_history_vs_stock_basic(store_root),
        'list_date_vs_history_min_start': list_date_vs_history_min_start(store_root),
    }


def write_reports(store_root: Path | str, out_dir: Path | str) -> dict[str, Any]:
    """三张差异表落 CSV（UTF-8 BOM，Excel 直接开）+ ``summary.json``。"""
    root = Path(store_root)
    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    tables = build_tables(root)
    written: dict[str, Any] = {}
    for name, frame in tables.items():
        path = target / f'{name}.csv'
        path.write_bytes(b'\xef\xbb\xbf' + frame.write_csv().encode('utf-8'))
        written[name] = {'path': str(path), 'rows': frame.height}
    basic = load_stock_basic(root)
    names = load_names_json(root)
    history = load_name_history(root)
    basic_codes = set(basic['ts_code'].to_list())
    history_codes = set(history['ts_code'].to_list())
    summary: dict[str, Any] = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'store_root': str(root),
        'sources': {
            'names_json_keys': len(names),
            'stock_basic_rows': basic.height,
            'name_history_rows': history.height,
            'name_history_codes': len(history_codes),
            'name_history_open_rows': int(history['end_date'].is_null().sum()),
            'name_history_open_codes': current_name_frame(history).filter(
                pl.col('history_end').is_null()).height,
        },
        'coverage': {
            'only_in_stock_basic': sorted(basic_codes - history_codes),
            'only_in_name_history': sorted(history_codes - basic_codes),
            'stock_basic_codes_missing_names_json': sorted(
                code for code in basic_codes if code not in names),
        },
        'tables': written,
        'category_counts': _category_counts(tables),
    }
    (target / 'summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    summary['summary_path'] = str(target / 'summary.json')
    return summary


def _category_counts(tables: dict[str, pl.DataFrame]) -> dict[str, dict[str, int]]:
    """每张表按 ``category`` 的互斥计数（用于巡检一眼看差异构成）。"""
    counts: dict[str, dict[str, int]] = {}
    for name, frame in tables.items():
        if frame.height == 0:
            counts[name] = {}
            continue
        grouped = frame.group_by('category').len().sort('category')
        counts[name] = {str(row['category']): int(row['len'])
                        for row in grouped.iter_rows(named=True)}
    return counts
