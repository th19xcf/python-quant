#!/usr/bin/env python3

r"""只用本地已验证数据重建 ``meta/stock_basic.parquet`` 的候选表。

为什么不再以 MySQL 为准
========================
MySQL ``stock_basic`` 是「库现在长什么样」，不是正确性依据：它缺少本地已有
证据的退市股，``920305.BJ`` 缺退市日，且无法自证任一格的正误。本脚本改为
全部从本地可验证资产推导，每列都能单独讲清依据：

====================  ======================================================
列                     依据
====================  ======================================================
``ts_code``           ``quotes/stock/{sh,sz,bj}/*.parquet`` 文件名（含已退市）
                      ∪ tushare 退市清单里无行情但证据完整的退市股
``market``            行情文件所在目录（sh / sz / bj），无行情行按代码后缀推
``list_date``         该标的**第一根 bar**；首根 bar 比官方上市日晚 1 年以上
                      时改用官方上市日（行情残缺），见 phi 规则
``delist_date``       有行情：**最后一根 bar**（语义＝最后交易日）；无行情：
                      tushare 官方摘牌日
``status``            ``lag_td``（末根 bar 之后还有几个交易日）＋退市证据
``name``              ``meta/names.json``（通达信当日码表），剥 ``N``/``C``/
                      ``XD``/``XR``/``DR`` 当日临时标记；缺条目留空
``exchange``          代码后缀推：SH→SSE / SZ→SZSE / BJ→BSE
``board``             代码前缀推：600/601/603/605→沪市主板，688/689→科创板，
                      300/301/302→创业板，000/001/002/003→深市主板，
                      900→沪市B股，200→深市B股，43/82/83/87/88/92→北交所；
                      ``T600018.SH`` 这类 TDX 合并码推不出，留空
``delist_reason``     ``退市``（退市证据）/ ``旧码迁移``（旧码被新码取代）
``list_date_basis``   ``quote_first_bar`` / ``official_list``
``delist_date_basis`` ``quote_last_bar`` / ``official_delist``
``date_precision``    两个日期都来自行情 → ``bar``；任一来自官方清单 → ``official``
====================  ======================================================

判定规则
========
- 有行情文件（5,818 只）：``lag_td=0`` → L；有退市证据 → D（理由 ``退市``）；
  在 ``meta/code_migration.parquet`` 里是旧码 → D（理由 ``旧码迁移``）。
- 无行情文件但证据完整（95 只）：只保留「旧码迁移之外」的退市股，日期取
  tushare 退市清单（``tmp/names_raw/stock_basic_{L,D,P}.parquet``），
  标 ``date_precision=official``，不让下游误当成行情直证。这不是「查不到
  所以删」，而是「无行情、但有外部证据」。
- 旧码迁移的旧码（12 只）剔除：同一实体的新码已有行情，留两行才是重复；
  脚本会先断言每个新码都在候选表内，缺一个就报错中止。
- 长期停牌（``lag_td>=20`` 且无退市证据）不判退市，保持 L 并单独打印。

产出方式
========
默认只写候选表到 ``data/local_store/_staging/stock_basic_<时间戳>/``，**不碰**
``meta/stock_basic.parquet``；目录内含 ``manifest.json``（行数、状态分布、
各列依据、证据来源、新旧 SHA256）。``--promote`` 才执行正式替换：
``copy2`` 备份原文件到 ``_backup/stock_basic_<时间戳>/`` → 写
``meta/stock_basic.manifest.json`` → 同盘 rename 原子替换 → 删除候选目录。

用法::

    python tools/build_stock_basic.py                 # 生成候选 + 三方对拍
    python tools/build_stock_basic.py --no-write      # 只对拍，不落盘
    python tools/build_stock_basic.py --compare-db    # 额外与 MySQL 对拍
    python tools/build_stock_basic.py --promote       # 生成候选并正式替换
"""
# ruff: noqa: T201  # 导出脚本面向终端输出

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from bisect import bisect_right
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402

from src.data.atomic_write import write_parquet_atomic  # noqa: E402
from src.data.local_store.store import resolve_store_root  # noqa: E402
from src.utils.config import get_config  # noqa: E402

#: 导出列（前 5 列与现有文件同名同序，其余为可解释的补充信息）
COLUMNS: tuple[str, ...] = (
    'ts_code', 'name', 'name_basis', 'list_date', 'delist_date', 'status',
    'exchange', 'market', 'board', 'delist_reason',
    'list_date_basis', 'delist_date_basis', 'date_precision',
)

SCHEMA: dict[str, Any] = {
    'ts_code': pl.String,
    'name': pl.String,
    'name_basis': pl.String,
    'list_date': pl.Date,
    'delist_date': pl.Date,
    'status': pl.String,
    'exchange': pl.String,
    'market': pl.String,
    'board': pl.String,
    'delist_reason': pl.String,
    'list_date_basis': pl.String,
    'delist_date_basis': pl.String,
    'date_precision': pl.String,
}

#: 连续无行情达到这么多个交易日仍无退市证据 → 记为「长期停牌」，不判退市
LONG_SUSPEND_TRADING_DAYS = 20

#: 首根 bar 比官方上市日晚超过这么多天，视为行情残缺，改用官方上市日
LIST_DATE_GAP_DAYS = 365

#: 虽有旧码迁移记录、且本地无行情，但仍需保留的旧码：继任码行情起点晚于旧码
#: 上市期，剔除会让该实体在早期年份（如 2005）凭空消失。日期取 tushare 官方
#: 退市清单，``date_precision`` 标 ``official``，``delist_date_basis`` 单列标记。
MIGRATION_OLD_CODES_KEPT: tuple[str, ...] = ('000418.SZ', '600087.SH')

_EXCHANGE = {'sh': 'SSE', 'sz': 'SZSE', 'bj': 'BSE'}

#: 与 ``src/data/name_consistency._MARKET_PREFIXES`` 同口径的行情端临时标记
MARKET_PREFIXES = ('XD', 'XR', 'DR', 'N', 'C')


def clean_name(raw: str | None) -> str | None:
    """剥掉 ``N``/``C``/``XD``/``XR``/``DR`` 当日标记，其余字符原样保留。"""
    if not raw:
        return None
    text = str(raw)
    for prefix in MARKET_PREFIXES:
        if text.startswith(prefix) and len(text) > len(prefix):
            return text[len(prefix):]
    return text


def scan_quotes(store_root: Path) -> pl.DataFrame:
    """扫描 ``quotes/stock`` 全部行情文件，取代码、首末 bar 日期与 bar 数。

    只读 ``date`` 一列，5,818 个文件约 12 秒；``quotes/stock`` 是「该标的有
    行情」的唯一凭据。
    """
    quote_root = Path(store_root) / 'quotes' / 'stock'
    if not quote_root.is_dir():
        raise RuntimeError(f'找不到行情目录: {quote_root}')
    records: list[dict[str, Any]] = []
    for path in sorted(quote_root.rglob('*.parquet')):
        stats = (
            pl.scan_parquet(path)
            .select(
                pl.col('date').min().alias('first_bar'),
                pl.col('date').max().alias('last_bar'),
                pl.len().alias('bars'),
            )
            .collect()
        )
        records.append({'ts_code': path.stem, 'market': path.parent.name,
                        **stats.row(0, named=True)})
    frame = pl.DataFrame(records).sort('ts_code')
    if frame.is_empty():
        raise RuntimeError('quotes/stock 下没有任何行情文件')
    return frame


def load_trade_calendar(store_root: Path) -> pl.Series:
    """交易日历取 ``meta/breadth/breadth_daily.parquet``（本地按行情聚合出的逐日表）。"""
    path = Path(store_root) / 'meta' / 'breadth' / 'breadth_daily.parquet'
    if not path.is_file():
        raise RuntimeError(f'找不到交易日历: {path}')
    return pl.read_parquet(path, columns=['trade_date'])['trade_date'].sort()


def load_names(store_root: Path) -> dict[str, str]:
    path = Path(store_root) / 'meta' / 'names.json'
    if not path.is_file():
        raise RuntimeError(f'找不到简称表: {path}')
    return json.loads(path.read_text(encoding='utf-8'))


def load_name_history(store_root: Path) -> dict[str, tuple[str, str]]:
    """名称史末端简称，用于补 ``names.json`` 缺失（主要是已退市标的）。

    返回 ``{ts_code: (简称, 依据)}``，依据取值：

    - ``name_history``：末段公告直证名（``tushare_namechange``）；
    - ``name_history_inferred``：末段是**覆盖到退市的开区间** ``inferred:strip_G``
      （去 G 前缀的推断名）。此时摘掉的 G 只可能是股改标记，真实简称不可能
      再带 G 前缀，故采用推断名，但依据与公告直证分开标注。

    末段是**已闭合**的 ``inferred:strip_G`` 时退回它之前的公告直证段；
    退市整理期的「XX退」原样保留。
    """
    path = Path(store_root) / 'meta' / 'name_history.parquet'
    if not path.is_file():
        return {}
    frame = pl.read_parquet(
        path, columns=['ts_code', 'name', 'start_date', 'end_date', 'source'])
    direct = (frame.filter(pl.col('source') != 'inferred:strip_G')
              .sort('start_date').unique(subset=['ts_code'], keep='last'))
    resolved = {row['ts_code']: (row['name'], 'name_history')
                for row in direct.iter_rows(named=True)}
    last = frame.sort('start_date').unique(subset=['ts_code'], keep='last')
    for row in last.iter_rows(named=True):
        if (row['source'] == 'inferred:strip_G' and row['end_date'] is None) or \
                row['ts_code'] not in resolved:
            resolved[row['ts_code']] = (row['name'], 'name_history_inferred'
                                        if row['source'] == 'inferred:strip_G'
                                        else 'name_history')
    return {code: value for code, value in resolved.items() if value[0]}


def load_delist_evidence(store_root: Path) -> tuple[set[str], dict[str, str]]:
    """返回 (有退市证据的代码集合, 代码→证据来源)。

    来源都是本地文件：名称史「退」字后缀、当前简称「退」字后缀、
    tushare 退市清单缓存。
    """
    evidence: dict[str, str] = {}
    history = Path(store_root) / 'meta' / 'name_history.parquet'
    if history.is_file():
        rows = (
            pl.read_parquet(history, columns=['ts_code', 'name'])
            .filter(pl.col('name').str.contains('退'))
            ['ts_code'].unique().to_list()
        )
        for code in rows:
            evidence.setdefault(code, 'name_history:退')
    for code, name in load_names(store_root).items():
        if '退' in str(name):
            evidence.setdefault(code, 'names_json:退')
    official = load_official_basic(store_root)
    if official is not None:
        for code in official.filter(pl.col('delist_date').is_not_null())['ts_code'].to_list():
            evidence.setdefault(code, 'tushare:退市清单')
    return set(evidence), evidence


def load_official_basic(store_root: Path) -> pl.DataFrame | None:
    """tushare 上市/退市清单缓存，提供官方 ``list_date`` / ``delist_date``。

    文件在 ``tmp/names_raw/stock_basic_{L,D,P}.parquet``（原始抓取缓存）。
    没有它就无法给「无行情的 95 只退市股」提供日期，调用方需显式处理。
    """
    base = Path(store_root).parents[1] / 'tmp' / 'names_raw'
    frames = []
    for tag in ('L', 'D', 'P'):
        path = base / f'stock_basic_{tag}.parquet'
        if path.is_file():
            frames.append(pl.read_parquet(path, columns=['ts_code', 'list_date', 'delist_date']))
    if not frames:
        return None
    frame = pl.concat(frames, how='vertical_relaxed').unique('ts_code', keep='first')
    return frame.with_columns(
        pl.col('list_date').cast(pl.String).str.strptime(pl.Date, '%Y%m%d', strict=False),
        pl.col('delist_date').cast(pl.String).str.strptime(pl.Date, '%Y%m%d', strict=False),
    )


def load_code_migration(store_root: Path) -> dict[str, str]:
    """旧码 → 新码；用于把「换码」和「真退市」区分开（表不存在则空）。"""
    path = Path(store_root) / 'meta' / 'code_migration.parquet'
    if not path.is_file():
        return {}
    rows = pl.read_parquet(path, columns=['old_ts_code', 'new_ts_code'])
    return dict(zip(rows['old_ts_code'].to_list(), rows['new_ts_code'].to_list(), strict=True))


def _board(ts_code: str) -> tuple[str, str | None, str | None]:
    """由代码后缀/前缀推 (market, exchange, board)；推不出的部分返回 None。"""
    symbol, _, suffix = ts_code.partition('.')
    market = {'SH': 'sh', 'SZ': 'sz', 'BJ': 'bj'}.get(suffix)
    exchange = _EXCHANGE.get(market) if market else None
    board: str | None = None
    if suffix == 'SH':
        if symbol.startswith(('688', '689')):
            board = '科创板'
        elif symbol.startswith(('600', '601', '603', '605')):
            board = '沪市主板'
        elif symbol.startswith('900'):
            board = '沪市B股'
    elif suffix == 'SZ':
        if symbol.startswith(('300', '301', '302')):
            board = '创业板'
        elif symbol.startswith(('000', '001', '002', '003')):
            board = '深市主板'
        elif symbol.startswith('200'):
            board = '深市B股'
    elif suffix == 'BJ':
        if symbol.startswith(('43', '82', '83', '87', '88', '92')):
            board = '北交所'
    return market or '', exchange, board


def _resolve_list_date(first_bar: date, official: date | None) -> tuple[date, str]:
    """首根 bar 为准；只有当它比官方上市日晚 1 年以上时，才改用官方日。"""
    if official is not None and (first_bar - official).days > LIST_DATE_GAP_DAYS:
        return official, 'official_list'
    return first_bar, 'quote_first_bar'


def _fill_names(frame: pl.DataFrame, name_history: dict[str, tuple[str, str]]) -> pl.DataFrame:
    """``names.json`` 缺名的标的用名称史末端简称补，并标注 ``name_basis``。"""
    codes = frame['ts_code'].to_list()
    fallback = pl.Series('name_fallback',
                         [name_history.get(code, (None, None))[0] for code in codes],
                         dtype=pl.String)
    basis = pl.Series('name_basis_fallback',
                      [name_history.get(code, (None, None))[1] for code in codes],
                      dtype=pl.String)
    missing = pl.col('name').is_null()
    return frame.with_columns(
        pl.when(missing).then(fallback).otherwise(pl.col('name')).alias('name'),
        pl.when(~missing).then(pl.lit('names_json'))
        .when(basis.is_not_null()).then(basis)
        .otherwise(None).alias('name_basis'),
    )


def _dedupe_t_codes(
    frame: pl.DataFrame, quotes: pl.DataFrame,
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """T 码去重：仅当继任码行情区间已覆盖 T 码区间时才剔除，否则保留。

    tushare 给「代码被继任者复用」的历史实体单列 ``T<code>``。若继任码行情
    已覆盖该区间的全部时间，两者是同一段历史的两种写法，留两条即重复计数；
    覆盖不到时（继任码行情起点更晚）T 码是唯一承载该段历史的行，必须保留。
    """
    bars = {row['ts_code']: (row['first_bar'], row['last_bar'])
            for row in quotes.iter_rows(named=True)}
    drop: list[str] = []
    decisions: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        code = row['ts_code']
        if not (code.startswith('T') and code[1:7].isdigit() and code[7:8] == '.'):
            continue
        successor = code[1:]
        span = bars.get(successor)
        covered = bool(
            span is not None and row['list_date'] is not None
            and row['delist_date'] is not None
            and span[0] <= row['list_date'] and span[1] >= row['delist_date'])
        decisions.append({
            'ts_code': code,
            'successor': successor,
            't_list_date': str(row['list_date']),
            't_delist_date': str(row['delist_date']),
            'successor_first_bar': str(span[0]) if span else None,
            'successor_last_bar': str(span[1]) if span else None,
            'covered': covered,
            'action': 'drop' if covered else 'keep',
        })
        if covered:
            drop.append(code)
    if drop:
        frame = frame.filter(~pl.col('ts_code').is_in(drop))
    return frame, decisions


def build_candidate(
    quotes: pl.DataFrame, calendar: pl.Series, names: dict[str, str],
    evidence: set[str], migrations: dict[str, str], official: pl.DataFrame,
    name_history: dict[str, tuple[str, str]] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str], list[dict[str, Any]]]:
    """构造候选表；返回 (候选表, 长期停牌清单, 剔除的旧码列表, T 码判断)。"""
    trading_days = calendar.to_list()
    official_list = {
        row['ts_code']: (row['list_date'], row['delist_date'])
        for row in official.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    suspended: list[dict[str, Any]] = []
    for record in quotes.iter_rows(named=True):
        code, last_bar = record['ts_code'], record['last_bar']
        lag_td = len(trading_days) - bisect_right(trading_days, last_bar)
        list_date, list_basis = _resolve_list_date(
            record['first_bar'], official_list.get(code, (None, None))[0])
        reason: str | None = None
        if lag_td == 0:
            status, delist_date, delist_basis = 'L', None, None
        elif code in migrations:
            # 换码优先于退市：这类代码是「被新码取代」，不是清算摘牌
            status, delist_date, delist_basis = 'D', last_bar, 'quote_last_bar'
            reason = '旧码迁移'
        elif code in evidence:
            status, delist_date, delist_basis = 'D', last_bar, 'quote_last_bar'
            reason = '退市'
        else:
            status, delist_date, delist_basis = 'L', None, None
            if lag_td >= LONG_SUSPEND_TRADING_DAYS:
                suspended.append({'ts_code': code, 'last_bar': last_bar,
                                  'lag_td': lag_td})
        market, exchange, board = _board(code)
        rows.append({
            'ts_code': code,
            'name': clean_name(names.get(code)),
            'name_basis': None,
            'list_date': list_date,
            'delist_date': delist_date,
            'status': status,
            'exchange': exchange,
            'market': market,
            'board': board,
            'delist_reason': reason,
            'list_date_basis': list_basis,
            'delist_date_basis': delist_basis,
            'date_precision': 'bar' if list_basis == 'quote_first_bar' else 'official',
        })

    quoted = set(quotes['ts_code'])
    dropped = sorted(c for c in migrations if c not in quoted)
    missing_new = sorted({migrations[c] for c in dropped} - quoted)
    if missing_new:
        raise RuntimeError(f'剔除旧码时发现新码不在行情里，拒绝继续: {missing_new}')

    # 无行情、但 tushare 退市清单证据完整的退市股：保留，日期打上 official 标记
    external = sorted(
        set(official.filter(pl.col('delist_date').is_not_null())['ts_code'])
        - quoted - set(dropped)
    )
    for code in external:
        list_date, delist_date = official_list[code]
        if list_date is None or delist_date is None:
            raise RuntimeError(f'{code} 无行情且 tushare 日期不全，无法给出可信日期')
        market, exchange, board = _board(code)
        rows.append({
            'ts_code': code,
            'name': clean_name(names.get(code)),
            'name_basis': None,
            'list_date': list_date,
            'delist_date': delist_date,
            'status': 'D',
            'exchange': exchange,
            'market': market,
            'board': board,
            'delist_reason': '退市',
            'list_date_basis': 'official_list',
            'delist_date_basis': 'official_delist',
            'date_precision': 'official',
        })

    # 旧码回填：继任码行情起点晚于旧码上市期，剔掉会让该实体在早期年份凭空消失
    for code in MIGRATION_OLD_CODES_KEPT:
        if code not in dropped:
            raise RuntimeError(f'{code} 不在「无行情旧码」清单内，回填逻辑有误')
        list_date, delist_date = official_list.get(code, (None, None))
        if list_date is None or delist_date is None:
            raise RuntimeError(f'{code} 缺官方上市/摘牌日，无法保留旧码行')
        market, exchange, board = _board(code)
        rows.append({
            'ts_code': code,
            'name': None,
            'name_basis': None,
            'list_date': list_date,
            'delist_date': delist_date,
            'status': 'D',
            'exchange': exchange,
            'market': market,
            'board': board,
            'delist_reason': '旧码迁移',
            'list_date_basis': 'official_list',
            'delist_date_basis': 'code_migration_old',
            'date_precision': 'official',
        })

    frame = pl.DataFrame(rows, schema=SCHEMA).sort('ts_code')
    frame, tcode_decisions = _dedupe_t_codes(frame, quotes)
    frame = _fill_names(frame, name_history or {})
    if frame.height != frame['ts_code'].n_unique():
        raise RuntimeError('候选表 ts_code 不唯一，构建逻辑有误')
    flag = pl.DataFrame(suspended, schema={'ts_code': pl.String,
                                           'last_bar': pl.Date,
                                           'lag_td': pl.Int64})
    return frame, flag, dropped, tcode_decisions


def _diff_columns(left: pl.DataFrame, right: pl.DataFrame) -> list[dict[str, Any]]:
    """按 ts_code 对齐后逐列比对（null 与具体值视为不同，两边都空视为相同）。"""
    shared = [c for c in COLUMNS if c in right.columns]
    common = left.select(['ts_code', *shared[1:]]).join(
        right.select(['ts_code', *shared[1:]]), on='ts_code', how='inner', suffix='_ref')
    diffs: list[dict[str, Any]] = []
    for column in shared[1:]:
        mine = common[column].cast(pl.String).fill_null('\x00NULL')
        ref = common[f'{column}_ref'].cast(pl.String).fill_null('\x00NULL')
        differs = mine != ref
        codes = common.filter(differs)['ts_code'].to_list()
        diffs.append({'column': column, 'rows': len(codes), 'codes': codes})
    return diffs


def compare(candidate: pl.DataFrame, other: pl.DataFrame) -> dict[str, Any]:
    left, right = set(candidate['ts_code']), set(other['ts_code'])
    return {
        'rows_candidate': candidate.height,
        'rows_other': other.height,
        'only_candidate': sorted(left - right),
        'only_other': sorted(right - left),
        'column_diffs': _diff_columns(candidate, other) if left & right else [],
    }


def _sample(codes: list[str], limit: int = 12) -> str:
    head = ', '.join(codes[:limit])
    return f'{head}{" …" if len(codes) > limit else ""}' if codes else '-'


def _print_one(title: str, result: dict[str, Any]) -> None:
    print(f'\n--- {title} ---')
    print(f"候选 {result['rows_candidate']} 行 / 对方 {result['rows_other']} 行")
    print(f"仅候选有 {len(result['only_candidate'])}: {_sample(result['only_candidate'])}")
    print(f"仅对方有 {len(result['only_other'])}: {_sample(result['only_other'])}")
    for item in result['column_diffs']:
        print(f"  {item['column']:<16} 差 {item['rows']:>5} 行  {_sample(item['codes'])}")


def _load_db_basic() -> pl.DataFrame | None:
    """仅用于说明差异来源，不用作判定依据；连不上就返回 None。"""
    from sqlalchemy import text  # noqa: PLC0415

    from src.database.db_manager import DatabaseManager  # noqa: PLC0415
    try:
        dbm = DatabaseManager(get_config())
        dbm.connect()
        rows = dbm.get_session().execute(text(
            'SELECT ts_code, name, list_date, delist_date, status FROM stock_basic')).all()
    except Exception as exc:  # noqa: BLE001 - 诊断用途，失败不影响主流程
        print(f'[warn] 读 MySQL 失败（仅影响三方对拍说明）: {type(exc).__name__}: {exc}')
        return None
    return pl.DataFrame(
        {column: [row[index] for row in rows]
         for index, column in enumerate(('ts_code', 'name', 'list_date', 'delist_date', 'status'))},
        schema={'ts_code': pl.String, 'name': pl.String, 'list_date': pl.Date,
                'delist_date': pl.Date, 'status': pl.String},
    ).sort('ts_code')


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _basis_summary(frame: pl.DataFrame) -> dict[str, dict[str, int]]:
    return {
        column: {str(row[column]): row['count']
                 for row in frame[column].value_counts().to_dicts()}
        for column in ('name_basis', 'list_date_basis', 'delist_date_basis', 'date_precision')
    }


def _manifest(candidate: pl.DataFrame, quotes: pl.DataFrame, calendar: pl.Series,
              suspended: pl.DataFrame, dropped: list[str], evidence: dict[str, str],
              source: Path, tcode_decisions: list[dict[str, Any]]) -> dict[str, Any]:
    """正式数据的自述文件：行数、状态分布、各列依据、证据来源、哈希。"""
    return {
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'builder': 'tools/build_stock_basic.py',
        'rows': candidate.height,
        'status_counts': {row['status']: row['count']
                          for row in candidate['status'].value_counts().to_dicts()},
        'delist_reason_counts': {row['delist_reason']: row['count']
                                 for row in candidate.filter(
                                     pl.col('delist_reason').is_not_null()
                                 )['delist_reason'].value_counts().to_dicts()},
        'latest_trade_date': str(calendar.max()),
        'quote_files': quotes.height,
        'external_delisted_no_quote': int(
            (candidate['delist_date_basis'] == 'official_delist').sum()),
        'dropped_migration_old_codes': dropped,
        'restored_migration_old_codes': dict.fromkeys(
            MIGRATION_OLD_CODES_KEPT,
            '旧码，实体被吸收，本地无行情；日期取 tushare 官方退市清单'),
        'tcode_decisions': tcode_decisions,
        'long_suspended': suspended.height,
        'name_missing': int(candidate['name'].is_null().sum()),
        'name_basis_counts': {str(row['name_basis']): row['count']
                              for row in candidate['name_basis'].value_counts().to_dicts()},
        'basis_counts': _basis_summary(candidate),
        'column_basis': {
            'ts_code': 'quotes/stock 行情文件名 ∪ tushare 退市清单（无行情退市股）',
            'name': 'meta/names.json 优先；缺条目用 meta/name_history.parquet 末段简称；仍缺留空',
            'name_basis': 'names_json（在市简称）/ name_history（公告直证）/ '
                          'name_history_inferred（末段开区间去 G 前缀推断名）；留空=两源皆无',
            'list_date': '首根 bar；比官方上市日晚 365 天以上时改用官方日',
            'delist_date': '有行情=末根 bar（最后交易日）；无行情=官方摘牌日；'
                           'code_migration_old=被吸收旧码取官方摘牌日',
            'status': 'lag_td + 退市证据 + code_migration',
            'exchange/market/board': '代码后缀/前缀推导',
            'delist_reason': '退市 / 旧码迁移',
            'T 码': '继任码行情区间覆盖 T 码区间才剔除，否则保留（见 tcode_decisions）',
        },
        'evidence': {
            'name_history_退': sum(1 for v in evidence.values() if v == 'name_history:退'),
            'names_json_退': sum(1 for v in evidence.values() if v == 'names_json:退'),
            'tushare_退市清单': sum(1 for v in evidence.values() if v == 'tushare:退市清单'),
        },
        'official_date_source': 'tmp/names_raw/stock_basic_{L,D,P}.parquet（tushare 缓存）',
        'source_parquet': str(source),
        'source_parquet_sha256': _sha256(source) if source.is_file() else None,
    }


def _promote(staging: Path, target: Path, store_root: Path, manifest: dict[str, Any]) -> None:
    """备份原文件 → 写正式 manifest → 同盘 rename 原子替换 → 删候选目录。"""
    backup_dir = store_root / '_backup' / f'stock_basic_{datetime.now():%Y%m%d_%H%M%S}'
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / target.name
    shutil.copy2(target, backup)
    old_sha = _sha256(backup)
    new_sha = _sha256(staging / 'stock_basic.parquet')
    if old_sha == new_sha:
        raise RuntimeError('候选与原文件字节相同，无需替换')
    manifest = {**manifest, 'promoted_at': datetime.now().isoformat(timespec='seconds'),
                'target_sha256': new_sha, 'previous_sha256': old_sha,
                'previous_backup': str(backup)}
    (store_root / 'meta' / 'stock_basic.manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(staging / 'stock_basic.parquet', target)
    print(f'已原子替换: {target}')
    print(f'  旧 SHA256 {old_sha}')
    print(f'  新 SHA256 {new_sha}')
    print(f'  备份 {backup}')
    shutil.rmtree(staging)
    print(f'  候选目录已删除: {staging}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='从本地行情/简称/名称史重建 stock_basic 候选表')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    parser.add_argument('--no-write', action='store_true', help='只对拍，不写候选文件')
    parser.add_argument('--compare-db', action='store_true', help='额外与 MySQL 对拍（仅作差异说明）')
    parser.add_argument('--promote', action='store_true',
                        help='写候选后立即备份并原子替换正式文件，再删除候选目录')
    args = parser.parse_args(argv)

    cfg = get_config()
    store_root = Path(args.store_root) if args.store_root else resolve_store_root(cfg)
    calendar = load_trade_calendar(store_root)
    quotes = scan_quotes(store_root)
    official = load_official_basic(store_root)
    if official is None:
        raise RuntimeError(
            '缺少 tmp/names_raw/stock_basic_{L,D,P}.parquet：无行情退市股的官方日期只能从这里取')
    evidence, evidence_source = load_delist_evidence(store_root)
    migrations = load_code_migration(store_root)
    names = load_names(store_root)
    name_history = load_name_history(store_root)
    candidate, suspended, dropped, tcode_decisions = build_candidate(
        quotes, calendar, names, evidence, migrations, official, name_history)

    print(f'行情文件 {quotes.height} 个；交易日历截至 {calendar.max()}；'
          f'退市证据 {len(evidence)} 个代码；旧码迁移 {len(migrations)} 条；'
          f'剔除旧码 {len(dropped)} 只')
    print('候选状态分布: ' + str(candidate['status'].value_counts().to_dicts()))
    print('候选退市理由: ' + str(
        candidate.filter(pl.col('delist_reason').is_not_null())['delist_reason']
        .value_counts().to_dicts()))
    print('日期依据: ' + str(_basis_summary(candidate)))
    print(f"名称缺失 {candidate['name'].is_null().sum()} 个；"
          f"名称依据 {_basis_summary(candidate)['name_basis']}；"
          f"board 推不出 {candidate['board'].is_null().sum()} 个 "
          f"{_sample(candidate.filter(pl.col('board').is_null())['ts_code'].to_list())}")

    target = store_root / 'meta' / 'stock_basic.parquet'
    existing = pl.read_parquet(target) if target.is_file() else pl.DataFrame()
    if not existing.is_empty():
        _print_one('候选 vs 现有 meta/stock_basic.parquet', compare(candidate, existing))
    if args.compare_db:
        db = _load_db_basic()
        if db is not None:
            _print_one('候选 vs MySQL stock_basic（仅说明差异来源）', compare(candidate, db))

    if not suspended.is_empty():
        print(f'\n长期停牌（lag_td>={LONG_SUSPEND_TRADING_DAYS} 但无退市证据，判 L）'
              f' {suspended.height} 只:')
        print(suspended.sort('lag_td', descending=True).head(20))

    if tcode_decisions:
        print(f'\nT 码判断 {len(tcode_decisions)} 条:')
        for item in tcode_decisions:
            print(f"  {item['ts_code']} → 继任 {item['successor']}，"
                  f"T 码区间 {item['t_list_date']}~{item['t_delist_date']}，"
                  f"继任行情 {item['successor_first_bar']}~{item['successor_last_bar']}，"
                  f"覆盖={item['covered']} → {item['action']}")

    if args.no_write:
        print('\n[--no-write] 未落盘')
        return 0

    staging = store_root / '_staging' / f'stock_basic_{datetime.now():%Y%m%d_%H%M%S}'
    staging.mkdir(parents=True, exist_ok=False)
    write_parquet_atomic(candidate, staging / 'stock_basic.parquet')
    manifest = _manifest(candidate, quotes, calendar, suspended, dropped,
                         evidence_source, target, tcode_decisions)
    manifest['sha256'] = _sha256(staging / 'stock_basic.parquet')
    (staging / 'manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'\n候选表已写入: {staging / "stock_basic.parquet"}')

    if args.promote:
        _promote(staging, target, store_root, manifest)
    return 0


if __name__ == '__main__':
    sys.exit(main())
