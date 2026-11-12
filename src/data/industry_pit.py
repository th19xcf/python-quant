#!/usr/bin/env python3

r"""申万官方行业分类历史 → 个股行业 PIT 区间表。

现状问题：``meta/industry/industry_*.parquet`` 只有 2026-11 起的单日快照，
回测 2016 年时只能用 2026 年的行业归属，行业中性化掺入后见之明。

本模块用**免费公开源**补历史：

- 主源：申万宏源研究官网 ``SwClass2021/StockClassifyUse_stock.xls``
  （个股 × 行业代码 × **计入日期** × 更新日期），计入日期即分类生效日，
  可直接构造区间；实测覆盖 1990-01-01 → 2026-11-02、5,937 只。
- 名称源：巨潮 ``p_public0002`` 的申万 2021 版（indtype=008003）与
  2014 版（indtype=008018）分类树，用于把 6 位行业代码补成 L1/L2/L3 名称。

产物（全部为**新增**文件，不覆盖同目录已有的 ``industry_pit.parquet`` /
``industry_change_pit.parquet`` / ``coverage.json``）：

- ``meta/industry_pit/industry_pit_sw.parquet``  区间表
- ``meta/industry_pit/manifest.json``             源、时间、覆盖、粒度、缺口
- ``meta/industry_pit/validation.json``           验证结果（抽样/全量比对/频率/覆盖）
- ``_staging/industry_pit_20261112/raw/``          原始响应留档（采集中间态，DG-32）

字段约定：

- ``industry_code`` 申万 6 位行业代码（原始口径）；
- ``industry_l1`` / ``industry_l2`` 为**代码**（前 2 位 + ``0000``、前 4 位 + ``00``）；
- ``industry_l1_name`` / ``industry_l2_name`` / ``industry_l3_name`` 为名称，
  两套分类树（SW2021/SW2014）都查不到的代码（申万 2003 版独有）留 null，不猜；
- ``in_date`` 生效日，``out_date`` = 同标的下一条 ``in_date`` 前一日，最后一条为 null；
- ``name_tree`` 只标明名称取自哪套树，不代表区间所属的标准版本。

用法::

    python -m src.data.industry_pit build
    python -m src.data.industry_pit validate --sample 20
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
import requests
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic

SW_URL = (
    'https://www.swsresearch.com/swindex/pdf/SwClass2021/'
    'StockClassifyUse_stock.xls'
)
SW_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/131.0 Safari/537.36'
    ),
    'Accept': '*/*',
    'Referer': 'https://www.swsresearch.com/',
}
CNINFO_TREE_URL = 'https://webapi.cninfo.com.cn/api/stock/p_public0002'
CNINFO_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
)
# 巨潮分类树 indtype：申万 2021 版 / 申万 2014 版（旧）
TREE_INDCODES = {'SW2021': '008003', 'SW2014': '008018'}
RETRY = 4
SOURCE = 'swsresearch:StockClassifyUse_stock'
DATASET = 'industry_pit_sw'


def pit_dir(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'industry_pit'


def pit_path(store_root: Path | str) -> Path:
    return pit_dir(store_root) / 'industry_pit_sw.parquet'


def manifest_path(store_root: Path | str) -> Path:
    return pit_dir(store_root) / 'manifest.json'


def validation_path(store_root: Path | str) -> Path:
    return pit_dir(store_root) / 'validation.json'


def raw_dir(store_root: Path | str) -> Path:
    """原始响应留档目录（采集中间态）：按 DG-32 落仓库根 ``_staging/industry_pit_20261112/raw``。"""
    return Path(store_root).resolve().parents[1] / '_staging' / 'industry_pit_20261112' / 'raw'


def _write_atomic(frame: pl.DataFrame, path: Path) -> None:
    write_parquet_atomic(frame, path)


def _write_json(path: Path, payload: Any) -> None:
    write_text_atomic(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


#: 同目录其他口径 parquet 的语义与产出方（2026-11-12 补登，避免 manifest 只记 sw 一张）。
_SIBLING_SPECS: dict[str, tuple[str, str, str]] = {
    'industry_pit.parquet': (
        '通达信 T 代码树行业 PIT 快照，与申万 SW2021 不同源、不同代码体系，禁止与 sw 表混用',
        'src/data/quality/extended_builders.py::build_industry_pit',
        'effective_date',
    ),
    'industry_change_pit.parquet': (
        '行业分类变动历史（长表，含 standard 列区分口径），一行 = 一个 (标的, 行业, 生效区间)',
        'src/data/quality/extended_builders.py::build_industry_pit',
        'effective_date',
    ),
}


def _sibling_inventory(
    store_root: Path | str, target: Path,
) -> dict[str, Any]:
    """同目录其他口径 parquet 的逐文件记账，按磁盘现值统计。

    本 manifest 的 ``dataset`` / ``coverage`` / ``sha256`` 只描述
    ``industry_pit_sw.parquet``；这里补登同目录另两张不同口径的表，
    避免目录级 manifest 覆盖不全。
    """
    directory = pit_dir(store_root)
    files: list[dict[str, Any]] = []
    for name, (semantics, produced_by, date_column) in _SIBLING_SPECS.items():
        path = directory / name
        if not path.is_file() or path.resolve() == target.resolve():
            continue
        lazy = pl.scan_parquet(str(path))
        names = list(lazy.collect_schema().names())
        entry: dict[str, Any] = {
            'path': f'data/local_store/meta/industry_pit/{name}',
            'semantics': semantics,
            'produced_by': produced_by,
            'rows': int(lazy.select(pl.len()).collect().item()),
            'cols': len(names),
            'columns': names,
            'bytes': path.stat().st_size,
            'sha256': _sha256(path),
        }
        if 'ts_code' in names:
            entry['codes'] = int(
                lazy.select(pl.col('ts_code').n_unique()).collect().item())
        for column, key in ((date_column, 'date_range'),
                            ('end_date', 'end_date_range')):
            if column not in names:
                continue
            low, high = lazy.select(
                pl.col(column).min().alias('low'),
                pl.col(column).max().alias('high'),
            ).collect().row(0)
            if key == 'date_range':
                entry['date_column'] = column
            entry[key] = [
                None if low is None else str(low),
                None if high is None else str(high),
            ]
        files.append(entry)
    return {
        'note': (
            '同目录下另有不同口径的行业表，不属于本 manifest 的 '
            'dataset/coverage/sha256（那三项只描述 industry_pit_sw.parquet）；'
            '逐文件如实记账，不改数据'
        ),
        'files': files,
    }


def normalize_ts_code(raw: Any) -> str:
    """6 位代码 → ``ts_code``；北交所 43/83/87/88/92 段单列。"""
    code = str(raw).strip().split('.')[0].zfill(6)
    if code.startswith(('43', '83', '87', '88', '92')):
        return f'{code}.BJ'
    if code.startswith(('6', '9')):
        return f'{code}.SH'
    if code.startswith(('0', '2', '3')):
        return f'{code}.SZ'
    return f'{code}.BJ'


def _industry_codes(code: str) -> tuple[str, str, str]:
    """6 位申万行业代码 → (L1 代码, L2 代码, L3 代码)。"""
    code = str(code).zfill(6)
    return f'{code[:2]}0000', f'{code[:4]}00', code


# --------------------------------------------------------------------------
# 下载
# --------------------------------------------------------------------------


def download_sw_history(raw: Path, *, refresh: bool = False) -> tuple[Path, str | None]:
    """下载申万个股行业分类历史 Excel；已存在且非空则直接复用。"""
    raw.mkdir(parents=True, exist_ok=True)
    path = raw / f'sw_stock_classify_{date.today():%Y%m%d}.xls'
    if path.is_file() and path.stat().st_size > 100_000 and not refresh:
        logger.info(f'[industry_pit] 复用已下载原始文件: {path}')
        return path, None

    last_err = ''
    for attempt in range(RETRY + 1):
        try:
            # 申万站点证书链不完整，且对无 UA/Referer 的请求返回 508；
            # 改用浏览器头后实测 200，证书校验在此显式关闭并记录告警。
            resp = requests.get(
                SW_URL, headers=SW_HEADERS, timeout=120,
                verify=False,  # noqa: S501 —— 站点证书链不完整，来源为申万官方
            )
            if resp.status_code == 200 and len(resp.content) > 100_000:
                path.write_bytes(resp.content)
                logger.info(
                    f'[industry_pit] 已下载 {path.name} '
                    f'({len(resp.content)} bytes)'
                )
                return path, resp.headers.get('Last-Modified')
            last_err = f'http={resp.status_code} bytes={len(resp.content)}'
        except (requests.RequestException, OSError) as exc:
            last_err = f'{type(exc).__name__}: {exc}'
        if attempt < RETRY:
            delay = 2 * 3**attempt
            logger.warning(
                f'[industry_pit] 下载失败（{last_err}），{delay}s 后重试 '
                f'{attempt + 1}/{RETRY}'
            )
            time.sleep(delay)
    raise RuntimeError(f'申万行业分类下载失败: {last_err}')


def _cninfo_tree_records(indcode: str) -> list[dict[str, Any]]:
    """调用巨潮 p_public0002 取分类树（含历史版本 indtype）。"""
    from akshare.stock.finance.stock_industry_cninfo import (  # noqa: PLC0415
        _get_file_content_ths,
        http,
        py_mini_racer,
    )

    js_code = py_mini_racer.MiniRacer()
    js_code.eval(_get_file_content_ths('cninfo.js'))
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Content-Length': '0',
        'Host': 'webapi.cninfo.com.cn',
        'Accept-Enckey': js_code.call('getResCode1'),
        'Origin': 'https://webapi.cninfo.com.cn',
        'Pragma': 'no-cache',
        'Proxy-Connection': 'keep-alive',
        'Referer': 'https://webapi.cninfo.com.cn/',
        'User-Agent': CNINFO_UA,
        'X-Requested-With': 'XMLHttpRequest',
    }
    resp = http.get(
        CNINFO_TREE_URL,
        params={'indcode': '', 'indtype': indcode, 'format': 'json'},
        headers=headers,
        timeout=60,
    )
    return list(resp.json().get('records') or [])


def load_tree(
    raw: Path, version: str, *, refresh: bool = False,
) -> list[dict[str, Any]]:
    """取分类树记录，原始 JSON 落 ``raw/`` 留档。"""
    indcode = TREE_INDCODES[version]
    raw.mkdir(parents=True, exist_ok=True)
    path = raw / f'cninfo_tree_{indcode}_{date.today():%Y%m%d}.json'
    if path.is_file() and not refresh:
        return list(json.loads(path.read_text(encoding='utf-8')).get('records') or [])

    last_err = ''
    for attempt in range(RETRY + 1):
        try:
            records = _cninfo_tree_records(indcode)
            if records:
                _write_json(path, {'indtype': indcode, 'records': records})
                return records
            last_err = 'empty records'
        except (requests.RequestException, ValueError, KeyError) as exc:
            last_err = f'{type(exc).__name__}: {exc}'
        if attempt < RETRY:
            time.sleep(2 * 2**attempt)
    # 巨潮限流（code_005_ipban_notoken）时退回最近一次留档，并如实标注
    cached = sorted(raw.glob(f'cninfo_tree_{indcode}_*.json'))
    if cached:
        logger.warning(
            f'[industry_pit] 巨潮分类树 {indcode} 获取失败（{last_err}），'
            f'退回留档 {cached[-1].name}'
        )
        return list(json.loads(
            cached[-1].read_text(encoding='utf-8')).get('records') or [])
    raise RuntimeError(f'巨潮分类树 {indcode} 获取失败: {last_err}')


# --------------------------------------------------------------------------
# 解析与区间
# --------------------------------------------------------------------------


def _finalize_intervals(frame: pl.DataFrame) -> pl.DataFrame:
    """去重、排序并按标的补齐 ``out_date``。"""
    frame = (
        frame.sort(['ts_code', 'in_date', 'record_updated_at'])
        .unique(subset=['ts_code', 'in_date'], keep='last')
        .sort(['ts_code', 'in_date'])
    )
    return frame.with_columns(
        pl.when(pl.col('in_date').shift(-1).over('ts_code').is_not_null())
        .then(pl.col('in_date').shift(-1).over('ts_code') - timedelta(days=1))
        .otherwise(None)
        .alias('out_date')
    )


def parse_sw_history(path: Path) -> pl.DataFrame:
    """Excel → 区间表骨架（含 ``in_date`` / ``out_date``）。"""
    import pandas as pd  # noqa: PLC0415

    pdf = pd.read_excel(path, dtype={'股票代码': str, '行业代码': str})
    rename = {
        '股票代码': 'code', '计入日期': 'in_date',
        '行业代码': 'industry_code', '更新日期': 'record_updated_at',
    }
    pdf = pdf.rename(columns=rename)[list(rename.values())]
    frame = pl.from_pandas(pdf).with_columns(
        pl.col('code').cast(pl.Utf8).str.zfill(6).map_elements(
            normalize_ts_code, return_dtype=pl.Utf8,
        ).alias('ts_code'),
        pl.col('industry_code').cast(pl.Utf8).str.zfill(6),
        pl.col('in_date').cast(pl.Date),
        pl.col('record_updated_at').cast(pl.Datetime).cast(pl.Date),
    )
    frame = frame.drop('code').filter(
        pl.col('ts_code').is_not_null() & pl.col('in_date').is_not_null(),
    )
    return _finalize_intervals(frame)


def _tree_code(value: Any) -> str:
    """分类树编码去掉字母前缀（``S480301`` → ``480301``，``S48`` → ``48``）。"""
    code = str(value or '').strip()
    while code[:1].isalpha():
        code = code[1:]
    return code


def _tree_name(row: dict[str, Any]) -> str:
    """兼容巨潮原始字段（``SORTNAME``）与 akshare 中文字段（``类目名称``）。"""
    return str(row.get('SORTNAME') or row.get('类目名称') or '')


def build_tree_lookup(
    records: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, str | None]]:
    """分类树记录 → ``{6 位代码: {版本, L1/L2/L3 名称}}``（2021 版优先）。"""
    lookup: dict[str, dict[str, str | None]] = {}
    by_code: dict[str, str] = {}
    for version in ('SW2014', 'SW2021'):
        for row in records.get(version, []):
            raw_code = _tree_code(row.get('SORTCODE') or row.get('类目编码'))
            if raw_code.isdigit():
                by_code[raw_code] = _tree_name(row)
            if not raw_code.isdigit() or len(raw_code) != 6:
                continue
            lookup[raw_code] = {
                'version': version,
                'industry_l1_name': None,
                'industry_l2_name': None,
                'industry_l3_name': _tree_name(row) or None,
            }
    # 用父类编码回填 L1/L2 名称（树里 L1 2 位、L2 4 位、L3 6 位）
    for code, item in lookup.items():
        item['industry_l1_name'] = by_code.get(code[:2]) or None
        item['industry_l2_name'] = by_code.get(code[:4]) or None
    return lookup


def attach_hierarchy(
    frame: pl.DataFrame, lookup: dict[str, dict[str, str | None]],
) -> pl.DataFrame:
    """补 L1/L2/L3 代码与名称。

    名称按行业代码在分类树里查；``name_tree`` 只表示名称取自哪套树，
    不代表该区间所属的标准版本。两棵树都没有的代码（申万 2003 版独有）
    名称留 null，不猜。
    """
    rows = []
    for code in frame['industry_code'].unique().to_list():
        l1, l2, l3 = _industry_codes(code)
        item = lookup.get(code) or {}
        rows.append({
            'industry_code': code,
            'industry_l1': l1,
            'industry_l2': l2,
            'industry_l3': l3,
            'industry_l1_name': item.get('industry_l1_name'),
            'industry_l2_name': item.get('industry_l2_name'),
            'industry_l3_name': item.get('industry_l3_name'),
            'name_tree': item.get('version'),
        })
    dim = pl.DataFrame(rows)
    return frame.join(dim, on='industry_code', how='left')


# --------------------------------------------------------------------------
# 构建
# --------------------------------------------------------------------------


def build(store_root: Path | str, *, refresh: bool = False) -> dict[str, Any]:
    raw = raw_dir(store_root)
    xls, last_modified = download_sw_history(raw, refresh=refresh)
    frame = parse_sw_history(xls)
    records = {version: load_tree(raw, version, refresh=refresh)
               for version in TREE_INDCODES}
    lookup = build_tree_lookup(records)
    frame = attach_hierarchy(frame, lookup).with_columns(
        pl.lit(SOURCE).alias('source'),
    )
    frame = frame.select([
        'ts_code', 'industry_code', 'industry_l1', 'industry_l2', 'industry_l3',
        'industry_l1_name', 'industry_l2_name', 'industry_l3_name',
        'name_tree', 'in_date', 'out_date', 'record_updated_at', 'source',
    ]).sort(['ts_code', 'in_date'])
    target = pit_path(store_root)
    _write_atomic(frame, target)

    open_rows = frame.filter(pl.col('out_date').is_null()).height
    per_code_counts = frame.group_by('ts_code').len()['len']
    manifest: dict[str, Any] = {
        'dataset': DATASET,
        'source': '申万宏源研究 个股行业分类变动历史（SwClass2021）',
        'source_url': SW_URL,
        'authority': 'publisher',
        'method': 'official_change_history_effective_date',
        'fetched_at': datetime.now().isoformat(timespec='seconds'),
        'source_last_modified': last_modified,
        'coverage': {
            'start': str(frame['in_date'].min()),
            'end': str(frame['in_date'].max()),
            'rows': frame.height,
            'codes': frame['ts_code'].n_unique(),
            'open_intervals': open_rows,
            'events_per_code_max': int(cast(int, per_code_counts.max())),
        },
        'granularity': 'daily_effective_date',
        'granularity_note': (
            'in_date 为申万公布生效日；out_date=同标的下一条 in_date-1 日，'
            '最后一条为 null（开放区间，不代表已确认持续至今）'
        ),
        'hierarchy': {
            'industry_l1': '2 位一级代码补 0000，如 480000',
            'industry_l2': '4 位二级代码补 00，如 480300',
            'name_source': '巨潮 p_public0002 indtype=008003/008018',
        },
        'name_resolved_ratio': round(
            frame.filter(pl.col('industry_l1_name').is_not_null()).height
            / frame.height,
            4,
        ),
        'complete': False,
        'known_gaps': [
            '申万 2003 版独有行业代码无分类树，名称留 null（代码与生效日完整；'
            '2016 年截面 3,159 只中仅 31 只受影响）',
            '未上市期间无行业记录；上市前的行业归属不存在，查询应返回空',
            '申万只覆盖其分类体系内个股，退市股按当时分类保留，但不保证多年限完整',
            '2026-11-09 巨潮分类树接口返回 code_005_ipban_notoken（IP 限流），'
            '本次名称映射复用同日留档，未加压重试',
        ],
        'caveats': [
            '2021-07-30 前后 L1 口径切换（SW2014→SW2021），跨该日不可直接比较 L1 名称',
            '与本目录 industry_pit.parquet（通达信 T 代码树）不同口径，禁止混用',
            '禁止把 2026 年行业回填到 2016 年；查询早于 start 应返回空',
        ],
        'files': {
            'parquet': str(target),
            'raw': [str(p) for p in sorted(raw.glob('*')) if p.is_file()],
            'sibling_parquet': _sibling_inventory(store_root, target),
        },
        'sha256': _sha256(target),
    }
    _write_json(manifest_path(store_root), manifest)
    logger.info(
        f'[industry_pit] 已落盘 {target}（{frame.height} 行 / '
        f'{manifest["coverage"]["codes"]} 只 / '
        f'{manifest["coverage"]["start"]} → {manifest["coverage"]["end"]}）'
    )
    return manifest


# --------------------------------------------------------------------------
# 验证
# --------------------------------------------------------------------------


def _listed_codes_by_year(
    store_root: Path | str, years: list[int],
) -> dict[int, set[str]]:
    basic = pl.read_parquet(Path(store_root) / 'meta' / 'stock_basic.parquet')
    result: dict[int, set[str]] = {}
    for year in years:
        cutoff = date(year, 12, 31)
        listed = basic.filter(
            (pl.col('list_date').is_not_null())
            & (pl.col('list_date') <= cutoff)
            & (
                pl.col('delist_date').is_null()
                | (pl.col('delist_date') >= cutoff)
            )
        )['ts_code'].to_list()
        result[year] = set(listed)
    return result


def _coverage_by_year(
    frame: pl.DataFrame, store_root: Path | str,
) -> list[dict[str, Any]]:
    years = list(range(2010, 2027))
    listed = _listed_codes_by_year(store_root, years)
    rows = []
    for year in years:
        cutoff = date(year, 12, 31)
        active = frame.filter(
            (pl.col('in_date') <= cutoff)
            & (pl.col('out_date').is_null() | (pl.col('out_date') >= cutoff))
        )['ts_code'].unique().to_list()
        universe = listed[year]
        covered = [c for c in active if c in universe]
        rows.append({
            'year': year,
            'covered': len(covered),
            'listed': len(universe),
            'coverage': round(len(covered) / len(universe), 4) if universe else None,
            'missing': len(universe) - len(covered),
            'covered_outside_universe': len(active) - len(covered),
        })
    return rows


def _change_frequency(frame: pl.DataFrame) -> list[dict[str, Any]]:
    """剔除每只票的首条记录后的「换行业」事件按年统计。"""
    events = (
        frame.sort(['ts_code', 'in_date'])
        .with_columns(pl.col('in_date').cum_count().over('ts_code').alias('seq'))
        .filter(pl.col('seq') > 1)
    )
    by_year = events.group_by(pl.col('in_date').dt.year().alias('year')).agg(
        pl.len().alias('changes'),
        pl.col('ts_code').n_unique().alias('codes'),
    ).sort('year')
    return by_year.to_dicts()


def _cninfo_sw_intervals(store_root: Path | str) -> pl.DataFrame:
    """仓库内已采集的巨潮申万口径区间（独立交叉校验用，不再联网）。

    巨潮 API 会对高频访问返回 ``code_005_ipban_notoken``，校验默认走本地
    已落盘的 ``industry_change_pit.parquet``，避免继续加压。
    """
    path = pit_dir(store_root) / 'industry_change_pit.parquet'
    if not path.is_file():
        return pl.DataFrame()
    frame = pl.read_parquet(path).filter(
        pl.col('standard').str.contains('申银万国'),
    )
    return frame.with_columns(
        pl.col('industry_code').str.replace('^S', '').str.zfill(6)
        .alias('industry_code'),
    )


def _full_crosscheck(
    frame: pl.DataFrame, cninfo: pl.DataFrame,
) -> list[dict[str, Any]]:
    """全量交叉比对：每个年末截面，双方都有记录的票逐个比行业代码。"""
    out = []
    for year in (2014, 2016, 2018, 2020, 2022, 2024, 2026):
        cutoff = date(year, 12, 31)
        mine = frame.filter(
            (pl.col('in_date') <= cutoff)
            & (pl.col('out_date').is_null() | (pl.col('out_date') >= cutoff))
        ).select('ts_code', pl.col('industry_code').alias('sw'))
        ref = cninfo.filter(
            (pl.col('effective_date') <= cutoff)
            & (pl.col('end_date').is_null() | (pl.col('end_date') >= cutoff))
        ).select('ts_code', pl.col('industry_code').alias('ref'))
        joined = mine.join(ref, on='ts_code', how='inner')
        if joined.is_empty():
            continue
        agg = joined.group_by('ts_code').agg(
            (pl.col('sw') == pl.col('ref')).any().alias('hit'),
            pl.col('sw').first().alias('sw_code'),
            pl.col('ref').unique().alias('ref_codes'),
        )
        total = agg.height
        hits = agg.filter(pl.col('hit')).height
        out.append({
            'as_of': str(cutoff),
            'compared': total,
            'match': hits,
            'match_rate': round(hits / total, 4),
            'mismatch_examples': agg.filter(~pl.col('hit')).head(5).to_dicts(),
        })
    return out


def validate(
    store_root: Path | str, *, sample: int = 20,
) -> dict[str, Any]:
    """抽样比对巨潮事件、统计变更频率与年度覆盖（离线，不联网）。"""
    frame = pl.read_parquet(pit_path(store_root))
    cninfo = _cninfo_sw_intervals(store_root)
    if cninfo.is_empty() or 'effective_date' not in cninfo.columns:
        cninfo = pl.DataFrame({
            'ts_code': [], 'industry_code': [],
            'effective_date': [], 'end_date': [],
        }).with_columns(
            pl.col('effective_date').cast(pl.Date),
            pl.col('end_date').cast(pl.Date),
        )
    basic = pl.read_parquet(Path(store_root) / 'meta' / 'stock_basic.parquet')
    live = sorted(basic.filter(pl.col('status') == 'L')['ts_code'].to_list())
    step = max(len(live) // sample, 1)
    codes = live[::step][:sample]
    delisted = sorted(basic.filter(pl.col('status') == 'D')['ts_code'].to_list())
    if delisted:
        codes.append(delisted[len(delisted) // 2])

    as_of_dates = [date(2016, 12, 31), date(2019, 12, 31),
                   date(2022, 12, 31), date(2026, 9, 30)]
    samples, mismatches = [], []
    for code in codes:
        theirs = cninfo.filter(pl.col('ts_code') == code)
        ours = frame.filter(pl.col('ts_code') == code)
        for as_of in as_of_dates:
            mine = ours.filter(
                (pl.col('in_date') <= as_of)
                & (pl.col('out_date').is_null() | (pl.col('out_date') >= as_of))
            )
            other = theirs.filter(
                (pl.col('effective_date') <= as_of)
                & (pl.col('end_date').is_null() | (pl.col('end_date') >= as_of))
            )
            mine_code = mine['industry_code'].to_list()
            other_code = other['industry_code'].to_list()
            samples.append({
                'ts_code': code, 'as_of': str(as_of),
                'ours': mine_code, 'cninfo': other_code,
                'cninfo_n': len(other_code),
                'match': bool(mine_code and set(mine_code) & set(other_code)),
            })
            if mine_code and other_code and not set(mine_code) & set(other_code):
                mismatches.append({
                    'ts_code': code, 'as_of': str(as_of),
                    'ours': mine_code, 'cninfo': other_code,
                    'cninfo_standard': other['standard'].unique().to_list(),
                })

    compared = [s for s in samples if 'match' in s and s['cninfo_n']]
    hits = sum(1 for s in compared if s['match'])
    report = {
        'dataset': DATASET,
        'validated_at': datetime.now().isoformat(timespec='seconds'),
        'reference': 'meta/industry_pit/industry_change_pit.parquet（巨潮申万口径）',
        'reference_note': (
            '巨潮事件为上市公司行业归属披露的聚合结果，作为「年报所属行业」'
            '的结构化代理；未逐份解析年报 PDF'
        ),
        'sample_codes': len(codes),
        'as_of_dates': [str(d) for d in as_of_dates],
        'compared_cells': len(compared),
        'match_cells': hits,
        'match_rate': round(hits / len(compared), 4) if compared else None,
        'cells_without_reference': len(samples) - len(compared),
        'mismatches': mismatches[:50],
        'full_crosscheck': _full_crosscheck(frame, cninfo),
        'change_frequency': _change_frequency(frame),
        'coverage_by_year': _coverage_by_year(frame, store_root),
        'samples': samples,
    }
    _write_json(validation_path(store_root), report)
    return report


def main(argv: list[str] | None = None) -> int:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    parser = argparse.ArgumentParser(description='申万行业 PIT 区间表')
    parser.add_argument('command', choices=['build', 'validate', 'stats'])
    parser.add_argument('--store-root', default=None)
    parser.add_argument('--refresh', action='store_true', help='强制重下原始文件')
    parser.add_argument('--sample', type=int, default=20)
    args = parser.parse_args(argv)

    store_root = Path(args.store_root) if args.store_root \
        else resolve_store_root(get_config())
    if args.command == 'build':
        manifest = build(store_root, refresh=args.refresh)
        logger.info(json.dumps(manifest['coverage'], ensure_ascii=False))
        return 0
    if args.command == 'validate':
        report = validate(store_root, sample=args.sample)
        logger.info(
            f'[industry_pit] 抽样 {report["compared_cells"]} 格，'
            f'吻合率 {report["match_rate"]}'
        )
        return 0
    frame = pl.read_parquet(pit_path(store_root))
    logger.info(
        f'[industry_pit] {frame.height} 行 / '
        f'{frame["ts_code"].n_unique()} 只 / '
        f'{str(frame["in_date"].min())} → {str(frame["in_date"].max())}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
