#!/usr/bin/env python3

"""历史 ST 状态区间表（网络取数 + 本地列存落地）。

与 :mod:`src.data.risk_flags` 的「每日快照」互补：快照只知道当日名单，本模块
从「证券简称变更时间序列」重建**历史区间**，使任意历史日期都能判定某只股票
当时是否被实施风险警示。

数据源（实测结论，2026-11-04）：

- 深交所官方「简称变更」表（经 akshare ``stock_info_sz_change_name``）：
  1994 年至今 7,500+ 条带日期记录，含变更前/后简称，**深市可完整重建**；
- 巨潮资讯公告检索（``cninfo`` hisAnnouncement）：沪市/北交所没有官方简称
  变更表，只能按「风险警示 / 特别处理」公告标题重建，2002 年起有记录，
  2005 年后可用性稳定；
- 上交所/深交所暂停终止上市名单：辅助校验已退市标的的末段状态；
- 2026-11-03 的 ``meta/st_status`` 快照：作为当日真值锚点合并进来。

落盘位置（本地列存，不写数据库）::

    data/local_store/meta/st/st_intervals.parquet   # 区间表（含人工裁定覆盖层）
    data/local_store/meta/st/st_coverage.parquet    # 每标的状态可信窗口
    data/local_store/meta/st/coverage.json          # 全局覆盖说明
    data/local_store/meta/st/pending_review.parquet # 证据不足待复核段
    data/local_store/meta/st/raw/*.parquet          # 原始抓取缓存（增量续跑）
    data/local_store/meta/st/raw/reviewed_conflicts.parquet  # 人工裁定输入
    data/local_store/meta/st/raw/reviewed_pending.parquet    # 裁定：证据不足
    data/local_store/meta/st/fetch_state.json       # 分年抓取完成状态
    logs/st_history/<run>/...                       # 原始响应留档

人工裁定层（``reviewed_conflicts`` / ``reviewed_pending``）是**固定输入**而非抓取
数据：``build_store`` 每次重建都会重新套用，避免重建把裁定结果悄悄回退。

查询语义（与 ``risk_flags.get_st_status`` 对齐，硬红线）：

- 区间命中 → ``st``；
- 区间未命中但 ``as_of`` 落在该标的可信窗口内 → ``non_st``；
- 其余一律 ``unknown``——**绝不默认 non_st**，避免前视偏差。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
import requests
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.utils.dataframe_columns import resolve_date_column

FLAG_ST = 'st'
FLAG_STAR = '*st'
FLAG_DELISTED = 'delisted'
FLAG_ANY = '*'

STATUS_ST = 'st'
STATUS_NON_ST = 'non_st'
STATUS_UNKNOWN = 'unknown'

SOURCE_SZ_OFFICIAL = 'szse_change_name'
SOURCE_CNINFO = 'cninfo_notice'
SOURCE_SH_DELIST = 'sse_delist'
SOURCE_SNAPSHOT = 'snapshot_anchor'
SOURCE_REVIEWED = 'tushare_namechange_reviewed'

#: coverage 的 ``source``：窗口由常规构建得出（未做证据下延）
COV_SOURCE_MERGED = 'merged'
#: coverage 的 ``source``：窗口下界取最早裁定 ST 段起点，行情存在性作佐证
COV_SOURCE_SEGMENT_QUOTE = 'coverage_ext_segment_quote'
#: coverage 的 ``source``：窗口下界取最早裁定 ST 段起点，上市日作佐证
COV_SOURCE_SEGMENT_LISTING = 'coverage_ext_segment_listing'
#: coverage 的 ``source``：窗口下界取最早裁定 ST 段起点，公告/名称史作佐证
COV_SOURCE_SEGMENT_EVIDENCE = 'coverage_ext_segment_evidence'
_EXT_SOURCE = {
    'quote': COV_SOURCE_SEGMENT_QUOTE,
    'listing': COV_SOURCE_SEGMENT_LISTING,
    'evidence': COV_SOURCE_SEGMENT_EVIDENCE,
}

#: 人工裁定输入 ``raw/reviewed_coverage.parquet`` 的列（窗口逐只裁定结论）。
_REVIEWED_COVERAGE_SCHEMA = {
    'ts_code': pl.Utf8,
    'coverage_start': pl.Date,
    'coverage_end': pl.Date,
    'source': pl.Utf8,
    'evidence': pl.Utf8,
    'decided_at': pl.Date,
}

#: 写进 ``coverage.json`` 的重放层说明。
COVERAGE_REPLAY_NOTE = (
    'reviewed_coverage：人工裁定的覆盖窗口逐只结论，最高优先级，套用在快照合并与'
    '证据下延之后；只命中表内列出的标的，不改深市/沪市默认口径。删掉该文件后重跑 '
    'build_store 会退回常规构建口径（深市=上市日/行情首行，沪市/北交所='
    'max(上市日, 2005-01-01) 再按最早 ST 段下延），故不要单独删除它。'
)

#: 写进 ``coverage.json`` 的窗口口径说明；放进 summary 是为了重建时说明不丢。
WINDOW_EXTENDED_NOTE = (
    'coverage_ext_segment_*：窗口下界取「最早裁定 ST 段起点」，不是上市日；'
    '段前仍为 unknown。后缀 quote/listing/evidence 只表示「段起点不早于上市日」'
    '这一佐证来源，不代表起算日等于上市日。行情存在 ≠ 名称非 ST。'
    '理由是读侧对 unknown 区别对待（回测降低 fill_confidence 并记 st_unknown_bars，'
    '建议侧 fail-closed 丢弃），不能凭空判非 ST。'
)

PRECISION_EXACT = 'exact'
PRECISION_ANNOUNCEMENT = 'announcement'
PRECISION_SNAPSHOT = 'snapshot'
PRECISION_INFERRED = 'inferred'

# 巨潮公告检索在 2005 年前明显不完整（2002 年仅 4 条「风险警示」），
# 沪市/北交所只在 2005 年之后按「无公告即非 ST」推断，之前保持 unknown。
CNINFO_SAFE_START = date(2005, 1, 1)

CNINFO_URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
CNINFO_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice',
    'X-Requested-With': 'XMLHttpRequest',
}
CNINFO_KEYWORDS = ('风险警示', '特别处理')
CNINFO_PAGE_SIZE = 30
CNINFO_SLEEP = 0.35

_TAG_RE = re.compile(r'<[^>]+>')
_SPACE_RE = re.compile(r'[\s\u3000]+')
_ST_PREFIX_RE = re.compile(r'^(S?\*ST|SST|PT|ST)')
_DATE_IN_TITLE_RE = re.compile(r'自(\d{4})年(\d{1,2})月(\d{1,2})日起')
_DELIST_RE = re.compile(r'^(退市|.*退)$')

# 公告标题里的噪声：进展/提示/可能/申请/中介意见等都不是生效事件
_TITLE_NOISE = (
    '进展', '措施', '提示', '可能', '拟', '申请', '核查意见', '法律意见',
    '独立财务顾问', '意见', '说明', '回复', '不予', '异常波动', '风险提示',
    '停牌核查', '关注函', '更正', '补充公告', '预告', '撤销申请', '陈述', '存在',
)

_IMPL_ST = re.compile(r'(?:实施|实行)(?:其他风险警示|其他特别处理|特别处理)')
_IMPL_STAR = re.compile(r'(?:实施|实行)(?:退市风险警示|警示存在终止上市风险的特别处理)')
_LIFT_STAR = re.compile(r'(?:撤销|撤消|解除)(?:.{0,8}?)(?:退市风险警示)')
_LIFT_ST = re.compile(r'(?:撤销|撤消|解除)(?:.{0,8}?)(?:其他风险警示|其他特别处理|特别处理)')
_LIFT_ANY = re.compile(r'(?:撤销|撤消|解除)(?:.{0,8}?)(?:风险警示)')

#: 内部构造区间用的核心列（源数据 → 区间段）；落盘时再补证据列。
_INTERVAL_SCHEMA = {
    'ts_code': pl.Utf8, 'start_date': pl.Date, 'end_date': pl.Date,
    'flag': pl.Utf8, 'source': pl.Utf8, 'precision': pl.Utf8,
    'fetched_at': pl.Datetime,
}
#: 落盘列（核心 7 列 + 人工裁定带来的证据列）。重建时按此 schema 输出，
#: 避免新增列被截断。
_INTERVAL_OUTPUT_SCHEMA = {
    **_INTERVAL_SCHEMA, 'evidence': pl.Utf8, 'raw_fetched_at': pl.Datetime,
}
#: 人工裁定输入（``meta/st/raw/reviewed_conflicts.parquet``）的列。
_REVIEWED_SCHEMA = {
    **_INTERVAL_SCHEMA, 'evidence': pl.Utf8, 'raw_fetched_at': pl.Datetime,
    'adjudication': pl.Utf8,
}
_COVERAGE_SCHEMA = {
    'ts_code': pl.Utf8, 'coverage_start': pl.Date, 'coverage_end': pl.Date,
    'source': pl.Utf8, 'fetched_at': pl.Datetime,
}
_RAW_NOTICE_SCHEMA = {
    'notice_id': pl.Utf8, 'sec_code': pl.Utf8, 'sec_name': pl.Utf8,
    'title': pl.Utf8, 'publish_date': pl.Date,
}
_RAW_SZ_SCHEMA = {
    'code': pl.Utf8, 'change_date': pl.Date, 'name_prev': pl.Utf8, 'name_new': pl.Utf8,
}
_ORG_SCHEMA = {'code': pl.Utf8, 'org_id': pl.Utf8}
_CLOSURE_SCHEMA = {'ts_code': pl.Utf8, 'close_date': pl.Date, 'evidence': pl.Utf8}

_INTERVAL_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}
_COVERAGE_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}



# ----------------------------------------------------------------------
# 名称归一与分类
# ----------------------------------------------------------------------
def clean_name(value: Any) -> str:
    """去掉 HTML 标签、全角/半角空白，便于前缀判定。"""
    if value is None:
        return ''
    text = _TAG_RE.sub('', str(value))
    return _SPACE_RE.sub('', text).upper()


def flag_from_name(value: Any) -> str:
    """按证券简称前缀返回 ``st`` / ``*st`` / ``delisted`` / 空串。"""
    name = clean_name(value)
    if not name:
        return ''
    if name.startswith('退市') or name.endswith('退'):
        return FLAG_DELISTED
    match = _ST_PREFIX_RE.match(name)
    if not match:
        return ''
    prefix = match.group(1)
    return FLAG_STAR if '*ST' in prefix else FLAG_ST


def classify_title(title: Any) -> list[tuple[str, str]]:
    """把公告标题解析成 ``[(flag, action)]``；action ∈ {impl, lift}。"""
    text = clean_name(title)
    if not text or any(marker in text for marker in _TITLE_NOISE):
        return []
    actions: list[tuple[str, str]] = []
    if _IMPL_STAR.search(text):
        actions.append((FLAG_STAR, 'impl'))
    if _IMPL_ST.search(text):
        actions.append((FLAG_ST, 'impl'))
    if _LIFT_STAR.search(text):
        actions.append((FLAG_STAR, 'lift'))
    if _LIFT_ST.search(text):
        actions.append((FLAG_ST, 'lift'))
    if not actions and _LIFT_ANY.search(text):
        # 「撤销风险警示」未点明类型：闭合当前打开的任意 ST 区间
        actions.append((FLAG_ANY, 'lift'))
    # 「撤销退市风险警示并实行其他特别处理」这类同日切换：先摘星再戴帽
    actions.sort(key=lambda item: 0 if item[1] == 'lift' else 1)
    return actions


def _event_date(title: str, publish: date) -> date:
    match = _DATE_IN_TITLE_RE.search(clean_name(title))
    if match:
        with_date = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return with_date
    # 公告多在收盘后发布、次一交易日生效；+1 天在交易日序列上等价
    return publish + timedelta(days=1)


def _code_to_ts(code: str) -> str:
    digits = str(code).strip()[-6:]
    if not digits.isdigit():
        return ''
    if digits[0] in ('6', '9'):
        return f'{digits}.SH'
    if digits[0] in ('0', '3'):
        return f'{digits}.SZ'
    if digits[0] in ('4', '8'):
        return f'{digits}.BJ'
    return ''


# ----------------------------------------------------------------------
# 路径
# ----------------------------------------------------------------------
def st_dir(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'st'


def intervals_path(store_root: Path | str) -> Path:
    return st_dir(store_root) / 'st_intervals.parquet'


def coverage_path(store_root: Path | str) -> Path:
    return st_dir(store_root) / 'st_coverage.parquet'


def reviewed_conflicts_path(store_root: Path | str) -> Path:
    """人工裁定输入：冲突段逐段裁定结论（最高优先级覆盖层）。"""
    return raw_dir(store_root) / 'reviewed_conflicts.parquet'


def reviewed_pending_path(store_root: Path | str) -> Path:
    """人工裁定输入：证据不足、待外部证据的段。"""
    return raw_dir(store_root) / 'reviewed_pending.parquet'


def reviewed_coverage_path(store_root: Path | str) -> Path:
    """人工裁定输入：覆盖窗口逐只结论（最高优先级重放层，可重建、幂等）。"""
    return raw_dir(store_root) / 'reviewed_coverage.parquet'


def pending_review_path(store_root: Path | str) -> Path:
    return st_dir(store_root) / 'pending_review.parquet'


def coverage_json_path(store_root: Path | str) -> Path:
    return st_dir(store_root) / 'coverage.json'


def raw_dir(store_root: Path | str) -> Path:
    return st_dir(store_root) / 'raw'


def state_path(store_root: Path | str) -> Path:
    return st_dir(store_root) / 'fetch_state.json'


# ----------------------------------------------------------------------
# 抓取（限速 + 退避 + 原始留档 + 断点续跑）
# ----------------------------------------------------------------------
def _cninfo_payload(keyword: str, year: int, page: int) -> dict[str, Any]:
    return {
        'pageNum': page, 'pageSize': CNINFO_PAGE_SIZE, 'column': 'sse',
        'tabName': 'fulltext', 'plate': '', 'stock': '', 'searchkey': keyword,
        'secid': '', 'category': '', 'trade': '',
        'seDate': f'{year}-01-01~{year}-12-31',
        'sortName': '', 'sortType': '', 'isHLtitle': 'true',
    }


def _post_with_retry(session: requests.Session, payload: dict[str, Any],
                     retries: int = 3) -> dict[str, Any]:
    delay = 2.0
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = session.post(CNINFO_URL, data=payload, headers=CNINFO_HEADERS, timeout=25)
            if resp.status_code == 200:
                return dict(resp.json())
            last_exc = RuntimeError(f'cninfo HTTP {resp.status_code}')
            if resp.status_code not in (403, 429, 500, 502, 503, 504) or attempt >= retries:
                raise last_exc
        except (requests.RequestException, ValueError) as exc:  # 网络/JSON 解析失败
            last_exc = exc
            if attempt >= retries:
                raise
        logger.warning(f'[st] 巨潮请求失败，退避 {delay:.0f}s 重试: {last_exc}')
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f'cninfo 请求失败: {last_exc}')


def fetch_cninfo_chunk(keyword: str, year: int, session: requests.Session) -> dict[str, Any]:
    """抓取「关键词 + 年份」的全部公告页（单次 30 条/页）。"""
    rows: list[dict[str, Any]] = []
    total = 0
    page = 1
    while page <= 400:
        data = _post_with_retry(session, _cninfo_payload(keyword, year, page))
        if page == 1:
            total = int(data.get('totalRecordNum') or 0)
        for item in data.get('announcements') or []:
            stamp = item.get('announcementTime')
            rows.append({
                'notice_id': str(item.get('announcementId') or ''),
                'sec_code': str(item.get('secCode') or '').zfill(6),
                'sec_name': str(item.get('secName') or ''),
                'title': _TAG_RE.sub('', str(item.get('announcementTitle') or '')),
                'publish_date': date.fromtimestamp(float(stamp) / 1000.0) if stamp else None,
            })
        if not data.get('hasMore') or not data.get('announcements'):
            break
        if len(rows) >= total > 0:
            break
        page += 1
        time.sleep(CNINFO_SLEEP)
    complete = len(rows) >= total
    return {'keyword': keyword, 'year': year, 'total': total,
            'fetched': len(rows), 'complete': complete, 'rows': rows}


def _fetch_sz_official() -> list[dict[str, Any]]:
    import akshare as ak  # 重量级可选依赖，延迟导入

    frame = ak.stock_info_sz_change_name(symbol='简称变更')
    rows: list[dict[str, Any]] = []
    for item in frame.itertuples(index=False):
        change_date = item.变更日期
        rows.append({
            'code': _code_to_ts(str(item.证券代码)),
            'change_date': change_date if isinstance(change_date, date)
            else date.fromisoformat(str(change_date)[:10]),
            'name_prev': str(item.变更前简称),
            'name_new': str(item.变更后简称),
        })
    return rows


def _load_frame(path: Path, schema: dict[str, Any]) -> pl.DataFrame:
    if path.is_file():
        return pl.read_parquet(path)
    return pl.DataFrame(schema=schema)


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    """统一走 ``src.data.atomic_write``（临时文件 → fsync → os.replace）。"""
    write_parquet_atomic(frame, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def sync_sources(store_root: Path | str, *, years: Iterable[int] | None = None,
                 force: bool = False, log_root: Path | str = Path('logs/st_history'),
                 with_sz: bool = True) -> dict[str, Any]:
    """抓取并落盘原始数据（增量：已完成的年份跳过，当年始终刷新）。"""
    root = Path(store_root)
    rdir = raw_dir(root)
    rdir.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {}
    if state_path(root).is_file():
        state = json.loads(state_path(root).read_text(encoding='utf-8'))

    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive = Path(log_root) / run_id
    archive.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {'run_id': run_id, 'archive': str(archive), 'chunks': []}

    if with_sz:
        sz_rows = _fetch_sz_official()
        sz_frame = pl.DataFrame(sz_rows, schema=_RAW_SZ_SCHEMA)
        _write_parquet_atomic(sz_frame, rdir / 'sz_change_name.parquet')
        sz_frame.write_csv(archive / 'sz_change_name.csv')
        state['sz'] = {'fetched_at': datetime.now().isoformat(timespec='seconds'),
                       'rows': sz_frame.height}
        report['sz_rows'] = sz_frame.height

    today_year = date.today().year
    target_years = list(years) if years else list(range(2002, today_year + 1))
    notices = _load_frame(rdir / 'cninfo_notices.parquet', _RAW_NOTICE_SCHEMA)
    session = requests.Session()
    cn_state: dict[str, dict[str, Any]] = state.setdefault('cninfo', {})
    for keyword in CNINFO_KEYWORDS:
        bucket = cn_state.setdefault(keyword, {})
        for year in target_years:
            key = str(year)
            cached = bucket.get(key) or {}
            if not force and cached.get('complete') and year != today_year:
                continue
            chunk = fetch_cninfo_chunk(keyword, year, session)
            bucket[key] = {'total': chunk['total'], 'fetched': chunk['fetched'],
                           'complete': chunk['complete'],
                           'fetched_at': datetime.now().isoformat(timespec='seconds')}
            if chunk['rows']:
                new_frame = pl.DataFrame(chunk['rows'], schema=_RAW_NOTICE_SCHEMA)
                notices = pl.concat([notices, new_frame], how='vertical_relaxed')
            (archive / f'cninfo_{keyword}_{year}.json').write_text(
                json.dumps(chunk['rows'], ensure_ascii=False, default=str),
                encoding='utf-8')
            report['chunks'].append({'keyword': keyword, 'year': year,
                                     'total': chunk['total'], 'fetched': chunk['fetched'],
                                     'complete': chunk['complete']})
            logger.info(f'[st] {keyword} {year}: {chunk["fetched"]}/{chunk["total"]}')

    if notices.height:
        notices = notices.unique(subset=['notice_id'], keep='last').sort(['publish_date', 'notice_id'])
    _write_parquet_atomic(notices, rdir / 'cninfo_notices.parquet')
    state['updated_at'] = datetime.now().isoformat(timespec='seconds')
    state_path(root).write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')
    report['notice_rows'] = notices.height
    (archive / 'manifest.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


# ----------------------------------------------------------------------
# 开区间闭合（沪深缺失摘帽公告时用个股全量公告补证）
# ----------------------------------------------------------------------
_CLOSURE_LIFT_RE = re.compile(r'(撤销|撤消).{0,12}(风险警示|特别处理)')
_CLOSURE_RENAME_RE = re.compile(
    r'公司名称.{0,25}(?:变更|更名)|(?:证券|股票)简称.{0,25}(?:变更|更名|实施)')


def _org_id_map(root: Path, archive: Path, session: requests.Session,
                force: bool = False) -> dict[str, str]:
    path = raw_dir(root) / 'org_ids.parquet'
    if path.is_file() and not force:
        return dict(pl.read_parquet(path).rows())
    resp = session.get('http://www.cninfo.com.cn/new/data/szse_stock.json', timeout=25)
    resp.raise_for_status()
    items = resp.json().get('stockList') or []
    rows = [{'code': str(item.get('code') or '').zfill(6), 'org_id': str(item.get('orgId') or '')}
            for item in items if item.get('code') and item.get('orgId')]
    frame = pl.DataFrame(rows, schema=_ORG_SCHEMA).unique(subset=['code'], keep='last')
    _write_parquet_atomic(frame, path)
    frame.write_csv(archive / 'org_ids.csv')
    return dict(frame.rows())


def _fetch_stock_notices(session: requests.Session, code: str, org_id: str,
                         start_day: date, end_day: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1
    while page <= 60:
        payload = {
            'pageNum': page, 'pageSize': CNINFO_PAGE_SIZE, 'column': 'sse',
            'tabName': 'fulltext', 'plate': '',
            'stock': f'{code},{org_id}', 'searchkey': '', 'secid': '', 'category': '',
            'trade': '', 'seDate': f'{start_day.isoformat()}~{end_day.isoformat()}',
            'sortName': '', 'sortType': '', 'isHLtitle': 'true',
        }
        data = _post_with_retry(session, payload)
        anns = data.get('announcements') or []
        total = int(data.get('totalRecordNum') or 0)
        for item in anns:
            stamp = item.get('announcementTime')
            rows.append({'notice_id': str(item.get('announcementId') or ''),
                         'sec_code': code,
                         'sec_name': str(item.get('secName') or ''),
                         'title': _TAG_RE.sub('', str(item.get('announcementTitle') or '')),
                         'publish_date': date.fromtimestamp(float(stamp) / 1000.0)
                         if stamp else None})
        if not anns or not data.get('hasMore') or len(rows) >= total:
            break
        page += 1
        time.sleep(CNINFO_SLEEP)
    return rows


def _closure_candidate(rows: list[dict[str, Any]],
                       since: date) -> tuple[str, dict[str, Any]] | None:
    """优先认「撤销风险警示/特别处理」，没有再退到「更名/简称变更」。"""
    lift: dict[str, Any] | None = None
    rename: dict[str, Any] | None = None
    for row in rows:
        day = row['publish_date']
        if not day or day < since:
            continue
        title = row['title']
        if any(marker in clean_name(title) for marker in _TITLE_NOISE):
            continue
        if _CLOSURE_LIFT_RE.search(title):
            if lift is None or day < lift['publish_date']:
                lift = row
        elif _CLOSURE_RENAME_RE.search(title):
            if rename is None or day < rename['publish_date']:
                rename = row
    if lift is not None:
        return 'lift', lift
    if rename is not None:
        return 'rename', rename
    return None


def resolve_open_closures(store_root: Path | str, *, log_root: Path | str = Path('logs/st_history'),
                          force: bool = False) -> dict[str, Any]:
    """对「仍为开区间但快照显示非 ST」的沪市/北交所标的，翻个股公告找摘帽证据。"""
    root = Path(store_root)
    intervals = load_intervals(root)
    snapshot, snap_day = _latest_snapshot(root)
    if intervals is None or snapshot is None or snap_day is None:
        return {'targets': 0, 'resolved': 0}
    open_rows = intervals.filter(
        pl.col('end_date').is_null()
        & ((pl.col('ts_code').str.ends_with('.SH')) | (pl.col('ts_code').str.ends_with('.BJ')))
    ).sort('start_date').to_dicts()
    if not open_rows:
        return {'targets': 0, 'resolved': 0}
    latest_open = {row['ts_code']: row['start_date'] for row in open_rows}
    non_st_codes = set(snapshot.filter(pl.col('status') == STATUS_NON_ST)['code'].to_list())
    targets = sorted(code for code in latest_open if code in non_st_codes)
    if not targets:
        return {'targets': 0, 'resolved': 0}

    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive = Path(log_root) / f'{run_id}_closure'
    archive.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    org_map = _org_id_map(root, archive, session, force=force)
    existing = _load_frame(raw_dir(root) / 'closure_evidence.parquet', _CLOSURE_SCHEMA)
    resolved = 0
    missing_org = 0
    all_rows: list[dict[str, Any]] = []
    for code in targets:
        org_id = org_map.get(code[:6])
        if not org_id:
            missing_org += 1
            continue
        since = latest_open[code]
        best: tuple[str, dict[str, Any]] | None = None
        window_start = since
        while window_start <= snap_day:
            window_end = min(snap_day, date(window_start.year + 2, 12, 31))
            rows = _fetch_stock_notices(session, code[:6], org_id, window_start, window_end)
            all_rows.extend(rows)
            candidate = _closure_candidate(rows, since)
            if candidate is not None and (best is None
                                          or (best[0] == 'rename' and candidate[0] == 'lift')):
                best = candidate
                if candidate[0] == 'lift':
                    break
            window_start = window_end + timedelta(days=1)
        (archive / f'{code}.json').write_text(
            json.dumps(all_rows, ensure_ascii=False, default=str), encoding='utf-8')
        all_rows = []
        if best is None:
            continue
        best_row = best[1]
        existing = pl.concat([existing, pl.DataFrame([{
            'ts_code': code, 'close_date': best_row['publish_date'],
            'evidence': best_row['title'][:120]}], schema=_CLOSURE_SCHEMA)], how='vertical')
        resolved += 1
        logger.info(f'[st] {code} 开区间以「{best_row["title"][:40]}」于 '
                    f'{best_row["publish_date"]} 闭合')
    if existing.height:
        existing = existing.unique(subset=['ts_code'], keep='last').sort('ts_code')
        _write_parquet_atomic(existing, raw_dir(root) / 'closure_evidence.parquet')
    return {'targets': len(targets), 'resolved': resolved,
            'missing_org': missing_org, 'archive': str(archive)}


# ----------------------------------------------------------------------
# 区间构建
# ----------------------------------------------------------------------
@dataclass
class _Timeline:
    ts_code: str
    listing: date
    segments: list[tuple[date, str]]  # (起始日, flag)
    source: str


def _listing_dates(store_root: Path | str,
                   codes: Iterable[str]) -> dict[str, tuple[date, str]]:
    """每标的上市日与证据来源：``quote``（行情首行）/ ``listing``（上市日）。"""
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    root = Path(store_root)
    store = LocalQuoteStore(root)
    out: dict[str, tuple[date, str]] = {}
    for code in codes:
        cov = store.coverage('stock', code)
        if cov is not None:
            out[code] = (cov.start, 'quote')
    # 已退市标的的行情文件可能已被清理，但名称变更/公告记录仍要能重建区间：
    # 缺行情覆盖时退回 stock_basic 的上市日，避免这些标的在重建时静默消失。
    missing = [code for code in codes if code not in out]
    basic_path = root / 'meta' / 'stock_basic.parquet'
    if missing and basic_path.is_file():
        basic = pl.read_parquet(basic_path, columns=['ts_code', 'list_date'])
        listed = {row['ts_code']: row['list_date'] for row in basic.to_dicts()
                  if row['list_date'] is not None}
        for code in missing:
            day = listed.get(code)
            if day is not None:
                out[code] = (day, 'listing')
    return out


def _changes_from_sz(frame: pl.DataFrame) -> dict[str, tuple[str, list[tuple[date, str]]]]:
    """深市官方简称变更 → ``{code: (上市时简称flag, [(变更日, 新flag)])}``。"""
    out: dict[str, tuple[str, list[tuple[date, str]]]] = {}
    for key, rows in frame.group_by('code'):
        code = str(key[0]) if isinstance(key, tuple) else str(key)
        items = sorted(rows.to_dicts(), key=lambda r: (r['change_date'], r['name_new']))
        # 同一天多条（A/B 股同步改名等）取最后一条
        by_date: dict[date, str] = {}
        for item in items:
            by_date[item['change_date']] = flag_from_name(item['name_new'])
        out[code] = (flag_from_name(items[0]['name_prev']), sorted(by_date.items()))
    return out


def _segments_from_notices(notices: pl.DataFrame, *, include_sz: bool = False
                           ) -> dict[str, list[tuple[date, str, str]]]:
    """巨潮公告 → 每标的 (生效日, flag, action)。

    默认剔除深市（深市有官方简称变更表，精度更高）；交叉校验时需要保留深市，
    以便用公告法的结果检验官方表的召回率。
    """
    out: dict[str, list[tuple[date, str, str]]] = {}
    for row in notices.to_dicts():
        ts_code = _code_to_ts(row['sec_code'])
        if not ts_code or (not include_sz and ts_code.endswith('.SZ')) \
                or row['publish_date'] is None:
            continue
        actions = classify_title(row['title'])
        if not actions:
            continue
        effective = _event_date(row['title'], row['publish_date'])
        out.setdefault(ts_code, []).extend(
            (effective, flag, action) for flag, action in actions)
    for code in out:
        out[code].sort(key=lambda item: (item[0], 0 if item[2] == 'lift' else 1))
    return out


def _build_timeline(code: str, prev_flag: str,
                    changes: list[tuple[date, str]], listing: date,
                    today: date, source: str) -> tuple[_Timeline, date | None]:
    """把「上市时简称 + 变更序列」压成状态段（跳过重复状态与退市）。"""
    state_segments: list[tuple[date, str]] = []
    delist_date: date | None = None
    for start, flag in [(listing, prev_flag), *changes]:
        if flag == FLAG_DELISTED:
            delist_date = start
            # 退市当日闭合此前 ST 区间（退市后不可交易，覆盖窗口到此为止）
            if state_segments and state_segments[-1][1] != '':
                state_segments.append((start, ''))
            break
        start = max(start, listing)
        if state_segments and state_segments[-1][1] == flag:
            continue
        state_segments.append((start, flag))
    return _Timeline(code, listing, state_segments, source), delist_date


def _cninfo_timeline(code: str, events: list[tuple[date, str, str]], listing: date,
                     today: date) -> tuple[list[tuple[date, str]], date | None]:
    start = max(listing, CNINFO_SAFE_START)
    segments: list[tuple[date, str]] = []
    for effective, flag, action in events:
        if effective < start:
            continue
        if action == 'lift':
            # 空 flag 段只用来闭合上一段 ST，不单独成为状态记录
            if segments and segments[-1][1] != '' and (flag == FLAG_ANY
                                                       or segments[-1][1] == flag):
                segments.append((effective, ''))
        elif not segments or segments[-1][1] != flag:
            segments.append((effective, flag))
    return segments, None


def _segments_to_intervals(code: str, segments: list[tuple[date, str]], coverage_end: date,
                           today: date, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, (start, flag) in enumerate(segments):
        if flag not in (FLAG_ST, FLAG_STAR):
            continue
        nxt = segments[index + 1][0] if index + 1 < len(segments) else None
        end = nxt - timedelta(days=1) if nxt else None
        if end is not None and end > coverage_end:
            end = coverage_end
        rows.append({
            'ts_code': code, 'start_date': start, 'end_date': end, 'flag': flag,
            'source': source,
            'precision': PRECISION_EXACT if source == SOURCE_SZ_OFFICIAL
            else PRECISION_ANNOUNCEMENT,
            'fetched_at': datetime.now(),
        })
    return rows


def _latest_snapshot(store_root: Path | str) -> tuple[pl.DataFrame | None, date | None]:
    dir_path = Path(store_root) / 'meta' / 'st_status'
    files = sorted(path for path in dir_path.glob('st_status_*.parquet')) \
        if dir_path.is_dir() else []
    if not files:
        return None, None
    path = files[-1]
    digits = path.stem.split('_')[-1]
    day = datetime.strptime(digits[:8], '%Y%m%d').date()
    return pl.read_parquet(path), day


def _snapshot_series(store_root: Path | str,
                     upto: date | None = None) -> list[tuple[pl.DataFrame, date]]:
    """按日期升序返回 ``meta/st_status`` 下的快照（只取 ``<= upto`` 的日子）。

    逐日合并的硬约束：每一天的区间状态只能由**不晚于该日**的快照决定。只取
    最新一份快照会把后一日才知道的状态回推到前一日（11-10 的 ST 起点曾被推到
    11-09，等于把今天的信息写成昨天的结论，回测里就是未来函数）。
    """
    dir_path = Path(store_root) / 'meta' / 'st_status'
    if not dir_path.is_dir():
        return []
    series: list[tuple[pl.DataFrame, date]] = []
    for path in sorted(dir_path.glob('st_status_*.parquet')):
        digits = path.stem.split('_')[-1]
        day = datetime.strptime(digits[:8], '%Y%m%d').date()
        if upto is not None and day > upto:
            continue
        series.append((pl.read_parquet(path), day))
    series.sort(key=lambda item: item[1])
    return series


def _close_snapshot_rows(rows: list[dict[str, Any]], code: str, snap_day: date,
                         *, only_snapshot: bool
                         ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把覆盖 ``snap_day`` 的区间停在 ``snap_day - 1``，返回 ``(已闭合, 已丢弃)``。

    ``only_snapshot=True`` 时只处理由快照锚定的区间（``unknown`` 不得动官方/
    公告源产出的区间，也不得把区间延展到 unknown 当日）。

    区间起算日恰好等于 ``snap_day`` 时，快照当日状态与区间起点直接矛盾：该区间
    一个有效日期都覆盖不到，按快照为准整行丢弃，不落出 ``start > end`` 的空区间。
    """
    closed: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for row in [item for item in rows if item['ts_code'] == code]:
        if only_snapshot and row['source'] != SOURCE_SNAPSHOT:
            continue
        if row['end_date'] is not None and row['end_date'] < snap_day:
            continue
        if row['start_date'] >= snap_day:
            rows.remove(row)
            dropped.append(row)
        else:
            row['end_date'] = snap_day - timedelta(days=1)
            closed.append(row)
    return closed, dropped


def _merge_snapshot(interval_frame: pl.DataFrame, coverage_frame: pl.DataFrame,
                    snapshot: pl.DataFrame, snap_day: date,
                    today: date) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """把快照当日状态并入区间表：缺区间则补锚点，冲突则以快照为准闭合。

    调用方必须按快照日期升序逐日调用；``unknown`` 只闭合由快照锚定的开区间
    （停在最后已知日），不延展、也不回填后一日状态。
    """
    anchors = 0
    closes = 0
    dropped_total = 0
    conflicts: list[dict[str, Any]] = []
    extra_coverage: list[dict[str, Any]] = []
    known = set(coverage_frame['ts_code'].to_list())
    rows = interval_frame.to_dicts()
    cov_rows = coverage_frame.to_dicts()
    for item in snapshot.to_dicts():
        code = str(item['code'])
        status = item['status']
        flag = flag_from_name(item['name'])
        if status == STATUS_UNKNOWN:
            closed, _ = _close_snapshot_rows(rows, code, snap_day, only_snapshot=True)
            for row in closed:
                closes += 1
                conflicts.append({
                    'ts_code': code, 'snapshot': 'unknown', 'action': 'closed',
                    'interval_flag': row['flag'],
                    'interval_start': row['start_date'].isoformat()})
            continue
        if status not in (STATUS_ST, STATUS_NON_ST):
            continue
        if status == STATUS_NON_ST:
            closed, dropped = _close_snapshot_rows(
                rows, code, snap_day, only_snapshot=False)
            for row in closed:
                row['precision'] = PRECISION_SNAPSHOT
                closes += 1
                conflicts.append({
                    'ts_code': code, 'snapshot': 'non_st', 'action': 'closed',
                    'interval_flag': row['flag'],
                    'interval_start': row['start_date'].isoformat()})
            for row in dropped:
                dropped_total += 1
                conflicts.append({
                    'ts_code': code, 'snapshot': 'non_st', 'action': 'dropped',
                    'interval_flag': row['flag'],
                    'interval_start': row['start_date'].isoformat()})
            continue
        # 以下 status == STATUS_ST
        covering = [row for row in rows
                    if row['ts_code'] == code and row['start_date'] <= snap_day
                    and (row['end_date'] is None or row['end_date'] >= snap_day)]
        if not covering and flag:
            rows.append({
                'ts_code': code, 'start_date': snap_day, 'end_date': None,
                'flag': flag, 'source': SOURCE_SNAPSHOT,
                'precision': PRECISION_SNAPSHOT, 'fetched_at': datetime.now(),
            })
            anchors += 1
            if code not in known:
                extra_coverage.append({
                    'ts_code': code, 'coverage_start': snap_day,
                    'coverage_end': today, 'source': SOURCE_SNAPSHOT,
                    'fetched_at': datetime.now()})
        for cov_row in cov_rows:
            if cov_row['ts_code'] != code:
                continue
            if code in known and not covering and flag:
                cov_row['coverage_start'] = min(cov_row['coverage_start'], snap_day)
            if cov_row['source'] == 'em_name_history_clean' \
                    and cov_row['coverage_start'] < snap_day:
                # 名称登记册的「从未 ST」负证据被当日快照推翻：
                # 只从快照日起算可知窗口，之前一律 unknown
                cov_row['coverage_start'] = snap_day
                cov_row['source'] = SOURCE_SNAPSHOT
    interval_frame = pl.DataFrame(rows, schema=_INTERVAL_SCHEMA)
    coverage_frame = pl.DataFrame(cov_rows, schema=_COVERAGE_SCHEMA)
    if extra_coverage:
        coverage_frame = pl.concat([
            coverage_frame,
            pl.DataFrame(extra_coverage, schema=_COVERAGE_SCHEMA)], how='vertical')
    return interval_frame, coverage_frame, {
        'anchors': anchors, 'closes': closes, 'dropped': dropped_total,
        'conflicts': conflicts}


def _local_codes(store_root: Path | str) -> list[str]:
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    try:
        return LocalQuoteStore(Path(store_root)).codes('stock')
    except Exception:  # noqa: BLE001
        return []


def _load_supplement(store_root: Path | str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """读取 :mod:`src.data.st_name_history` 的补充表（不存在时返回空表）。"""
    base = st_dir(store_root) / 'supplement'
    interval_path = base / 'supplement_intervals.parquet'
    coverage_path_ = base / 'supplement_coverage.parquet'
    intervals = pl.read_parquet(interval_path) if interval_path.is_file() \
        else pl.DataFrame(schema=_INTERVAL_SCHEMA)
    coverage = pl.read_parquet(coverage_path_) if coverage_path_.is_file() \
        else pl.DataFrame(schema=_COVERAGE_SCHEMA)
    return intervals, coverage


def _with_output_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """补齐落盘列（``evidence`` / ``raw_fetched_at``），缺列时填 null。"""
    for name, dtype in _INTERVAL_OUTPUT_SCHEMA.items():
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(name))
    return frame.select(list(_INTERVAL_OUTPUT_SCHEMA))


def _merge_reviewed_contiguous(rows: pl.DataFrame) -> pl.DataFrame:
    """裁定层内的连续性合并：同标的、同 flag、``end+1 == 下一段 start`` 合并。

    只作用于人工裁定的新增段，**不动源端已产出的区间**。precision 取两者中
    较弱的一档（合并后的段界由两份证据共同决定，宁严不宽）。
    """
    order = {PRECISION_EXACT: 0, PRECISION_ANNOUNCEMENT: 1, PRECISION_INFERRED: 2}
    out: list[dict[str, Any]] = []
    for row in rows.sort(['ts_code', 'start_date']).to_dicts():
        prev = out[-1] if out else None
        if (prev is not None and prev['ts_code'] == row['ts_code']
                and prev['flag'] == row['flag'] and prev['end_date'] is not None
                and row['start_date'] is not None
                and prev['end_date'] + timedelta(days=1) == row['start_date']):
            prev['end_date'] = row['end_date']
            prev['evidence'] = ' ; '.join(
                part for part in (prev['evidence'], row['evidence']) if part)
            if order.get(row['precision'], 9) > order.get(prev['precision'], 9):
                prev['precision'] = row['precision']
            continue
        out.append(dict(row))
    return pl.DataFrame(out, schema=_REVIEWED_SCHEMA)


def apply_reviewed_conflicts(frame: pl.DataFrame,
                             store_root: Path | str) -> pl.DataFrame:
    """把人工裁定输入作为**最高优先级覆盖层**并入区间表。

    这是「合并结果可重建」的关键：重建时从原始源汇总出来的区间可能与人工
    裁定结论冲突（旧表漏记、口径不同），必须再叠加这份固定输入，否则下一次
    ``build_store`` 会把裁定结果悄悄回退。
    """
    path = reviewed_conflicts_path(store_root)
    if not path.is_file():
        return frame
    reviewed = pl.read_parquet(path)
    missing = set(_REVIEWED_SCHEMA) - set(reviewed.columns)
    if missing:
        raise ValueError(f'{path} 缺列: {sorted(missing)}')
    rows = _merge_reviewed_contiguous(reviewed)
    merged = pl.concat(
        [_with_output_columns(frame),
         _with_output_columns(rows.drop('adjudication'))],
        how='vertical_relaxed')
    return merged.unique(
        subset=['ts_code', 'start_date', 'end_date', 'flag', 'source'], keep='last'
    ).sort(['ts_code', 'start_date'])


def _write_reviewed_pending(root: Path, out_root: Path) -> int:
    """把「证据不足待复核」固定输入原样落到输出目录，重建后仍然存在。"""
    source = reviewed_pending_path(root)
    if not source.is_file():
        return 0
    pending = pl.read_parquet(source)
    _write_parquet_atomic(pending, pending_review_path(out_root))
    return pending.height


def _first_evidence_dates(
    sz_changes: dict[str, tuple[str, list[tuple[date, str]]]],
    notice_events: dict[str, list[tuple[date, str, str]]],
) -> dict[str, date]:
    """每个标的最早一条名称变更/公告日期（上市日缺失时的区间起点兜底）。"""
    out: dict[str, date] = {}
    for code, (_, changes) in sz_changes.items():
        if changes:
            out[code] = changes[0][0]
    for code, events in notice_events.items():
        if events:
            day = min(item[0] for item in events)
            out[code] = min(out.get(code, day), day)
    return out


def _extend_coverage_to_evidence(
    coverage_frame: pl.DataFrame, interval_frame: pl.DataFrame,
    listings: dict[str, tuple[date, str]],
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """把早于可信起点的 ST 段纳入 coverage 窗口（下界取段起点）。

    沪市/北交所按巨潮公告重建区间时，2005 年前公告不全，区间起点被
    ``CNINFO_SAFE_START`` 截断；人工裁定补回的早期 ST 段因此落在 coverage
    窗口之外，回测取不到。

    **下界取最早裁定 ST 段的起点，而不是上市日**：读侧对 coverage 之外的
    日期按 unknown 处理且区别对待（``src/advice/risk_filters.py`` fail-closed
    剔除、回测 ``execution_core`` 记 ``st_unknown_bars`` 并降 fill_confidence），
    若把下界下延到上市日，段前的日子会从 unknown 变成「已知非 ST」，凭空
    放大确定性。上市日只用来佐证「段起点不早于上市」，不参与窗口取值。

    仅对「确实存在早于窗口的 ST 段、且上市日不晚于段起点」的标的生效；
    证据来源写进 ``source``，未生效的行保持原值，无证据的计入 ``skipped``。
    """
    first_st: dict[str, date] = {}
    if interval_frame.height:
        grouped = interval_frame.group_by('ts_code').agg(
            pl.col('start_date').min().alias('first_st'))
        first_st = {row['ts_code']: row['first_st'] for row in grouped.to_dicts()}
    rows = coverage_frame.to_dicts()
    by_source: dict[str, int] = {}
    skipped: list[dict[str, Any]] = []
    for row in rows:
        code = row['ts_code']
        start = first_st.get(code)
        # 深市窗口本就以行情首行/上市日为起点，不存在 2005 截断
        if code.endswith('.SZ') or start is None or start >= row['coverage_start']:
            continue
        info = listings.get(code)
        if info is None or info[0] > start:
            skipped.append({
                'ts_code': code, 'first_st': start.isoformat(),
                'coverage_start': row['coverage_start'].isoformat(),
                'reason': 'no_listing_evidence' if info is None
                else 'evidence_after_segment'})
            continue
        _, kind = info
        row['coverage_start'] = start
        row['source'] = _EXT_SOURCE[kind]
        by_source[row['source']] = by_source.get(row['source'], 0) + 1
    return pl.DataFrame(rows, schema=_COVERAGE_SCHEMA), {
        'changed': sum(by_source.values()), 'by_source': by_source,
        'skipped': skipped}


def _input_timestamp(store_root: Path | str, as_of: date) -> datetime:
    """确定性构建时间戳：取输入缓存的最新 mtime（截到秒）。

    覆盖窗口每次重建都会重写 ``fetched_at``；若用 ``datetime.now()``，同输入两次
    重建会产出不同字节，无法做「重建结果 == 正式表、SHA256 相等」的对拍，也就
    无法用校验发现"重跑一次就静默回退"。改取输入（``raw/``、``supplement/``、
    ``meta/st_status/``）的最大 mtime：输入不变则时间戳不变。
    """
    root = Path(store_root)
    stamps: list[float] = []
    for base in (raw_dir(root), st_dir(root) / 'supplement', root / 'meta' / 'st_status'):
        if not base.is_dir():
            continue
        for path in base.rglob('*.parquet'):
            try:
                stamps.append(path.stat().st_mtime)
            except OSError:  # pragma: no cover - 文件被并发删除时忽略
                continue
    if not stamps:
        return datetime.combine(as_of, datetime.min.time())
    return datetime.fromtimestamp(max(stamps)).replace(microsecond=0)


def apply_reviewed_coverage(coverage_frame: pl.DataFrame,
                            store_root: Path | str, today: date,
                            run_at: datetime) -> tuple[pl.DataFrame, dict[str, Any]]:
    """把覆盖窗口的逐只人工裁定结论作为**最高优先级覆盖层**并入。

    与 :func:`apply_reviewed_conflicts` 同理：常规构建对深市按上市日/行情首行起算、
    对沪市/北交所按 2005 年可信起点起算，人工裁定过的窗口（下界取最早裁定 ST 段
    起点）必须能从固定输入重放，否则下一次 ``build_store`` 会把结论悄悄退回。
    只命中表内列出的标的，未列出的标的**一个字段都不动**。
    """
    path = reviewed_coverage_path(store_root)
    if not path.is_file():
        return coverage_frame, {'applied': 0, 'appended': 0}
    reviewed = pl.read_parquet(path)
    missing = set(_REVIEWED_COVERAGE_SCHEMA) - set(reviewed.columns)
    if missing:
        raise ValueError(f'{path} 缺列: {sorted(missing)}')
    patch = reviewed.select(['ts_code', 'coverage_start', 'coverage_end', 'source']) \
        .unique(subset=['ts_code'], keep='last')
    bad = patch.filter(
        pl.col('coverage_start').is_null()
        | (pl.col('coverage_end').is_not_null()
           & (pl.col('coverage_end') < pl.col('coverage_start'))))
    if bad.height:
        raise ValueError(f'{path} 有 {bad.height} 行窗口非法: {bad.head(5).to_dicts()}')
    known = set(coverage_frame['ts_code'].to_list())
    appended_codes = sorted(set(patch['ts_code'].to_list()) - known)
    merged = coverage_frame.join(patch, on='ts_code', how='left', suffix='_rev')
    merged = merged.with_columns(
        pl.coalesce([pl.col('coverage_start_rev'), pl.col('coverage_start')])
        .alias('coverage_start'),
        pl.coalesce([pl.col('coverage_end_rev'), pl.col('coverage_end')])
        .alias('coverage_end'),
        pl.coalesce([pl.col('source_rev'), pl.col('source')]).alias('source'),
    ).drop(['coverage_start_rev', 'coverage_end_rev', 'source_rev'])
    appended = pl.DataFrame(
        [{'ts_code': row['ts_code'], 'coverage_start': row['coverage_start'],
          'coverage_end': row['coverage_end'] if row['coverage_end'] is not None else today,
          'source': row['source'], 'fetched_at': run_at}
         for row in patch.filter(pl.col('ts_code').is_in(appended_codes)).to_dicts()]
        if appended_codes else [],
        schema=_COVERAGE_SCHEMA)
    merged = pl.concat([merged, appended], how='vertical_relaxed') \
        .unique(subset=['ts_code'], keep='last').sort('ts_code')
    return merged, {'applied': patch.height, 'appended': len(appended_codes)}


def _write_coverage_store(coverage_frame: pl.DataFrame, summary: dict[str, Any],
                          out_root: Path, conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    """覆盖表的**唯一写入口**：parquet 落盘后立刻把其 SHA256 / 行数写进 json。

    两个文件必须由同一个调用产出、同批落盘：任何外部脚本单独重写 parquet 都会让
    :func:`coverage_meta_status` 报 ``sha_match=False``，不再出现「coverage.json
    停在 14:17、st_coverage.parquet 已被 17:12 改写」这种无痕漂移。
    """
    target = coverage_path(out_root)
    _write_parquet_atomic(coverage_frame, target)
    stamped = {
        **summary,
        'coverage_rows': coverage_frame.height,
        'coverage_sha256': _sha256_file(target),
        'conflicts': conflicts[:200],
    }
    write_text_atomic(
        json.dumps(stamped, ensure_ascii=False, indent=2),
        coverage_json_path(out_root))
    return stamped


def build_store(store_root: Path | str, *, apply_snapshot: bool = True,
                output_dir: Path | str | None = None,
                as_of: date | None = None,
                apply_coverage_replay: bool = True) -> dict[str, Any]:
    """从原始缓存重建区间表与覆盖窗口（幂等，原子替换）。

    ``output_dir`` 用于把重建结果写到另一目录（如临时目录）做一致性对拍，
    默认原地写入 ``store_root``。

    ``as_of`` 固定「今天」（默认 ``date.today()``）；``apply_coverage_replay``
    控制是否套用 ``raw/reviewed_coverage.parquet`` 覆盖窗口重放层（默认开）。
    覆盖窗口的 ``fetched_at`` 取输入缓存最新 mtime 而非 ``datetime.now()``，
    因此同一批输入 + 同一个 ``as_of`` 重跑得到的 ``st_coverage.parquet`` 逐字节
    相同，可直接与正式表做 SHA256 对拍。

    ``st_coverage.parquet`` 与 ``coverage.json`` 只经 :func:`_write_coverage_store`
    在同一处落盘，json 里带 ``coverage_sha256`` / ``coverage_rows``，可用
    :func:`coverage_meta_status` 自证同源；外部脚本单独重写 parquet 会被检出。
    """
    root = Path(store_root)
    out_root = Path(output_dir) if output_dir is not None else root
    rdir = raw_dir(root)
    today = as_of or date.today()
    run_at = _input_timestamp(root, today)
    sz_raw = _load_frame(rdir / 'sz_change_name.parquet', _RAW_SZ_SCHEMA)
    notices = _load_frame(rdir / 'cninfo_notices.parquet', _RAW_NOTICE_SCHEMA)
    if sz_raw.height == 0 and notices.height == 0:
        raise RuntimeError('无原始数据，请先执行 sync')

    sz_changes = _changes_from_sz(sz_raw) if sz_raw.height else {}
    notice_events = _segments_from_notices(notices) if notices.height else {}
    # 深交所简称变更表是完整登记册：无任何记录的深市标的即「上市至今未改名」
    # → 全程非 ST。沪市/北交所没有官方登记册，只对出现过 ST 记录的标的建窗口。
    local_codes = [code for code in _local_codes(root) if code.endswith('.SZ')]
    all_codes = {code for code in
                 set(sz_changes) | set(notice_events) | set(local_codes) if code}
    listings = _listing_dates(root, all_codes)
    # 终极兜底：既无行情覆盖、也不在 stock_basic 的标的（B 股/北交所个别代码），
    # 用最早一条名称变更/公告日期作为区间起点，保证其 ST 区间重建不丢失。
    evidence_start = _first_evidence_dates(sz_changes, notice_events)

    intervals: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    listing_evidence: dict[str, tuple[date, str]] = {}
    unknown_start: list[str] = []
    for code in sorted(all_codes):
        info = listings.get(code)
        if info is None:
            fallback = evidence_start.get(code)
            if fallback is None:
                # 上市日缺失：历史实现用 ``date(1990, 1, 1)`` 兜底，会把覆盖窗口
                # 前移到 1990，方向偏乐观（凭空多出「已知非 ST」的年份）。现在
                # 该标的不参与覆盖统计，只记为 unknown_start 供审计；当日快照
                # 若判其为 ST，仍会由 ``_merge_snapshot`` 补锚点与覆盖。
                unknown_start.append(code)
                continue
            listing, listing_kind = fallback, 'evidence'
        else:
            listing, listing_kind = info
        listing_evidence[code] = (listing, listing_kind)
        if code.endswith('.SZ'):
            prev_flag, changes = sz_changes.get(code, ('', []))
            timeline, delist = _build_timeline(
                code, prev_flag, changes, listing, today, SOURCE_SZ_OFFICIAL)
            cov_start = listing
            cov_end = min(delist, today) if delist else today
            intervals.extend(_segments_to_intervals(
                code, timeline.segments, cov_end, today, SOURCE_SZ_OFFICIAL))
        else:
            segments, _ = _cninfo_timeline(code, notice_events.get(code, []), listing, today)
            cov_start = max(listing, CNINFO_SAFE_START)
            cov_end = today
            intervals.extend(_segments_to_intervals(
                code, segments, cov_end, today, SOURCE_CNINFO))
        coverage.append({'ts_code': code, 'coverage_start': cov_start,
                         'coverage_end': cov_end, 'source': COV_SOURCE_MERGED,
                         'fetched_at': run_at})

    interval_frame = pl.DataFrame(intervals, schema=_INTERVAL_SCHEMA)
    coverage_frame = pl.DataFrame(coverage, schema=_COVERAGE_SCHEMA)

    # 开区间闭合：用个股全量公告找到的摘帽/更名证据
    closure = _load_frame(rdir / 'closure_evidence.parquet', _CLOSURE_SCHEMA)
    closure_fixed = 0
    if closure.height:
        mapping = {row['ts_code']: row['close_date'] for row in closure.to_dicts()}
        rows = interval_frame.to_dicts()
        for row in rows:
            close_day = mapping.get(row['ts_code'])
            if row['end_date'] is None and close_day and close_day > row['start_date']:
                row['end_date'] = close_day - timedelta(days=1)
                row['precision'] = PRECISION_INFERRED
                closure_fixed += 1
        interval_frame = pl.DataFrame(rows, schema=_INTERVAL_SCHEMA)

    # 合并名称登记册补充覆盖（src.data.st_name_history）：只补原表没有的标的
    supp_intervals, supp_coverage = _load_supplement(root)
    if supp_coverage.height:
        coverage_frame = pl.concat([coverage_frame, supp_coverage], how='vertical_relaxed') \
            .unique(subset=['ts_code'], keep='first')
    if supp_intervals.height:
        interval_frame = pl.concat([interval_frame, supp_intervals], how='vertical_relaxed')

    # 合并快照：当日真值锚点，防止公告缺失导致历史/当前状态错判。
    # 必须按快照日期升序逐日合并：某一天的状态只能由不晚于该日的快照决定，
    # 否则后一日才知道的 ST 状态会被回推到前一日（未来函数）。
    snapshots = _snapshot_series(root, upto=today) if apply_snapshot else []
    stats: dict[str, Any] = {'anchors': 0, 'closes': 0, 'dropped': 0, 'conflicts': []}
    snap_day: date | None = None
    for snapshot, one_day in snapshots:
        interval_frame, coverage_frame, one_stats = _merge_snapshot(
            interval_frame, coverage_frame, snapshot, one_day, today)
        stats['anchors'] += one_stats['anchors']
        stats['closes'] += one_stats['closes']
        stats['dropped'] += one_stats['dropped']
        stats['conflicts'].extend(one_stats['conflicts'])
        snap_day = one_day
    conflicts = stats['conflicts']

    frame = interval_frame.unique(
        subset=['ts_code', 'start_date', 'end_date', 'flag', 'source'], keep='last'
    ).sort(['ts_code', 'start_date'])
    # 人工裁定覆盖层必须在汇总之后套用：只补冲突段，不动源端已产出的区间值
    frame = apply_reviewed_conflicts(frame, root)
    coverage_frame = coverage_frame.unique(subset=['ts_code'], keep='last') \
        .sort('ts_code')
    # 裁定补回的早期 ST 段若早于沪市/北交所可信起点，把 coverage 下延到上市证据，
    # 否则这些段在回测侧会被窗口裁掉、等于合并不生效。
    coverage_frame, ext_stats = _extend_coverage_to_evidence(
        coverage_frame, frame, listing_evidence)
    # 人工裁定的窗口结论作为最高优先级重放层：只命中表内标的，未列出的不动；
    # 关闭时行为与旧版完全一致（深市按上市日、沪市/北交所按 2005 可信起点）。
    if apply_coverage_replay:
        coverage_frame, replay_stats = apply_reviewed_coverage(
            coverage_frame, root, today, run_at)
    else:
        replay_stats = {'applied': 0, 'appended': 0}
    # 覆盖窗口的 fetched_at 统一取确定性构建时间戳（同输入重跑逐字节一致），
    # 来源信息保留在 source 列里，不靠时间戳区分。
    coverage_frame = coverage_frame.with_columns(
        pl.lit(run_at).alias('fetched_at')).sort('ts_code')

    _write_parquet_atomic(frame, intervals_path(out_root))
    pending_rows = _write_reviewed_pending(root, out_root)
    summary = {
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'snapshot_date': snap_day.isoformat() if snap_day else None,
        'sources': [SOURCE_SZ_OFFICIAL, SOURCE_CNINFO, SOURCE_SNAPSHOT,
                    SOURCE_REVIEWED],
        'sse_bse_safe_start': CNINFO_SAFE_START.isoformat(),
        'interval_rows': frame.height,
        'security_count': frame['ts_code'].n_unique(),
        'coverage_rows': coverage_frame.height,
        'unknown_start_count': len(unknown_start),
        'unknown_start_codes': unknown_start[:200],
        'snapshot_anchors': stats['anchors'],
        'snapshot_conflicts': len(conflicts),
        'snapshot_closed_intervals': stats['closes'],
        'snapshot_dropped_intervals': stats['dropped'],
        'closure_fixed_intervals': closure_fixed,
        'supplement_interval_rows': supp_intervals.height,
        'supplement_coverage_rows': supp_coverage.height,
        'reviewed_interval_rows': int(
            (frame['source'] == SOURCE_REVIEWED).sum()),
        'pending_review_rows': pending_rows,
        'coverage_extended_rows': ext_stats['changed'],
        'coverage_extended_by_source': ext_stats['by_source'],
        'coverage_extension_skipped': ext_stats['skipped'][:200],
        'coverage_replay_applied': replay_stats['applied'],
        'coverage_replay_appended': replay_stats['appended'],
        'coverage_replay_enabled': bool(apply_coverage_replay),
        'coverage_replay_note': COVERAGE_REPLAY_NOTE,
        'deterministic_run_at': run_at.isoformat(timespec='seconds'),
        'window_extended_note': WINDOW_EXTENDED_NOTE,
    }
    summary = _write_coverage_store(coverage_frame, summary, out_root, conflicts)
    logger.info(f'[st] 区间表已落盘: {intervals_path(out_root)}（{frame.height} 行 / '
                f'{frame["ts_code"].n_unique()} 只）')
    if unknown_start:
        logger.warning(f'[st] {len(unknown_start)} 只标的上市日缺失，已排除出覆盖统计'
                       f'（unknown_start，不再用 1990-01-01 兜底）')
    return summary


# ----------------------------------------------------------------------
# 查询
# ----------------------------------------------------------------------
def _read_cached(path: Path, cache: dict[str, tuple[int, pl.DataFrame]],
                 schema: dict[str, Any]) -> pl.DataFrame | None:
    if not path.is_file():
        return None
    mtime = path.stat().st_mtime_ns
    cached = cache.get(str(path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    frame = pl.read_parquet(path)
    cache[str(path)] = (mtime, frame)
    return frame


def load_intervals(store_root: Path | str) -> pl.DataFrame | None:
    return _read_cached(intervals_path(store_root), _INTERVAL_CACHE, _INTERVAL_SCHEMA)


def load_coverage(store_root: Path | str) -> pl.DataFrame | None:
    return _read_cached(coverage_path(store_root), _COVERAGE_CACHE, _COVERAGE_SCHEMA)


@dataclass(frozen=True)
class StHistoryResult:
    """单标的单日 ST 历史判定结果。"""

    status: str
    flag: str | None = None
    source: str = ''
    start: date | None = None
    end: date | None = None
    coverage_start: date | None = None
    coverage_end: date | None = None
    reason: str = ''
    suspected: bool = False


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def _normalize(code: str) -> str:
    text = str(code).strip().upper()
    if '.' in text:
        return text
    return _code_to_ts(text) or text


def status_at(store_root: Path | str, code: str, as_of: Any, *,
              include_suspected: bool = False) -> StHistoryResult:
    """按历史区间判定 ST；无记录且不在可信窗口内 → ``unknown``。

    ``include_suspected=True`` 时才采信「行情反推」的疑似区间（``flag='st'``,
    ``suspected=True``, ``source='price_inference'``）。默认只认确证区间，
    回测风险过滤走默认值。
    """
    day = _as_date(as_of)
    if day is None:
        return StHistoryResult(STATUS_UNKNOWN, reason='bad_date')
    target = _normalize(code)
    intervals = load_intervals(store_root)
    coverage = load_coverage(store_root)
    if intervals is None or intervals.height == 0:
        return StHistoryResult(STATUS_UNKNOWN, reason='no_history_table')
    hits = intervals.filter(
        (pl.col('ts_code') == target) & (pl.col('start_date') <= day)
        & ((pl.col('end_date').is_null()) | (pl.col('end_date') >= day)))
    if hits.height:
        row = hits.sort('start_date').to_dicts()[-1]
        return StHistoryResult(STATUS_ST, flag=row['flag'], source=row['source'],
                               start=row['start_date'], end=row['end_date'],
                               reason='interval_hit')
    cov_rows = coverage.filter(pl.col('ts_code') == target).to_dicts() \
        if coverage is not None else []
    if not cov_rows:
        result = StHistoryResult(STATUS_UNKNOWN, reason='code_not_in_coverage')
        return _apply_suspected(store_root, target, day, result, include_suspected)
    cov = cov_rows[0]
    if cov['coverage_start'] <= day <= cov['coverage_end']:
        return StHistoryResult(STATUS_NON_ST, source=cov['source'],
                               coverage_start=cov['coverage_start'],
                               coverage_end=cov['coverage_end'],
                               reason='outside_interval_in_coverage')
    result = StHistoryResult(STATUS_UNKNOWN, coverage_start=cov['coverage_start'],
                             coverage_end=cov['coverage_end'], reason='outside_coverage')
    return _apply_suspected(store_root, target, day, result, include_suspected)


def _apply_suspected(store_root: Path | str, code: str, day: date,
                     base: StHistoryResult, include: bool) -> StHistoryResult:
    """``unknown`` 且显式允许时，用行情推断的疑似区间兜底（绝不覆盖 non_st）。"""
    if not include or base.status != STATUS_UNKNOWN:
        return base
    from src.data import st_price_infer  # noqa: PLC0415

    hit = st_price_infer.suspected_status_at(store_root, code, day)
    if hit is None:
        return base
    return StHistoryResult(STATUS_ST, flag=hit.flag, source=hit.source,
                           start=hit.start, end=hit.end, reason='suspected_interval_hit',
                           coverage_start=base.coverage_start,
                           coverage_end=base.coverage_end, suspected=True)


def intervals_for_backtest(store_root: Path | str, codes: Iterable[str],
                           start: Any, end: Any) -> dict[str, list[dict[str, Any]]]:
    """导出与 ``StStatusService.get_status_intervals`` 同构的区间列表。

    区间外但落在可信窗口内的空档，显式补 ``is_st=False`` 段，让回测把
    「已知非 ST」与「未知」区分开；窗口外不补，保持 unknown。
    """
    start_day = _as_date(start) or date(1990, 1, 1)
    end_day = _as_date(end) or date.today()
    intervals = load_intervals(store_root)
    coverage = load_coverage(store_root)
    if intervals is None or coverage is None:
        return {}
    wanted = {_normalize(code) for code in codes}
    st_rows = intervals.filter(
        pl.col('ts_code').is_in(list(wanted))
        & (pl.col('start_date') <= end_day)
        & ((pl.col('end_date').is_null()) | (pl.col('end_date') >= start_day)))
    cov_rows = coverage.filter(pl.col('ts_code').is_in(list(wanted)))
    result: dict[str, list[dict[str, Any]]] = {}
    for cov in cov_rows.to_dicts():
        code = cov['ts_code']
        window_start = max(start_day, cov['coverage_start'])
        window_end = min(end_day, cov['coverage_end'])
        if window_start > window_end:
            continue
        pieces: list[dict[str, Any]] = []
        for row in st_rows.filter(pl.col('ts_code') == code).sort('start_date').to_dicts():
            seg_start = max(window_start, row['start_date'])
            seg_end = min(window_end, row['end_date'] or window_end)
            if seg_start <= seg_end:
                pieces.append({'start_date': seg_start, 'end_date': seg_end,
                               'is_st': True})
        result[code] = _carve_non_st(pieces, window_start, window_end)
    for item in result.values():
        item.sort(key=lambda seg: seg['start_date'])
        for seg in item:
            seg['start_date'] = seg['start_date'].isoformat()
            seg['end_date'] = seg['end_date'].isoformat()
    return result


def _carve_non_st(st_pieces: list[dict[str, Any]], window_start: date,
                  window_end: date) -> list[dict[str, Any]]:
    """在可信窗口内挖掉 ST 子区间，其余显式标为非 ST，保证区间不重叠。"""
    out: list[dict[str, Any]] = []
    cursor = window_start
    for seg in sorted(st_pieces, key=lambda p: p['start_date']):
        if seg['start_date'] > cursor:
            out.append({'start_date': cursor,
                        'end_date': min(window_end, seg['start_date'] - timedelta(days=1)),
                        'is_st': False})
        out.append(seg)
        cursor = max(cursor, seg['end_date'] + timedelta(days=1))
    if cursor <= window_end:
        out.append({'start_date': cursor, 'end_date': window_end, 'is_st': False})
    return out


def coverage_summary(store_root: Path | str) -> dict[str, Any]:
    path = coverage_json_path(store_root)
    if not path.is_file():
        return {'available': False}
    payload = json.loads(path.read_text(encoding='utf-8'))
    payload['available'] = True
    intervals = load_intervals(store_root)
    coverage = load_coverage(store_root)
    if intervals is not None and coverage is not None:
        payload['st_code_count'] = intervals['ts_code'].n_unique()
        payload['covered_code_count'] = coverage.height
        payload['first_coverage'] = str(coverage['coverage_start'].min())
        payload['last_coverage'] = str(coverage['coverage_end'].max())
    return payload


def coverage_meta_status(store_root: Path | str) -> dict[str, Any]:
    """校验 ``coverage.json`` 与 ``st_coverage.parquet`` 是否同源一致。

    ``build_store`` 是唯一写入口：parquet 落盘后立刻取其 SHA256 写进 json。
    ``sha_match`` / ``rows_match`` 任一为假，就说明有外部脚本绕过 ``build_store``
    单独重写了其中一个文件（历史事故：parquet 于 17:12 被补丁脚本重写，json 仍
    停在 14:17 且 ``coverage_replay_applied`` 为 null）。
    """
    root = Path(store_root)
    json_path = coverage_json_path(root)
    parquet_path = coverage_path(root)
    status: dict[str, Any] = {
        'json_present': json_path.is_file(),
        'parquet_present': parquet_path.is_file(),
        'ok': False,
    }
    if not status['json_present'] or not status['parquet_present']:
        status['reason'] = 'missing_file'
        return status
    payload = json.loads(json_path.read_text(encoding='utf-8'))
    declared_sha = payload.get('coverage_sha256')
    declared_rows = payload.get('coverage_rows')
    actual_sha = _sha256_file(parquet_path)
    actual_rows = pl.read_parquet(parquet_path, columns=['ts_code']).height
    status.update({
        'built_at': payload.get('built_at'),
        'coverage_replay_applied': payload.get('coverage_replay_applied'),
        'coverage_replay_enabled': payload.get('coverage_replay_enabled'),
        'declared_sha256': declared_sha,
        'actual_sha256': actual_sha,
        'sha_match': declared_sha == actual_sha,
        'declared_rows': declared_rows,
        'actual_rows': actual_rows,
        'rows_match': declared_rows == actual_rows,
    })
    status['ok'] = bool(status['sha_match'] and status['rows_match'])
    if not status['ok']:
        status['reason'] = 'sha_mismatch' if not status['sha_match'] else 'rows_mismatch'
    return status


# ----------------------------------------------------------------------
# 验证（对拍快照 / 深市交叉校验 / 只数时序 / 回测影响）
# ----------------------------------------------------------------------
def _snapshot_agreement(root: Path, snapshot: pl.DataFrame,
                        snap_day: date) -> dict[str, Any]:
    """与快照对拍：区分「可判定对」与「覆盖缺口」。

    ``unknown`` 只说明本地证据没有覆盖到该标的（沪市/北交所从未 ST 的票），
    不是「判错」——这是语义红线要求的保守结果，因此单独统计覆盖率，
    可判定一致率只算 history ∈ {st, non_st} 的对。
    """
    mismatches: list[dict[str, Any]] = []
    undecidable: list[dict[str, Any]] = []
    checked = agreed = decided = 0
    decided_agreed = 0
    by_market: dict[str, dict[str, int]] = {}
    for item in snapshot.filter(pl.col('status') != STATUS_UNKNOWN).to_dicts():
        checked += 1
        res = status_at(root, item['code'], snap_day)
        market = str(item['code'])[-2:]
        bucket = by_market.setdefault(market, {'checked': 0, 'decided': 0, 'agreed': 0})
        bucket['checked'] += 1
        if res.status == item['status']:
            agreed += 1
            bucket['agreed'] += 1
        if res.status in (STATUS_ST, STATUS_NON_ST):
            decided += 1
            bucket['decided'] += 1
            if res.status == item['status']:
                decided_agreed += 1
            else:
                mismatches.append({
                    'code': item['code'], 'name': item['name'],
                    'snapshot': item['status'], 'history': res.status,
                    'history_reason': res.reason,
                    'interval_start': res.start.isoformat() if res.start else '',
                    'interval_end': res.end.isoformat() if res.end else ''})
        else:
            undecidable.append({
                'code': item['code'], 'name': item['name'],
                'snapshot': item['status'], 'history': res.status,
                'history_reason': res.reason, 'market': market})
    return {'checked': checked, 'agreed': agreed,
            'rate': agreed / checked if checked else 0.0,
            'decidable': decided, 'decidable_agreed': decided_agreed,
            'decidable_rate': decided_agreed / decided if decided else 0.0,
            'coverage_rate': decided / checked if checked else 0.0,
            'by_market': by_market,
            'undecidable': undecidable, 'mismatches': mismatches}


def _sz_cross_validation(root: Path, sz_raw: pl.DataFrame,
                         notices: pl.DataFrame) -> dict[str, Any]:
    """用深交所官方简称变更表检验「公告标题法」的精确率/召回率。"""
    official: dict[str, list[tuple[date, str]]] = {}
    for key, rows in sz_raw.group_by('code'):
        code = str(key[0]) if isinstance(key, tuple) else str(key)
        items = sorted(rows.to_dicts(), key=lambda r: (r['change_date'], r['name_new']))
        by_date: dict[date, str] = {item['change_date']: flag_from_name(item['name_new'])
                                    for item in items}
        segments = [(items[0]['change_date'], flag_from_name(items[0]['name_prev']))]
        segments.extend(sorted(by_date.items()))
        official[code] = segments

    derived: dict[str, list[tuple[date, str]]] = {}
    for code, events in _segments_from_notices(notices, include_sz=True).items():
        if not code.endswith('.SZ'):
            continue
        derived[code] = _cninfo_timeline(code, events, date(1990, 1, 1), date.today())[0]

    tp, fp, fn, details = _score_pairs(official, derived)
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    return {'precision': precision, 'recall': recall, 'tp': tp, 'fp': fp, 'fn': fn,
            'samples': details[:100]}


def _score_pairs(official: dict[str, list[tuple[date, str]]],
                 derived: dict[str, list[tuple[date, str]]]
                 ) -> tuple[int, int, int, list[dict[str, Any]]]:
    tp = fp = fn = 0
    details: list[dict[str, Any]] = []

    def state_at(segments: list[tuple[date, str]], day: date) -> bool | None:
        state: bool | None = None
        for start, flag in segments:
            if start > day:
                break
            state = flag in (FLAG_ST, FLAG_STAR)
        return state

    for code, segments in official.items():
        for day, flag in segments:
            if day < CNINFO_SAFE_START:
                continue
            expect = flag in (FLAG_ST, FLAG_STAR)
            got = state_at(derived.get(code, []), day) if code in derived else None
            if got is None:
                fn += int(expect)
                continue
            if got and expect:
                tp += 1
            elif got:
                fp += 1
                details.append({'code': code, 'date': day.isoformat(),
                                'issue': 'false_st'})
            elif expect:
                fn += 1
                details.append({'code': code, 'date': day.isoformat(),
                                'issue': 'missed_st'})
    return tp, fp, fn, details


def _trading_dates(root: Path, start: date, end: date) -> list[date]:
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    store = LocalQuoteStore(root)

    def _dates(frame: pl.DataFrame | None) -> list[date]:
        if frame is None:
            return []
        column = resolve_date_column(frame.columns)
        if column is None:
            return []
        values = [value if isinstance(value, date) else _as_date(value)
                  for value in frame[column].to_list()]
        return [day for day in values if day is not None]

    days = _dates(store.read_file('stock', '000001.SZ'))
    if not days:
        for code in store.codes('stock')[:3]:
            days.extend(_dates(store.read_file('stock', code)))
            if days:
                break
    return sorted({day for day in days if day is not None and start <= day <= end})


def _daily_series(intervals: pl.DataFrame,
                  calendar: list[date]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按交易日统计 ST 只数与环比突变日。"""
    deltas: dict[date, int] = {}
    for row in intervals.to_dicts():
        deltas[row['start_date']] = deltas.get(row['start_date'], 0) + 1
        if row['end_date'] is not None:
            after = row['end_date'] + timedelta(days=1)
            deltas[after] = deltas.get(after, 0) - 1
    points = sorted(deltas.items())
    out: list[dict[str, Any]] = []
    count = 0
    index = 0
    for day in calendar:
        while index < len(points) and points[index][0] <= day:
            count += points[index][1]
            index += 1
        out.append({'date': day.isoformat(), 'st_count': count})
    jumps = []
    for prev, cur in zip(out, out[1:], strict=False):
        diff = cur['st_count'] - prev['st_count']
        if abs(diff) > 20:
            jumps.append({'date': cur['date'], 'delta': diff,
                          'st_count': cur['st_count']})
    return out, jumps


def _impact(root: Path, intervals: pl.DataFrame, coverage: pl.DataFrame,
            calendar: list[date]) -> dict[str, Any]:
    """量化：历史 (标的, 交易日) 中由 unknown 变为可判定的比例。"""
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    store = LocalQuoteStore(root)
    codes = store.codes('stock')
    cov_map = {row['ts_code']: (row['coverage_start'], row['coverage_end'])
               for row in coverage.to_dicts()}
    st_by_code: dict[str, list[tuple[date, date]]] = {}
    for row in intervals.to_dicts():
        st_by_code.setdefault(row['ts_code'], []).append(
            (row['start_date'], row['end_date'] or date.today()))
    total = covered = st_pairs = 0
    for code in codes:
        cov = cov_map.get(code)
        if cov is None:
            total += len(calendar)
            continue
        start_cov, end_cov = max(cov[0], calendar[0]), min(cov[1], calendar[-1])
        in_window = [day for day in calendar if start_cov <= day <= end_cov]
        total += len(calendar)
        covered += len(in_window)
        for interval_start, interval_end in st_by_code.get(code, []):
            st_pairs += sum(1 for day in in_window
                            if interval_start <= day <= interval_end)
    return {'calendar_days': len(calendar), 'total_pairs': total,
            'covered_pairs': covered,
            'covered_ratio': covered / total if total else 0.0,
            'st_pairs': st_pairs,
            'still_unknown_pairs': total - covered}


def verify(store_root: Path | str, *, output_dir: Path | str = Path('logs/st_history/verify')
           ) -> dict[str, Any]:
    root = Path(store_root)
    intervals = load_intervals(root)
    coverage = load_coverage(root)
    if intervals is None or coverage is None:
        raise RuntimeError('区间表尚未生成，先执行 build')
    snapshot, snap_day = _latest_snapshot(root)
    rdir = raw_dir(root)
    sz_raw = _load_frame(rdir / 'sz_change_name.parquet', _RAW_SZ_SCHEMA)
    notices = _load_frame(rdir / 'cninfo_notices.parquet', _RAW_NOTICE_SCHEMA)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    result: dict[str, Any] = {'store_root': str(root)}
    if snapshot is not None and snap_day is not None:
        result['snapshot'] = _snapshot_agreement(root, snapshot, snap_day)
        pl.DataFrame(result['snapshot']['mismatches']).write_csv(out / 'snapshot_mismatch.csv')
        pl.DataFrame(result['snapshot']['undecidable']).write_csv(out / 'snapshot_undecidable.csv')
    if sz_raw.height:
        result['sz_cross_validation'] = _sz_cross_validation(root, sz_raw, notices)
        pl.DataFrame(result['sz_cross_validation']['samples']).write_csv(
            out / 'sz_cross_validation_samples.csv')
    first_cov = _as_date(coverage['coverage_start'].min())
    start = max(first_cov if first_cov is not None else date(2015, 1, 1), date(2015, 1, 1))
    calendar = _trading_dates(root, start, date.today())
    if calendar:
        series, jumps = _daily_series(intervals, calendar)
        pl.DataFrame(series).write_csv(out / 'st_count_by_date.csv')
        pl.DataFrame(jumps).write_csv(out / 'st_count_jumps.csv')
        result['daily_series'] = {'days': len(series), 'jumps': jumps[:50],
                                 'max_st_count': max((p['st_count'] for p in series),
                                                     default=0)}
        result['impact'] = _impact(root, intervals, coverage, calendar)
    (out / 'report.json').write_text(
        json.dumps(result, ensure_ascii=False, default=str, indent=2), encoding='utf-8')
    return result


def _cmd_sync(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    years = list(range(args.start_year, args.end_year + 1)) if args.end_year else None
    report = sync_sources(root, years=years, force=args.force)
    logger.info(f'[st] 抓取完成: {report}')
    return 0


def _cmd_build(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    logger.info(f'[st] 构建完成: {build_store(root, apply_snapshot=not args.no_snapshot)}')
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    report = verify(root, output_dir=args.output_dir)
    snap = report.get('snapshot') or {}
    short = {
        'snapshot': {key: snap.get(key) for key in
                     ('checked', 'agreed', 'rate', 'decidable', 'decidable_agreed',
                      'decidable_rate', 'coverage_rate', 'by_market')},
        'sz_cross_validation': {key: (report.get('sz_cross_validation') or {}).get(key)
                                for key in ('precision', 'recall', 'tp', 'fp', 'fn')},
        'daily_series': {key: value for key, value in
                         (report.get('daily_series') or {}).items() if key != 'jumps'},
        'jump_count': len((report.get('daily_series') or {}).get('jumps', [])),
        'impact': report.get('impact'),
        'output_dir': str(args.output_dir),
    }
    logger.info(f'[st] 验证结果: {json.dumps(short, ensure_ascii=False, default=str)}')
    return 0


def _cmd_stats(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    logger.info(f'[st] 覆盖信息: {coverage_summary(root)}')
    meta = coverage_meta_status(root)
    if meta.get('ok'):
        logger.info(f'[st] 元数据一致性 ok: rows={meta["actual_rows"]} '
                    f'sha256={meta["actual_sha256"]} built_at={meta["built_at"]} '
                    f'replay_applied={meta["coverage_replay_applied"]}')
        return 0
    logger.error(f'[st] coverage.json 与 st_coverage.parquet 不一致: {meta}')
    return 1


def _cmd_resolve(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    logger.info(f'[st] 开区间闭合: {resolve_open_closures(root, force=args.force)}')
    return 0


def _cmd_refresh(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    sync_sources(root, force=args.force)
    build_store(root, apply_snapshot=False)
    stats = resolve_open_closures(root, force=args.force)
    build_store(root)
    logger.info(f'[st] 全量刷新完成: {stats}')
    return 0


def _resolve_root(args: argparse.Namespace) -> Path:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    return Path(args.store_root) if args.store_root else resolve_store_root(get_config())


def main() -> int:
    parser = argparse.ArgumentParser(description='历史 ST 区间表（网络取数 + 本地落地）')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    sub = parser.add_subparsers(dest='command', required=True)
    sync = sub.add_parser('sync', help='抓取原始数据（限速 + 断点续跑）')
    sync.add_argument('--start-year', type=int, default=2002)
    sync.add_argument('--end-year', type=int, default=None)
    sync.add_argument('--force', action='store_true', help='忽略断点全量重抓')
    sync.set_defaults(func=_cmd_sync)
    build = sub.add_parser('build', help='从原始缓存重建区间表')
    build.add_argument('--no-snapshot', action='store_true', help='暂不合并每日快照')
    build.set_defaults(func=_cmd_build)
    resolve = sub.add_parser('resolve', help='为开区间标的补抓个股公告')
    resolve.add_argument('--force', action='store_true')
    resolve.set_defaults(func=_cmd_resolve)
    refresh = sub.add_parser('refresh', help='sync + build + resolve + build')
    refresh.add_argument('--force', action='store_true')
    refresh.set_defaults(func=_cmd_refresh)
    stats = sub.add_parser('stats', help='打印覆盖信息')
    stats.set_defaults(func=_cmd_stats)
    verify_cmd = sub.add_parser('verify', help='对拍快照 + 交叉校验 + 时序/影响量化')
    verify_cmd.add_argument('--output-dir', default='logs/st_history/verify')
    verify_cmd.set_defaults(func=_cmd_verify)
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == '__main__':
    raise SystemExit(main())
