#!/usr/bin/env python3

"""A 股「曾用名全历史」补充覆盖（东方财富 F10 名称登记册 + 巨潮公告定日）。

补 :mod:`src.data.st_history` 的缺口：沪深官方名称变更表只覆盖**发生过改名**的
标的，导致大量从未 ST 的沪市/北交所标的没有任何可信窗口，查询恒为
``unknown``。本模块用东方财富 F10 ``RPT_F10_ORG_BASICINFO`` 的全市场曾用名
登记册补「负证据」：

- 名称历史（含当前简称）里**从未出现** ``ST`` / ``*ST`` / ``PT`` 前缀
  → 该标的自上市至行情末日都不是风险警示股，写 ``non_st`` 覆盖窗口；
- 曾用名里出现 ST 前缀但本地没有区间 → 按个股公告（巨潮）逐年窗口补抓，
  只认能定到生效日的事件；定不到日期的一律继续 ``unknown``，绝不猜。

硬约束：

- **不默认 non_st**：只有名称登记册给出负证据、或公告给出定日事件才落覆盖，
  其余保持 ``unknown``；
- 点时正确：覆盖窗口从上市日（行情首日）开始，到行情末日为止；公告事件只在
  生效日之后进入区间；
- 幂等 + 断点续跑：东方财富按页缓存（``_staging/st_em_pages_cache_20261112/page_XXXX.parquet``），
  巨潮按标的缓存（``_staging/st_candidate_notices_cache_20261112/<code>.parquet``），重跑跳过已完成
  单元；失败清单单独落 CSV（两处缓存按 DG-32 落在仓库根 ``_staging/``，不入正式层）；
- 只写本地 parquet，不动 MySQL：
  ``meta/st/supplement/supplement_intervals.parquet``
  ``meta/st/supplement/supplement_coverage.parquet``
  ``meta/st/supplement/manifest.json``
  ``meta/st/supplement/unresolved.csv``
  ``_staging/st_em_names/{em_state.json,em_failures.csv}``（抓取断点，DG-7 不入正式层）
  ``_staging/st_candidate_state/{candidate_state.json,candidate_failures.csv}``（同上）

``st_history.build_store`` 会把 supplement 并入规范表，三态语义不变：只把原来
的 ``unknown`` 变成有证据的 ``non_st`` / ``st``。

可复现（2026-11-12 起）：``fetched_at`` 不再取 ``datetime.now()``，改取**对应证据
文件的 mtime**（名称登记册 → ``em_org_basic.parquet``；定日区间 → 该标的公告
缓存分片），``built_at`` 取这些证据时刻的最大值；``today`` 默认取**行情末日**
而不是 ``date.today()``。同一批输入重跑，三件产物逐字节相同。

写盘事务（DG-10，单文件原子不等于多文件事务）：``build`` 一次写 4 个文件，固定
顺序为 **快照 → 区间表 → 覆盖表 → unresolved.csv → manifest.json**。manifest 最后
写，它是"本次构建已完成"的提交标记：

- 快照之前失败：正式层零改动，无需回滚；
- 区间表/覆盖表已换、csv 未换：manifest 仍是旧值，``manifest_tool --check``
  会报 sha256 不一致，据此可判定处于中间态；
- csv 已换、manifest 写失败：同上，manifest 的 ``unresolved`` 计数与磁盘 csv
  行数不等即可检出；
- 回滚方式：把 ``_backup/st_supplement_<日期>/`` 下的 4 个文件 ``copy2`` 回
  ``meta/st/supplement/`` 即可回到上一版，快照与原文逐字节相同（SHA256 相等）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
import requests
from loguru import logger

from src.data import st_history
from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.staging import staging_dir_for_store

EM_URL = 'https://datacenter.eastmoney.com/securities/api/data/v1/get'
EM_REPORT = 'RPT_F10_ORG_BASICINFO'
EM_COLUMNS = 'SECUCODE,SECURITY_NAME_ABBR,FORMERNAME,ORG_NAME,LISTING_DATE'
EM_PAGE_SIZE = 500
EM_RETRY = 4

SOURCE_EM_CLEAN = 'em_name_history_clean'
SOURCE_CNINFO_DATED = 'cninfo_notice_dated'
#: 本模块自己写入的覆盖来源；重建时不能拿它当「已覆盖」基准。
SUPPLEMENT_SOURCES: frozenset[str] = frozenset({SOURCE_EM_CLEAN, SOURCE_CNINFO_DATED})

FLAG_ST = st_history.FLAG_ST
FLAG_STAR = st_history.FLAG_STAR
FLAG_ANY = st_history.FLAG_ANY
FLAG_NONE = ''

EM_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
}

_EM_SCHEMA = {
    'secucode': pl.Utf8, 'name': pl.Utf8, 'former_names': pl.Utf8,
    'org_name': pl.Utf8, 'listing_date': pl.Date,
}
_INTERVAL_SCHEMA = {
    'ts_code': pl.Utf8, 'start_date': pl.Date, 'end_date': pl.Date,
    'flag': pl.Utf8, 'source': pl.Utf8, 'precision': pl.Utf8,
    'fetched_at': pl.Datetime,
}
_COVERAGE_SCHEMA = {
    'ts_code': pl.Utf8, 'coverage_start': pl.Date, 'coverage_end': pl.Date,
    'source': pl.Utf8, 'fetched_at': pl.Datetime,
}
_NOTICE_SCHEMA = {
    'notice_id': pl.Utf8, 'code': pl.Utf8, 'title': pl.Utf8,
    'publish_date': pl.Date,
}

# 「申请/拟/可能/不予/驳回/核查意见」都不是生效事件；但「提示性公告」里的
# 「实施其他风险警示」是真事件，不能因为标题带“提示”就丢弃。
_NOT_EFFECTIVE = ('拟', '申请', '可能', '不予', '驳回', '驳', '核查意见', '法律意见',
                  '异常波动', '关注函', '更正', '补充公告', '进展')

_IMPL_STAR = re.compile(r'(?:实施|实行)(?:退市风险警示|警示存在终止上市风险的特别处理)')
_IMPL_ST = re.compile(r'(?:实施|实行)(?:其他风险警示|其他特别处理|特别处理)')
_LIFT_STAR = re.compile(r'(?:撤销|撤消|解除).{0,8}?退市风险警示')
_LIFT_ST = re.compile(r'(?:撤销|撤消|解除).{0,8}?(?:其他风险警示|其他特别处理|特别处理)')
_LIFT_ANY = re.compile(r'(?:撤销|撤消|解除).{0,8}?风险警示')


# ----------------------------------------------------------------------
# 路径
# ----------------------------------------------------------------------
def supplement_dir(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'st' / 'supplement'


def supplement_intervals_path(store_root: Path | str) -> Path:
    return supplement_dir(store_root) / 'supplement_intervals.parquet'


def supplement_coverage_path(store_root: Path | str) -> Path:
    return supplement_dir(store_root) / 'supplement_coverage.parquet'


def manifest_path(store_root: Path | str) -> Path:
    return supplement_dir(store_root) / 'manifest.json'


def unresolved_path(store_root: Path | str) -> Path:
    return supplement_dir(store_root) / 'unresolved.csv'


def em_state_path(store_root: Path | str) -> Path:
    """东财曾用名抓取断点。按 DG-7 落 ``_staging/st_em_names/``（稳定任务名，不带运行日）。"""
    return staging_dir_for_store(store_root, 'st_em_names') / 'em_state.json'


def em_failures_path(store_root: Path | str) -> Path:
    """东财曾用名抓取失败清单，与 ``em_state.json`` 同域（DG-7）。"""
    return staging_dir_for_store(store_root, 'st_em_names') / 'em_failures.csv'


def candidate_state_path(store_root: Path | str) -> Path:
    """候选标的公告抓取断点。按 DG-7 落 ``_staging/st_candidate_state/``（不带运行日）。"""
    return staging_dir_for_store(store_root, 'st_candidate_state') / 'candidate_state.json'


def candidate_failures_path(store_root: Path | str) -> Path:
    """候选标的公告抓取失败清单，与 ``candidate_state.json`` 同域（DG-7）。"""
    return staging_dir_for_store(store_root, 'st_candidate_state') / 'candidate_failures.csv'


def em_page_dir(store_root: Path | str) -> Path:
    """东财曾用名分页缓存。无运行期消费者（同批 parquet 已汇总），按 DG-32 落 ``_staging``。"""
    return staging_dir_for_store(store_root, 'st_em_pages_cache_20261112')


def em_raw_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'st' / 'raw' / 'em_org_basic.parquet'


def candidate_notice_dir(store_root: Path | str) -> Path:
    """候选标的公告缓存。无运行期消费者，按 DG-32 落 ``_staging``。"""
    return staging_dir_for_store(store_root, 'st_candidate_notices_cache_20261112')


def _write_atomic(frame: pl.DataFrame, path: Path) -> None:
    write_parquet_atomic(frame, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _parquet_entry(
    path: Path, root: Path, *, date_column: str | None = None,
    end_column: str | None = None,
) -> dict[str, Any]:
    """supplement 目录逐文件记账（DG-3：rows / bytes / sha256 / 列清单 / 日期区间）。"""
    frame = pl.read_parquet(path)
    entry: dict[str, Any] = {
        'path': str(path.relative_to(root)).replace('\\', '/'),
        'rows': frame.height,
        'cols': len(frame.columns),
        'columns': frame.columns,
        'bytes': path.stat().st_size,
        'sha256': _sha256(path),
    }
    if date_column is not None:
        entry['date_column'] = date_column
        entry['date_range'] = [
            str(frame[date_column].min()), str(frame[date_column].max())]
    if end_column is not None:
        entry['end_column'] = end_column
        entry['end_date_range'] = [
            str(frame[end_column].min()), str(frame[end_column].max())]
    return entry


def _write_text(path: Path, text: str) -> None:
    """文本/CSV/json 一律原子写（DG-9）；裸 ``write_text`` 会在半途失败时留下半截文件。"""
    write_text_atomic(text, path)


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    """DataFrame → CSV 也走原子写，避免 ``unresolved.csv`` 中断后只剩半张表。"""
    write_text_atomic(frame.write_csv(), path)


def _file_stamp(path: Path) -> datetime:
    """确定性时刻：证据文件的 mtime（截到秒）。

    ``fetched_at`` 用它而不是 ``datetime.now()``——否则同一批输入重跑会产出不同
    字节，manifest 里的 sha256 每次都不一样，等于放弃"重跑结果可对拍"。
    """
    return datetime.fromtimestamp(path.stat().st_mtime).replace(microsecond=0)


def _repo_root(store_root: Path) -> Path:
    """正式布局 ``<repo>/data/local_store`` → 仓库根；其它（测试临时 store）就地。"""
    return store_root.parent.parent \
        if store_root.parent.name.lower() == 'data' else store_root


def _snapshot_paths(store_root: Path, paths: list[Path], label: str) -> Path | None:
    """DG-11：覆盖正式文件前，把现有版本整体快照到仓库根 ``_backup/<label>_<日期>/``。

    用 ``copy2`` 复制而不是移动（同 ``local_store/build.py::_snapshot_quotes_dir``）：
    移动会让构建失败时正式目录缺位，而快照已经能提供完整回滚能力。目标已存在时
    追加序号，绝不覆盖既有快照；同内容重复快照同样另起序号，不做内容去重。
    """
    existing = [path for path in paths if path.is_file()]
    if not existing:
        return None
    base = _repo_root(store_root) / '_backup' / f'{label}_{date.today():%Y%m%d}'
    target = base
    seq = 1
    while target.exists():
        target = base.with_name(f'{label}_{date.today():%Y%m%d}_{seq}')
        seq += 1
    target.mkdir(parents=True, exist_ok=True)
    for path in existing:
        shutil.copy2(path, target / path.name)
    return target


def _csv_entry(path: Path, root: Path) -> dict[str, Any]:
    """CSV 逐文件记账（DG-3）：``unresolved.csv`` 也必须进 manifest 的 files 段。"""
    frame = pl.read_csv(path, infer_schema_length=0)
    return {
        'path': str(path.relative_to(root)).replace('\\', '/'),
        'rows': frame.height,
        'cols': len(frame.columns),
        'columns': frame.columns,
        'bytes': path.stat().st_size,
        'sha256': _sha256(path),
    }


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text.replace('/', '-'), fmt).date()
        except ValueError:
            continue
    return None


# ----------------------------------------------------------------------
# 名称证据
# ----------------------------------------------------------------------
def has_st_token(name: Any) -> bool:
    """简称或曾用名链里是否出现过 ST / *ST / PT（含 S*ST、GST、G*ST 等写法）。"""
    text = st_history.clean_name(name)
    if not text:
        return False
    for part in re.split(r'→|,|;|、', text):
        candidate = part[1:] if part.startswith('G') else part
        flag = st_history.flag_from_name(candidate)
        if flag in (FLAG_ST, FLAG_STAR) or candidate.startswith('PT'):
            return True
    return False


def classify_event_title(title: Any) -> list[tuple[str, str]]:
    """把公告标题解析成 ``[(flag, action)]``（较 :mod:`st_history` 放宽）。

    与 ``st_history.classify_title`` 的差异：过滤词只保留真正表示「未生效」的
    短语，不再因为标题含「提示」而丢掉「实施其他风险警示」的真事件。
    """
    text = st_history.clean_name(title)
    if not text:
        return []
    if any(marker in text for marker in _NOT_EFFECTIVE):
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
        actions.append((FLAG_ANY, 'lift'))
    actions.sort(key=lambda item: 0 if item[1] == 'lift' else 1)
    return actions


def _segments_from_events(rows: Iterable[dict[str, Any]]) -> list[tuple[date, str]]:
    """公告事件 → ``[(生效日, flag)]`` 状态段；空串表示明确非 ST。"""
    events: list[tuple[date, str, str]] = []
    for row in rows:
        publish = _as_date(row.get('publish_date'))
        if publish is None:
            continue
        for flag, action in classify_event_title(row.get('title')):
            events.append((st_history._event_date(str(row['title']), publish), flag, action))
    events.sort(key=lambda item: (item[0], 0 if item[2] == 'lift' else 1))
    segments: list[tuple[date, str]] = []
    for effective, flag, action in events:
        if action == 'impl':
            if segments and segments[-1][1] == flag:
                continue
            segments.append((effective, flag))
        elif segments and segments[-1][1] != FLAG_NONE \
                and (flag == FLAG_ANY or segments[-1][1] == flag):
            segments.append((effective, FLAG_NONE))
    return segments


def _intervals_from_segments(ts_code: str, segments: list[tuple[date, str]],
                             coverage_end: date, *, fetched_at: datetime,
                             quote_last: date | None = None) -> list[dict[str, Any]]:
    """事件段 → 区间行。

    ``quote_last`` 是本轮行情末日（默认等于 ``coverage_end`` 的上界）：退市标的
    ``coverage_end < quote_last`` 时用它判「行情已终止」，不再读 ``date.today()``，
    否则同一批输入隔天重跑会产出不同区间。
    """
    rows: list[dict[str, Any]] = []
    for index, (start, flag) in enumerate(segments):
        if flag not in (FLAG_ST, FLAG_STAR):
            continue
        nxt = segments[index + 1][0] if index + 1 < len(segments) else None
        end = nxt - timedelta(days=1) if nxt else None
        if end is None and (quote_last or coverage_end) > coverage_end:
            # 仍开放但行情已终止（退市）：区间止于最后一个可交易日的状态可知边界
            end = coverage_end
        if end is not None and end > coverage_end:
            end = coverage_end
        rows.append({
            'ts_code': ts_code, 'start_date': start, 'end_date': end, 'flag': flag,
            'source': SOURCE_CNINFO_DATED, 'precision': st_history.PRECISION_ANNOUNCEMENT,
            'fetched_at': fetched_at,
        })
    return rows


# ----------------------------------------------------------------------
# 东方财富名称登记册抓取（分页缓存 + 断点续跑）
# ----------------------------------------------------------------------
def _fetch_em_page(session: requests.Session, page: int, size: int) -> dict[str, Any] | None:
    params: dict[str, str | int] = {
        'reportName': EM_REPORT, 'columns': EM_COLUMNS,
        'pageNumber': page, 'pageSize': size, 'source': 'HSF10', 'client': 'PC',
    }
    delay = 1.0
    last: str = ''
    for attempt in range(EM_RETRY + 1):
        try:
            resp = session.get(EM_URL, params=params, headers=EM_HEADERS, timeout=30)
            payload = resp.json()
            if payload.get('success'):
                return dict(payload['result'])
            last = str(payload.get('message') or 'success=false')
        except (requests.RequestException, ValueError) as exc:
            last = f'{type(exc).__name__}: {exc}'
        if attempt >= EM_RETRY:
            break
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f'东方财富第 {page} 页抓取失败: {last}')


def _rows_to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    parsed = [{
        'secucode': str(item.get('SECUCODE') or '').strip().upper(),
        'name': str(item.get('SECURITY_NAME_ABBR') or '').strip(),
        'former_names': str(item.get('FORMERNAME') or '').strip(),
        'org_name': str(item.get('ORG_NAME') or '').strip(),
        'listing_date': _as_date(item.get('LISTING_DATE')),
    } for item in rows if item.get('SECUCODE')]
    return pl.DataFrame(parsed, schema=_EM_SCHEMA)


def sync_em_names(store_root: Path | str, *, force: bool = False,
                  max_pages: int | None = None) -> dict[str, Any]:
    """抓全市场曾用名登记册；按页落盘，重跑只补缺页。"""
    root = Path(store_root)
    pdir = em_page_dir(root)
    pdir.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {}
    if em_state_path(root).is_file() and not force:
        state = json.loads(em_state_path(root).read_text(encoding='utf-8'))
    if force:
        state = {}

    session = requests.Session()
    first = _fetch_em_page(session, 1, EM_PAGE_SIZE)
    pages = int((first or {}).get('pages') or 0)
    if pages <= 0:
        raise RuntimeError('东方财富未返回分页信息')
    target = min(pages, max_pages) if max_pages else pages
    done: set[int] = set(state.get('completed_pages') or [])
    failures: list[dict[str, Any]] = []
    written = 0
    for page in range(1, target + 1):
        path = pdir / f'page_{page:04d}.parquet'
        if page in done and path.is_file() and not force:
            continue
        try:
            result = first if page == 1 else _fetch_em_page(session, page, EM_PAGE_SIZE)
            frame = _rows_to_frame((result or {}).get('data') or [])
            _write_atomic(frame, path)
            done.add(page)
            written += 1
        except RuntimeError as exc:
            failures.append({'page': page, 'error': str(exc)[:200]})
            logger.warning(f'[st-names] 第 {page} 页失败: {exc}')
        if written and written % 10 == 0:
            logger.info(f'[st-names] 已写 {written} 页 / 目标 {target}')
        time.sleep(0.2)
    state.update({
        'report': EM_REPORT, 'page_size': EM_PAGE_SIZE, 'pages': pages,
        'completed_pages': sorted(done), 'updated_at': datetime.now().isoformat(
            timespec='seconds'), 'failures': failures,
    })
    _write_text(em_state_path(root), json.dumps(state, ensure_ascii=False, indent=2))

    frames = [pl.read_parquet(pdir / f'page_{page:04d}.parquet')
              for page in sorted(done) if (pdir / f'page_{page:04d}.parquet').is_file()]
    combined = pl.concat(frames, how='vertical_relaxed') if frames else pl.DataFrame(
        schema=_EM_SCHEMA)
    combined = combined.filter(pl.col('secucode') != '').unique(
        subset=['secucode'], keep='last').sort('secucode')
    _write_atomic(combined, em_raw_path(root))
    failure_frame = pl.DataFrame(
        failures, schema={'page': pl.Int64, 'error': pl.Utf8}) if failures \
        else pl.DataFrame(schema={'page': pl.Int64, 'error': pl.Utf8})
    _write_csv(failure_frame, em_failures_path(root))
    return {'pages': pages, 'completed': len(done), 'written': written,
            'rows': combined.height, 'failures': failures,
            'complete': len(done) >= pages}


# ----------------------------------------------------------------------
# 候选标的公告补抓（按标的缓存）
# ----------------------------------------------------------------------
def _load_em(root: Path) -> pl.DataFrame | None:
    path = em_raw_path(root)
    if not path.is_file():
        return None
    return pl.read_parquet(path)


def _quote_window(store_root: Path, code: str) -> tuple[date, date] | None:
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    cov = LocalQuoteStore(store_root).coverage('stock', code)
    if cov is None:
        return None
    return cov.start, cov.end


def official_coverage(coverage: pl.DataFrame) -> pl.DataFrame:
    """剔除本模块上次写入的补充覆盖，只留下官方/快照证据。

    ``apply`` 之后的规范覆盖表会包含 supplement 行；若直接用它当「已覆盖」
    基准，重跑 ``build_supplement`` 只会补增量并覆盖掉 supplement 表，
    覆盖规模会逐次收缩。
    """
    if coverage.height == 0 or 'source' not in coverage.columns:
        return coverage
    return coverage.filter(~pl.col('source').is_in(sorted(SUPPLEMENT_SOURCES)))


def _candidate_codes(root: Path, em: pl.DataFrame,
                     coverage: pl.DataFrame) -> list[dict[str, Any]]:
    """池内无官方覆盖、且名称历史含 ST/PT 的标的（需要定日证据）。"""
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    pool = LocalQuoteStore(root).codes('stock')
    covered = set(official_coverage(coverage)['ts_code'].to_list())
    st_names = {row['secucode']: row for row in em.to_dicts()
                if has_st_token(row['name']) or has_st_token(row['former_names'])}
    return [{'ts_code': code, **st_names[code]} for code in pool
            if code not in covered and code in st_names]


def sync_candidates(store_root: Path | str, *, force: bool = False,
                    em: pl.DataFrame | None = None) -> dict[str, Any]:
    """对「名称有 ST 但本地无区间」的标的抓个股公告（逐个缓存，可续跑）。"""
    root = Path(store_root)
    if em is None:
        em = _load_em(root)
    coverage = st_history.load_coverage(root)
    if em is None or coverage is None:
        raise RuntimeError('缺少曾用名或覆盖表，先执行 sync-em / st_history build')
    candidates = _candidate_codes(root, em, coverage)
    ndir = candidate_notice_dir(root)
    ndir.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {}
    state_file = candidate_state_path(root)
    if state_file.is_file() and not force:
        state = json.loads(state_file.read_text(encoding='utf-8'))

    archive = Path('logs/st_history') / f'{datetime.now():%Y%m%d_%H%M%S}_candidates'
    archive.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    org_map = st_history._org_id_map(root, archive, session)
    failures: list[dict[str, Any]] = []
    fetched = 0
    for item in candidates:
        code = item['ts_code']
        path = ndir / f'{code}.parquet'
        if path.is_file() and not force:
            continue
        org_id = org_map.get(code[:6])
        if not org_id:
            failures.append({'ts_code': code, 'stage': 'org_id', 'window_start': None,
                             'window_end': None, 'error': 'no_org_id', 'retries': 0})
            continue
        listing = _as_date(item.get('listing_date'))
        if listing is None:
            failures.append({'ts_code': code, 'stage': 'listing', 'window_start': None,
                             'window_end': None, 'error': 'no_listing_date',
                             'retries': 0})
            continue
        rows: list[dict[str, Any]] = []
        error: str | None = None
        window_start = listing
        failed_window: tuple[date, date] | None = None
        while window_start <= date.today():
            window_end = min(date(window_start.year + 2, 12, 31), date.today())
            try:
                part = st_history._fetch_stock_notices(
                    session, code[:6], org_id, window_start, window_end)
            except (RuntimeError, requests.RequestException) as exc:
                error = f'{type(exc).__name__}: {exc}'[:200]
                failed_window = (window_start, window_end)
                break
            rows.extend(part)
            window_start = window_end + timedelta(days=1)
        if error is not None:
            failures.append({'ts_code': code, 'stage': 'notice',
                             'window_start': failed_window[0] if failed_window else None,
                             'window_end': failed_window[1] if failed_window else None,
                             'error': error, 'retries': 0})
            continue
        unique = {row['notice_id']: row for row in rows}
        frame = pl.DataFrame([
            {'notice_id': str(row['notice_id']), 'code': code,
             'title': str(row['title']), 'publish_date': _as_date(row['publish_date'])}
            for row in unique.values()], schema=_NOTICE_SCHEMA)
        _write_atomic(frame.sort('publish_date'), path)
        state[code] = {'rows': frame.height,
                       'fetched_at': datetime.now().isoformat(timespec='seconds')}
        fetched += 1
        logger.info(f'[st-names] {code} 公告 {frame.height} 条')
    _write_text(state_file, json.dumps(state, ensure_ascii=False, indent=2))
    failure_frame = pl.DataFrame(
        failures, schema={'ts_code': pl.Utf8, 'stage': pl.Utf8,
                          'window_start': pl.Date, 'window_end': pl.Date,
                          'error': pl.Utf8, 'retries': pl.Int64}) if failures \
        else pl.DataFrame(schema={'ts_code': pl.Utf8, 'stage': pl.Utf8,
                                  'window_start': pl.Date, 'window_end': pl.Date,
                                  'error': pl.Utf8, 'retries': pl.Int64})
    _write_csv(failure_frame, candidate_failures_path(root))
    return {'candidates': len(candidates), 'fetched': fetched,
            'cached': len(list(ndir.glob('*.parquet'))), 'failures': failures}


# ----------------------------------------------------------------------
# 覆盖计划（纯函数，便于单测）
# ----------------------------------------------------------------------
@dataclass
class SupplementPlan:
    intervals: list[dict[str, Any]] = field(default_factory=list)
    coverage: list[dict[str, Any]] = field(default_factory=list)
    unresolved: list[dict[str, Any]] = field(default_factory=list)
    clean: int = 0
    dated: int = 0
    skipped: int = 0


def plan_supplement(em: pl.DataFrame, coverage: pl.DataFrame,
                    pool: Iterable[str], quote_windows: dict[str, tuple[date, date]],
                    notices: dict[str, list[dict[str, Any]]], *,
                    today: date | None = None,
                    snapshot_st: set[str] | None = None,
                    em_stamp: datetime | None = None,
                    notice_stamps: dict[str, datetime] | None = None,
                    quote_last: date | None = None) -> SupplementPlan:
    """按证据生成补充覆盖；无证据一律不写（保持 unknown）。

    ``em_stamp`` / ``notice_stamps`` 是**证据文件时刻**，写进 ``fetched_at``；缺省时退到
    ``today`` 零点，保证纯函数在无外部输入时也能确定性产出。
    """
    today = today or date.today()
    snapshot_st = snapshot_st or set()
    notice_stamps = notice_stamps or {}
    fallback_stamp = datetime.combine(today, datetime.min.time())
    registry_stamp = em_stamp or fallback_stamp
    covered = set(coverage['ts_code'].to_list())
    plan = SupplementPlan()
    for code in pool:
        if code in covered:
            continue
        window = quote_windows.get(code)
        if window is None:
            plan.unresolved.append({'ts_code': code, 'reason': 'no_quote_window',
                                    'former_names': '', 'evidence': ''})
            continue
        start, end = max(window[0], date(1990, 1, 1)), min(window[1], today)
        row = em.filter(pl.col('secucode') == code)
        if row.height == 0:
            plan.unresolved.append({'ts_code': code, 'reason': 'not_in_name_registry',
                                    'former_names': '', 'evidence': ''})
            continue
        info = row.to_dicts()[0]
        former = str(info.get('former_names') or '')
        name = str(info.get('name') or '')
        # 当前快照已判 ST 的标的不能用「名称历史干净」做负证据（登记册可能缺历史）
        if not has_st_token(name) and not has_st_token(former) \
                and code not in snapshot_st:
            plan.coverage.append({
                'ts_code': code, 'coverage_start': start, 'coverage_end': end,
                'source': SOURCE_EM_CLEAN, 'fetched_at': registry_stamp,
            })
            plan.clean += 1
            continue
        segments = _segments_from_events(notices.get(code, []))
        if not segments:
            plan.unresolved.append({
                'ts_code': code, 'reason': 'st_name_without_dated_event',
                'former_names': former, 'evidence': name})
            plan.skipped += 1
            continue
        plan.intervals.extend(_intervals_from_segments(
            code, segments, end,
            fetched_at=notice_stamps.get(code, registry_stamp),
            quote_last=quote_last))
        plan.coverage.append({
            'ts_code': code, 'coverage_start': segments[0][0], 'coverage_end': end,
            'source': SOURCE_CNINFO_DATED,
            'fetched_at': notice_stamps.get(code, registry_stamp),
        })
        plan.dated += 1
    return plan


def build_supplement(store_root: Path | str, *,
                     today: date | None = None) -> dict[str, Any]:
    """读取缓存 → 生成 supplement 表与未决清单（不写规范表）。

    ``today`` 缺省取**行情末日**（而不是 ``date.today()``）：覆盖窗口的上界本来就
    是 ``min(个股行情末日, 构建日)``，取行情末日既让窗口推进到最新可证日期，也让
    隔天重跑得到同一批字节。写盘顺序与失败残留见模块 docstring「写盘事务」。
    """
    root = Path(store_root)
    em = _load_em(root)
    coverage = st_history.load_coverage(root)
    if em is None:
        raise RuntimeError('缺少东方财富曾用名缓存，先执行 sync-em')
    if coverage is None:
        raise RuntimeError('缺少 ST 覆盖表，先执行 st_history build')
    # 「已覆盖」只认官方/快照证据：apply 后规范表含上次 supplement 行，
    # 直接复用会让本次重算只剩增量，把 supplement 表覆盖成残缺版本。
    coverage = official_coverage(coverage)
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    store = LocalQuoteStore(root)
    pool = store.codes('stock')
    quote_windows = {code: (cov.start, cov.end) for code in pool
                     if (cov := store.coverage('stock', code)) is not None}
    if not quote_windows:
        raise RuntimeError('行情池为空，无法确定覆盖窗口')
    quote_last = max(end for _, end in quote_windows.values())
    cutoff = today or quote_last
    notice_files = {path.stem: path
                    for path in candidate_notice_dir(root).glob('*.parquet')}
    notices = {code: pl.read_parquet(path).to_dicts()
               for code, path in notice_files.items()}
    em_stamp = _file_stamp(em_raw_path(root))
    notice_stamps = {code: _file_stamp(path) for code, path in notice_files.items()}
    snapshot, _ = st_history._latest_snapshot(root)
    snapshot_st = set(snapshot.filter(pl.col('status') == st_history.STATUS_ST)['code']
                      .to_list()) if snapshot is not None else set()
    plan = plan_supplement(em, coverage, pool, quote_windows, notices,
                           today=cutoff, snapshot_st=snapshot_st,
                           em_stamp=em_stamp, notice_stamps=notice_stamps,
                           quote_last=quote_last)

    interval_frame = pl.DataFrame(plan.intervals, schema=_INTERVAL_SCHEMA) \
        if plan.intervals else pl.DataFrame(schema=_INTERVAL_SCHEMA)
    coverage_frame = pl.DataFrame(plan.coverage, schema=_COVERAGE_SCHEMA) \
        if plan.coverage else pl.DataFrame(schema=_COVERAGE_SCHEMA)
    unresolved = pl.DataFrame(plan.unresolved) if plan.unresolved else pl.DataFrame(
        schema={'ts_code': pl.Utf8, 'reason': pl.Utf8, 'former_names': pl.Utf8,
                'evidence': pl.Utf8})

    # 写盘事务（DG-10）：快照 → 区间表 → 覆盖表 → csv → manifest（最后写，作提交标记）
    snapshot_dir = _snapshot_paths(
        root, [supplement_intervals_path(root), supplement_coverage_path(root),
               unresolved_path(root), manifest_path(root)], 'st_supplement')
    if snapshot_dir is not None:
        logger.info(f'[st-names] 覆盖前已快照旧版 supplement：{snapshot_dir}')
    _write_atomic(interval_frame, supplement_intervals_path(root))
    _write_atomic(coverage_frame, supplement_coverage_path(root))
    _write_csv(unresolved, unresolved_path(root))

    run_at = max([em_stamp, *notice_stamps.values()])
    summary = {
        'built_at': run_at.isoformat(timespec='seconds'),
        'built_at_basis': '上游证据文件 mtime 最大值（确定性，非运行时刻）',
        'quote_last_date': quote_last.isoformat(),
        'quote_codes': len(quote_windows),
        'coverage_cutoff': cutoff.isoformat(),
        'em_rows': em.height,
        'pool': len(pool),
        'uncovered_before': len([c for c in pool if c not in set(coverage['ts_code'])]),
        'clean_negative_coverage': plan.clean,
        'dated_candidate_coverage': plan.dated,
        'unresolved': len(plan.unresolved),
        'unresolved_by_reason': _count_reason(plan.unresolved),
        'supplement_interval_rows': interval_frame.height,
        'supplement_coverage_rows': coverage_frame.height,
        'files': [
            _parquet_entry(supplement_intervals_path(root), root,
                           date_column='start_date', end_column='end_date'),
            _parquet_entry(supplement_coverage_path(root), root,
                           date_column='coverage_start', end_column='coverage_end'),
            _csv_entry(unresolved_path(root), root),
        ],
    }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    apply_semantics(summary, 'meta/st/supplement/manifest.json')
    _write_text(manifest_path(root), json.dumps(summary, ensure_ascii=False, indent=2))
    logger.info(f'[st-names] supplement 已生成: {supplement_coverage_path(root)}'
                f'（覆盖 +{plan.clean + plan.dated} / 未决 {summary["unresolved"]} / '
                f'行情末日 {quote_last}）')
    return summary


def _count_reason(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        key = str(row['reason'])
        out[key] = out.get(key, 0) + 1
    return out


# ----------------------------------------------------------------------
# 规范化：把 supplement 并入 st_history 规范表
# ----------------------------------------------------------------------
def apply_supplement(store_root: Path | str) -> dict[str, Any]:
    """重建 st_history 规范表（build_store 会自动并入 supplement）。

    DG-11：``build_store`` 会整表重写 ``meta/st/st_intervals.parquet``、
    ``meta/st/st_coverage.parquet``（另含 ``coverage.json``），覆盖前先把这三件
    快照到仓库根 ``_backup/st_build_store_<日期>/``，失败可直接 copy2 回滚。
    """
    root = Path(store_root)
    snapshot_dir = _snapshot_paths(
        root, [st_history.intervals_path(root), st_history.coverage_path(root),
               st_history.coverage_json_path(root)], 'st_build_store')
    if snapshot_dir is not None:
        logger.info(f'[st-names] apply 前已快照旧版区间/覆盖表：{snapshot_dir}')
    summary = st_history.build_store(root)
    logger.info(f'[st-names] 规范表已重建: {summary}')
    return summary


# ----------------------------------------------------------------------
# 验证
# ----------------------------------------------------------------------
def _raw_event_dates(root: Path, code: str) -> list[date]:
    """从原始返回提取该标的的 ST 事件日（深市官方表 / 巨潮公告）。"""
    out: list[date] = []
    rdir = st_history.raw_dir(root)
    sz_path = rdir / 'sz_change_name.parquet'
    if sz_path.is_file() and code.endswith('.SZ'):
        sz = pl.read_parquet(sz_path)
        for row in sz.filter(pl.col('code') == code).to_dicts():
            flag = st_history.flag_from_name(row['name_new'])
            prev = st_history.flag_from_name(row['name_prev'])
            if flag in (FLAG_ST, FLAG_STAR) or prev in (FLAG_ST, FLAG_STAR):
                out.append(row['change_date'])
    code_notices = candidate_notice_dir(root) / f'{code}.parquet'
    if code_notices.is_file():
        for row in pl.read_parquet(code_notices).to_dicts():
            if classify_event_title(row['title']):
                out.append(st_history._event_date(str(row['title']),
                                                  row['publish_date']))
    if not out:
        np_path = rdir / 'cninfo_notices.parquet'
        if np_path.is_file():
            notices = pl.read_parquet(np_path)
            for row in notices.filter(pl.col('sec_code') == code[:6]).to_dicts():
                if classify_event_title(row['title']) and row['publish_date']:
                    out.append(st_history._event_date(str(row['title']),
                                                      row['publish_date']))
    return sorted(set(out))


def _spot_check(root: Path, intervals: pl.DataFrame, sample: int = 5) -> dict[str, Any]:
    """库内区间边界 vs 源端事件日：±3 天内视为一致。"""
    codes = sorted(intervals['ts_code'].unique().to_list())
    checked = matched = 0
    details: list[dict[str, Any]] = []
    for code in codes[:sample]:
        rows = intervals.filter(pl.col('ts_code') == code).sort('start_date').to_dicts()
        events = _raw_event_dates(root, code)
        for row in rows:
            for label, day in (('start', row['start_date']),
                               ('end', row['end_date'] + timedelta(days=1)
                                if row['end_date'] else None)):
                if day is None:
                    continue
                checked += 1
                ok = any(abs((day - ev).days) <= 3 for ev in events)
                matched += int(ok)
                details.append({'ts_code': code, 'flag': row['flag'], 'bound': label,
                                'date': day.isoformat(), 'matched': ok,
                                'nearest_events': [ev.isoformat() for ev in events[:6]]})
    return {'codes': codes[:sample], 'checked': checked, 'matched': matched,
            'rate': matched / checked if checked else 0.0, 'details': details}


def verify(store_root: Path | str, *, output_dir: Path | str =
           Path('logs/st_name_history/verify')) -> dict[str, Any]:
    """覆盖对比 + 召回率 + 点时抽样 + 语义不变性检查。"""
    root = Path(store_root)
    intervals = st_history.load_intervals(root)
    coverage = st_history.load_coverage(root)
    if intervals is None or coverage is None:
        raise RuntimeError('规范表未生成')
    em = _load_em(root)
    if em is None:
        raise RuntimeError('缺少曾用名缓存')
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    pool = set(LocalQuoteStore(root).codes('stock'))
    covered = set(coverage['ts_code'].to_list())
    em_st = {row['secucode'] for row in em.to_dicts()
             if has_st_token(row['name']) or has_st_token(row['former_names'])}
    local_st = set(intervals['ts_code'].to_list())
    recall_hits = local_st & em_st
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        'pool': len(pool), 'covered': len(pool & covered),
        'uncovered': len(pool - covered),
        'st_codes': len(local_st),
        'name_registry_recall': {
            'denominator': len(local_st), 'numerator': len(recall_hits),
            'rate': len(recall_hits) / len(local_st) if local_st else 0.0,
            'missed': sorted(local_st - em_st)[:20],
        },
        'spot_check': _spot_check(root, intervals),
    }
    # 点时抽样：supplement 产生的区间，生效日前一天不应判 st
    supp = pl.read_parquet(supplement_intervals_path(root)) \
        if supplement_intervals_path(root).is_file() else None
    pit_violations: list[dict[str, Any]] = []
    pit_checked = 0
    if supp is not None:
        for row in supp.to_dicts():
            before = st_history.status_at(root, row['ts_code'],
                                          row['start_date'] - timedelta(days=1))
            on = st_history.status_at(root, row['ts_code'], row['start_date'])
            pit_checked += 2
            if before.status == st_history.STATUS_ST or on.status != st_history.STATUS_ST:
                pit_violations.append({'ts_code': row['ts_code'],
                                       'start': row['start_date'].isoformat(),
                                       'before': before.status, 'on': on.status})
    result['point_in_time'] = {'checked': pit_checked, 'violations': pit_violations}
    report = out / 'report.json'
    _write_text(report, json.dumps(result, ensure_ascii=False, default=str, indent=2))
    return result


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _resolve_root(args: argparse.Namespace) -> Path:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    return Path(args.store_root) if args.store_root else resolve_store_root(get_config())


def main() -> int:
    parser = argparse.ArgumentParser(description='A 股曾用名全历史补充 ST 覆盖')
    parser.add_argument('--store-root', default=None)
    sub = parser.add_subparsers(dest='command', required=True)
    sync_em = sub.add_parser('sync-em', help='抓东方财富曾用名登记册（分页续跑）')
    sync_em.add_argument('--force', action='store_true')
    sync_em.add_argument('--max-pages', type=int, default=None)
    sub.add_parser('sync-candidates', help='抓候选标的个股公告（按标的续跑）')
    sub.add_parser('build', help='生成 supplement 表与未决清单')
    sub.add_parser('apply', help='build + 重建 st_history 规范表')
    verify_cmd = sub.add_parser('verify', help='覆盖对比 / 召回率 / 点时抽查')
    verify_cmd.add_argument('--output-dir', default='logs/st_name_history/verify')
    args = parser.parse_args()
    root = _resolve_root(args)
    if args.command == 'sync-em':
        logger.info(f'[st-names] sync-em: {sync_em_names(root, force=args.force, max_pages=args.max_pages)}')
        return 0
    if args.command == 'sync-candidates':
        logger.info(f'[st-names] sync-candidates: {sync_candidates(root)}')
        return 0
    if args.command == 'build':
        logger.info(f'[st-names] build: {build_supplement(root)}')
        return 0
    if args.command == 'apply':
        build_supplement(root)
        logger.info(f'[st-names] apply: {apply_supplement(root)}')
        return 0
    logger.info(f'[st-names] verify: {verify(root, output_dir=args.output_dir)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
