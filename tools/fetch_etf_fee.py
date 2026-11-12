#!/usr/bin/env python3

"""抓取多资产 ETF 候选池的管理费 / 托管费 / 销售服务费，并回溯费率变更分段。

取数口径（单一来源串行 + 节流，遇限速即停）：

1. 当前费率：天天基金 F10 两个页面互为交叉核对
   - ``https://fundf10.eastmoney.com/jbgk_<code>.html``（基本概况：管理费/托管费/销售服务费）
   - ``https://fundf10.eastmoney.com/jjfl_<code>.html``（基金费率：管理费/托管费）
   销售服务费只在 jbgk 出现，缺失记 null，不填 0。
2. 费率历史（仅 core）：基金公告列表 ``api.fund.eastmoney.com/f10/JJGG``
   - 取「基金产品资料概要更新」各版本 PDF，逐版解析管理费/托管费；
   - 同一只的相邻版本费率不同即视为一次变更，生效日优先取变更前的
     「基金合同 / 托管协议」公告日，取不到则退回产品资料概要发布日并标注精度。
3. 全部原始响应落 ``_staging/etf_fee_20261110/raw/``，进度写 ``progress_*.csv``，
   失败写 ``failed.csv``，中断后按进度续跑，不从零重来。

本脚本只做取数与整理，不修改任何既有正式数据；``--apply`` 才原子写
``data/local_store/meta/multi_asset/``，并按 DG-11 先把旧目录移入 ``_backup``。
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
STAGING = ROOT / '_staging' / 'etf_fee_20261110'
RAW = STAGING / 'raw'
TARGET = STORE / 'meta' / 'multi_asset'

POOL = STORE / 'meta' / 'multi_asset' / 'etf_pool.parquet'

THROTTLE = 1.2  # 秒，单连接串行（`--throttle` 可覆盖）
TIMEOUT = 25
MAX_RETRY = 2
ABORT_AFTER = 5  # 连续失败 / 连续空返回即停压

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
HEADERS = {'User-Agent': UA, 'Referer': 'https://fundf10.eastmoney.com/'}

HISTORY_WINDOW_START = date(2023, 7, 3)   # 2023-07 降费轮次后的起点
HISTORY_WINDOW_END = date(2026, 11, 9)    # 行情末行
HISTORY_SCAN_SINCE = date(2021, 6, 1)     # 公告列表只取这之后的版本
REPARSE_TAG = 'v4'                        # 解析口径版本号，改口径时递增


class StopCrawl(RuntimeError):
    """限速 / 封禁 / 连续空返回，必须停压。"""


_session = requests.Session()
_session.headers.update(HEADERS)
_counter = {'n': 0, 'fail_streak': 0, 'empty_streak': 0}


def _get(url: str, *, binary: bool = False):
    last_err = None
    for attempt in range(MAX_RETRY + 1):
        time.sleep(THROTTLE)
        try:
            resp = _session.get(url, timeout=TIMEOUT)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            continue
        _counter['n'] += 1
        if resp.status_code in (403, 429):
            raise StopCrawl(f'HTTP {resp.status_code} {url}')
        if resp.status_code != 200:
            last_err = f'HTTP {resp.status_code}'
            continue
        if binary:
            if not resp.content:
                last_err = 'empty body'
                continue
            return resp.content
        resp.encoding = resp.apparent_encoding or 'utf-8'
        if not resp.text.strip():
            last_err = 'empty body'
            continue
        return resp.text
    _counter['fail_streak'] += 1
    if last_err == 'empty body':
        _counter['empty_streak'] += 1
        if _counter['empty_streak'] >= ABORT_AFTER:
            raise StopCrawl(f'连续 {_counter["empty_streak"]} 次空返回，停压')
    if _counter['fail_streak'] >= ABORT_AFTER:
        raise StopCrawl(f'连续 {_counter["fail_streak"]} 次失败，最后错误 {last_err}')
    raise RuntimeError(f'{url} 取数失败: {last_err}')


def _text(html: str) -> str:
    txt = re.sub(r'<[^>]+>', '|', html)
    txt = txt.replace('&nbsp;', ' ')
    return re.sub(r'[ \t\r\n]+', ' ', txt)


def _pct(value: str | None) -> float | None:
    if not value:
        return None
    v = value.strip()
    if v.startswith('---') or set(v) <= {'-', '—'}:
        return None
    try:
        return float(v.rstrip('%'))
    except ValueError:
        return None


def parse_jbgk(html: str) -> dict[str, float | None]:
    txt = _text(html)
    out = {}
    for label, key in [('管理费率', 'mgmt'), ('托管费率', 'cust'), ('销售服务费率', 'sales')]:
        m = re.search(label + r'\s*\|*\s*([0-9]+(?:\.[0-9]+)?%|---+|—+)', txt)
        out[key] = _pct(m.group(1)) if m else None
    return out


def parse_jjfl(html: str) -> dict[str, float | None]:
    txt = _text(html)
    out = {}
    for label, key in [('管理费率', 'mgmt'), ('托管费率', 'cust')]:
        m = re.search(label + r'\s*\|*\s*([0-9]+(?:\.[0-9]+)?%|---+|—+)', txt)
        out[key] = _pct(m.group(1)) if m else None
    return out


_FEE_LABELS = [('mgmt', '管理费'), ('cust', '托管费'), ('sales', '销售服务费')]
_FEE_ROW = (r'\s*(?:年费率|固定费率|固定比例|费率|率)?\s*[：:＝=]?\s*[-—–]?\s*'
            r'([0-9]+(?:\.[0-9]+)?)\s*%')


def _fee_in(region: str, label: str) -> float | None:
    m = re.search(label + _FEE_ROW, region)
    return float(m.group(1)) if m else None


def parse_pds(pdf_bytes: bytes) -> dict[str, float | None]:
    """解析产品资料概要的费率表。

    PDS 里费率写法有 ``管理费 0.50%`` / ``管理费 年费率 0.15%`` /
    ``管理费 固定费率0.50%`` / ``管理费 - 0.15%`` 四种；另有备注句
    ``管理费率由0.50%调低至0.15%`` 会先于表格出现，所以优先只扫
    ``收费方式`` 表头之后 800 字，扫不到再退回全文。
    """
    import pdfplumber
    out: dict[str, float | None] = {}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        txt = '\n'.join((p.extract_text() or '') for p in pdf.pages)
    starts = [m.start() for m in re.finditer('收费方式', txt)]
    regions = [txt[s:s + 800] for s in starts]
    # 运作费率表的表头是「收费方式/年费率」，优先；其次其余费率表
    regions = [r for r in regions if '年费率' in r[:40]] + regions
    for key, label in _FEE_LABELS:
        val = None
        for region in regions:
            val = _fee_in(region, label)
            if val is not None:
                break
        out[key] = val if val is not None else _fee_in(txt, label)
    return out


def stage_reparse(limit: int | None = None) -> None:
    """PDF 已缓存时重解析全部版本费率（口径修正后无需重新下载）。

    逐只落盘 + ``reparsed`` 标记，中断后从断点续跑；改解析口径时递增
    ``reparse_tag`` 即可触发全量重解析。
    """
    from src.data.atomic_write import write_text_atomic
    tag = REPARSE_TAG
    path = STAGING / 'progress_history.csv'
    df = _read_csv(path).unique(subset=['ts_code'], keep='last')
    if 'reparsed' not in df.columns:
        df = df.with_columns(pl.lit('').alias('reparsed'))
    else:
        df = df.with_columns(pl.col('reparsed').fill_null(''))
    todo = df.filter(pl.col('reparsed') != tag)['ts_code'].to_list()
    if limit:
        todo = todo[:limit]
    print(f'reparse: {df.height} funds, todo {len(todo)}', flush=True)
    rows = df.to_dicts()
    changed = 0
    anomalies = []
    for n, r in enumerate(rows, 1):
        if r['ts_code'] not in todo:
            continue
        versions = json.loads(r['versions']) if r['versions'] else []
        for v in versions:
            pdf = RAW / f"{r['ts_code']}_{v['id']}.pdf"
            if not pdf.exists():
                continue
            new = parse_pds(pdf.read_bytes())
            if any(new.get(k) != v.get(k) for k in ('mgmt', 'cust', 'sales')):
                changed += 1
            v.update(new)
            if v.get('mgmt') is not None and v.get('cust') is not None \
                    and v['mgmt'] <= v['cust']:
                anomalies.append((r['ts_code'], v['date'], v['mgmt'], v['cust']))
        r['versions'] = json.dumps(versions, ensure_ascii=False)
        r['reparsed'] = tag
        write_text_atomic(pl.DataFrame(rows).write_csv(), path)
        if n % 10 == 0:
            print(f'  reparse {n}/{df.height} last={r["ts_code"]} changed={changed}',
                  flush=True)
    print(f'reparse done: {changed} versions changed, {len(anomalies)} anomalies',
          flush=True)
    for a in anomalies[:40]:
        print('  anomaly', a)


def _read_csv(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    try:
        return pl.read_csv(path, infer_schema_length=0)
    except Exception:  # noqa: BLE001
        return pl.DataFrame()


def _append_csv(path: Path, row: dict) -> None:
    df = pl.DataFrame([{k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
                        for k, v in row.items()}])
    header = not path.exists()
    with open(path, 'a', encoding='utf-8', newline='') as fh:
        df.write_csv(fh, include_header=header)


def stage_current(limit: int | None = None) -> None:
    pool = pl.read_parquet(POOL).filter(pl.col('in_scope'))
    done_df = _read_csv(STAGING / 'progress_current.csv')
    done = set(done_df.filter(pl.col('status') == 'ok')['ts_code'].to_list()) \
        if done_df.height else set()
    rows = pool.sort(['tier', 'ts_code']).to_dicts()
    todo = [r for r in rows if r['ts_code'] not in done]
    if limit:
        todo = todo[:limit]
    print(f'current: total {len(rows)}, done {len(done)}, todo {len(todo)}')
    for i, row in enumerate(todo, 1):
        code = row['ts_code']
        bare = code.split('.')[0]
        rec = {'ts_code': code, 'tier': row['tier'], 'status': '', 'jbgk_ok': False,
               'jjfl_ok': False, 'mgmt_a': None, 'cust_a': None, 'sales_a': None,
               'mgmt_b': None, 'cust_b': None}
        try:
            html_a = _get(f'https://fundf10.eastmoney.com/jbgk_{bare}.html')
            (RAW / f'{code}_jbgk.html').write_text(html_a, encoding='utf-8')
            a = parse_jbgk(html_a)
            rec.update(jbgk_ok=True, mgmt_a=a['mgmt'], cust_a=a['cust'], sales_a=a['sales'])
            html_b = _get(f'https://fundf10.eastmoney.com/jjfl_{bare}.html')
            (RAW / f'{code}_jjfl.html').write_text(html_b, encoding='utf-8')
            b = parse_jjfl(html_b)
            rec.update(jjfl_ok=True, mgmt_b=b['mgmt'], cust_b=b['cust'])
            rec['status'] = 'ok'
        except StopCrawl:
            _append_csv(STAGING / 'failed.csv', {'ts_code': code, 'stage': 'current',
                                                 'error': 'stop_crawl'})
            raise
        except Exception as exc:  # noqa: BLE001
            rec['status'] = 'error'
            _append_csv(STAGING / 'failed.csv', {'ts_code': code, 'stage': 'current',
                                                 'error': str(exc)[:200]})
        _append_csv(STAGING / 'progress_current.csv', rec)
        if i % 25 == 0:
            print(f'  current {i}/{len(todo)} last={code} status={rec["status"]}')
    print(f'current done. requests={_counter["n"]}')


def _jjgg(code: str, kind: int) -> list[dict]:
    cache = RAW / f'{code}_jjgg_{kind}.json'
    if cache.exists():
        return json.loads(cache.read_text(encoding='utf-8'))
    url = (f'https://api.fund.eastmoney.com/f10/JJGG?fundcode={code.split(".")[0]}'
           f'&pageIndex=1&pageSize=200&type={kind}')
    data = json.loads(_get(url)).get('Data') or []
    cache.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    return data


def _ann_attach(code: str, ann_id: str) -> bytes:
    cache = RAW / f'{code}_{ann_id}.pdf'
    if cache.exists():
        return cache.read_bytes()
    meta = json.loads(_get(
        f'https://np-cnotice-stock.eastmoney.com/api/content/ann?client_source=web'
        f'&page_index=1&art_code={ann_id}')).get('data') or {}
    url = meta.get('attach_url')
    if not url:
        raise RuntimeError('no attach_url')
    blob = _get(url, binary=True)
    cache.write_bytes(blob)
    return blob


def stage_history(limit: int | None = None, codes: list[str] | None = None) -> None:
    pool = pl.read_parquet(POOL).filter(pl.col('in_scope'))
    if codes:
        pool = pool.filter(pl.col('ts_code').is_in(codes))
    else:
        pool = pool.filter(pl.col('tier') == 'core')
    done_path = STAGING / 'progress_history.csv'
    done_df = _read_csv(done_path)
    done = set(done_df.filter(pl.col('status') == 'ok')['ts_code'].to_list()) \
        if done_df.height else set()
    rows = pool.sort('ts_code').to_dicts()
    todo = [r for r in rows if r['ts_code'] not in done]
    if limit:
        todo = todo[:limit]
    print(f'history: total {len(rows)}, done {len(done)}, todo {len(todo)}')
    for i, row in enumerate(todo, 1):
        code = row['ts_code']
        rec = {'ts_code': code, 'status': '', 'n_pds': 0, 'versions': [],
               'contract_dates': []}
        try:
            anns = _jjgg(code, 1) + _jjgg(code, 2)
            pds = [a for a in anns
                   if '产品资料概要' in a['TITLE']
                   and a['PUBLISHDATEDesc'] >= HISTORY_SCAN_SINCE.isoformat()]
            contracts = [a['PUBLISHDATEDesc'] for a in anns
                         if a['TITLE'].endswith('基金合同') or a['TITLE'].endswith('托管协议')]
            versions = []
            for a in sorted(pds, key=lambda x: x['PUBLISHDATEDesc']):
                try:
                    rates = parse_pds(_ann_attach(code, a['ID']))
                except Exception as exc:  # noqa: BLE001
                    rec.setdefault('errors', []).append(f'{a["ID"]}:{exc}'[:120])
                    continue
                versions.append({'date': a['PUBLISHDATEDesc'], 'id': a['ID'], **rates})
            rec.update(n_pds=len(versions), versions=versions,
                       contract_dates=sorted(set(contracts)), status='ok')
        except StopCrawl:
            _append_csv(STAGING / 'failed.csv', {'ts_code': code, 'stage': 'history',
                                                 'error': 'stop_crawl'})
            raise
        except Exception as exc:  # noqa: BLE001
            rec['status'] = 'error'
            _append_csv(STAGING / 'failed.csv', {'ts_code': code, 'stage': 'history',
                                                 'error': str(exc)[:200]})
        _append_csv(done_path, rec)
        if i % 10 == 0:
            print(f'  history {i}/{len(todo)} last={code} pds={rec["n_pds"]}')
    print(f'history done. requests={_counter["n"]}')


def _burden(segs: list[dict], win_start: date, win_end: date) -> float | None:
    """按分段费率对窗口内逐日加权，返回年化费用负担（百分数）。"""
    if not segs:
        return None
    pts = sorted((date.fromisoformat(s['effective_from']), s) for s in segs)
    total_days = 0
    acc = 0.0
    for i, (start, seg) in enumerate(pts):
        seg_end = pts[i + 1][0] if i + 1 < len(pts) else win_end
        lo = max(start, win_start)
        hi = min(seg_end, win_end)
        if hi <= lo:
            continue
        days = (hi - lo).days
        rate = (seg['mgmt'] or 0.0) + (seg['cust'] or 0.0) + (seg.get('sales') or 0.0)
        acc += rate * days
        total_days += days
    if not total_days:
        return None
    return round(acc / total_days, 6)


def _segments(versions: list[dict], contracts: list[str]) -> tuple[list[dict], str, str]:
    """把按发布日排列的费率版本切成 (生效日, 费率) 段。"""
    vers = [v for v in versions if v.get('mgmt') is not None]
    if not vers:
        return [], 'none', 'no_pds'
    segs: list[dict] = []
    prev = None
    for v in vers:
        key = (v['mgmt'], v['cust'], v.get('sales'))
        if prev is None or key != (prev['mgmt'], prev['cust'], prev.get('sales')):
            # 生效日优先取 45 天内最近一次基金合同/托管协议公告日
            cand = [c for c in contracts
                    if date.fromisoformat(v['date']) - timedelta(days=45)
                    <= date.fromisoformat(c) <= date.fromisoformat(v['date'])]
            if cand:
                eff, basis = max(cand), 'contract_ann_date'
            else:
                eff, basis = v['date'], 'pds_publish_date'
            segs.append({'effective_from': eff, 'mgmt': v['mgmt'], 'cust': v['cust'],
                         'sales': v.get('sales'), 'basis': basis,
                         'pds_id': v['id'], 'pds_date': v['date']})
        prev = v
    if len(segs) <= 1:
        return segs, 'single_version', 'pds_publish_date'
    prec = 'ann_date' if all(s['basis'] == 'contract_ann_date' for s in segs[1:]) else 'mixed'
    return segs, 'multi_version', prec


def build() -> pl.DataFrame:  # noqa: C901 - 逐只拼装字段，拆函数反而更难对读
    pool = pl.read_parquet(POOL).filter(pl.col('in_scope'))
    cur = _read_csv(STAGING / 'progress_current.csv')
    hist = _read_csv(STAGING / 'progress_history.csv')
    if cur.height:
        cur = cur.unique(subset=['ts_code'], keep='last')
    if hist.height:
        hist = hist.unique(subset=['ts_code'], keep='last')
    cur = cur.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in
                            ['mgmt_a', 'cust_a', 'sales_a', 'mgmt_b', 'cust_b']])
    hist_map = {}
    for r in hist.to_dicts():
        hist_map[r['ts_code']] = r

    out = []
    for row in pool.sort(['tier', 'asset_class', 'ts_code']).to_dicts():
        code = row['ts_code']
        c = cur.filter(pl.col('ts_code') == code)
        rec = {
            'ts_code': code, 'name': row['name'], 'asset_class': row['asset_class'],
            'tier': row['tier'], 'status': row['status'],
            'list_date': row['list_date'].isoformat() if row.get('list_date') else None,
            'delist_date': row['delist_date'].isoformat() if row.get('delist_date') else None,
            'mgmt_fee_pct': None, 'custody_fee_pct': None, 'sales_service_fee_pct': None,
            'mgmt_source': None, 'custody_source': None, 'sales_source': None,
            'cross_check_status': 'not_fetched', 'mgmt_diff_pct': None, 'custody_diff_pct': None,
            'has_fee_history': False, 'fee_history_segments': None,
            'history_basis': 'not_checked', 'history_precision': None,
            'history_window_covered': False, 'missing_reason': None,
            'burden_current_pct': None, 'burden_segmented_pct': None, 'burden_basis': None,
        }
        if c.height:
            d = c.to_dicts()[0]
            a_m, a_c, a_s = d['mgmt_a'], d['cust_a'], d['sales_a']
            b_m, b_c = d['mgmt_b'], d['cust_b']
            if a_m is not None:
                rec.update(mgmt_fee_pct=a_m, custody_fee_pct=a_c,
                           sales_service_fee_pct=a_s,
                           mgmt_source='em.fundf10.jbgk',
                           custody_source='em.fundf10.jbgk',
                           sales_source='em.fundf10.jbgk' if a_s is not None else None)
            elif b_m is not None:
                rec.update(mgmt_fee_pct=b_m, custody_fee_pct=b_c,
                           mgmt_source='em.fundf10.jjfl',
                           custody_source='em.fundf10.jjfl')
            if a_m is not None and b_m is not None:
                dm, dc = round(a_m - b_m, 6), (round(a_c - b_c, 6)
                                               if a_c is not None and b_c is not None else None)
                rec['mgmt_diff_pct'], rec['custody_diff_pct'] = dm, dc
                rec['cross_check_status'] = 'consistent' if (dm == 0 and (dc in (0, None))) \
                    else 'conflict'
            elif a_m is not None or b_m is not None:
                rec['cross_check_status'] = 'single_source'
            else:
                rec['cross_check_status'] = 'missing'
                rec['missing_reason'] = '两个页面均未解析到管理费率'
        else:
            rec['missing_reason'] = '未取数'

        h = hist_map.get(code)
        if h and h.get('status') == 'ok' and h.get('versions') not in (None, '', '[]'):
            versions = json.loads(h['versions'])
            contracts = json.loads(h.get('contract_dates') or '[]')
            segs, basis, prec = _segments(versions, contracts)
            if segs:
                rec.update(has_fee_history=len(segs) > 1,
                           fee_history_segments=json.dumps(segs, ensure_ascii=False),
                           history_basis=basis, history_precision=prec)
                first = date.fromisoformat(min(s['effective_from'] for s in segs))
                floor = HISTORY_WINDOW_START
                if row.get('list_date') and row['list_date']:
                    floor = max(floor, row['list_date'] + timedelta(days=45))
                rec['history_window_covered'] = first <= floor
            else:
                rec['history_basis'] = 'no_pds'

        if rec['missing_reason'] is None and rec['mgmt_fee_pct'] is None:
            rec['missing_reason'] = '页面存在但费率字段为空'
        if rec['mgmt_fee_pct'] is not None:
            rec['burden_current_pct'] = round(
                (rec['mgmt_fee_pct'] or 0) + (rec['custody_fee_pct'] or 0)
                + (rec['sales_service_fee_pct'] or 0), 6)
            if rec['fee_history_segments']:
                segs = json.loads(rec['fee_history_segments'])
                start = HISTORY_WINDOW_START
                if row.get('list_date'):
                    start = max(start, row['list_date'])
                b = _burden(segs, start, HISTORY_WINDOW_END)
                if b is not None and rec['history_window_covered']:
                    rec['burden_segmented_pct'], rec['burden_basis'] = b, 'segments'
                else:
                    rec['burden_segmented_pct'], rec['burden_basis'] = \
                        rec['burden_current_pct'], 'current_fallback'
            else:
                rec['burden_segmented_pct'], rec['burden_basis'] = \
                    rec['burden_current_pct'], 'current_fallback'
        out.append(rec)

    df = pl.from_dicts(out, infer_schema_length=None)
    return df.with_columns(pl.col('missing_reason').cast(pl.String),
                           pl.col('fee_history_segments').cast(pl.String))


def summarize(df: pl.DataFrame) -> dict:
    core = df.filter(pl.col('tier') == 'core')

    def dist(col: str) -> dict:
        s = core[col].drop_nulls()
        if not s.len():
            return {}
        return {'n': s.len(), 'min': round(s.min(), 4), 'median': round(s.median(), 4),
                'max': round(s.max(), 4)}

    def hist_cov(t: str) -> dict:
        d = df.filter(pl.col('tier') == t)
        return {'total': d.height,
                'covered': d.filter(pl.col('history_basis').is_in(
                    ['single_version', 'multi_version'])).height,
                'multi': d.filter(pl.col('has_fee_history')).height,
                'not_checked': d.filter(pl.col('history_basis') == 'not_checked').height}

    return {
        'rows': df.height,
        'tier_counts': df['tier'].value_counts().sort('tier').to_dicts(),
        'core_total': core.height,
        'core_with_fee': core.filter(pl.col('mgmt_fee_pct').is_not_null()).height,
        'core_cross_check': core['cross_check_status'].value_counts().to_dicts(),
        'all_cross_check': df['cross_check_status'].value_counts().to_dicts(),
        'core_history_multi': core.filter(pl.col('has_fee_history')).height,
        'core_history_window_covered': core.filter(pl.col('history_window_covered')).height,
        'history_coverage_by_tier': {t: hist_cov(t) for t in ['core', 'extend', 'delisted']},
        'burden_current_col': dist('burden_current_pct'),
        'burden_segmented_col': dist('burden_segmented_pct'),
        'burden_basis': core['burden_basis'].value_counts().to_dicts(),
        'missing_core': core.filter(pl.col('mgmt_fee_pct').is_null())
                              .select(['ts_code', 'name', 'missing_reason']).to_dicts(),
    }


def write_outputs(df: pl.DataFrame) -> None:
    from src.data.atomic_write import write_parquet_atomic, write_text_atomic

    STAGING.mkdir(parents=True, exist_ok=True)
    pq = STAGING / 'etf_fee.parquet'
    write_parquet_atomic(df, pq)
    write_text_atomic(df.write_csv(), STAGING / 'etf_fee.csv')

    gaps = df.filter(pl.col('mgmt_fee_pct').is_null() | pl.col('cross_check_status').is_in(
        ['conflict', 'single_source', 'missing']))
    write_text_atomic(gaps.write_csv(), STAGING / 'etf_fee_gaps.csv')

    summary = summarize(df)
    write_text_atomic(json.dumps(summary, ensure_ascii=False, indent=2),
                      STAGING / 'etf_fee_summary.json')

    def sha(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()

    manifest = {
        'semantic': '多资产 ETF 管理费/托管费/销售服务费（当前值 + 费率变更分段）',
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'source': ['fundf10.eastmoney.com/jbgk_<code>.html',
                   'fundf10.eastmoney.com/jjfl_<code>.html',
                   'api.fund.eastmoney.com/f10/JJGG + pdf.dfcfw.com 产品资料概要'],
        'rows': df.height,
        'tier_counts': summary['tier_counts'],
        'core_coverage': f"{summary['core_with_fee']}/{summary['core_total']}",
        'cross_check': summary['core_cross_check'],
        'history_window_start': HISTORY_WINDOW_START.isoformat(),
        'history_scope': 'core 153 只 + extend 330 只 + 退市日 >= 2023-07-03 的 delisted 6 只',
        'history_coverage': summary['history_coverage_by_tier'],
        'parse_tag': REPARSE_TAG,
        'files': {p.name: {'bytes': p.stat().st_size, 'sha256': sha(p)}
                  for p in [pq, STAGING / 'etf_fee.csv', STAGING / 'etf_fee_gaps.csv',
                            STAGING / 'etf_fee_summary.json']},
    }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    apply_semantics(manifest, 'meta/multi_asset/etf_fee.manifest.json')
    write_text_atomic(json.dumps(manifest, ensure_ascii=False, indent=2),
                      STAGING / 'etf_fee.manifest.json')
    print(json.dumps(summary, ensure_ascii=False, indent=2)[:2000])


def apply_staging() -> None:
    """原子写入正式目录。

    目标目录 `meta/multi_asset/` 里已有 etf_pool.* 等他人产物，本次只新增
    `etf_fee*` 五个文件，**不移动、不覆盖整个目录**；若同名文件已存在，
    先 copy2 到 `meta/_backup/multi_asset_fee_<时间戳>/` 再替换（DG-11/DG-22）。
    """
    import shutil

    from src.data.atomic_write import write_parquet_atomic, write_text_atomic

    TARGET.mkdir(parents=True, exist_ok=True)
    names = ['etf_fee.parquet', 'etf_fee.csv', 'etf_fee_gaps.csv',
             'etf_fee_summary.json', 'etf_fee.manifest.json']
    exist = [n for n in names if (TARGET / n).exists()]
    if exist:
        backup = ROOT / '_backup' / 'meta' / f'multi_asset_fee_{datetime.now():%Y%m%d_%H%M%S}'
        backup.mkdir(parents=True, exist_ok=True)
        for n in exist:
            shutil.copy2(TARGET / n, backup / n)
        print(f'已备份既有 {len(exist)} 个文件到 {backup}')
    for name in names:
        src = STAGING / name
        if name.endswith('.parquet'):
            write_parquet_atomic(pl.read_parquet(src), TARGET / name)
        else:
            write_text_atomic(src.read_text(encoding='utf-8'), TARGET / name)
    print(f'已写入 {TARGET} ({len(names)} 文件)')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--stage', choices=['current', 'history', 'reparse', 'build', 'apply'],
                    required=True)
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--codes', type=str, default=None,
                    help='逗号分隔的 ts_code，指定后 history 阶段只取这些标的')
    ap.add_argument('--throttle', type=float, default=None,
                    help='单请求间隔秒数，默认 1.2')
    args = ap.parse_args()
    if args.throttle is not None:
        global THROTTLE
        THROTTLE = args.throttle
    codes = [c.strip() for c in args.codes.split(',') if c.strip()] if args.codes else None
    STAGING.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(ROOT))
    try:
        if args.stage == 'current':
            stage_current(args.limit)
        elif args.stage == 'history':
            stage_history(args.limit, codes)
        elif args.stage == 'reparse':
            stage_reparse(args.limit)
        elif args.stage == 'build':
            write_outputs(build())
        else:
            apply_staging()
    except StopCrawl as exc:
        print(f'STOP: {exc}')
        raise SystemExit(2) from exc


if __name__ == '__main__':
    main()
