# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""18 个「双方越界」事件取公告原文（只取证，不落盘数据）。

流程（DG-25：单连接串行、逐请求节流）：
1. 东财基金公告索引 ``api.fund.eastmoney.com/f10/JJGG`` 取该基金公告列表；
2. 挑出标题含「折算」且发布日期在事件日前 45 天内的条目；
3. 下载 ``pdf.dfcfw.com/pdf/H2_<ID>_1.pdf``，用 pdfplumber 抽文本；
4. 从文本里抽出所有 4~9 位小数的比例值，与 akshare 比例 / 因子台阶 /
   价值守恒区间（``cprev/high`` ~ ``cprev/low``）对照。
"""
from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pdfplumber
import polars as pl
import requests

WORK = Path(__file__).parent
CACHE = WORK / '_ann_cache'
HEAD = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'http://fundf10.eastmoney.com/'}
RATIO_RE = re.compile(r'\d\.\d{4,9}')


def get_json(url: str, params: dict, tries: int = 3) -> dict:
    for k in range(tries):
        try:
            r = requests.get(url, params=params, headers=HEAD, timeout=25)
            if r.status_code == 200:
                return r.json()
        except Exception as exc:  # noqa: BLE001
            print('   err', k, exc)
        time.sleep(1.5 * (k + 1))
    return {}


def announcement_list(code: str) -> list[dict]:
    cache = CACHE / f'list_{code}.json'
    if cache.exists():
        return json.loads(cache.read_text(encoding='utf-8'))
    params = {'fundcode': code, 'pageIndex': 1, 'pageSize': 50, 'type': 2,
              'startDate': '2010-01-01', 'endDate': '2026-12-31'}
    for k in range(3):
        j = get_json('https://api.fund.eastmoney.com/f10/JJGG', params)
        data = j.get('Data') or []
        if data:
            CACHE.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
            return data
        print(f'   empty total={j.get("TotalCount")} retry {k + 1} 后等待')
        time.sleep(6.0 * (k + 1))
    return []


def pdf_text(ann_id: str) -> tuple[str, Path | None]:
    CACHE.mkdir(parents=True, exist_ok=True)
    p = CACHE / f'{ann_id}.pdf'
    if not p.exists():
        for k in range(3):
            try:
                r = requests.get(f'http://pdf.dfcfw.com/pdf/H2_{ann_id}_1.pdf',
                                 headers=HEAD, timeout=40)
                if r.status_code == 200 and r.content[:4] == b'%PDF':
                    p.write_bytes(r.content)
                    break
            except Exception as exc:  # noqa: BLE001
                print('   pdf err', k, exc)
            time.sleep(1.5 * (k + 1))
        else:
            return '', None
        time.sleep(1.0)
    try:
        with pdfplumber.open(p) as doc:
            return '\n'.join((pg.extract_text() or '') for pg in doc.pages), p
    except Exception as exc:  # noqa: BLE001
        print('   pdf parse err', exc)
        return '', p


def main() -> None:
    ev = json.loads((WORK / 'b2s2_d4_evidence.json').read_text(encoding='utf-8'))
    targets = [r for r in ev['rows'] if r['verdict'].startswith('两者均越界')]
    codes = sorted({r['ts_code'] for r in targets})
    lists = {}
    for c in codes:
        lists[c] = announcement_list(c)
        print(f'[{c}] announcements {len(lists[c])}  最早 '
              f'{min((a["PUBLISHDATEDesc"] for a in lists[c]), default="-")}')
        time.sleep(1.2)

    out = []
    for r in targets:
        code, d = r['ts_code'], date.fromisoformat(r['date'])
        lo = r['cprev'] / r['high']
        hi = r['cprev'] / r['low']
        best = None
        for a in lists[code]:
            pd = date.fromisoformat(a['PUBLISHDATEDesc'])
            if '折算' not in a['TITLE'] or pd > d or (d - pd).days > 45:
                continue
            if best is None or pd > best[0]:
                best = (pd, a)
        rec = {'ts_code': code, 'date': r['date'], 'step': r['step'],
               'ak_ratio': r['ak_ratio'], 'cprev': r['cprev'], 'low': r['low'],
               'high': r['high'], 'implied_lo': round(lo, 6), 'implied_hi': round(hi, 6),
               'single_price': r['single_price'], 'ann_title': None, 'ann_date': None,
               'ann_id': None, 'ratios_in_text': [], 'nearest_ratio': None,
               'nearest_gap': None, 'ratio_match': None, 'note': ''}
        if best is None:
            rec['note'] = '索引内无该事件日前的折算公告（东财公告索引仅含近年条目）'
            out.append(rec)
            continue
        pd, a = best
        text, p = pdf_text(a['ID'])
        rec.update({'ann_title': a['TITLE'], 'ann_date': pd.isoformat(), 'ann_id': a['ID']})
        ratios = sorted({float(x) for x in RATIO_RE.findall(text)}) if text else []
        rec['ratios_in_text'] = ratios[:40]
        if not text:
            rec['note'] = '公告 PDF 无文本层或下载失败'
        elif ratios:
            near = min(ratios, key=lambda x: abs(x - r['ak_ratio']))
            rec['nearest_ratio'] = near
            rec['nearest_gap'] = round(abs(near - r['ak_ratio']), 9)
            rec['ratio_match'] = rec['nearest_gap'] < 1e-6
        out.append(rec)
        print(f"[{code} {d}] 公告 {pd} {a['TITLE'][:36]} 最近比例 {rec['nearest_ratio']} "
              f"gap {rec['nearest_gap']}")

    flat = [{**{k: v for k, v in o.items() if k != 'ratios_in_text'},
             'ratios_in_text': ','.join(f'{x:g}' for x in o['ratios_in_text'])} for o in out]
    pl.DataFrame(flat).write_csv(WORK / 'b2s2_ann18.csv')
    (WORK / 'b2s2_ann18.json').write_text(
        json.dumps({'rows': out, 'generated_at': datetime.now().isoformat(timespec='seconds')},
                   ensure_ascii=False, indent=1), encoding='utf-8')
    got = sum(1 for o in out if o['ann_id'])
    matched = sum(1 for o in out if o['ratio_match'])
    print('events', len(out), 'ann_found', got, 'ratio_confirmed', matched)


if __name__ == '__main__':
    main()
