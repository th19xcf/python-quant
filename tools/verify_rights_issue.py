#!/usr/bin/env python3

"""用独立第二源核验配股事件参数。

支持三个互相独立的核验源，任一通过即可解除该事件的「无第二源」存疑：

``baostock``
    取除权日当日不复权 ``preclose``（已按除权调整的前收盘），与本地参数按
    标准除权公式算出的理论除权价对比::

        theo = (prev_close - cash_div + rights_issue_price * rights_issue_ratio)
               / (1 + share_div + rights_issue_ratio)

``gbbq``
    直接解析通达信股本变迁库（``T0002/hq_cache/gbbq``，由 pytdx 解密），
    取 ``category=1``（除权除息）记录的配股价与配股比例，与本地参数逐事件
    直比。字段口径为「每 10 股」，比例需乘 10 后比较。

``em``
    东财 F10「分红融资」接口的配股明细（``pgmx``）：``ISSUE_PRICE`` 为配股价，
    ``EVENT_EXPLAIN`` 形如「每10股配3股」，``EX_DIVIDEND_DATEE`` 为除权日。
    与本地事件按除权日对齐后直比价格与比例，用于前两源缺记录或参数冲突的
    老事件（1992-2004）。

通过核验的事件写入 ``meta/anomalies/rights_issue_verified.csv``；
``refresh_anomaly_lists`` 生成存疑清单时会排除这些事件。一只股票的**全部**
配股事件都通过核验，该标的才会重新进入 strict 池（逐事件排除语义天然保证）。

写入按 ``verify_source`` 分来源合并：重跑某个源只替换该源的行，不会冲掉
另一个源已积累的结果。

用法::

    python tools/verify_rights_issue.py --source gbbq
    python tools/verify_rights_issue.py --source all
    python tools/verify_rights_issue.py --tol 0.01 --limit 20 --store-root data/local_store

通达信安装目录默认取 ``QUANT_TDX_ROOT``（缺省 ``D:/zxzq``），可用
``--gbbq-path`` 直接指定 gbbq 文件路径。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

DEFAULT_TOL = 0.01
DEFAULT_GBBQ_TOL = 0.02
PROGRESS_NAME = 'rights_verify_progress.json'

SRC_BAOSTOCK = 'baostock_history_preclose_adjustflag3'
SRC_GBBQ = 'tdx_gbbq_category1'
SRC_EM = 'em_f10_pgmx'

EM_URL = 'https://emweb.securities.eastmoney.com/PC_HSF10/BonusFinancing/PageAjax'
EM_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
}
EM_SLEEP = 0.3
EM_RETRY = 3
_PLAN_RATIO_RE = re.compile(r'每10股配([0-9.]+)股')

# gbbq.market 编码 → 交易所后缀
_MARKET_SUFFIX = {0: 'SZ', 1: 'SH', 2: 'BJ'}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='配股事件第二源核验（baostock / gbbq / em）')
    parser.add_argument('--store-root', default='',
                        help='本地列存根目录（默认 data/local_store）')
    parser.add_argument('--source', default='baostock',
                        choices=['baostock', 'gbbq', 'em', 'all'],
                        help='核验源：baostock（默认）/ gbbq / em / all')
    parser.add_argument('--tol', type=float, default=DEFAULT_TOL,
                        help=f'baostock / em 相对误差阈值（默认 {DEFAULT_TOL}）')
    parser.add_argument('--gbbq-tol', type=float, default=DEFAULT_GBBQ_TOL,
                        help=f'gbbq 相对误差阈值（默认 {DEFAULT_GBBQ_TOL}）')
    parser.add_argument('--gbbq-path', default=None,
                        help='gbbq 文件路径（默认 $QUANT_TDX_ROOT/T0002/hq_cache/gbbq）')
    parser.add_argument('--limit', type=int, default=0,
                        help='只处理前 N 只（0=全部，调试用）')
    parser.add_argument('--codes', default='',
                        help='只核验指定代码（逗号分隔，如 000417.SZ,600601.SH）')
    parser.add_argument('--reset', action='store_true',
                        help='忽略断点进度，从头核验（仅 baostock）')
    return parser.parse_args(argv)


def _default_gbbq_path() -> Path:
    root = os.environ.get('QUANT_TDX_ROOT', 'D:/zxzq')
    return Path(root) / 'T0002' / 'hq_cache' / 'gbbq'


def _gbbq_rows(ev: pd.DataFrame, gbbq_path: Path, tol: float) -> list[dict] | None:
    """解析 gbbq 并与本地配股事件逐条直比，返回通过核验的事件行。"""
    try:
        from pytdx.reader.gbbq_reader import GbbqReader
    except ImportError:
        print('缺少 pytdx，请执行 poetry install --with tdx 后重试', file=sys.stderr)
        return None
    if not gbbq_path.exists():
        print(f'gbbq 文件不存在: {gbbq_path}', file=sys.stderr)
        return None

    raw = GbbqReader().get_df(str(gbbq_path))
    ref = raw[raw['category'] == 1].copy()
    ref['ex_date'] = pd.to_datetime(ref['datetime'].astype(str),
                                    format='%Y%m%d', errors='coerce')
    ref['code'] = ref['code'].astype(str).str.zfill(6)
    ref['ts_code'] = [
        f'{c}.' + _MARKET_SUFFIX.get(int(m), 'SZ')
        for c, m in zip(ref['code'], ref['market'], strict=True)
    ]
    ref = ref.dropna(subset=['ex_date'])
    ref = ref.drop_duplicates(subset=['ts_code', 'ex_date'])
    ref = ref[['ts_code', 'ex_date', 'peigujia_qianzongguben', 'peigu_houzongguben']]

    m = ev.merge(ref, on=['ts_code', 'ex_date'], how='inner')
    lp = m['rights_issue_price'].fillna(0).astype(float)
    lr = m['rights_issue_ratio'].fillna(0).astype(float) * 10
    gp = m['peigujia_qianzongguben'].fillna(0).astype(float)
    gr = m['peigu_houzongguben'].fillna(0).astype(float)

    price_rel = (lp - gp).abs() / gp.abs().clip(lower=1e-9)
    ratio_rel = (lr - gr).abs() / gr.abs().clip(lower=1e-9)
    rel = pd.concat([price_rel, ratio_rel], axis=1).max(axis=1)
    # 本地有值而 gbbq 为 0：该源无法佐证，视为不通过
    rel = rel.mask(((lp > 0) & (gp == 0)) | ((lr > 0) & (gr == 0)), float('inf'))

    m = m.assign(rel_err=rel.round(6))
    ok = m[m['rel_err'] < tol]
    print(f'gbbq 匹配 {len(m)} 个事件，通过 {len(ok)} 个 / {ok["ts_code"].nunique()} 只')
    return [{'ts_code': r.ts_code, 'ex_date': r.ex_date.strftime('%Y-%m-%d'),
             'rel_err': r.rel_err} for r in ok.itertuples()]


def _fetch_em_pgmx(code: str) -> list[dict]:
    """抓取单只标的的东财 F10 配股明细（``pgmx``）。"""
    symbol, market = code.split('.')
    delay = 1.0
    last = ''
    for attempt in range(EM_RETRY + 1):
        try:
            resp = requests.get(EM_URL, params={'code': f'{market}{symbol}'},
                                headers=EM_HEADERS, timeout=25)
            payload = resp.json()
            return list(payload.get('pgmx') or [])
        except (requests.RequestException, ValueError) as exc:
            last = f'{type(exc).__name__}: {exc}'
        if attempt >= EM_RETRY:
            break
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f'东财配股明细抓取失败 {code}: {last}')


def _parse_em_rows(rows: list[dict]) -> pd.DataFrame:
    """东财 ``pgmx`` → ``ex_date / em_price / em_ratio``（比例已除 10）。"""
    parsed = []
    for item in rows:
        ex_date = pd.to_datetime(str(item.get('EX_DIVIDEND_DATEE'))[:10], errors='coerce')
        match = _PLAN_RATIO_RE.search(str(item.get('EVENT_EXPLAIN') or ''))
        if pd.isna(ex_date) or not match:
            continue
        parsed.append({'ex_date': ex_date,
                       'em_price': float(item.get('ISSUE_PRICE') or 0),
                       'em_ratio': float(match.group(1)) / 10})
    return pd.DataFrame(parsed, columns=['ex_date', 'em_price', 'em_ratio'])


def _em_rows(ev: pd.DataFrame, tol: float) -> list[dict]:
    """用东财 F10 配股明细逐事件核对配股价与配股比例。

    本地同一除权日可能被拆成多行，先按除权日合并（比例相加、配股价按比例
    加权）；东财一侧同样只保留除权日、配股价、每 10 股配股比例。
    """
    work = ev.copy()
    work['_ratio'] = work['rights_issue_ratio'].fillna(0).astype(float)
    work['_price'] = work['rights_issue_price'].fillna(0).astype(float)
    work['_amount'] = work['_price'] * work['_ratio']
    local = (work.groupby(['ts_code', 'ex_date'], as_index=False)
             .agg(ratio=('_ratio', 'sum'), amount=('_amount', 'sum')))
    local['price'] = local['amount'] / local['ratio'].where(local['ratio'] > 0)

    matched, rows, fetched = 0, [], 0
    for code, grp in local.groupby('ts_code', sort=True):
        try:
            pg = _fetch_em_pgmx(code)
        except RuntimeError as exc:
            print(exc, file=sys.stderr)
            continue
        fetched += 1
        ref = _parse_em_rows(pg).drop_duplicates(subset=['ex_date'])
        if ref.empty:
            continue
        m = grp.merge(ref, on='ex_date', how='inner')
        if m.empty:
            continue
        price_rel = (m['price'] - m['em_price']).abs() / m['em_price'].abs().clip(lower=1e-9)
        ratio_rel = (m['ratio'] - m['em_ratio']).abs() / m['em_ratio'].abs().clip(lower=1e-9)
        rel = pd.concat([price_rel, ratio_rel], axis=1).max(axis=1)
        # 本地有值而东财为 0：该源无法佐证，视为不通过
        rel = rel.mask(((m['price'] > 0) & (m['em_price'] <= 0))
                       | ((m['ratio'] > 0) & (m['em_ratio'] <= 0)), float('inf'))
        m = m.assign(rel_err=rel.round(6))
        matched += len(m)
        ok = m[m['rel_err'] < tol]
        rows.extend({'ts_code': r.ts_code,
                     'ex_date': r.ex_date.strftime('%Y-%m-%d'),
                     'rel_err': r.rel_err} for r in ok.itertuples())
        time.sleep(EM_SLEEP)

    print(f'东财配股明细抓取 {fetched} 只，匹配 {matched} 个事件，'
          f'通过 {len(rows)} 个 / {len({r["ts_code"] for r in rows})} 只')
    return rows


def _merge_verified(rows: list[dict], source: str, out_dir: Path) -> pd.DataFrame:
    """按来源合并写入 verified 清单，重跑某源只替换该源的行。"""
    path = out_dir / 'rights_issue_verified.csv'
    old = pd.read_csv(path) if path.exists() else pd.DataFrame(
        columns=['ts_code', 'ex_date', 'rel_err', 'verify_source', 'verified_at'])
    old = old[old['verify_source'] != source]

    new = pd.DataFrame(rows)
    if not new.empty:
        new['verify_source'] = source
        new['verified_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    merged = pd.concat([old, new], ignore_index=True)
    if not merged.empty:
        # 同一事件被多个源命中时保留误差最小的那条
        merged = merged.sort_values(['ts_code', 'ex_date', 'rel_err'])
        merged = merged.drop_duplicates(subset=['ts_code', 'ex_date'], keep='first')
        merged = merged.sort_values(['ts_code', 'ex_date']).reset_index(drop=True)
    merged.to_csv(path, index=False, encoding='utf-8')

    by_src = merged['verify_source'].value_counts().to_dict() if len(merged) else {}
    print(f'verified 合计 {len(merged)} 个事件 / {merged["ts_code"].nunique()} 只；来源 {by_src}')
    print(f'输出: {path}')
    return merged


def _to_bs(code: str) -> str:
    symbol, market = code.split('.')
    return f'{"sh" if market == "SH" else "sz"}.{symbol}'


def _fetch(bs, code: str, start: str, end: str) -> pd.DataFrame:
    rs = bs.query_history_k_data_plus(
        _to_bs(code), 'date,open,high,low,close,preclose,volume',
        start_date=start, end_date=end, frequency='d', adjustflag='3')
    rows = []
    while rs.error_code == '0' and rs.next():
        rows.append(rs.get_row_data())
    return pd.DataFrame(rows, columns=rs.fields) if rows else pd.DataFrame()


def _anchor_before(traded: pd.DataFrame, frame: pd.DataFrame,
                   day: pd.Timestamp) -> float | None:
    """事件日之前最后一个有效收盘价（优先近 30 天内有成交的行）。"""
    window = traded[(traded.index < day)
                    & (traded.index >= day - pd.Timedelta(days=30))]
    if not window.empty:
        return float(window['close'].iloc[-1])
    prev = frame[frame.index < day]
    return float(prev['close'].iloc[-1]) if not prev.empty else None


def _anchor_after(traded: pd.DataFrame, frame: pd.DataFrame,
                  day: pd.Timestamp) -> float | None:
    """事件日（含）之后第一个有效前收盘（优先近 30 天内有成交的行）。"""
    window = traded[(traded.index >= day)
                    & (traded.index <= day + pd.Timedelta(days=30))]
    if not window.empty:
        return float(window['preclose'].iloc[0])
    after = frame[frame.index >= day]
    return float(after['preclose'].iloc[0]) if not after.empty else None


def _verify_code(bs, code: str, sub: pd.DataFrame, tol: float) -> dict:
    """核验单只股票的全部配股事件。

    ``sub`` 需要传**该股全部**除权事件（不只是配股行），因为同一除权日常被
    不同来源拆成「配股」与「送股」两行，只有全部行参与合并才能还原标准除权价。

    停牌期间行情源会给出成交量 0、价格不动的占位行，这类行既不是有效的前收
    盘锚点，也不能当作除权生效日。因此两个锚点都优先取「有成交量」的交易日：
    事件日之前最后一个有量收盘价作为 ``prev_close``，事件日（含）之后第一个
    有量交易日的 ``preclose`` 作为除权参考价；近 30 天内无成交时退回相邻行，
    保持与历史口径一致。
    """
    rights = sub[(sub['rights_issue_ratio'].fillna(0) > 0)
                 | (sub['rights_issue_price'].fillna(0) > 0)]
    if rights.empty:
        return {'status': 'ok', 'events': []}
    right_dates = rights['ex_date']
    right_set = set(right_dates)
    start = (right_dates.min() - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
    end = (right_dates.max() + pd.Timedelta(days=15)).strftime('%Y-%m-%d')
    try:
        frame = _fetch(bs, code, start, end)
    except Exception as exc:  # noqa: BLE001
        return {'status': 'fetch_failed', 'error': str(exc)[:200], 'events': []}
    if frame.empty:
        return {'status': 'no_data', 'events': []}

    frame['date'] = pd.to_datetime(frame['date'])
    frame = frame.set_index('date')
    for col in ('close', 'preclose', 'volume'):
        frame[col] = pd.to_numeric(frame[col], errors='coerce')
    traded = frame[frame['volume'].fillna(0) > 0]

    events = []
    # 同一除权日可能被不同来源拆成多行（配股/送股分行），核验前需先合并，
    # 否则配股项与送股项各套一次前收，理论价必然对不上。
    merged = []
    for day, grp in sub.groupby('ex_date', sort=True):
        ratio = float(grp['rights_issue_ratio'].fillna(0).sum())
        amount = float((grp['rights_issue_price'].fillna(0)
                        * grp['rights_issue_ratio'].fillna(0)).sum())
        merged.append({
            'ex_date': day,
            'cash_div': float(grp['cash_div'].fillna(0).sum()),
            'share_div': float(grp['share_div'].fillna(0).sum()),
            'rights_issue_ratio': ratio,
            'rights_issue_price': amount / ratio if ratio > 0 else 0.0,
        })
    for row in merged:
        day = row['ex_date']
        if day not in right_set:
            continue
        item = {'ex_date': str(day.date()), 'verified': False, 'rel_err': None}
        preclose = _anchor_after(traded, frame, day)
        prev_close = _anchor_before(traded, frame, day)
        if (preclose is not None and prev_close is not None
                and preclose > 0 and prev_close > 0):
            theo = ((prev_close - row['cash_div']
                     + row['rights_issue_price'] * row['rights_issue_ratio'])
                    / (1 + row['share_div'] + row['rights_issue_ratio']))
            rel = abs(theo - preclose) / preclose
            item.update({'rel_err': round(rel, 6),
                         'verified': bool(rel < tol)})
        events.append(item)
    return {'status': 'ok', 'events': events}


def _write_verified(done: dict, out_dir: Path) -> None:
    """汇总 baostock 核验结果并合并写入 verified 清单。"""
    rows, all_ok, none_ok = [], [], []
    for code, rec in done.items():
        events = rec['events']
        if events and all(e['verified'] for e in events):
            all_ok.append(code)
        if events and not any(e['verified'] for e in events):
            none_ok.append(code)
        for e in events:
            if e['verified']:
                rows.append({'ts_code': code, 'ex_date': e['ex_date'],
                             'rel_err': e['rel_err']})

    print(f'baostock 通过核验事件 {len(rows)} 个 / '
          f'{len({r["ts_code"] for r in rows})} 只')
    print(f'全部事件通过（可解除存疑）: {len(all_ok)} 只')
    print(f'全部事件不通过（保持存疑）: {len(none_ok)} 只')
    _merge_verified(rows, SRC_BAOSTOCK, out_dir)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = (Path(args.store_root) if args.store_root
            else Path(__file__).resolve().parents[1] / 'data' / 'local_store')
    store_root = root

    div = pd.read_parquet(store_root / 'factors' / 'stock_dividend.parquet')
    div = div[div['ex_date'].notna()].copy()
    div['ex_date'] = pd.to_datetime(div['ex_date'])
    ev = div[(div['rights_issue_ratio'] > 0) | (div['rights_issue_price'] > 0)]
    codes = ev['ts_code'].drop_duplicates().tolist()
    if args.codes:
        wanted = {c.strip().upper() for c in args.codes.split(',') if c.strip()}
        codes = [c for c in codes if c in wanted]
    if args.limit:
        codes = codes[:args.limit]
    ev = ev[ev['ts_code'].isin(codes)]
    print(f'配股事件 {len(ev)} 个 / {len(codes)} 只')

    out_dir = store_root / 'meta' / 'anomalies'
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.source in ('gbbq', 'all'):
        gbbq_path = Path(args.gbbq_path) if args.gbbq_path else _default_gbbq_path()
        rows = _gbbq_rows(ev, gbbq_path, args.gbbq_tol)
        if rows is None:
            return 1
        _merge_verified(rows, SRC_GBBQ, out_dir)

    if args.source in ('em', 'all'):
        _merge_verified(_em_rows(ev, args.tol), SRC_EM, out_dir)

    if args.source == 'baostock' or args.source == 'all':
        try:
            import baostock as bs
        except ImportError:
            print('缺少 baostock，请安装可选依赖后重试', file=sys.stderr)
            return 1

        prog_path = out_dir / PROGRESS_NAME
        done: dict = {}
        if prog_path.exists() and not args.reset:
            done = json.loads(prog_path.read_text(encoding='utf-8'))
            print(f'断点续跑：已完成 {len(done)} 只')

        login = bs.login()
        if login.error_code != '0':
            print(f'baostock 登录失败: {login.error_msg}', file=sys.stderr)
            return 1

        pending = [c for c in codes if c not in done]
        for index, code in enumerate(pending, 1):
            done[code] = _verify_code(bs, code, div[div['ts_code'] == code], args.tol)
            if index % 50 == 0 or index == len(pending):
                prog_path.write_text(json.dumps(done, ensure_ascii=False),
                                     encoding='utf-8')
                passed = sum(1 for v in done.values()
                             if v['events'] and all(e['verified'] for e in v['events']))
                print(f'进度 {index}/{len(pending)}；全部事件通过 {passed} 只', flush=True)

        bs.logout()
        prog_path.write_text(json.dumps(done, ensure_ascii=False), encoding='utf-8')
        _write_verified(done, out_dir)
    return 0


if __name__ == '__main__':
    sys.exit(main())
