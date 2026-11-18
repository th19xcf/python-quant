"""个股分红逐只取证（DG-7 断点续跑；规则 probe-2req-stable-v1）。

背景：``akshare.stock_fhps_detail_em`` 在源端「无记录」时 ``result=None``，
会抛 ``TypeError: 'NoneType' object is not subscriptable``，无法区分
「确实无分红」与「接口故障」（688235.SH 即此因被判 error）。本脚本直接请求
同一东财接口并显式识别空结果（``success=False & code=9201``），再叠加
巨潮 ``stock_dividend_cninfo`` 作第二证据源（东财空返 ≠ 源端未覆盖，
实测 920011.BJ 东财空返而巨潮有 1 条分红）。

判定（每只至少东财 2 次、间隔 ≥2s；东财两次都空才追加巨潮 2 次）：
* ``rows``：两次都有记录；
* ``unstable_source``：一次有记录一次空，采用有记录结果；
* ``stable_empty``：东财 2 次空 + 巨潮 2 次空（两个独立源都正常返回 0 行）；
* ``error``：其余（接口报错、空返与报错混杂、单源空返另一源报错）。

只做取证，不写任何正式表。产出：``progress.csv`` / ``failed.csv`` / ``rows.csv``。
"""
from __future__ import annotations

import argparse
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / 'data' / 'local_store' / 'meta' / 'dividend_status' / 'no_dividend_ledger.parquet'
EM_URL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
EM_SOURCE = 'em:RPT_SHAREBONUS_DET(同 akshare:stock_fhps_detail_em，空结果修正)'
CNINFO_SOURCE = 'akshare:stock_dividend_cninfo(巨潮 p_sysapi1139)'
EM_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'),
    'Referer': 'https://data.eastmoney.com/',
}
RATE_LIMIT_HINTS = ('频率', '限流', '429', '403', '访问过于频繁', 'block', 'too many',
                    'forbidden', '频繁', '超时', 'timed out')
ROW_COLUMNS = [
    'ts_code', 'symbol', 'name', 'dividend_year', 'report_date', 'record_date',
    'ex_date', 'pay_date', 'cash_div', 'share_div', 'total_div',
    'rights_issue_price', 'rights_issue_ratio', 'source', 'report_period',
    'plan_notice_date', 'notice_date', 'progress', 'total_shares', 'fetched_at',
]
PROGRESS_COLS = ['ts_code', 'name', 'kind', 'em_seq', 'cninfo_seq', 'rows_em',
                 'rows_cninfo', 'verdict', 'error', 'elapsed_sec', 'probed_at']
CNINFO_EMPTY_KEYS = {'实施方案公告日期', '派息比例', '送股比例', '转增比例'}


def _d10(value: Any) -> float | None:
    """「每 10 股」比例 → 每股；缺失/非数按 None（不臆造 0）。"""
    if value is None or value == '':
        return None
    try:
        return float(value) / 10.0
    except (TypeError, ValueError):
        return None


def _date10(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)[:10]
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


def fetch_em(symbol: str) -> tuple[str, list[dict], str]:
    """东财分红送配详情：返回 (rows|empty|error, 原始记录, 错误)。"""
    params = {
        'sortColumns': 'REPORT_DATE', 'sortTypes': '-1', 'pageSize': '500',
        'pageNumber': '1', 'reportName': 'RPT_SHAREBONUS_DET', 'columns': 'ALL',
        'quoteColumns': '', 'source': 'WEB', 'client': 'WEB',
        'filter': f'(SECURITY_CODE="{symbol}")',
    }
    try:
        resp = requests.get(EM_URL, params=params, headers=EM_HEADERS, timeout=25)
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001 - 东财异常类型不稳定
        return 'error', [], f'EM {type(exc).__name__}: {exc}'[:200]
    if resp.status_code != 200:
        return 'error', [], f'EM HTTP {resp.status_code}'
    result = payload.get('result')
    if result is None:
        # 9201 = 源端无该代码记录（正常空返回）；其余 code 视为接口异常
        if payload.get('code') == 9201 or payload.get('success') is True:
            return 'empty', [], ''
        return 'error', [], f'EM code={payload.get("code")} msg={payload.get("message")}'
    records = result.get('data') or []
    return ('rows' if records else 'empty'), records, ''


def fetch_cninfo(symbol: str) -> tuple[str, list[dict], str]:
    """巨潮历史分红：返回 (rows|empty|error, 记录, 错误)。

    ``stock_dividend_cninfo`` 在 ``records`` 为空时构造空 DataFrame 且无列，
    随后按列名改名抛 KeyError；据此把「已知列名 KeyError」判为空返回。
    """
    import akshare as ak  # noqa: PLC0415

    try:
        frame = ak.stock_dividend_cninfo(symbol=symbol)
    except KeyError as exc:
        if CNINFO_EMPTY_KEYS & {str(exc).strip("'\"")}:
            return 'empty', [], ''
        return 'error', [], f'CNINFO KeyError: {exc}'[:200]
    except Exception as exc:  # noqa: BLE001
        return 'error', [], f'CNINFO {type(exc).__name__}: {exc}'[:200]
    if frame is None or len(frame) == 0:
        return 'empty', [], ''
    return 'rows', pl.from_pandas(frame, include_index=False).to_dicts(), ''


def em_rows(code: str, records: list[dict], fetched_at: datetime) -> list[dict]:
    out: list[dict] = []
    for rec in records:
        report = _date10(rec.get('REPORT_DATE'))
        out.append({
            'ts_code': code,
            'symbol': rec.get('SECURITY_CODE') or code.split('.')[0],
            'name': rec.get('SECURITY_NAME_ABBR'),
            'dividend_year': report[:4] if report else None,
            'report_date': report,
            'record_date': _date10(rec.get('EQUITY_RECORD_DATE')),
            'ex_date': _date10(rec.get('EX_DIVIDEND_DATE')),
            'pay_date': None,
            'cash_div': _d10(rec.get('PRETAX_BONUS_RMB')),
            'share_div': _d10(rec.get('BONUS_IT_RATIO')),
            'total_div': None,
            'rights_issue_price': None,
            'rights_issue_ratio': None,
            'source': EM_SOURCE,
            'report_period': report,
            'plan_notice_date': _date10(rec.get('PLAN_NOTICE_DATE')),
            'notice_date': _date10(rec.get('NOTICE_DATE')),
            'progress': rec.get('ASSIGN_PROGRESS'),
            'total_shares': float(rec['TOTAL_SHARES']) if rec.get('TOTAL_SHARES') else None,
            'fetched_at': fetched_at,
            'plan_profile': rec.get('IMPL_PLAN_PROFILE'),
        })
    return out


def cninfo_rows(code: str, records: list[dict], fetched_at: datetime) -> list[dict]:
    out: list[dict] = []
    for rec in records:
        # 巨潮「报告时间」是「2023年报」这类文本，取年份并落到年末
        raw_period = str(rec.get('报告时间') or '')
        year = raw_period[:4] if raw_period[:4].isdigit() else ''
        report = f'{year}-12-31' if year else None
        cash = _d10(rec.get('派息比例'))
        share_raw = [v for v in (rec.get('送股比例'), rec.get('转增比例')) if v is not None]
        share = sum(float(v) for v in share_raw) / 10.0 if share_raw else None
        out.append({
            'ts_code': code,
            'symbol': code.split('.')[0],
            'name': None,
            'dividend_year': report[:4] if report else None,
            'report_date': report,
            'record_date': _date10(rec.get('股权登记日')),
            'ex_date': _date10(rec.get('除权日')),
            'pay_date': _date10(rec.get('派息日')),
            'cash_div': cash,
            'share_div': share,
            'total_div': None,
            'rights_issue_price': None,
            'rights_issue_ratio': None,
            'source': CNINFO_SOURCE,
            'report_period': report,
            'plan_notice_date': _date10(rec.get('实施方案公告日期')),
            'notice_date': _date10(rec.get('实施方案公告日期')),
            'progress': '实施分配',
            'total_shares': None,
            'fetched_at': fetched_at,
            'plan_profile': rec.get('实施方案分红说明'),
        })
    return out


def load_done(progress: Path) -> set[str]:
    if not progress.exists():
        return set()
    try:
        return set(pl.read_csv(progress, columns=['ts_code'])['ts_code'].to_list())
    except Exception:  # noqa: BLE001 - 半截文件按空处理
        return set()


def _append_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
    if not rows:
        return
    new = pl.DataFrame(rows)
    if path.exists():
        old = pl.read_csv(path, infer_schema_length=0)
        new = pl.concat([old, new.cast(old.schema)], how='vertical_relaxed')
    else:
        new = new.select([c for c in columns if c in new.columns])
    new.write_csv(path)


def main() -> int:  # noqa: C901 - 单文件探针脚本，分支集中在此
    ap = argparse.ArgumentParser()
    ap.add_argument('--work-dir', default=str(ROOT / 'work' / '个股分红取证_2026-11-18'))
    ap.add_argument('--interval', type=float, default=2.0, help='同代码两次请求间隔')
    ap.add_argument('--throttle', type=float, default=1.0, help='相邻代码间隔')
    ap.add_argument('--limit', type=int, default=0, help='本轮最多处理只数（0=不限）')
    args = ap.parse_args()

    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    progress, failed, rows_csv = work / 'progress.csv', work / 'failed.csv', work / 'rows.csv'

    ledger = pl.read_parquet(LEDGER)
    pool = ledger.filter((pl.col('status') == 'unconfirmed_coverage')
                         & (pl.col('kind') == '个股'))
    targets = pool.select('ts_code', 'name').sort('ts_code').to_dicts()
    done = load_done(progress)
    todo = [t for t in targets if t['ts_code'] not in done]
    print(f'清单 {len(targets)} 只，已完成 {len(done)}，本轮待办 {len(todo)}', flush=True)
    if args.limit:
        todo = todo[:args.limit]

    progress_rows: list[dict] = []
    rows_buffer: list[dict] = []
    t_all = time.time()
    abort_reason = ''
    for i, item in enumerate(todo, 1):
        code = item['ts_code']
        symbol = code.split('.')[0]
        fetched_at = datetime.now()
        t0 = time.time()
        em_seq: list[str] = []
        em_records: list[dict] = []
        err = ''
        for attempt in range(2):
            if attempt:
                time.sleep(args.interval)
            result, records, error = fetch_em(symbol)
            em_seq.append(result)
            if records:
                em_records = records
            if error:
                err = err or error
                _append_csv(failed, [{
                    'ts_code': code, 'source': 'em', 'attempt': attempt + 1,
                    'error': error, 'probed_at': fetched_at.isoformat(timespec='seconds'),
                }], ['ts_code', 'source', 'attempt', 'error', 'probed_at'])
                if any(h in error for h in RATE_LIMIT_HINTS):
                    abort_reason = error
            if abort_reason:
                break

        cn_seq: list[str] = []
        cn_records: list[dict] = []
        if not abort_reason and em_seq.count('rows') == 0:
            # 东财空返不等于源端无覆盖：追加巨潮双次取证（920011.BJ 即此情形）
            for attempt in range(2):
                if attempt:
                    time.sleep(args.interval)
                result, records, error = fetch_cninfo(symbol)
                cn_seq.append(result)
                if records:
                    cn_records = records
                if error:
                    err = err or error
                    _append_csv(failed, [{
                        'ts_code': code, 'source': 'cninfo', 'attempt': attempt + 1,
                        'error': error,
                        'probed_at': fetched_at.isoformat(timespec='seconds'),
                    }], ['ts_code', 'source', 'attempt', 'error', 'probed_at'])
                    if any(h in error for h in RATE_LIMIT_HINTS):
                        abort_reason = error
                if abort_reason:
                    break

        if em_records or cn_records:
            # 有记录优先用东财（字段全）；东财空返时才用巨潮记录
            try:
                new_rows = (em_rows(code, em_records, fetched_at) if em_records
                            else cninfo_rows(code, cn_records, fetched_at))
            except Exception as exc:  # noqa: BLE001 - 单只转换失败不中断整批
                err = err or f'ROW {type(exc).__name__}: {exc}'[:200]
                verdict_error, new_rows = True, []
            else:
                verdict_error = False
                rows_buffer.extend(new_rows)
            stable = em_seq.count('rows') == 2 or (
                em_seq.count('rows') == 0 and cn_seq.count('rows') == 2)
            verdict = 'error' if verdict_error else (
                'rows' if stable else 'unstable_source')
        elif 'error' in em_seq or 'error' in cn_seq:
            verdict = 'error'
        elif em_seq.count('empty') == 2 and cn_seq.count('empty') == 2:
            verdict = 'stable_empty'
        else:
            verdict = 'error'

        progress_rows.append({
            'ts_code': code, 'name': item['name'], 'kind': '个股',
            'em_seq': '/'.join(em_seq), 'cninfo_seq': '/'.join(cn_seq),
            'rows_em': len(em_records), 'rows_cninfo': len(cn_records),
            'verdict': verdict, 'error': err[:200],
            'elapsed_sec': round(time.time() - t0, 2),
            'probed_at': fetched_at.isoformat(timespec='seconds'),
        })

        if i % 5 == 0 or i == len(todo) or abort_reason:
            _append_csv(progress, progress_rows, PROGRESS_COLS)
            progress_rows = []
            if rows_buffer:
                _append_csv(rows_csv, rows_buffer, ROW_COLUMNS + ['plan_profile'])
                rows_buffer = []
            print(f'  {i}/{len(todo)} 累计 {(time.time() - t_all) / 60:.1f} 分钟', flush=True)
            if abort_reason:
                print(f'[ABORT] 限流/封禁信号：{abort_reason}', flush=True)
                break
        time.sleep(args.throttle)

    _append_csv(progress, progress_rows, PROGRESS_COLS)
    _append_csv(rows_csv, rows_buffer, ROW_COLUMNS + ['plan_profile'])

    if abort_reason:
        print('[ABORT] 已停止，未加压重试；用同一命令续跑即可', flush=True)
        return 3
    frame = pl.read_csv(progress)
    print('累计判定分布：', flush=True)
    print(frame.group_by('verdict').len().sort('verdict'), flush=True)
    print(f'总耗时 {(time.time() - t_all) / 60:.1f} 分钟', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
