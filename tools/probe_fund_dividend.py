"""基金分红全量取证（DG-7 断点续跑）。

对未确认清单里的基金逐只串行取证：每只 2 次请求（间隔 ≥2s），
结果写 progress.csv / failed.csv，可中断续跑；有记录的原始行落到 rows.csv。
只做取证，不写任何正式表。
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / 'data' / 'local_store' / 'meta' / 'dividend_status' / 'no_dividend_ledger.parquet'
PROGRESS_COLS = ['ts_code', 'name', 'kind', 'attempts', 'result_seq', 'max_rows',
                 'verdict', 'elapsed_sec', 'probed_at']
RATE_LIMIT_HINTS = ('频率', '限流', '429', '403', '访问过于频繁', 'block', 'too many',
                    'forbidden', '频繁')


def probe_once(symbol: str) -> tuple[str, int | None, str, float, pl.DataFrame | None]:
    import akshare as ak
    t0 = time.time()
    try:
        frame = ak.fund_open_fund_info_em(symbol=symbol, indicator='分红送配详情')
    except Exception as exc:  # noqa: BLE001 - 东财异常类型不稳定
        return 'error', None, f'{type(exc).__name__}: {exc}'[:200], time.time() - t0, None
    n = len(frame)
    return ('rows' if n else 'empty'), n, '', time.time() - t0, (
        pl.from_pandas(frame) if n else None)


def load_done(progress: Path) -> set[str]:
    if not progress.exists():
        return set()
    try:
        return set(pl.read_csv(progress, columns=['ts_code'])['ts_code'].to_list())
    except Exception:  # noqa: BLE001 - 半截文件按空处理
        return set()


def main() -> int:  # noqa: C901 - 单文件探针脚本，分支集中在此
    ap = argparse.ArgumentParser()
    ap.add_argument('--work-dir', default=str(ROOT / 'work' / '基金分红全量取证_2026-11-18'))
    ap.add_argument('--interval', type=float, default=2.0)
    ap.add_argument('--throttle', type=float, default=0.5)
    ap.add_argument('--limit', type=int, default=0, help='本轮最多处理只数（0=不限）')
    args = ap.parse_args()

    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    progress, failed = work / 'progress.csv', work / 'failed.csv'
    rows_csv = work / 'rows.csv'

    ledger = pl.read_parquet(LEDGER)
    pool = ledger.filter((pl.col('status') == 'unconfirmed_coverage')
                         & pl.col('kind').is_in(['ETF', 'LOF', '分级', '未知']))
    targets = pool.select('ts_code', 'name', 'kind').sort('ts_code').to_dicts()
    done = load_done(progress)
    todo = [t for t in targets if t['ts_code'] not in done]
    print(f'清单 {len(targets)} 只，已完成 {len(done)}，本轮待办 {len(todo)}', flush=True)
    if args.limit:
        todo = todo[:args.limit]

    rows_buffer: list[dict] = []
    progress_rows: list[dict] = []
    t_all = time.time()
    abort_reason = ''
    for i, item in enumerate(todo, 1):
        symbol = item['ts_code'].split('.')[0]
        seq: list[str] = []
        max_rows = 0
        elapsed = 0.0
        error = ''
        for attempt in range(2):
            if attempt:
                time.sleep(args.interval)
            result, n, err, cost, frame = probe_once(symbol)
            seq.append(result)
            elapsed += cost
            if n:
                max_rows = max(max_rows, n)
                for rec in frame.to_dicts():
                    rows_buffer.append({'ts_code': item['ts_code'],
                                        **{k: str(v) for k, v in rec.items()}})
            if err:
                with failed.open('a', encoding='utf-8') as fh:
                    if fh.tell() == 0:
                        fh.write('ts_code,attempt,error_type,error_msg,probed_at\n')
                    etype = err.split(':', 1)[0]
                    fh.write(f'{item["ts_code"]},{attempt + 1},'
                             f'{etype},"{err.replace(chr(34), chr(39))}",'
                             f'{datetime.now().isoformat(timespec="seconds")}\n')
                if any(h in err for h in RATE_LIMIT_HINTS):
                    abort_reason = err
                    break

        has_rows = max_rows > 0
        if has_rows:
            verdict = 'rows' if seq.count('rows') == 2 else 'unstable_source'
        elif 'error' in seq:
            verdict = 'error'
        else:
            verdict = 'stable_empty'
        progress_rows.append({
            'ts_code': item['ts_code'], 'name': item['name'], 'kind': item['kind'],
            'attempts': len(seq), 'result_seq': '/'.join(seq), 'max_rows': max_rows,
            'verdict': verdict, 'elapsed_sec': round(elapsed, 2),
            'probed_at': datetime.now().isoformat(timespec='seconds'),
        })

        if i % 25 == 0 or i == len(todo) or abort_reason:
            new = pl.DataFrame(progress_rows)
            if progress.exists():
                old = pl.read_csv(progress, infer_schema_length=0)
                new = pl.concat([old, new.cast(old.schema)], how='vertical_relaxed')
            new.write_csv(progress)
            progress_rows = []
            if rows_buffer:
                rnew = pl.DataFrame(rows_buffer)
                if rows_csv.exists():
                    oldr = pl.read_csv(rows_csv, infer_schema_length=0)
                    rnew = pl.concat([oldr, rnew.cast(oldr.schema)], how='vertical_relaxed')
                rnew.write_csv(rows_csv)
                rows_buffer = []
            print(f'  {i}/{len(todo)} 累计 {(time.time()-t_all)/60:.1f} 分钟', flush=True)
            if abort_reason:
                print(f'[ABORT] 限流/封禁信号：{abort_reason}', flush=True)
                break
        time.sleep(args.throttle)

    # 收尾落盘
    if progress_rows:
        new = pl.DataFrame(progress_rows)
        if progress.exists():
            old = pl.read_csv(progress, infer_schema_length=0)
            new = pl.concat([old, new.cast(old.schema)], how='vertical_relaxed')
        new.write_csv(progress)
    if rows_buffer:
        rnew = pl.DataFrame(rows_buffer)
        if rows_csv.exists():
            oldr = pl.read_csv(rows_csv, infer_schema_length=0)
            rnew = pl.concat([oldr, rnew.cast(oldr.schema)], how='vertical_relaxed')
        rnew.write_csv(rows_csv)

    if abort_reason:
        print('[ABORT] 已停止，未加压重试；用同一命令续跑即可', flush=True)
        return 3
    frame = pl.read_csv(progress)
    print('本轮完成', frame.height, '只；判定分布：', flush=True)
    print(frame.group_by('verdict').len().sort('verdict'), flush=True)
    print(f'总耗时 {(time.time()-t_all)/60:.1f} 分钟', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
