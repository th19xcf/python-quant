"""构建「已确认无分红 / 未确认覆盖」判定清单（只读原始因子与分红表，不覆盖记录）。

判定门槛（严格）：
* ``confirmed_no_dividend``：只认独立证据——分红接口对该代码返回明确空记录
  （代码在本地名录中存在、接口正常返回 0 行）。由 ``--probe`` 逐只串行取证。
* ``unconfirmed_coverage``：其余无分红记录的标的。因子全 1 或全市场年度分红表
  无该代码都不算「确认无分红」，只能是「源端未覆盖/无法区分」。

输出：``data/local_store/meta/dividend_status/no_dividend_ledger.parquet``
（字段说明见同目录 ``README.md``）。
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.atomic_write import write_parquet_atomic

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
OUT_DIR = STORE / 'meta' / 'dividend_status'
OUT_PATH = OUT_DIR / 'no_dividend_ledger.parquet'

COLUMNS = ['ts_code', 'name', 'kind', 'status', 'judgement_basis',
           'evidence_source', 'evidence_date', 'decided_at',
           'probe_attempts', 'probe_result_seq', 'rule_version']
RULE_VERSION = 'probe-2req-stable-v1'
FUND_PROBE_SOURCE = 'akshare:fund_open_fund_info_em(分红送配详情)'
#: 个股两源：东财详情（akshare 同源，``result=None`` 空结果已修正）+ 巨潮历史分红
STOCK_PROBE_SOURCE = ('em:RPT_SHAREBONUS_DET(akshare:stock_fhps_detail_em 同源，'
                      '空结果修正)+akshare:stock_dividend_cninfo')


def load_evidence(csv_path: str) -> dict[str, dict]:
    """读全量取证明细（ts_code/尝试次数/各次结果/判定/错误信息/探测时刻）。"""
    frame = pl.read_csv(csv_path, infer_schema_length=0)
    out: dict[str, dict] = {}
    for rec in frame.to_dicts():
        # 兼容两种表头：outputs 中文明细 与 work/ 断点 progress.csv（英文）
        seq_txt = rec.get('各次结果') or rec.get('result_seq') or ''
        seq = [s for s in str(seq_txt).split('/') if s]
        if not seq and (rec.get('em_seq') or rec.get('cninfo_seq')):
            # 个股探针分源记录：东财在前、巨潮在后，逐次带来源前缀
            for source_key, label in (('em_seq', 'em'), ('cninfo_seq', 'cninfo')):
                seq.extend(f'{label}:{s}' for s in
                           str(rec.get(source_key) or '').split('/') if s)
        code = rec.get('ts_code') or rec.get('代码') or ''
        out[code] = {
            'source': rec.get('source') or '',
            'date': str(rec.get('探测时刻') or rec.get('probed_at') or '')[:10],
            'attempts': int(rec.get('尝试次数') or rec.get('attempts') or len(seq)),
            'seq': seq,
            'verdict': rec.get('判定') or rec.get('verdict') or '',
            'error': rec.get('错误信息') or rec.get('error') or '',
            'rows': int(rec.get('最大返回条数') or rec.get('max_rows') or 0),
        }
    return out


def all_one_codes(path: Path, factor_col: str = 'qfq_factor') -> dict[str, int]:
    """因子全为 1.0 的代码与行数。"""
    frame = pl.read_parquet(path, columns=['ts_code', factor_col])
    grouped = frame.group_by('ts_code').agg(
        pl.len().alias('rows'),
        pl.col(factor_col).min().alias('lo'),
        pl.col(factor_col).max().alias('hi'),
        pl.col(factor_col).null_count().alias('nulls'),
    )
    hit = grouped.filter(
        (pl.col('lo') == 1.0) & (pl.col('hi') == 1.0) & (pl.col('nulls') == 0))
    return dict(zip(hit['ts_code'].to_list(), hit['rows'].to_list()))


def dividend_counts(path: Path) -> dict[str, int]:
    frame = pl.read_parquet(path, columns=['ts_code'])
    grouped = frame.group_by('ts_code').len()
    return dict(zip(grouped['ts_code'].to_list(), grouped['len'].to_list()))


def probe_fund(symbol: str) -> tuple[str, int | None, str]:
    """单只基金分红接口取证；返回 (状态, 行数, 错误)。"""
    import akshare as ak
    try:
        frame = ak.fund_open_fund_info_em(symbol=symbol, indicator='分红送配详情')
    except Exception as exc:  # noqa: BLE001 - 东财接口异常类型不稳定
        return 'error', None, f'{type(exc).__name__}: {exc}'
    return ('empty' if len(frame) == 0 else 'rows'), len(frame), ''


def probe_stock(symbol: str) -> tuple[str, int | None, str]:
    import akshare as ak
    try:
        frame = ak.stock_fhps_detail_em(symbol=symbol)
    except Exception as exc:  # noqa: BLE001
        return 'error', None, f'{type(exc).__name__}: {exc}'
    return ('empty' if len(frame) == 0 else 'rows'), len(frame), ''


def build(probe_codes: list[str], delay: float,  # noqa: C901
          evidence: dict[str, dict] | None = None,
          preserve_non_stock: bool = False,
          window_rejected: dict[str, dict] | None = None) -> pl.DataFrame:
    fund_basic = pl.read_parquet(STORE / 'meta' / 'fund_basic.parquet',
                                 columns=['ts_code', 'name', 'fund_type'])
    window_rejected = window_rejected or {}
    stock_basic = pl.read_parquet(STORE / 'meta' / 'stock_basic.parquet',
                                  columns=['ts_code', 'name'])
    fund_names = dict(zip(fund_basic['ts_code'].to_list(), fund_basic['name'].to_list()))
    fund_types = dict(zip(fund_basic['ts_code'].to_list(),
                          fund_basic['fund_type'].to_list()))
    stock_names = dict(zip(stock_basic['ts_code'].to_list(),
                           stock_basic['name'].to_list()))
    stock_status = {
        rec['ts_code']: (rec['status'], rec['delist_date'])
        for rec in pl.read_parquet(
            STORE / 'meta' / 'stock_basic.parquet',
            columns=['ts_code', 'status', 'delist_date']).to_dicts()
    }

    fund_one = all_one_codes(STORE / 'factors' / 'fund_adj_factor.parquet')
    stock_one = all_one_codes(STORE / 'factors' / 'stock_adj_factor.parquet')
    fund_div = dividend_counts(STORE / 'factors' / 'fund_dividend.parquet')
    stock_div = dividend_counts(STORE / 'factors' / 'stock_dividend.parquet')

    decided = date.today().isoformat()
    probed_evidence: dict[str, dict] = {}
    if evidence is None:
        evidence = {}
        for code in probe_codes:
            symbol = code.split('.')[0]
            is_fund = code in fund_types
            probe = probe_fund if is_fund else probe_stock
            source = FUND_PROBE_SOURCE if is_fund else STOCK_PROBE_SOURCE
            # 同一代码连查两次：单次空返回可能是接口瞬时空，两次都空才算确认无分红
            results = [probe(symbol) for _ in range(2)]
            time.sleep(delay)
            seq = [r[0] for r in results]
            counts = [r[1] for r in results if r[0] == 'rows' and r[1]]
            if counts:
                verdict = 'rows' if seq.count('rows') == 2 else 'unstable_source'
            elif 'error' in seq:
                verdict = 'error'
            else:
                verdict = 'stable_empty'
            evidence[code] = {
                'source': source, 'date': decided, 'attempts': len(results),
                'seq': seq, 'verdict': verdict,
                'error': next((r[2] for r in results if r[2]), ''),
                'rows': max(counts) if counts else 0,
            }
            probed_evidence[code] = {
                'results': [{'result': r[0], 'rows': r[1], 'error': r[2]}
                            for r in results],
                'source': source, 'date': decided,
            }

    def _probe_status(ev: dict) -> tuple[str, int | None]:
        """取证明细 → (状态键, 有记录条数)。"""
        verdict = ev.get('verdict') or ''
        if verdict == 'stable_empty':
            return 'empty', 0
        if verdict in ('rows', 'unstable_source'):
            return 'rows', ev.get('rows') or 0
        if verdict == 'error':
            return 'error', None
        return '', None

    rows: list[dict] = []
    for code, factor_rows in fund_one.items():
        if fund_div.get(code, 0) > 0:
            continue
        ev = evidence.get(code)
        result, probed_rows = _probe_status(ev) if ev else ('', None)
        if result == 'empty':
            status = 'confirmed_no_dividend'
            basis = (f'{ev["source"]} 连续 {ev["attempts"]} 次正常返回 0 行'
                     f'（各次 {"→".join(ev["seq"])}，代码存在、无分红事件）；'
                     f'本地因子 {factor_rows} 行全 1.0')
            source, ev_date = ev['source'], ev['date']
        elif result == 'rows':
            status = 'local_dividend_missing'
            stable = '' if ev.get('verdict') == 'rows' else '（一次有记录一次空，源端不稳定）'
            basis = (f'{ev["source"]} 对该代码返回 {probed_rows} 条分红记录{stable}'
                     f'（各次 {"→".join(ev["seq"])}），'
                     f'本地 fund_dividend 0 条、因子 {factor_rows} 行全 1.0，'
                     '属本地分红/因子缺失，需补采重建')
            source, ev_date = ev['source'], ev['date']
        else:  # 无取证或取证失败/不一致：一律未确认
            status = 'unconfirmed_coverage'
            if ev:
                basis = (f'{ev["source"]} 逐只取证 {ev["attempts"]} 次仍报错'
                         f'（{ev.get("error", "")[:120]}），'
                         '不能区分「确实未分红」与「源端未覆盖」')
                source, ev_date = ev['source'], ev['date']
            else:
                basis = ('本地分红表无记录；未取得逐只证据（全市场年度分红表 '
                         'akshare:fund_fh_em 无该代码不能区分「确实未分红」与「源端未覆盖」）')
                source, ev_date = ('factors/fund_dividend.parquet (akshare:fund_fh_em)',
                                   None)
        rows.append({
            'ts_code': code, 'name': fund_names.get(code) or '未知',
            'kind': fund_types.get(code) or '未知',
            'status': status, 'judgement_basis': basis,
            'evidence_source': source, 'evidence_date': ev_date, 'decided_at': decided,
            'probe_attempts': ev.get('attempts') if ev else None,
            'probe_result_seq': ('/'.join(ev.get('seq') or []) if ev else None),
            'rule_version': RULE_VERSION if ev else None,
        })

    for code, factor_rows in stock_one.items():
        if stock_div.get(code, 0) > 0:
            continue
        ev = evidence.get(code)
        result, probed_rows = _probe_status(ev) if ev else ('', None)
        src = (ev.get('source') if ev else '') or STOCK_PROBE_SOURCE
        list_status, delist_date = stock_status.get(code, ('', None))
        win = window_rejected.get(code)
        if win is not None:
            # 源端记录全部早于本代码本地行情起点（上市前/前身代码，北交所代码换段）：
            # 本代码行情区间内无分红事件，因子全 1.0 正确；外部历史记录不写入本表
            status = 'confirmed_no_dividend'
            basis = (f'{src} 返回 {win["records"]} 条记录，但除息日 '
                     f'{win["first_ex_date"]}~{win["last_ex_date"]} 全部早于本代码'
                     f'本地行情起点（{win["reason"].split("起点")[-1].strip()}），'
                     f'本代码行情区间内无分红事件，因子 {factor_rows} 行全 1.0 正确；'
                     '上市前/前身代码（含北交所代码换段）的历史记录不写入本地分红表')
            source, ev_date = src, ev['date']
        elif result == 'empty' and (not list_status or list_status != 'L'):
            # 退市股/名录未收录：两源空返不能区分「无分红」与「源端未覆盖」
            status = 'unconfirmed_coverage'
            guard = (f'该代码已于 {delist_date} 退市，源端对退市标的不覆盖'
                     if list_status else '本地 stock_basic 未收录该代码，无法核验在市状态')
            basis = (f'{src} 逐只各 2 次均正常空返（{"→".join(ev["seq"])}），'
                     f'但{guard}，空返不能区分「确实无分红」与「源端未覆盖」')
            source, ev_date = src, ev['date']
        elif result == 'empty':
            status = 'confirmed_no_dividend'
            basis = (f'{src} 两源各连续 2 次正常返回 0 行'
                     f'（{"→".join(ev["seq"])}，代码在市、无分红事件）；'
                     f'本地因子 {factor_rows} 行全 1.0')
            source, ev_date = src, ev['date']
        elif result == 'rows':
            status = 'local_dividend_missing'
            stable = '' if ev.get('verdict') == 'rows' else '（一次有记录一次空，源端不稳定）'
            basis = (f'{src} 对该代码返回 {probed_rows} 条分红记录{stable}'
                     f'（{"→".join(ev["seq"])}），'
                     f'本地 stock_dividend 0 条、因子 {factor_rows} 行全 1.0，'
                     '属本地分红/因子缺失，需补采重建')
            source, ev_date = src, ev['date']
        else:
            status = 'unconfirmed_coverage'
            if ev:
                basis = (f'{src} 逐只取证 {ev["attempts"]} 次未取得稳定结果'
                         f'（{"→".join(ev["seq"])}；{ev.get("error", "")[:120]}），'
                         '不能确认无分红')
                source, ev_date = src, ev['date']
            else:
                status = 'unconfirmed_coverage'
                basis = '本地分红表无记录；未取得逐只证据，不能确认无分红'
                source, ev_date = 'factors/stock_dividend.parquet', None
        rows.append({
            'ts_code': code, 'name': stock_names.get(code) or '未知', 'kind': '个股',
            'status': status, 'judgement_basis': basis,
            'evidence_source': source, 'evidence_date': ev_date, 'decided_at': decided,
            'probe_attempts': ev.get('attempts') if ev else None,
            'probe_result_seq': ('/'.join(ev.get('seq') or []) if ev else None),
            'rule_version': RULE_VERSION if ev else None,
        })

    frame = pl.DataFrame(rows, schema={
        'ts_code': pl.Utf8, 'name': pl.Utf8, 'kind': pl.Utf8, 'status': pl.Utf8,
        'judgement_basis': pl.Utf8, 'evidence_source': pl.Utf8,
        'evidence_date': pl.Utf8, 'decided_at': pl.Utf8,
        'probe_attempts': pl.Int64, 'probe_result_seq': pl.Utf8,
        'rule_version': pl.Utf8,
    }).sort(['status', 'kind', 'ts_code'])
    if preserve_non_stock and OUT_PATH.exists():
        # 只重建个股部分：基金侧（ETF/LOF/分级/未知）记录原样保留，不重写
        old = pl.read_parquet(OUT_PATH)
        frame = pl.concat([old.filter(pl.col('kind') != '个股'),
                           frame.filter(pl.col('kind') == '个股')],
                          how='vertical_relaxed').sort(['status', 'kind', 'ts_code'])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(frame, OUT_PATH, compression='zstd', statistics=True)
    if probed_evidence:
        probe_log = OUT_DIR / f'probe_evidence_{decided}.json'
        existing = {}
        if probe_log.exists():
            try:
                existing = json.loads(probe_log.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                existing = {}
        existing.update({k: {**v, 'probed_at': datetime.now().isoformat(timespec='seconds')}
                         for k, v in probed_evidence.items()})
        probe_log.write_text(json.dumps(existing, ensure_ascii=False, indent=2),
                             encoding='utf-8')
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description='构建无分红判定清单')
    parser.add_argument('--probe', default='', help='逐只取证的代码，逗号分隔')
    parser.add_argument('--delay', type=float, default=0.6, help='逐只取证间隔秒数')
    parser.add_argument('--evidence', default='',
                        help='全量取证明细 CSV（outputs/基金分红全量取证_*.csv）')
    parser.add_argument('--preserve-non-stock', action='store_true',
                        help='只重建个股部分，基金侧记录原样保留')
    parser.add_argument('--window-rejected', default='',
                        help='补数时被判为「事件早于本地行情起点」的汇总 CSV（ts_code/records/...）')
    args = parser.parse_args()
    codes = [c.strip() for c in args.probe.split(',') if c.strip()]
    evidence = load_evidence(args.evidence) if args.evidence else None
    window_rejected = {}
    if args.window_rejected:
        window_rejected = {
            rec['ts_code']: rec
            for rec in pl.read_csv(args.window_rejected, infer_schema_length=0).to_dicts()}
    frame = build(codes, args.delay, evidence=evidence,
                  preserve_non_stock=args.preserve_non_stock,
                  window_rejected=window_rejected)
    summary = frame.group_by(['status', 'kind']).agg(pl.len().alias('n')).sort(
        ['status', 'kind'])
    print(frame.height, 'rows ->', OUT_PATH)
    print(summary)
    print(frame.group_by('status').agg(pl.len().alias('n')).sort('status'))


if __name__ == '__main__':
    main()
