#!/usr/bin/env python3
"""构建申万一级行业指数（801xxx 段）日线并入库 quotes/index/sh/。

数据源：申万宏源研究官网 ``/institute-sw/api/index_publish/trend/``（免费、无 token），
单次返回该指数全历史 OHLC，无需分页。单位换算：成交量 亿股→股、成交额 亿元→元（×1e8）。

口径（2026-11-19 与用户确认）：
* 落位 ``quotes/index/sh/8010xx.SH.parquet``，列契约与既有指数分片一致；
* 源端 OHLC 只到 2026-11-09，缺口 11-10~11-18 不伪造，manifest 记 ``tail_gap``；
* 空返回一律按失败处理（源端对不存在的代码返回 code=200 + data=[]，不可当「无数据」）；
* ``--re-pull`` 幂等：只追加库内没有的新日期，既有行一律不改。

用法::

    python tools/build_sw_industry_index.py --stage-only   # 只构建候选并对拍
    python tools/build_sw_industry_index.py --promote      # 对拍通过后原子替换 + 回填 meta
    python tools/build_sw_industry_index.py --re-pull      # 源端补齐后追加新日期
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
import requests
import urllib3

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.quote_schema import QUOTE_COLUMNS, assert_quote_schema  # noqa: E402

urllib3.disable_warnings()

STORE = ROOT / 'data' / 'local_store'
QUOTES_INDEX = STORE / 'quotes' / 'index' / 'sh'
META = STORE / 'meta'
TREND_URL = 'https://www.swsresearch.com/institute-sw/api/index_publish/trend/'
ANA_URL = 'https://www.swsresearch.com/institute-sw/api/index_analysis/index_analysis_report/'
HEADERS = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')}
YI = 1e8  # 亿股→股、亿元→元
COLS = QUOTE_COLUMNS['index']

# 申万一级行业 31 只（2026-11-19 实测 akshare sw_index_first_info 一致）
ROSTER: dict[str, str] = {
    '801010': '农林牧渔', '801030': '基础化工', '801040': '钢铁', '801050': '有色金属',
    '801080': '电子', '801110': '家用电器', '801120': '食品饮料', '801130': '纺织服饰',
    '801140': '轻工制造', '801150': '医药生物', '801160': '公用事业', '801170': '交通运输',
    '801180': '房地产', '801200': '商贸零售', '801210': '社会服务', '801230': '综合',
    '801710': '建筑材料', '801720': '建筑装饰', '801730': '电力设备', '801740': '国防军工',
    '801750': '计算机', '801760': '传媒', '801770': '通信', '801780': '银行',
    '801790': '非银金融', '801880': '汽车', '801890': '机械设备', '801950': '煤炭',
    '801960': '石油石化', '801970': '环保', '801980': '美容护理',
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _get(url: str, params: dict, tries: int = 3) -> dict:
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=60)
            if r.status_code != 200:
                last = f'HTTP {r.status_code}'
            else:
                return r.json()
        except Exception as exc:  # 传输层失败（非 JSON / 超时）一律当故障
            last = f'{type(exc).__name__}: {exc}'
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f'{url} {params} 三次失败，最后一次 {last}')


def fetch_trend(code: str, pause: float) -> list[dict]:
    """取单只全历史；空数组按失败处理（源端对未知代码也返回 code=200 + []）。"""
    js = _get(TREND_URL, {'swindexcode': code, 'period': 'DAY'})
    if str(js.get('code')) != '200':
        raise RuntimeError(f'{code} 源端 code={js.get("code")} message={js.get("message")}')
    data = js.get('data') or []
    if not data:
        raise RuntimeError(f'{code} 返回空数组（源端对未知代码亦如此），按失败处理')
    time.sleep(pause)
    return data


def to_frame(code: str, rows: list[dict]) -> pl.DataFrame:
    """源行 → 正式列契约；change/pct 由 close/pre_close 派生以保证自洽。"""
    recs = []
    for r in rows:
        d = str(r['bargaindate'])[:10]
        close = float(r['closeindex'])
        recs.append({
            'date': datetime.strptime(d, '%Y-%m-%d').date(),
            'ts_code': f'{code}.SH',
            'open': float(r['openindex']),
            'high': float(r['maxindex']),
            'low': float(r['minindex']),
            'close': close,
            'volume': float(r['bargainamount']) * YI,
            'amount': float(r['bargainsum']) * YI,
            '_markup': float(r['markup']),
        })
    df = pl.DataFrame(recs).sort('date')
    # 首行无昨收：与既有指数分片口径一致取当日收盘（change/pct 为 0，保持自洽）
    prev = df['close'].shift(1).fill_null(df['close'].head(1)[0])
    df = df.with_columns([
        prev.alias('pre_close'),
        (df['close'] - prev).round(2).alias('change'),
    ])
    df = df.with_columns([
        pl.when(pl.col('pre_close') == 0).then(0.0)
        .otherwise(pl.col('change') / pl.col('pre_close') * 100).alias('pct_chg'),
        pl.lit(1.0).alias('qfq_factor'),
        pl.lit(1.0).alias('hfq_factor'),
    ])
    return df.select(COLS)


def fetch_analysis(day: str) -> dict[str, dict]:
    js = _get(ANA_URL, {'page': 1, 'page_size': 500, 'index_type': '一级行业',
                        'start_date': day, 'end_date': day, 'type': 'DAY',
                        'swindexcode': 'all'})
    data = js.get('data') or {}
    out = {}
    for r in data.get('results') or []:
        if str(r['bargaindate'])[:10] == day:
            out[str(r['swindexcode'])] = {'close': float(r['closeindex']),
                                          'pct': float(r['markup'])}
    time.sleep(0.3)
    return out


def local_calendar() -> list[date]:
    cal = pl.read_parquet(QUOTES_INDEX / '000001.SH.parquet', columns=['date'])
    return sorted(cal['date'].to_list())


def verify(frames: dict[str, pl.DataFrame], raw: dict[str, list[dict]],
           cal: list[date]) -> dict:
    """逐只对拍：自洽 1e-12 级、源端两点逐值、analysis 端点交叉、日期集合差。"""
    report = {'per_index': {}, 'checks': {}}
    max_self_change = 0.0
    max_self_pct = 0.0
    max_pct_src = 0.0
    max_pct_ana = 0.0
    close_src_mismatch = 0
    close_ana_mismatch = 0
    missing_days_total = 0

    ana_cache: dict[str, dict[str, dict]] = {}
    for code in frames:
        df = frames[code]
        tp1 = df['date'][-1].isoformat()
        by_date = {str(r['bargaindate'])[:10]: r for r in raw[code]}
        # 第二时点：2014-12-31；2021-12-13 才设立的 3 只改用 2022-12-30
        tp2 = next((t for t in ('2014-12-31', '2022-12-30') if t in by_date), '2014-12-31')
        # 自洽
        sc = (df['change'] - (df['close'] - df['pre_close'])).abs().max()
        sp = df.select(
            (pl.col('pct_chg') - pl.when(pl.col('pre_close') == 0).then(0.0)
             .otherwise(pl.col('change') / pl.col('pre_close') * 100)).abs().max()).item()
        max_self_change = max(max_self_change, float(sc))
        max_self_pct = max(max_self_pct, float(sp))
        # 源端两点
        by_date = {str(r['bargaindate'])[:10]: r for r in raw[code]}
        diffs = {}
        for tp in (tp1, tp2):
            if tp not in by_date:
                diffs[tp] = {'present': False}
                continue
            row = df.filter(pl.col('date') == datetime.strptime(tp, '%Y-%m-%d').date())
            if row.height != 1:
                diffs[tp] = {'present': False}
                continue
            sr = by_date[tp]
            dc = abs(float(row['close'][0]) - float(sr['closeindex']))
            dp = abs(float(row['pct_chg'][0]) - float(sr['markup']))
            max_pct_src = max(max_pct_src, dp)
            if dc > 1e-9:
                close_src_mismatch += 1
            diffs[tp] = {'present': True, 'close_diff': dc, 'pct_diff': dp}
        # analysis 交叉
        cross = {}
        for tp in (tp1, tp2):
            if tp not in ana_cache:
                ana_cache[tp] = fetch_analysis(tp)
            a = ana_cache[tp].get(code)
            if not a:
                cross[tp] = {'present': False}
                continue
            row = df.filter(pl.col('date') == datetime.strptime(tp, '%Y-%m-%d').date())
            if row.height != 1:
                cross[tp] = {'present': False}
                continue
            dc = abs(float(row['close'][0]) - a['close'])
            dp = abs(float(row['pct_chg'][0]) - a['pct'])
            max_pct_ana = max(max_pct_ana, dp)
            if dc > 1e-9:
                close_ana_mismatch += 1
            cross[tp] = {'present': True, 'close_diff': dc, 'pct_diff': dp}
        # 日期集合差（只报不补）
        d0, d1 = df['date'][0], df['date'][-1]
        want = {d for d in cal if d0 <= d <= d1}
        have = set(df['date'].to_list())
        missing = sorted(want - have)
        missing_days_total += len(missing)
        report['per_index'][code] = {
            'name': ROSTER[code], 'rows': df.height,
            'first': d0.isoformat(), 'last': d1.isoformat(), 'tp2': tp2,
            'src': diffs, 'ana': cross,
            'missing_trade_days': len(missing),
            'missing_sample': [d.isoformat() for d in missing[:5]],
        }

    report['checks'] = {
        'self_change_max': max_self_change,
        'self_pct_max': max_self_pct,
        'src_pct_max': max_pct_src,
        'ana_pct_max': max_pct_ana,
        'src_close_mismatch': close_src_mismatch,
        'ana_close_mismatch': close_ana_mismatch,
        'missing_trade_days_total': missing_days_total,
        'indices': len(frames),
        'rows_total': sum(df.height for df in frames.values()),
    }
    return report


def write_sidecar(parquet: Path, ts_code: str, df: pl.DataFrame, tail_gap: dict) -> None:
    path = parquet.with_name(f'{ts_code}.manifest.json')
    payload = {
        'path': f'quotes/index/sh/{ts_code}.parquet',
        'ts_code': ts_code,
        'dataset': (f'{ts_code} 申万一级行业指数日线（{df.height:,} 行 / '
                    f'{df["date"][0]}~{df["date"][-1]}）：一行 = 一个交易日的 OHLC、'
                    '成交量、成交额与复权因子。'),
        'source': ('申万宏源研究官网 /institute-sw/api/index_publish/trend/（period=DAY，'
                   '单次返回全历史，无需分页）；成交量 亿股×1e8→股、成交额 亿元×1e8→元；'
                   'produced_by tools/build_sw_industry_index.py'),
        'authority': '本分片是申万行业指数（801 段）在本地 quotes/index 的唯一正式层；'
                     '列表元数据登记于 meta/index_basic.parquet',
        'semantics': ('一行 = 一个交易日；pre_close 取前一交易日 close（首行取当日 close），'
                      'change 与 pct_chg 由 close/pre_close 派生；指数不复权，'
                      'qfq_factor/hfq_factor 恒为 1.0'),
        'rows': df.height,
        'bytes': parquet.stat().st_size,
        'sha256': _sha256(parquet),
        'columns': list(df.columns),
        'stats': {
            'rows': df.height,
            'start': str(df['date'][0]),
            'end': str(df['date'][-1]),
            'dup_dates': df.height - df['date'].n_unique(),
            'date_sorted': df['date'].is_sorted(),
            'null_cells': int(sum(df[c].null_count() for c in df.columns)),
        },
        'index_basic': {'name': ROSTER[ts_code.split('.')[0]], 'publisher': '申万宏源',
                        'index_type': '行业指数', 'category': '申万一级行业'},
        'tail_gap': tail_gap,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
    }
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def update_index_basic(frames: dict[str, pl.DataFrame], stage: Path) -> dict:
    path = META / 'index_basic.parquet'
    manifest_path = META / 'index_basic.manifest.json'
    old = pl.read_parquet(path)
    scheme = dict(old.schema)
    codes = {f'{c}.SH' for c in ROSTER}
    base = old.filter(~pl.col('ts_code').is_in(list(codes)))
    nid = int(old['id'].max()) + 1
    now = datetime.now()
    rows = []
    for code, name in ROSTER.items():
        first = frames[code]['date'][0]
        rows.append({
            'id': nid, 'ts_code': f'{code}.SH', 'name': name, 'market': 'SH',
            'publisher': '申万宏源', 'index_type': '行业指数', 'category': '申万一级行业',
            'base_date': first, 'base_point': None,
            'list_date': first, 'weight_rule': '市值加权',
            'desc': f'申万一级行业指数({code}.SH)，来源申万宏源研究官网 trend 接口，'
                    'tools/build_sw_industry_index.py 构建',
            'created_at': now, 'updated_at': now,
        })
        nid += 1
    new_rows = pl.DataFrame(rows).select(
        [pl.col(c).cast(scheme[c]) for c in scheme])
    merged = pl.concat([base, new_rows]).sort('ts_code')

    stage.mkdir(parents=True, exist_ok=True)
    tmp_parquet = stage / 'index_basic.parquet'
    merged.write_parquet(tmp_parquet, compression='zstd', statistics=True)
    os.replace(tmp_parquet, path)

    man = json.loads(manifest_path.read_text(encoding='utf-8'))
    man.update({
        'rows': merged.height, 'codes': merged.height,
        'sha256': _sha256(path),
        'built_at': now.isoformat(timespec='seconds'),
        'sw_industry_append': {
            'codes': len(ROSTER), 'rows_added': len(ROSTER),
            'index_type': '行业指数', 'category': '申万一级行业', 'publisher': '申万宏源',
            'note': '801 段申万一级行业指数；id 续号，既有行零改动',
            'produced_by': 'tools/build_sw_industry_index.py',
            'at': now.isoformat(timespec='seconds'),
        },
    })
    tmp_json = manifest_path.with_suffix('.json.tmp')
    tmp_json.write_text(json.dumps(man, ensure_ascii=False, indent=2) + '\n',
                        encoding='utf-8')
    os.replace(tmp_json, manifest_path)
    return {'rows_before': old.height, 'rows_after': merged.height,
            'added': len(ROSTER), 'ts_codes': sorted(codes)[:3]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--stage-only', action='store_true', help='只构建候选并对拍，不写正式库')
    ap.add_argument('--promote', action='store_true', help='对拍通过后原子替换并回填 meta')
    ap.add_argument('--re-pull', action='store_true', help='幂等续跑：只追加库内没有的新日期')
    ap.add_argument('--pause', type=float, default=0.3, help='请求间隔秒（单连接串行）')
    args = ap.parse_args(argv)
    if not (args.stage_only or args.promote or args.re_pull):
        ap.error('至少指定 --stage-only / --promote / --re-pull 之一')

    stamp = datetime.now().strftime('%Y%m%d')
    stage = STORE / '_staging' / f'sw_index_{stamp}'
    stage.mkdir(parents=True, exist_ok=True)

    raw = {c: fetch_trend(c, args.pause) for c in ROSTER}
    frames = {c: to_frame(c, raw[c]) for c in ROSTER}
    missing_roster = [c for c in ROSTER if frames[c].is_empty()]
    if missing_roster:
        raise SystemExit(f'以下代码无数据（按失败处理）：{missing_roster}')

    # 候选落盘
    for c, df in frames.items():
        assert_quote_schema('index', df)
        df.write_parquet(stage / f'{c}.SH.parquet', compression='zstd', statistics=True)

    cal = local_calendar()
    last_cal = cal[-1]
    rep = verify(frames, raw, cal)

    # 尾部缺口（源端 OHLC 落后本地日历）
    tail_dates = []
    for c, df in frames.items():
        d1 = df['date'][-1]
        tail_dates = [d.isoformat() for d in cal if d > d1]
    tail_gap = {
        'start': tail_dates[0] if tail_dates else None,
        'end': tail_dates[-1] if tail_dates else None,
        'rows': len(tail_dates),
        'dates': tail_dates,
        'reason': '源端 trend 接口 OHLC 只更新到此为止；analysis 端点虽有收盘但无 open/high/low，'
                  '不伪造缺失 OHLC',
        're_pull': 'python tools/build_sw_industry_index.py --re-pull',
    }
    rep['tail_gap'] = tail_gap
    rep['local_calendar_last'] = last_cal.isoformat()
    rep['stage'] = str(stage)
    (stage / 'verify_report.json').write_text(
        json.dumps(rep, ensure_ascii=False, indent=1), encoding='utf-8')

    chk = rep['checks']
    print(f"stage={stage}")
    print(f"index={chk['indices']} rows={chk['rows_total']} "
          f"first={min(v['first'] for v in rep['per_index'].values())} "
          f"last={max(v['last'] for v in rep['per_index'].values())}")
    print(f"self |change-(close-pre)|max={chk['self_change_max']:.2e} "
          f"|pct-recalc|max={chk['self_pct_max']:.2e}")
    print(f"src^close_mismatch={chk['src_close_mismatch']} pct_max={chk['src_pct_max']:.4f} "
          f"ana^close_mismatch={chk['ana_close_mismatch']} pct_max={chk['ana_pct_max']:.4f} "
          f"missing_days={chk['missing_trade_days_total']}")
    print(f"tail_gap={tail_gap['start']}~{tail_gap['end']} ({tail_gap['rows']} 个交易日)")

    if chk['self_pct_max'] > 1e-12 or chk['src_close_mismatch'] or chk['ana_close_mismatch']:
        raise SystemExit('对拍未通过，停止入库')

    if not (args.promote or args.re_pull):
        return 0

    # 原子替换 + 侧车 + 覆盖度索引
    from src.data.local_store.store import LocalQuoteStore
    store = LocalQuoteStore(STORE)
    written = {}
    for c, df in frames.items():
        ts_code = f'{c}.SH'
        path = QUOTES_INDEX / f'{ts_code}.parquet'
        if args.re_pull and path.is_file():
            old = pl.read_parquet(path)
            last_old = old['date'][-1]
            add = df.filter(pl.col('date') > last_old)
            if add.height == 0:
                written[ts_code] = 0
                continue
            merged = pl.concat([old, add.select(COLS)]).sort('date')
            merged = merged.with_columns(
                (pl.col('close') - pl.col('pre_close')).round(2).alias('change'))
            merged = merged.with_columns(
                pl.when(pl.col('pre_close') == 0).then(0.0)
                .otherwise(pl.col('change') / pl.col('pre_close') * 100).alias('pct_chg'))
            df = merged
        rows = store.write_daily('index', ts_code, df)
        write_sidecar(QUOTES_INDEX / f'{ts_code}.parquet', ts_code, df, tail_gap)
        written[ts_code] = rows
    store.flush_meta()
    ib = update_index_basic(frames, stage)
    print(f"promoted={sum(1 for v in written.values() if v)} "
          f"skipped={sum(1 for v in written.values() if not v)} "
          f"index_basic {ib['rows_before']}->{ib['rows_after']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
