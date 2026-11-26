#!/usr/bin/env python3
"""组合层规则 + 随机基线对照（只读 data/local_store，产物写本目录）

候选生成：REV20 = -(C/C[-20]-1)，后复权价，横截面排序取前 1.5N。
组合构建：N=20 等权、单票 8%、申万一级行业 30%、缓冲带 1.5N（掉出才卖）、
          周频（每 5 个交易日）调仓、T+1 开盘成交、往返成本 0.13%。
随机基线：把评分替换为随机数，其余规则完全相同，重复 100 次取分布。
"""
from __future__ import annotations

import json
import math
import os
import time
from glob import glob

import numpy as np
import polars as pl

ROOT = r'e:\code\python\quant'
DATA = os.path.join(ROOT, 'data', 'local_store')
OUT = os.path.join(ROOT, 'work', '组合层规则与随机基线_2026-11-22')
os.makedirs(OUT, exist_ok=True)

COST = 0.00065          # 单边，往返 0.13%
INIT_CAP = 1_000_000.0
N_HOLD = 20
N_CAND = 30             # 1.5N
BUFFER = 30             # 1.5N
REBAL = 5               # 周频（每 5 个交易日）
MAX_W = 0.08            # 单票上限
IND_CAP = 0.30          # 行业上限
START = np.datetime64('2010-01-04')
PRELOAD = np.datetime64('2009-01-01')
N_REP = 100


def log(*a):
    print(*a, flush=True)


# ------------------------------------------------------------------ 数据
def load_limit_panel():
    files = sorted(glob(os.path.join(DATA, 'meta', 'limit', 'daily', 'limit_daily_*.parquet')))
    log(f'limit files={len(files)}')
    lf = (pl.scan_parquet(files)
          .filter(pl.col('trade_date') >= pl.lit(__import__('datetime').date(2009, 1, 1)))
          .select(['trade_date', 'ts_code', 'can_buy', 'can_sell', 'suspended',
                   'listing_days', 'st_status']))
    return lf.collect()


def build_matrices(lim: pl.DataFrame):
    dates = np.sort(lim['trade_date'].unique().to_numpy().astype('datetime64[D]').astype(np.int64))
    codes = sorted(lim['ts_code'].unique().to_list())
    D, S = len(dates), len(codes)
    log(f'calendar {D} days {dates[0]}..{dates[-1]}  codes={S}')

    cdf = pl.DataFrame({'ts_code': codes, 'ci': np.arange(S, dtype=np.int32)})
    ddf = pl.DataFrame({'trade_date': dates.astype('datetime64[D]'),
                        'di': np.arange(D, dtype=np.int32)})
    ddf = ddf.with_columns(pl.col('trade_date').cast(pl.Date))
    lim = lim.join(cdf, on='ts_code', how='left').join(ddf, on='trade_date', how='left')
    lim = lim.filter(pl.col('ci').is_not_null() & pl.col('di').is_not_null())

    ci = lim['ci'].to_numpy().astype(np.int64)
    di = lim['di'].to_numpy().astype(np.int64)
    flat = ci * D + di

    can_buy = np.zeros(S * D, dtype=bool)
    can_sell = np.zeros(S * D, dtype=bool)
    has_row = np.zeros(S * D, dtype=bool)
    listed_ok = np.zeros(S * D, dtype=bool)
    is_st = np.zeros(S * D, dtype=bool)
    can_buy[flat] = lim['can_buy'].fill_null(False).to_numpy()
    can_sell[flat] = lim['can_sell'].fill_null(False).to_numpy()
    has_row[flat] = True
    listed_ok[flat] = lim['listing_days'].fill_null(0).to_numpy() >= 60
    is_st[flat] = (lim['st_status'].fill_null('non_st') != 'non_st').to_numpy()

    shape = (S, D)
    L = {
        'dates': dates, 'codes': codes, 'D': D, 'S': S,
        'can_buy': can_buy.reshape(shape), 'can_sell': can_sell.reshape(shape),
        'has_row': has_row.reshape(shape), 'listed_ok': listed_ok.reshape(shape),
        'is_st': is_st.reshape(shape),
        'O': np.full(shape, np.nan, np.float32),
        'C': np.full(shape, np.nan, np.float32),
        'REV': np.full(shape, np.nan, np.float32),
        'IND': np.full(shape, -1, np.int32),
    }
    return L


def fill_quotes(L):
    D = L['D']
    code_idx = {c: k for k, c in enumerate(L['codes'])}
    files = []
    for mkt in ('sh', 'sz', 'bj'):
        files.extend(sorted(glob(os.path.join(DATA, 'quotes', 'stock', mkt, '*.parquet'))))
    log(f'quote files={len(files)}')
    t0 = time.time()
    n_ok = 0
    for k, path in enumerate(files):
        try:
            df = pl.read_parquet(path, columns=['date', 'ts_code', 'open', 'close', 'hfq_factor'])
        except Exception:
            continue
        if df.height < 140:
            continue
        code = str(df['ts_code'][0])
        sidx = code_idx.get(code)
        if sidx is None:
            continue
        d = df['date'].to_numpy().astype('datetime64[D]').astype(np.int64)
        m = d >= np.int64(PRELOAD.astype('datetime64[D]').astype(np.int64))
        if m.sum() < 140:
            continue
        d = d[m]
        pos = np.searchsorted(L['dates'], d)
        pos = np.clip(pos, 0, D - 1)
        good = L['dates'][pos] == d
        if good.sum() < 140:
            continue
        pos = pos[good]
        fac = df['hfq_factor'].to_numpy().astype(np.float64)[m][good]
        fac = np.where(np.isfinite(fac) & (fac > 0), fac, 1.0)
        o = df['open'].to_numpy().astype(np.float64)[m][good] * fac
        c = df['close'].to_numpy().astype(np.float64)[m][good] * fac
        o = np.where(np.isfinite(o) & (o > 0), o, np.nan)
        c = np.where(np.isfinite(c) & (c > 0), c, np.nan)

        n = c.size
        rev = np.full(n, np.nan)
        if n > 20:
            with np.errstate(invalid='ignore', divide='ignore'):
                rev[20:] = -(c[20:] / c[:-20] - 1.0)

        L['O'][sidx, pos] = o
        L['C'][sidx, pos] = c
        L['REV'][sidx, pos] = rev
        n_ok += 1
        if (k + 1) % 1500 == 0:
            log(f'  quotes {k+1}/{len(files)}  ok={n_ok}  {time.time()-t0:.0f}s')
    log(f'quotes filled {n_ok} in {time.time()-t0:.0f}s')


def fill_industry(L):
    """申万一级 PIT：按 in_date/out_date 区间逐行填充 (ci, di) 行业 id。"""
    path = os.path.join(DATA, 'meta', 'industry_pit', 'industry_pit_sw.parquet')
    df = pl.read_parquet(path, columns=['ts_code', 'industry_l1', 'in_date', 'out_date'])
    df = df.filter(pl.col('industry_l1').is_not_null())
    code_idx = {c: k for k, c in enumerate(L['codes'])}
    ind_ids, ind_map = {}, {}
    D = L['D']
    dates = L['dates']
    filled = 0
    for ts, ind, din, dout in zip(df['ts_code'].to_list(), df['industry_l1'].to_list(),
                                  df['in_date'].to_list(), df['out_date'].to_list()):
        ci = code_idx.get(ts)
        if ci is None:
            continue
        if ind not in ind_map:
            ind_map[ind] = len(ind_map)
        a = int(np.datetime64(din, 'D').astype(np.int64))
        b = int(dates[-1]) if dout is None else int(np.datetime64(dout, 'D').astype(np.int64))
        i0 = int(np.searchsorted(dates, a, side='left'))
        i1 = int(np.searchsorted(dates, b, side='right'))
        if i1 <= i0:
            continue
        L['IND'][ci, i0:i1] = ind_map[ind]
        filled += i1 - i0
    cov = (L['IND'] >= 0).mean()
    log(f'industry intervals={df.height} cells_filled={filled} coverage={cov:.4%}')
    return ind_map


def fill_delist(L):
    sb = pl.read_parquet(os.path.join(DATA, 'meta', 'stock_basic.parquet'),
                         columns=['ts_code', 'delist_date'])
    code_idx = {c: k for k, c in enumerate(L['codes'])}
    delist_i = {}
    for code, dd in zip(sb['ts_code'].to_list(), sb['delist_date'].to_list()):
        if dd is None:
            continue
        k = code_idx.get(code)
        if k is not None:
            delist_i[k] = int(np.datetime64(dd, 'D').astype(np.int64))
    L['delist_i'] = delist_i
    log(f'delist codes={len(delist_i)}')


# ------------------------------------------------------------------ 指标
def metrics(nav, expo, buy_amt, sell_amt, trade_rets, blocked_days, hold_days,
            sell_intents, sell_same_day, dates):
    ok = np.isfinite(nav)
    idx = np.flatnonzero(ok)
    i0, i1 = idx[0], idx[-1]
    nv = nav[i0:i1 + 1]
    years = (nv.size - 1) / 252.0
    ann = (nv[-1] / nv[0]) ** (1.0 / years) - 1.0 if years > 0 and nv[0] > 0 else np.nan
    peak = np.maximum.accumulate(nv)
    mdd = float((nv / peak - 1.0).min())
    ret = np.diff(nv) / nv[:-1]
    sharpe = float(np.nanmean(ret) / np.nanstd(ret) * math.sqrt(252)) if np.nanstd(ret) > 0 else np.nan
    avg_nav = float(np.mean(nv))
    buy_to = buy_amt / avg_nav / years if years > 0 else np.nan
    sell_to = sell_amt / avg_nav / years if years > 0 else np.nan
    rr = np.asarray(trade_rets, dtype=float)
    return dict(
        ann=ann, mdd=mdd, calmar=(ann / abs(mdd)) if mdd < 0 else np.nan, sharpe=sharpe,
        buy_turnover=buy_to, sell_turnover=sell_to, turnover=buy_to + sell_to,
        trades=int(rr.size), avg_expo=float(np.nanmean(expo[i0:i1 + 1])),
        win_rate=float((rr > 0).mean()) if rr.size else np.nan,
        max_loss=float(rr.min()) if rr.size else np.nan,
        avg_trade=float(rr.mean()) if rr.size else np.nan,
        hold_days=int(hold_days), blocked_sell_days=int(blocked_days),
        unsellable_pct=(blocked_days / hold_days) if hold_days else np.nan,
        fill_rate=1.0 - (blocked_days / hold_days) if hold_days else np.nan,
        sell_intents=int(sell_intents),
        same_day_fill=(sell_same_day / sell_intents) if sell_intents else np.nan,
        final_nav=float(nv[-1]),
    )


# ------------------------------------------------------------------ 模拟
def simulate(L, *, score='rev', seed=0, buffer_n=BUFFER, use_caps=True,
             n_hold=N_HOLD, n_cand=N_CAND, keep_trades=False):
    """score: rev | rand_iid | rand_static。其余规则完全一致。"""
    D, S = L['D'], L['S']
    O, C, IND = L['O'], L['C'], L['IND']
    can_buy, can_sell, has_row = L['can_buy'], L['can_sell'], L['has_row']
    listed_ok, is_st, dates = L['listed_ok'], L['is_st'], L['dates']
    rng = np.random.default_rng(seed)
    static_score = rng.random(S).astype(np.float32) if score == 'rand_static' else None

    start_i = int(np.searchsorted(dates, START.astype('datetime64[D]').astype(np.int64)))
    exec_days = set(range(start_i + REBAL, D, REBAL))

    cash = INIT_CAP
    pos = {}          # ci -> dict
    nav = np.full(D, np.nan)
    expo = np.zeros(D)
    trade_rets = []
    trade_rows = []
    buy_amt = sell_amt = 0.0
    blocked_days = 0
    hold_days = 0
    sell_intents = 0
    sell_same_day = 0
    cap_skip = 0
    ind_skip = 0
    unknown_ind_hold = 0
    n_rebal = 0

    def sell_ok(ci, i):
        px = O[ci, i]
        if not np.isfinite(px):
            return False
        if has_row[ci, i]:
            return bool(can_sell[ci, i])
        return px > 0

    def buy_ok(ci, i):
        px = O[ci, i]
        if not np.isfinite(px):
            return False
        if has_row[ci, i]:
            return bool(can_buy[ci, i])
        return px > 0

    def record(ci, i, px, p, reason):
        nonlocal cash, sell_amt, sell_intents, sell_same_day
        net_px = px * (1.0 - COST)
        cash += p['shares'] * net_px
        sell_amt += p['shares'] * px
        gross_in = p['entry_px'] * (1.0 + COST)
        ret = (net_px - gross_in) / gross_in
        trade_rets.append(ret)
        if keep_trades:
            trade_rows.append(dict(score=score, seed=seed, code=L['codes'][ci],
                                   entry_i=int(p['entry_i']), exit_i=int(i),
                                   hold_days=int(i - p['entry_i']),
                                   ret=float(ret), reason=reason))
        pos.pop(ci)

    def do_sell(ci, i, px, reason, immediate):
        nonlocal sell_intents, sell_same_day
        sell_intents += 1
        if immediate:
            sell_same_day += 1
        record(ci, i, px, pos[ci], reason)

    def ind_key(ci, i):
        k = int(IND[ci, i])
        return k if k >= 0 else -100000 - ci

    peak_nav = INIT_CAP
    pending = {}

    for i in range(start_i, D):
        # 1) 顺延的卖单
        if pending:
            nxt = {}
            for ci, reason in pending.items():
                if ci not in pos:
                    continue
                if sell_ok(ci, i):
                    do_sell(ci, i, float(O[ci, i]), reason, immediate=False)
                else:
                    blocked_days += 1
                    nxt[ci] = reason
            pending = nxt

        # 2) 调仓日：信号 i-1 收盘，i 开盘执行
        if i in exec_days:
            s = i - 1
            if score == 'rev':
                sig = L['REV'][:, s]
            elif score == 'rand_iid':
                sig = rng.random(S).astype(np.float32)
            else:
                sig = static_score
            ok = (np.isfinite(sig) & np.isfinite(C[:, s]) & listed_ok[:, s]
                  & ~is_st[:, s] & np.isfinite(O[:, i]))
            if ok.any():
                vals = np.where(ok, sig, -np.inf)
                full = [int(k) for k in np.argsort(-vals) if ok[k]]
            else:
                full = []
            cand = full[:n_cand]
            hold_set = set(full[:max(buffer_n, n_hold)])

            # 卖出不在保留集合的持仓
            for ci in list(pos):
                if ci in hold_set:
                    continue
                if sell_ok(ci, i):
                    do_sell(ci, i, float(O[ci, i]), 'rebalance', immediate=True)
                else:
                    blocked_days += 1
                    pending[ci] = 'rebalance_blocked'

            # 等权买入
            n_rebal += 1
            unknown_ind_hold += sum(1 for ci in pos if IND[ci, i] < 0)
            eq = cash + sum(p['last_val'] for p in pos.values())
            if eq > 0:
                target = eq / n_hold
                ind_val = {}
                for ci, p in pos.items():
                    k = ind_key(ci, i)
                    ind_val[k] = ind_val.get(k, 0.0) + p['last_val']
                for ci in cand:
                    if len(pos) >= n_hold:
                        break
                    if ci in pos or not buy_ok(ci, i):
                        continue
                    px = float(O[ci, i])
                    amt = min(target, MAX_W * eq if use_caps else target, cash)
                    if use_caps and MAX_W * eq < min(target, cash) - 1e-9:
                        cap_skip += 1
                    if use_caps:
                        k = ind_key(ci, i)
                        room = IND_CAP * eq - ind_val.get(k, 0.0)
                        if room < amt:
                            amt = room
                        if amt < 0.5 * target:      # 行业剩余额度不足半个仓位则跳过
                            ind_skip += 1
                            continue
                    if amt <= 0:
                        continue
                    shares = math.floor(amt / (px * (1.0 + COST)) / 100.0) * 100
                    if shares < 100:
                        continue
                    cost = shares * px * (1.0 + COST)
                    if cost > cash:
                        continue
                    cash -= cost
                    buy_amt += shares * px
                    pos[ci] = dict(shares=float(shares), entry_px=px, entry_i=i,
                                   last_val=shares * px, last_px=px)
                    k = ind_key(ci, i)
                    ind_val[k] = ind_val.get(k, 0.0) + shares * px

        # 3) 退市/长期无行情强制了结
        for ci in list(pos):
            p = pos[ci]
            if np.isfinite(C[ci, i]):
                continue
            dcode = L['delist_i'].get(ci)
            if dcode is not None and int(dates[i]) >= dcode:
                if sell_ok(ci, i):
                    do_sell(ci, i, float(O[ci, i]), 'delist', immediate=True)
                else:
                    blocked_days += 1
                    net_px = p['last_px'] * (1.0 - COST)
                    cash += p['shares'] * net_px
                    sell_amt += p['shares'] * p['last_px']
                    gross_in = p['entry_px'] * (1.0 + COST)
                    trade_rets.append((net_px - gross_in) / gross_in)
                    if keep_trades:
                        trade_rows.append(dict(score=score, seed=seed, code=L['codes'][ci],
                                               entry_i=int(p['entry_i']), exit_i=int(i),
                                               hold_days=int(i - p['entry_i']),
                                               ret=float((net_px - gross_in) / gross_in),
                                               reason='delist'))
                    pos.pop(ci)

        # 4) 收盘盯市
        val = 0.0
        for ci, p in pos.items():
            c = C[ci, i]
            if np.isfinite(c):
                p['last_val'] = p['shares'] * float(c)
                p['last_px'] = float(c)
            val += p['last_val']
        nav[i] = cash + val
        expo[i] = val / nav[i] if nav[i] > 0 else 0.0
        hold_days += len(pos)

    m = metrics(nav, expo, buy_amt, sell_amt, trade_rets, blocked_days, hold_days,
                sell_intents, sell_same_day, dates)
    m['score'] = score
    m['seed'] = seed
    m['buffer_n'] = buffer_n
    m['use_caps'] = use_caps
    m['n_hold'] = n_hold
    m['cap_skip'] = cap_skip
    m['ind_skip'] = ind_skip
    m['n_rebal'] = n_rebal
    m['unknown_ind_pct'] = unknown_ind_hold / (n_rebal * n_hold) if n_rebal else np.nan
    return m, (trade_rows if keep_trades else None), nav


def year_stats(nav, dates):
    ok = np.isfinite(nav)
    idx = np.flatnonzero(ok)
    if idx.size < 2:
        return {}
    i0, i1 = idx[0], idx[-1]
    yrs = dates[i0:i1 + 1].astype('datetime64[D]').astype('datetime64[Y]').astype(int) + 1970
    nv = nav[i0:i1 + 1]
    out = {}
    for y in np.unique(yrs):
        m = yrs == y
        v = nv[m]
        if v.size < 20:
            continue
        pk = np.maximum.accumulate(v)
        mdd = float((v / pk - 1.0).min())
        ann = (v[-1] / v[0]) ** (252.0 / max(v.size - 1, 1)) - 1.0
        out[int(y)] = dict(ret=float(v[-1] / v[0] - 1.0), mdd=mdd,
                           calmar=float(ann / abs(mdd)) if mdd < 0 else np.nan)
    return out


def industry_coverage(L):
    elig = L['listed_ok'] & np.isfinite(L['C'])
    yrs = L['dates'].astype('datetime64[D]').astype('datetime64[Y]').astype(int) + 1970
    rows = []
    for y in np.unique(yrs):
        m = elig[:, yrs == y]
        n = int(m.sum())
        k = int((m & (L['IND'][:, yrs == y] >= 0)).sum())
        rows.append(dict(year=int(y), eligible_cells=n, with_industry=k,
                         coverage=k / n if n else np.nan))
    return pl.DataFrame(rows)


def main():
    t0 = time.time()
    lim = load_limit_panel()
    L = build_matrices(lim)
    del lim
    fill_quotes(L)
    ind_map = fill_industry(L)
    fill_delist(L)
    log(f'load done {time.time()-t0:.0f}s')

    # ---- 策略组合层消融（同一 REV20 评分）
    arms = [
        ('full', dict(score='rev', buffer_n=BUFFER, use_caps=True)),
        ('no_cap', dict(score='rev', buffer_n=BUFFER, use_caps=False)),
        ('no_buffer', dict(score='rev', buffer_n=N_HOLD, use_caps=True)),
        ('naive_top20', dict(score='rev', buffer_n=N_HOLD, use_caps=False)),
    ]
    industry_coverage(L).write_csv(os.path.join(OUT, 'industry_coverage.csv'))

    rows = []
    yearly = {}
    for name, kw in arms:
        t1 = time.time()
        m, _, nav = simulate(L, **kw)
        m['arm'] = name
        rows.append(m)
        yearly[name] = year_stats(nav, L['dates'])
        log(f"{name:12s} ann={m['ann']*100:7.2f}% mdd={m['mdd']*100:7.2f}% "
            f"calmar={m['calmar']:6.3f} sharpe={m['sharpe']:6.3f} to={m['turnover']:5.2f} "
            f"trades={m['trades']:5d} maxloss={m['max_loss']*100:6.1f}% "
            f"fill={m['fill_rate']*100:6.2f}% ind_skip={m['ind_skip']} "
            f"unk={m['unknown_ind_pct']*100:.1f}% {time.time()-t1:.1f}s")
    pl.DataFrame(rows).write_csv(os.path.join(OUT, 'metrics.csv'))
    pl.DataFrame([dict(arm=a, year=y, ret=v['ret'] * 100, mdd=v['mdd'] * 100,
                       calmar=v['calmar']) for a, ys in yearly.items()
                  for y, v in ys.items()]).write_csv(os.path.join(OUT, 'yearly.csv'))

    # ---- 随机基线：同规则、同缓冲、同频率、同成本、同 T+1 开盘
    families = [
        ('rand_iid', dict(buffer_n=BUFFER, use_caps=True)),
        ('rand_static', dict(buffer_n=BUFFER, use_caps=True)),
        ('rand_iid_naive', dict(buffer_n=N_HOLD, use_caps=False)),
    ]
    rnd_runs = []
    for family, cfg in families:
        t1 = time.time()
        for rep in range(N_REP):
            m, _, _ = simulate(L, score=family.split('_naive')[0], seed=1000 + rep, **cfg)
            m['family'] = family
            m['rep'] = rep
            rnd_runs.append(m)
        log(f'{family} {N_REP} reps done {time.time()-t1:.0f}s')
    rnd = pl.DataFrame(rnd_runs)
    rnd.write_csv(os.path.join(OUT, 'random_runs.csv'))

    # ---- 分布汇总 + 策略分位
    keys = ['ann', 'mdd', 'calmar', 'sharpe', 'turnover', 'trades', 'max_loss',
            'unsellable_pct', 'fill_rate', 'same_day_fill', 'avg_expo']
    full = next(r for r in rows if r['arm'] == 'full')
    dist = []
    for family in ('rand_iid', 'rand_static', 'rand_iid_naive'):
        sub = rnd.filter(pl.col('family') == family)
        for k in keys:
            v = sub[k].to_numpy().astype(float)
            v = v[np.isfinite(v)]
            sv = float(full[k])
            # 方向：mdd/max_loss/unsellable 越小越好，其余越大越好
            if k in ('mdd', 'max_loss', 'unsellable_pct'):
                pct = float((v <= sv).mean())
            else:
                pct = float((v >= sv).mean())
            dist.append(dict(family=family, metric=k, strat=sv,
                             mean=float(v.mean()), median=float(np.median(v)),
                             std=float(v.std()), p05=float(np.percentile(v, 5)),
                             p95=float(np.percentile(v, 95)),
                             p_one_sided=pct, n=int(v.size)))
    pl.DataFrame(dist).write_csv(os.path.join(OUT, 'random_summary.csv'))

    # ---- 增量分解：候选生成 vs 组合构建
    def fam_mean(fam, k):
        v = rnd.filter(pl.col('family') == fam)[k].to_numpy().astype(float)
        return float(np.nanmean(v))

    naive_rev = next(r for r in rows if r['arm'] == 'naive_top20')
    cand_eff = full['ann'] - fam_mean('rand_iid', 'ann')
    constr_eff = full['ann'] - naive_rev['ann']
    rand_constr = fam_mean('rand_iid_naive', 'ann') - fam_mean('rand_iid', 'ann')
    dec = pl.DataFrame([
        dict(component='candidate_generation(REV20 vs 随机)', value_pp=cand_eff * 100,
             detail='full - mean(rand_iid full)'),
        dict(component='portfolio_construction(full vs naive20)',
             value_pp=constr_eff * 100, detail='full - naive_top20'),
        dict(component='random_construction(full vs naive20)',
             value_pp=rand_constr * 100, detail='mean(rand_naive) - mean(rand_iid full)'),
        dict(component='interaction', value_pp=(constr_eff - rand_constr) * 100,
             detail='construction effect minus random construction effect'),
    ])
    dec.write_csv(os.path.join(OUT, 'decomposition.csv'))
    log(dec)

    with open(os.path.join(OUT, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump({'arms': [{k: (float(v) if isinstance(v, (int, float, np.floating)) else v)
                             for k, v in r.items()} for r in rows],
                   'random': [{k: (float(v) if isinstance(v, (int, float, np.floating)) else v)
                               for k, v in r.items()} for r in dist],
                   'config': dict(N_HOLD=N_HOLD, N_CAND=N_CAND, BUFFER=BUFFER, REBAL=REBAL,
                                  MAX_W=MAX_W, IND_CAP=IND_CAP, COST=COST,
                                  N_REP=N_REP, industry_l1_groups=len(ind_map))},
                  f, ensure_ascii=False, indent=1, default=float)
    log(f'done {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
