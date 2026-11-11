#!/usr/bin/env python
"""换手—超额曲线：把合成 Q5 篮子的换手从 48.4% 往下压，看多头超额还剩多少。

三种压低手段各扫一遍：
    A 缓冲带   ：老仓排名在 exit 阈值内继续持有，新标的需排名进入 entry 阈值；
                 仅当持仓超过 Q5 只数 k 时按得分截断，**不补位**（标准 buffer band 行为）。
    B 持仓平滑 ：w_t = (1-λ)·w_{t-1} + λ·w_target，w_target 为当期 Q5 等权；
                 混合后剔除权重低于「目标权重 × 0.2」的尾巴并归一。
    C 放宽宽度 ：直接把多头篮子从 top 20% 放宽到 top N%（N>20%，重叠更多、换手更低）。

分组与基准口径与 run_l1.py / factor_lib 完全一致：
    分组     = 截面 rank 升序后 5·r//(n+1)+1（与 group_stats 同式，含并列处理）
    Q5       = 组 5；Q1 = 组 1
    Q5−Q1   = mean_t(Q5 期收益) − mean_t(Q1 期收益)
    Q5 超额 = mean_t(Q5 期收益) − 全样本 pooled 均值（同 group_stats 的 total_mean）
    换手    = |S_{t-1} \\ S_t| / |S_{t-1}| 对 t≥1 取均值

成本口径（项目三套并存，本轮只用第 ① 套，不混用）：
    ① src.quant.factor_lib.evaluate.cost_rates()  → 买 35.2 bp / 卖 40.2 bp
       （CostModel + COST_DEFAULTS；未给 ts_code/total_mv 时滑点落 30 bp 小盘档）
    ② src.advice.config.CostSettings              → 买 7.0 bp / 卖 12.0 bp（无过户费）
    ③ src.backtest.costs.cost_model.CostModel     → 逐只按市值分档 15/20/30 bp + 按 trade_date 取历史印花税
    ②③ 的数字写进 meta 但不用于本曲线；bp 同时给毛值与按 ① 扣成本后的净值。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.quant.factor_lib.evaluate import cost_rates  # noqa: E402

# 中间缓存（可重建；由 run_l1.py --stage prep 与 open_to_open.py 产出）
WORK = ROOT / '_staging' / 'l1_equal_weight_20261111'
# 证据产物（曲线数字与口径说明）
OUT = ROOT / 'reports' / 'l1_baseline'
PRE = WORK / 'pre_h20.parquet'
LAB = WORK / 'labels_oo_h20.parquet'
EVAL = OUT / 'eval_results.json'

BUY, SELL = cost_rates()          # 0.00352 / 0.00402
ROUND_TRIP_BP = (BUY + SELL) * 1e4

TARGETS = (46.0, 42.0, 38.0, 36.7, 33.0, 30.0)
REF_TURNOVER = 36.7
TRIM_REL = 0.2                   # 平滑法：权重低于「目标权重 × 0.2」的尾巴剔除
N_GROUPS = 5


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# 载入：合成得分 / turnover_20 得分，按日期切块并打上与 group_stats 同式的组号
# ---------------------------------------------------------------------------
def load_periods() -> dict[str, list[dict]]:
    info = json.loads(EVAL.read_text(encoding='utf-8'))
    sel = info['selected']
    signs = info['signs_h20_oo']

    df = pl.read_parquet(PRE, columns=['date', 'ts_code', *sorted(set(sel))])
    df = df.with_columns(
        pl.mean_horizontal([pl.col(c) * signs[c] for c in sel]).alias('score_comp'),
        -pl.col('turnover_20').alias('score_turn'),
    )
    lab = pl.read_parquet(LAB)
    df = df.join(lab, on=['date', 'ts_code'], how='left')

    out: dict[str, list[dict]] = {}
    for col, tag in (('score_comp', 'comp'), ('score_turn', 'turn')):
        d = (df.filter(pl.col(col).is_not_null() & pl.col('fwd_20_oo').is_not_null())
             .select(['date', 'ts_code', col, 'fwd_20_oo']))
        # 组号与 run_l1.group_stats 逐字一致（含 rank 并列的 average 处理）
        d = d.with_columns(
            pl.col(col).rank().over('date').alias('_r'),
            pl.len().over('date').alias('_n'),
        ).with_columns(
            ((pl.col('_r').cast(pl.UInt32) * N_GROUPS) // (pl.col('_n') + 1) + 1)
            .cast(pl.Int8).alias('_g')
        )
        parts = []
        for p in d.partition_by('date', maintain_order=True):
            p = p.sort(col, descending=True)
            g = p['_g'].to_numpy()
            parts.append({
                'date': p['date'][0],
                'codes': np.array(p['ts_code'].to_list(), dtype=object),
                'ret': p['fwd_20_oo'].to_numpy().astype(np.float64),
                'g': g,
                'q5': np.flatnonzero(g == N_GROUPS),
                'q1': np.flatnonzero(g == 1),
            })
        out[tag] = parts
    return out


# ---------------------------------------------------------------------------
# 组合评估
# ---------------------------------------------------------------------------
def evaluate_path(periods: list[dict], holdings: list[dict[str, float]],
                  kind: str = 'set') -> dict:
    """kind='set'：换手 = |prev\\cur|/|prev|（与 group_stats 同式，等权）；
    kind='weight'：换手 = 0.5·Σ|Δw|（平滑法，权重连续）。成本按同一换手计。"""
    rets: list[float] = []
    longs: list[int] = []
    turns: list[float] = []
    prev: dict[str, float] = {}
    for t, (per, hold) in enumerate(zip(periods, holdings, strict=True)):
        code_ret = dict(zip(per['codes'].tolist(), per['ret'].tolist(), strict=True))
        w_sum = sum(hold.values())
        gross = (sum(w * code_ret[c] for c, w in hold.items()) / w_sum
                 if w_sum > 0 else 0.0)
        rets.append(gross)
        longs.append(len(hold))
        if t == 0 or not prev:
            turns.append(np.nan)
            prev = hold
            continue
        keys = set(prev) | set(hold)
        traded = 0.5 * sum(abs(hold.get(k, 0.0) - prev.get(k, 0.0)) for k in keys)
        prev_set, cur_set = set(prev), set(hold)
        sold_set = len(prev_set - cur_set) / len(prev_set) if prev_set else np.nan
        turns.append(sold_set if kind == 'set' else traded)
        prev = hold
    turn = float(np.nanmean(turns)) if np.any(np.isfinite(turns)) else 0.0
    return {
        'returns': np.array(rets),
        'turnover': turn,
        'holdings': float(np.mean(longs)),
        'cost_bp': (BUY + SELL) * turn * 1e4,
    }


def group_mean(periods: list[dict], key: str) -> np.ndarray:
    return np.array([float(per['ret'][per[key]].mean()) for per in periods])


def pooled_mean(periods: list[dict]) -> float:
    return float(np.concatenate([per['ret'] for per in periods]).mean())


def summarize(periods: list[dict], holdings: list[dict[str, float]],
              q1_mean: np.ndarray, univ: float, kind: str = 'set') -> dict:
    ev = evaluate_path(periods, holdings, kind)
    r = ev['returns']
    mean_r = float(np.mean(r))
    return {
        'turnover': ev['turnover'],
        'holdings': ev['holdings'],
        'q5_excess_gross_bp': (mean_r - univ) * 1e4,
        'q5_excess_net_bp': (mean_r - ev['cost_bp'] / 1e4 - univ) * 1e4,
        'q5_minus_q1_gross_bp': float(np.mean(r - q1_mean)) * 1e4,
        'q5_minus_q1_net_bp': float(np.mean(r - q1_mean)) * 1e4 - ev['cost_bp'],
        'cost_bp': ev['cost_bp'],
        'n_periods': len(r),
    }


# ---------------------------------------------------------------------------
# 三种手段
# ---------------------------------------------------------------------------
def path_topk(periods: list[dict]) -> list[dict[str, float]]:
    """Q5 组等权持有。"""
    out = []
    for per in periods:
        names = per['codes'][per['q5']].tolist()
        w = 1.0 / len(names)
        out.append(dict.fromkeys(names, w))
    return out


def path_buffer(periods: list[dict], entry: float, exit_: float) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    prev: set[str] = set()
    for per in periods:
        n = len(per['codes'])
        k = len(per['q5'])
        pos = {c: i + 1 for i, c in enumerate(per['codes'].tolist())}
        keep = [c for c in prev if pos.get(c, n + 1) <= exit_ * n]
        fresh = [c for c in per['codes'].tolist()
                 if c not in prev and pos[c] <= entry * n]
        cand = keep + fresh
        if len(cand) > k:
            cand = sorted(cand, key=lambda c: pos[c])[:k]
        if not cand:                                   # 极端窄阈值下退化为最强 1 只
            cand = per['codes'][:1].tolist()
        w = 1.0 / len(cand)
        hold = dict.fromkeys(cand, w)
        out.append(hold)
        prev = set(hold)
    return out


def path_smooth(periods: list[dict], lam: float) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    prev: dict[str, float] = {}
    for per in periods:
        idx = per['q5']
        target = {c: 1.0 / len(idx) for c in per['codes'][idx].tolist()}
        if not prev:
            mixed = dict(target)
        else:
            keys = set(prev) | set(target)
            mixed = {c: (1 - lam) * prev.get(c, 0.0) + lam * target.get(c, 0.0)
                     for c in keys}
        univ = set(per['codes'].tolist())      # 当期无 fwd 的（停牌等）按强制平仓处理
        alive = {c: x for c, x in mixed.items() if c in univ}
        w = {c: x for c, x in alive.items() if x >= TRIM_REL / len(idx)}
        if not w:                              # 权重过度分散时退回权重最大的 50 只
            w = dict(sorted(alive.items(), key=lambda kv: kv[1], reverse=True)[:50])
        s = sum(w.values())
        out.append({c: x / s for c, x in w.items()})
        prev = out[-1]
    return out


def interp(rows: list[dict], target: float, key: str) -> float | None:
    pts = sorted(rows, key=lambda r: r['turnover'])
    for a, b in zip(pts[:-1], pts[1:], strict=False):
        ta, tb = a['turnover'] * 100, b['turnover'] * 100
        if ta <= target <= tb and tb > ta:
            f = (target - ta) / (tb - ta)
            return a[key] + f * (b[key] - a[key])
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--work-dir', default=None,
                    help='中间缓存目录（默认 _staging/l1_equal_weight_20261111）')
    ap.add_argument('--out-dir', default=None,
                    help='证据产物目录（默认 reports/l1_baseline）')
    args = ap.parse_args()
    global WORK, OUT, PRE, LAB, EVAL
    if args.work_dir:
        WORK = Path(args.work_dir).resolve()
        PRE, LAB = WORK / 'pre_h20.parquet', WORK / 'labels_oo_h20.parquet'
    if args.out_dir:
        OUT = Path(args.out_dir).resolve()
        EVAL = OUT / 'eval_results.json'
    OUT.mkdir(parents=True, exist_ok=True)

    data = load_periods()
    comp, turn = data['comp'], data['turn']
    log(f'期数 comp={len(comp)} 只数均值={np.mean([len(p["codes"]) for p in comp]):.0f} '
        f'Q5只数均值={np.mean([len(p["q5"]) for p in comp]):.1f}')

    q1_c = group_mean(comp, 'q1')
    univ_c = pooled_mean(comp)
    q1_t = group_mean(turn, 'q1')
    univ_t = pooled_mean(turn)

    # --- 基准对账（不控换手）：应回到 48.4% / 172.7 / 53.1 ---
    base_s = summarize(comp, path_topk(comp), q1_c, univ_c)
    log(f'基准(无控制) 换手={base_s["turnover"]:.4f} Q5超额={base_s["q5_excess_gross_bp"]:.1f} '
        f'Q5-Q1={base_s["q5_minus_q1_gross_bp"]:.1f} 只数={base_s["holdings"]:.0f}')

    # --- turnover_20 对照（Q5 = 低换手端，与 factor_lib 取向一致）---
    ref = summarize(turn, path_topk(turn), q1_t, univ_t)
    log(f'turnover_20 换手={ref["turnover"]:.4f} Q5超额={ref["q5_excess_gross_bp"]:.1f} '
        f'Q5-Q1={ref["q5_minus_q1_gross_bp"]:.1f} 只数={ref["holdings"]:.0f} '
        f'净超额={ref["q5_excess_net_bp"]:.1f}')

    rows: list[dict] = []

    def add(method: str, param: str, holdings: list[dict[str, float]],
            kind: str = 'set') -> None:
        s = summarize(comp, holdings, q1_c, univ_c, kind)
        s.update({'method': method, 'param': param,
                  'turnover_pct': s['turnover'] * 100})
        rows.append(s)

    for entry, exit_ in ((0.20, 0.20), (0.19, 0.21), (0.18, 0.22), (0.17, 0.23),
                         (0.16, 0.24), (0.15, 0.25), (0.14, 0.26), (0.12, 0.28),
                         (0.10, 0.30), (0.08, 0.32), (0.06, 0.34), (0.04, 0.36)):
        add('A缓冲带', f'entry={entry:.2f},exit={exit_:.2f}',
            path_buffer(comp, entry, exit_))
    for lam in (1.0, 0.8, 0.6, 0.5, 0.4, 0.3, 0.25, 0.2, 0.15, 0.1, 0.05):
        add('B持仓平滑', f'lambda={lam:.2f}', path_smooth(comp, lam), kind='weight')
    for breadth in (0.20, 0.25, 0.30, 0.35, 0.40, 0.50):
        add('C放宽宽度', f'top={breadth:.2f}', path_topk_frac(comp, breadth))

    pl.DataFrame(rows).write_csv(OUT / 'turnover_curve.csv')

    picked = []
    for tgt in TARGETS:
        best = min(rows, key=lambda r: abs(r['turnover_pct'] - tgt))
        picked.append({'target_pct': tgt, 'method': best['method'], 'param': best['param'],
                       'actual_turnover_pct': best['turnover_pct'],
                       'holdings': best['holdings'],
                       'q5_excess_gross_bp': best['q5_excess_gross_bp'],
                       'q5_excess_net_bp': best['q5_excess_net_bp'],
                       'q5_minus_q1_gross_bp': best['q5_minus_q1_gross_bp'],
                       'q5_minus_q1_net_bp': best['q5_minus_q1_net_bp'],
                       'cost_bp': best['cost_bp']})
        log(f'目标 {tgt:>4.1f}% -> {best["method"]} {best["param"]} '
            f'实际 {best["turnover_pct"]:.1f}% 只数 {best["holdings"]:.0f} '
            f'毛超额 {best["q5_excess_gross_bp"]:.1f}bp 净 {best["q5_excess_net_bp"]:.1f}bp '
            f'Q5-Q1 {best["q5_minus_q1_gross_bp"]:.1f}bp')

    by_method = {m: [r for r in rows if r['method'] == m] for m in
                 ('A缓冲带', 'B持仓平滑', 'C放宽宽度')}
    interp_by_method = {m: {
        'range_turnover_pct': [min(r['turnover_pct'] for r in v),
                               max(r['turnover_pct'] for r in v)],
        'at_36.7_gross_bp': interp(v, REF_TURNOVER, 'q5_excess_gross_bp'),
        'at_36.7_net_bp': interp(v, REF_TURNOVER, 'q5_excess_net_bp'),
        'at_36.7_spread_bp': interp(v, REF_TURNOVER, 'q5_minus_q1_gross_bp'),
    } for m, v in by_method.items()}
    overall_gross = interp(rows, REF_TURNOVER, 'q5_excess_gross_bp')
    overall_net = interp(rows, REF_TURNOVER, 'q5_excess_net_bp')
    log(f'36.7% 插值（全体包络）: 毛 {overall_gross:.1f}bp 净 {overall_net:.1f}bp')

    # --- 判定：压回 36.7% 后 vs turnover_20 只做多线 ---
    ref_gross = ref['q5_excess_gross_bp']
    ref_net = ref['q5_excess_net_bp']
    verdict = {
        'ref_gross_bp': ref_gross, 'ref_net_bp': ref_net,
        'ref_turnover_pct': ref['turnover'] * 100,
        'best_at_36.7_gross_bp': overall_gross,
        'best_at_36.7_net_bp': overall_net,
        'gap_gross_bp': (overall_gross - ref_gross) if overall_gross is not None else None,
        'gap_net_bp': (overall_net - ref_net) if overall_net is not None else None,
        'win_long_only': bool(overall_gross is not None and overall_gross > ref_gross),
    }
    log(f'判定: 合成@36.7% 毛{overall_gross:.1f} vs turnover_20 毛{ref_gross:.1f} '
        f'差 {verdict["gap_gross_bp"]:+.1f}bp；净差 {verdict["gap_net_bp"]:+.1f}bp')

    meta = {
        'label': 'T+1 开盘 -> T+1+20 开盘（open->open）',
        'n_periods': len(comp),
        'baseline_uncontrolled': base_s,
        'benchmark_turnover_20': ref,
        'cost': {
            'used': 'factor_lib.evaluate.cost_rates()（CostModel + COST_DEFAULTS）',
            'buy': BUY, 'sell': SELL, 'round_trip_bp': ROUND_TRIP_BP,
            'others_not_used': {
                'advice.CostSettings': '买 7.0bp / 卖 12.0bp（无过户费）',
                'backtest.CostModel': '逐只市值分档 15/20/30bp + 历史印花税（需 ts_code，组合层不可直接换算）',
            },
        },
        'algorithm': {
            'A缓冲带': '老仓排名<=exit 继续持有，新标的需排名<=entry；仅超 k 时按得分截断，不补位，等权',
            'B持仓平滑': f'w_t=(1-lambda)w_{{t-1}}+lambda*w_target，w_target 为 Q5 等权；剔除 <目标权重×{TRIM_REL} 的尾巴后归一',
            'C放宽宽度': '直接持有得分 top N% 等权',
            'Q5-Q1': 'mean_t(Q5 期收益) - mean_t(Q1 期收益)，Q1 恒为组1（不随多头控制变化）',
            'Q5超额': 'mean_t(Q5 期收益) - 全样本 pooled 均值（同 group_stats 的 total_mean）',
            '换手': '|S_{t-1} \\ S_t| / |S_{t-1}|，t>=1 取均值',
            '成本': '每期 cost = buy*ΣΔw+ + sell*ΣΔw-；净超额 = 毛超额 - mean_t(cost)',
        },
        'picked_near_targets': picked,
        'interp_at_36.7': interp_by_method,
        'interp_at_36.7_overall': {'gross_bp': overall_gross, 'net_bp': overall_net},
        'verdict': verdict,
        'curve': rows,
    }
    (OUT / 'turnover_curve_meta.json').write_text(
        json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    log('写入 turnover_curve.csv / turnover_curve_meta.json')


def path_topk_frac(periods: list[dict], frac: float) -> list[dict[str, float]]:
    """按 top frac 等权持有（放宽宽度用）。"""
    out = []
    for per in periods:
        k = max(1, int(round(frac * len(per['codes']))))
        names = per['codes'][:k].tolist()
        w = 1.0 / k
        out.append(dict.fromkeys(names, w))
    return out


if __name__ == '__main__':
    main()
