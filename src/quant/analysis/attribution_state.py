"""收益归因（口径降级）：run 级超额分解、行业暴露与风格暴露（纯计算）。

完整 Brinson 两段分解需要「逐期持仓权重 + 逐笔成交 + 基准成分行业权重」，现有
回测记录只落 ``daily_nav.csv``（date, equity），因此本模块只做可自证的部分：

1. run 级超额：组合净值 vs 基准指数，给年化收益、超额、beta、跟踪误差与信息比率；
2. 行业暴露：目标持仓按申万一级聚合，与「全 A 等权行业占比」对照；
3. 风格暴露：持仓加权市值分位与 PB 分位，与全市场中位（50%）对照。

口径与缺失项见 ``docs/组合仓位风控与归因方法_2026-11-16.md``。本地装配与 CLI
在 ``attribution_local.py``。
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Mapping

#: 交易日常数年化系数
TRADING_DAYS = 252
#: 行业暴露展示条数
TOP_INDUSTRIES = 8
#: 全市场基准分位（等权样本的中位）
BASELINE_PERCENTILE = 50.0


@dataclass(frozen=True)
class RunSummary:
    """被归因的回测 run。"""

    run_id: str
    name: str
    start: str
    end: str


@dataclass(frozen=True)
class ExcessStats:
    """run 级超额统计。"""

    annual_return: float | None
    bench_annual: float | None
    excess_annual: float | None
    beta: float | None
    tracking_error: float | None
    information_ratio: float | None
    days: int


@dataclass(frozen=True)
class ExposureRow:
    """一条暴露：组合权重/分位 vs 基准，diff 为偏离。"""

    name: str
    weight: float
    baseline: float | None
    diff: float | None
    note: str = ''


@dataclass(frozen=True)
class BrinsonPeriod:
    """一个调仓区间的两段分解。"""

    start: str
    end: str
    allocation: float | None
    selection: float | None
    interaction: float | None
    portfolio_return: float
    benchmark_return: float


@dataclass(frozen=True)
class BrinsonResult:
    """完整 Brinson 两段分解（含累计、IR、α 与 t 值）。"""

    benchmark: str
    periods: tuple[BrinsonPeriod, ...]
    allocation: float | None
    selection: float | None
    interaction: float | None
    portfolio_return: float
    benchmark_return: float
    excess: float
    information_ratio: float | None
    alpha: float | None
    alpha_t: float | None


@dataclass(frozen=True)
class AttributionState:
    """归因页面结果。"""

    asof: str | None
    run: RunSummary | None
    excess: ExcessStats | None
    industries: tuple[ExposureRow, ...]
    styles: tuple[ExposureRow, ...]
    degraded: bool
    missing: tuple[str, ...]
    notes: tuple[str, ...]
    brinson: BrinsonResult | None = None
    benchmark: str = ''


def annualized_return(values: Sequence[float]) -> float | None:
    """净值序列年化收益（几何）；样本不足 2 个返回 None。"""
    clean = [float(item) for item in values if item is not None and item > 0]
    if len(clean) < 2:
        return None
    periods = len(clean) - 1
    growth = clean[-1] / clean[0]
    if growth <= 0:
        return None
    return growth ** (TRADING_DAYS / periods) - 1.0


def _daily_returns(values: Sequence[float]) -> list[float]:
    clean = [float(item) for item in values if item is not None and item > 0]
    return [clean[i] / clean[i - 1] - 1.0 for i in range(1, len(clean))]


def _beta_and_te(port: Sequence[float], bench: Sequence[float]) -> tuple[float | None, float | None]:
    size = min(len(port), len(bench))
    if size < 3:
        return None, None
    port_ret = _daily_returns(port)[-size:]
    bench_ret = _daily_returns(bench)[-size:]
    size = min(len(port_ret), len(bench_ret))
    if size < 3:
        return None, None
    port_ret = port_ret[-size:]
    bench_ret = bench_ret[-size:]
    mean_p = sum(port_ret) / size
    mean_b = sum(bench_ret) / size
    cov = sum((port_ret[i] - mean_p) * (bench_ret[i] - mean_b) for i in range(size)) / (size - 1)
    var_b = sum((item - mean_b) ** 2 for item in bench_ret) / (size - 1)
    beta = cov / var_b if var_b > 0 else None
    diff = [port_ret[i] - bench_ret[i] for i in range(size)]
    mean_d = sum(diff) / size
    te_var = sum((item - mean_d) ** 2 for item in diff) / (size - 1)
    te = math.sqrt(te_var) * math.sqrt(TRADING_DAYS)
    return beta, te


def excess_stats(nav: Sequence[float], bench: Sequence[float]) -> ExcessStats | None:
    """组合与基准的 run 级超额统计；任一序列不足返回 None。"""
    if len(nav) < 3 or len(bench) < 3:
        return None
    ann_port = annualized_return(nav)
    ann_bench = annualized_return(bench)
    beta, te = _beta_and_te(nav, bench)
    excess = None if ann_port is None or ann_bench is None else ann_port - ann_bench
    ir = excess / te if (excess is not None and te and te > 0) else None
    return ExcessStats(
        annual_return=ann_port,
        bench_annual=ann_bench,
        excess_annual=excess,
        beta=beta,
        tracking_error=te,
        information_ratio=ir,
        days=len(nav) - 1,
    )


def industry_exposure(
    weights: Mapping[str, float],
    industry_of: Mapping[str, str],
    universe_share: Mapping[str, float] | None = None,
    top: int = TOP_INDUSTRIES,
) -> tuple[ExposureRow, ...]:
    """持仓按申万一级聚合，与全 A 等权行业占比对照，按偏离绝对值排序。"""
    buckets: dict[str, float] = {}
    unknown = 0.0
    for code, weight in weights.items():
        industry = industry_of.get(code)
        if not industry:
            unknown += abs(weight)
            continue
        buckets[industry] = buckets.get(industry, 0.0) + abs(weight)
    if not buckets:
        return ()
    total = sum(buckets.values()) + unknown
    if total <= 0:
        return ()
    basis = universe_share or {}
    rows = [
        ExposureRow(
            name=industry,
            weight=value / total,
            baseline=basis.get(industry),
            diff=None if industry not in basis else value / total - basis[industry],
            note='' if industry in basis else '基准占比缺失',
        )
        for industry, value in buckets.items()
    ]
    rows.sort(key=lambda item: abs(item.diff) if item.diff is not None else item.weight,
              reverse=True)
    return tuple(rows[:top])


def style_exposure(
    weights: Mapping[str, float],
    mv_percentile: Mapping[str, float] | None,
    pb_percentile: Mapping[str, float] | None,
) -> tuple[ExposureRow, ...]:
    """持仓加权市值分位与 PB 分位，对照全市场中位 50。"""
    rows: list[ExposureRow] = []
    for name, percentiles in (('市值分位', mv_percentile), ('PB 分位', pb_percentile)):
        value = _weighted_percentile(weights, percentiles or {})
        if value is None:
            rows.append(ExposureRow(name, 0.0, BASELINE_PERCENTILE, None, '样本缺失'))
            continue
        rows.append(ExposureRow(
            name=name,
            weight=value,
            baseline=BASELINE_PERCENTILE,
            diff=value - BASELINE_PERCENTILE,
            note='高于 50 表示偏大盘/偏高估值',
        ))
    return tuple(rows)


def _weighted_percentile(weights: Mapping[str, float], percentiles: Mapping[str, float]) -> float | None:
    covered = [(abs(weight), percentiles[code])
               for code, weight in weights.items() if code in percentiles]
    total = sum(item[0] for item in covered)
    if total <= 0:
        return None
    return sum(weight * pct for weight, pct in covered) / total


#: 可切基准：全市场等权 / 行业等权 / 沪深 300（月末成分权重）
BENCHMARK_LABELS = {
    'equal_all': '全市场等权',
    'equal_industry': '行业等权',
    'csi300': '沪深 300',
}


def _industry_buckets(
    weights: Mapping[str, float],
    returns: Mapping[str, float],
    industry_of: Mapping[str, str],
) -> tuple[dict[str, float], dict[str, float]]:
    """按行业聚合权重与加权收益；缺收益按 0（停牌）处理。"""
    bucket_w: dict[str, float] = {}
    bucket_r: dict[str, float] = {}
    for code, weight in weights.items():
        industry = industry_of.get(code, '未归类')
        bucket_w[industry] = bucket_w.get(industry, 0.0) + weight
        bucket_r[industry] = bucket_r.get(industry, 0.0) + weight * returns.get(code, 0.0)
    return bucket_w, bucket_r


def _with_cash(weights: Mapping[str, float]) -> dict[str, float]:
    """权重合计不足 1 的部分补成现金（收益 0），保证分解恒等式成立。"""
    merged = dict(weights)
    gap = 1.0 - sum(merged.values())
    if gap > 1e-9:
        merged['__CASH__'] = gap
    return merged


def brinson_period(
    weights_p: Mapping[str, float],
    weights_b: Mapping[str, float],
    returns: Mapping[str, float],
    industry_of: Mapping[str, str],
) -> tuple[float, float, float, float, float]:
    """单期 Brinson：返回 (行业配置, 个股选择, 交叉项, 组合收益, 基准收益)。

    不足 100% 的部分视作现金（收益 0），这样三项之和恒等于组合减基准。
    """
    weights_p = _with_cash(weights_p)
    weights_b = _with_cash(weights_b)
    industry_of = dict(industry_of)
    industry_of.setdefault('__CASH__', '现金')
    p_w, p_r = _industry_buckets(weights_p, returns, industry_of)
    b_w, b_r = _industry_buckets(weights_b, returns, industry_of)
    allocation = selection = interaction = 0.0
    for industry in set(p_w) | set(b_w):
        weight_p = p_w.get(industry, 0.0)
        weight_b = b_w.get(industry, 0.0)
        ret_p = p_r.get(industry, 0.0) / weight_p if weight_p > 0 else 0.0
        ret_b = b_r.get(industry, 0.0) / weight_b if weight_b > 0 else 0.0
        allocation += (weight_p - weight_b) * ret_b
        selection += weight_b * (ret_p - ret_b)
        interaction += (weight_p - weight_b) * (ret_p - ret_b)
    return allocation, selection, interaction, sum(p_r.values()), sum(b_r.values())


def alpha_tvalue(excess: Sequence[float],
                 periods_per_year: float) -> tuple[float | None, float | None]:
    """区间超额序列的年化 α 与 t 值（均值 / 标准误）。"""
    clean = [float(item) for item in excess]
    if len(clean) < 3:
        return None, None
    mean = sum(clean) / len(clean)
    variance = sum((item - mean) ** 2 for item in clean) / (len(clean) - 1)
    std = math.sqrt(variance)
    if std <= 0:
        return mean * periods_per_year, None
    return mean * periods_per_year, mean / (std / math.sqrt(len(clean)))


def build_brinson(
    periods: Sequence[BrinsonPeriod],
    benchmark: str,
    periods_per_year: float = 12.0,
) -> BrinsonResult | None:
    """把逐期分解汇总为累计结果，并给出 IR / α / t 值。"""
    if not periods:
        return None
    has_split = all(item.allocation is not None for item in periods)
    excess = [item.portfolio_return - item.benchmark_return for item in periods]
    mean = sum(excess) / len(excess)
    variance = (sum((item - mean) ** 2 for item in excess) / (len(excess) - 1)
                if len(excess) > 1 else 0.0)
    std = math.sqrt(variance)
    ir = mean / std * math.sqrt(periods_per_year) if std > 0 else None
    alpha, t_value = alpha_tvalue(excess, periods_per_year)
    return BrinsonResult(
        benchmark=benchmark,
        periods=tuple(periods),
        allocation=sum(item.allocation or 0.0 for item in periods) if has_split else None,
        selection=sum(item.selection or 0.0 for item in periods) if has_split else None,
        interaction=sum(item.interaction or 0.0 for item in periods) if has_split else None,
        portfolio_return=sum(item.portfolio_return for item in periods),
        benchmark_return=sum(item.benchmark_return for item in periods),
        excess=sum(excess),
        information_ratio=ir,
        alpha=alpha,
        alpha_t=t_value,
    )


def build_state(
    *,
    asof: str | None,
    run: RunSummary | None,
    excess: ExcessStats | None,
    industries: Sequence[ExposureRow] = (),
    styles: Sequence[ExposureRow] = (),
    degraded: bool = True,
    missing: Sequence[str] = (),
    notes: Sequence[str] = (),
    brinson: BrinsonResult | None = None,
    benchmark: str = '',
) -> AttributionState:
    """汇总归因结果（装配层只负责取数，判定口径在本模块）。"""
    return AttributionState(
        asof=asof,
        run=run,
        excess=excess,
        industries=tuple(industries),
        styles=tuple(styles),
        degraded=degraded,
        missing=tuple(missing),
        notes=tuple(notes),
        brinson=brinson,
        benchmark=benchmark,
    )
