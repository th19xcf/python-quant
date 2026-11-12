"""守护用例：打分方向与「软筛选多头排列」的方向一致性。

背景（2026-11-11 实测，缺陷存在时本用例应为红）：

- `cb55a9e` 把 legacy 动量/趋势因子翻成 direction=-1，而 `440d228` 引入的扩展
  动量/趋势因子最初仍是 +1，软筛选又要求 `ma20_gap>0 & ma60_gap>0`。
- F3（`bdc571e`）把 `gtja_003`/`gtja_024`/`gtja_010` 翻为 -1 后，打分更偏反转，
  与「多头排列」软筛选的对立更强：Top15 中 MA 多头只数由 1/0/6/2/0 变为
  2/0/1/0/0，全链路通过只数由 12/0/16/2/0 变为 8/0/5/0/0。

| 信号日 | 市场状态 | 候选数 | Top15 MA 多头 | 全链路通过 |
|---|---|---|---|---|
| 2025-07-16 | 平静（多头 66.4%） | 95 | 2 | 8 |
| 2025-10-17 | 高波动（多头 19.4%） | 95 | 0 | 0 |
| 2026-01-14 | 平静（多头 67.3%） | 94 | 1 | 5 |
| 2026-04-20 | 平静 | 93 | 0 | 0 |
| 2026-07-20 | 高波动（多头 7.4%） | 90 | 0 | 0 |

即：只要「打分选出的票」与「软筛选要求的多头排列」方向相反，本用例就会失败。
修复方向应是让两者同向（改软筛选口径或补打分维度），**不是**通过放宽门槛
让数字好看。

口径（2026-11-11 修正）：
- **只读活配置**：`src.advice.config.default_config()`（其 direction/权重来自
  `src/advice/factor_sets.py`），不再从基线快照反读，避免「快照过期导致用例
  静默测旧值」；
- 另用 `test_baseline_snapshot_matches_live_config` 断言最新快照与活配置逐项
  一致，快照过期即失败，把「导出快照」这件事变成显式动作。

只读：不修改任何配置、不写 data/。首次运行若无因子面板缓存会重建（较慢），
有缓存时 <30s。
"""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from src.advice.config import default_config
from src.advice.scoring import attach_score
from tools import tune_advice as T

# 固定信号日：高波动期 2 个 + 平静期 3 个，均为历史交易日，不依赖运行当天。
SIGNAL_DATES = (
    date(2025, 7, 16),    # 平静：全市场 MA 多头 66.4%
    date(2025, 10, 17),   # 高波动：全市场 MA 多头 19.4%
    date(2026, 1, 14),    # 平静：全市场 MA 多头 67.3%
    date(2026, 4, 20),    # 平静
    date(2026, 7, 20),    # 高波动：全市场 MA 多头 7.4%
)

TOP_N = 15

# 与生产配置一致的子分组成（用于失败时打印，不改权重）。
MOMENTUM = ('momentum_6m', 'momentum_12m', 'gtja_024', 'a101_024')
TREND = ('trend_slope_60', 'qlib_rsqr20', 'qlib_rsqr10', 'gtja_003')


def _z(column: str) -> pl.Expr:
    raw = pl.col(f'{column}__win')
    win = pl.when(raw.is_finite()).then(raw).otherwise(None)
    z = (win - win.mean().over('date')) / win.std().over('date')
    return z.clip(lower_bound=-3.0, upper_bound=3.0)


def _subscore(columns: tuple[str, ...], specs: dict) -> pl.Expr:
    contrib = [
        (specs[c].weight * specs[c].direction * _z(c)).fill_null(0.0)
        for c in columns if c in specs
    ]
    denom = pl.lit(0.0) + sum(
        (pl.when(_z(c).is_not_null()).then(abs(specs[c].weight)).otherwise(0.0)
         for c in columns if c in specs), pl.lit(0.0))
    return pl.sum_horizontal(contrib) / denom


def _signature(config) -> dict[str, tuple[tuple[str, float, float], ...]]:
    """档位 -> ((列名, 权重, 方向), ...)，用于快照与活配置逐项比对。"""
    return {
        key: tuple((s.column, round(s.weight, 6), float(s.direction))
                   for s in tier.factors)
        for key, tier in config.tiers.items()
    }


def test_baseline_snapshot_matches_live_config() -> None:
    """最新基线快照必须与活配置逐项一致，不一致说明快照过期。"""
    live = _signature(default_config())
    snapshot = json.loads(T.latest_baseline().read_text(encoding='utf-8'))
    saved = _signature(T.dict_to_config(snapshot['config']))

    diffs: list[str] = []
    for key in sorted(set(live) | set(saved)):
        a, b = live.get(key, ()), saved.get(key, ())
        if a == b:
            continue
        a_map = {c: (w, d) for c, w, d in a}
        b_map = {c: (w, d) for c, w, d in b}
        for col in sorted(set(a_map) | set(b_map)):
            if a_map.get(col) != b_map.get(col):
                diffs.append(f'{key}.{col}: 活配置 {a_map.get(col)} != 快照 {b_map.get(col)}')

    assert not diffs, (
        '基线快照已过期，与活配置不一致（改完配置必须重新导出：'
        'python tools/tune_advice.py baseline --out docs/策略参数基线_<日期>_v2.json）：\n'
        + '\n'.join(diffs))


@pytest.fixture(scope='module')
def scored_day_frame() -> tuple[pl.DataFrame, dict, object]:
    config = default_config()
    tier = config.tier('long')
    panel, _info = T.load_factor_panel(
        int(T.PROTOCOL['universe']),
        '2013-12-01',
        T.PROTOCOL['eval_end'])
    panel = panel.filter(pl.col('date').is_in(list(SIGNAL_DATES)))
    scored = attach_score(panel, tier,
                          min_history_rows=int(T.PROTOCOL['min_history_rows']))
    return scored, {spec.column: spec for spec in tier.factors}, tier


@pytest.mark.xfail(
    strict=True,
    reason='G-102：打分方向（偏反转）与软筛「均线多头」口径冲突，Top15 多头只数常为 0；'
           '断言 / 阈值 / TOP_N / 软筛条件一律不动，出现 XPASS 才视为口径已变化需重评')
def test_top_candidates_contain_ma_bullish_names(scored_day_frame) -> None:
    scored, specs, tier = scored_day_frame
    tier_pct = tier.buy_percentile
    soft = default_config().soft_filter

    failures: list[str] = []
    summary: list[str] = []
    for day in SIGNAL_DATES:
        frame = scored.filter(pl.col('date') == day).with_columns(
            _subscore(MOMENTUM, specs).alias('__momentum'),
            _subscore(TREND, specs).alias('__trend'),
        )
        q = frame.select([
            pl.col('volume_momentum_20').quantile(soft.volume_quantile).alias('vm'),
            pl.col('atr_pct').quantile(soft.atr_quantile).alias('atr'),
            pl.col('rsi_14').quantile(soft.rsi_low).alias('rsi_lo'),
            pl.col('rsi_14').quantile(soft.rsi_high).alias('rsi_hi'),
        ]).row(0, named=True)
        ma_bull = (pl.col('ma20_gap') > 0) & (pl.col('ma60_gap') > 0)
        hard = (
            (pl.col('amount_ma20') >= tier.min_amount)
            & (pl.col('volatility_20') <= tier.max_volatility)
            & (pl.col('atr_pct') <= tier.max_atr_pct)
            & (pl.col('raw_close') >= 1.0)
            & (pl.col('volume') > 0)
        ).fill_null(False)
        rest = (
            (pl.col('volume_momentum_20') >= q['vm'])
            & (pl.col('atr_pct') <= q['atr'])
            & (pl.col('rsi_14') >= q['rsi_lo'])
            & (pl.col('rsi_14') <= q['rsi_hi'])
        ).fill_null(False)

        candidates = frame.filter(pl.col('score_percentile') >= tier_pct)
        top = candidates.sort('score', descending=True).head(TOP_N)
        if top.height == 0:
            failures.append(f'{day}: 分数门槛后 0 只候选（门槛过严或数据缺失）')
            continue

        ma_bull_n = top.filter(ma_bull.fill_null(False)).height
        full_chain = candidates.filter(
            hard & ma_bull.fill_null(False) & rest).height
        market_bull = frame.select(
            ((pl.col('ma20_gap') > 0) & (pl.col('ma60_gap') > 0)).mean()
        ).item()
        summary.append(
            f'{day}: 候选 {candidates.height} 只，Top{top.height} MA 多头 '
            f'{ma_bull_n} 只，全链路通过 {full_chain} 只，'
            f'全市场多头占比 {market_bull:.1%}')
        if ma_bull_n == 0:
            detail = top.head(5).select(['ts_code', 'score', 'ma20_gap',
                                         'ma60_gap', '__momentum', '__trend'])
            failures.append(
                f'{day}: 候选 {candidates.height} 只，Top{top.height} 中 MA 多头 0 只；'
                f'全链路通过 {full_chain} 只；全市场多头占比 {market_bull:.1%}。'
                f'Top5 明细：\n{detail}')

    if failures:
        pytest.fail(
            '打分方向与软筛选「多头排列」方向不一致：\n'
            + '\n'.join(failures)
            + '\n\n全部日期汇总：\n' + '\n'.join(summary)
            + '\n\n判读提示：若某日全市场多头占比本身很低（如 <10%），是市场状态问题；'
            '若全市场占比正常但 Top 里为 0，则是打分与软筛选方向自相矛盾。')
