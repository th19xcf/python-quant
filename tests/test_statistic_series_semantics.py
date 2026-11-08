"""通达信「涨跌/停板家数」统计指标序列的语义回归测试。

2026-11-07 发现：``880xxx`` 段里混着两类完全不同的东西——板块价格指数
与涨跌/停板家数统计。后者四个价格列装的是家数（``high``=板块总家数），
家数逐日大幅摆动是常态，套用价格类契约会 100% 误报。本文件锁住：

1. 统计指标清单与识别函数；
2. 价格语义标记：统计指标跳过价格类规则，结构性规则照跑；
3. 同样是 ``data_type='index'``，真实价格指数仍要正常报警。
"""

from __future__ import annotations

from datetime import date

import polars as pl

from src.data.quality.checker import QualityChecker
from src.data.quality.rules import (
    RULES,
    QualityRule,
    RuleContext,
    rule_pre_close_mismatch,
)
from src.data.quality.semantics import (
    STATISTIC_SERIES,
    STATISTIC_SERIES_CODES,
    describe,
    is_statistic_series,
)


class FakeSource:
    """测试用巡检源。"""

    name = 'local'

    def __init__(self, frames: dict[str, dict[str, pl.DataFrame]]) -> None:
        self._frames = frames

    def codes(self, data_type: str) -> list[str]:
        return sorted(self._frames.get(data_type, {}))

    def load(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        return self._frames.get(data_type, {}).get(ts_code)


def _stat_frame() -> pl.DataFrame:
    """模拟 880026 中小停板：pre_close 与上一日 close 完全无关。

    源端对这类统计指标不维护「前收」语义（实测 880026 有 46 处
    pre_close=38/expected=7 这类无关联值），价格口径下必然误报。
    """
    return pl.DataFrame([
        {'date': date(2026, 11, 5), 'open': 6.0, 'high': 17.0,
         'low': 1.0, 'close': 7.0, 'volume': 1e5, 'amount': 1e9,
         'pre_close': 7.0, 'change': 0.0, 'pct_chg': 0.0,
         'qfq_factor': 1.0, 'hfq_factor': 1.0},
        # 停板家数从 7 跳到 38：家数正常波动；pre_close 却不等于 7
        {'date': date(2026, 11, 6), 'open': 20.0, 'high': 40.0,
         'low': 2.0, 'close': 38.0, 'volume': 1.1e5, 'amount': 1.2e9,
         'pre_close': 38.0, 'change': -2.0, 'pct_chg': -5.0,
         'qfq_factor': 1.0, 'hfq_factor': 1.0},
    ])


def _price_index_frame() -> pl.DataFrame:
    """真实价格指数：前收确实不等于上一交易日收盘，属真缺陷。"""
    return pl.DataFrame([
        {'date': date(2026, 10, 19), 'open': 4953.66, 'high': 4960.0,
         'low': 4950.0, 'close': 4957.17, 'volume': 1e8, 'amount': 1e11,
         'pre_close': 4953.66, 'change': 3.51, 'pct_chg': 0.071,
         'qfq_factor': 1.0, 'hfq_factor': 1.0},
        {'date': date(2026, 10, 20), 'open': 4942.64, 'high': 4950.0,
         'low': 4900.0, 'close': 4942.64, 'volume': 1.1e8, 'amount': 1.1e11,
         'pre_close': 4142.95, 'change': 799.69, 'pct_chg': 19.3,
         'qfq_factor': 1.0, 'hfq_factor': 1.0},
    ])


def test_statistic_series_catalog_covers_all_twelve() -> None:
    # 全市场 + 主板/中小/创业/科创/北证 各自的「涨跌」与「停板」
    expected = {
        '880005.SH', '880006.SH', '880015.SH', '880016.SH',
        '880025.SH', '880026.SH', '880035.SH', '880036.SH',
        '880045.SH', '880046.SH', '880075.SH', '880076.SH',
    }
    assert set(STATISTIC_SERIES) == expected
    assert STATISTIC_SERIES_CODES == frozenset(expected)
    # 每只都要有名称与统计含义，不允许空label
    for code, (name, meaning) in STATISTIC_SERIES.items():
        assert name and meaning, code


def test_is_statistic_series_rejects_price_index() -> None:
    assert is_statistic_series('880005.SH')
    assert is_statistic_series('880005.sh')  # 大小写不敏感
    # 板块价格指数与常见宽基都不是统计指标
    for code in ('880301.SH', '880001.SH', '000001.SH', '399007.SZ', '600000.SH'):
        assert not is_statistic_series(code), code
    assert describe('880005.SH') == '涨跌家数(全市场 上涨/下跌/平盘 家数)'
    assert describe('880301.SH') is None


def test_price_semantics_flag_splits_rules() -> None:
    by_id = {rule.rule_id: rule for rule in RULES}
    # 结构性规则对统计指标照跑
    for rule_id in ('required_columns', 'price_non_positive', 'volume_invalid',
                    'future_trade_date', 'weekend_trade_date', 'duplicate_key'):
        assert by_id[rule_id].price_semantics is False, rule_id
    # 价格类规则对统计指标跳过
    for rule_id in ('pre_close_mismatch', 'change_mismatch', 'pct_chg_mismatch',
                    'high_lt_open_close', 'low_gt_open_close', 'high_lt_low'):
        assert by_id[rule_id].price_semantics is True, rule_id
    # 直接构造时默认按价格语义处理，避免新规则漏标
    assert QualityRule('x', 'x', 'error', ('index',), lambda f, c: f).price_semantics


def test_statistic_series_skips_price_rules_but_price_index_does_not() -> None:
    source = FakeSource({
        'index': {'880005.SH': _stat_frame(), '399007.SZ': _price_index_frame()},
    })
    checker = QualityChecker(source, latest_trade_date=date(2026, 11, 6))
    report = checker.run(write_report=False)

    flagged = {v.ts_code for v in report.violations
               if v.rule_id == 'pre_close_mismatch'}
    # 统计指标不报，真实价格指数照报
    assert '880005.SH' not in flagged
    assert '399007.SZ' in flagged
    assert report.statistic_series == ['880005.SH']
    assert 'pre_close_mismatch' in report.skipped_price_rules


def test_statistic_series_still_checks_structure() -> None:
    # 家数序列里混入 volume 为空的行：结构性规则必须还能抓出来
    bad = _stat_frame().with_columns(
        pl.when(pl.col('date') == pl.lit(date(2026, 11, 6)))
        .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col('volume'))
        .alias('volume'))
    source = FakeSource({'index': {'880005.SH': bad}})
    report = QualityChecker(source, latest_trade_date=date(2026, 11, 6)).run(
        write_report=False)
    assert 'volume_invalid' in report.by_rule
    assert 'pre_close_mismatch' not in report.by_rule


def test_rule_pre_close_mismatch_alone_still_flags_stat_frame() -> None:
    """规则函数本身不做语义判断——跳过发生在巡检编排层。

    这样单独调用规则时行为可预期，不会因「代码长相」而产生隐式分支。
    """
    bad = rule_pre_close_mismatch(_stat_frame(), RuleContext(data_type='index'))
    assert bad.height == 1
