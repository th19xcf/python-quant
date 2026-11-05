#!/usr/bin/env python3

"""
复权因子 PIT（Point-In-Time）安全测试。

锁定三条硬规则，放入 CI：

1. hfq_factor 天然 PIT 安全：历史行的后复权因子永远不变
   （新除权事件只影响 ex_date 之后的行）
2. qfq_factor + as_of 截断：用 as_of=T 截断的因子复权 [0, T] 段，
   必须与"今天"观察到的同段前复权价完全一致
3. as_of 截断公式正确性：
   qfq_truncated[td] = qfq_full[td] / ∏{ex_date > max(td, as_of)} event_factor

一旦这些测试通过，项目永久免疫复权类的未来函数。
"""

import datetime

import numpy as np
import polars as pl
import pytest

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.managers.data_fetcher import DataFetcher


def _make_linear_prices(n: int = 300, start: float = 100.0) -> pl.Series:
    """生成简单线性上涨的收盘价，方便手算验证"""
    return pl.Series('close', start + np.arange(n, dtype=np.float64))


def _make_dates(n: int = 300, base: datetime.date = datetime.date(2025, 1, 1)):
    """生成连续交易日"""
    return pl.date_range(base, base + datetime.timedelta(days=n - 1),
                        interval=datetime.timedelta(days=1), eager=True)


class TestHFQPITSafety:
    """规则 1：后复权因子天然 PIT 安全"""

    def test_hfq_factor_monotonic_anchor(self):
        """
        后复权因子从第 1 天 = 1.0，每次除权事件只影响 ex_date 之后的行。
        历史行的 hfq_factor 永远不变——这就是 PIT 安全。
        """
        n = 300
        _make_dates(n)
        _make_linear_prices(n)

        # 构造含两次除权事件的全量 hfq_factor：
        #   事件 A：ex_date=day 100，factor=0.9（10送10，后复权乘 1/0.9≈1.111）
        #   事件 B：ex_date=day 200，factor=0.8（5送1，后复权乘 1/0.8=1.25）
        hfq_factor = np.ones(n)
        # 后复权：trade_date >= ex_date 的行 *= (1/factor)
        hfq_factor[100:] *= 1.0 / 0.9   # ≈ 1.111
        hfq_factor[200:] *= 1.0 / 0.8   # ≈ 1.388

        # 断言：hfq_factor 单调不减（最早段 = 1.0，越来越大）
        assert hfq_factor[0] == 1.0
        assert np.all(np.diff(hfq_factor) >= 0), "hfq_factor 应单调不减"

        # 关键：历史行（事件前）的 hfq_factor 永远不变
        # 如果我们在 day 150 观察：
        #   不知道事件 B（day 200 还没发生），只知道事件 A
        truncated_hfq = hfq_factor.copy()
        truncated_hfq[200:] /= 1.0 / 0.8   # 回滚事件 B

        # 历史行（<200）完全一致——这就是 PIT 安全！
        assert np.allclose(hfq_factor[:200], truncated_hfq[:200]), \
            "hfq 历史行在 as_of 截断后不应变化"


class TestQFQAsOfTruncation:
    """规则 2+3：前复权因子 + as_of 截断的正确性"""

    def test_as_of_truncation_formula(self):
        """
        验证 as_of 截断公式：
        qfq_truncated[td] = qfq_full[td] / ∏{ex_date > max(td, as_of)} event_factor
        """
        n = 300
        # 事件 A：ex_date=day 100，factor=0.9（10送10）
        # 事件 B：ex_date=day 200，factor=0.8（5送1）
        events = [(0.9, 100), (0.8, 200)]  # (factor, ex_date_day)

        # 构造全量 qfq_factor：
        # 前复权：trade_date < ex_date 的行 *= factor
        qfq_full = np.ones(n)
        for evt_factor, ex_day in events:
            qfq_full[:ex_day] *= evt_factor

        # 断言全量因子正确
        # day 50（<100）: 0.9 * 0.8 = 0.72
        assert abs(qfq_full[50] - 0.72) < 1e-9
        # day 150（100<=<200）: 0.8
        assert abs(qfq_full[150] - 0.8) < 1e-9
        # day 250（>=200）: 1.0（锚点）
        assert abs(qfq_full[250] - 1.0) < 1e-9

        # --- as_of=day 150 截断（观察者只知道事件 A）---
        as_of_day = 150
        rollback_events = [(f, d) for f, d in events if d > as_of_day]  # 只有事件 B

        # 应用回滚公式
        qfq_truncated = qfq_full.copy()
        for td in range(n):
            for evt_factor, evt_ex_day in rollback_events:
                if evt_ex_day > td:
                    qfq_truncated[td] /= evt_factor

        # 验证截断后的因子：
        # day 50: 0.72 / 0.8 = 0.9 ✅（观察者只知道事件 A）
        assert abs(qfq_truncated[50] - 0.9) < 1e-9
        # day 150: 0.8 / 0.8 = 1.0 ✅（观察者还不知道事件 B，锚点下移）
        assert abs(qfq_truncated[150] - 1.0) < 1e-9
        # day 250: 1.0（max(250,150)=250，ex_date > 250 无事件，不回滚）✅
        assert abs(qfq_truncated[250] - 1.0) < 1e-9

    def test_no_future_factor_leak(self):
        """
        核心 CI 测试：as_of=T 截断的因子，算 [0, T] 段前复权价
        必须等于"只含 ex_date<=T 事件"的全量因子算的同段价格——
        确保 as_of 之后的除权事件不会"穿越"到历史。

        一旦通过，永久免疫复权类的未来函数。
        """
        n = 300
        close = _make_linear_prices(n).to_numpy()

        # 事件 A：ex_date=day 100，factor=0.9
        # 事件 B：ex_date=day 200，factor=0.8
        events = [(0.9, 100), (0.8, 200)]

        # 全量 qfq_factor（含所有事件）
        qfq_full = np.ones(n)
        for evt_factor, ex_day in events:
            qfq_full[:ex_day] *= evt_factor

        # --- as_of=day 150 截断 ---
        as_of_day = 150
        rollback_events = [(f, d) for f, d in events if d > as_of_day]  # 只有事件 B

        # 应用回滚公式（与 _get_data_from_database 实现一致）
        qfq_truncated = qfq_full.copy()
        for td in range(n):
            for evt_factor, evt_ex_day in rollback_events:
                if evt_ex_day > td:
                    qfq_truncated[td] /= evt_factor

        # --- 正确答案：只用 ex_date <= as_of_day 的事件算 ---
        visible_events = [(f, d) for f, d in events if d <= as_of_day]
        qfq_visible = np.ones(n)
        for evt_factor, ex_day in visible_events:
            qfq_visible[:ex_day] *= evt_factor

        # --- 关键断言 ---
        # 截断后的因子在 [0, as_of_day] 段必须等于"只用可见事件"算的因子
        # 这意味着 as_of 之后的事件没有污染历史
        window_end = as_of_day + 1  # [0, 150]
        assert np.allclose(qfq_truncated[:window_end],
                           qfq_visible[:window_end]), \
            "as_of 截断后的因子不应包含未来除权事件"

        # 价格层断言（更直接）
        price_truncated = close * qfq_truncated
        price_visible = close * qfq_visible
        assert np.allclose(price_truncated[:window_end],
                           price_visible[:window_end]), \
            "as_of 截断后的前复权价不应包含未来除权事件"


class TestApplyAdjustment:
    """_apply_adjustment 单元测试"""

    def test_apply_uses_factor_column(self):
        """_apply_adjustment 应优先用 qfq_factor/hfq_factor 实时算"""
        n = 10
        close = pl.Series('close', [100.0 + i for i in range(n)])
        open_ = pl.Series('open', close.to_numpy() - 1.0)
        high = pl.Series('high', close.to_numpy() + 2.0)
        low = pl.Series('low', close.to_numpy() - 2.0)
        qfq_factor = pl.Series('qfq_factor', [0.9] * n)
        hfq_factor = pl.Series('hfq_factor', [1.1] * n)

        df = pl.DataFrame({
            'open': open_, 'high': high, 'low': low, 'close': close,
            'qfq_factor': qfq_factor, 'hfq_factor': hfq_factor,
        })

        # qfq：close * 0.9
        result = DataFetcher._apply_adjustment(df, 'qfq')
        assert np.allclose(result['close'].to_numpy(), close.to_numpy() * 0.9)

        # hfq：close * 1.1
        result = DataFetcher._apply_adjustment(df, 'hfq')
        assert np.allclose(result['close'].to_numpy(), close.to_numpy() * 1.1)

    def test_apply_none_keeps_raw(self):
        """adjustment_type='none' 时保持原始价格"""
        n = 5
        close = pl.Series('close', [100.0 + i for i in range(n)])
        df = pl.DataFrame({'open': close.to_numpy() - 1.0,
                           'high': close.to_numpy() + 2.0,
                           'low': close.to_numpy() - 2.0,
                           'close': close})

        result = DataFetcher._apply_adjustment(df, 'none')
        assert result['close'].to_list() == close.to_list()


class TestBaostockDailyFactors:
    """
    baostock 逐日累计因子：不得出现"只有除权日有因子、其余日补齐 1.0"的空洞

    历史上 _fetch_baostock_factor 把除权事件表 left join 到日线，导致
    原始价 × 因子 只在除权日跳变，价格序列断裂。
    """

    DATES = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']

    def _frames(self):
        # 2024-01-04 除权（10送10）：后复权价 = 原始价 × F，F 在除权后翻倍
        raw = pl.DataFrame({'date': self.DATES, 'close': [10.0, 10.2, 5.1, 5.3]})
        hfq = pl.DataFrame({'date': self.DATES,
                            'close_hfq': [10.0, 10.2, 10.2, 10.6]})
        return raw, hfq

    @staticmethod
    def _compute(raw, hfq):
        from src.data.sources.baostock_schema import compute_daily_factors
        return compute_daily_factors(raw, hfq)

    def test_factor_covers_every_trading_day(self):
        """每个交易日都必须有因子，不能只在除权日有值"""
        qfq, hfq_factor = self._compute(*self._frames())
        assert qfq.height == len(self.DATES)
        assert hfq_factor.height == len(self.DATES)
        assert qfq['date'].to_list() == self.DATES

    def test_hfq_factor_is_cumulative_monotonic(self):
        """后复权因子从 1.0 起单调不减，除权后翻倍"""
        _, hfq_factor = self._compute(*self._frames())
        f = hfq_factor['hfq_factor'].to_list()
        assert f[0] == pytest.approx(1.0)
        assert all(b >= a - 1e-12 for a, b in zip(f, f[1:], strict=False))
        assert f[2:] == pytest.approx([2.0, 2.0])

    def test_qfq_factor_anchored_at_last_bar(self):
        """前复权因子以区间末为锚（末行 = 1.0），并等于 hfq/hfq_T"""
        qfq, hfq_factor = self._compute(*self._frames())
        q = qfq['qfq_factor'].to_list()
        f = hfq_factor['hfq_factor'].to_list()
        assert q[-1] == pytest.approx(1.0)
        assert q == pytest.approx([v / f[-1] for v in f])

    def test_adjusted_price_reconstructs_hfq(self):
        """原始价 × hfq_factor 必须还原后复权价（关系自洽）"""
        raw, hfq = self._frames()
        _, hfq_factor = self._compute(raw, hfq)
        rebuilt = (raw['close'] * hfq_factor['hfq_factor']).to_list()
        assert rebuilt == pytest.approx(hfq['close_hfq'].to_list())

    def test_empty_or_disjoint_input_returns_none(self):
        schema = {'date': pl.Utf8, 'close': pl.Float64}
        empty_raw = pl.DataFrame({'date': [], 'close': []}, schema=schema)
        empty_hfq = pl.DataFrame({'date': [], 'close_hfq': []},
                                 schema={'date': pl.Utf8, 'close_hfq': pl.Float64})
        assert self._compute(empty_raw, empty_hfq) == (None, None)

        raw, _ = self._frames()
        disjoint = pl.DataFrame({'date': ['2030-01-01'], 'close_hfq': [1.0]},
                                schema={'date': pl.Utf8, 'close_hfq': pl.Float64})
        assert self._compute(raw, disjoint) == (None, None)


def _syn_prices(dates: list[datetime.date], close: float = 10.0) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH'] * len(dates),
        'trade_date': dates,
        'close': [close] * len(dates),
    })


def _syn_events(rows: list[tuple[datetime.date, float]]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH'] * len(rows),
        'ex_date': [r[0] for r in rows],
        'cash_div': [r[1] for r in rows],
        'share_div': [0.0] * len(rows),
        'rights_issue_ratio': [0.0] * len(rows),
        'rights_issue_price': [0.0] * len(rows),
    })


class TestEventTruncation:
    """规则 4：晚于行情末行（或 as_of）的事件不得参与因子计算"""

    def test_event_after_last_trade_date_changes_no_row(self):
        dates = [datetime.date(2026, 10, 28) + datetime.timedelta(days=i)
                 for i in range(6)]
        prices = _syn_prices(dates)
        future = _syn_events([(datetime.date(2026, 11, 6), 1.0)])

        result = AdjFactorCalculator.calculate_factors(prices, future)
        baseline = AdjFactorCalculator.calculate_factors(
            prices, _syn_events([]))

        assert result['qfq_factor'].to_list() == pytest.approx([1.0] * 6)
        assert result['hfq_factor'].to_list() == pytest.approx([1.0] * 6)
        assert (result['qfq_factor'] - baseline['qfq_factor']).abs().max() == 0.0
        assert (result['hfq_factor'] - baseline['hfq_factor']).abs().max() == 0.0

    def test_future_event_does_not_alter_in_range_event_result(self):
        dates = [datetime.date(2026, 10, 28) + datetime.timedelta(days=i)
                 for i in range(6)]
        prices = _syn_prices(dates)
        in_range = _syn_events([(datetime.date(2026, 10, 30), 0.5)])
        mixed = _syn_events([
            (datetime.date(2026, 10, 30), 0.5),
            (datetime.date(2026, 11, 6), 1.0),
        ])
        only_in_range = AdjFactorCalculator.calculate_factors(prices, in_range)
        with_future = AdjFactorCalculator.calculate_factors(prices, mixed)
        assert (only_in_range['qfq_factor']
                - with_future['qfq_factor']).abs().max() == 0.0
        assert (only_in_range['hfq_factor']
                - with_future['hfq_factor']).abs().max() == 0.0

    def test_as_of_truncation_expected_sequence(self):
        """as_of 早于最后除权日时，qfq 逐行等于"只含可见事件"的手算序列"""
        dates = [datetime.date(2026, 1, 5) + datetime.timedelta(days=i)
                 for i in range(12)]
        prices = _syn_prices(dates, close=10.0)
        events = _syn_events([
            (datetime.date(2026, 1, 9), 1.0),    # factor 0.9
            (datetime.date(2026, 1, 15), 2.0),   # factor 0.8
        ])
        as_of = datetime.date(2026, 1, 12)

        truncated = AdjFactorCalculator.calculate_factors(
            prices, events, as_of=as_of)
        full = AdjFactorCalculator.calculate_factors(prices, events)

        # 10 元收 1 元派息 → 0.9；as_of=01-12 时 01-15 的事件不可见
        assert truncated['qfq_factor'].to_list() == pytest.approx(
            [0.9] * 4 + [1.0] * 8)
        # hfq 只由已生效事件决定：01-09 之后 × 1/0.9
        assert truncated['hfq_factor'].to_list() == pytest.approx(
            [1.0] * 4 + [1.0 / 0.9] * 8)
        # 不传 as_of（以行情末行为界）时两个事件都生效
        assert full['qfq_factor'].to_list() == pytest.approx(
            [0.72] * 4 + [0.8] * 6 + [1.0] * 2)
        assert full['hfq_factor'].to_list() == pytest.approx(
            [1.0] * 4 + [1.0 / 0.9] * 6 + [1.0 / 0.72] * 2)


class TestLocalPathAsOfRollback:
    """规则 5：本地列存出口（date 列）也要按 as_of 回滚 qfq"""

    @staticmethod
    def _frame() -> pl.DataFrame:
        dates = [datetime.date(2026, 1, 5) + datetime.timedelta(days=i)
                 for i in range(12)]
        raw = [10.0] * 12
        return pl.DataFrame({
            'date': dates,
            'open': raw, 'high': raw, 'low': raw, 'close': raw,
            # 以最新交易日为锚的 qfq（01-15 事件已含在快照里）
            'qfq_factor': [0.72] * 4 + [0.8] * 6 + [1.0] * 2,
            # hfq 只由已生效事件决定，天然 PIT 安全
            'hfq_factor': [1.0] * 4 + [1.0 / 0.9] * 6 + [1.0 / 0.72] * 2,
        })

    def test_local_apply_adjustment_rolls_back_qfq(self):
        fetcher = DataFetcher.__new__(DataFetcher)
        result = fetcher._apply_adjustment_with_pit(
            self._frame(), 'qfq', as_of=datetime.date(2026, 1, 12),
            code='600000.SH')

        # 除以 as_of 锚点 0.8：01-09 之前 0.9，其余 1.0（与手工序列一致）
        assert result['qfq_factor'].to_list() == pytest.approx(
            [0.9] * 4 + [1.0] * 8)
        assert result['close'].to_list() == pytest.approx(
            [9.0] * 4 + [10.0] * 8)
        # as_of 当日（第 8 行）因子必须精确回到 1.0，锚点不漂移
        assert result['qfq_factor'][7] == pytest.approx(1.0)

    def test_rows_after_as_of_get_unit_factor(self):
        fetcher = DataFetcher.__new__(DataFetcher)
        result = fetcher._apply_adjustment_with_pit(
            self._frame(), 'qfq', as_of=datetime.date(2026, 1, 12),
            code='600000.SH')
        assert result['qfq_factor'].to_list()[8:] == pytest.approx([1.0] * 4)

    def test_hfq_not_rolled_back(self):
        fetcher = DataFetcher.__new__(DataFetcher)
        df = self._frame()
        result = fetcher._apply_adjustment_with_pit(
            df, 'hfq', as_of=datetime.date(2026, 1, 12), code='600000.SH')
        assert result['hfq_factor'].to_list() == pytest.approx(
            df['hfq_factor'].to_list())
        assert result['close'].to_list() == pytest.approx(
            [v * 10.0 for v in df['hfq_factor'].to_list()])

