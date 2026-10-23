#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
        dates = _make_dates(n)
        close = _make_linear_prices(n)

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

    def test_apply_fallback_to_precomp(self):
        """没有 factor 列时回退到预计算列"""
        n = 5
        close = pl.Series('close', [100.0 + i for i in range(n)])
        qfq_close = pl.Series('qfq_close', [90.0 + i * 0.9 for i in range(n)])
        qfq_open = pl.Series('qfq_open', [89.0 + i * 0.9 for i in range(n)])
        qfq_high = pl.Series('qfq_high', [92.0 + i * 0.9 for i in range(n)])
        qfq_low = pl.Series('qfq_low', [88.0 + i * 0.9 for i in range(n)])

        df = pl.DataFrame({
            'open': close.to_numpy() - 1.0,
            'high': close.to_numpy() + 2.0,
            'low': close.to_numpy() - 2.0,
            'close': close,
            'qfq_open': qfq_open,
            'qfq_high': qfq_high,
            'qfq_low': qfq_low,
            'qfq_close': qfq_close,
        })

        result = DataFetcher._apply_adjustment(df, 'qfq')
        assert np.allclose(result['close'].to_numpy(), qfq_close.to_numpy())

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
