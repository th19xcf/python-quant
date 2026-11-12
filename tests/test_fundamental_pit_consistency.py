#!/usr/bin/env python3

"""财务因子 PIT（时点可得性）口径一致性测试。

背景与实测证据见 ``docs/财务PIT口径一致性_2026-11-12.md``。这里锁两类事实：

一、回归锁定（当前已正确，改坏就红）
- ``store.latest_announced_as_of`` 只认 ``ann_date <= as_of``，``ann_date`` 为空永不可见；
- 同一 ``ann_date`` 有多期公告时取 ``end_date`` 最新的一期；
- 同一报告期多版本时，``store`` / ``fundamental_factors`` / ``build_new_factors`` 均取
  「截至 t 的最新公告版」（``build_new_factors.first_period`` 原为最早版，已统一）；
- 单版本数据（当前正式表的实际形态）上三者逐值等价；
- 正式表 ``ann_date`` 无空值且不早于 ``end_date``；``ann_date == end_date`` 的占位行全部
  落在 2009-12-31 之前且全部来自 ``sina_publish``。

二、当前违反（``xfail(strict=True)``，写明违反事实与待修方案，不许为变绿放宽）
- ``fundamental_factors`` 的 TTM 无「上年年报/上年同期公告日不晚于本期」守卫，
  与 ``build_new_factors`` 在真实数据上不一致；
- 三条消费路径均不按 ``ann_date_source`` 过滤，新浪旧报表段的「报告期占位」会被当作可见日；
- 真实数据中大量 ``ann_date`` 晚于法定披露截止日，仍被原样接受；
- 总资产同比使用了公告日晚于本期的上年同期数据（两条路径都缺守卫）。

本文件只读数据、只读源码，不写 ``data/``、不改 ``src/``。
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import polars as pl
import pytest

from src.advice import fundamental_factors as ff
from src.data.financials import store

ROOT = Path(__file__).resolve().parents[1]
FIN = ROOT / 'data' / 'local_store' / 'meta' / 'financials'


def _load_build_new_factors():
    """按文件路径加载 ``tools/research/build_new_factors.py``（该目录不是包）。"""
    path = ROOT / 'tools' / 'research' / 'build_new_factors.py'
    spec = importlib.util.spec_from_file_location('_bnf_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)      # 模块底部有 __main__ 守卫，import 不跑主流程
    return module


bnf = _load_build_new_factors()


def _income_frame(rows: list[tuple[str, str, str, float]]) -> pl.DataFrame:
    """(ts_code, end_date, ann_date, parent_net_profit) → 与正式表同构的最小帧。"""
    return pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'end_date': [dt.date.fromisoformat(r[1]) for r in rows],
        'ann_date': [dt.date.fromisoformat(r[2]) for r in rows],
        'parent_net_profit': [r[3] for r in rows],
    }, schema_overrides={'ts_code': pl.Utf8, 'end_date': pl.Date,
                         'ann_date': pl.Date, 'parent_net_profit': pl.Float64})


@pytest.fixture(scope='module')
def real_income() -> pl.DataFrame:
    path = FIN / 'income.parquet'
    if not path.exists():
        pytest.skip(f'缺少正式财务表: {path}')
    return pl.read_parquet(path)


# ---------------------------------------------------------------------------
# 一、回归锁定：store 的点时查询
# ---------------------------------------------------------------------------
class TestStorePointInTime:
    """``store`` 的三个可见性规则（今天是正确行为，改坏即前视）。"""

    def _store(self, tmp_path: Path) -> Path:
        rows = [
            # 2020Q3：2020-10-28 公告
            ('000001.SZ', '2020-09-30', '2020-10-28', 300.0),
            # 2020Q4：2021-03-30 公告
            ('000001.SZ', '2020-12-31', '2021-03-30', 400.0),
        ]
        frame = _income_frame(rows)
        store.write_table(frame, 'income', tmp_path)
        return tmp_path

    def test_respects_ann_date_boundary(self, tmp_path):
        root = self._store(tmp_path)
        # 2020-10-27 尚未公告 2020Q3
        assert store.latest_announced_as_of('000001.SZ', '2020-10-27', 'income', root) is None
        # 公告当日即可见
        hit = store.latest_announced_as_of('000001.SZ', '2020-10-28', 'income', root)
        assert hit['end_date'] == dt.date(2020, 9, 30)
        assert hit['parent_net_profit'] == 300.0

    def test_null_ann_date_never_visible(self, tmp_path):
        frame = _income_frame([('000002.SZ', '2020-09-30', '2020-10-28', 1.0)])
        frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias('ann_date'))
        store.write_table(frame, 'income', tmp_path)
        # end_date 早已过去，但 ann_date 为空 → 永远不可见，不能让 end_date 蒙混过关
        assert store.latest_announced_as_of('000002.SZ', '2030-01-01', 'income', tmp_path) is None

    def test_same_ann_date_picks_latest_period(self, tmp_path):
        rows = [
            # 年报与一季报同日披露
            ('600000.SH', '2022-12-31', '2023-04-28', 800.0),
            ('600000.SH', '2023-03-31', '2023-04-28', 200.0),
        ]
        store.write_table(_income_frame(rows), 'income', tmp_path)
        hit = store.latest_announced_as_of('600000.SH', '2023-04-28', 'income', tmp_path)
        assert hit['end_date'] == dt.date(2023, 3, 31)


# ---------------------------------------------------------------------------
# 一、回归锁定：三条去重口径的定义
# ---------------------------------------------------------------------------
class TestVersionSelectionConventions:
    """同报告期多版本：三条口径统一取「截至 t 的最新公告版」。"""

    ROWS = [
        ('000001.SZ', '2020-09-30', '2020-10-28', 100.0),   # 原始公告
        ('000001.SZ', '2020-09-30', '2021-04-30', 130.0),   # 更正/追溯调整
    ]

    def test_fundamental_factors_keeps_latest_version(self):
        got = ff._dedupe_period(_income_frame(self.ROWS), ['parent_net_profit'])
        assert got.height == 1
        assert got['ann_date'][0] == dt.date(2021, 4, 30)
        assert got['parent_net_profit'][0] == 130.0

    def test_build_new_factors_keeps_latest_version(self):
        """``build_new_factors.first_period`` 的口径已统一为「最新公告版」。

        原名 ``test_build_new_factors_keeps_earliest_version``，锁的是旧的
        ``.first()`` 实现；现按新实现锁死两条：
        - 不截断时取 ``ann_date`` 更新的一版（2021-04-30 / 130.0）；
        - 截断到 t=2020-12-31（与 ``points_at`` 的 ``pub_date <= t`` 同语义）后，
          ``ann_date`` 更晚的那一版不可见，只能取 2020-10-28 / 100.0。
        """
        full = bnf.first_period(_income_frame(self.ROWS), ['parent_net_profit'])
        assert full.height == 1
        assert full['ann_date'][0] == dt.date(2021, 4, 30)
        assert full['parent_net_profit'][0] == 130.0

        before = bnf.first_period(
            _income_frame(self.ROWS).filter(pl.col('ann_date') <= dt.date(2020, 12, 31)),
            ['parent_net_profit'])
        assert before.height == 1
        assert before['ann_date'][0] == dt.date(2020, 10, 28)
        assert before['parent_net_profit'][0] == 100.0

    def test_three_paths_agree_on_latest_version(self, tmp_path):
        """同一多版本构造输入下，三条口径必须取到同一个 ``ann_date`` 与同一个值。"""
        frame = _income_frame(self.ROWS)
        store.write_table(frame, 'income', tmp_path)
        a = store.latest_announced_as_of('000001.SZ', '2021-05-01', 'income', tmp_path)
        b = ff._dedupe_period(frame, ['parent_net_profit'])
        c = bnf.first_period(frame, ['parent_net_profit'])
        expected_date = dt.date(2021, 4, 30)
        assert (a['ann_date'], a['parent_net_profit']) == (expected_date, 130.0)
        assert b['ann_date'].to_list() == [expected_date]
        assert b['parent_net_profit'].to_list() == [130.0]
        assert c['ann_date'].to_list() == [expected_date]
        assert c['parent_net_profit'].to_list() == [130.0]

    def test_store_returns_latest_version_at_panel_level(self, tmp_path):
        store.write_table(_income_frame(self.ROWS), 'income', tmp_path)
        hit = store.latest_announced_as_of('000001.SZ', '2021-05-01', 'income', tmp_path)
        assert hit['ann_date'] == dt.date(2021, 4, 30)
        assert hit['parent_net_profit'] == 130.0

    def test_conventions_agree_when_single_version(self):
        """正式表当前每期只有一行：此时三条口径必须逐值等价。"""
        rows = [('000001.SZ', '2020-06-30', '2020-08-28', 50.0),
                ('000001.SZ', '2020-09-30', '2020-10-28', 90.0)]
        frame = _income_frame(rows)
        a = ff._dedupe_period(frame, ['parent_net_profit']).sort('end_date')
        b = bnf.first_period(frame, ['parent_net_profit']).sort('end_date')
        assert a['parent_net_profit'].to_list() == b['parent_net_profit'].to_list()
        assert a['ann_date'].to_list() == b['ann_date'].to_list()


# ---------------------------------------------------------------------------
# 一、回归锁定：正式表的既有事实
# ---------------------------------------------------------------------------
class TestRealDataInvariants:
    def test_ann_date_present_and_not_before_end_date(self, real_income):
        assert real_income['ann_date'].null_count() == 0
        assert real_income.filter(
            pl.col('ann_date') < pl.col('end_date')).height == 0

    def test_placeholder_rows_confined_to_pre2010_sina(self, real_income):
        """``ann_date == end_date`` 只允许出现在 2009-12-31 之前的 sina 段。

        这是当前的数据事实（income 720 / balance 649 / cashflow 7）。一旦出现
        2010 年之后的占位行，说明补数流程又开始用报告期冒充公告日，必须立刻红。
        """
        ph = real_income.filter(pl.col('ann_date') == pl.col('end_date'))
        assert ph.height == 720
        assert set(ph['ann_date_source'].unique().to_list()) == {'sina_publish'}
        assert ph['end_date'].max() == dt.date(2009, 12, 31)

    def test_each_period_has_single_row(self, real_income):
        """正式表每个 (ts_code, end_date) 只有一行——三条口径今天不可能取到不同版本。"""
        periods = real_income.select(['ts_code', 'end_date']).n_unique()
        assert real_income.height == periods


# ---------------------------------------------------------------------------
# 二、当前违反（待修，不许放宽）
# ---------------------------------------------------------------------------
class TestKnownPitViolations:
    @pytest.mark.xfail(strict=True, reason='ff 的 TTM 缺「上年公告日不晚于本期」守卫，实测 7,977 期冲突')
    def test_ttm_guard_divergence_between_paths(self, real_income):
        """违反事实：``fundamental_factors.ttm_from_cumulative`` 没有「上年年报 /
        上年同期累计的公告日不晚于本期公告日」守卫，``build_new_factors.ttm`` 有。

        实测：income.parent_net_profit 有 7,977 个非年报期（占 3.258%）在 A 路径算出
        非空 TTM、B 路径为 null；2015 年后仍有 864 期。待修方案：把 bnf 的
        ``usable`` 守卫搬进 ``fundamental_factors.ttm_from_cumulative``（改 src 需单独立项）。
        """
        t = ff.ttm_from_cumulative(real_income, ['parent_net_profit'])
        # 复刻 bnf 的守卫
        cur = bnf.first_period(real_income, ['parent_net_profit'])
        prev = cur.select(['ts_code', pl.col('end_date').alias('_prev_end'),
                           pl.col('ann_date').alias('_prev_ann')])
        annual = (cur.filter(pl.col('end_date').dt.month() == 12)
                  .select(['ts_code', (pl.col('end_date').dt.year() + 1).alias('_fy'),
                           pl.col('ann_date').alias('_annual_ann')]))
        guarded = (t.filter(pl.col('end_date').dt.month() != 12)
                   .with_columns(pl.col('end_date').dt.offset_by('-1y').alias('_prev_end'),
                                 pl.col('end_date').dt.year().alias('_fy'))
                   .join(prev, on=['ts_code', '_prev_end'], how='left')
                   .join(annual, on=['ts_code', '_fy'], how='left'))
        late = guarded.filter(
            (pl.col('_prev_ann') > pl.col('ann_date'))
            | (pl.col('_annual_ann') > pl.col('ann_date')))
        assert late.height == 0, (
            f'ff 的 TTM 使用了 {late.height} 个公告日晚于本期的上年报表（实测 7,977 期）')

    @pytest.mark.xfail(strict=True, reason='三条路径都不看 ann_date_source，占位行在 2009 年末被判可见')
    def test_consumers_do_not_filter_placeholder_ann_source(self, real_income):
        """违反事实：三条路径都只校验 ``ann_date`` 非空，不看 ``ann_date_source``。

        新浪旧报表段的 1,376 行（income 720 / balance 649 / cashflow 7）把报告期本身
        当公告日；虽然全部早于 2010 且当前面板从 2015 起，实际暴露为 0 个月，但
        任何把评估窗口前移到 2010 前的改动都会静默引入前视。
        待修方案：在 ``store`` 读取侧增加 ``ann_date_source != 'report_period_placeholder'``
        的显式过滤（当前数据没有该标记，需先在采集侧把占位行单列标记）。
        """
        ph = real_income.filter(pl.col('ann_date') == pl.col('end_date'))
        visible = store.latest_announced_panel(
            list(ph['ts_code'].unique()), '2009-12-31', 'income')
        assert visible.height == 0, '占位行在 2009 年末仍被判为可见（静默前视）'

    @pytest.mark.xfail(strict=True, reason='73,450 行 ann_date 晚于法定披露截止日，消费侧无标记无过滤')
    def test_ann_date_within_statutory_deadline(self, real_income):
        """违反事实：``ann_date`` 晚于法定披露截止日的行有 73,450 行（21.308%），
        其中 52,150 行来自 ``sina_publish``、21,300 行来自 ``em_notice``。

        ann_date 偏晚是保守方向（晚可见不会前视），但它同时说明该列不可单独作
        可信边界：正因如此才会有 7,977 期 TTM 守卫冲突。
        待修方案：数据侧按 ``ann_date > statutory_deadline(end_date)`` 单列标记，
        消费侧对该段 TTL 或改口径，需走数据治理流程。
        """
        deadline = pl.when(pl.col('end_date').dt.month() == 12).then(
            pl.date(pl.col('end_date').dt.year() + 1, 4, 30)
        ).when(pl.col('end_date').dt.month() == 3).then(
            pl.date(pl.col('end_date').dt.year(), 4, 30)
        ).when(pl.col('end_date').dt.month() == 6).then(
            pl.date(pl.col('end_date').dt.year(), 8, 31)
        ).otherwise(pl.date(pl.col('end_date').dt.year(), 10, 31))
        late = real_income.filter(pl.col('ann_date') > deadline)
        assert late.height == 0, f'{late.height} 行 ann_date 晚于法定披露截止日（实测 73,450）'

    @pytest.mark.xfail(strict=True, reason='总资产同比缺上年同期公告日守卫，实测 37,042 期会被拦截')
    def test_yoy_uses_only_already_announced_prior_year(self, real_income):
        """违反事实：``fundamental_factors.balance_metrics`` 与 bnf 的应计路径都用
        上年同期总资产算同比，却不校验上年同期的公告日是否早于本期公告日。

        实测（若施加该守卫）：balance.total_equity 有 37,042 个非年报期（15.872%）
        会被拦截。待修方案：与 TTM 守卫同批统一到读数层。
        """
        cur = (real_income.filter(pl.col('ann_date').is_not_null())
               .sort(['ts_code', 'end_date', 'ann_date'])
               .group_by(['ts_code', 'end_date'], maintain_order=True)
               .agg([pl.col('ann_date').last()]))
        prev = cur.select(['ts_code', pl.col('end_date').alias('_prev_end'),
                           pl.col('ann_date').alias('_prev_ann')])
        merged = (cur.filter(pl.col('end_date').dt.month() != 12)
                  .with_columns(pl.col('end_date').dt.offset_by('-1y').alias('_prev_end'))
                  .join(prev, on=['ts_code', '_prev_end'], how='left'))
        late = merged.filter(pl.col('_prev_ann') > pl.col('ann_date'))
        assert late.height == 0, (
            f'{late.height} 个报告期的上年同期公告日晚于本期（实测 income 26 期）')
