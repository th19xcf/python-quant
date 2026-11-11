#!/usr/bin/env python3
"""锁定 ``build_new_factors`` 的 PIT 版本语义：同一报告期多版本取**最新公告版**。

背景：``tools/research/build_new_factors.py`` 原先按 ``ann_date`` 升序取 ``first()``，
即「最早公告版」；而 ``src/data/financials/store.py::latest_announced_as_of`` 与
``src/advice/fundamental_factors.py::_dedupe_period`` 都取「最新公告版」。三条路径
口径不一致。2026-11-12 统一为「as of 时点可见的最新版本」，本文件锁死该语义。

``ann_date`` 同时是可见性门槛，因此取最新版不会引入前视：
在任意时点 T，只认 ``ann_date <= T`` 的行，其中公告日最新的一版覆盖旧版。

本文件只读源码，不读写 ``data/``。既有 ``tests/test_fundamental_pit_consistency.py``
里的 ``test_build_new_factors_keeps_latest_version`` 与本文件锁同一语义，互为冗余校验。
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import polars as pl
import pytest

from src.advice import fundamental_factors as ff

ROOT = Path(__file__).resolve().parents[1]


def _load_build_new_factors():
    """按文件路径加载 ``tools/research/build_new_factors.py``（该目录不是包）。"""
    path = ROOT / 'tools' / 'research' / 'build_new_factors.py'
    spec = importlib.util.spec_from_file_location('_bnf_pit_latest', path)
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


# 同一 2020Q3 两版：原始公告 100，次年更正为 130
TWO_VERSIONS = [
    ('000001.SZ', '2020-09-30', '2020-10-28', 100.0),
    ('000001.SZ', '2020-09-30', '2021-04-30', 130.0),
]


class TestLatestAnnouncedVersion:
    def test_keeps_latest_version_not_earliest(self):
        got = bnf.first_period(_income_frame(TWO_VERSIONS), ['parent_net_profit'])
        assert got.height == 1
        assert got['ann_date'][0] == dt.date(2021, 4, 30), '必须取最新公告版'
        assert got['parent_net_profit'][0] == 130.0, '必须取更正后的值'

    def test_agrees_with_fundamental_factors_dedupe(self):
        """与 ``fundamental_factors._dedupe_period`` 逐值一致（口径统一）。"""
        frame = _income_frame(TWO_VERSIONS)
        a = ff._dedupe_period(frame, ['parent_net_profit']).sort('end_date')
        b = bnf.first_period(frame, ['parent_net_profit']).sort('end_date')
        assert a['ann_date'].to_list() == b['ann_date'].to_list()
        assert a['parent_net_profit'].to_list() == b['parent_net_profit'].to_list()

    def test_ann_date_is_visibility_gate_no_lookahead(self):
        """最新版只在更正公告日之后可见：更正前仍应看到旧版。"""
        frame = _income_frame(TWO_VERSIONS)
        # 模拟 as-of 2020-12-31：更正公告尚未发生，只能看到原始公告
        asof = frame.filter(pl.col('ann_date') <= dt.date(2020, 12, 31))
        got = bnf.first_period(asof, ['parent_net_profit'])
        assert got['parent_net_profit'][0] == 100.0
        assert got['ann_date'][0] == dt.date(2020, 10, 28)

    def test_null_ann_date_rows_dropped(self):
        frame = _income_frame([('000002.SZ', '2020-09-30', '2020-10-28', 1.0)])
        frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias('ann_date'))
        assert bnf.first_period(frame, ['parent_net_profit']).height == 0

    def test_single_version_unchanged(self):
        """单版本数据（当前正式表形态）上新旧口径必然逐值相同。"""
        rows = [('000001.SZ', '2020-06-30', '2020-08-28', 50.0),
                ('000001.SZ', '2020-09-30', '2020-10-28', 90.0)]
        got = bnf.first_period(_income_frame(rows), ['parent_net_profit'])
        assert got['parent_net_profit'].to_list() == [50.0, 90.0]
        assert got['ann_date'].to_list() == [dt.date(2020, 8, 28),
                                            dt.date(2020, 10, 28)]


class TestTtmGuardUntouched:
    """TTM 守卫的不对称本次**不改**（另一个待定口径），此处只锁「守卫仍然生效」。"""

    def test_ttm_requires_prior_year_announced(self):
        rows = [
            ('000001.SZ', '2020-12-31', '2021-04-30', 400.0),   # 上年年报
            ('000001.SZ', '2020-09-30', '2020-10-28', 90.0),    # 上年同期累计
            ('000001.SZ', '2021-09-30', '2021-10-28', 100.0),   # 本期累计
        ]
        got = bnf.ttm(_income_frame(rows), ['parent_net_profit'])
        cur = got.filter(pl.col('end_date') == dt.date(2021, 9, 30))
        assert cur.height == 1
        # 上年年报/上年同期公告日均早于本期公告日 → TTM 可算 = 100 + 400 - 90
        assert cur['parent_net_profit__ttm'][0] == 410.0

    def test_ttm_blocked_when_prior_year_announced_later(self):
        rows = [
            ('000001.SZ', '2020-12-31', '2021-04-30', 400.0),
            ('000001.SZ', '2020-09-30', '2022-01-10', 90.0),   # 上年同期公告晚于本期
            ('000001.SZ', '2021-09-30', '2021-10-28', 100.0),
        ]
        got = bnf.ttm(_income_frame(rows), ['parent_net_profit'])
        cur = got.filter(pl.col('end_date') == dt.date(2021, 9, 30))
        assert cur['parent_net_profit__ttm'][0] is None
