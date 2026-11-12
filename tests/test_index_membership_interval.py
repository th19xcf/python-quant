#!/usr/bin/env python3

"""指数成分 PIT 区间的开闭口径测试。

背景：`membership_intervals` 的 `out_date` 由生产侧
`src/data/quality/extended_builders.py:495` 定义为「首个该标的不在册的快照日」，
即半开区间 `[in_date, out_date)`。官方月度快照两条证据已把口径定死为半开
（20/20 计数一致；`out_date` 当日仍在册 0/5,935），详见
`2026-11-12/data-lineage/outputs/b2_slice2_recommendations.md` §1。

本文件锁死两个消费侧：
- `tools/research/l1_baseline/run_l1.py::index_long`
- `tools/research/domain_composite/run_domain.py::domain_members`

任一处把 `>` 改回 `>=`，`test_out_date_excluded_*` 必然失败。
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]

IN = dt.date(2024, 1, 2)
OUT = dt.date(2024, 6, 28)          # 首个不在册日（半开区间的右端，不含）
BEFORE = dt.date(2024, 6, 27)
AFTER = dt.date(2024, 7, 31)

INTERVALS = pl.DataFrame({
    'index_code': ['000300', '000300', '000905'],
    'ts_code': ['600000.SH', '000001.SZ', '600519.SH'],
    # 第三只 out_date 为 null：一旦纳入，永不出册
    'in_date': [IN, IN, IN],
    'out_date': [OUT, OUT, None],
}, schema_overrides={'in_date': pl.Date, 'out_date': pl.Date})


def _load(rel: str, name: str):
    """按文件路径加载 tools 下的脚本（这些目录不是包）。"""
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def idx_file(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp('idx') / 'membership_intervals.parquet'
    INTERVALS.write_parquet(path)
    return path


@pytest.fixture(scope='module')
def l1(idx_file):
    module = _load('tools/research/l1_baseline/run_l1.py', '_l1_interval_under_test')
    module.IDX = idx_file
    return module


@pytest.fixture(scope='module')
def domain(idx_file):
    module = _load('tools/research/domain_composite/run_domain.py',
                   '_domain_interval_under_test')
    module.IDX = idx_file
    module.DOMAINS = {'沪深300': '000300', '中证500': '000905'}
    return module


class TestIndexLongHalfOpen:
    def test_out_date_excluded_at_boundary(self, l1):
        """`out_date` 当日必须判为不在册（改回 `>=` 时本行必然红）。

        `600519.SH` 的 `out_date` 为 null，仍应在册，故只比对其余两只。
        """
        got = l1.index_long([OUT])
        assert got['ts_code'].to_list() == ['600519.SH']

    def test_day_before_out_still_member(self, l1):
        got = l1.index_long([BEFORE]).sort('ts_code')
        assert got['ts_code'].to_list() == ['000001.SZ', '600000.SH', '600519.SH']

    def test_day_after_out_excluded(self, l1):
        got = l1.index_long([AFTER]).sort('ts_code')
        assert got['ts_code'].to_list() == ['600519.SH']

    def test_null_out_date_never_expires(self, l1):
        got = l1.index_long([dt.date(2030, 1, 2)])
        assert got['ts_code'].to_list() == ['600519.SH']


class TestDomainMembersHalfOpen:
    def test_out_date_excluded_at_boundary(self, domain):
        got = domain.domain_members([OUT])
        assert got['沪深300'].height == 0, '沪深300 在 out_date 当日仍纳入成分'
        # 000905 只含 out_date 为 null 的那只，应保留且仅此一只
        assert got['中证500']['ts_code'].to_list() == ['600519.SH']

    def test_day_before_out_still_member(self, domain):
        got = domain.domain_members([BEFORE])
        assert sorted(got['沪深300']['ts_code'].to_list()) == ['000001.SZ', '600000.SH']
        assert got['中证500']['ts_code'].to_list() == ['600519.SH']
