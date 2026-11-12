#!/usr/bin/env python3

"""风险过滤契约测试：ST 三态、行业未知桶、事件冷却与开关。"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from src.advice.config import PortfolioConstraints, RiskFilterConfig
from src.advice.portfolio import Candidate, allocate
from src.advice.risk_filters import (
    INDUSTRY_SOURCE_L2,
    INDUSTRY_SOURCE_MISSING,
    INDUSTRY_UNKNOWN,
    RiskContext,
)
from src.advice.universe import StockMeta
from src.data.risk_flags import NON_ST, ST
from src.data.risk_flags import UNKNOWN as ST_UNKNOWN

DAY = date(2025, 1, 10)
TRADING = [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8),
           date(2025, 1, 9), date(2025, 1, 10)]


def _write_st(root, rows) -> None:
    path = root / 'meta' / 'st_status' / 'st_status_history.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'code': [row[0] for row in rows],
        'name': [row[1] for row in rows],
        'status': [row[2] for row in rows],
        'source': ['test'] * len(rows),
        'reason': [''] * len(rows),
        'as_of': [DAY] * len(rows),
        'is_st': [row[2] == ST for row in rows],
    }).write_parquet(path)


def _write_industry(root, rows) -> None:
    path = root / 'meta' / 'industry' / 'industry_history.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'code': [row[0] for row in rows],
        'industry_l1': ['test'] * len(rows),
        'industry_l2': [row[1] for row in rows],
        'industry_code': [''] * len(rows),
        'source': ['tdxhy'] * len(rows),
        'as_of': [DAY] * len(rows),
    }).write_parquet(path)


def _write_events(root) -> None:
    path = root / 'meta' / 'events' / 'events.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'event_type': ['监管处罚'],
        'publish_time': [datetime(2025, 1, 8, 16, 0)],
        'title': ['处罚'],
        'event_id': ['e1'],
        'source': ['test'],
    }).write_parquet(path)
    (root / 'meta' / 'events' / 'coverage.json').write_text(
        '{"source":"test","start":"2025-01-06","end":"2025-01-10",'
        '"rows":1,"complete":true}', encoding='utf-8')


@pytest.fixture
def ctx(tmp_path):
    _write_st(tmp_path, [
        ('600000.SH', '浦发银行', NON_ST),
        ('000001.SZ', 'ST某某', ST),
        ('600519.SH', '某某', ST_UNKNOWN),
    ])
    _write_industry(tmp_path, [
        ('600000.SH', '银行'),
        ('000001.SZ', '银行'),
        ('600519.SH', INDUSTRY_UNKNOWN),
    ])
    return RiskContext(RiskFilterConfig(), tmp_path, TRADING)


class TestST:
    def test_three_states(self, ctx):
        assert ctx.st_status('600000.SH', DAY) == NON_ST
        assert ctx.st_status('000001.SZ', DAY) == ST
        assert ctx.st_status('600519.SH', DAY) == ST_UNKNOWN

    def test_unknown_is_not_non_st(self, ctx):
        # 默认 fail-closed：unknown 一律不进买入候选，单列 st_unknown_rejected
        verdict = ctx.evaluate('600519.SH', DAY)
        assert not verdict.ok
        assert verdict.kind == 'st_unknown'
        assert ctx.counts['st_unknown_rejected'] == 1
        assert ctx.counts['st_unknown_passed'] == 0
        assert ctx.counts['st_dropped'] == 0

    def test_drop_policy_rejects_unknown(self, tmp_path):
        _write_st(tmp_path, [('600519.SH', '某某', ST_UNKNOWN)])
        context = RiskContext(RiskFilterConfig(st_unknown_policy='drop'),
                              tmp_path, TRADING)
        assert not context.evaluate('600519.SH', DAY).ok
        assert context.counts['st_unknown_rejected'] == 1
        assert context.counts['st_dropped'] == 0
        assert context.counts['st_unknown_passed'] == 0

    def test_st_is_rejected_with_audit(self, ctx):
        verdict = ctx.evaluate('000001.SZ', DAY)
        assert not verdict.ok
        assert verdict.kind == 'st'
        assert ctx.audit[0]['ts_code'] == '000001.SZ'
        assert ctx.audit[0]['kind'] == 'st'

    def test_no_snapshot_is_unknown(self, tmp_path):
        context = RiskContext(RiskFilterConfig(), tmp_path, TRADING)
        assert context.st_status('600000.SH', DAY) == ST_UNKNOWN
        # fail-closed：没有快照不等于安全放行
        assert not context.evaluate('600000.SH', DAY).ok
        assert context.counts['st_unknown_rejected'] == 1


class TestIndustry:
    def test_bucket_and_source(self, ctx):
        assert ctx.industry('600000.SH', DAY) == ('银行', INDUSTRY_SOURCE_L2)
        assert ctx.industry('600519.SH', DAY) == (INDUSTRY_UNKNOWN,
                                                  INDUSTRY_SOURCE_MISSING)

    def test_unknown_is_separate_bucket(self, ctx):
        meta = {
            '600000.SH': StockMeta('600000.SH', '浦发银行', 'tdx_names', '银行',
                                   INDUSTRY_SOURCE_L2),
            '600519.SH': StockMeta('600519.SH', '某某', 'tdx_names', '沪主板',
                                   'code_prefix_board'),
        }
        enriched = ctx.enrich_meta(meta, DAY)
        assert enriched['600000.SH'].industry == '银行'
        assert enriched['600519.SH'].industry == INDUSTRY_UNKNOWN
        assert enriched['600519.SH'].industry != meta['600519.SH'].industry
        assert enriched['600519.SH'].industry_source == INDUSTRY_SOURCE_MISSING

    def test_disabled_keeps_original_meta(self, ctx):
        meta = {'600000.SH': StockMeta('600000.SH', '浦发银行', 'tdx_names',
                                       '银行', INDUSTRY_SOURCE_L2)}
        assert ctx.enrich_meta(meta, DAY) == meta

    def test_unknown_bucket_has_no_industry_cap(self):
        codes = ['600000.SH', '000001.SZ', '600519.SH', '300750.SZ']
        candidates = [Candidate(code, 1.0, 10.0, INDUSTRY_UNKNOWN,
                                INDUSTRY_SOURCE_MISSING) for code in codes]
        allocations = allocate(candidates, 1_000_000.0,
                               PortfolioConstraints(cash_buffer=0.0), max_positions=4)
        # 单票上限 8% × 4 = 32%，超过 25% 的行业上限也**不被压缩**
        assert sum(item.target_weight for item in allocations) == pytest.approx(0.32)

    def test_known_industry_is_capped(self):
        constraints = PortfolioConstraints(cash_buffer=0.0)
        codes = ['600000.SH', '000001.SZ', '600519.SH', '300750.SZ']
        candidates = [Candidate(code, 1.0, 10.0, '银行', INDUSTRY_SOURCE_L2)
                      for code in codes]
        allocations = allocate(candidates, 1_000_000.0, constraints, max_positions=4)
        bank = sum(item.target_weight for item in allocations)
        # 整手下单会造成向下取整的微小偏差，上限仍然成立
        assert bank <= constraints.max_industry_weight + 1e-9
        assert bank >= constraints.max_industry_weight - 0.01


class TestEvent:
    def test_cooling_drops_candidate(self, tmp_path):
        _write_events(tmp_path)
        _write_st(tmp_path, [('000001.SZ', '某某', NON_ST)])
        context = RiskContext(RiskFilterConfig(), tmp_path, TRADING)
        verdict = context.evaluate('000001.SZ', DAY)
        assert not verdict.ok
        assert verdict.kind == 'event'
        assert '监管处罚' in verdict.reason
        assert context.audit[-1]['event_type'] == '监管处罚'

    def test_coverage_outside_is_unknown_not_clear(self, tmp_path):
        _write_events(tmp_path)
        _write_st(tmp_path, [('000001.SZ', '某某', NON_ST)])
        context = RiskContext(RiskFilterConfig(), tmp_path,
                              [date(2025, 2, 3)])
        status = context.event_status('000001.SZ', date(2025, 2, 3))
        assert status['status'] == 'unknown'
        assert status['reason'] == 'no_coverage'
        assert context.evaluate('000001.SZ', date(2025, 2, 3)).ok
        assert context.counts['event_unknown_passed'] == 1

    def test_unknown_drop_policy(self, tmp_path):
        context = RiskContext(
            RiskFilterConfig(event_unknown_policy='drop', st_enabled=False),
            tmp_path, TRADING)
        assert not context.evaluate('600000.SH', DAY).ok

    def test_missing_table_is_unknown(self, tmp_path):
        _write_st(tmp_path, [('600000.SH', '浦发银行', NON_ST)])
        context = RiskContext(RiskFilterConfig(), tmp_path, TRADING)
        assert context.event_status('600000.SH', DAY)['reason'] == 'no_data'
        assert context.evaluate('600000.SH', DAY).ok


class TestSwitches:
    def test_all_disabled_never_blocks(self, tmp_path):
        _write_st(tmp_path, [('000001.SZ', 'ST某某', ST)])
        _write_events(tmp_path)
        context = RiskContext(
            RiskFilterConfig(st_enabled=False, industry_enabled=False,
                             event_enabled=False),
            tmp_path, TRADING)
        assert context.evaluate('000001.SZ', DAY).ok
        assert context.evaluate('000001.SZ', DAY).ok
        assert context.counts['st_dropped'] == 0
        assert context.counts['event_dropped'] == 0

    def test_invalid_policy_rejected(self):
        with pytest.raises(ValueError, match='st_unknown_policy'):
            RiskFilterConfig(st_unknown_policy='auto').validate()

    def test_defaults_are_explicit(self):
        config = RiskFilterConfig()
        assert config.st_enabled and config.industry_enabled and config.event_enabled
        assert config.st_unknown_policy == 'drop'
        assert config.event_unknown_policy == 'keep'
        assert config.cooldowns() == {'并购重组': 20, '重大合同/中标': 5,
                                      '监管处罚': 5}
