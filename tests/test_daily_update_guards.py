"""单入口行情末日断言与基金/因子刷新步骤的回归测试。

覆盖：
1. 行情末日 != 最近交易日时 build 步判失败，并使退出码为 1（负向用例）；
2. 末日相等时通过（证明断言不是恒真）；
3. 基金行情 / 复权因子在已最新时跳过、在源仍落后时判失败；
4. 新增两步位于构建之后、洁净集之前，备份与门禁仍是末尾两步。
"""

import types
from datetime import date

import polars as pl

from tools import daily_update as du


def _write_daily_index(root, rows: list[tuple[str, str, date]]) -> None:
    path = root / 'meta' / 'daily_index.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [row[0] for row in rows],
        'data_type': [row[1] for row in rows],
        'rows': [10] * len(rows),
        'start': [date(2026, 11, 1)] * len(rows),
        'end': [row[2] for row in rows],
    }).write_parquet(path)


def _write_factor(root, name: str, codes: list[str], trade_day: date) -> None:
    path = root / 'factors' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': codes,
        'trade_date': [trade_day] * len(codes),
        'qfq_factor': [1.0] * len(codes),
        'hfq_factor': [1.0] * len(codes),
    }).write_parquet(path)


def _build_args(**overrides) -> types.SimpleNamespace:
    base = {'start': None, 'end': None, 'skip_fund_refresh': False,
            'skip_factor_refresh': False}
    base.update(overrides)
    return types.SimpleNamespace(**base)


def test_daily_index_end_mismatch_fails_build_step(tmp_path, monkeypatch):
    """负向用例：daily_index 停在 11-13、最近交易日 11-16 → 必红且退出码 1。"""
    _write_daily_index(tmp_path, [('600000.SH', 'stock', date(2026, 11, 13))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 codes=None, range_label='2026-11-13~2026-11-16')
    args = _build_args()
    monkeypatch.setattr(du, '_run_build', lambda **kwargs: 0)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2026, 11, 16))

    ok, detail = du._step_build(plan, args)
    assert ok is False
    assert '2026-11-13' in detail and '2026-11-16' in detail

    outcomes = du._run_steps([
        ('build', '本地行情构建 + 末日断言', True, lambda: du._step_build(plan, args)),
    ])
    assert du._print_run_summary(plan, outcomes, {}, 0.0) == 1


def test_daily_index_end_match_passes_build_step(tmp_path, monkeypatch):
    _write_daily_index(tmp_path, [('600000.SH', 'stock', date(2026, 11, 16))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 codes=None, range_label='2026-11-16~2026-11-16')
    monkeypatch.setattr(du, '_run_build', lambda **kwargs: 0)
    monkeypatch.setattr(du, '_recent_completed_trade_day', lambda root: date(2026, 11, 16))

    ok, detail = du._step_build(plan, _build_args())
    assert ok is True
    assert '=' in detail


def test_fund_quote_step_skips_when_up_to_date(tmp_path, monkeypatch):
    _write_daily_index(tmp_path, [('510300.SH', 'fund', date(2026, 11, 16))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16))

    def _boom(*args, **kwargs):
        raise AssertionError('末日已最新时不应重算')

    monkeypatch.setattr(du, '_run_module', _boom)
    ok, detail = du._step_fund_quotes(plan, _build_args())
    assert ok is True
    assert '跳过' in detail


def test_fund_quote_step_fails_when_source_still_behind(tmp_path, monkeypatch):
    """源端仍停在 11-13 时不能绿：跑完仍是旧末日即判失败并登记白名单。"""
    _write_daily_index(tmp_path, [('510300.SH', 'fund', date(2026, 11, 13))])
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 store=du.LocalQuoteStore(tmp_path),
                                 expected=set(), plan_quote=set())
    monkeypatch.setattr(du, '_run_module', lambda cmd, label: 0)

    ok, detail = du._step_fund_quotes(plan, _build_args())
    assert ok is False
    assert '11-13' in detail
    assert any(rel.startswith('quotes/fund/') for rel in plan.expected)


def test_factor_step_skips_when_aligned(tmp_path):
    _write_daily_index(tmp_path, [
        ('600000.SH', 'stock', date(2026, 11, 16)),
        ('510300.SH', 'fund', date(2026, 11, 16)),
    ])
    _write_factor(tmp_path, 'stock_adj_factor.parquet', ['600000.SH'], date(2026, 11, 16))
    _write_factor(tmp_path, 'fund_adj_factor.parquet', ['510300.SH'], date(2026, 11, 16))
    plan = types.SimpleNamespace(root=tmp_path, latest=date(2026, 11, 16),
                                 store=None, factor_store=None)

    ok, detail = du._step_factors(plan, _build_args())
    assert ok is True
    assert '跳过' in detail


def test_step_plan_keeps_hard_order_and_new_steps():
    keys = [key for key, _, _ in du.STEP_PLAN]
    assert len(keys) == 17
    assert keys[-2:] == ['backup', 'gate']
    assert keys.index('build') < keys.index('fund_quote') < keys.index('factor')
    assert keys.index('factor') < keys.index('clean_set')
    assert all(critical for key, _, critical in du.STEP_PLAN
               if key in {'fund_quote', 'factor'})
