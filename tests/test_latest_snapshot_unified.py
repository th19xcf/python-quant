#!/usr/bin/env python3

"""CLI / GUI 双写 ``latest.json`` 的统一 schema 与「可下单」口径回归。

覆盖代码审查 3.2（同一天两套 schema）、3.3（延后票被显示成可下单）、
3.1（GUI 默认档位与 CLI 不一致）与 3.4（收盘前生成没有闸门）。
"""

from __future__ import annotations

import importlib
import json
from datetime import date, datetime

from src.advice import cli
from src.advice.config import default_output_tiers
from src.advice.latest_snapshot import (
    ITEM_KEYS,
    LATEST_KEYS,
    build_latest_payload,
    load_latest_snapshot,
    normalize_latest_payload,
)
from src.advice.models import (
    OUTPUT_COLUMNS,
    AdviceResult,
    Recommendation,
    TierAdvice,
)
from src.core.usecases.get_daily_recommendations import (
    DailyRecommendationBundle,
    _to_dto,
    get_daily_recommendations,
)
from src.ui.advice.table_model import COL_DIRECTION, display_value
from src.ui.advice.widgets import DetailPanel

#: 模块对象（``src.core.usecases`` 包导出的是同名函数，不能直接当模块用）
usecase = importlib.import_module('src.core.usecases.get_daily_recommendations')

TRADE_DATE = date(2026, 11, 3)
EXEC_DATE = date(2026, 11, 4)
EQUITY = 1_000_000.0


def _rec(code: str, direction: str, percentile: float, weight: float, *,
         disposition: str = 'active', defer_reason: str = '',
         defer_until: date | None = None) -> Recommendation:
    return Recommendation(
        trade_date=TRADE_DATE, ts_code=code, name=f'标的{code[:3]}',
        direction=direction, target_weight=weight, shares=10_000,
        ref_price=10.0, horizon='中线', validity_days=20, confidence=0.8,
        top_factors=[('momentum_1m', 0.4), ('volume_momentum_20', 0.2)],
        risk_notes=['测试数据'], industry='沪主板',
        industry_source='code_prefix_board', score=1.2,
        score_percentile=percentile, disposition=disposition,
        defer_reason=defer_reason, defer_until=defer_until)


def _tier(key: str, label: str,
          rows: list[Recommendation]) -> TierAdvice:
    return TierAdvice(tier=key, label=label, trade_date=TRADE_DATE,
                      rows=rows, equity=EQUITY, invested=EQUITY * 0.5,
                      cash=EQUITY * 0.5, candidates=10, turnover=0.084,
                      notes=['档位说明'])


def _sample_result() -> AdviceResult:
    """同一天同一份输入：两条可下单 + 一条延后未成交。"""
    return AdviceResult(
        trade_date=TRADE_DATE, equity=EQUITY,
        tiers={
            'mid': _tier('mid', '中线', [
                _rec('600000.SH', 'BUY', 0.93, 0.06),
                _rec('600519.SH', 'BUY', 0.88, 0.05,
                     disposition='deferred', defer_reason='单次换手上限',
                     defer_until=date(2026, 11, 5)),
            ]),
            'long': _tier('long', '长线', [
                _rec('000001.SZ', 'SELL', 0.81, 0.04),
            ]),
        },
        signal_date=TRADE_DATE, earliest_exec_date=EXEC_DATE,
        exec_status='pending', exec_date_basis='calendar')


def _flat(obj, prefix: str = '') -> dict[str, object]:
    """把嵌套 payload 压成「路径 → 叶子值」，便于逐字段比对。"""
    flat: dict[str, object] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            flat.update(_flat(value, f'{prefix}.{key}' if prefix else str(key)))
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            flat.update(_flat(value, f'{prefix}[{index}]'))
    else:
        flat[prefix] = obj
    return flat


def _compare(report: list[str], left: dict, right: dict,
             label: str) -> tuple[int, int, int]:
    flat_left, flat_right = _flat(left), _flat(right)
    keys = sorted(set(flat_left) | set(flat_right))
    diffs = [key for key in keys
             if flat_left.get(key, '<缺失>') != flat_right.get(key, '<缺失>')]
    report.append(f'[{label}] 字段数={len(keys)} 一致={len(keys) - len(diffs)} '
                  f'差异={len(diffs)}')
    for key in diffs:
        report.append(f'  差异 {key}: CLI={flat_left.get(key, "<缺失>")!r} '
                      f'GUI={flat_right.get(key, "<缺失>")!r}')
    return len(keys), len(keys) - len(diffs), len(diffs)


# ------------------------------------------------- schema 字段集完整性
def test_item_keys_cover_every_export_column():
    """``ITEM_KEYS`` 必须覆盖推荐导出的每一列。

    ``limit_up_risk`` / ``limit_up_basis`` 由 ``Recommendation.to_row()`` 产出、
    已进 ``OUTPUT_COLUMNS``，经 ``build_latest_payload`` 原样写进 ``items[]``；
    旧集合没跟上时快照 schema 就少两个输出项，读侧按 ``ITEM_KEYS`` 判会
    把真实字段当成未知键。
    """
    missing = sorted(set(OUTPUT_COLUMNS) - set(ITEM_KEYS))
    assert not missing, f'ITEM_KEYS 缺导出列：{missing}'


# ------------------------------------------------- CLI 与 GUI 逐字段比对
def test_cli_and_gui_latest_payload_field_by_field(tmp_path, monkeypatch):
    result = _sample_result()
    monkeypatch.setattr(usecase.AdviceEngine, 'generate',
                        lambda self, **kwargs: result)

    gui_dir = tmp_path / 'gui'
    get_daily_recommendations(
        trade_date=TRADE_DATE, horizon='all', refresh=True, out_dir=gui_dir,
        state_dir=tmp_path / 'state', equity=EQUITY)
    gui_payload = json.loads(
        (gui_dir / 'latest.json').read_text(encoding='utf-8'))

    # CLI 路径用同一份 AdviceResult、同一个 generated_at，保证可逐字段比对
    stamp = datetime.fromisoformat(gui_payload['generated_at'])
    cli_dir = tmp_path / 'cli'
    cli._write_snapshot(result, cli_dir, EQUITY, generated_at=stamp)
    cli_payload = json.loads(
        (cli_dir / 'latest.json').read_text(encoding='utf-8'))

    report: list[str] = []
    total, same, diff = _compare(report, cli_payload, gui_payload, '顶层')
    for index, (left, right) in enumerate(
            zip(cli_payload['items'], gui_payload['items'], strict=True)):
        t, s, d = _compare(report, left, right, f'items[{index}]')
        total, same, diff = total + t, same + s, diff + d

    assert set(cli_payload) == set(LATEST_KEYS)
    assert set(gui_payload) == set(LATEST_KEYS)
    assert all(set(item) == set(ITEM_KEYS) for item in gui_payload['items'])
    assert diff == 0, '\n'.join(report)


# ------------------------------------------------- GUI 覆盖不丢 CLI 字段
def test_gui_overwrite_keeps_exec_status_and_usage_boundary(
        tmp_path, monkeypatch):
    result = _sample_result()
    monkeypatch.setattr(usecase.AdviceEngine, 'generate',
                        lambda self, **kwargs: result)
    out_dir = tmp_path / 'advice'
    out_dir.mkdir()
    legacy_cli = {
        'usage_boundary': {'confidence': 'CLI 旧声明'},
        'signal_date': TRADE_DATE.isoformat(),
        'earliest_exec_date': EXEC_DATE.isoformat(),
        'exec_status': 'expired',
        'trade_date': TRADE_DATE.isoformat(),
        'generated_at': '2026-11-03T16:30:00',
        'equity': EQUITY,
        'is_sample': False,
        'tiers': {'mid': {'turnover_pct': 8.4}},
        'items': [],
    }
    (out_dir / 'latest.json').write_text(
        json.dumps(legacy_cli, ensure_ascii=False), encoding='utf-8')

    get_daily_recommendations(
        trade_date=TRADE_DATE, horizon='all', refresh=True, out_dir=out_dir,
        state_dir=tmp_path / 'state', equity=EQUITY)

    payload = json.loads(
        (out_dir / 'latest.json').read_text(encoding='utf-8'))
    assert 'exec_status' in payload
    assert payload['exec_status'] in ('pending', 'expired')
    assert 'usage_boundary' in payload and payload['usage_boundary']
    assert 'earliest_exec_date' in payload and payload['earliest_exec_date']
    assert payload['schema_version'] == 2
    assert payload['items']
    assert all(item['disposition'] in ('active', 'deferred')
               for item in payload['items'])


# ------------------------------------------------- 旧格式降级读出
def test_legacy_snapshot_degrades_instead_of_raising(tmp_path):
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps({
        'trade_date': TRADE_DATE.isoformat(),
        'generated_at': '2026-11-03T16:30:00',
        'equity': EQUITY,
        'is_sample': False,
        'tiers': {'mid': {'turnover_pct': 8.4}},
        'items': [{'ts_code': '600000.SH', 'name': '浦发银行',
                   'direction': 'BUY', 'target_weight_pct': 5.0,
                   'ref_price': 10.0, 'confidence': 0.8},
                  {'ts_code': '600519.SH', 'name': '贵州茅台',
                   'direction': 'BUY', 'disposition': 'deferred',
                   'target_weight_pct': 3.0, 'ref_price': 20.0}],
    }, ensure_ascii=False), encoding='utf-8')

    payload = load_latest_snapshot(path)
    assert payload is not None
    # signal=11-03 → earliest=11-04；生成时间 11-03 16:30 早于 exec，判 pending
    assert payload['exec_status'] == 'pending'
    assert payload['usage_boundary']
    assert payload['items'][0]['disposition'] == 'active'
    assert payload['items'][0]['actionable'] is True
    assert payload['items'][1]['disposition'] == 'deferred'
    assert payload['items'][1]['actionable'] is False
    assert payload['load_warnings']

    bundle = get_daily_recommendations(
        horizon='all', out_dir=tmp_path, refresh=False)
    assert bundle.source == 'snapshot'
    assert [item.ts_code for item in bundle.items] == ['600000.SH', '600519.SH']
    assert bundle.items[0].actionable is True
    assert bundle.items[1].actionable is False
    assert any('补齐' in warning or '缺' in warning
               for warning in bundle.warnings)


def test_unparseable_signal_warns_instead_of_silent_pass():
    """signal 解析不了时必须留 warning；改前是裸 ``except: pass``，本用例会红。"""
    from loguru import logger

    messages: list[str] = []
    handler_id = logger.add(lambda m: messages.append(m.record['message']), level='WARNING')
    try:
        payload = normalize_latest_payload({'signal_date': '2026-13-99', 'items': []})
    finally:
        logger.remove(handler_id)

    assert payload['earliest_exec_date'] is None
    assert any('无法解析为日期' in m for m in messages), messages


# ------------------------------------------------- 延后票不得显示为可下单
def test_deferred_item_not_actionable_in_snapshot_dto_table_detail():
    dto = _to_dto({
        'ts_code': '600519.SH', 'name': '贵州茅台', 'direction': 'BUY',
        'disposition': 'deferred', 'defer_reason': '单次换手上限',
        'defer_until': '2026-11-05', 'exec_status': 'pending',
        'target_weight_pct': 3.0, 'shares': 100, 'ref_price': 20.0,
        'confidence': 0.8, 'score': 1.2, 'score_percentile': 0.9,
        'horizon': '中线', 'validity_days': 20,
    }, 'mid')
    assert dto.disposition == 'deferred'
    assert dto.actionable is False
    assert '延后' in dto.action
    assert display_value(dto, COL_DIRECTION) == 'BUY(延后)'
    assert '不可下单' in DetailPanel._state_text(dto)

    payload = build_latest_payload(_sample_result(),
                                   generated_at=datetime(2026, 11, 3, 16, 30))
    deferred = [item for item in payload['items']
                if item['disposition'] == 'deferred']
    assert len(deferred) == 1
    assert deferred[0]['actionable'] is False
    assert deferred[0]['action'] == '买入(延后)'
    active = [item for item in payload['items']
              if item['direction'] == 'BUY' and item['disposition'] == 'active']
    assert active and all(item['actionable'] for item in active)


def test_page_kpi_counts_only_actionable(qapp):
    from src.ui.advice.page import AdvicePage

    rows = [
        {'ts_code': '600000.SH', 'name': '浦发银行', 'direction': 'BUY',
         'disposition': 'active'},
        {'ts_code': '000001.SZ', 'name': '平安银行', 'direction': 'SELL',
         'disposition': 'active'},
        {'ts_code': '600519.SH', 'name': '贵州茅台', 'direction': 'BUY',
         'disposition': 'deferred', 'defer_reason': '单次换手上限'},
        {'ts_code': '510300.SH', 'name': '沪深300ETF', 'direction': 'HOLD',
         'disposition': 'active'},
    ]
    items = [_to_dto({**row, 'target_weight_pct': 1.0, 'shares': 100,
                      'ref_price': 10.0, 'confidence': 0.5,
                      'score': 1.0, 'score_percentile': 0.5,
                      'exec_status': 'pending'}, 'mid') for row in rows]
    page = AdvicePage(host=None)
    page.set_bundle(DailyRecommendationBundle(
        items=items, trade_date=TRADE_DATE,
        generated_at=datetime(2026, 11, 3, 16, 30), data_lag_days=0,
        equity=EQUITY, summary={'mid': {}}))
    assert page._kpi['count'].value_label.text() == '2'
    assert '可下单 2' in page.result_label.text()
    assert '延后 1' in page.result_label.text()


# ------------------------------------------------- 档位与收盘闸门口径
def test_usecase_default_tiers_align_with_cli(tmp_path, monkeypatch):
    captured: dict = {}

    def _fake_generate(self, **kwargs):
        captured.update(kwargs)
        return _sample_result()

    monkeypatch.setattr(usecase.AdviceEngine, 'generate', _fake_generate)
    get_daily_recommendations(
        trade_date=TRADE_DATE, horizon='all', refresh=True,
        out_dir=tmp_path / 'out', state_dir=tmp_path / 'state', equity=EQUITY)
    assert captured['tiers'] == default_output_tiers()
    assert 'short' not in captured['tiers']


def test_intraday_generation_warns_and_skips_daily_archive(
        tmp_path, monkeypatch):
    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 3, 10, 0, 0)

    captured: dict = {}

    def _fake_generate(self, **kwargs):
        captured.update(kwargs)
        return _sample_result()

    monkeypatch.setattr(usecase, 'datetime', _FrozenDatetime)
    monkeypatch.setattr(usecase.AdviceEngine, 'generate', _fake_generate)
    out_dir = tmp_path / 'out'
    bundle = get_daily_recommendations(
        trade_date=None, horizon='all', refresh=True, out_dir=out_dir,
        state_dir=tmp_path / 'state', equity=EQUITY)

    assert bundle.intraday is True
    assert captured['persist_daily'] is False
    assert any('未到收盘后' in warning for warning in bundle.warnings)
    daily_dir = out_dir / 'daily'
    assert not daily_dir.is_dir() or not list(daily_dir.iterdir())
