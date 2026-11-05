"""证券代码迁移映射的离线单测（不连数据库、不写业务数据）。"""

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from src.data.quality import code_migration
from src.data.quality.code_migration import (
    EntityIndex,
    Sources,
    build_mapping,
    detect_cycles,
    entity_key,
    is_a_share,
    load_entity_index,
    load_mapping,
    mapping_path,
    resolve_ts_code,
    validate_mapping,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_STORE = PROJECT_ROOT / 'data' / 'local_store'
HAS_REAL_STORE = (REAL_STORE / 'meta' / 'code_migration.parquet').is_file()


def _write_pairs(tmp_path, pairs: list[tuple[str, str]]) -> None:
    path = mapping_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'old_ts_code': [old for old, _ in pairs],
        'new_ts_code': [new for _, new in pairs],
    }).write_parquet(path)


def _mapping(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    """构造最小映射帧（old, new, confidence）。"""
    return pl.DataFrame({
        'old_ts_code': [row[0] for row in rows],
        'new_ts_code': [row[1] for row in rows],
        'confidence': [row[2] for row in rows],
        'verified_at': [datetime(2026, 11, 5) for _ in rows],
    })


def test_a_share_filter_excludes_b_share_and_ipo_codes():
    assert is_a_share('830001.BJ')
    assert is_a_share('430001.BJ')
    assert is_a_share('920001.BJ')
    assert is_a_share('600000.SH')
    assert not is_a_share('201872.SZ')  # 深 B
    assert not is_a_share('900945.SH')  # 沪 B
    assert not is_a_share('A23143.SZ')  # IPO 受理伪码
    assert not is_a_share('830001.NQ')  # 新三板


def test_resolve_follows_chain_and_returns_latest(tmp_path):
    _write_pairs(tmp_path, [('832023.BJ', '920023.BJ'),
                            ('920023.BJ', '920023.SZ')])
    assert resolve_ts_code('832023.BJ', tmp_path) == '920023.SZ'
    assert resolve_ts_code('920023.BJ', tmp_path) == '920023.SZ'
    # 已是新代码或大小写/空白差异都归一化后原样返回
    assert resolve_ts_code('920023.SZ', tmp_path) == '920023.SZ'
    assert resolve_ts_code(' 832023.bj ', tmp_path) == '920023.SZ'


def test_resolve_returns_input_when_unmapped_or_missing(tmp_path):
    assert resolve_ts_code('600000.SH', tmp_path) == '600000.SH'
    _write_pairs(tmp_path, [('839680.BJ', '920680.BJ')])
    assert resolve_ts_code('600000.SH', tmp_path) == '600000.SH'
    assert load_mapping(tmp_path).height == 1


def test_resolve_is_cycle_safe(tmp_path):
    _write_pairs(tmp_path, [('A00001.SZ', 'A00002.SZ'),
                            ('A00002.SZ', 'A00001.SZ')])
    result = resolve_ts_code('A00001.SZ', tmp_path)
    assert result in {'A00001.SZ', 'A00002.SZ'}


def test_detect_cycles_and_validate_mapping():
    assert detect_cycles([('A', 'B'), ('B', 'C')]) == []
    cycles = detect_cycles([('A', 'B'), ('B', 'C'), ('C', 'A')])
    assert cycles == [['A', 'B', 'C', 'A']]

    ok = validate_mapping(_mapping([('600001.SH', '601001.SH', 'high')]))
    assert ok['ok'] is True and ok['two_targets'] == []

    bad = _mapping([
        ('600001.SH', '601001.SH', 'high'),
        ('600001.SH', '601002.SH', 'low'),
        ('600002.SH', '600002.SH', 'low'),
    ])
    report = validate_mapping(bad)
    assert report['two_targets'] == ['600001.SH']
    assert report['self_loops'] == ['600002.SH']
    assert report['ok'] is False


def _sources(*, em: list[dict], notices: list[dict] | None = None,
             stock_basic: list[dict] | None = None) -> Sources:
    return Sources(
        stock_basic=pl.DataFrame(stock_basic or [], schema={
            'ts_code': pl.Utf8, 'name': pl.Utf8, 'list_date': pl.Date,
            'delist_date': pl.Date, 'status': pl.Utf8}, strict=False),
        daily_index=pl.DataFrame(schema={
            'ts_code': pl.Utf8, 'start': pl.Date, 'end': pl.Date}),
        names={},
        em=pl.DataFrame(em, schema={
            'secucode': pl.Utf8, 'name': pl.Utf8, 'former_names': pl.Utf8,
            'org_name': pl.Utf8, 'listing_date': pl.Date}, strict=False),
        notices=pl.DataFrame(notices or [], schema={
            'sec_code': pl.Utf8, 'sec_name': pl.Utf8,
            'publish_date': pl.Date}, strict=False),
    )


def test_build_mapping_same_org_pair_needs_two_criteria():
    sources = _sources(
        em=[
            {'secucode': '600001.SH', 'name': '旧甲', 'former_names': '',
             'org_name': '甲公司', 'listing_date': date(1998, 1, 1)},
            {'secucode': '601001.SH', 'name': '新甲', 'former_names': '旧甲',
             'org_name': '甲公司', 'listing_date': date(2010, 1, 1)},
        ],
        stock_basic=[
            {'ts_code': '600001.SH', 'name': '旧甲', 'list_date': date(1998, 1, 1),
             'delist_date': date(2009, 12, 31), 'status': 'D'},
            {'ts_code': '601001.SH', 'name': '新甲', 'list_date': date(2010, 1, 1),
             'delist_date': None, 'status': 'L'},
        ],
    )
    mapping, unresolved = build_mapping(sources)
    assert mapping.height == 1 and unresolved.height == 0
    row = mapping.to_dicts()[0]
    assert row['old_ts_code'] == '600001.SH'
    assert row['new_ts_code'] == '601001.SH'
    assert row['confidence'] == 'high'
    assert '满足依据' in row['evidence']


def test_build_mapping_old_bse_code_matches_former_name():
    sources = _sources(
        em=[{'secucode': '920001.BJ', 'name': '乙股份', 'former_names': '乙股份',
             'org_name': '乙公司', 'listing_date': date(2021, 8, 26)}],
        notices=[{'sec_code': '830001', 'sec_name': '乙股份',
                  'publish_date': date(2025, 6, 13)}],
    )
    mapping, unresolved = build_mapping(sources)
    assert mapping.height == 1 and unresolved.height == 0
    row = mapping.to_dicts()[0]
    assert (row['old_ts_code'], row['new_ts_code']) == ('830001.BJ', '920001.BJ')
    assert row['source'] == 'cninfo_old_bse_code'
    # 旧代码无本地行情、无上市日期 → 仅名称一条硬证据，按规则保留为 low
    assert row['confidence'] == 'low'


def test_build_mapping_reports_ambiguous_and_missing_names():
    sources = _sources(
        em=[
            {'secucode': '920001.BJ', 'name': '乙股份', 'former_names': '乙股份',
             'org_name': '乙公司', 'listing_date': date(2021, 1, 1)},
            {'secucode': '920002.BJ', 'name': '丙股份', 'former_names': '乙股份',
             'org_name': '丙公司', 'listing_date': date(2022, 1, 1)},
        ],
        notices=[
            {'sec_code': '830001', 'sec_name': '乙股份',
             'publish_date': date(2025, 1, 1)},
            {'sec_code': '839999', 'sec_name': '无名股份',
             'publish_date': date(2025, 1, 1)},
        ],
    )
    _, unresolved = build_mapping(sources)
    reasons = sorted(unresolved['reason'].to_list())
    assert reasons == ['ambiguous_name_match', 'no_name_match']


def test_build_mapping_is_idempotent_on_verified_at():
    sources = _sources(
        em=[
            {'secucode': '600001.SH', 'name': '旧甲', 'former_names': '',
             'org_name': '甲公司', 'listing_date': date(1998, 1, 1)},
            {'secucode': '601001.SH', 'name': '新甲', 'former_names': '旧甲',
             'org_name': '甲公司', 'listing_date': date(2010, 1, 1)},
        ],
        stock_basic=[
            {'ts_code': '600001.SH', 'name': '旧甲', 'list_date': date(1998, 1, 1),
             'delist_date': date(2009, 12, 31), 'status': 'D'},
            {'ts_code': '601001.SH', 'name': '新甲', 'list_date': date(2010, 1, 1),
             'delist_date': None, 'status': 'L'},
        ],
    )
    first, _ = build_mapping(sources)
    second, _ = build_mapping(sources, previous=first)
    assert first.equals(second)
    assert first['verified_at'].to_list() == second['verified_at'].to_list()


# ----------------------------------------------------------------------
# 读侧实体索引（旧码 → 新码收敛）
# ----------------------------------------------------------------------
def _write_store(root: Path, pairs: list[tuple[str, str]], spans: dict,
                 quotes: dict[str, list[tuple[date, float]]] | None = None) -> None:
    """构造最小本地列存：映射表 + 日线索引 + 新旧码行情文件。"""
    _write_pairs(root, pairs)
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    rows = [{'ts_code': code, 'data_type': 'stock', 'start': span[0], 'end': span[1]}
            for code, span in spans.items()]
    pl.DataFrame(rows, schema={'ts_code': pl.Utf8, 'data_type': pl.Utf8,
                               'start': pl.Date, 'end': pl.Date},
                 strict=False).write_parquet(root / 'meta' / 'daily_index.parquet')
    for code, series in (quotes or {}).items():
        market = code.rsplit('.', 1)[-1].lower()
        path = root / 'quotes' / 'stock' / market / f'{code}.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(series, schema={'date': pl.Date, 'close': pl.Float64},
                     orient='row').write_parquet(path)


def test_entity_index_cached_and_idempotent(tmp_path):
    _write_store(
        tmp_path,
        [('600001.SH', '601001.SH')],
        {'600001.SH': (date(2010, 1, 4), date(2015, 12, 31)),
         '601001.SH': (date(2010, 1, 4), date(2026, 11, 3))},
        {'600001.SH': [(date(2010, 1, 4), 10.0), (date(2010, 1, 5), 10.5)],
         '601001.SH': [(date(2010, 1, 4), 10.0), (date(2010, 1, 5), 10.5),
                       (date(2020, 1, 2), 30.0)]},
    )
    first = load_entity_index(tmp_path)
    second = load_entity_index(tmp_path)
    # 同一 store 且文件未变 → 命中缓存，返回同一对象
    assert first is second
    assert first.superseded == frozenset({'600001.SH'})
    assert first.entity_key('600001.SH') == '601001.SH'
    assert first.dedupe(['600001.SH', '601001.SH']) == ['601001.SH']


def test_entity_index_keeps_old_code_when_new_has_no_quotes(tmp_path):
    _write_store(
        tmp_path,
        [('300114.SZ', '302132.SZ')],
        {'300114.SZ': (date(2010, 8, 27), date(2019, 9, 17))},
        {'300114.SZ': [(date(2010, 8, 27), 20.0)]},
    )
    index = load_entity_index(tmp_path)
    assert '300114.SZ' not in index.superseded
    assert index.entity_key('300114.SZ') == '302132.SZ'
    assert index.dedupe(['300114.SZ']) == ['300114.SZ']


def test_entity_key_is_idempotent_on_new_and_unmapped_codes(tmp_path):
    _write_store(
        tmp_path,
        [('600001.SH', '601001.SH')],
        {'600001.SH': (date(2010, 1, 4), date(2015, 12, 31)),
         '601001.SH': (date(2010, 1, 4), date(2026, 11, 3))},
        {'600001.SH': [(date(2010, 1, 4), 10.0)],
         '601001.SH': [(date(2010, 1, 4), 10.0)]},
    )
    assert entity_key('601001.SH', tmp_path) == '601001.SH'
    assert entity_key('600000.SH', tmp_path) == '600000.SH'
    assert entity_key('601001.sh', tmp_path) == '601001.SH'


def test_entity_index_rejects_merge_when_prices_differ(tmp_path, monkeypatch):
    warnings: list[str] = []

    class _FakeLogger:
        @staticmethod
        def warning(message: str) -> None:
            warnings.append(message)

    monkeypatch.setattr(code_migration, 'logger', _FakeLogger)
    _write_store(
        tmp_path,
        [('600001.SH', '601001.SH')],
        {'600001.SH': (date(2010, 1, 4), date(2015, 12, 31)),
         '601001.SH': (date(2010, 1, 4), date(2026, 11, 3))},
        {'600001.SH': [(date(2010, 1, 4), 10.0), (date(2010, 1, 5), 10.5)],
         '601001.SH': [(date(2010, 1, 4), 11.0), (date(2010, 1, 5), 12.0)]},
    )
    index = load_entity_index(tmp_path)
    assert index.superseded == frozenset()
    assert index.dedupe(['600001.SH', '601001.SH']) == ['600001.SH', '601001.SH']
    assert any('安全闸' in message for message in warnings)


def test_entity_index_degrades_without_mapping(tmp_path):
    (tmp_path / 'meta').mkdir(parents=True, exist_ok=True)
    index = load_entity_index(tmp_path)
    assert index == EntityIndex(canonical={}, superseded=frozenset())
    assert index.dedupe(['600000.SH']) == ['600000.SH']


@pytest.mark.skipif(not HAS_REAL_STORE, reason='本地列存未就绪')
def test_real_store_supersedes_fully_covered_old_codes():
    index = load_entity_index(REAL_STORE)
    assert '000043.SZ' in index.superseded
    assert index.entity_key('000043.SZ') == '001914.SZ'
    # 2026-11-05 回补后 302132.SZ 已覆盖 300114.SZ 全历史（2010-08-27 起），
    # 旧码同样应被收敛；「新码无行情时保留旧码」由
    # test_entity_index_keeps_old_code_when_new_has_no_quotes 单独覆盖。
    assert '300114.SZ' in index.superseded
    assert index.dedupe(['000043.SZ', '001914.SZ', '300114.SZ']) == ['001914.SZ']
