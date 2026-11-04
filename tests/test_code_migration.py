"""证券代码迁移映射的离线单测（不连数据库、不写业务数据）。"""

from datetime import date, datetime

import polars as pl

from src.data.quality.code_migration import (
    Sources,
    build_mapping,
    detect_cycles,
    is_a_share,
    load_mapping,
    mapping_path,
    resolve_ts_code,
    validate_mapping,
)


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
