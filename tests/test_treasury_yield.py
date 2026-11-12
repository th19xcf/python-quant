#!/usr/bin/env python3

"""``tools/treasury_yield.py`` 的分片刷新语义与分段组装口径（DG-31）。

锁定的行为：
1. 中债分片缺失 → 抓取；
2. 分片已覆盖全部应有交易日 → 跳过（不发请求）；
3. 分片存在但缺应有交易日 → 重取刷新；
4. 「应有交易日」= ``000001.SH`` 交易日 ∩ ``>= SWITCH``（2006-01/02 归东财，不计入中债分片）；
5. 三段组装：``< SWITCH`` 全东财、``>= SWITCH`` 中债优先、中债缺日由东财补。

旧 ``_staging/state_inputs_20261110/collect.py`` 的 ``if target.exists(): continue`` /
``return`` 让第 3、4 条永远不成立。最后一条用例用旧规则跑同一份 fixture，并对
「刷新后应覆盖末个交易日」的同一断言取反，证明该断言在旧行为下**会失败**
（DG-31：断言必须能失败）。
"""
import json
from datetime import date

import polars as pl
import pytest

import tools.treasury_yield as T

D1 = date(2026, 11, 10)
D2 = date(2026, 11, 11)
SW = T.SWITCH


def _frame(days, value=1.7):
    return pl.DataFrame({'date': list(days), 'y10': [value] * len(days)})


def _cb(parts, year, days, value=1.7):
    _frame(days, value).write_parquet(parts / f'treasury_chinabond_{year}.parquet')


# ------------------------------------------------------------ 缺口判定
def test_missing_shard_is_fetched(tmp_path):
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    T.collect_chinabond(tmp_path, {2026: [D1, D2]}, fetch, sleep=0)

    assert calls == [2026]
    assert pl.read_parquet(tmp_path / 'treasury_chinabond_2026.parquet')['date'].max() == D2


def test_covered_shard_is_skipped(tmp_path):
    _cb(tmp_path, 2026, [D1, D2])
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    status = T.collect_chinabond(tmp_path, {2026: [D1, D2]}, fetch, sleep=0)

    assert status == {2026: 'ok'}
    assert calls == [], '已覆盖应有交易日的分片不应发请求'


def test_stale_shard_is_refreshed(tmp_path):
    """旧行为下这条失败：分片存在即跳过，11-11 永远不会补进来。"""
    _cb(tmp_path, 2026, [D1])
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    status = T.collect_chinabond(tmp_path, {2026: [D1, D2]}, fetch, sleep=0)

    assert status == {2026: 'gap'}
    assert calls == [2026]
    assert pl.read_parquet(tmp_path / 'treasury_chinabond_2026.parquet')['date'].max() == D2


def test_shard_state_triplet(tmp_path):
    part = tmp_path / 'treasury_chinabond_2026.parquet'
    assert T.shard_state(part, [D1, D2]) == 'missing'
    _frame([D1]).write_parquet(part)
    assert T.shard_state(part, [D1, D2]) == 'gap'
    _frame([D1, D2]).write_parquet(part)
    assert T.shard_state(part, [D1, D2]) == 'ok'


def test_old_exists_skip_rule_fails_refresh_assertion(tmp_path):
    """DG-31 对照组：旧规则下「刷新后覆盖末个交易日」必然失败。"""
    part = tmp_path / 'treasury_chinabond_2026.parquet'
    _frame([D1]).write_parquet(part)
    fetched = []

    def old_collect(year):
        if part.exists():      # 旧 collect.py L47-48 / L81-82：断点即跳过
            return
        fetched.append(year)
        _frame([D1, D2]).write_parquet(part)

    old_collect(2026)
    assert fetched == [], '旧规则不会重取已存在的分片'
    with pytest.raises(AssertionError):
        assert pl.read_parquet(part)['date'].max() == D2


def test_refresh_current_forces_current_year_refetch(tmp_path):
    _cb(tmp_path, 2026, [D1, D2])
    _cb(tmp_path, 2025, [date(2025, 12, 31)])
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    T.collect_chinabond(tmp_path, {2025: [date(2025, 12, 31)], 2026: [D1, D2]},
                        fetch, refresh_current=True, sleep=0)

    assert calls == [2026], '只有最新年份被强制刷新，2025 仍跳过'


def test_cb_expected_starts_at_switch():
    days = [date(2005, 6, 1), date(2006, 1, 4), date(2006, 2, 28),
            date(2006, 3, 1), date(2006, 12, 29), date(2027, 1, 4)]
    exp = T.cb_expected(days)

    assert set(exp) == {2006}, '2005 与 2027 不在中债抓取年份内'
    assert exp[2006] == [date(2006, 3, 1), date(2006, 12, 29)], (
        '2006-01/02 的交易日本就归东财承担，不得计入中债分片应有集合')


def test_assemble_three_segments(tmp_path):
    pre_date = date(2005, 6, 1)          # < SWITCH，只能来自东财
    overlap = date(2024, 1, 3)           # 两源同日，中债优先
    em_only = date(2024, 1, 4)           # >= SWITCH 但中债缺，东财补
    cb_only = date(2024, 1, 5)           # 仅中债有
    _cb(tmp_path, 2024, [overlap, cb_only], value=4.0)
    pl.DataFrame({'date': [pre_date, overlap, em_only],
                  'y10': [1.1, 9.9, 1.2]}).write_parquet(
        tmp_path / 'treasury_eastmoney.parquet')

    out, meta = T.assemble(tmp_path)

    assert meta['segments'] == {'pre_eastmoney': 1, 'post_chinabond': 2,
                                'post_eastmoney_fill': 1}
    assert out.height == 4
    assert out['date'].n_unique() == out.height
    got = {r['date']: (r['y10'], r['source']) for r in out.iter_rows(named=True)}
    assert got[pre_date] == (1.1, T.EM_SOURCE)
    assert got[overlap] == (4.0, T.CB_SOURCE), '>= SWITCH 同日以中债为准，不得取东财 9.9'
    assert got[cb_only] == (4.0, T.CB_SOURCE)
    assert got[em_only] == (1.2, T.EM_SOURCE), '中债缺日由东财补，source 标东财'
    assert meta['switch'] == str(SW)


def test_assemble_missing_parts_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match='中债分片缺失'):
        T.assemble(tmp_path)
    _cb(tmp_path, 2024, [D1])
    with pytest.raises(FileNotFoundError, match='东财全量缺失'):
        T.assemble(tmp_path)


# ------------------------------------------------------------ 发布
def _manifest():
    return {
        'as_of': 'x', 'generated_at': 'x', 'produced_by': 'x',
        'files': {'cn_treasury_yield_10y.parquet': {'rows': 0, 'sha256': '', 'bytes': 0}},
        'tables': {'cn_treasury_yield_10y': {
            'source_segments': [{'rows': 0, 'range': ''}],
            'coverage_check': '', 'known_limits': []}},
    }


def test_publish_appends_only_new_dates(tmp_path):
    old = pl.DataFrame({'date': [date(2024, 1, 2), date(2024, 1, 3)],
                       'y10': [2.0, 2.1], 'source': [T.CB_SOURCE] * 2})
    cand = pl.concat([old, pl.DataFrame({'date': [date(2024, 1, 4)], 'y10': [2.2],
                                        'source': [T.CB_SOURCE]})])
    target = tmp_path / 't.parquet'
    target_csv = tmp_path / 't.csv'
    manifest_path = tmp_path / 'manifest.json'
    old.write_parquet(target)
    old.write_csv(target_csv)
    manifest_path.write_text(json.dumps(_manifest()), encoding='utf-8')

    plan = T.plan_publish(cand, old)
    assert plan['rows_to_add'] == 1 and plan['add_dates'] == ['2024-01-04']

    rep = T.publish(cand, yes=True, stamp='unit', target=target, target_csv=target_csv,
                    manifest_path=manifest_path, backup_root=tmp_path / 'bk')

    assert rep['rows_before'] == 2 and rep['rows_after'] == 3 and rep['added'] == 1
    assert rep['value_changed'] == {'y10': 0} and rep['source_changed'] == 0
    assert (tmp_path / 'bk' / 'meta' / 'macro_treasury_unit' / 't.parquet').is_file()
    out = pl.read_parquet(target).sort('date')
    assert out['date'].to_list() == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    assert out['y10'].to_list() == [2.0, 2.1, 2.2]


def test_publish_dry_run_writes_nothing(tmp_path):
    old = pl.DataFrame({'date': [date(2024, 1, 2)], 'y10': [2.0],
                       'source': [T.CB_SOURCE]})
    target = tmp_path / 't.parquet'
    old.write_parquet(target)
    before = target.read_bytes()

    rep = T.publish(old, target=target, target_csv=tmp_path / 't.csv',
                    manifest_path=tmp_path / 'manifest.json',
                    backup_root=tmp_path / 'bk')

    assert rep['dry_run'] is True
    assert target.read_bytes() == before, 'dry-run 不得写任何文件'
    assert not (tmp_path / 'bk').exists()
