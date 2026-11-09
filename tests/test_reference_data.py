"""ST 三态与行业分类纯数据 API 回归测试。

覆盖：
1. ST 名称前缀判定（ST / *ST / S*ST / SST / 普通 / 空）
2. 快照构建：本地名称、退市标的 unknown、无名称 unknown
3. 点位查询：恰好快照日、早于首份快照（不可回溯）、晚于快照日
4. 历史区间：累积快照合并、中间日期取不晚于它的最近快照
5. 行业 cfg 解析（市场映射、一级/二级名称、未知行业）
6. 行业覆盖率统计与 API 三态
"""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.industry import (
    UNKNOWN as IND_UNKNOWN,
)
from src.data.industry import (
    build_snapshot as build_industry,
)
from src.data.industry import (
    coverage_report,
    get_industry,
    parse_industry_names,
    parse_tdxhy,
)
from src.data.industry import (
    snapshot_path as industry_snapshot_path,
)
from src.data.risk_flags import (
    NON_ST,
    ST,
    UNKNOWN,
    get_st_intervals,
    get_st_status,
    get_st_status_detail,
    history_coverage,
    status_from_name,
)
from src.data.risk_flags import (
    build_snapshot as build_st,
)
from src.data.risk_flags import (
    snapshot_path as st_snapshot_path,
)


def _inject_parquet_write_crash(monkeypatch) -> None:
    """让下一次 parquet 写入先落半截字节再抛异常，模拟写盘中断。"""
    def _crashing(self, file, *args, **kwargs):
        target = Path(file)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b'PARTIAL-PARQUET')
        raise OSError('注入写盘中断')

    monkeypatch.setattr(pl.DataFrame, 'write_parquet', _crashing)


def _leftover_tmp(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir() if p.name.endswith('.tmp'))


@pytest.fixture()
def names_file(tmp_path):
    path = tmp_path / 'names.json'
    path.write_text(json.dumps({
        '600000.SH': '浦发银行',
        '000669.SZ': 'ST金鸿',
    }, ensure_ascii=False), encoding='utf-8')
    return path


def test_status_from_name_variants():
    assert status_from_name('ST海王') == ST
    assert status_from_name('*ST美丽') == ST
    assert status_from_name('S*ST龙涤') == ST
    assert status_from_name('SST天海') == ST
    assert status_from_name('浦发银行') == NON_ST
    assert status_from_name('ST') == ST
    assert status_from_name('') == UNKNOWN
    assert status_from_name(None) == UNKNOWN
    # 名称中间含 ST 不算风险警示
    assert status_from_name('国泰ST产业') == NON_ST


def test_build_st_snapshot_three_states(tmp_path, names_file):
    frame = build_st(
        tmp_path, date(2026, 11, 3), names_path=names_file,
        codes=['600000.SH', '000669.SZ', '000004.SZ', '000005.SZ'],
        db_basic={'000004.SZ': ('*ST国华', 'D'), '000005.SZ': (None, 'D')},
    )
    rows = {row['code']: row for row in frame.to_dicts()}
    assert rows['000669.SZ']['status'] == ST
    assert rows['000669.SZ']['is_st'] is True
    assert rows['600000.SH']['status'] == NON_ST
    assert rows['600000.SH']['is_st'] is False
    # 退市标的：当前无风险警示状态，必须 unknown 且 is_st 为空
    assert rows['000004.SZ']['status'] == UNKNOWN
    assert rows['000004.SZ']['reason'] == 'delisted'
    assert rows['000004.SZ']['is_st'] is None
    assert rows['000005.SZ']['status'] == UNKNOWN
    assert st_snapshot_path(tmp_path, date(2026, 11, 3)).is_file()
    assert frame['as_of'].unique().to_list() == [date(2026, 11, 3)]


def test_st_query_is_point_in_time(tmp_path, names_file):
    build_st(tmp_path, date(2026, 11, 3), names_path=names_file,
             codes=['000669.SZ', '600000.SH'])
    kw = {'store_root': tmp_path}
    # 快照日当天可查
    assert get_st_status('000669.SZ', date(2026, 11, 3), **kw) == ST
    assert get_st_status('600000.SH', date(2026, 11, 3), **kw) == NON_ST
    # 早于首份快照：不可回溯，必须 unknown（禁止拿今天的名单回填）
    assert get_st_status('000669.SZ', date(2026, 10, 30), **kw) == UNKNOWN
    assert get_st_status('000669.SZ', date(2025, 1, 2), **kw) == UNKNOWN
    # 晚于快照日：沿用最近一份
    assert get_st_status('000669.SZ', date(2026, 11, 5), **kw) == ST
    # 未覆盖代码：unknown
    detail = get_st_status_detail('002594.SZ', date(2026, 11, 3), **kw)
    assert detail['status'] == UNKNOWN
    assert detail['reason'] == 'code_not_covered'
    # 无任何快照时 unknown
    empty = get_st_status('000669.SZ', None, store_root=tmp_path / 'none')
    assert empty == UNKNOWN


def test_st_history_and_intervals(tmp_path, names_file):
    old = tmp_path / 'old.json'
    old.write_text(json.dumps({'000669.SZ': '金鸿控股'}, ensure_ascii=False),
                   encoding='utf-8')
    build_st(tmp_path, date(2026, 11, 1), names_path=old, codes=['000669.SZ'])
    build_st(tmp_path, date(2026, 11, 3), names_path=names_file,
             codes=['000669.SZ'])

    kw = {'store_root': tmp_path}
    assert get_st_status('000669.SZ', date(2026, 11, 2), **kw) == NON_ST
    intervals = get_st_intervals('000669.SZ', '2026-11-01', '2026-11-03', **kw)
    assert intervals == [
        {'status': NON_ST, 'start': '2026-11-01', 'end': '2026-11-01'},
        {'status': ST, 'start': '2026-11-03', 'end': '2026-11-03'},
    ]
    coverage = history_coverage(tmp_path)
    assert coverage['backfillable'] is False
    assert coverage['first_snapshot'] == '2026-11-01'
    assert coverage['last_snapshot'] == '2026-11-03'


def _write_cfg(cache, tdxhy_lines, name_lines):
    cache.mkdir(parents=True, exist_ok=True)
    (cache / 'tdxhy.cfg').write_bytes(
        '\n'.join(tdxhy_lines).encode('gbk'))
    for index, lines in enumerate(name_lines):
        name = 'tdxzs.cfg' if index == 0 else 'tdxzs3.cfg'
        (cache / name).write_bytes('\n'.join(lines).encode('gbk'))


@pytest.fixture()
def industry_cache(tmp_path):
    cache = tmp_path / 'hq_cache'
    _write_cfg(
        cache,
        ['1|600000|T1001|||X500102', '0|000001|T110101|||X530101',
         '2|430001|T0703|||X3201', '0|000002|||'],
        [['TDX 金融|880990|2|1|0|T10', '银行|880471|2|1|1|T1001'],
         ['TDX 建设|880991|2|1|0|T11', '建筑|880476|2|1|0|T1101',
          '建筑工程|880477|2|1|1|T110101',
          'TDX 制造|880987|2|1|0|T07', '运输设备|880432|2|1|1|T0703']],
    )
    return cache


def test_parse_tdxhy_market_mapping(industry_cache):
    mapping = parse_tdxhy(industry_cache / 'tdxhy.cfg')
    assert mapping == {
        '600000.SH': 'T1001', '000001.SZ': 'T110101', '430001.BJ': 'T0703',
    }
    names = parse_industry_names(
        [industry_cache / 'tdxzs.cfg', industry_cache / 'tdxzs3.cfg'])
    assert names['T1001'] == '银行'
    assert names['T10'] == 'TDX 金融'


def test_industry_snapshot_and_api(tmp_path, industry_cache):
    codes = ['600000.SH', '000001.SZ', '430001.BJ', '000002.SZ', '999999.SH']
    frame = build_industry(tmp_path, industry_cache, date(2026, 11, 3), codes=codes)
    rows = {row['code']: row for row in frame.to_dicts()}
    assert rows['600000.SH']['industry_l1'] == 'TDX 金融'
    assert rows['600000.SH']['industry_l2'] == '银行'
    assert rows['000001.SZ']['industry_l1'] == 'TDX 建设'
    assert rows['000001.SZ']['industry_l2'] == '建筑'
    assert rows['430001.BJ']['industry_l1'] == 'TDX 制造'
    # 无配置 / 未知代码：unknown，不编造行业
    assert rows['000002.SZ']['industry_l1'] == IND_UNKNOWN
    assert rows['000002.SZ']['source'] == 'missing'
    assert rows['999999.SH']['industry_l2'] == IND_UNKNOWN
    assert industry_snapshot_path(tmp_path, date(2026, 11, 3)).is_file()

    kw = {'store_root': tmp_path}
    got = get_industry('600000.SH', date(2026, 11, 3), **kw)
    assert got['status'] == 'known'
    assert got['industry_l1'] == 'TDX 金融'
    assert got['industry_l2'] == '银行'
    assert got['source'] == 'tdx_hq_cache'
    # 早于首份快照：unknown（避免用当前行业回填历史）
    assert get_industry('600000.SH', date(2026, 10, 30), **kw)['status'] == 'unknown'
    # 配置缺失的代码：unknown
    missing = get_industry('000002.SZ', date(2026, 11, 3), **kw)
    assert missing['status'] == 'unknown'
    assert missing['reason'] == 'tdx_cfg_missing'

    report = coverage_report(frame, {
        'stock': codes, 'fund': ['510050.SH'], 'index': ['000001.SH'],
    })
    assert report['stock']['covered'] == 3
    assert report['stock']['coverage'] == '60.00%'
    assert report['fund']['covered'] == 0
    assert report['index']['coverage'] == '0.00%'


def test_industry_snapshot_without_names_keeps_unknown(tmp_path):
    cache = tmp_path / 'cache'
    _write_cfg(cache, ['1|600000|T0201|||'], [])
    frame = build_industry(tmp_path, cache, date(2026, 11, 3), codes=['600000.SH'])
    row = frame.to_dicts()[0]
    assert row['industry_code'] == 'T0201'
    assert row['industry_l1'] == IND_UNKNOWN
    assert row['industry_l2'] == IND_UNKNOWN
    assert row['source'] == 'tdx_hq_cache'


def test_st_snapshot_write_crash_keeps_previous_file(
        tmp_path, names_file, monkeypatch):
    """同日 ST 快照重写中断时，旧快照字节与内容必须原样保留。"""
    target = st_snapshot_path(tmp_path, date(2026, 11, 3))
    build_st(tmp_path, date(2026, 11, 3), names_path=names_file,
             codes=['600000.SH'])
    before = target.read_bytes()

    _inject_parquet_write_crash(monkeypatch)
    with pytest.raises(OSError, match='注入写盘中断'):
        build_st(tmp_path, date(2026, 11, 3), names_path=names_file,
                 codes=['600000.SH', '000669.SZ'])

    assert target.read_bytes() == before
    assert _leftover_tmp(target.parent) == []
