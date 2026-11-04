"""names.json 生成脚本的布局、幂等与读取方降级行为测试。"""

from __future__ import annotations

import json
import re
from pathlib import Path

import polars as pl

from src.advice.universe import load_name_map
from src.data.quality.clean_set import EligibilityEngine
from src.data.risk_flags import _load_names
from tools.build_names_json import (
    CODE_LENGTH,
    CODE_OFFSET,
    HEADER_SIZE,
    NAME_LENGTH,
    NAME_OFFSET,
    RECORD_SIZE,
    TRAILER_SIZE,
    build_names,
    diff_names,
    dump_names,
    merge_missing,
    parse_tnf,
    write_names,
)

KEY_PATTERN = re.compile(r'^\d{6}\.(SH|SZ|BJ)$')


def _record(code: str, name: str) -> bytes:
    """按 360 字节记录布局构造一条码表记录。"""
    buf = bytearray(RECORD_SIZE)
    buf[CODE_OFFSET:CODE_OFFSET + CODE_LENGTH] = code.encode('ascii')
    raw = name.encode('gbk')[:NAME_LENGTH]
    buf[NAME_OFFSET:NAME_OFFSET + len(raw)] = raw
    return bytes(buf)


def _tnf(*records: bytes, header: bool = True, trailer: bool = True) -> bytes:
    parts = [bytes(HEADER_SIZE)] if header else []
    parts.extend(records)
    if trailer:
        parts.append(bytes(TRAILER_SIZE))
    return b''.join(parts)


def test_parse_tnf_layout_and_truncation(tmp_path: Path) -> None:
    path = tmp_path / 'shs.tnf'
    path.write_bytes(_tnf(
        _record('600000', '浦发银行'),
        _record('000001', '平安银行'),
        # 17 字节名称超过 16 字节字段，尾部半个字符必须丢弃
        _record('158000', '港股通金融ETF鹏华'),
        _record('000002', ''),
        _record('ABCDEF', '非法记录'),
    ))
    names = parse_tnf(path)
    assert names['600000'] == '浦发银行'
    assert names['000001'] == '平安银行'
    assert names['158000'] == '港股通金融ETF鹏'
    assert '000002' not in names
    assert all(key.isdigit() and len(key) == 6 for key in names)


def test_build_names_schema_and_sorted_output(tmp_path: Path) -> None:
    cache = tmp_path / 'hq_cache'
    cache.mkdir()
    (cache / 'shs.tnf').write_bytes(_tnf(
        _record('600000', '浦发银行'),
        _record('999999', '上证指数'),
    ))
    (cache / 'szs.tnf').write_bytes(_tnf(_record('000001', '平安银行')))
    (cache / 'bjs.tnf').write_bytes(_tnf(_record('920000', '安徽凤凰')))

    names, counts = build_names(cache)
    assert counts == {'shs.tnf': 2, 'szs.tnf': 1, 'bjs.tnf': 1}
    assert set(names) == {
        '600000.SH', '999999.SH', '000001.SZ', '920000.BJ',
    }
    assert all(KEY_PATTERN.match(key) for key in names)
    assert all(isinstance(value, str) and value for value in names.values())

    text = dump_names(names)
    payload = json.loads(text)
    assert isinstance(payload, dict)
    assert list(payload) == sorted(payload)
    assert payload == names
    # 与现有文件一致：紧凑、无空格、无尾随换行
    assert ' ' not in text and text.endswith('}')


def test_write_is_idempotent_and_backs_up_before_overwrite(tmp_path: Path) -> None:
    target = tmp_path / 'meta' / 'names.json'
    names = {'600000.SH': '浦发银行'}

    assert write_names(target, names, tmp_path) is True
    first = target.read_text(encoding='utf-8')
    assert write_names(target, names, tmp_path) is False
    assert target.read_text(encoding='utf-8') == first
    assert not (tmp_path / '_backup').exists()

    target.write_text('{"600000.SH":"旧名称"}', encoding='utf-8')
    assert write_names(target, names, tmp_path) is True
    backups = list((tmp_path / '_backup').glob('names.*.json'))
    assert len(backups) == 1
    # 再写一次内容一致，不应产生第二份备份
    assert write_names(target, names, tmp_path) is False
    assert len(list((tmp_path / '_backup').glob('names.*.json'))) == 1


def test_diff_and_missing_names_are_not_invented(tmp_path: Path) -> None:
    baseline = {'600000.SH': '浦发银行', '999999.SH': '上证指数'}
    rebuilt = {'600000.SH': '浦发银行', '000001.SZ': '平安银行'}

    diff = diff_names(baseline, rebuilt)
    assert diff.added == ['000001.SZ']
    assert diff.missing == ['999999.SH']
    assert diff.changed == []

    merged, gaps = merge_missing(baseline, rebuilt)
    assert gaps == ['999999.SH']
    assert merged['999999.SH'] == '上证指数'  # 保留原值，不臆造
    assert diff_names(baseline, merged).missing == []


def test_diff_reports_changed_values() -> None:
    diff = diff_names({'000972.SZ': '中基健康'}, {'000972.SZ': '*ST中基'})
    assert diff.changed == [('000972.SZ', '中基健康', '*ST中基')]


def test_readers_keep_behavior_when_names_missing(tmp_path: Path) -> None:
    missing = tmp_path / 'no_such' / 'names.json'
    assert load_name_map(missing) == {}
    assert _load_names(missing) == {}

    # clean_set：names.json 缺失时 suspected_st 为空集合，不抛错
    root = tmp_path / 'store'
    (root / 'meta' / 'st').mkdir(parents=True)
    pl.DataFrame(schema={
        'ts_code': pl.Utf8, 'start_date': pl.Date, 'end_date': pl.Date,
    }).write_parquet(root / 'meta' / 'st' / 'st_intervals.parquet')
    pl.DataFrame(schema={
        'ts_code': pl.Utf8, 'coverage_start': pl.Date, 'coverage_end': pl.Date,
    }).write_parquet(root / 'meta' / 'st' / 'st_coverage.parquet')

    engine = object.__new__(EligibilityEngine)
    engine.root = root
    engine.codes = {}
    engine._load_st()
    assert engine.suspected_st == set()
