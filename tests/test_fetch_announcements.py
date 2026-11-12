"""巨潮空响应判定与重试的单测（G-074）。

覆盖三种情形：
1. 正常有数据 → 一次请求直接返回；
2. ``totalRecordNum=0`` 且响应结构异常 → 重试，第二次/第三次拿到数据后返回；
3. 重试耗尽仍为 0 → 抛 :class:`CninfoEmptyResponseError`，不静默返回空表。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

RESEARCH_DIR = Path(__file__).resolve().parents[1] / 'research' / 'events'
if str(RESEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(RESEARCH_DIR))

import fetch_announcements as fa  # noqa: E402


def _payload(total: int, announcements: list[dict] | None = None) -> dict:
    return {'totalRecordNum': total, 'announcements': announcements}


def _item(code: str = '000001', art: str = 'a1') -> dict:
    return {
        'secCode': code,
        'secName': '测试',
        'announcementTitle': '股权激励公告',
        'announcementTime': 1600000000000,
        'announcementId': art,
        'adjunctUrl': '',
    }


def test_normal_returns_records(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_get(params: dict, retries: int = 6) -> dict:
        calls.append(params)
        return _payload(1, [_item()])

    monkeypatch.setattr(fa, '_cninfo_get', fake_get)
    monkeypatch.setattr(fa.time, 'sleep', lambda _s: None)

    records, total = fa.fetch_cninfo_chunk('股权激励', '2021-01-01', '2021-01-31')

    assert total == 1
    assert len(records) == 1
    assert records[0]['stock_code'] == '000001'
    assert len(calls) == 1, '正常有数据时不应重试'


def test_anomalous_structure_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    sleeps: list[float] = []

    def fake_get(params: dict, retries: int = 6) -> dict:
        calls.append(params)
        if len(calls) < 3:
            return {'error': 'overload'}  # 缺 totalRecordNum / announcements
        return _payload(1, [_item()])

    monkeypatch.setattr(fa, '_cninfo_get', fake_get)
    monkeypatch.setattr(fa.time, 'sleep', lambda s: sleeps.append(s))

    records, total = fa.fetch_cninfo_chunk('股权激励', '2021-01-01', '2021-01-31')

    assert len(calls) == 3, '结构异常必须重试'
    assert total == 1 and len(records) == 1
    assert sleeps == [fa.EMPTY_BACKOFF_BASE, fa.EMPTY_BACKOFF_BASE * 2], '退避须逐次翻倍'


def test_retry_exhausted_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_get(params: dict, retries: int = 6) -> dict:
        calls.append(params)
        return _payload(0, None)  # 结构正常但 total=0：仍不可信

    monkeypatch.setattr(fa, '_cninfo_get', fake_get)
    monkeypatch.setattr(fa.time, 'sleep', lambda _s: None)

    with pytest.raises(fa.CninfoEmptyResponseError) as excinfo:
        fa.fetch_cninfo_chunk('股权激励', '2020-12-01', '2020-12-31')

    assert len(calls) == fa.EMPTY_RETRIES, '必须用满重试次数才判失败'
    detail = str(excinfo.value)
    assert f'尝试={fa.EMPTY_RETRIES}次' in detail
    assert 'totalRecordNum=0' in detail, '失败信息须写明重试次数与原因'


def test_allow_empty_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(params: dict, retries: int = 6) -> dict:
        return _payload(0, None)

    monkeypatch.setattr(fa, '_cninfo_get', fake_get)
    monkeypatch.setattr(fa.time, 'sleep', lambda _s: None)

    records, total = fa.fetch_cninfo_chunk(
        '不存在关键词', '2020-12-01', '2020-12-31', allow_empty=True)

    assert records == [] and total == 0


def test_empty_reason_separates_structure_anomaly() -> None:
    assert fa._cninfo_empty_reason(_payload(0, None)) == ''
    assert fa._cninfo_empty_reason({'error': 'x'}) == 'missing_totalRecordNum'
    assert fa._cninfo_empty_reason(None) == 'payload_not_dict:NoneType'
    assert fa._cninfo_empty_reason({'totalRecordNum': 'abc', 'announcements': []}) != ''
