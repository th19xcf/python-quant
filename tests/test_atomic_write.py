#!/usr/bin/env python3

"""行业快照落盘原子性：写入中断不得破坏同日既有快照。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.industry import build_snapshot, snapshot_path


def _write_cfg(cache: Path) -> None:
    cache.mkdir(parents=True, exist_ok=True)
    (cache / 'tdxhy.cfg').write_bytes(
        '1|600000|T1001|||X500102\n0|000001|T110101|||X530101\n'.encode('gbk'))
    (cache / 'tdxzs.cfg').write_bytes(
        'TDX 金融|880990|2|1|0|T10\n银行|880471|2|1|1|T1001\n'.encode('gbk'))


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


def test_industry_snapshot_write_crash_keeps_previous_file(
        tmp_path, monkeypatch):
    """同日行业快照重写中断时，旧快照字节与内容必须原样保留。"""
    cache = tmp_path / 'hq_cache'
    _write_cfg(cache)
    day = date(2026, 11, 3)
    target = snapshot_path(tmp_path, day)
    build_snapshot(tmp_path, cache, day, codes=['600000.SH'])
    before = target.read_bytes()

    _inject_parquet_write_crash(monkeypatch)
    with pytest.raises(OSError, match='注入写盘中断'):
        build_snapshot(tmp_path, cache, day, codes=['600000.SH', '000001.SZ'])

    assert target.read_bytes() == before
    assert _leftover_tmp(target.parent) == []

    # 中断后重跑仍能正常落盘，不残留中间状态
    monkeypatch.undo()
    build_snapshot(tmp_path, cache, day, codes=['600000.SH', '000001.SZ'])
    assert pl.read_parquet(target).height == 2
