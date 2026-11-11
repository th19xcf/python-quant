#!/usr/bin/env python3

"""行业快照落盘原子性：写入中断不得破坏同日既有快照。"""

from __future__ import annotations

import io
import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import src.data.atomic_write as atomic_write
from src.data.atomic_write import (
    write_bytes_atomic,
    write_parquet_atomic,
    write_text_atomic,
)
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


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def _legacy_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    """改动前的写法：临时文件 + ``os.replace``，全程没有 fsync。"""
    tmp = path.with_name(path.name + '.tmp')
    frame.write_parquet(tmp)
    os.replace(tmp, path)


class _PowerLossModel:
    """把「只有 fsync 过的数据才保证落盘」编码成可复现的断电模型。

    真实断电无法在进程内注入，这里用两条规则等价替代：

    - 写过盘的文件被 ``fsync`` 过 → ``os.replace`` 后目标内容保留；
    - 从未 ``fsync`` → 断电时数据块仍在页缓存中丢失，``os.replace`` 虽已
      生效，目标退化为零长度（ext4 等文件系统的典型表现）。
    """

    def __init__(self) -> None:
        self.synced = False
        self._real_replace = os.replace
        self._real_fsync = os.fsync

    def fsync(self, path: Path) -> None:
        self.synced = True
        with open(path, 'rb+') as handle:
            self._real_fsync(handle.fileno())

    def replace(self, src: str | Path, dst: str | Path) -> None:
        self._real_replace(src, dst)
        if not self.synced:
            Path(dst).write_bytes(b'')


def test_parquet_fsync_failure_keeps_target_and_cleans_tmp(
        tmp_path, monkeypatch):
    """fsync 抛异常时目标保持改前字节，临时文件被清理。"""
    target = tmp_path / 'factor.parquet'
    old = pl.DataFrame({'ts_code': ['600000.SH'], 'v': [1.0]})
    new = pl.DataFrame({'ts_code': ['600000.SH', '000001.SZ'], 'v': [2.0, 3.0]})
    write_parquet_atomic(old, target)
    before = target.read_bytes()

    def _boom(path: Path) -> None:
        raise OSError('注入 fsync 故障')

    monkeypatch.setattr(atomic_write, '_fsync', _boom)
    with pytest.raises(OSError, match='注入 fsync 故障'):
        write_parquet_atomic(new, target)

    assert target.read_bytes() == before
    assert pl.read_parquet(target).equals(old)
    assert not (tmp_path / '.factor.parquet.tmp').exists()


def test_text_and_bytes_fsync_failure_keep_target(tmp_path, monkeypatch):
    """文本与二进制写入同样在 fsync 失败时保持目标不变。"""
    text_target = tmp_path / 'meta.json'
    bytes_target = tmp_path / 'events.csv'
    write_text_atomic('{"rows": 1}', text_target)
    write_bytes_atomic(b'run_id,timestamp\n', bytes_target)
    text_before = text_target.read_bytes()
    bytes_before = bytes_target.read_bytes()

    def _boom(fd: int) -> None:
        raise OSError('注入 fsync 故障')

    monkeypatch.setattr(os, 'fsync', _boom)
    with pytest.raises(OSError, match='注入 fsync 故障'):
        write_text_atomic('{"rows": 2}', text_target)
    with pytest.raises(OSError, match='注入 fsync 故障'):
        write_bytes_atomic(b'run_id,timestamp\nr1,t1\n', bytes_target)

    assert text_target.read_bytes() == text_before
    assert bytes_target.read_bytes() == bytes_before
    assert not (tmp_path / '.meta.json.tmp').exists()
    assert not (tmp_path / '.events.csv.tmp').exists()


def test_parquet_atomic_supports_lazy_frame(tmp_path):
    """LazyFrame 走 sink_parquet 流式落盘，内容与 eager 路径一致。"""
    target = tmp_path / 'lazy.parquet'
    expected = pl.DataFrame({'a': [1, 2, 3]})
    write_parquet_atomic(expected.lazy(), target)
    assert pl.read_parquet(target).equals(expected)


def test_atomic_write_survives_power_loss(tmp_path, monkeypatch):
    """对照组（新路径）：fsync 先于 replace，断电模型下新内容完好。"""
    target = tmp_path / 'atomic.parquet'
    write_parquet_atomic(pl.DataFrame({'v': [1]}), target)
    expected = pl.DataFrame({'v': [2, 3]})

    model = _PowerLossModel()
    monkeypatch.setattr(atomic_write, '_fsync', model.fsync)
    monkeypatch.setattr(os, 'replace', model.replace)
    write_parquet_atomic(expected, target)

    assert model.synced is True
    assert pl.read_parquet(target).equals(expected)


def test_handwritten_write_loses_data_under_same_power_loss(
        tmp_path, monkeypatch):
    """对照组（旧路径）：没有 fsync，断电后目标被破坏为零长度。

    这条是本次收口的核心反证：同样的断电模型下，手写路径写成功但数据丢失，
    说明「改成调用 atomic_write」不只是换了个函数名。
    """
    target = tmp_path / 'legacy.parquet'
    target.write_bytes(_parquet_bytes(pl.DataFrame({'v': [1]})))
    expected = pl.DataFrame({'v': [2, 3]})

    model = _PowerLossModel()
    monkeypatch.setattr(os, 'replace', model.replace)
    _legacy_write_parquet(expected, target)

    assert model.synced is False
    assert target.read_bytes() == b''
    assert target.read_bytes() != _parquet_bytes(expected)
