"""代理事件表读取入口（G-070 定增 / G-071 限售解禁）的读取与 schema 校验。

真实表在 ``data/local_store/meta/events/proxy/``，不进 Git；本地列存未就绪时
真实读取用例按 ``skip`` 处理，但「文件缺失必须报错」与「schema 不符必须报错」
两条始终执行——静默返回空表会被下游误判为「期间内没有事件」。
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.data.events import (
    EQUITY_ISSUE_PROXY_SCHEMA,
    UNLOCK_PROXY_SCHEMA,
    load_equity_issue_events,
    load_unlock_events,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_STORE = PROJECT_ROOT / 'data' / 'local_store'
PROXY_DIR = REAL_STORE / 'meta' / 'events' / 'proxy'
HAS_REAL_PROXY = (
    (PROXY_DIR / 'equity_issue_proxy.parquet').is_file()
    and (PROXY_DIR / 'unlock_proxy.parquet').is_file()
)


@pytest.mark.skipif(not HAS_REAL_PROXY, reason='本地代理事件表未就绪（data/ 不进 Git）')
def test_load_equity_issue_events_real() -> None:
    frame = load_equity_issue_events(REAL_STORE)
    assert frame.height == 7810
    assert dict(frame.schema) == EQUITY_ISSUE_PROXY_SCHEMA
    assert set(frame['event_kind'].unique().to_list()) == {'equity_issue_listing_proxy'}
    assert frame.select(
        (pl.col('basis').is_null() | (pl.col('basis').str.len_chars() == 0)).sum()
    ).item() == 0
    assert frame.select(['ts_code', 'event_date']).is_duplicated().sum() == 0


@pytest.mark.skipif(not HAS_REAL_PROXY, reason='本地代理事件表未就绪（data/ 不进 Git）')
def test_load_unlock_events_real() -> None:
    frame = load_unlock_events(REAL_STORE)
    assert frame.height == 24323
    assert dict(frame.schema) == UNLOCK_PROXY_SCHEMA
    assert set(frame['event_kind'].unique().to_list()) == {'unlock_listing_proxy'}
    assert frame.select(
        (pl.col('basis').is_null() | (pl.col('basis').str.len_chars() == 0)).sum()
    ).item() == 0
    assert frame.select(['ts_code', 'event_date']).is_duplicated().sum() == 0


def test_missing_proxy_table_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match='定增代理事件表缺失'):
        load_equity_issue_events(tmp_path)
    with pytest.raises(FileNotFoundError, match='限售解禁代理事件表缺失'):
        load_unlock_events(tmp_path)


def test_proxy_table_schema_violation_raises(tmp_path: Path) -> None:
    proxy_dir = tmp_path / 'meta' / 'events' / 'proxy'
    proxy_dir.mkdir(parents=True)
    pl.DataFrame({'ts_code': ['000001.SZ']}).write_parquet(
        proxy_dir / 'equity_issue_proxy.parquet')
    with pytest.raises(ValueError, match='缺列'):
        load_equity_issue_events(tmp_path)
