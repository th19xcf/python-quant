"""fund_basic 结构字段契约：名称里的结构标记必须与 ``fund_type`` 一致。

背景（2026-11-10 fund_type 错分核查）：沪市 502 段混有分级母/子份额与整改后的
普通 LOF，代码段无法单独判定结构；名称是唯一可判据。约定「母份额与子份额同标
分级，分子母靠名称 A/B 后缀」，不新增词表值。
"""

from pathlib import Path

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FUND_BASIC = PROJECT_ROOT / 'data' / 'local_store' / 'meta' / 'fund_basic.parquet'


@pytest.fixture(scope='module')
def basic() -> pl.DataFrame:
    if not FUND_BASIC.is_file():
        pytest.skip(f'缺少本地基金基础表: {FUND_BASIC}')
    return pl.read_parquet(FUND_BASIC)


def test_name_with_jifen_must_be_graded(basic: pl.DataFrame) -> None:
    """名称含「分级」⇒ fund_type 必须是「分级」（母份额与 A/B 子份额一视同仁）。"""
    offenders = basic.filter(
        pl.col('name').is_not_null()
        & pl.col('name').str.contains('分级')
        & (pl.col('fund_type') != '分级')
    )
    assert offenders.height == 0, (
        f'{offenders.height} 只名称含「分级」但 fund_type 不是分级：'
        f'{offenders.select(["ts_code", "name", "fund_type"]).to_dicts()[:10]}')


def test_etf_feeder_stays_lof(basic: pl.DataFrame) -> None:
    """ETF 联接基金是 LOF 份额，名称含「ETF联接」不得被判成 ETF。"""
    offenders = basic.filter(
        pl.col('name').is_not_null()
        & pl.col('name').str.contains('ETF联接')
        & (pl.col('fund_type') != 'LOF')
    )
    assert offenders.height == 0, (
        f'{offenders.height} 只 ETF 联接基金 fund_type 不是 LOF：'
        f'{offenders.select(["ts_code", "name", "fund_type"]).to_dicts()[:10]}')
