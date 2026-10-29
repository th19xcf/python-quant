"""AkShare 上游契约测试：列断言可归因、映射/别名解析、端点缺失。"""

import polars as pl
import pytest

from src.data.sources import akshare_schema as schema


def test_missing_columns_raise_with_endpoint_and_actual_columns():
    df = pl.DataFrame({'日期': ['2024-01-02'], '开盘': [1.0]})

    with pytest.raises(schema.AkShareContractError) as exc:
        schema.validate_columns(df, schema.STOCK_HIST)

    error = exc.value
    assert error.endpoint == 'stock_zh_a_hist'
    assert '昨收' in error.missing
    assert '日期' in error.actual
    assert '上游接口很可能已变更' in str(error)


def test_resolve_renames_upstream_columns_to_standard():
    spec = schema.EndpointSpec(
        name='demo', required=('日期', '开盘'),
        column_map={'date': '日期', 'open': '开盘'},
    )
    df = pl.DataFrame({'日期': ['2024-01-02'], '开盘': [1.0], '额外列': ['x']})

    out = schema.resolve(df, spec)

    assert out.columns == ['date', 'open', '额外列']
    assert out['date'].to_list() == ['2024-01-02']


def test_resolve_is_noop_for_identity_mapping():
    df = pl.DataFrame({'date': ['2024-01-02'], 'open': [1.0], 'high': [1.0],
                       'low': [1.0], 'close': [1.0], 'volume': [1.0]})

    out = schema.resolve(df, schema.INDEX_DAILY)

    assert out.columns == df.columns


def test_pick_alias_skips_empty_and_nan():
    row = {'业绩披露日期': float('nan'), '公告日期': '2024-01-02'}

    assert schema.pick_alias(row, ('业绩披露日期', '公告日期')) == '2024-01-02'
    assert schema.pick_alias({'a': '', 'b': 'NaT'}, ('a', 'b'), default='x') == 'x'
    assert schema.pick_alias({}, ('a',), default=0.0) == 0.0


def test_select_spec_switches_between_cn_and_en_columns():
    cn_df = pl.DataFrame({'日期': ['2024-01-02'], '开盘': [1.0], '最高': [1.0],
                          '最低': [1.0], '收盘': [1.0], '成交量': [1.0]})
    en_df = cn_df.rename({'日期': 'date', '开盘': 'open', '最高': 'high',
                          '最低': 'low', '收盘': 'close', '成交量': 'volume'})

    candidates = (schema.INDEX_DAILY_EM_EN, schema.INDEX_DAILY_EM_CN)
    assert schema.select_spec(candidates, cn_df) is schema.INDEX_DAILY_EM_CN
    assert schema.select_spec(candidates, en_df) is schema.INDEX_DAILY_EM_EN


def test_select_spec_raises_when_neither_matches():
    df = pl.DataFrame({'foo': [1.0]})

    with pytest.raises(schema.AkShareContractError) as exc:
        schema.select_spec(
            (schema.INDEX_DAILY_EM_EN, schema.INDEX_DAILY_EM_CN), df)

    assert '候选规格（中/英文列）均不匹配' in str(exc.value)


def test_fetch_raw_unknown_endpoint_raises(monkeypatch):
    monkeypatch.setattr(schema, '_load_ak', lambda: object())

    with pytest.raises(schema.AkShareContractError) as exc:
        schema.fetch_raw('no_such_endpoint')

    assert '端点不存在' in str(exc.value)
    assert exc.value.endpoint == 'no_such_endpoint'


def test_fetch_spec_asserts_columns_after_fetch(monkeypatch):
    fake_ak = type('FakeAk', (), {
        'stock_zh_a_hist': staticmethod(
            lambda **kwargs: pl.DataFrame({'日期': ['2024-01-02']})),
    })
    monkeypatch.setattr(schema, '_load_ak', lambda: fake_ak)

    with pytest.raises(schema.AkShareContractError) as exc:
        schema.fetch_spec(schema.STOCK_HIST, symbol='600000')

    assert '收盘' in exc.value.missing


def test_fetch_raw_returns_empty_on_none(monkeypatch):
    fake_ak = type('FakeAk', (), {
        'stock_zh_a_hist': staticmethod(lambda **kwargs: None),
    })
    monkeypatch.setattr(schema, '_load_ak', lambda: fake_ak)

    assert schema.fetch_raw('stock_zh_a_hist').is_empty()


def test_handler_no_longer_depends_on_akshare_module():
    import src.data.akshare_handler as handler_module

    assert not hasattr(handler_module, 'ak')
    assert handler_module.__doc__.count('akshare_schema') == 1
