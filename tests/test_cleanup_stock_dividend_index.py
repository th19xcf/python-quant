"""stock_dividend 指数清理脚本的安全校验测试。"""

from tools.cleanup_stock_dividend_index import find_non_index_hits, self_check


def test_detects_injected_non_index_hit():
    """命中集合混入非指数代码时必须被识别，保护不能形同虚设。"""
    all_codes = ['000001.SH', '399001.SZ', '600519.SH']
    hits = ['000001.SH', '600519.SH']
    assert find_non_index_hits(all_codes, hits) == ['600519.SH']


def test_empty_when_hits_all_index():
    all_codes = ['000001.SH', '399001.SZ', '600519.SH']
    assert find_non_index_hits(all_codes, ['000001.SH', '399001.SZ']) == []


def test_same_symbol_different_market_not_misjudged():
    """000001.SZ 是平安银行个股，不能因与上证指数同号而被判为指数。"""
    all_codes = ['000001.SH', '000001.SZ']
    assert find_non_index_hits(all_codes, ['000001.SH']) == []
    assert find_non_index_hits(all_codes, ['000001.SZ']) == ['000001.SZ']


def test_self_check_passes(capsys):
    self_check()
    assert '保护可触发' in capsys.readouterr().out
