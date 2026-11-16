"""零成本实验口径：印花税 0 税率只允许 experimental=true。"""

from __future__ import annotations

import pytest

from src.backtest.runner.recipe import parse_recipe
from src.backtest.runner.spec import SpecError, validate_spec

RECIPE = """\
名称 cost0
区间 2010-01-04 ~ 2026-11-13，预热 103 个交易日
股票池 全市场
剔除 波动率 最高 20%，窗口 60 个交易日，最少 50 只
剔除 上市天数 少于 250
调仓 月频，首次 2009-09-01
成本 滑点 0bp，印花税 固定千0，佣金 万0，最低佣金 0 元，冲击 0
资金 10 亿，价格取整按手，现金缓冲 1%，共用现金池
"""


def _raw(text: str) -> dict:
    """parse_recipe 返回已校验结果，去掉 _resolved 后可再次送校验。"""
    return {key: value for key, value in parse_recipe(text).items()
            if key != '_resolved'}


def test_zero_cost_recipe_marks_experimental():
    spec = parse_recipe(RECIPE)
    assert spec['cost']['stamp_tax_basis'] == 'fixed_0.0%'
    assert spec['experimental'] is True


def test_zero_cost_without_experimental_rejected():
    raw = _raw(RECIPE)
    raw.pop('experimental')
    with pytest.raises(SpecError, match='0 税率'):
        validate_spec(raw)


def test_fixed_stamp_tax_not_experimental():
    text = (RECIPE.replace('固定千0', '固定千1')
            .replace('0bp', '10bp').replace('万0', '万2.5')
            .replace('最低佣金 0 元', '最低佣金 5 元')
            .replace('冲击 0', '冲击 0.015'))
    spec = parse_recipe(text)
    assert spec['cost']['stamp_tax_basis'] == 'fixed_0.1%'
    assert spec['experimental'] is False
