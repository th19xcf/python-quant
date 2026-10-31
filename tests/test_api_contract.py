#!/usr/bin/env python3

"""
契约层一致性测试（Protocol 结构化子类型校验）

src/api 的契约既不是 ABC、也没有任何测试覆盖，缺失实现只会在运行时才暴露。
这里补一层运行时兜底：实现类必须提供契约声明的全部方法，且新增契约必须显式
标注"预留"或接入实现，避免契约面继续虚增。
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest

from src.api import api_version, dto
from src.api.business_api import (
    IFactorAnalyzer,
    ISignalGenerator,
    IStrategyAnalyzer,
    ITechnicalAnalyzer,
)
from src.api.data_api import IDataCache, IDataProcessor, IDataProvider, IDataStorage
from src.api.presentation_api import IController, IView
from src.data.data_manager import DataManager
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

#: 零实现方/零调用方的预留契约，仅作占位，不应被当作已生效约束
RESERVED_CONTRACTS = [
    IDataStorage,
    IDataCache,
    IStrategyAnalyzer,
    ISignalGenerator,
    IFactorAnalyzer,
    IView,
    IController,
]


def contract_members(contract) -> list[str]:
    """返回契约声明的方法名（不含 dunder）。"""
    return [name for name, member in vars(contract).items()
            if callable(member) and not name.startswith('__')]


def unresolved_members(implementation, contracts) -> list[str]:
    """返回 contract 声明但 implementation 未提供的方法名。"""
    missing = []
    for contract in contracts:
        for name in contract_members(contract):
            if not callable(getattr(implementation, name, None)):
                missing.append(f'{contract.__name__}.{name}')
    return missing


class TestProtocolConformance:

    def test_data_manager_satisfies_data_contracts(self):
        assert unresolved_members(DataManager, [IDataProvider, IDataProcessor]) == []

    def test_technical_analyzer_satisfies_business_contract(self):
        assert unresolved_members(TechnicalAnalyzer, [ITechnicalAnalyzer]) == []

    def test_contracts_are_protocols(self):
        for contract in (IDataProvider, IDataProcessor, ITechnicalAnalyzer):
            assert getattr(contract, '_is_protocol', False), contract.__name__

    def test_implementations_do_not_inherit_contracts(self):
        """继承会让类型检查器把缺失实现误判为已存在，必须走结构化子类型。"""
        assert IDataProvider not in DataManager.__mro__
        assert IDataProcessor not in DataManager.__mro__
        assert ITechnicalAnalyzer not in TechnicalAnalyzer.__mro__

    def test_fund_data_removed_from_contract(self):
        """get_fund_data 全仓无实现也无调用，留在契约里只会虚增契约面。"""
        assert 'get_fund_data' not in contract_members(IDataProvider)


class TestReservedContracts:

    @pytest.mark.parametrize('contract', RESERVED_CONTRACTS, ids=lambda c: c.__name__)
    def test_reserved_contract_annotated(self, contract):
        assert '预留契约' in (contract.__doc__ or ''), contract.__name__

    def test_reserved_modules_annotated(self):
        assert '预留契约' in (dto.__doc__ or '')
        assert '预留契约' in (api_version.__doc__ or '')

    def test_reserved_contracts_have_no_implementations(self):
        """预留契约不应被任何实现类继承，否则应改为接入实现。"""
        for contract in RESERVED_CONTRACTS:
            assert contract not in DataManager.__mro__
            assert contract not in TechnicalAnalyzer.__mro__


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
