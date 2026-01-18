#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
业务规则引擎，用于管理和执行业务规则
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

T = TypeVar('T')


class BusinessRule:
    """业务规则基类"""
    
    def __init__(self, name: str, description: str = ""):
        """初始化业务规则
        
        Args:
            name: 规则名称
            description: 规则描述
        """
        self.name = name
        self.description = description
    
    def evaluate(self, data: Any, **kwargs) -> Any:
        """评估规则
        
        Args:
            data: 输入数据
            **kwargs: 其他参数
        
        Returns:
            Any: 规则评估结果
        """
        raise NotImplementedError("子类必须实现evaluate方法")
    
    def is_applicable(self, data: Any, **kwargs) -> bool:
        """检查规则是否适用于给定数据
        
        Args:
            data: 输入数据
            **kwargs: 其他参数
        
        Returns:
            bool: 是否适用
        """
        return True


class TechnicalAnalysisRule(BusinessRule):
    """技术分析规则基类"""
    
    def __init__(self, name: str, indicator_type: str, description: str = ""):
        """初始化技术分析规则
        
        Args:
            name: 规则名称
            indicator_type: 指标类型
            description: 规则描述
        """
        super().__init__(name, description)
        self.indicator_type = indicator_type


class IndicatorCalculationRule(TechnicalAnalysisRule):
    """指标计算规则"""
    
    def __init__(self, name: str, indicator_type: str, calculation_func: Callable, params: Dict[str, Any] = None, description: str = ""):
        """初始化指标计算规则
        
        Args:
            name: 规则名称
            indicator_type: 指标类型
            calculation_func: 计算函数
            params: 计算参数
            description: 规则描述
        """
        super().__init__(name, indicator_type, description)
        self.calculation_func = calculation_func
        self.params = params or {}
    
    def evaluate(self, data: Any, **kwargs) -> Any:
        """评估指标计算规则
        
        Args:
            data: 输入数据
            **kwargs: 其他参数
        
        Returns:
            Any: 指标计算结果
        """
        # 合并默认参数和传入参数
        merged_params = self.params.copy()
        merged_params.update(kwargs)
        
        # 调用计算函数
        return self.calculation_func(data, **merged_params)
    
    def update_params(self, params: Dict[str, Any]):
        """更新规则参数
        
        Args:
            params: 新的参数
        """
        self.params.update(params)


class RuleEngine:
    """业务规则引擎"""
    
    def __init__(self):
        """初始化规则引擎"""
        self._rules: Dict[str, List[BusinessRule]] = {}
        self._rule_types: Dict[str, List[str]] = {}
    
    def register_rule(self, rule: BusinessRule):
        """注册业务规则
        
        Args:
            rule: 业务规则对象
        """
        # 按规则类型分类
        rule_type = type(rule).__name__
        if rule_type not in self._rules:
            self._rules[rule_type] = []
        self._rules[rule_type].append(rule)
        
        # 如果是技术分析规则，按指标类型分类
        if isinstance(rule, TechnicalAnalysisRule):
            indicator_type = rule.indicator_type
            if indicator_type not in self._rule_types:
                self._rule_types[indicator_type] = []
            self._rule_types[indicator_type].append(rule.name)
    
    def register_rules(self, rules: List[BusinessRule]):
        """批量注册业务规则
        
        Args:
            rules: 业务规则列表
        """
        for rule in rules:
            self.register_rule(rule)
    
    def get_rules_by_type(self, rule_type: str) -> List[BusinessRule]:
        """按规则类型获取规则
        
        Args:
            rule_type: 规则类型
        
        Returns:
            List[BusinessRule]: 规则列表
        """
        return self._rules.get(rule_type, [])
    
    def get_rules_by_indicator(self, indicator_type: str) -> List[BusinessRule]:
        """按指标类型获取规则
        
        Args:
            indicator_type: 指标类型
        
        Returns:
            List[BusinessRule]: 规则列表
        """
        rule_names = self._rule_types.get(indicator_type, [])
        rules = []
        for rule_list in self._rules.values():
            for rule in rule_list:
                if rule.name in rule_names:
                    rules.append(rule)
        return rules
    
    def evaluate_rule(self, rule_name: str, data: Any, **kwargs) -> Any:
        """评估单个规则
        
        Args:
            rule_name: 规则名称
            data: 输入数据
            **kwargs: 其他参数
        
        Returns:
            Any: 规则评估结果
        """
        # 查找规则
        for rule_list in self._rules.values():
            for rule in rule_list:
                if rule.name == rule_name:
                    if rule.is_applicable(data, **kwargs):
                        return rule.evaluate(data, **kwargs)
                    return None
        raise ValueError(f"规则{rule_name}不存在")
    
    def evaluate_rules(self, rule_type: Optional[str] = None, data: Any = None, **kwargs) -> Dict[str, Any]:
        """评估多个规则
        
        Args:
            rule_type: 规则类型，None表示评估所有规则
            data: 输入数据
            **kwargs: 其他参数
        
        Returns:
            Dict[str, Any]: 规则评估结果，键为规则名称，值为评估结果
        """
        results = {}
        
        if rule_type is None:
            # 评估所有规则
            for rule_list in self._rules.values():
                for rule in rule_list:
                    if rule.is_applicable(data, **kwargs):
                        results[rule.name] = rule.evaluate(data, **kwargs)
        else:
            # 按规则类型评估
            rule_list = self._rules.get(rule_type, [])
            for rule in rule_list:
                if rule.is_applicable(data, **kwargs):
                    results[rule.name] = rule.evaluate(data, **kwargs)
        
        return results
    
    def evaluate_indicator_rules(self, indicator_type: str, data: Any, **kwargs) -> Dict[str, Any]:
        """评估特定指标的所有规则
        
        Args:
            indicator_type: 指标类型
            data: 输入数据
            **kwargs: 其他参数
        
        Returns:
            Dict[str, Any]: 规则评估结果，键为规则名称，值为评估结果
        """
        results = {}
        rules = self.get_rules_by_indicator(indicator_type)
        
        for rule in rules:
            if rule.is_applicable(data, **kwargs):
                results[rule.name] = rule.evaluate(data, **kwargs)
        
        return results
    
    def remove_rule(self, rule_name: str):
        """移除业务规则
        
        Args:
            rule_name: 规则名称
        """
        for rule_type in list(self._rules.keys()):
            rule_list = self._rules[rule_type]
            for rule in list(rule_list):
                if rule.name == rule_name:
                    rule_list.remove(rule)
                    # 如果规则列表为空，移除规则类型
                    if not rule_list:
                        del self._rules[rule_type]
                    
                    # 如果是技术分析规则，从指标类型映射中移除
                    if isinstance(rule, TechnicalAnalysisRule):
                        indicator_type = rule.indicator_type
                        if indicator_type in self._rule_types:
                            if rule_name in self._rule_types[indicator_type]:
                                self._rule_types[indicator_type].remove(rule_name)
                                # 如果指标类型映射为空，移除该指标类型
                                if not self._rule_types[indicator_type]:
                                    del self._rule_types[indicator_type]
                    return
    
    def clear_rules(self):
        """清除所有业务规则"""
        self._rules.clear()
        self._rule_types.clear()
    
    def get_all_rules(self) -> Dict[str, List[BusinessRule]]:
        """获取所有业务规则
        
        Returns:
            Dict[str, List[BusinessRule]]: 所有业务规则，按规则类型分类
        """
        return self._rules.copy()
    
    def get_all_rule_types(self) -> List[str]:
        """获取所有规则类型
        
        Returns:
            List[str]: 规则类型列表
        """
        return list(self._rules.keys())
    
    def get_all_indicator_types(self) -> List[str]:
        """获取所有指标类型
        
        Returns:
            List[str]: 指标类型列表
        """
        return list(self._rule_types.keys())


# 创建全局规则引擎实例
rule_engine = RuleEngine()
