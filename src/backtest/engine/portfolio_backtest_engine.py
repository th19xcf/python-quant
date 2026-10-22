#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
组合回测引擎，用于执行多股票组合策略回测
"""

from typing import Dict, Any
import polars as pl
from loguru import logger

from src.backtest.engine.base_engine import BaseBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy


class PortfolioBacktestEngine(BaseBacktestEngine):
    """
    组合回测引擎类，提供多股票组合策略回测功能
    """

    def __init__(self, data_dict: Dict[str, Any]):
        """
        初始化组合回测引擎

        Args:
            data_dict: 股票数据字典，键为股票代码，值为Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.data_dict = {
            stock_code: self.convert_to_polars(data)
            for stock_code, data in data_dict.items()
        }
        self.strategy_dict = {}
        self.positions = {}
        self.strategies = {}

    def set_strategy(self, stock_code: str, strategy: BaseStrategy):
        """
        为特定股票设置策略

        Args:
            stock_code: 股票代码
            strategy: 策略对象
        """
        self.strategies[stock_code] = strategy
    
    def run_backtest(self, initial_capital: float = 1000000.0, allocation_strategy: str = 'equal', lot_size: int = 100, limit_pct: float = 0.1, **params) -> Dict[str, Any]:
        """
        运行组合回测
        
        交易规则（遵循A股制度）：
        1. t日信号延迟到该股票下一交易日以开盘价成交（避免收盘价未来函数）
        2. 买入股数按lot_size（默认100股）整数倍取整，买入时即扣除买方成本
        3. 一字涨停日不可买入、一字跌停日不可卖出、停牌日（无数据或成交量为0）不可交易
        
        Args:
            initial_capital: 初始资金
            allocation_strategy: 资金分配策略，可选值：'equal'（等权分配）、'value'（价值权重）
            lot_size: 最小交易单位（股），A股买入为100股整数倍
            limit_pct: 涨跌停幅度阈值，用于识别一字板
            **params: 回测参数
            
        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategies:
            logger.error("未设置策略")
            return {}
        
        self.trades = []
        self.equity_curve = []
        self.positions = {stock: 0 for stock in self.data_dict.keys()}
        
        stock_capitals = {}
        stock_count = len(self.data_dict)
        for stock in self.data_dict.keys():
            stock_capitals[stock] = initial_capital / stock_count
        
        # 每只股票独立的待执行信号、K线索引与最新收盘价缓存
        pending_signals = {stock: None for stock in self.data_dict.keys()}
        bar_indices = {stock: 0 for stock in self.data_dict.keys()}
        last_close = {stock: None for stock in self.data_dict.keys()}
        prev_close = {stock: None for stock in self.data_dict.keys()}
        
        all_dates = set()
        for stock, data in self.data_dict.items():
            dates = data['date'].to_list()
            all_dates.update(dates)
        
        sorted_dates = sorted(all_dates)
        
        for date in sorted_dates:
            daily_trades = []
            
            for stock_code, data in self.data_dict.items():
                stock_data = data.filter(pl.col('date') == date)
                if stock_data.is_empty():
                    # 停牌（无数据）：保留挂单，持仓沿用最近收盘价估值
                    continue
                
                current_data = stock_data.to_dict(as_series=False)
                current_data = {k: v[0] for k, v in current_data.items()}
                
                open_price = current_data.get('open', current_data['close'])
                close_price = current_data['close']
                high_price = current_data.get('high')
                low_price = current_data.get('low')
                volume = current_data.get('volume')
                
                suspended = volume is not None and volume <= 0
                one_way_limit = False
                pc = prev_close[stock_code]
                # 每只股票按自身代码推导涨跌停幅度（创业板 20%、科创板 20%、主板 10% 等）
                stock_limit_pct = self._get_limit_pct(stock_code)
                if pc and pc > 0 and high_price is not None and low_price is not None and high_price == low_price:
                    pct = close_price / pc - 1
                    if pct >= stock_limit_pct - 0.005:
                        one_way_limit = 'up'
                    elif pct <= -(stock_limit_pct - 0.005):
                        one_way_limit = 'down'
                
                # 1. 执行该股票的挂单（以今日开盘价成交，订单当日有效）
                pending = pending_signals[stock_code]
                if pending == 'buy' and self.positions[stock_code] == 0:
                    if not suspended and one_way_limit != 'up':
                        cost_rate = self.cost_model.get_buy_cost_rate()
                        affordable = stock_capitals[stock_code] / (open_price * (1 + cost_rate))
                        shares = int(affordable // lot_size) * lot_size
                        if shares > 0:
                            buy_cost = self.cost_model.calculate_buy_cost(shares, open_price)
                            total_pay = shares * open_price + buy_cost
                            if total_pay <= stock_capitals[stock_code]:
                                self.positions[stock_code] = shares
                                stock_capitals[stock_code] -= total_pay
                                daily_trades.append({
                                    'date': date,
                                    'stock_code': stock_code,
                                    'signal': 'buy',
                                    'price': open_price,
                                    'shares': shares,
                                    'capital': stock_capitals[stock_code],
                                    'position': shares,
                                    'cost': buy_cost
                                })
                    pending_signals[stock_code] = None
                elif pending == 'sell' and self.positions[stock_code] > 0:
                    if not suspended and one_way_limit != 'down':
                        sell_cost = self.cost_model.calculate_sell_cost(self.positions[stock_code], open_price)
                        stock_capitals[stock_code] = self.positions[stock_code] * open_price - sell_cost
                        daily_trades.append({
                            'date': date,
                            'stock_code': stock_code,
                            'signal': 'sell',
                            'price': open_price,
                            'shares': self.positions[stock_code],
                            'capital': stock_capitals[stock_code],
                            'position': 0,
                            'cost': sell_cost
                        })
                        self.positions[stock_code] = 0
                    pending_signals[stock_code] = None
                
                # 2. 基于当日数据生成信号，延迟到下一交易日执行
                strategy = self.strategies.get(stock_code)
                if strategy:
                    signal = strategy.generate_signal(current_data, bar_indices[stock_code])
                    if signal in ('buy', 'sell'):
                        pending_signals[stock_code] = signal
                
                bar_indices[stock_code] += 1
                prev_close[stock_code] = close_price
                if not suspended:
                    last_close[stock_code] = close_price
            
            self.trades.extend(daily_trades)
            
            # 3. 组合权益 = 各股票现金 + 持仓按最近收盘价估值
            current_equity = sum(stock_capitals.values())
            for stock_code, position in self.positions.items():
                if position > 0 and last_close[stock_code] is not None:
                    current_equity += position * last_close[stock_code]
            
            self.equity_curve.append({
                'date': date,
                'equity': current_equity
            })
        
        final_equity = self.equity_curve[-1]['equity'] if self.equity_curve else initial_capital
        total_return = (final_equity - initial_capital) / initial_capital * 100
        
        performance = self.performance_analyzer.analyze(self.equity_curve, self.trades)
        
        self.backtest_results = {
            'initial_capital': initial_capital,
            'final_equity': final_equity,
            'total_return': total_return,
            'trades': self.trades,
            'equity_curve': self.equity_curve,
            'performance': performance,
            'strategy_names': {stock: strategy.name for stock, strategy in self.strategies.items()}
        }
        
        logger.info(f"组合回测完成: 总收益率 = {total_return:.2f}%")
        return self.backtest_results