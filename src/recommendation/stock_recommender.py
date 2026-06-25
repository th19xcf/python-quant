#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票推荐器，用于基于多因子模型推荐股票
支持多种推荐算法：多因子选股、行业轮动、动量策略
"""

from typing import Dict, Any, List
import polars as pl
import numpy as np
from loguru import logger


class StockRecommender:
    """
    股票推荐器类，提供股票评分和推荐功能
    """
    
    def __init__(self):
        """
        初始化股票推荐器
        """
        self.recommendations = []
    
    def recommend_stocks(self, stocks_data: pl.DataFrame, top_n: int = 10, algorithm: str = "多因子选股", 
                        industry: str = "全部行业", weights: Dict[str, float] = None, **params) -> List[Dict[str, Any]]:
        """
        推荐股票
        
        Args:
            stocks_data: 股票数据
            top_n: 推荐数量
            algorithm: 推荐算法，可选值：多因子选股、行业轮动、动量策略
            industry: 行业过滤，"全部行业"表示不过滤
            weights: 因子权重字典
            **params: 其他推荐参数
            
        Returns:
            List[Dict[str, Any]]: 推荐结果
        """
        if stocks_data.is_empty():
            logger.warning("没有股票数据")
            return []
        
        if weights is None:
            weights = {
                'momentum': 0.2,
                'value': 0.2,
                'growth': 0.2,
                'quality': 0.2,
                'volatility': 0.2
            }
        
        if industry != "全部行业":
            stocks_data = stocks_data.filter(pl.col('industry') == industry)
            if stocks_data.is_empty():
                logger.warning(f"没有{industry}行业的股票数据")
                return []
        
        stock_groups = stocks_data.group_by('stock_code')
        
        scored_stocks = []
        
        for stock_code, group in stock_groups:
            latest_data = group.sort('date').tail(1)
            if latest_data.is_empty():
                continue
            
            latest_data_dict = latest_data.to_dict(as_series=False)
            latest_data = {}
            for col, values in latest_data_dict.items():
                if values:
                    latest_data[col] = values[0]
            
            prices = group.sort('date')['close'].to_numpy()
            if len(prices) < 20:
                continue
            
            if algorithm == "动量策略":
                score, risk_score = self._calculate_momentum_score(prices, group, params)
            elif algorithm == "行业轮动":
                score, risk_score = self._calculate_industry_rotation_score(prices, group, params)
            else:
                score, risk_score = self._calculate_multi_factor_score(prices, group, weights, params)
            
            risk_adjusted_score = score * (1 - risk_score / 100)
            signal = self._generate_signal(risk_adjusted_score, score, risk_score)
            risk_level = '低' if risk_score < 30 else '中' if risk_score < 60 else '高'
            
            scored_stocks.append({
                'stock_code': stock_code,
                'stock_name': latest_data.get('stock_name', f'股票{stock_code}'),
                'score': score,
                'risk_score': risk_score,
                'risk_adjusted_score': risk_adjusted_score,
                'signal': signal,
                'risk_level': risk_level,
                'current_price': latest_data.get('close', 0),
                'industry': latest_data.get('industry', '未知'),
                'expected_return': score * 100
            })
        
        scored_stocks.sort(key=lambda x: x['risk_adjusted_score'], reverse=True)
        self.recommendations = scored_stocks[:top_n]
        
        logger.info(f"推荐完成，共推荐 {len(self.recommendations)} 只股票")
        return self.recommendations
    
    def _calculate_multi_factor_score(self, prices: np.ndarray, group: pl.DataFrame, 
                                     weights: Dict[str, float], params: Dict[str, Any]) -> tuple:
        """
        多因子评分
        
        Args:
            prices: 价格数组
            group: 股票数据分组
            weights: 因子权重
            params: 参数
            
        Returns:
            tuple: (score, risk_score)
        """
        momentum_10 = (prices[-1] - prices[-10]) / prices[-10] if len(prices) >= 10 else 0
        momentum_20 = (prices[-1] - prices[-20]) / prices[-20] if len(prices) >= 20 else 0
        volatility = np.std(prices[-20:]) / np.mean(prices[-20:]) if len(prices) >= 20 else 0
        
        volumes = group.sort('date')['volume'].to_numpy()
        volume_change = (volumes[-1] - volumes[-10]) / volumes[-10] if len(volumes) >= 10 else 0
        
        returns = np.diff(prices) / prices[:-1]
        sharpe_ratio = np.mean(returns) / (np.std(returns) + 1e-10) if len(returns) > 1 else 0
        
        score = (
            (momentum_10 + momentum_20) * weights.get('momentum', 0.2) +
            (1 - volatility) * weights.get('volatility', 0.2) +
            volume_change * weights.get('value', 0.2) +
            sharpe_ratio * weights.get('quality', 0.2) +
            np.mean(returns) * weights.get('growth', 0.2)
        )
        score = max(0, min(1, score))
        
        risk_score = min(100, volatility * 200)
        return score, risk_score
    
    def _calculate_momentum_score(self, prices: np.ndarray, group: pl.DataFrame, 
                                  params: Dict[str, Any]) -> tuple:
        """
        动量策略评分
        
        Args:
            prices: 价格数组
            group: 股票数据分组
            params: 参数
            
        Returns:
            tuple: (score, risk_score)
        """
        period = params.get('period', 20)
        if len(prices) < period:
            period = len(prices)
        
        momentum = (prices[-1] - prices[-period]) / prices[-period] if period > 0 else 0
        
        volatility = np.std(prices[-period:]) / np.mean(prices[-period:]) if period > 1 else 0
        
        score = max(0, min(1, momentum * 2))
        
        risk_score = min(100, volatility * 200)
        return score, risk_score
    
    def _calculate_industry_rotation_score(self, prices: np.ndarray, group: pl.DataFrame, 
                                          params: Dict[str, Any]) -> tuple:
        """
        行业轮动策略评分
        
        Args:
            prices: 价格数组
            group: 股票数据分组
            params: 参数
            
        Returns:
            tuple: (score, risk_score)
        """
        momentum_20 = (prices[-1] - prices[-20]) / prices[-20] if len(prices) >= 20 else 0
        momentum_60 = (prices[-1] - prices[-60]) / prices[-60] if len(prices) >= 60 else 0
        
        volatility = np.std(prices[-20:]) / np.mean(prices[-20:]) if len(prices) >= 20 else 0
        
        volumes = group.sort('date')['volume'].to_numpy()
        volume_momentum = (volumes[-5:] - volumes[-10:-5]).mean() / (volumes[-10:-5].mean() + 1e-10) if len(volumes) >= 10 else 0
        
        score = momentum_20 * 0.4 + momentum_60 * 0.3 + volume_momentum * 0.3
        score = max(0, min(1, score + 0.5))
        
        risk_score = min(100, volatility * 200)
        return score, risk_score
    
    def _generate_signal(self, risk_adjusted_score: float, score: float, risk_score: float) -> str:
        """
        生成推荐信号
        
        Args:
            risk_adjusted_score: 风险调整后的评分
            score: 原始评分
            risk_score: 风险评分
            
        Returns:
            str: 推荐信号
        """
        if risk_adjusted_score > 0.7:
            return 'buy'
        elif risk_adjusted_score > 0.3:
            return 'hold'
        else:
            return 'sell'
    
    def get_recommendations(self) -> List[Dict[str, Any]]:
        """
        获取推荐结果
        
        Returns:
            List[Dict[str, Any]]: 推荐结果
        """
        return self.recommendations
    
    def visualize_recommendations(self) -> Any:
        """
        可视化推荐结果
        
        Returns:
            Any: 可视化结果
        """
        if not self.recommendations:
            logger.error("尚未生成推荐")
            return None
        
        # 简单的可视化实现
        import plotly.graph_objects as go
        
        stock_codes = [rec['stock_code'] for rec in self.recommendations]
        scores = [rec['risk_adjusted_score'] for rec in self.recommendations]
        risk_scores = [rec['risk_score'] for rec in self.recommendations]
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=stock_codes, y=scores, name='风险调整评分'))
        fig.add_trace(go.Scatter(x=stock_codes, y=risk_scores, name='风险评分', yaxis='y2'))
        
        fig.update_layout(
            title='股票推荐结果',
            yaxis_title='风险调整评分',
            yaxis2=dict(title='风险评分', overlaying='y', side='right'),
            height=400
        )
        
        return fig
    
    def generate_recommendation_report(self) -> Dict[str, Any]:
        """
        生成推荐报告
        
        Returns:
            Dict[str, Any]: 推荐报告
        """
        if not self.recommendations:
            logger.error("尚未生成推荐")
            return {}
        
        # 统计信息
        buy_signals = sum(1 for rec in self.recommendations if rec['signal'] == 'buy')
        sell_signals = sum(1 for rec in self.recommendations if rec['signal'] == 'sell')
        hold_signals = sum(1 for rec in self.recommendations if rec['signal'] == 'hold')
        
        # 平均评分
        avg_score = np.mean([rec['score'] for rec in self.recommendations])
        avg_risk_score = np.mean([rec['risk_score'] for rec in self.recommendations])
        avg_risk_adjusted_score = np.mean([rec['risk_adjusted_score'] for rec in self.recommendations])
        
        # 按行业分类
        industry_stats = {}
        for rec in self.recommendations:
            industry = rec.get('industry', '未知')
            if industry not in industry_stats:
                industry_stats[industry] = {
                    'count': 0,
                    'average_score': 0
                }
            industry_stats[industry]['count'] += 1
            industry_stats[industry]['average_score'] += rec['score']
        
        # 计算行业平均
        for industry, stats in industry_stats.items():
            stats['average_score'] /= stats['count']
        
        return {
            'recommended_stocks': self.recommendations,
            'industry_distribution': industry_stats,
            'total_recommendations': len(self.recommendations),
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'hold_signals': hold_signals,
            'average_score': avg_score,
            'average_risk_score': avg_risk_score,
            'average_risk_adjusted_score': avg_risk_adjusted_score
        }
