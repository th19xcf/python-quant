#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
分析报告生成器，用于生成详细的量化分析报告
"""

from typing import Dict, Any, List
import os
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import webbrowser


class ReportGenerator:
    """
    分析报告生成器，用于生成详细的量化分析报告
    """
    
    def __init__(self):
        """
        初始化报告生成器
        """
        pass
    
    def generate_strategy_report(self, backtest_results: Dict[str, Any], file_path: str = None) -> str:
        """
        生成策略回测报告
        
        Args:
            backtest_results: 回测结果
            file_path: 报告文件路径
            
        Returns:
            str: 生成的报告文件路径
        """
        # 生成HTML报告
        html_content = self._generate_strategy_report_html(backtest_results)
        
        # 保存报告
        if not file_path:
            file_path = f"strategy_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return file_path
    
    def _generate_strategy_report_html(self, backtest_results: Dict[str, Any]) -> str:
        """
        生成策略回测报告的HTML内容
        
        Args:
            backtest_results: 回测结果
            
        Returns:
            str: HTML内容
        """
        # 提取数据
        equity_curve = backtest_results.get('equity_curve', [])
        trades = backtest_results.get('trades', [])
        performance = backtest_results.get('performance', {})
        strategy_name = backtest_results.get('strategy_name', 'Strategy')
        initial_capital = backtest_results.get('initial_capital', 0)
        final_equity = backtest_results.get('final_equity', 0)
        total_return = backtest_results.get('total_return', 0)
        
        # 生成图表
        equity_chart = self._generate_equity_chart(equity_curve, strategy_name)
        performance_chart = self._generate_performance_chart(performance)
        trade_chart = self._generate_trade_chart(trades)
        
        # 生成HTML
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{strategy_name} 回测报告</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }}
                h1, h2, h3 {{
                    color: #333;
                }}
                .summary {{
                    background-color: #f0f8ff;
                    padding: 20px;
                    border-radius: 8px;
                    margin-bottom: 20px;
                }}
                .metrics {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .metric-card {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 8px;
                    text-align: center;
                }}
                .metric-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #007bff;
                }}
                .metric-label {{
                    font-size: 14px;
                    color: #666;
                }}
                .chart-container {{
                    margin-bottom: 30px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }}
                th, td {{
                    padding: 10px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>{strategy_name} 回测报告</h1>
                <p>生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="summary">
                    <h2>回测摘要</h2>
                    <div class="metrics">
                        <div class="metric-card">
                            <div class="metric-value">{initial_capital:,.2f}</div>
                            <div class="metric-label">初始资金</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{final_equity:,.2f}</div>
                            <div class="metric-label">最终权益</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{total_return:.2f}%</div>
                            <div class="metric-label">总收益率</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{performance.get('annual_return', 0):.2f}%</div>
                            <div class="metric-label">年化收益率</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{performance.get('sharpe_ratio', 0):.2f}</div>
                            <div class="metric-label">夏普比率</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{performance.get('max_drawdown', 0):.2f}%</div>
                            <div class="metric-label">最大回撤</div>
                        </div>
                    </div>
                </div>
                
                <h2>绩效指标</h2>
                <div class="chart-container">
                    {performance_chart}
                </div>
                
                <h2>权益曲线</h2>
                <div class="chart-container">
                    {equity_chart}
                </div>
                
                <h2>交易记录</h2>
                <div class="chart-container">
                    {trade_chart}
                </div>
                
                <h2>交易详情</h2>
                <table>
                    <tr>
                        <th>日期</th>
                        <th>信号</th>
                        <th>价格</th>
                        <th>股数</th>
                        <th>资金</th>
                        <th>持仓</th>
                    </tr>
                """
        
        # 添加交易记录
        for trade in trades:
            html += f"""
            <tr>
                <td>{trade['date']}</td>
                <td>{'买入' if trade['signal'] == 'buy' else '卖出'}</td>
                <td>{trade['price']:.2f}</td>
                <td>{trade['shares']:.2f}</td>
                <td>{trade.get('capital', 0):.2f}</td>
                <td>{trade['position']:.2f}</td>
            </tr>
            """
        
        html += f"""
                </table>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _generate_equity_chart(self, equity_curve: List[Dict[str, Any]], strategy_name: str) -> str:
        """
        生成权益曲线图表
        
        Args:
            equity_curve: 权益曲线数据
            strategy_name: 策略名称
            
        Returns:
            str: 图表HTML
        """
        if not equity_curve:
            return "<p>无数据</p>"
        
        dates = [item['date'] for item in equity_curve]
        equities = [item['equity'] for item in equity_curve]
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dates, y=equities, name='权益', line=dict(color='blue')))
        fig.update_layout(
            title=f'{strategy_name} 权益曲线',
            xaxis_title='日期',
            yaxis_title='权益',
            height=400
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='cdn')
    
    def _generate_performance_chart(self, performance: Dict[str, Any]) -> str:
        """
        生成绩效指标图表
        
        Args:
            performance: 绩效指标
            
        Returns:
            str: 图表HTML
        """
        metrics = [
            {'name': '总收益率', 'value': performance.get('total_return', 0), 'unit': '%'},
            {'name': '年化收益率', 'value': performance.get('annual_return', 0), 'unit': '%'},
            {'name': '夏普比率', 'value': performance.get('sharpe_ratio', 0), 'unit': ''},
            {'name': '最大回撤', 'value': performance.get('max_drawdown', 0), 'unit': '%'},
            {'name': '波动率', 'value': performance.get('volatility', 0), 'unit': '%'}
        ]
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=[m['name'] for m in metrics], 
                           y=[m['value'] for m in metrics],
                           text=[f"{m['value']:.2f}{m['unit']}" for m in metrics],
                           textposition='auto'))
        fig.update_layout(
            title='绩效指标',
            height=400
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='cdn')
    
    def _generate_trade_chart(self, trades: List[Dict[str, Any]]) -> str:
        """
        生成交易信号图表
        
        Args:
            trades: 交易记录
            
        Returns:
            str: 图表HTML
        """
        if not trades:
            return "<p>无交易记录</p>"
        
        buy_dates = []
        buy_prices = []
        sell_dates = []
        sell_prices = []
        
        for trade in trades:
            if trade['signal'] == 'buy':
                buy_dates.append(trade['date'])
                buy_prices.append(trade['price'])
            elif trade['signal'] == 'sell':
                sell_dates.append(trade['date'])
                sell_prices.append(trade['price'])
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=buy_dates, y=buy_prices, name='买入信号', 
                               mode='markers', marker=dict(color='green', size=10, symbol='triangle-up')))
        fig.add_trace(go.Scatter(x=sell_dates, y=sell_prices, name='卖出信号', 
                               mode='markers', marker=dict(color='red', size=10, symbol='triangle-down')))
        fig.update_layout(
            title='交易信号',
            xaxis_title='日期',
            yaxis_title='价格',
            height=400
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='cdn')
    
    def generate_alpha_factor_report(self, factor_results: Dict[str, Any], file_path: str = None) -> str:
        """
        生成Alpha因子分析报告
        
        Args:
            factor_results: 因子分析结果
            file_path: 报告文件路径
            
        Returns:
            str: 生成的报告文件路径
        """
        # 生成HTML报告
        html_content = self._generate_alpha_factor_report_html(factor_results)
        
        # 保存报告
        if not file_path:
            file_path = f"alpha_factor_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return file_path
    
    def _generate_alpha_factor_report_html(self, factor_results: Dict[str, Any]) -> str:
        """
        生成Alpha因子分析报告的HTML内容
        
        Args:
            factor_results: 因子分析结果
            
        Returns:
            str: HTML内容
        """
        # 生成HTML
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Alpha因子分析报告</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }}
                h1, h2, h3 {{
                    color: #333;
                }}
                .factor-metrics {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .factor-card {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 8px;
                }}
                .factor-name {{
                    font-size: 18px;
                    font-weight: bold;
                    margin-bottom: 10px;
                }}
                .factor-value {{
                    font-size: 20px;
                    color: #007bff;
                    margin-bottom: 5px;
                }}
                .factor-label {{
                    font-size: 14px;
                    color: #666;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }}
                th, td {{
                    padding: 10px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Alpha因子分析报告</h1>
                <p>生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <h2>因子绩效</h2>
                <div class="factor-metrics">
        """
        
        # 添加因子绩效
        for factor_name, metrics in factor_results.get('factor_metrics', {}).items():
            html += f"""
                <div class="factor-card">
                    <div class="factor-name">{factor_name}</div>
                    <div class="factor-value">{metrics.get('ic', 0):.4f}</div>
                    <div class="factor-label">IC值</div>
                    <div class="factor-value">{metrics.get('rank_ic', 0):.4f}</div>
                    <div class="factor-label">Rank IC值</div>
                    <div class="factor-value">{metrics.get('ic_ir', 0):.4f}</div>
                    <div class="factor-label">IC IR</div>
                </div>
            """
        
        html += f"""
                </div>
                
                <h2>因子相关性</h2>
                <table>
                    <tr>
                        <th>因子</th>
        """
        
        # 添加因子相关性表头
        factor_names = list(factor_results.get('factor_metrics', {}).keys())
        for factor in factor_names:
            html += f"<th>{factor}</th>"
        
        html += f"""
                    </tr>
        """
        
        # 添加因子相关性数据
        correlation_matrix = factor_results.get('correlation_matrix', {})
        for i, factor1 in enumerate(factor_names):
            html += f"<tr><td>{factor1}</td>"
            for factor2 in factor_names:
                correlation = correlation_matrix.get(factor1, {}).get(factor2, 0)
                html += f"<td>{correlation:.4f}</td>"
            html += "</tr>"
        
        html += f"""
                </table>
                
                <h2>因子分层测试</h2>
                <table>
                    <tr>
                        <th>分层</th>
                        <th>平均收益率</th>
                        <th>标准差</th>
                        <th>夏普比率</th>
                    </tr>
        """
        
        # 添加分层测试数据
        for i, layer in enumerate(factor_results.get('quantile_test', [])):
            html += f"""
            <tr>
                <td>第{i+1}层</td>
                <td>{layer.get('average_return', 0):.4f}</td>
                <td>{layer.get('std', 0):.4f}</td>
                <td>{layer.get('sharpe_ratio', 0):.4f}</td>
            </tr>
            """
        
        html += f"""
                </table>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def generate_stock_recommendation_report(self, recommendation_results: Dict[str, Any], file_path: str = None) -> str:
        """
        生成股票推荐报告
        
        Args:
            recommendation_results: 推荐结果
            file_path: 报告文件路径
            
        Returns:
            str: 生成的报告文件路径
        """
        # 生成HTML报告
        html_content = self._generate_stock_recommendation_report_html(recommendation_results)
        
        # 保存报告
        if not file_path:
            file_path = f"stock_recommendation_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return file_path
    
    def _generate_stock_recommendation_report_html(self, recommendation_results: Dict[str, Any]) -> str:
        """
        生成股票推荐报告的HTML内容
        
        Args:
            recommendation_results: 推荐结果
            
        Returns:
            str: HTML内容
        """
        # 生成HTML
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>股票推荐报告</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }}
                h1, h2, h3 {{
                    color: #333;
                }}
                .stock-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .stock-card {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 8px;
                    border-left: 4px solid #007bff;
                }}
                .stock-name {{
                    font-size: 18px;
                    font-weight: bold;
                    margin-bottom: 10px;
                }}
                .stock-score {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #007bff;
                    margin-bottom: 10px;
                }}
                .stock-metric {{
                    display: flex;
                    justify-content: space-between;
                    margin-bottom: 5px;
                }}
                .metric-label {{
                    font-size: 14px;
                    color: #666;
                }}
                .metric-value {{
                    font-size: 14px;
                    font-weight: bold;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }}
                th, td {{
                    padding: 10px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
                .signal-buy {{
                    color: green;
                    font-weight: bold;
                }}
                .signal-sell {{
                    color: red;
                    font-weight: bold;
                }}
                .risk-low {{
                    color: green;
                }}
                .risk-medium {{
                    color: orange;
                }}
                .risk-high {{
                    color: red;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>股票推荐报告</h1>
                <p>生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <h2>推荐股票</h2>
                <div class="stock-grid">
        """
        
        # 添加推荐股票
        for stock in recommendation_results.get('recommended_stocks', []):
            risk_class = 'risk-low' if stock.get('risk_level') == '低' else 'risk-medium' if stock.get('risk_level') == '中' else 'risk-high'
            signal_class = 'signal-buy' if stock.get('signal') == 'buy' else 'signal-sell'
            
            html += f"""
                <div class="stock-card">
                    <div class="stock-name">{stock.get('stock_code')} - {stock.get('stock_name')}</div>
                    <div class="stock-score">{stock.get('score', 0):.2f}</div>
                    <div class="stock-metric">
                        <span class="metric-label">信号:</span>
                        <span class="metric-value {signal_class}">{stock.get('signal', 'hold')}</span>
                    </div>
                    <div class="stock-metric">
                        <span class="metric-label">风险等级:</span>
                        <span class="metric-value {risk_class}">{stock.get('risk_level', '中')}</span>
                    </div>
                    <div class="stock-metric">
                        <span class="metric-label">行业:</span>
                        <span class="metric-value">{stock.get('industry', '未知')}</span>
                    </div>
                    <div class="stock-metric">
                        <span class="metric-label">最新价格:</span>
                        <span class="metric-value">{stock.get('current_price', 0):.2f}</span>
                    </div>
                </div>
            """
        
        html += f"""
                </div>
                
                <h2>推荐详情</h2>
                <table>
                    <tr>
                        <th>股票代码</th>
                        <th>股票名称</th>
                        <th>评分</th>
                        <th>信号</th>
                        <th>风险等级</th>
                        <th>行业</th>
                        <th>最新价格</th>
                    </tr>
        """
        
        # 添加推荐详情
        for stock in recommendation_results.get('recommended_stocks', []):
            signal_class = 'signal-buy' if stock.get('signal') == 'buy' else 'signal-sell'
            risk_class = 'risk-low' if stock.get('risk_level') == '低' else 'risk-medium' if stock.get('risk_level') == '中' else 'risk-high'
            
            html += f"""
            <tr>
                <td>{stock.get('stock_code')}</td>
                <td>{stock.get('stock_name')}</td>
                <td>{stock.get('score', 0):.2f}</td>
                <td class="{signal_class}">{stock.get('signal', 'hold')}</td>
                <td class="{risk_class}">{stock.get('risk_level', '中')}</td>
                <td>{stock.get('industry', '未知')}</td>
                <td>{stock.get('current_price', 0):.2f}</td>
            </tr>
            """
        
        html += f"""
                </table>
                
                <h2>行业分布</h2>
                <table>
                    <tr>
                        <th>行业</th>
                        <th>股票数量</th>
                        <th>平均评分</th>
                    </tr>
        """
        
        # 添加行业分布
        for industry, stats in recommendation_results.get('industry_distribution', {}).items():
            html += f"""
            <tr>
                <td>{industry}</td>
                <td>{stats.get('count', 0)}</td>
                <td>{stats.get('average_score', 0):.2f}</td>
            </tr>
            """
        
        html += f"""
                </table>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def open_report(self, file_path: str):
        """
        打开报告文件
        
        Args:
            file_path: 报告文件路径
        """
        webbrowser.open(f'file://{os.path.abspath(file_path)}')
