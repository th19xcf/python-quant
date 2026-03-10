#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
报告生成器
"""

import os
import webbrowser
from datetime import datetime

from src.utils.logger import logger


class ReportGenerator:
    """
    报告生成器
    """
    
    def __init__(self):
        """
        初始化报告生成器
        """
        self.reports_dir = os.path.join(os.getcwd(), "reports")
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)
    
    def generate_backtest_report(self, backtest_result):
        """
        生成策略回测报告
        
        Args:
            backtest_result: 回测结果
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 生成报告文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"backtest_report_{timestamp}.html"
            report_path = os.path.join(self.reports_dir, report_filename)
            
            # 生成HTML报告
            html_content = self._generate_backtest_html(backtest_result)
            
            # 写入文件
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # 打开浏览器预览
            webbrowser.open(f"file://{report_path}")
            
            logger.info(f"回测报告生成成功: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"生成回测报告失败: {e}")
            raise
    
    def generate_recommendation_report(self, recommendation_result):
        """
        生成股票推荐报告
        
        Args:
            recommendation_result: 推荐结果
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 生成报告文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"recommendation_report_{timestamp}.html"
            report_path = os.path.join(self.reports_dir, report_filename)
            
            # 生成HTML报告
            html_content = self._generate_recommendation_html(recommendation_result)
            
            # 写入文件
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # 打开浏览器预览
            webbrowser.open(f"file://{report_path}")
            
            logger.info(f"推荐报告生成成功: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"生成推荐报告失败: {e}")
            raise
    
    def generate_factor_analysis_report(self, factor_analysis_result):
        """
        生成因子分析报告
        
        Args:
            factor_analysis_result: 因子分析结果
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 生成报告文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"factor_analysis_report_{timestamp}.html"
            report_path = os.path.join(self.reports_dir, report_filename)
            
            # 生成HTML报告
            html_content = self._generate_factor_analysis_html(factor_analysis_result)
            
            # 写入文件
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # 打开浏览器预览
            webbrowser.open(f"file://{report_path}")
            
            logger.info(f"因子分析报告生成成功: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"生成因子分析报告失败: {e}")
            raise
    
    def _generate_backtest_html(self, result):
        """
        生成回测报告的HTML内容
        
        Args:
            result: 回测结果
            
        Returns:
            str: HTML内容
        """
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>策略回测报告</title>
            <style>
                body {
                    font-family: 'Microsoft YaHei', Arial, sans-serif;
                    background-color: #f5f5f5;
                    margin: 0;
                    padding: 20px;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 30px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }
                h1 {
                    color: #333;
                    text-align: center;
                    border-bottom: 2px solid #0066cc;
                    padding-bottom: 10px;
                }
                h2 {
                    color: #555;
                    margin-top: 30px;
                    border-left: 4px solid #0066cc;
                    padding-left: 10px;
                }
                .info-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                .info-table th, .info-table td {
                    border: 1px solid #ddd;
                    padding: 10px;
                    text-align: left;
                }
                .info-table th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                .metrics {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin: 20px 0;
                }
                .metric-card {
                    background-color: #f9f9f9;
                    padding: 20px;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .metric-card h3 {
                    margin-top: 0;
                    color: #333;
                }
                .metric-value {
                    font-size: 24px;
                    font-weight: bold;
                    color: #0066cc;
                }
                .footer {
                    margin-top: 40px;
                    text-align: center;
                    color: #666;
                    font-size: 14px;
                    border-top: 1px solid #ddd;
                    padding-top: 20px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>策略回测报告</h1>
                
                <h2>基本信息</h2>
                <table class="info-table">
                    <tr>
                        <th>策略类型</th>
                        <td>{result.get('strategy_type', '未知')}</td>
                    </tr>
                    <tr>
                        <th>股票代码</th>
                        <td>{result.get('stock_code', '未知')}</td>
                    </tr>
                    <tr>
                        <th>回测周期</th>
                        <td>{result.get('start_date', '未知')} 至 {result.get('end_date', '未知')}</td>
                    </tr>
                    <tr>
                        <th>初始资金</th>
                        <td>{result.get('initial_capital', 0):,.2f} 元</td>
                    </tr>
                </table>
                
                <h2>绩效指标</h2>
                <div class="metrics">
                    <div class="metric-card">
                        <h3>最终资金</h3>
                        <div class="metric-value">{result.get('final_capital', 0):,.2f} 元</div>
                    </div>
                    <div class="metric-card">
                        <h3>总收益率</h3>
                        <div class="metric-value">{result.get('total_return', 0):.2f}%</div>
                    </div>
                    <div class="metric-card">
                        <h3>年化收益率</h3>
                        <div class="metric-value">{result.get('annual_return', 0):.2f}%</div>
                    </div>
                    <div class="metric-card">
                        <h3>夏普比率</h3>
                        <div class="metric-value">{result.get('sharpe_ratio', 0):.2f}</div>
                    </div>
                    <div class="metric-card">
                        <h3>最大回撤</h3>
                        <div class="metric-value">{result.get('max_drawdown', 0):.2f}%</div>
                    </div>
                    <div class="metric-card">
                        <h3>交易次数</h3>
                        <div class="metric-value">{result.get('trade_count', 0)}</div>
                    </div>
                    <div class="metric-card">
                        <h3>胜率</h3>
                        <div class="metric-value">{result.get('win_rate', 0):.2f}%</div>
                    </div>
                </div>
                
                <h2>结论与建议</h2>
                <p>根据回测结果，该策略在测试期间表现{"良好" if result.get('total_return', 0) > 0 else "不佳"}。建议{"继续优化参数以提高收益" if result.get('total_return', 0) > 0 else "重新评估策略逻辑"}。</p>
                
                <div class="footer">
                    <p>报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html
    
    def _generate_recommendation_html(self, result):
        """
        生成推荐报告的HTML内容
        
        Args:
            result: 推荐结果
            
        Returns:
            str: HTML内容
        """
        # 生成推荐股票列表
        stocks_html = ""
        if isinstance(result, list):
            for i, stock in enumerate(result):
                stocks_html += f"""
                <tr>
                    <td>{i + 1}</td>
                    <td>{stock.get('code', '')}</td>
                    <td>{stock.get('name', '')}</td>
                    <td>{stock.get('industry', '')}</td>
                    <td>{stock.get('score', 0):.2f}</td>
                    <td>{stock.get('expected_return', 0):.2f}%</td>
                    <td>{stock.get('risk', '中')}</td>
                </tr>
                """
        
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>股票推荐报告</title>
            <style>
                body {
                    font-family: 'Microsoft YaHei', Arial, sans-serif;
                    background-color: #f5f5f5;
                    margin: 0;
                    padding: 20px;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 30px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }
                h1 {
                    color: #333;
                    text-align: center;
                    border-bottom: 2px solid #0066cc;
                    padding-bottom: 10px;
                }
                h2 {
                    color: #555;
                    margin-top: 30px;
                    border-left: 4px solid #0066cc;
                    padding-left: 10px;
                }
                .stock-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                .stock-table th, .stock-table td {
                    border: 1px solid #ddd;
                    padding: 10px;
                    text-align: left;
                }
                .stock-table th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                .stock-table tr:hover {
                    background-color: #f5f5f5;
                }
                .footer {
                    margin-top: 40px;
                    text-align: center;
                    color: #666;
                    font-size: 14px;
                    border-top: 1px solid #ddd;
                    padding-top: 20px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>股票推荐报告</h1>
                
                <h2>推荐股票列表</h2>
                <table class="stock-table">
                    <tr>
                        <th>排名</th>
                        <th>股票代码</th>
                        <th>股票名称</th>
                        <th>行业</th>
                        <th>综合评分</th>
                        <th>预期收益</th>
                        <th>风险评级</th>
                    </tr>
                    {stocks_html}
                </table>
                
                <h2>投资建议</h2>
                <p>根据推荐结果，建议投资者关注上述股票，并结合自身风险承受能力进行投资决策。建议分散投资，控制仓位，定期回顾和调整投资组合。</p>
                
                <div class="footer">
                    <p>报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html
    
    def _generate_factor_analysis_html(self, result):
        """
        生成因子分析报告的HTML内容
        
        Args:
            result: 因子分析结果
            
        Returns:
            str: HTML内容
        """
        # 生成因子结果表格
        factors_html = ""
        if 'factor_results' in result:
            for factor, factor_result in result['factor_results'].items():
                factors_html += f"""
                <tr>
                    <td>{factor}</td>
                    <td>{factor_result.get('ic', 0):.4f}</td>
                    <td>{factor_result.get('long_return', 0):.4f}</td>
                    <td>{factor_result.get('short_return', 0):.4f}</td>
                    <td>{factor_result.get('long_short_return', 0):.4f}</td>
                </tr>
                """
        
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>因子分析报告</title>
            <style>
                body {
                    font-family: 'Microsoft YaHei', Arial, sans-serif;
                    background-color: #f5f5f5;
                    margin: 0;
                    padding: 20px;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 30px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }
                h1 {
                    color: #333;
                    text-align: center;
                    border-bottom: 2px solid #0066cc;
                    padding-bottom: 10px;
                }
                h2 {
                    color: #555;
                    margin-top: 30px;
                    border-left: 4px solid #0066cc;
                    padding-left: 10px;
                }
                .info-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                .info-table th, .info-table td {
                    border: 1px solid #ddd;
                    padding: 10px;
                    text-align: left;
                }
                .info-table th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                .factor-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                .factor-table th, .factor-table td {
                    border: 1px solid #ddd;
                    padding: 10px;
                    text-align: left;
                }
                .factor-table th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                .footer {
                    margin-top: 40px;
                    text-align: center;
                    color: #666;
                    font-size: 14px;
                    border-top: 1px solid #ddd;
                    padding-top: 20px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>因子分析报告</h1>
                
                <h2>基本信息</h2>
                <table class="info-table">
                    <tr>
                        <th>分析类型</th>
                        <td>{result.get('analysis_type', '未知')}</td>
                    </tr>
                    <tr>
                        <th>股票代码</th>
                        <td>{result.get('stock_code', '未知')}</td>
                    </tr>
                    <tr>
                        <th>分析周期</th>
                        <td>{result.get('start_date', '未知')} 至 {result.get('end_date', '未知')}</td>
                    </tr>
                    <tr>
                        <th>分析因子</th>
                        <td>{', '.join(result.get('factors', []))}</td>
                    </tr>
                </table>
                
                <h2>因子分析结果</h2>
                <table class="factor-table">
                    <tr>
                        <th>因子</th>
                        <th>IC值</th>
                        <th>多头收益</th>
                        <th>空头收益</th>
                        <th>多空收益</th>
                    </tr>
                    {factors_html}
                </table>
                
                <h2>结论与建议</h2>
                <p>根据因子分析结果，建议投资者关注IC值较高的因子，并结合市场环境进行因子组合配置。</p>
                
                <div class="footer">
                    <p>报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html
