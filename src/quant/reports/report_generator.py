#!/usr/bin/env python3

"""
报告生成器
"""

import os
import webbrowser
from datetime import datetime

from src.utils.logger import logger

# 回测报告静态页头（含样式），动态内容由 _generate_backtest_html 拼接
_BACKTEST_REPORT_HEAD = """<!DOCTYPE html>
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
"""

# 股票推荐报告静态页头
_RECOMMENDATION_REPORT_HEAD = """<!DOCTYPE html>
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
"""

# 因子分析报告静态页头
_FACTOR_REPORT_HEAD = """<!DOCTYPE html>
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
"""


class ReportGenerator:
    """
    报告生成器
    """

    # 各报告的最低字段要求：缺失即拒绝生成，避免导出用户拿到的只是空壳页面
    _BACKTEST_REQUIRED_FIELDS = ('initial_capital', 'final_equity', 'total_return', 'equity_curve')
    _FACTOR_RESULT_KEYS = ('factor_results', 'correlation_matrix', 'factor_combination')

    def __init__(self):
        """
        初始化报告生成器
        """
        self.reports_dir = os.path.join(os.getcwd(), "reports")
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)

    @staticmethod
    def _validate_mapping_result(result, required_fields, report_name) -> None:
        """校验 dict 结果包含关键字段；类型不符抛 TypeError，字段缺失抛 ValueError。"""
        if not isinstance(result, dict):
            raise TypeError(f"{report_name}: 结果必须为 dict，实际为 {type(result).__name__}")
        missing = [field for field in required_fields if result.get(field) is None]
        if missing:
            raise ValueError(f"{report_name}: 缺少关键字段 {missing}，拒绝生成空壳报告")

    def generate_backtest_report(self, backtest_result):
        """
        生成策略回测报告

        Args:
            backtest_result: 回测结果

        Returns:
            str: 报告文件路径
        """
        self._validate_mapping_result(
            backtest_result, self._BACKTEST_REQUIRED_FIELDS, '生成回测报告失败')
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
        if not isinstance(recommendation_result, (list, tuple)) or not recommendation_result:
            raise ValueError('生成推荐报告失败: 推荐结果为空，拒绝生成空壳报告')
        if not all(isinstance(item, dict) for item in recommendation_result):
            raise ValueError('生成推荐报告失败: 推荐结果条目必须为 dict')
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
        self._validate_mapping_result(
            factor_analysis_result, (), '生成因子分析报告失败')
        if not any(
            isinstance(factor_analysis_result.get(key), (dict, list))
            and factor_analysis_result.get(key)
            for key in self._FACTOR_RESULT_KEYS
        ):
            raise ValueError(
                '生成因子分析报告失败: factor_results / correlation_matrix / '
                'factor_combination 均为空，拒绝生成空壳报告'
            )
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
        total_return = result.get('total_return', 0)
        performance = "良好" if total_return > 0 else "不佳"
        suggestion = "继续优化参数以提高收益" if total_return > 0 else "重新评估策略逻辑"
        body = f"""
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
                <p>根据回测结果，该策略在测试期间表现{performance}。建议{suggestion}。</p>
"""
        return _BACKTEST_REPORT_HEAD + body + self._report_footer()

    @staticmethod
    def _report_footer():
        """统一报告页脚（生成时间 + 闭合标签）。"""
        return f"""
                <div class="footer">
                    <p>报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """

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
                    <td>{stock.get('risk', '中')}</td>
                </tr>
                """

        body = f"""
                <h2>推荐股票列表</h2>
                <table class="stock-table">
                    <tr>
                        <th>排名</th>
                        <th>股票代码</th>
                        <th>股票名称</th>
                        <th>行业</th>
                        <th>综合评分</th>
                        <th>风险评级</th>
                    </tr>
                    {stocks_html}
                </table>

                <h2>投资建议</h2>
                <p>根据推荐结果，建议投资者关注上述股票，并结合自身风险承受能力进行投资决策。建议分散投资，控制仓位，定期回顾和调整投资组合。</p>
"""
        return _RECOMMENDATION_REPORT_HEAD + body + self._report_footer()

    def _generate_factor_analysis_html(self, result):
        """
        生成因子分析报告的HTML内容

        Args:
            result: 因子分析结果

        Returns:
            str: HTML内容
        """
        body = f"""
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

                {self._factor_detail_sections(result)}

                <h2>结论与建议</h2>
                <p>根据因子分析结果，建议投资者关注IC值较高的因子，并结合市场环境进行因子组合配置。</p>
"""
        return _FACTOR_REPORT_HEAD + body + self._report_footer()

    @staticmethod
    def _factor_detail_sections(result) -> str:
        """按分析类型渲染结果表：有效性 / 相关性 / 组合三种结果都有真实数值。"""
        sections = ""
        factor_results = result.get('factor_results')
        if factor_results:
            rows = "".join(
                f"""
                <tr>
                    <td>{factor}</td>
                    <td>{values.get('ic', 0):.4f}</td>
                    <td>{values.get('long_return', 0):.4f}</td>
                    <td>{values.get('short_return', 0):.4f}</td>
                    <td>{values.get('long_short_return', 0):.4f}</td>
                </tr>
                """
                for factor, values in factor_results.items()
            )
            sections += f"""
                <h2>因子有效性结果</h2>
                <table class="factor-table">
                    <tr>
                        <th>因子</th>
                        <th>IC值</th>
                        <th>多头收益</th>
                        <th>空头收益</th>
                        <th>多空收益</th>
                    </tr>
                    {rows}
                </table>
"""

        correlation_matrix = result.get('correlation_matrix')
        if correlation_matrix:
            columns = result.get('factor_columns') or [
                f'因子{i + 1}' for i in range(len(correlation_matrix[0]))
            ]
            header = "".join(f"<th>{name}</th>" for name in columns)
            rows = "".join(
                "<tr>" + "".join(f"<td>{value:.4f}</td>" for value in row) + "</tr>"
                for row in correlation_matrix
            )
            sections += f"""
                <h2>因子相关性矩阵</h2>
                <table class="factor-table">
                    <tr><th></th>{header}</tr>
                    {rows}
                </table>
"""

        factor_combination = result.get('factor_combination')
        if factor_combination:
            sections += f"""
                <h2>因子组合结果</h2>
                <table class="factor-table">
                    <tr>
                        <th>IC值</th>
                        <th>多头收益</th>
                        <th>空头收益</th>
                        <th>多空收益</th>
                    </tr>
                    <tr>
                        <td>{factor_combination.get('ic', 0):.4f}</td>
                        <td>{factor_combination.get('long_return', 0):.4f}</td>
                        <td>{factor_combination.get('short_return', 0):.4f}</td>
                        <td>{factor_combination.get('long_short_return', 0):.4f}</td>
                    </tr>
                </table>
"""
        return sections
