#!/usr/bin/env python3

"""个股数据控制器：个股K线加载、指标重算与个股行情表格展示。"""

import contextlib
from datetime import datetime

import polars as pl

from src.data.services.indicator_service import calculate_indicators_for_period
from src.ui.task_manager import global_task_manager
from src.utils.logger import logger


class StockDataController:
    """个股K线异步加载与个股行情表格展示。"""

    def __init__(self, window, scanner, renderer):
        self.window = window
        self.scanner = scanner
        self.renderer = renderer

    def _clear_kline_display(self):
        """清空K线显示区域，避免旧图表残留。"""
        try:
            if hasattr(self.window, '_clear_plots'):
                self.window._clear_plots()
        except (AttributeError, TypeError) as e:
            logger.debug(f"清空K线图表失败: {e}")

        self.window.current_kline_data = {}
        self.window.current_kline_index = -1
        self.window.current_stock_data = None
        self.window.current_stock_name = ''
        self.window.current_stock_code = ''

        if hasattr(self.window, 'ma_values_label') and self.window.ma_values_label is not None:
            self.window.ma_values_label.setText('')

        if hasattr(self.window, 'chart_title_label') and self.window.chart_title_label is not None:
            self.window.chart_title_label.setText('暂无数据')

    def process_stock_data(self, stock_code, stock_name):
        """
        处理股票数据并显示K线图

        Args:
            stock_code: 股票代码
            stock_name: 股票名称
        """
        def _process_stock_data_task(stock_code, stock_name, task_id=None, signals=None):
            """后台任务函数"""
            from datetime import timedelta

            try:
                # 计算日期范围：根据柱体数获取足够的数据
                end_date = datetime.now().strftime("%Y-%m-%d")

                # 根据柱体数动态计算日期范围
                # 假设每个交易日约 1.4 天（考虑周末和节假日）
                bar_count = getattr(self.window, 'displayed_bar_count', 100)
                # 计算需要的历史天数：柱体数 * 1.4 + 缓冲 100 天
                days_needed = int(bar_count * 1.4) + 100
                # 确保至少获取 2 年的数据
                days_needed = max(days_needed, 730)
                # 最多获取 10 年的数据
                days_needed = min(days_needed, 3650)

                start_date = (datetime.now() - timedelta(days=days_needed)).strftime("%Y-%m-%d")

                # 从数据管理器获取股票历史数据
                adjustment_type = getattr(self.window, 'adjustment_type', 'qfq')

                # 根据当前周期确定数据频率
                period = getattr(self.window, 'current_period', '日线')
                freq_map = {'日线': '1d', '周线': '1w', '月线': '1m'}
                frequency = freq_map.get(period, '1d')

                df = self.window.data_manager.get_stock_data(stock_code, start_date, end_date, frequency=frequency, adjustment_type=adjustment_type)

                if df is None or df.is_empty():
                    logger.warning(f"未获取到股票数据: {stock_name}({stock_code})")
                    return {"success": False, "message": f"未获取到 {stock_name}({stock_code}) 的数据"}

                logger.info(f"成功获取 {len(df)} 条 {stock_name}({stock_code}) 的数据")

                # 保存当前股票数据（用于柱体加减按钮）
                self.window.current_stock_data = df
                self.window.current_stock_name = stock_name
                self.window.current_stock_code = stock_code

                # 在后台线程预热分红标记缓存：绘图槽函数在 GUI 线程里取分红，
                # 避免每次重绘都同步查库导致界面卡顿
                dividend_service = getattr(self.window, 'dividend_service', None)
                if dividend_service is not None:
                    dividend_service.prefetch(stock_code)

                # 重新计算技术指标（确保数据包含所有指标列）
                df = self.window._recalculate_indicators_for_period(df)

                # 更新当前股票数据为重新计算后的数据（包含技术指标）
                self.window.current_stock_data = df

                return {"success": True, "data": df, "stock_name": stock_name, "stock_code": stock_code}
            except (OSError, RuntimeError, ValueError) as e:
                logger.exception(f"处理股票数据失败: {e}")
                return {"success": False, "message": f"处理股票数据失败: {str(e)}"}

        # 启动后台任务
        logger.info(f"启动处理股票数据任务: {stock_name}({stock_code})")
        self.window.statusBar().showMessage(f"加载 {stock_name}({stock_code}) 数据...", 0)

        # 创建任务
        global_task_manager.create_task(
            f"处理股票数据: {stock_name}({stock_code})",
            _process_stock_data_task,
            (stock_code, stock_name)
        )

        # 连接任务信号
        def on_task_completed(task_id, result):
            logger.info(f"[切换] 回调触发: {result.get('stock_code')} success={result.get('success')}")
            if result["success"]:
                # 检查是否有必要的键（股票数据）
                if "data" in result and "stock_name" in result and "stock_code" in result:
                    # 切换到图表标签页
                    if hasattr(self.window, 'tab_widget'):
                        self.window.tab_widget.setCurrentIndex(1)

                    # 绘制K线图
                    if hasattr(self.window, 'plot_k_line'):
                        logger.info(f"[切换] 调用 plot_k_line: {result['stock_name']}({result['stock_code']})")
                        self.window.plot_k_line(result["data"], result["stock_name"], result["stock_code"])

                    self.window.statusBar().showMessage(f"已加载 {result['stock_name']}({result['stock_code']}) 的K线图", 3000)
                else:
                    # 不是股票数据，不处理
                    pass
            else:
                self.window._clear_kline_display()
                self.window.statusBar().showMessage(result["message"], 5000)

        def on_task_error(task_id, error_message):
            self.window._clear_kline_display()
            self.window.statusBar().showMessage(f"加载失败: {error_message}", 5000)

        # 断开上一次的回调（避免信号累积导致同一信号触发多个回调）
        prev_completed = getattr(self.window, '_stock_data_completed_handler', None)
        if prev_completed is not None:
            with contextlib.suppress(RuntimeError, TypeError):
                global_task_manager.task_completed.disconnect(prev_completed)
        prev_error = getattr(self.window, '_stock_data_error_handler', None)
        if prev_error is not None:
            with contextlib.suppress(RuntimeError, TypeError):
                global_task_manager.task_error.disconnect(prev_error)

        # 保存本次回调引用，便于下次断开
        self.window._stock_data_completed_handler = on_task_completed
        self.window._stock_data_error_handler = on_task_error

        # 连接信号
        global_task_manager.task_completed.connect(on_task_completed)
        global_task_manager.task_error.connect(on_task_error)

    def _show_stock_data_by_type_impl(self, stock_type):
        """
        Show stock data by type (e.g. "全部A股", "上证A股", "深证A股", "创业板", "科创板")
        直接从通达信日线文件读取最新交易日的对应股票数据
        """
        def _show_stock_data_by_type_task(stock_type, task_id=None, signals=None):
            """后台任务函数"""
            try:
                return self.scanner._collect_market_rows(stock_type, task_id, signals)
            except (OSError, RuntimeError, ValueError) as e:
                logger.exception(f"显示{stock_type}数据失败: {e}")
                return {"success": False, "message": f"显示{stock_type}数据失败: {str(e)[:50]}..."}

        # 启动后台任务
        logger.info(f"启动获取{stock_type}数据任务")
        self.window.statusBar().showMessage(f"Loading {stock_type} data...", 0)

        # 显示进度条
        if hasattr(self.window, 'progress_bar'):
            self.window.market_progress_bar.setVisible(True)
            self.window.market_progress_bar.setValue(0)

        # 创建任务
        global_task_manager.create_task(
            f"获取{stock_type}数据",
            _show_stock_data_by_type_task,
            (stock_type,)
        )

        # 连接任务信号
        def on_task_progress(task_id, current, total):
            if hasattr(self.window, 'progress_bar'):
                self.window.market_progress_bar.setValue(current)

        def on_task_completed(task_id, result):
            self.renderer._render_market_result(result)

            # 隐藏进度条
            if hasattr(self.window, 'progress_bar'):
                self.window.market_progress_bar.setVisible(False)

            # 断开信号连接，避免影响其他任务
            self._disconnect_task_signals([
                (global_task_manager.task_progress, on_task_progress),
                (global_task_manager.task_completed, on_task_completed),
                (global_task_manager.task_error, on_task_error),
            ])

        def on_task_error(task_id, error_message):
            self.window._clear_market_table_display()
            self.window.statusBar().showMessage(f"显示{stock_type}数据失败: {error_message}", 5000)
            if hasattr(self.window, 'progress_bar'):
                self.window.market_progress_bar.setVisible(False)

            # 断开信号连接，避免影响其他任务
            self._disconnect_task_signals([
                (global_task_manager.task_progress, on_task_progress),
                (global_task_manager.task_completed, on_task_completed),
                (global_task_manager.task_error, on_task_error),
            ])

        # 连接信号
        global_task_manager.task_progress.connect(on_task_progress)
        global_task_manager.task_completed.connect(on_task_completed)
        global_task_manager.task_error.connect(on_task_error)

    @staticmethod
    def _disconnect_task_signals(handlers):
        """统一断开任务信号，忽略已断开引发的 RuntimeError。"""
        for signal, handler in handlers:
            with contextlib.suppress(RuntimeError):
                signal.disconnect(handler)

    def _recalculate_indicators_for_period(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        为周线或月线数据重新计算技术指标

        Args:
            df: 周线或月线数据（只有基础OHLCV数据）

        Returns:
            pl.DataFrame: 包含技术指标的数据
        """
        try:
            result_pl = calculate_indicators_for_period(df)
            logger.info(f"为{df.height}条数据重新计算了技术指标")
            return result_pl

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"重新计算技术指标失败: {e}")
            return df

    def _show_hs_aj_stock_data_impl(self):
        self.window._show_stock_data_by_type_impl("全部A股")

    def _refresh_stock_data_impl(self):
         logger.info("Refresh requested")
