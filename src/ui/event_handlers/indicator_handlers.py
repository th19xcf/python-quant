class IndicatorHandler:
    """
    指标事件处理器，负责处理指标切换和参数调整等事件
    """
    
    def __init__(self, main_window):
        """
        初始化指标事件处理器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def handle_indicator_changed(self, window_index, indicator_name):
        """
        处理指标切换事件
        
        Args:
            window_index: 窗口索引
            indicator_name: 新的指标名称
        """
        try:
            self.main_window.logger.info(f"切换窗口{window_index}的指标为{indicator_name}")
            
            # 更新窗口指标设置
            self.main_window.window_indicators[window_index] = indicator_name
            
            # 重新绘制K线图，更新指标显示
            if hasattr(self.main_window, 'current_stock_data') and hasattr(self.main_window, 'current_stock_name') and hasattr(self.main_window, 'current_stock_code'):
                self.main_window.plot_k_line(self.main_window.current_stock_data, self.main_window.current_stock_name, self.main_window.current_stock_code)
        except Exception as e:
            self.main_window.logger.exception(f"处理指标切换事件时发生错误: {e}")
    
    def handle_indicator_params_changed(self, indicator_name, params):
        """
        处理指标参数调整事件
        
        Args:
            indicator_name: 指标名称
            params: 新的指标参数
        """
        try:
            self.main_window.logger.info(f"调整{indicator_name}指标参数为{params}")
            
            # 这里可以添加指标参数调整逻辑
            # 例如更新指标参数设置，然后重新绘制指标
            
            # 重新绘制K线图，更新指标显示
            if hasattr(self.main_window, 'current_stock_data') and hasattr(self.main_window, 'current_stock_name') and hasattr(self.main_window, 'current_stock_code'):
                self.main_window.plot_k_line(self.main_window.current_stock_data, self.main_window.current_stock_name, self.main_window.current_stock_code)
        except Exception as e:
            self.main_window.logger.exception(f"处理指标参数调整事件时发生错误: {e}")