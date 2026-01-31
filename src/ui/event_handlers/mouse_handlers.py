import pyqtgraph as pg
import numpy as np

from src.utils.logger import logger

class MouseHandler:
    """
    鼠标事件处理器，负责处理鼠标相关事件
    """
    
    def __init__(self, main_window):
        """
        初始化鼠标事件处理器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def _format_value(self, value, format_str="{:.2f}"):
        """
        格式化指标值，处理 None 值
        
        Args:
            value: 要格式化的值
            format_str: 格式字符串
            
        Returns:
            str: 格式化后的字符串，如果值为 None 则返回 "--"
        """
        if value is None:
            return "--"
        try:
            return format_str.format(value)
        except (TypeError, ValueError):
            return "--"
    
    def handle_mouse_moved(self, pos, dates, opens, highs, lows, closes):
        """
        处理鼠标移动事件
        
        Args:
            pos: 鼠标位置
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        try:
            # 获取三个图表的视图框
            tech_view_box = self.main_window.tech_plot_widget.getViewBox()
            volume_view_box = self.main_window.volume_plot_widget.getViewBox()
            kdj_view_box = self.main_window.kdj_plot_widget.getViewBox()
            
            # 获取三个图表在场景中的区域
            tech_scene_rect = tech_view_box.viewRect()
            volume_scene_rect = volume_view_box.viewRect()
            kdj_scene_rect = kdj_view_box.viewRect()
            
            # 初始化x_val为无效值
            x_val = -1
            
            # 检查鼠标在哪个图表上，并获取对应的x_val
            # 检查K线图
            tech_pos = tech_view_box.mapSceneToView(pos)
            if 0 <= tech_pos.x() < len(dates):
                x_val = tech_pos.x()
            # 检查成交量图
            else:
                volume_pos = volume_view_box.mapSceneToView(pos)
                if 0 <= volume_pos.x() < len(dates):
                    x_val = volume_pos.x()
                # 检查KDJ图
                else:
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    if 0 <= kdj_pos.x() < len(dates):
                        x_val = kdj_pos.x()
            
            # 找到最接近的K线索引
            index = int(round(x_val))
            if 0 <= index < len(dates):
                # 保存当前鼠标位置和K线索引
                self.main_window.current_mouse_pos = pos
                self.main_window.current_kline_index = index
                
                # 保存当前K线数据
                self.main_window.current_kline_data = {
                    'dates': dates,
                    'opens': opens,
                    'highs': highs,
                    'lows': lows,
                    'closes': closes,
                    'index': index
                }
                
                # 更新顶部均线值显示
                self.main_window.update_ma_values_display(index, dates, opens, highs, lows, closes)
                
                # 更新第二个窗口标签，根据当前指标类型显示不同内容
                if hasattr(self.main_window, 'volume_values_label'):
                    # 获取当前窗口指标
                    current_indicator = self.main_window.window_indicators[2]
                    
                    if current_indicator == "VOL" and hasattr(self.main_window, 'current_volume_data') and 0 <= index < len(self.main_window.current_volume_data['volume']):
                        # 更新成交量标签
                        current_volume = self.main_window.current_volume_data['volume'][index]
                        
                        # 检查vol_ma5和vol_ma10列表是否足够长
                        current_vol_ma5 = self.main_window.current_volume_data['vol_ma5'][index] if index < len(self.main_window.current_volume_data['vol_ma5']) else 0
                        current_vol_ma10 = self.main_window.current_volume_data['vol_ma10'][index] if index < len(self.main_window.current_volume_data['vol_ma10']) else 0
                        
                        self.main_window.volume_values_label.setText(f"<font color='#C0C0C0'>VOLUME: {int(current_volume):,}</font>  <font color='white'>MA5: {int(current_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(current_vol_ma10):,}</font>")
                
                # 更新第3窗口标签，根据当前指标类型显示不同内容
                if hasattr(self.main_window, 'kdj_values_label'):
                    # 获取当前第3窗口指标
                    current_indicator = self.main_window.window_indicators[3]
                    
                    if current_indicator == "KDJ" and hasattr(self.main_window, 'current_kdj_data') and 0 <= index < len(self.main_window.current_kdj_data['k']):
                        current_k = self.main_window.current_kdj_data['k'][index]
                        current_d = self.main_window.current_kdj_data['d'][index]
                        current_j = self.main_window.current_kdj_data['j'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='white'>K: {self._format_value(current_k)}</font>  <font color='yellow'>D: {self._format_value(current_d)}</font>  <font color='magenta'>J: {self._format_value(current_j)}</font>")
                    elif current_indicator == "RSI" and hasattr(self.main_window, 'current_rsi_data') and 0 <= index < len(self.main_window.current_rsi_data['rsi']):
                        current_rsi = self.main_window.current_rsi_data['rsi'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='blue'>RSI14: {self._format_value(current_rsi)}</font>")
                    elif current_indicator == "MACD" and hasattr(self.main_window, 'current_macd_data') and 0 <= index < len(self.main_window.current_macd_data['macd']):
                        current_macd = self.main_window.current_macd_data['macd'][index]
                        current_macd_signal = self.main_window.current_macd_data['macd_signal'][index]
                        current_macd_hist = self.main_window.current_macd_data['macd_hist'][index]
                        macd_hist_color = '#FF0000' if current_macd_hist >= 0 else '#00FF00'
                        self.main_window.kdj_values_label.setText(f"<font color='#FFFFFF'>DIF: {self._format_value(current_macd)}</font>  <font color='#FFFF00'>DEA: {self._format_value(current_macd_signal)}</font>  <font color='{macd_hist_color}'>MACD: {self._format_value(current_macd_hist)}</font>")
                    elif current_indicator == "WR" and hasattr(self.main_window, 'current_wr_data') and 0 <= index < len(self.main_window.current_wr_data['wr1']):
                        current_wr1 = self.main_window.current_wr_data['wr1'][index]
                        current_wr2 = self.main_window.current_wr_data['wr2'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='white'>WR(10,6) </font><font color='yellow'>WR1: {self._format_value(current_wr1)}</font>  <font color='white'>WR2: {self._format_value(current_wr2)}</font>")
                    elif current_indicator == "BOLL" and hasattr(self.main_window, 'current_boll_data') and 0 <= index < len(self.main_window.current_boll_data['mb']):
                        current_mb = self.main_window.current_boll_data['mb'][index]
                        current_up = self.main_window.current_boll_data['up'][index]
                        current_dn = self.main_window.current_boll_data['dn'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='white'>MB: {self._format_value(current_mb)}</font>  <font color='red'>UP: {self._format_value(current_up)}</font>  <font color='#00FF00'>DN: {self._format_value(current_dn)}</font>")
                    elif current_indicator == "DMI" and hasattr(self.main_window, 'current_dmi_data') and 0 <= index < len(self.main_window.current_dmi_data['pdi']):
                        current_pdi = self.main_window.current_dmi_data['pdi'][index]
                        current_ndi = self.main_window.current_dmi_data['ndi'][index]
                        current_adx = self.main_window.current_dmi_data['adx'][index]
                        current_adxr = self.main_window.current_dmi_data['adxr'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='#FFFFFF'>PDI: {self._format_value(current_pdi)}</font>  <font color='#FFFF00'>NDI: {self._format_value(current_ndi)}</font>  <font color='#FF00FF'>ADX: {self._format_value(current_adx)}</font>  <font color='#00FF00'>ADXR: {self._format_value(current_adxr)}</font>")
                    elif current_indicator == "TRIX" and hasattr(self.main_window, 'current_trix_data') and 0 <= index < len(self.main_window.current_trix_data['trix']):
                        current_trix = self.main_window.current_trix_data['trix'][index]
                        current_trma = self.main_window.current_trix_data['trma'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='#FFFFFF'>TRIX: {self._format_value(current_trix)}</font>  <font color='#FFFF00'>MATRIX: {self._format_value(current_trma)}</font>")
                    elif current_indicator == "BRAR" and hasattr(self.main_window, 'current_brar_data') and 0 <= index < len(self.main_window.current_brar_data['br']):
                        current_br = self.main_window.current_brar_data['br'][index]
                        current_ar = self.main_window.current_brar_data['ar'][index]
                        self.main_window.kdj_values_label.setText(f"<font color='#FFFF00'>BR: {self._format_value(current_br)}</font>  <font color='#FFFFFF'>AR: {self._format_value(current_ar)}</font>")
                    elif current_indicator == "VR" and hasattr(self.main_window, 'current_vr_data') and 0 <= index < len(self.main_window.current_vr_data['vr']):
                        current_vr = self.main_window.current_vr_data['vr'][index]
                        current_mavr = self.main_window.current_vr_data['mavr'][index] if index < len(self.main_window.current_vr_data['mavr']) else 0
                        self.main_window.kdj_values_label.setText(f"<font color='#FFFFFF'>VR: {self._format_value(current_vr)}</font>  <font color='#FFFF00'>MAVR: {self._format_value(current_mavr)}</font>")
                
                # 更新第二个窗口标签（重复检查，确保所有情况下都正确显示）
                if hasattr(self.main_window, 'volume_values_label'):
                    # 获取当前窗口指标
                    current_indicator = self.main_window.window_indicators[2]
                    
                    if current_indicator == "VOL" and hasattr(self.main_window, 'current_volume_data') and 0 <= index < len(self.main_window.current_volume_data['volume']):
                        # 更新成交量标签
                        current_volume = self.main_window.current_volume_data['volume'][index]
                        
                        # 检查vol_ma5和vol_ma10列表是否足够长
                        current_vol_ma5 = self.main_window.current_volume_data['vol_ma5'][index] if index < len(self.main_window.current_volume_data['vol_ma5']) else 0
                        current_vol_ma10 = self.main_window.current_volume_data['vol_ma10'][index] if index < len(self.main_window.current_volume_data['vol_ma10']) else 0
                        
                        self.main_window.volume_values_label.setText(f"<font color='#C0C0C0'>VOLUME: {int(current_volume):,}</font>  <font color='white'>MA5: {int(current_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(current_vol_ma10):,}</font>")
                    elif current_indicator == "MACD" and hasattr(self.main_window, 'current_macd_data') and 0 <= index < len(self.main_window.current_macd_data['macd']):
                        # 更新MACD标签，使用通达信风格：DIF白色，DEA黄色，MACD根据正负值变色
                        current_macd = self.main_window.current_macd_data['macd'][index]
                        current_macd_signal = self.main_window.current_macd_data['macd_signal'][index]
                        current_macd_hist = self.main_window.current_macd_data['macd_hist'][index]
                        macd_hist_color = '#FF0000' if current_macd_hist >= 0 else '#00FF00'
                        self.main_window.volume_values_label.setText(f"<font color='#FFFFFF'>DIF: {current_macd:.2f}</font>  <font color='#FFFF00'>DEA: {current_macd_signal:.2f}</font>  <font color='{macd_hist_color}'>MACD: {current_macd_hist:.2f}</font>")
                    elif current_indicator == "RSI" and hasattr(self.main_window, 'current_rsi_data') and 0 <= index < len(self.main_window.current_rsi_data['rsi']):
                        # 更新RSI标签
                        current_rsi = self.main_window.current_rsi_data['rsi'][index]
                        self.main_window.volume_values_label.setText(f"<font color='blue'>RSI14: {current_rsi:.2f}</font>")
                    elif current_indicator == "KDJ" and hasattr(self.main_window, 'current_kdj_data') and 0 <= index < len(self.main_window.current_kdj_data['k']):
                        # 更新KDJ标签
                        current_k = self.main_window.current_kdj_data['k'][index]
                        current_d = self.main_window.current_kdj_data['d'][index]
                        current_j = self.main_window.current_kdj_data['j'][index]
                        self.main_window.volume_values_label.setText(f"<font color='white'>K: {current_k:.2f}</font>  <font color='yellow'>D: {current_d:.2f}</font>  <font color='magenta'>J: {current_j:.2f}</font>")
                    elif current_indicator == "WR" and hasattr(self.main_window, 'current_wr_data') and 0 <= index < len(self.main_window.current_wr_data['wr1']):
                        # 更新WR标签，颜色与图中指标一致（WR1黄色，WR2白色）
                        current_wr1 = self.main_window.current_wr_data['wr1'][index]
                        current_wr2 = self.main_window.current_wr_data['wr2'][index]
                        self.main_window.volume_values_label.setText(f"<font color='white'>WR(10,6) </font><font color='yellow'>WR1: {current_wr1:.2f}</font> <font color='white'>WR2: {current_wr2:.2f}</font>")
                    elif current_indicator == "BOLL" and hasattr(self.main_window, 'current_boll_data') and 0 <= index < len(self.main_window.current_boll_data['mb']):
                        # 更新BOLL标签，颜色与图中指标一致（中轨白色，上轨红色，下轨绿色）
                        current_mb = self.main_window.current_boll_data['mb'][index]
                        current_up = self.main_window.current_boll_data['up'][index]
                        current_dn = self.main_window.current_boll_data['dn'][index]
                        self.main_window.volume_values_label.setText(f"<font color='white'>MB: {current_mb:.2f}</font>  <font color='red'>UP: {current_up:.2f}</font>  <font color='#00FF00'>DN: {current_dn:.2f}</font>")
            
            # 如果十字线功能启用，更新十字线位置和信息框
            if self.main_window.crosshair_enabled and 0 <= index < len(dates):
                # 检查十字线是否已经初始化
                if hasattr(self.main_window, 'vline') and hasattr(self.main_window, 'hline') and hasattr(self.main_window, 'volume_vline') and hasattr(self.main_window, 'volume_hline') and hasattr(self.main_window, 'kdj_vline') and hasattr(self.main_window, 'kdj_hline'):
                    # 更新K线图十字线
                    self.main_window.vline.setValue(index)
                    self.main_window.vline.show()
                    
                    # 更新K线图水平线
                    self.main_window.hline.setValue(tech_view_box.mapSceneToView(pos).y())
                    self.main_window.hline.show()
                    
                    # 更新成交量图十字线
                    self.main_window.volume_vline.setValue(index)
                    self.main_window.volume_vline.show()
                    
                    # 获取成交量图的鼠标位置
                    volume_pos = volume_view_box.mapSceneToView(pos)
                    volume_y_val = volume_pos.y()
                    self.main_window.volume_hline.setValue(volume_y_val)
                    self.main_window.volume_hline.show()
                    
                    # 更新KDJ图十字线
                    self.main_window.kdj_vline.setValue(index)
                    self.main_window.kdj_vline.show()
                    
                    # 获取KDJ图的鼠标位置
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    kdj_y_val = kdj_pos.y()
                    self.main_window.kdj_hline.setValue(kdj_y_val)
                    self.main_window.kdj_hline.show()
                    
                    # 直接显示信息框，不需要等待鼠标移动
                    self.main_window.show_info_box()
        except Exception as e:
            logger.exception(f"处理鼠标移动事件时发生错误: {e}")
    
    def handle_mouse_clicked(self, event, dates, opens, highs, lows, closes):
        """
        处理鼠标点击事件
        
        Args:
            event: 鼠标点击事件
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        try:
            # 检查是否是双击事件
            if event.double():  # 检查是否是双击
                # 切换十字线和信息框显示状态
                self.main_window.crosshair_enabled = not self.main_window.crosshair_enabled
                
                if self.main_window.crosshair_enabled:
                    logger.debug("双击K线图，启用十字线和信息框")
                    # 如果当前有K线数据，显示十字线
                    if self.main_window.current_kline_data:
                        # 获取当前鼠标位置对应的K线索引
                        pos = event.pos()
                        view_box = self.main_window.tech_plot_widget.getViewBox()
                        view_pos = view_box.mapSceneToView(pos)
                        x_val = view_pos.x()
                        index = int(round(x_val))
                        
                        # 确保索引在有效范围内
                        index = max(0, min(len(dates) - 1, index))
                        
                        # 保存当前索引和鼠标位置
                        self.main_window.current_kline_index = index
                        self.main_window.current_mouse_pos = pos
                        
                        # 保存当前K线数据
                        self.main_window.current_kline_data = {
                            'dates': dates,
                            'opens': opens,
                            'highs': highs,
                            'lows': lows,
                            'closes': closes,
                            'index': index
                        }
                        
                        # 显示K线图十字线
                        self.main_window.vline.setValue(index)
                        self.main_window.hline.setValue(view_pos.y())
                        self.main_window.vline.show()
                        self.main_window.hline.show()
                        
                        # 显示成交量图十字线
                        self.main_window.volume_vline.setValue(index)
                        volume_view_box = self.main_window.volume_plot_widget.getViewBox()
                        volume_pos = volume_view_box.mapSceneToView(pos)
                        self.main_window.volume_hline.setValue(volume_pos.y())
                        self.main_window.volume_vline.show()
                        self.main_window.volume_hline.show()
                        
                        # 显示KDJ图十字线
                        self.main_window.kdj_vline.setValue(index)
                        kdj_view_box = self.main_window.kdj_plot_widget.getViewBox()
                        kdj_pos = kdj_view_box.mapSceneToView(pos)
                        self.main_window.kdj_hline.setValue(kdj_pos.y())
                        self.main_window.kdj_vline.show()
                        self.main_window.kdj_hline.show()
                        
                        # 直接显示信息框，不需要等待鼠标移动
                        self.main_window.show_info_box()
                else:
                    logger.debug("双击K线图，禁用十字线和信息框")
                    # 隐藏K线图十字线
                    self.main_window.vline.hide()
                    self.main_window.hline.hide()
                    
                    # 隐藏成交量图十字线
                    self.main_window.volume_vline.hide()
                    self.main_window.volume_hline.hide()
                    
                    # 隐藏KDJ图十字线
                    self.main_window.kdj_vline.hide()
                    self.main_window.kdj_hline.hide()
                    
                    # 隐藏信息框
                    if self.main_window.info_text is not None:
                        self.main_window.info_text.hide()
            else:
                # 单击事件，调用均线点击处理函数
                self.main_window.on_ma_clicked(event)
        except Exception as e:
            logger.exception(f"处理鼠标点击事件时发生错误: {e}")