class IndicatorLabelManager:
    """
    指标标签管理器，负责指标值计算和标签更新逻辑
    """
    
    def __init__(self, main_window):
        """
        初始化指标标签管理器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def update_indicator_values_label(self, indicator_name, df_pl):
        """
        更新指标值显示标签
        
        Args:
            indicator_name: 指标名称
            df_pl: polars DataFrame，包含指标数据
        """
        # 创建指标值显示标签
        if not hasattr(self.main_window, 'kdj_values_label'):
            # 创建新的标签
            from .utils.label_utils import LabelUtils
            self.main_window.kdj_values_label = LabelUtils.create_label()
        
        # 将Polars DataFrame转换为pandas DataFrame以便处理
        df_pd = df_pl.to_pandas() if hasattr(df_pl, 'to_pandas') else df_pl
        
        # 保存指标数据，用于鼠标移动时更新指标数值
        self.main_window.save_indicator_data(df_pl)
        
        # 获取当前第3个窗口的指标
        current_indicator = self.main_window.window_indicators.get(3, "KDJ")
        
        # 日志记录：当前指标和可用列
        from src.utils.logger import logger
        logger.info(f"当前指标: {current_indicator}, 可用列: {df_pd.columns.tolist()}")
        
        # 根据当前指标更新标签文本
        if current_indicator == "KDJ":
            self._update_kdj_label(df_pd)
        elif current_indicator == "RSI":
            self._update_rsi_label(df_pd)
        elif current_indicator == "MACD":
            self._update_macd_label(df_pd)
        elif current_indicator == "BOLL":
            self._update_boll_label(df_pd)
        elif current_indicator == "WR":
            self._update_wr_label(df_pd)
        elif current_indicator == "DMI":
            self._update_dmi_label(df_pd)
        elif current_indicator == "VR":
            self._update_vr_label(df_pd)
        elif current_indicator == "TRIX":
            self._update_trix_label(df_pd)
        elif current_indicator == "BRAR":
            self._update_brar_label(df_pd)
        else:
            self._update_default_label(df_pd)
    
    def _update_kdj_label(self, df_pd):
        """
        更新KDJ标签
        
        Args:
            df_pd: pandas DataFrame，包含KDJ数据
        """
        if 'k' in df_pd.columns and 'd' in df_pd.columns and 'j' in df_pd.columns:
            latest_k = df_pd['k'].iloc[-1]
            latest_d = df_pd['d'].iloc[-1]
            latest_j = df_pd['j'].iloc[-1]
            # 更新标签文本
            kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
        else:
            kdj_text = f"<font color='white'>KDJ指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(kdj_text)
    
    def _update_rsi_label(self, df_pd):
        """
        更新RSI标签
        
        Args:
            df_pd: pandas DataFrame，包含RSI数据
        """
        if 'rsi14' in df_pd.columns:
            latest_rsi = df_pd['rsi14'].iloc[-1]
            # 更新标签文本
            rsi_text = f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>"
        else:
            rsi_text = f"<font color='white'>RSI指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(rsi_text)
    
    def _update_macd_label(self, df_pd):
        """
        更新MACD标签
        
        Args:
            df_pd: pandas DataFrame，包含MACD数据
        """
        if 'macd' in df_pd.columns and 'macd_signal' in df_pd.columns and 'macd_hist' in df_pd.columns:
            latest_macd = df_pd['macd'].iloc[-1]
            latest_macd_signal = df_pd['macd_signal'].iloc[-1]
            latest_macd_hist = df_pd['macd_hist'].iloc[-1]
            # 更新标签文本，使用通达信风格：DIF白色，DEA黄色，MACD根据正负值变色
            macd_hist_color = '#FF0000' if latest_macd_hist >= 0 else '#00FF00'
            macd_text = f"<font color='white'>MACD(12,26,9) </font><font color='#FFFFFF'>DIF: {latest_macd:.2f}</font> <font color='#FFFF00'>DEA: {latest_macd_signal:.2f}</font> <font color='{macd_hist_color}'>MACD: {latest_macd_hist:.2f}</font>"
        else:
            macd_text = f"<font color='white'>MACD指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(macd_text)
    
    def _update_boll_label(self, df_pd):
        """
        更新BOLL标签
        
        Args:
            df_pd: pandas DataFrame，包含BOLL数据
        """
        if 'mb' in df_pd.columns and 'up' in df_pd.columns and 'dn' in df_pd.columns:
            latest_mb = df_pd['mb'].iloc[-1]
            latest_up = df_pd['up'].iloc[-1]
            latest_dn = df_pd['dn'].iloc[-1]
            # 更新标签文本
            boll_text = f"<font color='white'>MB: {latest_mb:.2f}</font>  <font color='red'>UP: {latest_up:.2f}</font>  <font color='#00FF00'>DN: {latest_dn:.2f}</font>"
        else:
            boll_text = f"<font color='white'>BOLL指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(boll_text)
    
    def _update_wr_label(self, df_pd):
        """
        更新WR标签
        
        Args:
            df_pd: pandas DataFrame，包含WR数据
        """
        # 获取最新的WR值（通达信风格：WR1和WR2）
        if 'wr1' in df_pd.columns and 'wr2' in df_pd.columns:
            latest_wr1 = df_pd['wr1'].iloc[-1]
            latest_wr2 = df_pd['wr2'].iloc[-1]
            # 更新标签文本，模拟通达信显示风格，颜色与图中指标一致（WR1黄色，WR2白色）
            wr_text = f"<font color='white'>WR(10,6) </font><font color='yellow'>WR1: {latest_wr1:.2f}</font> <font color='white'>WR2: {latest_wr2:.2f}</font>"
        else:
            # 兼容旧格式
            latest_wr = df_pd['wr'].iloc[-1] if 'wr' in df_pd.columns else 0
            wr_text = f"<font color='white'>WR: {latest_wr:.2f}</font>"
        self.main_window.kdj_values_label.setText(wr_text)
    
    def _update_dmi_label(self, df_pd):
        """
        更新DMI标签
        
        Args:
            df_pd: pandas DataFrame，包含DMI数据
        """
        if 'pdi' in df_pd.columns and 'ndi' in df_pd.columns and 'adx' in df_pd.columns and 'adxr' in df_pd.columns:
            latest_pdi = df_pd['pdi'].iloc[-1]
            latest_ndi = df_pd['ndi'].iloc[-1]
            latest_adx = df_pd['adx'].iloc[-1]
            latest_adxr = df_pd['adxr'].iloc[-1]
            # 更新标签文本，与图中指标颜色一致
            dmi_text = f"<font color='#FFFFFF'>PDI: {latest_pdi:.2f}</font>  <font color='#FFFF00'>NDI: {latest_ndi:.2f}</font>  <font color='#FF00FF'>ADX: {latest_adx:.2f}</font>  <font color='#00FF00'>ADXR: {latest_adxr:.2f}</font>"
        else:
            dmi_text = f"<font color='white'>DMI指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(dmi_text)
    
    def _update_vr_label(self, df_pd):
        """
        更新VR标签
        
        Args:
            df_pd: pandas DataFrame，包含VR数据
        """
        if 'vr' in df_pd.columns:
            latest_vr = df_pd['vr'].iloc[-1]
            # 检查mavr列是否存在，如果不存在则计算
            if 'mavr' in df_pd.columns:
                latest_mavr = df_pd['mavr'].iloc[-1]
                # 更新标签文本，使用通达信风格：VR: xxx MAVR: xxx
                vr_text = f"<font color='#FFFFFF'>VR: {latest_vr:.2f}</font>  <font color='#FFFF00'>MAVR: {latest_mavr:.2f}</font>"
            else:
                # 如果mavr列不存在，只显示VR值
                vr_text = f"<font color='#FFFFFF'>VR: {latest_vr:.2f}</font>"
        else:
            vr_text = f"<font color='white'>VR指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(vr_text)
    
    def _update_trix_label(self, df_pd):
        """
        更新TRIX标签
        
        Args:
            df_pd: pandas DataFrame，包含TRIX数据
        """
        if 'trix' in df_pd.columns and 'trma' in df_pd.columns:
            latest_trix = df_pd['trix'].iloc[-1]
            latest_trma = df_pd['trma'].iloc[-1]
            # 更新标签文本，使用通达信风格
            trix_text = f"<font color='#FFFFFF'>TRIX: {latest_trix:.2f}</font>  <font color='#FFFF00'>MATRIX: {latest_trma:.2f}</font>"
        else:
            trix_text = f"<font color='white'>TRIX指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(trix_text)
    
    def _update_brar_label(self, df_pd):
        """
        更新BRAR标签
        
        Args:
            df_pd: pandas DataFrame，包含BRAR数据
        """
        if 'br' in df_pd.columns and 'ar' in df_pd.columns:
            latest_br = df_pd['br'].iloc[-1]
            latest_ar = df_pd['ar'].iloc[-1]
            # 更新标签文本，使用通达信风格
            brar_text = f"<font color='#FFFF00'>BR: {latest_br:.2f}</font>  <font color='#FFFFFF'>AR: {latest_ar:.2f}</font>"
        else:
            brar_text = f"<font color='white'>BRAR指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(brar_text)
    
    def _update_default_label(self, df_pd):
        """
        更新默认标签
        
        Args:
            df_pd: pandas DataFrame，包含指标数据
        """
        # 默认绘制KDJ指标，显示KDJ数值
        if 'k' in df_pd.columns and 'd' in df_pd.columns and 'j' in df_pd.columns:
            latest_k = df_pd['k'].iloc[-1]
            latest_d = df_pd['d'].iloc[-1]
            latest_j = df_pd['j'].iloc[-1]
            # 更新标签文本
            kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
        else:
            kdj_text = f"<font color='white'>指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(kdj_text)