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
        self.main_window.chart_manager.save_indicator_data(df_pl)
        
        # 使用传入的indicator_name，而不是从window_indicators获取
        # 这样可以确保显示的是当前正在绘制的指标
        current_indicator = indicator_name
        
        # 日志记录：当前指标和可用列
        from src.utils.logger import logger
        logger.info(f"更新指标值标签 - 指标: {current_indicator}, 可用列: {df_pd.columns.tolist()}")
        
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
        elif current_indicator == "DMA":
            self._update_dma_label(df_pd)
        elif current_indicator == "OBV":
            self._update_obv_label(df_pd)
        elif current_indicator == "EXPMA":
            self._update_expma_label(df_pd)
        elif current_indicator == "BBI":
            self._update_bbi_label(df_pd)
        elif current_indicator == "HSL":
            self._update_hsl_label(df_pd)
        elif current_indicator == "LB":
            self._update_lb_label(df_pd)
        elif current_indicator == "CYC":
            self._update_cyc_label(df_pd)
        elif current_indicator == "CYS":
            self._update_cys_label(df_pd)
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

    def _update_dma_label(self, df_pd):
        """
        更新DMA标签

        Args:
            df_pd: pandas DataFrame，包含DMA数据
        """
        if 'dma' in df_pd.columns and 'ama' in df_pd.columns:
            latest_dma = df_pd['dma'].iloc[-1]
            latest_ama = df_pd['ama'].iloc[-1]
            # 更新标签文本，使用通达信风格：DMA白色，AMA黄色
            dma_text = f"<font color='white'>DMA(10,50): {latest_dma:.2f}</font>  <font color='yellow'>AMA: {latest_ama:.2f}</font>"
        else:
            dma_text = f"<font color='white'>DMA指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(dma_text)

    def _update_obv_label(self, df_pd):
        """
        更新OBV标签

        Args:
            df_pd: pandas DataFrame，包含OBV数据
        """
        if 'obv' in df_pd.columns:
            latest_obv = df_pd['obv'].iloc[-1]
            obv_text = f"<font color='white'>OBV: {latest_obv:,.0f}</font>"
        else:
            obv_text = f"<font color='white'>OBV指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(obv_text)

    def _update_expma_label(self, df_pd):
        """
        更新EXPMA标签

        Args:
            df_pd: pandas DataFrame，包含EXPMA数据
        """
        if 'expma12' in df_pd.columns and 'expma50' in df_pd.columns:
            latest_expma12 = df_pd['expma12'].iloc[-1]
            latest_expma50 = df_pd['expma50'].iloc[-1]
            expma_text = f"<font color='white'>EXPMA(12,50) </font><font color='#FFFF00'>EXPMA12: {latest_expma12:.2f}</font> <font color='#FF00FF'>EXPMA50: {latest_expma50:.2f}</font>"
        else:
            expma_text = f"<font color='white'>EXPMA指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(expma_text)

    def _update_bbi_label(self, df_pd):
        """
        更新BBI标签

        Args:
            df_pd: pandas DataFrame，包含BBI数据
        """
        if 'bbi' in df_pd.columns:
            latest_bbi = df_pd['bbi'].iloc[-1]
            bbi_text = f"<font color='white'>BBI(3,6,12,24): {latest_bbi:.2f}</font>"
        else:
            bbi_text = f"<font color='white'>BBI指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(bbi_text)

    def _update_hsl_label(self, df_pd):
        """
        更新HSL（换手率）标签

        Args:
            df_pd: pandas DataFrame，包含HSL数据
        """
        if 'hsl' in df_pd.columns:
            latest_hsl = df_pd['hsl'].iloc[-1]
            hsl_color = '#FF0000' if latest_hsl > 10 else '#FFA500' if latest_hsl > 5 else '#00BFFF'
            hsl_text = f"<font color='{hsl_color}'>HSL: {latest_hsl:.2f}%</font>"
            if 'hsl_ma5' in df_pd.columns:
                latest_hsl_ma5 = df_pd['hsl_ma5'].iloc[-1]
                hsl_text += f"  <font color='white'>MA5: {latest_hsl_ma5:.2f}%</font>"
            if 'hsl_ma10' in df_pd.columns:
                latest_hsl_ma10 = df_pd['hsl_ma10'].iloc[-1]
                hsl_text += f"  <font color='cyan'>MA10: {latest_hsl_ma10:.2f}%</font>"
        else:
            hsl_text = f"<font color='white'>HSL指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(hsl_text)

    def _update_lb_label(self, df_pd):
        """
        更新LB（量比）标签

        Args:
            df_pd: pandas DataFrame，包含LB数据
        """
        if 'lb' in df_pd.columns:
            latest_lb = df_pd['lb'].iloc[-1]
            lb_color = '#FF0000' if latest_lb > 2 else '#FFA500' if latest_lb > 1.5 else '#00FF7F' if latest_lb > 1 else '#00BFFF'
            lb_text = f"<font color='{lb_color}'>LB: {latest_lb:.2f}</font>"
        else:
            lb_text = f"<font color='white'>LB指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(lb_text)

    def _update_cyc_label(self, df_pd):
        """
        更新CYC（成本均线）标签

        Args:
            df_pd: pandas DataFrame，包含CYC数据
        """
        if 'cyc5' in df_pd.columns and 'cyc13' in df_pd.columns:
            latest_cyc5 = df_pd['cyc5'].iloc[-1]
            latest_cyc13 = df_pd['cyc13'].iloc[-1]
            cyc_text = f"<font color='#FFFF00'>CYC5: {latest_cyc5:.2f}</font> <font color='#FFA500'>CYC13: {latest_cyc13:.2f}</font>"
            if 'cyc34' in df_pd.columns:
                latest_cyc34 = df_pd['cyc34'].iloc[-1]
                cyc_text += f" <font color='#FF00FF'>CYC34: {latest_cyc34:.2f}</font>"
            if 'cyc_inf' in df_pd.columns:
                latest_cyc_inf = df_pd['cyc_inf'].iloc[-1]
                cyc_text += f" <font color='#00FFFF'>CYC∞: {latest_cyc_inf:.2f}</font>"
        else:
            cyc_text = f"<font color='white'>CYC指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(cyc_text)

    def _update_cys_label(self, df_pd):
        """
        更新CYS（市场盈亏）标签

        Args:
            df_pd: pandas DataFrame，包含CYS数据
        """
        if 'cys' in df_pd.columns:
            latest_cys = df_pd['cys'].iloc[-1]
            cys_color = '#FF0000' if latest_cys > 0 else '#00FF00'
            cys_text = f"<font color='{cys_color}'>CYS: {latest_cys:.2f}%</font>"
            if 'cys_ma5' in df_pd.columns:
                latest_cys_ma5 = df_pd['cys_ma5'].iloc[-1]
                cys_text += f"  <font color='white'>MA5: {latest_cys_ma5:.2f}%</font>"
        else:
            cys_text = f"<font color='white'>CYS指标数据不可用</font>"
        self.main_window.kdj_values_label.setText(cys_text)

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