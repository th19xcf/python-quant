import pyqtgraph as pg

class VOLTDXDrawer:
    """
    VOL-TDX指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制VOL-TDX指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含vol_tdx相关列

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的VOL_TDX列
        """
        # 检查VOL-TDX相关列是否存在
        if 'vol_tdx' not in df_pl.columns:
            raise ValueError(
                f"VOL-TDX绘制失败：数据缺少'vol_tdx'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算VOL_TDX指标。"
            )
        
        if 'close' not in df_pl.columns:
            raise ValueError(
                f"VOL-TDX绘制失败：数据缺少'close'列。"
            )

        # 绘制VOL-TDX柱状图（红色/绿色）
        vol_tdx_values = df_pl['vol_tdx'].to_numpy()

        # 根据涨跌决定颜色
        close_values = df_pl['close'].to_numpy()
        prev_close = df_pl['close'].shift(1).to_numpy()

        pos_mask = close_values >= prev_close
        neg_mask = close_values < prev_close

        # 绘制红色柱（上涨）
        if pos_mask.any():
            pos_bar = pg.BarGraphItem(
                x=x[pos_mask],
                height=vol_tdx_values[pos_mask],
                width=0.35,
                brush='r',
                pen='r'
            )
            plot_widget.addItem(pos_bar)

        # 绘制绿色柱（下跌）
        if neg_mask.any():
            neg_bar = pg.BarGraphItem(
                x=x[neg_mask],
                height=vol_tdx_values[neg_mask],
                width=0.35,
                brush='g',
                pen='g'
            )
            plot_widget.addItem(neg_bar)

        return df_pl
