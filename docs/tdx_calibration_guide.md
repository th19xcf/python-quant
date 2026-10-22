# 通达信单股票校准指南

## 目的

用于校准项目算法准确性：

1. 通达信原始日线数据 vs 项目取数结果（OHLCVA）
2. 通达信软件导出指标 vs 项目指标（可选）

推荐先用 000001.SZ 做基准样本。

## 脚本位置

tools/calibrate_tdx_accuracy.py

## 基础用法（只比对K线）

```bash
python tools/calibrate_tdx_accuracy.py --stock 000001.SZ --start 2023-01-01 --end 2024-12-31
```

## 指标对账用法（含通达信导出CSV）

```bash
python tools/calibrate_tdx_accuracy.py \
  --stock 000001.SZ \
  --start 2023-01-01 \
  --end 2024-12-31 \
  --tdx-indicator-csv path/to/tdx_export.csv
```

## CSV字段要求（通达信导出）

脚本支持常见字段名自动映射，至少需要日期列。

- 日期列：日期 / date / Date / DATE
- MA：MA5、MA10、MA20、MA60
- MACD：DIF、DEA、MACD
- RSI：RSI 或 RSI14

## 输出结果

默认写入 logs/calibration：

- {stock}_price_summary.csv：K线字段误差汇总
- {stock}_price_detail.csv：K线字段逐日误差明细
- {stock}_project_indicators.csv：项目计算的指标结果
- {stock}_indicator_summary.csv：指标误差汇总（提供导出CSV时）
- {stock}_indicator_detail.csv：指标逐日误差明细（提供导出CSV时）

## 判定建议

- K线字段：max_abs_diff 应接近 0
- MA/RSI：通常应非常接近（浮点误差级别）
- MACD：若存在系统性偏差，优先检查 EMA 初始化方式与通达信口径是否一致

## 注意

- 请先确认 .env 中 DATA__TDX_DATA_PATH 指向本机通达信目录。
- 若项目数据库未配置，本脚本仍可运行（会使用本地通达信读取路径）。
