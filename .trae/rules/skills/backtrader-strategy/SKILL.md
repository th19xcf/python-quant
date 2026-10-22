# backtrader-strategy

**状态**：暂未使用。

本文件原本用于 Trae AI IDE 对 Backtrader 策略开发场景的引导。
项目当前回测引擎为自研（`src/backtest/engine/`），未集成 Backtrader。

如果未来需要支持 Backtrader 兼容层，在此补充：
- 策略适配接口
- 与自研引擎的差异对照表
- 迁移指南

目前 Trae 对本项目量化场景的引导，请参考：
- `docs/architecture.md`
- `docs/tdx_calibration_guide.md`
- `src/backtest/engine/base_engine.py` 顶部模块文档
