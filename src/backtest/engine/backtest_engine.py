#!/usr/bin/env python3

"""
回测引擎（单票），撮合逻辑全部委托给 execution_core 统一内核
"""

from typing import Any, Dict, List

from loguru import logger

from src.backtest.engine._parallel_worker import clone_strategy, run_backtest_worker
from src.backtest.engine.base_engine import BaseBacktestEngine
from src.backtest.engine.execution_core import EventBacktestRunner, StStatusEntry


class BacktestEngine(BaseBacktestEngine):
    """
    回测引擎类，提供单只股票的策略回测功能

    交易规则（遵循A股制度，实现见 execution_core）：
    1. t日信号延迟到t+1日以开盘价成交（避免收盘价未来函数）
    2. 买入股数按lot_size（默认100股）整数倍取整，买入时即扣除买方成本
    3. 开盘触及涨停不可买入、触及跌停不可卖出、停牌日（成交量为0）不可交易
    4. 涨跌停幅度按 ts_code 自动推导（创业板/科创板 20%、ST 5%、北交所 30%）

    风控层（可选）：
    - 设置 self.risk_engine 后，buy/sell 前会先过 gate()
    - 被拒的单子记进 self.rejected_orders，包含规则名和原因
    """

    def __init__(self, data):
        """
        初始化回测引擎

        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.pl_df = self.convert_to_polars(data)

    def run_backtest(self, initial_capital: float = 1000000.0, lot_size: int = 100,
                     ts_code: str = '', industry: str = '',
                     is_st: StStatusEntry | None = None, **params) -> Dict[str, Any]:
        """
        运行回测

        Args:
            initial_capital: 初始资金
            lot_size: 最小交易单位（股），A股买入为100股整数倍
            ts_code: 股票代码，涨跌停幅度据此自适应推导
            industry: 行业标签（风控 MaxIndustryWeight 用）
            is_st: ST 三态——True=ST、False=明确非 ST、None=未知（默认）。
                未知时涨跌停按板块默认幅度判定（与 False 相同），结果标记
                st_unknown=True、逐笔 fill_confidence=low（数据质量提示）。
            **params: 其他回测参数

        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}

        self.ts_code = ts_code or self.ts_code
        # 执行上下文供参数优化复用：子回测必须与主回测同口径
        self.industry = industry
        self.is_st = is_st

        # 指标预计算：根据策略参数一次性向量化算好，避免每根K线在策略里 O(n²) 循环
        self.pl_df = self._precompute_indicators(self.pl_df, self.strategy)

        # 单票 = 单标的账户，初始资金全部归属该标的
        code = self.ts_code or '__default__'

        runner = EventBacktestRunner(
            cost_model=self.cost_model,
            performance_analyzer=self.performance_analyzer,
            risk_engine=self.risk_engine,
        )
        result = runner.run(
            universe={code: self.pl_df},
            strategies={code: self.strategy},
            initial_capital=initial_capital,
            lot_size=lot_size,
            industry_map={code: industry} if industry else {},
            result_meta={'strategy_name': self.strategy.name},
            st_status={code: is_st},
        )

        self.trades = result.get('trades', [])
        self.equity_curve = result.get('equity_curve', [])
        self.rejected_orders = result.get('rejected_orders', [])
        self.expired_orders = result.get('expired_orders', [])
        self.positions = result.get('positions', {})
        self.backtest_results = result
        return result

    def optimize_strategy(self, param_ranges: Dict[str, List[float]],
                          initial_capital: float = 1000000.0,
                          objective: str = 'total_return', max_workers: int = 4,
                          validation: str = 'oos', oos_ratio: float = 0.3,
                          walk_forward_windows: int = 0) -> Dict[str, Any]:
        """
        优化策略参数，并附带样本外（OOS）验证与参数稳定性报告。

        Args:
            param_ranges: 参数范围
            initial_capital: 初始资金
            objective: 优化目标
            max_workers: 并行计算的最大工作线程数
            validation: 'oos' 时按时间顺序切出后段作为样本外验证；
                'none' 时只做全样本网格搜索（旧行为）
            oos_ratio: 样本外占比（0~1），仅 validation='oos' 时生效
            walk_forward_windows: ≥2 时追加 walk-forward 滚动验证折数；
                0/1 表示不做滚动验证

        Returns:
            Dict[str, Any]: 优化结果（含 oos_validation / walk_forward）
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}

        param_combinations = self._generate_param_combinations(param_ranges)
        context = {
            'ts_code': self.ts_code,
            'industry': self.industry,
            'is_st': self.is_st,
        }
        results = self._run_parallel_backtests(
            param_combinations, initial_capital, max_workers, context=context)
        best_params, best_value = self._select_best_params(results, objective)

        logger.info(f"参数优化完成: 最佳参数 = {best_params}, 最佳{objective} = {best_value:.4f}")

        self.strategy.set_params(best_params)

        response: Dict[str, Any] = {
            'best_params': best_params,
            'best_value': best_value,
            'optimization_results': results,
            'objective': objective,
            # 全样本网格上的参数稳定性：最优点是否为孤立尖峰
            'param_stability_report': self._build_stability_report(results, objective),
        }

        if validation == 'oos':
            response['oos_validation'] = self._validate_out_of_sample(
                param_ranges, initial_capital, max_workers, context,
                objective, oos_ratio, walk_forward_windows,
            )
            logger.info(
                f"样本外验证: {response['oos_validation'].get('out_of_sample')}"
            )

        return response

    def _validate_out_of_sample(self, param_ranges: Dict[str, List[float]],
                                initial_capital: float, max_workers: int,
                                context: Dict[str, Any], objective: str,
                                oos_ratio: float, walk_forward_windows: int) -> Dict[str, Any]:
        """样本内选参 → 样本外验证，并返回衰减指标与稳定性报告。"""
        split = self._split_out_of_sample(self.pl_df, oos_ratio)
        if split is None:
            return {
                'enabled': False,
                'reason': f'数据仅 {self.pl_df.height} 行，不足以切出样本内/样本外两段',
            }

        is_df, oos_df = split
        # 严格样本内选参：不在样本外上做任何选择，避免信息泄漏
        is_results = self._run_parallel_backtests(
            self._generate_param_combinations(param_ranges), initial_capital,
            max_workers, context=context, data=is_df,
        )
        is_best_params, is_best_value = self._select_best_params(is_results, objective)
        best_row = next((r for r in is_results if r.get('params') == is_best_params), {})
        is_metrics = self._metric_snapshot(best_row)

        oos_result = self._run_single_backtest(is_best_params, initial_capital, context, oos_df)
        oos_metrics = self._metric_snapshot(oos_result)

        report: Dict[str, Any] = {
            'enabled': True,
            'objective': objective,
            'oos_ratio': oos_ratio,
            'split_index': is_df.height,
            'in_sample_range': self._slice_label(is_df),
            'out_of_sample_range': self._slice_label(oos_df),
            'in_sample_best_params': is_best_params,
            'in_sample_best_value': is_best_value,
            'in_sample': is_metrics,
            'out_of_sample': oos_metrics,
            # 衰减 = 样本外 - 样本内；收益/夏普明显转负说明参数过拟合
            'decay': {
                'total_return': oos_metrics['total_return'] - is_metrics['total_return'],
                'sharpe_ratio': oos_metrics['sharpe_ratio'] - is_metrics['sharpe_ratio'],
            },
            'param_stability': self._build_stability_report(is_results, objective),
        }

        if walk_forward_windows and walk_forward_windows >= 2:
            report['walk_forward'] = self._walk_forward_validate(
                param_ranges, initial_capital, max_workers, context,
                objective, walk_forward_windows,
            )

        return report

    def _walk_forward_validate(self, param_ranges: Dict[str, List[float]],
                                initial_capital: float, max_workers: int,
                                context: Dict[str, Any], objective: str,
                                windows: int) -> Dict[str, Any]:
        """滚动 walk-forward：逐折用前段选参，在其后一段上验证。"""
        total = self.pl_df.height
        chunk = total // (windows + 1)
        if chunk < 20:
            return {
                'enabled': False,
                'reason': f'数据仅 {total} 行，无法切出 {windows} 折（每折至少 20 行）',
            }

        combinations = self._generate_param_combinations(param_ranges)
        folds = []
        for i in range(1, windows + 1):
            train_df = self.pl_df.head(chunk * i)
            test_df = self.pl_df.slice(chunk * i, chunk)
            train_results = self._run_parallel_backtests(
                combinations, initial_capital, max_workers,
                context=context, data=train_df,
            )
            fold_params, fold_value = self._select_best_params(train_results, objective)
            fold_result = self._run_single_backtest(
                fold_params, initial_capital, context, test_df,
            )
            folds.append({
                'fold': i,
                'train_range': self._slice_label(train_df),
                'test_range': self._slice_label(test_df),
                'best_params': fold_params,
                'in_sample_value': fold_value,
                'out_of_sample': self._metric_snapshot(fold_result),
            })

        oos_returns = [f['out_of_sample']['total_return'] for f in folds]
        positive_ratio = (
            sum(1 for value in oos_returns if value > 0) / len(oos_returns)
            if oos_returns else 0.0
        )
        return {
            'enabled': True,
            'windows': windows,
            'folds': folds,
            'positive_ratio': positive_ratio,
            'mean_oos_total_return': sum(oos_returns) / len(oos_returns) if oos_returns else 0.0,
        }

    def _should_use_process_pool(self, frame, param_combinations) -> bool:
        """判断是否值得切进程池：需可 pickle 且预计串行耗时超过启动成本。"""
        import pickle

        # 进程池的启动成本必须由任务量摊销。Windows 用 spawn 启动子进程，每个
        # 子进程都要重新导入 numpy/polars/回测引擎（实测 2.1–2.4s）；而单次回测
        # 500 行仅 ~14ms、5000 行 ~26ms。实测 4 组参数：进程池 3358ms vs 串行
        # 22.5ms（慢 147 倍）。因此只有预计串行总耗时超过启动成本时才切进程池。
        pool_startup_ms = 3000.0
        backtest_fixed_ms = 8.0
        backtest_per_row_ms = 0.004
        rows = frame.height if frame is not None else 0
        est_serial_ms = len(param_combinations) * (backtest_fixed_ms + rows * backtest_per_row_ms)

        # 检查策略、数据、成本模型、风控引擎是否可 pickle，决定能否使用多进程
        picklable = True
        try:
            pickle.dumps((self.strategy, frame, self.cost_model, self.risk_engine))
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            picklable = False
            logger.warning(f"策略/数据/成本模型不可 pickle（{exc}），回退到 ThreadPoolExecutor 并行回测")

        use_process_pool = picklable and len(param_combinations) > 1 and est_serial_ms > pool_startup_ms
        if picklable and not use_process_pool:
            logger.debug(
                f"参数组合 {len(param_combinations)} 组 × {rows} 行，预计串行 "
                f"{est_serial_ms:.0f}ms，低于进程池启动成本 {pool_startup_ms:.0f}ms，改用串行回测"
            )
        return use_process_pool

    def _run_process_pool_backtests(self, frame, param_combinations, initial_capital,
                                    max_workers, context) -> List[Dict[str, Any]]:
        """用进程池并行执行回测并收集结果。"""
        import concurrent.futures
        import os

        results = []
        # 纯 Python 回测循环受 GIL 限制，线程无法并行 CPU 计算，改用进程池利用多核
        workers = max(1, min(max_workers, len(param_combinations), os.cpu_count() or 1))
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_param = {
                executor.submit(run_backtest_worker, BacktestEngine, self.strategy,
                                frame, self.cost_model, params, initial_capital,
                                self.risk_engine, context): params
                for params in param_combinations
            }

            for future in concurrent.futures.as_completed(future_to_param):
                try:
                    result = future.result()
                    results.append(result)
                except (OSError, RuntimeError, ValueError) as exc:
                    logger.error(f"参数组合 {future_to_param[future]} 回测失败: {exc}")

        return results

    def _run_thread_pool_backtests(self, frame, param_combinations, initial_capital,
                                   max_workers, context) -> List[Dict[str, Any]]:
        """用线程池并行执行回测并收集结果。"""
        import concurrent.futures

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as thread_executor:
            future_to_param = {
                thread_executor.submit(
                    self._run_single_backtest, params, initial_capital, context, frame,
                ): params
                for params in param_combinations
            }

            for future in concurrent.futures.as_completed(future_to_param):
                try:
                    result = future.result()
                    results.append(result)
                except (OSError, RuntimeError, ValueError) as exc:
                    logger.error(f"参数组合 {future_to_param[future]} 回测失败: {exc}")

        return results

    def _run_parallel_backtests(self, param_combinations: List[Dict[str, float]],
                                initial_capital: float, max_workers: int,
                                context: Dict[str, Any] | None = None,
                                data = None) -> List[Dict[str, Any]]:
        """
        并行运行回测

        Args:
            param_combinations: 参数组合列表
            initial_capital: 初始资金
            max_workers: 最大工作线程数
            context: 主回测执行上下文 {ts_code, industry, is_st}，逐个子回测透传
            data: 本次回测使用的数据段；None 表示全量 self.pl_df（OOS/滚动验证传子段）

        Returns:
            List[Dict[str, Any]]: 回测结果列表
        """
        frame = self.pl_df if data is None else data

        if self._should_use_process_pool(frame, param_combinations):
            return self._run_process_pool_backtests(
                frame, param_combinations, initial_capital, max_workers, context
            )
        return self._run_thread_pool_backtests(
            frame, param_combinations, initial_capital, max_workers, context
        )

    def _run_single_backtest(self, params: Dict[str, float], initial_capital: float,
                             context: Dict[str, Any] | None, data) -> Dict[str, Any]:
        """用指定参数在指定数据段上跑一次回测（OOS / walk-forward 复用）。"""
        strategy_copy = clone_strategy(self.strategy, params)

        engine_copy = BacktestEngine(self.pl_df if data is None else data)
        engine_copy.set_strategy(strategy_copy)
        engine_copy.set_cost_model(self.cost_model)
        engine_copy.risk_engine = self.risk_engine

        # 与进程池路径同口径：ts_code/行业/ST 必须透传，否则涨跌停档位退化
        ctx = context or {}
        result = engine_copy.run_backtest(
            initial_capital,
            ts_code=ctx.get('ts_code', ''),
            industry=ctx.get('industry', ''),
            is_st=ctx.get('is_st'),
        )
        result['params'] = params
        return result
