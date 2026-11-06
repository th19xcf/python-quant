#!/usr/bin/env python3

"""
回测引擎（单票），撮合逻辑全部委托给 execution_core 统一内核
"""

from collections.abc import Mapping
from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.backtest.engine._parallel_worker import (
    clone_strategy,
    init_worker,
    run_backtest_worker_cached,
)
from src.backtest.engine.base_engine import BaseBacktestEngine
from src.backtest.engine.corporate_actions import CorporateActionSchedule
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

    def __init__(self, data: Any) -> None:
        """
        初始化回测引擎

        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.pl_df = self.convert_to_polars(data)

    def run_backtest(self, initial_capital: float = 1000000.0, lot_size: int = 100,
                     ts_code: str = '', industry: str = '',
                     is_st: StStatusEntry | None = None,
                     dividends: Mapping[str, pl.DataFrame] | None = None,
                     dividend_tax_rate: float = 0.0,
                     rights_policy: str = 'partial',
                     **params: Any) -> Dict[str, Any]:
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
            dividends: 标的 → 除权除息记录（列同 stock_dividend 表），
                None/空表表示不记账分红。只在原始价口径下启用，hfq 价格序列
                不得再传本参数（否则分红双重计入）。
            dividend_tax_rate: 现金分红税率（0~1），默认 0；未建模持股期限
                差异化税率，按单一税率近似。
            rights_policy: 配股现金不足时的处置：'partial'（按现金能力部分
                认购，默认）或 'abandon'（放弃配股）。
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

        # 除权除息事件表：按标的构建一次，单标的只取当前代码的记录
        self.dividends = dividends
        self.dividend_tax_rate = dividend_tax_rate
        self.rights_policy = rights_policy
        schedule = CorporateActionSchedule.from_frames(dividends)

        runner = EventBacktestRunner(
            cost_model=self.cost_model,
            performance_analyzer=self.performance_analyzer,
            risk_engine=self.risk_engine,
            on_missing_risk=self.risk_gate_policy,
        )
        result = runner.run(
            universe={code: self.pl_df},
            strategies={code: self.strategy},
            initial_capital=initial_capital,
            lot_size=lot_size,
            industry_map={code: industry} if industry else {},
            result_meta={'strategy_name': self.strategy.name},
            st_status={code: is_st},
            corporate_actions=schedule,
            dividend_tax_rate=dividend_tax_rate,
            rights_policy=rights_policy,
        )

        self.trades = result.get('trades', [])
        self.equity_curve = result.get('equity_curve', [])
        self.rejected_orders = result.get('rejected_orders', [])
        self.expired_orders = result.get('expired_orders', [])
        self.overwritten_orders = result.get('overwritten_orders', [])
        self.filtered_signals = result.get('filtered_signals', [])
        self.positions = result.get('positions', {})
        self.backtest_results = result
        return result

    def optimize_strategy(self, param_ranges: Dict[str, List[float]],
                          initial_capital: float = 1000000.0,
                          objective: str = 'total_return', max_workers: int = 4,
                          validation: str = 'oos', oos_ratio: float = 0.3,
                          walk_forward_windows: int = 0,
                          oos_horizon: int = 10, oos_embargo: int = 20,
                          max_failed_ratio: float = 0.0) -> Dict[str, Any]:
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
            oos_horizon: 标签跨度（交易日），默认 10；训练窗末尾按该跨度 purge
            oos_embargo: 训练窗与测试窗隔离带（交易日），默认 20，须 ≥ horizon
            max_failed_ratio: 允许失败的参数组合占比，默认 0.0（零容忍）。
                超过比例直接抛 RuntimeError，避免在残缺网格上选最优参数。

        Returns:
            Dict[str, Any]: 优化结果（含 oos_validation / walk_forward）
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}

        # P0-4：一次优化的所有子网格（全样本参考 / IS / walk-forward 各折）
        # 共用一份失败台账；开头清零，结尾统一验收。
        self._failed_combinations = []
        self._optimization_attempts = 0
        param_combinations = self._generate_param_combinations(param_ranges)
        context = {
            'ts_code': self.ts_code,
            'industry': self.industry,
            'is_st': self.is_st,
        }
        # 全样本网格保留为「参考 + 兼容」数据（历史调用方直接读它做对账），
        # 但 P0-2 起**绝不**用它选择最终参数：它含 OOS 段，用它选参即信息泄漏。
        results = self._run_parallel_backtests(
            param_combinations, initial_capital, max_workers, context=context)
        full_best_params, full_best_value = self._select_best_params(results, objective)

        best_params, best_value = full_best_params, full_best_value
        selection_results = results
        selection_sample = 'full_sample_no_oos'
        oos_report: Dict[str, Any] | None = None

        if validation == 'oos':
            oos_report = self._validate_out_of_sample(
                param_ranges, initial_capital, max_workers, context,
                objective, oos_ratio, walk_forward_windows,
                oos_horizon, oos_embargo,
            )
            if oos_report.get('enabled'):
                # 严格样本内选参：IS 网格、IS 最优值、IS 稳定性报告三者同源
                best_params = oos_report['in_sample_best_params']
                best_value = oos_report['in_sample_best_value']
                selection_results = oos_report['in_sample_grid']
                selection_sample = 'in_sample'
            else:
                # 切分不可用时也不退回全样本选参，只退回全样本「描述性」网格，
                # 并显式标记本次没有干净的样本外验证，避免调用方误用结果。
                selection_sample = 'full_sample_oos_unavailable'
                logger.warning(
                    "样本外切分不可用，本次优化未完成严格的样本内选参；"
                    "best_params 仅作全样本描述性参考，不可用于实盘决策"
                )
            logger.info(f"样本外验证: {oos_report.get('out_of_sample')}")
        else:
            logger.warning(
                "validation='none'：在全样本网格上选参，存在过拟合/信息泄漏风险；"
                "生产路径请使用默认 validation='oos'"
            )

        logger.info(
            f"参数优化完成: 最佳参数 = {best_params}, 最佳{objective} = {best_value:.4f}"
            f" (选参样本: {selection_sample})")
        self.strategy.set_params(best_params)

        response: Dict[str, Any] = {
            'best_params': best_params,
            'best_value': best_value,
            # 兼容字段：全样本网格参考（不参与选参）
            'optimization_results': results,
            'objective': objective,
            # 本次真正用于选参的样本与结果
            'selection_sample': selection_sample,
            'in_sample_optimization_results': (
                selection_results if selection_sample == 'in_sample' else []),
            # 稳定性报告必须与选参样本同源，不能再看含 OOS 的全样本
            'param_stability_report': self._build_stability_report(
                selection_results, objective),
            'leakage_guard': {
                'strict_in_sample_selection': selection_sample == 'in_sample',
                'full_sample_best_params': full_best_params,
                'full_sample_best_value': full_best_value,
                'selection_sample': selection_sample,
                'oos_evaluations': (
                    1 if oos_report and oos_report.get('enabled') else 0),
                'walk_forward_windows': walk_forward_windows,
                'walk_forward_enabled': bool(
                    oos_report and oos_report.get('walk_forward', {}).get('enabled')),
                # PBO / Deflated Sharpe 尚未实现：显式标注而不是默认声称已校正
                'multiple_testing_correction': 'not_implemented',
            },
        }
        if oos_report is not None:
            response['oos_validation'] = oos_report

        failed = list(self._failed_combinations)
        attempts = self._optimization_attempts
        ratio = (len(failed) / attempts) if attempts else 0.0
        response['failed_combinations'] = failed
        response['failed_ratio'] = ratio
        response['optimization_attempts'] = attempts
        if failed:
            logger.error(
                "参数优化有 {}/{} 组失败（{:.1%}）：{}",
                len(failed), attempts, ratio, failed[:3],
            )
        if ratio > max_failed_ratio:
            raise RuntimeError(
                f"参数优化失败组合占比 {ratio:.1%} 超过上限 {max_failed_ratio:.1%}"
                f"（{len(failed)}/{attempts} 组）；拒绝在残缺网格上选最优参数。"
                "请修复失败原因，或显式调高 max_failed_ratio 并接受结果偏差"
            )
        return response

    def _validate_out_of_sample(self, param_ranges: Dict[str, List[float]],
                                initial_capital: float, max_workers: int,
                                context: Dict[str, Any], objective: str,
                                oos_ratio: float, walk_forward_windows: int,
                                horizon: int = 10, embargo: int = 20) -> Dict[str, Any]:
        """样本内选参 → 样本外验证，并返回衰减指标与稳定性报告。"""
        split = self._split_out_of_sample(
            self.pl_df, oos_ratio, horizon=horizon, embargo=embargo)
        if split is None:
            return {
                'enabled': False,
                'oos_ratio': oos_ratio,
                'horizon': horizon,
                'embargo': embargo,
                'reason': (
                    f'数据仅 {self.pl_df.height} 行，不足以在 purge+embargo 后'
                    '切出样本内/样本外两段'
                ),
            }

        is_df, oos_df = split.is_df, split.oos_df
        bounds = split.bounds
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
            'horizon': horizon,
            'embargo': embargo,
            'purged_train_end': bounds.purged_train_end,
            'purge_gap_days': bounds.purge_gap_days,
            'split_index': bounds.purged_train_end,
            'in_sample_range': self._slice_label(is_df),
            'out_of_sample_range': self._slice_label(oos_df),
            'in_sample_best_params': is_best_params,
            'in_sample_best_value': is_best_value,
            # P0-2：把样本内网格结果回传，供 optimize_strategy 复用同一份结果
            # 做最终选参与稳定性报告，避免为了「修泄漏」再跑一遍网格。
            'in_sample_grid': is_results,
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
                objective, walk_forward_windows, horizon, embargo,
            )
        else:
            # 未启用滚动验证时显式告知原因，避免调用方误以为已做过 walk-forward
            logger.warning(
                f"未启用 walk-forward 滚动验证（walk_forward_windows={walk_forward_windows}，"
                f"需 ≥2）；仅执行一次样本内/样本外切分"
            )
            report['walk_forward'] = {
                'enabled': False,
                'reason': f'walk_forward_windows={walk_forward_windows}，需 ≥2 才启用滚动验证',
            }

        return report

    def _walk_forward_validate(self, param_ranges: Dict[str, List[float]],
                                initial_capital: float, max_workers: int,
                                context: Dict[str, Any], objective: str,
                                windows: int, horizon: int = 10,
                                embargo: int = 20) -> Dict[str, Any]:
        """滚动 walk-forward：逐折用前段选参，在其后一段上验证。

        每折训练窗末尾按标签跨度 purge，测试窗从 ``train_end + embargo``
        开始，训练标签不会跨进测试窗（与学习层 splits 口径一致）。
        """
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
            train_end = chunk * i
            test_start = train_end + embargo
            test_end = min(test_start + chunk, total)
            purged_train_end = min(test_start - horizon - 1, train_end)
            if purged_train_end < 20 or test_end - test_start < 20:
                continue
            train_df = self.pl_df.head(purged_train_end)
            test_df = self.pl_df.slice(test_start, test_end - test_start)
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
                'purged_train_end': purged_train_end,
                'purge_gap_days': test_start - purged_train_end,
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
            'enabled': bool(folds),
            'windows': windows,
            'horizon': horizon,
            'embargo': embargo,
            'folds': folds,
            'positive_ratio': positive_ratio,
            'mean_oos_total_return': sum(oos_returns) / len(oos_returns) if oos_returns else 0.0,
        }

    def _should_use_process_pool(self, frame: Any, param_combinations: List[Dict[str, float]]) -> bool:
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

    def _run_process_pool_backtests(self, frame: Any, param_combinations: List[Dict[str, float]],
                                    initial_capital: float, max_workers: int,
                                    context: Dict[str, Any] | None) -> List[Dict[str, Any]]:
        """用进程池并行执行回测并收集结果。"""
        import concurrent.futures
        import os

        results = []
        # 纯 Python 回测循环受 GIL 限制，线程无法并行 CPU 计算，改用进程池利用多核
        workers = max(1, min(max_workers, len(param_combinations), os.cpu_count() or 1))
        # 行情数据在池启动时注入一次，各参数组合只传参数字典，
        # 避免 N 个组合各 pickle 一遍大 DataFrame
        payload = {
            'engine_cls': BacktestEngine,
            'strategy': self.strategy,
            'data': frame,
            'cost_model': self.cost_model,
            'initial_capital': initial_capital,
            'risk_engine': self.risk_engine,
            'context': context,
        }
        with concurrent.futures.ProcessPoolExecutor(
                max_workers=workers, initializer=init_worker, initargs=(payload,)
        ) as executor:
            future_to_param = {
                executor.submit(run_backtest_worker_cached, params): params
                for params in param_combinations
            }

            for future in concurrent.futures.as_completed(future_to_param):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:  # noqa: BLE001 - 失败组合必须入台账，不得静默丢弃
                    params = future_to_param[future]
                    logger.error(f"参数组合 {params} 回测失败: {exc}")
                    self._failed_combinations.append({
                        'params': params, 'error': f'{type(exc).__name__}: {exc}',
                        'executor': 'process_pool',
                    })

        return results

    def _run_thread_pool_backtests(self, frame: Any, param_combinations: List[Dict[str, float]],
                                   initial_capital: float, max_workers: int,
                                   context: Dict[str, Any] | None) -> List[Dict[str, Any]]:
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
                except Exception as exc:  # noqa: BLE001 - 失败组合必须入台账，不得静默丢弃
                    params = future_to_param[future]
                    logger.error(f"参数组合 {params} 回测失败: {exc}")
                    self._failed_combinations.append({
                        'params': params, 'error': f'{type(exc).__name__}: {exc}',
                        'executor': 'thread_pool',
                    })

        return results

    def _run_parallel_backtests(self, param_combinations: List[Dict[str, float]],
                                initial_capital: float, max_workers: int,
                                context: Dict[str, Any] | None = None,
                                data: Any = None) -> List[Dict[str, Any]]:
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
        self._optimization_attempts += len(param_combinations)

        if self._should_use_process_pool(frame, param_combinations):
            return self._run_process_pool_backtests(
                frame, param_combinations, initial_capital, max_workers, context
            )
        return self._run_thread_pool_backtests(
            frame, param_combinations, initial_capital, max_workers, context
        )

    def _run_single_backtest(self, params: Dict[str, float], initial_capital: float,
                             context: Dict[str, Any] | None, data: Any) -> Dict[str, Any]:
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
