#!/usr/bin/env python3

"""
run_backtest —— 回测唯一用例入口。

CLI / GUI / 服务端 都应该调用这个函数，不要各自 new BacktestEngine 然后手写循环。
风控闸门默认装配安全规则集（涨跌停不可成交 + 停牌不可交易，与撮合内核
既有判定同源，不改变成交结果）；需要权重/行业/回撤等规则时显式传 risk_engine，
需要完全关闭风控时传 enable_default_risk=False。

Usage:
    from src.core.usecases import run_backtest
    from src.risk import RiskEngine, MaxPositionWeight, MaxDrawdownCircuit
    from src.backtest.strategies.ma_strategy import MAStrategy

    risk = RiskEngine([MaxPositionWeight(0.10), MaxDrawdownCircuit(0.15)])
    result = run_backtest(
        ts_code='000001.SZ',
        start_date='2023-01-01',
        end_date='2024-01-01',
        strategy=MAStrategy(),
        risk_engine=risk,
        initial_capital=1_000_000,
    )
    print(result['risk_summary'])   # 风控拒单情况
    print(result['total_return'])
"""

from __future__ import annotations

import copy
import datetime as dt
import threading
from collections.abc import Mapping
from typing import Any, Callable, Dict, Union

import polars as pl
from loguru import logger

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.holdout import holdout_bounds
from src.backtest.engine.input_contract import (
    BAR_CONTRACT,
    ROLE_DATE,
    ROLE_RAW_CLOSE,
    ROLE_RAW_OPEN,
    ROLE_VOLUME,
    validate_universe,
)
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.data.quality.code_migration import load_entity_index
from src.risk import (
    DEFAULT_RISK_LIMITS,
    RiskEngine,
    default_risk_engine,
    single_asset_risk_engine,
)

# 长驻取数资源：CLI / GUI 会反复调用 run_backtest，若每次新建
# DatabaseManager 与 DataFetcher，连接池与线程池随之反复创建，
# 线程数随调用次数单调增长。这里进程内只保留一份，跨调用复用。
_shared_lock = threading.Lock()
_shared_fetcher: Any = None
_shared_db_manager: Any = None


def _get_shared_data_fetcher() -> tuple[Any, Any]:
    """获取（或懒创建）进程内长驻的 ``(DataFetcher, DatabaseManager)``。"""
    global _shared_fetcher, _shared_db_manager
    with _shared_lock:
        if _shared_fetcher is None or _shared_db_manager is None:
            from src.data.managers.data_fetcher import DataFetcher
            from src.database.db_manager import DatabaseManager
            from src.utils.config import Config

            db = DatabaseManager(Config())
            db.connect()
            _shared_db_manager = db
            _shared_fetcher = DataFetcher(db.config, db_manager=db)
        return _shared_fetcher, _shared_db_manager


def close_shared_data_fetcher() -> None:
    """关闭进程内长驻取数资源（测试清理 / 进程退出钩子使用），可重复调用。"""
    global _shared_fetcher, _shared_db_manager
    with _shared_lock:
        fetcher, db = _shared_fetcher, _shared_db_manager
        _shared_fetcher = None
        _shared_db_manager = None
    if fetcher is not None:
        fetcher.close()
    if db is not None:
        db.disconnect()


def _assemble_risk_engine(
    risk_engine: RiskEngine | None,
    enable_default_risk: bool,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None,
    ts_code: str | None,
    industry: str,
    industry_map: Dict[str, str] | None,
) -> tuple[RiskEngine | None, Dict[str, Any]]:
    """风控装配：显式传入优先；否则按单票/组合模式装配默认规则集。

    默认集补全后不能对单票账户直接套用 10%/30% 上限：单票天然满仓且只有
    一个行业，照搬会让回测 0 成交。因此：
    - 单票：权重上限 100%，关闭行业/HHI，保留回撤/现金/流动性；
    - 组合：权重上限 ``max(10%, 1/N + 1pp)``，行业上限按行业数可行域放宽
      （``max(30%, 1/m + 3pp)``、HHI ``max(25%, 1/m + 5pp)``），
      无行业映射时关闭行业规则；
    - 数据缺 volume 列时关闭成交量/ADV 规则（否则 fail-closed 会拒掉全部订单）。

    返回 ``(engine, profile)``；profile 记录实际生效的参数与放宽理由，
    供报告审计。
    """
    if risk_engine is not None:
        return risk_engine, {
            'source': 'explicit',
            'rules': [rule.name for rule in risk_engine.rules],
            'notes': ['调用方显式传入 risk_engine，默认集未生效'],
        }
    if not enable_default_risk:
        return None, {
            'source': 'disabled',
            'rules': [],
            'notes': ['enable_default_risk=False，完全关闭风控闸门'],
        }

    is_portfolio = isinstance(data, dict)
    codes = list(data.keys()) if isinstance(data, dict) else ([ts_code] if ts_code else [])
    frames = list(data.values()) if isinstance(data, dict) else ([data] if data is not None else [])
    volume_column = BAR_CONTRACT.column(ROLE_VOLUME)
    # volume 已是行情必需列（缺列由输入契约直接抛错），这里只做装配期预判：
    # 空面板不参与判定，避免把"无数据"误算成"缺 volume"
    has_volume = all(
        frame is None or volume_column in frame.columns for frame in frames)
    notes: list[str] = []

    if not is_portfolio:
        engine = single_asset_risk_engine()
        if not has_volume:
            engine = default_risk_engine(
                position_limit=1.0, include_industry=False, include_liquidity=False)
            notes.append('数据缺 volume 列，已关闭单日成交量/ADV 参与率规则')
        profile = {
            'source': 'default',
            'mode': 'single',
            'rules': [rule.name for rule in engine.rules],
            'effective_limits': {
                'max_position_weight': 1.0,
                'max_industry_weight': None,
                'max_industry_hhi': None,
                'max_drawdown': DEFAULT_RISK_LIMITS['max_drawdown'],
                'drawdown_window': DEFAULT_RISK_LIMITS['drawdown_window'],
                'min_cash_reserve': DEFAULT_RISK_LIMITS['min_cash_reserve'],
                'max_order_volume_ratio': (
                    DEFAULT_RISK_LIMITS['max_order_volume_ratio'] if has_volume else None),
                'max_adv_participation': (
                    DEFAULT_RISK_LIMITS['max_adv_participation'] if has_volume else None),
            },
            'notes': [
                '单票账户天然满仓且只有一个行业：行业/HHI 规则关闭，'
                '权重上限放宽到 100%，避免默认集把回测压成 0 成交',
                *notes,
            ],
        }
        return engine, profile

    n_codes = len(codes)
    position_limit = max(
        DEFAULT_RISK_LIMITS['max_position_weight'],
        (1.0 / n_codes if n_codes else 1.0) + 0.01,
    )
    if position_limit > DEFAULT_RISK_LIMITS['max_position_weight']:
        notes.append(
            f'组合仅 {n_codes} 只，等权目标 {1.0 / n_codes:.0%} 高于默认 10%，'
            f'单票上限放宽为 {position_limit:.0%}（等权 +1pp 漂移余量，'
            '避免整手取整/价格波动把正常调仓判为超限）')

    mapping = dict(industry_map or {})
    missing = [code for code in codes if not mapping.get(code)]
    include_industry = bool(codes) and not missing and n_codes >= 2
    industry_limit = hhi_limit = None
    if include_industry:
        n_industries = len({mapping[code] for code in codes})
        # 可行域 = 行业等权下限；再给 3pp / 5pp 漂移余量，否则 2 只同行业
        # 组合在价格微涨后就会从 28.6% 漂到 30%+ 被反复拒单
        industry_limit = max(DEFAULT_RISK_LIMITS['max_industry_weight'],
                             1.0 / n_industries + 0.03)
        hhi_limit = max(DEFAULT_RISK_LIMITS['max_industry_hhi'],
                        1.0 / n_industries + 0.05)
        if industry_limit > DEFAULT_RISK_LIMITS['max_industry_weight']:
            notes.append(
                f'组合仅覆盖 {n_industries} 个行业，行业上限按可行域放宽为 '
                f'{industry_limit:.0%}，HHI 上限 {hhi_limit:.0%}'
                '（等权下限 + 漂移余量）')
    else:
        if missing:
            notes.append(
                f'行业映射缺失 {len(missing)} 只（示例 {missing[:3]}），'
                '已关闭行业权重/HHI 规则；如需行业约束请传 industry_map')
        else:
            notes.append('组合标的 <2，行业集中度规则无意义，已关闭')

    if not has_volume:
        notes.append('数据缺 volume 列，已关闭单日成交量/ADV 参与率规则')

    engine = default_risk_engine(
        position_limit=position_limit,
        industry_limit=industry_limit,
        hhi_limit=hhi_limit,
        include_industry=include_industry,
        include_liquidity=has_volume,
    )
    return engine, {
        'source': 'default',
        'mode': 'portfolio',
        'rules': [rule.name for rule in engine.rules],
        'effective_limits': {
            'max_position_weight': position_limit,
            'max_industry_weight': industry_limit,
            'max_industry_hhi': hhi_limit,
            'max_drawdown': DEFAULT_RISK_LIMITS['max_drawdown'],
            'drawdown_window': DEFAULT_RISK_LIMITS['drawdown_window'],
            'min_cash_reserve': DEFAULT_RISK_LIMITS['min_cash_reserve'],
            'max_order_volume_ratio': (
                DEFAULT_RISK_LIMITS['max_order_volume_ratio'] if has_volume else None),
            'max_adv_participation': (
                DEFAULT_RISK_LIMITS['max_adv_participation'] if has_volume else None),
        },
        'notes': notes or ['默认阈值全部生效，无需放宽'],
    }


# ---------------------------------------------------------------------------
# 样本内/样本外（purge + embargo）
# ---------------------------------------------------------------------------

def _sorted_dates(data: Union[pl.DataFrame, Dict[str, pl.DataFrame]]) -> list[Any]:
    """提取数据覆盖的升序交易日（组合取各标的并集）。"""
    frames = list(data.values()) if isinstance(data, dict) else [data]
    date_column = BAR_CONTRACT.column(ROLE_DATE)
    dates: set[Any] = set()
    for frame in frames:
        if frame is not None and date_column in frame.columns:
            dates.update(frame[date_column].drop_nulls().to_list())
    return sorted(dates, key=str)


def _slice_data(data: Union[pl.DataFrame, Dict[str, pl.DataFrame]],
                start: Any, end: Any) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
    """按日期闭区间裁剪数据；组合分支丢弃区间内无行情的标的。"""
    date_column = BAR_CONTRACT.column(ROLE_DATE)

    def _filter(frame: pl.DataFrame) -> pl.DataFrame:
        if date_column not in frame.columns:
            return frame
        return frame.filter(
            (pl.col(date_column) >= start) & (pl.col(date_column) <= end))

    if isinstance(data, dict):
        sliced = {code: _filter(frame) for code, frame in data.items()}
        return {code: frame for code, frame in sliced.items() if frame.height > 0}
    return _filter(data)


def _segment_metrics(result: Dict[str, Any]) -> Dict[str, Any]:
    """从单次回测结果抽取分段指标（收益/年化/回撤/夏普/换手/拒单）。"""
    performance = result.get('performance', {}) or {}
    trades = result.get('trades', []) or []
    initial = float(result.get('initial_capital', 0.0) or 0.0)
    traded_notional = sum(
        abs(float(trade.get('price', 0.0) or 0.0) * float(trade.get('shares', 0) or 0))
        for trade in trades
    )
    return {
        'total_return': float(result.get('total_return', 0.0) or 0.0),
        'annual_return': float(performance.get('annual_return', 0.0) or 0.0),
        'max_drawdown': float(performance.get('max_drawdown', 0.0) or 0.0),
        'sharpe_ratio': float(performance.get('sharpe_ratio', 0.0) or 0.0),
        'trades': len(trades) // 2,
        'turnover': (traded_notional / initial) if initial > 0 else 0.0,
        'rejected': len(result.get('rejected_orders', []) or []),
        'final_equity': float(result.get('final_equity', 0.0) or 0.0),
    }


def _run_oos_validation(
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]],
    run_segment: Callable[[Any], Dict[str, Any]],
    oos_ratio: float,
    horizon: int,
    embargo: int,
) -> Dict[str, Any]:
    """对固定参数策略做样本内/样本外分段回测（含 purge + embargo）。

    注意：``run_backtest`` 不做参数选择，因此这里的样本外是「时间切分
    稳健性检查」；真正的「样本内选参 → 样本外验证」在
    ``BacktestEngine.optimize_strategy``。
    """
    dates = _sorted_dates(data)
    bounds = holdout_bounds(len(dates), oos_ratio, horizon=horizon, embargo=embargo)
    if bounds is None:
        return {
            'enabled': False,
            'oos_ratio': oos_ratio,
            'horizon': horizon,
            'embargo': embargo,
            'reason': (
                f'仅 {len(dates)} 个交易日，purge+embargo 后不足以切出'
                '样本内/样本外两段'
            ),
        }

    is_data = _slice_data(data, dates[0], dates[bounds.purged_train_end - 1])
    oos_data = _slice_data(
        data, dates[bounds.test_start], dates[bounds.test_end - 1])
    is_empty = (not is_data) if isinstance(is_data, dict) else is_data.is_empty()
    oos_empty = (not oos_data) if isinstance(oos_data, dict) else oos_data.is_empty()
    if is_empty or oos_empty:
        return {
            'enabled': False,
            'oos_ratio': oos_ratio,
            'horizon': horizon,
            'embargo': embargo,
            'reason': '切分后样本内或样本外数据为空',
        }

    is_result = run_segment(is_data)
    oos_result = run_segment(oos_data)
    is_metrics = _segment_metrics(is_result)
    oos_metrics = _segment_metrics(oos_result)
    return {
        'enabled': True,
        'oos_ratio': oos_ratio,
        'horizon': horizon,
        'embargo': embargo,
        'purged_train_end': bounds.purged_train_end,
        'purge_gap_days': bounds.purge_gap_days,
        'in_sample_range': f'{dates[0]} ~ {dates[bounds.purged_train_end - 1]}',
        'out_of_sample_range': f'{dates[bounds.test_start]} ~ {dates[-1]}',
        'in_sample_days': bounds.purged_train_end,
        'out_of_sample_days': bounds.test_end - bounds.test_start,
        'in_sample': is_metrics,
        'out_of_sample': oos_metrics,
        'decay': {
            'total_return': oos_metrics['total_return'] - is_metrics['total_return'],
            'annual_return': oos_metrics['annual_return'] - is_metrics['annual_return'],
            'sharpe_ratio': oos_metrics['sharpe_ratio'] - is_metrics['sharpe_ratio'],
        },
        'note': (
            '固定参数的时间切分检查；训练窗末尾按 horizon 做标签 purge，'
            '测试窗从 train_end + embargo 开始'
        ),
    }


def _prepare_backtest_data(
    ts_code: str | None,
    start_date: str | None,
    end_date: str | None,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None,
    db_manager: Any,
    ts_codes: list[str] | None = None,
) -> tuple[Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None, Any]:
    """数据准备：data 已提供时直接返回；否则按需取数并返回可复用的 db 句柄。"""
    db = db_manager
    if data is not None or not (ts_code or ts_codes):
        return data, db

    # 按需 import 避免循环依赖
    from src.data.managers.data_fetcher import DataFetcher

    if db is None:
        # 默认路径复用进程内长驻实例；本次调用不负责关闭它
        fetcher, db = _get_shared_data_fetcher()
    else:
        # 调用方显式持有 db_manager，连接复用，避免每次回测新建连接池
        if not db.is_connected():
            db.connect()
        fetcher = DataFetcher(db.config, db_manager=db)
    if ts_codes:
        # 组合/横截面一次加载面板，避免逐票重复路由
        # 同一实体的旧码已被新码完整覆盖时先去重，避免同一只票重复计入
        codes = load_entity_index().dedupe(ts_codes)
        return fetcher.get_stock_batch(
            codes, start_date or '', end_date or '', adjustment_type='hfq',
        ), db
    data = fetcher.get_stock_data(
        ts_code, start_date or '', end_date or '',
        adjustment_type='hfq',
    )
    return data, db


def _load_st_status(
    st_status: Dict[str, Any] | None,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None,
    ts_code: str | None,
    start_date: str | None,
    end_date: str | None,
    db: Any,
    db_manager: Any,
    warnings_out: list[dict] | None = None,
) -> Dict[str, Any] | None:
    """ST 时间维表：未显式传入时按标的与日期范围加载。

    ``warnings_out`` 收集降级事件：P0-4 起加载失败不再只写日志后静默按
    "未知/无事件"处理，调用方必须能从回测结果里看到 ``data_quality.degraded``。

    优先读本地历史区间表（``meta/st``，含可信窗口内的显式非 ST 段），
    本地无记录再回退数据库维表；两者都没有才按未知处理。
    """
    if st_status is not None:
        return st_status
    if data is None:
        return None
    if isinstance(data, dict):
        codes = list(data.keys())
    else:
        codes = [ts_code] if ts_code else []
    if not codes:
        return None
    try:
        from src.data import st_history
        from src.data.local_store.store import resolve_store_root
        from src.utils.config import get_config

        local = st_history.intervals_for_backtest(
            resolve_store_root(get_config()), codes, start_date, end_date)
        if local:
            return local
    # 兜底理由：本地区间表读取失败时继续走数据库维表，不阻断回测
    except Exception as e:  # noqa: BLE001
        logger.warning(f"读取本地 ST 历史区间表失败，回退数据库维表: {e}")
        if warnings_out is not None:
            warnings_out.append({
                'stage': 'st_local_history', 'severity': 'fallback',
                'fallback': 'database_dimension', 'error': f'{type(e).__name__}: {e}',
            })
    st_db = db if db is not None else db_manager
    if st_db is None:
        return None
    try:
        from src.data.services.st_status_service import StStatusService

        return StStatusService(db_manager=st_db).get_status_intervals(
            codes, start_date, end_date,
        )
    # ST 维表加载失败会按「ST 状态未知」处理，涨跌停档位不可信 → 降级
    except Exception as e:  # noqa: BLE001
        logger.warning(f"加载 ST 时间维表失败，按未知处理: {e}")
        if warnings_out is not None:
            warnings_out.append({
                'stage': 'st_database_dimension', 'severity': 'degraded',
                'impact': 'ST 状态未知，涨跌停按板块默认幅度判定',
                'error': f'{type(e).__name__}: {e}',
            })
        return None


def _load_local_dividends(
    codes: list[str], db: Any, db_manager: Any,
    warnings_out: list[dict] | None = None,
) -> Dict[str, pl.DataFrame]:
    """从本地列存分红快照加载除权事件（只读；任何失败均降级为空并留痕）。

    不经过 ``DatabaseManager`` / 远程接口：分红数据缺失只意味着本次回测不记账
    除权事件，不会阻塞回测，也不会静默从远端补数。
    """
    if not codes:
        return {}
    config = getattr(db, 'config', None) or getattr(db_manager, 'config', None)
    if config is None:
        try:
            from src.utils.config import Config
            config = Config()
        # 配置不可用 → 本次回测不记账任何分红，净值会偏离真实可交易结果
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"分红配置不可用，跳过除权事件: {exc}")
            if warnings_out is not None:
                warnings_out.append({
                    'stage': 'dividend_config', 'severity': 'degraded',
                    'impact': '不记账现金分红/送转/配股',
                    'error': f'{type(exc).__name__}: {exc}',
                })
            return {}
    try:
        from src.backtest.engine.corporate_actions import load_local_dividend_frames
        frames = load_local_dividend_frames(codes, config)
        if not frames and warnings_out is not None:
            # 快照整体缺失/未命中不再静默：结果里必须能看到降级标记
            warnings_out.append({
                'stage': 'dividend_local_snapshot',
                'severity': 'degraded',
                'impact': '所有标的除权事件按「无事件」处理，现金分红/送转缺失',
                'error': f'分红快照未命中任何标的（请求 {len(codes)} 只）',
            })
        return frames
    # 快照损坏/缺失依旧跑完回测，但结果必须标记 degraded
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"加载本地分红快照失败，跳过除权事件: {exc}")
        if warnings_out is not None:
            warnings_out.append({
                'stage': 'dividend_local_snapshot', 'severity': 'degraded',
                'impact': '除权除息事件按「无事件」处理，现金分红/送转缺失',
                'error': f'{type(exc).__name__}: {exc}',
            })
        return {}


def _as_date(value: Any) -> Any:
    """把 datetime 规整成 date，便于与 last_trade_date 比较。"""
    return value.date() if isinstance(value, dt.datetime) else value


def _infer_backtest_window(
    data: Dict[str, pl.DataFrame],
    start_date: str | None,
    end_date: str | None,
) -> tuple[Any, Any]:
    """回测区间：显式参数优先，否则从传入面板推断（用于 PIT 停更过滤）。"""
    start = dt.date.fromisoformat(start_date) if start_date else None
    end = dt.date.fromisoformat(end_date) if end_date else None
    date_column = BAR_CONTRACT.column(ROLE_DATE)
    mins: list[Any] = []
    maxs: list[Any] = []
    for panel in data.values():
        if (isinstance(panel, pl.DataFrame) and date_column in panel.columns
                and panel.height):
            mins.append(_as_date(panel[date_column].min()))
            maxs.append(_as_date(panel[date_column].max()))
    valid_mins = [v for v in mins if v is not None]
    valid_maxs = [v for v in maxs if v is not None]
    if start is None and valid_mins:
        start = min(valid_mins)
    if end is None and valid_maxs:
        end = max(valid_maxs)
    return start, end


def _filter_stale_universe(
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]],
    strategies: Dict[str, BaseStrategy] | None,
    enabled: bool,
    warnings_out: list[dict],
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[
    Union[pl.DataFrame, Dict[str, pl.DataFrame]],
    Dict[str, BaseStrategy] | None,
    dict | None,
]:
    """组合回测股票池：按停更/退市状态表做点时（PIT）过滤。

    单票回测由调用方显式指定，不做强制过滤（原有 warning 保留）。
    状态表带 ``last_trade_date`` 时启用 PIT 口径：只剔除「回测开始前已停更」
    的标的。若用「当前是否停更」整段砍历史池，会把回测期结束后才退市的
    公司提前剔掉，属于前视偏差；区间内停更的标的保留但记 warning。
    状态表缺失或读取失败时 fail-open，但记为 degraded，避免静默放过。
    """
    if not enabled or not isinstance(data, dict) or not data:
        return data, strategies, None
    # 价格恒为 1.0 的占位桩基金不是「停更」而是「数据本身无效」，不受 PIT
    # 窗口约束，必须无条件剔除；清单由 anomaly_lists 自动维护（含尾部占位）。
    from src.data.quality.quarantine import placeholder_codes  # noqa: PLC0415

    placeholder = placeholder_codes()
    pit_cutoff: str | None = None
    late_stale: list[str] = []
    try:
        from src.data.quality import stale_status as _stale  # noqa: PLC0415

        frame = _stale.load_stale_status()
        if frame is None:
            warnings_out.append({
                'stage': 'stale_universe_filter', 'severity': 'degraded',
                'impact': '停更状态表缺失，组合回测未剔除退市/源端停更标的',
                'error': f'missing: {_stale.status_path()}',
            })
            return data, strategies, None
        excluded = _stale.stale_codes()
        if 'last_trade_date' in frame.columns:
            start, end = _infer_backtest_window(data, start_date, end_date)
            rows = list(frame.filter(pl.col('action') == 'exclude').select(
                ['ts_code', 'last_trade_date']).iter_rows(named=True))
            if start is not None:
                pit_cutoff = start.isoformat()
                excluded = {
                    row['ts_code'] for row in rows
                    if row['last_trade_date'] is not None
                    and _as_date(row['last_trade_date']) < start
                }
                late_stale = sorted(set(data) & {
                    row['ts_code'] for row in rows
                    if row['last_trade_date'] is not None
                    and _as_date(row['last_trade_date']) >= start
                    and (end is None or _as_date(row['last_trade_date']) < end)
                })
                if late_stale:
                    warnings_out.append({
                        'stage': 'stale_universe_filter', 'severity': 'warning',
                        'impact': f'{len(late_stale)} 只标的在回测区间内停更，'
                                  '停更后使用陈旧价格，收益需人工复核',
                        'error': 'late_stale_codes: ' + ','.join(late_stale[:10]),
                    })
    except Exception as exc:  # noqa: BLE001
        warnings_out.append({
            'stage': 'stale_universe_filter', 'severity': 'degraded',
            'impact': '停更状态表读取失败，组合回测未剔除退市/源端停更标的',
            'error': f'{type(exc).__name__}: {exc}',
        })
        return data, strategies, None

    # 占位桩不属于 PIT 停更口径，无条件并入剔除集合
    excluded = set(excluded) | set(placeholder)
    removed = sorted(set(data) & excluded)
    stats = {
        'enabled': True,
        'universe_before': len(data),
        'universe_after': len(data) - len(removed),
        'excluded_count': len(removed),
        'excluded_codes': removed[:20],
        'placeholder_excluded_count': len(set(data) & set(placeholder)),
    }
    if pit_cutoff:
        stats['mode'] = 'pit'
        stats['pit_cutoff'] = pit_cutoff
        stats['late_stale_count'] = len(late_stale)
        stats['late_stale_codes'] = late_stale[:20]
    if not removed:
        return data, strategies, stats

    kept = {code: panel for code, panel in data.items() if code not in excluded}
    if not kept:
        raise ValueError(
            '组合回测股票池经停更过滤后为空；如需保留退市股请传 exclude_stale=False'
        )
    if strategies:
        strategies = {
            code: strategy for code, strategy in strategies.items() if code in kept
        }
    logger.warning(
        '组合回测已剔除 {} 只停更/退市标的（{} → {}），'
        '避免幸存者偏差；可用 exclude_stale=False 显式关闭',
        len(removed), len(data), len(kept),
    )
    return kept, strategies, stats


def _run_portfolio_backtest(
    data: Dict[str, pl.DataFrame],
    strategies: Dict[str, BaseStrategy] | None,
    risk_engine: RiskEngine | None,
    st_status: Dict[str, Any] | None,
    initial_capital: float,
    lot_size: int,
    industry_map: Dict[str, str] | None,
    kwargs: Dict[str, Any],
    risk_gate_policy: str = 'adv_gate',
) -> Dict[str, Any]:
    """组合回测分支。"""
    strategies = strategies or {}
    if not strategies:
        raise ValueError("组合回测需要 strategies dict")

    engine = PortfolioBacktestEngine(data)
    # 装配层显式声明输入契约与价格口径（内核不再持有列名归属）
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = risk_engine
    # enable_default_risk=False 是显式关闭，必须透传 'off'；
    # 否则内核会按 A1 默认兜底装配 ADV 闸门，与"完全关闭"矛盾
    engine.risk_gate_policy = risk_gate_policy
    engine.set_strategies(strategies)

    # 显式 st_status 参数优先；调用方 kwargs 里已有的同名参数不覆盖
    if st_status:
        kwargs.setdefault('st_status', st_status)
    return engine.run_backtest(
        initial_capital=initial_capital,
        lot_size=lot_size,
        industry_map=industry_map or {},
        **kwargs,
    )


def _run_single_backtest(
    data: pl.DataFrame,
    strategy: BaseStrategy | None,
    risk_engine: RiskEngine | None,
    st_status: Dict[str, Any] | None,
    ts_code: str | None,
    initial_capital: float,
    lot_size: int,
    industry: str,
    kwargs: Dict[str, Any],
    risk_gate_policy: str = 'adv_gate',
) -> Dict[str, Any]:
    """单票回测分支。"""
    if strategy is None:
        raise ValueError("单票回测需要 strategy")

    single_engine = BacktestEngine(data)
    # 装配层显式声明输入契约与价格口径（内核不再持有列名归属）
    single_engine.input_contract = BAR_CONTRACT
    single_engine.price_basis = 'auto'
    single_engine.risk_engine = risk_engine
    # enable_default_risk=False 是显式关闭，必须透传 'off'
    single_engine.risk_gate_policy = risk_gate_policy
    single_engine.set_strategy(strategy)

    if 'is_st' not in kwargs:
        kwargs['is_st'] = st_status.get(ts_code) if st_status and ts_code else None

    return single_engine.run_backtest(
        initial_capital=initial_capital,
        lot_size=lot_size,
        ts_code=ts_code or '',
        industry=industry,
        **kwargs,
    )


def run_backtest(
    ts_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None = None,
    strategy: BaseStrategy | None = None,
    strategies: Dict[str, BaseStrategy] | None = None,
    risk_engine: RiskEngine | None = None,
    enable_default_risk: bool = True,
    initial_capital: float = 1_000_000.0,
    lot_size: int = 100,
    industry: str = '',
    industry_map: Dict[str, str] | None = None,
    st_status: Dict[str, Any] | None = None,
    db_manager: Any = None,
    ts_codes: list[str] | None = None,
    apply_dividends: bool = True,
    exclude_stale: bool = True,
    dividend_tax_rate: float = 0.0,
    rights_policy: str = 'partial',
    dividends: Mapping[str, pl.DataFrame] | None = None,
    oos_ratio: float = 0.3,
    oos_horizon: int = 10,
    oos_embargo: int = 20,
    **kwargs,
) -> Dict[str, Any]:
    """
    运行回测（单票 或 组合）。

    两种模式：
        1. 单票回测：传 ts_code + strategy，或 data（单票 DataFrame）+ strategy
        2. 组合回测：传 data（dict of DataFrame）+ strategies（dict of Strategy）

    Args:
        ts_code:       单票代码（'000001.SZ'）。传了就会用 DataFetcher 取数。
        start_date:    '2023-01-01' 字符串。
        end_date:      '2024-01-01' 字符串。
        data:          直接传入数据（跳过取数）。单票=pl.DataFrame，组合=Dict[str, pl.DataFrame]。
        strategy:      单票策略实例。
        strategies:    组合策略字典 {ts_code: BaseStrategy}。
        risk_engine:   风控引擎。None 时按 enable_default_risk 装配默认安全规则集。
        enable_default_risk: 默认 True——装配「涨跌停不可成交 + 停牌不可交易」，
                       与撮合内核既有判定同源、不改变成交结果；False 表示完全关闭风控。
        initial_capital: 初始资金。
        lot_size:      最小交易单位（A股 100 股）。
        industry:      单票行业标签（风控 MaxIndustryWeight 用）。
        industry_map:  组合行业映射 {ts_code: industry}。
        st_status:     ST 状态映射，值可为 bool / None / 区间列表。
                       不传时若持有 db_manager 会自动从 ST 时间维表加载。
        db_manager:    可选的数据库管理器；传入后即使 data 已提供也会加载
                       ST 时间维表。为 None 且需要取数时自动创建。
        ts_codes:      组合回测的标的列表；未传 data 时一次批量加载面板。
        apply_dividends: 默认 True——从本地分红快照（stock_dividend）加载除权
                       除息事件，按「原始价成交 + 分红入现金」记账，权益口径为
                       现金 + Σ(股数 × 原始收盘价)。只在原始价口径下启用；
                       显式传入的 dividends 优先，不再自动加载。
        dividend_tax_rate: 现金分红税率（0~1），默认 0；未建模持股期限差异化税率。
        rights_policy:  配股现金不足处置：'partial'（按现金能力部分认购）或
                       'abandon'（放弃配股）。
        dividends:      显式指定的除权除息事件表 {ts_code: DataFrame}，
                       传给 apply_dividends=False 时完全不做分红记账。
        exclude_stale:  默认 True——组合回测按 meta/universe/stale_status.parquet
                       整段剔除 action='exclude' 的停更/退市标的，并同步裁剪
                       strategies；单票回测不强制过滤（保留 warning）。
                       传 False 可显式保留退市股，用于幸存者偏差对照实验。
        oos_ratio:     样本外占比，默认 0.3（训练 70% / 样本外 30%）。
                       默认开启的理由：固定参数回测也常被拿来做决策，30% 的
                       留出段足以覆盖一轮信用/风格周期；传 0 可显式关闭。
                       启用后会额外跑样本内、样本外两次回测，耗时约 ×3。
        oos_horizon:   标签跨度（交易日），默认 10；训练窗末尾按该跨度 purge。
        oos_embargo:   训练窗与测试窗隔离带（交易日），默认 20，须 ≥ horizon。

    Returns:
        dict —— BacktestEngine.run_backtest() 的返回值，包含：
            - initial_capital, final_equity, total_return
            - trades, equity_curve, performance
            - risk_summary（如有风控）、rejected_orders（如有风控）
            - risk_profile（本次实际生效的风控规则与参数）
            - oos_validation（样本内/样本外分段指标；未启用时 enabled=False）
    """
    if not 0 <= oos_ratio < 1:
        raise ValueError(f"oos_ratio 必须在 [0, 1) 内，实际 {oos_ratio}")

    # ---- 数据准备 + ST 时间维表 ----
    data, db = _prepare_backtest_data(
        ts_code, start_date, end_date, data, db_manager, ts_codes,
    )
    # P0-4：所有数据降级事件统一收集，最终写入结果 data_quality
    data_warnings: list[dict] = []
    st_status = _load_st_status(
        st_status, data, ts_code, start_date, end_date, db, db_manager,
        warnings_out=data_warnings,
    )

    if data is None:
        raise ValueError("run_backtest: 必须传 data 或 ts_code")

    # ---- 输入契约：装配层统一声明（缺必需列/列名不符/复权退化直接抛错）----
    # 换数据源只改 src.backtest.engine.input_contract 的列映射；
    # price_basis='auto'：带 raw_* 时按复权价校验退化，无 raw_* 视为原始价输入。
    validate_universe(
        data if isinstance(data, dict) else {ts_code or '__single__': data},
        contract=BAR_CONTRACT, price_basis='auto',
        context='run_backtest 行情 ',
    )

    # 组合回测股票池收口：剔除停更/退市标的，避免幸存者偏差。
    # 必须在风控装配与 OOS 分段之前完成，保证 IS/OOS 使用同一池。
    data, strategies, universe_filter = _filter_stale_universe(
        data, strategies, exclude_stale, data_warnings, start_date, end_date,
    )

    # 单票回测由用户显式指定，不强制剔除退市股，但必须留下可观测提示：
    # 停更标的在区间末端会用陈旧价格，直接采信收益会引入退市偏差。
    if ts_code:
        from src.data.quality.stale_status import is_stale_code  # noqa: PLC0415

        if is_stale_code(ts_code):
            logger.warning(
                f'回测标的 {ts_code} 在停更状态表中标记为 exclude（退市/源端停更），'
                '区间末端可能是陈旧价格；结论请结合退市偏差人工复核'
            )

    # ---- 风控装配：显式优先；否则按单票/组合模式装配默认安全集 ----
    risk_engine, risk_profile = _assemble_risk_engine(
        risk_engine, enable_default_risk, data, ts_code, industry, industry_map,
    )
    # 只有显式 enable_default_risk=False 才是「完全关闭」；其余情况由内核
    # A1 默认 ADV 闸门兜底（risk_engine 已装配时该策略不生效）
    risk_gate_policy = 'off' if risk_engine is None else 'adv_gate'

    # ---- 除权除息：只读本地分红快照，运行时主路径不发远程查询 ----
    # 只有带原始价列（raw_open/raw_close）的数据才记账分红：主列是后复权价时
    # 再加现金分红就是双重计入，必须跳过
    if dividends is None and apply_dividends:
        frames = data if isinstance(data, dict) else ({ts_code: data} if ts_code else {})
        raw_open_column = BAR_CONTRACT.column(ROLE_RAW_OPEN)
        raw_close_column = BAR_CONTRACT.column(ROLE_RAW_CLOSE)
        raw_codes = [
            code for code, frame in frames.items()
            if frame is not None and (
                raw_open_column in frame.columns or raw_close_column in frame.columns)
        ]
        if raw_codes:
            dividends = _load_local_dividends(
                raw_codes, db, db_manager, warnings_out=data_warnings)
        elif frames:
            logger.warning(
                "回测数据缺少 raw_open/raw_close 原始价列，已跳过除权记账，"
                "避免与后复权价双重计入；如需记账请传入原始价数据")
            data_warnings.append({
                'stage': 'raw_price_columns',
                'severity': 'degraded',
                'impact': '撮合与涨跌停判定退化为后复权主列，除权记账已跳过',
                'error': 'raw_open/raw_close 缺失',
            })
    if dividends:
        kwargs.setdefault('dividends', dividends)
    kwargs.setdefault('dividend_tax_rate', dividend_tax_rate)
    kwargs.setdefault('rights_policy', rights_policy)

    # ---- 分支：单票 vs 组合 ----
    def _clone(obj: Any) -> Any:
        """分段回测各自持有策略副本，避免状态跨段污染。

        P0-4：深拷贝失败时旧实现 ``return obj`` 会让 IS 与 OOS 共享同一策略
        对象，任何 per-bar 状态都会从样本内泄漏到样本外。宁可中断回测，
        也不能在共享状态下算出"看起来正常"的样本外净值。
        """
        try:
            return copy.deepcopy(obj)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"分段回测无法复制策略 {type(obj).__name__}（{exc}）；"
                "拒绝让 IS/OOS 共享同一策略对象。请为策略实现 __deepcopy__ "
                "或移除不可拷贝资源（锁/连接/线程）后重试"
            ) from exc

    def run_segment(segment_data: Union[pl.DataFrame, Dict[str, pl.DataFrame]]) -> Dict[str, Any]:
        segment_kwargs = dict(kwargs)
        if isinstance(segment_data, dict):
            return _run_portfolio_backtest(
                segment_data, _clone(strategies), risk_engine, st_status,
                initial_capital, lot_size, industry_map, segment_kwargs,
                risk_gate_policy=risk_gate_policy,
            )
        return _run_single_backtest(
            segment_data, _clone(strategy), risk_engine, st_status, ts_code,
            initial_capital, lot_size, industry, segment_kwargs,
            risk_gate_policy=risk_gate_policy,
        )

    result = run_segment(data)
    result['risk_profile'] = risk_profile
    degraded = any(w.get('severity') == 'degraded' for w in data_warnings)
    result['data_quality'] = {
        'degraded': degraded,
        'warnings': data_warnings,
    }
    # 无条件写入：未应用过滤时为 None，调用方无需猜字段是否存在
    result['data_quality']['universe_filter'] = universe_filter
    if degraded:
        # 降级会影响成交假设或权益口径，置信度必须与撮合内核的判定同步下调
        result['fill_confidence'] = 'low'
        logger.warning(
            "本次回测存在 {} 条数据降级事件，结果已标记 "
            "data_quality.degraded=True / fill_confidence=low",
            len(data_warnings),
        )
    if oos_ratio > 0:
        result['oos_validation'] = _run_oos_validation(
            data, run_segment, oos_ratio, oos_horizon, oos_embargo)
    else:
        result['oos_validation'] = {
            'enabled': False,
            'oos_ratio': 0.0,
            'reason': '调用方显式传 oos_ratio=0，关闭样本外切分',
        }
    return result
