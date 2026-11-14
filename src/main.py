#!/usr/bin/env python3

"""
中国股市量化分析系统主入口

仅负责应用组装与启动；数据同步等领域逻辑已收敛至 src.data.services。
"""

# 标准库导入
import argparse
import os
import sys
from datetime import date
from pathlib import Path

# 第三方库导入
from alembic.util.exc import CommandError
from loguru import logger
from PySide6.QtWidgets import QApplication
from sqlalchemy.exc import SQLAlchemyError

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 真实 QApplication 类的引用：测试会把 main 模块里的 QApplication 打桩成假对象，
# 用它在退出前判定「是不是真的 GUI 进程」，避免把测试进程一起硬退出
_REAL_QAPPLICATION = QApplication

from src.data.data_manager import DataManager
from src.data.services.sync_service import (
    backfill_fund_basic_from_local_tdx,
    repair_stock_basic,
    sync_stock_fund_to_fund_basic,
    sync_tdx_index_to_database,
    sync_tdx_stock_to_database,
)
from src.database.db_manager import DatabaseManager
from src.plugin.plugin_manager import PluginManager
from src.ui.main_window import MainWindow
from src.ui.theme_manager import ThemeManager

# 内部模块导入
from src.utils.config import config_manager, get_config, remote_sources_enabled
from src.utils.event_bus import EventType, publish, shutdown_event_bus
from src.utils.exception_handler import setup_global_exception_handler
from src.utils.exceptions import DataSourceException
from src.utils.logger import setup_logger
from src.utils.memory_manager import global_memory_manager
from src.utils.monitoring import log_cache_stats

# 启动期数据库相关异常：SQLAlchemy 的 OperationalError 等不会以 OSError/RuntimeError
# 形式出现，必须显式纳入捕获范围，否则离线模式降级分支永远走不到。
# alembic 迁移失败抛出的 CommandError 同样属于可预期的数据库不可用场景。
_STARTUP_ERRORS = (SQLAlchemyError, CommandError, OSError, RuntimeError)


def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='中国股市量化分析系统')
    parser.add_argument('--update-stock', action='store_true', help='从通达信数据源同步股票信息到数据库')
    parser.add_argument('--update-index', action='store_true', help='以通达信指数代码为基准更新 index_basic（Baostock/AkShare 补全）')
    parser.add_argument('--update-fund', action='store_true', help='将 stock_basic 中基金迁移到 fund_basic 并补全信息')
    parser.add_argument('--backfill-fund-basic', action='store_true',
                        help='用本地通达信基金 .day 补齐 fund_basic/closed_fund_basic（离线，不猜名称与退市状态；配合 --dry-run）')
    parser.add_argument('--sync-stock-daily', action='store_true',
                        help='用通达信日线补库 stock_daily（默认增量：网络源优先，失败回退本地 .day）')
    parser.add_argument('--source', default='auto', choices=['auto', 'local', 'mootdx'],
                        help='配合 --sync-stock-daily：auto=网络优先→本地回退（默认，'
                             'allow_remote_sources=false 时自动降级为 local 并提示）、'
                             'local=只读本地 .day、mootdx=只走网络源（离线配置下拒绝执行）')
    parser.add_argument('--sync-fund-daily', action='store_true',
                        help='用本地列存补库 fund_daily / closed_fund_daily（离线，幂等可续跑）')
    parser.add_argument('--sync-index-daily', action='store_true',
                        help='用本地列存补库 index_daily（默认 index_basic 标的池，本地为准覆盖）')
    parser.add_argument('--codes', default=None,
                        help='逗号分隔的标的代码，配合 --sync-stock-daily / --sync-fund-daily / --sync-index-daily 限定标的池')
    parser.add_argument('--full', action='store_true',
                        help='配合 --sync-stock-daily：全量补历史（默认只补库内水位之后）')
    parser.add_argument('--dry-run', action='store_true',
                        help='配合各 --sync-* 参数：只统计行数与区间，不写库')
    parser.add_argument('--progress-file', default='',
                        help='断点续跑进度文件路径；留空用 logs/sync_progress/ 下的默认文件')
    parser.add_argument('--max-date', default='',
                        help='配合 --sync-stock-daily：只写入不晚于该日期的记录（YYYY-MM-DD）')
    parser.add_argument('--since', default='',
                        help='配合各 --sync-* 参数：只写入不早于该日期的记录（YYYY-MM-DD）')
    parser.add_argument('--repair-stock-basic', action='store_true', help='一次性修复 stock_basic 存量的地域/行业/市场字段')
    parser.add_argument('--deep-industry', action='store_true', help='与 --repair-stock-basic 配合：批量源覆盖不到的行业逐只查巨潮公司概况兜底')
    parser.add_argument('--skip-anomaly-lists', action='store_true',
                        help='数据写入完成后不刷新异常台账清单（默认刷新；失败不影响补数）')
    parser.add_argument('--plugins', action='store_true', help='显示已加载的插件')
    parser.add_argument('--workbench', action='store_true',
                        help='只启动回测工作台的浏览器通道（仅绑 127.0.0.1，打印地址）；'
                             '桌面通道仍是默认行为，入口在菜单「量化分析 → 回测工作台」')
    return parser.parse_args()


def _initialize_system(config):
    """初始化系统组件

    Args:
        config: 配置对象

    Returns:
        tuple: (plugin_manager, cpu_count)
    """
    import os

    setup_global_exception_handler()
    logger.info("全局异常处理器初始化成功")

    config_manager.start_watching(interval=5)
    logger.info("配置热加载功能已启动")

    setup_logger(config)
    logger.info("日志系统初始化成功")

    # 内置指标注册有一次性开销（约 1.2s），放到后台线程预热，避免阻塞首屏
    from src.tech_analysis.indicator_manager import prewarm_indicators_async
    prewarm_indicators_async()
    logger.info("指标注册预热已在后台启动")

    # 插件系统已暂停：保留空管理器实例以兼容 DataManager 构造，不再扫描/加载/初始化插件
    plugin_manager = PluginManager(config)
    logger.info("插件系统已暂停，跳过插件加载与初始化")

    cpu_count = os.cpu_count() or 4
    os.environ['POLARS_MAX_THREADS'] = str(cpu_count)
    logger.info(f"已通过环境变量设置Polars线程池大小为 {cpu_count}")

    return plugin_manager, cpu_count


def _display_plugins(plugin_manager):
    """显示已加载的插件

    Args:
        plugin_manager: 插件管理器
    """
    logger.info("已加载的插件列表:")
    for plugin_info in plugin_manager.get_all_plugin_info():
        logger.info(f"  - {plugin_info['name']} (v{plugin_info['version']}) - {plugin_info['description']}")


def _run_local_stock_basic_builder() -> bool:
    """本地收口：用本地行情/公告证据重建 stock_basic（合并式，不整表覆盖）。

    该重建耗时与行情文件数成正比，放在 ``--update-stock`` 末尾执行；
    失败时向上抛出，避免「库里更新了、本地没更新」被静默放过。
    """
    import subprocess

    script = Path(__file__).resolve().parents[1] / 'tools' / 'build_stock_basic.py'
    if not script.is_file():
        logger.warning(f"未找到本地 stock_basic 重建脚本: {script}")
        return False
    proc = subprocess.run(
        [sys.executable, str(script), '--promote'],
        cwd=str(script.parents[1]), capture_output=True, text=True, timeout=1800,
    )
    if proc.stdout:
        logger.info(proc.stdout.strip())
    if proc.returncode != 0:
        logger.error(f"本地 stock_basic 重建失败: {proc.stderr.strip()}")
        raise RuntimeError('stock_basic 本地重建失败，见上方日志')
    return True


def _enforce_freshness(config) -> None:
    """每日收口：本地正式表落后行情超过阈值即非零退出，避免静默陈旧。

    与 ``--update-*`` 同处执行：更新完只写 MySQL 的旁路、却没跑本地入口时，
    这里会当场报 error，而不是等下一个人发现表停在旧日期。
    """
    from src.data.local_store.paths import resolve_store_root
    from src.data.quality.freshness_check import format_report, run_check

    try:
        report = run_check(resolve_store_root(config))
    except (FileNotFoundError, ValueError) as check_error:
        logger.error(f"数据新鲜度自检无法执行: {check_error}")
        raise SystemExit(1) from check_error
    logger.info("数据新鲜度自检:\n" + format_report(report))
    if not report.ok:
        stale = ', '.join(row.rel_path for row in report.errors)
        logger.error(f"数据新鲜度自检未通过（error 级）: {stale}")
        raise SystemExit(1)


def _handle_update_stock_arg(config):
    """处理股票更新参数

    Args:
        config: 配置对象

    Returns:
        bool: 是否执行了股票更新
    """
    logger.info("检测到 --update-stock 参数，将执行股票信息同步")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except _STARTUP_ERRORS as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        sync_tdx_stock_to_database(config, db_manager)

        if remote_sources_enabled(config):
            from src.data.akshare_handler import AkShareHandler
            akshare_handler = AkShareHandler(config, db_manager)
            logger.info("开始更新 ETF 基本信息...")
            try:
                etf_result = akshare_handler.update_etf_basic()
                if etf_result is not None:
                    logger.info("ETF 基本信息更新完成")
            except DataSourceException as etf_e:
                logger.warning(f"更新 ETF 基本信息失败: {etf_e}")
        else:
            logger.info(
                "data.allow_remote_sources=false，跳过 AkShare ETF 基本信息补全"
            )

        # 本地优先：stock_basic 迁移与 ETF 补全都可能改到 fund_basic，落库后合并导出
        try:
            from src.data.services.local_first_basic import export_fund_basic

            export_result = export_fund_basic(config, db_manager)
            logger.info(
                f"fund_basic 本地合并导出: {export_result['counters']}，"
                f"差异 {export_result['diff_rows']} 条 -> {export_result['diff_csv']}"
            )
        finally:
            db_manager.cleanup()
            logger.info("数据库资源已清理")

        _run_local_stock_basic_builder()
        return True

    except _STARTUP_ERRORS as db_e:
        logger.error(f"数据库连接失败，无法执行同步: {db_e}")
        return True


#: 离线配置下 ``--source auto`` 降级提示；显式打印，不静默回退
OFFLINE_SOURCE_NOTICE = (
    'data.allow_remote_sources=false，--source auto 已跳过远端源，'
    '本次仅使用本地通达信 .day'
)


def resolve_fill_source(requested: str, allow_remote_sources: bool) -> tuple[str, bool]:
    """解析 ``--source``：``auto`` 只在允许联网时保留，否则显式降级为 ``local``。

    Returns:
        (实际使用的 source, 是否发生了离线降级)
    """
    if requested == 'auto' and not allow_remote_sources:
        return 'local', True
    return requested, False


def _handle_sync_stock_daily_arg(config, args):
    """处理 --sync-stock-daily：本地 TDX → stock_daily 离线补库。"""
    from src.data.services.stock_daily_fill import fill_stock_daily

    logger.info("检测到 --sync-stock-daily 参数，将执行 stock_daily 本地补库")

    requested_source = getattr(args, 'source', 'auto')
    allow_remote = remote_sources_enabled(config)
    if requested_source == 'mootdx' and not allow_remote:
        raise DataSourceException(
            'stock_daily_fill',
            'data.allow_remote_sources=false，--source mootdx 必须联网，已拒绝执行；'
            '如需联网请先在配置中开启 allow_remote_sources',
        )
    source, remote_skipped = resolve_fill_source(requested_source, allow_remote)
    if remote_skipped:
        logger.warning(OFFLINE_SOURCE_NOTICE)

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        codes = None
        if args.codes:
            codes = [code.strip() for code in args.codes.split(',') if code.strip()]

        max_date = None
        if getattr(args, 'max_date', ''):
            max_date = date.fromisoformat(args.max_date)

        since = None
        if getattr(args, 'since', ''):
            since = date.fromisoformat(args.since)

        full = getattr(args, 'full', False)
        progress_file = getattr(args, 'progress_file', '')
        if not progress_file:
            mode = 'full' if full else 'incremental'
            progress_file = str(Path('logs') / 'sync_progress'
                                / f'stock_daily_{mode}.json')

        summary = fill_stock_daily(
            config, db_manager,
            ts_codes=codes,
            full=full,
            dry_run=getattr(args, 'dry_run', False),
            since=since,
            max_date=max_date,
            progress_path=progress_file,
            source=source,
        )
        summary['source_requested'] = requested_source
        summary['remote_skipped'] = remote_skipped
        logger.info(f"stock_daily 补库结果: {summary}")
    except (DataSourceException, *_STARTUP_ERRORS) as e:
        logger.exception(f"stock_daily 补库失败: {e}")
    finally:
        if db_manager is not None:
            try:
                db_manager.cleanup()
            except _STARTUP_ERRORS as cleanup_e:
                logger.warning(f"清理数据库资源失败: {cleanup_e}")


def _handle_sync_local_store_daily_arg(config, args, asset):
    """处理 --sync-fund-daily / --sync-index-daily：本地列存 → MySQL。"""
    from src.data.services.local_store_daily_sync import sync_local_store_daily

    logger.info(f"检测到 --sync-{asset}-daily 参数，将执行本地列存入库")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        # dry-run 不执行迁移，避免“只估算”却改了表结构
        if not getattr(args, 'dry_run', False):
            try:
                db_manager.create_tables()
                logger.info("数据库表创建/更新成功")
            except _STARTUP_ERRORS as table_e:
                logger.warning(f"创建数据库表时发生错误: {table_e}")

        codes = None
        if args.codes:
            codes = [code.strip() for code in args.codes.split(',') if code.strip()]

        since = None
        if getattr(args, 'since', ''):
            since = date.fromisoformat(args.since)

        summary = sync_local_store_daily(
            config, db_manager,
            asset=asset,
            ts_codes=codes,
            since=since,
            dry_run=getattr(args, 'dry_run', False),
            progress_path=getattr(args, 'progress_file', '') or None,
            refresh_lists=not getattr(args, 'skip_anomaly_lists', False),
        )
        logger.info(f"{asset} 日线入库结果: {summary}")
    except (DataSourceException, *_STARTUP_ERRORS) as e:
        logger.exception(f"{asset} 日线入库失败: {e}")
    finally:
        if db_manager is not None:
            try:
                db_manager.cleanup()
            except _STARTUP_ERRORS as cleanup_e:
                logger.warning(f"清理数据库资源失败: {cleanup_e}")


def _handle_update_index_arg(config):
    """处理指数更新参数。"""
    logger.info("检测到 --update-index 参数，将执行指数信息同步")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except _STARTUP_ERRORS as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        sync_tdx_index_to_database(config, db_manager)

        # 本地优先：库同步成功后，按单元格合并导出到本地正式 parquet（不整表覆盖）
        try:
            from src.data.services.local_first_basic import export_index_basic

            export_result = export_index_basic(config, db_manager)
            logger.info(
                f"index_basic 本地合并导出: {export_result['counters']}，"
                f"差异 {export_result['diff_rows']} 条 -> {export_result['diff_csv']}"
            )
        finally:
            db_manager.cleanup()
            logger.info("数据库资源已清理")
        return True

    except _STARTUP_ERRORS as db_e:
        logger.error(f"数据库连接失败，无法执行指数同步: {db_e}")
        return True


def _handle_update_fund_arg(config):
    """处理基金迁移参数。"""
    logger.info("检测到 --update-fund 参数，将执行基金迁移和补全")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except _STARTUP_ERRORS as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        sync_stock_fund_to_fund_basic(config, db_manager)
        db_manager.cleanup()
        logger.info("数据库资源已清理")
        return True

    except _STARTUP_ERRORS as db_e:
        logger.error(f"数据库连接失败，无法执行基金迁移: {db_e}")
        return True


def _handle_backfill_fund_basic_arg(config, dry_run=False):
    """处理基金基础表补齐参数（离线，只读 .day）。"""
    logger.info(f"检测到 --backfill-fund-basic 参数，dry_run={dry_run}")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        # 不做 create_tables：本次仅补数据行，禁止任何表结构变更
        result = backfill_fund_basic_from_local_tdx(config, db_manager, dry_run=dry_run)
        logger.info(f"基金基础表补齐结果: {result}")
        db_manager.cleanup()
        return True

    except _STARTUP_ERRORS as db_e:
        logger.error(f"数据库连接失败，无法补齐基金基础表: {db_e}")
        return True


def _handle_repair_stock_basic_arg(config, deep_industry=False):
    """处理 stock_basic 存量修复参数。"""
    logger.info("检测到 --repair-stock-basic 参数，将修复 stock_basic 的地域/行业/市场字段")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except _STARTUP_ERRORS as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        repair_stock_basic(config, db_manager, deep_industry=deep_industry)
        db_manager.cleanup()
        logger.info("数据库资源已清理")
        return True

    except _STARTUP_ERRORS as db_e:
        logger.error(f"数据库连接失败，无法执行 stock_basic 修复: {db_e}")
        return True


def _initialize_data_manager(config, plugin_manager):
    """初始化数据管理器

    Args:
        config: 配置对象
        plugin_manager: 插件管理器

    Returns:
        DataManager: 数据管理器实例
    """
    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except _STARTUP_ERRORS as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        data_manager = DataManager(config, db_manager, plugin_manager)
        logger.info("数据管理器初始化成功")
    except _STARTUP_ERRORS as db_e:
        logger.warning(
            f"数据库连接失败，将以离线模式运行（错误类型 {type(db_e).__name__}）: {db_e}"
        )
        data_manager = DataManager(config, None, plugin_manager)
        logger.info("数据管理器（离线模式）初始化成功")

    return data_manager


def _cleanup_system(plugin_manager, db_manager):
    """清理系统资源

    Args:
        plugin_manager: 插件管理器
        db_manager: 数据库管理器
    """
    try:
        global_memory_manager.stop_monitoring()
        memory_usage = global_memory_manager.get_memory_usage()
        logger.info(f"最终内存使用: {memory_usage['process_rss_mb']:.2f} MB ({memory_usage['process_percent']:.1f}%)")
    # 兜底理由：退出阶段清理失败不得阻断进程退出
    except Exception as e:  # noqa: BLE001
        logger.warning(f"停止内存监控时发生错误: {e}")

    publish(EventType.SYSTEM_SHUTDOWN)
    log_cache_stats()

    try:
        if db_manager:
            db_manager.cleanup()
    except _STARTUP_ERRORS as cleanup_e:
        logger.warning(f"清理数据库资源时发生错误: {cleanup_e}")

    if plugin_manager:
        plugin_manager.shutdown_plugins()

    config_manager.stop_watching()
    logger.info("配置热加载功能已停止")

    # 回测工作台：停当前回测、关回环服务、清临时目录（未启动时为空操作）
    try:
        from src.ui.workbench.server import shutdown as workbench_shutdown

        workbench_shutdown()
    except Exception as workbench_error:  # noqa: BLE001
        logger.warning(f"回测工作台收尾失败: {workbench_error}")

    shutdown_event_bus()
    logger.info("事件总线已关闭")

    logger.info("系统已关闭")


def main():
    """主函数"""
    args = parse_args()

    plugin_manager = None
    db_manager = None
    # GUI 事件循环退出码；None 表示走命令行分支（不需要强制终止进程）
    app_exit_code = None

    try:
        config = get_config()
        logger.info("配置加载成功")

        plugin_manager, cpu_count = _initialize_system(config)

        if args.plugins:
            _display_plugins(plugin_manager)
            return

        if args.workbench:
            # 浏览器通道：只起回测工作台服务（仅绑 127.0.0.1），不开桌面窗口
            from src.ui.workbench.server import run_browser_channel

            return run_browser_channel()

        if args.update_stock:
            _handle_update_stock_arg(config)
            _enforce_freshness(config)
            return

        if args.sync_stock_daily:
            _handle_sync_stock_daily_arg(config, args)
            return

        if args.sync_fund_daily:
            _handle_sync_local_store_daily_arg(config, args, 'fund')
            return

        if args.sync_index_daily:
            _handle_sync_local_store_daily_arg(config, args, 'index')
            return

        if args.update_index:
            _handle_update_index_arg(config)
            _enforce_freshness(config)
            return

        if args.update_fund:
            _handle_update_fund_arg(config)
            _enforce_freshness(config)
            return

        if args.backfill_fund_basic:
            _handle_backfill_fund_basic_arg(config, dry_run=args.dry_run)
            return

        if args.repair_stock_basic or args.deep_industry:
            _handle_repair_stock_basic_arg(config, deep_industry=args.deep_industry)
            return

        data_manager = _initialize_data_manager(config, plugin_manager)

        # QtWebEngine 必须先于 QApplication 就绪（共享 GL 上下文），否则内嵌工作台标签页
        # 会在创建时被 Chromium 拒绝；缺 QtWebEngine 时只降级该标签页，不影响主界面。
        from PySide6.QtCore import QCoreApplication, Qt

        QCoreApplication.setAttribute(Qt.AA_ShareOpenGLContexts, True)
        try:
            import PySide6.QtWebEngineWidgets  # noqa: F401
        except Exception as webengine_error:  # noqa: BLE001
            logger.warning(f"QtWebEngine 不可用，回测工作台标签页将不可用: {webengine_error}")

        app = QApplication(sys.argv)
        app.setApplicationName("中国股市量化分析系统")

        ThemeManager.set_dark_theme(app)

        main_window = MainWindow(config, data_manager, plugin_manager)
        main_window.showMaximized()

        publish(EventType.SYSTEM_INIT, app=app, config=config, data_manager=data_manager)
        logger.info("中国股市量化分析系统启动成功")

        global_memory_manager.start_monitoring()
        logger.info("内存监控已启动")

        memory_usage = global_memory_manager.get_memory_usage()
        logger.info(f"初始内存使用: {memory_usage['process_rss_mb']:.2f} MB ({memory_usage['process_percent']:.1f}%)")

        log_cache_stats()

        app_exit_code = app.exec()

    except _STARTUP_ERRORS as e:
        logger.exception(f"系统启动失败: {e}")
        sys.exit(1)
    finally:
        _cleanup_system(plugin_manager, db_manager)

    # 桌面 GUI 正常关窗：资源已清理，强制终止进程。
    # ThreadPoolExecutor 的工作线程是非 daemon 的，若某个取数/网络任务卡住，
    # 解释器退出会在 atexit 阶段被它拖住，表现为关窗后进程残留。
    if app_exit_code is not None:
        if isinstance(app, _REAL_QAPPLICATION):
            logger.info(f"GUI 已退出（退出码 {app_exit_code}），强制终止进程")
            os._exit(app_exit_code)
        # 非真实 GUI（测试打桩的 App）：保持原有 SystemExit 语义，不硬退出
        sys.exit(app_exit_code)


if __name__ == "__main__":
    main()
