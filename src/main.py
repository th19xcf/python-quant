#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
中国股市量化分析系统主入口
"""

# 标准库导入
import argparse
import sys
from pathlib import Path

# 第三方库导入
from loguru import logger
from PySide6.QtWidgets import QApplication

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 内部模块导入
from src.utils.config import get_config, config_manager
from src.utils.logger import setup_logger
from src.utils.event_bus import publish, EventType, shutdown_event_bus
from src.database.db_manager import DatabaseManager
from src.data.data_manager import DataManager
from src.plugin.plugin_manager import PluginManager
from src.ui.main_window import MainWindow
from src.ui.theme_manager import ThemeManager
from src.utils.cache_monitor import log_cache_stats
from src.utils.exception_handler import setup_global_exception_handler
from src.utils.memory_manager import global_memory_manager


def sync_tdx_stock_to_database(config, db_manager):
    """
    从通达信数据源同步股票信息到数据库
    
    工作流程：
    1. 从通达信数据源提取所有股票代码
    2. 与数据库 stock_basic 表进行匹配比较
    3. 对于通达信有但数据库没有的股票：
       a. 使用 baostock 和 akshare API 获取完整股票信息
       b. 收集所有相关的财务和市场数据字段
       c. 将完整的股票信息插入 stock_basic 表
    
    Args:
        config: 配置对象
        db_manager: 数据库管理器实例
        
    Returns:
        dict: 同步结果统计
    """
    from src.data.tdx_handler import TdxHandler
    from src.data.baostock_handler import BaostockHandler
    from src.data.akshare_handler import AkShareHandler
    from src.database.models.stock import StockBasic
    from datetime import datetime
    import time
    
    result = {
        'total_tdx_stocks': 0,
        'existing_stocks': 0,
        'new_stocks': 0,
        'failed_stocks': [],
        'updated_stocks': 0
    }
    
    logger.info("=" * 60)
    logger.info("开始从通达信数据源同步股票信息到数据库")
    logger.info("=" * 60)
    
    try:
        tdx_handler = TdxHandler(config, db_manager)
        
        tdx_stock_codes = tdx_handler.get_stock_list()
        result['total_tdx_stocks'] = len(tdx_stock_codes)
        logger.info(f"通达信数据源共有 {len(tdx_stock_codes)} 只股票")
        
        if not tdx_stock_codes:
            logger.warning("通达信数据源没有找到任何股票数据")
            return result
        
        session = db_manager.get_session()
        
        existing_codes = set()
        for stock in session.query(StockBasic.ts_code).all():
            existing_codes.add(stock.ts_code)
        result['existing_stocks'] = len(existing_codes)
        logger.info(f"数据库 stock_basic 表已有 {len(existing_codes)} 只股票")
        
        new_codes = [code for code in tdx_stock_codes if code not in existing_codes]
        result['new_stocks'] = len(new_codes)
        logger.info(f"发现 {len(new_codes)} 只新股票需要添加到数据库")
        
        if not new_codes:
            logger.info("所有通达信股票都已在数据库中，无需同步")
            return result
        
        baostock_handler = BaostockHandler(config, db_manager)
        akshare_handler = AkShareHandler(config, db_manager)
        
        logger.info("开始从 Baostock 获取股票基本信息...")
        baostock_basic = None
        try:
            baostock_basic = baostock_handler.update_stock_basic()
            if baostock_basic is not None:
                logger.info(f"从 Baostock 获取到 {baostock_basic.height} 条股票信息")
        except Exception as e:
            logger.warning(f"从 Baostock 获取股票基本信息失败: {e}")
        
        logger.info("开始从 AkShare 获取股票基本信息...")
        akshare_basic = None
        try:
            akshare_basic = akshare_handler.update_stock_basic()
            if akshare_basic is not None:
                logger.info(f"从 AkShare 获取到 {akshare_basic.height} 条股票信息")
        except Exception as e:
            logger.warning(f"从 AkShare 获取股票基本信息失败: {e}")
        
        merged_basic = merge_stock_basic(baostock_basic, akshare_basic, existing_codes, tdx_stock_codes)
        
        for i, ts_code in enumerate(new_codes):
            try:
                logger.info(f"[{i+1}/{len(new_codes)}] 处理股票: {ts_code}")
                
                # 再次检查股票是否已经存在，避免重复插入
                if ts_code in existing_codes:
                    logger.info(f"股票 {ts_code} 已存在，跳过")
                    continue
                
                stock_info = get_stock_info_from_apis(ts_code, merged_basic, baostock_handler, akshare_handler)
                
                if stock_info is None:
                    logger.warning(f"无法获取股票 {ts_code} 的信息，跳过")
                    result['failed_stocks'].append(ts_code)
                    continue
                
                # 确保使用原始的 ts_code，避免 StockInfo 中的 ts_code 与原始 ts_code 不一致
                # 截断过长的字段以防止数据库错误
                name = stock_info.get('name', '未知')
                if len(name) > 45:
                    name = name[:45]
                symbol = stock_info.get('symbol', ts_code.split('.')[0])
                if len(symbol) > 9:
                    symbol = symbol[:9]
                stock = StockBasic(
                    ts_code=ts_code,
                    symbol=symbol,
                    name=name,
                    area=stock_info.get('area'),
                    industry=stock_info.get('industry'),
                    market=stock_info.get('market'),
                    list_date=stock_info.get('list_date'),
                    delist_date=stock_info.get('delist_date'),
                    status=stock_info.get('status', 'L')
                )
                session.add(stock)
                result['updated_stocks'] += 1
                existing_codes.add(ts_code)  # 将已处理的股票添加到 existing_codes 中
                
                if (i + 1) % 50 == 0:
                    try:
                        session.commit()
                        logger.info(f"已提交 {i + 1} 条股票记录到数据库")
                    except Exception as commit_e:
                        logger.exception(f"提交数据库时失败: {commit_e}")
                        session.rollback()
                
                time.sleep(0.1)
                
            except Exception as e:
                logger.exception(f"处理股票 {ts_code} 失败: {e}")
                result['failed_stocks'].append(ts_code)
                continue
        
        try:
            session.commit()
        except Exception as commit_e:
            logger.exception(f"最终提交数据库时失败: {commit_e}")
            session.rollback()
        logger.info(f"股票信息同步完成，共新增 {result['updated_stocks']} 条记录")
        
    except Exception as e:
        logger.exception(f"同步股票信息失败: {e}")
        if session:
            session.rollback()
    
    logger.info("=" * 60)
    logger.info("同步结果统计:")
    logger.info(f"  通达信股票总数: {result['total_tdx_stocks']}")
    logger.info(f"  数据库已有股票: {result['existing_stocks']}")
    logger.info(f"  新增股票数量: {result['updated_stocks']}")
    logger.info(f"  失败股票数量: {len(result['failed_stocks'])}")
    if result['failed_stocks']:
        logger.warning(f"  失败股票列表: {result['failed_stocks'][:10]}...")
    logger.info("=" * 60)
    
    return result


def merge_stock_basic(baostock_df, akshare_df, existing_codes=None, tdx_stock_codes=None):
    """
    合并 Baostock 和 AkShare 的股票基本信息
    
    Args:
        baostock_df: Baostock 股票信息 DataFrame
        akshare_df: AkShare 股票信息 DataFrame
        existing_codes: 已存在的股票代码集合（用于过滤）
        tdx_stock_codes: 通达信股票代码列表，包含市场信息（如 600000.SH、000001.SZ、920181.BJ）
        
    Returns:
        dict: 股票代码到信息的映射
    """
    import polars as pl
    
    merged = {}
    
    if existing_codes is None:
        existing_codes = set()
    
    # 从通达信股票代码中提取市场信息
    tdx_market_map = {}
    if tdx_stock_codes:
        for ts_code in tdx_stock_codes:
            if '.' in ts_code:
                symbol, market = ts_code.split('.')
                tdx_market_map[symbol] = market
    
    if baostock_df is not None and not baostock_df.is_empty():
        for row in baostock_df.iter_rows(named=True):
            try:
                code = row.get('code', '')
                if not code:
                    continue
                
                # 处理 Baostock 格式的代码（如 sh.600000, sz.000001, bj.830000）
                if '.' in code:
                    parts = code.split('.')
                    if len(parts) == 2:
                        exchange, symbol = parts
                        if exchange == 'sh':
                            ts_code = f"{symbol}.SH"
                        elif exchange == 'sz':
                            ts_code = f"{symbol}.SZ"
                        elif exchange == 'bj':
                            ts_code = f"{symbol}.BJ"
                        else:
                            continue
                    else:
                        continue
                else:
                    symbol = code
                    # 优先使用通达信数据中的市场信息
                    if code in tdx_market_map:
                        market = tdx_market_map[code]
                        ts_code = f"{code}.{market}"
                    else:
                        # 处理普通格式的代码
                        if code.startswith('6'):
                            ts_code = f"{code}.SH"
                        elif code.startswith('8') and len(code) == 6:
                            num = int(code)
                            if 800000 <= num <= 899999:
                                ts_code = f"{code}.BJ"
                            else:
                                ts_code = f"{code}.SZ"
                        elif code.startswith('92') and len(code) == 6:
                            num = int(code)
                            if 920000 <= num <= 920999:
                                ts_code = f"{code}.BJ"
                            else:
                                ts_code = f"{code}.SZ"
                        else:
                            ts_code = f"{code}.SZ"
                
                if ts_code in existing_codes:
                    continue
                
                merged[ts_code] = {
                    'symbol': symbol,
                    'name': row.get('code_name', ''),
                    'area': row.get('area', ''),
                    'industry': row.get('industry', ''),
                    'market': row.get('market', ''),
                    'list_date': row.get('list_date'),
                    'status': row.get('status', 'L'),
                    'source': 'baostock'
                }
            except Exception:
                continue
    
    if akshare_df is not None and not akshare_df.is_empty():
        for row in akshare_df.iter_rows(named=True):
            try:
                code = row.get('code', '')
                if not code:
                    continue
                
                # 处理类似 Baostock 格式的代码（如 sh.600000, sz.000001, bj.830000）
                if '.' in code:
                    parts = code.split('.')
                    if len(parts) == 2:
                        exchange, symbol = parts
                        if exchange == 'sh':
                            ts_code = f"{symbol}.SH"
                        elif exchange == 'sz':
                            ts_code = f"{symbol}.SZ"
                        elif exchange == 'bj':
                            ts_code = f"{symbol}.BJ"
                        else:
                            continue
                    else:
                        continue
                else:
                    symbol = code
                    # 北交所新股：920000-920999
                    if code.startswith('92') and len(code) == 6:
                        num = int(code)
                        if 920000 <= num <= 920999:
                            ts_code = f"{code}.BJ"
                        else:
                            ts_code = f"{code}.SZ"
                    # 北交所股票：800000-899999
                    elif code.startswith('8') and len(code) == 6:
                        num = int(code)
                        if 800000 <= num <= 899999:
                            ts_code = f"{code}.BJ"
                        else:
                            ts_code = f"{code}.SZ"
                    # 上海 ETF：510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 588xxx
                    elif code.startswith('51') or code.startswith('58'):
                        ts_code = f"{code}.SH"
                    # 深圳 ETF：150xxx, 151xxx, 152xxx, 153xxx, 159xxx, 160xxx, 161xxx
                    elif code.startswith('15') or code.startswith('16'):
                        ts_code = f"{code}.SZ"
                    # 优先使用通达信数据中的市场信息
                    elif code in tdx_market_map:
                        market = tdx_market_map[code]
                        ts_code = f"{code}.{market}"
                    # 处理普通格式的代码
                    elif code.startswith('6'):
                        ts_code = f"{code}.SH"
                    else:
                        ts_code = f"{code}.SZ"
                
                if ts_code in existing_codes:
                    continue
                
                if ts_code in merged:
                    merged[ts_code].update({
                        'name': row.get('name', merged[ts_code].get('name', '')),
                        'area': row.get('area', merged[ts_code].get('area', '')),
                        'industry': row.get('industry', merged[ts_code].get('industry', '')),
                    })
                else:
                    merged[ts_code] = {
                        'symbol': symbol,
                        'name': row.get('name', ''),
                        'area': row.get('area', ''),
                        'industry': row.get('industry', ''),
                        'market': '',
                        'list_date': None,
                        'status': 'L',
                        'source': 'akshare'
                    }
            except Exception:
                continue
    
    logger.info(f"合并后共有 {len(merged)} 条股票基本信息（已过滤已存在的 {len(existing_codes)} 条）")
    return merged


def get_stock_info_from_apis(ts_code, merged_basic, baostock_handler, akshare_handler):
    """
    从 API 获取单个股票的详细信息
    
    Args:
        ts_code: 股票代码 (如 600000.SH)
        merged_basic: 合并后的股票基本信息
        baostock_handler: Baostock 处理器
        akshare_handler: AkShare 处理器
        
    Returns:
        dict: 股票信息字典
    """
    import polars as pl
    
    symbol = ts_code.split('.')[0]
    
    if ts_code in merged_basic:
        info = merged_basic[ts_code].copy()
        
        # 确保返回的ts_code与输入的ts_code一致
        info['ts_code'] = ts_code
        
        if info.get('list_date') and isinstance(info['list_date'], str):
            try:
                from datetime import datetime
                info['list_date'] = datetime.strptime(info['list_date'], '%Y-%m-%d').date()
            except:
                info['list_date'] = None
        
        return info
    
    for mapped_code, info in merged_basic.items():
        if info.get('symbol') == symbol:
            result = info.copy()
            # 确保返回的ts_code与输入的ts_code一致
            result['ts_code'] = ts_code
            return result
    
    logger.warning(f"在 Baostock/AkShare 数据中未找到股票 {ts_code} 的信息")
    if symbol.startswith('880'):
        # 880开头的股票是行业指数，使用更有意义的名称
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'行业指数{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('92') and len(symbol) == 6:
        # 920开头的股票是北交所新股
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'北交所新股{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('8') and len(symbol) == 6:
        # 8开头的股票是北交所股票
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'北交所股票{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('51') or symbol.startswith('58'):
        # 上海 ETF
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'ETF{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('15') or symbol.startswith('16'):
        # 深圳 ETF
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'ETF{symbol}',
            'status': 'L'
        }
    else:
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'股票{symbol}',
            'status': 'L'
        }


def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='中国股市量化分析系统')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库表')
    parser.add_argument('--update-stock', action='store_true', help='从通达信数据源同步股票信息到数据库')
    parser.add_argument('--plugins', action='store_true', help='显示已加载的插件')
    return parser.parse_args()


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    plugin_manager = None
    
    try:
        # 设置全局异常处理器
        setup_global_exception_handler()
        logger.info("全局异常处理器初始化成功")
        
        # 初始化配置
        config = get_config()
        logger.info("配置加载成功")
        
        # 启动配置热加载
        config_manager.start_watching(interval=5)
        logger.info("配置热加载功能已启动")
        
        # 初始化日志
        setup_logger(config)
        logger.info("日志系统初始化成功")
        
        # 初始化插件管理器
        plugin_manager = PluginManager(config)
        logger.info("插件管理器初始化成功")
        
        # 加载插件
        loaded_count = plugin_manager.load_plugins()
        logger.info(f"插件加载完成，共加载 {loaded_count} 个插件")
        
        # 设置Polars线程池大小，匹配系统CPU核心数
        import os
        cpu_count = os.cpu_count() or 4
        # 在Polars 1.36.1中，线程池大小通过环境变量设置
        os.environ['POLARS_MAX_THREADS'] = str(cpu_count)
        logger.info(f"已通过环境变量设置Polars线程池大小为 {cpu_count}")
        
        # 初始化插件
        initialized_count = plugin_manager.initialize_plugins()
        logger.info(f"插件初始化完成，共初始化 {initialized_count} 个插件")
        
        # 显示已加载的插件（如果指定了--plugins参数）
        if args.plugins:
            logger.info("已加载的插件列表:")
            for plugin_info in plugin_manager.get_all_plugin_info():
                logger.info(f"  - {plugin_info['name']} (v{plugin_info['version']}) - {plugin_info['description']}")
            return
        
        # 处理 --update-stock 参数：从通达信数据源同步股票信息到数据库
        if args.update_stock:
            logger.info("检测到 --update-stock 参数，将执行股票信息同步")
            
            # 初始化数据库
            db_manager = None
            try:
                db_manager = DatabaseManager(config)
                db_manager.connect()
                logger.info("数据库连接成功")
                
                # 创建数据库表（如果不存在）
                try:
                    db_manager.create_tables()
                    logger.info("数据库表创建/更新成功")
                except (OSError, RuntimeError) as table_e:
                    logger.warning(f"创建数据库表时发生错误: {table_e}")
                
                # 执行同步
                sync_result = sync_tdx_stock_to_database(config, db_manager)
                
                # 更新 ETF 基本信息
                from src.data.akshare_handler import AkShareHandler
                akshare_handler = AkShareHandler(config, db_manager)
                logger.info("开始更新 ETF 基本信息...")
                try:
                    etf_result = akshare_handler.update_etf_basic()
                    if etf_result is not None:
                        logger.info(f"ETF 基本信息更新完成")
                except Exception as etf_e:
                    logger.warning(f"更新 ETF 基本信息失败: {etf_e}")
                
                # 清理数据库资源并退出
                db_manager.cleanup()
                logger.info("数据库资源已清理")
                return
                
            except (OSError, RuntimeError) as db_e:
                logger.error(f"数据库连接失败，无法执行同步: {db_e}")
                return
        
        # 初始化数据库（可选）
        db_manager = None
        data_manager = None
        
        try:
            db_manager = DatabaseManager(config)
            db_manager.connect()
            logger.info("数据库连接成功")
            
            # 创建数据库表（如果不存在）
            try:
                db_manager.create_tables()
                logger.info("数据库表创建/更新成功")
            except (OSError, RuntimeError) as table_e:
                logger.warning(f"创建数据库表时发生错误: {table_e}")
                # 表创建失败不影响程序启动，继续运行
            
            # 初始化数据管理器
            data_manager = DataManager(config, db_manager, plugin_manager)
            logger.info("数据管理器初始化成功")
        except (OSError, RuntimeError) as db_e:
            logger.warning(f"数据库连接失败，将以离线模式运行: {db_e}")
            # 离线模式下也初始化数据管理器，不传入db_manager
            data_manager = DataManager(config, None, plugin_manager)
            logger.info("数据管理器（离线模式）初始化成功")
        
        # 创建Qt应用
        app = QApplication(sys.argv)
        app.setApplicationName("中国股市量化分析系统")
        
        # 应用深色主题
        ThemeManager.set_dark_theme(app)
        
        # 初始化主窗口
        main_window = MainWindow(config, data_manager, plugin_manager)
        main_window.showMaximized()
        
        # 发布系统初始化完成事件
        publish(EventType.SYSTEM_INIT, app=app, config=config, data_manager=data_manager)
        logger.info("中国股市量化分析系统启动成功")
        
        # 启动内存监控
        global_memory_manager.start_monitoring()
        logger.info("内存监控已启动")
        
        # 记录初始内存使用情况
        memory_usage = global_memory_manager.get_memory_usage()
        logger.info(f"初始内存使用: {memory_usage['process_rss_mb']:.2f} MB ({memory_usage['process_percent']:.1f}%)")
        
        # 记录初始缓存统计
        log_cache_stats()
        
        # 运行主循环
        sys.exit(app.exec())

    except (OSError, RuntimeError) as e:
        logger.exception(f"系统启动失败: {e}")
        sys.exit(1)
    finally:
        # 停止内存监控
        try:
            global_memory_manager.stop_monitoring()
            # 记录最终内存使用情况
            memory_usage = global_memory_manager.get_memory_usage()
            logger.info(f"最终内存使用: {memory_usage['process_rss_mb']:.2f} MB ({memory_usage['process_percent']:.1f}%)")
        except Exception as e:
            logger.warning(f"停止内存监控时发生错误: {e}")
        
        # 发布系统关闭事件
        publish(EventType.SYSTEM_SHUTDOWN)
        
        # 记录系统关闭前的缓存统计
        log_cache_stats()
        
        # 清理资源
        try:
            if 'db_manager' in locals() and db_manager:
                db_manager.cleanup()
        except (OSError, RuntimeError) as cleanup_e:
            logger.warning(f"清理数据库资源时发生错误: {cleanup_e}")
        
        # 关闭插件
        if plugin_manager:
            plugin_manager.shutdown_plugins()
        
        # 停止配置热加载
        config_manager.stop_watching()
        logger.info("配置热加载功能已停止")
        
        # 关闭事件总线
        shutdown_event_bus()
        logger.info("事件总线已关闭")
        
        logger.info("系统已关闭")


if __name__ == "__main__":
    main()
