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
from src.utils.config import Config
from src.utils.logger import setup_logger
from src.utils.event_bus import publish
from src.database.db_manager import DatabaseManager
from src.data.data_manager import DataManager
from src.plugin.plugin_manager import PluginManager
from src.ui.main_window import MainWindow
from src.ui.theme_manager import ThemeManager


def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='中国股市量化分析系统')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库表')
    parser.add_argument('--update-stock', action='store_true', help='更新股票基本信息')
    parser.add_argument('--plugins', action='store_true', help='显示已加载的插件')
    return parser.parse_args()


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    plugin_manager = None
    
    try:
        # 初始化配置
        config = Config()
        logger.info("配置加载成功")
        
        # 初始化日志
        setup_logger(config)
        logger.info("日志系统初始化成功")
        
        # 初始化插件管理器
        plugin_manager = PluginManager(config)
        logger.info("插件管理器初始化成功")
        
        # 加载插件
        loaded_count = plugin_manager.load_plugins()
        logger.info(f"插件加载完成，共加载 {loaded_count} 个插件")
        
        # 初始化插件
        initialized_count = plugin_manager.initialize_plugins()
        logger.info(f"插件初始化完成，共初始化 {initialized_count} 个插件")
        
        # 显示已加载的插件（如果指定了--plugins参数）
        if args.plugins:
            logger.info("已加载的插件列表:")
            for plugin_info in plugin_manager.get_all_plugin_info():
                logger.info(f"  - {plugin_info['name']} (v{plugin_info['version']}) - {plugin_info['description']}")
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
            except Exception as table_e:
                logger.warning(f"创建数据库表时发生错误: {table_e}")
                # 表创建失败不影响程序启动，继续运行
            
            # 初始化数据管理器
            data_manager = DataManager(config, db_manager, plugin_manager)
            logger.info("数据管理器初始化成功")
        except Exception as db_e:
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
        publish('system_init', app=app, config=config, data_manager=data_manager)
        logger.info("中国股市量化分析系统启动成功")
        
        # 运行主循环
        sys.exit(app.exec())
        
    except Exception as e:
        logger.exception(f"系统启动失败: {e}")
        sys.exit(1)
    finally:
        # 发布系统关闭事件
        publish('system_shutdown')
        
        # 清理资源
        try:
            if 'db_manager' in locals() and db_manager:
                db_manager.disconnect()
        except Exception as cleanup_e:
            logger.warning(f"清理数据库资源时发生错误: {cleanup_e}")
        
        # 关闭插件
        if plugin_manager:
            plugin_manager.shutdown_plugins()
        
        logger.info("系统已关闭")


if __name__ == "__main__":
    main()
