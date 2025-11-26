#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
中国股市量化分析系统主入口
"""

from loguru import logger
from pathlib import Path

# 添加项目根目录到Python路径
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.utils.logger import setup_logger
from src.database.db_manager import DatabaseManager
from src.data.data_manager import DataManager

# UI相关导入
from PySide6.QtWidgets import QApplication
from src.ui.main_window import MainWindow


def main():
    """主函数"""
    try:
        # 初始化配置
        config = Config()
        logger.info("配置加载成功")
        
        # 初始化日志
        setup_logger(config)
        logger.info("日志系统初始化成功")
        
        # 初始化数据库（可选）
        db_manager = None
        data_manager = None
        
        try:
            db_manager = DatabaseManager(config)
            db_manager.connect()
            logger.info("数据库连接成功")
            
            # 初始化数据管理器
            data_manager = DataManager(config, db_manager)
            logger.info("数据管理器初始化成功")
        except Exception as db_e:
            logger.warning(f"数据库连接失败，将以离线模式运行: {db_e}")
            # 离线模式下也初始化数据管理器，不传入db_manager
            data_manager = DataManager(config, None)
            logger.info("数据管理器（离线模式）初始化成功")
        
        # 创建Qt应用
        app = QApplication(sys.argv)
        app.setApplicationName("中国股市量化分析系统")
        
        # 应用深色主题
        from src.ui.theme_manager import ThemeManager
        ThemeManager.set_dark_theme(app)
        
        # 初始化主窗口
        main_window = MainWindow(config, data_manager)
        main_window.show()
        
        logger.info("中国股市量化分析系统启动成功")
        
        # 运行主循环
        sys.exit(app.exec())
        
    except Exception as e:
        logger.exception(f"系统启动失败: {e}")
        sys.exit(1)
    finally:
        # 清理资源
        if 'db_manager' in locals() and db_manager:
            db_manager.disconnect()
        logger.info("系统已关闭")


if __name__ == "__main__":
    main()
