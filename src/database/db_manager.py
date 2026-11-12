#!/usr/bin/env python3

"""
数据库管理模块
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

# 数据库层可预期异常：驱动/网络错误（OSError、RuntimeError、ValueError）
# 与 SQLAlchemy 自身的 OperationalError / DBAPIError 等，统一收口处理
_DB_ERRORS = (SQLAlchemyError, OSError, RuntimeError, ValueError)

# 显式标注为 Any：declarative_base() 在 SQLAlchemy 中返回 Any，
# 未标注时 mypy 会把它当作普通变量而拒绝作为类基类使用
Base: Any = declarative_base()


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, config):
        """
        初始化数据库管理器

        Args:
            config: 配置对象，包含数据库相关配置
        """
        self.config = config
        self.engine = None
        self.Session = None
        self.session = None

    def connect(self):
        """
        连接数据库

        Returns:
            sqlalchemy.orm.Session: 数据库会话对象
        """
        try:
            # 创建数据库连接引擎
            db_config = self.config.database
            # 用 URL.create 构造，用户名/密码中的 @ : / # ? 等字符会被自动转义；
            # f-string 直拼在密码含特殊字符时会解析出错误的 host/port
            connection_url = URL.create(
                drivername="mysql+pymysql",
                username=db_config.username,
                password=db_config.password,
                host=db_config.host,
                port=db_config.port,
                database=db_config.database,
                query={"charset": db_config.charset},
            )

            # 传输层加密：只有显式配置 MySQL CA 证书（DATABASE__SSL_CA）时才把
            # ssl 参数交给驱动；留空时 create_engine 的实参集合与引入该开关之前逐字相同
            engine_kwargs: dict[str, Any] = {
                'pool_size': db_config.pool_size,
                'max_overflow': db_config.max_overflow,
                'pool_timeout': db_config.pool_timeout,
                'pool_recycle': db_config.pool_recycle,
                'pool_pre_ping': True,
                'echo': False,
            }
            ssl_ca = getattr(db_config, 'ssl_ca', '')
            if ssl_ca:
                engine_kwargs['connect_args'] = {'ssl': {'ca': ssl_ca}}

            self.engine = create_engine(connection_url, **engine_kwargs)

            # 创建会话工厂
            self.Session = scoped_session(sessionmaker(bind=self.engine))

            # 创建会话
            self.session = self.Session()

            # 测试连接
            self.engine.connect()

            logger.info(f"成功连接到数据库: {db_config.host}:{db_config.port}/{db_config.database}")

            # 返回创建的会话对象
            return self.session

        except _DB_ERRORS as e:
            # OperationalError / DBAPIError 等驱动异常原先会直接穿透到调用方，
            # 这里统一记录可读日志后原样抛出，保持上层 fail-fast 的既有约定
            logger.exception(
                f"数据库连接失败: {self.config.database.host}:{self.config.database.port}"
                f"/{self.config.database.database}，错误类型: {type(e).__name__}，原因: {e}"
            )
            raise

    def disconnect(self):
        """
        断开数据库连接
        """
        try:
            if self.session:
                try:
                    self.session.close()
                    self.Session.remove()
                except _DB_ERRORS as session_e:
                    logger.warning(f"关闭数据库会话时发生错误（可能是连接已丢失）: {session_e}")
                    # 即使会话关闭失败，也要继续释放其他资源
                    self.session = None

            if self.engine:
                try:
                    self.engine.dispose()
                except _DB_ERRORS as engine_e:
                    logger.warning(f"释放数据库引擎时发生错误: {engine_e}")

            logger.info("数据库连接已断开")

        except _DB_ERRORS as e:
            logger.exception(f"数据库断开连接失败: {e}")

    def create_tables(self):
        """
        建表：统一经 Alembic 迁移执行（等价于 `alembic upgrade head`）

        这样 schema 的创建与升级只有一个来源，并且会写入 `alembic_version` 版本表，
        后续变更必须通过 `alembic revision --autogenerate` 生成增量脚本。
        对已由旧版 create_all 建过表的库可重复执行（基线使用 checkfirst 语义）。

        Raises:
            Exception: 迁移失败时抛出，由调用方决定是否降级为离线模式
        """
        try:
            from alembic import command
            from alembic.config import Config as AlembicConfig
            from alembic.util.exc import CommandError

            project_root = Path(__file__).resolve().parents[2]
            alembic_cfg = AlembicConfig(str(project_root / 'alembic.ini'))
            command.upgrade(alembic_cfg, 'head')
            logger.info("数据库表迁移成功（alembic upgrade head）")

        except ImportError as e:
            # 依赖漂移：当前解释器未安装 alembic（如 IDE 指向旧 venv）。
            # 按"迁移失败抛出、调用方决定是否降级"的契约转成 RuntimeError，
            # 否则 ImportError 会穿透 OSError/RuntimeError 捕获链，让整个程序起不来
            raise RuntimeError(
                f"缺少 alembic 依赖，无法执行数据库迁移，请确认当前解释器已安装 alembic: {e}"
            ) from e
        except (CommandError, *_DB_ERRORS) as e:
            logger.exception(f"数据库表迁移失败: {e}")
            raise

    def drop_tables(self):
        """
        删除数据库表
        """
        try:
            # 删除所有表
            Base.metadata.drop_all(self.engine)
            logger.info("数据库表删除成功")

        except _DB_ERRORS as e:
            logger.exception(f"数据库表删除失败: {e}")
            raise

    def list_stock_codes(self, include_delisted: bool = False, include_suspended: bool = True) -> list:
        """
        获取股票代码列表。

        Args:
            include_delisted: 是否包含退市股（status='D'）。
                False 时只返回上市/暂停上市股票，避免全市场扫描时的幸存者偏差。
                注意：数据源（TDX / BaoStock / AKShare）是否返回退市股数据取决于
                其本身的覆盖范围，本开关仅控制查询侧过滤。
            include_suspended: 是否包含暂停上市股（status='P'）。

        Returns:
            list[str]: ts_code 列表，如 ['000001.SZ', '600000.SH', ...]
        """
        from src.database.models.stock import StockBasic

        session = self.get_session()
        try:
            q = session.query(StockBasic.ts_code).filter(
                StockBasic.ts_code.isnot(None),
            )
            if not include_delisted:
                q = q.filter(StockBasic.status != 'D')
            if not include_suspended:
                q = q.filter(StockBasic.status != 'P')

            rows = q.all()
            return [r[0] for r in rows]
        except (OSError, RuntimeError) as e:
            logger.exception(f"查询股票代码列表失败: {e}")
            return []

    def get_session(self, expire_on_commit=False):
        """
        获取数据库会话

        Args:
            expire_on_commit: 是否在提交后过期对象，默认为False以提高性能

        Returns:
            sqlalchemy.orm.Session: 数据库会话对象
        """
        retry_count = 3
        for i in range(retry_count):
            try:
                # 检查现有会话是否有效
                if self.session:
                    try:
                        # 测试会话是否仍然有效
                        self.session.execute(text("SELECT 1"))
                    # 兜底理由：会话探活失败按失效处理并重建
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"现有数据库会话已失效，将创建新会话: {e}")
                        self._cleanup_session()

                # 如果没有有效会话，创建新会话
                if not self.session:
                    if not self.engine:
                        self.connect()
                    else:
                        # 创建新的会话
                        self.Session = scoped_session(sessionmaker(
                            bind=self.engine,
                            expire_on_commit=expire_on_commit
                        ))
                        self.session = self.Session()
                        logger.debug("创建新的数据库会话")

                return self.session

            except Exception as e:
                logger.warning(f"获取数据库会话失败 (尝试 {i+1}/{retry_count}): {e}")
                # 清理无效会话
                self._cleanup_session()
                # 如果是最后一次尝试，抛出异常
                if i == retry_count - 1:
                    logger.exception(f"获取数据库会话失败: {e}")
                    raise
                # 等待一段时间后重试
                import time
                time.sleep(1)

    @contextmanager
    def transaction(self):
        """
        跨表写事务边界：成功提交、异常回滚，保证同一逻辑单元的多表写入原子性。

        用法：
            with db_manager.transaction() as session:
                session.add(...)
                session.delete(...)
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise

    def _cleanup_session(self):
        """
        清理无效的数据库会话
        """
        try:
            if self.session:
                try:
                    self.session.close()
                except (OSError, RuntimeError, ValueError) as e:
                    logger.debug(f"关闭会话时出错（可能已失效）: {e}")
                finally:
                    self.session = None

            if self.Session:
                try:
                    self.Session.remove()
                except (OSError, RuntimeError, ValueError) as e:
                    logger.debug(f"移除会话作用域时出错: {e}")
                finally:
                    self.Session = None
        except (OSError, RuntimeError, ValueError) as e:
            logger.warning(f"清理数据库会话时出错: {e}")
            # 强制重置
            self.session = None
            self.Session = None

    def cleanup(self):
        """
        清理所有数据库资源
        """
        try:
            self.disconnect()
        # 兜底理由：资源清理失败不得阻断退出流程
        except Exception as e:  # noqa: BLE001
            logger.warning(f"清理数据库资源时出错: {e}")

    def is_connected(self):
        """
        检查数据库连接状态

        Returns:
            bool: 如果数据库连接正常返回True，否则返回False
        """
        if not self.engine:
            return False

        try:
            # 尝试通过引擎获取连接，测试连接是否有效
            with self.engine.connect():
                return True
        except (OSError, RuntimeError, ValueError) as e:
            logger.warning(f"数据库连接检查失败: {e}")
            return False
