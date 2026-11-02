#!/usr/bin/env python3

"""
Baostock 会话管理 Mixin

集中登录、重试、登录态校验、登出与析构逻辑。
"""

import baostock as bs
from loguru import logger

from src.utils.exception_handler import handle_exception_with_retry
from src.utils.exceptions import DataSourceConnectionError


class BaostockSessionMixin:
    """Baostock 登录会话管理（登录/重试/登出）。"""

    # Baostock 的 recv 循环没有自带超时，而全市场分页查询单次需要几十秒。
    # 30s 的进程级默认超时会在服务端换页时打断接收，导致半截报文解码失败
    # （'utf-8' codec can't decode ... unexpected end of data）。
    BAOSTOCK_READ_TIMEOUT_SECONDS = 120.0

    def _login_baostock(self):
        """
        登录Baostock
        """
        try:
            logger.info("正在连接Baostock服务器...")
            lg = bs.login()

            if lg is None:
                logger.warning("Baostock API返回None")
                self.bs_login = False
                return

            if lg.error_code == '0':
                logger.info("Baostock登录成功")
                self.bs_login = True
                self._apply_baostock_read_timeout()
            else:
                logger.warning(f"Baostock登录失败: {lg.error_msg}")
                self.bs_login = False
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.exception(f"Baostock登录异常: {e}")
            self.bs_login = False
        # 兜底理由：第三方 SDK 异常类型不可预知，登录失败标记不可用
        except Exception as e:  # noqa: BLE001
            # 捕获其他所有异常
            logger.exception(f"Baostock登录发生未知异常: {e}")
            self.bs_login = False

    def _apply_baostock_read_timeout(self) -> None:
        """给 baostock 专用 socket 设置读超时。

        只作用于 baostock 自己的连接：socket.setdefaulttimeout 是进程级设置，
        会连带影响进程内其它所有裸 socket，且 30s 对全市场分页查询过短。
        """
        try:
            from baostock.common import context
            sock = getattr(context, 'default_socket', None)
            if sock is not None:
                sock.settimeout(self.BAOSTOCK_READ_TIMEOUT_SECONDS)
        # 兜底理由：上游内部结构可能变化，设置失败不应阻断登录
        except Exception as e:  # noqa: BLE001
            logger.debug(f"设置 Baostock socket 读超时失败: {e}")

    @handle_exception_with_retry(max_retries=3, retry_delay=2)
    def _login_with_retry(self):
        """登录Baostock（带统一重试装饰器），登录失败抛出可重试异常"""
        self._login_baostock()
        if not self.bs_login:
            raise DataSourceConnectionError(
                source="baostock", message="登录失败")

    def _ensure_baostock_login(self, max_retries=3, retry_delay=2):
        """
        确保Baostock已登录，如果未登录则尝试登录

        Args:
            max_retries: 最大重试次数（保留兼容参数，实际由装饰器固定为3次）
            retry_delay: 重试间隔（秒）（保留兼容参数，实际由装饰器固定为2秒）

        Returns:
            bool: 登录状态，True表示已登录，False表示登录失败
        """
        if self.bs_login:
            return True

        logger.info("Baostock未登录，尝试登录...")
        try:
            self._login_with_retry()
            logger.info("Baostock登录成功")
            return True
        # 兜底理由：重试耗尽后返回失败，不向上抛出
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Baostock登录失败，已达到最大重试次数: {e}")
            return False

    def _logout_baostock(self):
        """
        登出Baostock
        """
        if self.bs_login:
            try:
                bs.logout()
                logger.info("Baostock登出成功")
                self.bs_login = False
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.exception(f"Baostock登出异常: {e}")

    def __del__(self) -> None:
        """
        析构函数，确保登出Baostock
        """
        self._logout_baostock()
