#!/usr/bin/env python3

"""行情表格扫描：通达信日线文件筛选、并发解析与行数据组装。"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from src.utils.logger import logger

# 行情表格扫描的并发线程数：每个文件只读末尾 2 条记录，属 IO 密集型
_QUOTE_SCAN_WORKERS = 8


class MarketScanService:
    """后台扫描通达信日线文件，组装行情表格行。"""

    def __init__(self, window):
        self.window = window

    @staticmethod
    def _list_day_files(directory, prefix):
        """列出指定目录下以 prefix 开头的 .day 文件；目录不存在时返回空列表。"""
        if not directory.exists():
            return []
        return list(directory.glob(f'{prefix}*.day'))

    @staticmethod
    def _filter_tdx_stock_files(tdx_data_path, stock_type):
        """按股票类型筛出对应的通达信日线文件列表。"""
        sh_files = MarketScanService._list_day_files(tdx_data_path / 'sh' / 'lday', 'sh')
        sz_files = MarketScanService._list_day_files(tdx_data_path / 'sz' / 'lday', 'sz')
        bj_files = MarketScanService._list_day_files(tdx_data_path / 'bj' / 'lday', 'bj')

        if stock_type == "全部A股":
            return sh_files + sz_files + bj_files
        if stock_type == "上证A股":
            return sh_files
        if stock_type == "深证A股":
            # 仅显示深证个股，指数/基金在后续通过 stock_basic 过滤
            return sz_files
        if stock_type == "创业板":
            return [f for f in sz_files if f.stem[2:5] == "300"]
        if stock_type == "科创板":
            return [f for f in sh_files if f.stem[2:5] == "688"]
        if stock_type in ("北交所", "京市个股"):
            return bj_files
        return []

    def _build_stock_name_map(self):
        """从 stock_basic 构建 {ts_code: name} 映射；空表返回 {}。"""
        stock_name_df = self.window.data_manager.get_stock_basic()
        if stock_name_df.is_empty():
            return {}
        # 转换为字典，格式：{ts_code: name}
        return dict(zip(stock_name_df['ts_code'].to_list(), stock_name_df['name'].to_list(), strict=False))

    @staticmethod
    def _resolve_stock_name(name_map, candidates):
        """按候选 ts_code 格式顺序命中即返回，全部未命中返回 None。"""
        for ts_code in candidates:
            if ts_code in name_map:
                return name_map[ts_code]
        return None

    @staticmethod
    def _parse_day_file_code(file_name):
        """从 sh/sz/bj 文件名解析 (code, market)；B 股或未知前缀返回 None。"""
        if file_name.startswith('sh'):
            code = file_name[2:]
            # 忽略 B 股：900 开头（沪市 B 股）
            if code.startswith('900'):
                return None
            return code, "SH"
        if file_name.startswith('sz'):
            code = file_name[2:]
            # 忽略 B 股：200 开头（深市 B 股）
            if code.startswith('200'):
                return None
            return code, "SZ"
        if file_name.startswith('bj'):
            return file_name[2:], "BJ"
        return None

    @staticmethod
    def _emit_market_progress(signals, task_id, value):
        """发送进度信号；未提供 signals 时静默跳过。"""
        if signals:
            signals.progress.emit(task_id, value, 100)

    @staticmethod
    def _format_market_row(quote, ts_code, stock_name):
        """把行情快照拼成 13 列表格行，列顺序与格式保持不变。"""
        return (
            quote.date,
            ts_code,
            stock_name,
            f"{quote.pct_chg:.2f}",
            f"{quote.close:.2f}",
            f"{quote.change:.2f}",
            f"{quote.volume:,}",
            f"{quote.amount:,}",
            f"{quote.open:.2f}",
            f"{quote.high:.2f}",
            f"{quote.low:.2f}",
            f"{quote.preclose:.2f}",
            f"{quote.amplitude:.2f}%"
        )

    def _quote_file_to_row(self, file_path, name_map):
        """解析单个 .day 文件为 (行情行, 日期)；股票不适用时对应位置返回 None。"""
        quote = self.window.data_manager.read_local_quote_file(file_path)
        if quote is None:
            return None, None

        date = datetime.strptime(quote.date, '%Y-%m-%d').date()

        parsed = self._parse_day_file_code(file_path.stem)
        if parsed is None:
            return None, date

        code, market = parsed
        ts_code = f"{code}.{market}"
        # 尝试不同的ts_code格式
        ts_code_formats = [
            ts_code,
            f"{code}.{market.lower()}",
            f"{market}{code}",
            f"{market.lower()}{code}"
        ]

        # 严格仅展示 stock_basic 已登记的个股（过滤指数/基金）
        stock_name = self._resolve_stock_name(name_map, ts_code_formats)
        if not stock_name:
            return None, date

        return self._format_market_row(quote, ts_code, stock_name), date

    def _scan_market_quotes_files(self, files, name_map, signals, task_id):
        """并发扫描文件列表，返回 (按原文件顺序排列的表格行, 全部文件中的最新交易日)。"""
        total_files = len(files)
        if total_files == 0:
            self._emit_market_progress(signals, task_id, 80)
            return [], None

        update_interval = max(1, total_files // 10)

        def _read_one(index, file_path):
            try:
                # 只在每100个文件记录一次日志，减少IO开销
                if index % 100 == 0:
                    logger.info(f"正在解析文件: {file_path} ({index+1}/{total_files})")
                return self._quote_file_to_row(file_path, name_map)
            except (OSError, RuntimeError, ValueError) as e:
                logger.warning(f"解析文件{file_path}失败: {e}")
                return None, None

        # 并发读取尾部记录，结果按原索引回填，保证表格行顺序与逐文件扫描时一致
        rows_by_index = [None] * total_files
        with ThreadPoolExecutor(max_workers=_QUOTE_SCAN_WORKERS) as pool:
            futures = {pool.submit(_read_one, i, f): i for i, f in enumerate(files)}
            for done, future in enumerate(as_completed(futures), 1):
                rows_by_index[futures[future]] = future.result()
                if done % update_interval == 0:
                    progress = 20 + int((done / total_files) * 60)
                    self._emit_market_progress(signals, task_id, progress)

        latest_date = None
        table_rows = []
        for row, date in rows_by_index:
            if date is not None and (latest_date is None or date > latest_date):
                latest_date = date
            if row is not None:
                table_rows.append(row)

        # 发送进度信号
        self._emit_market_progress(signals, task_id, 80)

        return table_rows, latest_date

    def _collect_market_rows(self, stock_type, task_id, signals):
        """后台任务编排：取路径 → 筛文件 → 扫文件 → 组装成功/失败结果。"""
        # 构建通达信日线数据目录路径
        tdx_data_path = Path(self.window.data_manager.config.data.tdx_data_path)
        filtered_files = self._filter_tdx_stock_files(tdx_data_path, stock_type)

        logger.info(f"找到{len(filtered_files)}个符合条件的通达信股票数据文件")

        if not filtered_files:
            return {"success": False, "message": f"没有找到{stock_type}的通达信股票数据文件，请检查路径是否正确"}

        name_map = self._build_stock_name_map()
        table_rows, latest_date = self._scan_market_quotes_files(filtered_files, name_map, signals, task_id)

        if not table_rows:
            return {"success": False, "message": f"没有解析到任何{stock_type}数据，请检查文件格式是否正确"}

        # 不再过滤出最新交易日的数据，保留所有股票的最新可用数据
        # 这样可以确保显示所有股票，而不是只显示最新交易日有数据的股票
        if latest_date:
            latest_date_str = latest_date.strftime('%Y-%m-%d')
            logger.info(f"最新交易日: {latest_date_str}，共{len(table_rows)}只{stock_type}股票有数据")

        # 发送进度信号
        self._emit_market_progress(signals, task_id, 90)
        self._emit_market_progress(signals, task_id, 100)

        return {"success": True, "table_data": table_rows, "stock_count": len(table_rows), "stock_type": stock_type}
