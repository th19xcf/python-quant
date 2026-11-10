#!/usr/bin/env python3

"""
内存管理器模块
提供更高级的内存管理功能，包括内存监控、智能缓存管理和资源限制
"""

import gc
import json
import linecache
import os
import threading
import time
import tracemalloc
from datetime import datetime
from typing import Any, Dict, List

import psutil
from loguru import logger

#: 显式开启 ``tracemalloc`` 的环境变量。默认不开启：``tracemalloc`` 会给进程内
#: 每一次 Python 分配插桩，实测把回测热路径拖慢 3.5×、分配密集型路径拖慢 9.3×，
#: 而 parquet/向量化部分（Rust 侧分配）完全不受益。内存总量监控走 psutil，不需要它。
MEM_TRACE_ENV = 'QUANT_MEM_TRACE'
_TRUTHY = {'1', 'true', 'yes', 'on'}


def mem_trace_requested() -> bool:
    """判断是否通过环境变量显式要求开启 ``tracemalloc`` 跟踪。"""
    return os.environ.get(MEM_TRACE_ENV, '').strip().lower() in _TRUTHY


def _get_caches() -> tuple[Any, Any]:
    """延迟导入缓存单例，避免 utils 在导入期反向依赖 data / tech_analysis。"""
    from src.data.data_cache import global_data_cache
    from src.tech_analysis.indicator_cache import global_indicator_cache

    return global_data_cache, global_indicator_cache


class MemoryManager:
    """
    内存管理器
    提供内存监控、智能缓存管理和资源限制功能
    """

    def __init__(self) -> None:
        """
        初始化内存管理器
        """
        self.process = psutil.Process(os.getpid())
        self.memory_thresholds = {
            'critical': 90.0,  # 临界内存使用百分比
            'high': 80.0,       # 高内存使用百分比
            'medium': 60.0,     # 中等内存使用百分比
            'low': 40.0         # 低内存使用百分比
        }
        self.memory_history: List[Dict[str, Any]] = []  # 内存使用历史记录
        self.monitoring_enabled = False
        self.monitoring_thread: threading.Thread | None = None
        self.monitoring_interval = 5  # 监控间隔（秒）
        # 停止信号：用 Event 等待代替 time.sleep，退出时立即唤醒监控线程，
        # 避免关窗被 sleep 中的 join(timeout) 拖住
        self._stop_event = threading.Event()
        self.tracemalloc_enabled = False
        self.memory_snapshots: List[Dict[str, Any]] = []  # 内存快照
        self.auto_optimization_enabled = True  # 自动优化开关
        self.memory_usage_patterns: List[Dict[str, Any]] = []  # 内存使用模式
        self.linecache_trim_threshold = 5000  # 超过该条目数时清理 linecache
        self.gc_min_interval = 60.0  # 两次强制 GC 的最小间隔（秒），避免监控循环里反复停顿
        self._last_gc_time = 0.0
        # 泄漏判定：同一 traceback 连续增长 3 次快照且累计超过 1MB 才告警。
        # 单次增长多是整表重建等瞬时分配（如 QTableWidgetItem），不应报 WARNING。
        self.leak_growth_min_snapshots = 3
        self.leak_growth_threshold_bytes = 1024 * 1024
        self._growth_streaks: Dict[str, int] = {}
        self._growth_totals: Dict[str, int] = {}

        # tracemalloc 默认关闭，仅在显式设置 QUANT_MEM_TRACE=1 时开启。
        # 关闭后只是拿不到 `_take_memory_snapshot` 的分配明细，内存总量、
        # 阈值判定、缓存回收、GC 冷却全部走 psutil，功能不降级为报错。
        if mem_trace_requested():
            try:
                tracemalloc.start()
                self.tracemalloc_enabled = True
                logger.info(f"内存跟踪已启动（{MEM_TRACE_ENV}=1）")
            except RuntimeError as e:
                logger.warning(f"启动内存跟踪失败: {e}")
        else:
            logger.debug(f"内存跟踪未启用（设置 {MEM_TRACE_ENV}=1 可开启分配明细）")

    def get_memory_usage(self) -> Dict[str, Any]:
        """
        获取当前内存使用情况

        Returns:
            Dict[str, Any]: 内存使用信息
        """
        memory_info = self.process.memory_info()
        virtual_memory = psutil.virtual_memory()

        # 计算内存使用百分比
        process_memory_percent = (memory_info.rss / 1024 / 1024) / (virtual_memory.total / 1024 / 1024) * 100

        return {
            'process_rss_mb': memory_info.rss / 1024 / 1024,  # 进程使用的物理内存
            'process_vms_mb': memory_info.vms / 1024 / 1024,  # 进程使用的虚拟内存
            'process_percent': process_memory_percent,
            'system_total_mb': virtual_memory.total / 1024 / 1024,
            'system_available_mb': virtual_memory.available / 1024 / 1024,
            'system_used_mb': virtual_memory.used / 1024 / 1024,
            'system_percent': virtual_memory.percent,
            'memory_level': self._get_memory_level(process_memory_percent),
            'timestamp': time.time()
        }

    def _get_memory_level(self, memory_percent: float) -> str:
        """
        获取内存使用级别

        Args:
            memory_percent: 内存使用百分比

        Returns:
            str: 内存使用级别
        """
        if memory_percent >= self.memory_thresholds['critical']:
            return 'critical'
        elif memory_percent >= self.memory_thresholds['high']:
            return 'high'
        elif memory_percent >= self.memory_thresholds['medium']:
            return 'medium'
        elif memory_percent >= self.memory_thresholds['low']:
            return 'low'
        else:
            return 'very_low'

    def start_monitoring(self) -> None:
        """
        启动内存监控
        """
        if not self.monitoring_enabled:
            self.monitoring_enabled = True
            self._stop_event.clear()
            self.monitoring_thread = threading.Thread(target=self._monitor_memory, daemon=True)
            self.monitoring_thread.start()
            logger.info("内存监控已启动")

    def stop_monitoring(self) -> None:
        """
        停止内存监控
        """
        self.monitoring_enabled = False
        # 立即唤醒可能正在 sleep 的监控线程，join 只作兜底
        self._stop_event.set()
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=1)
            logger.info("内存监控已停止")

    def _monitor_memory(self) -> None:
        """
        内存监控线程函数
        """
        while self.monitoring_enabled:
            try:
                memory_info = self.get_memory_usage()
                self.memory_history.append({
                    'timestamp': time.time(),
                    'memory_usage': memory_info
                })

                # 只保留最近100条记录
                if len(self.memory_history) > 100:
                    self.memory_history = self.memory_history[-100:]

                # 分析内存使用模式
                self._analyze_memory_patterns()

                # 检查内存使用情况并采取相应措施
                if self.auto_optimization_enabled:
                    self._check_memory_and_act(memory_info)

                # 定期拍摄内存快照
                if self.tracemalloc_enabled and len(self.memory_history) % 10 == 0:
                    self._take_memory_snapshot()

                # 动态调整监控间隔
                self._adjust_monitoring_interval(memory_info)

                # 可被 stop_monitoring 立即唤醒，退出时不必等满一个监控间隔
                self._stop_event.wait(self.monitoring_interval)
            except MemoryError as e:
                # MemoryError 不能被监控循环吞掉继续轮询，否则会在内存耗尽时反复抖动
                logger.critical(f"内存监控线程内存耗尽，停止监控: {e}")
                self.monitoring_enabled = False
                return
            # 兜底理由：内存监控为旁路线程，失败不得终止主流程
            except Exception as e:  # noqa: BLE001
                logger.error(f"内存监控出错: {str(e)}")
                # 可被 stop_monitoring 立即唤醒，退出时不必等满一个监控间隔
                self._stop_event.wait(self.monitoring_interval)

    def _analyze_memory_patterns(self) -> None:
        """
        分析内存使用模式
        """
        if len(self.memory_history) < 10:
            return

        # 计算最近10分钟的内存使用趋势
        recent_history = self.memory_history[-10:]
        memory_values = [h['memory_usage']['process_percent'] for h in recent_history]

        # 计算平均值和标准差
        avg_memory = sum(memory_values) / len(memory_values)
        std_memory = (sum((x - avg_memory) ** 2 for x in memory_values) / len(memory_values)) ** 0.5

        # 检测内存使用模式
        pattern = {
            'timestamp': time.time(),
            'average_memory': avg_memory,
            'std_memory': std_memory,
            'trend': 'stable' if std_memory < 5 else 'volatile',
            'level': self._get_memory_level(avg_memory)
        }

        self.memory_usage_patterns.append(pattern)

        # 只保留最近20个模式
        if len(self.memory_usage_patterns) > 20:
            self.memory_usage_patterns = self.memory_usage_patterns[-20:]

    def _adjust_monitoring_interval(self, memory_info: Dict[str, Any]) -> None:
        """
        动态调整监控间隔

        Args:
            memory_info: 内存使用信息
        """
        memory_level = memory_info['memory_level']

        # 根据内存使用级别调整监控间隔
        if memory_level in ['critical', 'high']:
            # 内存使用较高时，增加监控频率
            self.monitoring_interval = 2
        elif memory_level == 'medium':
            # 内存使用中等时，保持默认监控频率
            self.monitoring_interval = 5
        else:
            # 内存使用较低时，减少监控频率
            self.monitoring_interval = 10

    def _take_memory_snapshot(self) -> None:
        """
        拍摄内存快照

        ``tracemalloc`` 未运行时（默认状态）直接跳过并返回「无数据」：
        快照属于旁路观测，缺失不影响内存阈值判定、缓存回收与 GC 冷却，
        因此不能抛错、也不应刷 ERROR 日志。
        """
        if not tracemalloc.is_tracing():
            logger.debug("tracemalloc 未运行，跳过内存快照")
            return
        try:
            # 先回收一次垃圾（受 gc_min_interval 冷却），避免把已断开引用的
            # 瞬时对象算成泄漏；再裁剪 linecache，减少 stdlib 缓存干扰
            self._collect_garbage()
            self._trim_linecache_cache()

            snapshot = tracemalloc.take_snapshot()
            self.memory_snapshots.append({
                'timestamp': time.time(),
                'snapshot': snapshot
            })

            # 只保留最近5个快照
            if len(self.memory_snapshots) > 5:
                self.memory_snapshots = self.memory_snapshots[-5:]

            # 分析快照，检测内存泄漏
            self._analyze_memory_snapshot(snapshot)
        except MemoryError:
            raise
        # 兜底理由：内存快照为旁路观测，失败不得影响主流程
        except Exception as e:  # noqa: BLE001
            logger.error(f"拍摄内存快照失败: {e}")

    def _analyze_memory_snapshot(self, snapshot: tracemalloc.Snapshot) -> None:
        """
        分析内存快照，检测内存泄漏

        Args:
            snapshot: 内存快照
        """
        try:
            # 按内存使用量排序，取前10个
            top_stats = snapshot.statistics('lineno')

            logger.debug("内存使用前10项:")
            for stat in top_stats[:10]:
                logger.debug(f"  {stat}")

            # 检测内存泄漏
            if len(self.memory_snapshots) >= 2:
                prev_snapshot = self.memory_snapshots[-2]['snapshot']
                stats = snapshot.compare_to(prev_snapshot, 'lineno')

                # 只看增长项（负增长无需追踪）
                growing_stats = [stat for stat in stats if stat.size_diff > 0]

                # 过滤标准库 linecache 噪音，避免误报“内存泄漏”
                leak_candidates = []
                filtered_noise = []
                for diff in growing_stats:
                    trace_text = str(diff.traceback).lower()
                    if 'linecache.py' in trace_text:
                        filtered_noise.append(diff)
                        continue
                    leak_candidates.append(diff)

                if filtered_noise:
                    logger.info(
                        f"检测到 {len(filtered_noise)} 条 linecache 缓存增长，"
                        "已按缓存噪音忽略"
                    )

                confirmed, single = self._classify_growth(leak_candidates)

                if confirmed:
                    logger.warning(
                        "检测到可能的内存泄漏（同一位置连续 "
                        f"{self.leak_growth_min_snapshots} 次快照持续增长）:"
                    )
                    for diff in confirmed[:5]:
                        logger.warning(f"  {diff}")
                if single:
                    logger.info(
                        f"检测到 {len(single)} 条单次内存增长，"
                        f"未达连续 {self.leak_growth_min_snapshots} 次快照判定，降为 INFO 忽略"
                    )
        except MemoryError:
            raise
        # 兜底理由：快照分析为旁路观测，失败不得影响主流程
        except Exception as e:  # noqa: BLE001
            logger.error(f"分析内存快照失败: {e}")

    def _classify_growth(self, diffs: List[Any]) -> tuple[List[Any], List[Any]]:
        """按 traceback 追踪连续增长，返回 (持续增长候选, 单次增长)。

        只有同一 traceback 连续 ``leak_growth_min_snapshots`` 次快照都在增长、
        且累计增长超过 ``leak_growth_threshold_bytes`` 才判定为泄漏候选；
        中间任一快照未增长即重置该位置的连续计数。

        Args:
            diffs: 当前快照相对上一快照的增长项（需含 traceback 与 size_diff）
        """
        streaks: Dict[str, int] = {}
        totals: Dict[str, int] = {}
        confirmed: List[Any] = []
        single: List[Any] = []

        for diff in diffs:
            key = str(diff.traceback)
            streak = self._growth_streaks.get(key, 0) + 1
            total = self._growth_totals.get(key, 0) + diff.size_diff
            streaks[key] = streak
            totals[key] = total

            if (streak >= self.leak_growth_min_snapshots
                    and total >= self.leak_growth_threshold_bytes):
                confirmed.append(diff)
            else:
                single.append(diff)

        # 只保留本轮仍增长的位置，未增长的自动清零，避免字典无限增长
        self._growth_streaks = streaks
        self._growth_totals = totals
        return confirmed, single

    def _trim_linecache_cache(self) -> None:
        """
        清理 linecache 缓存，降低快照分析噪音。
        """
        try:
            cache_size = len(linecache.cache)
            if cache_size >= self.linecache_trim_threshold:
                linecache.clearcache()
                logger.debug(f"linecache 缓存已清理，原条目数: {cache_size}")
        # 兜底理由：linecache 裁剪为旁路优化，失败可忽略
        except Exception as e:  # noqa: BLE001
            logger.debug(f"裁剪 linecache 缓存失败: {e}")

    def _check_memory_and_act(self, memory_info: Dict[str, Any]) -> None:
        """
        检查内存使用情况并采取相应措施

        Args:
            memory_info: 内存使用信息
        """
        memory_level = memory_info['memory_level']

        if memory_level == 'critical':
            logger.warning(f"内存使用临界: {memory_info['process_percent']:.2f}%")
            # 采取紧急措施
            self._take_emergency_measures()
        elif memory_level == 'high':
            logger.warning(f"内存使用较高: {memory_info['process_percent']:.2f}%")
            # 采取缓解措施
            self._take_relief_measures()
        elif memory_level == 'medium':
            logger.info(f"内存使用中等: {memory_info['process_percent']:.2f}%")
            # 采取预防措施
            self._take_preventive_measures()
        else:
            # 内存使用正常
            pass

    def _collect_garbage(self, force: bool = False) -> None:
        """
        带冷却的 GC：避免监控线程每轮（2~10 秒）都触发全量回收造成停顿抖动。

        force=True 用于 critical 级紧急处置，可绕过冷却。
        """
        now = time.time()
        if not force and now - self._last_gc_time < self.gc_min_interval:
            return
        self._last_gc_time = now
        gc.collect()

    def _take_emergency_measures(self) -> None:
        """
        采取紧急内存管理措施
        """
        logger.info("采取紧急内存管理措施")

        # 1. 强制垃圾回收（critical 级不受冷却限制）
        self._collect_garbage(force=True)

        # 2. 清空所有缓存（统一走缓存的 release_memory 入口，避免外部直接操作内部状态）
        try:
            global_data_cache, global_indicator_cache = _get_caches()
            global_data_cache.release_memory(0.0)
            global_indicator_cache.release_memory(0.0)
            logger.info("已清空数据缓存与指标缓存")
        # 兜底理由：清空缓存为旁路操作，失败不得影响主流程
        except Exception as e:  # noqa: BLE001
            logger.error(f"清空缓存失败: {str(e)}")

        # 3. 清理linecache
        try:
            linecache.clearcache()
            logger.info("已清空linecache缓存")
        # 兜底理由：linecache 清理为旁路操作，失败可忽略
        except Exception as e:  # noqa: BLE001
            logger.error(f"清空linecache失败: {str(e)}")

        # 4. 减少线程池大小
        # 注意：这里需要根据实际情况调整

        # 5. 记录内存使用情况
        self._log_memory_crisis()

    def _take_relief_measures(self) -> None:
        """
        采取缓解内存管理措施
        """
        logger.info("采取缓解内存管理措施")

        # 1. 执行垃圾回收（受冷却限制）
        self._collect_garbage()

        # 2. 清理linecache
        try:
            linecache.clearcache()
            logger.info("已清空linecache缓存")
        # 兜底理由：linecache 清理为旁路操作，失败可忽略
        except Exception as e:  # noqa: BLE001
            logger.error(f"清空linecache失败: {str(e)}")

        # 3. 优化缓存大小
        self.optimize_cache(target_ratio=0.5)

    def _take_preventive_measures(self) -> None:
        """
        采取预防内存管理措施
        """
        # 定期执行垃圾回收（受冷却限制，避免每轮监控都停顿）
        self._collect_garbage()

        # 分析内存使用趋势，预测未来内存需求
        self._predict_memory_usage()

    def _log_memory_crisis(self) -> None:
        """
        记录内存危机情况
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"memory_crisis_{timestamp}.json"

        try:
            global_data_cache, global_indicator_cache = _get_caches()
            crisis_info = {
                'timestamp': time.time(),
                'memory_info': self.get_memory_usage(),
                'memory_history': self.memory_history[-20:],
                'cache_stats': global_data_cache.get_stats() if global_data_cache else None,
                'indicator_cache_stats': global_indicator_cache.get_stats() if global_indicator_cache else None
            }

            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(crisis_info, f, ensure_ascii=False, indent=2)

            logger.info(f"内存危机信息已记录到 {log_file}")
        # 兜底理由：内存危机信息落盘为旁路操作，失败可忽略
        except Exception as e:  # noqa: BLE001
            logger.error(f"记录内存危机信息失败: {e}")

    def _predict_memory_usage(self) -> None:
        """
        预测内存使用趋势
        """
        if len(self.memory_history) < 5:
            return

        # 使用简单的线性回归预测内存使用
        recent_history = self.memory_history[-5:]
        times = [h['timestamp'] for h in recent_history]
        memory_values = [h['memory_usage']['process_percent'] for h in recent_history]

        # 计算趋势
        if len(times) > 1:
            # 计算斜率
            slope = (memory_values[-1] - memory_values[0]) / (times[-1] - times[0])

            # 预测5分钟后的内存使用
            predicted_memory = memory_values[-1] + slope * 300

            if predicted_memory > self.memory_thresholds['high']:
                logger.warning(f"预测5分钟后内存使用将达到 {predicted_memory:.2f}%，接近高内存阈值")
                # 只触发一次带冷却的 GC。不能回调 _take_preventive_measures：
                # 后者又会调用本方法，形成无限互递归（RecursionError）
                self._collect_garbage()

    def optimize_cache(self, target_ratio: float | None = None) -> None:
        """
        按内存压力释放缓存

        淘汰策略唯一归缓存自身所有：本方法不再读写缓存的 _max_size，
        只调用 release_memory 下压水位，避免与缓存的 LRU / 命中率策略互相覆盖。

        Args:
            target_ratio: 目标水位占 max_size 的比例，None 表示根据内存压力自动决定
        """
        memory_info = self.get_memory_usage()

        if target_ratio is None:
            if memory_info['memory_level'] in ['high', 'critical']:
                # 高压力才下压水位；正常时既不扩张也不收缩，交给缓存自身按命中率调节
                target_ratio = 0.5
            else:
                return

        logger.info(f"按目标水位 {target_ratio:.0%} 释放缓存")
        global_data_cache, global_indicator_cache = _get_caches()
        global_data_cache.release_memory(target_ratio)
        global_indicator_cache.release_memory(target_ratio)

# 创建全局内存管理器实例
global_memory_manager = MemoryManager()
