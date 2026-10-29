#!/usr/bin/env python3

"""
指标缓存模块，提供高效的指标计算结果缓存机制
"""

import hashlib
import threading
import time
from typing import Any, Dict, List, TypeVar

import polars as pl
from loguru import logger

# 定义缓存键和值的类型变量
K = TypeVar('K')
V = TypeVar('V')

# 参与缓存键哈希的原始输入列：内置指标读取的全部列必须在此覆盖。
# 派生指标列（ma5、macd 等）不参与哈希，保证 get/set 两侧列集变化不影响键。
_CACHE_KEY_INPUT_COLUMNS = (
    'date', 'datetime', 'time',
    'open', 'high', 'low', 'close', 'volume',
    'amount', 'turnover',
    'up_count', 'down_count',
)

# 原始输入列集合，用于整表缓存结果裁剪
_RAW_INPUT_COLUMNS = frozenset(_CACHE_KEY_INPUT_COLUMNS)

# 缓存控制参数：只影响存放策略，不影响计算结果，不参与语义键
_CACHE_KEY_EXCLUDED_PARAMS = frozenset({'ttl', 'formula_version'})

# 参与公式指纹计算的源码：公式实现或其注册表变更都必须使旧缓存失效
_FORMULA_SOURCE_FILES = (
    'indicator_calculator.py',
    'indicator_registry.py',
    'gpu_acceleration.py',
)
_FORMULA_SOURCE_DIRS = ('indicators',)

_formula_version: str | None = None
_formula_version_lock = threading.Lock()


def _compute_formula_version() -> str:
    """对指标公式相关源码做内容指纹（与 git 版本解耦，改动即失效）"""
    from pathlib import Path

    base = Path(__file__).resolve().parent
    files = [base / name for name in _FORMULA_SOURCE_FILES]
    for dirname in _FORMULA_SOURCE_DIRS:
        directory = base / dirname
        if directory.is_dir():
            files.extend(sorted(directory.rglob('*.py')))

    digest = hashlib.md5()
    for path in files:
        if not path.is_file():
            continue
        digest.update(path.name.encode('utf-8'))
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def get_formula_version() -> str:
    """获取当前公式版本（懒计算，供缓存键使用）"""
    global _formula_version
    if _formula_version is None:
        with _formula_version_lock:
            if _formula_version is None:
                try:
                    _formula_version = _compute_formula_version()
                except Exception as e:  # 源码不可读时退化为固定标记，避免缓存键随机化
                    logger.warning(f"计算指标公式版本失败，使用 unknown: {e}")
                    _formula_version = 'unknown'
    return _formula_version


def set_formula_version(version: str | None) -> None:
    """
    显式指定公式版本

    Args:
        version: 版本标识；None 表示恢复为源码指纹自动计算
    """
    global _formula_version
    with _formula_version_lock:
        _formula_version = version


def _hash_polars_data(data: pl.DataFrame) -> str:
    """
    对原始输入列做全量、顺序敏感的内容哈希。

    旧实现只采样首尾各 50 行并用 sum 聚合：既丢失行内顺序，也看不到中间行，
    不同数据可能命中同一缓存条目。这里逐行哈希后按行序拼接字节。
    """
    if not isinstance(data, pl.DataFrame):
        raise ValueError(f"缓存键仅支持 Polars DataFrame，实际为 {type(data).__name__}")

    cols = [c for c in _CACHE_KEY_INPUT_COLUMNS if c in data.columns]
    if not cols:
        raise ValueError("数据中没有可用于生成缓存键的输入列")

    hasher = hashlib.md5()
    hasher.update('|'.join(f'{c}:{data.schema[c]}' for c in cols).encode())
    hasher.update(f'|rows={data.height}'.encode())

    if data.height:
        try:
            row_hashes = data.select(cols).hash_rows(seed=42)
        except Exception as e:
            logger.debug(f"Polars hash_rows 失败，降级 numpy 哈希: {e}")
            hasher.update(data.select(cols).to_numpy().tobytes())
        else:
            hasher.update(row_hashes.to_numpy().tobytes())

    return hasher.hexdigest()


def _canonical_param(value: Any) -> str:
    """
    将单个参数值规范化为带类型标记的确定性字符串。

    使用类型标记避免 int/str、bool/int 等不同取值碰撞。
    """
    if isinstance(value, (bool, int, float, str, type(None))):
        return f'{type(value).__name__}:{value!r}'
    if isinstance(value, (list, tuple)):
        return f'{type(value).__name__}[' + ','.join(_canonical_param(v) for v in value) + ']'
    if isinstance(value, (set, frozenset)):
        return 'set{' + ','.join(sorted(_canonical_param(v) for v in value)) + '}'
    if isinstance(value, dict):
        return 'dict{' + ','.join(
            f'{k}={_canonical_param(value[k])}' for k in sorted(value, key=str)
        ) + '}'
    if isinstance(value, pl.DataFrame):
        return f'pl.DataFrame:{_hash_polars_data(value)}'
    if isinstance(value, pl.Series):
        try:
            return f'pl.Series:{hashlib.md5(value.to_numpy().tobytes()).hexdigest()}'
        except Exception:
            return f'pl.Series:{value!r}'
    if hasattr(value, 'tobytes'):
        return f'{type(value).__name__}:{hashlib.md5(value.tobytes()).hexdigest()}'
    try:
        import pandas as pd

        if isinstance(value, (pd.DataFrame, pd.Series)):
            row_hashes = pd.util.hash_pandas_object(value, index=True)
            return f'{type(value).__name__}:{hashlib.md5(row_hashes.to_numpy().tobytes()).hexdigest()}'
    except Exception:
        pass
    return f'{type(value).__name__}:{value!r}'


def _canonical_params(params: Dict[str, Any]) -> str:
    """把参数字典规范化为确定性字符串，排除缓存控制参数。"""
    items = [(k, v) for k, v in params.items() if k not in _CACHE_KEY_EXCLUDED_PARAMS]
    return '&'.join(
        f'{k}={_canonical_param(v)}' for k, v in sorted(items, key=lambda kv: str(kv[0]))
    )


class IndicatorCacheEntry[V]:
    """
    缓存条目类，用于存储缓存值及其元数据
    """

    def __init__(self, value: V, expire_time: float | None = None):
        """
        初始化缓存条目

        Args:
            value: 缓存值
            expire_time: 过期时间（时间戳），None表示永不过期
        """
        self.value = value
        self.expire_time = expire_time
        self.access_time = time.time()  # 上次访问时间
        self.hit_count = 1  # 命中次数

    def is_expired(self) -> bool:
        """
        检查缓存是否过期

        Returns:
            bool: 是否过期
        """
        if self.expire_time is None:
            return False
        return time.time() > self.expire_time

    def update_access(self):
        """
        更新访问信息
        """
        self.access_time = time.time()
        self.hit_count += 1


class IndicatorCache:
    """
    指标缓存类，提供高效的指标计算结果缓存机制
    支持自动过期、LRU淘汰、命中率统计等功能
    """

    def __init__(self, max_size: int = 1000, default_ttl: int = 3600, dynamic_size: bool = True):
        """
        初始化指标缓存

        Args:
            max_size: 缓存最大条目数
            default_ttl: 默认过期时间（秒），0表示永不过期
            dynamic_size: 是否启用动态缓存大小调整
        """
        # 缓存存储
        self._cache: Dict[str, IndicatorCacheEntry[pl.DataFrame]] = {}
        # 缓存配置
        self._max_size = max_size
        self._default_ttl = default_ttl if default_ttl > 0 else None
        self._dynamic_size = dynamic_size
        self._min_size = max(100, max_size // 2)  # 最小缓存大小
        self._max_size_limit = max_size * 2  # 最大缓存大小限制

        # 缓存统计
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._last_size_adjustment = time.time()
        self._size_adjustment_interval = 3600  # 缓存大小调整间隔（秒）

        # 内存监控线程会并发触发释放/淘汰，所有缓存读写必须串行化
        self._lock = threading.RLock()

    def _generate_cache_key(self, data: pl.DataFrame, indicator_type: str, **params) -> str:
        """
        生成唯一的缓存键

        数据侧：对原始输入列做全量、顺序敏感哈希（周期、复权、数据源差异都会
        体现在价格/成交量/日期取值上）；参数侧：带类型标记的确定性序列化，
        并排除 ttl 等缓存控制参数。

        Args:
            data: 输入数据
            indicator_type: 指标类型
            **params: 指标计算参数

        Returns:
            str: 唯一的缓存键
        """
        data_hash = _hash_polars_data(data)
        # 公式版本参与参数哈希：公式源码变更后旧结果不再命中
        version = params.get('formula_version') or get_formula_version()
        params_hash = hashlib.md5(
            f"{_canonical_params(params)}&formula={version}".encode('utf-8')
        ).hexdigest()
        return f"{indicator_type}_{data_hash}_{params_hash}"

    def get(self, data: pl.DataFrame, indicator_type: str, **params) -> pl.DataFrame | None:
        """
        获取缓存的指标计算结果

        Args:
            data: 输入数据
            indicator_type: 指标类型
            **params: 指标计算参数

        Returns:
            Optional[pl.DataFrame]: 缓存的计算结果，如果不存在或已过期则返回None
        """
        try:
            cache_key = self._generate_cache_key(data, indicator_type, **params)
        except ValueError as e:
            logger.warning(f"生成缓存键失败: {str(e)}")
            with self._lock:
                self._misses += 1
            return None

        with self._lock:
            if cache_key not in self._cache:
                self._misses += 1
                # 尝试调整缓存大小
                self._adjust_cache_size()
                return None

            entry = self._cache[cache_key]

            # 检查是否过期
            if entry.is_expired():
                del self._cache[cache_key]
                self._evictions += 1
                self._misses += 1
                # 尝试调整缓存大小
                self._adjust_cache_size()
                return None

            # 更新访问信息
            entry.update_access()
            self._hits += 1

        logger.debug(f"缓存命中: {indicator_type}")
        # 尝试调整缓存大小
        self._adjust_cache_size()
        return entry.value

    def set(self, data: pl.DataFrame, result: pl.DataFrame, indicator_type: str, **params):
        """
        设置指标计算结果到缓存

        Args:
            data: 输入数据
            result: 计算结果
            indicator_type: 指标类型
            **params: 指标计算参数
        """
        try:
            cache_key = self._generate_cache_key(data, indicator_type, **params)
        except ValueError as e:
            logger.warning(f"生成缓存键失败: {str(e)}")
            return

        # 计算过期时间（ttl 只控制存放策略，已在键生成时排除）
        ttl = params.get('ttl', self._default_ttl)
        expire_time = time.time() + ttl if ttl is not None else None

        # 整表缓存（result 与 data 为同一对象）只保留对齐键 + 派生列，
        # 避免命中合并时把 OHLCV 等原始列重复并入（产生 _right 列膨胀）
        value = self._narrow_full_frame_result(data, result)

        # 创建缓存条目
        entry = IndicatorCacheEntry(value, expire_time)

        with self._lock:
            # 添加到缓存
            self._cache[cache_key] = entry

            # 检查缓存大小，超过上限则进行LRU淘汰
            if len(self._cache) > self._max_size:
                self._evict_lru()

            # 尝试调整缓存大小
            self._adjust_cache_size()

    def release_memory(self, target_ratio: float = 0.5) -> Dict[str, int]:
        """
        按目标水位释放缓存内存（内存管理器唯一允许的释放入口）

        Args:
            target_ratio: 目标水位占 max_size 的比例，0 表示清空全部

        Returns:
            Dict[str, int]: 释放结果，含过期删除数、LRU 淘汰数与释放后条目数
        """
        ratio = max(0.0, min(1.0, target_ratio))
        with self._lock:
            expired_keys = [k for k, e in self._cache.items() if e.is_expired()]
            for key in expired_keys:
                del self._cache[key]
            self._evictions += len(expired_keys)

            target_size = int(self._max_size * ratio)
            evicted = 0
            if target_size <= 0:
                evicted = len(self._cache)
                self._cache.clear()
                self._evictions += evicted
            else:
                while len(self._cache) > target_size:
                    self._evict_lru()
                    evicted += 1

            size = len(self._cache)

        logger.info(
            f"指标缓存释放内存: 目标水位={ratio:.0%}, 过期删除={len(expired_keys)}, "
            f"LRU淘汰={evicted}, 剩余={size}/{self._max_size}"
        )
        return {'expired': len(expired_keys), 'evicted': evicted, 'size': size}

    @staticmethod
    def _narrow_full_frame_result(data: pl.DataFrame, result: pl.DataFrame) -> pl.DataFrame:
        """
        当 result 与 data 为同一整表对象时，只保留对齐键与派生指标列。

        调用方普遍用 ``set(self.pl_df, self.pl_df, ...)`` 缓存整表，若原样存储，
        命中后 join 会把 OHLCV（含 date）再次并入，Polars 会生成 ``close_right``
        之类的重复列，污染后续计算所用 DataFrame。
        """
        if result is not data or not data.columns:
            return result
        key_col = data.columns[0]
        derived_cols = [c for c in result.columns if c not in _RAW_INPUT_COLUMNS and c != key_col]
        if not derived_cols:
            return result
        return result.select([key_col] + derived_cols)

    def _evict_lru(self):
        """
        执行LRU淘汰策略，移除最少使用的缓存条目
        """
        with self._lock:
            if not self._cache:
                return

            # 找到最少使用的条目（按访问时间和命中次数排序）
            # 优先淘汰访问时间早且命中次数少的条目
            lru_key = min(self._cache.keys(),
                         key=lambda k: (self._cache[k].access_time, -self._cache[k].hit_count))

            del self._cache[lru_key]
            self._evictions += 1
            logger.debug(f"LRU淘汰缓存: {lru_key}")

    def _adjust_cache_size(self):
        """
        动态调整缓存大小
        根据命中率和缓存使用情况自动调整缓存大小
        """
        with self._lock:
            if not self._dynamic_size:
                return

            current_time = time.time()
            if current_time - self._last_size_adjustment < self._size_adjustment_interval:
                return

            self._last_size_adjustment = current_time

            total_requests = self._hits + self._misses
            if total_requests < 100:  # 数据不足，不调整
                return

            hit_rate = self._hits / total_requests
            current_size = len(self._cache)

            # 根据命中率调整缓存大小
            if hit_rate > 0.7 and current_size < self._max_size_limit:
                # 命中率高，增加缓存大小
                new_max_size = min(int(self._max_size * 1.2), self._max_size_limit)
                if new_max_size > self._max_size:
                    self._max_size = new_max_size
                    logger.info(f"缓存命中率高({hit_rate:.2f})，增加缓存最大大小到{new_max_size}")
            elif hit_rate < 0.3 and current_size > self._min_size:
                # 命中率低，减少缓存大小
                new_max_size = max(int(self._max_size * 0.8), self._min_size)
                if new_max_size < self._max_size:
                    self._max_size = new_max_size
                    logger.info(f"缓存命中率低({hit_rate:.2f})，减少缓存最大大小到{new_max_size}")
                    # 立即执行一次LRU淘汰，确保缓存大小不超过新的限制
                    while len(self._cache) > self._max_size:
                        self._evict_lru()

    def warmup_cache(self, data: pl.DataFrame, indicator_types: List[str], **params):
        """
        缓存预热
        预先计算并缓存指定的指标

        Args:
            data: 输入数据
            indicator_types: 指标类型列表
            **params: 指标计算参数
        """
        from src.tech_analysis.indicator_calculator import (
            calculate_multiple_indicators_polars,
        )

        logger.info(f"开始缓存预热，计算{len(indicator_types)}个指标")
        start_time = time.time()

        # 批量计算所有指标
        result_df = calculate_multiple_indicators_polars(data, indicator_types, **params)

        # 将计算结果保存到缓存
        for indicator_type in indicator_types:
            # 提取该指标的结果
            indicator_cols = [col for col in result_df.columns if col.startswith(indicator_type) or col in ['dma', 'ama', 'fsl', 'sar', 'obv']]
            if indicator_cols:
                indicator_result = result_df.select([result_df.columns[0]] + indicator_cols)
                self.set(data, indicator_result, indicator_type, **params)

        total_time = time.time() - start_time
        logger.info(f"缓存预热完成，耗时{total_time:.2f}秒，缓存了{len(indicator_types)}个指标")

    def clear(self, indicator_type: str | None = None):
        """
        清除缓存

        Args:
            indicator_type: 可选，指定要清除的指标类型，None表示清除所有
        """
        with self._lock:
            if indicator_type:
                # 清除指定指标类型的缓存
                keys_to_remove = [key for key in self._cache
                                 if key.startswith(f"{indicator_type}_")]
                for key in keys_to_remove:
                    del self._cache[key]
                self._evictions += len(keys_to_remove)
                logger.info(f"清除指标{indicator_type}的{len(keys_to_remove)}个缓存条目")
            else:
                # 清除所有缓存
                evicted = len(self._cache)
                self._cache.clear()
                self._evictions += evicted
                logger.info(f"清除所有{evicted}个缓存条目")

    def invalidate(self, data: pl.DataFrame, indicator_type: str | None = None):
        """
        使相关缓存失效

        Args:
            data: 输入数据
            indicator_type: 可选，指定要失效的指标类型，None表示失效所有相关指标
        """
        try:
            data_hash = _hash_polars_data(data)
        except ValueError as e:
            logger.warning(f"使缓存失效失败: {e}")
            return

        with self._lock:
            if indicator_type:
                prefix = f"{indicator_type}_{data_hash}_"
                keys_to_remove = [key for key in self._cache if key.startswith(prefix)]
            else:
                keys_to_remove = [key for key in self._cache if f"_{data_hash}_" in key]

            for key in keys_to_remove:
                del self._cache[key]

            self._evictions += len(keys_to_remove)
        logger.info(f"使{len(keys_to_remove)}个缓存条目失效")

    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0.0

            return {
                'size': len(self._cache),
                'max_size': self._max_size,
                'min_size': self._min_size,
                'max_size_limit': self._max_size_limit,
                'dynamic_size': self._dynamic_size,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
                'evictions': self._evictions,
                'total_requests': total_requests,
                'last_size_adjustment': self._last_size_adjustment
            }

    def reset_stats(self):
        """
        重置缓存统计信息
        """
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._evictions = 0

    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取详细的缓存信息

        Returns:
            Dict[str, Any]: 详细的缓存信息
        """
        # 获取缓存条目的信息
        with self._lock:
            entries_info = []
            for key, entry in self._cache.items():
                entries_info.append({
                    'key': key,
                    'size': entry.value.shape[0] * entry.value.shape[1],
                    'access_time': entry.access_time,
                    'hit_count': entry.hit_count,
                    'expire_time': entry.expire_time,
                    'is_expired': entry.is_expired()
                })

        return {
            'stats': self.get_stats(),
            'config': {
                'max_size': self._max_size,
                'min_size': self._min_size,
                'max_size_limit': self._max_size_limit,
                'dynamic_size': self._dynamic_size,
                'default_ttl': self._default_ttl,
                'size_adjustment_interval': self._size_adjustment_interval
            },
            'entries': entries_info
        }


# 创建全局缓存实例
global_indicator_cache = IndicatorCache(max_size=1000, default_ttl=3600, dynamic_size=True)


def cached_calculation(cache: IndicatorCache | None = None, ttl: int | None = None):
    """
    缓存装饰器，用于缓存指标计算结果

    Args:
        cache: 缓存实例，None表示使用全局缓存
        ttl: 缓存过期时间（秒），None表示使用缓存默认值

    Returns:
        Callable: 装饰后的函数
    """
    cache_instance = cache or global_indicator_cache

    def decorator(func):
        def wrapper(data: pl.DataFrame, **params):
            # 获取指标类型（从函数名或参数中获取）
            indicator_type = params.get('indicator_type', func.__name__)

            # 尝试从缓存获取结果
            cache_result = cache_instance.get(data, indicator_type, **params)
            if cache_result is not None:
                return cache_result

            # 执行计算
            result = func(data, **params)

            # 设置缓存
            cache_params = params.copy()
            if ttl is not None:
                cache_params['ttl'] = ttl
            cache_instance.set(data, result, indicator_type, **cache_params)

            return result

        return wrapper

    return decorator
