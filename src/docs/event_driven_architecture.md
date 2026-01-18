# 事件驱动架构文档

## 1. 简介

事件驱动架构是量化分析系统的核心设计模式之一，它允许系统组件之间通过事件进行松耦合通信，提高了系统的可扩展性、可维护性和响应性。本文档详细介绍了系统中的事件驱动架构设计和使用方法。

## 2. 事件总线

事件总线是事件驱动架构的核心组件，负责事件的发布、订阅和分发。系统使用基于blinker库实现的EventBus类作为中央事件管理器。

### 2.1 核心组件

- **EventBus类**：中央事件管理中心，提供事件发布、订阅和管理功能
- **EventType类**：事件类型常量定义，统一管理所有事件名称
- **全局事件总线实例**：`event_bus`，系统中的唯一事件总线实例
- **便捷函数**：提供简化的事件订阅和发布API

### 2.2 事件类型常量

系统定义了以下几类事件：

| 事件类型 | 事件常量 | 描述 |
|---------|---------|------|
| 系统事件 | SYSTEM_INIT | 系统初始化完成 |
| 系统事件 | SYSTEM_SHUTDOWN | 系统即将关闭 |
| 数据事件 | DATA_UPDATED | 数据更新完成 |
| 数据事件 | DATA_ERROR | 数据获取错误 |
| 数据事件 | DATA_READ_PROGRESS | 数据读取进度 |
| 技术分析事件 | INDICATOR_CALCULATED | 指标计算完成 |
| 技术分析事件 | INDICATOR_ERROR | 指标计算错误 |
| 技术分析事件 | INDICATOR_PROGRESS | 指标计算进度 |
| 策略事件 | STRATEGY_SIGNAL | 策略发出信号 |
| 策略事件 | STRATEGY_ERROR | 策略执行错误 |
| 策略事件 | STRATEGY_BACKTEST_COMPLETED | 策略回测完成 |
| UI事件 | UI_REFRESH | UI需要刷新 |
| UI事件 | TAB_CHANGED | 标签页切换 |
| UI事件 | WINDOW_COUNT_CHANGED | 窗口数量变化 |
| 插件事件 | PLUGIN_MESSAGE | 插件间消息传递 |
| 插件事件 | PLUGIN_REQUEST | 插件间请求-响应 |
| 插件事件 | PLUGIN_RESPONSE | 插件间响应 |
| 插件事件 | PLUGIN_EVENT | 插件自定义事件 |

## 3. 事件总线使用方法

### 3.1 订阅事件

使用`subscribe`函数订阅事件：

```python
from src.utils.event_bus import subscribe, EventType


def on_data_updated(data_type, ts_code, data, **kwargs):
    """处理数据更新事件"""
    print(f"数据更新: {data_type} - {ts_code}")
    # 处理数据...

# 订阅数据更新事件
subscribe(EventType.DATA_UPDATED, on_data_updated)
```

### 3.2 发布事件

使用`publish`函数发布事件：

```python
from src.utils.event_bus import publish, EventType

# 发布数据更新事件
publish(
    EventType.DATA_UPDATED,
    data_type='stock',
    ts_code='600000.SH',
    data=df,
    name='浦发银行'
)
```

### 3.3 取消订阅

使用`unsubscribe`函数取消订阅：

```python
from src.utils.event_bus import unsubscribe, EventType

# 取消订阅数据更新事件
unsubscribe(EventType.DATA_UPDATED, on_data_updated)
```

## 4. 系统事件流

### 4.1 数据处理流程

1. **数据读取**：`DataReadThread`异步读取股票数据
2. **发布数据更新事件**：`DATA_UPDATED`
3. **指标计算**：`IndicatorCalculateThread`接收数据并计算指标
4. **发布指标计算完成事件**：`INDICATOR_CALCULATED`
5. **图表数据准备**：`ChartDataPrepareThread`接收计算结果并准备图表数据
6. **更新UI**：主窗口接收事件并更新图表显示

### 4.2 系统启动流程

1. **初始化配置**
2. **初始化日志**
3. **初始化插件管理器**
4. **初始化数据库**
5. **初始化数据管理器**
6. **创建主窗口**
7. **发布系统初始化完成事件**：`SYSTEM_INIT`
8. **启动主循环**

## 5. 事件使用最佳实践

### 5.1 事件设计原则

- **事件命名清晰**：使用描述性的事件名称，表明事件的目的和内容
- **事件数据精简**：只包含必要的数据，避免传递过大的对象
- **事件类型统一**：所有事件类型必须在`EventType`类中定义，避免使用字符串字面量
- **事件处理高效**：事件处理函数应快速执行，避免阻塞事件总线

### 5.2 异步事件处理

对于耗时的事件处理，建议使用异步处理：

```python
from src.utils.event_bus import subscribe, EventType
import threading


def on_data_updated(data_type, ts_code, data, **kwargs):
    """异步处理数据更新事件"""
    def process_data():
        # 耗时的数据处理操作
        print(f"开始处理数据: {data_type} - {ts_code}")
        # 处理数据...
        print(f"数据处理完成: {data_type} - {ts_code}")
    
    # 启动异步线程处理
    threading.Thread(target=process_data).start()

# 订阅数据更新事件
subscribe(EventType.DATA_UPDATED, on_data_updated)
```

### 5.3 事件过滤

使用事件过滤功能可以只处理符合条件的事件：

```python
from src.utils.event_bus import subscribe, EventType


def stock_filter(data_type, **kwargs):
    """只处理股票数据事件"""
    return data_type == 'stock'

@subscribe(EventType.DATA_UPDATED, filter_func=stock_filter)
def on_stock_data_updated(data_type, ts_code, data, **kwargs):
    """只处理股票数据更新事件"""
    print(f"股票数据更新: {ts_code}")
    # 处理股票数据...
```

## 6. 事件监控和调试

系统提供了丰富的事件监控和调试功能，帮助开发者理解和调试事件流。

### 6.1 事件历史记录

事件总线自动记录所有事件的历史记录，可以通过以下方法获取：

```python
from src.utils.event_bus import event_bus

# 获取最近100个事件的历史记录
event_history = event_bus.get_event_history(limit=100)
for event in event_history:
    print(f"事件: {event['signal_name']}, 时间: {event['timestamp']}, 线程: {event['thread_id']}")
```

### 6.2 事件统计信息

系统会统计事件的发布、订阅和取消订阅次数：

```python
from src.utils.event_bus import event_bus

# 获取详细的事件统计
event_stats = event_bus.get_event_stats()
print("事件统计:", event_stats)

# 获取事件统计摘要
summary = event_bus.get_event_stat_summary()
print("事件统计摘要:", summary)
```

### 6.3 日志监控

系统默认配置了事件日志监控，所有事件活动都会被记录到日志中。可以通过调整日志级别来控制日志详细程度：

```python
import logging

# 设置事件总线日志级别为DEBUG
logging.getLogger('src.utils.event_bus').setLevel(logging.DEBUG)
```

### 6.4 自定义事件监控器

可以添加自定义事件监控器来监控特定的事件活动：

```python
from src.utils.event_bus import event_bus


def custom_monitor(event_type, signal_name, *args, **kwargs):
    """自定义事件监控器"""
    if event_type == 'publish' and signal_name == 'data_updated':
        print(f"监控到数据更新事件: {kwargs['ts_code']}")

# 添加自定义监控器
event_bus.add_monitor(custom_monitor)
```

## 7. 示例：使用事件驱动架构

以下是一个完整的示例，展示了如何使用事件驱动架构来处理股票数据更新和指标计算：

```python
from src.utils.event_bus import subscribe, publish, EventType
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer


class StockDataProcessor:
    """股票数据处理器"""
    
    def __init__(self):
        # 订阅数据更新事件
        subscribe(EventType.DATA_UPDATED, self.on_data_updated)
        # 订阅指标计算完成事件
        subscribe(EventType.INDICATOR_CALCULATED, self.on_indicator_calculated)
    
    def on_data_updated(self, data_type, ts_code, data, **kwargs):
        """处理数据更新事件"""
        if data_type != 'stock':
            return
        
        print(f"收到股票数据: {ts_code}")
        
        # 计算技术指标
        analyzer = TechnicalAnalyzer(data)
        result_df = analyzer.calculate_all_indicators(data)
        
        # 发布指标计算完成事件
        publish(
            EventType.INDICATOR_CALCULATED,
            data_type='stock',
            ts_code=ts_code,
            data=result_df,
            success=True
        )
    
    def on_indicator_calculated(self, data_type, ts_code, data, **kwargs):
        """处理指标计算完成事件"""
        if data_type != 'stock':
            return
        
        print(f"指标计算完成: {ts_code}")
        # 使用计算结果...

# 初始化处理器
processor = StockDataProcessor()
```

## 8. 常见问题和解决方案

### 8.1 事件处理函数执行慢

**问题**：事件处理函数执行时间过长，导致事件总线阻塞

**解决方案**：
- 将耗时操作放在异步线程中执行
- 优化事件处理逻辑，减少执行时间
- 使用事件过滤减少不必要的事件处理

### 8.2 事件订阅丢失

**问题**：事件处理函数没有接收到预期的事件

**解决方案**：
- 检查事件名称是否正确，使用EventType常量
- 检查事件过滤条件是否正确
- 检查事件发布时的参数是否符合预期
- 查看事件日志，确认事件是否被正确发布

### 8.3 内存泄漏

**问题**：事件订阅导致内存泄漏

**解决方案**：
- 确保在不再需要时取消订阅
- 使用弱引用订阅（默认行为）
- 定期检查事件订阅情况

## 9. 总结

事件驱动架构是量化分析系统的重要设计模式，它通过松耦合的事件通信方式，提高了系统的可扩展性和可维护性。系统提供了丰富的事件类型、便捷的API和强大的监控调试功能，方便开发者使用和调试事件流。

遵循事件使用最佳实践，可以确保系统中的事件流高效、可靠地运行，为量化分析提供稳定的基础架构支持。
