## 依赖管理优化计划

### 分析现状
- **当前requirements.txt**：包含核心依赖但缺少部分已安装的包
- **已安装依赖**：包含packaging、pyarrow等必要包
- **缺失依赖**：akshare、backtrader、plotly等

### 更新内容

1. **保留现有依赖**：
   - 数据处理：pandas、polars、numpy
   - 数据获取：requests、beautifulsoup4
   - 技术分析：ta
   - UI框架：PySide6、pyqtgraph
   - 数据库：pymysql、SQLAlchemy
   - 配置管理：pydantic、pydantic-settings
   - 日志管理：loguru

2. **添加缺失依赖**：
   - 数据获取：akshare、aiohttp
   - 量化框架：backtrader
   - UI框架：plotly
   - 数据库：redis
   - NLP：transformers
   - 开发与测试：pytest、black

3. **添加必要的间接依赖**：
   - packaging（插件系统需要）
   - pyarrow（polars数据转换需要）
   - python-dotenv（环境变量加载）
   - baostock（数据源）

### 执行步骤
1. 读取当前requirements.txt文件
2. 分析已安装依赖和缺失依赖
3. 更新requirements.txt文件，添加所有必要依赖
4. 确保版本兼容性和依赖关系正确

### 预期结果
- 生成完整的requirements.txt文件
- 包含所有运行项目所需的依赖
- 确保依赖版本兼容
- 便于新环境的快速搭建