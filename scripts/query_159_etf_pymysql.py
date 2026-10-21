import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pymysql

from src.utils.config import get_config

# 从项目配置读取数据库连接信息（.env 中的 DATABASE__* 配置项）
db = get_config().database

# 连接数据库
conn = pymysql.connect(
    host=db.host,
    port=db.port,
    user=db.username,
    password=db.password,
    database=db.database,
    charset='utf8mb4'
)

# 创建游标
cursor = conn.cursor()

# 执行查询
cursor.execute('SELECT ts_code, name FROM stock_basic WHERE symbol LIKE "159%" LIMIT 10')

# 获取结果
results = cursor.fetchall()

# 打印结果
print('159 开头的 ETF 记录:')
for row in results:
    print(f'{row[0]}: {row[1]}')

# 关闭连接
cursor.close()
conn.close()
