from src.database.db_manager import DatabaseManager
from src.config import load_config
from src.database.models.stock import StockBasic

# 加载配置
config = load_config()

# 初始化数据库管理器
db = DatabaseManager(config)

# 获取会话
session = db.get_session()

try:
    # 查询 159 开头的 ETF
    etfs = session.query(StockBasic).filter(StockBasic.symbol.like('159%')).all()
    
    print(f'159 开头的 ETF 数量: {len(etfs)}')
    
    if etfs:
        print('\nETF 列表:')
        for etf in etfs:
            print(f'{etf.ts_code}: {etf.name}')
    else:
        print('\n没有找到 159 开头的 ETF')
        
finally:
    # 关闭会话
    session.close()
    db.cleanup()