import sys

try:
    import pymysql
    import cryptography
    print(f"✅ PyMySQL 版本: {pymysql.__version__}")
    print(f"✅ Cryptography 版本: {cryptography.__version__}")
except ImportError as e:
    print(f"❌ 缺少库: {e}")
    print(f"请执行: {sys.executable} -m pip install cryptography pymysql")

# 尝试模拟 SQLAlchemy 的握手
from sqlalchemy import create_engine, text
try:
    # 临时测试串，请换成你的
    engine = create_engine("mysql+pymysql://root:Zhao123123@127.0.0.1:3306/zgh")
    with engine.connect() as conn:
        print("🚀 恭喜！连接完全正常。")
except Exception as e:
    print(f"❌ 连接依然失败，错误信息: \n{e}")