
import pandas as pd
from sqlalchemy import create_engine, text
from common import constants


class StockUtils:
    """静态工具类：自动管理数据库连接"""

    # 静态类属性，用于存储唯一的数据库连接
    _engine = None

    @classmethod
    def init_db(cls, db_url=constants.dbStr):
        """全局只需在程序启动时调用一次"""
        if cls._engine is None:
            cls._engine = create_engine(db_url)
            print("🚀 数据库连接池已初始化")

    @classmethod
    def get_hfq_return(cls, code, start_date, end_date):
        """
        参数已简化：不再需要传入 engine
        """
        # 自动检查：如果没初始化，先按默认地址初始化
        if cls._engine is None:
            cls.init_db()

        try:
            # 1. 计算区间天数
            count_sql = text("""
                SELECT COUNT(*) FROM daily_hfq_data 
                WHERE code = :code AND trade_date BETWEEN :start AND :end
            """)

            with cls._engine.connect() as conn:
                count = conn.execute(count_sql, {"code": code, "start": start_date, "end": end_date}).scalar()

            if count == 0:
                return None

            # 2. 价格比值法计算
            query = f"""
                SELECT close FROM daily_hfq_data 
                WHERE code = '{code}' AND trade_date <= '{end_date}'
                ORDER BY trade_date DESC LIMIT {count + 1}
            """
            df = pd.read_sql(query, con=cls._engine)

            final_price = float(df.iloc[0]['close'])
            base_price = float(df.iloc[-1]['close'])

            return (final_price / base_price) - 1

        except Exception as e:
            print(f"❌ 计算失败({code}): {e}")
            return None
