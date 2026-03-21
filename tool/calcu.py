
import pandas as pd
from sqlalchemy import create_engine
from common import constants
from tool.commonTool import StockUtils


class StockAnalyzer:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)

    def get_performance_comparison(self, code, start_date, end_date):
        # 1. 读取数据
        query = f"""
            SELECT trade_date, close, pct_chg 
            FROM daily_hfq_data 
            WHERE code = '{code}' AND trade_date <= '{end_date}'
            ORDER BY trade_date ASC
        """
        df = pd.read_sql(query, con=self.engine)

        if df.empty:
            return "❌ 未找到相关数据"

        # --- 核心修正点：类型对齐 ---
        # 将输入字符串转换为 Timestamp，确保与 df['trade_date'] 类型一致
        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)

        # 强制确保 df['trade_date'] 也是 datetime 类型（防止数据库返回字符串或 date 对象）
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        # 2. 定位区间索引
        start_mask = df['trade_date'] >= start_ts
        if not start_mask.any():
            return "❌ 该日期范围内无数据"

        start_idx = df[start_mask].index[0]

        end_mask = df['trade_date'] <= end_ts
        end_idx = df[end_mask].index[-1]

        # --- 方法一：价格比值法 ---
        if start_idx > 0:
            base_price = float(df.iloc[start_idx - 1]['close'])
            final_price = float(df.iloc[end_idx]['close'])
            return_by_price = (final_price / base_price) - 1
            comparison_note = f"基准日: {df.iloc[start_idx - 1]['trade_date'].strftime('%Y-%m-%d')}"
        else:
            base_price = float(df.iloc[0]['close'])
            final_price = float(df.iloc[end_idx]['close'])
            return_by_price = (final_price / base_price) - 1
            comparison_note = "注意: 该code为区间内上市，以首日收盘价为基准"

        # --- 方法二：涨跌幅连乘法 ---
        target_pct_chgs = df.iloc[start_idx: end_idx + 1]['pct_chg'].astype(float)
        return_by_prod = (1 + target_pct_chgs.fillna(0)).prod() - 1

        # 3. 输出结果对比
        print(f"\n📊 code： {code} 收益分析 ({start_ts.date()} 至 {end_ts.date()})")
        print(f"{'-' * 60}")
        print(f"🔹 方法 A (价格比值法): {return_by_price:.6%}")
        print(f"   [公式: ({final_price:.2f} / {base_price:.2f}) - 1]")
        print(f"🔹 方法 B (涨跌幅连乘): {return_by_prod:.6%}")
        print(f"   [包含交易日天数: {len(target_pct_chgs)} 天]")
        print(f"🔹 计算差异: {abs(return_by_price - return_by_prod):.10f}")
        print(f"📌 {comparison_note}")

        return return_by_price, return_by_prod


# --- 运行示例 ---
if __name__ == "__main__":
    DB_URL = constants.dbStr
    analyzer = StockAnalyzer(DB_URL)

    var = StockUtils.get_hfq_return('600809.SH', '2025-01-01', '2025-12-31')
    print("*" * 8)
    print(var)
    print("*" * 8)
    # 填入你想查询的代码和区间
    analyzer.get_performance_comparison('600809.SH', '2025-01-01', '2025-12-31')