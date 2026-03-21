import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from common import constants

engine = create_engine(constants.dbStr)


def find_best_trading_window(code_list, start_mmdd, end_mmdd, start_year, end_year):
    """
    寻找在指定月份区间内，平均收益最高的买入日和卖出日组合
    """
    # 1. 从数据库读取所有相关数据
    query = text("""
        SELECT code, trade_date, close 
        FROM daily_hfq_data 
        WHERE code IN :codes 
          AND DATE_FORMAT(trade_date, '%m-%d') BETWEEN :start AND :end
          AND YEAR(trade_date) BETWEEN :s_year AND :e_year
        ORDER BY trade_date ASC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "codes": tuple(code_list),
            "start": start_mmdd,
            "end": end_mmdd,
            "s_year": start_year,
            "e_year": end_year
        })

    if df.empty:
        print("未查询到数据")
        return

    # 2. 遍历每个 code 分别计算
    final_results = []

    for code in code_list:
        code_df = df[df['code'] == code].copy()
        # 提取月日作为索引，年份作为列
        # code_df['mmdd'] = code_df['trade_date'].dt.strftime('%m-%d')
        code_df['trade_date'] = pd.to_datetime(code_df['trade_date'])  # 强制转换
        code_df['mmdd'] = code_df['trade_date'].dt.strftime('%m-%d')
        code_df['year'] = code_df['trade_date'].dt.year

        # 透视表：行是月日，列是年份，值是后复权价格
        # 使用 ffill 填充非交易日（如周末），确保矩阵完整
        price_matrix = code_df.pivot(index='mmdd', columns='year', values='close').ffill().bfill()

        mmdd_list = price_matrix.index.tolist()
        num_days = len(mmdd_list)

        best_avg_ret = -np.inf
        best_window = (None, None)

        # 3. 穷举所有日期组合 (i 为买入日索引, j 为卖出日索引)
        # 注意：j 必须在 i 之后
        for i in range(num_days):
            for j in range(i + 1, num_days):
                buy_prices = price_matrix.iloc[i].values
                sell_prices = price_matrix.iloc[j].values

                # 计算每年的收益率序列
                returns = (sell_prices / buy_prices) - 1
                avg_return = np.mean(returns)

                if avg_return > best_avg_ret:
                    best_avg_ret = avg_return
                    best_window = (mmdd_list[i], mmdd_list[j])

        final_results.append({
            'code': code,
            'buy': best_window[0],
            'sell': best_window[1],
            'max': f"{best_avg_ret:.2%}"
        })

    return pd.DataFrame(final_results)


if __name__ == "__main__":
    # 示例：看看 006331.OF 在每年 5月到 10月间，哪个细分波段最赚钱
    my_codes = ['004898.OF', '007172.OF', '009803.OF', '600809.SH']
    result_df = find_best_trading_window(
        code_list=my_codes,
        start_mmdd='03-21',
        end_mmdd='12-31',
        start_year=2022,
        end_year=2025
    )

    print("### 最优交易区间分析 ###")
    print(result_df.to_string(index=False))