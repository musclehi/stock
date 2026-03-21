import pandas as pd
from sqlalchemy import create_engine, text
from common import constants

# 数据库连接
engine = create_engine(constants.dbStr)


def calculate_annual_returns(code_list, start_mmdd, end_mmdd, start_year, end_year):
    """
    计算指定代码列表在每年固定月日区间的收益率
    :param start_mmdd: 格式如 '01-01'
    :param end_mmdd: 格式如 '12-31'
    """
    results = []

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-{start_mmdd}"
        end_date = f"{year}-{end_mmdd}"

        # SQL逻辑：一次性取出该年份该区间内每个 code 的第一天和最后一天的后复权收盘价
        # 使用窗口函数获取每个 code 在该区间内的起始价和终点价
        query = text("""
            SELECT code, 
                   SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY trade_date ASC), ',', 1) as first_price,
                   SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY trade_date DESC), ',', 1) as last_price
            FROM daily_hfq_data
            WHERE code IN :codes 
              AND trade_date BETWEEN :start AND :end
            GROUP BY code
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "codes": tuple(code_list),
                "start": start_date,
                "end": end_date
            })

        if df.empty:
            continue

        # 转换为浮点数并计算收益率
        df['first_price'] = df['first_price'].astype(float)
        df['last_price'] = df['last_price'].astype(float)
        df['return_rate'] = (df['last_price'] / df['first_price']) - 1
        df['year'] = year

        results.append(df[['year', 'code', 'return_rate']])

    # 合并所有年份数据
    full_df = pd.concat(results)

    # 1. 整理输出：透视表展示每年的收益率
    pivot_df = full_df.pivot(index='code', columns='year', values='return_rate')

    # 2. 计算平均值
    pivot_df['mean_return'] = pivot_df.mean(axis=1)

    # 格式化百分比显示
    styled_df = pivot_df.applymap(lambda x: f"{x:.2%}" if pd.notnull(x) else "-")

    return pivot_df, styled_df


if __name__ == "__main__":
    # 示例参数
    my_codes = ['004898.OF', '007172.OF', '009803.OF']

    # 计算每年 1月1日 到 12月31日 的收益率
    raw_res, display_res = calculate_annual_returns(
        code_list=my_codes,
        start_mmdd='03-21',
        end_mmdd='04-21',
        start_year=2020,
        end_year=2025
    )

    print("### result ###")
    print(display_res)