import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from common import constants

# 数据库连接
engine = create_engine(constants.dbStr)



def calculate_annual_returns_v34(code_list, start_mmdd, end_mmdd, start_year, end_year):
    """
    计算指定代码列表在每年固定月日区间的收益率，包含首日涨跌幅，并换算年化收益
    """
    results = []

    # 计算区间跨越的自然日天数，用于年化换算
    # 使用 2024 闰年作为基准计算天数
    d1 = datetime.strptime(f"2024-{start_mmdd}", "%Y-%m-%d")
    d2 = datetime.strptime(f"2024-{end_mmdd}", "%Y-%m-%d")
    delta_days = (d2 - d1).days
    if delta_days <= 0: delta_days = 365  # 处理跨年或无效日期

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-{start_mmdd}"
        end_date = f"{year}-{end_mmdd}"

        # SQL 逻辑改进：
        # 1. 寻找区间前的最后一个价格作为 base_price (真正基准)
        # 2. 寻找区间内的最后一个价格作为 last_price
        query = text("""
            WITH BasePrices AS (
                SELECT code, close as base_price,
                       ROW_NUMBER() OVER(PARTITION BY code ORDER BY trade_date DESC) as rn
                FROM daily_hfq_data
                WHERE code IN :codes AND trade_date < :start
            ),
            LastPrices AS (
                SELECT code, close as last_price,
                       ROW_NUMBER() OVER(PARTITION BY code ORDER BY trade_date DESC) as rn
                FROM daily_hfq_data
                WHERE code IN :codes AND trade_date BETWEEN :start AND :end
            )
            SELECT l.code, b.base_price, l.last_price
            FROM LastPrices l
            LEFT JOIN BasePrices b ON l.code = b.code AND b.rn = 1
            WHERE l.rn = 1
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "codes": tuple(code_list),
                "start": start_date,
                "end": end_date
            })

        if df.empty:
            continue

        # 转换为浮点数
        df['base_price'] = df['base_price'].astype(float)
        df['last_price'] = df['last_price'].astype(float)

        # 容错：如果没找到 base_price (新上市)，则退而求其次用区间第一天，但会丢失首日涨跌
        df['base_price'] = df['base_price'].fillna(df['last_price'])

        # 计算包含首日在内的区间收益率
        df['return_rate'] = (df['last_price'] / df['base_price']) - 1
        df['year'] = year

        results.append(df[['year', 'code', 'return_rate']])

    # 合并数据
    full_df = pd.concat(results)

    # 1. 整理输出：透视表展示每年的收益率
    pivot_df = full_df.pivot(index='code', columns='year', values='return_rate')

    # 2. 计算区间平均收益 (Mean Return)
    pivot_df['mean_return'] = pivot_df.mean(axis=1)

    # 3. 计算年化收益率 (Annualized Return)
    # 公式: (1 + 周期平均收益) ^ (365 / 实际自然日天数) - 1
    pivot_df['annualized_return'] = pivot_df['mean_return'].apply(
        lambda x: (1 + x) ** (365 / delta_days) - 1 if (1 + x) > 0 else -1
    )

    # 格式化百分比显示
    styled_df = pivot_df.copy()
    for col in styled_df.columns:
        styled_df[col] = styled_df[col].apply(lambda x: f"{x:+.2%}" if pd.notnull(x) else "-")

    return pivot_df, styled_df

if __name__ == "__main__":
    # 示例参数
    my_codes = ['009803.OF'
                ]

    # 计算每年 1月1日 到 12月31日 的收益率
    raw_res, display_res = calculate_annual_returns_v34(
        code_list=my_codes,
        start_mmdd='03-14',
        end_mmdd='08-07',
        start_year=2023,
        end_year=2025
    )

    print("### result ###")
    print(display_res)