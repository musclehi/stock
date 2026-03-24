import pandas as pd
from sqlalchemy import create_engine

import numpy as np
from datetime import datetime, timedelta
from collections import Counter
from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)


def analyze_seasonal_drawdown(code, start_mmdd, end_mmdd, start_year, end_year):
    # 1. SQL 预取 (包含前一年基准)
    sql = f"""
    SELECT trade_date, close FROM daily_hfq_data 
    WHERE code = '{code}' AND YEAR(trade_date) BETWEEN {start_year - 1} AND {end_year}
    ORDER BY trade_date ASC
    """
    all_df = pd.read_sql(sql, engine)
    if all_df.empty: return "数据为空"
    all_df['trade_date'] = pd.to_datetime(all_df['trade_date'])
    all_df['mmdd'] = all_df['trade_date'].dt.strftime('%m-%d')
    all_df['year'] = all_df['trade_date'].dt.year

    yearly_data_list = []
    # 准备对齐矩阵 (用于块2穷举)
    all_years_nav = []

    for year in range(start_year, end_year + 1):
        mask = (all_df['year'] == year) & (all_df['mmdd'] >= start_mmdd) & (all_df['mmdd'] <= end_mmdd)
        indices = all_df.index[mask].tolist()
        if not indices: continue

        base_p = all_df.loc[indices[0] - 1, 'close']
        y_win = all_df.loc[indices].copy()
        # 记录相对于窗口前一天的净值
        y_win['rel_nav'] = y_win['close'] / base_p
        all_years_nav.append(y_win.set_index('mmdd')['rel_nav'].rename(year))
        yearly_data_list.append((year, y_win, base_p, indices))

    # --- 块 1：历年独立回撤 (V6逻辑) ---
    print(f"\n{'=' * 20} 块1：历年独立回撤 {'=' * 20}")
    yearly_details = []
    for year, y_win, base_p, indices in yearly_data_list:
        curr_max, max_dd, end_idx = base_p, 0, None
        for idx, row in y_win.iterrows():
            if row['close'] > curr_max: curr_max = row['close']
            dd = (row['close'] / curr_max) - 1
            if dd < max_dd: max_dd, end_idx = dd, idx

        if max_dd < 0:
            start_idx = None
            for idx, row in y_win.loc[:end_idx].iterrows():
                if row['close'] < max(base_p, y_win.loc[:idx, 'close'].max()):
                    start_idx = idx
                    break
            yearly_details.append({'年份': year, '起始': all_df.loc[start_idx, 'trade_date'].strftime('%Y-%m-%d'),
                                   '结束': all_df.loc[end_idx, 'trade_date'].strftime('%Y-%m-%d'),
                                   '跌幅': f"{max_dd:.2%}"})
        else:
            yearly_details.append({'年份': year, '起始': '-', '结束': '-', '跌幅': '0.00%'})
    print(pd.DataFrame(yearly_details).to_string(index=False))

    # --- 块 2：穷举子区间寻找“年均跌幅最大”的固定窗口 ---
    # matrix = pd.concat(all_years_nav, axis=1).sort_index().fillna(method='ffill')
    matrix = pd.concat(all_years_nav, axis=1).sort_index().ffill()
    mmdds = matrix.index.tolist()
    best_start, best_end, max_avg_loss = None, None, 0

    # 穷举所有可能的子区间 [i, j]
    for i in range(len(mmdds)):
        for j in range(i, len(mmdds)):
            current_drops = []
            for col in matrix.columns:
                p_end = matrix.iloc[j][col]
                # 基准是 i 的前一天
                p_start_base = matrix.iloc[i - 1][col] if i > 0 else 1.0
                current_drops.append((p_end / p_start_base) - 1)

            avg_loss = np.mean(current_drops)
            if avg_loss < max_avg_loss:
                max_avg_loss = avg_loss
                best_start, best_end = mmdds[i], mmdds[j]

    print(f"\n{'=' * 20} 块2：最优固定跌幅区间汇总 {'=' * 20}")
    if best_start:
        print(f"在 {start_mmdd} ~ {end_mmdd} 范围内探测到：")
        print(f"年均跌幅最大的固定区间: 每年 {best_start} 到 {best_end}")
        print(f"该区间 5 年平均跌幅: {max_avg_loss:.2%}")
    print("=" * 60)

# 某段日期内的每年最大跌幅
# 某段日期内的平均最大跌幅及日期
if __name__ == '__main__':
    analyze_seasonal_drawdown('004898.OF',
                              '11-01', '11-30', 2021, 2025)