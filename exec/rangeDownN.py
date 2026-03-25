import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)


def analyze_seasonal_drawdown_v25(code, start_mmdd, end_mmdd, start_year, end_year, top_n=3):
    # 1. SQL 预取
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
    all_years_nav = []

    for year in range(start_year, end_year + 1):
        mask = (all_df['year'] == year) & (all_df['mmdd'] >= start_mmdd) & (all_df['mmdd'] <= end_mmdd)
        indices = all_df.index[mask].tolist()
        if not indices: continue

        # 基准价：当前年份窗口第一天的前一个交易日
        base_p = all_df.loc[indices[0] - 1, 'close']
        y_win = all_df.loc[indices].copy()
        y_win['rel_nav'] = y_win['close'] / base_p
        all_years_nav.append(y_win.set_index('mmdd')['rel_nav'].rename(year))
        yearly_data_list.append((year, y_win, base_p))

    # --- 块 1：历年独立最大回撤 (非固定日期) ---
    print(f"\n{'=' * 20} 块1：历年各自最大回撤 {'=' * 20}")
    yearly_details = []

    for year, y_win, _ in yearly_data_list:
        # 确保索引是日期或连续的，以便 idxmin/idxmax 准确
        # y_win 应该是该年份的 DataFrame 副本

        if y_win.empty or len(y_win) < 2:
            yearly_details.append({'年份': year, '起始': '-', '结束': '-', '跌幅': '0.00%'})
            continue

        # 1. 计算该年内的滚动最高价
        # 注意：如果 y_win 只有 'close'，直接计算
        roll_max = y_win['close'].cummax()

        # 2. 计算每日回撤
        drawdown = (y_win['close'] - roll_max) / roll_max

        # 3. 找到最大回撤值和发生日期（波谷）
        max_dd = drawdown.min()

        if max_dd < 0:
            end_date_idx = drawdown.idxmin()  # 跌到最低点的那天索引

            # 4. 寻找波谷之前的最高点日期
            # 在起点到最低点这个区间内，找 close 最大的那天
            start_date_idx = y_win.loc[:end_date_idx, 'close'].idxmax()

            # 5. 获取具体日期字符串
            # 假设 y_win 的 index 本身就是 trade_date，或者有这一列
            s_date = y_win.loc[start_date_idx, 'trade_date'].strftime('%Y-%m-%d')
            e_date = y_win.loc[end_date_idx, 'trade_date'].strftime('%Y-%m-%d')

            yearly_details.append({
                '年份': year,
                '起始': s_date,
                '结束': e_date,
                '跌幅': f"{max_dd:.2%}"
            })
        else:
            yearly_details.append({'年份': year, '起始': '-', '结束': '-', '跌幅': '0.00%'})
    print(pd.DataFrame(yearly_details).to_string(index=False))

    # --- 块 2：寻找 N 个不重叠的“年均最惨”固定窗口 ---
    matrix = pd.concat(all_years_nav, axis=1).sort_index().ffill().bfill()
    mmdds = matrix.index.tolist()

    candidate_intervals = []
    # 穷举所有子区间
    for i in range(len(mmdds)):
        for j in range(i, len(mmdds)):
            # 基准：i的前一天。如果i=0，基准为1.0
            p_start_base = matrix.iloc[i - 1] if i > 0 else pd.Series(1.0, index=matrix.columns)
            p_end = matrix.iloc[j]

            # 计算每一年在该区间的实际跌幅并求平均
            annual_drops = (p_end / p_start_base) - 1
            avg_loss = annual_drops.mean()

            if avg_loss < 0:
                candidate_intervals.append({
                    'start_idx': i,
                    'end_idx': j,
                    'start_md': mmdds[i],
                    'end_md': mmdds[j],
                    'avg_loss': avg_loss,
                    'win_rate': (annual_drops < 0).sum() / len(annual_drops)  # 统计下跌年份占比
                })

    # 核心修复点：使用正确的列表排序方法
    sorted_candidates = sorted(candidate_intervals, key=lambda x: x['avg_loss'])

    selected_windows = []
    for cand in sorted_candidates:
        # 检查是否与已选区间重叠
        overlap = False
        for sel in selected_windows:
            if not (cand['end_idx'] < sel['start_idx'] or cand['start_idx'] > sel['end_idx']):
                overlap = True
                break

        if not overlap:
            selected_windows.append(cand)

        if len(selected_windows) >= top_n:
            break

    print(f"\n{'=' * 20} 块2：探测到前 {len(selected_windows)} 个最惨固定跌幅区间 {'=' * 20}")
    for idx, win in enumerate(selected_windows):
        print(f"排行 No.{idx + 1}:")
        print(f"  固定区间: 每年 {win['start_md']} 到 {win['end_md']}")
        print(f"  5年平均跌幅: {win['avg_loss']:.2%}")
        print(f"  下跌频率: {win['win_rate']:.0%}")
        print("-" * 30)


if __name__ == '__main__':
    # 某段日期内的每年最大跌幅
    # 某段日期内的平均最大跌幅及日期的top n
    analyze_seasonal_drawdown_v25('004898.OF',
                              '01-01', '12-31', 2021, 2025,3)