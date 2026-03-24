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
    for year, y_win, base_p in yearly_data_list:
        curr_max, max_dd, end_idx = base_p, 0, None
        for idx, row in y_win.iterrows():
            if row['close'] > curr_max: curr_max = row['close']
            dd = (row['close'] / curr_max) - 1
            if dd < max_dd: max_dd, end_idx = dd, idx

        if max_dd < 0:
            # 寻找回撤开始点
            start_idx = y_win.index[0]
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