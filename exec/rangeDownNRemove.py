import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime

from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)

# 设置 Pandas 参数以解决控制台中文对齐问题
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)



def analyze_safe_period_returns_v33(code, start_mmdd, end_mmdd, start_year, end_year, top_n=1):
    # 1. SQL 预取
    sql = f"""
    SELECT trade_date, close FROM daily_hfq_data 
    WHERE code = '{code}' AND YEAR(trade_date) BETWEEN {start_year - 1} AND {end_year}
    ORDER BY trade_date ASC
    """
    with engine.connect() as conn:
        all_df = pd.read_sql(text(sql), conn)

    if all_df.empty: return "数据为空"
    all_df['trade_date'] = pd.to_datetime(all_df['trade_date'])
    all_df['mmdd'] = all_df['trade_date'].dt.strftime('%m-%d')

    years = range(start_year, end_year + 1)
    matrix_list = []

    for y in years:
        y_df = all_df[all_df['trade_date'].dt.year == y]
        target_range = y_df[(y_df['mmdd'] >= start_mmdd) & (y_df['mmdd'] <= end_mmdd)]
        if target_range.empty: continue

        first_date = target_range['trade_date'].min()
        pre_data = all_df[all_df['trade_date'] < first_date].tail(1)
        base_p = pre_data['close'].iloc[0] if not pre_data.empty else target_range['close'].iloc[0]

        y_nav = target_range.copy()
        y_nav['rel_nav'] = y_nav['close'] / base_p
        matrix_list.append(y_nav.set_index('mmdd')['rel_nav'].rename(y))

    if not matrix_list: return "指定区间内无有效交易数据"

    matrix = pd.concat(matrix_list, axis=1).sort_index().ffill().bfill()
    mmdds = matrix.index.tolist()

    # 2. 寻找避险窗口
    candidates = []
    for i in range(len(mmdds)):
        for j in range(i, len(mmdds)):
            p_start = matrix.iloc[i - 1] if i > 0 else pd.Series(1.0, index=matrix.columns)
            annual_rets = (matrix.iloc[j] / p_start - 1)
            avg_loss = annual_rets.mean()
            if avg_loss < 0:
                candidates.append({'s_idx': i, 'e_idx': j, 's_md': mmdds[i], 'e_md': mmdds[j], 'loss': avg_loss})

    sorted_cand = sorted(candidates, key=lambda x: x['loss'])
    avoid_windows = []
    for c in sorted_cand:
        if not any(not (c['e_idx'] < s['s_idx'] or c['s_idx'] > s['e_idx']) for s in avoid_windows):
            avoid_windows.append(c)
        if len(avoid_windows) >= top_n: break
    avoid_windows = sorted(avoid_windows, key=lambda x: x['s_idx'])

    # 3. 切割安全段
    safe_segments = []
    last_idx = 0
    for win in avoid_windows:
        if win['s_idx'] > last_idx:
            safe_segments.append((last_idx, win['s_idx'] - 1))
        last_idx = win['e_idx'] + 1
    if last_idx < len(mmdds):
        safe_segments.append((last_idx, len(mmdds) - 1))

    # 4. 打印原始头部明细 (逻辑保持不变)
    print(f"\n 代码: {code}")
    print(f"{'=' * 20} 安全段交易回测 (已剔除 {len(avoid_windows)} 个大坑) {'=' * 20}")
    print(f"原始总区间: {start_mmdd} ~ {end_mmdd}")
    for i, win in enumerate(avoid_windows):
        print(f"排除坑位 {i + 1}: {win['s_md']} 到 {win['e_md']} (年均跌幅 {win['loss']:.2%})")
    print("-" * 110)

    # 5. 计算细节并打印表格 (增加年化收益列)
    results_table = []
    total_annual_rets = pd.Series(0.0, index=matrix.columns)

    for i, (s_idx, e_idx) in enumerate(safe_segments):
        p_start_base = matrix.iloc[s_idx - 1] if s_idx > 0 else pd.Series(1.0, index=matrix.columns)
        p_end = matrix.iloc[e_idx]
        annual_performance = (p_end / p_start_base - 1)
        total_annual_rets += annual_performance

        # --- 新增：计算当前安全段的年化收益 ---
        d_start = datetime.strptime(f"2024-{mmdds[s_idx]}", "%Y-%m-%d")
        d_end = datetime.strptime(f"2024-{mmdds[e_idx]}", "%Y-%m-%d")
        delta_days = (d_end - d_start).days
        if delta_days <= 0: delta_days = 1  # 防止除以0

        avg_period_ret = annual_performance.mean()
        # 几何年化公式
        annualized_ret = (1 + avg_period_ret) ** (365 / delta_days) - 1 if (1 + avg_period_ret) > 0 else -1

        seg_info = {
            '段落': f"安全段 {i + 1}",
            '日期范围': f"{mmdds[s_idx]} ~ {mmdds[e_idx]}",
            '周期均值': f"{avg_period_ret:+.2%}",
            '换算年化': f"{annualized_ret:+.2%}"  # 新增列
        }
        for yr in years:
            seg_info[f"{yr}收益"] = f"{annual_performance.get(yr, 0):+.2%}"
        results_table.append(seg_info)

    df_output = pd.DataFrame(results_table)
    print(df_output.to_string(index=False, justify='center', col_space=12))
    print("-" * 110)

    # 6. 汇总对比
    bench_annual = (matrix.iloc[-1] / 1.0 - 1)
    summary_data = []
    rows = [("安全段累加", total_annual_rets), ("全时段基准", bench_annual),
            ("超额(Alpha)", total_annual_rets - bench_annual)]

    for label, data_ser in rows:
        row = {"项目": label, "均值": f"{data_ser.mean():+.2%}"}
        for yr in years:
            row[f"{yr}年"] = f"{data_ser.get(yr, 0):+.2%}"
        summary_data.append(row)

    print("【策略综合对比表】")
    print(pd.DataFrame(summary_data).to_string(index=False, justify='center', col_space=12))
    print("=" * 110)

# 持有的是“长牛”或“高波动收益”品种，默认长期持有
if __name__ == "__main__":
    analyze_safe_period_returns_v33('004898.OF','01-02', '12-31', 2023, 2025,5)
    analyze_safe_period_returns_v33('009803.OF', '01-02', '12-31', 2023, 2025, 5)
    # analyze_safe_period_returns_v33('018846.OF', '01-01', '12-31', 2023, 2025, 5)
    # analyze_safe_period_returns_v33('000001.ZS','01-01', '12-31', 2023, 2025,5)
    # analyze_safe_period_returns_v33('000852.ZS','01-01', '12-31', 2023, 2025,5)
    # analyze_safe_period_returns_v33('IXIC.GI','01-01', '12-31', 2023, 2025,5)