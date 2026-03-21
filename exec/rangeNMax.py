import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from common import constants

engine = create_engine(constants.dbStr)


def analyze_seasonal_avoidance_final_v9(code_list, start_mmdd, end_mmdd, start_year, end_year, top_n=3):
    all_results = []

    for code in code_list:
        # 1. 获取数据
        query = text("""
            SELECT trade_date, close FROM daily_hfq_data 
            WHERE code = :code AND YEAR(trade_date) BETWEEN :s_year AND :e_year
            ORDER BY trade_date ASC
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"code": code, "s_year": start_year, "e_year": end_year})

        if df.empty: continue
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['mmdd'] = df['trade_date'].dt.strftime('%m-%d')

        # 2. 识别平均走势 (Peak-to-Trough)
        price_matrix = df.pivot_table(index='mmdd', columns=df['trade_date'].dt.year, values='close',
                                      aggfunc='mean').ffill().bfill()
        price_matrix = price_matrix[(price_matrix.index >= start_mmdd) & (price_matrix.index <= end_mmdd)]

        avg_series = price_matrix.mean(axis=1)
        mmdd_list = avg_series.index.tolist()
        prices = avg_series.values

        all_drops = []
        for i in range(len(prices)):
            for j in range(i + 1, len(prices)):
                if prices[j] < prices[i]:
                    depth = (prices[j] - prices[i]) / prices[i]
                    all_drops.append({'p_idx': i, 't_idx': j, 'depth': depth})

        # 过滤 Top N 不重叠区间
        temp_windows = []
        if all_drops:
            drop_df = pd.DataFrame(all_drops).sort_values('depth')
            for _, row in drop_df.iterrows():
                p, t = int(row['p_idx']), int(row['t_idx'])
                if not any(not (t < s or p > e) for s, e, _ in temp_windows):
                    temp_windows.append((p, t, row['depth']))
                if len(temp_windows) >= top_n: break

        # 构造窗期信息
        final_windows_info = []
        for p, t, d in sorted(temp_windows):
            final_windows_info.append({
                'sell_mmdd': mmdd_list[p],  # 峰值日（持有该日，收盘卖）
                'buy_mmdd': mmdd_list[t],  # 谷底日（收盘买入）
                'avg_depth': d  # 历史平均跌幅
            })

        # 3. 逐年回测
        yearly_stats = []
        for year in df['trade_date'].dt.year.unique():
            y_all = df[df['trade_date'].dt.year == year].sort_values('trade_date')
            y_range = y_all[(y_all['mmdd'] >= start_mmdd) & (y_all['mmdd'] <= end_mmdd)]
            if len(y_range) < 2: continue

            bench_mult = y_range['close'].iloc[-1] / y_range['close'].iloc[0]

            row_data = {'Year': int(year), 'Benchmark': bench_mult - 1}
            avoided_mult_product = 1.0

            for idx, w in enumerate(final_windows_info):
                p_sell_df = y_all[y_all['mmdd'] == w['sell_mmdd']]
                p_buy_df = y_all[y_all['mmdd'] == w['buy_mmdd']]

                if p_sell_df.empty: p_sell_df = y_all[y_all['mmdd'] < w['sell_mmdd']].tail(1)
                if p_buy_df.empty: p_buy_df = y_all[y_all['mmdd'] <= w['buy_mmdd']].tail(1)

                real_impact = 0.0
                if not p_sell_df.empty and not p_buy_df.empty:
                    p_sell = p_sell_df['close'].iloc[-1]
                    p_buy = p_buy_df['close'].iloc[-1]
                    real_seg_mult = p_buy / p_sell
                    real_impact = -(real_seg_mult - 1)  # 避险贡献
                    avoided_mult_product *= real_seg_mult

                # 记录该窗口的：历史平均跌幅 vs 今年实际贡献
                row_data[f"W{idx + 1}_Avg"] = w['avg_depth']
                row_data[f"W{idx + 1}_Real"] = real_impact

            strat_mult = bench_mult / avoided_mult_product
            row_data['Strategy'] = strat_mult - 1
            row_data['Alpha'] = (strat_mult - 1) - (bench_mult - 1)
            yearly_stats.append(row_data)

        if yearly_stats:
            all_results.append({'code': code, 'windows': final_windows_info, 'details': pd.DataFrame(yearly_stats)})

    return all_results


# --- Main 调用输出 ---
if __name__ == "__main__":
    results = analyze_seasonal_avoidance_final_v9(
        code_list=['004898.OF', '009803.OF'],
        start_mmdd='03-01', end_mmdd='04-30',
        start_year=2023, end_year=2025, top_n=3
    )

    for item in results:
        print(f"\n🚀 代码: {item['code']}")
        print(f"🛡️ 避险窗口定义 (已剔除 Peak 第一天):")
        for i, w in enumerate(item['windows']):
            print(
                f"   W{i + 1}: {w['sell_mmdd']}(峰值收盘卖) -> {w['buy_mmdd']}(谷底收盘买), 历史平均跌幅: {w['avg_depth']:.2%}")

        df_show = item['details'].copy()

        # 整理列顺序：Year, Benchmark, Strategy, Alpha, 然后是各个 Window 的对比
        cols = ['Year', 'Benchmark', 'Strategy', 'Alpha']
        for i in range(1, len(item['windows']) + 1):
            cols += [f"W{i}_Avg", f"W{i}_Real"]

        # 格式化百分比
        for col in cols[1:]:
            df_show[col] = df_show[col].apply(lambda x: f"{x:+.2%}")

        print("-" * 120)
        print(df_show[cols].to_string(index=False))
        print("-" * 120)