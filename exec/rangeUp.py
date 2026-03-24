import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)


def analyze_seasonal_growth_final_v13(code_list, start_mmdd, end_mmdd, start_year, end_year, top_n=1):
    all_results = []

    for code in code_list:
        # 1. 获取数据 (s_year - 1 确保有前置基准)
        query = text("""
            SELECT trade_date, close FROM daily_hfq_data 
            WHERE code = :code AND YEAR(trade_date) BETWEEN :s_year AND :e_year
            ORDER BY trade_date ASC
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"code": code, "s_year": start_year - 1, "e_year": end_year})

        if df.empty: continue
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['mmdd'] = df['trade_date'].dt.strftime('%m-%d')
        df['year'] = df['trade_date'].dt.year

        # 2. 识别上涨区间
        valid_years = range(start_year, end_year + 1)
        matrix_list = []
        for y in valid_years:
            y_df = df[df['year'] == y]
            if y_df.empty: continue

            # 获取该年第一个交易日之前的最近一个收盘价
            first_date = y_df[y_df['mmdd'] >= start_mmdd]['trade_date'].min()
            pre_data = df[df['trade_date'] < first_date].tail(1)

            if pre_data.empty: continue  # 彻底没基准则跳过该年

            base_p = pre_data['close'].iloc[0]
            y_range = y_df[(y_df['mmdd'] >= start_mmdd) & (y_df['mmdd'] <= end_mmdd)].copy()
            if y_range.empty: continue

            y_range['rel_nav'] = y_range['close'] / base_p
            matrix_list.append(y_range.set_index('mmdd')['rel_nav'].rename(y))

        if not matrix_list: continue
        price_matrix = pd.concat(matrix_list, axis=1).sort_index().ffill().bfill()
        mmdd_list = price_matrix.index.tolist()

        # 穷举寻优
        all_gains = []
        for i in range(len(mmdd_list)):
            for j in range(i, len(mmdd_list)):
                # 安全获取基准：i=0 时使用 1.0，否则使用 iloc[i-1]
                p_start_base = price_matrix.iloc[i - 1] if i > 0 else pd.Series(1.0, index=price_matrix.columns)
                p_end = price_matrix.iloc[j]
                avg_profit = (p_end / p_start_base - 1).mean()
                if avg_profit > 0:
                    all_gains.append({'p_idx': i, 't_idx': j, 'profit': avg_profit})

        temp_windows = []
        if all_gains:
            gain_df = pd.DataFrame(all_gains).sort_values('profit', ascending=False)
            for _, row in gain_df.iterrows():
                p, t = int(row['p_idx']), int(row['t_idx'])
                if not any(not (t < s or p > e) for s, e, _ in temp_windows):
                    temp_windows.append((p, t, row['profit']))
                if len(temp_windows) >= top_n: break

        final_windows_info = []
        for p, t, pr in sorted(temp_windows):
            final_windows_info.append({'buy_mmdd': mmdd_list[p], 'sell_mmdd': mmdd_list[t], 'avg_profit': pr})

        # 3. 逐年回测
        yearly_stats = []
        for year in valid_years:
            y_all = df[df['year'] <= year].sort_values('trade_date')
            y_target = y_all[(y_all['year'] == year) & (y_all['mmdd'] >= start_mmdd) & (y_all['mmdd'] <= end_mmdd)]
            if y_target.empty: continue

            # 修正 Benchmark
            pre_series = y_all[y_all['trade_date'] < y_target['trade_date'].min()]['close']
            if pre_series.empty: continue
            pre_close = pre_series.iloc[-1]

            bench_ret = (y_target['close'].iloc[-1] / pre_close) - 1
            row_data = {'Year': str(year), 'Benchmark': bench_ret}

            for idx, w in enumerate(final_windows_info):
                # 寻找买入前一天的价格
                buy_day_data = y_all[(y_all['year'] == year) & (y_all['mmdd'] == w['buy_mmdd'])]
                if buy_day_data.empty: continue

                # 核心修复：用索引位置获取前一天，避免 iloc 越界
                p_buy_pre_series = y_all[y_all['trade_date'] < buy_day_data['trade_date'].min()]['close']
                sell_day_data = y_all[(y_all['year'] == year) & (y_all['mmdd'] == w['sell_mmdd'])]

                if not p_buy_pre_series.empty and not sell_day_data.empty:
                    p_buy_pre = p_buy_pre_series.iloc[-1]
                    p_sell = sell_day_data['close'].iloc[-1]
                    real_gain = (p_sell / p_buy_pre) - 1
                    row_data[f"W{idx + 1}_Real"] = real_gain
                else:
                    row_data[f"W{idx + 1}_Real"] = 0.0
                row_data[f"W{idx + 1}_Avg"] = w['avg_profit']

            # 计算策略 Alpha
            row_data['Strategy'] = row_data.get('W1_Real', 0.0)
            row_data['Alpha'] = row_data['Strategy'] - row_data['Benchmark']
            yearly_stats.append(row_data)

        # 4. 统计平均值并合并
        if yearly_stats:
            df_res = pd.DataFrame(yearly_stats)
            num_cols = [c for c in df_res.columns if c != 'Year']
            avg_row = df_res[num_cols].mean()
            avg_df = pd.DataFrame([avg_row])
            avg_df['Year'] = 'Average'

            df_final = pd.concat([df_res, avg_df], ignore_index=True)
            all_results.append({'code': code, 'windows': final_windows_info, 'details': df_final})

    return all_results


# 只找top的几个，适合持有的是“趋势不明”或“垃圾时间较多”的品种
# 容易得到一个很大的区间，整体上涨，但中间可能下跌很多
if __name__ == "__main__":
    results = analyze_seasonal_growth_final_v13(
        code_list=[
            # '004898.OF'
            # , '009803.OF'
            # , '018846.OF'
             '000001.ZS'
            , '000852.ZS'
            , 'IXIC.GI'
        ],
        start_mmdd='01-01', end_mmdd='12-31',
        start_year=2023, end_year=2025,
        top_n=5
    )
    for item in results:
        print(f"\n 代码: {item['code']}")
        print(f" 黄金持有窗口 (穷举发现的最大涨幅区间):")
        for i, w in enumerate(item['windows']):
            print(
                f"   W{i + 1}: {w['buy_mmdd']} 前夕买入 -> {w['sell_mmdd']} 收盘卖出, 历史平均涨幅: {w['avg_profit']:.2%}")

        df_show = item['details'].copy()
        display_cols = ['Year', 'Benchmark', 'Strategy', 'Alpha']
        for i in range(1, len(item['windows']) + 1):
            display_cols += [f"W{i}_Avg", f"fW{i}_Real"]  # 修正此处列名显示逻辑

        # 格式化百分比
        for col in df_show.columns:
            if col == 'Year': continue
            df_show[col] = df_show[col].apply(lambda x: f"{x:+.2%}" if pd.notnull(x) else "-")

        print("-" * 110)
        # 注意：这里直接打印 df_show 即可，列名会自动匹配
        print(df_show[['Year', 'Benchmark', 'Strategy', 'Alpha'] + [c for c in df_show.columns if 'W1' in c]].to_string(
            index=False))
        print("-" * 110)