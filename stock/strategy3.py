import pandas as pd
from common import constants
from sqlalchemy import create_engine


class StockBacktester:
    def __init__(self):
        # 建立数据库连接
        self.engine = create_engine(constants.dbStr)

    def load_data(self, code, start_date, end_date):
        """根据给定的表结构加载数据"""
        query = f"""
        SELECT 
            trade_date, 
            close as close_hfq, 
            close_real, 
            adj_factor 
        FROM daily_hfq_data 
        WHERE code = '{code}' 
          AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date ASC
        """
        df = pd.read_sql(query, self.engine)
        if df.empty:
            raise ValueError(f"未找到股票 {code} 在 {start_date} 至 {end_date} 间的数据")
        return df

    def run(self, code, start_date, end_date, initial_cash=100000):
        # 1. 加载数据
        df = self.load_data(code, start_date, end_date)

        # 2. 归一化价格 (计算基于区间起始日的相对价格)
        # 这样判断“上涨2%”是基于回测首日的实际感受
        base_hfq = df['close_hfq'].iloc[0]
        df['adj_price'] = df['close_hfq'] / base_hfq * df['close_real'].iloc[0]

        # 3. 初始化账户状态
        cash = initial_cash
        hold_shares = 0
        history = []

        print(f">>> 开始回测: {code} [{start_date} -> {end_date}]")

        # 4. 遍历每个交易日
        for i in range(len(df)):
            row = df.iloc[i]
            today = row['trade_date']

            # 价格定义
            p_logic = row['close_hfq']  # 用于判断涨跌百分比 (后复权)
            p_real = float(row['close_real'])  # 用于计算买卖股数 (原始价)

            # 辅助价格计算
            p_last_logic = df['close_hfq'].iloc[i - 1] if i > 0 else p_logic
            p_week_logic = df['close_hfq'].iloc[i - 5] if i >= 5 else df['close_hfq'].iloc[0]

            # 涨跌幅计算
            daily_ret = (p_logic / p_last_logic) - 1
            week_ret = (p_logic / p_week_logic) - 1

            action = "None"

            # --- 策略逻辑执行 ---

            # 条件1：下跌3% 买入1万 (抄底)
            if daily_ret <= -0.03 and cash >= 10000:
                buy_qty = 10000 / p_real
                # A股实操：buy_qty = (10000 // (p_real * 100)) * 100
                hold_shares += buy_qty
                cash -= 10000
                action = f"BUY 10k ({buy_qty:.0f} shares)"

            # 条件2：单日上涨2% 卖出持仓的10%
            elif daily_ret >= 0.02 and hold_shares > 0:
                sell_qty = hold_shares * 0.1
                cash += sell_qty * p_real
                hold_shares -= sell_qty
                action = f"SELL 10% ({sell_qty:.0f} shares)"

            # 条件3：近一周上涨5% 卖出总持仓的30%
            elif week_ret >= 0.05 and hold_shares > 0:
                sell_qty = hold_shares * 0.3
                cash += sell_qty * p_real
                hold_shares -= sell_qty
                action = f"SELL 30% ({sell_qty:.0f} shares)"

            # 5. 计算净值
            market_value = hold_shares * p_real
            total_value = cash + market_value

            history.append({
                'date': today,
                'price_real': p_real,
                'daily_ret': daily_ret,
                'action': action,
                'hold_shares': hold_shares,
                'cash': cash,
                'total_value': total_value
            })

        return pd.DataFrame(history)




tester = StockBacktester()
try:
    results = tester.run(
        code='600809.SH',
        start_date='2026-01-01',
        end_date='2026-03-16'
    )

    # 输出结果摘要
    print("\n--- 交易流水记录 ---")
    print(results[results['action'] != "None"][['date', 'action', 'total_value']])

    final_nav = results['total_value'].iloc[-1]
    print(f"\n初始资金: 100,000 | 最终净值: {final_nav:.2f}")
    print(f"累计收益率: {(final_nav / 100000 - 1) * 100:.2f}%")

except Exception as e:
    print(f"回测失败: {e}")