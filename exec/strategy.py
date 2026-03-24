import pandas as pd
from sqlalchemy import create_engine
import importlib
from common import constants


class ProfessionalBacktestEngine:
    def __init__(self, db_url, initial_cash=100000, threshold=0.5):
        self.engine = create_engine(db_url)
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.hold_shares = 0
        self.threshold = threshold
        self.strategies = []
        self.logs = []
        self.net_value_history = []  # 记录每日净值

    def add_strategy(self, name, weight, **params):
        module = importlib.import_module(f"strategies.{name.lower()}_strategy")
        cls = getattr(module, f"{name.upper()}Strategy")
        self.strategies.append(cls(name=name, weight=weight, **params))

    def fetch_data(self, code, start, end):
        query = f"""
            SELECT trade_date, close, close_real, adj_factor ,pct_chg ,code
            FROM daily_hfq_data 
            WHERE code = '{code}' AND trade_date BETWEEN '{start}' AND '{end}'
            ORDER BY trade_date ASC
        """
        df = pd.read_sql(query, con=self.engine)
        if df.empty:
            raise ValueError(f"未找到 {code} 的数据")
        return df

    def get_combined_signal(self, df_slice):
        total_score = 0.0
        total_weight = 0.0
        for s in self.strategies:
            # 策略内部计算使用 'close' (后复权价)
            score = s.get_signal_score(df_slice)
            total_score += score * s.weight
            total_weight += s.weight

        final_score = total_score / total_weight if total_weight > 0 else 0
        if final_score >= self.threshold: return "BUY"
        if final_score <= -self.threshold: return "SELL"
        return "HOLD"

    def run(self, code, start_date, end_date):
        df = self.fetch_data(code, start_date, end_date)

        # 初始复权因子
        last_adj_factor = df.iloc[0]['adj_factor']

        # for i in range(20, len(df)):
        for i in range(1,len(df)):
            row = df.iloc[i]
            curr_date = row['trade_date']
            hfq_price = float(row['close'])  # 后复权价 (逻辑用)
            real_price = float(row['close_real'])  # 原始价 (下单用)
            curr_adj_factor = row['adj_factor']  # 当前复权因子

            # --- 步骤 1: 处理除权除息 (持仓自动调整) ---
            # 如果因子变了，说明发生了拆股或送股，需要按比例调整持仓数量
            if curr_adj_factor != last_adj_factor and self.hold_shares > 0:
                ratio = float(curr_adj_factor / last_adj_factor)
                old_shares = self.hold_shares
                self.hold_shares = int(self.hold_shares * ratio)
                self.logs.append(f"🔄 {curr_date}: 发生除权，持仓由 {old_shares} 调整为 {self.hold_shares}")
            last_adj_factor = curr_adj_factor

            # --- 步骤 2: 获取策略信号 ---
            df_slice = df.iloc[:i + 1]
            signal = self.get_combined_signal(df_slice)

            # --- 步骤 3: 执行交易 (基于原始价 real_price) ---
            if signal == "BUY":
                target_amount = 20000
                # 用原始价格计算能买多少手
                to_buy = (target_amount // real_price // 100) * 100
                cost = to_buy * real_price
                if to_buy > 0 and self.cash >= cost:
                    self.hold_shares += to_buy
                    self.cash -= cost
                    self.logs.append(f" {curr_date}: buy {to_buy}share @ 真实价 {real_price:.2f} (耗资 {cost:.2f})")

            elif signal == "SELL":
                target_sell_amount = 20000
                # 计算 10000 元对应原始价的股数，且不能超过持仓
                theoretical_sell = target_sell_amount // real_price
                to_sell = (min(theoretical_sell, self.hold_shares) // 100) * 100
                if to_sell > 0:
                    income = to_sell * real_price
                    self.hold_shares -= to_sell
                    self.cash += income
                    self.logs.append(f" {curr_date}: sell {to_sell} share @ realPrice {real_price:.2f} (收入 {income:.2f})")

            # --- 步骤 4: 每日净值记录 ---
            daily_net_value = self.cash + (self.hold_shares * real_price)
            self.net_value_history.append({"date": curr_date, "net_value": daily_net_value})

        self.print_summary(df.iloc[-1])

    def print_summary(self, last_row):
        final_real_price = float(last_row['close_real'])
        final_value = self.cash + self.hold_shares * final_real_price

        print("\n" + "=" * 60)
        print(f" 完整交易日志 ({len(self.logs)} 条记录):")
        for log in self.logs: print(f"  {log}")
        print("-" * 60)
        print(f"start资金: {self.initial_cash:,.2f}")
        print(f"end总资产: {final_value:,.2f}")
        print(f"end现金: {self.cash:,.2f}")
        print(f"end持仓: {self.hold_shares} 股 (市值: {self.hold_shares * final_real_price:,.2f})")
        print(f"end收益率: {(final_value - self.initial_cash) / self.initial_cash:.2%}")
        print("=" * 60)


# --- 启动 ---
if __name__ == "__main__":
    # 请根据实际情况修改数据库连接
    DB_URL = constants.dbStr

    engine = ProfessionalBacktestEngine(db_url=DB_URL, initial_cash=1000000)

    # 添加策略与权重
    # engine.add_strategy('trend', weight=0.6, period=20)
    engine.add_strategy('yesterday', weight=0.4)
    engine.add_strategy('risk', weight=0.6)

    # 运行
    engine.run('600809.SH', '2026-01-01', '2026-03-17')