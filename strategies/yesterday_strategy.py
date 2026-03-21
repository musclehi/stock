from strategies.base_strategy import BaseStrategy


class YESTERDAYStrategy(BaseStrategy):
    """
    自定义策略：
    """

    def get_signal_score(self, df_slice):
        if len(df_slice) < 2: return 0.0

        last_close = df_slice['close'].iloc[-1]
        prev_close = df_slice['close'].iloc[-2]
        change = (last_close - prev_close) / prev_close
        # change = df_slice['pct_chg'].iloc[-1]

        if change >= 0.02: return -1.0  # 涨多了，投卖出票
        if change <= -0.02: return 1.0  # 跌多了，投买入票
        return 0.0