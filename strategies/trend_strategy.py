from base_strategy import BaseStrategy


class TRENDStrategy(BaseStrategy):
    """均线趋势策略：价格在均线上方投买入票，下方投卖出票"""

    def get_signal_score(self, df_slice):
        period = self.params.get('period', 20)
        if len(df_slice) < period: return 0.0

        ma = df_slice['close'].rolling(period).mean().iloc[-1]
        last_price = df_slice['close'].iloc[-1]
        return 1.0 if last_price > ma else -1.0