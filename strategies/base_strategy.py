class BaseStrategy:
    def __init__(self, name, weight=1.0, **kwargs):
        self.name = name
        self.weight = weight  # 该策略的发言权权重
        self.params = kwargs

    def get_signal_score(self, df):
        """
        返回建议分值：
        1.0  -> 强烈建议买入
        0.0  -> 观望
        -1.0 -> 强烈建议卖出
        """
        raise NotImplementedError