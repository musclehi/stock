import pandas as pd
from strategies.base_strategy import BaseStrategy
from tool.risk import RiskAnalyzer


class RISKStrategy(BaseStrategy):
    """
        高频回测优化版风险策略：
        1. 支持缓存，避免同代码重复查库
        2. 自动处理固定观察时长
        """

    def __init__(self, name, weight=1.0, **kwargs):
        super().__init__(name, weight, **kwargs)
        # 内部缓存：key 为 code, value 为 risk_score
        self._cache = {}
        # 从参数中读取观察天数，默认 252 天（约 1 年）
        self.lookback = self.params.get('lookback', 252)

    """
    风险规避策略类：
    调用全局 RiskAnalyzer 工具类进行高中低风险判定。
    """

    def get_signal_score(self, df):
        """
        df: 必须包含 'code' 字段，且按日期升序排列
        """
        if df is None or len(df) < 10:
            return 0.0

        # 1. 从传入的 df 中提取必要的元数据
        # 假设 df 中有 code 列，如果没有，可以通过 self.params 传递
        code = df['code'].iloc[0] if 'code' in df.columns else self.params.get('code')

        if not code:
            print("⚠️ 策略警告: df 未包含 code 且 params 中未指定，无法分析风险")
            return 0.0

        if code in self._cache:
            print(f"从缓存中获取 {code} 的风险分值 {self._cache[code]}")
            return self._cache[code]

        # 获取当前 df 的时间范围
        start_date = df['trade_date'].iloc[0]
        end_date = df['trade_date'].iloc[-1]

        # 2. 调用之前的 RiskAnalyzer 工具类（复用逻辑）
        # 注意：RiskAnalyzer 内部会查数据库获取大盘数据以计算 Beta
        RiskAnalyzer.init_db()
        risk_report = RiskAnalyzer.get_risk_level(
            code=code,
            start_date="2025-01-01",
            end_date="2025-12-31"
            # start_date=start_date,
            # end_date=end_date
        )

        if isinstance(risk_report, str):  # 处理“数据不足”等字符串提示
            return 0.0

        # 3. 信号分值映射逻辑
        risk_score = risk_report['risk_score']
        level = risk_report['risk_level']
        print(f"risk策略警告: {level}")
        # 根据风险等级给出负向分值
        if "高风险" in level:
            score = -1.0  # 强烈建议卖出/避险
        elif "中风险" in level:
            score = -0.5  # 建议减仓/保守
        else:
            score = 0.0  # 低风险，不干预（不给正分，由其他逻辑决定买入）
        self._cache[code] = score
        return score