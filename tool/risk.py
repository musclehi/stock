
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tool.RiskConfig import RiskConfig
from common import constants


# 波动率、最大回撤、Beta 系数和夏普比率
class RiskAnalyzer:
    _engine = None

    @classmethod
    def init_db(cls, db_url=constants.dbStr):
        cls._engine = create_engine(db_url)

    @classmethod
    def get_risk_level(cls, code, start_date, end_date, index_code='000300.SH'):
        """
        核心方法：判断指定时间段的风险等级
        """
        if cls._engine is None: raise Exception("请先调用 init_db 初始化连接")

        # 1. 获取个股及大盘数据
        query = f"""
            SELECT trade_date, pct_chg, close 
            FROM daily_hfq_data 
            WHERE code = '{{code}}' AND trade_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY trade_date ASC
        """
        df_stock = pd.read_sql(query.format(code=code), cls._engine)
        df_index = pd.read_sql(query.format(code=index_code), cls._engine)

        if df_stock.empty or len(df_stock) < 10: return "数据不足"

        # 2. 计算各核心指标
        metrics = cls._calculate_metrics(df_stock, df_index)

        # 3. 评分逻辑 (0-100分，分数越高风险越大)
        score = cls._score_risk(metrics)

        # 4. 定级
        level = "低风险 (Low)" if score < 35 else "中风险 (Medium)" if score < 65 else "高风险 (High)"

        return {
            "code": code,
            "risk_level": level,
            "risk_score": round(score, 2),
            "details": metrics
        }

    @staticmethod
    def _calculate_metrics(df, df_idx):
        # A. 年化波动率
        vol = df['pct_chg'].std() * np.sqrt(252)

        # B. 最大回撤
        roll_max = df['close'].cummax()
        drawdown = (df['close'] - roll_max) / roll_max
        mdd = abs(drawdown.min())

        # C. Beta 系数 (协方差 / 指数方差)
        # 需要对齐日期
        combined = pd.merge(df[['trade_date', 'pct_chg']], df_idx[['trade_date', 'pct_chg']],
                            on='trade_date', suffixes=('_s', '_i')).dropna()
        if len(combined) > 5:
            cov = combined['pct_chg_s'].cov(combined['pct_chg_i'])
            var = combined['pct_chg_i'].var()
            beta = cov / var if var != 0 else 1.0
        else:
            beta = 1.0

        # D. 夏普比率 (假设无风险利率 2%) $(年化收益率 - 无风险利率) / 年化波动率
        annual_ret = (df['close'].iloc[-1] / df['close'].iloc[0]) ** (252 / len(df)) - 1
        sharpe = (annual_ret - 0.02) / vol if vol != 0 else 0

        return {"vol": vol, "mdd": mdd, "beta": beta, "sharpe": sharpe}

    @staticmethod
    def _score_risk(m):
        """将各项指标映射为 0-100 的风险分"""

        def interpolate(val, limits):
            low, high = limits
            if val <= low: return 0
            if val >= high: return 100
            return (val - low) / (high - low) * 100

        # 夏普比率风险分数逻辑相反：越低越危险
        def interpolate_reverse(val, limits):
            high_safe, low_safe = limits
            if val >= high_safe: return 0
            if val <= low_safe: return 100
            return (high_safe - val) / (high_safe - low_safe) * 100

        s_vol = interpolate(m['vol'], RiskConfig.VOL_LIMITS)
        s_mdd = interpolate(m['mdd'], RiskConfig.MDD_LIMITS)
        s_beta = interpolate(m['beta'], RiskConfig.BETA_LIMITS)
        s_sharpe = interpolate_reverse(m['sharpe'], RiskConfig.SHARPE_LIMITS)

        total_score = (s_vol * RiskConfig.WEIGHTS['volatility'] +
                       s_mdd * RiskConfig.WEIGHTS['max_drawdown'] +
                       s_beta * RiskConfig.WEIGHTS['beta'] +
                       s_sharpe * RiskConfig.WEIGHTS['sharpe'])
        return total_score