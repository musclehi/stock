from backtesting import Backtest
from strategy import SmaCross
from backtesting.test import GOOG
# 假设你已经定义了 class SmaCross 和函数 SMA
# 并且已经准备好了数据 GOOG

# 1. 初始化回测对象
bt = Backtest(GOOG, SmaCross, cash=10_000, commission=.002)

# 2. 执行优化
stats = bt.optimize(
    n1=range(5, 30, 5),      # 测试 n1 为 5, 10, 15, 20, 25
    n2=range(10, 70, 5),     # 测试 n2 为 10, 15, ..., 65
    maximize='Equity Final [$]',  # 优化目标：最终账户总资产最大化
    constraint=lambda param: param.n1 < param.n2, # 约束条件：短周期必须小于长周期
    return_heatmap=True      # 额外建议：返回热力图数据，方便分析
)

# 3. 输出优化后的结果
print(stats)

# 4. 查看优化后的最佳参数
print("\n最佳参数：")
result_stats = stats[0]
print(result_stats['_strategy']) # 这会显示优化后的 n1 和 n2 到底是多少
print()