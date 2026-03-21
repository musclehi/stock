from backtesting import Backtest
from backtesting.test import GOOG
from strategy import SmaCross

bt = Backtest(GOOG, SmaCross, cash=10_000, commission=.002)
stats = bt.run()
# print(stats)
print(stats)
print([key for key in stats.index if key.startswith('_')])
print(stats['_equity_curve'])
print(stats['_trades'])
# bt.plot()