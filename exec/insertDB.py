




import os
import time
import pandas as pd
import warnings
# 过滤掉特定的 FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
import tushare as ts
import numpy as np
import akshare as ak
from common import constants
from sqlalchemy import create_engine, text

# 1. 环境与配置
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

ts.set_token(constants.ts_TOKEN)
pro = ts.pro_api()
engine = create_engine(constants.dbStr)


def save_data(symbol, start, end):
    """
    通用保存函数：支持股票(.SH/.SZ)和场外基金(.OF)
    """
    if symbol.endswith('.OF'):
        return _save_fund_data_ak(symbol, start, end)
    elif symbol.endswith('.ZS'):
        # 国内指数 (上证、沪深300等)
        return _save_china_index_data(symbol, start, end)

    elif symbol.endswith('.GI'):
        # 国际指数 (纳斯达克、日经等)
        return _save_global_index_data(symbol, start, end)
    else:
        return _save_stock_data(symbol, start, end)


import akshare as ak
import pandas as pd
import numpy as np
import time


def _save_china_index_data(symbol, start, end):
    """
    抓取国内指数并调用通用的 _perform_insert 入库
    """
    # 1. 自动处理代码前缀 (000300.ZS -> sh000300)
    raw_code = symbol.split('.')[0]
    if not (raw_code.startswith('sh') or raw_code.startswith('sz')):
        code = f"sz{raw_code}" if raw_code.startswith('399') else f"sh{raw_code}"
    else:
        code = raw_code

    try:
        # 2. 抓取数据 (ak.stock_zh_index_daily)
        df = ak.stock_zh_index_daily(symbol=code)
        if df is None or df.empty:
            print(f"⚠️ 指数 {symbol} 无返回数据")
            return

        # 3. 基础清洗与列名对齐 (关键步骤)
        # 将 Index 变为列，并处理重复键冲突
        df = df.reset_index()
        df = df.loc[:, ~df.columns.duplicated()]

        # 映射 Akshare 字段到你数据库要求的字段
        # 这里尽可能多地保留指数数据（开盘、最高、最低、收盘、成交量、成交额）
        mapping = {
            '日期': 'trade_date', 'date': 'trade_date',
            '开盘': 'open', 'open': 'open',
            '最高': 'high', 'high': 'high',
            '最低': 'low', 'low': 'low',
            '收盘': 'close', 'close': 'close',
            '成交量': 'volume', 'volume': 'volume',
            '成交额': 'amount', 'amount': 'amount'
        }
        df = df.rename(columns=mapping)

        # 4. 转换日期并按需筛选
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        mask = (df['trade_date'] >= pd.to_datetime(start)) & (df['trade_date'] <= pd.to_datetime(end))
        df = df.loc[mask].copy()

        # 5. 补充必须的 code 列
        df['code'] = symbol
        df['open_real'] = df['open']
        df['high_real'] = df['high']
        df['low_real'] = df['low']
        df['close_real'] = df['close']
        df['adj_factor'] = 1.0  # 指数复权因子恒为 1

        # 6. 【核心步骤】调用你原有的通用入库方法
        if not df.empty:
            _perform_insert(symbol, df)
        else:
            print(f"[*] {symbol} 在指定日期范围内无新数据。")

    except Exception as e:
        print(f"❌ 抓取或处理指数 {symbol} 失败: {e}")


def _save_global_index_data(symbol, start, end):
    # 提取 investing 对应的名称
    investing_name = symbol.split('.')[0].replace('_', ' ')

    # 获取国际指数
    df = ak.index_investing_global(symbol=investing_name,
                                   period="每日",
                                   start_date=start.replace('-', '/'),
                                   end_date=end.replace('-', '/'))

    df = df.reset_index().rename(columns={'日期': 'trade_date', '收盘': 'close'})
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['code'] = symbol

    # 存入数据库
    return df[['trade_date', 'close', 'code']]


def _save_fund_data_ak(symbol, start, end):
    """
    三维度合并版：确保单位净值、累计净值、涨跌幅全部与官方一致
    """
    try:
        pure_code = symbol.split('.')[0]

        # 1. 获取单位净值 & 日增长率 (这个接口其实包含三个核心信息)
        df_unit_raw = ak.fund_open_fund_info_em(symbol=pure_code, indicator="单位净值走势")
        # 2. 获取累计净值
        df_acc_raw = ak.fund_open_fund_info_em(symbol=pure_code, indicator="累计净值走势")

        if df_unit_raw.empty or df_acc_raw.empty:
            print(f"[-] {symbol} 数据源不完整")
            return

        # 3. 清洗列名
        df_unit_raw.columns = [str(c).replace('走势', '') for c in df_unit_raw.columns]
        df_acc_raw.columns = [str(c).replace('走势', '') for c in df_acc_raw.columns]

        # 4. 合并数据
        # df_unit_raw 通常包含: ['净值日期', '单位净值', '日增长率']
        # df_acc_raw 通常包含: ['净值日期', '累计净值']
        df_merged = pd.merge(df_unit_raw, df_acc_raw, on='净值日期', how='inner')

        # 5. 构建入库标准表
        df = pd.DataFrame()
        df['trade_date'] = pd.to_datetime(df_merged['净值日期'])
        df['code'] = symbol
        df['close_real'] = pd.to_numeric(df_merged['单位净值'], errors='coerce')
        df['close'] = pd.to_numeric(df_merged['累计净值'], errors='coerce')

        # 6. 【核心修复】获取原始涨跌幅
        # 东方财富的日增长率可能带 '%'，需要清洗
        if '日增长率' in df_merged.columns:
            df['pct_chg'] = df_merged['日增长率'].astype(str).str.replace('%', '').replace('', '0')
            df['pct_chg'] = pd.to_numeric(df['pct_chg'], errors='coerce').fillna(0)
        else:
            # 兜底计算（如果接口没给增长率）
            df = df.sort_values('trade_date')
            df['pct_chg'] = df['close'].pct_change() * 100

        # 7. 过滤日期 & 补全 OHLC
        df = df[(df['trade_date'] >= pd.to_datetime(start)) &
                (df['trade_date'] <= pd.to_datetime(end))].copy()

        if df.empty: return

        df = df.sort_values('trade_date')
        df['open'] = df['close'];
        df['high'] = df['close'];
        df['low'] = df['close']
        df['open_real'] = df['close_real'];
        df['high_real'] = df['close_real'];
        df['low_real'] = df['close_real']
        df['adj_factor'] = (df['close'] / df['close_real']).fillna(1.0)
        df['volume'] = 0;
        df['amount'] = 0

        # 8. 入库
        _perform_insert(symbol, df)
        print(f"[+] {symbol} 字段全同步完成（单位、累计、原始涨跌幅）")

    except Exception as e:
        print(f"[-] 抓取基金 {symbol} 失败: {e}")

def _save_stock_data(symbol, start, end):
    """原有的股票处理逻辑 (保持不变但提取了公用部分)"""
    try:
        df_factors = pro.adj_factor(ts_code=symbol, start_date=start, end_date=end)
        df_hfq = ts.pro_bar(ts_code=symbol, adj='hfq', start_date=start, end_date=end)
        df_real = ts.pro_bar(ts_code=symbol, adj=None, start_date=start, end_date=end)

        if df_hfq is None or df_real is None: return

        df_real_subset = df_real[['trade_date', 'open', 'high', 'low', 'close']]
        df_real_subset.columns = ['trade_date', 'open_real', 'high_real', 'low_real', 'close_real']
        df_factors = df_factors[['trade_date', 'adj_factor']]

        df = pd.merge(df_hfq, df_real_subset, on='trade_date', how='left')
        df = pd.merge(df, df_factors, on='trade_date', how='left')
        df['adj_factor'] = df['adj_factor'].ffill().bfill()

        rename_dict = {'ts_code': 'code', 'vol': 'volume', 'amount': 'amount'}
        df = df.rename(columns=rename_dict)
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        _perform_insert(symbol, df)

    except Exception as e:
        print(f"[!] 股票 {symbol} 处理失败: {e}")


def _perform_insert(symbol, df):
    """执行数据库查重与插入的公用逻辑"""
    final_cols = [
        'code', 'trade_date',
        'open', 'high', 'low', 'close',
        'open_real', 'high_real', 'low_real', 'close_real',
        'volume', 'amount', 'adj_factor', 'pct_chg'
    ]

    # 补齐缺失列
    for col in final_cols:
        if col not in df.columns: df[col] = None

    df = df[final_cols].copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 查重
    existing_data = pd.read_sql(text("SELECT trade_date, code FROM daily_hfq_data WHERE code = :c"),
                                engine, params={'c': symbol})
    existing_data['trade_date'] = pd.to_datetime(existing_data['trade_date'])

    combined = df.merge(existing_data, on=['trade_date', 'code'], how='left', indicator=True)
    new_data = combined[combined['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not new_data.empty:
        new_data.to_sql('daily_hfq_data', con=engine, if_exists='append', index=False)
        print(f"[+] {symbol} 同步成功，新增 {len(new_data)} 条。")
    else:
        print(f"[*] {symbol} 无新数据。")


if __name__ == "__main__":

    # 测试基金 (场外基金请使用 .OF 后缀)
    # save_data("021277.OF", "20000101", "20260320")
    # save_data("004898.OF", "20000101", "20260320")
    # 测试股票
    # save_data("000001.SH", "20000101", "20260320")
#--------------------------------
    # 1. 上证指数
    # save_data("000001.ZS", "2000-01-01", "2026-03-21")
    # 50
    # save_data("000016.ZS", "2000-01-01", "2026-03-21")
    #300
    # save_data("000300.ZS", "2000-01-01", "2026-03-21")
    #500
    # save_data("000905.ZS", "2000-01-01", "2026-03-21")
    #1000
    # save_data("000852.ZS", "2000-01-01", "2026-03-21")
#------------------------------------
    # # 3. 纳斯达克 100
    save_data("NASDAQ_100.GI", "2026-01-01", "2026-03-21")
    #
    # # 4. 日经 225
    # save_data("NIKKEI_225.GI", "2022-01-01", "2026-03-21")