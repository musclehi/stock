import os
import warnings
import requests
import json
import time
from datetime import datetime, timedelta
# 过滤掉特定的 FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
import tushare as ts
from common import constants
from sqlalchemy import create_engine, text

import akshare as ak
import pandas as pd

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
    symbol_upper = symbol.upper()

    # 1. 基金数据 (中欧医疗 004898.OF)
    if symbol_upper.endswith('.OF'):
        return _save_fund_data(symbol, start, end)

    # 2. 国内指数 (沪深300 000300.ZS / 上证指数 000001.ZS)
    elif symbol_upper.endswith('.ZS'):
        return _save_china_index_data(symbol, start, end)

    # 3. 国际指数 (基于你 Java 的逻辑：后缀匹配 或 关键字匹配)
    # 兼容 .GI 后缀，同时也兼容你 Java 里的 "纳斯达克", "N225" 等原始输入
    elif symbol_upper.endswith('.GI') or \
            any(kw in symbol_upper for kw in ["IXIC", "N225", "KOSPI", "DJI", "纳斯达克"]):

        # 移除可能存在的 .GI 后缀，方便进入 Java 的映射逻辑
        clean_symbol = symbol_upper.replace(".GI", "")
        return _save_global_index_data(clean_symbol, start, end)

    # 4. 普通个股
    else:
        return _save_stock_data(symbol, start, end)





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


# 禁用代理，防止请求腾讯接口失败
os.environ['HTTP_PROXY'] = ""
os.environ['HTTPS_PROXY'] = ""


def _save_global_index_data(stock_code, start_date, end_date):
    """
    重构版：自动拆分日期区间，多次请求腾讯接口并合并数据
    """
    # 1. 代码映射逻辑 (保持你的 Java 核心逻辑)
    symbol = ""
    if "NDX".upper() in stock_code.upper():
        symbol = "usNDX"
    elif "IXIC".upper() in stock_code.upper() or "纳斯达克" in stock_code:
        symbol = "us.IXIC"
    elif "N225".upper() in stock_code.upper():
        symbol = "intN225"
    elif "KOSPI".upper() in stock_code.upper():
        symbol = "hkKOSPI"
    elif "DJI".upper() in stock_code.upper():
        symbol = "us.DJI"
    else:
        symbol = stock_code

    # 2. 自动拆分日期区间 (每段约 1000 天，即 3 年左右)
    all_dfs = []
    current_start = pd.to_datetime(start_date)
    final_end = pd.to_datetime(end_date)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://gu.qq.com/"
    }

    print(f"🔍 开始分段同步 {stock_code} ({symbol})，区间: {start_date} 至 {end_date}")

    while current_start < final_end:
        # 腾讯接口 2000 条约对应 5-8 年，为了稳妥，我们每次请求 3 年
        # 如果接口限制更严（如 1000 天），可改为 days=1000
        step_end = min(current_start + timedelta(days=1000), final_end)

        fmt_start = current_start.strftime('%Y-%m-%d')
        fmt_end = step_end.strftime('%Y-%m-%d')

        url = (
            f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
            f"_var=kline_dayqfq&param={symbol},day,{fmt_start},{fmt_end},2000,qfq"
        )

        try:
            print(f"  🚀 请求分片: {fmt_start} -> {fmt_end}")
            response = requests.get(url, headers=headers, timeout=15)
            content = response.text

            # 清洗与解析 (Java 逻辑)
            if "=" in content:
                json_str = content[content.index("=") + 1:]
                root = json.loads(json_str)
                data_root = root.get("data", {}).get(symbol, {})
                klines = data_root.get("qfqday") or data_root.get("day")

                if klines:
                    # 构造临时 DataFrame
                    temp_df = pd.DataFrame([k[:6] for k in klines],
                                           columns=['trade_date', 'open', 'close', 'high', 'low', 'volume'])
                    all_dfs.append(temp_df)

            # 适当延时，保护接口
            time.sleep(0.5)

        except Exception as e:
            print(f"  ❌ 分片 {fmt_start} 抓取失败: {e}")

        # 步进：下一次从当前的结束日期 + 1天开始
        current_start = step_end + timedelta(days=1)

    # 3. 合并、去重与入库
    if not all_dfs:
        print(f"⚠️ {stock_code} 未获取到任何有效数据。")
        return None

    full_df = pd.concat(all_dfs).drop_duplicates(subset=['trade_date'])
    full_df['trade_date'] = pd.to_datetime(full_df['trade_date'])
    full_df = full_df.sort_values('trade_date')

    # 4. 数据类型与字段适配
    for col in ['open', 'close', 'high', 'low', 'volume']:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')

    db_code = f"{stock_code}.GI" if not stock_code.endswith(".GI") else stock_code
    full_df['code'] = db_code
    for col in ['open', 'high', 'low', 'close']:
        full_df[f'{col}_real'] = full_df[col]
    full_df['adj_factor'] = 1.0

    if not full_df.empty:
        print(f"✅ {db_code} 同步完成！共计 {len(full_df)} 行数据。")
        _perform_insert(db_code, full_df) # 调用你的入库函数

    return full_df

def _save_fund_data(symbol, start, end):
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
            s_pct = df_merged['日增长率'].astype(str).str.replace('%', '', regex=False).str.strip()
            # 2. 转换为数值后直接除以 100 (使 1.25% 变成 0.0125)
            df['pct_chg'] = pd.to_numeric(s_pct, errors='coerce').fillna(0) / 100
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
        # Tushare 返回的是 1.25，执行后变为 0.0125 (即 1.25%)
        if 'pct_chg' in df.columns:
            df['pct_chg'] = df['pct_chg'].astype(float) / 100
        # -----------------------------------
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

    #基金 有pct，重跑
    # save_data("020741.OF", "20000101", "20260325")
    # save_data("018846.OF", "20000101", "20260323")
    # 测试股票 有pct，重跑
    save_data("600821.SH", "20000101", "20260325")
#--------------------------------
    # 1. 上证指数 无pct
    # save_data("000001.ZS", "2000-01-01", "2026-03-25")
    # 50
    # save_data("000016.ZS", "2000-01-01", "2026-03-23")
    #300
    # save_data("000300.ZS", "2000-01-01", "2026-03-23")
    #500
    # save_data("000905.ZS", "2000-01-01", "2026-03-23")
    #1000
    # save_data("000852.ZS", "2000-01-01", "2026-03-23")
#------------------------------------
    # # 3. 纳斯达克 无pct
    # save_data("IXIC", "2000-01-01", "2026-03-25")
    # save_data("DJI", "2000-01-01", "2026-03-23")
    # not work
    # save_data("N225", "2026-01-01", "2026-03-21")