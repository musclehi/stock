import akshare as ak

# 尝试抓取真正的纳指 100 点位（点号开头是新浪的标准）
df = ak.index_global_hist_sina(symbol=".NDX")

if df is not None:
    print("--- 原始数据预览 ---")
    print(df.tail()) # 看看最后几行，close 那一列是不是 20000+
else:
    print("❌ 依然抓不到点位数据")