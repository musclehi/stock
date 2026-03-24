import pandas as pd
from sqlalchemy import create_engine, text
from common import constants


def update_all_missing_pct_chg(db_url):
    engine = create_engine(db_url)

    # 1. 巧妙的 SQL 查询：
    # 我们不仅要查 pct_chg 为 NULL 的行，
    # 还要查出这些行对应的 code 在表里的所有数据，以便 pct_change() 有“前一天”参考。
    query = """
    SELECT id, code, trade_date, close 
    FROM daily_hfq_data 
    WHERE code IN (SELECT DISTINCT code FROM daily_hfq_data WHERE pct_chg IS NULL)
    ORDER BY code, trade_date ASC
    """

    print("⏳ 正在从数据库读取相关数据...")
    df = pd.read_sql(query, con=engine)

    if df.empty:
        print("✅ 没有需要更新的数据（pct_chg 已全部填充）。")
        return

    # 2. 分组计算涨跌幅
    # groupby('code') 确保涨跌幅计算局限在同一只code内
    print("🧮 正在并行计算涨跌幅...")
    df['pct_chg_calc'] = df.groupby('code')['close'].pct_change()

    # 3. 筛选出原本是 NULL 且现在算出结果的行
    # 我们只更新数据库里确实缺失的那部分
    # 先获取数据库中目前为 NULL 的 ID 列表（或者直接在内存中过滤）
    null_ids_query = "SELECT id FROM daily_hfq_data WHERE pct_chg IS NULL"
    null_ids = pd.read_sql(null_ids_query, con=engine)['id'].tolist()

    update_df = df[df['id'].isin(null_ids) & df['pct_chg_calc'].notnull()]

    if update_df.empty:
        print(" gap 无法填充（可能是因为缺失的是每只code的第一行，没有前日参考价）。")
        return

    # 4. 批量写回数据库
    print(f"🚀 正在更新 {len(update_df)} 条缺失数据...")

    update_sql = text("UPDATE daily_hfq_data SET pct_chg = :val WHERE id = :id")

    # 转换为字典列表
    params = [
        {"val": float(row['pct_chg_calc']), "id": int(row['id'])}
        for _, row in update_df.iterrows()
    ]

    with engine.begin() as conn:
        # 分批处理防止大数据量下事务过大
        batch_size = 5000
        for i in range(0, len(params), batch_size):
            batch = params[i: i + batch_size]
            conn.execute(update_sql, batch)
            print(f"  已写入 {i + len(batch)} / {len(params)} 条...")

    print("✨ 全量补全任务完成！")


if __name__ == "__main__":
    DB_URL = constants.dbStr
    update_all_missing_pct_chg(DB_URL)