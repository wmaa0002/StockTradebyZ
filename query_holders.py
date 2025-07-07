import os
import pandas as pd
import datetime as dt
from pathlib import Path

def get_latest_csv():
    """获取stock_data目录下最新日期的CSV文件"""
    data_dir = Path("./stock_data")
    csv_files = list(data_dir.glob("top10_stockholders_*.csv"))
    if not csv_files:
        raise FileNotFoundError("未找到任何股东数据CSV文件")
    
    # 按日期排序获取最新文件
    latest_file = max(csv_files, key=lambda f: f.stem.split("_")[-1])
    return pd.read_csv(latest_file)

def query_and_save():
    """查询并保存结果"""
    try:
        df = get_latest_csv()
    except FileNotFoundError as e:
        print(e)
        return
    
    print(f"当前数据文件包含 {len(df)} 条记录")
    print("请输入要查询的股东名称(多个名称用空格分隔):")
    query_names = input().strip().split()
    
    if not query_names:
        print("未输入任何查询名称")
        return
    
    # 模糊查询每个名称
    result_dfs = []
    for name in query_names:
        name_df = df[df["holder_name"].str.contains(name, case=False, na=False)]
        if not name_df.empty:
            result_dfs.append(name_df)
    
    if not result_dfs:
        print("未找到匹配的记录")
        return
    
    # 合并结果
    result_df = pd.concat(result_dfs).drop_duplicates()
    
    # 如果是多条件查询，需要筛选出同时满足所有条件的股票代码
    if len(query_names) > 1:
        common_codes = set(result_df["ts_code"].unique())
        for name in query_names:
            name_codes = set(df[df["holder_name"].str.contains(name, case=False, na=False)]["ts_code"])
            common_codes &= name_codes
        
        if not common_codes:
            print("未找到同时包含所有指定股东的股票")
            return
            
        result_df = result_df[result_df["ts_code"].isin(common_codes)]
    
    # 保存结果
    data_dir = Path("./stock_data")
    data_dir.mkdir(exist_ok=True)
    
    today = dt.datetime.now().strftime("%Y%m%d")
    query_str = "_".join(query_names)[:50]  # 限制文件名长度
    filename = data_dir / f"query_{query_str}_{today}.csv"
    
    result_df.to_csv(filename, index=False, encoding="utf_8_sig")
    print(f"查询结果已保存至: {filename}")

if __name__ == "__main__":
    query_and_save()