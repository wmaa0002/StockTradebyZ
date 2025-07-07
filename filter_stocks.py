import pandas as pd
import json

def filter_stocks_with_specific_holders(holder_name):
    """
    筛选具有特定流通股东的股票
    :param holder_name: 要筛选的股东名称
    :return: 符合条件的股票代码列表
    """
    # 读取股票数据
    stock_data = []
    
    # 这里需要根据实际情况读取股东数据
    # 示例代码 - 需要根据实际数据源调整
    try:
        with open('appendix.json', 'r') as f:
            stock_data = json.load(f)
    except FileNotFoundError:
        print("未找到股东数据文件")
        return []
    
    # 筛选符合条件的股票
    filtered_stocks = []
    for stock in stock_data:
        if 'holders' in stock and holder_name in stock['holders']:
            filtered_stocks.append(stock['code'])
    
    return filtered_stocks

if __name__ == "__main__":
    # 示例用法
    target_holder = input("请输入要筛选的股东名称: ")
    result = filter_stocks_with_specific_holders(target_holder)
    print(f"具有股东'{target_holder}'的股票代码:", result)