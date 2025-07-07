from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import random
import time
import warnings
from typing import List, Optional

import pandas as pd
import tushare as ts

warnings.filterwarnings("ignore")

# --------------------------- 全局日志配置 --------------------------- #
LOG_FILE = "filter.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("filter_stocks")

# --------------------------- 工具函数 --------------------------- #

def _to_ts_code(code: str) -> str:
    """将股票代码转换为Tushare格式"""
    return f"{code.zfill(6)}.SH" if code.startswith(("60", "68", "9")) else f"{code.zfill(6)}.SZ"

# --------------------------- 前十大流通股东筛选 --------------------------- #

def filter_top10_floatholders(
    ts_code: Optional[str] = None,
    period: Optional[str] = None,
    ann_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    holder_name: Optional[str] = None,
    pro: Optional[ts.pro_api] = None
) -> pd.DataFrame:
    """
    筛选包含特定股东的股票
    
    Args:
        ts_code: TS股票代码
        period: 报告期(YYYYMMDD格式)
        ann_date: 公告日期
        start_date: 开始日期(YYYYMMDD)
        end_date: 结束日期(YYYYMMDD)
        holder_name: 股东名称
    
    Returns:
        筛选后的股东数据
    """
    ts.set_token(ts_token)
    pro = ts.pro_api()
    
    df = pro.top10_floatholders(
        ts_code=ts_code,
        period=period,
        ann_date=ann_date,
        start_date=start_date,
        end_date=end_date
    )
    
    if ts_token:
        print("ts_token:", ts_token)
        ts.set_token(ts_token)
    pro = ts.pro_api()
    
    for attempt in range(1, 4):
        try:
            df = pro.top10_floatholders(
                ts_code=ts_code,
                period=period,
                ann_date=ann_date,
                start_date=start_date,
                end_date=end_date
            )
            break
        except Exception as e:
            logger.warning("Tushare 拉取前十大流通股东失败(%d/3): %s", attempt, e)
            time.sleep(random.uniform(5, 10) * attempt)
    else:
        logger.error("Tushare 连续三次拉取前十大流通股东失败")
        return pd.DataFrame()
        
    if df is None or df.empty:
        return pd.DataFrame()
        
    if holder_name:
        df = df[df["holder_name"] == holder_name]
    
    return df


def get_all_stock_basic(pro) -> pd.DataFrame:
    """
    获取所有上市股票的基础信息
    
    Args:
        ts_token: Tushare token(可选)
    
    Returns:
        包含股票基础信息的DataFrame
    """
    # if ts_token:
    #     ts.set_token(ts_token)
    # pro = ts.pro_api()
    
    # 获取所有上市股票的基础信息
    df = pro.stock_basic(
        exchange='', 
        list_status='L', 
        fields='ts_code,symbol,name,list_status,is_hs'
    )
    
    print(f"共获取到 {df['ts_code'].nunique()} 只上市股票")
    return df


def batch_query_top10_floatholders(
    start_date: str,
    end_date: str,
    pro: ts.pro_api
) -> pd.DataFrame:
    """
    批量查询所有上市股票的十大流通股东数据
    
    Args:
        start_date: 开始日期(YYYYMMDD)
        end_date: 结束日期(YYYYMMDD)
        pro: Tushare pro_api实例
    
    Returns:
        合并后的十大流通股东数据
    """
    # 使用全局pro变量
    stock_df = get_all_stock_basic(pro)
    
    all_data = pd.DataFrame()
    count = 0
    
    for _, row in stock_df.iterrows():
        ts_code = row['ts_code']
        
        for attempt in range(1, 4):
            try:
                # 使用全局pro变量
                df = pro.top10_floatholders(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if df is not None and not df.empty:
                    all_data = pd.concat([all_data, df])
                
                count += 1
                if count % 200 == 0:
                    print(f"已查询 {count} 只股票，暂停20秒...")
                    time.sleep(20)
                break
                
            except Exception as e:
                logger.warning(f"查询股票 {ts_code} 的十大流通股东失败(尝试 {attempt}/3): {e}")
                if attempt == 3:
                    logger.error(f"股票 {ts_code} 查询失败，跳过该股票")
                else:
                    time.sleep(10 * attempt)
    
    return all_data


def main():
    parser = argparse.ArgumentParser(description="筛选包含特定股东的股票")
    parser.add_argument("--mode", type=str, choices=['single', 'batch'], default='batch', 
                      help="查询模式: single-单只股票, batch-批量查询")
    parser.add_argument("--ts_code", type=str, help="TS股票代码")
    parser.add_argument("--period", type=str, help="报告期(YYYYMMDD格式)")
    parser.add_argument("--ann_date", type=str, help="公告日期")
    parser.add_argument("--holder", type=str, help="股东名称")
    
    args = parser.parse_args()

    # 全局变量
    global ts_token
    ts_token = "6b4902cede56f56fb0ca8cb1e1c75100deabab77e695b8813a741c9c"

    # 初始化Tushare连接
    ts.set_token(ts_token)
    pro = ts.pro_api()
    
    # 获取用户输入的日期
    print("请输入查询日期范围(格式: YYYYMMDD YYYYMMDD):")
    start_date, end_date = input().strip().split()
    
    if args.mode == 'single':
        df = filter_top10_floatholders(
            ts_code=args.ts_code,
            period=args.period,
            ann_date=args.ann_date,
            start_date=start_date,
            end_date=end_date,
            holder_name=args.holder,
            pro=pro
        )
    else:
        df = batch_query_top10_floatholders(
            start_date=start_date,
            end_date=end_date,
            pro=pro
        )
    
    if df.empty:
        print("未找到匹配的股东数据")
    else:
        print(df.to_string(index=False))
        
        # 统计去重后的ts_code数量
        unique_codes = df['ts_code'].nunique()
        print(f"\n去重后共有 {unique_codes} 个股票代码")
        
        from pathlib import Path
        import os
        
        data_dir = Path("./stock_data")
        data_dir.mkdir(exist_ok=True)
        
        today = dt.datetime.now().strftime("%Y%m%d")
        filename = data_dir / f"top10_stockholders_{today}.csv"
        
        # 添加统计信息到DataFrame
        stats_row = pd.DataFrame([{"统计信息": f"去重后共有 {unique_codes} 个股票代码"}])
        
        # 如果文件已存在则追加数据
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([stats_row, existing_df, df]).drop_duplicates()
            combined_df.to_csv(filename, index=False, encoding="utf_8_sig")
        else:
            combined_df = pd.concat([stats_row, df]).drop_duplicates()
            combined_df.to_csv(filename, index=False, encoding="utf_8_sig")
        
        logger.info(f"数据已保存至 {filename}")

if __name__ == "__main__":
    main()