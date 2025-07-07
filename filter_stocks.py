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
    ts_token: Optional[str] = None
) -> pd.DataFrame:
    """
    筛选包含指定前十大流通股东的股票代码
    
    Args:
        codes: 股票代码列表
        holder_name: 股东名称
        start_date: 开始日期(YYYYMMDD)
        end_date: 结束日期(YYYYMMDD)
        ts_token: Tushare token(可选)
    
    Returns:
        包含指定股东的股票代码列表
    """
    if ts_token:
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

# --------------------------- 主函数 --------------------------- #

def main():
    parser = argparse.ArgumentParser(description="筛选包含特定股东的股票")
    parser.add_argument("--ts_code", type=str, help="TS股票代码")
    parser.add_argument("--period", type=str, help="报告期(YYYYMMDD格式)")
    parser.add_argument("--ann_date", type=str, help="公告日期")
    parser.add_argument("--start_date", type=str, help="报告期开始日期")
    parser.add_argument("--end_date", type=str, help="报告期结束日期")
    parser.add_argument("--holder", type=str, help="股东名称")
    parser.add_argument("--token", type=str, help="Tushare token")
    
    args = parser.parse_args()
    
    df = filter_top10_floatholders(
        ts_code=args.ts_code,
        period=args.period,
        ann_date=args.ann_date,
        start_date=args.start_date,
        end_date=args.end_date,
        holder_name=args.holder,
        ts_token=args.token
    )
    
    if df.empty:
        print("未找到匹配的股东数据")
    else:
        print(df.to_string(index=False))
        
        # 保存数据到CSV文件
        from pathlib import Path
        import os
        
        data_dir = Path("./stock_data")
        data_dir.mkdir(exist_ok=True)
        
        today = dt.datetime.now().strftime("%Y%m%d")
        filename = data_dir / f"top10_stockholders_{today}.csv"
        
        # 如果文件已存在则追加数据
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, df]).drop_duplicates()
            combined_df.to_csv(filename, index=False, encoding="utf_8_sig")
        else:
            df.to_csv(filename, index=False, encoding="utf_8_sig")
        
        logger.info(f"数据已保存至 {filename}")

if __name__ == "__main__":
    main()