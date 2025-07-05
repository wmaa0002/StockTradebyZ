import argparse
import subprocess
import sys
from pathlib import Path

# 解析命令行参数
def parse_args():
    parser = argparse.ArgumentParser(description='股票数据抓取和选股工具')
    
    # 模式选择参数
    parser.add_argument('--mode', choices=['fetch', 'select', 'both'], default='fetch',
                      help='运行模式: fetch-仅抓取, select-仅选股, both-两者都运行')
    
    # fetch_kline.py的参数
    parser.add_argument('--datasource', choices=['tushare', 'akshare', 'mootdx'], default='tushare', 
                      help='历史 K 线数据源')
    parser.add_argument('--frequency', type=int, choices=list(range(12)), default=4, 
                      help='K线频率编码，参见说明')
    parser.add_argument('--exclude-gem', action='store_true', 
                      help='True则排除创业板/科创板/北交所')
    parser.add_argument('--min-mktcap', type=float, default=2e9 , 
                      help='最小总市值（含），单位：元')
    parser.add_argument('--max-mktcap', type=float, default=float('inf'), 
                      help='最大总市值（含），单位：元，默认无限制')
    parser.add_argument('--start', default='20200101', 
                      help='起始日期 YYYYMMDD 或 today')
    parser.add_argument('--end', default='today', 
                      help='结束日期 YYYYMMDD 或 today')
    parser.add_argument('--out', default='./data', 
                      help='输出目录')
    parser.add_argument('--workers', type=int, default=3, 
                      help='并发线程数')
    
    # select_stock.py的参数
    parser.add_argument('--data-dir', default='./data',
                      help='CSV行情数据目录')
    parser.add_argument('--config', default='./configs.json',
                      help='Selector配置文件路径')
    parser.add_argument('--date', 
                      help='交易日YYYY-MM-DD；缺省=数据最新日期')
    parser.add_argument('--tickers', default='all',
                      help="'all'或逗号分隔股票代码列表")
    
    return parser.parse_args()

def run_fetch_kline(args):
    cmd = [
        sys.executable, 'fetch_kline.py',
        '--datasource', args.datasource,
        '--frequency', str(args.frequency),
        '--min-mktcap', str(args.min_mktcap),
        '--max-mktcap', str(args.max_mktcap),
        '--start', args.start,
        '--end', args.end,
        '--out', args.out,
        '--workers', str(args.workers)
    ]
    if args.exclude_gem:
        cmd.append('--exclude-gem')
    
    print('运行fetch_kline.py...')
    subprocess.run(cmd, check=True)

def run_select_stock(args):
    cmd = [
        sys.executable, 'select_stock.py',
        '--data-dir', args.data_dir,
        '--config', args.config,
        '--tickers', args.tickers
    ]
    if args.date:
        cmd.extend(['--date', args.date])
    
    print('运行select_stock.py...')
    subprocess.run(cmd, check=True)

def main():
    args = parse_args()
    
    if args.mode in ['fetch', 'both']:
        run_fetch_kline(args)
    
    if args.mode in ['select', 'both']:
        run_select_stock(args)

if __name__ == '__main__':
    main()