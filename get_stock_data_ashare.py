"""
A股20日涨幅榜数据获取脚本（使用Ashare - 优化版）
获取近20个交易日涨幅前400的股票
"""

import sys
sys.path.insert(0, 'G:/MiniMAX-agent/project/a_stock_analysis')

from Ashare import *
import pandas as pd
from datetime import datetime, timedelta
import warnings
import time
import concurrent.futures
warnings.filterwarnings('ignore')

def get_stock_change(code):
    """获取单只股票的20日涨幅"""
    try:
        df = get_price(code, frequency='1d', count=25)
        
        if df is None or len(df) < 20:
            return None
        
        latest_close = df.iloc[-1]['close']
        old_close = df.iloc[-20]['close']
        change_pct = ((latest_close - old_close) / old_close) * 100
        
        exchange = 'SH' if code.startswith('sh') else 'SZ'
        stock_code = code.replace('sh', '').replace('sz', '')
        
        return {
            '股票代码': stock_code,
            '最新收盘价': round(latest_close, 2),
            '20日前收盘价': round(old_close, 2),
            '20日涨幅%': round(change_pct, 2),
            '交易所': exchange,
            'raw_code': code
        }
    except:
        return None

def main():
    print("=" * 50)
    print("A股20日涨幅榜数据获取（Ashare优化版）")
    print("=" * 50)
    
    # 生成股票代码列表
    print("\n生成股票代码列表...")
    stock_codes = []
    
    # 上海主板
    for i in range(600000, 604000):
        stock_codes.append(f"sh{i}")
    # 科创板
    for i in range(688000, 689000):
        stock_codes.append(f"sh{i}")
    # 深圳主板
    for i in range(1, 4000):
        stock_codes.append(f"sz{i:06d}")
    # 创业板
    for i in range(300001, 301000):
        stock_codes.append(f"sz{i}")
    
    print(f"共 {len(stock_codes)} 个股票代码")
    
    # 使用多线程并行获取
    print("\n并行获取股票数据...")
    results = []
    
    # 使用线程池并行处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_code = {executor.submit(get_stock_change, code): code for code in stock_codes}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_code):
            completed += 1
            if completed % 100 == 0:
                print(f"已处理 {completed}/{len(stock_codes)} 只股票...")
            
            result = future.result()
            if result:
                results.append(result)
    
    print(f"\n成功获取 {len(results)} 只股票的数据")
    
    if not results:
        print("没有获取到数据")
        return
    
    # 转换为DataFrame
    df = pd.DataFrame(results)
    
    # 按涨幅排序
    df = df.sort_values('20日涨幅%', ascending=False)
    
    # 取前400只
    df_top400 = df.head(400).copy()
    
    # 添加排名
    df_top400.insert(0, '排名', range(1, 401))
    
    # 重置索引
    df_top400 = df_top400.reset_index(drop=True)
    
    # 添加股票名称（用代码代替）
    df_top400['股票名称'] = df_top400['股票代码']
    
    # 选择需要的列并排序
    final_df = df_top400[['排名', '股票代码', '股票名称', '最新收盘价', '20日前收盘价', '20日涨幅%', '交易所']]
    
    # 保存CSV
    output_file = "G:/MiniMAX-agent/project/a_stock_analysis/a_stock_top400.csv"
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n数据已保存到: {output_file}")
    print(f"共 {len(final_df)} 只股票")
    
    # 显示前20名
    print("\n" + "=" * 50)
    print("涨幅前20名:")
    print("=" * 50)
    print(final_df.head(20).to_string(index=False))
    
    # 统计信息
    print("\n" + "=" * 50)
    print("统计信息:")
    print("=" * 50)
    print(f"平均涨幅: {final_df['20日涨幅%'].mean():.2f}%")
    print(f"最大涨幅: {final_df['20日涨幅%'].max():.2f}%")
    print(f"最小涨幅: {final_df['20日涨幅%'].min():.2f}%")
    print(f"上海交易所: {len(final_df[final_df['交易所']=='SH'])} 只")
    print(f"深圳交易所: {len(final_df[final_df['交易所']=='SZ'])} 只")

if __name__ == "__main__":
    main()
