"""
A股20日涨幅榜数据获取脚本（优化版）
获取近20个交易日涨幅前400的股票
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import warnings
import os

warnings.filterwarnings('ignore')

# 使用相对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def generate_stock_codes():
    """生成A股股票代码列表（不包含北交所）"""
    codes = []
    
    # 上海主板: 600000-603999
    for i in range(600000, 604000):
        codes.append(f"{i}.SS")
    
    # 科创板: 688000-688999
    for i in range(688000, 689000):
        codes.append(f"{i}.SS")
    
    # 深圳主板: 000001-003999
    for i in range(1, 4000):
        codes.append(f"{i:06d}.SZ")
    
    # 创业板: 300001-300999
    for i in range(300001, 301000):
        codes.append(f"{i}.SZ")
    
    return codes

def main():
    print("=" * 50)
    print("A股20日涨幅榜数据获取（优化版）")
    print("=" * 50)
    
    # 设置结束日期为今天
    end_date = datetime.now()
    start_date = end_date - timedelta(days=35)
    
    print(f"\n获取数据期间: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    # 生成股票代码列表
    print("\n生成股票代码列表...")
    stock_codes = generate_stock_codes()
    print(f"共 {len(stock_codes)} 个股票代码")
    
    # 批量下载所有股票数据
    print("\n开始批量下载股票数据...")
    data = yf.download(stock_codes, start=start_date, end=end_date, progress=True, threads=True)
    
    print(f"\n数据下载完成，处理中...")
    
    all_data = []
    
    # 处理数据
    if isinstance(data.columns, pd.MultiIndex):
        for symbol in stock_codes:
            try:
                close_prices = data['Close'][symbol].dropna()
                if len(close_prices) >= 20:
                    latest_close = close_prices.iloc[-1]
                    old_close = close_prices.iloc[-20]
                    change_pct = ((latest_close - old_close) / old_close) * 100
                    
                    # 判断交易所
                    exchange = 'SH' if symbol.endswith('.SS') else 'SZ'
                    code = symbol.split('.')[0]
                    
                    all_data.append({
                        '股票代码': code,
                        '最新收盘价': round(latest_close, 2),
                        '20日前收盘价': round(old_close, 2),
                        '20日涨幅%': round(change_pct, 2),
                        '交易所': exchange
                    })
            except:
                continue
    
    if not all_data:
        print("未能获取到任何数据，请检查网络连接")
        return
    
    print(f"\n成功获取 {len(all_data)} 只股票的数据")
    
    # 转换为DataFrame
    df = pd.DataFrame(all_data)
    
    # 按涨幅排序
    df = df.sort_values('20日涨幅%', ascending=False)
    
    # 取前400只
    df_top400 = df.head(400).copy()
    
    # 添加排名
    df_top400.insert(0, '排名', range(1, 401))
    
    # 重置索引
    df_top400 = df_top400.reset_index(drop=True)
    
    # 选择需要的列并排序
    final_df = df_top400[['排名', '股票代码', '最新收盘价', '20日前收盘价', '20日涨幅%', '交易所']]
    
    # 保存CSV - 使用相对路径
    output_file = os.path.join(SCRIPT_DIR, "a_stock_top400.csv")
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
