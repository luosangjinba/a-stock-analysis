"""
A股20日涨幅榜数据获取脚本（使用AkShare前复权数据）
获取近20个交易日涨幅前400的股票
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import warnings
import time
import os

warnings.filterwarnings('ignore')

# 使用相对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_stock_code(code):
    """将股票代码转换为akshare格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return code  # 上海
    else:
        return code  # 深圳

def main():
    print("=" * 50)
    print("A股20日涨幅榜数据获取（AkShare前复权）")
    print("=" * 50)
    
    # 获取日期
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=35)).strftime('%Y%m%d')
    
    print(f"日期范围: {start_date} ~ {end_date}")
    
    # 获取A股股票列表
    print("\n获取A股股票列表...")
    try:
        stock_info = ak.stock_info_a_code_name()
    except:
        stock_info = pd.DataFrame()
    
    # 过滤北交所（8开头）
    stock_info = stock_info[~stock_info['code'].str.startswith('8')]
    codes = stock_info['code'].tolist()
    print(f"共 {len(codes)} 只股票（不含北交所）")
    
    # 获取上证指数交易日历
    print("\n获取交易日历...")
    try:
        sh_index = ak.stock_zh_index_daily(symbol="sh000001")
        sh_index = sh_index[sh_index['date'] >= start_date]
        trading_dates = sh_index['date'].tolist()[-20:]
        print(f"最近20个交易日: {trading_dates[0]} ~ {trading_dates[-1]}")
    except:
        print("无法获取交易日历，使用自然日计算")
        trading_dates = None
    
    # 获取数据
    print("\n获取股票前复权数据...")
    results = []
    batch_size = 50
    
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        if batch_num % 20 == 0:
            print(f"处理批次 {batch_num}/{len(codes)//batch_size} ({i+1}-{min(i+batch_size, len(codes))})...")
        
        for code in batch:
            try:
                # 获取前复权数据
                df = ak.stock_zh_a_hist(symbol=code, start_date=start_date, end_date=end_date, adjust='qfq')
                
                if df is None or len(df) < 20:
                    continue
                
                # 获取最新和20日前的数据
                df = df.sort_values('日期')
                latest = df.iloc[-1]
                old = df.iloc[-20]
                
                latest_close = float(latest['收盘'])
                old_close = float(old['收盘'])
                change_pct = ((latest_close - old_close) / old_close) * 100
                
                # 获取股票名称
                stock = stock_info[stock_info['code'] == code]
                name = stock.iloc[0]['name'] if len(stock) > 0 else code
                
                exchange = 'SH' if code.startswith('6') else 'SZ'
                
                results.append({
                    '股票代码': code,
                    '股票名称': name,
                    '最新收盘价': round(latest_close, 2),
                    '20日前收盘价': round(old_close, 2),
                    '20日涨幅%': round(change_pct, 2),
                    '交易所': exchange
                })
                
            except Exception as e:
                continue
        
        time.sleep(0.2)
    
    print(f"\n成功获取 {len(results)} 只股票的数据")
    
    if not results:
        print("没有获取到数据")
        return
    
    # 排序并取前400
    df = pd.DataFrame(results)
    df = df.sort_values('20日涨幅%', ascending=False)
    df_top400 = df.head(400).copy()
    
    # 添加排名
    df_top400.insert(0, '排名', range(1, 401))
    df_top400 = df_top400.reset_index(drop=True)
    
    # 选择列
    final_df = df_top400[['排名', '股票代码', '股票名称', '最新收盘价', '20日前收盘价', '20日涨幅%', '交易所']]
    
    # 保存CSV - 使用相对路径
    output_file = os.path.join(SCRIPT_DIR, "a_stock_top400_akshare.csv")
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 替换旧文件
    old_file = os.path.join(SCRIPT_DIR, "a_stock_top400.csv")
    if os.path.exists(old_file):
        try:
            os.remove(old_file)
        except:
            pass
    os.rename(output_file, old_file)
    
    print(f"\n数据已保存到: {old_file}")
    print(f"共 {len(final_df)} 只股票")
    
    # 显示前20名
    print("\n" + "=" * 50)
    print("涨幅前20名:")
    print("=" * 50)
    print(final_df.head(20).to_string(index=False))
    
    # 统计
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
