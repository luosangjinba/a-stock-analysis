"""
A股20日涨幅榜数据获取脚本（使用Tushare复权因子计算前复权）
获取近20个交易日涨幅前400的股票
"""

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import warnings
import time
import os
warnings.filterwarnings('ignore')

# Tushare Token
TUSHARE_TOKEN = '3169a014b7c2f832cb7be1fb33080ccb70284d663397f856042aad5a'

def main():
    print("=" * 50)
    print("A股20日涨幅榜数据获取（Tushare前复权）")
    print("=" * 50)
    
    # 初始化
    pro = ts.pro_api(TUSHARE_TOKEN)
    
    # 获取日期
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    
    print(f"日期范围: {start_date} ~ {end_date}")
    
    # 获取所有A股股票列表
    print("\n获取A股股票列表...")
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
    
    # 过滤掉北交所（8开头）
    stocks = stocks[~stocks['ts_code'].str.startswith('8')]
    ts_codes = stocks['ts_code'].tolist()
    print(f"共 {len(ts_codes)} 只股票（不含北交所）")
    
    # 分批获取日线数据和复权因子
    print("\n分批获取日线数据和复权因子...")
    all_daily_data = []
    all_adj_data = []
    batch_size = 50
    total_batches = (len(ts_codes) + batch_size - 1) // batch_size
    
    for i in range(0, len(ts_codes), batch_size):
        batch_num = i // batch_size + 1
        batch = ts_codes[i:i+batch_size]
        
        if batch_num % 20 == 0:
            print(f"处理批次 {batch_num}/{total_batches} ({i+1}-{min(i+batch_size, len(ts_codes))})...")
        
        # 获取日线数据
        try:
            df_daily = pro.daily(ts_code=','.join(batch), start_date=start_date, end_date=end_date)
            if df_daily is not None and len(df_daily) > 0:
                all_daily_data.append(df_daily)
        except:
            pass
        
        # 获取复权因子
        try:
            df_adj = pro.adj_factor(ts_code=','.join(batch), start_date=start_date, end_date=end_date)
            if df_adj is not None and len(df_adj) > 0:
                all_adj_data.append(df_adj)
        except:
            pass
        
        time.sleep(0.15)
    
    if not all_daily_data:
        print("未能获取到任何日线数据")
        return
    
    # 合并数据
    print("\n合并数据...")
    daily_df = pd.concat(all_daily_data, ignore_index=True)
    adj_df = pd.concat(all_adj_data, ignore_index=True) if all_adj_data else pd.DataFrame()
    
    print(f"日线数据: {len(daily_df)} 条")
    print(f"复权因子: {len(adj_df)} 条")
    
    # 过滤北交所
    daily_df = daily_df[~daily_df['ts_code'].str.startswith('8')]
    if len(adj_df) > 0:
        adj_df = adj_df[~adj_df['ts_code'].str.startswith('8')]
    
    # 转换日期格式
    daily_df['trade_date'] = daily_df['trade_date'].astype(str)
    if len(adj_df) > 0:
        adj_df['trade_date'] = adj_df['trade_date'].astype(str)
    
    # 计算前复权价格
    if len(adj_df) > 0:
        print("\n计算前复权价格...")
        
        # 获取每只股票的最新复权因子
        latest_factors = adj_df.sort_values('trade_date').groupby('ts_code').last()['adj_factor'].to_dict()
        
        # 合并复权因子
        daily_df = daily_df.merge(adj_df, on=['ts_code', 'trade_date'], how='left')
        
        # 计算前复权价格
        def calc_qfq(row):
            ts_code = row['ts_code']
            if pd.isna(row['adj_factor']) or ts_code not in latest_factors:
                return row['close']
            latest_factor = latest_factors[ts_code]
            return row['close'] * latest_factor / row['adj_factor']
        
        daily_df['close'] = daily_df.apply(calc_qfq, axis=1)
    
    # 排序
    daily_df = daily_df.sort_values(['ts_code', 'trade_date'])
    
    # 计算涨幅
    print("\n计算20日涨幅...")
    results = []
    
    for ts_code in ts_codes:
        try:
            stock_data = daily_df[daily_df['ts_code'] == ts_code].copy()
            
            if len(stock_data) < 20:
                continue
            
            stock_data = stock_data.sort_values('trade_date')
            latest = stock_data.iloc[-1]
            old = stock_data.iloc[-20]
            
            latest_close = float(latest['close'])
            old_close = float(old['close'])
            change_pct = ((latest_close - old_close) / old_close) * 100
            
            # 获取股票名称
            stock_info = stocks[stocks['ts_code'] == ts_code]
            name = stock_info.iloc[0]['name'] if len(stock_info) > 0 else ''
            symbol = stock_info.iloc[0]['symbol'] if len(stock_info) > 0 else ts_code
            
            exchange = 'SH' if ts_code.startswith('6') else 'SZ'
            
            results.append({
                '股票代码': symbol,
                '股票名称': name,
                '最新收盘价': round(latest_close, 2),
                '20日前收盘价': round(old_close, 2),
                '20日涨幅%': round(change_pct, 2),
                '交易所': exchange
            })
            
        except Exception as e:
            continue
    
    print(f"\n成功计算 {len(results)} 只股票的涨幅")
    
    if not results:
        print("没有可用的数据")
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
    
    # 保存CSV
    output_file = "G:/MiniMAX-agent/project/a_stock_analysis/a_stock_top400_qfq.csv"
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 替换旧文件
    old_file = "G:/MiniMAX-agent/project/a_stock_analysis/a_stock_top400.csv"
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
