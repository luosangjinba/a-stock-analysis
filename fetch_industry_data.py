"""
A股二级行业排名数据获取脚本
获取每日申万二级行业涨幅排名并保存为CSV
"""
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Tushare Token
TUSHARE_TOKEN = '3169a014b7c2f832cb7be1fb33080ccb70284d663397f856042aad5a'

# 数据目录
DATA_DIR = "G:/MiniMAX-agent/project/a_stock_analysis/data"

def get_trade_dates(start_date, end_date):
    """获取交易日历"""
    pro = ts.pro_api(TUSHARE_TOKEN)
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
    return df['cal_date'].tolist()

def get_stock_industry():
    """获取股票行业分类"""
    pro = ts.pro_api(TUSHARE_TOKEN)
    df = pro.stock_basic(exchange='', fields='ts_code,symbol,industry')
    return df

def get_stock_prices(trade_date):
    """获取指定日期的股票数据（使用前复权价格）"""
    pro = ts.pro_api(TUSHARE_TOKEN)
    
    # 获取前30个交易日
    dates = get_trade_dates(
        (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=45)).strftime('%Y%m%d'),
        trade_date
    )
    
    if len(dates) < 20:
        print(f"交易日不足20天，跳过 {trade_date}")
        return None
    
    end_date = dates[-1]
    start_date = dates[-20]  # 20个交易日前
    
    # 获取所有A股基本信息和复权因子
    stock_basic = pro.stock_basic(exchange='', fields='ts_code,symbol')
    adj_factor = pro.adj_factor(trade_date=trade_date)
    
    # 合并获取前复权因子
    stock_basic = stock_basic.merge(adj_factor[['ts_code', 'adj_factor']], on='ts_code', how='left')
    
    # 获取20日前的复权因子
    adj_factor_start = pro.adj_factor(trade_date=start_date)
    stock_basic = stock_basic.merge(adj_factor_start[['ts_code', 'adj_factor']], on='ts_code', how='left', suffixes=('_end', '_start'))
    
    # 获取当日收盘价
    daily = pro.daily(trade_date=trade_date)
    stock_basic = stock_basic.merge(daily[['ts_code', 'close']], on='ts_code', how='left')
    stock_basic = stock_basic.rename(columns={'close': 'close_end'})
    
    # 获取20日前收盘价
    daily_start = pro.daily(trade_date=start_date)
    stock_basic = stock_basic.merge(daily_start[['ts_code', 'close']], on='ts_code', how='left')
    stock_basic = stock_basic.rename(columns={'close': 'close_start'})
    
    # 计算前复权价格和涨幅
    stock_basic = stock_basic.dropna(subset=['close_end', 'close_start', 'adj_factor_end', 'adj_factor_start'])
    
    if len(stock_basic) == 0:
        return None
    
    # 计算前复权价格
    stock_basic['price_end'] = stock_basic['close_end'] * stock_basic['adj_factor_end']
    stock_basic['price_start'] = stock_basic['close_start'] * stock_basic['adj_factor_start']
    
    # 计算20日涨幅
    stock_basic['change_pct'] = ((stock_basic['price_end'] - stock_basic['price_start']) / stock_basic['price_start'] * 100).round(2)
    
    # 获取行业分类
    industry_df = get_stock_industry()
    stock_basic = stock_basic.merge(industry_df[['ts_code', 'industry']], on='ts_code', how='left')
    
    return stock_basic[['ts_code', 'symbol', 'industry', 'price_end', 'price_start', 'change_pct']]

def calculate_industry_ranking(stock_data):
    """计算行业排名"""
    if stock_data is None or len(stock_data) == 0:
        return None
    
    # 按行业分组计算
    industry_stats = stock_data.groupby('industry').agg({
        'change_pct': ['mean', 'count'],
        'ts_code': 'count'
    }).round(2)
    
    industry_stats.columns = ['平均涨幅', '股票数量', 'count']
    industry_stats = industry_stats.reset_index()
    industry_stats = industry_stats.rename(columns={'industry': '行业名称'})
    
    # 排序
    industry_stats = industry_stats.sort_values('平均涨幅', ascending=False).reset_index(drop=True)
    industry_stats.insert(0, '排名', range(1, len(industry_stats) + 1))
    
    return industry_stats

def save_industry_data(trade_date):
    """保存指定日期的行业数据"""
    print(f"\n{'='*50}")
    print(f"获取 {trade_date} 行业数据...")
    print(f"{'='*50}")
    
    # 获取股票数据
    stock_data = get_stock_prices(trade_date)
    
    if stock_data is None:
        print(f"无法获取 {trade_date} 的股票数据")
        return False
    
    print(f"获取到 {len(stock_data)} 只股票数据")
    
    # 计算行业排名
    industry_ranking = calculate_industry_ranking(stock_data)
    
    if industry_ranking is None:
        print(f"无法计算 {trade_date} 的行业排名")
        return False
    
    print(f"行业数量: {len(industry_ranking)}")
    
    # 保存CSV
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file = os.path.join(DATA_DIR, f"{trade_date}.csv")
    industry_ranking.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"已保存: {output_file}")
    
    return True

def main():
    """主函数 - 获取最近20个交易日的数据"""
    print("A股二级行业排名数据获取")
    print("="*50)
    
    # 获取最近20个交易日
    today = datetime.now()
    dates = get_trade_dates(
        (today - timedelta(days=35)).strftime('%Y%m%d'),
        today.strftime('%Y%m%d')
    )
    
    print(f"最近交易日: {dates[-5:]}")
    
    # 获取数据
    success_count = 0
    for date in dates[-20:]:
        if save_industry_data(date):
            success_count += 1
    
    print(f"\n{'='*50}")
    print(f"完成！成功获取 {success_count} 个交易日数据")
    print(f"数据保存目录: {DATA_DIR}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
