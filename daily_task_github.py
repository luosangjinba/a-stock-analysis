"""
A股行业数据定时任务脚本 - GitHub Actions版本
"""
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 从环境变量获取配置
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', '')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

# 数据目录
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def send_telegram_message(message):
    """发送Telegram消息"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram配置未设置，跳过通知")
        return
        
    import requests
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=data, timeout=10)
        print("Telegram通知已发送")
    except Exception as e:
        print(f"Telegram通知失败: {e}")

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
    """获取指定日期的股票数据"""
    pro = ts.pro_api(TUSHARE_TOKEN)
    
    dates = get_trade_dates(
        (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=45)).strftime('%Y%m%d'),
        trade_date
    )
    
    if len(dates) < 20:
        return None
    
    end_date = dates[-1]
    start_date = dates[-20]
    
    stock_basic = pro.stock_basic(exchange='', fields='ts_code,symbol')
    adj_factor = pro.adj_factor(trade_date=trade_date)
    stock_basic = stock_basic.merge(adj_factor[['ts_code', 'adj_factor']], on='ts_code', how='left')
    
    adj_factor_start = pro.adj_factor(trade_date=start_date)
    stock_basic = stock_basic.merge(adj_factor_start[['ts_code', 'adj_factor']], on='ts_code', how='left', suffixes=('_end', '_start'))
    
    daily = pro.daily(trade_date=trade_date)
    stock_basic = stock_basic.merge(daily[['ts_code', 'close']], on='ts_code', how='left')
    stock_basic = stock_basic.rename(columns={'close': 'close_end'})
    
    daily_start = pro.daily(trade_date=start_date)
    stock_basic = stock_basic.merge(daily_start[['ts_code', 'close']], on='ts_code', how='left')
    stock_basic = stock_basic.rename(columns={'close': 'close_start'})
    
    stock_basic = stock_basic.dropna(subset=['close_end', 'close_start', 'adj_factor_end', 'adj_factor_start'])
    
    if len(stock_basic) == 0:
        return None
    
    stock_basic['price_end'] = stock_basic['close_end'] * stock_basic['adj_factor_end']
    stock_basic['price_start'] = stock_basic['close_start'] * stock_basic['adj_factor_start']
    stock_basic['change_pct'] = ((stock_basic['price_end'] - stock_basic['price_start']) / stock_basic['price_start'] * 100).round(2)
    
    industry_df = get_stock_industry()
    stock_basic = stock_basic.merge(industry_df[['ts_code', 'industry']], on='ts_code', how='left')
    
    return stock_basic[['ts_code', 'symbol', 'industry', 'price_end', 'price_start', 'change_pct']]

def calculate_industry_ranking(stock_data):
    """计算行业排名"""
    if stock_data is None or len(stock_data) == 0:
        return None
    
    industry_stats = stock_data.groupby('industry').agg({
        'change_pct': ['mean', 'count'],
        'ts_code': 'count'
    }).round(2)
    
    industry_stats.columns = ['平均涨幅', '股票数量', 'count']
    industry_stats = industry_stats.reset_index()
    industry_stats = industry_stats.rename(columns={'industry': '行业名称'})
    industry_stats = industry_stats.sort_values('平均涨幅', ascending=False).reset_index(drop=True)
    industry_stats.insert(0, '排名', range(1, len(industry_stats) + 1))
    
    return industry_stats

def save_industry_data(trade_date):
    """保存指定日期的行业数据"""
    print(f"获取 {trade_date} 行业数据...")
    
    stock_data = get_stock_prices(trade_date)
    if stock_data is None:
        print(f"无法获取 {trade_date} 的股票数据")
        return False, None
    
    industry_ranking = calculate_industry_ranking(stock_data)
    if industry_ranking is None:
        print(f"无法计算 {trade_date} 的行业排名")
        return False, None
    
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file = os.path.join(DATA_DIR, f"{trade_date}.csv")
    industry_ranking.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"已保存: {output_file}")
    
    return True, industry_ranking

def main():
    """主函数"""
    print("="*50)
    print("A股行业数据定时任务开始 (GitHub Actions)")
    print("="*50)
    
    # 获取最新交易日
    today = datetime.now()
    dates = get_trade_dates(
        (today - timedelta(days=10)).strftime('%Y%m%d'),
        today.strftime('%Y%m%d')
    )
    
    if not dates:
        send_telegram_message("❌ 无法获取交易日历")
        return
    
    latest_date = dates[-1]
    print(f"最新交易日: {latest_date}")
    
    # 检查是否已有数据
    data_file = os.path.join(DATA_DIR, f"{latest_date}.csv")
    if os.path.exists(data_file):
        print(f"数据已存在: {data_file}")
        os.remove(data_file)
    
    # 获取数据
    success, industry_ranking = save_industry_data(latest_date)
    
    if not success:
        send_telegram_message(f"❌ 获取 {latest_date} 数据失败")
        return
    
    # 获取前一日数据进行对比
    prev_date = dates[-2] if len(dates) >= 2 else None
    prev_data = None
    if prev_date:
        prev_file = os.path.join(DATA_DIR, f"{prev_date}.csv")
        if os.path.exists(prev_file):
            prev_data = pd.read_csv(prev_file)
    
    # 发送Telegram通知
    if success:
        top10 = industry_ranking.head(10)
        msg = f"✅ 行业数据更新完成！\n\n"
        msg += f"📅 日期: {latest_date}\n"
        msg += f"📊 行业数: {len(industry_ranking)}\n\n"
        msg += "🏆 行业涨幅 Top 10:\n"
        
        for i, row in top10.iterrows():
            rank = row['排名']
            name = row['行业名称']
            change = row['平均涨幅']
            msg += f"{rank}. {name}: +{change}%\n"
        
        if prev_data is not None:
            prev_top10 = set(prev_data.head(10)['行业名称'])
            curr_top10 = set(top10['行业名称'])
            new_enter = curr_top10 - prev_top10
            
            if new_enter:
                msg += f"\n✨ 新进Top 10: {', '.join(new_enter)}"
        
        msg += f"\n\n🌐 查看网站: (请访问GitHub Pages)"
        
        send_telegram_message(msg)
    
    print("任务完成!")

if __name__ == "__main__":
    main()
