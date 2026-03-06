"""
A股20日涨幅榜 - 添加申万二级行业分类
"""

import tushare as ts
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Tushare Token
TUSHARE_TOKEN = '3169a014b7c2f832cb7be1fb33080ccb70284d663397f856042aad5a'

def main():
    print("=" * 50)
    print("添加申万二级行业分类")
    print("=" * 50)
    
    # 读取现有数据
    input_file = "G:/MiniMAX-agent/project/a_stock_analysis/a_stock_top400.csv"
    df = pd.read_csv(input_file)
    print(f"共 {len(df)} 只股票")
    
    # 初始化Tushare
    pro = ts.pro_api(TUSHARE_TOKEN)
    
    # 获取所有A股的行业分类
    print("\n获取申万二级行业分类...")
    industry_df = pro.stock_basic(exchange='', fields='ts_code,industry')
    print(f"获取到 {len(industry_df)} 条行业数据")
    
    # 转换股票代码格式
    industry_df['symbol'] = industry_df['ts_code'].str.replace('.SH', '').str.replace('.SZ', '')
    
    # 将股票代码转为字符串以便合并
    df['股票代码'] = df['股票代码'].astype(str)
    
    # 合并到原数据
    df = df.merge(industry_df[['symbol', 'industry']], left_on='股票代码', right_on='symbol', how='left')
    df = df.drop(columns=['symbol'])
    
    # 重命名列
    df = df.rename(columns={'industry': '申万二级行业'})
    
    # 保存带行业的数据
    output_file1 = "G:/MiniMAX-agent/project/a_stock_analysis/a_stock_top400_with_industry.csv"
    df.to_csv(output_file1, index=False, encoding='utf-8-sig')
    print(f"\n已保存带行业的CSV: {output_file1}")
    
    # 统计申万二级行业
    print("\n统计申万二级行业...")
    industry_count = df['申万二级行业'].value_counts().reset_index()
    industry_count.columns = ['申万二级行业', '股票数量']
    industry_count = industry_count.sort_values('股票数量', ascending=False)
    
    # 取前12名
    top12 = industry_count.head(12).copy()
    top12.insert(0, '排名', range(1, 13))
    
    # 保存行业统计CSV
    output_file2 = "G:/MiniMAX-agent/project/a_stock_analysis/industry_top12.csv"
    top12.to_csv(output_file2, index=False, encoding='utf-8-sig')
    print(f"已保存行业统计CSV: {output_file2}")
    
    # 显示结果
    print("\n" + "=" * 50)
    print("申万二级行业 Top 12:")
    print("=" * 50)
    print(top12.to_string(index=False))
    
    print(f"\n行业总数: {len(industry_count)}")

if __name__ == "__main__":
    main()
