import akshare as ak
import pandas as pd
import time
import inspect
from datetime import datetime, timedelta
import yfinance as yf

# 时间范围设置（建议单次不超过1年）
start_date = "2024-01-01"
end_date = "2025-06-30"
batch_size = 30  # 每次获取的天数（1个月）



def get_gold_reserves_batch(start, end):
    """安全分批获取函数"""
    try:
        df = ak.macro_china_fx_gold()
        # 过滤时间范围
        mask = (df['日期'] >= pd.to_datetime(start)) & (df['日期'] <= pd.to_datetime(end))
        return df.loc[mask].copy()
    except Exception as e:
        print(f"获取{start}至{end}数据失败: {str(e)}")
        return pd.DataFrame()

def safe_fetch(start_date, end_date, batch_size=180):
    current = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    result = []
    
    while current <= end:
        batch_end = min(current + timedelta(days=batch_size), end)
        print(f"正在获取 {current.date()} 至 {batch_end.date()} 数据...")
        
        batch_data = get_gold_reserves_batch(current, batch_end)
        if not batch_data.empty:
            result.append(batch_data)
        
        # 防封禁策略
        time.sleep(5)  # 每次请求间隔5秒
        if len(result) % 3 == 0:  # 每3批后延长等待
            time.sleep(30)
        
        current = batch_end + timedelta(days=1)
    
    return pd.concat(result).drop_duplicates() if result else pd.DataFrame()



def validate_data(df):
    required_cols = ['日期', '黄金储备(万盎司)', '外汇储备(亿美元)']
    if not all(col in df.columns for col in required_cols):
        raise ValueError("缺失关键字段，接口可能已变更")
    
    # 单位转换：万盎司 → 吨
    df['黄金储备(吨)'] = df['黄金储备(万盎司)'] * 0.0321507
    return df.sort_values('日期')


def clean_data(df):
    # 过滤极端值（中国黄金储备应在1000-2500吨之间）
    df = df[(df['黄金储备(吨)'] > 1000) & (df['黄金储备(吨)'] < 2500)]
    
    # 填充缺失日期（线性插值）
    df = df.set_index('日期').resample('M').interpolate().reset_index()
    return df

# 获取VIX指数历史数据
def get_vix_data(start_date='2010-01-01', end_date='2023-12-31'):
    vix_data = ak.vix_index_hist(start_date=start_date, end_date=end_date)
    return vix_data




import yfinance as yf

# 获取VIX指数的历史数据
def get_vix_data_yf(start_date='2025-01-01', end_date='2025-06-30'):
    vix = yf.Ticker("^VIX")
    vix_data = vix.history(start=start_date, end=end_date)
    return vix_data

# 示例：获取2010年到2023年VIX数据
vix_data = get_vix_data_yf()
print(vix_data.head())




# 示例：获取2010年到2023年VIX数据


# stock_xgsglb_em_df = ak.stock_xgsglb_em(symbol="北交所")
# print(stock_xgsglb_em_df)





# # 获取VIX指数数据
# vix = yf.Ticker("^VIX")

# # 获取VIX的历史数据
# vix_data = vix.history(period="1mo")  # 获取最近一个月的数据

# # 显示数据
# print(vix_data)



# macro_info_ws_df = ak.macro_info_ws(date="20250716")
# print(macro_info_ws_df)

# 获取函数签名
# sig = inspect.signature(ak.stock_xgsglb_em)
# print("函数参数：")
# for name, param in sig.parameters.items():
#     print(f"{name}: {param.default if param.default != param.empty else '无默认值'}")

# #     # 步骤1：安全获取数据
# raw_data = ak.macro_cons_gold()
# ak.macro_cons_gold()
# macro_gold_central_bank
# # raw_data = ak.spot_golden_benchmark_sge()
# # print(ak.signature(ak.macro_china_fx_gold))

# 步骤2：数据清洗
# cleaned_data = clean_data(validate_data(raw_data))

# # 步骤3：保存结果
# output_file = f"黄金储备_{start_date}_至_{end_date}.csv"
# raw_data.to_csv(output_file, index=False, encoding='utf-8-sig')

# # print(f"获取完成！共{len(cleaned_data)}条数据，已保存到 {output_file}")
# print(raw_data.tail())

