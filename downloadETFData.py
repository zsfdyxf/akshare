

import logging
logging.basicConfig(filename='etf_download.log', level=logging.INFO)

import akshare as ak
import pandas as pd
import time
from datetime import datetime, timedelta

def fetch_etf_data_in_chunks(symbol, start_date, end_date, chunk_months=1):
    """
    分时间段获取ETF历史数据
    
    参数：
    - symbol: ETF代码（如 "518880"）
    - start_date: 开始日期（格式 "YYYY-MM-DD"）
    - end_date: 结束日期
    - chunk_months: 每次请求的月份跨度（默认3个月）
    """
    all_data = []
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=chunk_months*30), end_date)
        
        try:
            print(f"正在下载 {symbol} {current_start.date()} 至 {current_end.date()} 数据...")
            df = ak.fund_etf_hist_em(symbol=symbol, period="daily")
            
            # 处理日期列
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[(df["日期"] >= current_start) & (df["日期"] <= current_end)]
            
            if not df.empty:
                df.insert(1, "股票代码", symbol)
                all_data.append(df)
            
            print(f"已获取 {len(df)} 条记录")
            time.sleep(3)  # 添加3秒延迟避免频繁请求
            
        except Exception as e:
            print(f"下载 {current_start.date()} 至 {current_end.date()} 数据失败: {str(e)}")
            time.sleep(10)  # 失败后等待更长时间
        
        current_start = current_end + timedelta(days=1)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# -------------------------------
# 这里是直接执行的入口逻辑
# -------------------------------
if __name__ == "__main__":
    # 示例调用（直接修改这里的参数即可）
    gold_etf = fetch_etf_data_in_chunks(
        symbol="518880",          # ETF代码
        start_date="2024-01-01",  # 开始日期
        end_date="2024-12-31",    # 结束日期
        chunk_months=1            # 每次请求1个月数据（更保守）
    )

    # 保存结果
    if not gold_etf.empty:
        gold_etf.to_csv("gold_etf_2024.csv", index=False)
        print("数据已保存至 gold_etf_2024.csv")
    else:
        print("未获取到有效数据")