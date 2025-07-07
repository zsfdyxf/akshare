#!/usr/bin/env python
# -*- coding:utf-8 -*-
import akshare as ak
import pandas as pd



# 获取全球最大黄金ETF-SPDR持仓数据
# df = ak.macro_china_fx_gold()
df = ak.macro_cons_gold()
print(df)  # 筛选SPDR黄金ETF

# # 获取数据
# df = ak.fund_etf_hist_em(symbol="518880", period="daily")

# # 处理日期列（根据实际列名调整）
# df["日期"] = pd.to_datetime(df["日期"])

# # 筛选时间段
# filtered_df = df[(df["日期"] >= "2025-01-01") & (df["日期"] <= "2025-06-30")]

# # 保存到CSV（可选）
# filtered_df.to_csv("gold_etf_20250101-0630.csv", index=False)

# print(filtered_df)

# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="518880", period="daily", start_date="20250601", end_date='20250630', adjust="")


# stock_zh_a_hist_df.to_excel("stock_data1.xlsx", index=False)  
# print("导出 stock_data1.xlsx")
# print(stock_zh_a_hist_df)

# 开盘	收盘	最高	最低	成交量	成交额	振幅	涨跌幅	涨跌额	换手率