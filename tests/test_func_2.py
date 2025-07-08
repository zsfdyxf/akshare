#!/usr/bin/env python
# -*- coding:utf-8 -*-
import akshare as ak
import pandas as pd
from tqdm import tqdm  # 进度条工具


# 获取全球最大黄金ETF-SPDR持仓数据
# df = ak.macro_china_fx_gold()
# df = ak.macro_cons_gold()
# print(df)  # 筛选SPDR黄金ETF

# 1.-ETF 获取数据 518880 黄金ETF
# df = ak.fund_etf_hist_em(symbol="518880", period="daily")
# # 处理日期列（根据实际列名调整）
# df["日期"] = pd.to_datetime(df["日期"])
# # 筛选时间段
# filtered_df = df[(df["日期"] >= "2024-01-01") & (df["日期"] <= "2024-12-31")]
# # 在第2列位置插入股票代码列（列索引从0开始）
# filtered_df.insert(1, "股票代码", "518880")  # 1表示第2列
# # 保存到CSV（可选）
# filtered_df.to_csv("gold_etf_20240101-20241231.csv", index=False)


# 2.- 获取所有股票列表
# 获取所有A股股票代码和名称
# stock_list = ak.stock_info_a_code_name()
# stock_list.to_excel("all_stocks_list_20250708.xlsx", index=False)

# 获取深交所、上交所、北交所股票列表（更详细）
# sz_stocks = ak.stock_zh_ah_name()
# sz_stocks.to_excel("sz_stocks_list_20250708.xlsx", index=False)
# sh_stocks = ak.stock_zh_ah_name()
# bj_stocks = ak.stock_zh_ah_name()

# 合并所有股票数据
# all_stocks = pd.concat([sz_stocks, sh_stocks, bj_stocks], ignore_index=True)


# 3.- 详细信息

# 以贵州茅台（600519）为例
data = ak.stock_individual_info_em(symbol="600519")
print("字段结构:\n", data)

# # 获取基础股票列表（代码+名称）
# stock_list = ak.stock_info_a_code_name()
# print("原始列名:", stock_list.columns.tolist())  # 确认列名是['code','name']

# # 为每只股票获取上市日期（此操作较耗时，约10-20分钟）
# stock_list['listing_date'] = ""  # 新增列

# # 为每只股票获取上市日期（注意：此操作较耗时）
# listings = []
# for code in tqdm(stock_list["code"]):
#     try:
#         info = ak.stock_individual_info_em(symbol=code)
#         listing_date = info[info["item"] == "上市时间"]["value"].iloc[0]
#         listings.append(listing_date)
#     except:
#         listings.append("")  # 获取失败时留空

# # 合并上市日期到原数据
# stock_list["上市日期"] = listings

# # 导出Excel（保留前导零）
# stock_list["代码"] = stock_list["代码"].astype(str).str.zfill(6)
# stock_list.to_excel("all_stocks_list_with_date.xlsx", index=False)


# 直接导出为Excel格式（保留原始文本）
#stock_list["代码"] = stock_list["代码"].astype(str).str.zfill(6)


# 导出CSV（无需特殊格式）
#stock_list.to_csv("all_stocks_list_20250708.csv", index=False, encoding="utf_8_sig")



print("股票列表已保存至 all_stocks_list.csv")

# 2.- 股票

# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="518880", period="daily", start_date="20250601", end_date='20250630', adjust="")

# stock_zh_a_hist_df.to_excel("stock_data1.xlsx", index=False)  
# print("导出 stock_data1.xlsx")
# print(stock_zh_a_hist_df)

# 开盘	收盘	最高	最低	成交量	成交额	振幅	涨跌幅	涨跌额	换手率