import akshare as ak
import pandas as pd
from tqdm import tqdm  # 进度条工具




all_codes = ak.stock_us_spot()
all_codes.to_excel("us_stock_full_info.xlsx")
            
           


