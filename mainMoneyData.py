import akshare as ak
import pandas as pd
import time
from datetime import datetime
import os
from tqdm import tqdm

class StockFundFlowExporter:
    def __init__(self):
        self.request_interval = 3  # 请求间隔(秒)
        self.max_retry = 3         # 最大重试次数
        self.output_dir = "fund_flow_data"  # 输出目录
        os.makedirs(self.output_dir, exist_ok=True)

    def _safe_request(self, func, **kwargs):
        """带错误处理和重试的请求封装"""
        for attempt in range(self.max_retry):
            try:
                time.sleep(self.request_interval + (0.5 if attempt > 0 else 0))  # 重试时增加延迟
                return func(**kwargs)
            except Exception as e:
                if attempt == self.max_retry - 1:
                    raise Exception(f"请求失败: {str(e)}")
                print(f"请求失败({attempt+1}/{self.max_retry}): {e}")

    def get_main_fund_flow(self, stock_code):
        """获取主力资金流数据（使用当前可用接口）"""
        market = "sh" if stock_code.startswith(("6", "9", "688")) else "sz"
        
        # 方法1：使用 stock_individual_fund_flow 接口
        try:
            df = self._safe_request(
                ak.stock_individual_fund_flow,
                stock=stock_code,
                market=market
            )
            return df
        except Exception as e:
            print(f"接口stock_individual_fund_flow不可用: {e}")
        
        # 方法2：备用接口 stock_main_fund_flow
        try:
            df_all = self._safe_request(ak.stock_main_fund_flow)
            df = df_all[df_all['股票代码'] == stock_code]
            return df
        except Exception as e:
            print(f"备用接口也不可用: {e}")
        
        raise Exception("所有资金流接口均不可用")

    def get_big_orders(self, stock_code, days=5):
        """获取大单交易数据"""
        market = "sh" if stock_code.startswith(("6", "9", "688")) else "sz"
        df_all = pd.DataFrame()
        
        for i in range(0, min(days, 30), 5):
            period = min(5, days - i)
            try:
                batch = self._safe_request(
                    ak.stock_big_order,
                    stock=stock_code,
                    period=str(period),
                    adjust="hfq"
                )
                df_all = pd.concat([df_all, batch])
                time.sleep(1)
            except Exception as e:
                print(f"获取{stock_code}第{i+1}-{i+period}天大单数据失败: {e}")
        
        return df_all

    def export_to_csv(self, df, stock_code, data_type="fund_flow"):
        """导出数据到CSV"""
        if df is None or df.empty:
            return False
            
        filename = f"{self.output_dir}/{stock_code}_{data_type}_{datetime.now().strftime('%Y%m%d')}.csv"
        try:
            df.to_csv(filename, index=False, encoding='utf_8_sig')
            print(f"数据已保存到: {filename}")
            return True
        except Exception as e:
            print(f"导出CSV失败: {e}")
            return False

    def run(self, stock_codes, days=5):
        """执行完整流程"""
        success_count = 0
        for code in tqdm(stock_codes, desc="处理进度"):
            try:
                # 获取主力资金流
                fund_flow = self.get_main_fund_flow(code)
                if fund_flow is not None:
                    self.export_to_csv(fund_flow, code, "fund_flow")
                
                # 获取大单交易
                big_orders = self.get_big_orders(code, days)
                if big_orders is not None:
                    self.export_to_csv(big_orders, code, "big_orders")
                
                success_count += 1
                time.sleep(1)  # 股票间间隔
            except Exception as e:
                print(f"处理股票{code}时出错: {e}")
        
        print(f"处理完成: 成功获取{success_count}/{len(stock_codes)}只股票数据")


# 使用示例
if __name__ == "__main__":
    exporter = StockFundFlowExporter()
    
    # 要查询的股票列表
    stocks = ["002607"]  # 贵州茅台, 五粮液, 宁德时代
    
    # 执行数据获取和导出
    exporter.run(stocks, days=5)