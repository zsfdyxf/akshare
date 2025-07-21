import akshare as ak
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime
import random
import traceback

# 配置参数
MAX_RETRIES = 3  # 单只股票最大重试次数
REQUEST_DELAY = (1, 5)  # 请求延迟区间(秒)
BATCH_SIZE = 30  # 每批处理数量
TIMEOUT = 10  # 请求超时时间(秒)

def get_all_stock_codes():
    """获取全市场股票代码（带自动重试和备用方案）"""
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            # 方法1：统一接口
            df = ak.stock_info_a_code_name()
            codes = df['code'].astype(str).str.zfill(6).tolist()
            return sorted(list(set(codes)))  # 去重排序
            
        except Exception as e:
            attempts += 1
            print(f"获取股票列表失败(尝试{attempts}/{MAX_RETRIES}): {str(e)}")
            time.sleep(random.uniform(*REQUEST_DELAY))
            if attempts == MAX_RETRIES:
                print("正在尝试备用方案...")
                try:
                    # 方法2：分交易所获取
                    df_sz = ak.stock_info_sz_name()
                    df_sh = ak.stock_info_sh_name()
                    df_bj = ak.stock_info_bj_name()
                    codes = pd.concat([
                        df_sz['A股代码'].astype(str).str.zfill(6),
                        df_sh['A股代码'].astype(str).str.zfill(6),
                        df_bj['证券代码'].astype(str)
                    ]).tolist()
                    return sorted(list(set(codes)))
                except Exception as e:
                    print(f"备用方案也失败: {str(e)}")
                    raise Exception("所有股票代码获取方式均失败")
    return []

def get_stock_info_with_retry(code):
    """带重试机制的个股信息获取"""
    for attempt in range(MAX_RETRIES):
        try:
            # 添加超时机制
            start_time = time.time()
            raw_data = ak.stock_individual_info_em(symbol=code)
            
            if time.time() - start_time > TIMEOUT:
                raise TimeoutError("请求超时")
                
            if raw_data.empty:
                raise ValueError("返回数据为空")
                
            info_dict = {
                '股票代码': code,
                'updatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 标准化字段处理
            for _, row in raw_data.iterrows():
                col_name = str(row['item']).strip().replace(' ', '')
                info_dict[col_name] = row['value']
            
            return info_dict
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"股票 {code} 获取最终失败: {str(e)}")
                print(traceback.format_exc())
                return None
            time.sleep(random.uniform(*REQUEST_DELAY))

def save_progress(progress_data, failed_list):
    """保存进度"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if progress_data:
        df = pd.DataFrame(progress_data)
        df.to_excel(f"stock_progress_{timestamp}.xlsx", index=False)
    if failed_list:
        with open(f"failed_list_{timestamp}.txt", 'w') as f:
            f.write('\n'.join(failed_list))

def main():
    print("=== 股票数据采集程序 ===")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 获取代码列表
    try:
        all_codes = get_all_stock_codes()
        print(f"成功获取 {len(all_codes)} 只股票代码")
    except Exception as e:
        print(f"股票代码获取失败: {str(e)}")
        return

    # 分批处理
    all_data = []
    failed_codes = []
    processed_codes = set()
    
    try:
        for i in tqdm(range(0, len(all_codes), BATCH_SIZE), desc="处理进度"):
            batch = all_codes[i:i+BATCH_SIZE]
            batch_data = []
            
            for code in batch:
                if code in processed_codes:
                    continue
                    
                data = get_stock_info_with_retry(code)
                if data:
                    batch_data.append(data)
                    processed_codes.add(code)
                else:
                    failed_codes.append(code)
            
            if batch_data:
                all_data.extend(batch_data)
                # 每批处理完保存一次进度
                save_progress(all_data, failed_codes)
            
            # 随机延迟
            time.sleep(random.uniform(*REQUEST_DELAY))
            
    except KeyboardInterrupt:
        print("\n用户中断执行，正在保存已获取数据...")
    except Exception as e:
        print(f"程序异常: {str(e)}")
        print(traceback.format_exc())
    finally:
        # 最终保存
        if all_data:
            df = pd.DataFrame(all_data)
            common_fields = ['股票代码', '股票简称', '上市时间', 'updatetime', '最新', '行业']
            existing_fields = [f for f in common_fields if f in df.columns]
            other_fields = [f for f in df.columns if f not in common_fields]
            df = df[existing_fields + other_fields]
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_file = f"stock_full_info_{timestamp}.xlsx"
            df.to_excel(excel_file, index=False, encoding='utf-8-sig')
            
            print(f"\n成功获取 {len(df)} 只股票数据")
            print(f"保存到: {excel_file}")
            
            if failed_codes:
                print(f"失败股票代码({len(failed_codes)}个): {failed_codes[:10]}...")
                with open(f"failed_codes_{timestamp}.txt", 'w') as f:
                    f.write('\n'.join(failed_codes))

if __name__ == '__main__':
    main()
