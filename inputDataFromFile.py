import sqlite3
import pandas as pd
import os
from datetime import datetime

# 字段名映射字典（中文列名 -> 英文列名）
COLUMN_MAPPING = {
    '股票代码': 'stock_code',
    'updatetime': 'update_time',
    '最新': 'latest_price',
    '股票简称': 'stock_name',
    '总股本': 'total_shares',
    '流通股': 'float_shares',
    '总市值': 'total_market_cap',
    '流通市值': 'float_market_cap',
    '行业': 'industry',
    '上市时间': 'listing_date'
}

def import_excel_to_sqlite():
    # 数据库配置
    db_name = 'DB_GoldETF_1.db'
    table_name = 'stock_info'
    data_folder = 'DataInput'
    
    # 连接SQLite数据库
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    try:
        # 检查是否有Excel文件
        excel_files = [f for f in os.listdir(data_folder) if f.endswith(('.xlsx', '.xls'))]
        if not excel_files:
            raise FileNotFoundError(f"{data_folder}文件夹中没有找到Excel文件")
        
        # 创建表（如果不存在）
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {', '.join([f'{col} TEXT' for col in COLUMN_MAPPING.values()])},
            import_time TEXT
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # 遍历所有Excel文件
        for file in excel_files:
            file_path = os.path.join(data_folder, file)
            print(f"\n正在处理文件: {file}")
            
            # 读取Excel文件
            df = pd.read_excel(file_path, engine='openpyxl')
            
            # 重命名列名（中文->英文）
            df.rename(columns=COLUMN_MAPPING, inplace=True)
            
            # 确保所有需要的英文列都存在
            for eng_col in COLUMN_MAPPING.values():
                if eng_col not in df.columns:
                    df[eng_col] = None  # 添加缺失列
                    
            # 特殊处理股票代码
            if 'stock_code' in df.columns:
                df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
            
            # # 添加导入时间
            # df['import_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 只保留数据库需要的列
            # df = df[list(COLUMN_MAPPING.values())]
            
            # 写入数据库
            df.to_sql(
                table_name, 
                conn, 
                if_exists='append', 
                index=False
            )
            
            print(f"成功导入 {len(df)} 条数据")
        
    except Exception as e:
        conn.rollback()
        print(f"\n错误: {str(e)}")
        print("解决方案建议:")
        print("1. 检查Excel文件是否包含所有必要的中文字段名")
        print("2. 确认COLUMN_MAPPING字典与Excel列名完全匹配")
    finally:
        # 验证结果
        if conn:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\n当前表记录总数: {count}")
            
            if count > 0:
                cursor.execute(f"PRAGMA table_info({table_name})")
                print("\n当前表结构:")
                for col in cursor.fetchall():
                    print(f"{col[1]:<20} {col[2]:<10}")
                
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                print("\n首条记录样本:")
                print(cursor.fetchone())
                
            conn.close()

if __name__ == '__main__':
    print(f"{'='*30}")
    print(f"开始执行 (时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    # 确保依赖库已安装
    try:
        import openpyxl
    except ImportError:
        print("正在安装依赖库openpyxl...")
        import subprocess
        subprocess.check_call(["pip", "install", "openpyxl"])
    
    import_excel_to_sqlite()
    print(f"\n执行完成 (时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"{'='*30}")