import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import sqlite3

# 假设您已经从数据库读取数据到DataFrame
# 这里提供一个示例数据加载方式（您需要根据实际数据源调整）

def load_stock_data():
    """
    从数据库或其他数据源加载股票数据
    返回包含日期和收盘价的DataFrame
    """
    # 这里应该是您从数据库读取数据的代码
    # 示例：使用SQL查询读取数据

    conn = sqlite3.connect('DB_GoldETF_1.db')
    query = "SELECT date, stock_code, close_price FROM stock_data WHERE stock_code = '518880' AND DATE BETWEEN '2025-01-01' AND '2025-06-30' ORDER BY date"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # 由于没有实际数据库连接，这里创建一个示例DataFrame
    # 您应该替换这部分代码为实际的数据加载逻辑
    # dates = pd.date_range(start='2025-01-01', periods=30)
    # close_prices = [100 + i + 2 * (i % 3) for i in range(30)]
    # df = pd.DataFrame({
    #     'date': dates,
    #     'stock_code': '518880',
    #     'close_price': close_prices
    # })
    return df

def calculate_5day_ma(df):
    """
    计算5日均线
    :param df: 包含日期和收盘价的DataFrame
    :return: 添加了5日均线的DataFrame
    """
    df['5_day_MA'] = df['close_price'].rolling(window=15).mean()
    return df

def plot_stock_with_ma(df, stock_code):
    """
    绘制股票收盘价和5日均线图
    :param df: 包含日期、收盘价和5日均线的DataFrame
    :param stock_code: 股票代码，用于标题显示
    """
    plt.figure(figsize=(14, 7))
    
    # 确保日期列是datetime类型
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # 绘制收盘价
    plt.plot(df['date'], df['close_price'], label='closePrice', color='blue', alpha=0.5)
    
    # 绘制5日均线
    plt.plot(df['date'], df['5_day_MA'], label='5DaysAvg', color='red', linewidth=2)
    
    # 设置图表标题和标签
    plt.title(f'{stock_code} 股票收盘价与5日均线走势图', fontsize=16)
    plt.xlabel('日期', fontsize=14)
    plt.ylabel('价格', fontsize=14)
    
    # 设置x轴为日期格式
    ax = plt.gca()
    
    # 根据数据时间跨度自动选择合适的刻度间隔
    date_range = (df['date'].max() - df['date'].min()).days
    if date_range <= 7:  # 一周内数据，每天显示
        locator = mdates.DayLocator()
        formatter = mdates.DateFormatter('%m-%d')
    elif date_range <= 30:  # 一个月内数据，每5天显示
        locator = mdates.DayLocator(interval=5)
        formatter = mdates.DateFormatter('%m-%d')
    elif date_range <= 365:  # 一年内数据，每月显示
        locator = mdates.MonthLocator()
        formatter = mdates.DateFormatter('%Y-%m')
    else:  # 超过一年，每季度显示
        locator = mdates.MonthLocator(interval=3)
        formatter = mdates.DateFormatter('%Y-%m')
    
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    
    # 自动旋转日期标记
    plt.gcf().autofmt_xdate()  
    
    # 添加网格和图例
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(fontsize=12)
    
    plt.tight_layout()
    plt.show()

def plot_stock_with_ma_and_volatility(df, stock_code, window=5):
    """
    绘制股票收盘价、5日均线和波动率线
    :param df: 包含日期和收盘价的DataFrame
    :param stock_code: 股票代码，用于标题显示
    :param window: 计算波动率的滚动窗口大小(默认20日)
    """
    plt.figure(figsize=(14, 8))
    
    # 确保日期列是datetime类型
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # 计算技术指标
    df['5_day_MA'] = df['close_price'].rolling(window=5).mean()
    df['volatility'] = df['close_price'].rolling(window=window).std()
    
    # 创建主图和次图
    ax1 = plt.gca()
    ax2 = ax1.twinx()  # 共享x轴，创建右侧y轴
    
    # 绘制收盘价(主坐标轴)
    ax1.plot(df['date'], df['close_price'], label='收盘价', color='blue', alpha=0.5)
    
    # 绘制5日均线(主坐标轴)
    ax1.plot(df['date'], df['5_day_MA'], label='5日均线', color='red', linewidth=2)
    
    # 绘制波动率线(次坐标轴)
    ax2.plot(df['date'], df['volatility'], label=f'{window}日波动率', 
             color='green', linestyle='--', linewidth=1.5)
    
    # 设置图表标题和标签
    plt.title(f'{stock_code} 股票走势与波动率分析', fontsize=16)
    ax1.set_xlabel('日期', fontsize=14)
    ax1.set_ylabel('价格', fontsize=14)
    ax2.set_ylabel('波动率', fontsize=14)
    
    # 设置x轴日期格式
    date_range = (df['date'].max() - df['date'].min()).days
    if date_range <= 7:
        locator = mdates.DayLocator()
        formatter = mdates.DateFormatter('%m-%d')
    elif date_range <= 30:
        locator = mdates.DayLocator(interval=5)
        formatter = mdates.DateFormatter('%m-%d')
    elif date_range <= 365:
        locator = mdates.MonthLocator()
        formatter = mdates.DateFormatter('%Y-%m')
    else:
        locator = mdates.MonthLocator(interval=3)
        formatter = mdates.DateFormatter('%Y-%m')
    
    ax1.xaxis.set_major_locator(locator)
    ax1.xaxis.set_major_formatter(formatter)
    
    # 自动旋转日期标记
    plt.gcf().autofmt_xdate()
    
    # 添加网格和图例
    ax1.grid(True, linestyle='--', alpha=0.7)
    
    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=12)
    
    plt.tight_layout()
    plt.show()

# 主程序
if __name__ == "__main__":

    # 设置全局中文字体
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # Windows系统推荐使用微软雅黑
    # plt.rcParams['font.sans-serif'] = ['SimHei']  # 或者使用黑体
    # plt.rcParams['font.sans-serif'] = ['PingFang SC']  # macOS系统推荐使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
    # 1. 加载股票数据
    stock_df = load_stock_data()
    
    # 2. 计算5日均线
    stock_df = calculate_5day_ma(stock_df)
    
    # 3. 绘制图表
    #plot_stock_with_ma(stock_df, stock_df['stock_code'].iloc[0])
    plot_stock_with_ma_and_volatility(stock_df,stock_df['stock_code'].iloc[0], window=20)