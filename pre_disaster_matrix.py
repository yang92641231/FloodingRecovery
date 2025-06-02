import pandas as pd
import numpy as np
from datetime import datetime


# 生成每日灾难矩阵表，每行是县，每列是一个日期，灾难发生标记为1。

# 参数:
# - input_csv: 输入的CSV文件路径
# - output_csv: 输出的CSV文件路径
# - start_date: 起始日期（字符串格式）
# - end_date: 结束日期（字符串格式）
# - county_col: 县名列名
# - code_col: 县代码列名
# - date_col: 灾难日期列名



def generate_disaster_matrix(input_csv, output_csv,
                              start_date='2012-01-01', end_date='2024-12-31',
                              county_col='County', code_col='countyCode',
                              date_col='Incident Begin Date'):

    # 读取数据
    df = pd.read_csv(input_csv, parse_dates=[date_col])
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # 日期范围
    all_dates = pd.date_range(start=start_date, end=end_date)

    # 获取所有县的基本信息
    unique_counties = df[[county_col, code_col]].drop_duplicates().reset_index(drop=True)
    date_matrix = pd.DataFrame(0, index=np.arange(len(unique_counties)), columns=all_dates)
    result_df = pd.concat([unique_counties, date_matrix], axis=1)

    # 标记灾难发生
    for idx, row in df.iterrows():
        county = row[county_col]
        code = row[code_col]
        date = row[date_col].date()
        match = (result_df[county_col] == county) & (result_df[code_col] == code)
        col = pd.Timestamp(date)
        if col in result_df.columns:
            result_df.loc[match, col] = 1

    # 排序列并保存
    result_df = result_df[[county_col, code_col] + sorted(all_dates)]
    result_df.to_csv(output_csv, index=False)
