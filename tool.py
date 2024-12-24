import math
import numpy as np
from math import sin, cos, sqrt, atan2, radians
from collections import Counter
import pandas as pd
from datetime import datetime
import re
from shapely.geometry import Point
from scipy.optimize import curve_fit
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday
from datetime import datetime


def id_home_dict(df):
    '''
    :param df:处理后的公园访问数据
    :return:用户家庭住址dict
    '''
    filtered_data_home = df.groupby('id').first().reset_index()
    id_home_dict = filtered_data_home.set_index('id')[['home_x', 'home_y']].to_dict(orient='index')
    # print(len(uid_home_dict))

    return id_home_dict


def id_home_flow_dict(df):
    '''
    :param df:处理后的公园访问数据
    :return:用户家庭住址dict
    '''
    filtered_data_home = df.groupby('id').first().reset_index()
    id_home_flow_dict = filtered_data_home.set_index('id')[['home_x', 'home_y']].to_dict(orient='index')
    print('id_home_dict')
    print(len(id_home_flow_dict))

    # 计算每个用户id的频次即用户flow并添加到字典中
    id_counts = df['id'].value_counts().to_dict()
    # 找到字典中值的最大值
    max_count = max(id_counts.values())
    print("最大值:", max_count)
    for user_id, count in id_counts.items():
        id_home_flow_dict[user_id]['flow'] = count
    print('id_home_flow_dict')
    print(len(id_home_flow_dict))
    # print(id_home_flow_dict)

    return id_home_flow_dict


def id_attr_dict(df):
    '''
    :param df:anjuke
    :return:小区id与小区经纬度、totalHouseHoldNum、price的dict
    '''
    id_attr_dict = df.set_index('id')[['lng', 'lat', 'totalHouseHoldNum', 'price']].to_dict(orient='index')
    # print(len(id_attr_dict))

    return id_attr_dict


# 定义函数来为每个多边形计算 area_label
def calculate_area_label(area):
    if area <= 30000:
        return 'A'
    elif 30000 < area <= 100000:
        return 'B'
    elif 100000 < area <= 500000:
        return 'C'
    elif 500000 < area <= 1000000:
        return 'D'
    elif area > 1000000:
        return 'E'


def urban_or_suburban(adcode):
    '''
    :param adcode: 行政区域编码
    :return: 判断城区or郊区
    '''
    if adcode == '310101' or '310106' or '310104' or '310105' or '310110' or '310109' or '310107':
        return 'urban'
    else:
        return 'suburban'


def distance(lon1, lat1, lon2, lat2):
    # 将经纬度转换为弧度
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # 使用 Haversine 公式计算距离
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = 6371 * c  # 地球半径（单位：公里）

    return distance


def haversine(coord1, coord2):
    # 将经纬度转换成弧度
    lat1, lon1 = map(math.radians, coord1)
    lat2, lon2 = map(math.radians, coord2)

    # 计算两点的距离
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r


def topark_trip_distance(df,loc):
    trip_distance = []
    if(loc == 'work'):
        # noncommuter的work2distance设为0
        for index, row in df.iterrows():
            if pd.isna(row['work_x']):
                trip_distance.append(0)
            else:
                delta = distance(row['park_x'], row['park_y'], row[loc + "_x"], row[loc + "_y"])
                trip_distance.append(delta)
    else:
        for index,row in df.iterrows():
            delta = distance(row['park_x'], row['park_y'], row[loc + "_x"], row[loc + "_y"])
            trip_distance.append(delta)

    return trip_distance


def Rg(df,loc):
    ''' caculate Rg '''

    trip_distance = topark_trip_distance(df,loc)

    Rg = 0
    for i in range(len(trip_distance)):
        Rg += trip_distance[i] ** 2
    Rg = math.sqrt(Rg / len(trip_distance))

    return Rg

def Rg_list(df,loc):
    ''' return Rg list '''

    Rg_list = []
    # 按照 "id" 列分组，并得到一系列子 DataFrame
    grouped_df = [group for _, group in df.groupby('id')]
    # everyone Rg
    for i, group_df in enumerate(grouped_df):
        Rg_list.append(Rg(group_df, loc))

    return Rg_list

def percentage(dict):
    # 计算字典值的总和
    total_value = sum(dict.values())
    # 计算每个字典值的占比，并存储为新的字典
    percentage_dict = {key: value / total_value for key, value in dict.items()}

    return percentage_dict


def visit_ratio(weekday_count):
    total_sum = sum(weekday_count.values())
    for key in weekday_count:
        weekday_count[key] /= total_sum

    return weekday_count

def Merge_inflow(df):
    # 将时间列转换为时间增量
    df['activity_start_time'] = pd.to_datetime(df['activity_start_time'], format='%Y-%m-%d %H:%M:%S')
    # 将时间按照每半小时进行归并
    df['activity_start_time'] = df['activity_start_time'].dt.floor('30min')
    # 统计每个时间段内的频次
    frequency_counts = df['activity_start_time'].value_counts().sort_index()
    # 将结果存储在字典中
    frequency_dict = frequency_counts.to_dict()
    # 将字典的键替换为自定义的索引
    new_index = [i / 2 for i in range(48)]
    frequency_dict = dict(zip(new_index, frequency_dict.values()))
    # percentage_dict = percentage(frequency_dict)

    return frequency_dict

def Merge_flow(df):
    df['activity_start_time'] = pd.to_datetime(df['activity_start_time'])
    df['activity_end_time'] = pd.to_datetime(df['activity_end_time'])
    start_hours = df['activity_start_time'].dt.hour.values
    end_hours = df['activity_end_time'].dt.hour.values
    flow = np.zeros(24, dtype=int)

    for start, end in zip(start_hours, end_hours):
        if start == end:
            flow[start] += 1
        else:
            flow[start] += 1
            hours_range = np.arange(start, end + 1) % 24
            flow[hours_range] += 1

    return flow

def Merge_hours(hour):
    '''
    以半小时为单位归并
    '''
    return math.floor(hour * 2) / 2  # 向下取整到最近的半小时


def Merge_minutes(minute):
    '''
    以15min为单位归并，返回hour
    '''
    merge = math.floor(minute / 15) * 15  # 向下取整到最近的刻钟

    return merge/60


def Merge_distance(trip_distance):
    '''
    :param trip_distance: list
    :return: merge list
    '''
    merge_distance = []
    for i in trip_distance:
        # merge_distance.append((i // 2) * 2 if i < 100 else 100)

        if i <= 10:
            merge_distance.append((i // 0.5) * 0.5)

        # if 10 < i <= 100:
        #     merge_distance.append((i // 5) * 5)
        # elif i < 5:
        #     merge_distance.append((i // 1) * 1)
        # elif i < 10:
        #     merge_distance.append((i // 2) * 2)
        # elif i < 30:
        #     merge_distance.append((i // 5) * 5)
        # elif i < 100:
        #     merge_distance.append((i // 10) * 10)

    # 统计每个归并后距离的频次，并存储为字典
    merge_distance_dict = dict(Counter(merge_distance))
    merge_distance_dict = dict(sorted(merge_distance_dict.items()))
    percentage_dict = percentage(merge_distance_dict)

    return percentage_dict


# convert to datetime
def parse_date(date_str):
    return datetime.strptime(date_str, "%Y/%m/%d")


# Gaussian function
def gaussian(x, amplitude, mean, stddev):
    return amplitude * np.exp(-((x - mean) / stddev) ** 2 / 2)


def gini_coefficient(wealths):
    cum_wealths = np.cumsum(sorted(np.append(wealths, 0)))
    sum_wealths = cum_wealths[-1]
    xarray = np.array(range(0, len(cum_wealths))) / float(len(cum_wealths) - 1)
    yarray = cum_wealths / sum_wealths
    B = np.trapz(yarray, x=xarray)
    A = 0.5 - B
    return A / (A + B)


def convert_to_point(point_str):
    # 使用正则表达式从字符串中提取坐标
    coords = re.findall(r"\d+\.\d+", point_str)
    if coords:
        x, y = map(float, coords)
        return Point(x, y)
    else:
        return None


def is_holiday(date, calendar):

    # get the first and last day of the current month
    first_day_of_month = date.replace(day=1)
    last_day_of_month = date.replace(day=(pd.Timestamp(date.year, date.month, 1) + pd.offsets.MonthEnd(0)).day)

    holidays = calendar.holidays(start=first_day_of_month, end=last_day_of_month)
    return date in holidays


def is_weekend(date):

    return date.weekday() >= 5  # 5:Saturday，6:Sunday


def is_tiaoxiu(date):
    date1 = datetime(2014, 1, 26)
    date2 = datetime(2014, 2, 8)
    return date == date1 or date == date2


class ChineseHolidaysCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('元旦', month=1, day=1),
        Holiday('春节假期第一天', month=1, day=31),
        Holiday('春节假期第二天', month=2, day=1),
        Holiday('春节假期第三天', month=2, day=2),
        Holiday('春节假期第四天', month=2, day=3),
        Holiday('春节假期第五天', month=2, day=4),
        Holiday('春节假期第六天', month=2, day=5),
        Holiday('春节假期第七天', month=2, day=6),
        Holiday('清明节第一天', month=4, day=5),
        Holiday('清明节第二天', month=4, day=6),
        Holiday('清明节第三天', month=4, day=7),

    ]

class ChineseHolidaysCalendar_2024(AbstractHolidayCalendar):
    rules = [
        Holiday('元旦', month=1, day=1),
        Holiday('春节假期第一天', month=2, day=10),
        Holiday('春节假期第二天', month=2, day=11),
        Holiday('春节假期第三天', month=2, day=12),
        Holiday('春节假期第四天', month=2, day=13),
        Holiday('春节假期第五天', month=2, day=14),
        Holiday('春节假期第六天', month=2, day=15),
        Holiday('春节假期第七天', month=2, day=16),
        Holiday('春节假期第八天', month=2, day=17),
        Holiday('清明节', month=4, day=4),
        Holiday('清明节', month=4, day=5),
        Holiday('清明节', month=4, day=6),
        Holiday('劳动节', month=5, day=1),
        Holiday('劳动节', month=5, day=2),
        Holiday('劳动节', month=5, day=3),
        Holiday('劳动节', month=5, day=4),
        Holiday('劳动节', month=5, day=5),
    ]


def is_tiaoxiu_2024(date):
    date1 = datetime(2024, 2, 4)
    date2 = datetime(2024, 2, 18)
    date3 = datetime(2024, 4, 7)
    date4 = datetime(2024, 4, 28)
    date5 = datetime(2024, 5, 11)
    return date == date1 or date == date2 or date == date3 or date == date4 or date == date5