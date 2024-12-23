import networkx as nx
import pandas as pd
import geopandas as gpd
from geopy.distance import geodesic
from shapely.wkt import loads
from shapely.geometry import Point
from multiprocessing import Pool, cpu_count
import numpy as np
import matplotlib.pyplot as plt
from tool import id_home_dict, distance
from data_load import load_town
from shapely.wkt import loads as load_wkt
from shapely.geometry import LineString
import pickle
import random
import matplotlib.colors as mcolors
import json
from shapely import wkt
from shapely.geometry import mapping
import sys
import os
sys.path.append("")
import openmeteo_requests
import requests_cache
from retry_requests import retry


park_shp = gpd.read_file('/data/yuting/Data/shanghai/park/polygon/buffer_50m/395_parks_50m_buffer_+parkid2.shp')
park_shp = park_shp[park_shp['park_id_s'] != 392]
park_shp = park_shp.rename(columns={'area_new': 'area_park'})
park_shp.to_file('/data/yuting/Data/shanghai/park/polygon/buffer_50m/395_parks_50m_buffer_+parkid3.shp', encoding='utf-8')
num_columns2 = len(park_shp.columns)
column_names2 = park_shp.columns.tolist()
print("列数：", num_columns2)
print("列名：", column_names2)

voronoi_shp = gpd.read_file('/data/yuting/Data/shanghai/Stations/Voronoi_visitors/Voronoi_8-4_50mbuffer/Voronoi_shanghai_3_expand.shp')
voronoi_shp = voronoi_shp.rename(columns={'area': 'area_poly'})
# voronoi_shp.to_file('/data/yuting/Data/shanghai/Stations/Voronoi_visitors/Voronoi_8-4_50mbuffer/Voronoi_shanghai_3_expand.shp', encoding='utf-8')
voronoi_shp = voronoi_shp.drop(columns=['PNAI'])
num_columns = len(voronoi_shp.columns)
column_names = voronoi_shp.columns.tolist()
print("列数：", num_columns)
print("列名：", column_names)

sparse_grid = voronoi_shp[voronoi_shp['visitors_d'] <= 0]
dense_grid = voronoi_shp[voronoi_shp['visitors_d'] > 0]
sparse_points = sparse_grid['point']

df = pd.read_csv('/data/yuting/Data/shanghai/park/visit/grid/park_visit_filtered8-4_grid_10m_50mbuffer_holiday.csv')
print('finish read')


# 计算每个grid对应的park_id频次
grid_park_id_counts = df.groupby(['grid_id', 'park_id']).size().unstack(fill_value=0)
# 去掉稀疏grid
grid_park_id_counts_filtered = grid_park_id_counts[~grid_park_id_counts.index.isin(sparse_points)]
# 计算每个grid对应的所有park_id的访问频次总和
grid_total_counts = grid_park_id_counts_filtered.sum(axis=1)
# 每个park_id的访问频次占比
grid_park_id_ratio = grid_park_id_counts_filtered.div(grid_total_counts, axis=0)

ratio_list = grid_park_id_ratio.values.flatten().tolist()
ratio_list = [ratio for ratio in ratio_list if ratio != 0]

'''访问公园频次比例的直方图'''
# plt.figure(figsize=(6, 4))
# plt.hist(ratio_list, bins=20, color='skyblue', edgecolor='black', alpha=0.7)
# plt.xlabel('park flow ratio per grid')
# plt.ylabel('')
#
# plt.grid(True)
# plt.savefig('/data/yuting/code/fig/Voronoi_grid/Graph/park_flow_ratio_per_grid_16-4.png')
# plt.show()


'''grids访问公园数量的直方图'''
# 计算每行中不为0的列数
# non_zero_counts_per_row = (grid_park_id_counts_filtered != 0).sum(axis=1)
# non_zero_counts_list = non_zero_counts_per_row.tolist()
#
# plt.figure(figsize=(6, 4))
# plt.hist(non_zero_counts_list, bins=20, color='skyblue', edgecolor='black', alpha=0.7)
# plt.xlabel('parks visited per grid')
# plt.ylabel('grids')
# plt.grid(True)
# plt.savefig('/data/yuting/code/fig/Voronoi_grid/Graph/parks_visited_per_grid_~1-4.png')
# plt.show()


'''grids访问公园数量 / residents 的直方图'''
# results = pd.Series(index=non_zero_counts_per_row.index, dtype=float)
# # 遍历非零计数，找到相应的voronoi_shp行，并计算比例
# for index, count in non_zero_counts_per_row.items():
#     # 根据point属性匹配行，这假设point属性是唯一的且与索引匹配
#     matched_row = grid_shp[grid_shp['point'] == index]
#     if not matched_row.empty:
#         residents = matched_row.iloc[0]['residents']  # 获取residents值
#         results[index] = count / residents if residents else None  # 计算比例，避免除以零
# result_list = results.tolist()
#
# plt.figure(figsize=(6, 4))
# plt.hist(result_list, bins=20, color='skyblue', edgecolor='black', alpha=0.7)
# plt.xlabel('parks visited / grid,residents(20min)')
# plt.ylabel('grids')
#
# plt.grid(True)
# plt.savefig('/data/yuting/code/fig/SH/Graph/parks_visited_per_grid_per_residents(20min).png')
# plt.show()

def daily_weather():
    '''
    open-metro daily weather
    '''
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 31.2222,
        "longitude": 121.4581,
        "start_date": "2014-01-01",
        "end_date": "2014-04-30",
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                  "precipitation_sum", "precipitation_hours"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(4).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(5).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours

    daily_weather = pd.DataFrame(data=daily_data)
    daily_weather['date'] = pd.to_datetime(daily_weather['date']).dt.date
    print(daily_weather)

    return daily_weather


def grid_park_graph(dense_grid,park_shp,df,weather,save_path):
    # 创建一个空的无向图
    G = nx.Graph()

    # grid节点
    for idx, polygon in dense_grid.iterrows():
        # 获取当前多边形的 GEOID
        geoid = polygon['GEOID']
        # node_name = geoid
        node_name = f"{geoid}grid"
        # 将 GEOID 添加为节点
        G.add_node(node_name)
        # 遍历其他属性列，并将其添加为节点的属性
        for column in dense_grid.columns:
            # 排除 'GEOID' 列
            if column != 'GEOID' and column != 'pop' and column != 'pop_d' and column != 'outflow' and column != 'resid_num' \
                    and column != 'visitors' and column != 'visitors_d' and column != 'users' and column != 'users_d' \
                    and column != 'users_t' and column != 'census_t' and column != 'exp_ratio':
                # 获取属性值并添加为节点的属性
                attribute_value = polygon[column]
                G.nodes[node_name][column] = attribute_value
        G.nodes[node_name]['type'] = 'grid'

    print(G)
    print(len(G))


    # park节点
    df_f = df[df['grid_id'].notnull()]
    df_inflow = df_f.groupby(['park_id'])
    inflow_counts = df_inflow.size().reset_index(name='inflow')
    for idx, polygon in park_shp.iterrows():
        # node_name = idx
        node_name = f"{idx}park"
        G.add_node(node_name)
        # 遍历 'name', 'lng', 'lat', 'area_new', 'park_id_s', 'area_label', 'geometry' 列，并将其添加为节点的属性
        for column in ['name', 'lng', 'lat', 'area_park', 'geometry']:
            # 获取属性值并添加为节点的属性
            attribute_value = polygon[column]
            G.nodes[node_name][column] = attribute_value
        # 添加inflow
        inflow = inflow_counts.loc[inflow_counts['park_id'] == idx, 'inflow']
        if not inflow.empty:
            G.nodes[node_name]['inflow'] = inflow.values[0]
        else:
            G.nodes[node_name]['inflow'] = 0

        # print(G.nodes[node_name]['inflow'])

        if 'type' not in G.nodes[node_name] or G.nodes[node_name]['type'] != 'park':
            G.nodes[node_name]['type'] = 'park'

    print(G)
    print(len(G))

    # grid-park 边
    df_f = df[df['grid_id'].notnull()]
    df_grouped = df_f.groupby(['grid_id', 'park_id'])
    # 统计每个组中 'commuter_label' 为 True 的占比
    commuter_ratio = df_grouped['commuter_label'].mean().reset_index(name='commuter_ratio')
    # 统计每个分组中的行数，即grid-park pairflow
    group_counts = df_grouped.size().reset_index(name='flow')
    # 每个 'grid_id' 下的总流量
    total_flow_per_grid = group_counts.groupby('grid_id')['flow'].sum()
    group_counts['flow_ratio'] = group_counts.apply(lambda row: row['flow'] / total_flow_per_grid[row['grid_id']],
                                                    axis=1)
    distance_data = []
    for name, group in df_grouped:
        first_row = group.iloc[0]  # 获取第一行数据
        station = first_row['station']
        park_x = first_row['park_x']
        park_y = first_row['park_y']
        station_point = loads(station)
        # 计算 stations 和 park 之间的距离
        d = distance(station_point.y, station_point.x, park_y, park_x)
        # distance = geodesic((station_point.y, station_point.x), (park_y, park_x)).meters
        distance_data.append({'grid_id': name[0], 'park_id': name[1], 'distance': d})
    distance_df = pd.DataFrame(distance_data)

    merged_df = pd.merge(group_counts, commuter_ratio, on=['grid_id', 'park_id'], how='inner')
    final_df = pd.merge(merged_df, distance_df, on=['grid_id', 'park_id'], how='inner')

    # 分组、排序和重置索引,flow_ratio从高到低
    df_sorted = final_df.sort_values(by='flow_ratio', ascending=False).reset_index(drop=True)
    # 按照 'grid_id' 和 'park_id' 分组，并计算每组的累积和
    df_sorted['cumulative_ratio'] = df_sorted.groupby(['grid_id', 'park_id'])['flow_ratio'].transform('cumsum')
    # .shift(1) <= 0.5第一个累积和超过50%的行
    df_sorted['include'] = (df_sorted['cumulative_ratio'] <= 1) | (
                df_sorted.groupby(['grid_id', 'park_id'])['cumulative_ratio'].shift(1) <= 1)
    final_df_filtered = df_sorted[df_sorted['include']]

    for _, row in final_df_filtered.iterrows():
        # 获取 'grid_id' 对应的节点名字
        grid_node_name = f"{int(row['grid_id'])}grid"
        # 获取 'park_id' 对应的节点名字
        park_node_name = f"{int(row['park_id'])}park"

        if grid_node_name not in G.nodes or park_node_name not in G.nodes:
            continue

        # flow也等比例/ratio扩大，flow_ratio、commuter_ratio、distance不需要变
        grid_exp_ratio = G.nodes[grid_node_name].get('exp_ratio', None)
        if grid_exp_ratio and grid_exp_ratio != 0:
            exp_flow = row['flow'] / grid_exp_ratio
        else:
            exp_flow = row['flow']  # 如果exp_ratio为0，则说明原flow就是0
        exp_flow = round(exp_flow) # 四舍五入，取整

        # 添加边并设置属性
        G.add_edge(grid_node_name, park_node_name,
                   flow=exp_flow,
                   flow_ratio=row['flow_ratio'],
                   commuter_ratio=row['commuter_ratio'],
                   distance=row['distance'])

    # 移除所有的孤立节点
    isolated_nodes = list(nx.isolates(G))
    G.remove_nodes_from(isolated_nodes)

    with open(save_path, "wb") as f:
        pickle.dump(G, f)
        pickle.dump(weather, f)
    # nx.write_gexf(G, save_path)

    print(G)
    print(len(G))


'''全量 graph'''
# weather = daily_weather()
# save_path = '/data/yuting/Data/shanghai/Stations/Graph/grid_park_graph_exp_8-4_geometry.pkl'
# grid_park_graph(dense_grid,park_shp,df,weather,save_path)


# '''weekday weekend graph'''
# print('week')
# week_group_df = df_filtered.groupby('Day_type')
# for Day_type, week_df in week_group_df:
#     print(len(week_df))
#     save_path = os.path.join('/data/yuting/Data/shanghai/Stations/Graph/week_graph_8-4/', f'{Day_type}.gexf')
#     grid_park_graph(dense_grid,park_shp,week_df,save_path)
#     print(save_path)


'''daily graph'''
print('daily')
weather = daily_weather()
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df['date'] = df['date'].dt.date
daily_group_df = df.groupby('date')
for date, daily_df in daily_group_df:
    print(len(daily_df))
    weather_d = weather[weather['date'] == date].iloc[0]
    weather_d = weather_d.drop('date').to_dict()
    save_path = os.path.join('/data/yuting/Data/shanghai/Stations/Graph/daily_graph_exp_8-4_geometry/', f'{date}.pkl')
    grid_park_graph(dense_grid,park_shp,daily_df,weather_d,save_path)
    print(save_path)


def graph_to_geojson(G, save_path):
    features = []
    # 转换节点
    for node in G.nodes(data=True):
        node_name, data = node
        if data['type'] == 'grid':
            point_str = data['point']
            lon1, lat1 = map(float, point_str.replace('POINT (', '').replace(')', '').split())
            geojson_geometry1 = {
                "type": "Point",
                "coordinates": [lon1, lat1]
            }
            features.append({
                "type": "Feature",
                "geometry": geojson_geometry1,
                # "properties": {key: value for key, value in data.items() if key != 'point'}
            })

        if data['type'] == 'park':
            lon2 = data['lng']
            lat2 = data['lat']
            # geojson_geometry2 = {
            #     "type": "Point",
            #     "coordinates": [lon2, lat2]
            # }
            # print(data['geometry'])

            geometry2 = data['geometry']
            # 转换为 GeoJSON 格式
            geojson_geometry2 = mapping(geometry2)
            # print(geojson_geometry2)
            features.append({
                "type": "Feature",
                "geometry": geojson_geometry2,
                "properties": {key: int(value) for key, value in data.items() if key=='inflow'}
            })

    for edge in G.edges(data=True):
        u, v, data = edge
        point_str = G.nodes[u]['point']
        lon11, lat11 = map(float, point_str.replace('POINT (', '').replace(')', '').split())
        lon22 = G.nodes[v]['lng']
        lat22 = G.nodes[v]['lat']
        geojson_geometry_line = {
            "type": "LineString",
            "coordinates": [
                [lon11, lat11],
                [lon22, lat22]
            ]
        }

        height = data['flow']
        source_target_property = {
        "source_lat": lat11,
        "source_lng": lon11,
        "target_lat": lat22,
        "target_lng": lon22,
        "elevation": 100
        }
        properties = {**source_target_property, **{key: value for key, value in data.items()}}
        features.append({
            "type": "Feature",
            "geometry": geojson_geometry_line,
            # "properties": {key: value for key, value in data.items()}
            "properties": properties
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(save_path, 'w') as f:
        json.dump(geojson, f, indent=4)

# with open('/data/yuting/Data/shanghai/Stations/Graph/grid_park_graph_exp_8-4_geometry.pkl', "rb") as f:
#     G = pickle.load(f)
# print(G)
# print(len(G))
# graph_to_geojson(G, '/data/yuting/Data/shanghai/Stations/Graph/geojson/exp_8-4.geojson')



#
# # 可视化
# # pos = nx.spring_layout(G)  # 选择布局算法
# # nx.draw(G, pos, with_labels=True, node_size=700)  # 绘制图形
# # plt.show()  # 显示图形
# #
# # # 根据节点的 'type' 属性绘制图形
# # pos = nx.spring_layout(G)  # 选择布局算法
# # # 获取节点的 'type' 属性
# # node_type = nx.get_node_attributes(G, 'type')
# # # 绘制不同类型的节点
# # nx.draw_networkx_nodes(G, pos, nodelist=[node for node, typ in node_type.items() if typ == 'grid'], node_color='red', node_size=700)
# # nx.draw_networkx_nodes(G, pos, nodelist=[node for node, typ in node_type.items() if typ == 'park'], node_color='blue', node_size=700)
# # # 绘制边
# # nx.draw_networkx_edges(G, pos)
# # # 显示标签
# # nx.draw_networkx_labels(G, pos)
# # plt.show()
#
#
# shanghai_town = load_town('/data/yuting/Data/shanghai/shanghai_town/shanghai_town.shp')
# grid_shp = gpd.read_file('/data/yuting/Data/shanghai/Stations/Voronoi_50mbuffer_new/Voronoi_shanghai.shp')
# grid_shp = grid_shp.to_crs(epsg=4326)
#
# '''
# 绘制所有连边
# '''
# lines = []
# flows = []
# for node1, node2, attr in G.edges(data=True):
#     # 提取节点的经纬度信息
#     node_attrs1 = G.nodes[node1]
#     node_type1 = node_attrs1['type']
#     node_attrs2 = G.nodes[node2]
#     node_type2 = node_attrs2['type']
#
#     point_str = node_attrs1['point']
#     lon1, lat1 = map(float, point_str.replace('POINT (', '').replace(')', '').split())
#     lon2 = node_attrs2['lng']
#     lat2 = node_attrs2['lat']
#
#     # 创建LineString并添加到列表中
#     line = LineString([(lon1, lat1), (lon2, lat2)])
#     lines.append(line)
#     # 之后需要根据flow来绘制热力线条
#     flows.append(attr['flow'])
#
# # 创建线条的 GeoDataFrame
# lines_gdf = gpd.GeoDataFrame(geometry=lines, crs='EPSG:4326')
#
# # 根据流量创建归一化颜色映射
# norm = mcolors.Normalize(vmin=min(flows), vmax=max(flows), clip=True)
# mapper = plt.cm.ScalarMappable(norm=norm, cmap='viridis')
#
# fig, ax = plt.subplots()
# grid_shp.plot(ax=ax, color='lightgray')
#
# # 首先绘制大值的线条
# for line, flow in sorted(zip(lines, flows), key=lambda x: x[1], reverse=True):
#     plt.plot(*line.xy, color=mapper.to_rgba(flow), linewidth=0.00001)
#
# plt.colorbar(mapper, ax=ax, label='Flow')
# plt.axis('off')
#
# plt.savefig('/data/yuting/code/fig/Voronoi_grid/Graph/graph_~1-4.png', dpi=300)
# plt.show()




'''
绘制单个grid的边
'''
# lines = []
# flows = []
# grid_nodes = [node for node, data in G.nodes(data=True) if data.get('type') == 'grid']
# if grid_nodes:
#     # 从 'grid_nodes' 中随机选择一个节点
#     random_grid_node = random.choice(grid_nodes)
#
# for edge in G.edges(random_grid_node):
#     node1, node2 = edge
#     edge_data = G.get_edge_data(node1, node2)
#
#     point_str = G.nodes[node1]['point']
#     lon1, lat1 = map(float, point_str.replace('POINT (', '').replace(')', '').split())
#     lon2 = G.nodes[node2]['lng']
#     lat2 = G.nodes[node2]['lat']
#
#     # 创建LineString并添加到列表中
#     line = LineString([(lon1, lat1), (lon2, lat2)])
#     lines.append(line)
#     # 之后需要根据flow来绘制热力线条
#     flows.append(edge_data.get('flow'))
#
# # 创建线条的 GeoDataFrame
# lines_gdf = gpd.GeoDataFrame(geometry=lines, crs='EPSG:4326')
#
# # 根据流量创建归一化颜色映射
# norm = mcolors.Normalize(vmin=min(flows), vmax=max(flows), clip=True)
# mapper = plt.cm.ScalarMappable(norm=norm, cmap='viridis')
#
# fig, ax = plt.subplots()
# grid_shp.plot(ax=ax, color='lightgray')
#
# # 首先绘制大值的线条
# for line, flow in sorted(zip(lines, flows), key=lambda x: x[1], reverse=True):
#     plt.plot(*line.xy, color=mapper.to_rgba(flow), linewidth=0.7)
#
# plt.colorbar(mapper, ax=ax, label='Flow')
# plt.axis('off')
# plt.savefig('/data/yuting/code/fig/Voronoi_grid/Graph/random/graph_random9~1-4.png', dpi=300)
# plt.show()