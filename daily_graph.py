import networkx as nx
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads
from tool import id_home_dict, distance
import pickle
import json
from shapely.geometry import mapping
import os
import openmeteo_requests
import requests_cache
from retry_requests import retry


park_shp = gpd.read_file('parks_50m_buffer_+parkid3.shp')
num_columns2 = len(park_shp.columns)
column_names2 = park_shp.columns.tolist()
print(num_columns2)
print(column_names2)

voronoi_shp = gpd.read_file('Voronoi_shanghai_3_expand.shp')
num_columns = len(voronoi_shp.columns)
column_names = voronoi_shp.columns.tolist()
print(num_columns)
print(column_names)

sparse_polygon = voronoi_shp[voronoi_shp['visitors_d'] <= 0]
dense_polygon = voronoi_shp[voronoi_shp['visitors_d'] > 0]
sparse_points = sparse_polygon['point']

df = pd.read_csv('park_visit_filtered8-4_grid_10m_50mbuffer_holiday.csv')
print('finish read')


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


def polygon_park_graph(dense_poly, park_shp, df, weather, save_path):
    G = nx.Graph()

    # polygon node
    for idx, polygon in dense_poly.iterrows():
        geoid = polygon['GEOID']
        # node_name = GEOID
        node_name = f"{geoid}poly"
        G.add_node(node_name)
        for column in dense_poly.columns:
            if column != 'GEOID' and column != 'pop' and column != 'pop_d' and column != 'outflow' and column != 'resid_num' \
                    and column != 'visitors' and column != 'visitors_d' and column != 'users' and column != 'users_d' \
                    and column != 'users_t' and column != 'census_t' and column != 'exp_ratio':
                attribute_value = polygon[column]
                G.nodes[node_name][column] = attribute_value
        G.nodes[node_name]['type'] = 'poly'

    # park node
    df_f = df[df['grid_id'].notnull()]
    df_inflow = df_f.groupby(['park_id'])
    inflow_counts = df_inflow.size().reset_index(name='inflow')
    for idx, polygon in park_shp.iterrows():
        # node_name = idx
        node_name = f"{idx}park"
        G.add_node(node_name)
        for column in ['name', 'lng', 'lat', 'area_park', 'geometry']:
            attribute_value = polygon[column]
            G.nodes[node_name][column] = attribute_value
        inflow = inflow_counts.loc[inflow_counts['park_id'] == idx, 'inflow']
        if not inflow.empty:
            G.nodes[node_name]['inflow'] = inflow.values[0]
        else:
            G.nodes[node_name]['inflow'] = 0
        # print(G.nodes[node_name]['inflow'])

        if 'type' not in G.nodes[node_name] or G.nodes[node_name]['type'] != 'park':
            G.nodes[node_name]['type'] = 'park'

    # print(G)
    # print(len(G))

    # polygon-park edge
    df_f = df[df['grid_id'].notnull()]
    df_grouped = df_f.groupby(['grid_id', 'park_id'])
    # ratio of commuter
    commuter_ratio = df_grouped['commuter_label'].mean().reset_index(name='commuter_ratio')
    # polygon-park pairflow
    group_counts = df_grouped.size().reset_index(name='flow')
    # total outflow per polygon
    total_flow_per_poly = group_counts.groupby('grid_id')['flow'].sum()
    group_counts['flow_ratio'] = group_counts.apply(lambda row: row['flow'] / total_flow_per_poly[row['grid_id']],
                                                    axis=1)
    distance_data = []
    for name, group in df_grouped:
        first_row = group.iloc[0]
        station = first_row['station']
        park_x = first_row['park_x']
        park_y = first_row['park_y']
        station_point = loads(station)
        # distance between polygon(cell tower) and park
        d = distance(station_point.y, station_point.x, park_y, park_x)
        # distance = geodesic((station_point.y, station_point.x), (park_y, park_x)).meters
        distance_data.append({'grid_id': name[0], 'park_id': name[1], 'distance': d})
    distance_df = pd.DataFrame(distance_data)

    merged_df = pd.merge(group_counts, commuter_ratio, on=['grid_id', 'park_id'], how='inner')
    final_df = pd.merge(merged_df, distance_df, on=['grid_id', 'park_id'], how='inner')

    df_sorted = final_df.sort_values(by='flow_ratio', ascending=False).reset_index(drop=True)
    df_sorted['cumulative_ratio'] = df_sorted.groupby(['grid_id', 'park_id'])['flow_ratio'].transform('cumsum')
    df_sorted['include'] = (df_sorted['cumulative_ratio'] <= 1) | (
                df_sorted.groupby(['grid_id', 'park_id'])['cumulative_ratio'].shift(1) <= 1)
    final_df_filtered = df_sorted[df_sorted['include']]

    for _, row in final_df_filtered.iterrows():
        poly_node_name = f"{int(row['grid_id'])}poly"
        park_node_name = f"{int(row['park_id'])}park"

        if poly_node_name not in G.nodes or park_node_name not in G.nodes:
            continue

        # scale up pairflow
        poly_exp_ratio = G.nodes[poly_node_name].get('exp_ratio', None)
        if poly_exp_ratio and poly_exp_ratio != 0:
            exp_flow = row['flow'] / poly_exp_ratio
        else:
            exp_flow = row['flow']
        exp_flow = round(exp_flow)

        G.add_edge(poly_node_name, park_node_name,
                   flow=exp_flow,
                   flow_ratio=row['flow_ratio'],
                   commuter_ratio=row['commuter_ratio'],
                   distance=row['distance'])

    # remove all isolated nodes
    isolated_nodes = list(nx.isolates(G))
    G.remove_nodes_from(isolated_nodes)

    with open(save_path, "wb") as f:
        pickle.dump(G, f)
        pickle.dump(weather, f)
    # nx.write_gexf(G, save_path)

    print(G)
    print(len(G))



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
    polygon_park_graph(dense_polygon, park_shp, daily_df, weather_d, save_path)
    print(save_path)
