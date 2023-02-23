import pandas as pd


def XGBoost_engineer():
    jan = pd.concat([pd.read_feather('../data/preprocessed_data_regression/delayed_01.feather'),pd.read_feather('../data/preprocessed_data_regression/delayed_02.feather')])
    #arr_weather = harvest_weather_data(jan[['dest_city_name','fl_date']],save=True,prefix='regression_inner_',rotate=True)
    arr_weather = pd.read_csv('../data/weather/regression_inner_weather_data.csv')
    dep_weather = pd.read_csv('../data/weather/regression_inner_dep_weather_data.csv')
    arr_weather.columns = ['Unnamed: 0','arr_weather']
    arr_weather = light_clean(arr_weather.arr_weather)
    dep_weather = light_clean(dep_weather.weather)
    dep_weather.rename('dep_weather', inplace=True)
    jan = pd.concat([jan.reset_index(drop=True),arr_weather.reset_index(drop=True), dep_weather.reset_index(drop=True)],axis=1)
    #jan.drop('Unnamed: 0', axis=1, inplace=True)
    carrier_stats = pd.read_csv('../data/raw_regression_passenger/carrier_passenger_stats.csv')
    jan = pd.merge(jan,carrier_stats,left_on='op_unique_carrier',right_on='unique_carrier')
    jan.drop('unique_carrier',axis=1,inplace=True)
    jan['rush_hour'] = jan.crs_arr_time.transform(lambda x: (x > 5) and (x < 20)).map({True:1,False:0})
    jan['rush_hour_dep'] = jan.crs_dep_time.transform(lambda x: (x > 5) and (x < 20)).map({True:1,False:0})
    origin_delay = jan.groupby(by='origin').arr_delay.mean()
    dest_delay = jan.groupby(by='dest').total_delay.mean()
    jan = pd.merge(jan,origin_delay,left_on='origin',right_on=origin_delay.index)
    jan = pd.merge(jan,dest_delay,left_on='dest',right_on=dest_delay.index)
    flight_time = pd.read_csv('../data/raw_regression_passenger/carrier_avg_flight_time.csv')
    jan = pd.merge(jan,flight_time,left_on='op_unique_carrier',right_on='op_unique_carrier')
    jan.columns = ['fl_date', 'op_unique_carrier', 'origin', 'dest', 'origin_city_name',
        'dest_city_name', 'crs_dep_time', 'crs_arr_time', 'crs_elapsed_time',
        'arr_delay', 'dep_delay', 'arr_weather', 'dep_weather',
        'avg_occupancy', 'avg_passengers', 'avg_seats', 'rush_hour',
        'rush_hour_dep', 'arr_delay_avg', 'dest_delay_avg', 'avg_carrier_speed']