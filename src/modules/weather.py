import requests
import pandas as pd
from os import environ

url = 'https://api.worldweatheronline.com/premium/v1/past-weather.ashx'

key = environ['WORLD_WEATHER_API']

estimated_response_time = 0.4


def harvest_weather_data(df,save = False, prefix ='tmp'):
    '''
    Uses world weather online API to get daily weather descriptor for date and city.
        Parameters:
            df (pd.DataFrame()) : must only contain two columns, city and date (respectively).
            save (bool) : set true to save series as csv in data/weather before returning.
            prefix (str) : prefix to add to saved file name. Default is 'tmp'
        Returns:
            Pandas Series indexed to match dataframe that was used to call this function.
        
    '''
    check_weather(df)
    distinct_df = df.drop_duplicates()
    distinct_df.columns = ['city','date']
    dates = distinct_df.date.values
    cities = distinct_df.city.values

    print('number of requests:',len(dates))
    print('ESTIMATED PROCESSING TIME:',estimated_response_time * len(dates),'seconds')
    if estimated_response_time * len(dates) > 1800: print('Better grab a coffee...')

    weather_df = pd.DataFrame(columns=['city','date','weather'])
    for i in range(len(dates)):
        params = {
        'key' : key,
        'date' : dates[i],
        'q' : cities[i],
        'tp' : '24',
        'format':'json'
        }
        response = requests.get(url=url,params=params).json()

        try: weather = response['data']['weather'][0]['hourly'][0]['weatherDesc'][0]['value']
        except:
            print("API DAILY LIMIT HIT\n~BREAKING AND SAVING~")
            save = True
            break

        tmp = {
            'city':cities[i],
            'date':dates[i],
            'weather': weather
        }

        weather_df = pd.concat([weather_df,pd.DataFrame([tmp])])
    
    merged_df = pd.merge(df,weather_df,left_on=['origin_city_name','fl_date'],right_on=['city','date'],how='outer')

    if save: merged_df.weather.to_csv('../data/weather/'+prefix+'weather_data.csv')

    return merged_df.weather

def check_weather(df):
    assert len(df) != 0, 'Do not call function with an empty dataframe. \ntry "harvest_weather_data(df[["origin_city_name","fl_date"]])'
    assert df.columns[0] == 'origin_city_name', 'first column in df needs to be "origin_city_name"'
    assert df.columns[1] == 'fl_date', 'second column in df needs to be "fl_date"'

def clean_weather(series):
    series = series.str.lower()
    series[series.str.contains('snow')] = 'snow'
    series[series.str.contains('freezing')] = 'snow'
    series[series.str.contains('sleet')] = 'snow'
    series[series.str.contains('overcast')] = 'cloudy'
    series[series.str.contains('partly cloudy')] = 'sunny'
    series[series.str.contains('rain')] = 'rain'
    series[series.str.contains('drizzle')] = 'rain'
    series[series.str.contains('blizzard')] = 'snow'
    series[series.str.contains('ice')] = 'snow'
    series[series.str.contains('fog')] = 'rain'
    series[series.str.contains('mist')] = 'rain'

    return series
