import requests
import pandas as pd
from os import environ
import time
url = 'https://api.worldweatheronline.com/premium/v1/past-weather.ashx'

estimated_response_time = 0.64


def harvest_weather_data(df,save = False, prefix ='tmp',start_at = 0, env_key = 'WORLD_WEATHER_API', rotate = False, additional_keys=['WORLD_WEATHER_API7','WORLD_WEATHER_API3','WORLD_WEATHER_API4','WORLD_WEATHER_API5','WORLD_WEATHER_API6','WORLD_WEATHER_API2'], return_clean=True):
    '''
    Uses world weather online API to get daily weather descriptor for date and city.
        Parameters:
            df (pd.DataFrame()) : Must only contain two columns, city and date (respectively).
            save (bool) : Set true to save series as csv in data/weather before returning.
            prefix (str) : Prefix to add to saved file name. Default is 'tmp'.
            start_at (int) : Will start request at x row. Use when doing many requests that hit daily API limit.
            env_key (str) : The name of your environment variable for this API. Default is 'WORLD_WEATHER_API'.
            rotate (bool) : If true, instead of breaking if you max out a key, switch to another env key and continue.
            additional_keys (lst(str)) : The names of your other API keys.
            return_clean (bool) : Run through cleaner (clean_weather()) before returning.
        Returns:
            Pandas Series indexed to match dataframe that was used to call this function.
        
    '''
    key = environ[env_key]
    check_weather(df)
    distinct_df = df.drop_duplicates()
    distinct_df.columns = ['city','date']
    df.columns = ['city','date']
    dates = distinct_df.date
    cities = distinct_df.city.value_counts().index
    key_num = -1
    break_for = False
    print(df.columns,distinct_df.columns)

    print('number of requests:',len(cities))
    print('Estimated execution time:',estimated_response_time * len(cities),'seconds')
    if estimated_response_time * len(cities) > 1800: print('Better grab a coffee...')
    start_date = dates.value_counts().sort_index().index[0]
    end_date = dates.value_counts().sort_index().index[-1]

    weather_df = pd.DataFrame(columns=['city','date','weather'])
    for i in range(start_at,len(cities)):
        if(rotate):
            loop = True
            while loop:
                params = {
                'key' : key,
                'date' : start_date,
                'enddate':end_date,
                'q' : cities[i],
                'tp' : '24',
                'format':'json'
                }
                while(True):
                    try:
                        response = requests.get(url=url,params=params).json()
                        break
                    except: 'JSON DECODE ERROR'
                

                try: 
                    for resp in response['data']['weather']:
                        tmp = {
                            'city':cities[i],
                            'date':resp['date'],
                            'weather':resp['hourly'][0]['weatherDesc'][0]['value']
                        }
                        weather_df = pd.concat([weather_df,pd.DataFrame([tmp])])
                    loop = False
                except:
                    if key_num +1 < len(additional_keys):
                        print(f"KEY NUMBER {key_num+1} MAXED OUT\nTRYING NEXT KEY...")
                        key_num+=1
                        key = environ[additional_keys[key_num]]
                        time.sleep(2)
                    else:
                        print(f"ALL KEYS MAXED AT LINE {i} \n~BREAKING AND SAVING~")
                        save = True
                        break_for = True
                        loop = False
        else:
            params = {
            'key' : key,
            'date' : start_date,
            'enddate':end_date,
            'q' : cities[i],
            'tp' : '24',
            'format':'json'
            }
            while(True):
                try:
                    response = requests.get(url=url,params=params).json()
                    break
                except: 'JSON DECODE ERROR'

            try: 
                for resp in response['data']['weather']:
                    tmp = {
                        'city':cities[i],
                        'date':resp['date'],
                        'weather':resp['hourly'][0]['weatherDesc'][0]['value']
                    }
                    weather_df = pd.concat([weather_df,pd.DataFrame([tmp])])
            except:
                print(f"API DAILY LIMIT HIT AT LINE {i} \n~BREAKING AND SAVING~")
                save = True
                break

        if break_for: break
    
    merged_df = pd.merge(df,weather_df,left_on=['city','date'],right_on=['city','date'])

    if save: merged_df.weather.to_csv('../data/weather/'+prefix+'weather_data.csv')

    if return_clean: return clean_weather(merged_df.weather)
    else: return merged_df.weather

    


def check_weather(df):
    assert len(df) != 0, 'Do not call function with an empty dataframe. \ntry "harvest_weather_data(df[["origin_city_name","fl_date"]])'
    assert (df.columns[0] == 'origin_city_name' or df.columns[0] == 'dest_city_name'), 'first column in df needs to be "origin_city_name" or "dest_city_name"'
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

def light_clean(series):
    series = series.str.lower()
    series[series.str.contains('moderate or heavy snow showers')] = 'heavy snow'
    series[series.str.contains('mist')] = 'light drizzle'
    series[series.str.contains('patchy light rain with thunder')] = 'light rain'
    series[series.str.contains('moderate or heavy snow with thunder')] = 'heavy snow'
    series[series.str.contains('moderate or heavy sleet showers')] = 'heavy snow'
    series[series.str.contains('patchy light snow with thunder')] = 'light snow'
    series[series.str.contains('light sleet showers')] = 'light snow'
    series[series.str.contains('blizzard')] = 'heavy snow'
    series[series.str.contains('ice pellets')] = 'moderate snow'
    series[series.str.contains('freezing fog')] = 'moderate snow'
    series[series.str.contains('patchy light snow with thunder')] = 'light snow'
    series[series.str.contains('moderate or heavy showers of ice pellets')] = 'heavy snow'
    series[series.str.contains('patchy sleet possible')] = 'light snow'
    series[series.str.contains('heavy freezing drizzle')] = 'heavy snow'
    series[series.str.contains('moderate or heavy sleet')] = 'heavy snow'
    series[series.str.contains('moderate or heavy rain at times')] = 'moderate rain'
    series[series.str.contains('light snow showers')] = 'light snow'
    series[series.str.contains('light freezing rain')] = 'light snow'
    series[series.str.contains('patchy snow possible')] = 'light snow'
    series[series.str.contains('patchy heavy snow')] = 'moderate snow'
    series[series.str.contains('torrential rain shower')] = 'heavy rain'
    series[series.str.contains('blowing snow')] = 'moderate snow'
    series[series.str.contains('moderate or heavy freezing rain')] = 'heavy snow'
    series[series.str.contains('moderate rain at times')] = 'moderate rain'
    series[series.str.contains('light sleet')] = 'light snow'
    series[series.str.contains('patchy moderate snow')] = 'moderate snow'
    series[series.str.contains('moderate or heavy rain shower')] = 'heavy rain'
    series[series.str.contains('fog')] = 'moderate rain'
    series[series.str.contains('patchy light snow')] = 'light snow'
    series[series.str.contains('patchy light rain')] = 'light rain'
    series[series.str.contains('patchy light drizzle')] = 'light rain'
    series[series.str.contains('moderate or heavy rain with thunder')] = 'heavy rain'
    return series
