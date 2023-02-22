import requests
import pandas as pd
from os import environ

url = 'https://api.worldweatheronline.com/premium/v1/past-weather.ashx'

estimated_response_time = 0.4


def harvest_weather_data(df,save = False, prefix ='tmp',start_at = 0, env_key = 'WORLD_WEATHER_API', rotate = False, additional_keys=['WORLD_WEATHER_API2','WORLD_WEATHER_API3','WORLD_WEATHER_API4'], return_clean=True):
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
    dates = distinct_df.date.values
    cities = distinct_df.city.values
    key_num = -1
    break_for = False

    print('number of requests:',len(dates))
    print('Estimated execution time:',estimated_response_time * len(dates),'seconds')
    if estimated_response_time * len(dates) > 1800: print('Better grab a coffee...')

    weather_df = pd.DataFrame(columns=['city','date','weather'])
    for i in range(start_at,len(dates)):
        if(rotate):
            loop = True
            while loop:
                params = {
                'key' : key,
                'date' : dates[i],
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
                    weather = response['data']['weather'][0]['hourly'][0]['weatherDesc'][0]['value']
                    tmp = {
                    'city':cities[i],
                    'date':dates[i],
                    'weather': weather
                    }
                    weather_df = pd.concat([weather_df,pd.DataFrame([tmp])])
                    loop = False
                except:
                    if key_num +1 < len(additional_keys):
                        print(f"KEY NUMBER {key_num} MAXED OUT\nTRYING NEXT KEY...")
                        key_num+=1
                        key = environ[additional_keys[key_num]]
                    else:
                        print(f"ALL KEYS MAXED AT LINE {i+1} \n~BREAKING AND SAVING~")
                        save = True
                        break_for = True
                        loop = False
        else:
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
                print(f"API DAILY LIMIT HIT AT LINE {i} \n~BREAKING AND SAVING~")
                save = True
                break

            tmp = {
                'city':cities[i],
                'date':dates[i],
                'weather': weather
            }

            weather_df = pd.concat([weather_df,pd.DataFrame([tmp])])
        if break_for: break
    
    merged_df = pd.merge(df,weather_df,left_on=['city','date'],right_on=['city','date'],how='outer')

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
