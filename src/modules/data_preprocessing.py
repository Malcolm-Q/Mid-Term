import glob
import pandas as pd
import numpy as np
import importlib.util

def clean_raw(overwrite = False, feather = True, strip = True, caps = True,take_hour = True, day_of_week=True, week_int = True, one_hot = False, drop = tuple()):
    '''
    Cleans CSVs in 'data/raw_data' and converts to feather.

        Parameters:
            overwrite (bool): overwrite data in '/preprocessed_data'.
            feather (bool): use .feather files, otherwise use CSVs.
            strip (bool): use .strip() on objects.
            caps (bool): use .upper() on objects for uniform values.
            take_hour (bool): only keep the hour from times.
            day_of_week (bool): only keep dt.dayofweek from dates.
            week_int (bool): If True, dayofweek is numeric.
            one_hot (bool): call pd.get_dummies() EXPENSIVE
            drop (tuple): clean_raw(drop=(1,2)) will drop the 2nd and 3rd column.
        Returns:
            status (str): progress updates.
            (Saves cleaned files in data/preprocessed_data)
    '''
    all_raw = glob.glob('../data/raw_data/*')
    check_clean(all_raw,overwrite,feather)

    for i,raw_file in enumerate(all_raw):
        cleaner = pd.read_csv(raw_file)
        # convert times to just hours
        if(take_hour):
            cleaner.crs_dep_time = cleaner.crs_dep_time.apply(get_hour)
            cleaner.crs_arr_time = cleaner.crs_arr_time.apply(get_hour)

        # date to dow
        if(day_of_week):
            if(week_int):
                cleaner.fl_date = pd.to_datetime(cleaner['fl_date']).dt.dayofweek
            else:
                cleaner.fl_date = pd.to_datetime(cleaner['fl_date']).dt.day_name
        
        # Clean and make all objects upper case
        if(strip):
            for col in cleaner.select_dtypes('object'):
                cleaner[col] = cleaner[col].str.strip()
                if(caps): cleaner[col] = cleaner[col].str.upper()

        # This will add many many dimensions. Only use if nescessary.
        if(one_hot):
            cleaner = pd.get_dummies(cleaner,dtype=np.int8)
        
        # drop specified columns
        if(len(drop) > 0):
            for col in drop:
                cleaner.drop(cleaner.columns[col],inplace=True,axis=1)
        

        


        # Save file and format name
        if(i > 11):
            month = str(i - 11)
            if(len(month) != 2): month = '0' + month
            if feather: cleaner.to_feather('../data/preprocessed_data/on_time_'+month+'.feather')
            else: cleaner.to_csv('../data/preprocessed_data/on_time_'+month+'.csv')
            print('on time flights for month:',month)
        else:
            month = str(i + 1)
            if(len(month) != 2): month = '0' + month
            if feather: cleaner.to_feather('../data/preprocessed_data/delayed_'+month+'.feather')
            else: cleaner.to_csv('../data/preprocessed_data/delayed_'+month+'.csv')
            print('delayed flights for month:',month)
    del all_raw
    return 'complete'

def loader(mega_df=False, joined = False, feather = True, months = list()):
    '''
    Loads feather/csv files from 'data/preprocessed_data'
        Parameters:
            mega_df (bool): Return one dataframe of every single concatenated file.
                (2,400,000, 13) uses ~225 mb
            joined (bool): Returns dataframe(s) for given month(s) where delayed and on time flights are concatenated (axis=0)
            months (list): specify months you want. (1,3,4) gets you Jan, Mar, Apr.
        Returns:
            Dataframe,
            list of dataframes,
            (If no params are passed it will return list of 12 classes containing the delayed and on time flight dfs for that month).
    '''
    month_names = ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec')
    joined_dfs = list()
    unjoined_dfs = list()
    all_data = glob.glob('../data/preprocessed_data/*')
    check_loader(all_data,months,feather)
    if(feather):
        if(len(months) > 0):
            if(joined):
                for val in months:
                    joined_dfs.append(pd.concat([pd.read_feather(all_data[val-1]),pd.read_feather(all_data[val+11])]))
                if mega_df: return pd.concat(joined_dfs)
                else: return joined_dfs

            elif(mega_df):
                for val in months:
                    unjoined_dfs.extend([pd.read_feather(all_data[val-1]),pd.read_feather(all_data[val+11])])
                return pd.concat(unjoined_dfs)
            else:
                if(len(months) == 1): return Month_data(month_names[months[0]-1],all_data[months[0]-1],all_data[months[0]+11])
                for val in months:
                    unjoined_dfs.append(Month_data(month_names[val-1],all_data[val-1],all_data[val+11]))
                return unjoined_dfs
        elif joined:
            for val in range(12):
                    joined_dfs.append(pd.concat([pd.read_feather(all_data[val]),pd.read_feather(all_data[val+12])]))
            if mega_df: return pd.concat(joined_dfs)
            else: return joined_dfs
        elif(mega_df):
            for val in range(12):
                unjoined_dfs.extend([pd.read_feather(all_data[val]),pd.read_feather(all_data[val+12])])
            return pd.concat(unjoined_dfs)
        else:
            for val in range(12):
                unjoined_dfs.append(Month_data(month_names[val],all_data[val],all_data[val+12]))
            return unjoined_dfs
    else:
        if(len(months) > 0):
            if(joined):
                for val in months:
                    joined_dfs.append(pd.concat([pd.read_csv(all_data[val-1]),pd.read_csv(all_data[val+11])]))
                if mega_df: return pd.concat(joined_dfs)
                else: return joined_dfs

            elif(mega_df):
                for val in months:
                    unjoined_dfs.extend([pd.read_csv(all_data[val-1]),pd.read_csv(all_data[val+11])])
                return pd.concat(unjoined_dfs)
            else:
                if(len(months) == 1): return Month_data_csv(month_names[months[0]-1],all_data[months[0]-1],all_data[months[0]+11])
                for val in months:
                    unjoined_dfs.append(Month_data_csv(month_names[val-1],all_data[val-1],all_data[val+11]))
                return unjoined_dfs
        elif joined:
            for val in range(12):
                    joined_dfs.append(pd.concat([pd.read_csv(all_data[val]),pd.read_csv(all_data[val+12])]))
            if mega_df: return pd.concat(joined_dfs)
            else: return joined_dfs
        elif(mega_df):
            for val in range(12):
                unjoined_dfs.extend([pd.read_csv(all_data[val]),pd.read_csv(all_data[val+12])])
            return pd.concat(unjoined_dfs)
        else:
            for val in range(12):
                unjoined_dfs.append(Month_data_csv(month_names[val],all_data[val],all_data[val+12]))
            return unjoined_dfs

########################################################################################################################
'                                        DEPENDENT FUNCTIONS AND CLASSES                                               '
########################################################################################################################
class Month_data:
    def __init__(self, name,delayed,on_time):
        self.name = name
        self.delayed = pd.read_feather(delayed)
        self.on_time = pd.read_feather(on_time)

class Month_data_csv:
    def __init__(self, name,delayed,on_time):
        self.name = name
        self.delayed = pd.read_csv(delayed)
        self.on_time = pd.read_csv(on_time)    

def check_loader(all_data,months,feather):
    assert len(all_data) > 0, 'NO DATA! call data_preprocessing.clean_raw()'
    try:
        if(len(months) > 0):
            for val in months:
                assert (val < 13 and val > 0), 'Only values between 1-12 should be passed in months=list()'
    except: assert 1==2,'month parameter needs to be type list()'
    if(feather): assert all_data[0][-1] == 'r','File is not .feather did you mean to pass feather = False?'
    else: assert all_data[0][-1] != 'r', 'File is not .csv did you mean to pass feather = True?'

def check_clean(all_raw, overwrite,feather):
    if feather:
        spec = importlib.util.find_spec('pyarrow')
        assert spec is not None, "pyarrow is not installed, Conda install pyarrow or use 'feather=False'"

    test = glob.glob('../data/preprocessed_data/*')
    if len(test) != 0:
        assert overwrite, "data already exists (../data/preprocessed_data) use 'overwrite=True' to overwrite"

    assert len(all_raw) == 24,'raw data files have been tampered with. There should be 24.'

def get_hour(n):
    if(len(str(n))) == 4:
        return np.int8(str(n)[:2])
    else: return np.int8(str(n)[0])