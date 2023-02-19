import glob
import pandas as pd
import numpy as np
import importlib.util
def clean_raw(overwrite = False, feather = True):
    '''
    Cleans CSVs in 'data/raw_data' and converts to feather.

        Parameters:
            overwrite (bool): overwrite data in '/preprocessed_data'.
            feather (bool): use .feather files, otherwise use CSVs.
            
        Returns:
            status (str): progress updates.
            (Saves cleaned files in data/preprocessed_data)
    '''
    all_raw = glob.glob('../data/raw_data/*')
    check_clean(all_raw,overwrite,feather)
    for i,raw_file in enumerate(all_raw):
        cleaner = pd.read_csv(raw_file)
        # convert times to just hours
        cleaner.loc[cleaner.crs_dep_time.astype(str).str.len() < 4,'crs_dep_time'].transform(lambda x: x.astype(str).str[0])
        cleaner.loc[cleaner.crs_dep_time.astype(str).str.len() == 4,'crs_dep_time'].transform(lambda x: x.astype(str).str[0:2])
        cleaner.crs_dep_time = cleaner.crs_dep_time.astype(np.int8)

        cleaner.loc[cleaner.crs_dep_time.astype(str).str.len() < 4,'crs_arr_time'].transform(lambda x: x.astype(str).str[0])
        cleaner.loc[cleaner.crs_dep_time.astype(str).str.len() == 4,'crs_arr_time'].transform(lambda x: x.astype(str).str[0:2])
        cleaner.crs_dep_time = cleaner.crs_dep_time.astype(np.int8)

        cleaner.fl_date = pd.to_datetime(cleaner['fl_date']).dt.dayofweek

        
        
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
    return 'complete'



def check_clean(all_raw, overwrite,feather):
    if feather:
        spec = importlib.util.find_spec('pyarrow')
        assert spec is not None, "pyarrow is not installed, Conda install pyarrow or use 'feather=False'"

    test = glob.glob('../data/preprocessed_data/*')
    if len(test) != 0:
        assert overwrite, "data already exists (../data/preprocessed_data) use 'overwrite=True' to overwrite"

    assert len(all_raw) == 24,'raw data files have been tampered with. There should be 24.'

