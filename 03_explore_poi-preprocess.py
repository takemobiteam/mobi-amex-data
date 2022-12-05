import os
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import csv

from sklearn import preprocessing
import joblib

def datetime_to_unix_timestamp(df):
    df['unix_timestamp'] = pd.to_datetime(df['date'] + ' ' + df['time']).map(pd.Timestamp.timestamp)
    df['unix_timestamp'] = df['unix_timestamp'].astype(float)
    
def unix_timestamp_to_datetime(df):
    df['date'] = pd.to_datetime(df['unix_timestamp'], unit='ms').dt.date
    df['time'] = pd.to_datetime(df['unix_timestamp'], unit='ms').dt.time
    return df 

def datetime_to_Yoochoose(df): 
    df['yoochoose_timestamp'] = df['date'] + 'T' + df['time']+ 'Z' 
    
def unix_timestamp_to_Yoochoose(df, timecol='unix_timestamp'):
    df['yoochoose_timestamp'] = pd.to_datetime(df['unix_timestamp'], unit='ms').dt.date.astype('str') + 'T' + pd.to_datetime(df['unix_timestamp'], unit='ms').dt.time.astype('str') + 'Z'
    
    
def fill_user_id(df):
    # Filter out IP's with no user_id's
    grouped_ip_and_users = df.groupby(['c-ip'], as_index = False).agg({'user_id': set})
    grouped_ip_and_users['length'] = grouped_ip_and_users['user_id'].str.len()
    IP_with_users = grouped_ip_and_users[~grouped_ip_and_users['length'].eq(1)]
    df = df[df['c-ip'].isin(IP_with_users['c-ip'])]
    #fill in user_id for each c-ip
    df['filled_user_id'] = df['user_id'].groupby(df['c-ip']).transform(lambda x: x.bfill().ffill())
    
def fill_session_id(df, set_session_id = False):
    #fill in session_id based on original, note that it is for each separate location (can be in the same time frame)
    df = df.sort_values(by = 'datetime')
    df['filled_session_id'] = df['session_id']
    df.groupby('filled_user_id')["filled_session_id"].fillna(method = 'bfill', limit = 2, inplace = True)
    df.groupby('filled_user_id')["filled_session_id"].fillna(method = 'ffill', inplace = True)

def assign_session_id(df, how='filled',set_session_id = False): #, save_new = False, newName = None
    hows = ['filled', 'login', 'time']
    if how not in hows:
        raise ValueError("Invalid how. Expected one of: %s" % hows)
        
    if how == 'login': 
#         #per user, create session_id nums based on login 
#         df['count'] = df[df["action"]=='login'].groupby('filled_user_id').cumcount()+1
#         df['count'] = df.groupby('filled_user_id')['count'].fillna(method='ffill')
#         #create true/false 
#         df['bool_is_login'] = df['action'] == 'login'
#         #assigns new number to every true value 
#         df['num'] = df['bool_is_login'].astype(int).ne(0).cumsum()
        
        #per user, create session_id nums based on login 
        df['count'] = df[df["action"]=='login'].groupby('user_id').cumcount()+1
        print(df['count'])
        df['count'] = df.groupby('user_id')['count'].fillna(method='ffill')
        #create true/false 
        df['bool_is_login'] = df['action'] == 'login'
        #assigns new number to every true value 
        df['bool_is_login'] = df['bool_is_login'].astype(int).ne(0)
        print(df)
        df['num'] = df.groupby('user_id')['bool_is_login'].cumsum()
#         df['num'] = df['num'].astype(str)
        df['filled_session_id'] = df['user_id'] + '_'+ df['num'].astype(str)
        return df 

#     else if how == 'filled': 
    if how == 'filled':
        df['filled_session_id'] = df['session_id']
        df.groupby('user_id')["filled_session_id"].fillna(method = 'bfill', inplace = True)
        df.groupby('user_id')["filled_session_id"].fillna(method = 'ffill', inplace = True)
        return df
#         df["filled_session_id"].fillna(method = 'ffill', inplace = True)
        
#     if how == 'filled': 
#     # Filter out IP's with no user_id's
#         grouped_ip_and_users = df.groupby(['c-ip'], as_index = False).agg({'user_id': set})
#         grouped_ip_and_users['length'] = grouped_ip_and_users['user_id'].str.len()
#         IP_with_users = grouped_ip_and_users[~grouped_ip_and_users['length'].eq(1)]
#         df = df[df['c-ip'].isin(IP_with_users['c-ip'])]
#         #fill in user_id for each c-ip
#         df['filled_user_id'] = df['user_id'].groupby(df['c-ip']).transform(lambda x: x.bfill().ffill())

#         df['filled_session_id'] = df['session_id']
#         df.groupby('filled_user_id')["filled_session_id"].fillna(method = 'bfill', limit = 2, inplace = True)
#         df.groupby('filled_user_id')["filled_session_id"].fillna(method = 'ffill', inplace = True)
        
# #         #don't fill in user_id, just fill in session_id
# #         separate_action_df.groupby('user_id')["filled_session_id"].fillna(method = 'bfill', inplace = True)
# #         separate_action_df.groupby('user_id')["filled_session_id"].fillna(method = 'ffill', inplace = True)
        
        
    if how == 'time': 
        if set_session_id == True: 
            session_id_colname = 'session_id'
        else: 
            session_id_colname = how + '_sep_session_id'
            
#         # Sorting is needed, otherwise .diff() will output wrong results
#         df = df.sort_values(['user_id', 'unix_timestamp'])

#         # Timestamp diff in seconds
#         diff_timestamp = df.groupby('user_id')['unix_timestamp'].diff() / 1000

#         # indexes where new session_id will be created
#         new_session = (diff_timestamp.isnull()) | (diff_timestamp > 900)

#         # Create unique session_id for every user
#         df[session_id_colname] = df.loc[new_session, ['user_id', 'unix_timestamp']].groupby('user_id').rank(method='first').astype(int)

#         # Propagate last valid observation forward (replace NaN)
#         df[session_id_colname] = df[session_id_colname].fillna(method='ffill').astype(int)

#         f = lambda t: t.diff().gt(pd.Timedelta('30T')).cumsum()
#         df[session_id_colname] = df.groupby('user_id')['unix_timestamp'].apply(f) + 1

#         df['s'] = (df.Timestamp-combined_df.Timestamp.shift(1) > pd.Timedelta(5, 'm')).cumsum()+1
#         print(df['unix_timestamp'])
        
        #OTHER TIME SEP 
        #convert to unix time 
#         df['sort_time']=pd.to_datetime(df['unix_timestamp'], unit = 'ms') 
#         print(df['sort_time'])
#         #sort and assign session_id 
#         df.sort_values(by=['user_id','sort_time'], inplace=True)
        
        #sort and assign session_id 
        df.sort_values(by=['user_id','unix_timestamp'], inplace=True)
#         df['unix_timestamp'] = pd.Timestamp(df['unix_timestamp'])
        df['datetime'] = pd.to_datetime(df['unix_timestamp'], unit = 's')
        cond1 = df.datetime-df.datetime.shift(1) > pd.Timedelta(40, 'm') #before 5 now 40 
        cond2 = (df['user_id'] != df['user_id'].shift(1)) & (~(pd.isnull(df['user_id']) & pd.isnull(df['user_id'])))
#         cond3 = ~(pd.isnull(df['user_id']) & pd.isnull(df['user_id']))
        df[session_id_colname] = (cond1|cond2).cumsum()
        return df 


    #row ops 
def combine_id_and_poi(row):
    if pd.isnull(row['orig_poi_id']) and pd.isnull(row['id']): 
        return np.NaN
    elif not pd.isnull(row['orig_poi_id']): 
        return row['orig_poi_id']
    else: 
        return row['id']
    
#explored POI to yoochoose click format 
dir_path = "s3://test-cj/raw_dataset/"

explored_experience_POI = pd.read_csv(os.path.join(dir_path , 'ExploredExperiencePOIs(0601).csv'))
explored_accommodation_POI = pd.read_csv(os.path.join(dir_path , 'ExploredAccommodationPOIs(0601).csv'))
explored_dining_POI = pd.read_csv(os.path.join(dir_path , 'ExploredDiningPOIs(0601).csv'))

explored_experience_POI['poi_cat'] = 'experience'
explored_accommodation_POI['poi_cat'] = 'accommodation'
explored_dining_POI['poi_cat'] = 'dining'

combined_df=pd.concat([explored_experience_POI, explored_accommodation_POI, explored_dining_POI],axis=0)

combined_df.rename(columns={"Timestamp": "unix_timestamp", 'User Id': 'user_id', 'POI Id': 'poi_id', 'Destination': 'context'}, inplace = True)
combined_df['unix_timestamp'] = pd.to_datetime(combined_df['unix_timestamp'], unit = 'ms') 
assign_session_id(combined_df, how = 'time', set_session_id = True)
unix_timestamp_to_Yoochoose(combined_df, timecol='unix_timestamp')
combined_df.rename(columns={"yoochoose_timestamp": "timestamp"}, inplace = True)

encoder_keynames = {'poi_cat': 'category', 'poi_id':'poi_id', 'context': 'context'}

new_encode_dict = joblib.load('all-label_encoder_dict.joblib')

def save_click_csv(df, itemcol, catcol): 
    orig_cols = ['session_id', 'timestamp', itemcol, catcol]
    
#     click_df = df.copy()
    click_df = df[orig_cols].copy()
    click_df.dropna(subset = orig_cols, inplace = True)
    
    encoder_keynames = {'poi_cat': 'category', 'poi_id':'poi_id', 'context': 'context'}
    for col in [itemcol, catcol]: #not transforming 'session_id' because it was assigned timewise 
        click_df[col] = new_encode_dict[encoder_keynames[col]].transform(click_df[col])
            
    #denote what catcol is used when the item used is poi_id 
    rename = {'poi_id': 'poi', 'poi_cat': 'category', 'context': 'context'}
    fname = 'poi_' + rename[catcol] if itemcol == 'poi_id' else rename[itemcol]
    
    click_df.to_csv(save_path + 'AMEX_explorepoi_' + fname + '_clicks.csv', sep = ',', index= False, header = False, columns = orig_cols)

save_path = "s3://test-cj/"
save_click_csv(combined_df, itemcol = 'poi_id', catcol = 'context')
save_click_csv(combined_df, itemcol = 'poi_id', catcol = 'poi_cat') 
save_click_csv(combined_df, itemcol = 'poi_cat', catcol = 'context')    
save_click_csv(combined_df, itemcol = 'context', catcol = 'poi_cat')

