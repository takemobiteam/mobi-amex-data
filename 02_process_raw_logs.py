import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import gzip
import csv
import time
import re
import os
from io import StringIO
from fuzzywuzzy import fuzz
from tqdm import tqdm
tqdm.pandas(position=0,leave=True)

#split up params
def split_to_dict(cs_uri_query_string):
    if cs_uri_query_string == '-' or not '=' in cs_uri_query_string:
        return np.NaN
    else:
        split = cs_uri_query_string.split('&')
        d = {}
        for info in split: 
            i = info.split("=")
            # d[i[0]] = i[1]
            if len(i)>1 :
                d[i[0]] = i[1]
            else:
                return np.NaN
        if 'user_id' in d.keys(): 
            user_id_set.add(d['user_id'])
        return d
    
login_url = 'https:%252F%252Fwww.americanexpress.com%252Faccount%252Foauth%252Fconnect%253Fclient_id'

def click_func(param_dict): 
    if param_dict == np.NaN or type(param_dict) != dict: 
        return 'click-none'
    elif login_url in param_dict['url']:
        return 'login'
    else:
        final_str = "click-external"
        return final_str
    
def booking_func(param_dict):
    if type(param_dict) == dict and param_dict['booking_status'] == 'true':
        return 'marked-as-booked' 
    else:
        return 'marked-as-unbooked'
        
def search_func(param_dict):
    final_str = 'search_'
    if type(param_dict) == dict and param_dict['source'] == 'worldcities':
        final_str += 'From'
    else: 
        final_str += 'To'
    return final_str 
    
def action(row): 
    if row['cs-uri-stem'] not in action_dict.keys(): 
        return np.nan
    else: 
        return action_dict[row['cs-uri-stem']]
    
def action_func(val):
    if val not in action_dict.keys(): 
        return 'NaN'
    else:
        return action_dict[val]

def add_activities_func(param_dict):
    if 'activity_keyword' not in param_dict.keys():
        return 'add-activities'
    else:
        if param_dict['activity_keyword'] == 'ANY':
            return 'find-activities'
        else:
            return 'add-activities' 

def special_action(row): 
    if row['cs-uri-stem'] not in action_dict.keys(): 
        return 'NaN'
    else: 
        return action_dict[row['cs-uri-stem']]

def get_device(row):
    return row['cs(User-Agent)'].split('%', 3)[1][3:]

def process_df(df):
    
    #actions from dict mapping cs-uri-stem to action
    df['action'] = df['cs-uri-stem'].map(lambda val: action_func(val))
    
#     #special actions
#     special_action_dict = {'change-booking-status': booking_func,
#                            'click': click_func,
#                            'add-activities': add_activities_func, 
#                            'search': search_func,
#                           }
    
#     for key, val in special_action_dict: 
#         mask = df['action'].eq(key)
# #         func = f"{val} + func"
#         func = val 
#         df.loc[mask, ['action']] = (df.loc[mask].apply(lambda row: func(row['params']), axis=1))
        
    # df.loc[['change-booking-status'], ['action']] = 50
    # df['action'].loc[df['action'] == 'change-booking-status'] = df.apply(lambda row: booking_func(row['params']), axis = 1)
    # df.loc[df['action'] == 'change-booking-status'] = 
    # df.loc[df['action'] == 'change-booking-status', 'column_name'] = 'value'
    #df.loc[df['action'] == 'change-booking-status', 'action'].apply(lambda row: booking_func(row['params']), axis = 1)
    
    mask = df['action'].eq('change-booking-status')
    df.loc[mask, ['action']] = (df.loc[mask].progress_apply(lambda row: booking_func(row['params']), axis=1))
    
    #df.loc[df['action'] == 'click', 'action'].apply(lambda row: click_func(row['params']), axis = 1)
    mask = df['action'].eq('click')
    df.loc[mask, ['action']] = (df.loc[mask].progress_apply(lambda row: click_func(row['params']), axis=1))
    #df['action'] = df['action'].apply(lambda row: click_func(row['params']) if row['action'] == 'click' else row['action'], axis = 1)
    
    mask = df['action'].eq('add-activities')
    df.loc[mask, ['action']] = (df.loc[mask].progress_apply(lambda row: add_activities_func(row['params']), axis=1))
    
    mask = df['action'].eq('search')
    df.loc[mask, ['action']] = (df.loc[mask].progress_apply(lambda row: search_func(row['params']), axis=1))
    
    return df 


path = "s3://test-cj/"
start=time.time()
raw_complete_df = pd.read_csv(os.path.join(path ,'raw_complete_df.csv'), header = 0)
print("It takes {:.4f} second to load data".format(time.time()-start))

raw_complete_df.columns = ['index', "date", "time", "x-edge-location", "sc-bytes", "c-ip", "cs-method",
"cs(Host)", "cs-uri-stem", "sc-status", "cs(Referer)", "cs(User-Agent)", 
"cs-uri-query", "cs(Cookie)", "x-edge-result-type", "x-edge-request-id", 
"x-host-header", "cs-protocol", "cs-bytes", "time-taken", "x-forwarded-for", 
"ssl-protocol", "ssl-cipher", "x-edge-response-result-type", "cs-protocol-version", 
"fle-status", "fle-encrypted-fields", "c-port", "time-to-first-byte", "x-edge-detailed-result-type", 
"sc-content-type", "sc-content-len", "sc-range-start", "sc-range-end"]

raw_complete_df.drop(['index'],inplace=True,axis=1)

user_id_set = set()
user_attributes_dict = dict() #{user_id: [c-ip's, devices]}
user_sessions_dict = dict() #{user_id: [session_id]}

action_dict = {'/mobility-planner/demo/v2/tracking/click': 'click', 
                '/mobility-planner/demo/v2/domain/contexts/suggestions':'search', 
              '/mobility-planner/demo/v2/problem/primary-context-trips': 'entered-place', 
               '/mobility-planner/demo/v2/problem/nearby-contexts': 'all-place-can-go-rec',
               '/mobility-planner/demo/v2/tracking/detail-view': 'click-rec-service-detailed',
               '/mobility-planner/demo/v2/available-timeslots': 'quick-select-time',
               '/mobility-planner/demo/v2/problem/new-activity': 'add-activity',
               '/mobility-planner/demo/v2/problem/new-activities': 'add-activities',
               '/mobility-planner/demo/v2/problem/modified-activity': 'modified-activity',
               '/mobility-planner/demo/v2/booking-status': 'change-booking-status', 
               '/mobility-planner/demo/v2/profile/survey/response': 'save-survey-response',
               '/mobility-planner/demo/v2/session': 'save-get-or-del-session',
               '/mobility-planner/demo/v2/session-details': 'get-prev-session-details-saved-trips-tab', 
               '/mobility-planner/demo/v2/survey/response': 'add-survey-response',
               '/mobility-planner/demo/v2/problem/poi-recommendations': 'retrieve_recommended_poi-for-context',
               '/mobility-planner/demo/v2/domain/poi/suggestions': 'search_poi',
                
              }
non_action_dict= {'/mobility-planner/demo/v2/survey/domain':'get-survey-domain',
                '/mobility-planner/demo/v2/profile/domain': 'Plan-Profile-Domain', 
                '/mobility-planner/demo/v2/profile/domain/recommendation':'Get Recommended Tags for Destination', 
                '/mobility-planner/demo/v2/profile/domain/recommendation': 'Get Recommended Tags Given Selection', 
                }

df = raw_complete_df[["date", "time", "x-edge-location",  "c-ip", "cs-method", 
                      "cs-uri-stem",  "cs(Referer)", "cs(User-Agent)", "cs-uri-query"]]

#remove ServiceMonitorTestUser
discard = ['ServiceMonitorTestUser'] #[-23:]
df = df[~df['cs(Referer)'].str.contains('|'.join(discard))]
df = df[~df['cs-uri-stem'].str.contains('|'.join(discard))]
df = df[~df['cs-uri-query'].str.contains('|'.join(discard))]

#turn params to dict
df['params'] = df['cs-uri-query'].map(lambda val: split_to_dict(val))
df=df.dropna(subset=['params'])

df = process_df(df)

all_fields=set()
for index,row in tqdm(df.iterrows(), total=df.shape[0], leave=True, position=0):
    all_fields.update(set(row['params'].keys()))
    
## check if there is any misspellation in the desired fields
names=['user_id', 'session_id', 'poi_id', 'id', 'category', 'context','booking_status','url','activity_keyword','source']
for v in names:
    for k in all_fields:
        if fuzz.token_set_ratio(v, k)>=90 and v!=k:
            print(v,k)
            

names=['user_id', 'session_id', 'poi_id', 'id', 'category', 'context','booking_status','url','activity_keyword','source'] 
df_dict={v:[] for v in names}
for index,row in tqdm(df.iterrows(), total=df.shape[0], leave=True, position=0):
    for k in df_dict:
        if k in row['params'].keys():
            df_dict[k].append(row['params'][k])
        else:
            df_dict[k].append(np.NaN)
            
temp_param_df = pd.DataFrame(df_dict)

complete_df = pd.concat([df.reset_index(drop=True), temp_param_df.reset_index(drop=True)], axis=1)

complete_df['category'].replace({'ACCOMMODATION%2526poi%255fid':'ACCOMMODATION','experience%2526poi%255fid':'EXPERIENCE'},inplace=True)


save_path = "s3://test-cj/"

complete_df.to_csv(os.path.join(save_path,'processed_complete_df.csv'))

