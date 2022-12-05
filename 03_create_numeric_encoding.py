import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import gzip
import csv

import pickle
import joblib
import boto3
import re
import os
from io import StringIO
from sklearn import preprocessing

raw_data_path = "s3://test-cj/raw_dataset/"
processed_data_path="s3://test-cj/"


logs = pd.read_csv(os.path.join(processed_data_path, 'processed_complete_df.csv'), usecols = ['user_id', 'session_id', 'poi_id', 'id', 'category', 'context'])
logs['category'].replace({'ACCOMMODATION%2526poi%255fid':'ACCOMMODATION','experience%2526poi%255fid':'EXPERIENCE'},inplace=True)

explorePOI = pd.read_csv(os.path.join(raw_data_path,'explorePOI.csv'), usecols = ['POI Id', 'User Id', 'Destination', 'poi_cat'])
explorePOI.rename(columns={'User Id': 'user_id', 'POI Id': 'poi_id', 'Destination': 'context'}, inplace = True)

all_trips = pd.read_csv(os.path.join(processed_data_path, 'csv_from_json' ,'all_trips_df.csv'), usecols = ['user_id', 'session_id', 'trip_id'])
all_items = pd.read_csv(os.path.join(processed_data_path, 'csv_from_json' , 'all_items_df.csv'), usecols = ['id'])

all_user_id = {*logs['user_id'].unique(), *explorePOI['user_id'].unique(), *all_trips['user_id'].unique()}
all_poi_id = {*logs['poi_id'].unique(), *logs['id'].unique(), *explorePOI['poi_id'].unique(), *all_items['id'].unique()}
all_session_id = {*logs['session_id'].unique(), *all_trips['session_id'].unique()}
all_category_id = {*logs['category'].unique(), *explorePOI['poi_cat'].unique()}
all_context_id = {*logs['context'].unique(), *explorePOI['context'].unique()}
# all_items['contexts'].agg(pd.unique), 
# {'ACCOMMODATION', 'DINING', 'EXPERIENCE'}

transform_cols = dict({'user_id': list(all_user_id),
      'poi_id': list(all_poi_id),
      'session_id': list(all_session_id),
      'category': list(all_category_id),
      'context': list(all_context_id)
       })

all_label_dict = defaultdict(preprocessing.LabelEncoder)
print(transform_cols.keys())
for colname, vals in transform_cols.items():
    new_encoder = preprocessing.LabelEncoder()
    vals.append('nan')
    new_encoder.fit_transform(vals)
    all_label_dict[colname] = new_encoder
    
joblib.dump(all_label_dict, 'all-label_encoder_dict.joblib')

