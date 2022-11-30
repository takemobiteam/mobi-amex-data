import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import gzip
import csv
from tqdm import tqdm
tqdm.pandas()
import boto3
import re
import os
from io import StringIO
import json

import time

#DONT RUN UNLESS YOU NEED TO RELOAD ENTIRE LOG FILES!!
#takes several hours 

s3_resource = boto3.resource('s3',
            aws_access_key_id='AKIA4AL7GZIFV7TVBAOX',
            aws_secret_access_key= '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')

bucket_name = 'mobi-planner-profiles'
prefix_name = 'temp_user_sessions/'
my_bucket = s3_resource.Bucket(bucket_name)
objects = my_bucket.objects.filter(Prefix = prefix_name)


def from_jsondata_fill_trip_info_row(data): 
    user_id = data['profile']['user_id']
    session_id = data['solution_response']['session_id']
    new_trip_info_row_dict = dict({'user_id': user_id, 
                                  'session_id': session_id,
                                 'create_time': data['create_time'], 
                                 'trip_id':data['trip_id']})
    return new_trip_info_row_dict

def find_price(activity_content): 
    if 'prices' in activity_content:
        prices = activity_content['prices'][0]['amount']
    else: 
        prices = np.NaN
    return prices 

def create_new_item_dict(activity_content): 
    new_item_dict = {k: activity_content.get(k, None) for k in ('name', 'category', 'rating', 'id', 'source_id', 'name', 'description', 'prices', 'url', 'rating', 'types', 'categories', 'tags', 'contexts', 'booking_url')}
    # internal_item_id = activity_content['id']
    return new_item_dict


#no sel items 
all_items = dict()
all_trips_dict_list = []
category_set = set(['ACCOMMODATION', 'EXPERIENCE', 'DINING'])

total_count = 0
for obj in objects: 
    total_count +=1
print('total files processed counter: ' + str(total_count))

start_time = time.time()

for obj in tqdm(objects,total=total_count,leave=True,position=0): 
    # remove the file name from the object key
    obj_path = os.path.dirname(obj.key)
    path, filename = os.path.split(obj.key)
    
    # # create nested directory structure
    # Path(obj_path).mkdir(parents=True, exist_ok=True)
    
    jsonStr = obj.get()['Body'].read().decode('utf-8')
    jsonObj = json.loads(jsonStr)
    
    data = jsonObj
    
    #new row in all trips
    trip_info_row = from_jsondata_fill_trip_info_row(data)

    #add trip items row and item info 
    trip_items = dict()
    
    trip_shown_items = set()
    trip_selected_items = set()
    
    trip_item_prices = dict()
    
    for activity in data['solution_response']['solution']['activities']:
        if 'category' in activity:
            if activity['category'] in category_set and 'content' in activity['source']:
                activity_content = activity['source']['content']
                internal_item_id = activity_content['id']
                source_item_id = activity_content['source_id']
                prices = find_price(activity_content)

                #adding to all_items list for item database
                if internal_item_id not in all_items: 
                    new_item_dict = create_new_item_dict(activity_content)
                    all_items[internal_item_id] = new_item_dict #using mobi id
                    
                selected_bool = not activity['spec']['curated']
                
                #dict of key = item, value = booked or not, true means booked
                trip_items[internal_item_id] = selected_bool
                trip_shown_items.add(internal_item_id) 
                
                if selected_bool == True: 
                    trip_selected_items.add(internal_item_id)
                    
                #for trivago 
                trip_item_prices[internal_item_id] = prices

    trip_info_row['items'] = trip_items
    
    trip_info_row['selected items'] = trip_selected_items
    trip_info_row['shown items']= trip_shown_items
    trip_info_row['shown item prices']= [val for key, val in trip_item_prices.items()]
    
    all_trips_dict_list.append(trip_info_row)
print()
print("--- %s seconds ---" % (time.time() - start_time))
print()

path = "s3://test-cj/"

if not os.path.exists(os.path.join(path,'csv_from_json')):
    os.makedirs(os.path.join(path,'csv_from_json'))
    
path = os.path.join(path,'csv_from_json')

all_items_df = pd.DataFrame.from_dict(all_items, orient='index')
all_trips_df = pd.DataFrame(all_trips_dict_list)

# def dict_to_str_impressions(dict_items):
#     items = list(dict_items.keys())
#     return " ".join([str(i) for i in items])

# def dict_to_str_prices(dict_items):
#     prices = list(dict_items.values())
#     return " ".join([str(i) for i in prices])

all_items_df.to_csv(os.path.join(path , 'all_items_df.csv'))
all_trips_df.to_csv(os.path.join(path ,  'all_trips_df.csv') )

