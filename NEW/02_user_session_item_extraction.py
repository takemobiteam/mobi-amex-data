import pandas as pd
import os
import time
from tqdm import tqdm
tqdm.pandas(position=0,leave=True)

path = "s3://test-cj/csv_from_json/"
start=time.time()
all_items_df=pd.read_csv(os.path.join(path,'all_items_df.csv'))
all_trip_df=pd.read_csv(os.path.join(path,'all_trips_df.csv'))
print("It takes {:.4f} seconds to read the data".format(time.time()-start))

all_trip_df.drop(all_trip_df.index[all_trip_df['user_id'] == 'ServiceMonitorTestUser'], inplace=True)

print()
print("{:<35}{:<15,}".format("num of rows before preprecessing",all_trip_df.shape[0]))
print()

all_trip_df["items"]=all_trip_df["items"].apply(eval)
all_trip_df["selected items"]=all_trip_df["selected items"].apply(eval)
all_trip_df["shown items"]=all_trip_df["shown items"].apply(eval)

all_trip_df=all_trip_df.loc[all_trip_df.loc[:,"shown items"].apply(lambda d: len(d)) > 0]
print()
print("{:<35}{:<15,}".format("num of rows after preprecessing",all_trip_df.shape[0]))
print()

all_trip_df=all_trip_df.dropna(subset=['user_id', 'session_id'])
print()
print(all_trip_df.shape)
print()

user_id=[]
session_id=[]
create_time=[]
trip_id=[]
items=[]
for index,row in tqdm(all_trip_df.iterrows(), total=all_trip_df.shape[0], leave=True, position=True):
    for v in row["shown items"]:
        user_id.append(row["user_id"])
        session_id.append(row["session_id"])
        create_time.append(row["create_time"])
        trip_id.append(row["trip_id"])
        items.append(v)
    
df=pd.DataFrame({"user_id":user_id, "session_id":session_id,"create_time":create_time,"trip_id":trip_id,"item_id":items})
df.drop_duplicates(inplace=True)
df.sort_values(by=["user_id","session_id","create_time"],ascending=False,inplace=True)
df['create_time'] = pd.to_datetime(df['create_time'], unit = 'ms')
print(df[df['create_time'].ne(0)].create_time.agg([min, max]))

path = "s3://test-cj/"

if not os.path.exists(os.path.join(path,'csv_from_json')):
    os.makedirs(os.path.join(path,'csv_from_json'))
    
path = os.path.join(path,'csv_from_json')
df.to_csv(os.path.join(path , 'user_session_items_df.csv'))

