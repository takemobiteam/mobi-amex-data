# !pip install fsspec
# !pip install s3fs

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import gzip
import csv
from tqdm import tqdm
import boto3
import re
import os
from io import StringIO

save_path = "s3://test-cj/"

def process_each_file(obj):
     with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
            content = gzipfile.read()
            # Split the long string into a list of lines
            s=str(content,'utf-8')
            s = s[13:] ## "remove #Version: 1.0"
            s = s[10:] ## remove "#Fields: "
            s = s[448:]
    #         s = s[471:]
            return s + '\n'

#takes several hours 
s3_resource = boto3.resource('s3',
            aws_access_key_id='AKIA4AL7GZIFV7TVBAOX',
            aws_secret_access_key= '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')

bucket_name = 'aws-cloudfront-logs-825437374987-us-east-1'
prefix_name = 'amex-production/'
my_bucket = s3_resource.Bucket('aws-cloudfront-logs-825437374987-us-east-1')
objects = my_bucket.objects.filter(Prefix = 'amex-production/')

counter = 0
for obj in objects: 
    counter +=1
print('total files processed counter: ' + str(counter))

big_string = ''
cur_date=set()
for obj in tqdm(objects,total=counter,leave=True,position=0): 
    path, filename = os.path.split(obj.key)
    currdate = filename[16:24]
    cur_date.add(currdate)
    big_string += process_each_file(obj)

data = StringIO(big_string)
df=pd.read_csv(data, delimiter = '\t')

df.to_csv(os.path.join(save_path,'raw_complete_df.csv'), sep = ',', index= True)
print('DONE: written to raw complete_df')
