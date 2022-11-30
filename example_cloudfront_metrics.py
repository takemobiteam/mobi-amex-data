import glob
import gzip
import io
from os.path import basename, exists
from pathlib import Path

import boto3
import pandas as pd
import os
from tqdm import tqdm
tqdm.pandas()

BUCKET_NAME = "aws-cloudfront-logs-825437374987-us-east-1"
RANGE_IN_DAYS = 7
PATH_TO_SAVE = "data/aws-cloudfront-logs/"
Path(PATH_TO_SAVE).mkdir(parents=True, exist_ok=True)

objs = []
s3 = boto3.client("s3")
# objs = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET_NAME)

# This is super chunky....
for page in pages:
    for obj in page["Contents"]:
        objs.append(obj)

print()
print("{:<20}{:<15,}".format("The number of file",len(objs)))
print()

objs_adj=[]
for obj in objs:
    if int(obj["LastModified"].strftime("%s")):
        obj["int_last_modified"]=int(obj["LastModified"].strftime("%s"))
    objs_adj.append(obj)

objs_adj.sort(key=lambda v: v["int_last_modified"])

last_modified_file = objs_adj[-1]
files_within_range = [
    obj
    for obj in objs
    if (last_modified_file["LastModified"] - obj["LastModified"]).days < RANGE_IN_DAYS
]
for file in files_within_range:
    if exists(PATH_TO_SAVE + basename(file["Key"])):
        continue
    s3.download_file(Bucket=BUCKET_NAME, Key=file["Key"], Filename=PATH_TO_SAVE + basename(file["Key"]))
    
    
num_file=len(os.listdir(PATH_TO_SAVE))
print()
print("{:<30}{:<15,}".format("number of downloaded dataset",num_file))
print()

data = pd.DataFrame([])
for f in tqdm(glob.glob(PATH_TO_SAVE + "*.gz"),total=num_file,position=0,leave=True):
    with gzip.open(f, mode="rt") as gzipfile:
        content = gzipfile.read()
        content = content.split("\n", 2)[2]
        data = pd.concat(
            [data, pd.read_table(io.StringIO(content), sep="\t", header=None)], axis=0
        )

example_file = glob.glob(PATH_TO_SAVE + "*.gz")[0]
with gzip.open(example_file, mode="rt") as gzipfile:
    content = gzipfile.read()
    content = content.split("\n", 2)[1]
colnames_str = content.split(": ")[1]
colnames = colnames_str.split(" ")
data.columns = colnames


# !pip install fsspec
# !pip install s3fs

file_output="cloudfront_metrics"
data_dir="s3://test-cj/"
data.to_pickle(os.path.join(data_dir,file_output))

import pandas as pd
df=pd.read_pickle(os.path.join(data_dir,file_output))
print()
print(df.shape)
print()
