import time
import csv
import pickle
import operator
import datetime
import os
from tqdm import tqdm
import pandas as pd
from collections import defaultdict
from sklearn import preprocessing


path = "./dataset"
start=time.time()
df=pd.read_csv(os.path.join(path,'user_session_items_df.csv'))
print("It takes {:.4f} seconds to read the data".format(time.time()-start))
df.drop(["Unnamed: 0"],inplace=True,axis=1)

def convert_id_integer(df,column_name):
    all_id={*df[column_name].unique()}
    dict_id={}
    cnt=0
    for v in tqdm(all_id):
        if v not in dict_id:
            dict_id[v]=cnt
            cnt+=1
    return dict_id

user_dict=convert_id_integer(df,"user_id")
session_dict=convert_id_integer(df,"session_id")
item_dict=convert_id_integer(df,"item_id")

user_id=[]
session_id=[]
item_id=[]
timestamp=[]
for index,row in tqdm(df.iterrows(), total=df.shape[0]):
    user_id.append(user_dict[row["user_id"]])
    session_id.append(session_dict[row["session_id"]])
    item_id.append(item_dict[row["item_id"]])
    timestamp.append(row["create_time"])
    
df=pd.DataFrame({"user_id":user_id, "session_id":session_id, "item_id":item_id, "timestamp":timestamp})

preprocess_output_log = dict()

sess_clicks = {}
sess_date = {}
ctr = 0
curid = -1
curdate = None
for index,row in tqdm(df.iterrows(), total=df.shape[0]):
    sessid = row['session_id']
    if curdate and not curid == sessid:
        date = time.mktime(time.strptime(curdate[:19], '%Y-%m-%d %H:%M:%S'))
        sess_date[curid] = date
    curid = sessid
    item=row["item_id"]
    curdate = row['timestamp']
    if sessid in sess_clicks:
        sess_clicks[sessid] += [item]
    else:
        sess_clicks[sessid] = [item]
    ctr += 1
    
date = time.mktime(time.strptime(curdate[:19], '%Y-%m-%d %H:%M:%S'))
sess_date[curid] = date

print("-- Reading data @ %ss" % datetime.datetime.now())
preprocess_output_log['start time'] = datetime.datetime.now()

len1_sessions_filtered_count = 0 
# Filter out length 1 sessions
for s in list(sess_clicks):
    if len(sess_clicks[s]) == 1:
        len1_sessions_filtered_count += 1
        del sess_clicks[s]
        del sess_date[s]
        
preprocess_output_log['length 1 sessions filtered'] = len1_sessions_filtered_count

# Count number of times each item appears
iid_counts = {}
for s in sess_clicks:
    seq = sess_clicks[s]
    for iid in seq:
        if iid in iid_counts:
            iid_counts[iid] += 1
        else:
            iid_counts[iid] = 1

sorted_counts = sorted(iid_counts.items(), key=operator.itemgetter(1))

length = len(sess_clicks)
for s in list(sess_clicks):
    curseq = sess_clicks[s]
    filseq = list(filter(lambda i: iid_counts[i] >= 5, curseq))
    if len(filseq) < 2:
        del sess_clicks[s]
        del sess_date[s]
    else:
        sess_clicks[s] = filseq

print()
print(f"before filtering:\t {length}")
print(f"after filtering:\t {len(sess_clicks)}")
print()

# Split out test set based on dates
dates = list(sess_date.items())
maxdate = dates[0][1]
mindate = dates[0][1]
for _, date in dates:
    if maxdate < date:
        maxdate = date
    if mindate > date:
        mindate = date
        
preprocess_output_log['max date'] = pd.to_datetime(int(maxdate), unit='s')
preprocess_output_log['min date'] = pd.to_datetime(int(mindate), unit='s')

amex_splitdate = 20
preprocess_output_log['amex split days before maxdate'] = amex_splitdate
splitdate = maxdate - 86400 * amex_splitdate 

print("{:<25}{:}".format('Minimal date', pd.to_datetime(int(mindate), unit='s')))
print("{:<25}{:}".format('Splitting date', pd.to_datetime(int(splitdate), unit='s')))
print("{:<25}{:}".format('Maximal date', pd.to_datetime(int(maxdate), unit='s')))
print()
tra_sess = filter(lambda x: x[1] < splitdate, dates)
tes_sess = filter(lambda x: x[1] > splitdate, dates)
# Sort sessions by date
tra_sess = sorted(tra_sess, key=operator.itemgetter(1))     # [(sessionId, timestamp), (), ]
tes_sess = sorted(tes_sess, key=operator.itemgetter(1))     # [(sessionId, timestamp), (), ]
print("{:<25}{:<20,}{:<20.2%}".format("Training Set",len(tra_sess),len(tra_sess)/len(dates)) )  
print("{:<25}{:<20,}{:<20.2%}".format("Test Set",len(tes_sess),len(tes_sess)/len(dates)) ) 
print()
print(tra_sess[:3])
print(tes_sess[:3])

preprocess_output_log['len training sessions (tra_sess)'] = len(tra_sess)
preprocess_output_log['len test sessions (tes_sess)'] = len(tes_sess)

item_dict = {}
# Convert training sessions to sequences and renumber items to start from 1
def obtian_tra():
    train_ids = []
    train_seqs = []
    train_dates = []
    item_ctr = 1
    for s, date in tra_sess:
        seq = sess_clicks[s]
        outseq = []
        for i in seq:
            if i in item_dict:
                outseq += [item_dict[i]]
            else:
                outseq += [item_ctr]
                item_dict[i] = item_ctr
                item_ctr += 1
        if len(outseq) < 2:  # Doesn't occur
            continue
        train_ids += [s]
        train_dates += [date]
        train_seqs += [outseq]

    preprocess_output_log['item count:'] = item_ctr
    print()
    print("{:<20}{:<15,}".format("item count:",item_ctr) )    
    print()
    return train_ids, train_dates, train_seqs


# Convert test sessions to sequences, ignoring items that do not appear in training set
def obtian_tes():
    test_ids = []
    test_seqs = []
    test_dates = []
    for s, date in tes_sess:
        seq = sess_clicks[s]
        outseq = []
        for i in seq:
            if i in item_dict:  ### avoid cold start issue
                outseq += [item_dict[i]]
        if len(outseq) < 2:
            continue
        test_ids += [s]
        test_dates += [date]
        test_seqs += [outseq]
    return test_ids, test_dates, test_seqs


tra_ids, tra_dates, tra_seqs = obtian_tra()
tes_ids, tes_dates, tes_seqs = obtian_tes()

def process_seqs(iseqs, idates):
    out_seqs = []
    out_dates = []
    labs = []
    ids = []
    for id, seq, date in zip(range(len(iseqs)), iseqs, idates):
        for i in range(1, len(seq)):
            tar = seq[-i]
            labs += [tar]
            out_seqs += [seq[:-i]]
            out_dates += [date]
            ids += [id]
    return out_seqs, out_dates, labs, ids


tr_seqs, tr_dates, tr_labs, tr_ids = process_seqs(tra_seqs, tra_dates)
te_seqs, te_dates, te_labs, te_ids = process_seqs(tes_seqs, tes_dates)
tra = (tr_seqs, tr_labs)
tes = (te_seqs, te_labs)
print("{:<35}{:<20,}".format("number of training sequences",len(tr_seqs))  )
print("{:<35}{:<20,}".format("number of test sequences",len(te_seqs))  )
print()
print(tr_seqs[:3], tr_dates[:3], tr_labs[:3])
# print(te_seqs[:3], te_dates[:3], te_labs[:3])
all = 0

preprocess_output_log['len training seqs (tr_seqs)'] = len(tr_seqs)
preprocess_output_log['len test seqs (te_seqs)'] = len(tr_seqs)

for seq in tra_seqs:
    all += len(seq)
for seq in tes_seqs:
    all += len(seq)
print()
print("{:<25}{:<20.2f}".format('avg length: ', all/(len(tra_seqs) + len(tes_seqs) * 1.0)))
print()
preprocess_output_log['avg length'] = all/(len(tra_seqs) + len(tes_seqs) * 1.0)
print(preprocess_output_log)


dir_path="./dataset"
if not os.path.exists(dir_path):
    os.makedirs(dir_path)
pickle.dump(tra, open(dir_path + '/train.txt', 'wb'))
pickle.dump(tes, open(dir_path + '/test.txt', 'wb'))
pickle.dump(tra_seqs, open(dir_path + '/all_train_seq.txt', 'wb'))
pickle.dump(tes_seqs, open(dir_path + '/all_test_seq.txt', 'wb'))


