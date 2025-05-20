#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import sqlalchemy 
import pytz
import requests
import datetime as dt
from urllib.parse import urlencode, quote
from sqlalchemy import create_engine
import sys
from sqlalchemy import text

# Start processing timer
first_time = dt.datetime.now()
dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

# In[2]:

# SETUP CONNECTIONS (EDIT PLACEHOLDERS)
# Connection to source (e.g., MySQL)
mysql_str = 'mysql+pymysql://<source_user>:<password>@<source_host>:<port>/<source_database>'
con_source = create_engine(mysql_str)

# In[3]:

# Connection to destination (e.g., PostgreSQL)
postgres_str_dest = 'postgresql://<dest_user>:<password>@<dest_host>:<port>/<dest_database>'
connect_dest = create_engine(postgres_str_dest)
con_dest = connect_dest.connect()

# In[4]:

# Update checkpoint file to set status active
sch_status = pd.read_csv('upsert_checkpoint_filename.csv')
sch_status['status'] = 'active'
sch_status.to_csv('upsert_checkpoint_filename.csv', index=False)

# In[5]:

# Update cron/pipeline status in the destination DB to 'running'
with con_dest.connect() as con_status:
    con_status.execute("UPDATE <pipeline_status_table> SET status = 'running' WHERE pipeline = '<pipeline_id>'")

# In[6]:

# LOAD CHECKPOINT TIME
load_date_cron = pd.read_csv('upsert_checkpoint_filename.csv')
ld = load_date_cron['date'].iloc[0]
ld = '"' + ld + '"'
ld

# In[7]:

# Compute current timestamp minus 1 minute
save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
current_time = dt.datetime.strptime(save_date_cron, "%Y-%m-%d %H:%M:%S")
save_date_cron = str(current_time - dt.timedelta(minutes=1))

# In[8]:

# QUERY DATA FROM SOURCE BASED ON LAST CHECKPOINT
df = pd.read_sql_query('''
SELECT * FROM <source_table>
WHERE <timestamp_column> >= {}
'''.format(ld), con_source)

# In[9]:

# IF NO DATA FOUND, UPDATE CHECKPOINT AND EXIT
if len(df) == 0:
    save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sv = pd.DataFrame({'date': [save_date_cron], 'status': ['non_active']})
    sv.to_csv('upsert_checkpoint_filename.csv', index=False)
    sv

    first_time = dt.datetime.now()
    later_time = dt.datetime.now()
    difference = later_time - first_time
    seconds_in_day = 24 * 60 * 60
    x_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)
    print(" processing_time, {}:{} seconds".format(x_time[0], x_time[1]))
    x_ms = str(round(difference.total_seconds() * 1000, 2)) + ' ms'

    con_dest = create_engine(postgres_str_dest)
    with con_dest.connect() as con_status:
        con_status.execute("UPDATE <pipeline_status_table> SET status = 'done', write_date = '{}', peformance = '{}' WHERE pipeline = '<pipeline_id>'".format(save_date_cron, x_ms)) 
    sys.exit()

# In[10]:

# APPEND METADATA
df['create_date'] = pd.Timestamp.now()

# In[11]:

# UPSERT FUNCTION
def upsert_data(dataframe):
    con_dest = create_engine(postgres_str_dest)
    con_update = con_dest.connect()
    if len(dataframe) == 0:
        return 'no updated'
    else:
        count = 0
        for row in dataframe.itertuples(index=False):
            # UPSERT QUERY — replace column names with yours
            con_update.execute("""
                INSERT INTO <destination_table>(<column1>, <column2>, ..., <columnN>)
                VALUES(%s, %s, ..., %s)
                ON CONFLICT (<unique_key_column>)
                DO UPDATE SET
                    <column1> = excluded.<column1>,
                    <column2> = excluded.<column2>,
                    ...
                    <columnN> = excluded.<columnN>
            """, tuple(row))
            count += 1
            print(count)

# In[12]:

# EXECUTE UPSERT
upsert_data(df)

# In[13]:

# SAVE CHECKPOINT TIME
sv = pd.DataFrame({'date': [save_date_cron], 'status': ['non_active']})
sv.to_csv('upsert_checkpoint_filename.csv', index=False)
sv

# In[14]:

# FINAL PERFORMANCE METRICS
later_time = dt.datetime.now()
difference = later_time - first_time
seconds_in_day = 24 * 60 * 60
save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
x_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)
print(" processing_time, {}:{} seconds".format(x_time[0], x_time[1]))
x_ms = str(round(difference.total_seconds() * 1000, 2)) + ' ms'

# In[15]:

# FINAL STATUS UPDATE
con_dest = create_engine(postgres_str_dest)
with con_dest.connect() as con_status:
    con_status.execute("UPDATE <pipeline_status_table> SET status = 'done', write_date = '{}', peformance = '{}' WHERE pipeline = '<pipeline_id>'".format(save_date_cron, x_ms))
