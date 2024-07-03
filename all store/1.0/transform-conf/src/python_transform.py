import psycopg2 as pc
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
import configparser 
import io

config = configparser.ConfigParser()
config.read('./conf/config.cfg')

conf_database_source=config['postgres']['DATABASE_SOURCE']
conf_database_target=config['postgres']['DATABASE_TARGET']
conf_user=config['postgres']['USER']
conf_password=config['postgres']['PASSWORD']
conf_host=config['postgres']['HOST']
conf_port=config['postgres']['PORT']

conf_sql_table=config['command']['SQL_TABLE']
conf_sql_query=config['command']['SQL_QUERY']
conf_csv_path=config['path']['CSV_PATH2']

conn = pc.connect(database=conf_database_source,user=conf_user,password=conf_password,host=conf_host,port=conf_port)
cursor = conn.cursor()
cursor.execute(conf_sql_table)
temp_name = cursor.fetchone()
name = temp_name[0]
sql = "select * from "+name+" "+conf_sql_query
date_start = name[15:19]+"-"+name[19:21]+"-"+name[21:23]+' 00:00:00'
#date_start = name[21:25]+"-"+name[25:27]+"-"+name[27:29]+' 00:00:00'
date_end = name[15:19]+"-"+name[19:21]+"-"+name[21:23]+' 23:59:59'
#date_end = name[21:25]+"-"+name[25:27]+"-"+name[27:29]+' 23:59:59'

cursor.execute(sql)
df1 = cursor.fetchall()

df2 = pd.DataFrame(df1)
df2.rename(columns={0:'tps',1:'data_time',2:'bank_send',3:'bank_receive',4:'proxy_type',5:'datatype'},inplace=True)

cursor.close()
conn.close()

print("Load data success.\n")

df = df2.copy()
df.drop(['bank_send','bank_receive','datatype'],axis=1,inplace=True)

KBNK_SCB_A = df.loc[df['proxy_type'] == 'ACCOUNT'].reset_index()
KBNK_SCB_B = df.loc[df['proxy_type'] == 'BILLERID'].reset_index()
KBNK_SCB_E = df.loc[df['proxy_type'] == 'EWALLET'].reset_index()
KBNK_SCB_M = df.loc[df['proxy_type'] == 'MSISDN'].reset_index()
KBNK_SCB_N = df.loc[df['proxy_type'] == 'NATID'].reset_index()

split_data = [KBNK_SCB_A,KBNK_SCB_B,KBNK_SCB_E,KBNK_SCB_M,KBNK_SCB_N]
type = ['ACCOUNT', 'BILLERID', 'EWALLET', 'MSISDN', 'NATID']
real = []

full_ts_secs = pd.date_range(date_start, end=date_end, freq='S')
full_ts_secs = pd.DataFrame(full_ts_secs)
full_ts_secs.rename(columns={0:'data_time'},inplace=True)

for data in range(len(split_data)):
        temp = split_data[data].copy()
        temp = temp.set_index(['data_time'])
        temp_new = temp.reindex(full_ts_secs['data_time'])
        temp_new.reset_index(inplace=True)
        temp_new['tps'] = temp_new['tps'].fillna(0)
        temp_new['proxy_type'] = temp_new['proxy_type'].fillna(type[data])
        real.append(temp_new)
print("Fill data success.\n")

new_df = pd.concat(real)
new_df.drop(['index'],axis=1,inplace=True)
one_hot = pd.get_dummies(new_df,columns=['proxy_type'])

train_data = one_hot.copy()

train_data['second']=train_data.data_time.dt.second
train_data['minute']=train_data.data_time.dt.minute
train_data['hour']=train_data.data_time.dt.hour
train_data['day'] =train_data.data_time.dt.day
train_data['weekday'] =train_data.data_time.dt.weekday
print("Data transform success.\n".encode())

#check table in database
conn1 = pc.connect(database=conf_database_target,user=conf_user,password=conf_password,host=conf_host,port=conf_port)
cursor1 = conn1.cursor()

cursor1.execute("select exists(select * from information_schema.tables where table_name=%s)",('transform_data',))
result = cursor1.fetchone()[0]

# Initialize postgresql
if result == False :
    sql = '''CREATE TABLE transform_data (
	data_time TIMESTAMP WITHOUT TIME ZONE,
    tps FLOAT(53),  
	"proxy_type_ACCOUNT" BOOLEAN, 
	"proxy_type_BILLERID" BOOLEAN, 
	"proxy_type_EWALLET" BOOLEAN, 
	"proxy_type_MSISDN" BOOLEAN, 
	"proxy_type_NATID" BOOLEAN, 
	second INTEGER, 
	minute INTEGER, 
	hour INTEGER, 
	day INTEGER, 
	weekday INTEGER
    );'''
    cursor1.execute(sql)
    conn1.commit()
    print("Create Postgresql table finish")

    values = train_data.values.tolist()
    args = ','.join(cursor1.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8') for i in values)
    cursor1.execute("INSERT INTO transform_data VALUES "+(args))
    conn1.commit()
    print("upload data finish")
else :
    values = train_data.values.tolist()
    args = ','.join(cursor1.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8') for i in values)
    cursor1.execute("INSERT INTO transform_data VALUES "+(args))
    conn1.commit()
    print("upload data finish")

cursor1.close()
conn1.close()

table = pa.Table.from_pandas(train_data)

conf_end=config['minio']['ENDPOINT']
conf_acc_key=config['minio']['ACCESS_KEY']
conf_ser_key=config['minio']['SECRET_KEY']
conf_port=config['minio']['PORT']
conf_bucket_name=config['minio']['BUCKET_NAME']
# Initialize MinIO client

output_stream = io.BytesIO()
pq.write_table(table, output_stream)
output_stream.seek(0)
minio_client = Minio(endpoint=conf_end+":"+conf_port,access_key=conf_acc_key,secret_key=conf_ser_key,secure=False)
minio_client.put_object(bucket_name=conf_bucket_name,object_name="transform_"+name+".parquet",data=output_stream, length=output_stream.getbuffer().nbytes)

print("Parquet file uploads to Minio successfully.")
print("Finish all")

