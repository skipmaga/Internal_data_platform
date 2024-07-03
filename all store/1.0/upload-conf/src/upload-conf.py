import configparser
from minio import Minio
import pyarrow.parquet as pq
import psycopg2 as pc
import os

config = configparser.ConfigParser()
config.read('./conf/config.cfg')
path = config['path']['FILE_PATH']

par_file = ""
for x in os.listdir(path):
    if x.endswith(".parquet"):
        par_file = x
file_path = path+par_file

parquet_file = pq.ParquetFile(file_path)

conf_end=config['minio']['ENDPOINT']
conf_acc_key=config['minio']['ACCESS_KEY']
conf_ser_key=config['minio']['SECRET_KEY']
conf_port=config['minio']['PORT']
conf_bucket_name=config['minio']['BUCKET_NAME']


print(file_path)
print(par_file)
print(conf_bucket_name)

minio_client = Minio(endpoint=conf_end+":"+conf_port,access_key=conf_acc_key,secret_key=conf_ser_key,secure=False)
minio_client.fput_object(conf_bucket_name, par_file, file_path)
print("load data to Minio success")

conf_database=config['postgres']['DATABASE']
conf_user=config['postgres']['USER']
conf_password=config['postgres']['PASSWORD']
conf_host=config['postgres']['HOST']
conf_port=config['postgres']['PORT']

csv_file = ""
for x in os.listdir(path):
    if x.endswith(".csv"):
        csv_file = x
file_name = csv_file[:-13]
print(file_name)

conn = pc.connect(database=conf_database,user=conf_user,password=conf_password,host=conf_host,port=conf_port)
cursor = conn.cursor()
sql = config['command']['SQL_CRE_TABLE']+" "+file_name+" "+config['command']['SQL_COM1']
cursor.execute(sql)
conn.commit()
print("create table success")

with open(path+csv_file, 'r') as f:
    next(f)
    cursor.copy_from(f, file_name, sep=',')
    conn.commit()
    conn.close()

print("load data to postgres success")
f.close()