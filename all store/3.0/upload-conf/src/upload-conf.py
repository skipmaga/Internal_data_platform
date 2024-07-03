import configparser
from minio import Minio
import pyarrow.parquet as pq
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

minio_client = Minio(endpoint=conf_end+":"+conf_port,access_key=conf_acc_key,secret_key=conf_ser_key,secure=False)
minio_client.fput_object(conf_bucket_name, par_file, file_path)
print("load data to Minio success")