import psycopg2 as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq
import csv
import configparser

config = configparser.ConfigParser()
config.read('./conf/config.cfg')

conf_database=config['postgres']['DATABASE']
conf_user=config['postgres']['USER']
conf_password=config['postgres']['PASSWORD']
conf_host=config['postgres']['HOST']
conf_port=config['postgres']['PORT']

conf_sql_query=config['command']['SQL_TABLE']

conf_csv_path=config['path']['CSV_PATH2']

conn = pc.connect(database=conf_database,user=conf_user,password=conf_password,host=conf_host,port=conf_port)
cursor = conn.cursor()
cursor.execute(conf_sql_query)
table_name = cursor.fetchone()
print(table_name[0])
path = conf_csv_path+table_name[0]+config['path']['CSV_SUR']
sql = "select * from "+table_name[0]

print(path)
print(sql)

cursor.execute(sql)
df1 = cursor.fetchall()

fields = ['tps', 'data_time', 'bank_send', 'bank_receive', 'proxy_type', 'datatype']
rows = df1

with open(path, 'w') as f:
     
    write = csv.writer(f)
     
    write.writerow(fields)
    write.writerows(rows)

filename = path
table = pv.read_csv(filename)
pq.write_table(table, filename.replace('csv', 'parquet'))
