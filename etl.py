import configparser
import redshift_connector
from sql_queries import sql_queries
from datetime import  datetime
def get_info(name)->str:
    configParser = configparser.RawConfigParser()
    configFilePath = 'dwh.cfg'
    configParser.read(configFilePath)
    details_dict = dict(configParser.items(name))
    return details_dict
redshift_info = get_info('redshift')
conn = redshift_connector.connect(
     host= redshift_info['host'],
     database= redshift_info['database'],
     port= int(redshift_info['port']),
     user= redshift_info['user'],
     password= redshift_info['password']
  )
cursor = conn.cursor()
# load data from s3 -> stg
start = datetime.now()
print('start load table to STG: song_data')
cursor.execute('truncate table public.song_data')
conn.commit()
cursor.execute(sql_queries['s3_to_redshift']['song_data'])
conn.commit()
end = datetime.now()
print(f'Succeed: song_data.Time:{str(end - start)}')
start = datetime.now()
print('start load table to STG: log_data')
cursor.execute('truncate table public.log_data')
conn.commit()
cursor.execute(sql_queries['s3_to_redshift']['log_data'])
conn.commit()
end = datetime.now()
print(f'Succeed: log_data.Time:{str(end - start)}')
# # load data from stg -> dwh
start = datetime.now()
print('start load data STG to DWH')
cursor.execute(sql_queries['stg_to_dwh'])
conn.commit()
end = datetime.now()
print(f'Succeed:  STG to DWH.Time:{str(end - start)}')