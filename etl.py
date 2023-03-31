import configparser
import redshift_connector
from sql_queries import sql_queries
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
cursor.execute('truncate table public.song_data')
conn.commit()
cursor.execute(sql_queries['s3_to_redshift']['song_data'])
conn.commit()
cursor.execute('truncate table public.log_data')
conn.commit()
cursor.execute(sql_queries['s3_to_redshift']['log_data'])
conn.commit()
# # load data from stg -> dwh
cursor.execute(sql_queries['stg_to_dwh'])
conn.commit()
conn.close()