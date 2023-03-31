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
cursor.execute(sql_queries['create_tb'])
conn.commit()
