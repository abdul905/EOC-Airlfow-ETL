
# from asyncio.windows_events import NULL
# from cmath import nan

from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import numpy as np
import sshtunnel as sshtunnel
from clickhouse_driver import connect
from clickhouse_driver import Client
import logging
import sshtunnel as sshtunnel

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

logger = logging.getLogger(__name__)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('logger-file_name')
c_handler.setLevel(logging.WARNING)
c_handler.setLevel(logging.INFO)
# f_handler.setLevel(logging.ERROR)
# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------- GENERATE DAG FILES  --------------------------------------------------------#
#------------------------------------------------ Author: AB MALIK -------------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataGeoLocationProv():
    logger.info('Function \' GetAndInsertApiDataGeoLocationProv \' Started Off')
    with sshtunnel.SSHTunnelForwarder(
        ('172.16.3.68', 22),
        ssh_username="root",
        ssh_password="COV!D@19#",
        remote_bind_address=('localhost', 9000)) as server:

        local_port = server.local_bind_port
        print(local_port)

        conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
        #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')
        
        cursor = conn.cursor()
        # cursor.execute('SHOW TABLES')
        # print(cursor.fetchall())

        client = Client(host='localhost',port=local_port, database='test',
                                user='default',
                                password='mm@1234',
                                settings={"use_numpy": True})

        url = 'https://www.eoc.gov.pk/api/geolocation/1?X-API-KEY=7b7aa78f506a40bc0320fa308efc19bf'
        logger.info('Requested Data From Api URL: '+url)

        r = requests.get(url)
        data = r.json()
        logger.info('Received Data  From Api URL: '+url)

        rowsData = data["data"]
        apiDataFrame = pd.DataFrame(rowsData)
        sql = " Create Table if not exists test.get_geolocation_province(id Int32, name String, code Int32, location_target Int32, location_status Int32, location_priority Int32, hr_status Int32, pcmname String, envname String)ENGINE = MergeTree PRIMARY KEY id ORDER BY id"
        client.execute(sql)

        df2 = client.query_dataframe(
            "SELECT * FROM test.get_geolocation_province")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.get_geolocation_province VALUES', apiDataFrame)       


def CreateJoinTableOfGeoLocationProv():
    logger.info(' Function  \' CreateJoinTableOfGeoLocationProv \' has been Initiated')
    with sshtunnel.SSHTunnelForwarder(
        ('172.16.3.68', 22),
        ssh_username="root",
        ssh_password="COV!D@19#",
        remote_bind_address=('localhost', 9000)) as server:

        local_port = server.local_bind_port
        print(local_port)

        conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
        #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')
        
        cursor = conn.cursor()
        # cursor.execute('SHOW TABLES')
        # print(cursor.fetchall())

        client = Client(host='localhost',port=local_port, database='test',
                                user='default',
                                password='mm@1234',
                                settings={"use_numpy": True})
        sql = "CREATE TABLE if not exists test.xbi_geolocation_province (id Int32, name String, code Int32, location_target Int32, location_status Int32, location_priority Int32, hr_status Int32, pcmname String, envname String)ENGINE = MergeTree PRIMARY KEY id ORDER BY id"
        client.execute(sql)
        cols = "test.get_geolocation_province.id, test.get_geolocation_province.name, test.get_geolocation_province.code, test.get_geolocation_province.location_target, test.get_geolocation_province.location_status, test.get_geolocation_province.location_priority, test.get_geolocation_province.hr_status,  test.get_geolocation_province.pcmname, test.get_geolocation_province.envname"
        sql = "SELECT " + cols + "  FROM test.get_geolocation_province"

        data = client.execute(sql)
        apiDataFrame = pd.DataFrame(data)
        all_columns = list(apiDataFrame)  # Creates list of all column headers
        cols = apiDataFrame.iloc[0]
        apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
        d = 'id', 'name', 'code', 'location_target', 'location_status', 'location_priority', 'hr_status', 'pcmname', 'envname'
        dff = pd.DataFrame(columns=d)
        for index, item in enumerate(d):
            dff[item] = apiDataFrame[index].values
        df2 = client.query_dataframe(
            "SELECT * FROM test.xbi_geolocation_province")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.xbi_geolocation_province  VALUES', dff)

            sql = "DROP table if exists test.get_geolocation_province"
            client.execute(sql)

        else:
            # df = pd.concat([dff, df2])
            # df = df.astype('str')
            # df = df.drop_duplicates(subset='id',
            #                         keep="first", inplace=False)
            # sql = "DROP TABLE if exists test.xbi_geolocation_province"
            # client.execute(sql)
            # sql = "CREATE TABLE if not exists test.xbi_geolocation_province (id Int32, name String, code Int32, location_target Int32, location_status Int32, location_priority Int32, hr_status Int32, pcmname String, envname String)ENGINE = MergeTree PRIMARY KEY id ORDER BY id"
            # client.execute(sql)
            sql = "TRUNCATE TABLE if exists test.xbi_geolocation_province"
            client.execute(sql)
            
            client.insert_dataframe(
                'INSERT INTO test.xbi_geolocation_province  VALUES', dff)

            sql = "DROP table if exists test.get_geolocation_province"
            client.execute(sql)


dag = DAG(
    'Prod_GeoLocation_Prov_Automated',
    #schedule_interval='0 0 * * 0', #Run once a week at midnight on Sunday morning
    schedule_interval=None,
    #schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataGeoLocationProv = PythonOperator(
        task_id='GetAndInsertApiDataGeoLocationProv',
        python_callable=GetAndInsertApiDataGeoLocationProv,
    )
    CreateJoinTableOfGeoLocationProv = PythonOperator(
        task_id='CreateJoinTableOfGeoLocationProv',
        python_callable=CreateJoinTableOfGeoLocationProv,
    )

GetAndInsertApiDataGeoLocationProv  >> CreateJoinTableOfGeoLocationProv      