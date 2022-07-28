
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
import numpy as np
import db_connection as dbConn
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
#----------------------------------------------------- Author: AB MALIK --------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataGeoLocation():
    logger.info('Function \' GetAndInsertApiDataGeoLocation \' Started Off')
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

        url = 'http://58.65.177.12/api_who/api/get_alllocation/5468XE2LN6CzR7qRG041'
        logger.info('Requested Data From Api URL:  \' http://58.65.177.12/api_who/api/get_alllocation/5468XE2LN6CzR7qRG041 \' ')

        r = requests.get(url)
        data = r.json()
        logger.info('Received Data  From Api URL:  \' http://58.65.177.12/api_who/api/get_alllocation/5468XE2LN6CzR7qRG041 \' ')

        rowsData = data["data"]["data"]
        apiDataFrame = pd.DataFrame(rowsData)
        sql = " Create Table if not exists test.get_geolocation (ID Int32, code Int64, name String, type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
        client.execute(sql)

        df2 = client.query_dataframe(
            "SELECT * FROM test.get_geolocation")
        if df2.empty:
            # apiDataFrame = apiDataFrame.astype({"code": int})
            # apiDataFrame[["code"]] = apiDataFrame[["code"]].apply(pd.to_numeric, errors='coerce')

            # apiDataFrame['code'] = pd.to_numeric(apiDataFrame['code'])

            apiDataFrame['code'] = pd.to_numeric(apiDataFrame['code'], errors='coerce').fillna(-404)
            apiDataFrame['location_target'] = pd.to_numeric(apiDataFrame['location_target'], errors='coerce').fillna(-404)
            apiDataFrame['location_status'] = pd.to_numeric(apiDataFrame['location_status'], errors='coerce').fillna(-404)

            # df['Age'] = pd.to_numeric(df['Age'], errors='coerce')

            # apiDataFrame['code'] = pd.to_numeric(apiDataFrame['code'], errors='coerce')
            # apiDataFrame['code'] = apiDataFrame['code'].replace(0,np.NaN)

            
            # apiDataFrame[["location_target"]] = apiDataFrame[["location_target"]].apply(pd.to_numeric)

            client.insert_dataframe(
                'INSERT INTO test.get_geolocation VALUES', apiDataFrame)


def CreateJoinTableOfGeoLocation():
    logger.info(' Function  \' CreateJoinTableOfGeoLocation \' has been Initiated')
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
        sql = "CREATE TABLE if not exists test.xbi_geolocation (ID Int32, code Int64, name String, type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
        client.execute(sql)
        cols = "test.get_geolocation.ID, test.get_geolocation.code, test.get_geolocation.name, test.get_geolocation.type, test.get_geolocation.location_target, test.get_geolocation.location_status, test.get_geolocation.location_priority, test.get_geolocation.hr_status "
        sql = "SELECT " + cols + "  FROM test.get_geolocation "

        data = client.execute(sql)
        apiDataFrame = pd.DataFrame(data)
        all_columns = list(apiDataFrame)  # Creates list of all column headers
        cols = apiDataFrame.iloc[0]
        apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
        d = 'ID', 'code', 'name', 'type', 'location_target', 'location_status','location_priority','hr_status' 
        dff = pd.DataFrame(columns=d)
        for index, item in enumerate(d):
            dff[item] = apiDataFrame[index].values
        df2 = client.query_dataframe(
            "SELECT * FROM test.xbi_geolocation")
        if df2.empty:
            
            dff[["code"]] = dff[["code"]].apply(pd.to_numeric )
            dff[["location_target"]] = dff[["location_target"]].apply(pd.to_numeric )
            dff[["location_status"]] = dff[["location_status"]].apply(pd.to_numeric )
            # dff[["location_target"]] = dff[["location_target"]].apply(pd.to_numeric )
            client.insert_dataframe(
                'INSERT INTO test.xbi_geolocation  VALUES', dff)
            logger.info(
                ' Data has been inserted into Table\' INSERT INTO test.xbi_geolocation  VALUES \' ')

            sql = "DROP table if exists test.get_geolocation"
            client.execute(sql)

        else:
            df = pd.concat([dff, df2])
            df = df.astype('str')
            df = df.drop_duplicates(subset='ID',
                                    keep="first", inplace=False)
            sql = "DROP TABLE test.xbi_geolocation;"
            client.execute(sql)
            sql = "CREATE TABLE if not exists test.xbi_geolocation (ID Int32, code Int64, name String, type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
            client.execute(sql)
            
            df[["code"]] = df[["code"]].apply(pd.to_numeric )
            df[["location_target"]] = df[["location_target"]].apply(pd.to_numeric )
            df[["location_status"]] = df[["location_status"]].apply(pd.to_numeric )
            # df[["location_target"]] = df[["location_target"]].apply(pd.to_numeric )
            client.insert_dataframe(
                'INSERT INTO test.xbi_geolocation  VALUES', df)

            sql = "DROP table if exists test.get_geolocation"
            client.execute(sql)


dag = DAG(
    'Prod_GeoLocation_Automated',
    schedule_interval='0 0 * * 0', #Run once a week at midnight on Sunday morning
    #schedule_interval=None,
    #schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataGeoLocation = PythonOperator(
        task_id='GetAndInsertApiDataGeoLocation',
        python_callable=GetAndInsertApiDataGeoLocation,
    )
    CreateJoinTableOfGeoLocation = PythonOperator(
        task_id='CreateJoinTableOfGeoLocation',
        python_callable=CreateJoinTableOfGeoLocation,
    )
GetAndInsertApiDataGeoLocation >> CreateJoinTableOfGeoLocation
