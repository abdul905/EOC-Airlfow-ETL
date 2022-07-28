
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

        url = 'https://www.eoc.gov.pk/api/locations?X-API-KEY=7b7aa78f506a40bc0320fa308efc19bf'
        logger.info('Requested Data From Api URL: '+url)

        r = requests.get(url)
        data = r.json()
        logger.info('Received Data  From Api URL: '+url)

        rowsData = data["data"]
        apiDataFrame = pd.DataFrame(rowsData)
        sql = " Create Table if not exists test.get_geolocation_eoc(pname String,	dname String,	tname String,	name String,	type Int32,	code Int32,	x_code Int32,	comnet Int32,	tier Int32,	division Int32,	mtap Int32,	rspuc Int32,	draining_uc Int32,	shruc Int32,	issmt Int32,	iscbv Int32,	status Int32,	upap_districts Int32,	hr Int32,	fcm Int32,	priority Int32,	target Int32 )ENGINE = MergeTree PRIMARY KEY code ORDER BY code"
        client.execute(sql)

        df2 = client.query_dataframe(
            "SELECT * FROM test.get_geolocation_eoc")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.get_geolocation_eoc VALUES', apiDataFrame)


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
        sql = "CREATE TABLE if not exists test.eoc_geolocation_t (pname String,	dname String,	tname String,	name String,	type Int32,	code Int32,	x_code Int32,	comnet Int32,	tier Int32,	division Int32,	mtap Int32,	rspuc Int32,	draining_uc Int32,	shruc Int32,	issmt Int32,	iscbv Int32,	status Int32,	upap_districts Int32,	hr Int32,	fcm Int32,	priority Int32,	target Int32)ENGINE = MergeTree PRIMARY KEY code ORDER BY code"
        client.execute(sql)
        cols = "test.get_geolocation_eoc.pname,	test.get_geolocation_eoc.dname,	test.get_geolocation_eoc.tname,	test.get_geolocation_eoc.name,	test.get_geolocation_eoc.type,	test.get_geolocation_eoc.code,	test.get_geolocation_eoc.x_code,	test.get_geolocation_eoc.comnet,	test.get_geolocation_eoc.tier,	test.get_geolocation_eoc.division,	test.get_geolocation_eoc.mtap,	test.get_geolocation_eoc.rspuc,	test.get_geolocation_eoc.draining_uc,	test.get_geolocation_eoc.shruc,	test.get_geolocation_eoc.issmt,	test.get_geolocation_eoc.iscbv,	test.get_geolocation_eoc.status,	test.get_geolocation_eoc.upap_districts,	test.get_geolocation_eoc.hr,	test.get_geolocation_eoc.fcm,	test.get_geolocation_eoc.priority,	test.get_geolocation_eoc.target"
        sql = "SELECT " + cols + "  FROM test.get_geolocation_eoc"

        data = client.execute(sql)
        apiDataFrame = pd.DataFrame(data)
        all_columns = list(apiDataFrame)  # Creates list of all column headers
        cols = apiDataFrame.iloc[0]
        apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
        d = 'pname',	'dname',	'tname',	'name',	'type',	'code',	'x_code',	'comnet',	'tier',	'division',	'mtap',	'rspuc',	'draining_uc',	'shruc',	'issmt',	'iscbv',	'status',	'upap_districts',	'hr',	'fcm',	'priority',	'target'

        dff = pd.DataFrame(columns=d)
        for index, item in enumerate(d):
            dff[item] = apiDataFrame[index].values
        df2 = client.query_dataframe(
            "SELECT * FROM test.eoc_geolocation_t")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.eoc_geolocation_t  VALUES', dff)

            sql = "DROP table if exists test.get_geolocation_eoc"
            client.execute(sql)

        else:
            sql = "TRUNCATE TABLE if exists test.eoc_geolocation_t"
            client.execute(sql)
            # df = pd.concat([dff, df2])
            # df = df.astype('str')
            # df = df.drop_duplicates(subset='ID',
            #                         keep="first", inplace=False)
            # sql = "DROP TABLE test.eoc_geolocation_t;"
            # client.execute(sql)
            # sql = "CREATE TABLE if not exists test.eoc_geolocation_t (pname String,	dname String,	tname String,	name String,	type Int32,	code Int32,	x_code Int32,	comnet Int32,	tier Int32,	division Int32,	mtap Int32,	rspuc Int32,	draining_uc Int32,	shruc Int32,	issmt Int32,	iscbv Int32,	status Int32,	upap_districts Int32,	hr Int32,	fcm Int32,	priority Int32,	target Int32)ENGINE = MergeTree PRIMARY KEY code ORDER BY code"
            # client.execute(sql)

            client.insert_dataframe(
                'INSERT INTO test.eoc_geolocation_t  VALUES', dff)

            sql = "DROP table if exists test.get_geolocation_eoc"
            client.execute(sql)


dag = DAG(
    'Prod_GeoLocation_eoc_Automated',
    #schedule_interval='0 0 * * 0', #Run once a week at midnight on Sunday morning
    schedule_interval=None,
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
