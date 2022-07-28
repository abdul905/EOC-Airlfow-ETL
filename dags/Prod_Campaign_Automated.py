
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
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

def GetAndInsertApiDataCampaign():
    logger.info('Function \' GetAndInsertApiDataCampaign \' Started Off')
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

        url = 'http://58.65.177.12/api_who/api/get_allactivity/Al68XE2L3N6CzR7qRR1'
        logger.info('Requested Data From Api URL:  \' http://58.65.177.12/api_who/api/get_allactivity/Al68XE2L3N6CzR7qRR1 \' ')

        r = requests.get(url)
        data = r.json()
        logger.info('Received Data  From Api URL:  \' http://58.65.177.12/api_who/api/get_allactivity/Al68XE2L3N6CzR7qRR1 \' ')

        rowsData = data["data"]["data"]
        apiDataFrame = pd.DataFrame(rowsData)
        sql = " Create Table if not exists test.get_campaign (ID Int32, ActivityName String, ActivityID_old Int32, Yr Int32, SubActivityName String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
        client.execute(sql)

        df2 = client.query_dataframe(
            "SELECT * FROM test.get_campaign")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.get_campaign VALUES', apiDataFrame)


def CreateJoinTableOfCampaign():
    logger.info(' Function  \' CreateJoinTableOfCampaign \' has been Initiated')
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
        sql = "CREATE TABLE if not exists test.xbi_campaign (campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String)ENGINE = MergeTree PRIMARY KEY campaign_ID ORDER BY campaign_ID"
        client.execute(sql)
        cols = "test.get_campaign.ID, test.get_campaign.ActivityName, test.get_campaign.ActivityID_old, test.get_campaign.Yr, test.get_campaign.SubActivityName "
        sql = "SELECT " + cols + "  FROM test.get_campaign campn  "

        data = client.execute(sql)
        apiDataFrame = pd.DataFrame(data)
        all_columns = list(apiDataFrame)  # Creates list of all column headers
        cols = apiDataFrame.iloc[0]
        apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
        d = 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName' 
        dff = pd.DataFrame(columns=d)
        for index, item in enumerate(d):
            dff[item] = apiDataFrame[index].values
        df2 = client.query_dataframe(
            "SELECT * FROM test.xbi_campaign")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.xbi_campaign  VALUES', dff)
            logger.info(
                ' Data has been inserted into Table\' INSERT INTO test.xbi_campaign  VALUES \' ')

            sql = "DROP table if exists test.get_campaign"
            client.execute(sql)

        else:
            df = pd.concat([dff, df2])
            df = df.astype('str')
            df = df.drop_duplicates(subset='campaign_ID',
                                    keep="first", inplace=False)
            sql = "DROP TABLE test.xbi_campaign;"
            client.execute(sql)
            sql = "CREATE TABLE if not exists test.xbi_campaign (campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String)ENGINE = MergeTree PRIMARY KEY campaign_ID ORDER BY campaign_ID"
            client.execute(sql)
            client.insert_dataframe(
                'INSERT INTO test.xbi_campaign  VALUES', df)

            sql = "DROP table if exists test.get_campaign"
            client.execute(sql)


dag = DAG(
    'Prod_Campaign_Automated',
    schedule_interval='0 0 * * 0', #Run once a week at midnight on Sunday morning
    #schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval=None,
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataCampaign = PythonOperator(
        task_id='GetAndInsertApiDataCampaign',
        python_callable=GetAndInsertApiDataCampaign,
    )
    CreateJoinTableOfCampaign = PythonOperator(
        task_id='CreateJoinTableOfCampaign',
        python_callable=CreateJoinTableOfCampaign,
    )
GetAndInsertApiDataCampaign >> CreateJoinTableOfCampaign
